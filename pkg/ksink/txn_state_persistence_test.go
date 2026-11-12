package ksink

import (
	"context"
	"encoding/binary"
	"os"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func recordBatchForSequence(pid int64, epoch int16, base, count int32, discriminator byte) []byte {
	b := make([]byte, 61)
	binary.BigEndian.PutUint32(b[8:12], 49)
	b[16] = 2
	b[21] = discriminator // content fingerprint (must be identical on retry)
	binary.BigEndian.PutUint64(b[43:51], uint64(pid))
	binary.BigEndian.PutUint16(b[51:53], uint16(epoch))
	binary.BigEndian.PutUint32(b[53:57], uint32(base))
	binary.BigEndian.PutUint32(b[57:61], uint32(count))
	return b
}

func TestTxnSequenceDedupAcrossTransactions(t *testing.T) {
	state := make(map[string]txnPartitionSequence)
	key := txnSequenceKey("orders", 3)
	first := recordBatchForSequence(99, 2, 0, 2, 0)
	second := recordBatchForSequence(99, 2, 2, 3, 0)
	bad := recordBatchForSequence(99, 2, 1, 1, 0)

	data, code := acceptTxnBatches(first, key, state)
	require.Zero(t, code)
	require.Equal(t, first, data)
	data, code = acceptTxnBatches(first, key, state)
	require.Zero(t, code)
	require.Empty(t, data, "identical retry must not be delivered twice")
	data, code = acceptTxnBatches(second, key, state)
	require.Zero(t, code)
	require.Equal(t, second, data)
	require.Equal(t, int32(5), state[key].Next)

	// The second sequence belongs to a new Kafka transaction with the SAME
	// producer epoch; sequence state must not reset on EndTxn.
	_, code = acceptTxnBatches(bad, key, state)
	require.Equal(t, kerr.OutOfOrderSequenceNumber.Code, code)
	_, code = acceptTxnBatches(recordBatchForSequence(99, 2, 0, 2, 1), key, state)
	require.Equal(t, kerr.OutOfOrderSequenceNumber.Code, code,
		"same sequence with different bytes cannot be treated as a duplicate")
	_, code = acceptTxnBatches(recordBatchForSequence(99, 2, 5, 0, 0), key, state)
	require.Equal(t, kerr.CorruptMessage.Code, code)

	otherPartition := txnSequenceKey("orders", 4)
	data, code = acceptTxnBatches(first, otherPartition, state)
	require.Zero(t, code)
	require.Equal(t, first, data, "sequences are per partition")
}

func TestTxnSequencesMixedRetryAndNewBatch(t *testing.T) {
	state := make(map[string]txnPartitionSequence)
	key := txnSequenceKey("topic", 0)
	one := recordBatchForSequence(1, 0, 0, 1, 1)
	two := recordBatchForSequence(1, 0, 1, 1, 2)
	_, code := acceptTxnBatches(one, key, state)
	require.Zero(t, code)
	combined := append(append([]byte{}, one...), two...)
	accepted, code := acceptTxnBatches(combined, key, state)
	require.Zero(t, code)
	require.Equal(t, two, accepted)
	_, code = acceptTxnBatches(two[:20], key, state)
	require.Equal(t, kerr.CorruptMessage.Code, code)
	require.Equal(t, int32(2), state[key].Next)
}

func TestDurableTxnStoreRestartFencesOldEpoch(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: true, TransactionStateDir: dir}
	first, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Start(context.Background()))
	startReadLoop(t, first)
	conn := newTxnStateTestConn(t, first)
	txnID := "restart-fencing"
	init := sendInitProducerID(t, conn, 1, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	add := addPartitionsTxnResponse(t, conn, 2, txnID, init.ProducerID, init.ProducerEpoch)
	require.Zero(t, add.Topics[0].Partitions[0].ErrorCode)
	require.NoError(t, conn.Close())
	require.NoError(t, first.Close(context.Background()))

	restarted, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, restarted.Start(context.Background()))
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	startReadLoop(t, restarted)
	conn2 := newTxnStateTestConn(t, restarted)
	defer func() { _ = conn2.Close() }()

	// Restart conservatively aborts all transactions without a completed
	// commit; an old PID/epoch cannot open another transaction.
	stale := addPartitionsTxnResponse(t, conn2, 3, txnID, init.ProducerID, init.ProducerEpoch)
	require.Equal(t, kerr.ProducerFenced.Code, stale.Topics[0].Partitions[0].ErrorCode)
	end := sendEndTxnForSafetyTest(t, conn2, 4, txnID, init.ProducerID, init.ProducerEpoch, true)
	require.Equal(t, kerr.InvalidTxnState.Code, end.ErrorCode)

	renewed := sendInitProducerID(t, conn2, 5, &txnID, 30000)
	require.Zero(t, renewed.ErrorCode)
	require.Equal(t, init.ProducerID, renewed.ProducerID)
	require.Equal(t, init.ProducerEpoch+1, renewed.ProducerEpoch)
	stale = addPartitionsTxnResponse(t, conn2, 6, txnID, init.ProducerID, init.ProducerEpoch)
	require.Equal(t, kerr.ProducerFenced.Code, stale.Topics[0].Partitions[0].ErrorCode)
	fresh := addPartitionsTxnResponse(t, conn2, 7, txnID, renewed.ProducerID, renewed.ProducerEpoch)
	require.Zero(t, fresh.Topics[0].Partitions[0].ErrorCode)
}

func TestDurableTxnCompletedEndTxnRetryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: true, TransactionStateDir: dir}
	first, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Start(context.Background()))
	startReadLoop(t, first)
	conn := newTxnStateTestConn(t, first)
	txnID := "completed-before-restart"
	init := sendInitProducerID(t, conn, 1, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	require.Zero(t, addPartitionsTxnResponse(t, conn, 2, txnID, init.ProducerID, init.ProducerEpoch).Topics[0].Partitions[0].ErrorCode)
	require.Zero(t, sendEndTxnForSafetyTest(t, conn, 3, txnID, init.ProducerID, init.ProducerEpoch, true).ErrorCode)
	require.NoError(t, conn.Close())
	require.NoError(t, first.Close(context.Background()))

	restarted, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, restarted.Start(context.Background()))
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	conn2 := newTxnStateTestConn(t, restarted)
	defer func() { _ = conn2.Close() }()

	// Even without a read loop a duplicate EndTxn must return immediately:
	// its acknowledged outcome was loaded, so no event is emitted again.
	require.Zero(t, sendEndTxnForSafetyTest(t, conn2, 4, txnID, init.ProducerID, init.ProducerEpoch, true).ErrorCode)
	require.Equal(t, kerr.InvalidTxnState.Code, sendEndTxnForSafetyTest(
		t, conn2, 5, txnID, init.ProducerID, init.ProducerEpoch, false).ErrorCode)
}

func TestDurableTxnStateLockAndCorruption(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: true, TransactionStateDir: dir}
	first, err := New(cfg)
	require.NoError(t, err)
	_, err = New(cfg)
	require.ErrorContains(t, err, "exclusive coordinator lock")
	require.NoError(t, first.Close(context.Background()))

	reopened, err := New(cfg)
	require.NoError(t, err)
	reopened.txnMu.Lock()
	reopened.producerIDCounter.Store(17)
	require.NoError(t, reopened.persistTxnStateLocked())
	reopened.txnMu.Unlock()
	require.NoError(t, reopened.Close(context.Background()))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), []byte("{broken"), 0600))
	_, err = New(cfg)
	require.ErrorContains(t, err, "decode coordinator state")
	// Corrupted state is not silently replaced with a blank snapshot.
	contents, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, "{broken", string(contents))
}

func TestDurableTxnSequenceSnapshotSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: true, TransactionStateDir: dir}
	first, err := New(cfg)
	require.NoError(t, err)
	first.txnMu.Lock()
	first.producerIDCounter.Store(72)
	first.txnStates["sequence-persist"] = &txnState{
		producerID: 72, epoch: 5,
		sequences: map[string]txnPartitionSequence{
			txnSequenceKey("orders", 1): {
				Next: 9,
				Recent: []txnBatchSignature{
					{Base: 6, Count: 3, Checksum: "abc"},
				},
			},
		},
	}
	require.NoError(t, first.persistTxnStateLocked())
	first.txnMu.Unlock()
	require.NoError(t, first.Close(context.Background()))

	restarted, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = restarted.Close(context.Background()) }()
	require.Equal(t, int64(72), restarted.producerIDCounter.Load())
	restarted.txnMu.Lock()
	restored := restarted.txnStates["sequence-persist"]
	require.NotNil(t, restored)
	require.Equal(t, int16(5), restored.epoch)
	require.True(t, restored.restartRequired)
	require.Equal(t, int32(9), restored.sequences[txnSequenceKey("orders", 1)].Next)
	require.Equal(t, "abc", restored.sequences[txnSequenceKey("orders", 1)].Recent[0].Checksum)
	restarted.txnMu.Unlock()
}

func TestDurableTxnSnapshotFailureFencesFurtherOperations(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: true, TransactionStateDir: dir}
	srv, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = srv.Close(context.Background()) }()
	// A directory at the temporary snapshot path prevents creating the
	// next journal checkpoint. The store must fail closed.
	require.NoError(t, os.Mkdir(filepath.Join(dir, "state.json.tmp"), 0700))
	srv.txnMu.Lock()
	srv.producerIDCounter.Add(1)
	require.Error(t, srv.persistTxnStateLocked())
	require.Error(t, srv.persistTxnStateLocked(), "the same instance must never recover silently from an uncertain snapshot")
	srv.txnMu.Unlock()
}

func newTxnStateTestConn(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	api := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 0, api.AppendTo(nil))
	return conn
}

func addPartitionsTxnResponse(t *testing.T, conn net.Conn, id int32, txnID string, pid int64, epoch int16) *kmsg.AddPartitionsToTxnResponse {
	t.Helper()
	req := kmsg.NewAddPartitionsToTxnRequest()
	req.Version = 0
	req.TransactionalID = txnID
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	topic := kmsg.NewAddPartitionsToTxnRequestTopic()
	topic.Topic = "orders"
	topic.Partitions = []int32{0}
	req.Topics = append(req.Topics, topic)
	reply := sendRawRequest(t, conn, int16(kmsg.AddPartitionsToTxn), 0, id, req.AppendTo(nil))
	response := &kmsg.AddPartitionsToTxnResponse{Version: 0}
	require.NoError(t, response.ReadFrom(reply))
	return response
}
