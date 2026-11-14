package ksink

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

func newOrderedTestServer(t *testing.T, log *TransactionLog) *Server {
	t.Helper()
	srv, err := New(Config{Address: "127.0.0.1:0", TransactionalWrite: true},
		WithTransactionLog(log), WithOrderedTransactionalOutput(func(m *Message) ([]byte, error) {
			return []byte(fmt.Sprintf("%s/%d:%s\n", m.Topic, m.Offset, m.Value)), nil
		}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	return srv
}

func TestOrderedModeZombieFencingAndCommitWithoutReadEvents(t *testing.T) {
	root := t.TempDir()
	wal, err := OpenTransactionLog(filepath.Join(root, "state"))
	require.NoError(t, err)
	require.NoError(t, wal.BindOutput(filepath.Join(root, "{topic}", "messages-{seq:08}.jsonl")))
	srv := newOrderedTestServer(t, wal)
	conn := newTxnStateTestConn(t, srv)
	txnID := "fencing-without-consumer"
	init := sendInitProducerID(t, conn, 1, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	sendAddPartitionsToTxn(t, conn, 2, txnID, init.ProducerID, init.ProducerEpoch, "orders")

	// A replacement producer must be able to fence the old one without a
	// separate Read loop or a completed output file.
	next := sendInitProducerID(t, conn, 3, &txnID, 30000)
	require.Zero(t, next.ErrorCode)
	require.Equal(t, init.ProducerEpoch+1, next.ProducerEpoch)
	sendAddPartitionsToTxn(t, conn, 4, txnID, next.ProducerID, next.ProducerEpoch, "orders")
	require.Zero(t, sendEndTxnForSafetyTest(t, conn, 5, txnID,
		next.ProducerID, next.ProducerEpoch, true).ErrorCode)

	require.NoError(t, conn.Close())
	require.NoError(t, srv.Close(context.Background()))
	require.NoError(t, wal.Close())
}

func TestOrderedModeRestartDurablyAbortsUndecidedTopicRecords(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	wal, err := OpenTransactionLog(stateDir)
	require.NoError(t, err)
	require.NoError(t, wal.BindOutput(filepath.Join(root, "{topic}", "messages-{seq:08}.jsonl")))
	srv := newOrderedTestServer(t, wal)
	conn := newTxnStateTestConn(t, srv)
	txnID := "aborted-on-restart"
	init := sendInitProducerID(t, conn, 1, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	sendAddPartitionsToTxn(t, conn, 2, txnID, init.ProducerID, init.ProducerEpoch, "orders")
	wal.mu.Lock()
	instance := wal.state.States[txnID].Instance
	wal.mu.Unlock()
	_, err = wal.AppendTopicBatch([]WALTopicRecord{{
		Topic: "orders", TransactionID: txnID, Instance: instance,
		Data: []byte("uncommitted"),
	}})
	require.NoError(t, err)
	require.Zero(t, wal.TopicStableOffset("orders"))

	require.NoError(t, conn.Close())
	require.NoError(t, srv.Close(context.Background()))
	require.NoError(t, wal.Close())

	recovered, err := OpenTransactionLog(stateDir)
	require.NoError(t, err)
	next := newOrderedTestServer(t, recovered)
	defer func() { _ = next.Close(context.Background()); _ = recovered.Close() }()
	require.Equal(t, "abort", recovered.TopicTransactionDecision(instance))
	require.Equal(t, int64(2), recovered.TopicStableOffset("orders"),
		"restart ABORT marker consumes offset 1")
	connection := newTxnStateTestConn(t, next)
	defer func() { _ = connection.Close() }()
	// Restarted producer must reinitialize, not resume the old epoch.
	invalid := addPartitionsTxnResponse(t, connection, 3, txnID, init.ProducerID, init.ProducerEpoch)
	require.Equal(t, kerr.ProducerFenced.Code, invalid.Topics[0].Partitions[0].ErrorCode)
	renewed := sendInitProducerID(t, connection, 4, &txnID, 30000)
	require.Zero(t, renewed.ErrorCode)
	require.Equal(t, init.ProducerEpoch+1, renewed.ProducerEpoch)
}
