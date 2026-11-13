package ksink

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testOpenWALWithActiveTransaction(t *testing.T, root string) (*TransactionLog, string) {
	t.Helper()
	wal, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.NoError(t, wal.BindOutput(filepath.Join(root, "{topic}-{txnID}.bin")))
	instance, err := newTxnInstance("orders-producer")
	require.NoError(t, err)
	snap := wal.CurrentState()
	snap.States["orders-producer"] = storedTxnState{
		ProducerID: snap.Counter, Epoch: 1, Active: true, Instance: instance,
		Sequences: make(map[string]txnPartitionSequence),
	}
	require.NoError(t, wal.AppendState(snap))
	return wal, instance
}

func TestTransactionWALReplayAndBinding(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("first\n")))
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("second\n")))
	require.ErrorContains(t, wal.BindOutput(filepath.Join(root, "different-{txnID}.bin")), "binding changed")
	require.NoError(t, wal.Close())

	restarted, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = restarted.Close() }()
	state := restarted.CurrentState()
	require.True(t, state.States["orders-producer"].Active)
	transactions := restarted.RecoveryTransactions()
	require.Len(t, transactions, 1)
	require.Equal(t, instance, transactions[0].Instance)
	require.Empty(t, transactions[0].Decision)
	require.Len(t, transactions[0].Records, 2)
	require.Equal(t, []byte("first\n"), transactions[0].Records[0].Data)
	require.Equal(t, []byte("second\n"), transactions[0].Records[1].Data)

	// Returned state is isolated from the authoritative log.
	delete(state.States, "orders-producer")
	require.Contains(t, restarted.CurrentState().States, "orders-producer")
}

func TestTransactionWALIrreversibleCommitAndCompaction(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("payload")))
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.Equal(t, "commit", wal.Decision("orders-producer", instance))
	require.ErrorContains(t, wal.MarkAborted("orders-producer", instance), "cannot abort")
	require.NoError(t, wal.MarkPublished("orders-producer", instance))
	require.True(t, wal.Published(instance))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	recovery := next.RecoveryTransactions()
	require.Len(t, recovery, 1)
	require.Equal(t, "commit", recovery[0].Decision)
	require.True(t, recovery[0].Published)
	require.True(t, recovery[0].PublishedHasOutput)
	require.Empty(t, recovery[0].Records, "published data can be reclaimed after checkpoint")
	require.ErrorContains(t, next.MarkAborted("orders-producer", instance), "cannot abort")
}

func TestTransactionWALCheckpointPreservesUnpublishedPayload(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	data := []byte("replay me")
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", data))
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	recovery := next.RecoveryTransactions()
	require.Len(t, recovery, 1)
	require.Equal(t, "commit", recovery[0].Decision)
	require.False(t, recovery[0].Published)
	require.Len(t, recovery[0].Records, 1)
	require.Equal(t, data, recovery[0].Records[0].Data)
}

func TestTransactionWALTornTailTruncatedAndChecksumCorruptionRejected(t *testing.T) {
	root := t.TempDir()
	wal, _ := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.Close())
	path := filepath.Join(root, ".ksink-kafka-state", "transactions.wal")
	before, err := os.Stat(path)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 40, 0}) // torn header
	require.NoError(t, err)
	require.NoError(t, f.Close())

	again, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.NoError(t, again.Close())
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size(), "recover only complete checksummed records")

	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], 3)
	_, err = f.Write(append(hdr[:], []byte("bad")...)) // full, corrupt frame
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestTransactionWALRejectsUndecidedPublicationAndBadOutputPath(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()
	require.ErrorContains(t, wal.MarkPublished("orders-producer", instance), "no durable commit decision")
	require.ErrorContains(t, wal.AppendOutput("orders-producer", instance, "../escaped", []byte("x")), "invalid")
	require.ErrorContains(t, wal.AppendOutput("orders-producer", instance, "/absolute", []byte("x")), "invalid")
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "safe.bin", []byte("x")))
	require.ErrorContains(t, wal.MarkPublished("orders-producer", "another-instance"), "no durable commit decision")
}

func TestTransactionWALMissingEstablishedLogFailsClosed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".ksink-kafka-state")
	wal, err := OpenTransactionLog(dir)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	walPath := filepath.Join(dir, "transactions.wal")
	require.NoError(t, os.Remove(walPath))
	_, err = OpenTransactionLog(dir)
	require.ErrorContains(t, err, "established transaction WAL is missing")

	// A zero-length replacement also cannot be mistaken for a fresh store.
	require.NoError(t, os.WriteFile(walPath, nil, 0600))
	_, err = OpenTransactionLog(dir)
	require.ErrorContains(t, err, "established transaction WAL is missing or empty")
}
