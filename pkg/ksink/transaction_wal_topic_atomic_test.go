package ksink

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedWALBatchAcceptsProducerSnapshotAtomically(t *testing.T) {
	dir := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, dir)
	snapshot := wal.CurrentState()
	state := snapshot.States["orders-producer"]
	state.Sequences = map[string]txnPartitionSequence{
		txnSequenceKey("orders", 0): {Next: 2},
	}
	snapshot.States["orders-producer"] = state
	offsets, err := wal.appendTopicBatchWithState([]WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("first")},
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("second")},
	}, snapshot)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, offsets)
	require.Equal(t, int32(2), wal.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	recovered, err := OpenTransactionLog(filepath.Join(dir, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = recovered.Close() }()
	require.Equal(t, int64(2), recovered.NextTopicOffset("orders"))
	require.Equal(t, int32(2), recovered.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	for i, value := range []string{"first", "second"} {
		rec, exists, err := recovered.ReadTopicRecord("orders", int64(i))
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, value, string(rec.Data))
	}
}

func TestSharedWALUnacceptedBatchDoesNotAdvanceProducerCheckpoint(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	initial := wal.CurrentState()
	snapshot := cloneTxnStateSnapshot(initial)
	st := snapshot.States["orders-producer"]
	st.Sequences = map[string]txnPartitionSequence{txnSequenceKey("orders", 0): {Next: 1}}
	snapshot.States["orders-producer"] = st

	// An accepted producer state is allowed only inside the topic acceptance
	// delimiter. Crash after a complete record frame but before that marker.
	frame, err := txnWALFrame(transactionWALRecord{
		Type: "topic-output", TopicOutput: &WALTopicRecord{
			Topic: "orders", Offset: 0, TransactionID: "orders-producer",
			Instance: instance, Data: []byte("not-accepted"),
		},
	})
	require.NoError(t, err)
	_, err = wal.logFile.Write(frame)
	require.NoError(t, err)
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())
	recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = recovered.Close() }()
	require.Zero(t, recovered.NextTopicOffset("orders"))
	require.Empty(t, recovered.CurrentState().States["orders-producer"].Sequences)
	offsets, err := recovered.appendTopicBatchWithState([]WALTopicRecord{{
		Topic: "orders", TransactionID: "orders-producer",
		Instance: instance, Data: []byte("accepted"),
	}}, snapshot)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, offsets)
	require.Equal(t, int32(1), recovered.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
}

func TestSharedWALRecoversCommitDecisionWhenSeparateMarkerMissing(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	_, err := wal.AppendTopicBatch([]WALTopicRecord{{
		Topic: "orders", TransactionID: "orders-producer",
		Instance: instance, Data: []byte("payload"),
	}})
	require.NoError(t, err)
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	// Simulate a crash after the complete persisted coordinator state but
	// before a separate topic-decision history marker is written.
	frame, err := txnWALFrame(transactionWALRecord{Type: "state", State: &state})
	require.NoError(t, err)
	_, err = wal.logFile.Write(frame)
	require.NoError(t, err)
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())

	recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.Equal(t, "commit", recovered.TopicTransactionDecision(instance))
	require.Equal(t, int64(1), recovered.TopicStableOffset("orders"))
	require.NoError(t, recovered.Compact())
	require.NoError(t, recovered.Close())
	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	require.Equal(t, "commit", next.TopicTransactionDecision(instance))
	require.Equal(t, int64(1), next.TopicStableOffset("orders"))
}

func TestSharedWALCheckpointReplayChunksLargeTopicStream(t *testing.T) {
	root := t.TempDir()
	wal, _ := testOpenWALWithActiveTransaction(t, root)
	records := make([]WALTopicRecord, 1100)
	for i := range records {
		records[i] = WALTopicRecord{
			Topic: "orders", Data: []byte(fmt.Sprintf("message-%04d", i)),
		}
	}
	offsets, err := wal.AppendTopicBatch(records)
	require.NoError(t, err)
	require.Len(t, offsets, len(records))
	require.Equal(t, int64(1099), offsets[1099])
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())
	loaded, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = loaded.Close() }()
	require.Equal(t, int64(1100), loaded.NextTopicOffset("orders"))
	for _, index := range []int64{0, 511, 512, 1023, 1024, 1099} {
		record, ok, err := loaded.ReadTopicRecord("orders", index)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("message-%04d", index), string(record.Data))
	}
}

func TestSharedWALFormatsAfterOffsetAssignmentWithoutConsumingFailedOffsets(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Sequences = map[string]txnPartitionSequence{
		txnSequenceKey("orders", 0): {Next: 2},
	}
	state.States["orders-producer"] = st
	records := []WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance},
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance},
	}
	_, err := wal.appendTopicBatchFormattedWithState(records, state, func(i int, offset int64) ([]byte, error) {
		if i == 1 { return nil, fmt.Errorf("format failed") }
		return []byte(fmt.Sprintf("%d", offset)), nil
	})
	require.ErrorContains(t, err, "format failed")
	require.Zero(t, wal.NextTopicOffset("orders"))
	require.Empty(t, wal.CurrentState().States["orders-producer"].Sequences,
		"format failure cannot checkpoint producer sequence state")

	offsets, err := wal.appendTopicBatchFormattedWithState(records, state,
		func(i int, offset int64) ([]byte, error) {
			return []byte(fmt.Sprintf("record-%d-offset-%d", i, offset)), nil
		})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, offsets)
	require.Equal(t, int32(2), wal.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	for i := range records {
		got, exists, readErr := wal.ReadTopicRecord("orders", int64(i))
		require.NoError(t, readErr)
		require.True(t, exists)
		require.Equal(t, fmt.Sprintf("record-%d-offset-%d", i, i), string(got.Data))
	}
}
