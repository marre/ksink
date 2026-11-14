package ksink

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedWALTopicStreamsIndependentOffsetsAcrossReplayAndCompaction(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	records := []WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("created\n")},
		{Topic: "payments", TransactionID: "orders-producer", Instance: instance, Data: []byte("paid\n")},
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("updated\n")},
	}
	offsets, err := wal.AppendTopicBatch(records)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0, 1}, offsets)
	require.Equal(t, int64(2), wal.NextTopicOffset("orders"))
	require.Equal(t, int64(1), wal.NextTopicOffset("payments"))

	// Reusing the same WAL input slice must not alter persisted bytes.
	records[0].Data[0] = 'X'
	for _, tt := range []struct {
		topic string
		offset int64
		want string
	}{
		{"orders", 0, "created\n"},
		{"payments", 0, "paid\n"},
		{"orders", 1, "updated\n"},
	} {
		record, ok, err := wal.ReadTopicRecord(tt.topic, tt.offset)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, tt.want, string(record.Data))
		require.Equal(t, tt.topic, record.Topic)
		require.Equal(t, tt.offset, record.Offset)
	}
	_, found, err := wal.ReadTopicRecord("orders", 2)
	require.NoError(t, err)
	require.False(t, found)

	// A committed but not yet materialized transaction must not be forgotten
	// after checkpointing or after a producer starts another generation.
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	reopened, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	require.Equal(t, int64(2), reopened.NextTopicOffset("orders"))
	require.Equal(t, int64(1), reopened.NextTopicOffset("payments"))
	for _, tt := range []struct{topic string; offset int64; want string}{
		{"orders", 0, "created\n"}, {"payments", 0, "paid\n"}, {"orders", 1, "updated\n"},
	} {
		record, ok, readErr := reopened.ReadTopicRecord(tt.topic, tt.offset)
		require.NoError(t, readErr)
		require.True(t, ok)
		require.Equal(t, tt.want, string(record.Data))
	}
}

func TestSharedWALTopicBatchAtomicValidation(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()

	_, err := wal.AppendTopicBatch([]WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("valid")},
		{Topic: "", Data: []byte("invalid")},
	})
	require.ErrorContains(t, err, "invalid WAL topic")
	require.Zero(t, wal.NextTopicOffset("orders"), "invalid batches must not reserve offsets")

	_, err = wal.AppendTopicBatch([]WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("valid")},
		{Topic: "payments", TransactionID: "orders-producer", Instance: "wrong-instance", Data: []byte("invalid")},
	})
	require.ErrorContains(t, err, "invalid transaction instance")
	require.Zero(t, wal.NextTopicOffset("orders"))

	// Nontransactional messages can participate in the same topic stream.
	offsets, err := wal.AppendTopicBatch([]WALTopicRecord{
		{Topic: "orders", Data: []byte("nontransactional")},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0}, offsets)
	r, ok, err := wal.ReadTopicRecord("orders", 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, r.TransactionID)
	require.Equal(t, "nontransactional", string(r.Data))
}

func TestSharedWALTopicOffsetOverflowFailsSafely(t *testing.T) {
	root := t.TempDir()
	wal, _ := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()
	wal.mu.Lock()
	wal.topicNext["orders"] = math.MaxInt64
	wal.mu.Unlock()
	_, err := wal.AppendTopicBatch([]WALTopicRecord{{Topic: "orders", Data: []byte("no")}})
	require.ErrorContains(t, err, "exhausted")
}

func TestSharedWALReadCommittedStableOffsetAndGenerationReuse(t *testing.T) {
	root := t.TempDir()
	wal, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.NoError(t, wal.BindOutput(filepath.Join(root, "data", "{topic}", "messages-{seq:08}.jsonl")))

	aInstance, err := newTxnInstance("A")
	require.NoError(t, err)
	bInstance, err := newTxnInstance("B")
	require.NoError(t, err)
	state := wal.CurrentState()
	state.States["A"] = storedTxnState{ProducerID: state.Counter, Epoch: 0, Active: true, Instance: aInstance}
	state.States["B"] = storedTxnState{ProducerID: state.Counter, Epoch: 0, Active: true, Instance: bInstance}
	require.NoError(t, wal.AppendState(state))
	app := func(topic, id, instance, value string) {
		t.Helper()
		_, appendErr := wal.AppendTopicBatch([]WALTopicRecord{{
			Topic: topic, TransactionID: id, Instance: instance, Data: []byte(value),
		}})
		require.NoError(t, appendErr)
	}
	app("orders", "A", aInstance, "created")
	app("orders", "B", bInstance, "paid")
	app("orders", "A", aInstance, "updated")
	_, err = wal.AppendTopicBatch([]WALTopicRecord{{Topic: "payments", Data: []byte("immediate")}})
	require.NoError(t, err)
	require.Equal(t, int64(0), wal.TopicStableOffset("orders"))
	require.Equal(t, int64(1), wal.TopicStableOffset("payments"), "other topic remains unblocked")

	state = wal.CurrentState()
	b := state.States["B"]
	b.Pending = "commit"
	state.States["B"] = b
	require.NoError(t, wal.AppendState(state))
	require.Equal(t, "commit", wal.TopicTransactionDecision(bInstance))
	require.Equal(t, int64(0), wal.TopicStableOffset("orders"), "committed B stays behind unresolved A")

	state = wal.CurrentState()
	a := state.States["A"]
	a.Pending = "abort"
	state.States["A"] = a
	require.NoError(t, wal.AppendState(state))
	require.Equal(t, int64(3), wal.TopicStableOffset("orders"))
	require.Equal(t, "abort", wal.TopicTransactionDecision(aInstance))

	// Producer A may now start a new generation before the old records have
	// reached the eventual output materializer.
	nextA, err := newTxnInstance("A")
	require.NoError(t, err)
	state = wal.CurrentState()
	a = state.States["A"]
	a.Instance = nextA
	a.Pending = ""
	a.Active = true
	state.States["A"] = a
	require.NoError(t, wal.AppendState(state))
	require.Equal(t, "abort", wal.TopicTransactionDecision(aInstance))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	reopened, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	require.Equal(t, "abort", reopened.TopicTransactionDecision(aInstance))
	require.Equal(t, "commit", reopened.TopicTransactionDecision(bInstance))
	require.Equal(t, int64(3), reopened.TopicStableOffset("orders"))
	require.Equal(t, int64(1), reopened.TopicStableOffset("payments"))
	for i, want := range []string{"created", "paid", "updated"} {
		rec, ok, readErr := reopened.ReadTopicRecord("orders", int64(i))
		require.NoError(t, readErr)
		require.True(t, ok)
		require.Equal(t, want, string(rec.Data))
	}
}

func TestSharedWALDiscardsCompleteUnacceptedTopicBatchTail(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	// Emulate power loss after multiple complete, checksummed payload frames
	// reached disk, but before their acceptance delimiter.
	for i := 0; i < 2; i++ {
		buf, err := txnWALFrame(transactionWALRecord{
			Type: "topic-output", TopicOutput: &WALTopicRecord{
				Topic: "orders", Offset: int64(i), TransactionID: "orders-producer",
				Instance: instance, Data: []byte("unaccepted"),
			},
		})
		require.NoError(t, err)
		_, err = wal.logFile.Write(buf)
		require.NoError(t, err)
	}
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())

	replayed, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.Zero(t, replayed.NextTopicOffset("orders"), "unaccepted complete frames must be rolled back")
	_, found, err := replayed.ReadTopicRecord("orders", 0)
	require.NoError(t, err)
	require.False(t, found)
	offsets, err := replayed.AppendTopicBatch([]WALTopicRecord{{Topic: "orders", Data: []byte("accepted")}})
	require.NoError(t, err)
	require.Equal(t, []int64{0}, offsets)
	require.NoError(t, replayed.Close())
	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	require.Equal(t, int64(1), next.NextTopicOffset("orders"))
	record, ok, err := next.ReadTopicRecord("orders", 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "accepted", string(record.Data))
}

func TestSharedWALRejectsMismatchedTopicAcceptanceMarker(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	frames := []transactionWALRecord{
		{Type: "topic-output", TopicOutput: &WALTopicRecord{
			Topic: "orders", Offset: 0, TransactionID: "orders-producer",
			Instance: instance, Data: []byte("first"),
		}},
		{Type: "topic-accept", BatchCount: 2},
	}
	for _, rec := range frames {
		buf, err := txnWALFrame(rec)
		require.NoError(t, err)
		_, err = wal.logFile.Write(buf)
		require.NoError(t, err)
	}
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())
	_, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.ErrorContains(t, err, "mismatched topic WAL acceptance")
}
