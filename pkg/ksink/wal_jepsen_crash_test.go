package ksink

import (
    "errors"
    "os"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/require"
)

// A post-fsync failure is deliberately ambiguous to the caller: the response
// must be an error and the live WAL must fail-stop. After restart, however,
// the entire acceptance batch and its producer checkpoint must exist together.
func TestWALJepsenPostSyncProduceAmbiguityReplaysAtomicBatch(t *testing.T) {
    root := t.TempDir()
    wal, instance := testOpenWALWithActiveTransaction(t, root)
    state := wal.CurrentState()
    producer := state.States["orders-producer"]
    producer.Sequences = map[string]txnPartitionSequence{
        txnSequenceKey("orders", 0): {Next: 2},
    }
    state.States["orders-producer"] = producer

    wal.faultHook = func(stage string) error {
        if stage == "append:fsynced" { return errors.New("injected lost Produce response") }
        return nil
    }
    _, err := wal.appendTopicBatchWithState([]WALTopicRecord{
        {Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("one")},
        {Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("two")},
    }, state)
    require.ErrorContains(t, err, "injected lost Produce response")
    _, err = wal.AppendTopicBatch([]WALTopicRecord{{Topic: "orders", Data: []byte("must not append")}})
    require.Error(t, err, "ambiguous WAL writer must stop before any further acknowledgement")
    require.NoError(t, wal.Close())

    recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
    require.NoError(t, err)
    defer func() { _ = recovered.Close() }()
    require.Equal(t, int64(2), recovered.NextTopicOffset("orders"))
    require.Equal(t, int32(2), recovered.CurrentState().States["orders-producer"].
        Sequences[txnSequenceKey("orders", 0)].Next)
    for offset, want := range []string{"one", "two"} {
        rec, ok, err := recovered.ReadTopicRecord("orders", int64(offset))
        require.NoError(t, err)
        require.True(t, ok)
        require.Equal(t, want, string(rec.Data))
    }
}

// One terminal frame decides all enrolled topics. A lost EndTxn response
// cannot cause a later restart to change commit to abort or lose a marker.
func TestWALJepsenPostSyncEndTxnAmbiguityHasOneDurableOutcome(t *testing.T) {
    root := t.TempDir()
    wal, instance := testOpenWALWithActiveTransaction(t, root)
    state := wal.CurrentState()
    producer := state.States["orders-producer"]
    producer.Topics = []string{"orders", "payments"}
    state.States["orders-producer"] = producer
    require.NoError(t, wal.AppendState(state))
    offsets, err := wal.AppendTopicBatch([]WALTopicRecord{
        {Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("order")},
        {Topic: "payments", TransactionID: "orders-producer", Instance: instance, Data: []byte("payment")},
    })
    require.NoError(t, err)
    require.Equal(t, []int64{0, 0}, offsets)

    wal.faultHook = func(stage string) error {
        if stage == "append:fsynced" { return errors.New("injected lost EndTxn response") }
        return nil
    }
    require.ErrorContains(t, wal.AppendTopicTerminal("orders-producer", instance, "commit"),
        "injected lost EndTxn response")
    require.NoError(t, wal.Close())

    recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
    require.NoError(t, err)
    defer func() { _ = recovered.Close() }()
    require.Equal(t, "commit", recovered.TopicTransactionDecision(instance))
    for _, topic := range []string{"orders", "payments"} {
        require.Equal(t, int64(2), recovered.NextTopicOffset(topic))
        require.Equal(t, int64(2), recovered.TopicStableOffset(topic))
        marker, visible, err := recovered.ReadTopicRecord(topic, 1)
        require.NoError(t, err)
        require.False(t, visible, "control marker must not be published as user data: %+v", marker)
    }
    require.Error(t, recovered.AppendTopicTerminal("orders-producer", instance, "abort"),
        "a restarted producer cannot reverse a durable decision")
}

// Every byte cut of a pending topic-output frame must preserve the previous
// acknowledged acceptance delimiter. In particular a completely written but
// unaccepted frame is not visible; its existence does not advance offsets.
func TestWALJepsenActiveTailEveryByteCutPreservesAcknowledgedPrefix(t *testing.T) {
    frame, err := txnWALFrame(transactionWALRecord{
        Type: "topic-output",
        TopicOutput: &WALTopicRecord{Topic: "orders", Offset: 1, Data: []byte("not-acknowledged")},
    })
    require.NoError(t, err)
    for cut := 0; cut <= len(frame); cut++ {
        // Separate state directory per cut ensures no earlier repair masks bugs.
        root := t.TempDir()
        wal, _ := testOpenWALWithActiveTransaction(t, root)
        offsets, err := wal.AppendTopicBatch([]WALTopicRecord{{
            Topic: "orders", Data: []byte("acknowledged"),
        }})
        require.NoError(t, err)
        require.Equal(t, []int64{0}, offsets)
        file := wal.walPath(wal.activeKey())
        before, err := os.Stat(file)
        require.NoError(t, err)
        require.NoError(t, wal.Close())

        f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
        require.NoError(t, err)
        _, err = f.Write(frame[:cut])
        require.NoError(t, err)
        require.NoError(t, f.Sync())
        require.NoError(t, f.Close())

        recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
        require.NoError(t, err, "replay must accept a torn uncommitted tail at byte %d", cut)
        require.Equal(t, int64(1), recovered.NextTopicOffset("orders"), "cut %d", cut)
        rec, exists, err := recovered.ReadTopicRecord("orders", 0)
        require.NoError(t, err)
        require.True(t, exists, "cut %d", cut)
        require.Equal(t, "acknowledged", string(rec.Data), "cut %d", cut)
        _, exists, err = recovered.ReadTopicRecord("orders", 1)
        require.NoError(t, err)
        require.False(t, exists, "cut %d", cut)
        require.NoError(t, recovered.Close())
        after, err := os.Stat(file)
        require.NoError(t, err)
        require.Equal(t, before.Size(), after.Size(), "trailing unaccepted bytes must be truncated at cut %d", cut)
    }
}
