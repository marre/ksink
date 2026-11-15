package ksink

import (
    "fmt"
    "math/rand"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/require"
)

type walJepsenModelSlot struct {
    payload  string
    instance string
    decision string
    marker   bool
}

// A deliberately independent model: we append expected slots only after
// successful API calls, never reconstruct them from WAL internals. Every
// close/reopen/compaction checks no acknowledged loss, reuse or reordering.
func checkWALJepsenModel(t *testing.T, wal *TransactionLog,
    expected map[string][]walJepsenModelSlot, decisions map[string]string) {
    t.Helper()
    for instance, decision := range decisions {
        require.Equal(t, decision, wal.TopicTransactionDecision(instance),
            "durable transaction outcome must survive restart")
    }
    for topic, slots := range expected {
        require.Equal(t, int64(len(slots)), wal.NextTopicOffset(topic),
            "logical offsets must never rewind or be reused")
        require.Equal(t, int64(len(slots)), wal.TopicStableOffset(topic),
            "all transactions in this model have reached a terminal decision")
        for offset, slot := range slots {
            record, found, err := wal.ReadTopicRecord(topic, int64(offset))
            require.NoError(t, err)
            if slot.marker {
                require.False(t, found, "control marker must not be user data at %s/%d", topic, offset)
                continue
            }
            if slot.decision == "abort" {
                // Compaction may legitimately discard aborted raw records.
                // If retained, they must remain tagged with the aborting txn.
                if found {
                    require.Equal(t, slot.instance, record.Instance)
                    require.Equal(t, "abort", wal.TopicTransactionDecision(record.Instance))
                }
                continue
            }
            require.True(t, found, "acknowledged committed payload missing at %s/%d", topic, offset)
            require.Equal(t, slot.payload, string(record.Data))
            require.Equal(t, slot.instance, record.Instance)
        }
    }
}

func TestWALJepsenSeededTransactionalHistorySurvivesCompactionAndRestarts(t *testing.T) {
    for seed := int64(0); seed < 4; seed++ {
        t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
            rng := rand.New(rand.NewSource(seed))
            dir := filepath.Join(t.TempDir(), "state")
            wal, err := OpenTransactionLog(dir)
            require.NoError(t, err)
            require.NoError(t, wal.BindOutput(filepath.Join(t.TempDir(), "{topic}", "messages-{seq:08}.jsonl")))

            topics := []string{"orders", "payments"}
            expected := map[string][]walJepsenModelSlot{"orders": {}, "payments": {}}
            decisions := map[string]string{}
            for step := 0; step < 32; step++ {
                if rng.Intn(4) == 0 {
                    // Ordinary nontransactional input is immediately stable.
                    topic := topics[rng.Intn(len(topics))]
                    payload := fmt.Sprintf("s%d-op%d-%s", seed, step, topic)
                    before := int64(len(expected[topic]))
                    offsets, err := wal.AppendTopicBatch([]WALTopicRecord{
                        {Topic: topic, Data: []byte(payload)},
                    })
                    require.NoError(t, err)
                    require.Equal(t, []int64{before}, offsets)
                    expected[topic] = append(expected[topic], walJepsenModelSlot{payload: payload})
                } else {
                    id := fmt.Sprintf("seed-%d-txn-%d", seed, step)
                    instance, err := newTxnInstance(id)
                    require.NoError(t, err)
                    participants := topics
                    if rng.Intn(2) == 0 {
                        participants = topics[:1+rng.Intn(2)]
                    }
                    state := wal.CurrentState()
                    state.Counter++
                    state.States[id] = storedTxnState{
                        ProducerID: state.Counter, Epoch: 1, Active: true,
                        Instance: instance, Topics: append([]string(nil), participants...),
                    }
                    require.NoError(t, wal.AppendState(state))
                    batch := make([]WALTopicRecord, 0, len(participants))
                    for _, topic := range participants {
                        batch = append(batch, WALTopicRecord{
                            Topic: topic, TransactionID: id, Instance: instance,
                            Data: []byte(fmt.Sprintf("s%d-op%d-%s", seed, step, topic)),
                        })
                    }
                    offsets, err := wal.AppendTopicBatch(batch)
                    require.NoError(t, err)
                    decision := "commit"
                    if rng.Intn(3) == 0 { decision = "abort" }
                    for i, topic := range participants {
                        want := int64(len(expected[topic]))
                        require.Equal(t, want, offsets[i])
                        require.Equal(t, want, wal.TopicStableOffset(topic),
                            "undecided transaction must block the topic's stable prefix")
                    }
                    require.NoError(t, wal.AppendTopicTerminal(id, instance, decision))
                    decisions[instance] = decision
                    for _, record := range batch {
                        expected[record.Topic] = append(expected[record.Topic],
                            walJepsenModelSlot{
                                payload: string(record.Data), instance: instance, decision: decision,
                            },
                            walJepsenModelSlot{instance: instance, decision: decision, marker: true})
                    }
                }
                checkWALJepsenModel(t, wal, expected, decisions)
                if step%8 == 7 {
                    require.NoError(t, wal.Compact())
                    require.NoError(t, wal.Close())
                    wal, err = OpenTransactionLog(dir)
                    require.NoError(t, err)
                    checkWALJepsenModel(t, wal, expected, decisions)
                }
            }
            require.NoError(t, wal.Close())
            final, err := OpenTransactionLog(dir)
            require.NoError(t, err)
            checkWALJepsenModel(t, final, expected, decisions)
            require.NoError(t, final.Close())
        })
    }
}
