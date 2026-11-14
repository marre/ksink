package ksink

import (
	"fmt"
	"math"
	"os"
)

// WALTopicRecord is a durable application record in one logical topic stream.
// Every topic has exactly one partition (0). Offset is assigned by the shared
// WAL and is independent of producer batch sequence or output-file number.
//
// These records are the foundation for the future read_committed materializer.
// They are intentionally NOT exposed as committed files until transaction
// visibility and publication cursors are implemented.
type WALTopicRecord struct {
	Topic         string
	Offset        int64
	TransactionID string
	Instance      string
	Data          []byte
}

// walTopicLocation indexes the authoritative checksummed frame, without
// retaining serialized payload bytes in the coordinator's memory.
type walTopicLocation struct {
	key    walFileKey
	offset int64
	size   int64
	meta   WALTopicRecord // Data is always nil.
}

// maxTopicAcceptanceBatchRecords bounds metadata retained by recovery while
// scanning an incomplete accepted batch; checkpoints use smaller chunks.
const maxTopicAcceptanceBatchRecords = 65536

// AppendTopicBatch assigns per-topic offsets, then fsyncs the accepted records
// and their acceptance delimiter. This low-level API is intended for records
// that require no additional producer checkpoint (e.g. nontransactional input).
// Callers performing idempotent Kafka Produce must instead use
// appendTopicBatchWithState to atomically persist producer sequence history.
func (l *TransactionLog) AppendTopicBatch(records []WALTopicRecord) ([]int64, error) {
	return l.appendTopicBatch(records, nil, nil)
}

// appendTopicBatchWithState couples topic records and their producer dedup /
// coordinator state in one replay-atomic acceptance frame. The entire batch
// and updated state become durable together, before Kafka Produce success.
// The caller must still validate producer epoch and batch signatures.
func (l *TransactionLog) appendTopicBatchWithState(records []WALTopicRecord, snapshot txnStateSnapshot) ([]int64, error) {
	copied := cloneTxnStateSnapshot(snapshot)
	return l.appendTopicBatch(records, &copied, nil)
}

// appendTopicBatchFormattedWithState formats serialized output only AFTER
// assigning authoritative per-topic offsets, but BEFORE WAL persistence.
// The formatter must be deterministic, bounded and must not call this WAL.
// A format error cannot consume offsets or change producer dedup state.
func (l *TransactionLog) appendTopicBatchFormattedWithState(records []WALTopicRecord,
	snapshot txnStateSnapshot, format func(int, int64) ([]byte, error)) ([]int64, error) {
	if format == nil {
		return nil, fmt.Errorf("missing topic output formatter")
	}
	copied := cloneTxnStateSnapshot(snapshot)
	return l.appendTopicBatch(records, &copied, format)
}

func (l *TransactionLog) appendTopicBatch(records []WALTopicRecord, snapshot *txnStateSnapshot,
	format func(int, int64) ([]byte, error)) ([]int64, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > maxTopicAcceptanceBatchRecords {
		return nil, fmt.Errorf("topic WAL batch exceeds %d records", maxTopicAcceptanceBatchRecords)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken != nil {
		return nil, l.broken
	}
	if l.binding == "" {
		return nil, fmt.Errorf("WAL output writer is not bound")
	}
	if snapshot != nil {
		if snapshot.Version != durableTxnStateVersion || snapshot.Counter <= 0 ||
			len(snapshot.States) > maxTxnStates {
			return nil, fmt.Errorf("invalid accepted producer state snapshot")
		}
		for id, old := range l.state.States {
			nextState, exists := snapshot.States[id]
			if !exists || old.ProducerID != nextState.ProducerID || old.Epoch != nextState.Epoch ||
				old.Instance != nextState.Instance || old.Pending != nextState.Pending {
				return nil, fmt.Errorf("accepted producer snapshot changed active identity %q", id)
			}
		}
	}

	next := make(map[string]int64)
	changes := make([]transactionWALRecord, 0, len(records))
	offsets := make([]int64, 0, len(records))
	for i, r := range records {
		if r.Topic == "" || len(r.Topic) > 249 {
			return nil, fmt.Errorf("invalid WAL topic name")
		}
		if (r.TransactionID == "") != (r.Instance == "") {
			return nil, fmt.Errorf("transaction ID and instance must both be present or absent")
		}
		if r.TransactionID != "" {
			if !txnWALValidInstance(r.Instance) {
				return nil, fmt.Errorf("invalid transaction instance")
			}
			state, found := l.state.States[r.TransactionID]
			if !found || !state.Active || state.Pending != "" || state.Instance != r.Instance {
				return nil, fmt.Errorf("transaction %q is not an active WAL instance", r.TransactionID)
			}
			if snapshot != nil {
				updated, ok := snapshot.States[r.TransactionID]
				if !ok || updated.Instance != r.Instance || !updated.Active {
					return nil, fmt.Errorf("accepted producer snapshot missing transaction %q", r.TransactionID)
				}
			}
		}
		offset, ok := next[r.Topic]
		if !ok {
			offset = l.topicNext[r.Topic]
		}
		if offset == math.MaxInt64 {
			return nil, fmt.Errorf("WAL topic offset exhausted for %q", r.Topic)
		}
		r.Offset = offset
		if format != nil {
			encoded, err := format(i, offset)
			if err != nil { return nil, fmt.Errorf("format topic record: %w", err) }
			r.Data = encoded
		}
		r.Data = append([]byte(nil), r.Data...)
		next[r.Topic] = offset + 1
		offsets = append(offsets, offset)
		changes = append(changes, transactionWALRecord{Type: "topic-output", TopicOutput: &r})
	}
	// The acceptance delimiter is replayed only after all complete record
	// frames have arrived. A crash before it discards the entire batch.
	changes = append(changes, transactionWALRecord{
		Type: "topic-accept", BatchCount: len(records), State: snapshot,
	})
	if err := l.appendBatchLocked(changes); err != nil {
		return nil, err
	}
	return offsets, nil
}

// NextTopicOffset returns the next append position for partition 0 of a topic.
func (l *TransactionLog) NextTopicOffset(topic string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.topicNext[topic]
}

// TopicTransactionDecision reports an irreversible transaction outcome for a
// logical topic record. An empty result means the transaction is still open
// or undecided. Nontransactional records are implicitly committed.
func (l *TransactionLog) TopicTransactionDecision(instance string) string {
	if instance == "" { return "commit" }
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.topicDecisions[instance]
}

// TopicStableOffset is the first offset whose visibility is not yet resolved.
// All smaller offsets are either committed or aborted and can be processed in
// offset order. This is per topic, never a global cross-topic barrier.
func (l *TransactionLog) TopicStableOffset(topic string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ref := range l.topicRecords[topic] {
		if ref.meta.Instance != "" && l.topicDecisions[ref.meta.Instance] == "" {
			return ref.meta.Offset
		}
	}
	return l.topicNext[topic]
}

// ReadTopicRecord reads exactly one checksummed WAL record by its topic offset.
// Keeping the mutex only during this bounded read allows the future output
// materializer to write files without holding back concurrent WAL appends.
func (l *TransactionLog) ReadTopicRecord(topic string, offset int64) (WALTopicRecord, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken != nil {
		return WALTopicRecord{}, false, l.broken
	}
	if l.logFile == nil {
		return WALTopicRecord{}, false, fmt.Errorf("transaction log is closed")
	}
	refs := l.topicRecords[topic]
	if offset < 0 || offset >= int64(len(refs)) {
		return WALTopicRecord{}, false, nil
	}
	ref := refs[int(offset)]
	f, err := os.Open(l.walPath(ref.key))
	if err != nil {
		return WALTopicRecord{}, false, fmt.Errorf("open topic WAL frame: %w", err)
	}
	defer f.Close() //nolint:errcheck
	rec, n, err := readTxnWALFrameAt(f, ref.offset)
	if err != nil {
		return WALTopicRecord{}, false, err
	}
	if n != ref.size || rec.Type != "topic-output" || rec.TopicOutput == nil {
		return WALTopicRecord{}, false, fmt.Errorf("invalid topic WAL index at offset %d", offset)
	}
	record := *rec.TopicOutput
	if record.Topic != topic || record.Offset != offset || record.TransactionID != ref.meta.TransactionID ||
		record.Instance != ref.meta.Instance {
		return WALTopicRecord{}, false, fmt.Errorf("topic WAL index/payload mismatch at offset %d", offset)
	}
	return record, true, nil
}
