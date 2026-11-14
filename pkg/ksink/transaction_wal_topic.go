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

// AppendTopicBatch assigns per-topic offsets and fsyncs all accepted records
// in ONE shared WAL batch before returning their offsets. A caller must perform
// producer deduplication before using this API: it does not recognize retries.
// The Kafka protocol path will be connected only together with an atomic
// producer-sequence checkpoint to prevent duplicate offset allocation.
func (l *TransactionLog) AppendTopicBatch(records []WALTopicRecord) ([]int64, error) {
	if len(records) == 0 {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken != nil {
		return nil, l.broken
	}
	if l.binding == "" {
		return nil, fmt.Errorf("WAL output writer is not bound")
	}

	next := make(map[string]int64)
	changes := make([]transactionWALRecord, 0, len(records))
	offsets := make([]int64, 0, len(records))
	for _, r := range records {
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
		}
		offset, ok := next[r.Topic]
		if !ok {
			offset = l.topicNext[r.Topic]
		}
		if offset == math.MaxInt64 {
			return nil, fmt.Errorf("WAL topic offset exhausted for %q", r.Topic)
		}
		r.Offset = offset
		r.Data = append([]byte(nil), r.Data...)
		next[r.Topic] = offset + 1
		offsets = append(offsets, offset)
		changes = append(changes, transactionWALRecord{Type: "topic-output", TopicOutput: &r})
	}
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
