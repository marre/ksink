package ksink

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// readWALOutputAt validates and reads one checksummed output frame by offset.
// The active log file and compacted-out files are safe for concurrent ReadAt.
func readWALOutputAt(file *os.File, ref walOutputLocation) (WALOutputRecord, error) {
	var header [8]byte
	if _, err := file.ReadAt(header[:], ref.offset); err != nil {
		return WALOutputRecord{}, fmt.Errorf("read WAL output header at %d: %w", ref.offset, err)
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 || length > transactionWALMaxFrame {
		return WALOutputRecord{}, fmt.Errorf("invalid WAL output length at %d", ref.offset)
	}
	raw := make([]byte, length)
	if _, err := file.ReadAt(raw, ref.offset+8); err != nil {
		return WALOutputRecord{}, fmt.Errorf("read WAL output at %d: %w", ref.offset, err)
	}
	if crc32.Checksum(raw, transactionWALCRCTable) != binary.BigEndian.Uint32(header[4:]) {
		return WALOutputRecord{}, fmt.Errorf("WAL output checksum mismatch at %d", ref.offset)
	}
	var record transactionWALRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return WALOutputRecord{}, fmt.Errorf("decode WAL output at %d: %w", ref.offset, err)
	}
	if record.Version != transactionWALVersion || record.Type != "output" || record.Output == nil {
		return WALOutputRecord{}, fmt.Errorf("invalid WAL output record at %d", ref.offset)
	}
	payload := *record.Output
	if payload.TransactionID != ref.meta.TransactionID || payload.Instance != ref.meta.Instance ||
		payload.Path != ref.meta.Path {
		return WALOutputRecord{}, fmt.Errorf("WAL output index disagrees with payload at %d", ref.offset)
	}
	return payload, nil
}

// StreamOutput reads a transaction's exact durable output bytes from its WAL
// frames one record at a time. Only small offsets and paths are retained in
// memory; the caller may write each payload directly to a staged output file.
// Callbacks must not call the same TransactionLog.
func (l *TransactionLog) StreamOutput(instance string, consume func(WALOutputRecord) error) error {
	l.mu.Lock()
	if l.broken != nil {
		err := l.broken
		l.mu.Unlock()
		return err
	}
	if l.logFile == nil {
		l.mu.Unlock()
		return fmt.Errorf("transaction log is closed")
	}
	refs := append([]walOutputLocation(nil), l.data[instance]...)
	reader, err := os.Open(l.logFile.Name())
	l.mu.Unlock()
	if err != nil {
		return err
	}
	defer reader.Close() //nolint:errcheck
	for _, ref := range refs {
		record, err := readWALOutputAt(reader, ref)
		if err != nil {
			return err
		}
		if err := consume(record); err != nil {
			return err
		}
	}
	return nil
}

// Ensure os.File ReadAt is the only reader interface we need; no full
// transaction payload is materialized in memory by the coordinator.
var _ io.ReaderAt = (*os.File)(nil)
