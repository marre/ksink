package output

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// TxnIDPlaceholder is the placeholder that must be used in the output pattern
// to control the naming of per-transaction files.
const TxnIDPlaceholder = "{txnID}"

// txnFileEntry tracks a single open temp file within a transaction.
type txnFileEntry struct {
	file          *os.File
	tmpPath       string
	committedPath string
	closed        bool
	published     bool
}

// txnFileWriter writes messages to per-transaction temporary files and uses
// rename-as-commit / delete-as-rollback semantics.
//
// The output path must contain a {txnID} placeholder. It may optionally
// contain {topic} and {partition} placeholders that are replaced with the sanitised topic
// name from each message. When {topic} is present, a single transaction
// that produces to multiple topics or partitions creates one file per resolved path.
// A random suffix distinguishes consecutive transactions using the same transactional.id.
// Publishing individual files is atomic, but publishing several files is not:
// consumers requiring transaction-wide atomic visibility must not use per-topic
// or per-partition output without an external commit protocol.
//
//	During transaction: pattern with placeholders replaced + "-<nonce>.tmp"
//	After commit:       pattern with placeholders replaced + "-<nonce>"
//	After abort:        temp files are deleted
type txnFileWriter struct {
	pattern      string // output path pattern (must contain {txnID})
	mu           sync.Mutex
	txnFiles     map[string]map[string]*txnFileEntry // producer transactional.id → path → entry
	txnInstances map[string]string                    // unique suffix for this transaction instance
	committing   map[string]bool                      // failed commits accept retries/aborts, not writes
}

// NewTxnFileWriter creates a [TransactionalWriter] backed by the filesystem.
// Each transaction writes to a temporary file that is renamed on commit or
// deleted on abort.
//
// The pattern must contain a {txnID} placeholder which is replaced with the
// actual transaction ID for each transaction. It may also contain a {topic}
// and {partition} placeholders. For example:
//
//	"messages-{txnID}.jsonl"
//	"{topic}-{txnID}.jsonl"
//	"{topic}-{partition}-{txnID}.jsonl"
//
// produces committed files like "messages-my-txn-<nonce>.jsonl" and
// temporary files like "messages-my-txn-<nonce>.jsonl.tmp".
func NewTxnFileWriter(pattern string) (TransactionalWriter, error) {
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	return &txnFileWriter{
		pattern:      pattern,
		txnFiles:     make(map[string]map[string]*txnFileEntry),
		txnInstances: make(map[string]string),
		committing:   make(map[string]bool),
	}, nil
}

// instancePath adds a collision-resistant suffix before the extension. The
// same suffix is retained for every file in one transaction and every retry.
func instancePath(base, instance string) string {
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) + "-" + instance + ext
}

func (w *txnFileWriter) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if msg == nil || msg.TransactionalID == "" {
		return fmt.Errorf("transactional file writer requires a TransactionalID on every message")
	}
	txnID := msg.TransactionalID
	if w.committing[txnID] {
		return fmt.Errorf("transaction %q has a pending commit; retry commit or abort before writing", txnID)
	}

	instance, ok := w.txnInstances[txnID]
	if !ok {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return fmt.Errorf("generate transaction instance ID: %w", err)
		}
		instance = hex.EncodeToString(nonce[:])
		w.txnInstances[txnID] = instance
	}
	committed := instancePath(w.resolvePath(txnID, msg.Topic, msg.Partition), instance)
	entries := w.txnFiles[txnID]
	if entries == nil {
		entries = make(map[string]*txnFileEntry)
		w.txnFiles[txnID] = entries
	}
	entry := entries[committed]
	if entry == nil {
		tmp := committed + ".tmp"
		// Never truncate a staging file that may belong to another process.
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("create transaction staging file %s: %w", tmp, err)
		}
		entry = &txnFileEntry{file: f, tmpPath: tmp, committedPath: committed}
		entries[committed] = entry
	}

	if _, err := entry.file.Write(data); err != nil {
		return fmt.Errorf("write transaction %q staging file %s: %w", txnID, entry.tmpPath, err)
	}
	return nil
}

func (w *txnFileWriter) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, ok := w.txnFiles[txnID]
	if !ok {
		// Empty transactions legitimately have no staged files. The server
		// validates the transaction state before forwarding a commit here.
		return nil
	}
	w.committing[txnID] = true

	// Validate and close ALL staged files before publishing any of them.
	for _, entry := range entries {
		if entry.closed {
			continue
		}
		if err := entry.file.Sync(); err != nil {
			return fmt.Errorf("sync transaction %q file %s: %w", txnID, entry.tmpPath, err)
		}
		if err := entry.file.Close(); err != nil {
			return fmt.Errorf("close transaction %q file %s: %w", txnID, entry.tmpPath, err)
		}
		entry.closed = true
	}

	for _, entry := range entries {
		if entry.published {
			continue
		}
		// Hard-linking a same-directory staging file atomically creates the
		// destination without replacing existing committed data. The staging
		// path remains available if a later operation needs a retry or abort.
		if err := os.Link(entry.tmpPath, entry.committedPath); err != nil {
			return fmt.Errorf("publish transaction %q file %s: %w", txnID, entry.committedPath, err)
		}
		entry.published = true
	}

	for _, entry := range entries {
		if err := os.Remove(entry.tmpPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove transaction %q staging file %s: %w", txnID, entry.tmpPath, err)
		}
	}
	delete(w.txnFiles, txnID)
	delete(w.txnInstances, txnID)
	delete(w.committing, txnID)
	return nil
}

func (w *txnFileWriter) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, ok := w.txnFiles[txnID]
	if !ok {
		return nil
	}
	w.committing[txnID] = true
	for _, entry := range entries {
		if !entry.closed {
			if err := entry.file.Close(); err != nil {
				return fmt.Errorf("close aborted transaction %q file %s: %w", txnID, entry.tmpPath, err)
			}
			entry.closed = true
		}
		// Clean up partially published files on abort. This does not make a
		// multi-file commit atomically visible to external readers.
		if entry.published {
			if err := os.Remove(entry.committedPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove partially published transaction %q file %s: %w", txnID, entry.committedPath, err)
			}
			entry.published = false
		}
		if err := os.Remove(entry.tmpPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove aborted transaction %q file %s: %w", txnID, entry.tmpPath, err)
		}
	}
	delete(w.txnFiles, txnID)
	delete(w.txnInstances, txnID)
	delete(w.committing, txnID)
	return nil
}

func (w *txnFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for _, entries := range w.txnFiles {
		for _, entry := range entries {
			if !entry.closed {
				if err := entry.file.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			if entry.published {
				if err := os.Remove(entry.committedPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
					firstErr = err
				}
			}
			if err := os.Remove(entry.tmpPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
		}
	}
	w.txnFiles = nil
	w.txnInstances = nil
	w.committing = nil
	return firstErr
}

func (w *txnFileWriter) resolvePath(txnID, topic string, partition int32) string {
	path := strings.ReplaceAll(w.pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	path = strings.ReplaceAll(path, TopicPlaceholder, SanitizePathSegment(topic))
	path = strings.ReplaceAll(path, PartitionPlaceholder, strconv.FormatInt(int64(partition), 10))
	return filepath.Clean(path)
}
