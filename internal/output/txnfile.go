package output

import (
	"fmt"
	"os"
	"path/filepath"
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
}

// txnFileWriter writes messages to per-transaction temporary files and uses
// rename-as-commit / delete-as-rollback semantics.
//
// The output path must contain a {txnID} placeholder. It may optionally
// contain a {topic} placeholder that is replaced with the sanitised topic
// name from each message. When {topic} is present, a single transaction
// that produces to multiple topics creates one file per topic; all are
// committed or aborted together.
//
//	During transaction: pattern with placeholders replaced + ".tmp"
//	After commit:       pattern with placeholders replaced
//	After abort:        temp files are deleted
type txnFileWriter struct {
	pattern  string // output path pattern (must contain {txnID})
	mu       sync.Mutex
	txnFiles map[string]map[string]*txnFileEntry // txnID → (resolvedPath → entry)
}

// NewTxnFileWriter creates a [TransactionalWriter] backed by the filesystem.
// Each transaction writes to a temporary file that is renamed on commit or
// deleted on abort.
//
// The pattern must contain a {txnID} placeholder which is replaced with the
// actual transaction ID for each transaction. It may also contain a {topic}
// placeholder. For example:
//
//	"messages-{txnID}.jsonl"
//	"{topic}-{txnID}.jsonl"
//
// produces committed files like "messages-my-txn.jsonl" and temp files like
// "messages-my-txn.jsonl.tmp".
func NewTxnFileWriter(pattern string) (TransactionalWriter, error) {
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	return &txnFileWriter{
		pattern:  pattern,
		txnFiles: make(map[string]map[string]*txnFileEntry),
	}, nil
}

func (w *txnFileWriter) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	txnID := ""
	if msg != nil {
		txnID = msg.TransactionalID
	}

	if txnID == "" {
		return fmt.Errorf("transactional file writer requires a TransactionalID on every message")
	}

	topic := ""
	if msg != nil {
		topic = msg.Topic
	}

	committed := w.resolvePath(txnID, topic)
	entries, ok := w.txnFiles[txnID]
	if !ok {
		entries = make(map[string]*txnFileEntry)
		w.txnFiles[txnID] = entries
	}

	entry, ok := entries[committed]
	if !ok {
		tmp := committed + ".tmp"
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("failed to open txn temp file: %w", err)
		}
		entry = &txnFileEntry{file: f, tmpPath: tmp, committedPath: committed}
		entries[committed] = entry
	}

	_, err := entry.file.Write(data)
	return err
}

func (w *txnFileWriter) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, ok := w.txnFiles[txnID]
	if !ok {
		return nil // nothing written for this transaction
	}
	delete(w.txnFiles, txnID)

	for _, entry := range entries {
		if err := entry.file.Close(); err != nil {
			return fmt.Errorf("failed to close txn temp file: %w", err)
		}

		// Remove an existing destination before rename so behaviour is
		// consistent across platforms (os.Rename replaces on Unix but
		// fails on Windows when the destination already exists).
		if info, err := os.Stat(entry.committedPath); err == nil {
			if info.IsDir() {
				return fmt.Errorf("committed path for transaction %s is a directory", txnID)
			}
			if err := os.Remove(entry.committedPath); err != nil {
				return fmt.Errorf("failed to remove existing committed file for transaction %s: %w", txnID, err)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat committed file for transaction %s: %w", txnID, err)
		}

		if err := os.Rename(entry.tmpPath, entry.committedPath); err != nil {
			return fmt.Errorf("failed to commit transaction %s: %w", txnID, err)
		}
	}
	return nil
}

func (w *txnFileWriter) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, ok := w.txnFiles[txnID]
	if !ok {
		return nil // nothing written for this transaction
	}
	delete(w.txnFiles, txnID)

	for _, entry := range entries {
		if err := entry.file.Close(); err != nil {
			return fmt.Errorf("failed to close txn temp file: %w", err)
		}
		if err := os.Remove(entry.tmpPath); err != nil {
			return fmt.Errorf("failed to abort transaction %s: %w", txnID, err)
		}
	}
	return nil
}

func (w *txnFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for _, entries := range w.txnFiles {
		for _, entry := range entries {
			if err := entry.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			// Remove uncommitted temp files on close.
			if err := os.Remove(entry.tmpPath); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	w.txnFiles = nil

	return firstErr
}

func (w *txnFileWriter) resolvePath(txnID, topic string) string {
	path := strings.ReplaceAll(w.pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	path = strings.ReplaceAll(path, TopicPlaceholder, SanitizePathSegment(topic))
	return filepath.Clean(path)
}
