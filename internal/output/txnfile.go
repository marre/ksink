package output

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// TxnIDPlaceholder is the placeholder that must be used in the output pattern
// to control the naming of per-transaction files.
const TxnIDPlaceholder = "{txnID}"

// txnFileWriter writes messages to per-transaction temporary files and uses
// rename-as-commit / delete-as-rollback semantics.
//
// The output path must contain a {txnID} placeholder. Per-transaction file
// names are derived by replacing {txnID} with the actual transaction ID:
//
//	During transaction: pattern with {txnID} replaced + ".tmp"
//	After commit:       pattern with {txnID} replaced
//	After abort:        temp file is deleted
type txnFileWriter struct {
	pattern  string // output path pattern (must contain {txnID})
	mu       sync.Mutex
	txnFiles map[string]*os.File // txnID → open temp file
}

// sanitizeTxnID normalises a TransactionalID so it cannot introduce new path
// components or escape the intended directory when interpolated into a
// filesystem path. It replaces directory separators, colons (volume prefixes)
// and ".." sequences with underscores.
func sanitizeTxnID(txnID string) string {
	safe := strings.ReplaceAll(txnID, "/", "_")
	safe = strings.ReplaceAll(safe, "\\", "_")
	safe = strings.ReplaceAll(safe, ":", "_")

	for strings.Contains(safe, "..") {
		safe = strings.ReplaceAll(safe, "..", "_")
	}

	for strings.HasPrefix(safe, ".") {
		safe = strings.TrimPrefix(safe, ".")
	}

	if safe == "" {
		return "txn"
	}
	return safe
}

// NewTxnFileWriter creates a [TransactionalWriter] backed by the filesystem.
// Each transaction writes to a temporary file that is renamed on commit or
// deleted on abort.
//
// The pattern must contain a {txnID} placeholder which is replaced with the
// actual transaction ID for each transaction. For example:
//
//	"messages-{txnID}.jsonl"
//
// produces committed files like "messages-my-txn.jsonl" and temp files like
// "messages-my-txn.jsonl.tmp".
func NewTxnFileWriter(pattern string) (TransactionalWriter, error) {
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	return &txnFileWriter{
		pattern:  pattern,
		txnFiles: make(map[string]*os.File),
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

	f, ok := w.txnFiles[txnID]
	if !ok {
		var err error
		f, err = os.OpenFile(w.tmpPath(txnID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("failed to open txn temp file: %w", err)
		}
		w.txnFiles[txnID] = f
	}

	_, err := f.Write(data)
	return err
}

func (w *txnFileWriter) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, ok := w.txnFiles[txnID]
	if !ok {
		return nil // nothing written for this transaction
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close txn temp file: %w", err)
	}
	delete(w.txnFiles, txnID)

	tmpPath := w.tmpPath(txnID)
	committedPath := w.committedPath(txnID)

	// Remove an existing destination before rename so behaviour is
	// consistent across platforms (os.Rename replaces on Unix but
	// fails on Windows when the destination already exists).
	if info, err := os.Stat(committedPath); err == nil {
		if info.IsDir() {
			return fmt.Errorf("committed path for transaction %s is a directory", txnID)
		}
		if err := os.Remove(committedPath); err != nil {
			return fmt.Errorf("failed to remove existing committed file for transaction %s: %w", txnID, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat committed file for transaction %s: %w", txnID, err)
	}

	if err := os.Rename(tmpPath, committedPath); err != nil {
		return fmt.Errorf("failed to commit transaction %s: %w", txnID, err)
	}
	return nil
}

func (w *txnFileWriter) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, ok := w.txnFiles[txnID]
	if !ok {
		return nil // nothing written for this transaction
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close txn temp file: %w", err)
	}
	delete(w.txnFiles, txnID)

	if err := os.Remove(w.tmpPath(txnID)); err != nil {
		return fmt.Errorf("failed to abort transaction %s: %w", txnID, err)
	}
	return nil
}

func (w *txnFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for txnID, f := range w.txnFiles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		// Remove uncommitted temp files on close.
		if err := os.Remove(w.tmpPath(txnID)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.txnFiles = nil

	return firstErr
}

func (w *txnFileWriter) tmpPath(txnID string) string {
	return w.committedPath(txnID) + ".tmp"
}

func (w *txnFileWriter) committedPath(txnID string) string {
	return strings.ReplaceAll(w.pattern, TxnIDPlaceholder, sanitizeTxnID(txnID))
}
