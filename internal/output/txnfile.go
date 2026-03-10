package output

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// txnIDPlaceholder is the placeholder that can be used in the output pattern
// to control the naming of per-transaction files.
const txnIDPlaceholder = "{txnID}"

// txnFileWriter writes messages to per-transaction temporary files and uses
// rename-as-commit / delete-as-rollback semantics.
//
// The output path may contain a {txnID} placeholder to control per-transaction
// file names. When a placeholder is present:
//
//	During transaction: pattern with {txnID} replaced + ".tmp"
//	After commit:       pattern with {txnID} replaced
//	After abort:        temp file is deleted
//
// When no placeholder is present, the legacy naming convention is used:
//
//	During transaction: <basePath>.txn-<txnID>.tmp
//	After commit:       <basePath>.txn-<txnID>
//	After abort:        file is deleted
//
// Messages without a TransactionalID are written to basePath directly (the
// raw pattern string when a placeholder is present).
type txnFileWriter struct {
	pattern    string // original output path (may contain {txnID})
	hasPattern bool   // true when pattern contains {txnID}
	mu         sync.Mutex
	txnFiles   map[string]*os.File // txnID → open temp file
	baseFile   *os.File            // lazily opened for non-transactional writes
}

// NewTxnFileWriter creates a [TransactionalWriter] backed by the filesystem.
// Each transaction writes to a temporary file that is renamed on commit or
// deleted on abort.
//
// The path may contain a {txnID} placeholder which is replaced with the
// actual transaction ID for each transaction. For example:
//
//	"messages-{txnID}.jsonl"
//
// produces committed files like "messages-my-txn.jsonl" and temp files like
// "messages-my-txn.jsonl.tmp".
//
// When no {txnID} placeholder is present, the legacy convention is used:
// committed files are "<path>.txn-<txnID>" and temp files are
// "<path>.txn-<txnID>.tmp".
func NewTxnFileWriter(path string) TransactionalWriter {
	return &txnFileWriter{
		pattern:    path,
		hasPattern: strings.Contains(path, txnIDPlaceholder),
		txnFiles:   make(map[string]*os.File),
	}
}

func (w *txnFileWriter) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	txnID := ""
	if msg != nil {
		txnID = msg.TransactionalID
	}

	if txnID == "" {
		return w.writeBase(data)
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

// writeBase writes non-transactional data to the base file.
func (w *txnFileWriter) writeBase(data []byte) error {
	if w.baseFile == nil {
		f, err := os.OpenFile(w.pattern, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("failed to open base output file: %w", err)
		}
		w.baseFile = f
	}
	_, err := w.baseFile.Write(data)
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

	if err := os.Rename(w.tmpPath(txnID), w.committedPath(txnID)); err != nil {
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

	if w.baseFile != nil {
		if err := w.baseFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (w *txnFileWriter) tmpPath(txnID string) string {
	return w.committedPath(txnID) + ".tmp"
}

func (w *txnFileWriter) committedPath(txnID string) string {
	if w.hasPattern {
		return strings.ReplaceAll(w.pattern, txnIDPlaceholder, txnID)
	}
	return fmt.Sprintf("%s.txn-%s", w.pattern, txnID)
}
