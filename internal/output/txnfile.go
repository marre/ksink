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

// TxnIDPlaceholder is required in transactional output patterns.
const TxnIDPlaceholder = "{txnID}"

// txnFileEntry is a staged file within one transaction directory.
type txnFileEntry struct {
	file   *os.File
	closed bool
}

// txnDirectory stores all output files for one instance of a Kafka
// transactional.id. The instance suffix is stable through commit retries.
type txnDirectory struct {
	stagingDir   string
	committedDir string
	files        map[string]*txnFileEntry
	committing   bool
}

// txnFileWriter atomically publishes a whole transaction by renaming ONE
// directory. Staging lives under <root>/.ksink-pending/<txnID>-<nonce> and
// committed data under <root>/committed/<txnID>-<nonce>. All paths are on
// the same filesystem; readers must only read under <root>/committed.
//
// The original output pattern names files INSIDE each transaction directory.
// {topic} and {partition} may select different files or subdirectories.
// This provides atomic visibility of all files, but does NOT provide durable
// transaction state or recovery after a crash.
type txnFileWriter struct {
	pattern  string
	root     string
	mu       sync.Mutex
	txnFiles map[string]*txnDirectory // Kafka producer transactional.id => instance
	closed   bool
}

// transactionOutputRoot returns the deepest fixed directory of the output
// pattern. Everything after it is reproduced inside the transaction directory.
// No interpolated directory may escape the single filesystem publication root.
func transactionOutputRoot(pattern string) string {
	dir := filepath.Clean(filepath.Dir(pattern))
	for strings.Contains(dir, TxnIDPlaceholder) ||
		strings.Contains(dir, TopicPlaceholder) ||
		strings.Contains(dir, PartitionPlaceholder) {
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return dir
}

// NewTxnFileWriter creates a filesystem transactional writer. For example,
// "messages-{txnID}.jsonl" produces:
//    .ksink-pending/my-txn-<nonce>/messages-my-txn.jsonl (staging)
//    committed/my-txn-<nonce>/messages-my-txn.jsonl    (after commit)
//
// A pattern such as "data/{topic}/{partition}-{txnID}.jsonl" produces
// per-topic/per-partition files within ONE atomically published directory.
// Staging and committed directories must reside on the same filesystem.
func NewTxnFileWriter(pattern string) (TransactionalWriter, error) {
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	return &txnFileWriter{
		pattern:  filepath.Clean(pattern),
		root:     transactionOutputRoot(pattern),
		txnFiles: make(map[string]*txnDirectory),
	}, nil
}

func (w *txnFileWriter) newTransaction(txnID string) (*txnDirectory, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate transaction instance ID: %w", err)
	}
	instance := SanitizePathSegment(txnID) + "-" + hex.EncodeToString(nonce[:])
	stagingRoot := filepath.Join(w.root, ".ksink-pending")
	committedRoot := filepath.Join(w.root, "committed")
	for _, dir := range []string{stagingRoot, committedRoot} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("prepare transaction directory %s: %w", dir, err)
		}
	}
	stagingDir := filepath.Join(stagingRoot, instance)
	if err := os.Mkdir(stagingDir, 0700); err != nil {
		return nil, fmt.Errorf("create transaction staging directory %s: %w", stagingDir, err)
	}
	return &txnDirectory{
		stagingDir:   stagingDir,
		committedDir: filepath.Join(committedRoot, instance),
		files:        make(map[string]*txnFileEntry),
	}, nil
}

func (w *txnFileWriter) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("transactional file writer is closed")
	}
	if msg == nil || msg.TransactionalID == "" {
		return fmt.Errorf("transactional file writer requires a TransactionalID on every message")
	}
	txnID := msg.TransactionalID
	txn := w.txnFiles[txnID]
	if txn != nil && txn.committing {
		return fmt.Errorf("transaction %q has a pending commit; retry commit or abort before writing", txnID)
	}

	resolved := w.resolvePath(txnID, msg.Topic, msg.Partition)
	relative, err := filepath.Rel(w.root, resolved)
	if err != nil || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("transaction output path %q escapes root %q", resolved, w.root)
	}
	if txn == nil {
		txn, err = w.newTransaction(txnID)
		if err != nil {
			return err
		}
		w.txnFiles[txnID] = txn
	}
	stagedFile := filepath.Join(txn.stagingDir, relative)
	entry := txn.files[stagedFile]
	if entry == nil {
		if err := os.MkdirAll(filepath.Dir(stagedFile), 0700); err != nil {
			return fmt.Errorf("prepare transaction file directory %s: %w", stagedFile, err)
		}
		f, err := os.OpenFile(stagedFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fmt.Errorf("create transaction staged file %s: %w", stagedFile, err)
		}
		entry = &txnFileEntry{file: f}
		txn.files[stagedFile] = entry
	}
	if _, err := entry.file.Write(data); err != nil {
		return fmt.Errorf("write transaction %q file %s: %w", txnID, stagedFile, err)
	}
	return nil
}

func (w *txnFileWriter) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("transactional file writer is closed")
	}
	txn := w.txnFiles[txnID]
	if txn == nil {
		// The server validates EndTxn state; empty transactions create no files.
		return nil
	}
	txn.committing = true
	for name, entry := range txn.files {
		if entry.closed {
			continue
		}
		if err := entry.file.Sync(); err != nil {
			return fmt.Errorf("sync transaction %q file %s: %w", txnID, name, err)
		}
		err := entry.file.Close()
		entry.closed = true
		if err != nil {
			return fmt.Errorf("close transaction %q file %s: %w", txnID, name, err)
		}
	}

	// Reconcile a potentially successful earlier rename whose response was
	// lost. Never overwrite a different directory, even if it is empty.
	_, stagingErr := os.Lstat(txn.stagingDir)
	_, committedErr := os.Lstat(txn.committedDir)
	if committedErr == nil {
		if os.IsNotExist(stagingErr) {
			delete(w.txnFiles, txnID)
			return nil // already published by this transaction
		}
		return fmt.Errorf("commit destination already exists: %s", txn.committedDir)
	}
	if !os.IsNotExist(committedErr) {
		return fmt.Errorf("stat commit destination %s: %w", txn.committedDir, committedErr)
	}
	if stagingErr != nil {
		return fmt.Errorf("stat staged transaction %s: %w", txn.stagingDir, stagingErr)
	}
	if err := os.Rename(txn.stagingDir, txn.committedDir); err != nil {
		// A successful move with a lost response must not become a false
		// failure or cause a duplicate commit on retry.
		if _, sourceErr := os.Lstat(txn.stagingDir); os.IsNotExist(sourceErr) {
			if info, destErr := os.Lstat(txn.committedDir); destErr == nil && info.IsDir() {
				delete(w.txnFiles, txnID)
				return nil
			}
		}
		return fmt.Errorf("publish transaction %q directory %s: %w", txnID, txn.committedDir, err)
	}
	delete(w.txnFiles, txnID)
	return nil
}

func (w *txnFileWriter) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("transactional file writer is closed")
	}
	txn := w.txnFiles[txnID]
	if txn == nil {
		return nil
	}
	// A published directory is a committed transaction. Never roll it back
	// in response to an ambiguous commit result.
	if _, sourceErr := os.Lstat(txn.stagingDir); os.IsNotExist(sourceErr) {
		if _, destErr := os.Lstat(txn.committedDir); destErr == nil {
			return fmt.Errorf("transaction %q is already committed; cannot abort", txnID)
		}
	}
	for name, entry := range txn.files {
		if !entry.closed {
			err := entry.file.Close()
			entry.closed = true
			if err != nil {
				return fmt.Errorf("close aborted transaction %q file %s: %w", txnID, name, err)
			}
		}
	}
	if err := os.RemoveAll(txn.stagingDir); err != nil {
		return fmt.Errorf("remove transaction %q staging directory %s: %w", txnID, txn.stagingDir, err)
	}
	delete(w.txnFiles, txnID)
	return nil
}

func (w *txnFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var firstErr error
	for _, txn := range w.txnFiles {
		for _, entry := range txn.files {
			if !entry.closed {
				if err := entry.file.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
		if err := os.RemoveAll(txn.stagingDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.txnFiles = nil
	return firstErr
}

func (w *txnFileWriter) resolvePath(txnID, topic string, partition int32) string {
	path := strings.ReplaceAll(w.pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	path = strings.ReplaceAll(path, TopicPlaceholder, SanitizePathSegment(topic))
	path = strings.ReplaceAll(path, PartitionPlaceholder, strconv.FormatInt(int64(partition), 10))
	return filepath.Clean(path)
}
