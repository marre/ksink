package output

import (
	"fmt"
	"os"
	"path/filepath"
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
	poisoned     bool // a failed Write/Sync cannot be committed, only aborted
}

// txnFileWriter stages formatted records as a disposable cache. The single
// WAL is authoritative for transaction data and commit/abort decisions.
// Only committed/<txnID>-<instance>/ directories are publicly visible.
type txnFileWriter struct {
	pattern  string
	root     string
	wal      *ksink.TransactionLog
	lock     *os.File // advisory lock, held until Close or process exit
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
		strings.Contains(dir, TopicPlaceholder) {
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return dir
}

// KafkaTransactionStateDir is the default durable coordinator-state directory
// for transactional filesystem output. Using the fixed pattern root keeps the
// coordinator and staged files in the same deployment volume.
func KafkaTransactionStateDir(pattern string) string {
	return filepath.Join(transactionOutputRoot(pattern), ".ksink-kafka-state")
}

// NewTxnFileWriter builds a WAL-backed filesystem writer, with a single
// authoritative commit decision shared with the Kafka coordinator. Its
// TransactionLog must remain open until the writer and server have stopped.
func NewTxnFileWriter(pattern string, log *ksink.TransactionLog) (TransactionalWriter, error) {
	if log == nil {
		return nil, fmt.Errorf("transactional output requires a transaction WAL")
	}
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	if strings.Contains(pattern, unsupportedPartitionPlaceholder) {
		return nil, fmt.Errorf("{partition} is unsupported: ksink exposes only partition 0 per topic")
	}
	if err := log.BindOutput(pattern); err != nil {
		return nil, fmt.Errorf("bind transaction WAL: %w", err)
	}
	root := transactionOutputRoot(pattern)
	for _, dir := range []string{root, filepath.Join(root, ".ksink-pending"), filepath.Join(root, "committed")} {
		if err := mkdirTxnDurable(dir); err != nil {
			return nil, fmt.Errorf("prepare transactional output directory %s: %w", dir, err)
		}
	}
	lockPath := filepath.Join(root, ".ksink-lock")
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open transaction output lock %s: %w", lockPath, err)
	}
	if err := acquireTxnFileLock(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("acquire exclusive transaction writer lock for %s: %w", root, err)
	}
	if err := syncTxnDirectory(root); err != nil {
		_ = releaseTxnFileLock(lock)
		_ = lock.Close()
		return nil, err
	}
	w := &txnFileWriter{
		pattern: filepath.Clean(pattern),
		root: root,
		wal: log,
		lock: lock,
		txnFiles: make(map[string]*txnDirectory),
	}
	if err := w.recoverFromWAL(); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("recover transaction output from WAL: %w", err)
	}
	return w, nil
}

func (w *txnFileWriter) newTransaction(txnID, instance string) (*txnDirectory, error) {
	if instance == "" || !strings.HasPrefix(instance, SanitizePathSegment(txnID)+"-") {
		return nil, fmt.Errorf("invalid WAL instance for transaction %q", txnID)
	}
	stagingRoot := filepath.Join(w.root, ".ksink-pending")
	committedRoot := filepath.Join(w.root, "committed")
	for _, dir := range []string{stagingRoot, committedRoot} {
		if err := mkdirTxnDurable(dir); err != nil {
			return nil, fmt.Errorf("prepare transaction directory %s: %w", dir, err)
		}
	}
	stagingDir := filepath.Join(stagingRoot, instance)
	if err := os.Mkdir(stagingDir, 0700); err != nil {
		return nil, fmt.Errorf("create transaction staging directory %s: %w", stagingDir, err)
	}
	if err := syncTxnDirectory(stagingRoot); err != nil {
		return nil, fmt.Errorf("persist transaction staging directory %s: %w", stagingDir, err)
	}
	return &txnDirectory{
		stagingDir:   stagingDir,
		committedDir: filepath.Join(committedRoot, instance),
		files:        make(map[string]*txnFileEntry),
	}, nil
}

// Write is the single-message form of WriteBatch.
func (w *txnFileWriter) Write(data []byte, msg *ksink.Message) error {
	return w.WriteBatch([][]byte{data}, []*ksink.Message{msg})
}

func (w *txnFileWriter) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("transactional file writer is closed")
	}
	return w.commitTxnWALLocked(txnID, w.txnFiles[txnID])
}

func (w *txnFileWriter) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("transactional file writer is closed")
	}
	return w.abortTxnWALLocked(txnID, w.txnFiles[txnID])
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
		// Staging is disposable. Reconstruct decided commits on restart.
		if err := os.RemoveAll(txn.stagingDir); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := syncTxnDirectory(filepath.Dir(txn.stagingDir)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.txnFiles = nil
	if w.lock != nil {
		if err := releaseTxnFileLock(w.lock); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("release transaction writer lock: %w", err)
		}
		if err := w.lock.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close transaction writer lock: %w", err)
		}
		w.lock = nil
	}
	return firstErr
}

func (w *txnFileWriter) resolvePath(txnID, topic string) string {
	path := strings.ReplaceAll(w.pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	path = strings.ReplaceAll(path, TopicPlaceholder, SanitizePathSegment(topic))
	return filepath.Clean(path)
}
