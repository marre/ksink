package output

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
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
	poisoned     bool // a failed Write/Sync cannot be committed, only aborted
}

// txnFileWriter atomically publishes a whole transaction by renaming ONE
// directory. Staging lives under <root>/.ksink-pending/<txnID>-<nonce> and
// committed data under <root>/committed/<txnID>-<nonce>. All paths are on
// the same filesystem; readers must only read under <root>/committed.
//
// The original output pattern names files INSIDE each transaction directory.
// {topic} and {partition} may select different files or subdirectories.
// This provides atomic visibility of all files. File payloads are synced
// before a successful Write acknowledgement; a durable commit-intent marker
// enables startup recovery of commits interrupted by a crash. Active staging
// directories without intent are discarded on startup. Producer state
// and outcomes are NOT persisted by the Kafka server.
type txnFileWriter struct {
	pattern  string
	root     string
	wal      *ksink.TransactionLog // nil for legacy standalone writer
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

// KafkaTransactionStateDir is the default durable coordinator-state directory
// for transactional filesystem output. Using the fixed pattern root keeps the
// coordinator and staged files in the same deployment volume.
func KafkaTransactionStateDir(pattern string) string {
	return filepath.Join(transactionOutputRoot(pattern), ".ksink-kafka-state")
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
	root := transactionOutputRoot(pattern)
	if err := mkdirTxnDurable(root); err != nil {
		return nil, fmt.Errorf("prepare transactional file output %s: %w", root, err)
	}
	lockPath := filepath.Join(root, ".ksink-lock")
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open transaction recovery lock %s: %w", lockPath, err)
	}
	if err := acquireTxnFileLock(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("acquire exclusive transaction writer lock for %s: %w", root, err)
	}
	// Keep the lock held until Close, even after recovery completes, so
	// a second ksink instance cannot delete this writer's active staging.
	if err := syncTxnDirectory(root); err != nil {
		_ = releaseTxnFileLock(lock)
		_ = lock.Close()
		return nil, err
	}
	if err := recoverTxnDirectories(root); err != nil {
		_ = releaseTxnFileLock(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("recover transactional file output under %s: %w", root, err)
	}
	return &txnFileWriter{
		pattern:  filepath.Clean(pattern),
		root:     root,
		lock:     lock,
		txnFiles: make(map[string]*txnDirectory),
	}, nil
}

func (w *txnFileWriter) newTransaction(txnID, instance string) (*txnDirectory, error) {
	if instance == "" && w.wal == nil {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, fmt.Errorf("generate transaction instance ID: %w", err)
		}
		instance = SanitizePathSegment(txnID) + "-" + hex.EncodeToString(nonce[:])
	}
	if w.wal != nil && (instance == "" || !strings.HasPrefix(instance, SanitizePathSegment(txnID)+"-")) {
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
		return fmt.Errorf("transaction %q has a pending commit; retry commit before writing", txnID)
	}
	if w.wal != nil && msg.TransactionInstance == "" {
		return fmt.Errorf("transaction %q lacks a durable instance identifier", txnID)
	}
	if txn != nil && w.wal != nil && filepath.Base(txn.stagingDir) != msg.TransactionInstance {
		return fmt.Errorf("transaction %q instance mismatch", txnID)
	}
	if txn != nil && txn.poisoned {
		return fmt.Errorf("transaction %q had an earlier write failure; abort before writing", txnID)
	}

	resolved := w.resolvePath(txnID, msg.Topic, msg.Partition)
	relative, err := filepath.Rel(w.root, resolved)
	if err != nil || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("transaction output path %q escapes root %q", resolved, w.root)
	}
	if txn == nil {
		txn, err = w.newTransaction(txnID, msg.TransactionInstance)
		if err != nil {
			return err
		}
		w.txnFiles[txnID] = txn
	}
	stagedFile := filepath.Join(txn.stagingDir, relative)
	if w.wal != nil {
		if err := w.wal.AppendOutput(txnID, msg.TransactionInstance, relative, data); err != nil {
			txn.poisoned = true
			return fmt.Errorf("persist transaction %q output in WAL: %w", txnID, err)
		}
	}
	entry := txn.files[stagedFile]
	if entry == nil {
		if err := os.MkdirAll(filepath.Dir(stagedFile), 0700); err != nil {
			txn.poisoned = true
			return fmt.Errorf("prepare transaction file directory %s: %w", stagedFile, err)
		}
		f, err := os.OpenFile(stagedFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			txn.poisoned = true
			return fmt.Errorf("create transaction staged file %s: %w", stagedFile, err)
		}
		entry = &txnFileEntry{file: f}
		txn.files[stagedFile] = entry
	}
	// A successful Produce acknowledgement means the staged payload and its
	// filename are persisted. The producer may still need to replay an
	// uncommitted transaction after a server restart.
	if n, err := entry.file.Write(data); err != nil || n != len(data) {
		txn.poisoned = true
		return fmt.Errorf("write transaction %q file %s: wrote %d/%d bytes: %w", txnID, stagedFile, n, len(data), func() error { if err != nil { return err }; return io.ErrShortWrite }())
	}
	if w.wal == nil {
		if err := entry.file.Sync(); err != nil {
			txn.poisoned = true
			return fmt.Errorf("sync transaction %q file %s: %w", txnID, stagedFile, err)
		}
		if err := syncTxnParentChain(filepath.Dir(stagedFile), txn.stagingDir); err != nil {
			txn.poisoned = true
			return fmt.Errorf("sync transaction %q staged filename %s: %w", txnID, stagedFile, err)
		}
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
	if w.wal != nil {
		return w.commitTxnWALLocked(txnID, txn)
	}
	if txn == nil {
		// The server validates EndTxn state; empty transactions create no files.
		return nil
	}
	if txn.poisoned {
		return fmt.Errorf("transaction %q has a failed write and cannot be committed; abort it", txnID)
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

	// The intent decision must only be recorded once all payloads and all
	// nested directory entries are safely on disk.
	if _, statErr := os.Lstat(txn.stagingDir); statErr == nil {
		for name := range txn.files {
			if err := syncTxnParentChain(filepath.Dir(name), txn.stagingDir); err != nil {
				return fmt.Errorf("sync transaction %q directory tree: %w", txnID, err)
			}
		}
		if err := writeTxnCommitIntent(txn.stagingDir, txnID); err != nil {
			return fmt.Errorf("record durable commit intent for %q: %w", txnID, err)
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat transaction %q staging directory: %w", txnID, statErr)
	}

	// Publishing the whole directory is atomic. A successful rename is not
	// acknowledged until both affected parent directories are synchronized.
	if err := renameTxnCommitted(txn.stagingDir, txn.committedDir); err != nil {
		return fmt.Errorf("commit transaction %q: %w", txnID, err)
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
	if w.wal != nil {
		return w.abortTxnWALLocked(txnID, txn)
	}
	if txn == nil {
		return nil
	}
	// The intent marker is an irreversible commit decision. A failed
	// directory rename must be retried/recovered; it cannot be aborted.
	if _, markerErr := os.Lstat(txnIntentPath(txn.stagingDir)); markerErr == nil {
		return fmt.Errorf("transaction %q has a durable commit intent; retry commit instead of aborting", txnID)
	} else if !os.IsNotExist(markerErr) {
		return fmt.Errorf("inspect commit intent for %q: %w", txnID, markerErr)
	}
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
	if err := syncTxnDirectory(filepath.Dir(txn.stagingDir)); err != nil {
		return fmt.Errorf("persist aborted transaction %q removal: %w", txnID, err)
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
		// WAL staging is disposable. Legacy standalone writers must preserve
		// a durable filesystem commit-intent directory across shutdown.
		if w.wal == nil {
			if _, err := os.Lstat(txnIntentPath(txn.stagingDir)); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = err
			}
			continue
			}
		}
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

func (w *txnFileWriter) resolvePath(txnID, topic string, partition int32) string {
	path := strings.ReplaceAll(w.pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	path = strings.ReplaceAll(path, TopicPlaceholder, SanitizePathSegment(topic))
	path = strings.ReplaceAll(path, PartitionPlaceholder, strconv.FormatInt(int64(partition), 10))
	return filepath.Clean(path)
}
