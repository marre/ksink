package output

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/marre/ksink/pkg/ksink"
)

// txnWALMarkerFile binds the published directory to the exact transaction
// generation chosen and journaled by the Kafka coordinator.
const txnWALMarkerFile = ".ksink-wal-instance"

// NewTxnFileWriterWithLog uses the shared transaction WAL as the ONLY
// authoritative state for commit/recovery. The caller owns the WAL and must
// close it only after the writer and the Kafka server have stopped.
//
// The standalone NewTxnFileWriter remains available for existing embedders.
// It uses the independent filesystem journal from PR #51.
func NewTxnFileWriterWithLog(pattern string, log *ksink.TransactionLog) (TransactionalWriter, error) {
	if log == nil {
		return nil, fmt.Errorf("transaction WAL must not be nil")
	}
	if !strings.Contains(pattern, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional output pattern must contain %s", TxnIDPlaceholder)
	}
	if err := log.BindOutput(pattern); err != nil {
		return nil, fmt.Errorf("bind transaction WAL to output: %w", err)
	}
	raw, err := NewTxnFileWriter(pattern)
	if err != nil {
		return nil, err
	}
	w := raw.(*txnFileWriter)
	w.wal = log
	if err := w.recoverFromWAL(); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("recover transaction output from WAL: %w", err)
	}
	return w, nil
}

func walTxnInstance(log *ksink.TransactionLog, id string) (ksink.WALRecoveryTransaction, error) {
	for _, txn := range log.RecoveryTransactions() {
		if txn.TransactionID == id {
			return txn, nil
		}
	}
	return ksink.WALRecoveryTransaction{}, fmt.Errorf("transaction %q has no durable WAL instance", id)
}

func walMarkerBytes(txn ksink.WALRecoveryTransaction) []byte {
	return []byte(txn.TransactionID + "\n" + txn.Instance + "\n")
}

func ensureWALMarker(dir string, txn ksink.WALRecoveryTransaction) error {
	marker := filepath.Join(dir, txnWALMarkerFile)
	data, err := os.ReadFile(marker)
	if err == nil {
		if !bytes.Equal(data, walMarkerBytes(txn)) {
			return fmt.Errorf("WAL directory %s has mismatched transaction identity", dir)
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read transaction marker %s: %w", marker, err)
	}
	f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	data = walMarkerBytes(txn)
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncTxnDirectory(dir)
}

func validateCommittedWALDirectory(dir string, txn ksink.WALRecoveryTransaction) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("committed WAL transaction is not a directory: %s", dir)
	}
	if err := ensureExistingWALMarker(dir, txn); err != nil {
		return fmt.Errorf("validate committed transaction %s: %w", dir, err)
	}
	return nil
}

func ensureExistingWALMarker(dir string, txn ksink.WALRecoveryTransaction) error {
	got, err := os.ReadFile(filepath.Join(dir, txnWALMarkerFile))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, walMarkerBytes(txn)) {
		return fmt.Errorf("committed transaction has wrong WAL identity")
	}
	return nil
}

func (w *txnFileWriter) commitTxnWALLocked(id string, cached *txnDirectory) error {
	txn, err := walTxnInstance(w.wal, id)
	if err != nil {
		return err
	}
	if txn.Decision != "commit" {
		return fmt.Errorf("transaction %q has no durable commit decision", id)
	}
	if cached != nil && filepath.Base(cached.stagingDir) != txn.Instance {
		return fmt.Errorf("transaction %q has mismatched cached instance", id)
	}
	if cached != nil {
		if cached.poisoned {
			return fmt.Errorf("transaction %q had a failed write; cannot publish", id)
		}
		cached.committing = true
		for name, entry := range cached.files {
			if entry.closed {
				continue
			}
			if err := entry.file.Sync(); err != nil {
				return fmt.Errorf("sync WAL staged file %s: %w", name, err)
			}
			if err := entry.file.Close(); err != nil {
				entry.closed = true
				return fmt.Errorf("close WAL staged file %s: %w", name, err)
			}
			entry.closed = true
		}
		// Do not trust the staging cache merely because its handles closed:
		// compare to the WAL's authoritative bytes before publishing.
		if err := w.validateWALStaging(txn); err != nil {
			return err
		}
	}
	if err := w.publishFromWAL(txn); err != nil {
		return err
	}
	delete(w.txnFiles, id)
	return nil
}

func (w *txnFileWriter) validateWALStaging(txn ksink.WALRecoveryTransaction) error {
	stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
	expected := make(map[string][]byte)
	for _, rec := range txn.Records {
		expected[rec.Path] = append(expected[rec.Path], rec.Data...)
	}
	for relative, data := range expected {
		path := filepath.Join(stage, relative)
		actual, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("verify staged WAL payload %s: %w", path, err)
		}
		want := sha256.Sum256(data)
		got := sha256.Sum256(actual)
		if len(actual) != len(data) || got != want {
			return fmt.Errorf("staged WAL file differs from logged payload: %s", path)
		}
		if err := syncTxnParentChain(filepath.Dir(path), stage); err != nil {
			return err
		}
	}
	return nil
}

func (w *txnFileWriter) publishFromWAL(txn ksink.WALRecoveryTransaction) error {
	stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
	final := filepath.Join(w.root, "committed", txn.Instance)

	if _, err := os.Lstat(final); err == nil {
		// Collision-resistant instance names are not sufficient proof of
		// ownership; validate the persisted marker before any success.
		if err := validateCommittedWALDirectory(final, txn); err != nil {
			return err
		}
		if err := syncTxnPublicationParents(stage, final); err != nil {
			return err
		}
		return w.wal.MarkPublished(txn.TransactionID, txn.Instance)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat WAL output %s: %w", final, err)
	}
	if txn.Published {
		// A committed output directory cannot simply disappear. We no
		// longer retain its payload after publication/compaction.
		if txn.PublishedHasOutput {
			return fmt.Errorf("published WAL transaction %q lost its committed directory %s", txn.TransactionID, final)
		}
		return nil // deliberately empty transaction
	}
	if len(txn.Records) == 0 {
		return w.wal.MarkPublished(txn.TransactionID, txn.Instance)
	}

	if _, err := os.Lstat(stage); errors.Is(err, fs.ErrNotExist) {
		if err := w.rebuildWALStaging(txn); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("stat WAL staging %s: %w", stage, err)
	}
	// A staging cache exists, possibly after a process crash. It cannot be
	// trusted; reconstruct it from the log for deterministic replay.
	if err := w.rebuildWALStaging(txn); err != nil {
		return err
	}
	if err := ensureWALMarker(stage, txn); err != nil {
		return err
	}
	if err := renameTxnCommitted(stage, final); err != nil {
		return fmt.Errorf("publish WAL transaction %q: %w", txn.TransactionID, err)
	}
	return w.wal.MarkPublished(txn.TransactionID, txn.Instance)
}

func (w *txnFileWriter) rebuildWALStaging(txn ksink.WALRecoveryTransaction) error {
	stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
	if err := os.RemoveAll(stage); err != nil {
		return fmt.Errorf("clear disposable WAL staging %s: %w", stage, err)
	}
	if err := mkdirTxnDurable(filepath.Dir(stage)); err != nil {
		return err
	}
	if err := os.Mkdir(stage, 0700); err != nil {
		return err
	}
	if err := syncTxnDirectory(filepath.Dir(stage)); err != nil {
		return err
	}
	for _, rec := range txn.Records {
		if rec.Instance != txn.Instance || rec.TransactionID != txn.TransactionID {
			return fmt.Errorf("WAL record is associated with wrong transaction instance")
		}
		path := filepath.Join(stage, rec.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		n, writeErr := f.Write(rec.Data)
		if writeErr == nil && n != len(rec.Data) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = f.Sync()
		}
		if closeErr := f.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			return fmt.Errorf("rebuild WAL staged file %s: %w", path, writeErr)
		}
		if err := syncTxnParentChain(filepath.Dir(path), stage); err != nil {
			return err
		}
	}
	return nil
}

func (w *txnFileWriter) abortTxnWALLocked(id string, cached *txnDirectory) error {
	txn, err := walTxnInstance(w.wal, id)
	if err != nil {
		return err
	}
	if txn.Decision != "abort" {
		return fmt.Errorf("transaction %q has no durable abort decision", id)
	}
	if cached != nil && filepath.Base(cached.stagingDir) != txn.Instance {
		return fmt.Errorf("transaction %q has mismatched cached instance", id)
	}
	if cached != nil {
		for name, entry := range cached.files {
			if !entry.closed {
				if err := entry.file.Close(); err != nil {
					return fmt.Errorf("close aborted WAL file %s: %w", name, err)
				}
				entry.closed = true
			}
		}
	}
	stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
	if err := os.RemoveAll(stage); err != nil {
		return err
	}
	if err := syncTxnDirectory(filepath.Dir(stage)); err != nil {
		return err
	}
	if err := w.wal.MarkAborted(id, txn.Instance); err != nil {
		return err
	}
	delete(w.txnFiles, id)
	return nil
}

func (w *txnFileWriter) recoverFromWAL() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, txn := range w.wal.RecoveryTransactions() {
		switch txn.Decision {
		case "commit":
			if err := w.publishFromWAL(txn); err != nil {
				return fmt.Errorf("finish decided commit %s: %w", txn.TransactionID, err)
			}
		case "abort":
			stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
			if err := syncTxnDirectory(filepath.Dir(stage)); err != nil {
				return err
			}
			if err := w.wal.MarkAborted(txn.TransactionID, txn.Instance); err != nil {
				return err
			}
		case "":
			// No irreversible decision: the producer must reinitialize.
			stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
			if err := syncTxnDirectory(filepath.Dir(stage)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected WAL decision %q", txn.Decision)
		}
	}
	return nil
}
