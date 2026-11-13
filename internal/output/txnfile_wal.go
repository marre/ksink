package output

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/marre/ksink/pkg/ksink"
)

// txnWALMarkerFile binds the published directory to the exact transaction
// generation chosen and journaled by the Kafka coordinator.
const txnWALMarkerFile = ".ksink-wal-instance"

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
	if err := w.publishFromWAL(txn, cached != nil); err != nil {
		return err
	}
	delete(w.txnFiles, id)
	return nil
}

func (w *txnFileWriter) validateWALStaging(txn ksink.WALRecoveryTransaction) error {
	stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
	type expectedFile struct {
		digest hash.Hash
		size int64
	}
	expected := make(map[string]*expectedFile)
	err := w.wal.StreamOutput(txn.Instance, func(rec ksink.WALOutputRecord) error {
		if rec.TransactionID != txn.TransactionID || rec.Instance != txn.Instance {
			return fmt.Errorf("WAL record belongs to a different transaction")
		}
		info := expected[rec.Path]
		if info == nil {
			info = &expectedFile{digest: sha256.New()}
			expected[rec.Path] = info
		}
		_, _ = info.digest.Write(rec.Data)
		info.size += int64(len(rec.Data))
		return nil
	})
	if err != nil {
		return err
	}
	for relative, want := range expected {
		path := filepath.Join(stage, relative)
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("verify staged WAL file %s: %w", path, err)
		}
		digest := sha256.New()
		size, readErr := io.Copy(digest, file)
		if closeErr := file.Close(); readErr == nil {
			readErr = closeErr
		}
		if readErr != nil {
			return fmt.Errorf("read staged WAL file %s: %w", path, readErr)
		}
		if size != want.size || !bytes.Equal(digest.Sum(nil), want.digest.Sum(nil)) {
			return fmt.Errorf("staged WAL file differs from logged payload: %s", path)
		}
		if err := syncTxnParentChain(filepath.Dir(path), stage); err != nil {
			return err
		}
	}
	return nil
}

func (w *txnFileWriter) publishFromWAL(txn ksink.WALRecoveryTransaction, useCachedStaging bool) error {
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

	if useCachedStaging {
		// All cached files were verified byte-for-byte against the WAL and
		// their handles were synced/closed by CommitTxn. Persist file names.
		for _, rec := range txn.Records {
			if err := syncTxnParentChain(filepath.Dir(filepath.Join(stage, rec.Path)), stage); err != nil {
				return err
			}
		}
	} else {
		// After a crash, staged bytes cannot be trusted. Throw them away
		// and reconstruct every byte from the authoritative WAL.
		if err := w.rebuildWALStaging(txn); err != nil {
			return err
		}
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
	// This streams one bounded WAL frame at a time. Memory usage does not
	// grow with the total payload size of an uncommitted transaction.
	return w.wal.StreamOutput(txn.Instance, func(rec ksink.WALOutputRecord) error {
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
		return syncTxnParentChain(filepath.Dir(path), stage)
	})
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
			if err := w.publishFromWAL(txn, false); err != nil {
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
			// No irreversible decision: discard staged output and record
			// the recovery abort in the WAL. This releases the payloads
			// for compaction even when a transactional ID is never reused.
			stage := filepath.Join(w.root, ".ksink-pending", txn.Instance)
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
			if err := syncTxnDirectory(filepath.Dir(stage)); err != nil {
				return err
			}
			if err := w.wal.MarkAborted(txn.TransactionID, txn.Instance); err != nil {
				return fmt.Errorf("record recovery abort for %q: %w", txn.TransactionID, err)
			}
		default:
			return fmt.Errorf("unexpected WAL decision %q", txn.Decision)
		}
	}
	// All recoverable staging is represented by the WAL. Never silently
	// process or delete an unfamiliar directory from an older transaction
	// implementation (or from another process). Require an explicit cleanup
	// by the operator instead of inventing a commit/abort decision.
	entries, err := os.ReadDir(filepath.Join(w.root, ".ksink-pending"))
	if err != nil {
		return fmt.Errorf("scan WAL staging directory: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("unrecognized staging entry %q is not recorded in the transaction WAL",
			entries[0].Name())
	}
	return nil
}
