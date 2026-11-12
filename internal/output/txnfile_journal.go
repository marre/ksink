package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// The commit-intent marker is the durable decision to commit a transaction.
// A staging directory without a valid marker is uncommitted and can be
// discarded after a crash. Once the marker has been published and synced,
// the transaction must be finished, never aborted.
const (
	txnCommitIntentFile = ".ksink-commit-intent"
	txnIntentTemporary  = ".ksink-commit-intent.tmp"
	txnIntentVersion    = 1
)

type txnCommitIntent struct {
	Version         int    `json:"version"`
	TransactionalID string `json:"transactional_id"`
	Instance        string `json:"instance"`
}

// syncTxnDirectory rejects filesystems on which directory fsync is unsupported;
// reporting success there would overstate crash-durability guarantees.
func syncTxnDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", path, err)
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("sync directory %s: %w", path, err)
	}
	return nil
}

// mkdirTxnDurable synchronizes the parent after creating any new directory.
// The recursive form also handles a missing static output-root prefix.
func mkdirTxnDurable(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("transaction output path %s is not a directory", path)
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat transaction output directory %s: %w", path, err)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return fmt.Errorf("cannot create filesystem root %s", path)
	}
	if err := mkdirTxnDurable(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create transaction directory %s: %w", path, err)
		}
		if info, statErr := os.Stat(path); statErr != nil || !info.IsDir() {
			return fmt.Errorf("transaction directory %s was created by another process: %v", path, statErr)
		}
	}
	return syncTxnDirectory(parent)
}

// syncTxnParentChain makes new file and subdirectory names durable. It walks
// up to and including stop, which is a previously persisted staging directory.
func syncTxnParentChain(path, stop string) error {
	for {
		if err := syncTxnDirectory(path); err != nil {
			return err
		}
		if path == stop {
			return nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			return fmt.Errorf("directory %s is outside transaction root %s", path, stop)
		}
		path = parent
	}
}

func txnIntentPath(txnDir string) string {
	return filepath.Join(txnDir, txnCommitIntentFile)
}

func validateTxnCommitIntent(txnDir string) error {
	data, err := os.ReadFile(txnIntentPath(txnDir))
	if err != nil {
		return err
	}
	var intent txnCommitIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		return fmt.Errorf("parse transaction commit intent in %s: %w", txnDir, err)
	}
	if intent.Version != txnIntentVersion || intent.TransactionalID == "" ||
		intent.Instance != filepath.Base(txnDir) {
		return fmt.Errorf("invalid transaction commit intent in %s", txnDir)
	}
	// An intent must refer to the name generated from its producer ID.
	if len(intent.Instance) <= 33 ||
		intent.Instance[:len(intent.Instance)-33] != SanitizePathSegment(intent.TransactionalID) ||
		intent.Instance[len(intent.Instance)-33] != '-' {
		return fmt.Errorf("transaction commit intent does not match staging directory %s", txnDir)
	}
	return nil
}

// writeTxnCommitIntent uses an atomic marker rename so a crash during marker
// construction cannot turn a truncated file into a valid commit decision.
func writeTxnCommitIntent(txnDir, txnID string) error {
	marker := txnIntentPath(txnDir)
	if err := validateTxnCommitIntent(txnDir); err == nil {
		// Even a previously successful marker requires syncing its parent
		// before proceeding after a failed earlier attempt.
		return syncTxnDirectory(txnDir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("existing transaction commit marker is invalid: %w", err)
	}
	tmp := filepath.Join(txnDir, txnIntentTemporary)
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stale transaction intent temporary file %s: %w", tmp, err)
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create transaction commit intent %s: %w", tmp, err)
	}
	raw, err := json.Marshal(txnCommitIntent{
		Version: txnIntentVersion, TransactionalID: txnID, Instance: filepath.Base(txnDir),
	})
	if err == nil {
		_, err = f.Write(raw)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("persist transaction intent %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		return fmt.Errorf("publish transaction intent %s: %w", marker, err)
	}
	if err := syncTxnDirectory(txnDir); err != nil {
		return err
	}
	return nil
}

// renameTxnCommitted publishes a transaction's directory in one operation.
// The caller must have synced the commit intent and all payloads first.
func renameTxnCommitted(stagingDir, committedDir string) error {
	_, stagingErr := os.Lstat(stagingDir)
	_, committedErr := os.Lstat(committedDir)
	if committedErr == nil {
		if !errors.Is(stagingErr, fs.ErrNotExist) {
			return fmt.Errorf("transaction destination already exists: %s", committedDir)
		}
		// A previous rename succeeded: finish the durability barrier.
		return syncTxnPublicationParents(stagingDir, committedDir)
	}
	if !errors.Is(committedErr, fs.ErrNotExist) {
		return fmt.Errorf("stat transaction destination %s: %w", committedDir, committedErr)
	}
	if stagingErr != nil {
		return fmt.Errorf("stat transaction source %s: %w", stagingDir, stagingErr)
	}
	if err := os.Rename(stagingDir, committedDir); err != nil {
		// Do not treat an ambiguous success as failure if the destination
		// now exists and the source is gone.
		if _, srcErr := os.Lstat(stagingDir); !errors.Is(srcErr, fs.ErrNotExist) {
			return fmt.Errorf("rename transaction %s to %s: %w", stagingDir, committedDir, err)
		}
		if info, dstErr := os.Lstat(committedDir); dstErr != nil || !info.IsDir() {
			return fmt.Errorf("rename transaction %s to %s: %w", stagingDir, committedDir, err)
		}
	}
	return syncTxnPublicationParents(stagingDir, committedDir)
}

func syncTxnPublicationParents(stagingDir, committedDir string) error {
	if err := syncTxnDirectory(filepath.Dir(stagingDir)); err != nil {
		return err
	}
	if err := syncTxnDirectory(filepath.Dir(committedDir)); err != nil {
		return err
	}
	return nil
}

// recoverTxnDirectories runs before this writer accepts any messages.
// Uncommitted active transactions are conservatively aborted. Durable commit
// intents are completed. Unrecognized/corrupt state stops startup rather than
// silently losing data.
func recoverTxnDirectories(root string) error {
	pending := filepath.Join(root, ".ksink-pending")
	committed := filepath.Join(root, "committed")
	for _, dir := range []string{root, pending, committed} {
		if err := mkdirTxnDurable(dir); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(pending)
	if err != nil {
		return fmt.Errorf("scan pending transactions %s: %w", pending, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return fmt.Errorf("unexpected entry in transaction staging root: %s", filepath.Join(pending, entry.Name()))
		}
		source := filepath.Join(pending, entry.Name())
		marker := txnIntentPath(source)
		if _, err := os.Lstat(marker); errors.Is(err, fs.ErrNotExist) {
			// No committed decision exists, so abandoning an active transaction
			// is safe. This intentionally does not attempt producer resumption.
			if err := os.RemoveAll(source); err != nil {
				return fmt.Errorf("discard uncommitted transaction %s: %w", source, err)
			}
			if err := syncTxnDirectory(pending); err != nil {
				return err
			}
			continue
		} else if err != nil {
			return fmt.Errorf("read transaction intent %s: %w", marker, err)
		}
		if err := validateTxnCommitIntent(source); err != nil {
			return err
		}
		destination := filepath.Join(committed, entry.Name())
		if err := renameTxnCommitted(source, destination); err != nil {
			return fmt.Errorf("recover interrupted commit %s: %w", source, err)
		}
	}
	return nil
}
