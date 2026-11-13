package output

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Filesystem durability and atomic publication helpers shared by staging
// and WAL reconstruction. No separate filesystem commit journal exists.

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

