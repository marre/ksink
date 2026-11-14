package ksink

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const durableTxnStateVersion = 1

// The coordinator snapshots are logical records INSIDE the shared WAL. There
// is no independent state.json or second durable transaction authority.
type txnBatchSignature struct {
	Base     int32
	Count    int32
	Checksum string
	// Original Kafka base offset. This survives exact batch retries and is
	// independent of producer sequence Base.
	Offset int64
}

type txnPartitionSequence struct {
	Next   int32
	Recent []txnBatchSignature
}

type storedTxnState struct {
	ProducerID int64
	Epoch      int16
	Active     bool
	HasOutcome bool
	LastCommit bool
	Pending    string
	Instance   string
	Sequences  map[string]txnPartitionSequence
	// Topics is the durable participant set for this transaction generation.
	// Every topic has exactly partition 0. Names are unique and sorted.
	Topics []string
}

type txnStateSnapshot struct {
	Version int
	Counter int64
	States  map[string]storedTxnState
}

func txnSyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open transaction state directory %s: %w", dir, err)
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("sync transaction state directory %s: %w", dir, err)
	}
	return nil
}

func createTxnStateDir(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("transaction state path %s is not a directory", dir)
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("cannot create transaction state root %s", dir)
	}
	if err := createTxnStateDir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
		if info, statErr := os.Stat(dir); statErr != nil || !info.IsDir() {
			return fmt.Errorf("transaction state directory %s is not a directory", dir)
		}
	}
	return txnSyncDirectory(parent)
}

// persistTxnStateLocked serializes the coordinator state into the same WAL
// used by the output writer. The caller holds s.txnMu. A write error poisons
// this server: it must not acknowledge any further state mutation.
func (s *Server) persistTxnStateLocked() error {
	if s.txnLog == nil {
		return nil
	}
	if s.txnStoreErr != nil {
		return s.txnStoreErr
	}
	snapshot := s.txnSnapshotLocked()
	if err := s.txnLog.AppendState(snapshot); err != nil {
		s.txnStoreErr = fmt.Errorf("transaction WAL is unavailable: %w", err)
		return s.txnStoreErr
	}
	return nil
}

// txnSnapshotLocked makes a coordinator snapshot while s.txnMu is held.
// Ordered output uses this snapshot inside the same WAL acceptance boundary
// as its serialized messages and producer batch signatures.
func (s *Server) txnSnapshotLocked() txnStateSnapshot {
	snapshot := txnStateSnapshot{
		Version: durableTxnStateVersion,
		Counter: s.producerIDCounter.Load(),
		States: make(map[string]storedTxnState, len(s.txnStates)),
	}
	for id, st := range s.txnStates {
		snapshot.States[id] = storedTxnState{
			ProducerID: st.producerID, Epoch: st.epoch, Active: st.active,
			HasOutcome: st.hasOutcome, LastCommit: st.lastCommit,
			Pending: st.pendingAction, Instance: st.instance, Sequences: st.sequences,
			Topics: append([]string(nil), st.topics...),
		}
	}
	return snapshot
}

// initializeTxnState replays the shared WAL. The filesystem materializer must
// have completed recovery *before* the server starts listening, when using a
// shared WAL. A decided commit is only treated as complete if the materializer
// has durably recorded publication of that exact transaction instance.
func (s *Server) initializeTxnState() error {
	if s.txnLog == nil && s.cfg.TransactionStateDir != "" {
		log, err := OpenTransactionLog(s.cfg.TransactionStateDir)
		if err != nil {
			return err
		}
		s.txnLog = log
		s.ownsTxnLog = true
	}
	if s.txnLog == nil {
		return nil
	}
	snapshot := s.txnLog.CurrentState()
	if s.orderedFormatter != nil {
		// A restart fences every old producer epoch. Any transaction that
		// has not durably decided COMMIT/ABORT must be aborted BEFORE new
		// producers can append behind its unresolved topic offsets.
		var mustAbort bool
		for id, old := range snapshot.States {
			if old.Active && old.Pending == "" {
				old.Pending = "abort"
				snapshot.States[id] = old
				mustAbort = true
			}
		}
		if mustAbort {
			if err := s.txnLog.AppendState(snapshot); err != nil {
				return fmt.Errorf("abort undecided ordered WAL transactions on restart: %w", err)
			}
		}
	}
	s.producerIDCounter.Store(snapshot.Counter)
	dirty := false
	for id, old := range snapshot.States {
		if old.Sequences == nil {
			old.Sequences = make(map[string]txnPartitionSequence)
		}
		st := &txnState{
			producerID: old.ProducerID, epoch: old.Epoch, instance: old.Instance,
			hasOutcome: old.HasOutcome, lastCommit: old.LastCommit,
			pendingAction: old.Pending, sequences: old.Sequences,
			topics: append([]string(nil), old.Topics...),
			restartRequired: true,
		}
		// A completed filesystem publication makes a pending COMMIT
		// unambiguous. A crashed pending ABORT is likewise recoverable.
		if old.Pending == "commit" && (s.orderedFormatter != nil || s.txnLog.Published(old.Instance)) {
			st.hasOutcome = true
			st.lastCommit = true
			st.pendingAction = ""
			dirty = true
		} else if old.Pending == "abort" && (s.orderedFormatter != nil || s.txnLog.Aborted(old.Instance)) {
			st.hasOutcome = true
			st.lastCommit = false
			st.pendingAction = ""
			dirty = true
		} else if old.Active && old.Pending == "" {
			// No irreversible decision: the filesystem recovery has
			// discarded the active staging. Never resume the old epoch.
			st.hasOutcome = false
			st.pendingAction = ""
			dirty = true
		} else if old.Pending != "" {
			// Without a matching materializer recovery marker, do not
			// acknowledge a decision or silently fence it away.
			st.needsResolution = true
		}
		s.txnStates[id] = st
	}
	if dirty {
		s.txnMu.Lock()
		err := s.persistTxnStateLocked()
		s.txnMu.Unlock()
		if err != nil {
			if s.ownsTxnLog {
				_ = s.txnLog.Close()
				s.txnLog = nil
			}
			return err
		}
	}
	return nil
}
