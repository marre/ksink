package ksink

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const durableTxnStateVersion = 1

// txnBatchSignature is a bounded idempotency record for a previously
// acknowledged Kafka RecordBatch. Sequences are scoped to the producer epoch,
// topic and partition, and do not reset when one transaction ends.
type txnBatchSignature struct {
	Base     int32  `json:"base"`
	Count    int32  `json:"count"`
	Checksum string `json:"checksum"`
}

type txnPartitionSequence struct {
	Next    int32               `json:"next"`
	Recent  []txnBatchSignature `json:"recent"`
}

type storedTxnState struct {
	ProducerID   int64                            `json:"producer_id"`
	Epoch        int16                            `json:"epoch"`
	Active       bool                             `json:"active"`
	HasOutcome   bool                             `json:"has_outcome"`
	LastCommit   bool                             `json:"last_commit"`
	Pending      string                           `json:"pending,omitempty"`
	Sequences    map[string]txnPartitionSequence  `json:"sequences,omitempty"`
}

type txnStateSnapshot struct {
	Version   int                       `json:"version"`
	Counter   int64                     `json:"counter"`
	States    map[string]storedTxnState `json:"states"`
}

type durableTxnStore struct {
	dir  string
	lock *os.File
}

func txnSyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open coordinator state directory %s: %w", dir, err)
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("sync coordinator state directory %s: %w", dir, err)
	}
	return nil
}

func createTxnStateDir(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("coordinator state path %s is not a directory", dir)
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("invalid transaction state directory %s", dir)
	}
	if err := createTxnStateDir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create coordinator state directory %s: %w", dir, err)
	}
	return txnSyncDirectory(parent)
}

func openDurableTxnStore(path string) (*durableTxnStore, txnStateSnapshot, error) {
	var snapshot txnStateSnapshot
	dir, err := filepath.Abs(path)
	if err != nil {
		return nil, snapshot, fmt.Errorf("resolve transaction state directory: %w", err)
	}
	if err := createTxnStateDir(dir); err != nil {
		return nil, snapshot, err
	}
	lockPath := filepath.Join(dir, ".ksink-coordinator.lock")
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, snapshot, fmt.Errorf("open coordinator lock: %w", err)
	}
	if err := lockTxnStateFile(lock); err != nil {
		_ = lock.Close()
		return nil, snapshot, fmt.Errorf("acquire exclusive coordinator lock %s: %w", lockPath, err)
	}
	store := &durableTxnStore{dir: dir, lock: lock}
	rollback := func(err error) (*durableTxnStore, txnStateSnapshot, error) {
		_ = store.close()
		return nil, txnStateSnapshot{}, err
	}
	if err := txnSyncDirectory(dir); err != nil {
		return rollback(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return store, txnStateSnapshot{Version: durableTxnStateVersion, States: make(map[string]storedTxnState)}, nil
	}
	if err != nil {
		return rollback(fmt.Errorf("load coordinator state: %w", err))
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return rollback(fmt.Errorf("decode coordinator state: %w", err))
	}
	if snapshot.Version != durableTxnStateVersion || snapshot.Counter < 0 ||
		len(snapshot.States) > maxTxnStates {
		return rollback(fmt.Errorf("invalid coordinator state version, counter or size"))
	}
	for id, st := range snapshot.States {
		if id == "" || st.ProducerID <= 0 || st.ProducerID > snapshot.Counter ||
			st.Epoch < 0 || (st.Pending != "" && st.Pending != "commit" && st.Pending != "abort") {
			return rollback(fmt.Errorf("invalid durable transaction record for %q", id))
		}
	}
	if snapshot.States == nil {
		snapshot.States = make(map[string]storedTxnState)
	}
	return store, snapshot, nil
}

func (store *durableTxnStore) save(snapshot txnStateSnapshot) error {
	if store == nil {
		return nil
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal coordinator snapshot: %w", err)
	}
	tmp := filepath.Join(store.dir, "state.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("create coordinator temporary state: %w", err)
	}
	if n, writeErr := f.Write(data); writeErr != nil || n != len(data) {
		_ = f.Close()
		return fmt.Errorf("write coordinator state: %v, %d/%d bytes", writeErr, n, len(data))
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync coordinator state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close coordinator state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(store.dir, "state.json")); err != nil {
		return fmt.Errorf("publish coordinator state: %w", err)
	}
	return txnSyncDirectory(store.dir)
}

func (store *durableTxnStore) close() error {
	if store == nil || store.lock == nil {
		return nil
	}
	err := unlockTxnStateFile(store.lock)
	if closeErr := store.lock.Close(); err == nil {
		err = closeErr
	}
	store.lock = nil
	return err
}

// persistTxnStateLocked must be called while holding s.txnMu. If persistence
// fails, the coordinator is immediately fenced from accepting further
// mutations. Recovery on restart chooses the last durable snapshot.
func (s *Server) persistTxnStateLocked() error {
	if s.txnStore == nil {
		return nil
	}
	if s.txnStoreErr != nil {
		return s.txnStoreErr
	}
	snapshot := txnStateSnapshot{
		Version: durableTxnStateVersion,
		Counter: s.producerIDCounter.Load(),
		States: make(map[string]storedTxnState, len(s.txnStates)),
	}
	for id, st := range s.txnStates {
		snapshot.States[id] = storedTxnState{
			ProducerID: st.producerID, Epoch: st.epoch,
			Active: st.active, HasOutcome: st.hasOutcome,
			LastCommit: st.lastCommit, Pending: st.pendingAction,
			Sequences: st.sequences,
		}
	}
	if err := s.txnStore.save(snapshot); err != nil {
		s.txnStoreErr = fmt.Errorf("durable transaction coordinator unavailable: %w", err)
		return s.txnStoreErr
	}
	return nil
}

// initializeTxnState reloads persisted producer IDs and outcomes. Any active
// transaction without an acknowledged EndTxn outcome was discarded by the
// filesystem staging recovery; it MUST NOT be resumed on a new server process.
// All producer epochs require a fresh InitProducerID before new Produce.
func (s *Server) initializeTxnState() error {
	if s.cfg.TransactionStateDir == "" {
		return nil
	}
	store, snapshot, err := openDurableTxnStore(s.cfg.TransactionStateDir)
	if err != nil {
		return err
	}
	s.txnStore = store
	s.producerIDCounter.Store(snapshot.Counter)
	for id, old := range snapshot.States {
		if old.Sequences == nil {
			old.Sequences = make(map[string]txnPartitionSequence)
		}
		s.txnStates[id] = &txnState{
			producerID: old.ProducerID, epoch: old.Epoch,
			hasOutcome: old.HasOutcome, lastCommit: old.LastCommit,
			pendingAction: old.Pending, sequences: old.Sequences,
			restartRequired: true,
		}
		// Pending commit/abort is outcome-ambiguous across a process crash.
		// Do not claim an EndTxn succeeded without a stored outcome.
		if old.Pending != "" {
			s.txnStates[id].hasOutcome = false
		}
	}
	// The abort-on-restart decision, including the fact that old producer
	// epochs cannot accept new writes, is made durable before opening port.
	s.txnMu.Lock()
	err = s.persistTxnStateLocked()
	s.txnMu.Unlock()
	if err != nil {
		_ = store.close()
		s.txnStore = nil
		return err
	}
	return nil
}
