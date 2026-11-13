package ksink

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	transactionWALVersion = 1
	transactionWALMaxFrame = 64 << 20
	transactionWALCompactAt = 64 << 20
)

var transactionWALCRCTable = crc32.MakeTable(crc32.Castagnoli)

// WALOutputRecord holds the exact formatted bytes and relative path for a
// transaction. Staging files are only a disposable cache of these records.
type WALOutputRecord struct {
	TransactionID string
	Instance      string
	Path          string
	Data          []byte
}

// walOutputLocation references the authoritative payload in the WAL
// without retaining message bodies in coordinator memory.
type walOutputLocation struct {
	offset int64
	meta   WALOutputRecord // Data is always nil
}

// WALRecoveryTransaction describes the last instance of a producer for a
// materializer. A commit decision is irreversible even if publishing failed.
type WALRecoveryTransaction struct {
	TransactionID string
	Instance      string
	Decision      string
	Published     bool
	PublishedHasOutput bool
	Aborted       bool
	Records       []WALOutputRecord // metadata only; read bytes through StreamOutput
}

type transactionWALRecord struct {
	Version  int
	Type     string
	Binding  string
	State    *txnStateSnapshot
	Output   *WALOutputRecord
	TxnID    string
	Instance string
	HasOutput bool
}

// TransactionLog is the single durable authority for transactional Kafka
// state and filesystem output. Only the owner holding the file lock can
// mutate it. Each append has one checksum and is fsynced before success.
//
// The log does NOT make application side effects in arbitrary Read consumers
// transactional; only the associated filesystem writer can materialize it.
type TransactionLog struct {
	mu        sync.Mutex
	dir       string
	logFile   *os.File
	lockFile  *os.File
	broken    error
	binding   string
	state     txnStateSnapshot
	data      map[string][]walOutputLocation
	published map[string]bool
	publishedHasOutput map[string]bool
	aborted   map[string]bool
	size      int64
}

func txnWALValidInstance(instance string) bool {
	if len(instance) < 34 || strings.ContainsAny(instance, "/\\") {
		return false
	}
	if instance[len(instance)-33] != '-' {
		return false
	}
	for _, ch := range instance[len(instance)-32:] {
		if !strings.ContainsRune("0123456789abcdef", ch) {
			return false
		}
	}
	return true
}

func validTxnWALPath(path string) bool {
	return path != "" && path != "." && !filepath.IsAbs(path) &&
		filepath.Clean(path) == path && path != ".." &&
		!strings.HasPrefix(path, ".."+string(filepath.Separator))
}

func seedTxnProducerCounter() (int64, error) {
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return 0, err
	}
	return int64(1<<60) | int64(binary.BigEndian.Uint64(seed[:])&((1<<60)-1)), nil
}

// OpenTransactionLog opens the WAL in an exclusive, durable state directory.
// A pre-existing state.json from the obsolete PR #52 prototype is not
// silently ignored; such a directory requires explicit migration.
func OpenTransactionLog(path string) (*TransactionLog, error) {
	dir, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := createTxnStateDir(dir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".ksink-coordinator.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open transaction log lock: %w", err)
	}
	if err := lockTxnStateFile(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("acquire exclusive coordinator lock: %w", err)
	}
	if err := txnSyncDirectory(dir); err != nil {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); err == nil {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("legacy state.json found in %s; migration is required before enabling the WAL", dir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, err
	}

	// A nonauthoritative bootstrap marker catches accidental deletion or
	// truncation of an established WAL. Never silently initialize a new
	// producer history alongside previously committed output.
	bootstrap := filepath.Join(dir, ".ksink-wal-created")
	_, bootErr := os.Lstat(bootstrap)
	if bootErr != nil && !errors.Is(bootErr, fs.ErrNotExist) {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, bootErr
	}
	walPath := filepath.Join(dir, "transactions.wal")
	walInfo, walErr := os.Stat(walPath)
	if bootErr == nil && (errors.Is(walErr, fs.ErrNotExist) ||
		(walErr == nil && walInfo.Size() == 0)) {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("established transaction WAL is missing or empty; refusing to reset producer history")
	}
	if walErr != nil && !errors.Is(walErr, fs.ErrNotExist) {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, walErr
	}
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		_ = unlockTxnStateFile(lock)
		_ = lock.Close()
		return nil, fmt.Errorf("open transaction WAL: %w", err)
	}
	log := &TransactionLog{
		dir: dir, lockFile: lock, logFile: file,
		state: txnStateSnapshot{Version: durableTxnStateVersion, States: make(map[string]storedTxnState)},
		data: make(map[string][]walOutputLocation),
		published: make(map[string]bool),
		publishedHasOutput: make(map[string]bool),
		aborted: make(map[string]bool),
	}
	if err := txnSyncDirectory(dir); err != nil {
		_ = log.Close()
		return nil, err
	}
	if err := log.load(); err != nil {
		_ = log.Close()
		return nil, err
	}
	if log.state.Counter == 0 {
		counter, err := seedTxnProducerCounter()
		if err != nil {
			_ = log.Close()
			return nil, fmt.Errorf("seed producer ID space: %w", err)
		}
		log.state.Counter = counter
		if err := log.AppendState(log.state); err != nil {
			_ = log.Close()
			return nil, err
		}
	}
	if errors.Is(bootErr, fs.ErrNotExist) {
		marker, err := os.OpenFile(bootstrap, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = log.Close()
			return nil, fmt.Errorf("create WAL bootstrap marker: %w", err)
		}
		_, err = marker.Write([]byte("ksink-wal-v1\n"))
		if err == nil {
			err = marker.Sync()
		}
		if closeErr := marker.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = txnSyncDirectory(dir)
		}
		if err != nil {
			_ = log.Close()
			return nil, fmt.Errorf("persist WAL bootstrap marker: %w", err)
		}
	}
	return log, nil
}

func (l *TransactionLog) load() error {
	// The caller has exclusive ownership of the file lock at this point.
	_, err := l.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	var offset int64
	for {
		var header [8]byte
		n, err := io.ReadFull(l.logFile, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			break
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break // a torn trailing header (no complete record)
		}
		if err != nil {
			return fmt.Errorf("read WAL header at %d: %w", offset, err)
		}
		length := binary.BigEndian.Uint32(header[:4])
		if length == 0 || length > transactionWALMaxFrame {
			return fmt.Errorf("invalid WAL record length %d at %d", length, offset)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(l.logFile, payload); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break // torn final record, not a durable acknowledged append
		} else if err != nil {
			return fmt.Errorf("read WAL record at %d: %w", offset, err)
		}
		if crc32.Checksum(payload, transactionWALCRCTable) != binary.BigEndian.Uint32(header[4:]) {
			return fmt.Errorf("transaction WAL checksum mismatch at %d", offset)
		}
		var record transactionWALRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode WAL record at %d: %w", offset, err)
		}
		if err := l.apply(record, offset); err != nil {
			return fmt.Errorf("invalid WAL record at %d: %w", offset, err)
		}
		offset += int64(len(header)) + int64(length)
	}
	info, err := l.logFile.Stat()
	if err != nil {
		return err
	}
	if info.Size() != offset {
		// Only incomplete trailing frames may be truncated. A checksum
		// mismatch or malformed complete record is a fatal error instead.
		if err := l.logFile.Truncate(offset); err != nil {
			return fmt.Errorf("truncate torn WAL tail: %w", err)
		}
		if err := l.logFile.Sync(); err != nil {
			return fmt.Errorf("persist WAL tail repair: %w", err)
		}
	}
	l.size = offset
	_, err = l.logFile.Seek(0, io.SeekEnd)
	return err
}

func (l *TransactionLog) apply(rec transactionWALRecord, frameOffset int64) error {
	if rec.Version != transactionWALVersion {
		return fmt.Errorf("unsupported WAL record version %d", rec.Version)
	}
	switch rec.Type {
	case "binding":
		if rec.Binding == "" || (l.binding != "" && l.binding != rec.Binding) {
			return fmt.Errorf("conflicting WAL output binding")
		}
		l.binding = rec.Binding
	case "state":
		if rec.State == nil || rec.State.Version != durableTxnStateVersion ||
			rec.State.Counter <= 0 || len(rec.State.States) > maxTxnStates {
			return fmt.Errorf("invalid coordinator state checkpoint")
		}
		l.state = *rec.State
		if l.state.States == nil {
			l.state.States = make(map[string]storedTxnState)
		}
		for id, st := range l.state.States {
			if id == "" || st.ProducerID <= 0 || st.ProducerID > l.state.Counter ||
				st.Epoch < 0 || (st.Pending != "" && st.Pending != "commit" && st.Pending != "abort") ||
				(st.Instance != "" && !txnWALValidInstance(st.Instance)) {
				return fmt.Errorf("invalid persisted transaction %q", id)
			}
		}
	case "output":
		if rec.Output == nil || rec.Output.TransactionID == "" ||
			!txnWALValidInstance(rec.Output.Instance) || !validTxnWALPath(rec.Output.Path) {
			return fmt.Errorf("invalid WAL output record")
		}
		if l.published[rec.Output.Instance] || l.aborted[rec.Output.Instance] {
			return fmt.Errorf("payload appended after completed transaction %s", rec.Output.Instance)
		}
		metadata := *rec.Output
		metadata.Data = nil
		l.data[rec.Output.Instance] = append(l.data[rec.Output.Instance],
			walOutputLocation{offset: frameOffset, meta: metadata})
	case "published":
		if rec.TxnID == "" || !txnWALValidInstance(rec.Instance) || l.aborted[rec.Instance] {
			return fmt.Errorf("invalid WAL publication marker")
		}
		l.published[rec.Instance] = true
		l.publishedHasOutput[rec.Instance] = rec.HasOutput
		delete(l.data, rec.Instance)
	case "aborted":
		if rec.TxnID == "" || !txnWALValidInstance(rec.Instance) || l.published[rec.Instance] {
			return fmt.Errorf("invalid WAL abort marker")
		}
		l.aborted[rec.Instance] = true
		delete(l.data, rec.Instance)
	default:
		return fmt.Errorf("unknown WAL record type %q", rec.Type)
	}
	return nil
}

func (l *TransactionLog) appendLocked(rec transactionWALRecord) error {
	if l.broken != nil {
		return l.broken
	}
	if l.logFile == nil {
		return errors.New("transaction log is closed")
	}
	rec.Version = transactionWALVersion
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > transactionWALMaxFrame {
		return fmt.Errorf("WAL record size %d exceeds maximum %d", len(payload), transactionWALMaxFrame)
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.Checksum(payload, transactionWALCRCTable))
	full := append(header[:], payload...)
	n, err := l.logFile.Write(full)
	if err == nil && n != len(full) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = l.logFile.Sync()
	}
	if err != nil {
		l.broken = fmt.Errorf("transaction WAL append permanently failed: %w", err)
		return l.broken
	}
	if err := l.apply(rec, l.size); err != nil {
		l.broken = fmt.Errorf("transaction WAL invariant failed: %w", err)
		return l.broken
	}
	l.size += int64(len(full))
	if l.size > transactionWALCompactAt {
		if err := l.compactLocked(); err != nil {
			l.broken = fmt.Errorf("transaction WAL compaction failed: %w", err)
			return l.broken
		}
	}
	return nil
}

func cloneTxnStateSnapshot(src txnStateSnapshot) txnStateSnapshot {
	out := txnStateSnapshot{
		Version: src.Version, Counter: src.Counter,
		States: make(map[string]storedTxnState, len(src.States)),
	}
	for key, state := range src.States {
		state.Sequences = copyTxnSequences(state.Sequences)
		out.States[key] = state
	}
	return out
}

// AppendState durably replaces the coordinator's logical state. It shares
// ordering and fsync with output records and irreversible decisions. Deep
// copying prevents in-memory server mutations from changing replay state
// without having passed the WAL durability barrier.
func (l *TransactionLog) AppendState(snapshot txnStateSnapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	copied := cloneTxnStateSnapshot(snapshot)
	return l.appendLocked(transactionWALRecord{Type: "state", State: &copied})
}

func (l *TransactionLog) CurrentState() txnStateSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneTxnStateSnapshot(l.state)
}

// BindOutput prevents a restart under an unintended output pattern/root.
// Records already contain formatted bytes, so a different serialization
// configuration cannot rewrite them accidentally.
func (l *TransactionLog) BindOutput(pattern string) error {
	abs, err := filepath.Abs(pattern)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.binding == abs {
		return nil
	}
	if l.binding != "" {
		return fmt.Errorf("transaction WAL output binding changed: %q != %q", l.binding, abs)
	}
	return l.appendLocked(transactionWALRecord{Type: "binding", Binding: abs})
}

// AppendOutput persists a message as the exact formatted bytes and relative
// filename which must be reproduced after a crash. The coordinator must
// already have journaled this transaction's active instance.
func (l *TransactionLog) AppendOutput(txnID, instance, relative string, data []byte) error {
	if !validTxnWALPath(relative) || !txnWALValidInstance(instance) {
		return fmt.Errorf("invalid transaction WAL output destination")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.binding == "" {
		return errors.New("WAL output writer is not bound")
	}
	st, exists := l.state.States[txnID]
	if !exists || !st.Active || st.Pending != "" || st.Instance != instance {
		return fmt.Errorf("transaction %q is not an active WAL instance", txnID)
	}
	payload := WALOutputRecord{TransactionID: txnID, Instance: instance, Path: relative, Data: append([]byte(nil), data...)}
	return l.appendLocked(transactionWALRecord{Type: "output", Output: &payload})
}

func (l *TransactionLog) Decision(txnID, instance string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state.States[txnID]
	if !ok || st.Instance != instance {
		return ""
	}
	if st.Pending != "" {
		return st.Pending
	}
	if st.HasOutcome {
		if st.LastCommit {
			return "commit"
		}
		return "abort"
	}
	return ""
}

func (l *TransactionLog) Published(instance string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.published[instance]
}

func (l *TransactionLog) Aborted(instance string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.aborted[instance]
}

// MarkPublished is written only after the output directory and both parent
// directories have been synced. It can be safely retried after a crash.
func (l *TransactionLog) MarkPublished(txnID, instance string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state.States[txnID]
	if !ok || st.Instance != instance || (st.Pending != "commit" && (!st.HasOutcome || !st.LastCommit)) {
		return fmt.Errorf("no durable commit decision for %s/%s", txnID, instance)
	}
	if l.published[instance] {
		return nil
	}
	return l.appendLocked(transactionWALRecord{
		Type: "published", TxnID: txnID, Instance: instance,
		HasOutput: len(l.data[instance]) > 0,
	})
}

func (l *TransactionLog) MarkAborted(txnID, instance string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state.States[txnID]
	if !ok || st.Instance != instance || st.Pending == "commit" || (st.HasOutcome && st.LastCommit) {
		return fmt.Errorf("cannot abort committed transaction %s/%s", txnID, instance)
	}
	if l.aborted[instance] {
		return nil
	}
	return l.appendLocked(transactionWALRecord{Type: "aborted", TxnID: txnID, Instance: instance})
}

func (l *TransactionLog) RecoveryTransactions() []WALRecoveryTransaction {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.state.States))
	for id := range l.state.States {
		names = append(names, id)
	}
	sort.Strings(names)
	out := make([]WALRecoveryTransaction, 0, len(names))
	for _, id := range names {
		st := l.state.States[id]
		if st.Instance == "" {
			continue
		}
		decision := st.Pending
		if decision == "" && st.HasOutcome {
			if st.LastCommit {
				decision = "commit"
			} else {
				decision = "abort"
			}
		}
		records := l.data[st.Instance]
		copied := make([]WALOutputRecord, len(records))
		for i, r := range records {
			copied[i] = r.meta
		}
		out = append(out, WALRecoveryTransaction{
			TransactionID: id, Instance: st.Instance, Decision: decision,
			Published: l.published[st.Instance], PublishedHasOutput: l.publishedHasOutput[st.Instance],
			Aborted: l.aborted[st.Instance],
			Records: copied,
		})
	}
	return out
}

func writeTransactionWALFrame(file *os.File, rec transactionWALRecord) (int64, error) {
	rec.Version = transactionWALVersion
	raw, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	if len(raw) > transactionWALMaxFrame {
		return 0, fmt.Errorf("WAL checkpoint record exceeds %d bytes", transactionWALMaxFrame)
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(raw)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.Checksum(raw, transactionWALCRCTable))
	buf := append(hdr[:], raw...)
	n, err := file.Write(buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	return int64(n), err
}

// compactLocked replaces the WAL with a durable checkpoint. The checkpoint
// contains producer states, the output binding, and only uncompleted payloads.
func (l *TransactionLog) compactLocked() error {
	temp := filepath.Join(l.dir, "transactions.wal.compacting")
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = file.Close()
		return err
	}
	var total int64
	newData := make(map[string][]walOutputLocation)
	write := func(rec transactionWALRecord) error {
		n, err := writeTransactionWALFrame(file, rec)
		total += n
		return err
	}
	if l.binding != "" {
		if err := write(transactionWALRecord{Type: "binding", Binding: l.binding}); err != nil {
			return fail(err)
		}
	}
	if err := write(transactionWALRecord{Type: "state", State: &l.state}); err != nil {
		return fail(err)
	}
	for _, txn := range l.RecoveryTransactionsLocked() {
		if txn.Published {
			if err := write(transactionWALRecord{
				Type: "published", TxnID: txn.TransactionID, Instance: txn.Instance,
				HasOutput: txn.PublishedHasOutput,
			}); err != nil {
				return fail(err)
			}
		} else if txn.Aborted {
			if err := write(transactionWALRecord{Type: "aborted", TxnID: txn.TransactionID, Instance: txn.Instance}); err != nil {
				return fail(err)
			}
		} else {
			for _, ref := range l.data[txn.Instance] {
				record, err := readWALOutputAt(l.logFile, ref)
				if err != nil {
					return fail(err)
				}
				start := total
				if err := write(transactionWALRecord{Type: "output", Output: &record}); err != nil {
					return fail(err)
				}
				newData[txn.Instance] = append(newData[txn.Instance],
					walOutputLocation{offset: start, meta: ref.meta})
			}
		}
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := os.Rename(temp, filepath.Join(l.dir, "transactions.wal")); err != nil {
		return fail(err)
	}
	if err := txnSyncDirectory(l.dir); err != nil {
		return fail(err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	old := l.logFile
	l.logFile = file
	l.data = newData
	l.size = total
	if err := old.Close(); err != nil {
		return err
	}
	return nil
}

func (l *TransactionLog) RecoveryTransactionsLocked() []WALRecoveryTransaction {
	// Caller already holds l.mu. This is the small, allocation-friendly
	// checkpoint form of RecoveryTransactions, without copying payloads.
	var out []WALRecoveryTransaction
	for id, st := range l.state.States {
		if st.Instance == "" {
			continue
		}
		out = append(out, WALRecoveryTransaction{
			TransactionID: id, Instance: st.Instance,
			Published: l.published[st.Instance], PublishedHasOutput: l.publishedHasOutput[st.Instance],
			Aborted: l.aborted[st.Instance],
			Records: func() []WALOutputRecord {
				refs := l.data[st.Instance]
				out := make([]WALOutputRecord, len(refs))
				for i, ref := range refs {
					out[i] = ref.meta
				}
				return out
			}(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TransactionID < out[j].TransactionID })
	return out
}

// Compact checkpoints the WAL after a successful publication and is also
// available for explicit maintenance. No acknowledged durable state is lost.
func (l *TransactionLog) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken != nil {
		return l.broken
	}
	return l.compactLocked()
}

// Close releases the exclusive WAL lock after all Kafka and writer handlers
// have stopped. Close is idempotent.
func (l *TransactionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var first error
	if l.logFile != nil {
		first = l.logFile.Close()
		l.logFile = nil
	}
	if l.lockFile != nil {
		if err := unlockTxnStateFile(l.lockFile); first == nil {
			first = err
		}
		if err := l.lockFile.Close(); first == nil {
			first = err
		}
		l.lockFile = nil
	}
	return first
}
