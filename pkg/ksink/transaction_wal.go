package ksink

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"math"
	"sync"
)

const (
	transactionWALVersion = 3
	transactionWALMaxFrame = 64 << 20
	// Compact only when enough WAL bytes are obsolete AND reclaiming
	// them would remove a meaningful fraction of the file. A 1 GiB
	// active transaction must not be copied repeatedly for small state
	// updates or a few completed transactions alongside it.
	transactionWALMinReclaim = 16 << 20
	transactionWALReclaimFraction = 4 // at least 25% of the WAL
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
	key walFileKey
	offset int64
	size int64 // full checksummed frame, including its header
	meta WALOutputRecord // Data is always nil
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
	TopicOutput *WALTopicRecord
	// TopicTerminal is ONE durable decision and all of its topic control
	// marker offsets; no crash may observe only some participants.
	TopicTerminal *walTopicTerminal
	TopicMarker *walTopicMarker // checkpoint-only representation
	TopicProgress *WALTopicProgress
	Decision string
	BatchCount int
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
	// Logical topic/partition-0 streams share physical WAL segments but have
	// independent append offsets and indexes.
	topicRecords map[string][]walTopicLocation
	topicMarkers map[string][]walTopicMarker
	topicNext map[string]int64
	topicProgress map[string]WALTopicProgress
	topicProgressRefs map[string]walFrameRef
	// Immutable decisions are keyed by transaction instance, not mutable
	// transactional.id. Their history is retained until topic output is safe.
	topicDecisions map[string]string
	topicDecisionOwners map[string]string
	topicDecisionRefs map[string]walFrameRef
	topicInstances map[string]bool
	published map[string]bool
	publishedHasOutput map[string]bool
	aborted   map[string]bool
	size      int64
	// reclaimable counts superseded coordinator snapshots, abandoned
	// transaction data, and published/aborted output frames. It is
	// reconstructed during replay and reset after a successful checkpoint.
	reclaimable int64
	// The manifest selects the authoritative physical WAL files. Kafka
	// transaction outcomes remain exclusively inside their records.
	manifest walManifest
	fileStats map[walFileKey]*walFileStats
	stateRef walFrameRef
	bindingRef walFrameRef
	markerRefs map[string]walFrameRef
	faultHook func(string) error // test-only, called while holding mu
	segmentLimit int64 // tests may reduce rotation threshold; defaults to 64 MiB
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

// OpenTransactionLog opens a WAL in an exclusively locked state directory.
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
	log := &TransactionLog{
		dir:dir, lockFile:lock,
		state:txnStateSnapshot{Version:durableTxnStateVersion,States:make(map[string]storedTxnState)},
		data:make(map[string][]walOutputLocation),
		topicRecords:make(map[string][]walTopicLocation),
		topicMarkers:make(map[string][]walTopicMarker),
		topicNext:make(map[string]int64),
		topicProgress:make(map[string]WALTopicProgress),
		topicProgressRefs:make(map[string]walFrameRef),
		topicDecisions:make(map[string]string),
		topicDecisionOwners:make(map[string]string),
		topicDecisionRefs:make(map[string]walFrameRef),
		topicInstances:make(map[string]bool),
		published:make(map[string]bool),
		publishedHasOutput:make(map[string]bool),
		aborted:make(map[string]bool),
		markerRefs:make(map[string]walFrameRef),
		fileStats:make(map[walFileKey]*walFileStats),
		segmentLimit:transactionWALSegmentBytes,
	}
	if err:=log.initSegmentStore();err!=nil{_ = log.Close();return nil,err}
	if err:=log.load();err!=nil{_ = log.Close();return nil,err}
	if err:=log.cleanupOrphanWALFiles();err!=nil{_ = log.Close();return nil,err}
	if log.state.Counter == 0 {
		if bootErr == nil {
			_ = log.Close()
			return nil, fmt.Errorf("established transaction WAL has lost its producer state; refusing to reset history")
		}
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
		_, err = marker.Write([]byte("ksink-wal-v3\n"))
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

func (l *TransactionLog) resetReplay() {
	l.binding=""
	l.state=txnStateSnapshot{Version:durableTxnStateVersion,States:make(map[string]storedTxnState)}
	l.data=make(map[string][]walOutputLocation)
	l.topicRecords=make(map[string][]walTopicLocation)
	l.topicMarkers=make(map[string][]walTopicMarker)
	l.topicNext=make(map[string]int64)
	l.topicProgress=make(map[string]WALTopicProgress)
	l.topicProgressRefs=make(map[string]walFrameRef)
	l.topicDecisions=make(map[string]string)
	l.topicDecisionOwners=make(map[string]string)
	l.topicDecisionRefs=make(map[string]walFrameRef)
	l.topicInstances=make(map[string]bool)
	l.published=make(map[string]bool)
	l.publishedHasOutput=make(map[string]bool)
	l.aborted=make(map[string]bool)
	l.fileStats=make(map[walFileKey]*walFileStats)
	l.markerRefs=make(map[string]walFrameRef)
	l.stateRef=walFrameRef{}
	l.bindingRef=walFrameRef{}
	l.size=0
	l.reclaimable=0
}

func (l *TransactionLog) load() error {
	l.resetReplay()
	if l.manifest.Snapshot!=0 {
		if err:=l.scanWALFile(walFileKey{id:l.manifest.Snapshot,snapshot:true},false);err!=nil{return err}
	}
	for i,id:=range l.manifest.Segments {
		active:=i==len(l.manifest.Segments)-1
		if err:=l.scanWALFile(walFileKey{id:id},active);err!=nil{return err}
	}
	_,err:=l.logFile.Seek(0,io.SeekEnd)
	return err
}

func (l *TransactionLog) scanWALFile(key walFileKey, active bool) error {
	path:=l.walPath(key)
	f,err:=os.Open(path)
	if err!=nil{return fmt.Errorf("open WAL segment %s: %w",path,err)}
	defer f.Close() //nolint:errcheck
	l.fileStats[key]=&walFileStats{}
	var offset int64
	var pending []struct {
		rec transactionWALRecord
		offset, bytes int64
	}
	var pendingStart int64
	for {
		var header [8]byte
		n,err:=io.ReadFull(f,header[:])
		if errors.Is(err,io.EOF)&&n==0{break}
		if errors.Is(err,io.EOF)||errors.Is(err,io.ErrUnexpectedEOF){
			if !active{return fmt.Errorf("truncated sealed WAL file %s at %d",path,offset)}
			break
		}
		if err!=nil{return fmt.Errorf("read WAL header in %s at %d: %w",path,offset,err)}
		length:=binary.BigEndian.Uint32(header[:4])
		if length==0 || length>transactionWALMaxFrame {
			return fmt.Errorf("invalid WAL record length %d in %s at %d",length,path,offset)
		}
		payload:=make([]byte,length)
		if _,err=io.ReadFull(f,payload);errors.Is(err,io.EOF)||errors.Is(err,io.ErrUnexpectedEOF){
			if !active{return fmt.Errorf("truncated sealed WAL record in %s at %d",path,offset)}
			break
		}else if err!=nil{return fmt.Errorf("read WAL record in %s: %w",path,err)}
		if crc32.Checksum(payload,transactionWALCRCTable)!=binary.BigEndian.Uint32(header[4:]) {
			return fmt.Errorf("transaction WAL checksum mismatch in %s at %d",path,offset)
		}
		rec,err:=decodeTxnWALRecord(payload)
		if err!=nil{return fmt.Errorf("decode WAL record in %s at %d: %w",path,offset,err)}
		frameSize := int64(len(header))+int64(length)
		if rec.Type == "topic-output" {
			if len(pending) == 0 { pendingStart = offset }
			if len(pending) >= maxTopicAcceptanceBatchRecords {
				return fmt.Errorf("topic WAL acceptance batch too large in %s at %d", path, offset)
			}
			// The CRC and codec have already been checked. Pending replay
			// needs only metadata and frame locations, never payload bytes.
			rec.TopicOutput.Data = nil
			pending = append(pending, struct {
				rec transactionWALRecord
				offset, bytes int64
			}{rec: rec, offset: offset, bytes: frameSize})
		} else {
			if len(pending)>0 {
				if rec.Type!="topic-accept" || rec.BatchCount!=len(pending) {
					return fmt.Errorf("missing or mismatched topic WAL acceptance boundary in %s at %d", path, offset)
				}
				for _, item := range pending {
					if err=l.apply(item.rec,item.offset,item.bytes,key);err!=nil {
						return fmt.Errorf("invalid topic WAL record in %s at %d: %w", path,item.offset,err)
					}
				}
				pending=nil
			} else if rec.Type=="topic-accept" {
				return fmt.Errorf("orphaned topic WAL acceptance boundary in %s at %d",path,offset)
			}
			if err=l.apply(rec,offset,frameSize,key);err!=nil {
				return fmt.Errorf("invalid WAL record in %s at %d: %w",path,offset,err)
			}
		}
		offset+=frameSize
	}
	info,err:=f.Stat()
	if err!=nil{return err}
	if len(pending)>0 {
		if !active { return fmt.Errorf("uncommitted batch in sealed WAL file %s at %d",path,pendingStart) }
		// A complete prefix of an unacknowledged multi-record batch is NOT
		// accepted data. Discard it atomically on active-tail recovery.
		offset=pendingStart
	}
	if info.Size()!=offset {
		if !active{return fmt.Errorf("sealed WAL %s has incomplete tail",path)}
		if err=l.logFile.Truncate(offset);err!=nil{return fmt.Errorf("truncate torn WAL tail: %w",err)}
		if err=l.logFile.Sync();err!=nil{return fmt.Errorf("sync WAL tail repair: %w",err)}
	}
	l.fileStats[key].size=offset
	l.size+=offset
	return nil
}

func (l *TransactionLog) apply(rec transactionWALRecord, frameOffset, frameBytes int64, source walFileKey) error {
	if rec.Version != transactionWALVersion {
		return fmt.Errorf("unsupported WAL record version %d", rec.Version)
	}
	switch rec.Type {
	case "binding":
		if rec.Binding == "" || (l.binding != "" && l.binding != rec.Binding) {
			return fmt.Errorf("conflicting WAL output binding")
		}
		l.markFrameDead(l.bindingRef)
		l.bindingRef = walFrameRef{key:source,size:frameBytes}
		l.binding = rec.Binding
	case "state":
		if rec.State == nil || rec.State.Version != durableTxnStateVersion ||
			rec.State.Counter <= 0 || len(rec.State.States) > maxTxnStates {
			return fmt.Errorf("invalid coordinator state checkpoint")
		}
		// Producer IDs can start a new transaction with a new instance.
		// References and markers from older instances are then obsolete
		// even if no explicit commit was recorded for the old generation.
		for id, old := range l.state.States {
			next, exists := rec.State.States[id]
			if !exists || (old.Instance != "" && old.Instance != next.Instance) {
				l.discardInstanceLocked(old.Instance)
			}
		}
		l.markFrameDead(l.stateRef)
		l.stateRef = walFrameRef{key:source,size:frameBytes}
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
			for i, topic := range st.Topics {
				if topic == "" || len(topic) > 249 ||
					(i > 0 && topic <= st.Topics[i-1]) {
					return fmt.Errorf("invalid or unsorted transaction participant in %q", id)
				}
			}
			// The coordinator state itself is a fully checksummed atomic
			// frame. It is already a durable COMMIT/ABORT decision if the
			// separate history marker was interrupted before it was written.
			if st.Pending != "" && l.topicInstances[st.Instance] {
				if old, exists := l.topicDecisions[st.Instance]; exists &&
					(old != st.Pending || l.topicDecisionOwners[st.Instance] != id) {
					return fmt.Errorf("conflicting durable decision for %s", st.Instance)
				}
				l.topicDecisions[st.Instance] = st.Pending
				l.topicDecisionOwners[st.Instance] = id
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
			walOutputLocation{key:source,offset: frameOffset, size: frameBytes, meta: metadata})
	case "topic-output":
		if rec.TopicOutput == nil || rec.TopicOutput.Topic == "" ||
			len(rec.TopicOutput.Topic) > 249 || rec.TopicOutput.Offset < 0 ||
			rec.TopicOutput.Offset != l.topicNext[rec.TopicOutput.Topic] ||
			((rec.TopicOutput.TransactionID == "") != (rec.TopicOutput.Instance == "")) ||
			(rec.TopicOutput.Instance != "" && !txnWALValidInstance(rec.TopicOutput.Instance)) {
			return fmt.Errorf("invalid or out-of-order logical topic WAL record")
		}
		metadata := *rec.TopicOutput
		metadata.Data = nil
		topic := metadata.Topic
		l.topicRecords[topic] = append(l.topicRecords[topic], walTopicLocation{
			key:source, offset:frameOffset, size:frameBytes, meta:metadata,
		})
		l.topicNext[topic]++
		if metadata.Instance != "" { l.topicInstances[metadata.Instance] = true }
	case "topic-accept":
		if rec.BatchCount <= 0 || rec.BatchCount > maxTopicAcceptanceBatchRecords {
			return fmt.Errorf("invalid topic WAL batch acceptance marker")
		}
		// If an accepted batch also updates producer deduplication state,
		// that state lives in the same acceptance frame. Replay either
		// applies BOTH payload and dedup checkpoint or neither.
		if rec.State != nil {
			if err := l.apply(transactionWALRecord{
				Version: transactionWALVersion, Type: "state", State: rec.State,
			}, frameOffset, frameBytes, source); err != nil {
				return fmt.Errorf("invalid accepted producer state: %w", err)
			}
		} else {
			// Delimiter with no producer state is obsolete after replay.
			l.markFrameDead(walFrameRef{key:source,size:frameBytes})
		}
	case "topic-terminal":
		terminal := rec.TopicTerminal
		if terminal == nil || terminal.TransactionID == "" ||
			!txnWALValidInstance(terminal.Instance) ||
			(terminal.Decision != "commit" && terminal.Decision != "abort") {
			return fmt.Errorf("invalid atomic topic terminal decision")
		}
		if existing, exists := l.topicDecisions[terminal.Instance]; exists {
			return fmt.Errorf("duplicate or conflicting terminal decision for %s: %s", terminal.Instance, existing)
		}
		current, exists := l.state.States[terminal.TransactionID]
		if !exists || current.Instance != terminal.Instance ||
			!current.Active || current.Pending != "" {
			return fmt.Errorf("terminal decision mismatches coordinator transaction")
		}
		if len(terminal.Markers) != len(current.Topics) {
			return fmt.Errorf("terminal participant count differs from enrolled topic set")
		}
		for i, marker := range terminal.Markers {
			if marker.Topic != current.Topics[i] || marker.Offset != l.topicNext[marker.Topic] ||
				marker.TransactionID != terminal.TransactionID ||
				marker.Instance != terminal.Instance || marker.Decision != terminal.Decision {
				return fmt.Errorf("invalid terminal marker position for %q", marker.Topic)
			}
			if marker.Offset == math.MaxInt64 {
				return fmt.Errorf("topic offset exhausted for %q", marker.Topic)
			}
		}
		l.topicDecisions[terminal.Instance] = terminal.Decision
		l.topicDecisionOwners[terminal.Instance] = terminal.TransactionID
		l.topicDecisionRefs[terminal.Instance] = walFrameRef{key:source,size:frameBytes}
		for _, marker := range terminal.Markers {
			l.topicMarkers[marker.Topic] = append(l.topicMarkers[marker.Topic], marker)
			l.topicNext[marker.Topic]++
		}
	case "topic-marker":
		marker := rec.TopicMarker
		if marker == nil || marker.Topic == "" ||
			len(marker.Topic) > 249 || marker.Offset != l.topicNext[marker.Topic] ||
			!txnWALValidInstance(marker.Instance) ||
			l.topicDecisions[marker.Instance] != marker.Decision ||
			l.topicDecisionOwners[marker.Instance] != marker.TransactionID {
			return fmt.Errorf("invalid checkpoint topic control marker")
		}
		if marker.Offset == math.MaxInt64 {
			return fmt.Errorf("topic offset exhausted for %q", marker.Topic)
		}
		l.topicMarkers[marker.Topic] = append(l.topicMarkers[marker.Topic], *marker)
		l.topicNext[marker.Topic]++
		// Checkpoint marker refs are kept until publication of that topic.
	case "topic-progress":
		return l.applyTopicProgress(rec.TopicProgress, frameBytes, source)
	case "topic-progress-checkpoint":
		return l.applyTopicProgressCheckpoint(rec.TopicProgress, frameBytes, source)
	case "topic-decision":
		if rec.TxnID == "" || !txnWALValidInstance(rec.Instance) ||
			(rec.Decision != "commit" && rec.Decision != "abort") {
			return fmt.Errorf("invalid durable topic transaction decision")
		}
		if previous, exists := l.topicDecisions[rec.Instance]; exists &&
			(previous != rec.Decision || l.topicDecisionOwners[rec.Instance] != rec.TxnID) {
			return fmt.Errorf("conflicting topic transaction decision for %s", rec.Instance)
		}
		l.markFrameDead(l.topicDecisionRefs[rec.Instance])
		l.topicDecisionRefs[rec.Instance] = walFrameRef{key: source, size: frameBytes}
		l.topicDecisions[rec.Instance] = rec.Decision
		l.topicDecisionOwners[rec.Instance] = rec.TxnID
	case "published":
		if rec.TxnID == "" || !txnWALValidInstance(rec.Instance) || l.aborted[rec.Instance] {
			return fmt.Errorf("invalid WAL publication marker")
		}
		l.published[rec.Instance] = true
		l.publishedHasOutput[rec.Instance] = rec.HasOutput
		l.discardOutputLocked(rec.Instance)
		l.markFrameDead(l.markerRefs[rec.Instance])
		l.markerRefs[rec.Instance] = walFrameRef{key:source,size:frameBytes}
	case "aborted":
		if rec.TxnID == "" || !txnWALValidInstance(rec.Instance) || l.published[rec.Instance] {
			return fmt.Errorf("invalid WAL abort marker")
		}
		l.aborted[rec.Instance] = true
		l.discardOutputLocked(rec.Instance)
		l.markFrameDead(l.markerRefs[rec.Instance])
		l.markerRefs[rec.Instance] = walFrameRef{key:source,size:frameBytes}
	default:
		return fmt.Errorf("unknown WAL record type %q", rec.Type)
	}
	return nil
}

// discardOutputLocked marks logged payload bytes as obsolete without having
// to scan the complete WAL. Only per-instance frame locations are visited.
func (l *TransactionLog) discardOutputLocked(instance string) {
	for _, ref := range l.data[instance] {
		l.markFrameDead(walFrameRef{key:ref.key,size:ref.size})
	}
	delete(l.data, instance)
}

func (l *TransactionLog) discardInstanceLocked(instance string) {
	if instance == "" {
		return
	}
	l.discardOutputLocked(instance)
	l.markFrameDead(l.markerRefs[instance])
	delete(l.markerRefs,instance)
	delete(l.published, instance)
	delete(l.publishedHasOutput, instance)
	delete(l.aborted, instance)
}

// shouldCompactWAL requires both a minimum absolute saving and a minimum
// fraction of dead space; a large, live transaction alone is never enough.
// The division form avoids multiplication overflow with very large logs.
func shouldCompactWAL(size, reclaimable int64) bool {
	return reclaimable >= transactionWALMinReclaim &&
		reclaimable >= size/transactionWALReclaimFraction
}

func (l *TransactionLog) maybeCompactLocked() error {
	if err:=l.deleteFullyDeadSegmentsLocked();err!=nil{return err}
	if !shouldCompactWAL(l.size,l.reclaimable){return nil}
	return l.compactLocked()
}

// appendBatchLocked writes all records before a single Sync, then applies
// their in-memory indexes. The acknowledgement barrier is the fsync, never
// the returned Write count. A failed append poisons the owner.
func (l *TransactionLog) appendBatchLocked(records []transactionWALRecord) error {
	if l.broken!=nil{return l.broken}
	if l.logFile==nil{return errors.New("transaction log is closed")}
	if len(records)==0{return nil}
	frames:=make([][]byte,0,len(records)+2)
	var total int64
	addFrame:=func(rec transactionWALRecord)error{
		buf,err:=txnWALFrame(rec)
		if err!=nil{return err}
		frames=append(frames,buf)
		total+=int64(len(buf))
		return nil
	}
	for _,rec:=range records{if err:=addFrame(rec);err!=nil{return err}}
	key:=l.activeKey()
	current:=l.fileStats[key]
	if current==nil{current=&walFileStats{};l.fileStats[key]=current}
	limit:=l.segmentLimit
	if limit<=0{limit=transactionWALSegmentBytes}
	if current.size>0 && current.size+total>limit {
		if err:=l.rotateSegment();err!=nil {
			l.broken=fmt.Errorf("rotate transaction WAL: %w",err)
			return l.broken
		}
		key=l.activeKey()
		// Repeat the current state and binding at each segment boundary so
		// earlier sealed segments become independently reclaimable once
		// their payloads and terminal markers are obsolete.
		var prefix [][]byte
		if l.binding!=""{
			b,err:=txnWALFrame(transactionWALRecord{Type:"binding",Binding:l.binding})
			if err!=nil{return err}
			prefix=append(prefix,b)
		}
		b,err:=txnWALFrame(transactionWALRecord{Type:"state",State:&l.state})
		if err!=nil{return err}
		prefix=append(prefix,b)
		frames=append(prefix,frames...)
		current=&walFileStats{}
		l.fileStats[key]=current
	}
	base:=current.size
	var written int64
	for _,buf:=range frames {
		n,err:=l.logFile.Write(buf)
		written+=int64(n)
		if err==nil && n!=len(buf){err=io.ErrShortWrite}
		if err!=nil {
			l.broken=fmt.Errorf("transaction WAL append permanently failed: %w",err)
			return l.broken
		}
	}
	if err:=l.logFile.Sync();err!=nil {
		l.broken=fmt.Errorf("transaction WAL fsync permanently failed: %w",err)
		return l.broken
	}
	if err:=l.failpoint("append:fsynced");err!=nil {
		l.broken=fmt.Errorf("transaction WAL injected post-fsync error: %w",err)
		return l.broken
	}
	index:=0
	if len(frames)>len(records) {
		if l.binding!="" {
			r:=transactionWALRecord{Version:transactionWALVersion,Type:"binding",Binding:l.binding}
			if err:=l.apply(r,base,int64(len(frames[index])),key);err!=nil{return err}
			base+=int64(len(frames[index]));index++
		}
		r:=transactionWALRecord{Version:transactionWALVersion,Type:"state",State:&l.state}
		if err:=l.apply(r,base,int64(len(frames[index])),key);err!=nil{return err}
		base+=int64(len(frames[index]));index++
	}
	for _,rec:=range records {
		rec.Version=transactionWALVersion
		if err:=l.apply(rec,base,int64(len(frames[index])),key);err!=nil{
			l.broken=fmt.Errorf("transaction WAL invariant failed: %w",err)
			return l.broken
		}
		base+=int64(len(frames[index]));index++
	}
	current.size+=written
	l.size+=written
	if err:=l.maybeCompactLocked();err!=nil{
		l.broken=fmt.Errorf("transaction WAL compaction failed: %w",err)
		return l.broken
	}
	return nil
}

func (l *TransactionLog) appendLocked(rec transactionWALRecord) error {
	return l.appendBatchLocked([]transactionWALRecord{rec})
}

func cloneTxnStateSnapshot(src txnStateSnapshot) txnStateSnapshot {
	out := txnStateSnapshot{
		Version: src.Version, Counter: src.Counter,
		States: make(map[string]storedTxnState, len(src.States)),
	}
	for key, state := range src.States {
		state.Sequences = copyTxnSequences(state.Sequences)
		state.Topics = append([]string(nil), state.Topics...)
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
	records := []transactionWALRecord{{Type: "state", State: &copied}}
	// A pending COMMIT/ABORT is the irreversible decision barrier. Store it
	// independently of the mutable transactional.id snapshot so that a
	// generation can be reused while its output still awaits publication.
	names := make([]string, 0, len(copied.States))
	for id := range copied.States { names = append(names, id) }
	sort.Strings(names)
	for _, id := range names {
		st := copied.States[id]
		if st.Instance == "" || st.Pending == "" || !l.topicInstances[st.Instance] { continue }
		if existing, exists := l.topicDecisions[st.Instance]; exists {
			if existing != st.Pending {
				return fmt.Errorf("cannot reverse durable transaction decision for %s", st.Instance)
			}
			continue
		}
		records = append(records, transactionWALRecord{
			Type: "topic-decision", TxnID: id, Instance: st.Instance, Decision: st.Pending,
		})
	}
	return l.appendBatchLocked(records)
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
// AppendOutputs persists a whole MessagesEvent with one fsync. The caller
// should pass exact formatted bytes and output paths. Frames are checked and
// indexed only after the entire batch has become durable. The caller may
// retry a failed batch only by aborting/reinitializing its transaction.
func (l *TransactionLog) AppendOutputs(records []WALOutputRecord) error {
	if len(records)==0{return nil}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken!=nil{return l.broken}
	if l.binding==""{return errors.New("WAL output writer is not bound")}
	changes:=make([]transactionWALRecord,0,len(records))
	for _,r:=range records {
		if r.TransactionID=="" || !txnWALValidInstance(r.Instance) || !validTxnWALPath(r.Path){
			return fmt.Errorf("invalid transaction WAL output destination")
		}
		st,exists:=l.state.States[r.TransactionID]
		if !exists || !st.Active || st.Pending!="" || st.Instance!=r.Instance {
			return fmt.Errorf("transaction %q is not an active WAL instance",r.TransactionID)
		}
		copied:=WALOutputRecord{TransactionID:r.TransactionID,Instance:r.Instance,Path:r.Path,Data:append([]byte(nil),r.Data...)}
		changes=append(changes,transactionWALRecord{Type:"output",Output:&copied})
	}
	return l.appendBatchLocked(changes)
}

// AppendOutput is the single-record specialization of AppendOutputs.
func (l *TransactionLog) AppendOutput(txnID,instance,relative string,data []byte) error {
	return l.AppendOutputs([]WALOutputRecord{{
		TransactionID:txnID,Instance:instance,Path:relative,Data:data,
	}})
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
	if err := l.compactLocked(); err != nil {
		// The checkpoint manifest may already be authoritative when a
		// filesystem error is returned. Never append using stale indexes.
		l.broken = fmt.Errorf("transaction WAL checkpoint outcome requires restart: %w", err)
		return l.broken
	}
	return nil
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
