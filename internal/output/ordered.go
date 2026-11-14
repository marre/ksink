package output

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marre/ksink/pkg/ksink"
)

// OrderedTopicMaterializer publishes each topic/partition 0 independently,
// in Kafka offset order, as immutable files containing only committed records.
// It never creates transaction-specific directories.
type OrderedTopicMaterializer struct {
	wal *ksink.TransactionLog
	pattern string
	root string
	width int
	size int64
	interval time.Duration
	now func() time.Time
	mu sync.Mutex // only one poll/flush may publish at once
	since map[string]time.Time
}

// KafkaTransactionStateDir is the default shared WAL root for ordered
// transactional file output. It is deliberately a sibling of topic
// directories, not a transaction staging directory.
func KafkaTransactionStateDir(pattern string) string {
	clean:=filepath.Clean(pattern)
	sep:=string(filepath.Separator)
	if i:=strings.Index(clean,sep+"{topic}"+sep);i>=0 {
		return filepath.Join(clean[:i],".ksink-kafka-state")
	}
	return filepath.Join(filepath.Dir(clean),".ksink-kafka-state")
}

// NewOrderedTopicMaterializer validates an absolute filesystem template.
// The topic placeholder must be a complete directory component. This v1
// grammar is deliberately narrow: {topic} and {seq:08}, nothing else.
func NewOrderedTopicMaterializer(wal *ksink.TransactionLog, pattern string,
	maxBytes int64, maxAge time.Duration) (*OrderedTopicMaterializer,error) {
	if wal==nil { return nil,errors.New("ordered output requires a shared WAL") }
	if maxBytes<0 || maxAge<0 || (maxBytes==0 && maxAge==0) {
		return nil,errors.New("ordered output needs a positive rotation size or interval")
	}
	if strings.Contains(pattern,"://") || strings.Contains(pattern,"{txnID}") ||
		strings.Contains(pattern,"{partition}") {
		return nil,errors.New("ordered output requires a filesystem path with {topic} and {seq:NN}")
	}
	abs,err:=filepath.Abs(pattern)
	if err!=nil{return nil,err}
	abs=filepath.Clean(abs)
	sep:=string(filepath.Separator)
	topicMarker:=sep+"{topic}"+sep
	if strings.Count(abs,topicMarker)!=1 || strings.Count(abs,"{seq:")!=1 {
		return nil,errors.New("ordered output pattern must contain one {topic} directory and one {seq:NN} filename")
	}
	prefix,after,found:=strings.Cut(abs,topicMarker)
	if !found || prefix=="" || after=="" || strings.Contains(after,sep) {
		return nil,errors.New("ordered output pattern must be ROOT/{topic}/FILE-{seq:NN}")
	}
	start:=strings.Index(after,"{seq:")
	stop:=strings.Index(after[start:],"}")
	if start<0 || stop<0 {return nil,errors.New("invalid ordered output sequence placeholder")}
	stop+=start
	spec:=after[start+5:stop]
	width,err:=strconv.Atoi(spec)
	if err!=nil || width<6 || width>20 {
		return nil,errors.New("ordered file sequence width must be between 6 and 20")
	}
	remainder:=after[:start]+after[stop+1:]
	if strings.ContainsAny(remainder,"{}") || strings.ContainsAny(prefix,"{}") ||
		remainder=="" || remainder=="." || remainder==".." {
		return nil,errors.New("unsupported filename placeholders or empty filename")
	}
	root:=prefix
	if err:=os.MkdirAll(root,0700);err!=nil{return nil,fmt.Errorf("create ordered output root: %w",err)}
	// Persist the root directory entry as well as future file entries.
	// Losing a newly-created output root after a durable receipt would break
	// the consumer's completed-file contract.
	if err:=syncOrderedDir(filepath.Dir(root));err!=nil{return nil,err}
	info,err:=os.Lstat(root)
	if err!=nil{return nil,err}
	if !info.IsDir() || info.Mode()&os.ModeSymlink!=0 {
		return nil,errors.New("ordered output root must be a real directory, not a symlink")
	}
	if err:=wal.BindOutput(abs);err!=nil{return nil,err}
	return &OrderedTopicMaterializer{
		wal:wal,pattern:abs,root:root,width:width,size:maxBytes,interval:maxAge,
		now:time.Now,since:make(map[string]time.Time),
	},nil
}

// topicName maps arbitrary topic bytes to a unique, safe directory component.
// Ordinary alphanumeric names stay readable; all other bytes use %HH escaping.
// Dots (including "." and "..") cannot become traversal components.
func orderedTopicName(topic string) string {
	var b strings.Builder
	const digits="0123456789ABCDEF"
	for _,c:=range []byte(topic) {
		if (c>='A'&&c<='Z') || (c>='a'&&c<='z') ||
			(c>='0'&&c<='9') || c=='-' || c=='_' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%');b.WriteByte(digits[c>>4]);b.WriteByte(digits[c&15])
		}
	}
	return b.String()
}

func (w *OrderedTopicMaterializer) filename(topic string, seq uint64) (string,string,error) {
	if topic=="" {return "","",errors.New("empty topic")}
	name:=strconv.FormatUint(seq,10)
	if len(name)>w.width || seq==0 {return "","",errors.New("ordered file sequence width exhausted")}
	digits:=fmt.Sprintf("%0*d",w.width,seq)
	abs:=strings.Replace(w.pattern,"{topic}",orderedTopicName(topic),1)
	abs=strings.Replace(abs,fmt.Sprintf("{seq:%02d}",w.width),digits,1)
	// Widths specified with one digit are allowed in parser but canonical
	// placeholders use exact text; construct it from the original pattern.
	if strings.Contains(abs,"{seq:") {
		i:=strings.Index(abs,"{seq:")
		j:=strings.Index(abs[i:],"}")
		if j<0{return "","",errors.New("invalid filename template")}
		abs=abs[:i]+digits+abs[i+j+1:]
	}
	rel,err:=filepath.Rel(w.root,abs)
	if err!=nil || rel=="." || rel==".." || filepath.IsAbs(rel) ||
		strings.HasPrefix(rel,".."+string(filepath.Separator)) {
		return "","",errors.New("ordered output escapes root")
	}
	return abs,rel,nil
}

func (w *OrderedTopicMaterializer) ensureParent(final string) error {
	dir:=filepath.Dir(final)
	if err:=os.MkdirAll(dir,0700);err!=nil{return err}
	// Also persist newly-created topic directories in their parent; syncing
	// only the topic directory itself is not sufficient after a power loss.
	if err:=syncOrderedDir(w.root);err!=nil{return err}
	// Prevent pre-created symlink topic directories redirecting publication.
	rel,err:=filepath.Rel(w.root,dir)
	if err!=nil{return err}
	cursor:=w.root
	for _,part:=range strings.Split(rel,string(filepath.Separator)) {
		if part=="."{continue}
		cursor=filepath.Join(cursor,part)
		info,err:=os.Lstat(cursor)
		if err!=nil{return err}
		if !info.IsDir() || info.Mode()&os.ModeSymlink!=0 {
			return fmt.Errorf("ordered output parent %q is not a real directory",cursor)
		}
	}
	return nil
}

// scan walks a stable topic prefix and writes only committed application
// payloads. It stops at a complete-record boundary after the byte limit.
// Skipped control/aborted offsets remain inside the selected offset range.
func (w *OrderedTopicMaterializer) scan(topic string,start,stable,limit int64,
	write func([]byte) error) (int64,int64,bool,error) {
	var length int64
	var visible bool
	end:=start
	for pos:=start;pos<stable;pos++ {
		rec,found,err:=w.wal.ReadTopicRecord(topic,pos)
		if err!=nil{return 0,0,false,err}
		if found {
			decision:=w.wal.TopicTransactionDecision(rec.Instance)
			if decision=="" {return 0,0,false,fmt.Errorf("unresolved transaction at %s/%d",topic,pos)}
			if decision=="commit" {
				if len(rec.Data)==0 {
					return 0,0,false,fmt.Errorf("ordered output does not support empty formatted record at %s/%d",topic,pos)
				}
				if write!=nil {
					if err:=write(rec.Data);err!=nil{return 0,0,false,err}
				}
				length+=int64(len(rec.Data))
				visible=true
			}
		}
		end=pos+1
		if visible && limit>0 && length>=limit {break}
	}
	return end,length,visible,nil
}

func (w *OrderedTopicMaterializer) currentLimit() int64 {
	if w.size>0 {return w.size}
	return math.MaxInt64
}

// Publish processes at most one immutable file per topic; force publishes a
// short file before its rotation deadline (e.g. on graceful shutdown).
// Calls are serialized to avoid duplicate selection of the next filename.
func (w *OrderedTopicMaterializer) Publish(ctx context.Context, force bool) (int,error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	published:=0
	for _,topic:=range w.wal.TopicNames() {
		if err:=ctx.Err();err!=nil{return published,err}
		count,err:=w.publishTopic(ctx,topic,force)
		if err!=nil{return published,fmt.Errorf("publish topic %q: %w",topic,err)}
		published+=count
	}
	return published,nil
}

func (w *OrderedTopicMaterializer) publishTopic(ctx context.Context,topic string,force bool) (int,error) {
	p:=w.wal.TopicProgress(topic)
	stable:=w.wal.TopicStableOffset(topic)
	if p.Cursor>=stable {
		delete(w.since,topic)
		return 0,nil
	}
	final,rel,err:=w.filename(topic,p.NextFile)
	if err!=nil{return 0,err}
	if err:=w.ensureParent(final);err!=nil{return 0,err}

	info,statErr:=os.Lstat(final)
	if statErr!=nil && !errors.Is(statErr,os.ErrNotExist){return 0,statErr}
	recoverExisting:=statErr==nil
	if recoverExisting && !info.Mode().IsRegular(){
		return 0,fmt.Errorf("unexpected non-regular finalized file %q",final)
	}

	// Before touching the filesystem, determine whether this prefix contains
	// any committed data. Only aborted/marker prefixes can be skipped.
	end,total,visible,err:=w.scan(topic,p.Cursor,stable,w.currentLimit(),nil)
	if err!=nil{return 0,err}
	if !visible {
		if err:=w.wal.SkipTopicOffsets(topic,end);err!=nil{return 0,err}
		delete(w.since,topic)
		return 1,nil
	}

	if !recoverExisting && !force && (w.size==0 || total<w.size) {
		since,ok:=w.since[topic]
		if !ok {w.since[topic]=w.now();return 0,nil}
		if w.interval>0 && w.now().Sub(since)<w.interval {return 0,nil}
	}

	// A file observed after a crash but before WAL receipt can be reconciled:
	// accept only bytes that exactly match the next committed WAL prefix.
	if recoverExisting {
		if info.Size()<=0 {return 0,fmt.Errorf("empty existing final file %q",final)}
		end,total,err=w.verifyExisting(ctx,topic,p.Cursor,stable,final,info.Size())
		if err!=nil{return 0,err}
		if end<=p.Cursor || total<=0{return 0,fmt.Errorf("cannot recover empty file %q",final)}
		// An earlier attempt may have linked a complete name before its
		// directory fsync failed. Verify durability before WAL receipt.
		if err:=syncOrderedDir(filepath.Dir(final));err!=nil{return 0,err}
	} else {
		// Files always contain a whole-number prefix of committed records.
		if err:=w.createFinal(ctx,topic,p.Cursor,end,final);err!=nil{return 0,err}
	}
	// Hash the finalized file as a stream; rotation sizes may be GiB, so
	// never load the entire published file into process memory.
	opened,err:=os.Open(final)
	if err!=nil{return 0,err}
	digest:=sha256.New()
	bytesOnDisk,hashErr:=io.Copy(digest,opened)
	if closeErr:=opened.Close();hashErr==nil{hashErr=closeErr}
	if hashErr!=nil{return 0,hashErr}
	receipt:=ksink.WALTopicFile{
		Sequence:p.NextFile,Start:p.Cursor,End:end,Path:rel,
		Size:bytesOnDisk,SHA256:hex.EncodeToString(digest.Sum(nil)),
	}
	if err:=w.wal.RecordTopicFile(topic,receipt);err!=nil{return 0,err}
	delete(w.since,topic)
	return 1,nil
}

func (w *OrderedTopicMaterializer) verifyExisting(ctx context.Context,topic string,
	start,stable int64,path string,expected int64) (int64,int64,error) {
	f,err:=os.Open(path)
	if err!=nil{return 0,0,err}
	defer f.Close() //nolint:errcheck
	var total int64
	for pos:=start;pos<stable;pos++ {
		if err:=ctx.Err();err!=nil{return 0,0,err}
		record,found,readErr:=w.wal.ReadTopicRecord(topic,pos)
		if readErr!=nil{return 0,0,readErr}
		if found && w.wal.TopicTransactionDecision(record.Instance)=="commit" {
			if len(record.Data)==0 || total+int64(len(record.Data))>expected {
				return 0,0,fmt.Errorf("existing finalized file %q differs from WAL",path)
			}
			got:=make([]byte,len(record.Data))
			if _,err:=io.ReadFull(f,got);err!=nil{return 0,0,err}
			if !bytes.Equal(got,record.Data){return 0,0,fmt.Errorf("existing finalized file %q has mismatched bytes",path)}
			total+=int64(len(record.Data))
		}
		if total==expected {return pos+1,total,nil}
	}
	return 0,0,fmt.Errorf("existing finalized file %q has unexpected length",path)
}

func (w *OrderedTopicMaterializer) createFinal(ctx context.Context,topic string,start,end int64,final string) error {
	dir:=filepath.Dir(final)
	f,err:=os.CreateTemp(dir,".ksink-ordered-*.tmp")
	if err!=nil{return err}
	temp:=f.Name()
	defer os.Remove(temp) //nolint:errcheck
	writeErr:=error(nil)
	for pos:=start;pos<end;pos++ {
		if writeErr=ctx.Err();writeErr!=nil{break}
		rec,found,err:=w.wal.ReadTopicRecord(topic,pos)
		if err!=nil{writeErr=err;break}
		if !found || w.wal.TopicTransactionDecision(rec.Instance)!="commit"{continue}
		if _,writeErr=f.Write(rec.Data);writeErr!=nil{break}
	}
	if writeErr==nil {writeErr=f.Sync()}
	if err:=f.Close();writeErr==nil{writeErr=err}
	if writeErr!=nil{return writeErr}

	// Hard-link publication is atomic and fails if the final filename exists.
	// Unlike os.Rename it can NEVER overwrite a previously finalized file.
	if err:=os.Link(temp,final);err!=nil{
		return fmt.Errorf("publish immutable file without replacement: %w",err)
	}
	if err:=syncOrderedDir(dir);err!=nil{return err}
	if err:=os.Remove(temp);err!=nil{return err}
	return syncOrderedDir(dir)
}

func syncOrderedDir(dir string) error {
	f,err:=os.Open(dir)
	if err!=nil{return err}
	defer f.Close() //nolint:errcheck
	return f.Sync()
}

// Run polls independent topic streams, including low-volume timeout
// rotation. Any error stops the worker and is returned to its caller.
func (w *OrderedTopicMaterializer) Run(ctx context.Context) error {
	tick:=200*time.Millisecond
	ticker:=time.NewTicker(tick)
	defer ticker.Stop()
	for {
		if _,err:=w.Publish(ctx,false);err!=nil{return err}
		select {
		case <-ctx.Done():return nil
		case <-ticker.C:
		}
	}
}

// Flush publishes every presently stable committed record, regardless of
// size/time thresholds. Unresolved transactions remain in the WAL.
func (w *OrderedTopicMaterializer) Flush(ctx context.Context) error {
	for {
		n,err:=w.Publish(ctx,true)
		if err!=nil{return err}
		if n==0 {return nil}
	}
}
