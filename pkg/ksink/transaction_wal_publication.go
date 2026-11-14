package ksink

import (
	"encoding/hex"
	"fmt"
	"math"
)

// WALTopicFile is one immutable, durably finalized file of committed records.
// This receipt is written only after fsync, same-filesystem atomic rename, and
// parent-directory sync. The consumer can later move/delete finalized files.
type WALTopicFile struct {
	Sequence uint64
	Start int64
	End int64
	Path string // relative to output root
	Size int64
	SHA256 string
}

// WALTopicProgress tracks one logical topic's next unpublished Kafka offset
// and next sortable output filename sequence. It never decreases or resets.
type WALTopicProgress struct {
	Topic string
	Cursor int64
	NextFile uint64
	LastFile *WALTopicFile
}

func validWALDigest(value string) bool {
	if len(value)!=64 { return false }
	_,err:=hex.DecodeString(value)
	return err==nil
}

func (l *TransactionLog) topicStableOffsetLocked(topic string) int64 {
	for _,ref:=range l.topicRecords[topic] {
		if ref.meta.Instance!="" && l.topicDecisions[ref.meta.Instance]=="" {
			return ref.meta.Offset
		}
	}
	return l.topicNext[topic]
}

// TopicProgress returns the durable publication cursor and next file number.
// The initial state for a topic is cursor 0 and file sequence 1.
func (l *TransactionLog) TopicProgress(topic string) WALTopicProgress {
	l.mu.Lock()
	defer l.mu.Unlock()
	p:=l.topicProgress[topic]
	if p.NextFile==0 {p.Topic=topic;p.NextFile=1}
	if p.LastFile!=nil {file:=*p.LastFile;p.LastFile=&file}
	return p
}

// RecordTopicFile advances publication only after its immutable file is fully
// durable. Reconciliation of already-existing final names is the future
// materializer's responsibility. Never blindly overwrite a final filename.
func (l *TransactionLog) RecordTopicFile(topic string, file WALTopicFile) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken!=nil{return l.broken}
	prev:=l.topicProgress[topic]
	if prev.NextFile==0 {prev.NextFile=1}
	if file.Start!=prev.Cursor || file.Sequence!=prev.NextFile ||
		file.End<=file.Start || file.Sequence==math.MaxUint64 {
		return fmt.Errorf("topic file publication is out of order")
	}
	p:=WALTopicProgress{Topic:topic,Cursor:file.End,NextFile:file.Sequence+1,LastFile:&file}
	return l.appendLocked(transactionWALRecord{Type:"topic-progress",TopicProgress:&p})
}

// SkipTopicOffsets durably advances over a stable range containing ONLY
// control markers or aborted payloads. It never creates an empty output file.
func (l *TransactionLog) SkipTopicOffsets(topic string, end int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken!=nil{return l.broken}
	prev:=l.topicProgress[topic]
	if prev.NextFile==0 {prev.NextFile=1}
	if end<=prev.Cursor {return fmt.Errorf("topic skip cursor must advance")}
	p:=WALTopicProgress{Topic:topic,Cursor:end,NextFile:prev.NextFile}
	return l.appendLocked(transactionWALRecord{Type:"topic-progress",TopicProgress:&p})
}

func (l *TransactionLog) applyTopicProgress(p *WALTopicProgress, size int64, source walFileKey) error {
	if p==nil || p.Topic=="" || len(p.Topic)>249 {
		return fmt.Errorf("invalid topic publication receipt")
	}
	prev:=l.topicProgress[p.Topic]
	if prev.NextFile==0 {prev.NextFile=1}
	if p.Cursor<=prev.Cursor || p.Cursor>l.topicStableOffsetLocked(p.Topic) {
		return fmt.Errorf("invalid publication cursor for topic %q",p.Topic)
	}
	if p.LastFile!=nil {
		file:=p.LastFile
		if file.Sequence!=prev.NextFile || file.Start!=prev.Cursor ||
			file.End!=p.Cursor || file.Size<=0 ||
			!validTxnWALPath(file.Path) || !validWALDigest(file.SHA256) ||
			prev.NextFile==math.MaxUint64 || p.NextFile!=prev.NextFile+1 {
			return fmt.Errorf("invalid immutable topic file receipt for %q",p.Topic)
		}
	} else if p.NextFile!=prev.NextFile {
		return fmt.Errorf("invalid skipped-range topic publication receipt for %q",p.Topic)
	}
	// No unresolved offsets are permitted in an advancing publication.
	// Skipping a committed record would silently lose application data.
	hasCommitted:=false
	for _,ref:=range l.topicRecords[p.Topic] {
		if ref.meta.Offset<prev.Cursor || ref.meta.Offset>=p.Cursor {continue}
		if ref.meta.Instance=="" || l.topicDecisions[ref.meta.Instance]=="commit" {
			hasCommitted=true
		} else if l.topicDecisions[ref.meta.Instance]!="abort" {
			return fmt.Errorf("unresolved transaction in topic publication range")
		}
	}
	if (p.LastFile!=nil)!=hasCommitted {
		return fmt.Errorf("publication would lose committed records or create an empty file")
	}
	l.markFrameDead(l.topicProgressRefs[p.Topic])
	l.topicProgressRefs[p.Topic]=walFrameRef{key:source,size:size}
	copied:=*p
	if p.LastFile!=nil {file:=*p.LastFile;copied.LastFile=&file}
	l.topicProgress[p.Topic]=copied
	return nil
}
