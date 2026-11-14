package ksink

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// compactLocked writes a self-contained snapshot in a NEW file, fsyncs it
// and a NEW empty append segment, and only then atomically publishes the
// manifest. Old segments are never removed before the manifest is durable.
// This makes every interrupted checkpoint replay as either the old or the
// new complete file set; there is no multi-file partially authoritative state.
func (l *TransactionLog) compactLocked() error {
	snapshotID:=l.manifest.NextID
	segmentID:=snapshotID+1
	snapshotKey:=walFileKey{id:snapshotID,snapshot:true}
	snapshotPath:=l.walPath(snapshotKey)
	f,err:=os.OpenFile(snapshotPath,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600)
	if err!=nil{return fmt.Errorf("create WAL checkpoint: %w",err)}
	failed:=func(err error)error{_ = f.Close();return err}
	write:=func(rec transactionWALRecord)error{
		_,err:=writeTxnWALFrame(f,rec)
		return err
	}
	if l.binding!=""{
		if err:=write(transactionWALRecord{Type:"binding",Binding:l.binding});err!=nil{return failed(err)}
	}
	if err:=write(transactionWALRecord{Type:"state",State:&l.state});err!=nil{return failed(err)}
	for _,txn:=range l.RecoveryTransactionsLocked(){
		switch {
		case txn.Published:
			if err:=write(transactionWALRecord{
				Type:"published",TxnID:txn.TransactionID,Instance:txn.Instance,
				HasOutput:txn.PublishedHasOutput,
			});err!=nil{return failed(err)}
		case txn.Aborted:
			if err:=write(transactionWALRecord{Type:"aborted",TxnID:txn.TransactionID,Instance:txn.Instance});err!=nil{return failed(err)}
		default:
			for _,ref:=range l.data[txn.Instance]{
				source,err:=os.Open(l.walPath(ref.key))
				if err!=nil{return failed(err)}
				record,readErr:=readWALOutputAt(source,ref)
				if closeErr:=source.Close();readErr==nil{readErr=closeErr}
				if readErr!=nil{return failed(readErr)}
				if err=write(transactionWALRecord{Type:"output",Output:&record});err!=nil{return failed(err)}
			}
		}
	}
	// A transaction may be committed but still blocked behind an earlier
	// unresolved offset. Its decision must survive producer identity reuse
	// and WAL compaction independently of the current producer state.
	decided := make([]string, 0, len(l.topicDecisions))
	for instance := range l.topicDecisions { decided = append(decided, instance) }
	sort.Strings(decided)
	for _, instance := range decided {
		if err := write(transactionWALRecord{
			Type: "topic-decision", TxnID: l.topicDecisionOwners[instance],
			Instance: instance, Decision: l.topicDecisions[instance],
		}); err != nil { return failed(err) }
	}
	// Rebuild each independent topic in ascending Kafka-log offset order.
	// Control markers share the same offset space but have no output payload.
	// The manifest's atomic switch makes multi-frame checkpoints all-or-none.
	topicsSet := make(map[string]bool)
	for topic := range l.topicNext { topicsSet[topic]=true }
	for topic := range l.topicRecords { topicsSet[topic]=true }
	for topic := range l.topicMarkers { topicsSet[topic]=true }
	topics := make([]string,0,len(topicsSet))
	for topic := range topicsSet {topics=append(topics,topic)}
	sort.Strings(topics)
	// Old published payload frames may be gone from this checkpoint. A
	// retained topic floor makes every later application/control offset
	// replayable without inventing or reusing any deleted offsets.
	for _, topic := range topics {
		floor:=l.topicProgress[topic].Cursor
		if err:=write(transactionWALRecord{Type:"topic-base",
			TopicBase:&walTopicBase{Topic:topic,Offset:floor},
		});err!=nil{return failed(err)}
	}
	pendingCount := 0
	flushPending := func() error {
		if pendingCount==0 {return nil}
		if err:=write(transactionWALRecord{Type:"topic-accept",BatchCount:pendingCount});err!=nil{return err}
		pendingCount=0
		return nil
	}
	for _, topic := range topics {
		refs, marks := l.topicRecords[topic], l.topicMarkers[topic]
		i,j:=0,0
		for i<len(refs) || j<len(marks) {
			if j<len(marks) && (i==len(refs) || marks[j].Offset < refs[i].meta.Offset) {
				if err:=flushPending();err!=nil{return failed(err)}
				marker:=marks[j]
				if err:=write(transactionWALRecord{Type:"topic-marker",TopicMarker:&marker});err!=nil{return failed(err)}
				j++
				continue
			}
			ref:=refs[i]
			source,openErr:=os.Open(l.walPath(ref.key))
			if openErr!=nil{return failed(openErr)}
			record,n,readErr:=readTxnWALFrameAt(source,ref.offset)
			if closeErr:=source.Close();readErr==nil{readErr=closeErr}
			if readErr!=nil{return failed(readErr)}
			if n!=ref.size || record.Type!="topic-output" || record.TopicOutput==nil ||
				record.TopicOutput.Topic!=topic || record.TopicOutput.Offset!=ref.meta.Offset ||
				record.TopicOutput.TransactionID!=ref.meta.TransactionID ||
				record.TopicOutput.Instance!=ref.meta.Instance {
				return failed(fmt.Errorf("invalid topic WAL frame at offset %d",ref.offset))
			}
			if err:=write(record);err!=nil{return failed(err)}
			pendingCount++
			if pendingCount==512 {
				if err:=flushPending();err!=nil{return failed(err)}
			}
			i++
		}
		if err:=flushPending();err!=nil{return failed(err)}
	}
	// Preserve per-topic durable publication cursors and file sequences. The
	// reader may have deleted historical finalized files; never recreate them.
	pubTopics:=make([]string,0,len(l.topicProgress))
	for topic:=range l.topicProgress { pubTopics=append(pubTopics,topic) }
	sort.Strings(pubTopics)
	for _,topic:=range pubTopics {
		state:=l.topicProgress[topic]
		if err:=write(transactionWALRecord{Type:"topic-progress-checkpoint",TopicProgress:&state});err!=nil {
			return failed(err)
		}
	}
	if err=f.Sync();err!=nil{return failed(fmt.Errorf("sync WAL checkpoint: %w",err))}
	if err=f.Close();err!=nil{return err}
	if err=txnSyncDirectory(filepath.Dir(snapshotPath));err!=nil{return err}
	if err=l.failpoint("compact:snapshot-synced");err!=nil{return err}

	nextSegment,err:=l.makeSegment(segmentID)
	if err!=nil{return err}
	m:=walManifest{Version:walManifestVersion,Snapshot:snapshotID,Segments:[]uint64{segmentID},NextID:segmentID+1}
	if err=l.persistManifest(m);err!=nil{_ = nextSegment.Close();return err}
	// From this point onward, the new generation is authoritative even if
	// the caller sees an error. The owner must stop or complete recovery.
	l.manifest=m
	old:=l.logFile
	l.logFile=nextSegment
	if err=old.Close();err!=nil{return err}
	if err=l.failpoint("compact:manifest-synced");err!=nil{return err}
	if err=l.load();err!=nil{return fmt.Errorf("replay freshly checkpointed WAL: %w",err)}
	if err=l.cleanupOrphanWALFiles();err!=nil{return fmt.Errorf("remove replaced WAL files: %w",err)}
	return nil
}
