package ksink

import (
	"fmt"
	"os"
	"path/filepath"
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
	if err=l.failpoint("compact:manifest-synced");err!=nil{return err}
	if err=old.Close();err!=nil{return err}
	if err=l.load();err!=nil{return fmt.Errorf("replay freshly checkpointed WAL: %w",err)}
	if err=l.cleanupOrphanWALFiles();err!=nil{return fmt.Errorf("remove replaced WAL files: %w",err)}
	return nil
}
