package ksink

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	walManifestVersion = 1
	transactionWALSegmentBytes = 64 << 20
)

type walManifest struct {
	Version int `json:"version"`
	Snapshot uint64 `json:"snapshot"`
	Segments []uint64 `json:"segments"`
	NextID uint64 `json:"next_id"`
}
type walFileKey struct {
	id uint64
	snapshot bool
}
type walFileStats struct { size, dead int64 }
type walFrameRef struct {
	key walFileKey
	size int64
}

func (l *TransactionLog) walPath(key walFileKey) string {
	d := "segments"
	if key.snapshot { d = "snapshots" }
	return filepath.Join(l.dir,d,fmt.Sprintf("%020d.wal",key.id))
}
func (l *TransactionLog) activeKey() walFileKey {
	return walFileKey{id:l.manifest.Segments[len(l.manifest.Segments)-1]}
}
func (l *TransactionLog) manifestPath() string {return filepath.Join(l.dir,"manifest.json")}

func validateWALManifest(m walManifest) error {
	if m.Version!=walManifestVersion || len(m.Segments)==0 || m.NextID==0 {
		return fmt.Errorf("invalid transaction WAL manifest")
	}
	last:=m.Snapshot
	for _,id:=range m.Segments {
		if id<=last || id>=m.NextID {return fmt.Errorf("invalid WAL segment ordering / generation")}
		last=id
	}
	if m.Snapshot>=m.NextID {return fmt.Errorf("invalid WAL checkpoint generation")}
	return nil
}

func (l *TransactionLog) persistManifest(m walManifest) error {
	if err:=validateWALManifest(m);err!=nil{return err}
	encoded,err:=json.Marshal(m)
	if err!=nil{return err}
	temp:=l.manifestPath()+".tmp"
	file,err:=os.OpenFile(temp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600)
	if err!=nil{return err}
	if _,err=file.Write(encoded);err==nil{err=file.Sync()}
	if closeErr:=file.Close();err==nil{err=closeErr}
	if err!=nil{return fmt.Errorf("write WAL manifest: %w",err)}
	if err=os.Rename(temp,l.manifestPath());err!=nil{return fmt.Errorf("replace WAL manifest: %w",err)}
	if err=txnSyncDirectory(l.dir);err!=nil{return fmt.Errorf("sync WAL manifest directory: %w",err)}
	return nil
}

// makeSegment writes a new, empty, fsynced segment before the manifest can
// reference it. An interrupted creation leaves an unreferenced orphan.
func (l *TransactionLog) makeSegment(id uint64)(*os.File,error){
	path:=l.walPath(walFileKey{id:id})
	file,err:=os.OpenFile(path,os.O_CREATE|os.O_EXCL|os.O_RDWR,0600)
	if err!=nil{return nil,err}
	if err=file.Sync();err!=nil{_ = file.Close();return nil,err}
	if err=txnSyncDirectory(filepath.Dir(path));err!=nil{_ = file.Close();return nil,err}
	return file,nil
}

func (l *TransactionLog) failpoint(stage string) error {
	if l.faultHook!=nil{return l.faultHook(stage)}
	return nil
}

func (l *TransactionLog) initSegmentStore() error {
	for _,p:=range []string{filepath.Join(l.dir,"segments"),filepath.Join(l.dir,"snapshots")} {
		if err:=createTxnStateDir(p);err!=nil{return err}
	}
	raw,err:=os.ReadFile(l.manifestPath())
	if errors.Is(err,fs.ErrNotExist){
		if _,legacyErr:=os.Lstat(filepath.Join(l.dir,"transactions.wal"));legacyErr==nil{
			return fmt.Errorf("old transaction WAL layout found; start with a new state directory")
		}else if legacyErr!=nil && !errors.Is(legacyErr,fs.ErrNotExist){return legacyErr}
		if _,bootErr:=os.Lstat(filepath.Join(l.dir,".ksink-wal-created"));bootErr==nil{
			return fmt.Errorf("established transaction WAL manifest is missing")
		}else if bootErr!=nil && !errors.Is(bootErr,fs.ErrNotExist){return bootErr}
		m:=walManifest{Version:walManifestVersion,Segments:[]uint64{1},NextID:2}
		file,err:=l.makeSegment(1)
		if err!=nil{return err}
		if err=l.persistManifest(m);err!=nil{_ = file.Close();return err}
		l.manifest=m
		l.logFile=file
		return nil
	}
	if err!=nil{return err}
	var m walManifest
	if err=json.Unmarshal(raw,&m);err!=nil{return fmt.Errorf("decode WAL manifest: %w",err)}
	if err=validateWALManifest(m);err!=nil{return err}
	for _,id:=range m.Segments{
		if _,err=os.Stat(l.walPath(walFileKey{id:id}));err!=nil{
			return fmt.Errorf("required WAL segment %d unavailable: %w",id,err)
		}
	}
	if m.Snapshot!=0 {
		if _,err=os.Stat(l.walPath(walFileKey{id:m.Snapshot,snapshot:true}));err!=nil{
			return fmt.Errorf("required WAL checkpoint %d unavailable: %w",m.Snapshot,err)
		}
	}
	file,err:=os.OpenFile(l.walPath(walFileKey{id:m.Segments[len(m.Segments)-1]}),os.O_RDWR,0600)
	if err!=nil{return err}
	l.manifest=m
	l.logFile=file
	return nil
}

// cleanupOrphanWALFiles runs only after the complete selected manifest has
// been replayed and verified. Unreferenced files are never authoritative.
func (l *TransactionLog) cleanupOrphanWALFiles() error {
	keep:=make(map[string]bool)
	for _,id:=range l.manifest.Segments {keep[l.walPath(walFileKey{id:id})]=true}
	if l.manifest.Snapshot!=0 {
		keep[l.walPath(walFileKey{id:l.manifest.Snapshot,snapshot:true})]=true
	}
	for _,dir:=range []string{filepath.Join(l.dir,"segments"),filepath.Join(l.dir,"snapshots")} {
		files,err:=os.ReadDir(dir)
		if err!=nil{return err}
		deleted:=false
		for _,entry:=range files {
			path:=filepath.Join(dir,entry.Name())
			if keep[path] || !entry.Type().IsRegular(){continue}
			// Only our own WAL files may be deleted. Never remove other
			// operator-managed files placed in the state directory.
			if filepath.Ext(path)!=".wal"{continue}
			if err=os.Remove(path);err!=nil{return err}
			deleted=true
		}
		if deleted {
			if err=txnSyncDirectory(dir);err!=nil{return err}
		}
	}
	return nil
}

// rotateSegment publishes the existence of a fresh segment before any
// records can be acknowledged in it. The previous file remains immutable.
func (l *TransactionLog) rotateSegment() error {
	nextID:=l.manifest.NextID
	f,err:=l.makeSegment(nextID)
	if err!=nil{return fmt.Errorf("create WAL segment: %w",err)}
	if err=l.failpoint("rotate:segment-synced");err!=nil{_ = f.Close();return err}
	m:=l.manifest
	m.Segments=append(append([]uint64(nil),m.Segments...),nextID)
	m.NextID++
	if err=l.persistManifest(m);err!=nil{_ = f.Close();return err}
	if err=l.failpoint("rotate:manifest-synced");err!=nil{_ = f.Close();return err}
	old:=l.logFile
	l.manifest=m
	l.logFile=f
	if err=old.Close();err!=nil{return err}
	return nil
}

func (l *TransactionLog) markFrameDead(ref walFrameRef) {
	if ref.size<=0 {return}
	l.reclaimable+=ref.size
	stat:=l.fileStats[ref.key]
	if stat==nil {stat=&walFileStats{};l.fileStats[ref.key]=stat}
	stat.dead+=ref.size
}

// deleteFullyDeadSegments safely drops any wholly obsolete *sealed* segment.
// An atomic manifest write precedes unlink, so interrupted deletion cannot
// lose the last surviving copy of an acknowledged record.
func (l *TransactionLog) deleteFullyDeadSegmentsLocked() error {
	if len(l.manifest.Segments)<=1 {return nil}
	m:=l.manifest
	m.Segments=make([]uint64,0,len(l.manifest.Segments))
	var removed []uint64
	for i,id:=range l.manifest.Segments {
		key:=walFileKey{id:id}
		st:=l.fileStats[key]
		if i<len(l.manifest.Segments)-1 && st!=nil && st.size>0 && st.dead==st.size {
			removed=append(removed,id)
		}else{m.Segments=append(m.Segments,id)}
	}
	if len(removed)==0{return nil}
	if err:=l.persistManifest(m);err!=nil{return err}
	// Crash-injection after switching physical authority, before deletion.
	l.manifest=m
	if err:=l.failpoint("gc:manifest-synced");err!=nil{return err}
	for _,id:=range removed {
		key:=walFileKey{id:id}
		if err:=os.Remove(l.walPath(key));err!=nil{return err}
		st:=l.fileStats[key]
		l.size-=st.size
		l.reclaimable-=st.dead
		delete(l.fileStats,key)
	}
	if err:=txnSyncDirectory(filepath.Join(l.dir,"segments"));err!=nil{return err}
	return nil
}

