package ksink

import (
	"fmt"
	"os"
)

// readWALOutputAt verifies a checksummed indexed output record and its
// transaction identity before returning the exact formatted bytes.
func readWALOutputAt(file *os.File, ref walOutputLocation) (WALOutputRecord, error) {
	rec,n,err:=readTxnWALFrameAt(file,ref.offset)
	if err!=nil{return WALOutputRecord{},fmt.Errorf("read WAL output at %d: %w",ref.offset,err)}
	if int64(n)!=ref.size || rec.Type!="output" || rec.Output==nil {
		return WALOutputRecord{},fmt.Errorf("invalid WAL output index at %d",ref.offset)
	}
	o:=*rec.Output
	if o.TransactionID!=ref.meta.TransactionID || o.Instance!=ref.meta.Instance || o.Path!=ref.meta.Path {
		return WALOutputRecord{},fmt.Errorf("WAL output index disagrees with payload at %d",ref.offset)
	}
	return o,nil
}

// StreamOutput reads bounded frames in transaction order. The WAL mutex is
// held across the stream, including callbacks, to prevent compaction from
// changing indexed file sets while data is being materialized. The callback
// must never invoke this TransactionLog.
func (l *TransactionLog) StreamOutput(instance string, consume func(WALOutputRecord) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken!=nil{return l.broken}
	if l.logFile==nil{return fmt.Errorf("transaction log is closed")}
	readers:=make(map[walFileKey]*os.File)
	defer func(){for _,file:=range readers{_ = file.Close()}}()
	for _,ref:=range l.data[instance]{
		file:=readers[ref.key]
		if file==nil {
			var err error
			file,err=os.Open(l.walPath(ref.key))
			if err!=nil{return fmt.Errorf("open WAL output segment: %w",err)}
			readers[ref.key]=file
		}
		output,err:=readWALOutputAt(file,ref)
		if err!=nil{return err}
		if err=consume(output);err!=nil{return err}
	}
	return nil
}
