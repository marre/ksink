package output

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/marre/ksink/pkg/ksink"
)

// stagedBatchRecord contains just enough information to materialize one
// WAL-durable formatted message into a disposable staging cache.
type stagedBatchRecord struct {
	data []byte
	file string
	txn *txnDirectory
}

// WriteBatch appends one MessagesEvent in a single WAL durability barrier.
// No Kafka Produce acknowledgement can precede that fsync. Staging files are
// a cache; write failures poison all participating transactions, preventing
// a partially staged event from being committed.
func (w *txnFileWriter) WriteBatch(data [][]byte, messages []*ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {return fmt.Errorf("transactional file writer is closed")}
	if len(data)!=len(messages){return fmt.Errorf("WAL output batch length mismatch")}
	if len(data)==0{return nil}

	records:=make([]ksink.WALOutputRecord,0,len(data))
	staged:=make([]stagedBatchRecord,0,len(data))
	touched:=make(map[*txnDirectory]bool)
	for i,msg:=range messages{
		if msg==nil || msg.TransactionalID=="" || msg.TransactionInstance=="" {
			return fmt.Errorf("transactional WAL requires a transaction ID and instance on every message")
		}
		txn:=w.txnFiles[msg.TransactionalID]
		if txn!=nil && (txn.committing || txn.poisoned || filepath.Base(txn.stagingDir)!=msg.TransactionInstance){
			return fmt.Errorf("transaction %q cannot accept more output",msg.TransactionalID)
		}
		resolved:=w.resolvePath(msg.TransactionalID,msg.Topic,msg.Partition)
		relative,err:=filepath.Rel(w.root,resolved)
		if err!=nil || relative==".." || filepath.IsAbs(relative) ||
			strings.HasPrefix(relative,".."+string(filepath.Separator)){
			return fmt.Errorf("transaction output path %q escapes root %q",resolved,w.root)
		}
		if txn==nil{
			txn,err=w.newTransaction(msg.TransactionalID,msg.TransactionInstance)
			if err!=nil{return err}
			w.txnFiles[msg.TransactionalID]=txn
		}
		touched[txn]=true
		records=append(records,ksink.WALOutputRecord{
			TransactionID:msg.TransactionalID,Instance:msg.TransactionInstance,
			Path:relative,Data:data[i],
		})
		staged=append(staged,stagedBatchRecord{
			data:data[i],file:filepath.Join(txn.stagingDir,relative),txn:txn,
		})
	}
	poison:=func(err error)error{
		for txn:=range touched{txn.poisoned=true}
		return err
	}
	if err:=w.wal.AppendOutputs(records);err!=nil{
		return poison(fmt.Errorf("persist transactional output batch: %w",err))
	}
	for _,item:=range staged {
		entry:=item.txn.files[item.file]
		if entry==nil {
			if err:=os.MkdirAll(filepath.Dir(item.file),0700);err!=nil{
				return poison(fmt.Errorf("create staged output directory: %w",err))
			}
			file,err:=os.OpenFile(item.file,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600)
			if err!=nil{return poison(fmt.Errorf("create staged output %s: %w",item.file,err))}
			entry=&txnFileEntry{file:file}
			item.txn.files[item.file]=entry
		}
		if entry.closed {return poison(fmt.Errorf("staged output %s is closed",item.file))}
		n,err:=entry.file.Write(item.data)
		if err==nil && n!=len(item.data){err=io.ErrShortWrite}
		if err!=nil{return poison(fmt.Errorf("append staged output %s: %w",item.file,err))}
	}
	return nil
}
