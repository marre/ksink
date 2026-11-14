package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/require"
)

func orderedTestLog(t *testing.T) *ksink.TransactionLog {
	t.Helper()
	wal,err:=ksink.OpenTransactionLog(filepath.Join(t.TempDir(),"wal"))
	require.NoError(t,err)
	t.Cleanup(func(){require.NoError(t,wal.Close())})
	return wal
}

func TestOrderedMaterializerTopicSequencesAndSizeRotation(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	pattern:=filepath.Join(root,"{topic}","messages-{seq:08}.jsonl")
	w,err:=NewOrderedTopicMaterializer(wal,pattern,4,time.Hour)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"orders",Data:[]byte("a\n")},
		{Topic:"payments",Data:[]byte("p\n")},
		{Topic:"orders",Data:[]byte("b\n")},
		{Topic:"orders",Data:[]byte("c\n")},
	})
	require.NoError(t,err)
	n,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,n,"orders reaches 4-byte size trigger; payments waits")
	first,err:=os.ReadFile(filepath.Join(root,"orders","messages-00000001.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"a\nb\n",string(first))
	require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)
	require.Equal(t,int64(0),wal.TopicProgress("payments").Cursor)
	require.NoError(t,w.Flush(context.Background()))
	for name,want:=range map[string]string{
		filepath.Join("orders","messages-00000002.jsonl"):"c\n",
		filepath.Join("payments","messages-00000001.jsonl"):"p\n",
	} {
		raw,readErr:=os.ReadFile(filepath.Join(root,name))
		require.NoError(t,readErr)
		require.Equal(t,want,string(raw))
	}
	require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
	require.Equal(t,uint64(2),wal.TopicProgress("payments").NextFile)
	n,err=w.Publish(context.Background(),true)
	require.NoError(t,err)
	require.Zero(t,n)
}

func TestOrderedMaterializerTimeRotationSparseInput(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","part-{seq:08}.jsonl"),
		128<<20,time.Minute)
	require.NoError(t,err)
	clock:=time.Date(2026,11,14,10,0,0,0,time.UTC)
	w.now=func()time.Time{return clock}
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("sparse\n")}})
	require.NoError(t,err)
	count,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Zero(t,count)
	clock=clock.Add(59*time.Second)
	count,err=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Zero(t,count)
	clock=clock.Add(time.Second)
	count,err=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,count)
	b,err:=os.ReadFile(filepath.Join(root,"orders","part-00000001.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"sparse\n",string(b))
}

func TestOrderedMaterializerReconcilesOrRejectsExistingFinalFile(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","messages-{seq:08}.jsonl"),
		2,time.Hour)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"orders",Data:[]byte("one\n")},
		{Topic:"orders",Data:[]byte("two\n")},
	})
	require.NoError(t,err)
	final:=filepath.Join(root,"orders","messages-00000001.jsonl")
	require.NoError(t,os.MkdirAll(filepath.Dir(final),0700))
	// Simulate crash after atomically publishing the completed file but
	// before recording its receipt in the WAL.
	require.NoError(t,os.WriteFile(final,[]byte("one\n"),0600))
	n,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,n)
	require.Equal(t,int64(1),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)
	require.NoError(t,w.Flush(context.Background()))
	finish,err:=os.ReadFile(filepath.Join(root,"orders","messages-00000002.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"two\n",string(finish))

	// A wrong existing final name is never overwritten.
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("three\n")}})
	require.NoError(t,err)
	bad:=filepath.Join(root,"orders","messages-00000003.jsonl")
	require.NoError(t,os.WriteFile(bad,[]byte("wrong\n"),0600))
	_,err=w.Publish(context.Background(),true)
	require.ErrorContains(t,err,"mismatched bytes")
	raw,readErr:=os.ReadFile(bad)
	require.NoError(t,readErr)
	require.Equal(t,"wrong\n",string(raw))
	require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
}

func TestOrderedMaterializerRejectsSymlinkAndInvalidTemplates(t *testing.T) {
	wal:=orderedTestLog(t)
	root:=t.TempDir()
	for _,pattern:=range []string{
		filepath.Join(root,"{txnID}","messages-{seq:08}.jsonl"),
		filepath.Join(root,"{topic}","messages.jsonl"),
		filepath.Join(root,"{seq:08}","{topic}","messages.jsonl"), // misplaced sequence must return an error, never panic
		filepath.Join(root,"{topic}","messages-{seq:03}.jsonl"),
		filepath.Join(root,"{topic}","messages-{seq:08}-{seq:08}.jsonl"),
		filepath.Join(root,"{topic}","x-{partition}-{seq:08}.jsonl"),
	} {
		_,err:=NewOrderedTopicMaterializer(wal,pattern,100,time.Second)
		require.Error(t,err,pattern)
	}
	topic:="a.b"
	safe:=orderedTopicName(topic)
	require.NotContains(t,safe,".")
	require.NotContains(t,safe,"/")
	require.NotEqual(t,orderedTopicName("a%b"),safe)
	require.NotEqual(t,orderedTopicName(".."),"..")
	require.Equal(t,"orders",orderedTopicName("orders"))
	require.Equal(t,"%4Frders",orderedTopicName("Orders"),
		"uppercase topic bytes must be escaped to avoid case-insensitive collisions")
	require.NotEqual(t,strings.ToLower(orderedTopicName("Orders")),
		strings.ToLower(orderedTopicName("orders")))
}

func TestOrderedMaterializerHashReceiptAndNoOverwrite(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","messages-{seq:08}.jsonl"),1,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("value\n")}})
	require.NoError(t,err)
	n,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,n)
	receipt:=wal.TopicProgress("orders").LastFile
	require.NotNil(t,receipt)
	hash:=sha256.Sum256([]byte("value\n"))
	require.Equal(t,hex.EncodeToString(hash[:]),receipt.SHA256)
	require.True(t,strings.HasPrefix(receipt.Path,"orders"))
	// Even if an operator deletes completed files, their numbers are never
	// reused; the receipt and WAL cursor remain authoritative.
	require.NoError(t,os.Remove(filepath.Join(root,receipt.Path)))
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("later\n")}})
	require.NoError(t,err)
	_,err=w.Publish(context.Background(),true)
	require.NoError(t,err)
	require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
	_,err=os.Stat(filepath.Join(root,"orders","messages-00000001.jsonl"))
	require.True(t,os.IsNotExist(err))
}

func TestOrderedMaterializerDistinctTopicNamesOnCaseInsensitiveFilesystems(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","messages-{seq:08}.jsonl"),128<<20,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"orders",Data:[]byte("lower\n")},
		{Topic:"Orders",Data:[]byte("upper\n")},
	})
	require.NoError(t,err)
	require.NoError(t,w.Flush(context.Background()))
	lower,err:=os.ReadFile(filepath.Join(root,"orders","messages-00000001.jsonl"))
	require.NoError(t,err)
	upper,err:=os.ReadFile(filepath.Join(root,"%4Frders","messages-00000001.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"lower\n",string(lower))
	require.Equal(t,"upper\n",string(upper))
	require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)
	require.Equal(t,uint64(2),wal.TopicProgress("Orders").NextFile)
}


func TestOrderedMaterializerFailedTopicStopsPublicationImmediately(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","messages-{seq:08}.jsonl"),1,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"a-failed",Data:[]byte("blocked\n")},
		{Topic:"z-healthy",Data:[]byte("good\n")},
	})
	require.NoError(t,err)
	// A conflicting immutable output name is a topic-local failure.
	failed:=filepath.Join(root,"a-failed","messages-00000001.jsonl")
	require.NoError(t,os.MkdirAll(filepath.Dir(failed),0700))
	require.NoError(t,os.WriteFile(failed,[]byte("wrong\n"),0600))
	n,err:=w.Publish(context.Background(),false)
	require.ErrorContains(t,err,"a-failed")
	require.Zero(t,n,"fatal output failure must stop publication immediately")
	_,readErr:=os.Stat(filepath.Join(root,"z-healthy","messages-00000001.jsonl"))
	require.ErrorIs(t,readErr,os.ErrNotExist)
	require.Equal(t,int64(0),wal.TopicProgress("a-failed").Cursor)
	require.Equal(t,int64(0),wal.TopicProgress("z-healthy").Cursor)
}

func TestOrderedMaterializerRunStopsOnOutputFailureAndRecoversAfterRestart(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,
		filepath.Join(root,"{topic}","messages-{seq:08}.jsonl"),1,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"a-blocked",Data:[]byte("recover\n")},
		{Topic:"z-healthy",Data:[]byte("healthy\n")},
	})
	require.NoError(t,err)
	blocked:=filepath.Join(root,"a-blocked","messages-00000001.jsonl")
	require.NoError(t,os.MkdirAll(filepath.Dir(blocked),0700))
	require.NoError(t,os.WriteFile(blocked,[]byte("conflict\n"),0600))
	ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second)
	defer cancel()
	err=w.Run(ctx)
	require.ErrorContains(t,err,"a-blocked")
	require.Equal(t,int64(0),wal.TopicProgress("a-blocked").Cursor)
	require.Equal(t,int64(0),wal.TopicProgress("z-healthy").Cursor,
		"fatal failure must stop the pass before other topics")
	require.NoError(t,os.Remove(blocked))
	require.NoError(t,w.Flush(context.Background()))
	raw,err:=os.ReadFile(blocked)
	require.NoError(t,err)
	require.Equal(t,"recover\n",string(raw))
	require.Equal(t,uint64(2),wal.TopicProgress("a-blocked").NextFile)
	require.Equal(t,int64(1),wal.TopicProgress("z-healthy").Cursor)
}

func TestOrderedMaterializerOversizedRecordAndRestartSequence(t *testing.T) {
	root:=t.TempDir()
	path:=filepath.Join(root,"wal")
	wal,err:=ksink.OpenTransactionLog(path)
	require.NoError(t,err)
	pattern:=filepath.Join(root,"{topic}","part-{seq:08}.jsonl")
	w,err:=NewOrderedTopicMaterializer(wal,pattern,3,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"orders",Data:[]byte("oversized\n")},
		{Topic:"orders",Data:[]byte("ok\n")},
	})
	require.NoError(t,err)
	n,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,n)
	first,err:=os.ReadFile(filepath.Join(root,"orders","part-00000001.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"oversized\n",string(first))
	require.NoError(t,wal.Close())
	wal,err=ksink.OpenTransactionLog(path)
	require.NoError(t,err)
	defer func(){_ = wal.Close()}()
	w,err=NewOrderedTopicMaterializer(wal,pattern,3,time.Minute)
	require.NoError(t,err)
	require.NoError(t,w.Flush(context.Background()))
	second,err:=os.ReadFile(filepath.Join(root,"orders","part-00000002.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"ok\n",string(second))
	require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
}

func TestOrderedMaterializerCrashAfterFinalizationBeforeReceiptReconcilesOnRestart(t *testing.T) {
	root:=t.TempDir()
	walPath:=filepath.Join(root,"wal")
	pattern:=filepath.Join(root,"{topic}","part-{seq:08}.jsonl")
	wal,err:=ksink.OpenTransactionLog(walPath)
	require.NoError(t,err)
	_,err=NewOrderedTopicMaterializer(wal,pattern,1,time.Minute)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
		{Topic:"orders",Data:[]byte("first\n")},
		{Topic:"orders",Data:[]byte("second\n")},
	})
	require.NoError(t,err)
	// Crash window: a durable final name exists, but its receipt was
	// never recorded. The restarted process must not overwrite the name.
	first:=filepath.Join(root,"orders","part-00000001.jsonl")
	require.NoError(t,os.MkdirAll(filepath.Dir(first),0700))
	require.NoError(t,os.WriteFile(first,[]byte("first\n"),0600))
	require.NoError(t,wal.Close())

	wal,err=ksink.OpenTransactionLog(walPath)
	require.NoError(t,err)
	defer func(){_ = wal.Close()}()
	w,err:=NewOrderedTopicMaterializer(wal,pattern,1,time.Minute)
	require.NoError(t,err)
	require.NoError(t,w.Flush(context.Background()))
	raw,err:=os.ReadFile(first)
	require.NoError(t,err)
	require.Equal(t,"first\n",string(raw))
	second,err:=os.ReadFile(filepath.Join(root,"orders","part-00000002.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"second\n",string(second))
	require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
}

func TestOrderedMaterializerSizeOnlyRotationWaitsForTarget(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,filepath.Join(root,"{topic}","part-{seq:08}.jsonl"),10,0)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("short")}})
	require.NoError(t,err)
	for i:=0;i<3;i++ {
		n,err:=w.Publish(context.Background(),false)
		require.NoError(t,err)
		require.Zero(t,n)
	}
	_,err=os.Stat(filepath.Join(root,"orders","part-00000001.jsonl"))
	require.ErrorIs(t,err,os.ErrNotExist)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("12345")}})
	require.NoError(t,err)
	n,err:=w.Publish(context.Background(),false)
	require.NoError(t,err)
	require.Equal(t,1,n)
	raw,err:=os.ReadFile(filepath.Join(root,"orders","part-00000001.jsonl"))
	require.NoError(t,err)
	require.Equal(t,"short12345",string(raw))
}

func TestOrderedMaterializerLongTopicNamesRemainPublishableAndDistinct(t *testing.T) {
	root:=t.TempDir()
	wal:=orderedTestLog(t)
	w,err:=NewOrderedTopicMaterializer(wal,filepath.Join(root,"{topic}","part-{seq:08}.jsonl"),1,0)
	require.NoError(t,err)
	first:=strings.Repeat("A",249)
	second:=strings.Repeat("A",248)+"B"
	require.NotEqual(t,orderedTopicName(first),orderedTopicName(second))
	require.LessOrEqual(t,len(orderedTopicName(first)),255)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:first,Data:[]byte("one")},{Topic:second,Data:[]byte("two")}})
	require.NoError(t,err)
	require.NoError(t,w.Flush(context.Background()))
	for topic,want:=range map[string]string{first:"one",second:"two"} {
		raw,err:=os.ReadFile(filepath.Join(root,orderedTopicName(topic),"part-00000001.jsonl"))
		require.NoError(t,err)
		require.Equal(t,want,string(raw))
	}
}
