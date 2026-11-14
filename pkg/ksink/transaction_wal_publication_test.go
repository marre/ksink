package ksink

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func walTestFile(seq uint64, start, end int64) WALTopicFile {
	digest := sha256.Sum256([]byte("immutable file"))
	return WALTopicFile{
		Sequence:seq,Start:start,End:end,
		Path:"orders/messages-00000001.jsonl",Size:14,
		SHA256:hex.EncodeToString(digest[:]),
	}
}

func TestSharedWALTopicPublicationSequencesSurviveCheckpoint(t *testing.T) {
	root:=t.TempDir()
	wal,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	require.NoError(t,wal.BindOutput(filepath.Join(root,"{topic}","messages-{seq:08}.jsonl")))
	require.Equal(t,uint64(1),wal.TopicProgress("orders").NextFile)
	for _,value:=range []string{"one","two"} {
		_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte(value)}})
		require.NoError(t,err)
	}
	require.NoError(t,wal.RecordTopicFile("orders",walTestFile(1,0,2)))
	require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)
	require.ErrorContains(t,wal.RecordTopicFile("orders",walTestFile(1,0,2)),"out of order")

	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("three")}})
	require.NoError(t,err)
	f:=walTestFile(2,2,3)
	f.Path="orders/messages-00000002.jsonl"
	require.NoError(t,wal.RecordTopicFile("orders",f))
	require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
	// The checkpoint retains only the latest file receipt and the absolute
	// cursor. Consumers are allowed to remove earlier processed files.
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())
	recovered,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	defer func(){_=recovered.Close()}()
	p:=recovered.TopicProgress("orders")
	require.Equal(t,int64(3),p.Cursor)
	require.Equal(t,uint64(3),p.NextFile)
	require.Equal(t,uint64(2),p.LastFile.Sequence)
	_,err=recovered.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("four")}})
	require.NoError(t,err)
	f=walTestFile(3,3,4)
	f.Path="orders/messages-00000003.jsonl"
	require.NoError(t,recovered.RecordTopicFile("orders",f))
	require.Equal(t,uint64(4),recovered.TopicProgress("orders").NextFile)
}

func TestSharedWALPublicationSkipsOnlyAbortedDataAndControlMarkers(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_=wal.Close()}()
	snapshot:=wal.CurrentState()
	st:=snapshot.States["orders-producer"]
	st.Topics=[]string{"orders"}
	snapshot.States["orders-producer"]=st
	require.NoError(t,wal.AppendState(snapshot))
	_,err:=wal.AppendTopicBatch([]WALTopicRecord{{
		Topic:"orders",TransactionID:"orders-producer",Instance:instance,
		Data:[]byte("aborted"),
	}})
	require.NoError(t,err)
	require.ErrorContains(t,wal.SkipTopicOffsets("orders",1),"invalid publication cursor")
	require.NoError(t,wal.AppendTopicTerminal("orders-producer",instance,"abort"))
	require.Equal(t,int64(2),wal.TopicStableOffset("orders"))
	require.NoError(t,wal.SkipTopicOffsets("orders",2))
	require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(1),wal.TopicProgress("orders").NextFile)

	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("committed")}})
	require.NoError(t,err)
	require.ErrorContains(t,wal.SkipTopicOffsets("orders",3),"lose committed records")
	file:=walTestFile(1,2,3)
	require.NoError(t,wal.RecordTopicFile("orders",file))
	require.Equal(t,int64(3),wal.TopicProgress("orders").Cursor)
	require.NoError(t,wal.Compact())
	require.Equal(t,int64(3),wal.TopicProgress("orders").Cursor)
	require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)
}

func TestSharedWALPublicationRejectsMalformedOrFutureReceipts(t *testing.T) {
	root:=t.TempDir()
	wal,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	defer func(){_=wal.Close()}()
	require.NoError(t,wal.BindOutput(filepath.Join(root,"{topic}","messages-{seq:08}.jsonl")))
	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("one")}})
	require.NoError(t,err)
	for _,f:=range []WALTopicFile{
		walTestFile(1,0,2),
		walTestFile(3,0,1),
		func() WALTopicFile {f:=walTestFile(1,0,1);f.Path="../escape";return f}(),
		func() WALTopicFile {f:=walTestFile(1,0,1);f.SHA256="bad";return f}(),
	} {
		require.Error(t,wal.RecordTopicFile("orders",f))
		require.Zero(t,wal.TopicProgress("orders").Cursor)
	}
	require.NoError(t,wal.RecordTopicFile("orders",walTestFile(1,0,1)))
	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"payments",Data:[]byte("independent")}})
	require.NoError(t,err)
	require.Equal(t,uint64(1),wal.TopicProgress("payments").NextFile)
	require.Zero(t,wal.TopicProgress("payments").Cursor)
}

func TestSharedWALGCReclaimsFinishedTopicWithoutResettingOffsets(t *testing.T) {
	root:=t.TempDir()
	wal,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	require.NoError(t,wal.BindOutput(filepath.Join(root,"{topic}","messages-{seq:08}.jsonl")))
	const count=600
	records:=make([]WALTopicRecord,count)
	for i:=range records {
		records[i]=WALTopicRecord{Topic:"orders",Data:make([]byte,2048)}
	}
	assigned,err:=wal.AppendTopicBatch(records)
	require.NoError(t,err)
	require.Equal(t,int64(count-1),assigned[count-1])
	file:=walTestFile(1,0,count)
	require.NoError(t,wal.RecordTopicFile("orders",file))
	require.Equal(t,0,len(wal.topicRecords["orders"]),"published payload indexes must be reclaimed")
	require.Equal(t,int64(count),wal.NextTopicOffset("orders"))
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())

	recovered,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	defer func(){_=recovered.Close()}()
	require.Equal(t,int64(count),recovered.NextTopicOffset("orders"))
	require.Equal(t,int64(count),recovered.TopicProgress("orders").Cursor)
	require.Empty(t,recovered.topicRecords["orders"])
	_,ok,err:=recovered.ReadTopicRecord("orders",0)
	require.NoError(t,err)
	require.False(t,ok,"GCed records are available only in their finalized files")
	offsets,err:=recovered.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("later")}})
	require.NoError(t,err)
	require.Equal(t,[]int64{count},offsets)
	rec,ok,err:=recovered.ReadTopicRecord("orders",count)
	require.NoError(t,err)
	require.True(t,ok)
	require.Equal(t,"later",string(rec.Data))
}

func TestSharedWALGCIndependentTopicProgressKeepsBlockedSuffix(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	state:=wal.CurrentState()
	st:=state.States["orders-producer"]
	st.Topics=[]string{"payments"}
	state.States["orders-producer"]=st
	require.NoError(t,wal.AppendState(state))
	_,err:=wal.AppendTopicBatch([]WALTopicRecord{
		{Topic:"orders",Data:[]byte("published")},
		{Topic:"payments",TransactionID:"orders-producer",Instance:instance,Data:[]byte("unresolved")},
	})
	require.NoError(t,err)
	require.NoError(t,wal.RecordTopicFile("orders",walTestFile(1,0,1)))
	require.Equal(t,int64(0),wal.TopicStableOffset("payments"))
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())
	reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err)
	defer func(){_=reopened.Close()}()
	require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
	require.Equal(t,int64(1),reopened.NextTopicOffset("payments"))
	require.Equal(t,int64(1),reopened.TopicProgress("orders").Cursor)
	require.Zero(t,reopened.TopicStableOffset("payments"))
	rec,ok,err:=reopened.ReadTopicRecord("payments",0)
	require.NoError(t,err)
	require.True(t,ok)
	require.Equal(t,"unresolved",string(rec.Data))
	_,ok,err=reopened.ReadTopicRecord("orders",0)
	require.NoError(t,err)
	require.False(t,ok)
}
