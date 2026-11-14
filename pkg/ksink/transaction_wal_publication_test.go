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
