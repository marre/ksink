package ksink

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedWALBatchAcceptsProducerSnapshotAtomically(t *testing.T) {
	dir := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, dir)
	snapshot := wal.CurrentState()
	state := snapshot.States["orders-producer"]
	state.Sequences = map[string]txnPartitionSequence{
		txnSequenceKey("orders", 0): {Next: 2},
	}
	snapshot.States["orders-producer"] = state
	offsets, err := wal.appendTopicBatchWithState([]WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("first")},
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance, Data: []byte("second")},
	}, snapshot)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, offsets)
	require.Equal(t, int32(2), wal.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	recovered, err := OpenTransactionLog(filepath.Join(dir, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = recovered.Close() }()
	require.Equal(t, int64(2), recovered.NextTopicOffset("orders"))
	require.Equal(t, int32(2), recovered.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	for i, value := range []string{"first", "second"} {
		rec, exists, err := recovered.ReadTopicRecord("orders", int64(i))
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, value, string(rec.Data))
	}
}

func TestSharedWALUnacceptedBatchDoesNotAdvanceProducerCheckpoint(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	initial := wal.CurrentState()
	snapshot := cloneTxnStateSnapshot(initial)
	st := snapshot.States["orders-producer"]
	st.Sequences = map[string]txnPartitionSequence{txnSequenceKey("orders", 0): {Next: 1}}
	snapshot.States["orders-producer"] = st

	// An accepted producer state is allowed only inside the topic acceptance
	// delimiter. Crash after a complete record frame but before that marker.
	frame, err := txnWALFrame(transactionWALRecord{
		Type: "topic-output", TopicOutput: &WALTopicRecord{
			Topic: "orders", Offset: 0, TransactionID: "orders-producer",
			Instance: instance, Data: []byte("not-accepted"),
		},
	})
	require.NoError(t, err)
	_, err = wal.logFile.Write(frame)
	require.NoError(t, err)
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())
	recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = recovered.Close() }()
	require.Zero(t, recovered.NextTopicOffset("orders"))
	require.Empty(t, recovered.CurrentState().States["orders-producer"].Sequences)
	offsets, err := recovered.appendTopicBatchWithState([]WALTopicRecord{{
		Topic: "orders", TransactionID: "orders-producer",
		Instance: instance, Data: []byte("accepted"),
	}}, snapshot)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, offsets)
	require.Equal(t, int32(1), recovered.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
}

func TestSharedWALRecoversCommitDecisionWhenSeparateMarkerMissing(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	_, err := wal.AppendTopicBatch([]WALTopicRecord{{
		Topic: "orders", TransactionID: "orders-producer",
		Instance: instance, Data: []byte("payload"),
	}})
	require.NoError(t, err)
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	// Simulate a crash after the complete persisted coordinator state but
	// before a separate topic-decision history marker is written.
	frame, err := txnWALFrame(transactionWALRecord{Type: "state", State: &state})
	require.NoError(t, err)
	_, err = wal.logFile.Write(frame)
	require.NoError(t, err)
	require.NoError(t, wal.logFile.Sync())
	require.NoError(t, wal.Close())

	recovered, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.Equal(t, "commit", recovered.TopicTransactionDecision(instance))
	require.Equal(t, int64(1), recovered.TopicStableOffset("orders"))
	require.NoError(t, recovered.Compact())
	require.NoError(t, recovered.Close())
	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	require.Equal(t, "commit", next.TopicTransactionDecision(instance))
	require.Equal(t, int64(1), next.TopicStableOffset("orders"))
}

func TestSharedWALCheckpointReplayChunksLargeTopicStream(t *testing.T) {
	root := t.TempDir()
	wal, _ := testOpenWALWithActiveTransaction(t, root)
	records := make([]WALTopicRecord, 1100)
	for i := range records {
		records[i] = WALTopicRecord{
			Topic: "orders", Data: []byte(fmt.Sprintf("message-%04d", i)),
		}
	}
	offsets, err := wal.AppendTopicBatch(records)
	require.NoError(t, err)
	require.Len(t, offsets, len(records))
	require.Equal(t, int64(1099), offsets[1099])
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())
	loaded, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = loaded.Close() }()
	require.Equal(t, int64(1100), loaded.NextTopicOffset("orders"))
	for _, index := range []int64{0, 511, 512, 1023, 1024, 1099} {
		record, ok, err := loaded.ReadTopicRecord("orders", index)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("message-%04d", index), string(record.Data))
	}
}

func TestSharedWALFormatsAfterOffsetAssignmentWithoutConsumingFailedOffsets(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Sequences = map[string]txnPartitionSequence{
		txnSequenceKey("orders", 0): {Next: 2},
	}
	state.States["orders-producer"] = st
	records := []WALTopicRecord{
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance},
		{Topic: "orders", TransactionID: "orders-producer", Instance: instance},
	}
	_, err := wal.appendTopicBatchFormattedWithState(records, state, func(i int, offset int64) ([]byte, error) {
		if i == 1 { return nil, fmt.Errorf("format failed") }
		return []byte(fmt.Sprintf("%d", offset)), nil
	})
	require.ErrorContains(t, err, "format failed")
	require.Zero(t, wal.NextTopicOffset("orders"))
	require.Empty(t, wal.CurrentState().States["orders-producer"].Sequences,
		"format failure cannot checkpoint producer sequence state")

	offsets, err := wal.appendTopicBatchFormattedWithState(records, state,
		func(i int, offset int64) ([]byte, error) {
			return []byte(fmt.Sprintf("record-%d-offset-%d", i, offset)), nil
		})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, offsets)
	require.Equal(t, int32(2), wal.CurrentState().States["orders-producer"].Sequences[txnSequenceKey("orders", 0)].Next)
	for i := range records {
		got, exists, readErr := wal.ReadTopicRecord("orders", int64(i))
		require.NoError(t, readErr)
		require.True(t, exists)
		require.Equal(t, fmt.Sprintf("record-%d-offset-%d", i, i), string(got.Data))
	}
}

func TestSharedWALAtomicControlMarkersAndTopicOffsets(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	st:=wal.CurrentState()
	prod:=st.States["orders-producer"]
	prod.Topics=[]string{"orders","payments"}
	st.States["orders-producer"]=prod
	require.NoError(t,wal.AppendState(st))

	offsets,err:=wal.AppendTopicBatch([]WALTopicRecord{
		{Topic:"orders",TransactionID:"orders-producer",Instance:instance,Data:[]byte("order")},
		{Topic:"payments",TransactionID:"orders-producer",Instance:instance,Data:[]byte("payment")},
	})
	require.NoError(t,err)
	require.Equal(t,[]int64{0,0},offsets)
	require.NoError(t,wal.AppendTopicTerminal("orders-producer",instance,"commit"))
	require.Equal(t,"commit",wal.TopicTransactionDecision(instance))
	require.Equal(t,int64(2),wal.NextTopicOffset("orders"))
	require.Equal(t,int64(2),wal.NextTopicOffset("payments"))
	for _,topic:=range []string{"orders","payments"} {
		record,ok,readErr:=wal.ReadTopicRecord(topic,1)
		require.NoError(t,readErr)
		require.False(t,ok,"control markers must never appear as user data: %+v",record)
	}
	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("after commit")}})
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"payments",Data:[]byte("after commit")}})
	require.NoError(t,err)
	require.Equal(t,int64(3),wal.NextTopicOffset("orders"))
	require.Equal(t,int64(3),wal.NextTopicOffset("payments"))
	require.ErrorContains(t,wal.AppendTopicTerminal("orders-producer",instance,"commit"),"already has a durable outcome")
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())

	next,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err)
	defer func(){_=next.Close()}()
	require.Equal(t,"commit",next.TopicTransactionDecision(instance))
	for _,topic:=range []string{"orders","payments"} {
		require.Equal(t,int64(3),next.NextTopicOffset(topic))
		marker,isRecord,err:=next.ReadTopicRecord(topic,1)
		require.NoError(t,err)
		require.False(t,isRecord,"control marker must remain a gap: %+v",marker)
		data,isRecord,err:=next.ReadTopicRecord(topic,2)
		require.NoError(t,err)
		require.True(t,isRecord)
		require.Equal(t,"after commit",string(data.Data))
	}
}

func TestSharedWALOneTopicBlockedWhileAnotherCommitsWithControlOffsets(t *testing.T) {
	root:=t.TempDir()
	wal,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err)
	defer func(){_=wal.Close()}()
	require.NoError(t,wal.BindOutput(filepath.Join(root,"data","{topic}","messages-{seq:08}.jsonl")))
	a,err:=newTxnInstance("A")
	require.NoError(t,err)
	b,err:=newTxnInstance("B")
	require.NoError(t,err)
	snapshot:=wal.CurrentState()
	snapshot.States["A"]=storedTxnState{ProducerID:snapshot.Counter,Active:true,Instance:a,Topics:[]string{"orders"}}
	snapshot.States["B"]=storedTxnState{ProducerID:snapshot.Counter,Active:true,Instance:b,Topics:[]string{"orders","payments"}}
	require.NoError(t,wal.AppendState(snapshot))
	for _,r:=range []WALTopicRecord{
		{Topic:"orders",TransactionID:"A",Instance:a,Data:[]byte("A0")},
		{Topic:"orders",TransactionID:"B",Instance:b,Data:[]byte("B1")},
		{Topic:"payments",TransactionID:"B",Instance:b,Data:[]byte("B-payment")},
	} {
		_,err=wal.AppendTopicBatch([]WALTopicRecord{r})
		require.NoError(t,err)
	}
	require.NoError(t,wal.AppendTopicTerminal("B",b,"commit"))
	require.Equal(t,int64(0),wal.TopicStableOffset("orders"))
	require.Equal(t,int64(2),wal.TopicStableOffset("payments"))
	require.NoError(t,wal.AppendTopicTerminal("A",a,"abort"))
	require.Equal(t,int64(4),wal.TopicStableOffset("orders"))
	// orders: A0, B1, B COMMIT at 2, A ABORT at 3.
	for _,pos:=range []int64{2,3} {
		_,found,err:=wal.ReadTopicRecord("orders",pos)
		require.NoError(t,err)
		require.False(t,found)
	}
	require.NoError(t,wal.Compact())
	require.Equal(t,int64(4),wal.TopicStableOffset("orders"))
	require.Equal(t,int64(2),wal.TopicStableOffset("payments"))
}

func TestSharedWALRejectsEmptyFormattedRecordsBeforeAcknowledgement(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_=wal.Close()}()
	before:=wal.CurrentState()
	state:=cloneTxnStateSnapshot(before)
	st:=state.States["orders-producer"]
	st.Sequences=map[string]txnPartitionSequence{txnSequenceKey("orders",0):{Next:1}}
	state.States["orders-producer"]=st
	_,err:=wal.appendTopicBatchFormattedWithState([]WALTopicRecord{{
		Topic:"orders",TransactionID:"orders-producer",Instance:instance,
	}},state,func(int,int64)([]byte,error){return nil,nil})
	require.ErrorContains(t,err,"no formatted bytes")
	require.Zero(t,wal.NextTopicOffset("orders"))
	require.Empty(t,wal.CurrentState().States["orders-producer"].Sequences)
	offsets,err:=wal.appendTopicBatchFormattedWithState([]WALTopicRecord{{
		Topic:"orders",TransactionID:"orders-producer",Instance:instance,
	}},state,func(int,int64)([]byte,error){return []byte("nonempty"),nil})
	require.NoError(t,err)
	require.Equal(t,[]int64{0},offsets)
}
