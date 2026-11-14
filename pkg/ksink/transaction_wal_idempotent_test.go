package ksink

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharedWALNonTransactionalIdempotentSequenceAndBaseOffsetRecovery(t *testing.T) {
	root:=t.TempDir()
	wal,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	require.NoError(t,wal.BindOutput(filepath.Join(root,"data","{topic}","messages-{seq:08}.jsonl")))
	pid:=wal.CurrentState().Counter
	initial:=wal.CurrentState()
	initial.Idempotent=map[int64]storedIdempotentProducer{
		pid:{Epoch:0,Sequences:make(map[string]txnPartitionSequence)},
	}
	require.NoError(t,wal.AppendState(initial))

	raw:=recordBatchForSequence(pid,0,0,1,9)
	key:=txnSequenceKey("orders",0)
	proposed:=make(map[string]txnPartitionSequence)
	accepted,code:=acceptTxnBatches(raw,key,proposed)
	require.Zero(t,code)
	require.Equal(t,raw,accepted)
	offsets,err:=wal.appendTopicBatchFormattedWithStateBuilder(
		[]WALTopicRecord{{Topic:"orders"}},
		func(_ int,offset int64)([]byte,error){
			return []byte(fmt.Sprintf("orders/%d:first\n",offset)),nil
		},
		func(assigned []int64)(txnStateSnapshot,error){
			state:=cloneTxnStateSnapshot(initial)
			seq:=proposed[key]
			if err:=assignTxnAcceptedOffsets(raw,0,assigned,&seq);err!=nil{
				return txnStateSnapshot{},err
			}
			st:=state.Idempotent[pid]
			st.Sequences=map[string]txnPartitionSequence{key:seq}
			state.Idempotent[pid]=st
			return state,nil
		})
	require.NoError(t,err)
	require.Equal(t,[]int64{0},offsets)
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())

	recovered,err:=OpenTransactionLog(filepath.Join(root,"state"))
	require.NoError(t,err)
	defer func(){_=recovered.Close()}()
	state:=recovered.CurrentState()
	producer,ok:=state.Idempotent[pid]
	require.True(t,ok)
	require.Equal(t,int64(1),recovered.NextTopicOffset("orders"))
	history:=copyTxnSequences(producer.Sequences)
	retry,code:=acceptTxnBatches(raw,key,history)
	require.Zero(t,code)
	require.Empty(t,retry,"exact retries must not be appended again")
	first,ok:=txnBatchFirstOffset(raw,history[key])
	require.True(t,ok)
	require.Equal(t,int64(0),first)
	wrong:=recordBatchForSequence(pid,0,0,1,10)
	_,code=acceptTxnBatches(wrong,key,history)
	require.NotZero(t,code,"same producer sequence with different bytes is not a duplicate")
}
