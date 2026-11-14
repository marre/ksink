package ksink

import (
 "path/filepath"
 "testing"
 "time"

 "github.com/stretchr/testify/require"
)

func TestIdempotentProducerExpiryBoundaryAndLegacyProtection(t *testing.T) {
 now:=time.Unix(2000000000,0)
 cutoff:=now.Add(-idempotentProducerRetention).UnixNano()
 snapshot:=txnStateSnapshot{Version:durableTxnStateVersion,Counter:10005,States:map[string]storedTxnState{},Idempotent:map[int64]storedIdempotentProducer{}}
 for id:=int64(1);id<=10005;id++ {
  snapshot.Idempotent[id]=storedIdempotentProducer{Epoch:0,LastActivityUnixNano:cutoff-1}
 }
 snapshot.Idempotent[1]=storedIdempotentProducer{LastActivityUnixNano:0}
 snapshot.Idempotent[2]=storedIdempotentProducer{LastActivityUnixNano:cutoff}
 snapshot.Idempotent[3]=storedIdempotentProducer{LastActivityUnixNano:now.UnixNano()}
 require.Equal(t,10002,reclaimExpiredIdempotent(&snapshot,cutoff))
 require.Len(t,snapshot.Idempotent,3)
 for _,id:=range []int64{1,2,3} {_,ok:=snapshot.Idempotent[id];require.True(t,ok)}
 require.Equal(t,0,reclaimExpiredIdempotent(&snapshot,cutoff))
}

func TestIdempotentProducerExpiryWALCheckpointSurvivesCompactionAndRestart(t *testing.T) {
 dir:=filepath.Join(t.TempDir(),"state")
 wal,err:=OpenTransactionLog(dir)
 require.NoError(t,err)
 snapshot:=wal.CurrentState()
 cutoff:=time.Now().Add(-idempotentProducerRetention).UnixNano()
 snapshot.Counter=10005
 snapshot.Idempotent=make(map[int64]storedIdempotentProducer)
 for id:=int64(1);id<=10005;id++ {
  snapshot.Idempotent[id]=storedIdempotentProducer{Epoch:0,LastActivityUnixNano:cutoff-1}
 }
 snapshot.Idempotent[1]=storedIdempotentProducer{Epoch:0,LastActivityUnixNano:cutoff+1,Sequences:map[string]txnPartitionSequence{"orders/0":{Next:2}}}
 require.NoError(t,wal.AppendState(snapshot))
 retained:=cloneTxnStateSnapshot(snapshot)
 require.Equal(t,10004,reclaimExpiredIdempotent(&retained,cutoff))
 require.NoError(t,wal.AppendState(retained))
 require.NoError(t,wal.Compact())
 require.NoError(t,wal.Close())
 recovered,err:=OpenTransactionLog(dir)
 require.NoError(t,err)
 defer func(){_ = recovered.Close()}()
 state:=recovered.CurrentState()
 require.Equal(t,int64(10005),state.Counter)
 require.Len(t,state.Idempotent,1)
 require.Equal(t,int32(2),state.Idempotent[1].Sequences["orders/0"].Next)
 require.Equal(t,cutoff+1,state.Idempotent[1].LastActivityUnixNano)
}
