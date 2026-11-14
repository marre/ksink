package ksink

import (
 "path/filepath"
 "context"
 "net"
 "github.com/twmb/franz-go/pkg/kmsg"
 "github.com/twmb/franz-go/pkg/kerr"
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

func TestIdempotentProducerExpiryFailedWALAppendDoesNotReclaimDurableState(t *testing.T) {
 dir:=filepath.Join(t.TempDir(),"state")
 wal,err:=OpenTransactionLog(dir)
 require.NoError(t,err)
 state:=wal.CurrentState()
 state.Idempotent=map[int64]storedIdempotentProducer{
  1:{LastActivityUnixNano:1,Sequences:map[string]txnPartitionSequence{"orders/0":{Next:7}}},
 }
 require.NoError(t,wal.AppendState(state))
 candidate:=cloneTxnStateSnapshot(state)
 require.Equal(t,1,reclaimExpiredIdempotent(&candidate,2))
 require.NoError(t,wal.Close())
 require.Error(t,wal.AppendState(candidate),"failed WAL append must not count as a committed reclamation")
 recovered,err:=OpenTransactionLog(dir)
 require.NoError(t,err)
 defer func(){_ = recovered.Close()}()
 retained,ok:=recovered.CurrentState().Idempotent[1]
 require.True(t,ok)
 require.Equal(t,int32(7),retained.Sequences["orders/0"].Next)
}

func produceIdempotentRetentionCode(t *testing.T,conn net.Conn,id int32,pid int64) int16 {
 t.Helper()
 req:=kmsg.NewProduceRequest()
 req.Version=3
 req.Acks=1
 req.TimeoutMillis=1000
 topic:=kmsg.NewProduceRequestTopic()
 topic.Topic="orders"
 part:=kmsg.NewProduceRequestTopicPartition()
 part.Partition=0
 part.Records=recordBatchForSequence(pid,0,0,1,1)
 topic.Partitions=append(topic.Partitions,part)
 req.Topics=append(req.Topics,topic)
 reply:=sendRawRequest(t,conn,int16(kmsg.Produce),3,id,req.AppendTo(nil))
 resp:=&kmsg.ProduceResponse{Version:3}
 require.NoError(t,resp.ReadFrom(reply))
 require.Len(t,resp.Topics,1)
 require.Len(t,resp.Topics[0].Partitions,1)
 return resp.Topics[0].Partitions[0].ErrorCode
}

func TestIdempotentProducerExpiryEndToEndInitAndFencing(t *testing.T) {
 root:=t.TempDir()
 wal,err:=OpenTransactionLog(filepath.Join(root,"state"))
 require.NoError(t,err)
 require.NoError(t,wal.BindOutput(filepath.Join(root,"data","{topic}","messages-{seq:08}.jsonl")))
 state:=wal.CurrentState()
 oldID:=state.Counter
 state.Idempotent=make(map[int64]storedIdempotentProducer,maxTxnStates)
 expired:=time.Now().Add(-idempotentProducerRetention-time.Hour).UnixNano()
 for i:=0;i<maxTxnStates;i++ {
  state.Idempotent[oldID-int64(i)]=storedIdempotentProducer{Epoch:0,LastActivityUnixNano:expired}
 }
 require.NoError(t,wal.AppendState(state))
 srv,err:=New(Config{Address:"127.0.0.1:0",TransactionalWrite:true,IdempotentWrite:true},
  WithTransactionLog(wal),WithOrderedTransactionalOutput(func(m *Message)([]byte,error){return []byte(m.Value),nil}))
 require.NoError(t,err)
 require.NoError(t,srv.Start(context.Background()))
 conn:=newTxnStateTestConn(t,srv)
 defer func(){_ = conn.Close();_ = srv.Close(context.Background());_ = wal.Close()}()
 init:=sendInitProducerID(t,conn,1,nil,30000)
 require.Zero(t,init.ErrorCode)
 require.Greater(t,init.ProducerID,oldID)
 require.Len(t,wal.CurrentState().Idempotent,1)
 require.Equal(t,kerr.ProducerFenced.Code,produceIdempotentRetentionCode(t,conn,2,oldID))
 // A newly allocated producer must not be fenced by the reclamation.
 require.NotEqual(t,kerr.ProducerFenced.Code,produceIdempotentRetentionCode(t,conn,3,init.ProducerID))
}
