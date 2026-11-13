package ksink

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALBinaryPayloadAndBatchOneSync(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_ = wal.Close()}()
	syncs:=0
	wal.faultHook=func(stage string)error{
		if stage=="append:fsynced"{syncs++}
		return nil
	}
	payload:=[]byte{0,0,255,254,128,'x','\n'}
	records:=[]WALOutputRecord{
		{TransactionID:"orders-producer",Instance:instance,Path:"first.bin",Data:payload},
		{TransactionID:"orders-producer",Instance:instance,Path:"second.bin",Data:[]byte("other")},
	}
	require.NoError(t,wal.AppendOutputs(records))
	require.Equal(t,1,syncs,"the batch must cross just one fsync barrier")
	// Raw payload must appear in the physical WAL; no base64/JSON inflation.
	raw,err:=os.ReadFile(wal.walPath(wal.activeKey()))
	require.NoError(t,err)
	require.True(t,bytes.Contains(raw,payload))
	require.False(t,bytes.Contains(raw,[]byte("\"Data\":")))
	var outputs []WALOutputRecord
	require.NoError(t,wal.StreamOutput(instance,func(r WALOutputRecord)error{
		outputs=append(outputs,r);return nil
	}))
	require.Equal(t,records,outputs)
	require.NoError(t,wal.Close())
	restarted,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err)
	defer func(){_ = restarted.Close()}()
	outputs=nil
	require.NoError(t,restarted.StreamOutput(instance,func(r WALOutputRecord)error{
		outputs=append(outputs,r);return nil
	}))
	require.Equal(t,records,outputs)
}

func TestWALRotationAndWholeSegmentReclamation(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_ = wal.Close()}()
	wal.segmentLimit=512
	for i:=0;i<8;i++{
		require.NoError(t,wal.AppendOutput("orders-producer",instance,"small.bin",bytes.Repeat([]byte{byte(i)},128)))
	}
	require.Greater(t,len(wal.manifest.Segments),1,"new append segments should rotate")
	snap:=wal.CurrentState()
	st:=snap.States["orders-producer"];st.Pending="commit";snap.States["orders-producer"]=st
	require.NoError(t,wal.AppendState(snap))
	var payloads [][]byte
	require.NoError(t,wal.StreamOutput(instance,func(o WALOutputRecord)error{
		payloads=append(payloads,append([]byte(nil),o.Data...));return nil
	}))
	require.Len(t,payloads,8)
	require.NoError(t,wal.MarkPublished("orders-producer",instance))
	require.True(t,wal.Published(instance))
	for _,seg:=range wal.manifest.Segments {
		require.FileExists(t,wal.walPath(walFileKey{id:seg}))
	}
	require.NoError(t,wal.Close())
	next,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err)
	defer func(){_ = next.Close()}()
	require.True(t,next.Published(instance))
	require.Empty(t,next.RecoveryTransactions()[0].Records)
}

func TestWALRotationCrashBoundaries(t *testing.T) {
	for _,stage:=range []string{"rotate:segment-synced","rotate:manifest-synced"}{
		t.Run(stage,func(t *testing.T){
			root:=t.TempDir()
			wal,instance:=testOpenWALWithActiveTransaction(t,root)
			wal.segmentLimit=1
			wal.faultHook=func(point string)error{
				if point==stage{return errors.New("simulated power failure")}
				return nil
			}
			err:=wal.AppendOutput("orders-producer",instance,"not-acknowledged.bin",[]byte("uncommitted"))
			require.ErrorContains(t,err,"simulated power failure")
			require.NoError(t,wal.Close())
			next,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
			require.NoError(t,err)
			defer func(){_ = next.Close()}()
			events:=next.RecoveryTransactions()
			require.Len(t,events,1)
			require.Empty(t,events[0].Records,"failed rotation cannot acknowledge payloads")
			require.True(t,next.CurrentState().States["orders-producer"].Active)
		})
	}
}

func TestWALCheckpointCrashBoundaries(t *testing.T) {
	for _,stage:=range []string{"compact:snapshot-synced","compact:manifest-synced"}{
		t.Run(stage,func(t *testing.T){
			root:=t.TempDir()
			wal,instance:=testOpenWALWithActiveTransaction(t,root)
			want:=[]byte("durable message before crash")
			require.NoError(t,wal.AppendOutput("orders-producer",instance,"out.bin",want))
			wal.faultHook=func(point string)error{
				if point==stage{return errors.New("simulated power failure")}
				return nil
			}
			require.ErrorContains(t,wal.Compact(),"simulated power failure")
			require.NoError(t,wal.Close())
			next,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
			require.NoError(t,err)
			defer func(){_ = next.Close()}()
			var got [][]byte
			require.NoError(t,next.StreamOutput(instance,func(o WALOutputRecord)error{
				got=append(got,o.Data);return nil
			}))
			require.Equal(t,[][]byte{want},got,
				"checkpoint must preserve acknowledged payload from either manifest generation")
		})
	}
}

func TestWALMissingReferencedSealedSegmentFailsClosed(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	wal.segmentLimit=512
	require.NoError(t,wal.AppendOutput("orders-producer",instance,"older.bin",[]byte("hello")))
	require.NoError(t,wal.AppendOutput("orders-producer",instance,"newer.bin",[]byte("world")))
	require.GreaterOrEqual(t,len(wal.manifest.Segments),2)
	old:=wal.walPath(walFileKey{id:wal.manifest.Segments[0]})
	require.NoError(t,wal.Close())
	require.NoError(t,os.Remove(old))
	_,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.ErrorContains(t,err,"required WAL segment")
}
