package ksink

import (
    "encoding/binary"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "os"
    "path/filepath"
    "syscall"
    "testing"

    "github.com/stretchr/testify/require"
)

// The writer must fail-stop on every write/fsync failure. A complete
// unacknowledged acceptance group may survive a simulated pre-sync failure;
// the independent safety requirement is that acknowledged earlier records
// survive and the poisoned owner never acknowledges anything afterwards.
func TestWALFaultDiskAppendFaultMatrix(t *testing.T) {
    cases:=[]struct{stage string;cause error}{
        {"append:write",syscall.EIO},
        {"append:write",syscall.ENOSPC},
        {"append:write",syscall.EDQUOT},
        {"append:fsync",syscall.EIO},
        {"append:fsync",syscall.ENOSPC},
        {"append:fsynced",errors.New("response lost after fsync")},
    }
    for _,tc:=range cases {
        t.Run(tc.stage+"/"+tc.cause.Error(),func(t *testing.T){
            root:=t.TempDir()
            wal,_:=testOpenWALWithActiveTransaction(t,root)
            offsets,err:=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("acknowledged")}})
            require.NoError(t,err)
            require.Equal(t,[]int64{0},offsets)
            wal.faultHook=func(stage string)error{
                if stage==tc.stage{return tc.cause}
                return nil
            }
            _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("ambiguous")}})
            require.ErrorIs(t,err,tc.cause)
            _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("must fail")}})
            require.Error(t,err,"an I/O-ambiguous WAL owner must not acknowledge again")
            require.NoError(t,wal.Close())

            reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
            require.NoError(t,err)
            defer func(){require.NoError(t,reopened.Close())}()
            first,found,err:=reopened.ReadTopicRecord("orders",0)
            require.NoError(t,err)
            require.True(t,found)
            require.Equal(t,"acknowledged",string(first.Data))
            next:=reopened.NextTopicOffset("orders")
            require.True(t,next==1||next==2,"only the acknowledged prefix or one ambiguous batch may survive: next=%d",next)
            if next==2 {
                second,ok,err:=reopened.ReadTopicRecord("orders",1)
                require.NoError(t,err)
                require.True(t,ok)
                require.Equal(t,"ambiguous",string(second.Data))
            }
            _,ok,err:=reopened.ReadTopicRecord("orders",2)
            require.NoError(t,err)
            require.False(t,ok)
        })
    }
}

func TestWALFaultShortAppendWriteRepairsUnacceptedTail(t *testing.T) {
    root:=t.TempDir()
    wal,_:=testOpenWALWithActiveTransaction(t,root)
    _,err:=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("acknowledged")}})
    require.NoError(t,err)
    wal.frameWrite=func(f *os.File,p []byte)(int,error){
        // Model a short successful write of an incomplete WAL frame.
        return f.Write(p[:len(p)/2])
    }
    _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("never accepted")}})
    require.ErrorIs(t,err,io.ErrShortWrite)
    require.Error(t,wal.AppendState(wal.CurrentState()))
    require.NoError(t,wal.Close())

    reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
    require.NoError(t,err)
    defer func(){require.NoError(t,reopened.Close())}()
    require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
    rec,found,err:=reopened.ReadTopicRecord("orders",0)
    require.NoError(t,err)
    require.True(t,found)
    require.Equal(t,"acknowledged",string(rec.Data))
}

func TestWALFaultManifestAndCheckpointFailureMatrix(t *testing.T) {
    stages:=[]string{
        "compact:create","compact:write","compact:sync","compact:dir-sync",
        "compact:snapshot-synced",
        "segment:create","segment:sync","segment:dir-sync",
        "manifest:create","manifest:write","manifest:sync",
        "manifest:rename","manifest:dir-sync",
        "compact:manifest-synced",
    }
    for _,stage:=range stages{
        t.Run(stage,func(t *testing.T){
            root:=t.TempDir()
            wal,_:=testOpenWALWithActiveTransaction(t,root)
            _,err:=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("durable")}})
            require.NoError(t,err)
            wal.faultHook=func(point string)error{
                if point==stage {return syscall.EIO}
                return nil
            }
            err=wal.Compact()
            require.ErrorIs(t,err,syscall.EIO)
            // If the failure occurred after manifest replacement, the new
            // snapshot is authoritative; otherwise the old segment is.
            require.NoError(t,wal.Close())
            reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
            require.NoError(t,err,"checkpoint stage %s",stage)
            defer func(){require.NoError(t,reopened.Close())}()
            require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
            rec,found,err:=reopened.ReadTopicRecord("orders",0)
            require.NoError(t,err)
            require.True(t,found)
            require.Equal(t,"durable",string(rec.Data))
            require.NoError(t,reopened.Compact())
        })
    }
}

func TestWALFaultManifestShortWriteFailsClosed(t *testing.T) {
    root:=t.TempDir()
    wal,_:=testOpenWALWithActiveTransaction(t,root)
    _,err:=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("durable")}})
    require.NoError(t,err)
    wal.manifestWrite=func(f *os.File,p []byte)(int,error){return f.Write(p[:1])}
    wal.segmentLimit=1
    _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("unacknowledged")}})
    require.ErrorIs(t,err,io.ErrShortWrite)
    _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("rejected")}})
    require.Error(t,err)
    require.NoError(t,wal.Close())
    reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
    require.NoError(t,err)
    defer func(){require.NoError(t,reopened.Close())}()
    require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
}

func TestWALFaultManifestRotationFailureMatrix(t *testing.T) {
    for _,stage:=range []string{"segment:create","segment:sync","segment:dir-sync",
        "manifest:create","manifest:write","manifest:sync","manifest:rename","manifest:dir-sync",
        "rotate:segment-synced","rotate:manifest-synced"}{
        t.Run(stage,func(t *testing.T){
            root:=t.TempDir()
            wal,_:=testOpenWALWithActiveTransaction(t,root)
            _,err:=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("durable")}})
            require.NoError(t,err)
            wal.segmentLimit=1
            wal.faultHook=func(point string)error{
                if point==stage{return syscall.ENOSPC}
                return nil
            }
            _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("not acknowledged")}})
            require.ErrorIs(t,err,syscall.ENOSPC)
            _,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("refused")}})
            require.Error(t,err)
            require.NoError(t,wal.Close())
            reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
            require.NoError(t,err,"rotation failure at %s",stage)
            defer func(){require.NoError(t,reopened.Close())}()
            require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
            rec,ok,err:=reopened.ReadTopicRecord("orders",0)
            require.NoError(t,err)
            require.True(t,ok)
            require.Equal(t,"durable",string(rec.Data))
        })
    }
}

func TestWALFaultFrameHeaderIntegrityAndSequence(t *testing.T){
    cases:=[]struct{
        name string
        modify func([]byte)
        want string
    }{
        {"magic",func(b []byte){b[0]^=0xff},"unrecognized WAL frame magic"},
        {"header-crc",func(b []byte){b[24]^=0xff},"header checksum mismatch"},
        {"version",func(b []byte){
            binary.BigEndian.PutUint16(b[4:6],99)
            binary.BigEndian.PutUint32(b[24:28],crc32.Checksum(b[:24],transactionWALCRCTable))
        },"unsupported WAL frame version"},
        {"sequence",func(b []byte){
            binary.BigEndian.PutUint64(b[8:16],1)
            binary.BigEndian.PutUint32(b[24:28],crc32.Checksum(b[:24],transactionWALCRCTable))
        },"non-monotonic WAL frame sequence"},
    }
    for _,tc:=range cases{
        t.Run(tc.name,func(t *testing.T){
            root:=t.TempDir()
            wal,_:=testOpenWALWithActiveTransaction(t,root)
            seq:=wal.nextFrameSeq
            path:=wal.walPath(wal.activeKey())
            require.NoError(t,wal.Close())
            frame,err:=txnWALFrameWithSequence(transactionWALRecord{Type:"binding",Binding:"unacknowledged"},seq)
            require.NoError(t,err)
            tc.modify(frame)
            f,err:=os.OpenFile(path,os.O_APPEND|os.O_WRONLY,0600)
            require.NoError(t,err)
            n,err:=f.Write(frame)
            require.NoError(t,err)
            require.Equal(t,len(frame),n)
            require.NoError(t,f.Close())
            _,err=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
            require.ErrorContains(t,err,tc.want,fmt.Sprintf("case %s",tc.name))
        })
    }
}
