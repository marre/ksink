package ksink_test

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/base64"
    "errors"
    "fmt"
    "path/filepath"
    "sync"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
    "github.com/aws/smithy-go"
    "github.com/marre/ksink/internal/output"
    "github.com/marre/ksink/pkg/ksink"
    "github.com/stretchr/testify/require"
)

// These race tests run against a real pinned S3-compatible implementation,
// not an in-memory fake with an accidentally serialized conditional write.
func exerciseConcurrentS3Publication(t *testing.T,ctx context.Context,client *s3.Client,
    bucket string,opts output.S3OrderedOpts) {
    t.Helper()
    const contenders=6
    const key="data/concurrent/orders/messages-00000001.jsonl"
    destination:="s3://"+bucket+"/data/concurrent/{topic}/messages-{seq:08}.jsonl"
    type contender struct {
        wal *ksink.TransactionLog
        publisher *output.OrderedTopicMaterializer
        data string
        err error
    }
    nodes:=make([]contender,contenders)
    for i:=range nodes {
        wal,err:=ksink.OpenTransactionLog(filepath.Join(t.TempDir(),fmt.Sprintf("wal-%d",i)))
        require.NoError(t,err)
        nodes[i].wal=wal
        nodes[i].data=fmt.Sprintf("concurrent-%02d\\n",i)
        writer,err:=output.NewS3OrderedTopicMaterializer(wal,destination,opts,1,time.Second)
        require.NoError(t,err)
        nodes[i].publisher=writer
        _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte(nodes[i].data)}})
        require.NoError(t,err)
    }
    gate:=make(chan struct{})
    var wg sync.WaitGroup
    for i:=range nodes {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            <-gate
            nodes[i].err=nodes[i].publisher.Flush(ctx)
        }(i)
    }
    close(gate)
    wg.Wait()

    winner:=-1
    for i:=range nodes {
        if nodes[i].err==nil {
            require.Equal(t,-1,winner,"only one independent WAL may publish an immutable S3 key")
            winner=i
            require.Equal(t,int64(1),nodes[i].wal.TopicProgress("orders").Cursor)
        } else {
            require.ErrorContains(t,nodes[i].err,"S3 immutable object conflict")
            require.Zero(t,nodes[i].wal.TopicProgress("orders").Cursor,
                "a failed competing publisher must not write a receipt")
        }
    }
    require.NotEqual(t,-1,winner,"at least one writer must publish")
    actual,err:=client.GetObject(ctx,&s3.GetObjectInput{Bucket:aws.String(bucket),Key:aws.String(key)})
    require.NoError(t,err)
    object:=new(bytes.Buffer)
    _,err=object.ReadFrom(actual.Body)
    require.NoError(t,err)
    require.NoError(t,actual.Body.Close())
    require.Equal(t,nodes[winner].data,object.String(),"the only winner owns the final bytes")
    for i:=range nodes {
        require.NoError(t,nodes[i].wal.Close())
    }
}

func exerciseConcurrentS3MultipartCompletion(t *testing.T,ctx context.Context,client *s3.Client,bucket string) {
    t.Helper()
    const key="data/concurrent/multipart/immutable.bin"
    type upload struct{
        id string
        parts []types.CompletedPart
        checksum string
        err error
    }
    sessions:=make([]upload,2)
    for i:=range sessions {
        started,err:=client.CreateMultipartUpload(ctx,&s3.CreateMultipartUploadInput{
            Bucket:aws.String(bucket),Key:aws.String(key),
            ChecksumAlgorithm:types.ChecksumAlgorithmSha256,
            ChecksumType:types.ChecksumTypeComposite,
        })
        require.NoError(t,err)
        sessions[i].id=aws.ToString(started.UploadId)
        var digests []byte
        for partIndex,body:=range [][]byte{
            bytes.Repeat([]byte{byte('A'+i)},5<<20),
            []byte(fmt.Sprintf("part-two-%d",i)),
        } {
            digest:=sha256.Sum256(body)
            digests=append(digests,digest[:]...)
            checksum:=base64.StdEncoding.EncodeToString(digest[:])
            n:=int32(partIndex+1)
            part,err:=client.UploadPart(ctx,&s3.UploadPartInput{
                Bucket:aws.String(bucket),Key:aws.String(key),
                UploadId:started.UploadId,PartNumber:aws.Int32(n),
                ChecksumSHA256:aws.String(checksum),
                ContentLength:aws.Int64(int64(len(body))),
                Body:bytes.NewReader(body),
            })
            require.NoError(t,err)
            require.Equal(t,checksum,aws.ToString(part.ChecksumSHA256))
            sessions[i].parts=append(sessions[i].parts,types.CompletedPart{
                PartNumber:aws.Int32(n),ETag:part.ETag,ChecksumSHA256:aws.String(checksum),
            })
        }
        digest:=sha256.Sum256(digests)
        sessions[i].checksum=base64.StdEncoding.EncodeToString(digest[:])+"-2"
    }
    gate:=make(chan struct{})
    var wg sync.WaitGroup
    for i:=range sessions {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            <-gate
            _,sessions[i].err=client.CompleteMultipartUpload(ctx,&s3.CompleteMultipartUploadInput{
                Bucket:aws.String(bucket),Key:aws.String(key),
                UploadId:aws.String(sessions[i].id),
                IfNoneMatch:aws.String("*"),
                MultipartUpload:&types.CompletedMultipartUpload{Parts:sessions[i].parts},
            })
        }(i)
    }
    close(gate)
    wg.Wait()
    winner:=-1
    for i:=range sessions {
        if sessions[i].err==nil {
            require.Equal(t,-1,winner,"conditional multipart completion must have one winner")
            winner=i
        } else {
            var api smithy.APIError
            require.True(t,errors.As(sessions[i].err,&api),
                "loser should receive 409/412, not a silent overwrite: %v",sessions[i].err)
            require.Contains(t,[]string{"PreconditionFailed","ConditionalRequestConflict","409","412"},api.ErrorCode())
        }
        // Reclaim the losing in-progress MPU if the backend still retains it.
        _,_ = client.AbortMultipartUpload(ctx,&s3.AbortMultipartUploadInput{
            Bucket:aws.String(bucket),Key:aws.String(key),
            UploadId:aws.String(sessions[i].id),
        })
    }
    require.NotEqual(t,-1,winner)
    head,err:=client.HeadObject(ctx,&s3.HeadObjectInput{
        Bucket:aws.String(bucket),Key:aws.String(key),ChecksumMode:types.ChecksumModeEnabled,
    })
    require.NoError(t,err)
    require.Equal(t,types.ChecksumTypeComposite,head.ChecksumType)
    require.Equal(t,sessions[winner].checksum,aws.ToString(head.ChecksumSHA256))
}

