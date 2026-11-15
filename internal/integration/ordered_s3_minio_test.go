package ksink_test

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/base64"
    "fmt"
    "io"
    "net/http"
    "path/filepath"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
    "github.com/marre/ksink/internal/output"
    "github.com/marre/ksink/pkg/ksink"
    "github.com/ory/dockertest/v3"
    "github.com/ory/dockertest/v3/docker"
    "github.com/stretchr/testify/require"
    "github.com/twmb/franz-go/pkg/kgo"
)

// This exercises real conditional PUT and conditional multipart completion
// against a pinned S3-compatible service, not the old CommitTxn writer.
func TestOrderedS3MinIOPublicationAndRecovery(t *testing.T) {
    checkDockerIntegration(t)
    t.Setenv("AWS_ACCESS_KEY_ID","minioadmin")
    t.Setenv("AWS_SECRET_ACCESS_KEY","minioadmin")
    t.Setenv("AWS_REGION","us-east-1")

    pool:=newDockerPool(t)
    resource,err:=pool.RunWithOptions(&dockertest.RunOptions{
        Repository:"ghcr.io/intikhab49/minio-from-source",Tag:"RELEASE.2025-09-07T16-13-09Z",
        Cmd:[]string{"server","/data","--address",":9000"},
        ExposedPorts:[]string{"9000/tcp"},
        Env:[]string{"MINIO_ROOT_USER=minioadmin","MINIO_ROOT_PASSWORD=minioadmin"},
    },func(c *docker.HostConfig){
        c.AutoRemove=true
        c.RestartPolicy=docker.RestartPolicy{Name:"no"}
    })
    require.NoError(t,err)
    t.Cleanup(func(){_ =pool.Purge(resource)})
    endpoint:="http://127.0.0.1:"+resource.GetPort("9000/tcp")
    clientHTTP:=&http.Client{Timeout:2*time.Second}
    err=pool.Retry(func()error{
        resp,err:=clientHTTP.Get(endpoint+"/minio/health/live")
        if err!=nil{return err}
        defer resp.Body.Close() //nolint:errcheck
        if resp.StatusCode!=http.StatusOK{return fmt.Errorf("MinIO readiness: HTTP %d",resp.StatusCode)}
        return nil
    })
    require.NoError(t,err)
    ctx:=context.Background()
    cfg,err:=config.LoadDefaultConfig(ctx,config.WithRegion("us-east-1"))
    require.NoError(t,err)
    client:=s3.NewFromConfig(cfg,func(o *s3.Options){
        o.BaseEndpoint=aws.String(endpoint)
        o.UsePathStyle=true
    })
    const bucket="ksink-ordered-test"
    _,err=client.CreateBucket(ctx,&s3.CreateBucketInput{Bucket:aws.String(bucket)})
    require.NoError(t,err)
    opts:=output.S3OrderedOpts{
        Region:"us-east-1",Endpoint:endpoint,ForcePathStyle:true,
        RetryMaxAttempts:3,MultipartPartSize:5<<20,
    }
    state:=filepath.Join(t.TempDir(),"wal")
    wal,err:=ksink.OpenTransactionLog(state)
    require.NoError(t,err)
    w,err:=output.NewS3OrderedTopicMaterializer(wal,
        "s3://"+bucket+"/data/{topic}/messages-{seq:08}.jsonl",
        opts,1,time.Minute)
    require.NoError(t,err)
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
        {Topic:"orders",Data:[]byte("first\n")},
        {Topic:"payments",Data:[]byte("paid\n")},
    })
    require.NoError(t,err)
    require.NoError(t,w.Flush(ctx))
    key:="data/orders/messages-00000001.jsonl"
    obj,err:=client.GetObject(ctx,&s3.GetObjectInput{Bucket:aws.String(bucket),Key:aws.String(key)})
    require.NoError(t,err)
    got,err:=io.ReadAll(obj.Body)
    require.NoError(t,err)
    require.NoError(t,obj.Body.Close())
    require.Equal(t,"first\n",string(got))
    sum:=sha256.Sum256(got)
    head,err:=client.HeadObject(ctx,&s3.HeadObjectInput{
        Bucket:aws.String(bucket),Key:aws.String(key),
        ChecksumMode:types.ChecksumModeEnabled,
    })
    require.NoError(t,err)
    require.Equal(t,base64.StdEncoding.EncodeToString(sum[:]),aws.ToString(head.ChecksumSHA256))
    require.Equal(t,types.ChecksumTypeFullObject,head.ChecksumType)
    require.Empty(t,obj.Metadata["ksink-sha256"])
    require.Equal(t,uint64(2),wal.TopicProgress("orders").NextFile)

    // Emulate a completed object before the corresponding WAL receipt.
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("second\n")}})
    require.NoError(t,err)
    key="data/orders/messages-00000002.jsonl"
    sum=sha256.Sum256([]byte("second\n"))
    _,err=client.PutObject(ctx,&s3.PutObjectInput{
        Bucket:aws.String(bucket),Key:aws.String(key),
        Body:bytes.NewReader([]byte("second\n")),
        ContentLength:aws.Int64(int64(len("second\n"))),
        IfNoneMatch:aws.String("*"),
        ChecksumSHA256:aws.String(base64.StdEncoding.EncodeToString(sum[:])),
    })
    require.NoError(t,err)
    require.NoError(t,wal.Close())
    wal,err=ksink.OpenTransactionLog(state)
    require.NoError(t,err)
    defer func(){require.NoError(t,wal.Close())}()
    w,err=output.NewS3OrderedTopicMaterializer(wal,
        "s3://"+bucket+"/data/{topic}/messages-{seq:08}.jsonl",opts,1,time.Minute)
    require.NoError(t,err)
    require.NoError(t,w.Flush(ctx))
    require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)

    // A large committed WAL record uses multipart completion with a
    // conditional no-overwrite precondition, then becomes one S3 object.
    big:=bytes.Repeat([]byte("x"),(5<<20)+1)
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"big",Data:big}})
    require.NoError(t,err)
    require.NoError(t,w.Flush(ctx))
    obj,err=client.GetObject(ctx,&s3.GetObjectInput{
        Bucket:aws.String(bucket),Key:aws.String("data/big/messages-00000001.jsonl"),
    })
    require.NoError(t,err)
    got,err=io.ReadAll(obj.Body)
    require.NoError(t,err)
    require.NoError(t,obj.Body.Close())
    require.Equal(t,big,got)

    // Never overwrite a preexisting key with conflicting content.
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("third\n")}})
    require.NoError(t,err)
    key="data/orders/messages-00000003.jsonl"
    wrongChecksum:=sha256.Sum256([]byte("wrong\n"))
    _,err=client.PutObject(ctx,&s3.PutObjectInput{
        Bucket:aws.String(bucket),Key:aws.String(key),
        Body:bytes.NewReader([]byte("wrong\n")),
        ChecksumSHA256:aws.String(base64.StdEncoding.EncodeToString(wrongChecksum[:])),
    })
    require.NoError(t,err)
    err=w.Flush(ctx)
    require.ErrorContains(t,err,"server-validated SHA256 checksum differs")
    require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
    // Run real Kafka multi-topic transactions into this same S3 backend.
    // A committed object may contain multiple transactions, while aborted
    // application records and all control markers must remain invisible.
    txWal,err:=ksink.OpenTransactionLog(filepath.Join(t.TempDir(),"transactional-wal"))
    require.NoError(t,err)
    defer func(){require.NoError(t,txWal.Close())}()
    txWriter,err:=output.NewS3OrderedTopicMaterializer(txWal,
        "s3://"+bucket+"/data/transactions/{topic}/messages-{seq:08}.jsonl",
        opts,128<<20,time.Minute)
    require.NoError(t,err)
    formatter:=func(msg *ksink.Message)([]byte,error){
        return []byte(fmt.Sprintf("%s/%d:%s\n",msg.Topic,msg.Offset,msg.Value)),nil
    }
    srv,err:=ksink.New(ksink.Config{
        Address:fmt.Sprintf("127.0.0.1:%d",getFreePort(t)),
        TransactionalWrite:true,Timeout:5*time.Second,
    },ksink.WithTransactionLog(txWal),
        ksink.WithOrderedTransactionalOutput(formatter),
        ksink.WithLogger(&integrationLogger{t}))
    require.NoError(t,err)
    require.NoError(t,srv.Start(ctx))
    txCtx,txCancel:=context.WithTimeout(ctx,30*time.Second)
    defer txCancel()
    txClient,err:=kgo.NewClient(
        kgo.SeedBrokers(srv.Addr().String()),
        kgo.RequestTimeoutOverhead(5*time.Second),
        kgo.TransactionalID("s3-ordered-transactions"),
    )
    require.NoError(t,err)
    require.NoError(t,txClient.BeginTransaction())
    require.NoError(t,txClient.ProduceSync(txCtx,
        &kgo.Record{Topic:"orders",Value:[]byte("first")},
        &kgo.Record{Topic:"payments",Value:[]byte("paid")},
    ).FirstErr())
    require.NoError(t,txClient.EndTransaction(txCtx,kgo.TryCommit))
    require.NoError(t,txClient.BeginTransaction())
    require.NoError(t,txClient.ProduceSync(txCtx,
        &kgo.Record{Topic:"orders",Value:[]byte("aborted")},
        &kgo.Record{Topic:"payments",Value:[]byte("aborted")},
    ).FirstErr())
    require.NoError(t,txClient.EndTransaction(txCtx,kgo.TryAbort))
    require.NoError(t,txClient.BeginTransaction())
    require.NoError(t,txClient.ProduceSync(txCtx,
        &kgo.Record{Topic:"orders",Value:[]byte("later")},
    ).FirstErr())
    require.NoError(t,txClient.EndTransaction(txCtx,kgo.TryCommit))
    txClient.Close()
    require.NoError(t,srv.Close(ctx))
    require.NoError(t,txWriter.Flush(ctx))
    for topic,want:=range map[string]string{
        "orders":"orders/0:first\norders/4:later\n",
        "payments":"payments/0:paid\n",
    } {
        object,readErr:=client.GetObject(ctx,&s3.GetObjectInput{
            Bucket:aws.String(bucket),
            Key:aws.String("data/transactions/"+topic+"/messages-00000001.jsonl"),
        })
        require.NoError(t,readErr)
        data,readErr:=io.ReadAll(object.Body)
        require.NoError(t,readErr)
        require.NoError(t,object.Body.Close())
        require.Equal(t,want,string(data))
        require.NotContains(t,string(data),"aborted")
        require.Equal(t,uint64(2),txWal.TopicProgress(topic).NextFile)
    }

    // Concurrent ksink writers and concurrent MPU completions exercise
    // the immutable-key preconditions on this exact pinned S3 backend.
    exerciseConcurrentS3Publication(t,ctx,client,bucket,opts)
    exerciseConcurrentS3MultipartCompletion(t,ctx,client,bucket)

}
