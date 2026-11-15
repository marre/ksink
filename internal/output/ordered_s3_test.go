package output

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
    "io"
    "path/filepath"
    "os"
    "strings"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/smithy-go"
    "github.com/marre/ksink/pkg/ksink"
    "github.com/stretchr/testify/require"
)

type fakeOrderedS3Object struct {
    data []byte
    meta map[string]string
}
type fakeOrderedS3Upload struct {
    key string
    meta map[string]string
    parts map[int32][]byte
}
type fakeOrderedS3 struct {
    objects map[string]fakeOrderedS3Object
    uploads map[string]*fakeOrderedS3Upload
    puts, gets, creates, parts, completes, aborts int
    putAmbiguous bool
    completeAmbiguous bool
    headError error
    getError error
}
func newFakeOrderedS3() *fakeOrderedS3 {
    return &fakeOrderedS3{
        objects:make(map[string]fakeOrderedS3Object),
        uploads:make(map[string]*fakeOrderedS3Upload),
    }
}
func (f *fakeOrderedS3) HeadObject(_ context.Context,in *s3.HeadObjectInput,_ ...func(*s3.Options)) (*s3.HeadObjectOutput,error) {
    if f.headError!=nil{return nil,f.headError}
    obj,ok:=f.objects[aws.ToString(in.Key)]
    if !ok {return nil,&smithy.GenericAPIError{Code:"NotFound",Message:"not found"}}
    return &s3.HeadObjectOutput{ContentLength:aws.Int64(int64(len(obj.data))),Metadata:obj.meta},nil
}
func (f *fakeOrderedS3) GetObject(_ context.Context,in *s3.GetObjectInput,_ ...func(*s3.Options)) (*s3.GetObjectOutput,error) {
    f.gets++
    if f.getError!=nil{return nil,f.getError}
    obj,ok:=f.objects[aws.ToString(in.Key)]
    if !ok {return nil,&smithy.GenericAPIError{Code:"NoSuchKey",Message:"not found"}}
    return &s3.GetObjectOutput{
        Body:io.NopCloser(bytes.NewReader(obj.data)),
        ContentLength:aws.Int64(int64(len(obj.data))),
    },nil
}
func fakeOrderedS3StoreMetadata(in map[string]string) map[string]string {
    out:=make(map[string]string,len(in))
    for k,v:=range in {out[k]=v}
    return out
}
func (f *fakeOrderedS3) PutObject(_ context.Context,in *s3.PutObjectInput,_ ...func(*s3.Options)) (*s3.PutObjectOutput,error) {
    f.puts++
    if aws.ToString(in.IfNoneMatch)!="*" {return nil,errors.New("missing conditional PUT")}
    if _,ok:=f.objects[aws.ToString(in.Key)];ok {
        return nil,&smithy.GenericAPIError{Code:"PreconditionFailed",Message:"exists"}
    }
    data,err:=io.ReadAll(in.Body)
    if err!=nil{return nil,err}
    if int64(len(data))!=aws.ToInt64(in.ContentLength){return nil,errors.New("wrong PUT length")}
    f.objects[aws.ToString(in.Key)]=fakeOrderedS3Object{
        data:data,meta:fakeOrderedS3StoreMetadata(in.Metadata),
    }
    if f.putAmbiguous {
        f.putAmbiguous=false
        return nil,errors.New("response lost after durable PUT")
    }
    return &s3.PutObjectOutput{},nil
}
func (f *fakeOrderedS3) CreateMultipartUpload(_ context.Context,in *s3.CreateMultipartUploadInput,_ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput,error) {
    f.creates++
    id:=fmt.Sprintf("upload-%d",f.creates)
    f.uploads[id]=&fakeOrderedS3Upload{key:aws.ToString(in.Key),
        meta:fakeOrderedS3StoreMetadata(in.Metadata),parts:make(map[int32][]byte)}
    return &s3.CreateMultipartUploadOutput{UploadId:aws.String(id)},nil
}
func (f *fakeOrderedS3) UploadPart(_ context.Context,in *s3.UploadPartInput,_ ...func(*s3.Options)) (*s3.UploadPartOutput,error) {
    f.parts++
    upload:=f.uploads[aws.ToString(in.UploadId)]
    if upload==nil {return nil,errors.New("unknown multipart ID")}
    data,err:=io.ReadAll(in.Body)
    if err!=nil{return nil,err}
    if int64(len(data))!=aws.ToInt64(in.ContentLength){return nil,errors.New("wrong part size")}
    upload.parts[aws.ToInt32(in.PartNumber)]=data
    return &s3.UploadPartOutput{ETag:aws.String(fmt.Sprintf("etag-%d",aws.ToInt32(in.PartNumber)))},nil
}
func (f *fakeOrderedS3) CompleteMultipartUpload(_ context.Context,in *s3.CompleteMultipartUploadInput,_ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput,error) {
    f.completes++
    if aws.ToString(in.IfNoneMatch)!="*" {return nil,errors.New("missing conditional complete")}
    key:=aws.ToString(in.Key)
    if _,ok:=f.objects[key];ok {
        return nil,&smithy.GenericAPIError{Code:"PreconditionFailed",Message:"exists"}
    }
    upload:=f.uploads[aws.ToString(in.UploadId)]
    if upload==nil {return nil,errors.New("missing multipart upload")}
    var payload []byte
    for _,part:=range in.MultipartUpload.Parts {
        data,ok:=upload.parts[aws.ToInt32(part.PartNumber)]
        if !ok {return nil,errors.New("missing part")}
        payload=append(payload,data...)
    }
    f.objects[key]=fakeOrderedS3Object{data:payload,meta:upload.meta}
    delete(f.uploads,aws.ToString(in.UploadId))
    if f.completeAmbiguous {
        f.completeAmbiguous=false
        return nil,errors.New("lost successful multipart completion response")
    }
    return &s3.CompleteMultipartUploadOutput{},nil
}
func (f *fakeOrderedS3) AbortMultipartUpload(_ context.Context,in *s3.AbortMultipartUploadInput,_ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput,error) {
    f.aborts++
    delete(f.uploads,aws.ToString(in.UploadId))
    return &s3.AbortMultipartUploadOutput{},nil
}

const testOrderedS3Pattern = "s3://example-bucket/root/{topic}/messages-{seq:08}.jsonl"
func testOrderedS3Materializer(t *testing.T,wal *ksink.TransactionLog,fake *fakeOrderedS3,
    size int64) *OrderedTopicMaterializer {
    t.Helper()
    p,err:=newS3OrderedPublisherWithClient(testOrderedS3Pattern,S3OrderedOpts{},fake)
    require.NoError(t,err)
    w,err:=newS3OrderedTopicMaterializerWithPublisher(wal,p,size,time.Minute)
    require.NoError(t,err)
    return w
}

func TestS3OrderedDestinationAndTopicEncoding(t *testing.T) {
    bucket,pattern,root,width,err:=parseS3OrderedDestination(testOrderedS3Pattern)
    require.NoError(t,err)
    require.Equal(t,"example-bucket",bucket)
    require.Equal(t,"root",root)
    require.Equal(t,8,width)
    require.Contains(t,pattern,"{topic}")
    p,err:=newS3OrderedPublisherWithClient(testOrderedS3Pattern,S3OrderedOpts{},newFakeOrderedS3())
    require.NoError(t,err)
    key,rel,err:=p.filename("Orders",1)
    require.NoError(t,err)
    require.Equal(t,"root/%4Frders/messages-00000001.jsonl",key)
    require.Equal(t,"%4Frders/messages-00000001.jsonl",rel)
    for _,bad:=range []string{
        "s3://example-bucket/", "s3://example-bucket/root/{topic}/no-sequence",
        "s3://example-bucket/root/{topic}/part-{seq:03}",
        "s3://example-bucket/root/{topic}/{txnID}-{seq:08}",
        "s3://example-bucket/root/{topic}/part-{seq:08}?x=y",
        "s3://user@example-bucket/root/{topic}/part-{seq:08}",
        "s3://example-bucket/root/../root/{topic}/part-{seq:08}",
        "s3://example-bucket/root/{topic}/part-{seq:08}#frag",
        "s3://example-bucket/root/{topic}/part-{seq:08}/extra",
        "s3://example-bucket/root/{topic}/part-{seq:08}-{seq:08}",
    } {
        _,_,_,_,err:=parseS3OrderedDestination(bad)
        require.Error(t,err,bad)
    }
}

func TestS3OrderedPublicationReceiptAndPerTopicOrdering(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    w:=testOrderedS3Materializer(t,wal,fake,4)
    _,err:=wal.AppendTopicBatch([]ksink.WALTopicRecord{
        {Topic:"orders",Data:[]byte("a\n")},{Topic:"payments",Data:[]byte("p\n")},
        {Topic:"orders",Data:[]byte("b\n")},{Topic:"orders",Data:[]byte("c\n")},
    })
    require.NoError(t,err)
    n,err:=w.Publish(context.Background(),false)
    require.NoError(t,err)
    require.Equal(t,1,n)
    require.Equal(t,"a\nb\n",string(fake.objects["root/orders/messages-00000001.jsonl"].data))
    require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
    require.Equal(t,int64(0),wal.TopicProgress("payments").Cursor)
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,"c\n",string(fake.objects["root/orders/messages-00000002.jsonl"].data))
    require.Equal(t,"p\n",string(fake.objects["root/payments/messages-00000001.jsonl"].data))
    receipt:=wal.TopicProgress("orders").LastFile
    require.Equal(t,"orders/messages-00000002.jsonl",receipt.Path)
    hash:=sha256.Sum256([]byte("c\n"))
    require.Equal(t,hex.EncodeToString(hash[:]),receipt.SHA256)
    require.Equal(t,3,fake.puts)
    // Removal after a durable receipt never reuses its sequence.
    delete(fake.objects,"root/orders/messages-00000001.jsonl")
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("later\n")}})
    require.NoError(t,err)
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,uint64(4),wal.TopicProgress("orders").NextFile)
    require.NotContains(t,fake.objects,"root/orders/messages-00000001.jsonl")
}

func TestS3OrderedAmbiguousPUTAndConflict(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    fake.putAmbiguous=true
    w:=testOrderedS3Materializer(t,wal,fake,1)
    _,err:=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("first\n")}})
    require.NoError(t,err)
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,1,fake.puts,"ambiguous success must be HEAD-reconciled without overwriting")
    require.Equal(t,int64(1),wal.TopicProgress("orders").Cursor)
    // An unrelated completed object is never replaced by subsequent output.
    fake.objects["root/orders/messages-00000002.jsonl"]=fakeOrderedS3Object{
        data:[]byte("wrong"),meta:map[string]string{"ksink-sha256":strings.Repeat("0",64)},
    }
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("second\n")}})
    require.NoError(t,err)
    err=w.Flush(context.Background())
    require.ErrorContains(t,err,"differs from committed WAL bytes")
    require.Equal(t,"wrong",string(fake.objects["root/orders/messages-00000002.jsonl"].data))
    require.Equal(t,int64(1),wal.TopicProgress("orders").Cursor)
    require.Equal(t,1,fake.puts)
}

func TestS3OrderedExistingObjectForgedMetadataFailsClosed(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    w:=testOrderedS3Materializer(t,wal,fake,1)
    expected:=[]byte("correct\\n")
    _,err:=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:expected}})
    require.NoError(t,err)
    digest:=sha256.Sum256(expected)
    // Same length and forged hash metadata: HEAD alone must not prove bytes.
    fake.objects["root/orders/messages-00000001.jsonl"]=fakeOrderedS3Object{
        data:[]byte("garbage\\n"),
        meta:map[string]string{"ksink-sha256":hex.EncodeToString(digest[:])},
    }
    err=w.Flush(context.Background())
    require.ErrorContains(t,err,"body differs from committed WAL bytes")
    require.Zero(t,wal.TopicProgress("orders").Cursor,"no receipt for a forged object")
    require.Equal(t,uint64(1),wal.TopicProgress("orders").NextFile)
    require.Zero(t,fake.puts,"immutable object must never be overwritten")
    require.Equal(t,1,fake.gets)
}

func TestS3OrderedExistingObjectGETFailureFailsClosed(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    w:=testOrderedS3Materializer(t,wal,fake,1)
    data:=[]byte("correct\\n")
    _,err:=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:data}})
    require.NoError(t,err)
    digest:=sha256.Sum256(data)
    fake.objects["root/orders/messages-00000001.jsonl"]=fakeOrderedS3Object{
        data:data,meta:map[string]string{"ksink-sha256":hex.EncodeToString(digest[:])},
    }
    fake.getError=errors.New("simulated GET outage")
    require.ErrorContains(t,w.Flush(context.Background()),"simulated GET outage")
    require.Zero(t,wal.TopicProgress("orders").Cursor)
    fake.getError=nil
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,int64(1),wal.TopicProgress("orders").Cursor)
    require.Zero(t,fake.puts)
}

func TestS3OrderedCrashAfterObjectBeforeReceipt(t *testing.T) {
    state:=filepath.Join(t.TempDir(),"wal")
    wal,err:=ksink.OpenTransactionLog(state)
    require.NoError(t,err)
    fake:=newFakeOrderedS3()
    _=testOrderedS3Materializer(t,wal,fake,1)
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{
        {Topic:"orders",Data:[]byte("first\n")},
        {Topic:"orders",Data:[]byte("second\n")},
    })
    require.NoError(t,err)
    digest:=sha256.Sum256([]byte("first\n"))
    fake.objects["root/orders/messages-00000001.jsonl"]=fakeOrderedS3Object{
        data:[]byte("first\n"),meta:map[string]string{"ksink-sha256":hex.EncodeToString(digest[:])},
    }
    require.NoError(t,wal.Close())
    wal,err=ksink.OpenTransactionLog(state)
    require.NoError(t,err)
    defer func(){require.NoError(t,wal.Close())}()
    w:=testOrderedS3Materializer(t,wal,fake,1)
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,1,fake.puts,"first object must be recovered, not uploaded again")
    require.Equal(t,int64(2),wal.TopicProgress("orders").Cursor)
    require.Equal(t,uint64(3),wal.TopicProgress("orders").NextFile)
    require.Equal(t,"second\n",string(fake.objects["root/orders/messages-00000002.jsonl"].data))
}

func TestS3OrderedMultipartAndAmbiguousCompletion(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    fake.completeAmbiguous=true
    p,err:=newS3OrderedPublisherWithClient(testOrderedS3Pattern,
        S3OrderedOpts{MultipartPartSize:5<<20},fake)
    require.NoError(t,err)
    w,err:=newS3OrderedTopicMaterializerWithPublisher(wal,p,1,time.Minute)
    require.NoError(t,err)
    data:=bytes.Repeat([]byte("x"),(5<<20)+1)
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"big",Data:data}})
    require.NoError(t,err)
    require.NoError(t,w.Flush(context.Background()))
    require.Equal(t,1,fake.creates)
    require.Equal(t,2,fake.parts)
    require.Equal(t,1,fake.completes)
    require.Equal(t,0,fake.puts)
    require.Equal(t,data,fake.objects["root/big/messages-00000001.jsonl"].data)
    require.Equal(t,int64(1),wal.TopicProgress("big").Cursor)
}

func TestS3OrderedOutputBindingAndHeadFailure(t *testing.T) {
    wal:=orderedTestLog(t)
    fake:=newFakeOrderedS3()
    w:=testOrderedS3Materializer(t,wal,fake,1)
    _,err:=newS3OrderedTopicMaterializerWithPublisher(wal,&S3OrderedPublisher{
        identity:"ordered-s3-v1|another-bucket",
    },1,time.Minute)
    require.ErrorContains(t,err,"binding changed")
    _,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("record")}})
    require.NoError(t,err)
    fake.headError=errors.New("remote S3 unavailable")
    _,err=w.Publish(context.Background(),false)
    require.ErrorContains(t,err,"remote S3 unavailable")
    require.Equal(t,int64(0),wal.TopicProgress("orders").Cursor)
    require.Equal(t,0,fake.puts)
}

func TestS3OrderedWALBindsEffectiveSDKEndpoint(t *testing.T) {
    // The SDK's endpoint configuration must be reflected in WAL identity,
    // even when --output-s3-endpoint is not supplied.
    t.Setenv("AWS_REGION","us-east-1")
    t.Setenv("AWS_ACCESS_KEY_ID","test")
    t.Setenv("AWS_SECRET_ACCESS_KEY","test")
    t.Setenv("AWS_ENDPOINT_URL","")
    t.Setenv("AWS_ENDPOINT_URL_S3","http://localhost:9000")
    wal:=orderedTestLog(t)
    first,err:=newS3OrderedPublisher(testOrderedS3Pattern,S3OrderedOpts{})
    require.NoError(t,err)
    require.Contains(t,first.identity,"http://localhost:9000")
    require.Contains(t,first.identity,`region="us-east-1"`)
    _,err=newS3OrderedTopicMaterializerWithPublisher(wal,first,1,time.Minute)
    require.NoError(t,err)
    t.Setenv("AWS_ENDPOINT_URL_S3","http://localhost:9001")
    second,err:=newS3OrderedPublisher(testOrderedS3Pattern,S3OrderedOpts{})
    require.NoError(t,err)
    require.Contains(t,second.identity,"http://localhost:9001")
    _,err=newS3OrderedTopicMaterializerWithPublisher(wal,second,1,time.Minute)
    require.ErrorContains(t,err,"binding changed")
    // An explicit endpoint overrides the SDK's environment endpoint.
    explicit,err:=newS3OrderedPublisher(testOrderedS3Pattern,
        S3OrderedOpts{Endpoint:"http://localhost:9000"})
    require.NoError(t,err)
    require.Equal(t,first.identity,explicit.identity)
}

func TestS3OrderedWALBindsSharedConfigEndpointAndRegion(t *testing.T) {
    t.Setenv("AWS_ACCESS_KEY_ID","test")
    t.Setenv("AWS_SECRET_ACCESS_KEY","test")
    t.Setenv("AWS_ENDPOINT_URL","")
    t.Setenv("AWS_ENDPOINT_URL_S3","")
    t.Setenv("AWS_REGION","")
    t.Setenv("AWS_PROFILE","default")
    configFile:=filepath.Join(t.TempDir(),"aws-config")
    require.NoError(t,os.WriteFile(configFile,[]byte(
        "[default]\nregion = eu-north-1\nendpoint_url = http://localhost:9100\n"),0600))
    t.Setenv("AWS_CONFIG_FILE",configFile)
    first,err:=newS3OrderedPublisher(testOrderedS3Pattern,S3OrderedOpts{})
    require.NoError(t,err)
    require.Contains(t,first.identity,"http://localhost:9100")
    require.Contains(t,first.identity,`region="eu-north-1"`)
    wal:=orderedTestLog(t)
    _,err=newS3OrderedTopicMaterializerWithPublisher(wal,first,1,time.Minute)
    require.NoError(t,err)
    require.NoError(t,os.WriteFile(configFile,[]byte(
        "[default]\nregion = eu-north-1\nendpoint_url = http://localhost:9101\n"),0600))
    changed,err:=newS3OrderedPublisher(testOrderedS3Pattern,S3OrderedOpts{})
    require.NoError(t,err)
    require.Contains(t,changed.identity,"http://localhost:9101")
    _,err=newS3OrderedTopicMaterializerWithPublisher(wal,changed,1,time.Minute)
    require.ErrorContains(t,err,"binding changed")
    // The AWS region also influences the default S3 service endpoint.
    t.Setenv("AWS_ENDPOINT_URL_S3","")
    require.NoError(t,os.WriteFile(configFile,[]byte(
        "[default]\nregion = eu-west-1\n"),0600))
    regionChanged,err:=newS3OrderedPublisher(testOrderedS3Pattern,S3OrderedOpts{})
    require.NoError(t,err)
    require.Contains(t,regionChanged.identity,`region="eu-west-1"`)
    _,err=newS3OrderedTopicMaterializerWithPublisher(wal,regionChanged,1,time.Minute)
    require.ErrorContains(t,err,"binding changed")
}
