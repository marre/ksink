package output

import (
    "context"
    "crypto/sha256"
    "encoding/base64"
    "encoding/hex"
    "errors"
    "fmt"
    "hash"
    "io"
    "net/url"
    "os"
    "path"
    "strconv"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
    "github.com/aws/smithy-go"
    "github.com/marre/ksink/pkg/ksink"
)

// S3OrderedOpts configures transport, not Kafka transaction or file boundaries.
type S3OrderedOpts struct {
    Region string
    Endpoint string
    ForcePathStyle bool
    RetryMaxAttempts int
    RequestTimeout time.Duration
    MultipartPartSize int64
}

// s3OrderedAPI isolates S3 transport for deterministic publication tests.
type s3OrderedAPI interface {
    HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
    PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
    CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
    UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
    CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
    AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3OrderedPublisher publishes immutable topic-sequence objects. Kafka
// transactions, WAL offsets, rotation, and receipts belong to the materializer.
type S3OrderedPublisher struct {
    client s3OrderedAPI
    bucket string
    pattern string
    root string
    width int
    identity string
    opts S3OrderedOpts
}

type s3OrderedObject struct {
    size int64
    checksum string
    checksumType types.ChecksumType
    partSize int64
}

const s3OrderedMinimumPartSize int64 = 5 << 20
const s3OrderedDefaultPartSize int64 = 8 << 20

// Multipart SHA256 is the base64 SHA256 of concatenated binary part digests,
// followed by "-<part count>". A deterministic boundary is needed to recreate
// this checksum from the WAL after an ambiguous completion or process crash.
func s3OrderedPartSize(size, configured int64) int64 {
    if required := (size-1)/10000+1; required > configured {
        return required
    }
    return configured
}

type s3OrderedHasher struct {
    whole hash.Hash
    part hash.Hash
    composite hash.Hash
    partSize int64 // zero means a single PutObject / FULL_OBJECT checksum
    partBytes int64
    partCount int
}

func newS3OrderedHasher(partSize int64) *s3OrderedHasher {
    h := &s3OrderedHasher{whole:sha256.New(),partSize:partSize}
    if partSize > 0 {
        h.part = sha256.New()
        h.composite = sha256.New()
    }
    return h
}

func (h *s3OrderedHasher) Write(data []byte) (int,error) {
    original := len(data)
    _,_ = h.whole.Write(data)
    if h.partSize == 0 {return original,nil}
    for len(data)>0 {
        count := int64(len(data))
        if available:=h.partSize-h.partBytes;count>available{count=available}
        n:=int(count)
        _,_=h.part.Write(data[:n])
        h.partBytes+=count
        data=data[n:]
        if h.partBytes==h.partSize {
            _,_=h.composite.Write(h.part.Sum(nil))
            h.part.Reset()
            h.partBytes=0
            h.partCount++
        }
    }
    return original,nil
}

func (h *s3OrderedHasher) sums() (digest,checksum string) {
    whole:=h.whole.Sum(nil)
    digest=hex.EncodeToString(whole)
    if h.partSize==0 {return digest,base64.StdEncoding.EncodeToString(whole)}
    if h.partBytes>0 {
        _,_=h.composite.Write(h.part.Sum(nil))
        h.partCount++
        h.partBytes=0
    }
    return digest,base64.StdEncoding.EncodeToString(h.composite.Sum(nil))+"-"+strconv.Itoa(h.partCount)
}

func s3OrderedChecksumMatch(key string, info s3OrderedObject, size int64, expected string, expectedType types.ChecksumType) error {
    if info.size!=size || info.checksumType!=expectedType || info.checksum!=expected {
        return fmt.Errorf("S3 immutable object conflict for %q (size or server-validated SHA256 checksum differs from committed WAL bytes)",key)
    }
    return nil
}

func parseS3OrderedDestination(dst string) (bucket, pattern, root string, width int, err error) {
    u, err := url.Parse(dst)
    if err != nil { return "", "", "", 0, fmt.Errorf("invalid S3 destination: %w", err) }
    if u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" ||
        u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" ||
        strings.ContainsAny(u.Host, ":@") {
        return "", "", "", 0, errors.New("S3 output requires s3://bucket/prefix/{topic}/file-{seq:NN} without credentials, query or fragment")
    }
    if len(u.Host)<3 || len(u.Host)>63 || strings.ContainsAny(u.Host,"/\\") {
        return "", "", "", 0, errors.New("invalid S3 bucket name")
    }
    for _, c := range u.Host {
        if (c<'a' || c>'z') && (c<'0' || c>'9') && c!='.' && c!='-' {
            return "", "", "", 0, errors.New("invalid S3 bucket name")
        }
    }
    key := strings.TrimPrefix(u.Path, "/")
    if key=="" || strings.Contains(key,"\\") || path.Clean(key)!=key ||
        strings.HasPrefix(key,"/") || strings.ContainsAny(key,"\x00\r\n") {
        return "", "", "", 0, errors.New("invalid S3 object key template")
    }
    const marker = "/{topic}/"
    if strings.Count(key,marker)!=1 || strings.Count(key,"{seq:")!=1 ||
        strings.Contains(key,"{txnID}") || strings.Contains(key,"{partition}") {
        return "", "", "", 0, errors.New("S3 ordered output requires exactly one {topic} directory and one {seq:NN} filename")
    }
    prefix, filename, ok := strings.Cut(key,marker)
    if !ok || prefix=="" || filename=="" || strings.Contains(filename,"/") ||
        strings.ContainsAny(prefix,"{}") {
        return "", "", "", 0, errors.New("S3 output must be prefix/{topic}/filename-{seq:NN}")
    }
    start:=strings.Index(filename,"{seq:")
    end:=strings.Index(filename[start:],"}")
    if end<0 {return "", "", "", 0, errors.New("invalid S3 sequence placeholder")}
    end+=start
    n, convErr:=strconv.Atoi(filename[start+5:end])
    if convErr!=nil || n<6 || n>20 || strings.ContainsAny(filename[:start]+filename[end+1:],"{}") ||
       filename[:start]+filename[end+1:]=="" {
        return "", "", "", 0, errors.New("S3 sequence width must be between 6 and 20")
    }
    return u.Host,key,prefix,n,nil
}

func normalizeS3OrderedOpts(opts S3OrderedOpts) (S3OrderedOpts,error) {
    if opts.RetryMaxAttempts<0 || opts.RequestTimeout<0 || opts.MultipartPartSize<0 {
        return opts,errors.New("negative S3 retry, timeout or part size")
    }
    if opts.RetryMaxAttempts==0 {opts.RetryMaxAttempts=3}
    if opts.RequestTimeout==0 {opts.RequestTimeout=30*time.Second}
    if opts.MultipartPartSize==0 {opts.MultipartPartSize=s3OrderedDefaultPartSize}
    if opts.MultipartPartSize<s3OrderedMinimumPartSize {
        return opts,fmt.Errorf("S3 multipart part size must be >= %d bytes",s3OrderedMinimumPartSize)
    }
    if opts.Endpoint!="" {
        u,err:=url.Parse(opts.Endpoint)
        if err!=nil || (u.Scheme!="https" && u.Scheme!="http") || u.Host=="" ||
            u.User!=nil || u.RawQuery!="" || u.Fragment!="" || u.Opaque!="" {
            return opts,errors.New("S3 endpoint must be an http(s) URL without credentials, query or fragment")
        }
    }
    return opts,nil
}

func newS3OrderedPublisher(dst string, opts S3OrderedOpts) (*S3OrderedPublisher,error) {
    _, _, _, _, err:=parseS3OrderedDestination(dst)
    if err!=nil{return nil,err}
    opts,err=normalizeS3OrderedOpts(opts)
    if err!=nil{return nil,err}
    loadOpts:=[]func(*config.LoadOptions)error{config.WithRetryMaxAttempts(opts.RetryMaxAttempts)}
    if opts.Region!="" {
        loadOpts=append(loadOpts,config.WithRegion(opts.Region))
    } else if opts.Endpoint!="" {
        loadOpts=append(loadOpts,config.WithRegion("us-east-1"))
    }
    cfg,err:=config.LoadDefaultConfig(context.Background(),loadOpts...)
    if err!=nil{return nil,fmt.Errorf("load AWS credentials/config: %w",err)}
    client:=s3.NewFromConfig(cfg,func(o *s3.Options) {
        o.UsePathStyle=opts.ForcePathStyle
        if opts.Endpoint!=""{o.BaseEndpoint=aws.String(opts.Endpoint)}
    })
    // The SDK can supply a service endpoint from AWS_ENDPOINT_URL_S3,
    // AWS_ENDPOINT_URL or shared configuration even without our CLI flag.
    // Bind the endpoint actually selected by the client, not just the flag.
    effective:=client.Options()
    opts.Endpoint=aws.ToString(effective.BaseEndpoint)
    opts.Region=effective.Region
    return newS3OrderedPublisherWithClient(dst,opts,client)
}

// The mockable constructor uses the exact same destination identity as the
// production AWS client. No S3 requests occur before WAL binding succeeds.
func newS3OrderedPublisherWithClient(dst string, opts S3OrderedOpts, client s3OrderedAPI) (*S3OrderedPublisher,error) {
    bucket,pattern,root,width,err:=parseS3OrderedDestination(dst)
    if err!=nil{return nil,err}
    opts,err=normalizeS3OrderedOpts(opts)
    if err!=nil{return nil,err}
    if client==nil{return nil,errors.New("S3 client is nil")}
    endpoint:=opts.Endpoint
    if endpoint=="" {endpoint="aws"}
    return &S3OrderedPublisher{
        client:client,bucket:bucket,pattern:pattern,root:root,width:width,opts:opts,
        identity:fmt.Sprintf("ordered-s3-v1|endpoint=%q|region=%q|bucket=%q|pattern=%q",endpoint,opts.Region,bucket,pattern),
    },nil
}

func (p *S3OrderedPublisher) filename(topic string, seq uint64) (key, relative string, err error) {
    if topic=="" || seq==0 || len(strconv.FormatUint(seq,10))>p.width {
        return "","",errors.New("invalid S3 topic or exhausted sequence width")
    }
    key=strings.Replace(p.pattern,"{topic}",orderedTopicName(topic),1)
    start:=strings.Index(key,"{seq:")
    end:=strings.Index(key[start:],"}")
    if end<0 {return "","",errors.New("invalid S3 key template")}
    key=key[:start]+fmt.Sprintf("%0*d",p.width,seq)+key[start+end+1:]
    relative=strings.TrimPrefix(key,p.root+"/")
    if !validS3OrderedRelative(relative) {return "","",errors.New("invalid S3 receipt path")}
    return key,relative,nil
}

func validS3OrderedRelative(rel string) bool {
    return rel!="" && rel!="." && !strings.HasPrefix(rel,"/") &&
        path.Clean(rel)==rel && !strings.HasPrefix(rel,"../") && rel!=".."
}

func s3OrderedNotFound(err error) bool {
    var api smithy.APIError
    if errors.As(err,&api) {
        return api.ErrorCode()=="NotFound" || api.ErrorCode()=="NoSuchKey" || api.ErrorCode()=="404"
    }
    return false
}

func (p *S3OrderedPublisher) inspect(ctx context.Context, key string) (s3OrderedObject,bool,error) {
    reqCtx,cancel:=context.WithTimeout(ctx,p.opts.RequestTimeout)
    defer cancel()
    out,err:=p.client.HeadObject(reqCtx,&s3.HeadObjectInput{
        Bucket:aws.String(p.bucket),Key:aws.String(key),ChecksumMode:types.ChecksumModeEnabled,
    })
    if err!=nil {
        if s3OrderedNotFound(err) {return s3OrderedObject{},false,nil}
        return s3OrderedObject{},false,fmt.Errorf("HEAD with checksum s3://%s/%s: %w",p.bucket,key,err)
    }
    if out==nil {return s3OrderedObject{},false,errors.New("empty S3 HEAD response")}
    info:=s3OrderedObject{
        size:aws.ToInt64(out.ContentLength),checksum:aws.ToString(out.ChecksumSHA256),
        checksumType:out.ChecksumType,
    }
    if info.size<=0 || info.checksum=="" ||
        (info.checksumType!=types.ChecksumTypeFullObject && info.checksumType!=types.ChecksumTypeComposite) {
        return s3OrderedObject{},false,fmt.Errorf("S3 object s3://%s/%s lacks server-validated SHA256 checksum and checksum type (S3 backend must support checksum-enabled HEAD)",p.bucket,key)
    }
    if info.checksumType==types.ChecksumTypeComposite {
        partSize,parseErr:=strconv.ParseInt(out.Metadata["ksink-part-size"],10,64)
        if parseErr!=nil || partSize<s3OrderedMinimumPartSize ||
            (info.size-1)/partSize+1>10000 {
            return s3OrderedObject{},false,fmt.Errorf("S3 object s3://%s/%s has invalid multipart part-size metadata",p.bucket,key)
        }
        info.partSize=partSize
    }
    return info,true,nil
}

func (p *S3OrderedPublisher) checksumForFile(file *os.File,size int64) (digest,checksum string,kind types.ChecksumType,err error) {
    partSize:=int64(0)
    kind=types.ChecksumTypeFullObject
    if size>=p.opts.MultipartPartSize {
        kind=types.ChecksumTypeComposite
        partSize=s3OrderedPartSize(size,p.opts.MultipartPartSize)
    }
    h:=newS3OrderedHasher(partSize)
    copied,err:=io.Copy(h,io.NewSectionReader(file,0,size))
    if err!=nil{return "","",kind,fmt.Errorf("hash local S3 publication spool: %w",err)}
    if copied!=size {return "","",kind,errors.New("local S3 publication spool was truncated")}
    digest,checksum=h.sums()
    return digest,checksum,kind,nil
}

func (p *S3OrderedPublisher) reconcile(ctx context.Context,key string,size int64,checksum string,kind types.ChecksumType) (bool,error) {
    info,exists,err:=p.inspect(ctx,key)
    if err!=nil{return false,err}
    if !exists{return false,nil}
    if err:=s3OrderedChecksumMatch(key,info,size,checksum,kind);err!=nil{return false,err}
    return true,nil
}

func (p *S3OrderedPublisher) publish(ctx context.Context,key string,file *os.File,size int64,digest string) error {
    if size<=0 || len(digest)!=sha256.Size*2 {return errors.New("invalid S3 object size/digest")}
    localDigest,checksum,kind,err:=p.checksumForFile(file,size)
    if err!=nil{return err}
    if localDigest!=digest {return errors.New("local S3 spool differs from committed WAL digest")}
    exists,err:=p.reconcile(ctx,key,size,checksum,kind)
    if err!=nil{return err}
    if exists{return nil}
    var uploadErr error
    if kind==types.ChecksumTypeFullObject {
        reqCtx,cancel:=context.WithTimeout(ctx,p.opts.RequestTimeout)
        _,uploadErr=p.client.PutObject(reqCtx,&s3.PutObjectInput{
            Bucket:aws.String(p.bucket),Key:aws.String(key),
            Body:io.NewSectionReader(file,0,size),ContentLength:aws.Int64(size),
            IfNoneMatch:aws.String("*"),
            ChecksumAlgorithm:types.ChecksumAlgorithmSha256,ChecksumSHA256:aws.String(checksum),
        })
        cancel()
    } else {
        uploadErr=p.multipart(ctx,key,file,size)
    }
    // Even a 200 response must be independently verified before a durable WAL
    // receipt. A compatible endpoint that silently ignores checksum inputs
    // must not be able to acknowledge bytes that differ from the WAL.
    exists,reconcileErr:=p.reconcile(ctx,key,size,checksum,kind)
    if reconcileErr!=nil{return errors.Join(uploadErr,reconcileErr)}
    if exists{return nil}
    if uploadErr!=nil{return fmt.Errorf("publish S3 object s3://%s/%s: %w",p.bucket,key,uploadErr)}
    return fmt.Errorf("S3 upload returned success but checksum-validated HEAD cannot find s3://%s/%s",p.bucket,key)
}

func (p *S3OrderedPublisher) multipart(ctx context.Context,key string,file *os.File,size int64) error {
    partSize:=s3OrderedPartSize(size,p.opts.MultipartPartSize)
    reqCtx,cancel:=context.WithTimeout(ctx,p.opts.RequestTimeout)
    started,err:=p.client.CreateMultipartUpload(reqCtx,&s3.CreateMultipartUploadInput{
        Bucket:aws.String(p.bucket),Key:aws.String(key),
        ChecksumAlgorithm:types.ChecksumAlgorithmSha256,ChecksumType:types.ChecksumTypeComposite,
        Metadata:map[string]string{"ksink-part-size":strconv.FormatInt(partSize,10)},
    })
    cancel()
    if err!=nil{return fmt.Errorf("initiate S3 multipart: %w",err)}
    if started==nil || aws.ToString(started.UploadId)=="" {
        return errors.New("S3 multipart initiation returned no upload ID")
    }
    uploadID:=started.UploadId
    completed:=false
    defer func(){
        if completed{return}
        cleanup,cancel:=context.WithTimeout(context.Background(),10*time.Second)
        defer cancel()
        _,_=p.client.AbortMultipartUpload(cleanup,&s3.AbortMultipartUploadInput{
            Bucket:aws.String(p.bucket),Key:aws.String(key),UploadId:uploadID,
        })
    }()
    parts:=make([]types.CompletedPart,0)
    for offset,number:=int64(0),int32(1);offset<size;offset,number=offset+partSize,number+1 {
        if err:=ctx.Err();err!=nil{return err}
        n:=partSize
        if size-offset<n {n=size-offset}
        partHash:=sha256.New()
        copied,err:=io.Copy(partHash,io.NewSectionReader(file,offset,n))
        if err!=nil{return fmt.Errorf("hash S3 part %d: %w",number,err)}
        if copied!=n {return fmt.Errorf("S3 part %d local spool truncated",number)}
        partChecksum:=base64.StdEncoding.EncodeToString(partHash.Sum(nil))
        reqCtx,cancel:=context.WithTimeout(ctx,p.opts.RequestTimeout)
        uploaded,partErr:=p.client.UploadPart(reqCtx,&s3.UploadPartInput{
            Bucket:aws.String(p.bucket),Key:aws.String(key),UploadId:uploadID,
            PartNumber:aws.Int32(number),ContentLength:aws.Int64(n),
            ChecksumSHA256:aws.String(partChecksum),
            Body:io.NewSectionReader(file,offset,n),
        })
        cancel()
        if partErr!=nil{return fmt.Errorf("upload S3 part %d: %w",number,partErr)}
        if uploaded==nil || aws.ToString(uploaded.ETag)=="" ||
            aws.ToString(uploaded.ChecksumSHA256)!=partChecksum {
            return fmt.Errorf("S3 part %d returned no ETag or a mismatched SHA256 checksum",number)
        }
        parts=append(parts,types.CompletedPart{
            PartNumber:aws.Int32(number),ETag:uploaded.ETag,ChecksumSHA256:aws.String(partChecksum),
        })
    }
    reqCtx,cancel=context.WithTimeout(ctx,p.opts.RequestTimeout)
    _,err=p.client.CompleteMultipartUpload(reqCtx,&s3.CompleteMultipartUploadInput{
        Bucket:aws.String(p.bucket),Key:aws.String(key),UploadId:uploadID,
        IfNoneMatch:aws.String("*"),
        MultipartUpload:&types.CompletedMultipartUpload{Parts:parts},
    })
    cancel()
    if err!=nil{return fmt.Errorf("complete S3 multipart: %w",err)}
    completed=true
    return nil
}

// NewS3OrderedTopicMaterializer uses the shared WAL to publish committed
// topic prefixes asynchronously. It never implements Kafka CommitTxn/AbortTxn.
func NewS3OrderedTopicMaterializer(wal *ksink.TransactionLog, dst string,
    opts S3OrderedOpts, maxBytes int64, maxAge time.Duration) (*OrderedTopicMaterializer,error) {
    p,err:=newS3OrderedPublisher(dst,opts)
    if err!=nil{return nil,err}
    return newS3OrderedTopicMaterializerWithPublisher(wal,p,maxBytes,maxAge)
}

func newS3OrderedTopicMaterializerWithPublisher(wal *ksink.TransactionLog,p *S3OrderedPublisher,
    maxBytes int64,maxAge time.Duration) (*OrderedTopicMaterializer,error) {
    if wal==nil || p==nil {return nil,errors.New("S3 ordered output requires WAL and publisher")}
    if maxBytes<0 || maxAge<0 || (maxBytes==0 && maxAge==0) {
        return nil,errors.New("ordered S3 output needs a positive rotation size or interval")
    }
    if err:=wal.BindOutputIdentity(p.identity);err!=nil{return nil,err}
    return &OrderedTopicMaterializer{
        wal:wal,s3:p,size:maxBytes,interval:maxAge,now:time.Now,
        since:make(map[string]time.Time),
    },nil
}

func (w *OrderedTopicMaterializer) verifyExistingS3(ctx context.Context,topic string,
    start,stable int64,key string,existing s3OrderedObject) (int64,int64,string,error) {
    if existing.size<=0 {return 0,0,"",fmt.Errorf("S3 object %q has invalid size",key)}
    h:=newS3OrderedHasher(existing.partSize)
    var total int64
    for pos:=start;pos<stable;pos++ {
        if err:=ctx.Err();err!=nil{return 0,0,"",err}
        record,found,err:=w.wal.ReadTopicRecord(topic,pos)
        if err!=nil{return 0,0,"",err}
        if found && w.wal.TopicTransactionDecision(record.Instance)=="commit" {
            if len(record.Data)==0 || int64(len(record.Data))>existing.size-total {
                return 0,0,"",fmt.Errorf("S3 object %q differs from committed WAL bytes",key)
            }
            _,_=h.Write(record.Data)
            total+=int64(len(record.Data))
        }
        if total==existing.size {
            digest,checksum:=h.sums()
            if err:=s3OrderedChecksumMatch(key,existing,total,checksum,
                map[bool]types.ChecksumType{true:types.ChecksumTypeComposite,false:types.ChecksumTypeFullObject}[existing.partSize>0]);err!=nil{
                return 0,0,"",err
            }
            return pos+1,total,digest,nil
        }
    }
    return 0,0,"",fmt.Errorf("S3 object %q length does not match a committed WAL prefix",key)
}

func (w *OrderedTopicMaterializer) publishTopicS3(ctx context.Context,topic string,force bool) (int,error) {
    p:=w.wal.TopicProgress(topic)
    stable:=w.wal.TopicStableOffset(topic)
    if p.Cursor>=stable {delete(w.since,topic);return 0,nil}
    key,relative,err:=w.s3.filename(topic,p.NextFile)
    if err!=nil{return 0,err}
    existing,exists,err:=w.s3.inspect(ctx,key)
    if err!=nil{return 0,err}
    end,total,visible,err:=w.scan(topic,p.Cursor,stable,w.currentLimit(),nil)
    if err!=nil{return 0,err}
    if !visible {
        if exists {return 0,fmt.Errorf("S3 final object %q exists without committed WAL data",key)}
        if err:=w.wal.SkipTopicOffsets(topic,end);err!=nil{return 0,err}
        delete(w.since,topic)
        return 1,nil
    }
    if !exists && !force && (w.size==0 || total<w.size) {
        since,ok:=w.since[topic]
        if !ok {w.since[topic]=w.now();return 0,nil}
        if w.interval==0 || w.now().Sub(since)<w.interval {return 0,nil}
    }

    digest:=""
    if exists {
        // A completed object may predate a crash before its WAL receipt.
        // Recover its actual prefix independently of today's rotation limit.
        end,total,digest,err=w.verifyExistingS3(ctx,topic,p.Cursor,stable,key,existing)
        if err!=nil{return 0,err}
    } else {
        // The WAL remains authoritative. A disposable spool keeps RAM bounded
        // even for multi-GiB objects and makes transport retries seekable.
        f,err:=os.CreateTemp("","ksink-ordered-s3-*.tmp")
        if err!=nil{return 0,fmt.Errorf("create S3 publication spool: %w",err)}
        defer func(){_ =os.Remove(f.Name())}()
        defer f.Close() //nolint:errcheck
        hash:=sha256.New()
        writer:=io.MultiWriter(f,hash)
        scannedEnd,scannedSize,_,err:=w.scan(topic,p.Cursor,stable,w.currentLimit(),func(data []byte)error{
            if err:=ctx.Err();err!=nil{return err}
            _,err:=writer.Write(data)
            return err
        })
        if err!=nil{return 0,err}
        if scannedEnd!=end || scannedSize!=total {
            return 0,errors.New("ordered S3 WAL prefix changed during publication")
        }
        digest=hex.EncodeToString(hash.Sum(nil))
        if err:=w.s3.publish(ctx,key,f,total,digest);err!=nil{return 0,err}
    }
    if err:=w.wal.RecordTopicFile(topic,ksink.WALTopicFile{
        Sequence:p.NextFile,Start:p.Cursor,End:end,Path:relative,Size:total,SHA256:digest,
    });err!=nil{return 0,err}
    delete(w.since,topic)
    return 1,nil
}
