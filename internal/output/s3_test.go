package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockS3Client struct {
	putObjects []mockPutObject
	mpCreates  []string
	mpUploads  []mockUploadPart
	mpDone     []string
	mpAborts   []string

	failPutAfter int
	putCalls     int
}

type mockPutObject struct {
	bucket string
	key    string
	body   string
}

type mockUploadPart struct {
	key        string
	partNumber int32
}

func (m *mockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.putCalls++
	if m.failPutAfter > 0 && m.putCalls >= m.failPutAfter {
		return nil, fmt.Errorf("put failed")
	}
	b, _ := io.ReadAll(params.Body)
	m.putObjects = append(m.putObjects, mockPutObject{
		bucket: aws.ToString(params.Bucket),
		key:    aws.ToString(params.Key),
		body:   string(b),
	})
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) CreateMultipartUpload(_ context.Context, params *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	m.mpCreates = append(m.mpCreates, aws.ToString(params.Key))
	uploadID := "upload-id"
	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (m *mockS3Client) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	m.mpUploads = append(m.mpUploads, mockUploadPart{
		key:        aws.ToString(params.Key),
		partNumber: aws.ToInt32(params.PartNumber),
	})
	etag := fmt.Sprintf("etag-%d", aws.ToInt32(params.PartNumber))
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(_ context.Context, params *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	m.mpDone = append(m.mpDone, aws.ToString(params.Key))
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3Client) AbortMultipartUpload(_ context.Context, params *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	m.mpAborts = append(m.mpAborts, aws.ToString(params.Key))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func TestParseS3Destination(t *testing.T) {
	bucket, key, err := parseS3Destination("s3://my-bucket/path/to/{topic}.jsonl")
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", bucket)
	assert.Equal(t, "path/to/{topic}.jsonl", key)
}

func TestParseS3DestinationRejectsUserinfo(t *testing.T) {
	_, _, err := parseS3Destination("s3://user@my-bucket/key.jsonl")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userinfo is not supported")
}

func TestParseS3DestinationRejectsQueryParams(t *testing.T) {
	_, _, err := parseS3Destination("s3://my-bucket/key.jsonl?x=y")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query parameters are not supported")
}

func TestParseS3DestinationRejectsFragment(t *testing.T) {
	_, _, err := parseS3Destination("s3://my-bucket/key.jsonl#section")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fragments are not supported")
}

func TestNewS3WriterRejectsTxnPlaceholder(t *testing.T) {
	_, err := NewS3Writer("s3://b/out-{txnID}.jsonl", S3Opts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "{txnID}")
}

func TestS3WriterFlush(t *testing.T) {
	client := &mockS3Client{}
	w := &s3Writer{
		client: client,
		opts: normalizeS3Opts(S3Opts{
			BatchMaxBytes:    100,
			BatchMaxMessages: 100,
		}),
		bucket:  "bucket",
		keyTpl:  "events/{topic}.jsonl",
		buffers: map[string]*s3Buffer{},
	}

	require.NoError(t, w.Write([]byte("hello\n"), &ksink.Message{Topic: "orders"}))
	require.NoError(t, w.Write([]byte("world\n"), &ksink.Message{Topic: "orders"}))
	require.NoError(t, w.Flush())
	require.Len(t, client.putObjects, 1)
	assert.Equal(t, "bucket", client.putObjects[0].bucket)
	assert.True(t, strings.HasPrefix(client.putObjects[0].key, "events/orders-"))
	assert.Equal(t, "hello\nworld\n", client.putObjects[0].body)
}

func TestS3WriterFlushesByThreshold(t *testing.T) {
	client := &mockS3Client{}
	w := &s3Writer{
		client: client,
		opts: normalizeS3Opts(S3Opts{
			BatchMaxBytes:    4,
			BatchMaxMessages: 100,
		}),
		bucket:  "bucket",
		keyTpl:  "events/{topic}.jsonl",
		buffers: map[string]*s3Buffer{},
	}

	require.NoError(t, w.Write([]byte("abcd"), &ksink.Message{Topic: "orders"}))
	require.Len(t, client.putObjects, 1)
}

func TestTxnS3WriterCommitPutObject(t *testing.T) {
	client := &mockS3Client{}
	w := &txnS3Writer{
		client: client,
		opts: normalizeS3Opts(S3Opts{
			MultipartPartSize: 10,
		}),
		bucket:  "bucket",
		keyTpl:  "events/{topic}-{txnID}.jsonl",
		txnData: map[string]map[string]*txnS3Buffer{},
	}

	require.NoError(t, w.Write([]byte("abc"), &ksink.Message{TransactionalID: "txn1", Topic: "orders"}))
	require.NoError(t, w.CommitTxn("txn1"))
	require.Len(t, client.putObjects, 1)
	assert.Equal(t, "events/orders-txn1.jsonl", client.putObjects[0].key)
	assert.Equal(t, "abc", client.putObjects[0].body)
}

func TestTxnS3WriterCommitMultipart(t *testing.T) {
	client := &mockS3Client{}
	w := &txnS3Writer{
		client: client,
		opts: normalizeS3Opts(S3Opts{
			MultipartPartSize: 5 * 1024 * 1024,
		}),
		bucket:  "bucket",
		keyTpl:  "events/{topic}-{txnID}.bin",
		txnData: map[string]map[string]*txnS3Buffer{},
	}

	largePayload := strings.Repeat("a", int(defaultS3MultipartPartSize)+3)
	require.NoError(t, w.Write([]byte(largePayload), &ksink.Message{TransactionalID: "txn1", Topic: "orders"}))
	require.NoError(t, w.CommitTxn("txn1"))
	require.Len(t, client.mpCreates, 1)
	require.Len(t, client.mpUploads, 2)
	require.Len(t, client.mpDone, 1)
}

func TestTxnS3WriterCommitPutObjectErrorIncludesBucketKey(t *testing.T) {
	client := &mockS3Client{failPutAfter: 1}
	w := &txnS3Writer{
		client:  client,
		opts:    normalizeS3Opts(S3Opts{RetryMaxAttempts: 1}),
		bucket:  "my-bucket",
		keyTpl:  "events/{topic}-{txnID}.jsonl",
		txnData: map[string]map[string]*txnS3Buffer{},
	}

	require.NoError(t, w.Write([]byte("data"), &ksink.Message{TransactionalID: "txn1", Topic: "orders"}))
	err := w.CommitTxn("txn1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3://my-bucket/")
}

func TestTxnS3WriterAbortTxn(t *testing.T) {
	client := &mockS3Client{}
	w := &txnS3Writer{
		client: client,
		opts:   normalizeS3Opts(S3Opts{}),
		bucket: "bucket",
		keyTpl: "events/{topic}-{txnID}.jsonl",
		txnData: map[string]map[string]*txnS3Buffer{
			"txn1": {
				"events/orders-txn1.jsonl": {},
			},
		},
	}
	require.NoError(t, w.AbortTxn("txn1"))
	require.NotContains(t, w.txnData, "txn1")
}

func TestResolveS3KeySanitizesPlaceholders(t *testing.T) {
	got := resolveS3Key("a/{topic}/{txnID}.jsonl", "../../evil", "../../topic")
	assert.Equal(t, "a/topic/evil.jsonl", got)
}

func TestWithS3Retry(t *testing.T) {
	opts := normalizeS3Opts(S3Opts{
		RetryMaxAttempts: 3,
		RetryDelay:       1,
	})
	attempts := 0
	err := withS3Retry(opts, func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("transient")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestDurableS3BufferRecoveryTruncatesToJournalOffset(t *testing.T) {
	root := t.TempDir()
	key := "events/orders.jsonl"
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:]) + ".data"
	active := filepath.Join(root, "active")
	require.NoError(t, os.MkdirAll(active, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(active, name), []byte("complete-partial"), 0600))
	meta := durableMeta{
		Bucket: "bucket", Key: key, KeyTpl: "events/{topic}.jsonl", ObjectKey: "events/orders-123-1.jsonl",
		Offset: 8, Count: 1,
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(active, name+".meta"), metaBytes, 0600))

	client := &mockS3Client{}
	b, err := newDurableS3Buffer(root, "bucket", "events/{topic}.jsonl", client,
		normalizeS3Opts(S3Opts{RetryMaxAttempts: 1}))
	require.NoError(t, err)
	require.NoError(t, b.close())
	require.Len(t, client.putObjects, 1)
	assert.Equal(t, "complete", client.putObjects[0].body)
}

func TestDurableS3BufferStartupFailsWithoutJournal(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "active"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "active", "orphan.data"), []byte("data"), 0600))
	_, err := newDurableS3Buffer(root, "bucket", "events/{topic}.jsonl", &mockS3Client{},
		normalizeS3Opts(S3Opts{RetryMaxAttempts: 1}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), ".meta")
}

var _ s3API = (*mockS3Client)(nil)
