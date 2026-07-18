package output

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/marre/ksink/pkg/ksink"
)

const (
	defaultS3BatchMaxBytes     = 5 * 1024 * 1024
	defaultS3BatchMaxMessages  = 1000
	defaultS3RetryDelay        = 250 * time.Millisecond
	defaultS3RetryMaxAttempts  = 3
	defaultS3MultipartPartSize = 5 * 1024 * 1024
)

// S3Opts holds configuration for S3 output writers.
type S3Opts struct {
	Region         string
	Endpoint       string
	ForcePathStyle bool

	RetryMaxAttempts int
	RetryDelay       time.Duration

	BatchMaxBytes    int
	BatchMaxMessages int

	MultipartPartSize int64

	// BufferDir enables durable local buffering. Files are recovered and
	// synchronised before NewS3Writer returns.
	BufferDir string
	// BatchMaxAge rotates an active durable buffer after this duration.
	BatchMaxAge time.Duration
}

type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

type s3Buffer struct {
	data  bytes.Buffer
	count int
}

type s3Writer struct {
	client s3API
	opts   S3Opts

	bucket string
	keyTpl string

	mu      sync.Mutex
	buffers map[string]*s3Buffer // resolved key template -> buffered payload

	nextObjectID uint64
	local        *durableS3Buffer
}

type txnS3Buffer struct {
	data bytes.Buffer
}

type txnS3Writer struct {
	client s3API
	opts   S3Opts

	bucket string
	keyTpl string

	mu      sync.Mutex
	txnData map[string]map[string]*txnS3Buffer // txn -> resolved key -> payload
}

// NewS3Writer creates a non-transactional S3 writer that stores data as S3
// objects under the key pattern from dst.
func NewS3Writer(dst string, opts S3Opts) (Writer, error) {
	bucket, keyTpl, err := parseS3Destination(dst)
	if err != nil {
		return nil, err
	}
	if strings.Contains(keyTpl, TxnIDPlaceholder) {
		return nil, fmt.Errorf("non-transactional S3 output does not support %s placeholder", TxnIDPlaceholder)
	}
	opts = normalizeS3Opts(opts)
	client, err := newS3Client(opts)
	if err != nil {
		return nil, err
	}
	w := &s3Writer{
		client:  client,
		opts:    opts,
		bucket:  bucket,
		keyTpl:  keyTpl,
		buffers: make(map[string]*s3Buffer),
	}
	if opts.BufferDir != "" {
		local, err := newDurableS3Buffer(opts.BufferDir, bucket, keyTpl, client, opts)
		if err != nil {
			return nil, err
		}
		w.local = local
	}
	return w, nil
}

// NewTxnS3Writer creates a transactional S3 writer.
func NewTxnS3Writer(dst string, opts S3Opts) (TransactionalWriter, error) {
	bucket, keyTpl, err := parseS3Destination(dst)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(keyTpl, TxnIDPlaceholder) {
		return nil, fmt.Errorf("transactional S3 output pattern must contain %s placeholder", TxnIDPlaceholder)
	}
	opts = normalizeS3Opts(opts)
	client, err := newS3Client(opts)
	if err != nil {
		return nil, err
	}
	return &txnS3Writer{
		client:  client,
		opts:    opts,
		bucket:  bucket,
		keyTpl:  keyTpl,
		txnData: make(map[string]map[string]*txnS3Buffer),
	}, nil
}

func (w *s3Writer) Write(data []byte, msg *ksink.Message) error {
	if w.local != nil {
		return w.local.write(data, msg)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	key := resolveS3Key(w.keyTpl, "", topicFromMsg(msg))
	buf, ok := w.buffers[key]
	if !ok {
		buf = &s3Buffer{}
		w.buffers[key] = buf
	}

	if _, err := buf.data.Write(data); err != nil {
		return fmt.Errorf("failed to buffer S3 payload: %w", err)
	}
	buf.count++
	if buf.data.Len() >= w.opts.BatchMaxBytes || buf.count >= w.opts.BatchMaxMessages {
		return w.flushKeyLocked(key, buf)
	}
	return nil
}

func (w *s3Writer) Flush() error {
	if w.local != nil {
		return w.local.flush()
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	for key, buf := range w.buffers {
		if buf.data.Len() == 0 {
			continue
		}
		if err := w.flushKeyLocked(key, buf); err != nil {
			return err
		}
	}
	return nil
}

func (w *s3Writer) Close() error {
	if w.local != nil {
		return w.local.close()
	}
	return w.Flush()
}

func (w *s3Writer) flushKeyLocked(key string, buf *s3Buffer) error {
	if buf.data.Len() == 0 {
		return nil
	}
	w.nextObjectID++
	objectKey := addS3BatchSuffix(key, w.nextObjectID)
	payload := append([]byte(nil), buf.data.Bytes()...)
	if err := withS3Retry(w.opts, func() error {
		_, err := w.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(payload),
		})
		return err
	}); err != nil {
		return fmt.Errorf("failed to put S3 object s3://%s/%s: %w", w.bucket, objectKey, err)
	}
	buf.count = 0
	buf.data.Reset()
	delete(w.buffers, key)
	return nil
}

func (w *txnS3Writer) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if msg == nil || msg.TransactionalID == "" {
		return fmt.Errorf("transactional S3 writer requires a TransactionalID on every message")
	}
	txnID := msg.TransactionalID
	key := resolveS3Key(w.keyTpl, txnID, topicFromMsg(msg))

	byKey, ok := w.txnData[txnID]
	if !ok {
		byKey = make(map[string]*txnS3Buffer)
		w.txnData[txnID] = byKey
	}
	buf, ok := byKey[key]
	if !ok {
		buf = &txnS3Buffer{}
		byKey[key] = buf
	}

	if _, err := buf.data.Write(data); err != nil {
		return fmt.Errorf("failed to buffer transactional S3 payload: %w", err)
	}
	return nil
}

func (w *txnS3Writer) CommitTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	byKey, ok := w.txnData[txnID]
	if !ok {
		return nil
	}

	for key, data := range byKey {
		payload := append([]byte(nil), data.data.Bytes()...)
		if int64(len(payload)) < w.opts.MultipartPartSize {
			if err := w.putObject(key, payload); err != nil {
				return err
			}
			continue
		}
		if err := w.multipartPutObject(key, payload); err != nil {
			return err
		}
	}

	delete(w.txnData, txnID)
	return nil
}

func (w *txnS3Writer) AbortTxn(txnID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.txnData, txnID)
	return nil
}

func (w *txnS3Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txnData = nil
	return nil
}

func (w *txnS3Writer) putObject(key string, payload []byte) error {
	if err := withS3Retry(w.opts, func() error {
		_, err := w.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(payload),
		})
		return err
	}); err != nil {
		return fmt.Errorf("failed to put S3 object s3://%s/%s: %w", w.bucket, key, err)
	}
	return nil
}

func (w *txnS3Writer) multipartPutObject(key string, payload []byte) error {
	var uploadID *string
	if err := withS3Retry(w.opts, func() error {
		createOut, err := w.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		uploadID = createOut.UploadId
		return nil
	}); err != nil {
		return fmt.Errorf("failed to start multipart upload for s3://%s/%s: %w", w.bucket, key, err)
	}

	parts := make([]types.CompletedPart, 0, (len(payload)+int(w.opts.MultipartPartSize)-1)/int(w.opts.MultipartPartSize))

	for partNum, start := int32(1), 0; start < len(payload); partNum, start = partNum+1, start+int(w.opts.MultipartPartSize) {
		end := start + int(w.opts.MultipartPartSize)
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[start:end]
		pn := partNum
		var completedPart types.CompletedPart
		if err := withS3Retry(w.opts, func() error {
			upOut, err := w.client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     aws.String(w.bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(pn),
				Body:       bytes.NewReader(chunk),
			})
			if err != nil {
				return err
			}
			completedPart = types.CompletedPart{
				ETag:       upOut.ETag,
				PartNumber: aws.Int32(pn),
			}
			return nil
		}); err != nil {
			_, _ = w.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(w.bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			return fmt.Errorf("failed to upload multipart part %d for s3://%s/%s: %w", pn, w.bucket, key, err)
		}
		parts = append(parts, completedPart)
	}

	if err := withS3Retry(w.opts, func() error {
		_, err := w.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(w.bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: parts,
			},
		})
		return err
	}); err != nil {
		_, _ = w.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(w.bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
		})
		return fmt.Errorf("failed to complete multipart upload for s3://%s/%s: %w", w.bucket, key, err)
	}

	return nil
}

func normalizeS3Opts(opts S3Opts) S3Opts {
	if opts.BatchMaxBytes <= 0 {
		opts.BatchMaxBytes = defaultS3BatchMaxBytes
	}
	if opts.BatchMaxMessages <= 0 {
		opts.BatchMaxMessages = defaultS3BatchMaxMessages
	}
	if opts.RetryMaxAttempts <= 0 {
		opts.RetryMaxAttempts = defaultS3RetryMaxAttempts
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = defaultS3RetryDelay
	}
	if opts.MultipartPartSize <= 0 {
		opts.MultipartPartSize = defaultS3MultipartPartSize
	} else if opts.MultipartPartSize < defaultS3MultipartPartSize {
		opts.MultipartPartSize = defaultS3MultipartPartSize
	}
	if opts.BatchMaxAge > 0 && opts.BatchMaxAge < durableMinBatchAge {
		opts.BatchMaxAge = durableMinBatchAge
	}
	return opts
}

func newS3Client(opts S3Opts) (s3API, error) {
	loadOpts := make([]func(*config.LoadOptions) error, 0, 1)
	if opts.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(opts.Region))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = opts.ForcePathStyle
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	}), nil
}

func parseS3Destination(dst string) (bucket, key string, err error) {
	u, err := url.Parse(dst)
	if err != nil {
		return "", "", fmt.Errorf("invalid S3 destination %q: %w", dst, err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid S3 destination scheme %q", u.Scheme)
	}
	if u.User != nil {
		return "", "", fmt.Errorf("invalid S3 destination %q: userinfo is not supported", dst)
	}
	if u.RawQuery != "" {
		return "", "", fmt.Errorf("invalid S3 destination %q: query parameters are not supported", dst)
	}
	if u.Fragment != "" {
		return "", "", fmt.Errorf("invalid S3 destination %q: fragments are not supported", dst)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("missing S3 bucket in %q", dst)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("missing S3 key pattern in %q", dst)
	}
	return u.Host, key, nil
}

func resolveS3Key(pattern, txnID, topic string) string {
	key := strings.ReplaceAll(pattern, TxnIDPlaceholder, SanitizePathSegment(txnID))
	key = strings.ReplaceAll(key, TopicPlaceholder, SanitizePathSegment(topic))
	return strings.TrimPrefix(path.Clean("/"+key), "/")
}

func topicFromMsg(msg *ksink.Message) string {
	if msg == nil {
		return ""
	}
	return msg.Topic
}

func addS3BatchSuffix(key string, seq uint64) string {
	ext := path.Ext(key)
	base := strings.TrimSuffix(key, ext)
	return fmt.Sprintf("%s-%d-%d%s", base, time.Now().UnixNano(), seq, ext)
}

func withS3Retry(opts S3Opts, fn func() error) error {
	var lastErr error
	for i := 0; i < opts.RetryMaxAttempts; i++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if i < opts.RetryMaxAttempts-1 {
			time.Sleep(opts.RetryDelay)
		}
	}
	return lastErr
}

var _ Writer = (*s3Writer)(nil)
var _ Flusher = (*s3Writer)(nil)
var _ TransactionalWriter = (*txnS3Writer)(nil)
var _ io.Closer = (*txnS3Writer)(nil)
