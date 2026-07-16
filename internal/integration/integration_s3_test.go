package ksink_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3WriterMinIO(t *testing.T) {
	checkDockerIntegration(t)

	ctx := context.Background()
	client, endpoint, bucket := startMinIOForTest(t)
	require.NoError(t, createBucket(ctx, client, bucket))

	w, err := output.NewS3Writer(fmt.Sprintf("s3://%s/events/{topic}.jsonl", bucket), output.S3Opts{
		Region:           "us-east-1",
		Endpoint:         endpoint,
		ForcePathStyle:   true,
		BatchMaxBytes:    1024,
		BatchMaxMessages: 100,
	})
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	require.NoError(t, w.Write([]byte("hello\n"), &ksink.Message{Topic: "orders"}))
	require.NoError(t, w.Write([]byte("world\n"), &ksink.Message{Topic: "orders"}))
	require.NoError(t, w.Close())

	key, body := firstObjectWithPrefix(t, ctx, client, bucket, "events/orders-")
	assert.True(t, strings.HasPrefix(key, "events/orders-"))
	assert.Equal(t, "hello\nworld\n", body)
}

func TestTxnS3WriterMinIOCommitAbort(t *testing.T) {
	checkDockerIntegration(t)

	ctx := context.Background()
	client, endpoint, bucket := startMinIOForTest(t)
	require.NoError(t, createBucket(ctx, client, bucket))

	w, err := output.NewTxnS3Writer(fmt.Sprintf("s3://%s/events/{topic}-{txnID}.jsonl", bucket), output.S3Opts{
		Region:            "us-east-1",
		Endpoint:          endpoint,
		ForcePathStyle:    true,
		MultipartPartSize: 5 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	require.NoError(t, w.Write([]byte("commit-data\n"), &ksink.Message{TransactionalID: "txn1", Topic: "orders"}))
	require.NoError(t, w.CommitTxn("txn1"))

	require.NoError(t, w.Write([]byte("abort-data\n"), &ksink.Message{TransactionalID: "txn2", Topic: "orders"}))
	require.NoError(t, w.AbortTxn("txn2"))

	key, body := firstObjectWithPrefix(t, ctx, client, bucket, "events/orders-txn1")
	assert.Equal(t, "events/orders-txn1.jsonl", key)
	assert.Equal(t, "commit-data\n", body)

	listOut, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("events/orders-txn2"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Contents, 0)
}

func startMinIOForTest(t *testing.T) (*awss3.Client, string, string) {
	t.Helper()

	const (
		accessKey = "minioadmin"
		secretKey = "minioadmin"
		region    = "us-east-1"
	)
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("AWS_REGION", region)

	pool := newDockerPool(t)
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "minio/minio",
		Tag:          "RELEASE.2025-09-07T16-13-09Z",
		Cmd:          []string{"server", "/data", "--address", ":9000"},
		ExposedPorts: []string{"9000/tcp"},
		Env: []string{
			"MINIO_ROOT_USER=" + accessKey,
			"MINIO_ROOT_PASSWORD=" + secretKey,
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Purge(resource)
	})

	port := resource.GetPort("9000/tcp")
	endpoint := "http://127.0.0.1:" + port

	httpClient := &http.Client{Timeout: 2 * time.Second}
	err = pool.Retry(func() error {
		resp, reqErr := httpClient.Get(endpoint + "/minio/health/live")
		if reqErr != nil {
			return reqErr
		}
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("minio not ready: status=%d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	require.NoError(t, err)
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return client, endpoint, "ksink-test"
}

func createBucket(ctx context.Context, client *awss3.Client, bucket string) error {
	_, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	return err
}

func firstObjectWithPrefix(t *testing.T, ctx context.Context, client *awss3.Client, bucket, prefix string) (string, string) {
	t.Helper()
	out, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Contents, "expected object with prefix %s", prefix)

	key := aws.ToString(out.Contents[0].Key)
	obj, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer obj.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	return key, string(body)
}
