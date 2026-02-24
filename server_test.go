package ksrv_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/marre/ksrv"
)

// startDrainer starts a goroutine that drains all batches from srv,
// acknowledging each one.  It returns a thread-safe function that returns all
// collected messages so far.
func startDrainer(t *testing.T, ctx context.Context, srv *ksrv.Server) func() []ksrv.MessageBatch {
	t.Helper()
	var (
		mu      sync.Mutex
		batches []ksrv.MessageBatch
	)
	go func() {
		for {
			batch, ackFn, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
			_ = ackFn(ctx, nil)
		}
	}()
	return func() []ksrv.MessageBatch {
		mu.Lock()
		defer mu.Unlock()
		result := make([]ksrv.MessageBatch, len(batches))
		copy(result, batches)
		return result
	}
}

// waitForBatches waits until at least n batches have been collected.
func waitForBatches(t *testing.T, get func() []ksrv.MessageBatch, n int, timeout time.Duration) []ksrv.MessageBatch {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := get(); len(got) >= n {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	return get()
}

// startServer creates and connects a server on the given address, registering
// cleanup with t.
func startServer(t *testing.T, opts ksrv.Options) *ksrv.Server {
	t.Helper()
	srv, err := ksrv.New(opts)
	require.NoError(t, err)
	require.NoError(t, srv.Connect(context.Background()))
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	return srv
}

// newKgoClient returns a franz-go client connected to addr.
func newKgoClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	all := append([]kgo.Opt{kgo.SeedBrokers(addr)}, opts...)
	cl, err := kgo.NewClient(all...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

func TestServer_BasicProduceConsume(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := startServer(t, ksrv.Options{Address: addr, Timeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	getBatches := startDrainer(t, ctx, srv)

	waitForTCPReady(t, addr, 3*time.Second)

	cl := newKgoClient(t, addr)
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "unit-test-topic",
		Value: []byte(`{"hello":"world"}`),
	})
	for _, r := range results {
		require.NoError(t, r.Err)
	}

	got := waitForBatches(t, getBatches, 1, 5*time.Second)
	require.Len(t, got, 1)
	require.Len(t, got[0], 1)
	assert.Equal(t, "unit-test-topic", got[0][0].Topic)
	assert.Equal(t, `{"hello":"world"}`, string(got[0][0].Value))
}

func TestServer_ReadBatchBeforeConnect(t *testing.T) {
	srv, err := ksrv.New(ksrv.Options{Address: "127.0.0.1:0", Timeout: time.Second})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err = srv.ReadBatch(ctx)
	assert.ErrorIs(t, err, ksrv.ErrNotConnected)
}

func TestServer_CloseBeforeConnect(t *testing.T) {
	srv, err := ksrv.New(ksrv.Options{Address: "127.0.0.1:0", Timeout: time.Second})
	require.NoError(t, err)
	assert.NoError(t, srv.Close(context.Background()))
}

func TestServer_DoubleClose(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := startServer(t, ksrv.Options{Address: addr, Timeout: 5 * time.Second})
	ctx := context.Background()
	require.NoError(t, srv.Close(ctx))
	require.NoError(t, srv.Close(ctx)) // second close must be a no-op
}

func TestServer_AllowedTopics(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := startServer(t, ksrv.Options{
		Address:       addr,
		AllowedTopics: []string{"allowed-topic"},
		Timeout:       5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	getBatches := startDrainer(t, ctx, srv)

	waitForTCPReady(t, addr, 3*time.Second)

	cl := newKgoClient(t, addr)
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "allowed-topic",
		Value: []byte("hello"),
	})
	for _, r := range results {
		require.NoError(t, r.Err)
	}

	got := waitForBatches(t, getBatches, 1, 5*time.Second)
	require.Len(t, got, 1)
	assert.Equal(t, "allowed-topic", got[0][0].Topic)
}

func TestServer_MessageMetadata(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := startServer(t, ksrv.Options{Address: addr, Timeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	getBatches := startDrainer(t, ctx, srv)

	waitForTCPReady(t, addr, 3*time.Second)

	cl := newKgoClient(t, addr)
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "meta-topic",
		Key:   []byte("my-key"),
		Value: []byte("my-value"),
		Headers: []kgo.RecordHeader{
			{Key: "x-header", Value: []byte("header-value")},
		},
	})
	for _, r := range results {
		require.NoError(t, r.Err)
	}

	got := waitForBatches(t, getBatches, 1, 5*time.Second)
	require.Len(t, got, 1)
	require.Len(t, got[0], 1)

	msg := got[0][0]
	assert.Equal(t, "meta-topic", msg.Topic)
	assert.Equal(t, []byte("my-key"), msg.Key)
	assert.Equal(t, []byte("my-value"), msg.Value)
	assert.Equal(t, "header-value", msg.Headers["x-header"])
	assert.NotEmpty(t, msg.ClientAddress)
}

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
