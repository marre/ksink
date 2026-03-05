package output_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marre/ksink/internal/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var received []string
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.Open(ts.URL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	verifyMessages(t, received, 3)
}

func TestHTTPWriteSuccess(t *testing.T) {
	var bodies []string
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{}, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("hello")))
	require.NoError(t, w.Write([]byte("world")))
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 2)
	assert.Equal(t, "hello", bodies[0])
	assert.Equal(t, "world", bodies[1])
}

func TestHTTPWriteNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{}, nil)
	require.NoError(t, err)

	err = w.Write([]byte("msg"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	require.NoError(t, w.Close())
}

func TestHTTPWriteRetry(t *testing.T) {
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{
		MaxRetries: 5,
		RetryDelay: 10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("msg")))
	assert.Equal(t, int32(3), attempts.Load())
	require.NoError(t, w.Close())
}

func TestHTTPWriteRetryExhausted(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)

	err = w.Write([]byte("msg"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "3 attempt(s)")
	require.NoError(t, w.Close())
}

func TestHTTPWriteDLQ(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	dlqPath := filepath.Join(t.TempDir(), "dlq.log")

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
		DLQPath:    dlqPath,
	}, nil)
	require.NoError(t, err)

	// Write should succeed because the message is sent to DLQ.
	require.NoError(t, w.Write([]byte("failed-msg-1\n")))
	require.NoError(t, w.Write([]byte("failed-msg-2\n")))
	require.NoError(t, w.Close())

	data, err := os.ReadFile(dlqPath)
	require.NoError(t, err)
	assert.Equal(t, "failed-msg-1\nfailed-msg-2\n", string(data))
}

func TestHTTPWriteRetryThenDLQ(t *testing.T) {
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer ts.Close()

	dlqPath := filepath.Join(t.TempDir(), "dlq.log")

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
		DLQPath:    dlqPath,
	}, nil)
	require.NoError(t, err)

	// All 3 attempts (1 + 2 retries) should fail, then go to DLQ.
	require.NoError(t, w.Write([]byte("msg")))
	assert.Equal(t, int32(3), attempts.Load())
	require.NoError(t, w.Close())

	data, err := os.ReadFile(dlqPath)
	require.NoError(t, err)
	assert.Equal(t, "msg", string(data))
}

func TestHTTPWriteConnectionError(t *testing.T) {
	// Use an address that is not listening with a short timeout to avoid slow tests.
	w, err := output.NewHTTPWriter("http://127.0.0.1:1", output.HTTPOpts{
		Timeout: 1 * time.Second,
	}, nil)
	require.NoError(t, err)

	err = w.Write([]byte("msg"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP POST")
	require.NoError(t, w.Close())
}

func TestHTTPWriteNegativeRetries(t *testing.T) {
	// Negative MaxRetries should be clamped to 0 (single attempt, no retries).
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{MaxRetries: -5}, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("msg")))
	assert.Equal(t, int32(1), attempts.Load())
	require.NoError(t, w.Close())
}

func TestHTTPWriteContentType(t *testing.T) {
	var contentType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{}, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("msg")))
	assert.Equal(t, "application/octet-stream", contentType)
	require.NoError(t, w.Close())
}

func TestHTTPOpenViaURL(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Verify that Open recognizes http:// URLs.
	w, err := output.Open(ts.URL, nil)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NoError(t, w.Write([]byte("test")))
	require.NoError(t, w.Close())
}

func TestHTTPOpenViaHTTPSURL(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Use the test server's TLS client to trust its self-signed cert.
	tlsCfg := ts.TLS.Clone()
	tlsCfg.InsecureSkipVerify = true //nolint:gosec // Test only

	w, err := output.Open(ts.URL, tlsCfg)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NoError(t, w.Write([]byte("test")))
	require.NoError(t, w.Close())
}

func TestHTTPWriteSequential(t *testing.T) {
	// Verify messages are sent one at a time (sequentially).
	var inflight atomic.Int32
	var maxInflight atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		// Track max concurrent requests.
		for {
			cur := maxInflight.Load()
			if n <= cur || maxInflight.CompareAndSwap(cur, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w, err := output.NewHTTPWriter(ts.URL, output.HTTPOpts{}, nil)
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, w.Write([]byte(fmt.Sprintf("msg-%d", i))))
	}
	require.NoError(t, w.Close())

	// All writes are serialized under the mutex, so max inflight should be 1.
	assert.Equal(t, int32(1), maxInflight.Load())
}
