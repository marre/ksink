package output

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// HTTPOpts holds configuration for the HTTP output writer.
type HTTPOpts struct {
	// MaxRetries is the number of retry attempts for failed HTTP requests.
	// Zero means no retries.
	MaxRetries int

	// RetryDelay is the delay between retry attempts.
	RetryDelay time.Duration

	// DLQPath is the file path for the dead-letter queue. Failed messages
	// are appended to this file when all retries are exhausted.
	// If empty, no DLQ is used and errors are returned to the caller.
	DLQPath string

	// Timeout is the HTTP client timeout per request. Zero uses the
	// default of 30 seconds.
	Timeout time.Duration
}

// httpWriter sends each message as an HTTP POST request body.
// It waits for a 200 OK before returning from Write.
type httpWriter struct {
	url    string
	client *http.Client
	opts   HTTPOpts

	mu  sync.Mutex
	dlq *os.File // lazily opened DLQ file
}

// NewHTTPWriter creates an HTTP output writer that POSTs messages to the
// given URL. It supports optional TLS configuration, retries, and a
// dead-letter queue for failed messages.
func NewHTTPWriter(url string, opts HTTPOpts, tlsCfg *tls.Config) (Writer, error) {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	transport := &http.Transport{}
	if tlsCfg != nil {
		transport.TLSClientConfig = tlsCfg
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
	return &httpWriter{
		url:    url,
		client: client,
		opts:   opts,
	}, nil
}

func (w *httpWriter) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var lastErr error
	attempts := 1 + w.opts.MaxRetries
	for i := range attempts {
		if err := w.doPost(data); err != nil {
			lastErr = err
			if i < attempts-1 {
				time.Sleep(w.opts.RetryDelay)
			}
			continue
		}
		return nil
	}

	// All attempts failed.
	if w.opts.DLQPath != "" {
		if err := w.writeDLQ(data); err != nil {
			return fmt.Errorf("HTTP POST to %s failed after %d attempt(s): %w; additionally, DLQ write to %s failed: %v", w.url, attempts, lastErr, w.opts.DLQPath, err)
		}
		return nil // written to DLQ, continue processing
	}
	return fmt.Errorf("HTTP POST to %s failed after %d attempt(s): %w", w.url, attempts, lastErr)
}

// doPost sends data as an HTTP POST and checks for 200 OK.
func (w *httpWriter) doPost(data []byte) error {
	resp, err := w.client.Post(w.url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused.
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	return nil
}

// writeDLQ appends a failed message to the dead-letter queue file.
func (w *httpWriter) writeDLQ(data []byte) error {
	if w.dlq == nil {
		f, err := os.OpenFile(w.opts.DLQPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("failed to open DLQ file: %w", err)
		}
		w.dlq = f
	}
	_, err := w.dlq.Write(data)
	return err
}

func (w *httpWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dlq != nil {
		return w.dlq.Close()
	}
	return nil
}
