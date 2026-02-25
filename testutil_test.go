package ksink

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testLogger is a simple logger that writes to testing.T.
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debugf(format string, args ...any) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Infof(format string, args ...any)  { l.t.Logf("[INFO] "+format, args...) }
func (l *testLogger) Warnf(format string, args ...any)  { l.t.Logf("[WARN] "+format, args...) }
func (l *testLogger) Errorf(format string, args ...any) { l.t.Logf("[ERROR] "+format, args...) }

// getFreePort finds an available TCP port.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// waitForTCPReady polls until the given TCP address is accepting connections.
func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// receivedMessage is used in tests to capture messages.
type receivedMessage struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// messageCapture captures messages.
type messageCapture struct {
	messages []receivedMessage
	mu       sync.Mutex
}

func (mc *messageCapture) add(msg receivedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.messages = append(mc.messages, msg)
}

func (mc *messageCapture) get() []receivedMessage {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	result := make([]receivedMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

func (mc *messageCapture) waitForMessages(count int, timeout time.Duration) []receivedMessage {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := mc.get()
		if len(msgs) >= count {
			return msgs
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mc.get()
}

// captureHandler creates a Handler that captures messages into a messageCapture.
func captureHandler(capture *messageCapture) Handler {
	return func(_ context.Context, msgs []*Message) error {
		for _, msg := range msgs {
			rm := receivedMessage{
				Topic:   msg.Topic,
				Value:   string(msg.Value),
				Headers: make(map[string]string),
			}
			if msg.Key != nil {
				rm.Key = string(msg.Key)
			}
			for k, v := range msg.Headers {
				rm.Headers[k] = v
			}
			capture.add(rm)
		}
		return nil
	}
}

// startTestServer creates and starts a server for testing, returning the server and its address.
func startTestServer(t *testing.T, cfg Config, handler Handler) (*Server, string) {
	t.Helper()

	port := getFreePort(t)
	cfg.Address = fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(cfg, handler, WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	t.Cleanup(func() {
		srv.Close(context.Background())
	})

	return srv, addr
}
