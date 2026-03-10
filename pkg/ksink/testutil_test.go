package ksink

import (
	"context"
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
	require.NoError(t, l.Close())
	return port
}

// waitForTCPReady polls until the given TCP address is accepting connections.
func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close() //nolint:errcheck
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

// startReadLoop starts a goroutine that reads batches from the server and
// captures messages. It acknowledges each batch with nil (success).
func startReadLoop(t *testing.T, srv *Server) *messageCapture {
	t.Helper()
	capture := &messageCapture{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
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
			ack(nil)
		}
	}()

	return capture
}
