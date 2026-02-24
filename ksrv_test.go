package ksrv

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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

func TestServerConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid minimal config",
			cfg: Config{
				Address: "127.0.0.1:0",
			},
		},
		{
			name: "valid config with topics",
			cfg: Config{
				Address: "127.0.0.1:0",
				Topics:  []string{"test-topic", "events"},
			},
		},
		{
			name: "valid config with timeout",
			cfg: Config{
				Address:         "127.0.0.1:0",
				Timeout:         10 * time.Second,
				MaxMessageBytes: 2097152,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, func(context.Context, []*Message) error { return nil })
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestServerBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}

	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, captureHandler(capture))

	// Create a franz-go producer client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	testTopic := "test-topic"
	testKey := "test-key"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Key:   []byte(testKey),
		Value: []byte(testValue),
		Headers: []kgo.RecordHeader{
			{Key: "header1", Value: []byte("value1")},
		},
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)

	assert.Equal(t, testTopic, msgs[0].Topic)
	assert.Equal(t, testKey, msgs[0].Key)
	assert.Equal(t, testValue, msgs[0].Value)
	assert.Equal(t, "value1", msgs[0].Headers["header1"])
}

func TestServerMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	for i := 0; i < 5; i++ {
		record := &kgo.Record{
			Topic: "test-topic",
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results[0].Err)
	}

	msgs := capture.waitForMessages(5, 10*time.Second)
	require.Len(t, msgs, 5)
}

func TestServerTopicFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		Topics:  []string{"allowed-topic"},
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce to allowed topic
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "allowed-topic",
		Value: []byte("allowed"),
	})
	require.NoError(t, results[0].Err)

	// Produce to disallowed topic should fail
	results = client.ProduceSync(ctx, &kgo.Record{
		Topic: "disallowed-topic",
		Value: []byte("disallowed"),
	})
	require.Error(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "allowed-topic", msgs[0].Topic)
	assert.Equal(t, "allowed", msgs[0].Value)
}

func TestServerSASLPlain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	}, captureHandler(capture))

	// With correct credentials
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{
			User: "user1",
			Pass: "pass1",
		}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("authed-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "authed-message", msgs[0].Value)
}

func TestServerSASLPlainWrongPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{
			User: "user1",
			Pass: "wrongpass",
		}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err)

	// Should not have received any messages
	time.Sleep(500 * time.Millisecond)
	msgs := capture.get()
	assert.Len(t, msgs, 0)
}

func TestServerSASLScram256(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "scrampass",
		}.AsSha256Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("scram-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "scram-message", msgs[0].Value)
}

func TestServerSASLScram512(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-512", Username: "scramuser", Password: "scrampass"},
		},
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "scrampass",
		}.AsSha512Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("scram512-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "scram512-message", msgs[0].Value)
}

func TestServerSASLScramWrongPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "wrongpass",
		}.AsSha256Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err)

	time.Sleep(500 * time.Millisecond)
	msgs := capture.get()
	assert.Len(t, msgs, 0)
}

func TestServerMultipleUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
			{Mechanism: "PLAIN", Username: "user2", Password: "pass2"},
		},
	}, captureHandler(capture))

	// User1
	client1, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{User: "user1", Pass: "pass1"}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client1.Close()

	results := client1.ProduceSync(ctx, &kgo.Record{
		Topic: "user1-topic",
		Value: []byte(`{"user": "user1"}`),
	})
	require.NoError(t, results[0].Err)

	// User2
	client2, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{User: "user2", Pass: "pass2"}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client2.Close()

	results = client2.ProduceSync(ctx, &kgo.Record{
		Topic: "user2-topic",
		Value: []byte(`{"user": "user2"}`),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(2, 10*time.Second)
	require.Len(t, msgs, 2)
}

func TestServerIdempotentProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout:         5 * time.Second,
		IdempotentWrite: true,
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "idempotent-topic",
		Value: []byte("idempotent-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "idempotent-message", msgs[0].Value)
}

func TestServerMessageWithKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "key-topic",
		Key:   []byte("my-key"),
		Value: []byte(`{"test": "with_key"}`),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "key-topic", msgs[0].Topic)
	assert.Equal(t, "my-key", msgs[0].Key)
	assert.Equal(t, `{"test": "with_key"}`, msgs[0].Value)
}

func TestServerProtocolApiVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, func(context.Context, []*Message) error { return nil })

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Send ApiVersions request (apiKey=18, version=0)
	req := &kmsg.ApiVersionsRequest{Version: 0}
	reqBuf := req.AppendTo(nil)

	// Build request: apiKey(2) + apiVersion(2) + correlationID(4) + clientID(2 + len)
	header := make([]byte, 0)
	header = kbin.AppendInt16(header, int16(kmsg.ApiVersions))
	header = kbin.AppendInt16(header, 0) // version
	header = kbin.AppendInt32(header, 1) // correlationID
	header = kbin.AppendInt16(header, -1) // no clientID

	fullReq := append(header, reqBuf...)

	// Write size prefix + request
	sizeBuf := kbin.AppendInt32(nil, int32(len(fullReq)))
	_, err = conn.Write(append(sizeBuf, fullReq...))
	require.NoError(t, err)

	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)

	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)

	// Parse: correlationID(4) + response body
	require.True(t, len(respBuf) >= 4)
	corrID := int32(binary.BigEndian.Uint32(respBuf[:4]))
	assert.Equal(t, int32(1), corrID)

	_ = ctx
}

func TestServerClose(t *testing.T) {
	capture := &messageCapture{}
	srv, _ := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, captureHandler(capture))

	// Close should not error
	err := srv.Close(context.Background())
	assert.NoError(t, err)

	// Double close should not error
	err = srv.Close(context.Background())
	assert.NoError(t, err)
}

func TestServerTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate CA certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Create server certificate
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Write certificates to temp files
	tmpDir := t.TempDir()

	serverCertFile := tmpDir + "/server-cert.pem"
	require.NoError(t, os.WriteFile(serverCertFile, serverCertPEM, 0600))

	serverKeyFile := tmpDir + "/server-key.pem"
	require.NoError(t, os.WriteFile(serverKeyFile, serverKeyPEM, 0600))

	capture := &messageCapture{}
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:  addr,
		CertFile: serverCertFile,
		KeyFile:  serverKeyFile,
		Timeout:  5 * time.Second,
	}, captureHandler(capture), WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	waitForTCPReady(t, addr, 5*time.Second)

	// Connect with TLS
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTLSConfig(tlsConfig),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "tls-topic",
		Value: []byte("tls-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "tls-message", msgs[0].Value)
}

func TestServerMTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Server cert
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Client cert
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			CommonName:   "test-client",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, &caTemplate, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	// Write certs to temp files
	tmpDir := t.TempDir()
	serverCertFile := tmpDir + "/server-cert.pem"
	require.NoError(t, os.WriteFile(serverCertFile, serverCertPEM, 0600))
	serverKeyFile := tmpDir + "/server-key.pem"
	require.NoError(t, os.WriteFile(serverKeyFile, serverKeyPEM, 0600))
	clientCAFile := tmpDir + "/client-ca.pem"
	require.NoError(t, os.WriteFile(clientCAFile, caCertPEM, 0600))

	capture := &messageCapture{}
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:      addr,
		CertFile:     serverCertFile,
		KeyFile:      serverKeyFile,
		MTLSAuth:     "require_and_verify",
		MTLSCAsFiles: []string{clientCAFile},
		Timeout:      5 * time.Second,
	}, captureHandler(capture), WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	waitForTCPReady(t, addr, 5*time.Second)

	// Test: Client with valid certificate should work
	t.Run("valid_client_cert", func(t *testing.T) {
		clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		require.NoError(t, err)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.DialTLSConfig(tlsConfig),
			kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		)
		require.NoError(t, err)
		defer client.Close()

		results := client.ProduceSync(ctx, &kgo.Record{
			Topic: "mtls-topic",
			Value: []byte("mtls-message"),
		})
		require.NoError(t, results[0].Err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		assert.Equal(t, "mtls-message", msgs[0].Value)
	})

	// Test: Client without certificate should be rejected
	t.Run("no_client_cert", func(t *testing.T) {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.DialTLSConfig(tlsConfig),
			kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		)
		require.NoError(t, err)
		defer client.Close()

		results := client.ProduceSync(ctx, &kgo.Record{
			Topic: "mtls-fail-topic",
			Value: []byte("should-fail"),
		})
		require.Error(t, results[0].Err)
	})
}

func TestServerMaxMessageBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capture := &messageCapture{}
	_, addr := startTestServer(t, Config{
		Timeout:         5 * time.Second,
		MaxMessageBytes: 100,
	}, captureHandler(capture))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	// Small message should work
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("small"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
}

func TestServerAdvertisedAddress(t *testing.T) {
	capture := &messageCapture{}
	port := getFreePort(t)

	cfg := Config{
		Address:           fmt.Sprintf("127.0.0.1:%d", port),
		AdvertisedAddress: fmt.Sprintf("myhost.local:%d", port),
		Timeout:           5 * time.Second,
	}

	srv, err := New(cfg, captureHandler(capture), WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	// Connect and verify via raw protocol that the advertised address is returned
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Send ApiVersions request
	req := &kmsg.ApiVersionsRequest{Version: 0}
	reqBuf := req.AppendTo(nil)

	header := make([]byte, 0)
	header = kbin.AppendInt16(header, int16(kmsg.ApiVersions))
	header = kbin.AppendInt16(header, 0)
	header = kbin.AppendInt32(header, 1)
	header = kbin.AppendInt16(header, -1)
	fullReq := append(header, reqBuf...)
	sizeBuf := kbin.AppendInt32(nil, int32(len(fullReq)))
	_, err = conn.Write(append(sizeBuf, fullReq...))
	require.NoError(t, err)

	// Read ApiVersions response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)
	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)

	// Send Metadata request
	metadataReq := &kmsg.MetadataRequest{Version: 1}
	metadataReqBuf := metadataReq.AppendTo(nil)

	header2 := make([]byte, 0)
	header2 = kbin.AppendInt16(header2, int16(kmsg.Metadata))
	header2 = kbin.AppendInt16(header2, 1) // version 1
	header2 = kbin.AppendInt32(header2, 2) // correlationID
	header2 = kbin.AppendInt16(header2, -1) // no clientID
	fullReq2 := append(header2, metadataReqBuf...)
	sizeBuf2 := kbin.AppendInt32(nil, int32(len(fullReq2)))
	_, err = conn.Write(append(sizeBuf2, fullReq2...))
	require.NoError(t, err)

	// Read Metadata response
	respSizeBuf2 := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf2)
	require.NoError(t, err)
	respSize2 := int32(binary.BigEndian.Uint32(respSizeBuf2))
	respBuf2 := make([]byte, respSize2)
	_, err = io.ReadFull(conn, respBuf2)
	require.NoError(t, err)

	// Parse the metadata response - skip correlationID (4 bytes)
	metadataResp := &kmsg.MetadataResponse{Version: 1}
	err = metadataResp.ReadFrom(respBuf2[4:])
	require.NoError(t, err)

	// Verify the advertised address is returned
	require.Len(t, metadataResp.Brokers, 1)
	assert.Equal(t, "myhost.local", metadataResp.Brokers[0].Host)
	assert.Equal(t, int32(port), metadataResp.Brokers[0].Port)
}

func TestServerHandlerError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Handler that always returns an error
	failHandler := func(_ context.Context, _ []*Message) error {
		return fmt.Errorf("handler error")
	}

	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, failHandler)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("should-get-error-response"),
	})
	// The produce should get an error response (UnknownServerError)
	require.Error(t, results[0].Err)
	assert.Contains(t, results[0].Err.Error(), kerr.UnknownServerError.Message)
}

// Ensure unused imports are referenced
var (
	_ = bytes.NewBuffer
	_ = kbin.AppendInt16
)
