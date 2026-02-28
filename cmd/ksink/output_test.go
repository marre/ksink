package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marre/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

// --- Test helpers ---

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

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

// startKsinkServer starts a ksink server and returns it with its address.
func startKsinkServer(t *testing.T, ctx context.Context) (*ksink.Server, string) {
	t.Helper()
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := ksink.New(ksink.Config{
		Address: addr,
		Timeout: 5 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { srv.Close(context.Background()) })
	waitForTCPReady(t, addr, 5*time.Second)
	return srv, addr
}

// startReadWriteLoop runs the message forwarding loop (same as the main run loop).
func startReadWriteLoop(t *testing.T, srv *ksink.Server, w writer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			var writeErr error
			for _, msg := range msgs {
				rec := messageRecord{
					Topic:      msg.Topic,
					Partition:  msg.Partition,
					Offset:     msg.Offset,
					Value:      string(msg.Value),
					Headers:    msg.Headers,
					ClientAddr: msg.ClientAddr,
				}
				if msg.Key != nil {
					rec.Key = string(msg.Key)
				}
				if !msg.Timestamp.IsZero() {
					rec.Timestamp = msg.Timestamp.String()
				}
				data, jerr := json.Marshal(rec)
				if jerr != nil {
					writeErr = jerr
					break
				}
				data = append(data, '\n')
				if werr := w.Write(data); werr != nil {
					writeErr = werr
					break
				}
			}
			ack(writeErr)
		}
	}()
}

// produceMessages sends count messages via a franz-go client.
func produceMessages(t *testing.T, ctx context.Context, kafkaAddr string, count int) {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	for i := 0; i < count; i++ {
		record := &kgo.Record{
			Topic: "test-topic",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
			Headers: []kgo.RecordHeader{
				{Key: "header-key", Value: []byte(fmt.Sprintf("hval-%d", i))},
			},
		}
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results[0].Err, "produce message %d", i)
	}
}

// testCA holds a test certificate authority for TLS tests.
type testCA struct {
	certPEM []byte
	cert    *x509.Certificate
	key     *rsa.PrivateKey
}

func generateTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	return &testCA{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		cert:    cert,
		key:     key,
	}
}

func (ca *testCA) generateServerCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func (ca *testCA) certPool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca.certPEM))
	return pool
}

func splitJSONLines(data []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func verifyMessages(t *testing.T, lines []string, count int) {
	t.Helper()
	require.Len(t, lines, count)
	for i, line := range lines {
		var rec messageRecord
		require.NoError(t, json.Unmarshal([]byte(line), &rec), "unmarshal line %d", i)
		assert.Equal(t, "test-topic", rec.Topic)
		assert.Equal(t, fmt.Sprintf("key-%d", i), rec.Key)
		assert.Equal(t, fmt.Sprintf("value-%d", i), rec.Value)
		assert.Equal(t, fmt.Sprintf("hval-%d", i), rec.Headers["header-key"])
		assert.NotEmpty(t, rec.ClientAddr)
	}
}

// --- Output integration tests ---

func TestOutputFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.jsonl")

	w, err := openWriter(outputFile, nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	// Poll until the file has the expected number of lines
	var lines []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(outputFile)
		if err == nil {
			lines = splitJSONLines(data)
			if len(lines) >= 3 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	verifyMessages(t, lines, 3)
}

func TestOutputTCP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a TCP listener to receive messages
	tcpPort := getFreePort(t)
	tcpAddr := fmt.Sprintf("127.0.0.1:%d", tcpPort)
	listener, err := net.Listen("tcp", tcpAddr)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	received := make(chan string, 100)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			received <- scanner.Text()
		}
	}()

	w, err := openWriter(fmt.Sprintf("tcp://%s", tcpAddr), nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	var lines []string
	timeout := time.After(5 * time.Second)
	for len(lines) < 3 {
		select {
		case line := <-received:
			lines = append(lines, line)
		case <-timeout:
			t.Fatalf("timed out waiting for TCP messages, got %d", len(lines))
		}
	}

	verifyMessages(t, lines, 3)
}

func TestOutputTCPTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ca := generateTestCA(t)
	serverCertPEM, serverKeyPEM := ca.generateServerCert(t)

	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	// Start TLS TCP listener (downstream server)
	tcpPort := getFreePort(t)
	tcpAddr := fmt.Sprintf("127.0.0.1:%d", tcpPort)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}
	listener, err := tls.Listen("tcp", tcpAddr, serverTLS)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	received := make(chan string, 100)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			received <- scanner.Text()
		}
	}()

	// Create TLS writer that trusts our CA
	clientTLS := &tls.Config{
		RootCAs:    ca.certPool(t),
		MinVersion: tls.VersionTLS12,
	}
	w, err := openWriter(fmt.Sprintf("tls://%s", tcpAddr), clientTLS)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	var lines []string
	timeout := time.After(5 * time.Second)
	for len(lines) < 3 {
		select {
		case line := <-received:
			lines = append(lines, line)
		case <-timeout:
			t.Fatalf("timed out waiting for TLS TCP messages, got %d", len(lines))
		}
	}

	verifyMessages(t, lines, 3)
}

func TestOutputNanomsg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a nanomsg PULL socket to receive messages
	nmPort := getFreePort(t)
	nmURL := fmt.Sprintf("tcp://127.0.0.1:%d", nmPort)

	pullSock, err := pull.NewSocket()
	require.NoError(t, err)
	t.Cleanup(func() { pullSock.Close() })
	require.NoError(t, pullSock.Listen(nmURL))

	w, err := openWriter(fmt.Sprintf("nanomsg://%s", nmURL), nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	require.NoError(t, pullSock.SetOption(mangos.OptionRecvDeadline, 5*time.Second))
	var lines []string
	for len(lines) < 3 {
		data, err := pullSock.Recv()
		require.NoError(t, err)
		lines = append(lines, strings.TrimSuffix(string(data), "\n"))
	}

	verifyMessages(t, lines, 3)
}

func TestOutputNanomsgTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ca := generateTestCA(t)
	serverCertPEM, serverKeyPEM := ca.generateServerCert(t)

	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	nmPort := getFreePort(t)
	nmURL := fmt.Sprintf("tls+tcp://127.0.0.1:%d", nmPort)

	// Start nanomsg PULL socket with TLS
	pullSock, err := pull.NewSocket()
	require.NoError(t, err)
	t.Cleanup(func() { pullSock.Close() })

	pullTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}
	require.NoError(t, pullSock.ListenOptions(nmURL, map[string]interface{}{
		mangos.OptionTLSConfig: pullTLS,
	}))

	// Create nanomsg writer with client TLS that trusts our CA
	clientTLS := &tls.Config{
		RootCAs:    ca.certPool(t),
		MinVersion: tls.VersionTLS12,
	}
	w, err := openWriter(fmt.Sprintf("nanomsg://%s", nmURL), clientTLS)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	srv, kafkaAddr := startKsinkServer(t, ctx)
	startReadWriteLoop(t, srv, w)
	produceMessages(t, ctx, kafkaAddr, 3)

	require.NoError(t, pullSock.SetOption(mangos.OptionRecvDeadline, 5*time.Second))
	var lines []string
	for len(lines) < 3 {
		data, err := pullSock.Recv()
		require.NoError(t, err)
		lines = append(lines, strings.TrimSuffix(string(data), "\n"))
	}

	verifyMessages(t, lines, 3)
}
