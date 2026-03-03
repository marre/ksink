package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/marre/ksink/internal/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
func startReadWriteLoop(t *testing.T, srv *ksink.Server, w output.Writer) {
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
