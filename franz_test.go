package ksrv

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// --- Franz-go integration tests ---

func TestFranzBasic(t *testing.T) {
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

func TestFranzMultipleMessages(t *testing.T) {
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

func TestFranzTopicFiltering(t *testing.T) {
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

func TestFranzSASLPlain(t *testing.T) {
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

func TestFranzSASLPlainWrongPassword(t *testing.T) {
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

func TestFranzSASLScram256(t *testing.T) {
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

func TestFranzSASLScram512(t *testing.T) {
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

func TestFranzSASLScramWrongPassword(t *testing.T) {
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

func TestFranzMultipleUsers(t *testing.T) {
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

func TestFranzIdempotentProducer(t *testing.T) {
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

func TestFranzHandlerError(t *testing.T) {
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

func TestFranzMaxMessageBytes(t *testing.T) {
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

func TestFranzTLS(t *testing.T) {
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

func TestFranzMTLS(t *testing.T) {
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

func TestFranzClose(t *testing.T) {
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
