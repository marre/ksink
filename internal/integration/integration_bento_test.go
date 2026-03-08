package ksink_test

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
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marre/ksink/internal/format"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestFranzHTTPSOutputToBento verifies the full pipeline where:
//  1. A Kafka producer (franz-go) sends messages to ksink.
//  2. ksink formats the messages and POSTs them over HTTPS to a Bento instance.
//  3. Bento receives the HTTPS POSTs and writes them to a file.
//  4. The test reads the file and verifies all messages were received.
func TestFranzHTTPSOutputToBento(t *testing.T) {
	checkDockerIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	// --- Generate TLS certificates for Bento HTTPS ---

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

	// Server certificate for Bento HTTPS
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Bento Server"},
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

	// Make temp dir traversable by the Bento container (runs as uid 10001).
	require.NoError(t, os.Chmod(tmpDir, 0755))

	// Write certs to temp dir
	certsDir := tmpDir + "/certs"
	require.NoError(t, os.MkdirAll(certsDir, 0755))
	require.NoError(t, os.Chmod(certsDir, 0755))
	require.NoError(t, os.WriteFile(certsDir+"/server-cert.pem", serverCertPEM, 0644))
	require.NoError(t, os.WriteFile(certsDir+"/server-key.pem", serverKeyPEM, 0644))

	// Create output dir (world-writable for Docker container)
	outputDir := tmpDir + "/output"
	require.NoError(t, os.MkdirAll(outputDir, 0777))
	require.NoError(t, os.Chmod(outputDir, 0777))

	// --- Write Bento configuration ---

	bentoConfig := `http:
  address: "0.0.0.0:4195"
  cert_file: /certs/server-cert.pem
  key_file: /certs/server-key.pem

input:
  http_server:
    path: /post

output:
  file:
    path: /output/messages.jsonl
    codec: lines
`
	configDir := tmpDir + "/config"
	require.NoError(t, os.MkdirAll(configDir, 0755))
	require.NoError(t, os.Chmod(configDir, 0755))
	require.NoError(t, os.WriteFile(configDir+"/bento.yaml", []byte(bentoConfig), 0644))

	// --- Start Bento Docker container ---

	pool := newDockerPool(t)

	bentoResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "ghcr.io/warpstreamlabs/bento",
		Tag:          "1.15.0",
		Cmd:          []string{"-c", "/config/bento.yaml"},
		ExposedPorts: []string{"4195/tcp"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.Binds = []string{
			certsDir + ":/certs:ro",
			configDir + ":/config:ro",
			outputDir + ":/output",
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Purge(bentoResource)
	})

	bentoPort := bentoResource.GetPort("4195/tcp")
	bentoURL := fmt.Sprintf("https://localhost:%s/post", bentoPort)
	t.Logf("Bento HTTPS URL: %s", bentoURL)

	// Wait for Bento HTTPS server to be ready
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	bentoTLSCfg := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	}

	readyClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: bentoTLSCfg.Clone()},
		Timeout:   2 * time.Second,
	}
	readyURL := fmt.Sprintf("https://localhost:%s/ready", bentoPort)
	err = pool.Retry(func() error {
		resp, reqErr := readyClient.Get(readyURL)
		if reqErr != nil {
			return reqErr
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("bento not ready: status %d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "Bento failed to become ready")
	t.Log("Bento HTTPS server is ready")

	// --- Create HTTPS output writer for ksink ---

	httpWriter, err := output.NewHTTPWriter(bentoURL, output.HTTPOpts{
		Timeout: 10 * time.Second,
	}, bentoTLSCfg)
	require.NoError(t, err)
	defer httpWriter.Close()

	// Create message formatter (binary format with newline separator)
	fmtr, err := format.New("binary", []byte("\n"))
	require.NoError(t, err)

	// --- Start ksink server ---

	port := getIntegrationFreePort(t)
	ksinkAddr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := ksink.New(ksink.Config{
		Address: ksinkAddr,
		Timeout: 10 * time.Second,
	}, ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)

	err = srv.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		srv.Close(context.Background()) //nolint:errcheck
	})

	integrationWaitForTCPReady(t, ksinkAddr, 5*time.Second)

	// Start read loop that forwards messages to Bento over HTTPS
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	go func() {
		for {
			msgs, ack, readErr := srv.ReadBatch(readCtx)
			if readErr != nil {
				return
			}
			var writeErr error
			for _, msg := range msgs {
				data, fmtErr := fmtr.Format(msg)
				if fmtErr != nil {
					writeErr = fmtErr
					break
				}
				if wErr := httpWriter.Write(data, msg); wErr != nil {
					writeErr = wErr
					break
				}
			}
			ack(writeErr)
		}
	}()

	// --- Produce messages to ksink via Kafka protocol ---

	client, err := kgo.NewClient(
		kgo.SeedBrokers(ksinkAddr),
		kgo.RequestTimeoutOverhead(10*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	testMessages := []string{
		"hello-bento-1",
		"hello-bento-2",
		"hello-bento-3",
	}

	for _, msg := range testMessages {
		results := client.ProduceSync(ctx, &kgo.Record{
			Topic: "bento-test-topic",
			Value: []byte(msg),
		})
		require.NoError(t, results[0].Err, "failed to produce message: %s", msg)
	}

	t.Log("All messages produced, waiting for Bento to write to file...")

	// --- Verify messages in Bento output file ---

	outputFile := outputDir + "/messages.jsonl"

	deadline := time.Now().Add(15 * time.Second)
	var fileContent string
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(outputFile)
		if readErr == nil {
			fileContent = string(data)
			allFound := true
			for _, msg := range testMessages {
				if !strings.Contains(fileContent, msg) {
					allFound = false
					break
				}
			}
			if allFound {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, msg := range testMessages {
		assert.Contains(t, fileContent, msg, "message %q not found in Bento output file", msg)
	}

	t.Logf("Bento output file content:\n%s", fileContent)
}
