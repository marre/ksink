package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
