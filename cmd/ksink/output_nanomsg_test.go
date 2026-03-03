package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marre/ksink/internal/output"
	"github.com/stretchr/testify/require"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

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

	w, err := output.Open(fmt.Sprintf("nanomsg://%s", nmURL), nil)
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
	w, err := output.Open(fmt.Sprintf("nanomsg://%s", nmURL), clientTLS)
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
