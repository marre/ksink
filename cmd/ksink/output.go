package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"strings"
)

// writer is the interface for message output backends.
type writer interface {
	io.Closer
	Write(data []byte) error
}

// tlsOpts holds TLS configuration options for output connections.
type tlsOpts struct {
	certFile   string
	keyFile    string
	caFile     string
	skipVerify bool
}

// buildTLSConfig creates a *tls.Config from the TLS options.
// Returns nil if no TLS options are set.
func (o *tlsOpts) buildTLSConfig() (*tls.Config, error) {
	if o.certFile == "" && o.keyFile == "" && o.caFile == "" && !o.skipVerify {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.skipVerify, //nolint:gosec // User-requested for testing
	}

	// Client certificate for mTLS
	if o.certFile != "" && o.keyFile != "" {
		cert, err := tls.LoadX509KeyPair(o.certFile, o.keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// CA certificate for server verification
	if o.caFile != "" {
		caCert, err := os.ReadFile(o.caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", o.caFile)
		}
		cfg.RootCAs = pool
	}

	return cfg, nil
}

// openWriter creates a writer based on the output string.
//
// Supported schemes:
//   - tcp://host:port             Plain TCP
//   - tls://host:port             TLS-wrapped TCP
//   - nanomsg://tcp://host:port   Nanomsg over plain TCP
//   - nanomsg://tls+tcp://host:port  Nanomsg over TLS
//   - <path>                      File output
func openWriter(output string, tlsCfg *tls.Config) (writer, error) {
	switch {
	case strings.HasPrefix(output, "tls://"):
		addr := strings.TrimPrefix(output, "tls://")
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return newTCPWriter(addr, tlsCfg)
	case strings.HasPrefix(output, "tcp://"):
		addr := strings.TrimPrefix(output, "tcp://")
		return newTCPWriter(addr, nil)
	case strings.HasPrefix(output, "nanomsg://"):
		url := strings.TrimPrefix(output, "nanomsg://")
		return newNanomsgWriter(url, tlsCfg)
	default:
		return newFileWriter(output)
	}
}
