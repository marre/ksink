// Package output provides message output backends for ksink.
//
// Supported schemes:
//   - tcp://host:port               Plain TCP
//   - tls://host:port               TLS-wrapped TCP
//   - nanomsg://tcp://host:port     Nanomsg over plain TCP
//   - nanomsg://tls+tcp://host:port Nanomsg over TLS
//   - <path>                        File output
package output

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"strings"
)

// Writer is the interface for message output backends.
type Writer interface {
	io.Closer
	Write(data []byte) error
}

// TLSOpts holds TLS configuration options for output connections.
type TLSOpts struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	SkipVerify bool
}

// BuildTLSConfig creates a *tls.Config from the TLS options.
// Returns nil if no TLS options are set.
func (o *TLSOpts) BuildTLSConfig() (*tls.Config, error) {
	if o.CertFile == "" && o.KeyFile == "" && o.CAFile == "" && !o.SkipVerify {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.SkipVerify, //nolint:gosec // User-requested for testing
	}

	// Client certificate for mTLS
	if o.CertFile != "" && o.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// CA certificate for server verification
	if o.CAFile != "" {
		caCert, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", o.CAFile)
		}
		cfg.RootCAs = pool
	}

	return cfg, nil
}

// Open creates a Writer based on the output string.
func Open(dst string, tlsCfg *tls.Config) (Writer, error) {
	switch {
	case strings.HasPrefix(dst, "tls://"):
		addr := strings.TrimPrefix(dst, "tls://")
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return newTCPWriter(addr, tlsCfg)
	case strings.HasPrefix(dst, "tcp://"):
		addr := strings.TrimPrefix(dst, "tcp://")
		return newTCPWriter(addr, nil)
	case strings.HasPrefix(dst, "nanomsg://"):
		url := strings.TrimPrefix(dst, "nanomsg://")
		return newNanomsgWriter(url, tlsCfg)
	default:
		return newFileWriter(dst)
	}
}
