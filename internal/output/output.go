// Package output provides message output backends for ksink.
//
// Supported schemes:
//   - -                              Standard output (stdout)
//   - http://host:port/path         HTTP POST
//   - https://host:port/path        HTTPS POST
//   - <path>                        File output
package output

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/marre/ksink/pkg/ksink"
)

// Writer is the interface for message output backends.
type Writer interface {
	io.Closer
	Write(data []byte, msg *ksink.Message) error
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
	if (o.CertFile != "") != (o.KeyFile != "") {
		return nil, fmt.Errorf("both --output-tls-cert and --output-tls-key must be provided for mTLS")
	}
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
// The special value "-" writes to standard output.
// For http:// and https:// URLs, Open creates an HTTP writer with default
// options (no retries, no DLQ). Use NewHTTPWriter for full configuration.
func Open(dst string, tlsCfg *tls.Config) (Writer, error) {
	switch {
	case dst == "-":
		return newStdoutWriter(), nil
	case strings.HasPrefix(dst, "https://"), strings.HasPrefix(dst, "http://"):
		return NewHTTPWriter(dst, HTTPOpts{}, tlsCfg)
	default:
		// Reject unknown URL-like schemes explicitly to avoid treating them as file paths.
		if strings.Contains(dst, "://") {
			return nil, fmt.Errorf("unsupported output scheme in %q; only -, http(s), or file paths are supported", dst)
		}
		return newFileWriter(dst)
	}
}
