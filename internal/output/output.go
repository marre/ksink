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

// TransactionalWriter extends [Writer] with transaction commit and abort
// operations. File-based backends can implement this to write into temporary
// files during a transaction and then rename on commit or delete on abort.
type TransactionalWriter interface {
	Writer
	CommitTxn(txnID string) error
	AbortTxn(txnID string) error
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

// TopicPlaceholder is the placeholder that can be used in output patterns
// to route messages to per-topic files.
const TopicPlaceholder = "{topic}"

// SanitizePathSegment removes or replaces characters that could cause path
// traversal when a user-controlled string is interpolated into a filesystem
// path. It replaces directory separators (/ and \), colons (:), and ".."
// sequences with underscores, and trims leading dots.
func SanitizePathSegment(s string) string {
	safe := strings.ReplaceAll(s, "/", "_")
	safe = strings.ReplaceAll(safe, "\\", "_")
	safe = strings.ReplaceAll(safe, ":", "_")
	safe = strings.ReplaceAll(safe, "..", "_")
	safe = strings.TrimLeft(safe, ".")

	if safe == "" {
		return "_"
	}
	return safe
}

// Open creates a Writer based on the output string.
// The special value "-" writes to standard output.
// For http:// and https:// URLs, Open creates an HTTP writer with default
// options (no retries, no DLQ). Use NewHTTPWriter for full configuration.
// If the destination contains the {topic} placeholder, a pattern-based writer
// is returned that routes messages to per-topic files.
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
		if strings.Contains(dst, TopicPlaceholder) {
			return newPatternFileWriter(dst), nil
		}
		return newFileWriter(dst)
	}
}
