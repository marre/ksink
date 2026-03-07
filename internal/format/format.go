// Package format provides message formatting for different output modes.
//
// Supported formats:
//   - binary:      Raw message value bytes (default, newline-delimited)
//   - json:        JSON lines with configurable UTF-8/base64 encoding for key/value
//   - kcat:        kcat-compatible format string (use --output-format-string)
package format

import (
	"fmt"

	"github.com/marre/ksink/pkg/ksink"
)

// Formatter converts a [ksink.Message] to bytes suitable for writing to an
// output sink.
type Formatter interface {
	Format(msg *ksink.Message) ([]byte, error)
}

type config struct {
	kcatFormatString string
	jsonBase64Key    bool
	jsonBase64Value  bool
}

// Option configures formatter behavior.
type Option func(*config)

// WithKcatFormatString configures the format string required by the kcat formatter.
func WithKcatFormatString(formatString string) Option {
	return func(cfg *config) {
		cfg.kcatFormatString = formatString
	}
}

// WithJSONBase64Key encodes the JSON key field as base64.
func WithJSONBase64Key() Option {
	return func(cfg *config) {
		cfg.jsonBase64Key = true
	}
}

// WithJSONBase64Value encodes the JSON value field as base64.
func WithJSONBase64Value() Option {
	return func(cfg *config) {
		cfg.jsonBase64Value = true
	}
}

// New creates a Formatter for the given format name.
// Accepted formats: "json", "binary", "kcat".
// The separator is appended after each formatted message (ignored for kcat,
// which embeds separators in the format string).

// For "json", WithJSONBase64Key and WithJSONBase64Value control whether the
// corresponding fields are emitted as base64 strings instead of UTF-8 strings.
// For "kcat", WithKcatFormatString supplies the kcat-compatible format string.
func New(name string, separator []byte, options ...Option) (Formatter, error) {
	cfg := config{}
	for _, option := range options {
		option(&cfg)
	}

	// Validate that options are only used with their intended format.
	if (cfg.jsonBase64Key || cfg.jsonBase64Value) && name != "json" {
		return nil, fmt.Errorf("WithJSONBase64Key/WithJSONBase64Value require format \"json\", got %q", name)
	}
	if cfg.kcatFormatString != "" && name != "kcat" {
		return nil, fmt.Errorf("WithKcatFormatString requires format \"kcat\", got %q", name)
	}

	switch name {
	case "json":
		return &jsonFormatter{
			separator:         separator,
			encodeKeyBase64:   cfg.jsonBase64Key,
			encodeValueBase64: cfg.jsonBase64Value,
		}, nil
	case "binary":
		return &binaryFormatter{separator: separator}, nil
	case "kcat":
		s := cfg.kcatFormatString
		if s == "" {
			return nil, fmt.Errorf("kcat format requires a format string (--output-format-string)")
		}
		return newKcatFormatter(s)
	default:
		return nil, fmt.Errorf("unknown output format: %q", name)
	}
}
