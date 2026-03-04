// Package format provides message formatting for different output modes.
//
// Supported formats:
//   - json:        JSON lines with key/value as UTF-8 strings (default)
//   - json-base64: JSON lines with key/value as base64-encoded strings
//   - text:        Raw message value followed by a separator (default: newline)
//   - binary:      Raw message value bytes with no separator by default
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

// New creates a Formatter for the given format name.
// Accepted formats: "json", "json-base64", "text", "binary", "kcat".
// The separator is appended after each formatted message (ignored for kcat,
// which embeds separators in the format string).
// For "kcat", fmtStr supplies the kcat-compatible format string.
func New(name string, separator []byte, fmtStr ...string) (Formatter, error) {
	switch name {
	case "json":
		return &jsonFormatter{separator: separator}, nil
	case "json-base64":
		return &jsonBase64Formatter{separator: separator}, nil
	case "text":
		return &textFormatter{separator: separator}, nil
	case "binary":
		return &binaryFormatter{separator: separator}, nil
	case "kcat":
		s := ""
		if len(fmtStr) > 0 {
			s = fmtStr[0]
		}
		if s == "" {
			return nil, fmt.Errorf("kcat format requires a format string (--output-format-string)")
		}
		return newKcatFormatter(s)
	default:
		return nil, fmt.Errorf("unknown output format: %q", name)
	}
}
