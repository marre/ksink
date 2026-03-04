// Package format provides message formatting for different output modes.
//
// Supported formats:
//   - json:        JSON lines with key/value as UTF-8 strings (default)
//   - json-base64: JSON lines with key/value as base64-encoded strings
//   - text:        Raw message value followed by a separator (default: newline)
//   - binary:      Raw message value bytes with no separator by default
package format

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/marre/ksink/pkg/ksink"
)

// Formatter converts a [ksink.Message] to bytes suitable for writing to an
// output sink.
type Formatter interface {
	Format(msg *ksink.Message) ([]byte, error)
}

// New creates a Formatter for the given format name.
// Accepted formats: "json", "json-base64", "text", "binary".
// The separator is appended after each formatted message.
func New(name string, separator []byte) (Formatter, error) {
	switch name {
	case "json":
		return &jsonFormatter{separator: separator}, nil
	case "json-base64":
		return &jsonBase64Formatter{separator: separator}, nil
	case "text":
		return &textFormatter{separator: separator}, nil
	case "binary":
		return &binaryFormatter{separator: separator}, nil
	default:
		return nil, fmt.Errorf("unknown output format: %q", name)
	}
}

// jsonRecord is the JSON structure written by the json formatter.
type jsonRecord struct {
	Topic      string            `json:"topic"`
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Key        string            `json:"key,omitempty"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Timestamp  string            `json:"timestamp,omitempty"`
	ClientAddr string            `json:"client_addr"`
}

// jsonBase64Record is the JSON structure written by the json-base64 formatter.
type jsonBase64Record struct {
	Topic      string            `json:"topic"`
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Key        string            `json:"key,omitempty"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Timestamp  string            `json:"timestamp,omitempty"`
	ClientAddr string            `json:"client_addr"`
	Encoding   string            `json:"encoding"`
}

// --- json formatter ---

type jsonFormatter struct {
	separator []byte
}

func (f *jsonFormatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonRecord{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Value:      string(msg.Value),
		Headers:    msg.Headers,
		ClientAddr: msg.ClientAddr,
	}
	if msg.Key != nil {
		rec.Key = string(msg.Key)
	}
	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp.String()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(data, f.separator...), nil
}

// --- json-base64 formatter ---

type jsonBase64Formatter struct {
	separator []byte
}

func (f *jsonBase64Formatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonBase64Record{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Value:      base64.StdEncoding.EncodeToString(msg.Value),
		Headers:    msg.Headers,
		ClientAddr: msg.ClientAddr,
		Encoding:   "base64",
	}
	if msg.Key != nil {
		rec.Key = base64.StdEncoding.EncodeToString(msg.Key)
	}
	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp.String()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(data, f.separator...), nil
}

// --- text formatter ---

// textFormatter writes the message value as-is. It is semantically distinct
// from binaryFormatter to allow independent evolution (e.g. adding charset
// handling in the future).
type textFormatter struct {
	separator []byte
}

func (f *textFormatter) Format(msg *ksink.Message) ([]byte, error) {
	return append(append([]byte(nil), msg.Value...), f.separator...), nil
}

// --- binary formatter ---

// binaryFormatter writes the raw message value bytes. It is kept separate
// from textFormatter for clarity even though the implementation is currently
// identical.
type binaryFormatter struct {
	separator []byte
}

func (f *binaryFormatter) Format(msg *ksink.Message) ([]byte, error) {
	return append(append([]byte(nil), msg.Value...), f.separator...), nil
}
