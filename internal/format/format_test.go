package format

import (
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUnknownFormat(t *testing.T) {
	_, err := New("unknown", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown output format")
}

func TestNewValidFormats(t *testing.T) {
	for _, name := range []string{"json", "json-base64", "text", "binary"} {
		f, err := New(name, []byte("\n"))
		require.NoError(t, err, name)
		require.NotNil(t, f, name)
	}
}

func TestNewKcatRequiresFormatString(t *testing.T) {
	_, err := New("kcat", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format string")
}

func TestNewKcatValid(t *testing.T) {
	f, err := New("kcat", nil, "%t %s\\n")
	require.NoError(t, err)
	require.NotNil(t, f)
}

func sampleMessage() *ksink.Message {
	return &ksink.Message{
		Topic:      "events",
		Partition:  0,
		Offset:     42,
		Key:        []byte("my-key"),
		Value:      []byte("hello world"),
		Headers:    map[string]string{"h": "v"},
		Timestamp:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		ClientAddr: "127.0.0.1:12345",
	}
}
