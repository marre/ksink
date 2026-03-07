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
	for _, name := range []string{"json", "binary"} {
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
	f, err := New("kcat", nil, WithKcatFormatString("%t %s\\n"))
	require.NoError(t, err)
	require.NotNil(t, f)
}

func TestNewRejectsKcatFormatStringForNonKcat(t *testing.T) {
	for _, name := range []string{"json", "binary"} {
		_, err := New(name, nil, WithKcatFormatString("%s"))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "kcat", name)
	}
}

func TestNewRejectsBase64OptionsForNonJSON(t *testing.T) {
	for _, name := range []string{"binary", "kcat"} {
		opts := []Option{WithKcatFormatString("%s")} // satisfy kcat requirement
		if name != "kcat" {
			opts = nil
		}
		_, err := New(name, nil, append(opts, WithJSONBase64Key())...)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "json", name)

		_, err = New(name, nil, append(opts, WithJSONBase64Value())...)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "json", name)
	}
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
