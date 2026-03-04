package format

import (
	"encoding/base64"
	"encoding/json"
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

func TestJSONFormatter(t *testing.T) {
	f, err := New("json", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)

	// Should end with newline
	assert.Equal(t, byte('\n'), data[len(data)-1])

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "events", rec.Topic)
	assert.Equal(t, int32(0), rec.Partition)
	assert.Equal(t, int64(42), rec.Offset)
	assert.Equal(t, "my-key", rec.Key)
	assert.Equal(t, "hello world", rec.Value)
	assert.Equal(t, "v", rec.Headers["h"])
	assert.NotEmpty(t, rec.Timestamp)
	assert.Equal(t, "127.0.0.1:12345", rec.ClientAddr)
}

func TestJSONFormatterNilKey(t *testing.T) {
	f, err := New("json", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	msg.Key = nil
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Empty(t, rec.Key)
}

func TestJSONFormatterZeroTimestamp(t *testing.T) {
	f, err := New("json", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	msg.Timestamp = time.Time{}
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Empty(t, rec.Timestamp)
}

func TestJSONBase64Formatter(t *testing.T) {
	f, err := New("json-base64", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)

	assert.Equal(t, byte('\n'), data[len(data)-1])

	var rec jsonBase64Record
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "events", rec.Topic)
	assert.Equal(t, "base64", rec.Encoding)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("my-key")), rec.Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello world")), rec.Value)
}

func TestJSONBase64FormatterBinaryData(t *testing.T) {
	f, err := New("json-base64", []byte("\n"))
	require.NoError(t, err)

	// Binary data that is not valid UTF-8
	binData := []byte{0x00, 0xFF, 0xFE, 0x80, 0x01}
	msg := &ksink.Message{
		Topic:      "binary-topic",
		Value:      binData,
		Key:        binData,
		ClientAddr: "127.0.0.1:1234",
	}
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonBase64Record
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "base64", rec.Encoding)

	decoded, err := base64.StdEncoding.DecodeString(rec.Value)
	require.NoError(t, err)
	assert.Equal(t, binData, decoded)

	decodedKey, err := base64.StdEncoding.DecodeString(rec.Key)
	require.NoError(t, err)
	assert.Equal(t, binData, decodedKey)
}

func TestTextFormatter(t *testing.T) {
	f, err := New("text", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "hello world\n", string(data))
}

func TestTextFormatterCustomSeparator(t *testing.T) {
	f, err := New("text", []byte("\r\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "hello world\r\n", string(data))
}

func TestTextFormatterNoSeparator(t *testing.T) {
	f, err := New("text", nil)
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func TestBinaryFormatter(t *testing.T) {
	f, err := New("binary", nil)
	require.NoError(t, err)

	binData := []byte{0x00, 0xFF, 0xFE, 0x80, 0x01}
	msg := &ksink.Message{
		Topic:      "bin",
		Value:      binData,
		ClientAddr: "127.0.0.1:1234",
	}
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, binData, data)
}

func TestBinaryFormatterWithSeparator(t *testing.T) {
	f, err := New("binary", []byte{0x0A})
	require.NoError(t, err)

	binData := []byte{0x01, 0x02}
	msg := &ksink.Message{
		Topic:      "bin",
		Value:      binData,
		ClientAddr: "127.0.0.1:1234",
	}
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02, 0x0A}, data)
}
