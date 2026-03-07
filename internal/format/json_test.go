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
	require.NotNil(t, rec.Key)
	assert.Equal(t, "my-key", *rec.Key)
	assert.Equal(t, "hello world", rec.Value)
	assert.Equal(t, "v", rec.Headers["h"])
	assert.NotEmpty(t, rec.Timestamp)
	assert.Equal(t, "127.0.0.1:12345", rec.ClientAddr)
	assert.Equal(t, "utf-8", rec.KeyEncoding)
	assert.Equal(t, "utf-8", rec.ValueEncoding)
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
	assert.Nil(t, rec.Key)
	assert.Empty(t, rec.KeyEncoding)
}

func TestJSONFormatterEmptyKey(t *testing.T) {
	f, err := New("json", []byte("\n"))
	require.NoError(t, err)

	msg := sampleMessage()
	msg.Key = []byte{}
	data, err := f.Format(msg)
	require.NoError(t, err)

	// Unmarshal into a map to verify the key field is present.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &raw))
	_, present := raw["key"]
	assert.True(t, present, "key field should be present for empty-but-non-nil key")

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	require.NotNil(t, rec.Key)
	assert.Equal(t, "", *rec.Key)
	assert.Equal(t, "utf-8", rec.KeyEncoding)
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

func TestJSONFormatterBase64EncodesKeyAndValue(t *testing.T) {
	f, err := New("json", []byte("\n"), WithJSONBase64Key(), WithJSONBase64Value())
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "base64", rec.KeyEncoding)
	assert.Equal(t, "base64", rec.ValueEncoding)
	require.NotNil(t, rec.Key)
	assert.Equal(t, "bXkta2V5", *rec.Key)
	assert.Equal(t, "aGVsbG8gd29ybGQ=", rec.Value)
}

func TestJSONFormatterCanBase64EncodeValueOnly(t *testing.T) {
	f, err := New("json", []byte("\n"), WithJSONBase64Value())
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "utf-8", rec.KeyEncoding)
	assert.Equal(t, "base64", rec.ValueEncoding)
	require.NotNil(t, rec.Key)
	assert.Equal(t, "my-key", *rec.Key)
	assert.Equal(t, "aGVsbG8gd29ybGQ=", rec.Value)
}

func TestJSONFormatterBase64EncodesBinaryData(t *testing.T) {
	f, err := New("json", []byte("\n"), WithJSONBase64Key(), WithJSONBase64Value())
	require.NoError(t, err)

	binData := []byte{0x00, 0xFF, 0xFE, 0x80, 0x01}
	msg := &ksink.Message{
		Topic:      "binary-topic",
		Value:      binData,
		Key:        binData,
		ClientAddr: "127.0.0.1:1234",
	}
	data, err := f.Format(msg)
	require.NoError(t, err)

	var rec jsonRecord
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	assert.Equal(t, "base64", rec.KeyEncoding)
	assert.Equal(t, "base64", rec.ValueEncoding)

	decoded, err := base64.StdEncoding.DecodeString(rec.Value)
	require.NoError(t, err)
	assert.Equal(t, binData, decoded)

	decodedKey, err := base64.StdEncoding.DecodeString(*rec.Key)
	require.NoError(t, err)
	assert.Equal(t, binData, decodedKey)
}
