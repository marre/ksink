package format

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	require.NotNil(t, rec.Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("my-key")), *rec.Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello world")), rec.Value)
}

func TestJSONBase64FormatterEmptyKey(t *testing.T) {
	f, err := New("json-base64", []byte("\n"))
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

	var rec jsonBase64Record
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &rec))
	require.NotNil(t, rec.Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{}), *rec.Key)
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

	decodedKey, err := base64.StdEncoding.DecodeString(*rec.Key)
	require.NoError(t, err)
	assert.Equal(t, binData, decodedKey)
}
