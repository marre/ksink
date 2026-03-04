package format

import (
	"encoding/json"
	"testing"
	"time"

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
