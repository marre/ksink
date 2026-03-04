package format

import (
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKcatFormatterBasic(t *testing.T) {
	f, err := New("kcat", nil, "Topic: %t Key: %k Value: %s\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "Topic: events Key: my-key Value: hello world\n", string(data))
}

func TestKcatFormatterAllSpecifiers(t *testing.T) {
	f, err := New("kcat", nil, "%t|%p|%o|%k|%s|%K|%S|%T\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "events|0|42|my-key|hello world|6|11|1735689600000\n", string(data))
}

func TestKcatFormatterNilKey(t *testing.T) {
	f, err := New("kcat", nil, "%k|%K\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	msg.Key = nil
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "|-1\n", string(data))
}

func TestKcatFormatterZeroTimestamp(t *testing.T) {
	f, err := New("kcat", nil, "%T")
	require.NoError(t, err)

	msg := sampleMessage()
	msg.Timestamp = time.Time{}
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "-1", string(data))
}

func TestKcatFormatterEscapeSequences(t *testing.T) {
	f, err := New("kcat", nil, "%s\\t%k\\r\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "hello world\tmy-key\r\n", string(data))
}

func TestKcatFormatterLiteralPercent(t *testing.T) {
	f, err := New("kcat", nil, "100%% %s\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "100% hello world\n", string(data))
}

func TestKcatFormatterLiteralBackslash(t *testing.T) {
	f, err := New("kcat", nil, "path\\\\dir %s\\n")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "path\\dir hello world\n", string(data))
}

func TestKcatFormatterUnknownSpecifier(t *testing.T) {
	_, err := New("kcat", nil, "%z")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown kcat format specifier")
}

func TestKcatFormatterBinaryValue(t *testing.T) {
	f, err := New("kcat", nil, "%s")
	require.NoError(t, err)

	binData := []byte{0x00, 0xFF, 0xFE}
	msg := &ksink.Message{
		Topic:      "bin",
		Value:      binData,
		ClientAddr: "127.0.0.1:1234",
	}
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, binData, data)
}

func TestKcatFormatterNullSeparator(t *testing.T) {
	f, err := New("kcat", nil, "%s\\0")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, append([]byte("hello world"), 0), data)
}

func TestKcatFormatterValueOnly(t *testing.T) {
	f, err := New("kcat", nil, "%s")
	require.NoError(t, err)

	msg := sampleMessage()
	data, err := f.Format(msg)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}
