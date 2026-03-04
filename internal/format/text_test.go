package format

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
