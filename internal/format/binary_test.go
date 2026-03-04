package format

import (
	"testing"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
