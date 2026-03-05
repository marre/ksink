package output_test

import (
	"testing"

	"github.com/marre/ksink/internal/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenUnsupportedScheme(t *testing.T) {
	for _, dst := range []string{"tcp://localhost:4444", "tls://localhost:4444", "nanomsg://tcp://localhost:4444", "ftp://example.com/file"} {
		w, err := output.Open(dst, nil)
		require.Error(t, err, "expected error for %q", dst)
		assert.Nil(t, w)
		assert.Contains(t, err.Error(), "unsupported output scheme")
	}
}
