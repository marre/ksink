package output_test

import (
	"testing"

	"github.com/marre/ksink/internal/output"
	"github.com/stretchr/testify/require"
)

func TestOpenStdout(t *testing.T) {
	w, err := output.Open("-", nil)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NoError(t, w.Close())
}
