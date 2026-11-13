package output_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizePathSegment(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"normal", "my-topic", "my-topic"},
		{"forward slash", "a/b", "b"},
		{"backslash", `a\b`, `a\b`},   // On Windows, filepath.Base would return "b"
		{"colon", "C:foo", "C:foo"},    // On Windows, filepath.Clean recognises the volume prefix
		{"double dot", "../etc", "etc"},
		{"path traversal", "../../etc/evil", "evil"},
		{"leading dot", ".hidden", ".hidden"},
		{"leading dots", "...tricky", "...tricky"},
		{"empty string", "", "_"},
		{"only dots", "..", "_"},
		{"only slashes", "///", "_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := output.SanitizePathSegment(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPatternFileWriterRoutesByTopic(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}.jsonl")

	w, err := output.Open(pattern, nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Write to two different topics.
	msg1 := &ksink.Message{Topic: "orders"}
	msg2 := &ksink.Message{Topic: "events"}

	require.NoError(t, w.Write([]byte("order-1\n"), msg1))
	require.NoError(t, w.Write([]byte("event-1\n"), msg2))
	require.NoError(t, w.Write([]byte("order-2\n"), msg1))

	// Flush by closing.
	require.NoError(t, w.Close())

	data1, err := os.ReadFile(filepath.Join(dir, "orders.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "order-1\norder-2\n", string(data1))

	data2, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "event-1\n", string(data2))
}

func TestPatternFileWriterSanitizesTopic(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}.jsonl")

	w, err := output.Open(pattern, nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// A topic with path traversal characters should be sanitised.
	msg := &ksink.Message{Topic: "../../etc/passwd"}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	require.NoError(t, w.Close())

	// File must be inside the temp dir, not escaped.
	expected := filepath.Join(dir, "passwd.jsonl")
	require.FileExists(t, expected)
}

func TestPatternFileWriterEmptyTopic(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}.jsonl")

	w, err := output.Open(pattern, nil)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// An empty topic should produce a safe filename via the fallback.
	msg := &ksink.Message{Topic: ""}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	require.NoError(t, w.Close())

	expected := filepath.Join(dir, "_.jsonl")
	require.FileExists(t, expected)
}

// Partition routing is opt-in: omitting {partition} preserves per-topic routing.
func TestPatternFileWriterRoutesByPartition(t *testing.T) {
 dir := t.TempDir()
 w, err := output.Open(filepath.Join(dir, "{topic}-{partition}.jsonl"), nil)
 require.NoError(t, err)
 require.NoError(t, w.Write([]byte("a\n"), &ksink.Message{Topic:"orders", Partition:0}))
 require.NoError(t, w.Write([]byte("b\n"), &ksink.Message{Topic:"orders", Partition:1}))
 require.NoError(t, w.Write([]byte("c\n"), &ksink.Message{Topic:"orders", Partition:0}))
 require.NoError(t, w.Write([]byte("d\n"), &ksink.Message{Topic:"events", Partition:0}))
 require.NoError(t, w.Close())
 for path, want := range map[string]string{
  "orders-0.jsonl":"a\nc\n",
  "orders-1.jsonl":"b\n",
  "events-0.jsonl":"d\n",
 } {
  b, err := os.ReadFile(filepath.Join(dir,path))
  require.NoError(t,err)
  assert.Equal(t,want,string(b))
 }
}

func TestPatternFileWriterPartitionOnly(t *testing.T) {
 dir := t.TempDir()
 w, err := output.Open(filepath.Join(dir, "part-{partition}.bin"), nil)
 require.NoError(t, err)
 require.NoError(t,w.Write([]byte("first\n"), &ksink.Message{Topic:"a",Partition:2}))
 require.NoError(t,w.Write([]byte("second\n"), &ksink.Message{Topic:"b",Partition:2}))
 require.NoError(t,w.Close())
 b,err:=os.ReadFile(filepath.Join(dir,"part-2.bin"))
 require.NoError(t,err)
 assert.Equal(t,"first\nsecond\n",string(b))
}

