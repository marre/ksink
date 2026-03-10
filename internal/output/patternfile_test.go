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

func TestTxnFileWriterWithTopicPlaceholder(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Write to two topics in the same transaction.
	msg1 := &ksink.Message{TransactionalID: "txn-1", Topic: "orders"}
	msg2 := &ksink.Message{TransactionalID: "txn-1", Topic: "events"}

	require.NoError(t, w.Write([]byte("order\n"), msg1))
	require.NoError(t, w.Write([]byte("event\n"), msg2))

	// Both temp files should exist before commit.
	require.FileExists(t, filepath.Join(dir, "orders-txn-1.jsonl.tmp"))
	require.FileExists(t, filepath.Join(dir, "events-txn-1.jsonl.tmp"))

	require.NoError(t, w.CommitTxn("txn-1"))

	// After commit: both committed files should exist.
	data1, err := os.ReadFile(filepath.Join(dir, "orders-txn-1.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "order\n", string(data1))

	data2, err := os.ReadFile(filepath.Join(dir, "events-txn-1.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "event\n", string(data2))

	// Temp files should be gone.
	requireNoFile(t, filepath.Join(dir, "orders-txn-1.jsonl.tmp"))
	requireNoFile(t, filepath.Join(dir, "events-txn-1.jsonl.tmp"))
}

func TestTxnFileWriterWithTopicAbort(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	msg := &ksink.Message{TransactionalID: "txn-abort", Topic: "orders"}
	require.NoError(t, w.Write([]byte("data\n"), msg))

	require.NoError(t, w.AbortTxn("txn-abort"))

	// After abort: temp and committed files should not exist.
	requireNoFile(t, filepath.Join(dir, "orders-txn-abort.jsonl.tmp"))
	requireNoFile(t, filepath.Join(dir, "orders-txn-abort.jsonl"))
}
