package output_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxnFileWriterRequiresPlaceholder(t *testing.T) {
	_, err := output.NewTxnFileWriter("/tmp/out.jsonl")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "{txnID}")
}

func TestTxnFileWriterCommit(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "messages-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	msg := &ksink.Message{TransactionalID: "txn-1"}
	require.NoError(t, w.Write([]byte("hello\n"), msg))

	// Temp file should exist, committed file should not.
	tmpPath := oneMatch(t, filepath.Join(dir, "messages-txn-1-*.jsonl.tmp"))
	committedPath := strings.TrimSuffix(tmpPath, ".tmp")
	require.FileExists(t, tmpPath)
	requireNoFile(t, committedPath)

	require.NoError(t, w.CommitTxn("txn-1"))

	// After commit: temp removed, committed file exists with content.
	requireNoFile(t, tmpPath)
	require.FileExists(t, committedPath)
	data, err := os.ReadFile(committedPath)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(data))
}

func TestTxnFileWriterAbort(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "messages-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	msg := &ksink.Message{TransactionalID: "txn-abort"}
	require.NoError(t, w.Write([]byte("should-be-discarded\n"), msg))

	tmpPath := oneMatch(t, filepath.Join(dir, "messages-txn-abort-*.jsonl.tmp"))
	require.FileExists(t, tmpPath)

	require.NoError(t, w.AbortTxn("txn-abort"))

	// After abort: temp file is deleted.
	requireNoFile(t, tmpPath)
	requireNoFile(t, strings.TrimSuffix(tmpPath, ".tmp"))
}

func TestTxnFileWriterRejectsNonTransactionalWrite(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Write without a TransactionalID should fail.
	err = w.Write([]byte("plain\n"), &ksink.Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TransactionalID")
}

func TestTxnFileWriterMultipleTransactions(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Two concurrent transactions.
	msg1 := &ksink.Message{TransactionalID: "t1"}
	msg2 := &ksink.Message{TransactionalID: "t2"}

	require.NoError(t, w.Write([]byte("a\n"), msg1))
	require.NoError(t, w.Write([]byte("b\n"), msg2))
	require.NoError(t, w.Write([]byte("c\n"), msg1))

	require.NoError(t, w.CommitTxn("t1"))
	require.NoError(t, w.AbortTxn("t2"))

	// t1 committed with both writes; extension stays at the end.
	data, err := os.ReadFile(oneMatch(t, filepath.Join(dir, "out-t1-*.jsonl")))
	require.NoError(t, err)
	assert.Equal(t, "a\nc\n", string(data))

	// t2 aborted.
	noMatches(t, filepath.Join(dir, "out-t2-*.jsonl.tmp"))
	noMatches(t, filepath.Join(dir, "out-t2-*.jsonl"))
}

func TestTxnFileWriterCloseRemovesUncommitted(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.bin")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)

	msg := &ksink.Message{TransactionalID: "open-txn"}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	tmpPath := oneMatch(t, filepath.Join(dir, "out-open-txn-*.bin.tmp"))

	require.NoError(t, w.Close())

	// Uncommitted temp file removed on close.
	requireNoFile(t, tmpPath)
}

func TestTxnFileWriterCommitNoData(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Commit/abort with no data written should be a no-op.
	require.NoError(t, w.CommitTxn("nonexistent"))
	require.NoError(t, w.AbortTxn("nonexistent"))
}

func TestTxnFileWriterSanitizesTxnID(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// A txnID with path separators should be sanitised so the file stays
	// inside the intended directory.
	msg := &ksink.Message{TransactionalID: "../../etc/evil"}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	require.NoError(t, w.CommitTxn("../../etc/evil"))

	// The committed file must be inside the temp dir, not escaped.
	expected := oneMatch(t, filepath.Join(dir, "out-evil-*.jsonl"))
	require.FileExists(t, expected)

	// Original traversal path must not exist.
	requireNoFile(t, filepath.Join(dir, "..", "..", "etc", "evil"))
}

func TestTxnFileWriterNeverReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")
	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	original := filepath.Join(dir, "out-reuse.jsonl")
	require.NoError(t, os.WriteFile(original, []byte("old\n"), 0600))

	msg := &ksink.Message{TransactionalID: "reuse"}
	require.NoError(t, w.Write([]byte("first\n"), msg))
	require.NoError(t, w.CommitTxn("reuse"))
	require.NoError(t, w.Write([]byte("second\n"), msg))
	require.NoError(t, w.CommitTxn("reuse"))

	// The original file and both independent successful commits survive.
	data, err := os.ReadFile(original)
	require.NoError(t, err)
	require.Equal(t, "old\n", string(data))
	paths, err := filepath.Glob(filepath.Join(dir, "out-reuse-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, paths, 2)
	first, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	second, err := os.ReadFile(paths[1])
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"first\n", "second\n"}, []string{string(first), string(second)})
}

func TestTxnFileWriterCommitCanRetryAfterPublishFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	msg := &ksink.Message{TransactionalID: "retry"}
	require.NoError(t, w.Write([]byte("payload\n"), msg))
	tmp := oneMatch(t, filepath.Join(dir, "retry-*.jsonl.tmp"))
	final := strings.TrimSuffix(tmp, ".tmp")
	// A directory at the final path makes os.Link fail without losing staging.
	require.NoError(t, os.Mkdir(final, 0700))
	require.Error(t, w.CommitTxn("retry"))
	require.FileExists(t, tmp)
	require.Error(t, w.Write([]byte("must not append\n"), msg))
	require.NoError(t, os.Remove(final))
	require.NoError(t, w.CommitTxn("retry"))
	requireNoFile(t, tmp)
	b, err := os.ReadFile(final)
	require.NoError(t, err)
	require.Equal(t, "payload\n", string(b))
}

func TestTxnFileWriterAbortAfterPartialMultiKeyCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "{topic}-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Write([]byte("a\n"), &ksink.Message{Topic: "orders", TransactionalID: "multi"}))
	require.NoError(t, w.Write([]byte("b\n"), &ksink.Message{Topic: "payments", TransactionalID: "multi"}))
	paths, err := filepath.Glob(filepath.Join(dir, "*-multi-*.jsonl.tmp"))
	require.NoError(t, err)
	require.Len(t, paths, 2)
	// A single publish fails; whichever was already linked must be removed on abort.
	failingFinal := strings.TrimSuffix(paths[0], ".tmp")
	require.NoError(t, os.Mkdir(failingFinal, 0700))
	require.Error(t, w.CommitTxn("multi"))
	require.Error(t, w.Write([]byte("blocked"), &ksink.Message{Topic: "orders", TransactionalID: "multi"}))
	require.NoError(t, w.AbortTxn("multi"))
	require.NoError(t, os.Remove(failingFinal))
	noMatches(t, filepath.Join(dir, "*-multi-*.jsonl.tmp"))
	noMatches(t, filepath.Join(dir, "*-multi-*.jsonl"))
}

func oneMatch(t *testing.T, pattern string) string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Len(t, paths, 1, "glob %q", pattern)
	return paths[0]
}

func noMatches(t *testing.T, pattern string) {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func requireNoFile(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected %s to not exist, but it does", path)
}
