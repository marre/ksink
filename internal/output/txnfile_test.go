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
	tmpPath := filepath.Join(dir, "messages-txn-1.jsonl.tmp")
	committedPath := filepath.Join(dir, "messages-txn-1.jsonl")
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

	tmpPath := filepath.Join(dir, "messages-txn-abort.jsonl.tmp")
	require.FileExists(t, tmpPath)

	require.NoError(t, w.AbortTxn("txn-abort"))

	// After abort: temp file is deleted.
	requireNoFile(t, tmpPath)
	requireNoFile(t, filepath.Join(dir, "messages-txn-abort.jsonl"))
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
	data, err := os.ReadFile(filepath.Join(dir, "out-t1.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "a\nc\n", string(data))

	// t2 aborted.
	requireNoFile(t, filepath.Join(dir, "out-t2.jsonl.tmp"))
	requireNoFile(t, filepath.Join(dir, "out-t2.jsonl"))
}

func TestTxnFileWriterCloseRemovesUncommitted(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.bin")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)

	msg := &ksink.Message{TransactionalID: "open-txn"}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	require.FileExists(t, filepath.Join(dir, "out-open-txn.bin.tmp"))

	require.NoError(t, w.Close())

	// Uncommitted temp file removed on close.
	requireNoFile(t, filepath.Join(dir, "out-open-txn.bin.tmp"))
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
	expected := filepath.Join(dir, "out-evil.jsonl")
	require.FileExists(t, expected)

	// Original traversal path must not exist.
	requireNoFile(t, filepath.Join(dir, "..", "..", "etc", "evil"))
}

func TestTxnFileWriterCommitReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	w, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Pre-create the committed file to simulate a reused txnID.
	committedPath := filepath.Join(dir, "out-reuse.jsonl")
	require.NoError(t, os.WriteFile(committedPath, []byte("old\n"), 0600))

	// Write and commit with the same txnID.
	msg := &ksink.Message{TransactionalID: "reuse"}
	require.NoError(t, w.Write([]byte("new\n"), msg))
	require.NoError(t, w.CommitTxn("reuse"))

	data, err := os.ReadFile(committedPath)
	require.NoError(t, err)
	assert.Equal(t, "new\n", string(data))
}

func requireNoFile(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected %s to not exist, but it does", path)
}
