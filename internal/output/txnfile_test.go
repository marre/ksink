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

func TestTxnFileWriterCommit(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	msg := &ksink.Message{TransactionalID: "txn-1"}
	require.NoError(t, w.Write([]byte("hello\n"), msg))

	// Temp file should exist, committed file should not.
	tmpPath := base + ".txn-txn-1.tmp"
	committedPath := base + ".txn-txn-1"
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
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	msg := &ksink.Message{TransactionalID: "txn-abort"}
	require.NoError(t, w.Write([]byte("should-be-discarded\n"), msg))

	tmpPath := base + ".txn-txn-abort.tmp"
	require.FileExists(t, tmpPath)

	require.NoError(t, w.AbortTxn("txn-abort"))

	// After abort: temp file is deleted.
	requireNoFile(t, tmpPath)
	requireNoFile(t, base+".txn-txn-abort")
}

func TestTxnFileWriterNonTransactional(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Write without a transactional ID goes to the base file.
	msg := &ksink.Message{}
	require.NoError(t, w.Write([]byte("plain\n"), msg))
	require.NoError(t, w.Close())

	data, err := os.ReadFile(base)
	require.NoError(t, err)
	assert.Equal(t, "plain\n", string(data))
}

func TestTxnFileWriterMultipleTransactions(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Two concurrent transactions.
	msg1 := &ksink.Message{TransactionalID: "t1"}
	msg2 := &ksink.Message{TransactionalID: "t2"}

	require.NoError(t, w.Write([]byte("a\n"), msg1))
	require.NoError(t, w.Write([]byte("b\n"), msg2))
	require.NoError(t, w.Write([]byte("c\n"), msg1))

	require.NoError(t, w.CommitTxn("t1"))
	require.NoError(t, w.AbortTxn("t2"))

	// t1 committed with both writes.
	data, err := os.ReadFile(base + ".txn-t1")
	require.NoError(t, err)
	assert.Equal(t, "a\nc\n", string(data))

	// t2 aborted.
	requireNoFile(t, base+".txn-t2.tmp")
	requireNoFile(t, base+".txn-t2")
}

func TestTxnFileWriterCloseRemovesUncommitted(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)

	msg := &ksink.Message{TransactionalID: "open-txn"}
	require.NoError(t, w.Write([]byte("data\n"), msg))
	require.FileExists(t, base+".txn-open-txn.tmp")

	require.NoError(t, w.Close())

	// Uncommitted temp file removed on close.
	requireNoFile(t, base+".txn-open-txn.tmp")
}

func TestTxnFileWriterCommitNoData(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "out.jsonl")

	w := output.NewTxnFileWriter(base)
	t.Cleanup(func() { w.Close() }) //nolint:errcheck

	// Commit/abort with no data written should be a no-op.
	require.NoError(t, w.CommitTxn("nonexistent"))
	require.NoError(t, w.AbortTxn("nonexistent"))
}

func requireNoFile(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected %s to not exist, but it does", path)
}
