package output

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/require"
)

// crashFileWriter simulates a process dying without calling Close. In
// particular, it must not run graceful shutdown cleanup on the staging tree.
func crashFileWriter(t *testing.T, writer TransactionalWriter) {
	t.Helper()
	w := writer.(*txnFileWriter)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, txn := range w.txnFiles {
		for _, entry := range txn.files {
			if !entry.closed {
				require.NoError(t, entry.file.Close())
				entry.closed = true
			}
		}
	}
	w.closed = true
	w.txnFiles = nil
}

func requireOneTxnDir(t *testing.T, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	return matches[0]
}

func TestTxnFileStartupDiscardsUncommittedTransaction(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}-{txnID}.jsonl")
	before, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	for _, topic := range []string{"orders", "payments"} {
		require.NoError(t, before.Write([]byte(topic), &ksink.Message{TransactionalID: "active", Topic: topic}))
	}
	stage := requireOneTxnDir(t, filepath.Join(dir, ".ksink-pending", "active-*"))
	require.FileExists(t, filepath.Join(stage, "orders-active.jsonl"))
	crashFileWriter(t, before)

	after, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	defer func() { _ = after.Close() }()
	_, err = os.Stat(stage)
	require.ErrorIs(t, err, os.ErrNotExist)
	committed, err := filepath.Glob(filepath.Join(dir, "committed", "active-*"))
	require.NoError(t, err)
	require.Empty(t, committed)

	// Reusing the same Kafka transactional ID does not reuse stale files.
	require.NoError(t, after.Write([]byte("new"), &ksink.Message{TransactionalID: "active", Topic: "orders"}))
	require.NoError(t, after.CommitTxn("active"))
	committed = []string{requireOneTxnDir(t, filepath.Join(dir, "committed", "active-*"))}
	data, err := os.ReadFile(filepath.Join(committed[0], "orders-active.jsonl"))
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
}

func TestTxnFileStartupFinishesInterruptedMultiFileCommit(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{topic}-{partition}-{txnID}.jsonl")
	before, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	for _, topic := range []string{"orders", "payments"} {
		for partition := int32(0); partition < 3; partition++ {
			require.NoError(t, before.Write([]byte(topic), &ksink.Message{
				TransactionalID: "recover", Topic: topic, Partition: partition,
			}))
		}
	}
	committedRoot := filepath.Join(dir, "committed")
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.WriteFile(committedRoot, []byte("block rename"), 0600))
	require.Error(t, before.CommitTxn("recover"))
	stage := requireOneTxnDir(t, filepath.Join(dir, ".ksink-pending", "recover-*"))
	require.FileExists(t, filepath.Join(stage, txnCommitIntentFile))
	require.ErrorContains(t, before.AbortTxn("recover"), "commit intent")
	crashFileWriter(t, before)

	// If recovery cannot publish, constructor must fail without discarding
	// either the durable intent or its staged payloads.
	_, err = NewTxnFileWriter(pattern)
	require.Error(t, err)
	require.FileExists(t, filepath.Join(stage, txnCommitIntentFile))
	require.FileExists(t, filepath.Join(stage, "orders-0-recover.jsonl"))

	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.Mkdir(committedRoot, 0700))
	after, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	require.NoError(t, after.Close())
	committed := requireOneTxnDir(t, filepath.Join(dir, "committed", "recover-*"))
	for _, topic := range []string{"orders", "payments"} {
		for partition := int32(0); partition < 3; partition++ {
			require.FileExists(t, filepath.Join(committed, topic+"-"+string(rune('0'+partition))+"-recover.jsonl"))
		}
	}
	_, err = os.Stat(stage)
	require.ErrorIs(t, err, os.ErrNotExist)

	// A second startup is idempotent; it cannot create a duplicate.
	reopened, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
	paths, err := filepath.Glob(filepath.Join(dir, "committed", "recover-*"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
}

func TestTxnFileClosePreservesIntentForRecovery(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.bin")
	w, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	require.NoError(t, w.Write([]byte("committed"), &ksink.Message{TransactionalID: "intent"}))

	root := filepath.Join(dir, "committed")
	require.NoError(t, os.Remove(root))
	require.NoError(t, os.WriteFile(root, []byte("blocker"), 0600))
	require.Error(t, w.CommitTxn("intent"))
	require.NoError(t, w.Close())
	requireOneTxnDir(t, filepath.Join(dir, ".ksink-pending", "intent-*"))

	require.NoError(t, os.Remove(root))
	require.NoError(t, os.Mkdir(root, 0700))
	next, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	committed := requireOneTxnDir(t, filepath.Join(dir, "committed", "intent-*"))
	content, err := os.ReadFile(filepath.Join(committed, "out-intent.bin"))
	require.NoError(t, err)
	require.Equal(t, "committed", string(content))
}

func TestTxnFileStartupDoesNotEraseCorruptCommitIntent(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.bin")
	w, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	require.NoError(t, w.Write([]byte("payload"), &ksink.Message{TransactionalID: "corrupt"}))
	stage := requireOneTxnDir(t, filepath.Join(dir, ".ksink-pending", "corrupt-*"))
	require.NoError(t, os.WriteFile(filepath.Join(stage, txnCommitIntentFile), []byte("not-json"), 0600))
	require.NoError(t, syncTxnDirectory(stage))
	crashFileWriter(t, w)

	_, err = NewTxnFileWriter(pattern)
	require.ErrorContains(t, err, "parse transaction commit intent")
	require.FileExists(t, filepath.Join(stage, "out-corrupt.bin"))
	require.FileExists(t, filepath.Join(stage, txnCommitIntentFile))
}

func TestTxnFileStartupAfterRenameDoesNotDuplicateCommit(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")
	w, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	require.NoError(t, w.Write([]byte("unchanged"), &ksink.Message{TransactionalID: "moved"}))
	committedRoot := filepath.Join(dir, "committed")
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.WriteFile(committedRoot, []byte("blocker"), 0600))
	require.Error(t, w.CommitTxn("moved"))
	stage := requireOneTxnDir(t, filepath.Join(dir, ".ksink-pending", "moved-*"))
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.Mkdir(committedRoot, 0700))
	committed := filepath.Join(committedRoot, filepath.Base(stage))
	// Simulate a successful rename followed by a process crash before the
	// EndTxn response was sent.
	require.NoError(t, os.Rename(stage, committed))
	crashFileWriter(t, w)

	after, err := NewTxnFileWriter(pattern)
	require.NoError(t, err)
	defer func() { _ = after.Close() }()
	data, err := os.ReadFile(filepath.Join(committed, "out-moved.jsonl"))
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(data))
	paths, err := filepath.Glob(filepath.Join(dir, "committed", "moved-*"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
}
