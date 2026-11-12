package output_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "messages-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.Write([]byte("hello\n"), &ksink.Message{TransactionalID: "txn-1"}))
	staged := oneMatch(t, filepath.Join(dir, ".ksink-pending", "txn-1-*", "messages-txn-1.jsonl"))
	noMatches(t, filepath.Join(dir, "committed", "*"))
	require.NoError(t, w.CommitTxn("txn-1"))

	noMatches(t, filepath.Join(dir, ".ksink-pending", "txn-1-*"))
	committed := oneMatch(t, filepath.Join(dir, "committed", "txn-1-*", "messages-txn-1.jsonl"))
	data, err := os.ReadFile(committed)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(data))
	requireNoFile(t, staged)
}

func TestTxnFileWriterAbort(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "messages-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.Write([]byte("discard\n"), &ksink.Message{TransactionalID: "txn-abort"}))
	oneMatch(t, filepath.Join(dir, ".ksink-pending", "txn-abort-*", "*.jsonl"))
	require.NoError(t, w.AbortTxn("txn-abort"))
	noMatches(t, filepath.Join(dir, ".ksink-pending", "txn-abort-*"))
	noMatches(t, filepath.Join(dir, "committed", "txn-abort-*"))
}

func TestTxnFileWriterRejectsNonTransactionalWrite(t *testing.T) {
	w, err := output.NewTxnFileWriter(filepath.Join(t.TempDir(), "out-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.ErrorContains(t, w.Write([]byte("plain"), &ksink.Message{}), "TransactionalID")
	require.ErrorContains(t, w.Write([]byte("plain"), nil), "TransactionalID")
}

func TestTxnFileWriterMultipleTransactions(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "out-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.Write([]byte("a\n"), &ksink.Message{TransactionalID: "t1"}))
	require.NoError(t, w.Write([]byte("b\n"), &ksink.Message{TransactionalID: "t2"}))
	require.NoError(t, w.Write([]byte("c\n"), &ksink.Message{TransactionalID: "t1"}))
	require.NoError(t, w.CommitTxn("t1"))
	require.NoError(t, w.AbortTxn("t2"))

	got, err := os.ReadFile(oneMatch(t, filepath.Join(dir, "committed", "t1-*", "out-t1.jsonl")))
	require.NoError(t, err)
	require.Equal(t, "a\nc\n", string(got))
	noMatches(t, filepath.Join(dir, "committed", "t2-*"))
	noMatches(t, filepath.Join(dir, ".ksink-pending", "t2-*"))
}

func TestTxnFileWriterCloseRemovesUncommitted(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "out-{txnID}.bin"))
	require.NoError(t, err)
	require.NoError(t, w.Write([]byte("saved\n"), &ksink.Message{TransactionalID: "saved"}))
	require.NoError(t, w.CommitTxn("saved"))
	require.NoError(t, w.Write([]byte("pending\n"), &ksink.Message{TransactionalID: "open-txn"}))
	oneMatch(t, filepath.Join(dir, ".ksink-pending", "open-txn-*"))
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())

	noMatches(t, filepath.Join(dir, ".ksink-pending", "open-txn-*"))
	oneMatch(t, filepath.Join(dir, "committed", "saved-*", "out-saved.bin"))
	require.ErrorContains(t, w.Write([]byte("late"), &ksink.Message{TransactionalID: "late"}), "closed")
}

func TestTxnFileWriterCommitNoData(t *testing.T) {
	w, err := output.NewTxnFileWriter(filepath.Join(t.TempDir(), "out-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.CommitTxn("nonexistent"))
	require.NoError(t, w.AbortTxn("nonexistent"))
}

func TestTxnFileWriterSanitizesTxnID(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "out-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Write([]byte("data\n"), &ksink.Message{TransactionalID: "../../etc/evil"}))
	require.NoError(t, w.CommitTxn("../../etc/evil"))
	oneMatch(t, filepath.Join(dir, "committed", "evil-*", "out-evil.jsonl"))
	noMatches(t, filepath.Join(dir, "..", "..", "etc", "evil"))
}

func TestTxnFileWriterNeverReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "out-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	original := filepath.Join(dir, "out-reuse.jsonl")
	require.NoError(t, os.WriteFile(original, []byte("old\n"), 0600))
	msg := &ksink.Message{TransactionalID: "reuse"}
	require.NoError(t, w.Write([]byte("first\n"), msg))
	require.NoError(t, w.CommitTxn("reuse"))
	require.NoError(t, w.Write([]byte("second\n"), msg))
	require.NoError(t, w.CommitTxn("reuse"))

	data, err := os.ReadFile(original)
	require.NoError(t, err)
	require.Equal(t, "old\n", string(data))
	paths, err := filepath.Glob(filepath.Join(dir, "committed", "reuse-*", "out-reuse.jsonl"))
	require.NoError(t, err)
	require.Len(t, paths, 2)
	contents := make([]string, 0, len(paths))
	for _, path := range paths {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		contents = append(contents, string(b))
	}
	require.ElementsMatch(t, []string{"first\n", "second\n"}, contents)
}

func TestTxnFileWriterCommitCanRetryAfterDirectoryPublishFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "{topic}-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	txnID := "retry"
	for _, topic := range []string{"orders", "payments"} {
		require.NoError(t, w.Write([]byte(topic+"\n"), &ksink.Message{TransactionalID: txnID, Topic: topic}))
	}

	// Replace the committed root with a file to force the single rename to
	// fail. No subset of the transaction may become visible.
	committedRoot := filepath.Join(dir, "committed")
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.WriteFile(committedRoot, []byte("blocker"), 0600))
	require.Error(t, w.CommitTxn(txnID))
	require.ErrorContains(t, w.Write([]byte("blocked"), &ksink.Message{TransactionalID: txnID}), "pending commit")
	oneMatch(t, filepath.Join(dir, ".ksink-pending", "retry-*", "orders-retry.jsonl"))
	oneMatch(t, filepath.Join(dir, ".ksink-pending", "retry-*", "payments-retry.jsonl"))
	noMatches(t, filepath.Join(dir, "committed", "retry-*"))

	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.Mkdir(committedRoot, 0700))
	require.NoError(t, w.CommitTxn(txnID))
	dirPath := oneMatch(t, filepath.Join(dir, "committed", "retry-*"))
	for _, topic := range []string{"orders", "payments"} {
		b, err := os.ReadFile(filepath.Join(dirPath, topic+"-retry.jsonl"))
		require.NoError(t, err)
		require.Equal(t, topic+"\n", string(b))
	}
	noMatches(t, filepath.Join(dir, ".ksink-pending", "retry-*"))
}

func TestTxnFileWriterAbortAfterFailedMultiFileCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "{topic}-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	for _, topic := range []string{"orders", "payments"} {
		require.NoError(t, w.Write([]byte(topic), &ksink.Message{TransactionalID: "multi", Topic: topic}))
	}
	committedRoot := filepath.Join(dir, "committed")
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.WriteFile(committedRoot, []byte("blocker"), 0600))
	require.Error(t, w.CommitTxn("multi"))
	require.NoError(t, w.AbortTxn("multi"))
	noMatches(t, filepath.Join(dir, ".ksink-pending", "multi-*"))
	require.NoError(t, os.Remove(committedRoot))
	require.NoError(t, os.Mkdir(committedRoot, 0700))
	noMatches(t, filepath.Join(dir, "committed", "multi-*"))
}

func TestTxnFileWriterAllTopicsVisibleAtOnce(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "{topic}", "{partition}-{txnID}.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	const count = 50
	for i := 0; i < count; i++ {
		msg := &ksink.Message{TransactionalID: "atomic", Topic: fmt.Sprintf("topic-%d", i), Partition: int32(i)}
		require.NoError(t, w.Write([]byte(fmt.Sprintf("%d\n", i)), msg))
	}

	// A concurrent observer sees zero transaction directories, or a complete
	// directory with every file. There is never a partially published set.
	var mu sync.Mutex
	var failures []string
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			dirs, err := filepath.Glob(filepath.Join(dir, "committed", "atomic-*"))
			if err != nil {
				mu.Lock()
				failures = append(failures, err.Error())
				mu.Unlock()
				return
			}
			for _, transactionDir := range dirs {
				for i := 0; i < count; i++ {
					path := filepath.Join(transactionDir, fmt.Sprintf("topic-%d", i), fmt.Sprintf("%d-atomic.jsonl", i))
					if _, err := os.Stat(path); err != nil {
						mu.Lock()
						failures = append(failures, path)
						mu.Unlock()
						return
					}
				}
			}
		}
	}()
	require.NoError(t, w.CommitTxn("atomic"))
	committed := oneMatch(t, filepath.Join(dir, "committed", "atomic-*"))
	for i := 0; i < count; i++ {
		require.FileExists(t, filepath.Join(committed, fmt.Sprintf("topic-%d", i), fmt.Sprintf("%d-atomic.jsonl", i)))
	}
	close(done)
	wg.Wait()
	require.Empty(t, failures, "reader observed a partially published transaction")
}

func TestTxnFileWriterPreservesSubdirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "static", "{topic}", "part-{partition}-{txnID}.bin"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Write([]byte("foo"), &ksink.Message{TransactionalID: "nested", Topic: "orders", Partition: 2}))
	require.NoError(t, w.CommitTxn("nested"))
	require.FileExists(t, oneMatch(t, filepath.Join(dir, "static", "committed", "nested-*", "orders", "part-2-nested.bin")))
	noMatches(t, filepath.Join(dir, "committed", "*")) // fixed prefix determines root
}

func TestTxnFileWriterWriteAfterCommitIsNewTransaction(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewTxnFileWriter(filepath.Join(dir, "out-{txnID}.bin"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	msg := &ksink.Message{TransactionalID: "repeat"}
	for i := range 3 {
		require.NoError(t, w.Write([]byte(fmt.Sprint(i)), msg))
		require.NoError(t, w.CommitTxn("repeat"))
	}
	dirs, err := filepath.Glob(filepath.Join(dir, "committed", "repeat-*"))
	require.NoError(t, err)
	require.Len(t, dirs, 3)
	contents := make([]string, 0, len(dirs))
	for _, d := range dirs {
		b, err := os.ReadFile(filepath.Join(d, "out-repeat.bin"))
		require.NoError(t, err)
		contents = append(contents, string(b))
	}
	sort.Strings(contents)
	require.Equal(t, []string{"0", "1", "2"}, contents)
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
	require.Empty(t, paths, "unexpected matches for %q", pattern)
}

func requireNoFile(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "expected %s to not exist, but it does", path)
}
