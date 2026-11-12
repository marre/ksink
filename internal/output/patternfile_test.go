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
	ordersTmp := oneMatch(t, filepath.Join(dir, "orders-txn-1-*.jsonl.tmp"))
	eventsTmp := oneMatch(t, filepath.Join(dir, "events-txn-1-*.jsonl.tmp"))

	require.NoError(t, w.CommitTxn("txn-1"))

	// After commit: both committed files should exist.
	data1, err := os.ReadFile(strings.TrimSuffix(ordersTmp, ".tmp"))
	require.NoError(t, err)
	assert.Equal(t, "order\n", string(data1))

	data2, err := os.ReadFile(strings.TrimSuffix(eventsTmp, ".tmp"))
	require.NoError(t, err)
	assert.Equal(t, "event\n", string(data2))

	// Temp files should be gone.
	requireNoFile(t, ordersTmp)
	requireNoFile(t, eventsTmp)
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
	noMatches(t, filepath.Join(dir, "orders-txn-abort-*.jsonl.tmp"))
	noMatches(t, filepath.Join(dir, "orders-txn-abort-*.jsonl"))
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
  b, err := os.ReadFile(oneMatch(t,filepath.Join(dir,path)))
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

func TestTxnFileWriterRoutesByPartitionCommitAbortAndClose(t *testing.T) {
 dir := t.TempDir()
 w, err := output.NewTxnFileWriter(filepath.Join(dir, "{topic}-{partition}-{txnID}.jsonl"))
 require.NoError(t, err)
 for _,tc:=range []struct{txn,topic,data string;partition int32}{
  {"commit","orders","one\n",0},
  {"commit","orders","two\n",1},
  {"commit","orders","three\n",0},
  {"commit","events","event\n",0},
  {"abort","orders","discard\n",3},
  {"pending","orders","pending\n",4},
 } {
  require.NoError(t,w.Write([]byte(tc.data), &ksink.Message{TransactionalID:tc.txn,Topic:tc.topic,Partition:tc.partition}))
 }
 require.FileExists(t,oneMatch(t,filepath.Join(dir,"orders-0-commit-*.jsonl.tmp")))
 require.FileExists(t,oneMatch(t,filepath.Join(dir,"orders-1-commit-*.jsonl.tmp")))
 require.NoError(t,w.CommitTxn("commit"))
 require.NoError(t,w.AbortTxn("abort"))
 for path,want:=range map[string]string{"orders-0-commit-*.jsonl":"one\nthree\n","orders-1-commit-*.jsonl":"two\n","events-0-commit-*.jsonl":"event\n"} {
  b,err:=os.ReadFile(filepath.Join(dir,path))
  require.NoError(t,err)
  assert.Equal(t,want,string(b))
  noMatches(t,filepath.Join(dir,path+".tmp"))
 }
 noMatches(t,filepath.Join(dir,"orders-3-abort-*.jsonl.tmp"))
 noMatches(t,filepath.Join(dir,"orders-3-abort-*.jsonl"))
 require.NoError(t,w.Close())
 noMatches(t,filepath.Join(dir,"orders-4-pending-*.jsonl.tmp"))
}

func TestTxnFileWriterPartitionOnly(t *testing.T) {
 dir:=t.TempDir()
 w,err:=output.NewTxnFileWriter(filepath.Join(dir,"part-{partition}-{txnID}.bin"))
 require.NoError(t,err)
 require.NoError(t,w.Write([]byte("one"),&ksink.Message{TransactionalID:"x",Topic:"a",Partition:5}))
 require.NoError(t,w.Write([]byte("two"),&ksink.Message{TransactionalID:"x",Topic:"b",Partition:5}))
 require.NoError(t,w.CommitTxn("x"))
 b,err:=os.ReadFile(oneMatch(t,filepath.Join(dir,"part-5-x-*.bin")))
 require.NoError(t,err)
 assert.Equal(t,"onetwo",string(b))
 require.NoError(t,w.Close())
}
