package ksink

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testOpenWALWithActiveTransaction(t *testing.T, root string) (*TransactionLog, string) {
	t.Helper()
	wal, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.NoError(t, wal.BindOutput(filepath.Join(root, "{topic}-{txnID}.bin")))
	instance, err := newTxnInstance("orders-producer")
	require.NoError(t, err)
	snap := wal.CurrentState()
	snap.States["orders-producer"] = storedTxnState{
		ProducerID: snap.Counter, Epoch: 1, Active: true, Instance: instance,
		Sequences: make(map[string]txnPartitionSequence),
	}
	require.NoError(t, wal.AppendState(snap))
	return wal, instance
}

func TestTransactionWALReplayAndBinding(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("first\n")))
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("second\n")))
	require.ErrorContains(t, wal.BindOutput(filepath.Join(root, "different-{txnID}.bin")), "binding changed")
	require.NoError(t, wal.Close())

	restarted, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = restarted.Close() }()
	state := restarted.CurrentState()
	require.True(t, state.States["orders-producer"].Active)
	transactions := restarted.RecoveryTransactions()
	require.Len(t, transactions, 1)
	require.Equal(t, instance, transactions[0].Instance)
	require.Empty(t, transactions[0].Decision)
	require.Len(t, transactions[0].Records, 2)
	var got []string
	require.NoError(t, restarted.StreamOutput(instance, func(rec WALOutputRecord) error {
		got = append(got, string(rec.Data))
		return nil
	}))
	require.Equal(t, []string{"first\n", "second\n"}, got)

	// Returned state is isolated from the authoritative log.
	delete(state.States, "orders-producer")
	require.Contains(t, restarted.CurrentState().States, "orders-producer")
}

func TestTransactionWALIrreversibleCommitAndCompaction(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", []byte("payload")))
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.Equal(t, "commit", wal.Decision("orders-producer", instance))
	require.ErrorContains(t, wal.MarkAborted("orders-producer", instance), "cannot abort")
	require.NoError(t, wal.MarkPublished("orders-producer", instance))
	require.True(t, wal.Published(instance))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	recovery := next.RecoveryTransactions()
	require.Len(t, recovery, 1)
	require.Equal(t, "commit", recovery[0].Decision)
	require.True(t, recovery[0].Published)
	require.True(t, recovery[0].PublishedHasOutput)
	require.Empty(t, recovery[0].Records, "published data can be reclaimed after checkpoint")
	require.ErrorContains(t, next.MarkAborted("orders-producer", instance), "cannot abort")
}

func TestTransactionWALCheckpointPreservesUnpublishedPayload(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	data := []byte("replay me")
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "orders-orders-producer.bin", data))
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.NoError(t, wal.Compact())
	require.NoError(t, wal.Close())

	next, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = next.Close() }()
	recovery := next.RecoveryTransactions()
	require.Len(t, recovery, 1)
	require.Equal(t, "commit", recovery[0].Decision)
	require.False(t, recovery[0].Published)
	require.Len(t, recovery[0].Records, 1)
	var replayed [][]byte
	require.NoError(t, next.StreamOutput(instance, func(rec WALOutputRecord) error {
		replayed = append(replayed, rec.Data)
		return nil
	}))
	require.Equal(t, [][]byte{data}, replayed)
}

func TestTransactionWALTornTailTruncatedAndChecksumCorruptionRejected(t *testing.T) {
	root := t.TempDir()
	wal, _ := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.Close())
	path := wal.walPath(wal.activeKey())
	before, err := os.Stat(path)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 40, 0}) // torn header
	require.NoError(t, err)
	require.NoError(t, f.Close())

	again, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	require.NoError(t, again.Close())
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size(), "recover only complete checksummed records")

	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], 3)
	_, err = f.Write(append(hdr[:], []byte("bad")...)) // full, corrupt frame
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestTransactionWALRejectsUndecidedPublicationAndBadOutputPath(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	defer func() { _ = wal.Close() }()
	require.ErrorContains(t, wal.MarkPublished("orders-producer", instance), "no durable commit decision")
	require.ErrorContains(t, wal.AppendOutput("orders-producer", instance, "../escaped", []byte("x")), "invalid")
	require.ErrorContains(t, wal.AppendOutput("orders-producer", instance, "/absolute", []byte("x")), "invalid")
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "safe.bin", []byte("x")))
	require.ErrorContains(t, wal.MarkPublished("orders-producer", "another-instance"), "no durable commit decision")
}

func TestTransactionWALMissingEstablishedLogFailsClosed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".ksink-kafka-state")
	wal, err := OpenTransactionLog(dir)
	require.NoError(t, err)
	walPath:=wal.walPath(wal.activeKey())
	require.NoError(t, wal.Close())
	require.NoError(t, os.Remove(walPath))
	_, err = OpenTransactionLog(dir)
	require.ErrorContains(t, err, "required WAL segment")
	require.NoError(t, os.WriteFile(walPath, nil, 0600))
	_, err = OpenTransactionLog(dir)
	require.ErrorContains(t, err, "lost its producer state")
}

func TestTransactionWALCompactionRequiresMeaningfulReclamation(t *testing.T) {
	// A large live transaction cannot be rewritten just because the WAL
	// exceeds the former unconditional 64 MiB trigger.
	require.False(t, shouldCompactWAL(1<<30, 0))
	require.False(t, shouldCompactWAL(1<<30, 64<<20))
	require.False(t, shouldCompactWAL(1<<30, 200<<20))
	require.True(t, shouldCompactWAL(1<<30, 256<<20))

	// Reclaimable space must be significant in absolute terms as well.
	require.False(t, shouldCompactWAL(2<<20, 1<<20))
	require.False(t, shouldCompactWAL(64<<20, 15<<20))
	require.True(t, shouldCompactWAL(64<<20, 16<<20))
	require.True(t, shouldCompactWAL(24<<20, 20<<20))
}

func TestTransactionWALDoesNotRepeatedlyCompactLargeLiveTransaction(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_ = wal.Close()}()
	// Binary payloads fill two segments without causing a live-data rewrite.
	data:=bytes.Repeat([]byte("x"),33<<20)
	require.NoError(t,wal.AppendOutput("orders-producer",instance,"one.bin",data))
	first:=wal.walPath(wal.activeKey())
	before,err:=os.Stat(first);require.NoError(t,err)
	require.NoError(t,wal.AppendOutput("orders-producer",instance,"two.bin",data))
	require.Len(t,wal.manifest.Segments,2)
	after,err:=os.Stat(first);require.NoError(t,err)
	require.True(t,os.SameFile(before,after),"rotation seals the old file without rewriting it")
	require.Greater(t,wal.size,int64(64<<20))
	require.Less(t,wal.reclaimable,int64(transactionWALMinReclaim))
	state:=wal.CurrentState()
	st:=state.States["orders-producer"];st.Pending="commit";state.States["orders-producer"]=st
	require.NoError(t,wal.AppendState(state))
	require.NoError(t,wal.MarkPublished("orders-producer",instance))
	require.Less(t,wal.size,int64(1<<20),"published data should be reclaimed at completion")
	require.True(t,wal.Published(instance))
	require.Zero(t,wal.reclaimable)
	require.NoError(t,wal.Close())
	reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err);defer func(){_ = reopened.Close()}()
	entries:=reopened.RecoveryTransactions()
	require.Len(t,entries,1);require.True(t,entries[0].Published)
	require.True(t,entries[0].PublishedHasOutput);require.Empty(t,entries[0].Records)
}

func TestTransactionWALCompactsImmediatelyAfterSubstantialAbort(t *testing.T) {
	root:=t.TempDir()
	wal,instance:=testOpenWALWithActiveTransaction(t,root)
	defer func(){_ = wal.Close()}()
	data:=bytes.Repeat([]byte("a"),16<<20)
	require.NoError(t,wal.AppendOutput("orders-producer",instance,"aborted.bin",data))
	require.Greater(t,wal.size,int64(transactionWALMinReclaim))
	state:=wal.CurrentState()
	st:=state.States["orders-producer"];st.Pending="abort";state.States["orders-producer"]=st
	require.NoError(t,wal.AppendState(state))
	require.NoError(t,wal.MarkAborted("orders-producer",instance))
	require.Less(t,wal.size,int64(1<<20))
	require.True(t,wal.Aborted(instance))
	require.Zero(t,wal.reclaimable)
	require.NoError(t,wal.Close())
	reopened,err:=OpenTransactionLog(filepath.Join(root,".ksink-kafka-state"))
	require.NoError(t,err);defer func(){_ = reopened.Close()}()
	require.True(t,reopened.Aborted(instance))
	require.Empty(t,reopened.RecoveryTransactions()[0].Records)
}

func TestTransactionWALReplayRetainsReclaimableAccounting(t *testing.T) {
	root := t.TempDir()
	wal, instance := testOpenWALWithActiveTransaction(t, root)
	require.NoError(t, wal.AppendOutput("orders-producer", instance, "discard.bin", []byte("abc")))
	state := wal.CurrentState()
	st := state.States["orders-producer"]
	st.Pending = "commit"
	state.States["orders-producer"] = st
	require.NoError(t, wal.AppendState(state))
	require.NoError(t, wal.MarkPublished("orders-producer", instance))
	beforeSize, beforeReclaimable := wal.size, wal.reclaimable
	require.Greater(t, beforeReclaimable, int64(0))
	require.NoError(t, wal.Close())
	reopened, err := OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	require.Equal(t, beforeSize, reopened.size)
	require.Equal(t, beforeReclaimable, reopened.reclaimable)
	// An explicit maintenance compaction still works with counters rebuilt.
	require.NoError(t, reopened.Compact())
	require.Zero(t, reopened.reclaimable)
}
