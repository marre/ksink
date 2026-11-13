package ksink_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This exercises the exact crash window which the independent journals in
// PR #51/#52 could not handle: Kafka has durably decided COMMIT but the
// filesystem writer has not even begun publishing the transaction.
func TestFranzSharedWALRecoversCommitBeforePublication(t *testing.T) {
	root := t.TempDir()
	stateDir := output.KafkaTransactionStateDir(filepath.Join(root, "{topic}-{txnID}.jsonl"))
	pattern := filepath.Join(root, "{topic}-{txnID}.jsonl")
	wal, err := ksink.OpenTransactionLog(stateDir)
	require.NoError(t, err)
	writer, err := output.NewTxnFileWriter(pattern, wal)
	require.NoError(t, err)

	addr := fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
	srv, err := ksink.New(ksink.Config{
		Address: addr, TransactionalWrite: true, Timeout: 5 * time.Second,
	}, ksink.WithTransactionLog(wal), ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	waitForTCPReady(t, srv.Addr().String(), 5*time.Second)

	readCtx, cancelRead := context.WithCancel(context.Background())
	commitDecisionSeen := make(chan struct{}, 1)
	readLoopFinished := make(chan struct{})
	go func() {
		defer close(readLoopFinished)
		for {
			event, ack, err := srv.Read(readCtx)
			if err != nil {
				return
			}
			switch e := event.(type) {
			case *ksink.MessagesEvent:
				var writeErr error
				for _, msg := range e.Messages {
					if writeErr = writer.Write(append(append([]byte(nil), msg.Value...), '\n'), msg); writeErr != nil {
						break
					}
				}
				if writeErr != nil {
					t.Logf("WAL materializer message error: %v", writeErr)
				}
				ack(writeErr)
			case *ksink.TxnCommitEvent:
				select {
				case commitDecisionSeen <- struct{}{}:
				default:
				}
				// Simulate crash between durable coordinator decision and
				// the first instruction of filesystem publication.
				ack(errors.New("process terminated before output materialization"))
			case *ksink.TxnAbortEvent:
				ack(writer.AbortTxn(e.TransactionalID))
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("decided-commit"),
	)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction())
	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: "orders", Value: []byte("order-1")},
		&kgo.Record{Topic: "payments", Value: []byte("payment-1")},
	)
	for _, result := range results {
		require.NoError(t, result.Err)
	}

	commitDone := make(chan error, 1)
	go func() { commitDone <- client.EndTransaction(ctx, kgo.TryCommit) }()
	select {
	case <-commitDecisionSeen:
	case <-ctx.Done():
		t.Fatal("transaction COMMIT decision was not delivered")
	}
	recoveryBefore := wal.RecoveryTransactions()
	require.Len(t, recoveryBefore, 1)
	require.Equal(t, "commit", recoveryBefore[0].Decision)
	require.Len(t, recoveryBefore[0].Records, 2)
	require.False(t, recoveryBefore[0].Published)
	publishedBefore, err := filepath.Glob(filepath.Join(root, "committed", "decided-commit-*"))
	require.NoError(t, err)
	require.Empty(t, publishedBefore)

	// Close components in their normal shutdown order but do not invoke
	// writer.CommitTxn. No filesystem-specific commit intent exists.
	cancel()
	client.Close()
	cancelRead()
	require.NoError(t, srv.Close(context.Background()))
	<-readLoopFinished
	require.NoError(t, writer.Close())
	require.NoError(t, wal.Close())
	select {
	case <-commitDone:
	default:
	}

	reopened, err := ksink.OpenTransactionLog(stateDir)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	recoveredWriter, err := output.NewTxnFileWriter(pattern, reopened)
	require.NoError(t, err)
	defer func() { _ = recoveredWriter.Close() }()
	committedDirs, err := filepath.Glob(filepath.Join(root, "committed", "decided-commit-*"))
	require.NoError(t, err)
	require.Len(t, committedDirs, 1, "the WAL must replay the whole durable commit exactly once")
	expected := map[string]string{
		"orders-decided-commit.jsonl":   "order-1\n",
		"payments-decided-commit.jsonl": "payment-1\n",
	}
	for filename, want := range expected {
		data, err := os.ReadFile(filepath.Join(committedDirs[0], filename))
		require.NoError(t, err)
		require.Equal(t, want, string(data))
	}
	after := reopened.RecoveryTransactions()
	require.Len(t, after, 1)
	require.True(t, after[0].Published)

	// The Kafka coordinator is now able to recognize the completed commit.
	recoveredServer, err := ksink.New(ksink.Config{
		Address: "127.0.0.1:0", TransactionalWrite: true,
	}, ksink.WithTransactionLog(reopened))
	require.NoError(t, err)
	require.NoError(t, recoveredServer.Close(context.Background()))
	require.NoError(t, reopened.Compact())
	// Reopening the WAL and writer cannot produce a second directory.
	require.NoError(t, recoveredWriter.Close())
	require.NoError(t, reopened.Close())
	last, err := ksink.OpenTransactionLog(stateDir)
	require.NoError(t, err)
	defer func() { _ = last.Close() }()
	lastWriter, err := output.NewTxnFileWriter(pattern, last)
	require.NoError(t, err)
	defer func() { _ = lastWriter.Close() }()
	committedDirs, err = filepath.Glob(filepath.Join(root, "committed", "decided-commit-*"))
	require.NoError(t, err)
	require.Len(t, committedDirs, 1)
}

func TestSharedWALRestartDiscardsUndecidedStaging(t *testing.T) {
	root := t.TempDir()
	pattern := filepath.Join(root, "out-{txnID}.bin")
	wal, err := ksink.OpenTransactionLog(output.KafkaTransactionStateDir(pattern))
	require.NoError(t, err)
	writer, err := output.NewTxnFileWriter(pattern, wal)
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
	srv, err := ksink.New(ksink.Config{Address: addr, TransactionalWrite: true},
		ksink.WithTransactionLog(wal), ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	waitForTCPReady(t, srv.Addr().String(), 5*time.Second)
	startTxnReadLoop(t, srv, writer)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second), kgo.TransactionalID("undecided"))
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction())
	results := client.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("never-committed")})
	require.NoError(t, results[0].Err)
	instances := wal.RecoveryTransactions()
	require.Len(t, instances, 1)
	require.Empty(t, instances[0].Decision)
	client.Close()
	require.NoError(t, srv.Close(context.Background()))
	require.NoError(t, writer.Close())
	require.NoError(t, wal.Close())

	newLog, err := ksink.OpenTransactionLog(output.KafkaTransactionStateDir(pattern))
	require.NoError(t, err)
	defer func() { _ = newLog.Close() }()
	newWriter, err := output.NewTxnFileWriter(pattern, newLog)
	require.NoError(t, err)
	defer func() { _ = newWriter.Close() }()
	require.True(t, newLog.Aborted(instances[0].Instance),
		"startup must mark undecided output as aborted so its WAL bytes are reclaimable")
	pending, err := filepath.Glob(filepath.Join(root, ".ksink-pending", "undecided-*"))
	require.NoError(t, err)
	require.Empty(t, pending)
	committed, err := filepath.Glob(filepath.Join(root, "committed", "undecided-*"))
	require.NoError(t, err)
	require.Empty(t, committed)
}
