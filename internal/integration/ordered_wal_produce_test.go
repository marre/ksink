package ksink_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestOrderedWALKafkaProducerWritesTopicOffsetsAndCommitsWithoutFileOutput(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	wal, err := ksink.OpenTransactionLog(stateDir)
	require.NoError(t, err)
	pattern := filepath.Join(root, "{topic}", "messages-{seq:08}.jsonl")
	require.NoError(t, wal.BindOutput(pattern))

	formatter := func(msg *ksink.Message) ([]byte, error) {
		return []byte(fmt.Sprintf("%s/%d: %s\n", msg.Topic, msg.Offset, msg.Value)), nil
	}
	srv, err := ksink.New(ksink.Config{
		Address: "127.0.0.1:0", TransactionalWrite: true, Timeout: 5 * time.Second,
	}, ksink.WithTransactionLog(wal), ksink.WithOrderedTransactionalOutput(formatter),
		ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(srv.Addr().String()),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("multi-topic-ordered"),
	)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction())
	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: "orders", Value: []byte("order-1")},
		&kgo.Record{Topic: "payments", Value: []byte("payment-1")},
	)
	require.NoError(t, results.FirstErr())
	require.NoError(t, client.EndTransaction(ctx, kgo.TryCommit))
	client.Close()

	require.Equal(t, int64(1), wal.NextTopicOffset("orders"))
	require.Equal(t, int64(1), wal.NextTopicOffset("payments"))
	require.Equal(t, int64(1), wal.TopicStableOffset("orders"))
	require.Equal(t, int64(1), wal.TopicStableOffset("payments"))
	for topic, want := range map[string]string{
		"orders": "orders/0: order-1\n",
		"payments": "payments/0: payment-1\n",
	} {
		record, exists, readErr := wal.ReadTopicRecord(topic, 0)
		require.NoError(t, readErr)
		require.True(t, exists)
		require.Equal(t, want, string(record.Data))
		require.Equal(t, int64(0), record.Offset)
		require.NotEmpty(t, record.Instance)
		require.Equal(t, "commit", wal.TopicTransactionDecision(record.Instance))
	}

	require.NoError(t, srv.Close(context.Background()))
	require.NoError(t, wal.Close())
	reopened, err := ksink.OpenTransactionLog(stateDir)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	// A durable Kafka EndTxn has succeeded even though no output files have
	// been materialized; recovery cannot mistake it for an open transaction.
	restarted, err := ksink.New(ksink.Config{
		Address: "127.0.0.1:0", TransactionalWrite: true,
	}, ksink.WithTransactionLog(reopened), ksink.WithOrderedTransactionalOutput(formatter),
		ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)
	require.NoError(t, restarted.Start(context.Background()))
	defer func() { _ = restarted.Close(context.Background()) }()
	require.Equal(t, int64(1), reopened.TopicStableOffset("orders"))
	require.Equal(t, int64(1), reopened.TopicStableOffset("payments"))
	for _, topic := range []string{"orders", "payments"} {
		record, exists, readErr := reopened.ReadTopicRecord(topic, 0)
		require.NoError(t, readErr)
		require.True(t, exists)
		require.Equal(t, "commit", reopened.TopicTransactionDecision(record.Instance))
	}
}
