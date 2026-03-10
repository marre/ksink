package ksink_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marre/ksink/internal/format"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xeipuuv/gojsonschema"
)

type Config = ksink.Config
type SASLCredential = ksink.SASLCredential
type Server = ksink.Server
type receivedMessage = integrationReceivedMessage
type messageCapture = integrationMessageCapture

var New = ksink.New
var WithLogger = ksink.WithLogger

// --- Franz-go integration tests ---

func TestFranzBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	testTopic := "test-topic"
	testKey := "test-key"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Key:   []byte(testKey),
		Value: []byte(testValue),
		Headers: []kgo.RecordHeader{
			{Key: "header1", Value: []byte("value1")},
		},
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)

	assert.Equal(t, testTopic, msgs[0].Topic)
	assert.Equal(t, testKey, msgs[0].Key)
	assert.Equal(t, testValue, msgs[0].Value)
	assert.Equal(t, "value1", msgs[0].Headers["header1"])
}

func TestFranzMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	for i := 0; i < 5; i++ {
		record := &kgo.Record{
			Topic: "test-topic",
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results[0].Err)
	}

	msgs := capture.waitForMessages(5, 10*time.Second)
	require.Len(t, msgs, 5)
}

func TestFranzTopicFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		Topics:  []string{"allowed-topic"},
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce to allowed topic
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "allowed-topic",
		Value: []byte("allowed"),
	})
	require.NoError(t, results[0].Err)

	// Produce to disallowed topic should fail
	results = client.ProduceSync(ctx, &kgo.Record{
		Topic: "disallowed-topic",
		Value: []byte("disallowed"),
	})
	require.Error(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "allowed-topic", msgs[0].Topic)
	assert.Equal(t, "allowed", msgs[0].Value)
}

func TestFranzSASLPlain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	})
	capture := startReadLoop(t, srv)

	// With correct credentials
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{
			User: "user1",
			Pass: "pass1",
		}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("authed-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "authed-message", msgs[0].Value)
}

func TestFranzSASLPlainWrongPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{
			User: "user1",
			Pass: "wrongpass",
		}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err)

	// Should not have received any messages
	time.Sleep(500 * time.Millisecond)
	msgs := capture.get()
	assert.Len(t, msgs, 0)
}

func TestFranzSASLScram256(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "scrampass",
		}.AsSha256Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("scram-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "scram-message", msgs[0].Value)
}

func TestFranzSASLScram512(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-512", Username: "scramuser", Password: "scrampass"},
		},
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "scrampass",
		}.AsSha512Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("scram512-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "scram512-message", msgs[0].Value)
}

func TestFranzSASLScramWrongPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(scram.Auth{
			User: "scramuser",
			Pass: "wrongpass",
		}.AsSha256Mechanism()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "scram-topic",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err)

	time.Sleep(500 * time.Millisecond)
	msgs := capture.get()
	assert.Len(t, msgs, 0)
}

func TestFranzMultipleUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
			{Mechanism: "PLAIN", Username: "user2", Password: "pass2"},
		},
	})
	capture := startReadLoop(t, srv)

	// User1
	client1, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{User: "user1", Pass: "pass1"}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client1.Close()

	results := client1.ProduceSync(ctx, &kgo.Record{
		Topic: "user1-topic",
		Value: []byte(`{"user": "user1"}`),
	})
	require.NoError(t, results[0].Err)

	// User2
	client2, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.SASL(plain.Auth{User: "user2", Pass: "pass2"}.AsMechanism()),
	)
	require.NoError(t, err)
	defer client2.Close()

	results = client2.ProduceSync(ctx, &kgo.Record{
		Topic: "user2-topic",
		Value: []byte(`{"user": "user2"}`),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(2, 10*time.Second)
	require.Len(t, msgs, 2)
}

func TestFranzIdempotentProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout:         5 * time.Second,
		IdempotentWrite: true,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "idempotent-topic",
		Value: []byte("idempotent-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "idempotent-message", msgs[0].Value)
}

func TestFranzTransactionalProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout:            5 * time.Second,
		TransactionalWrite: true,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("test-txn-id"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Begin transaction
	err = client.BeginTransaction()
	require.NoError(t, err)

	// Produce within transaction
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "txn-topic",
		Key:   []byte("txn-key"),
		Value: []byte("txn-message"),
	})
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	// Commit transaction
	err = client.EndTransaction(ctx, kgo.TryCommit)
	require.NoError(t, err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "txn-topic", msgs[0].Topic)
	assert.Equal(t, "txn-key", msgs[0].Key)
	assert.Equal(t, "txn-message", msgs[0].Value)
}

func TestFranzTransactionalProducerAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout:            5 * time.Second,
		TransactionalWrite: true,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("test-txn-abort"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Begin transaction
	err = client.BeginTransaction()
	require.NoError(t, err)

	// Produce within transaction
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "txn-topic",
		Value: []byte("aborted-message"),
	})
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	// Abort transaction
	err = client.EndTransaction(ctx, kgo.TryAbort)
	require.NoError(t, err)

	// The message was still delivered (fake transactional support)
	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "aborted-message", msgs[0].Value)
}

// TestFranzTransactionalFileCommit verifies that messages produced inside a
// committed transaction end up in the expected file on disk.
func TestFranzTransactionalFileCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	tw, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { tw.Close() }) //nolint:errcheck

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:            addr,
		Timeout:            5 * time.Second,
		TransactionalWrite: true,
	}, WithLogger(&integrationLogger{t}),
		ksink.WithTxnEndFunc(func(txnID string, commit bool) {
			if commit {
				if err := tw.CommitTxn(txnID); err != nil {
					log.Printf("[ERROR] commit txn %s: %v", txnID, err)
				}
			} else {
				if err := tw.AbortTxn(txnID); err != nil {
					log.Printf("[ERROR] abort txn %s: %v", txnID, err)
				}
			}
		}),
	)
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { srv.Close(context.Background()) }) //nolint:errcheck
	waitForTCPReady(t, addr, 5*time.Second)

	// Start a read loop that writes formatted data through the txn writer.
	readCtx, readCancel := context.WithCancel(context.Background())
	t.Cleanup(readCancel)
	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(readCtx)
			if err != nil {
				return
			}
			var writeErr error
			for _, msg := range msgs {
				data := append([]byte(msg.Value), '\n')
				if werr := tw.Write(data, msg); werr != nil {
					writeErr = werr
					break
				}
			}
			ack(writeErr)
		}
	}()

	// Produce two messages in a committed transaction.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("commit-txn"),
	)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.BeginTransaction())
	results := client.ProduceSync(ctx,
		&kgo.Record{Topic: "txn-topic", Value: []byte("msg-1")},
		&kgo.Record{Topic: "txn-topic", Value: []byte("msg-2")},
	)
	for _, r := range results {
		require.NoError(t, r.Err)
	}
	require.NoError(t, client.EndTransaction(ctx, kgo.TryCommit))

	// Wait for the committed file to appear.
	committedPath := filepath.Join(dir, "out-commit-txn.jsonl")
	deadline := time.Now().Add(5 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		data, err = os.ReadFile(committedPath)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err, "committed file should exist after commit")
	assert.Equal(t, "msg-1\nmsg-2\n", string(data))

	// Temp file should be gone.
	_, err = os.Stat(committedPath + ".tmp")
	assert.True(t, os.IsNotExist(err), "temp file should be removed after commit")
}

// TestFranzTransactionalFileAbort verifies that messages produced inside an
// aborted transaction are discarded and the temp file is removed.
func TestFranzTransactionalFileAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	tw, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { tw.Close() }) //nolint:errcheck

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:            addr,
		Timeout:            5 * time.Second,
		TransactionalWrite: true,
	}, WithLogger(&integrationLogger{t}),
		ksink.WithTxnEndFunc(func(txnID string, commit bool) {
			if commit {
				if err := tw.CommitTxn(txnID); err != nil {
					log.Printf("[ERROR] commit txn %s: %v", txnID, err)
				}
			} else {
				if err := tw.AbortTxn(txnID); err != nil {
					log.Printf("[ERROR] abort txn %s: %v", txnID, err)
				}
			}
		}),
	)
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { srv.Close(context.Background()) }) //nolint:errcheck
	waitForTCPReady(t, addr, 5*time.Second)

	readCtx, readCancel := context.WithCancel(context.Background())
	t.Cleanup(readCancel)
	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(readCtx)
			if err != nil {
				return
			}
			var writeErr error
			for _, msg := range msgs {
				data := append([]byte(msg.Value), '\n')
				if werr := tw.Write(data, msg); werr != nil {
					writeErr = werr
					break
				}
			}
			ack(writeErr)
		}
	}()

	// Produce a message inside an aborted transaction.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("abort-txn"),
	)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.BeginTransaction())
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "txn-topic",
		Value: []byte("should-be-discarded"),
	})
	require.NoError(t, results[0].Err)
	require.NoError(t, client.EndTransaction(ctx, kgo.TryAbort))

	// Give the server time to process the abort callback.
	time.Sleep(500 * time.Millisecond)

	// Neither the committed file nor the temp file should exist.
	committedPath := filepath.Join(dir, "out-abort-txn.jsonl")
	_, err = os.Stat(committedPath)
	assert.True(t, os.IsNotExist(err), "committed file should not exist after abort")

	_, err = os.Stat(committedPath + ".tmp")
	assert.True(t, os.IsNotExist(err), "temp file should not exist after abort")
}

// TestFranzTransactionalFileCommitAndAbort verifies a scenario with two
// concurrent transactions where one is committed and the other is aborted.
func TestFranzTransactionalFileCommitAndAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	pattern := filepath.Join(dir, "out-{txnID}.jsonl")

	tw, err := output.NewTxnFileWriter(pattern)
	require.NoError(t, err)
	t.Cleanup(func() { tw.Close() }) //nolint:errcheck

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:            addr,
		Timeout:            5 * time.Second,
		TransactionalWrite: true,
	}, WithLogger(&integrationLogger{t}),
		ksink.WithTxnEndFunc(func(txnID string, commit bool) {
			if commit {
				if err := tw.CommitTxn(txnID); err != nil {
					log.Printf("[ERROR] commit txn %s: %v", txnID, err)
				}
			} else {
				if err := tw.AbortTxn(txnID); err != nil {
					log.Printf("[ERROR] abort txn %s: %v", txnID, err)
				}
			}
		}),
	)
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { srv.Close(context.Background()) }) //nolint:errcheck
	waitForTCPReady(t, addr, 5*time.Second)

	readCtx, readCancel := context.WithCancel(context.Background())
	t.Cleanup(readCancel)
	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(readCtx)
			if err != nil {
				return
			}
			var writeErr error
			for _, msg := range msgs {
				data := append([]byte(msg.Value), '\n')
				if werr := tw.Write(data, msg); werr != nil {
					writeErr = werr
					break
				}
			}
			ack(writeErr)
		}
	}()

	// Transaction 1: commit
	client1, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("txn-keep"),
	)
	require.NoError(t, err)
	defer client1.Close()

	require.NoError(t, client1.BeginTransaction())
	results := client1.ProduceSync(ctx, &kgo.Record{
		Topic: "txn-topic", Value: []byte("committed-msg"),
	})
	require.NoError(t, results[0].Err)
	require.NoError(t, client1.EndTransaction(ctx, kgo.TryCommit))

	// Transaction 2: abort (using a separate client with a different txn ID)
	client2, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.TransactionalID("txn-discard"),
	)
	require.NoError(t, err)
	defer client2.Close()

	require.NoError(t, client2.BeginTransaction())
	results = client2.ProduceSync(ctx, &kgo.Record{
		Topic: "txn-topic", Value: []byte("aborted-msg"),
	})
	require.NoError(t, results[0].Err)
	require.NoError(t, client2.EndTransaction(ctx, kgo.TryAbort))

	// Wait for the committed file.
	committedPath := filepath.Join(dir, "out-txn-keep.jsonl")
	deadline := time.Now().Add(5 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		data, err = os.ReadFile(committedPath)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err)
	assert.Equal(t, "committed-msg\n", string(data))

	// Give abort time to process.
	time.Sleep(500 * time.Millisecond)

	// The aborted transaction should have no file.
	abortedPath := filepath.Join(dir, "out-txn-discard.jsonl")
	_, err = os.Stat(abortedPath)
	assert.True(t, os.IsNotExist(err), "aborted transaction file should not exist")
	_, err = os.Stat(abortedPath + ".tmp")
	assert.True(t, os.IsNotExist(err), "aborted transaction temp file should not exist")
}

func TestFranzHandlerError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	})

	// Read loop that always acks with error
	startFailReadLoop(t, srv, fmt.Errorf("handler error"))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("should-get-error-response"),
	})
	// The produce should get an error response (UnknownServerError)
	require.Error(t, results[0].Err)
	assert.Contains(t, results[0].Err.Error(), kerr.UnknownServerError.Message)
}

func TestFranzMaxMessageBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout:         5 * time.Second,
		MaxMessageBytes: 100,
	})
	capture := startReadLoop(t, srv)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	// Small message should work
	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "test-topic",
		Value: []byte("small"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
}

func TestFranzTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate CA certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Create server certificate
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Write certificates to temp files
	tmpDir := t.TempDir()

	serverCertFile := tmpDir + "/server-cert.pem"
	require.NoError(t, os.WriteFile(serverCertFile, serverCertPEM, 0600))

	serverKeyFile := tmpDir + "/server-key.pem"
	require.NoError(t, os.WriteFile(serverKeyFile, serverKeyPEM, 0600))

	capture := &messageCapture{}
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:  addr,
		CertFile: serverCertFile,
		KeyFile:  serverKeyFile,
		Timeout:  5 * time.Second,
	}, WithLogger(&integrationLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	// Start read loop
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(readCtx)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				rm := receivedMessage{
					Topic:   msg.Topic,
					Value:   string(msg.Value),
					Headers: make(map[string]string),
				}
				if msg.Key != nil {
					rm.Key = string(msg.Key)
				}
				for k, v := range msg.Headers {
					rm.Headers[k] = v
				}
				capture.add(rm)
			}
			ack(nil)
		}
	}()

	waitForTCPReady(t, addr, 5*time.Second)

	// Connect with TLS
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTLSConfig(tlsConfig),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "tls-topic",
		Value: []byte("tls-message"),
	})
	require.NoError(t, results[0].Err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "tls-message", msgs[0].Value)
}

func TestFranzMTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Server cert
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Client cert
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			CommonName:   "test-client",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, &caTemplate, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	// Write certs to temp files
	tmpDir := t.TempDir()
	serverCertFile := tmpDir + "/server-cert.pem"
	require.NoError(t, os.WriteFile(serverCertFile, serverCertPEM, 0600))
	serverKeyFile := tmpDir + "/server-key.pem"
	require.NoError(t, os.WriteFile(serverKeyFile, serverKeyPEM, 0600))
	clientCAFile := tmpDir + "/client-ca.pem"
	require.NoError(t, os.WriteFile(clientCAFile, caCertPEM, 0600))

	capture := &messageCapture{}
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv, err := New(Config{
		Address:      addr,
		CertFile:     serverCertFile,
		KeyFile:      serverKeyFile,
		MTLSAuth:     "require_and_verify",
		MTLSCAsFiles: []string{clientCAFile},
		Timeout:      5 * time.Second,
	}, WithLogger(&integrationLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	// Start read loop
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(readCtx)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				rm := receivedMessage{
					Topic:   msg.Topic,
					Value:   string(msg.Value),
					Headers: make(map[string]string),
				}
				if msg.Key != nil {
					rm.Key = string(msg.Key)
				}
				for k, v := range msg.Headers {
					rm.Headers[k] = v
				}
				capture.add(rm)
			}
			ack(nil)
		}
	}()

	waitForTCPReady(t, addr, 5*time.Second)

	// Test: Client with valid certificate should work
	t.Run("valid_client_cert", func(t *testing.T) {
		clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		require.NoError(t, err)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.DialTLSConfig(tlsConfig),
			kgo.RequestTimeoutOverhead(5*time.Second),
			kgo.DisableIdempotentWrite(),
		)
		require.NoError(t, err)
		defer client.Close()

		results := client.ProduceSync(ctx, &kgo.Record{
			Topic: "mtls-topic",
			Value: []byte("mtls-message"),
		})
		require.NoError(t, results[0].Err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		assert.Equal(t, "mtls-message", msgs[0].Value)
	})

	// Test: Client without certificate should be rejected
	t.Run("no_client_cert", func(t *testing.T) {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.DialTLSConfig(tlsConfig),
			kgo.RequestTimeoutOverhead(5*time.Second),
			kgo.DisableIdempotentWrite(),
		)
		require.NoError(t, err)
		defer client.Close()

		results := client.ProduceSync(ctx, &kgo.Record{
			Topic: "mtls-fail-topic",
			Value: []byte("should-fail"),
		})
		require.Error(t, results[0].Err)
	})
}

// TestFranzJSONOutputMatchesSchema produces a message through the full server
// pipeline and validates that formatting the resulting *ksink.Message with the
// jsonl formatter, across supported encoding options, produces output that
// conforms to the JSON schema.
func TestFranzJSONOutputMatchesSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	})

	// Collect raw *ksink.Message from ReadBatch in a goroutine so that
	// ProduceSync (which waits for acks) does not deadlock.
	type batchResult struct {
		msgs []*ksink.Message
		err  error
	}
	batchCh := make(chan batchResult, 1)
	go func() {
		msgs, ack, err := srv.ReadBatch(ctx)
		if err != nil {
			batchCh <- batchResult{err: err}
			return
		}
		ack(nil)
		batchCh <- batchResult{msgs: msgs}
	}()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer client.Close()

	results := client.ProduceSync(ctx, &kgo.Record{
		Topic: "schema-test",
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
		Headers: []kgo.RecordHeader{
			{Key: "h1", Value: []byte("v1")},
		},
	})
	require.NoError(t, results[0].Err)

	br := <-batchCh
	require.NoError(t, br.err)
	require.NotEmpty(t, br.msgs)

	schemaLoader := gojsonschema.NewStringLoader(format.JSONSchema)

	testCases := []struct {
		name    string
		options []format.Option
	}{
		{name: "jsonl"},
		{name: "jsonl_base64_key", options: []format.Option{format.WithJSONBase64Key()}},
		{name: "jsonl_base64_value", options: []format.Option{format.WithJSONBase64Value()}},
		{name: "jsonl_base64_key_value", options: []format.Option{format.WithJSONBase64Key(), format.WithJSONBase64Value()}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fmtr, err := format.New("jsonl", nil, testCase.options...)
			require.NoError(t, err)

			for _, msg := range br.msgs {
				data, err := fmtr.Format(msg)
				require.NoError(t, err)

				// Strip trailing separator.
				jsonBytes := data[:len(data)-1]
				require.True(t, json.Valid(jsonBytes), "output is not valid JSON: %s", string(jsonBytes))

				docLoader := gojsonschema.NewBytesLoader(jsonBytes)
				result, err := gojsonschema.Validate(schemaLoader, docLoader)
				require.NoError(t, err)

				for _, e := range result.Errors() {
					t.Errorf("schema validation error (%s): %s", testCase.name, e)
				}
				assert.True(t, result.Valid(), "JSON output does not match schema for format %s", testCase.name)
			}
		})
	}
}

func TestFranzClose(t *testing.T) {
	srv, _ := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	})

	// Close should not error
	err := srv.Close(context.Background())
	assert.NoError(t, err)

	// Double close should not error
	err = srv.Close(context.Background())
	assert.NoError(t, err)
}

func getFreePort(t *testing.T) int {
	t.Helper()
	return getIntegrationFreePort(t)
}

func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	integrationWaitForTCPReady(t, addr, timeout)
}

func startTestServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg.Address = addr

	srv, err := New(cfg, WithLogger(&integrationLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)

	waitForTCPReady(t, addr, 5*time.Second)

	t.Cleanup(func() {
		srv.Close(context.Background())
	})

	return srv, addr
}

func startReadLoop(t *testing.T, srv *Server) *messageCapture {
	t.Helper()
	return integrationStartReadLoop(t, srv)
}

func startFailReadLoop(t *testing.T, srv *Server, ackErr error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		for {
			_, ack, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			ack(ackErr)
		}
	}()
}
