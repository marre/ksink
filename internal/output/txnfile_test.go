package output_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/require"
)

func TestTxnFileWriterRequiresSharedWAL(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "orders-{txnID}.jsonl")
	_, err := output.NewTxnFileWriter(pattern, nil)
	require.ErrorContains(t, err, "requires a transaction WAL")

	log, err := ksink.OpenTransactionLog(filepath.Join(dir, ".ksink-kafka-state"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	_, err = output.NewTxnFileWriter(filepath.Join(dir, "no-transaction-placeholder.jsonl"), log)
	require.ErrorContains(t, err, "must contain {txnID}")
}

func TestTxnFileWriterExclusiveLockAndRecovery(t *testing.T) {
	root := t.TempDir()
	pattern := filepath.Join(root, "{topic}-{partition}-{txnID}.jsonl")
	log, err := ksink.OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = log.Close() }()

	first, err := output.NewTxnFileWriter(pattern, log)
	require.NoError(t, err)
	_, err = output.NewTxnFileWriter(pattern, log)
	require.ErrorContains(t, err, "exclusive transaction writer lock")
	require.NoError(t, first.Close())

	reopened, err := output.NewTxnFileWriter(pattern, log)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestTxnFileWriterRejectsUnjournaledStaging(t *testing.T) {
	root := t.TempDir()
	pattern := filepath.Join(root, "{topic}-{txnID}.jsonl")
	log, err := ksink.OpenTransactionLog(filepath.Join(root, ".ksink-kafka-state"))
	require.NoError(t, err)
	defer func() { _ = log.Close() }()

	pending := filepath.Join(root, ".ksink-pending", "untracked-old-transaction")
	require.NoError(t, os.MkdirAll(pending, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(pending, ".ksink-commit-intent"), []byte("old"), 0600))

	_, err = output.NewTxnFileWriter(pattern, log)
	require.ErrorContains(t, err, "not recorded in the transaction WAL")
	require.FileExists(t, filepath.Join(pending, ".ksink-commit-intent"),
		"unknown staged data must never be deleted or migrated automatically")
}
