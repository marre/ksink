package ksink

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestValidateTxnBatchProducer(t *testing.T) {
	// A v2 record batch header is 61 bytes including the 12-byte length prefix.
	batch := make([]byte, 61)
	binary.BigEndian.PutUint32(batch[8:12], 49)
	batch[16] = 2
	binary.BigEndian.PutUint64(batch[43:51], 42)
	binary.BigEndian.PutUint16(batch[51:53], 3)

	require.Zero(t, validateTxnBatchProducer(batch, 42, 3))
	require.Equal(t, kerr.ProducerFenced.Code, validateTxnBatchProducer(batch, 41, 3))
	require.Equal(t, kerr.ProducerFenced.Code, validateTxnBatchProducer(batch, 42, 2))
	require.Equal(t, kerr.CorruptMessage.Code, validateTxnBatchProducer(batch[:60], 42, 3))
	require.Equal(t, kerr.CorruptMessage.Code, validateTxnBatchProducer([]byte{1, 2, 3}, 42, 3))
	require.Zero(t, validateTxnBatchProducer(append(append([]byte{}, batch...), batch...), 42, 3))
}

func sendEndTxnForSafetyTest(t *testing.T, conn net.Conn, requestID int32, txnID string, pid int64, epoch int16, commit bool) *kmsg.EndTxnResponse {
	t.Helper()
	req := kmsg.NewEndTxnRequest()
	req.Version = 0
	req.TransactionalID = txnID
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	req.Commit = commit
	response := sendRawRequest(t, conn, int16(kmsg.EndTxn), 0, requestID, req.AppendTo(nil))
	parsed := &kmsg.EndTxnResponse{Version: 0}
	require.NoError(t, parsed.ReadFrom(response))
	return parsed
}

func TestFailedEndTxnRetainsActiveStateUntilRetry(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0", TransactionalWrite: true})
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	commitAttempts := make(chan struct{}, 4)
	go func() {
		for {
			event, ack, err := srv.Read(ctx)
			if err != nil {
				return
			}
			if _, ok := event.(*TxnCommitEvent); ok {
				commitAttempts <- struct{}{}
				if len(commitAttempts) == 1 {
					ack(errors.New("simulated output failure"))
					continue
				}
			}
			ack(nil)
		}
	}()

	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	apiReq := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 1, apiReq.AppendTo(nil))
	txnID := "retry-state"
	init := sendInitProducerID(t, conn, 2, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	sendAddPartitionsToTxn(t, conn, 3, txnID, init.ProducerID, init.ProducerEpoch, "orders")

	failed := sendEndTxnForSafetyTest(t, conn, 4, txnID, init.ProducerID, init.ProducerEpoch, true)
	require.Equal(t, kerr.UnknownServerError.Code, failed.ErrorCode)
	srv.txnMu.Lock()
	st := srv.txnStates[txnID]
	require.True(t, st.active)
	require.True(t, st.needsResolution)
	require.False(t, st.ending)
	srv.txnMu.Unlock()

	retried := sendEndTxnForSafetyTest(t, conn, 5, txnID, init.ProducerID, init.ProducerEpoch, true)
	require.Zero(t, retried.ErrorCode)
	duplicate := sendEndTxnForSafetyTest(t, conn, 6, txnID, init.ProducerID, init.ProducerEpoch, true)
	require.Zero(t, duplicate.ErrorCode)
	require.Len(t, commitAttempts, 2, "duplicate EndTxn must not replay output commit")
	srv.txnMu.Lock()
	require.False(t, st.active)
	require.False(t, st.needsResolution)
	require.True(t, st.hasOutcome)
	srv.txnMu.Unlock()
}

func TestFailedZombieAbortDoesNotFenceOrForgetTransaction(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0", TransactionalWrite: true})
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	aborts := make(chan struct{}, 4)
	go func() {
		for {
			event, ack, err := srv.Read(ctx)
			if err != nil {
				return
			}
			if _, ok := event.(*TxnAbortEvent); ok {
				aborts <- struct{}{}
				if len(aborts) == 1 {
					ack(errors.New("cannot delete staged data"))
					continue
				}
			}
			ack(nil)
		}
	}()

	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	apiReq := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 1, apiReq.AppendTo(nil))
	txnID := "zombie-abort"
	init := sendInitProducerID(t, conn, 2, &txnID, 30000)
	require.Zero(t, init.ErrorCode)
	sendAddPartitionsToTxn(t, conn, 3, txnID, init.ProducerID, init.ProducerEpoch, "orders")

	failed := sendInitProducerID(t, conn, 4, &txnID, 30000)
	require.Equal(t, kerr.UnknownServerError.Code, failed.ErrorCode)
	srv.txnMu.Lock()
	st := srv.txnStates[txnID]
	require.True(t, st.active)
	require.True(t, st.needsResolution)
	require.Equal(t, init.ProducerEpoch, st.epoch)
	srv.txnMu.Unlock()

	retry := sendInitProducerID(t, conn, 5, &txnID, 30000)
	require.Zero(t, retry.ErrorCode)
	require.Equal(t, init.ProducerEpoch+1, retry.ProducerEpoch)
	require.Len(t, aborts, 2)
}
