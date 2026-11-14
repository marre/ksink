package ksink

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func startSinglePartitionTestServer(t *testing.T, transactional bool) *Server {
	t.Helper()
	cfg := Config{Address: "127.0.0.1:0", TransactionalWrite: transactional}
	if transactional {
		cfg.TransactionStateDir = t.TempDir()
	}
	srv, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	return srv
}

func TestMetadataAdvertisesOnlyPartitionZero(t *testing.T) {
	srv := startSinglePartitionTestServer(t, false)
	conn := newTxnStateTestConn(t, srv)
	defer func() { _ = conn.Close() }()

	req := kmsg.NewMetadataRequest()
	req.Version = 0
	name := "orders"
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = &name
	req.Topics = append(req.Topics, topic)
	reply := sendRawRequest(t, conn, int16(kmsg.Metadata), 0, 1, req.AppendTo(nil))
	resp := &kmsg.MetadataResponse{Version: 0}
	require.NoError(t, resp.ReadFrom(reply))
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	require.Equal(t, int32(0), resp.Topics[0].Partitions[0].Partition)
}

func producePartitionCode(t *testing.T, conn net.Conn, correlationID int32, partition int32) int16 {
	t.Helper()
	req := kmsg.NewProduceRequest()
	req.Version = 0
	req.Acks = 1
	req.TimeoutMillis = 1000
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewProduceRequestTopicPartition()
	part.Partition = partition
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)
	reply := sendRawRequest(t, conn, int16(kmsg.Produce), 0, correlationID, req.AppendTo(nil))
	resp := &kmsg.ProduceResponse{Version: 0}
	require.NoError(t, resp.ReadFrom(reply))
	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.Topics[0].Partitions, 1)
	return resp.Topics[0].Partitions[0].ErrorCode
}

func TestProduceRejectsUnsupportedPartitions(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		name := "nontransactional"
		if transactional {
			name = "transactional"
		}
		t.Run(name, func(t *testing.T) {
			srv := startSinglePartitionTestServer(t, transactional)
			conn := newTxnStateTestConn(t, srv)
			defer func() { _ = conn.Close() }()
			require.Zero(t, producePartitionCode(t, conn, 1, 0))
			require.Equal(t, kerr.UnknownTopicOrPartition.Code, producePartitionCode(t, conn, 2, 1))
			require.Equal(t, kerr.UnknownTopicOrPartition.Code, producePartitionCode(t, conn, 3, -1))
		})
	}
}

func addPartitionsForSinglePartitionTest(t *testing.T, conn net.Conn, correlationID int32,
	txnID string, producerID int64, epoch int16, partitions []int32) *kmsg.AddPartitionsToTxnResponse {
	t.Helper()
	req := kmsg.NewAddPartitionsToTxnRequest()
	req.Version = 0
	req.TransactionalID = txnID
	req.ProducerID = producerID
	req.ProducerEpoch = epoch
	topic := kmsg.NewAddPartitionsToTxnRequestTopic()
	topic.Topic = "orders"
	topic.Partitions = partitions
	req.Topics = append(req.Topics, topic)
	reply := sendRawRequest(t, conn, int16(kmsg.AddPartitionsToTxn), 0, correlationID, req.AppendTo(nil))
	resp := &kmsg.AddPartitionsToTxnResponse{Version: 0}
	require.NoError(t, resp.ReadFrom(reply))
	require.Len(t, resp.Topics, 1)
	return resp
}

func TestAddPartitionsToTxnRejectsUnsupportedPartitions(t *testing.T) {
	srv := startSinglePartitionTestServer(t, true)
	conn := newTxnStateTestConn(t, srv)
	defer func() { _ = conn.Close() }()
	txnID := "single-partition-test"
	init := sendInitProducerID(t, conn, 1, &txnID, 30000)
	require.Zero(t, init.ErrorCode)

	invalid := addPartitionsForSinglePartitionTest(t, conn, 2, txnID,
		init.ProducerID, init.ProducerEpoch, []int32{1, -1})
	require.Len(t, invalid.Topics[0].Partitions, 2)
	for _, part := range invalid.Topics[0].Partitions {
		require.Equal(t, kerr.UnknownTopicOrPartition.Code, part.ErrorCode)
	}
	srv.txnMu.Lock()
	active := srv.txnStates[txnID].active
	srv.txnMu.Unlock()
	require.False(t, active, "requesting only invalid partitions must not activate a transaction")

	mixed := addPartitionsForSinglePartitionTest(t, conn, 3, txnID,
		init.ProducerID, init.ProducerEpoch, []int32{0, 1})
	require.Len(t, mixed.Topics[0].Partitions, 2)
	require.Zero(t, mixed.Topics[0].Partitions[0].ErrorCode)
	require.Equal(t, kerr.UnknownTopicOrPartition.Code, mixed.Topics[0].Partitions[1].ErrorCode)
	srv.txnMu.Lock()
	active = srv.txnStates[txnID].active
	srv.txnMu.Unlock()
	require.True(t, active, "partition 0 remains usable")
}
