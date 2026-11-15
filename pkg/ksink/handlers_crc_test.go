package ksink

import (
    "context"
    "encoding/binary"
    "hash/crc32"
    "net"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/twmb/franz-go/pkg/kerr"
    "github.com/twmb/franz-go/pkg/kmsg"
)

// kafkaV2OneRecordBatch builds a real Kafka v2 record batch, including the
// wire-format CRC32C. The sequence is deliberately fixed to zero so that a
// corrupt first attempt can be retried with identical producer identity.
func kafkaV2OneRecordBatch(pid int64, epoch int16, value []byte) []byte {
    record := []byte{0, 0, 0, 1} // attributes, timestamp delta, offset delta, nil key
    var varint [binary.MaxVarintLen64]byte
    n := binary.PutVarint(varint[:], int64(len(value)))
    record = append(record, varint[:n]...)
    record = append(record, value...)
    record = append(record, 0) // no headers
    n = binary.PutVarint(varint[:], int64(len(record)))
    body := append(append([]byte{}, varint[:n]...), record...)

    batch := make([]byte, 61+len(body))
    binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-12))
    batch[16] = 2
    binary.BigEndian.PutUint64(batch[43:51], uint64(pid))
    binary.BigEndian.PutUint16(batch[51:53], uint16(epoch))
    binary.BigEndian.PutUint32(batch[57:61], 1)
    copy(batch[61:], body)
    binary.BigEndian.PutUint32(batch[17:21],
        crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)))
    return batch
}

func sendKafkaCRCProduce(t *testing.T, conn net.Conn, correlation int32, batch []byte) *kmsg.ProduceResponseTopicPartition {
    t.Helper()
    req := kmsg.NewProduceRequest()
    req.Version = 3
    req.Acks = 1
    req.TimeoutMillis = 1000
    topic := kmsg.NewProduceRequestTopic()
    topic.Topic = "orders"
    partition := kmsg.NewProduceRequestTopicPartition()
    partition.Partition = 0
    partition.Records = batch
    topic.Partitions = append(topic.Partitions, partition)
    req.Topics = append(req.Topics, topic)
    raw := sendRawRequest(t, conn, int16(kmsg.Produce), 3, correlation, req.AppendTo(nil))
    response := &kmsg.ProduceResponse{Version: 3}
    require.NoError(t, response.ReadFrom(raw))
    require.Len(t, response.Topics, 1)
    require.Len(t, response.Topics[0].Partitions, 1)
    return &response.Topics[0].Partitions[0]
}

func TestKafkaCRCCorruptionDoesNotAdvanceOrderedWALOrProducerSequence(t *testing.T) {
    root := t.TempDir()
    wal, err := OpenTransactionLog(filepath.Join(root, "state"))
    require.NoError(t, err)
    require.NoError(t, wal.BindOutput(filepath.Join(root, "{topic}", "messages-{seq:08}.jsonl")))
    srv := newOrderedTestServer(t, wal)
    conn := newTxnStateTestConn(t, srv)
    defer func() {
        _ = conn.Close()
        _ = srv.Close(context.Background())
        _ = wal.Close()
    }()
    init := sendInitProducerID(t, conn, 1, nil, 30000)
    require.Zero(t, init.ErrorCode)

    good := kafkaV2OneRecordBatch(init.ProducerID, init.ProducerEpoch, []byte("payload"))
    bad := append([]byte(nil), good...)
    bad[len(bad)-2] ^= 0x01 // change value without changing batch length or CRC
    rejected := sendKafkaCRCProduce(t, conn, 2, bad)
    require.Equal(t, kerr.CorruptMessage.Code, rejected.ErrorCode)
    require.Zero(t, wal.NextTopicOffset("orders"))
    require.Zero(t, wal.CurrentState().Idempotent[init.ProducerID].
        Sequences[txnSequenceKey("orders", 0)].Next)

    accepted := sendKafkaCRCProduce(t, conn, 3, good)
    require.Zero(t, accepted.ErrorCode)
    require.Equal(t, int64(0), accepted.BaseOffset)
    require.Equal(t, int64(1), wal.NextTopicOffset("orders"))
    require.Equal(t, int32(1), wal.CurrentState().Idempotent[init.ProducerID].
        Sequences[txnSequenceKey("orders", 0)].Next)

    duplicate := sendKafkaCRCProduce(t, conn, 4, good)
    require.Zero(t, duplicate.ErrorCode)
    require.Equal(t, accepted.BaseOffset, duplicate.BaseOffset)
    require.Equal(t, int64(1), wal.NextTopicOffset("orders"),
        "idempotent retries must never allocate a second offset")
}
