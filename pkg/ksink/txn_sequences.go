package ksink

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
)

const txnDedupWindow = 5

// copyTxnSequences creates a candidate sequence snapshot so a failed sink
// write never advances the producer sequence. All access is serialized by
// txnState.produceMu and the journal is committed under Server.txnMu.
func copyTxnSequences(src map[string]txnPartitionSequence) map[string]txnPartitionSequence {
	out := make(map[string]txnPartitionSequence, len(src))
	for key, partition := range src {
		partition.Recent = append([]txnBatchSignature(nil), partition.Recent...)
		out[key] = partition
	}
	return out
}

// acceptTxnBatches verifies producer sequence ordering for every v2 batch.
// Exact retransmission of a recently acknowledged batch is accepted without
// re-delivery. This is per producer epoch and topic-partition, *not* per txn.
// Each validated new batch's bytes are returned for record decoding.
func acceptTxnBatches(data []byte, key string, sequences map[string]txnPartitionSequence) ([]byte, int16) {
	if len(data) == 0 {
		return nil, 0
	}
	var accepted []byte
	for len(data) != 0 {
		if len(data) < 61 {
			return nil, kerr.CorruptMessage.Code
		}
		length := int64(int32(binary.BigEndian.Uint32(data[8:12])))
		if length < 49 || length > int64(len(data)-12) || data[16] != 2 {
			return nil, kerr.CorruptMessage.Code
		}
		batch := data[:12+int(length)]
		seqBase := int32(binary.BigEndian.Uint32(batch[53:57]))
		count := int32(binary.BigEndian.Uint32(batch[57:61]))
		if count <= 0 {
			return nil, kerr.CorruptMessage.Code
		}
		sha := sha256.Sum256(batch)
		signature := txnBatchSignature{
			Base: seqBase, Count: count, Checksum: hex.EncodeToString(sha[:]),
		}
		previous, exists := sequences[key]
		duplicate := false
		for _, recent := range previous.Recent {
			if recent.Base == signature.Base && recent.Count == signature.Count &&
				recent.Checksum == signature.Checksum {
				duplicate = true
				break
			}
		}
		if !duplicate {
			if seqBase != previous.Next {
				return nil, kerr.OutOfOrderSequenceNumber.Code
			}
			previous.Next = seqBase + count // int32 wraps by Kafka sequence semantics
			previous.Recent = append(previous.Recent, signature)
			if len(previous.Recent) > txnDedupWindow {
				previous.Recent = previous.Recent[len(previous.Recent)-txnDedupWindow:]
			}
			sequences[key] = previous
			accepted = append(accepted, batch...)
		} else if !exists {
			return nil, kerr.OutOfOrderSequenceNumber.Code
		}
		data = data[len(batch):]
	}
	return accepted, 0
}

func txnSequenceKey(topic string, partition int32) string {
	return fmt.Sprintf("%s\x00%d", topic, partition)
}
