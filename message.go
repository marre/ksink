package ksrv

import (
	"context"
	"time"
)

// Message represents a single Kafka message received by the server.
// The field names mirror the metadata keys set by the Bento kafka_server input
// so that a Bento adapter can map them directly.
type Message struct {
	Value         []byte
	Key           []byte
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     time.Time
	ClientAddress string
	Tombstone     bool
	Headers       map[string]string
}

// MessageBatch is a slice of Messages produced in a single Kafka produce request.
type MessageBatch []*Message

// AckFunc is called to acknowledge or reject processing of a message batch.
// If err is nil the producer receives a success response; otherwise it receives
// an error response.
type AckFunc func(ctx context.Context, err error) error
