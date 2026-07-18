# ksink

A lightweight Kafka-protocol-compatible server library and tool for Go. It accepts produce requests from standard Kafka producers without requiring a full Kafka cluster.

## Features

- Accepts produce requests from any Kafka producer (kafka-console-producer, librdkafka, franz-go, etc.)
- Pull-based API: call `Read` to receive typed events, then acknowledge
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- TLS and mutual TLS (mTLS)
- Topic filtering
- Idempotent producer support
- Transactional producer support with filesystem-backed commit/abort semantics

## ksink Tool

The `cmd/ksink` tool forwards received messages to an output sink:

```bash
# Write raw message values (binary) to stdout, one per line (default)
go run ./cmd/ksink --addr :9092

# Write to a file instead of stdout
go run ./cmd/ksink --addr :9092 --output messages.bin

# Write JSON lines to a file
go run ./cmd/ksink --addr :9092 --output messages.jsonl --output-format jsonl

# Send JSON lines via HTTP POST to an HTTP endpoint
go run ./cmd/ksink --addr :9092 --output http://localhost:8080/ingest --output-format jsonl

# Send JSON lines via HTTPS POST to a remote endpoint
go run ./cmd/ksink --addr :9092 --output https://example.com/ingest --output-format jsonl

# Write JSON lines to S3 (requires AWS credentials via default SDK chain)
go run ./cmd/ksink --addr :9092 --output 's3://my-bucket/messages/{topic}.jsonl' --output-format jsonl --output-s3-region us-east-1

# Transactional write to S3 using {txnID} in object key
go run ./cmd/ksink --addr :9092 --transactional --output 's3://my-bucket/messages/{topic}-{txnID}.jsonl' --output-format jsonl --output-s3-region us-east-1

# Print the JSON schema for the jsonl output format
go run ./cmd/ksink json-schema

# Enable transactional produce support
go run ./cmd/ksink --addr :9092 --transactional --output 'messages-{txnID}.jsonl'

# Route messages to per-topic files using the {topic} placeholder
go run ./cmd/ksink --addr :9092 --output '{topic}.jsonl'
# produces orders.jsonl, events.jsonl, etc. — one file per topic

# Combine {topic} with transactional output
go run ./cmd/ksink --addr :9092 --transactional --output '{topic}-{txnID}.jsonl'
# produces orders-my-txn.jsonl, events-my-txn.jsonl, etc.

# Transactional file output — the --output pattern must contain {txnID}.
# Messages are written to per-transaction temp files; on commit the temp
# file is renamed; on abort the temp file is deleted:
#   During transaction: messages-<txnID>.jsonl.tmp
#   After commit:       messages-<txnID>.jsonl
#   After abort:        temp file is deleted
go run ./cmd/ksink --addr :9092 --transactional --output 'messages-{txnID}.jsonl'
```

### Message Formats

Use `--output-format` to control how messages are serialized:

| Format        | Description                                                      |
|---------------|------------------------------------------------------------------|
| `binary`      | Raw message value bytes, newline-delimited (default)             |
| `jsonl`       | JSON lines with per-field UTF-8/base64 encoding options          |
| `kcat`        | kcat-compatible format string (requires `--output-format-string`)|

Use `--output-separator` to set the delimiter appended after each binary
message (default: `\n`). Common escape sequences (`\n`, `\r`, `\t`, `\0`) are
interpreted. Use `--output-separator-hex` for hex-encoded binary delimiters
(e.g. `0a` for newline, `00` for null byte). Use `--no-separator` to clear
the delimiter entirely. Separator options only apply to binary format; jsonl
is always newline-delimited and HTTP output is delimited by HTTP requests.

For JSON output, use `--output-json-base64-key` and/or
`--output-json-base64-value` to base64-encode those fields when payloads are
not safe to emit as UTF-8.

When using HTTP output, Kafka message metadata is sent as HTTP headers:
`X-Kafka-Topic`, `X-Kafka-Partition`, `X-Kafka-Offset`, `X-Kafka-Key`
(base64-encoded), `X-Kafka-Header-{name}`, `X-Kafka-Timestamp`
(Unix milliseconds), and `X-Kafka-Client-Addr`.

### S3 Output

Use an `s3://bucket/key-pattern` destination to write messages to Amazon S3 or
S3-compatible object storage (for example MinIO with `--output-s3-endpoint` and
`--output-s3-force-path-style`).

Supported placeholders in the S3 key pattern:
- `{topic}` for per-topic object partitioning
- `{txnID}` for transactional output (`--transactional` only)

S3 flags:
- `--output-s3-region`: AWS region for S3 output (uses AWS SDK default chain if empty).
- `--output-s3-endpoint`: Custom S3 endpoint URL (e.g. `http://127.0.0.1:9000` for MinIO).
- `--output-s3-force-path-style`: Use path-style S3 addressing (required by some S3-compatible endpoints like MinIO).
- `--output-s3-retries`: Maximum S3 write attempts for each operation (default: `3`).
- `--output-s3-retry-delay`: Delay between S3 retry attempts (default: `250ms`).
- `--output-s3-batch-max-bytes`: Maximum buffered bytes per S3 object before flush (default: `5242880` / 5 MiB).
- `--output-s3-batch-max-messages`: Maximum buffered messages per S3 object before flush (default: `1000`).
- `--output-s3-multipart-part-size`: Multipart upload part size in bytes (minimum `5242880` / 5 MiB).
- `--output-s3-buffer-dir`: Directory for durable local S3 buffering and startup recovery (memory-only if empty).
- `--output-s3-batch-max-age`: Maximum age of a durable local S3 buffer before it is uploaded (disabled if `0`).

#### S3 Upload Mechanics

Under the hood, S3 uploads operate in one of two modes depending on whether `--transactional` is enabled:

##### 1. Non-Transactional Uploads

By default, messages are routed to partitions based on the resolved S3 key (derived from the `--output` S3 key pattern and the message's topic).

- **In-Memory Buffering (Default):**
  - Messages are buffered in memory per resolved S3 key.
  - When the buffer reaches `--output-s3-batch-max-bytes` or `--output-s3-batch-max-messages`, an upload to S3 is triggered.
  - Each uploaded batch is saved as a unique object using a suffix based on the current Unix nanoseconds timestamp and a sequence counter to prevent overwriting existing objects (e.g., `key-pattern-<timestamp>-<sequence>`).
  - Active buffers are also flushed and uploaded when the process shuts down cleanly.

- **Durable Local Buffering:**
  - If `--output-s3-buffer-dir` is provided, `ksink` utilizes a filesystem write-ahead state machine with `active`, `pending`, and `uploading` subdirectories for durable message persistence.
  - **Write-Ahead Logging:** In-progress data is written and synchronized (`fsync`'d) to disk under the `active/` directory before any metadata journal updates.
  - **Hashing Key Names:** The resolved S3 key is hashed using SHA-256 to generate stable, collision-free local filenames, avoiding any path traversal issues.
  - **Rotation & Upload:** Once local size/count limits are reached or the buffer exceeds `--output-s3-batch-max-age` (checked periodically by a background loop), the files are closed and moved sequentially to `pending/`, then `uploading/` to be uploaded via `PutObject`. Once the S3 upload succeeds, the local temporary files are deleted.
  - **Startup Recovery:** On initialization, `ksink` checks the buffer directory. It recovers any half-written files from the `active/` directory by validating them against their metadata journals, truncating them to the last synced offset, moving them to `pending/`, and immediately uploading all pending/uploading batches to S3 before starting to accept new incoming writes.

##### 2. Transactional Uploads

To enable transactional S3 uploads, you must provide the `--transactional` flag and include the `{txnID}` placeholder in the output pattern (e.g., `s3://my-bucket/messages/{topic}-{txnID}.jsonl`).

- **In-Memory Partitioned Buffering:**
  - Messages are buffered in memory, partitioned by both the transaction ID and the resolved S3 key pattern.
  - Active transactional payloads are held in memory and are **not** persisted to the local filesystem. If the `ksink` process crashes or restarts, any uncommitted transactional data in memory is lost.
- **Commit Semantics:**
  - When the producer commits the transaction (`CommitTxn`), `ksink` flushes and uploads the buffered messages to S3.
  - **PutObject (Small Transactions):** If the buffered payload size is smaller than the multipart threshold specified by `--output-s3-multipart-part-size` (default: 5 MiB), the payload is uploaded as a single object via a standard `PutObject` call.
  - **Multipart Upload (Large Transactions):** If the payload size exceeds the threshold, `ksink` splits the payload into part-sized chunks (using the `--output-s3-multipart-part-size` configuration) and uploads them using S3 Multipart Upload APIs. On successful completion of all part uploads, the upload is finalized (`CompleteMultipartUpload`). If any part upload fails, the multipart upload is aborted (`AbortMultipartUpload`) to clean up resources in S3.
- **Abort Semantics:**
  - When the producer aborts the transaction (`AbortTxn`), `ksink` simply discards the corresponding in-memory buffers without uploading any objects to S3.

Durability semantics:
- Non-transactional S3 output acknowledges after the current write batch is
  persisted to S3.
- Transactional S3 output acknowledges commit only after successful S3 commit
  (`PutObject` for small transactions, multipart upload complete for larger
  transactions).
- Delivery is **at-least-once**: duplicates are possible after retries/failures.

Crash recovery note:
- In-progress transactional payloads are held in memory and are not recovered
  after process crash/restart.

Cost guidance:
- Prefer larger batches to reduce `PutObject` request count and tiny-object
  overhead.
- Use lifecycle policies to move older data to cheaper storage classes.

```bash
# Binary output with no separator to a file
go run ./cmd/ksink --no-separator --output data.bin

# Binary values, one per line
go run ./cmd/ksink --output messages.bin

# JSON with base64-encoded key/value for binary payloads
go run ./cmd/ksink --output-format jsonl --output-json-base64-key --output-json-base64-value --output messages.jsonl

# Binary delimiter using hex encoding (null byte)
go run ./cmd/ksink --output-separator-hex "00" --output messages.bin

# kcat-compatible output formatting
go run ./cmd/ksink --output-format kcat \
  --output-format-string 'Topic: %t Partition: %p Offset: %o\nKey: %k\nValue: %s\n---\n'
```

## Library

### Installation

```bash
go get github.com/marre/ksink
```

### Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/marre/ksink"
)

func main() {
    srv, err := ksink.New(ksink.Config{
        Address: ":9092",
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := srv.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer srv.Close(ctx)

    log.Printf("Listening on %s", srv.Addr())

    for {
        event, ack, err := srv.Read(ctx)
        if err != nil {
            log.Fatal(err)
        }

        switch e := event.(type) {
        case *ksink.MessagesEvent:
            for _, msg := range e.Messages {
                fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
            }
        case *ksink.TxnCommitEvent:
            fmt.Printf("transaction %s committed\n", e.TransactionalID)
        case *ksink.TxnAbortEvent:
            fmt.Printf("transaction %s aborted\n", e.TransactionalID)
        }

        ack(nil) // acknowledge successful processing
    }
}
```

Then send messages using any Kafka producer:

```bash
echo "hello world" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

### Configuration

```go
cfg := ksink.Config{
    Address:           ":9092",           // Listen address
    AdvertisedAddress: "myhost:9092",     // Address advertised to clients
    Topics:            []string{"events"},// Restrict to specific topics
    CertFile:          "server.pem",      // TLS certificate
    KeyFile:           "server-key.pem",  // TLS private key
    MTLSAuth:          "require_and_verify",
    MTLSCAsFiles:      []string{"ca.pem"},
    SASL: []ksink.SASLCredential{
        {Mechanism: "PLAIN", Username: "user", Password: "pass"},
    },
    Timeout:         30 * time.Second,
    IdleTimeout:     60 * time.Second,
    MaxMessageBytes: 1048576,
    IdempotentWrite:    false,
    TransactionalWrite: false,
}
```

### Transactional Producer Support

When `TransactionalWrite` is enabled, ksink accepts the Kafka transactional
protocol requests (`InitProducerID`, `AddPartitionsToTxn`, `EndTxn`) and
tracks which transaction each message belongs to via the `TransactionalID`
field on `Message`.

Transaction lifecycle events are delivered through `Read` as distinct event
types (`*TxnCommitEvent`, `*TxnAbortEvent`). This ensures that all data
messages for a transaction have been processed before the commit/abort event
is handled:

```go
for {
    event, ack, err := srv.Read(ctx)
    if err != nil {
        break
    }
    switch e := event.(type) {
    case *ksink.MessagesEvent:
        for _, msg := range e.Messages {
            // write message to output
        }
    case *ksink.TxnCommitEvent:
        // e.g. rename temp file to final name
        commitTxn(e.TransactionalID)
    case *ksink.TxnAbortEvent:
        // e.g. delete temp file
        abortTxn(e.TransactionalID)
    }
    ack(nil)
}
```

#### How Kafka Transactions Work (Background)

In a real Kafka cluster, transactions provide exactly-once semantics (EOS)
for produce workflows:

1. A producer is configured with a unique `transactional.id` and calls
   `initTransactions()` to register with the cluster's transaction
   coordinator.
2. `beginTransaction()` starts a new transaction.
3. Records are produced to one or more topic-partitions. These records are
   written to the log but are invisible to consumers using
   `isolation.level=read_committed` until the transaction commits.
4. Optionally, consumer offsets can be committed as part of the transaction
   (`sendOffsetsToTransaction`), enabling atomic read-process-write loops.
5. `commitTransaction()` makes all records visible atomically, or
   `abortTransaction()` discards them.

The broker uses the `transactional.id` for **zombie fencing**: when a
producer restarts with the same ID, the broker aborts any in-flight
transaction from the previous instance to prevent duplicates.

#### Limitations of ksink's Transaction Support

ksink is **not** a full Kafka broker. Its transactional support is
intentionally simplified:

| Real Kafka behaviour | ksink API behaviour | ksink tool behaviour |
|---|---|---|
| Records written inside a transaction are invisible to `read_committed` consumers until commit. | Records are delivered to `Read` as `*MessagesEvent` immediately when produced. The `TransactionalID` field is populated so the consumer can buffer them. When the transaction ends, a `*TxnCommitEvent` or `*TxnAbortEvent` is delivered so the consumer can decide what to do. | Messages are written to a temp file (`.tmp` suffix) during the transaction. On commit the temp file is renamed to the final path; on abort it is deleted. |
| The transaction coordinator tracks transaction state across broker restarts. | No persistent transaction state. If ksink restarts, in-flight transactions are lost. | On clean shutdown (when `Close()` runs), uncommitted temp files are cleaned up. If the process crashes or is killed, `.tmp` files may remain across restarts. |
| `sendOffsetsToTransaction` atomically commits consumer offsets with the transaction. | Not supported. ksink is a sink, not a full broker with consumer groups. | Not supported. |
| Zombie fencing: restarting a producer with the same `transactional.id` aborts the old instance's pending transaction. | **Supported.** When `InitProducerID` is called with an existing `transactional.id` that has an in-flight transaction, the old transaction is automatically aborted via a `*TxnAbortEvent` delivered through `Read` and the producer epoch is bumped. | The old transaction's temp file is deleted when the abort event is processed. |
| Transactions can span multiple topic-partitions atomically. | Supported — all events for the same `transactional.id` share the same transaction context regardless of topic/partition. | All messages with the same `transactional.id` go to the same temp file. When the `{topic}` placeholder is used, each topic within a transaction gets its own file; all are committed or aborted together. |
| Aborted records are never visible to `read_committed` consumers. | Aborted records were already delivered to `Read`. The `*TxnAbortEvent` signals the consumer to clean up, but application-level processing that happened before the abort is not rolled back. | The temp file is deleted on abort. |
| `isolation.level` consumer configuration. | Not applicable — ksink does not implement the consumer protocol. | Not applicable. |

**In summary:** ksink provides a best-effort transactional file output where
committed transactions produce a final file and aborted transactions clean
up their temp file. It does **not** provide the exactly-once guarantees of a
real Kafka cluster. It is designed for testing and lightweight sink
scenarios where the rename-as-commit / delete-as-abort file semantics are
sufficient.
