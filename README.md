# ksink

A lightweight Kafka-protocol-compatible server library and tool for Go. It accepts produce requests from standard Kafka producers without requiring a full Kafka cluster.

## Features

- Accepts produce requests from any Kafka producer (kafka-console-producer, librdkafka, franz-go, etc.)
- Pull-based API: call `ReadBatch` to receive messages, then acknowledge
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
        msgs, ack, err := srv.ReadBatch(ctx)
        if err != nil {
            log.Fatal(err)
        }

        for _, msg := range msgs {
            fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
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

Transaction lifecycle events (commit/abort) are delivered through `ReadBatch`
as messages with a non-zero `TxnEvent` field. This ensures that all data
messages for a transaction have been processed before the commit/abort event
is handled:

```go
for {
    msgs, ack, err := srv.ReadBatch(ctx)
    if err != nil {
        break
    }
    for _, msg := range msgs {
        switch msg.TxnEvent {
        case ksink.TxnCommit:
            // e.g. rename temp file to final name
        case ksink.TxnAbort:
            // e.g. delete temp file
        default:
            // Normal data message — write to output
        }
    }
    ack(nil)
}
```

<details>
<summary>Deprecated: <code>WithTxnEndFunc</code> callback</summary>

The push-based `WithTxnEndFunc` callback is still supported for backward
compatibility but is deprecated. Prefer handling `TxnEvent` in the
`ReadBatch` loop instead.

```go
// Deprecated — use TxnEvent in ReadBatch instead.
srv, _ := ksink.New(cfg,
    ksink.WithTxnEndFunc(func(txnID string, commit bool) {
        if commit {
            // e.g. rename temp file to final name
        } else {
            // e.g. delete temp file
        }
    }),
)
```

</details>

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

| Real Kafka behaviour | ksink behaviour |
|---|---|
| Records written inside a transaction are invisible to `read_committed` consumers until commit. | Records are delivered to `ReadBatch` immediately when produced. The `TransactionalID` field is populated so the consumer can buffer them. When the transaction ends, a `TxnEvent` marker is delivered through `ReadBatch` so the consumer can decide what to do on commit/abort. |
| The transaction coordinator tracks transaction state across broker restarts. | No persistent transaction state. If ksink restarts, in-flight transactions are lost (uncommitted temp files are cleaned up on `Close()`). |
| `sendOffsetsToTransaction` atomically commits consumer offsets with the transaction. | Not supported. ksink is a sink, not a full broker with consumer groups. |
| Zombie fencing: restarting a producer with the same `transactional.id` aborts the old instance's pending transaction. | **Supported.** When `InitProducerID` is called with an existing `transactional.id` that has an in-flight transaction, the old transaction is automatically aborted via a `TxnAbort` event delivered through `ReadBatch` and the producer epoch is bumped. |
| Transactions can span multiple topic-partitions atomically. | Supported at the file level — all messages with the same `transactional.id` go to the same temp file regardless of topic/partition. When the `{topic}` placeholder is used, each topic within a transaction gets its own file; all are committed or aborted together. |
| Aborted records are never visible to `read_committed` consumers. | Aborted records were already delivered to `ReadBatch`. The `TxnAbort` event signals the consumer to clean up (e.g. delete the temp file), but application-level processing that happened before the abort is not rolled back. |
| `isolation.level` consumer configuration. | Not applicable — ksink does not implement the consumer protocol. |

**In summary:** ksink provides a best-effort transactional file output where
committed transactions produce a final file and aborted transactions clean
up their temp file. It does **not** provide the exactly-once guarantees of a
real Kafka cluster. It is designed for testing and lightweight sink
scenarios where the rename-as-commit / delete-as-abort file semantics are
sufficient.
