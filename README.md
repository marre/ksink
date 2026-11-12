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

# Print the JSON schema for the jsonl output format
go run ./cmd/ksink json-schema

# Enable transactional produce support
go run ./cmd/ksink --addr :9092 --transactional --output 'messages-{txnID}.jsonl'

# Route messages to per-topic files using the {topic} placeholder
go run ./cmd/ksink --addr :9092 --output '{topic}.jsonl'
# produces orders.jsonl, events.jsonl, etc. — one file per topic

# Route to separate files per partition (or combine topic and partition)
go run ./cmd/ksink --addr :9092 --output '{topic}-{partition}.jsonl'
# produces orders-0.jsonl, orders-1.jsonl, etc.

# Combine {topic} with transactional output
go run ./cmd/ksink --addr :9092 --transactional --output '{topic}-{txnID}.jsonl'
# produces committed/my-txn-<unique-id>/orders-my-txn.jsonl
#          committed/my-txn-<unique-id>/events-my-txn.jsonl

# Transactional files may also be split by partition:
# go run ./cmd/ksink --transactional --output '{topic}-{partition}-{txnID}.jsonl'

# Transactional file output — the --output pattern must contain {txnID}.
# All files in a transaction live in ONE directory beneath the first static
# path prefix. A single same-filesystem directory rename publishes the whole
# transaction. Readers should only scan committed/, never .ksink-pending/.
#   During transaction: .ksink-pending/<txnID>-<nonce>/messages-<txnID>.jsonl
#   After commit:       committed/<txnID>-<nonce>/messages-<txnID>.jsonl
#   After abort:        the private staging directory is deleted
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
| Records written inside a transaction are invisible to `read_committed` consumers until commit. | Records are delivered to `Read` as `*MessagesEvent` immediately when produced. The `TransactionalID` field is populated so the consumer can buffer them. When the transaction ends, a `*TxnCommitEvent` or `*TxnAbortEvent` is delivered so the consumer can decide what to do. | Messages are staged in a private transaction directory. On commit the entire directory is renamed into `committed/` in one atomic filesystem operation; abort removes staging. Consumers must read only from `committed/`. |
| The transaction coordinator tracks transaction state across broker restarts. | No persistent producer ID, epoch, deduplication or transaction state; a Kafka producer must reinitialize after a server restart. | On writer startup, staged transactions **without** a durable commit intent are aborted, and transactions **with** an intent are finished idempotently before accepting new messages. Committed directories survive process restarts. |
| `sendOffsetsToTransaction` atomically commits consumer offsets with the transaction. | Not supported. ksink is a sink, not a full broker with consumer groups. | Not supported. |
| Zombie fencing: restarting a producer with the same `transactional.id` aborts the old instance's pending transaction. | **Supported in-process.** When `InitProducerID` is called with an existing `transactional.id` that has an in-flight transaction, the old transaction is aborted through `Read` before the epoch is bumped. | The old transaction's private staging directory is deleted on abort. |
| Transactions can span multiple topic-partitions atomically. | All events for the same `transactional.id` share a transaction context while active, but there is no durable coordinator. | All files within one transaction directory are made visible together via one same-filesystem directory rename. The atomicity guarantee applies only to consumers reading complete directories under `committed/`, not to consumers scanning staging or to process crashes. |
| Aborted records are never visible to `read_committed` consumers. | Aborted records were already delivered to `Read`. The `*TxnAbortEvent` signals the consumer to clean up, but application-level processing that happened before the abort is not rolled back. | The private staging directory is deleted on abort, so it never appears under `committed/`. |
| `isolation.level` consumer configuration. | Not applicable — ksink does not implement the consumer protocol. | Not applicable. |

**In summary:** ksink atomically publishes every file of a transaction as
one directory under `committed/<transactional.id>-<nonce>/`. The output
layout is **breaking** compared with the prior flat-file layout: output
patterns now name files *inside* transaction directories, and downstream
readers must scan only `committed/`. The fixed directory prefix of the
pattern determines the output root. A rename requires staging and committed
directories on the **same filesystem**, with filesystem semantics supporting
atomic directory moves (test network-mounted filesystems separately).

### Filesystem recovery and durability

The transactional filesystem writer maintains a **durable commit-intent
journal** inside each staging directory. It synchronizes each accepted
message to disk before acknowledging its write. On commit, it closes and
synchronizes the payloads and subdirectories, writes and synchronizes
`.ksink-commit-intent`, and publishes the complete directory using one
same-filesystem rename. It synchronizes the affected parent directories
before reporting commit success.

On startup, ksink takes an exclusive OS-level lock on the output root and
reconciles pending directories **before accepting new messages**:

- No commit-intent marker: discard the uncommitted staging directory.
- Valid commit-intent marker: finish publishing the transaction.
- Corrupt marker or irreconcilable filesystem error: fail startup rather than
  silently discard data.

**A commit intent is irreversible.** Once recorded, an aborted/failed
directory publication must be retried or recovered, not rolled back. A
successful directory rename is never rolled back. On graceful shutdown,
uncommitted staging is cleaned; pending committed intents are preserved
for recovery. One active writer process per output root is enforced by
the lock file `.ksink-lock`. The marker is metadata, not a message file,
and readers must ignore it.

This filesystem journal does **not** persist Kafka producer IDs, epochs,
record sequence/deduplication state or EndTxn outcomes in the broker.
Thus it cannot resume active producer transactions after a server restart
and does not provide cross-restart exactly-once Kafka delivery. A crash
before durable commit intent means an active transaction is aborted even
if some Produce requests were acknowledged. A crash after intent means the
entire transaction is eventually published on next successful startup.
Consumers must inspect only `committed/`. Durable behavior requires a
filesystem honoring file/parent-directory fsync and same-filesystem
atomic directory rename; unsupported filesystems fail instead of silently
claiming durability. Filesystem failure or lost power during unacknowledged
commit may leave an ambiguous outcome until startup recovery runs.

**Performance note:** each transactional write performs a file fsync and
synchronizes its directory path. This favors correctness and crash safety
over maximum ingest throughput; batching/durability policy controls can
be considered separately.
