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
| The transaction coordinator tracks transaction state across broker restarts. | A shared transactional WAL persists producer IDs, epochs, record-batch sequence history and commit/abort decisions. In-flight transactions without a decision are abandoned on restart, and old producers must reinitialize. | The WAL also includes formatted payload bytes and output paths, so a committed directory can be reconstructed from the same authoritative decision. |
| `sendOffsetsToTransaction` atomically commits consumer offsets with the transaction. | Not supported. ksink is a sink, not a full broker with consumer groups. | Not supported. |
| Zombie fencing: restarting a producer with the same `transactional.id` aborts the old instance's pending transaction. | **Supported.** `InitProducerID` fences an in-flight producer after aborting it; after a process restart, an old epoch cannot start a new transaction without first reinitializing. Durable epoch fencing requires `TransactionStateDir`. | The old transaction's private staging directory is deleted on abort. |
| Transactions can span multiple topic-partitions atomically. | All events for the same `transactional.id` share a durable instance ID and one irreversible WAL decision. | All files within one transaction directory are made visible together via one same-filesystem directory rename. The atomicity guarantee applies only to readers of `committed/`. |
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

### Unified transaction WAL and filesystem recovery

The `ksink --transactional` CLI uses a **single durable append-only WAL**
under `.ksink-kafka-state/transactions.wal` at the fixed output root.
`--transaction-state-dir` selects a different location for this WAL.
The WAL is the authoritative record of:

- Producer IDs and epochs, per-partition batch sequences and deduplication
  history, active transaction instances and EndTxn outcomes.
- **Exact formatted message bytes** and their relative output paths.
- Irreversible COMMIT/ABORT decisions and completed publication markers.

WAL records are checksummed and length-framed. Each acknowledged state
transition or payload append is synchronized to disk. An incomplete final
record can be discarded during replay; an invalid complete record fails
startup. Compaction uses an fsynced, atomically replaced checkpoint,
retaining undecided or unpublished transaction data.

**Space reclamation:** ksink tracks obsolete WAL record bytes (superseded
coordinator snapshots, abandoned transactions, and published/aborted output).
It compacts automatically once **at least 16 MiB is reclaimable** and those
bytes make up **at least 25% of the current WAL**. Completion of a commit or
abort checks this policy immediately, without waiting for another Produce.
A large active transaction does not repeatedly trigger compaction merely
because the total log exceeds a fixed size. The WAL can still exceed these
thresholds while holding live transaction payloads; plan disk space for
the largest pending transactions. No timed compaction is required.

**Commit protocol:** first drain all accepted Produce requests, then append
and synchronize a COMMIT decision. The writer validates/rebuilds staging
from the WAL if necessary, synchronizes files and directory entries, and
publishes the whole transaction directory with one same-filesystem rename.
It synchronizes the rename parents and records a durable publication
outcome before EndTxn can return success. Once COMMIT is decided, abort is
not allowed. `committed/<transactional.id>-<nonce>/` directories have
a `.ksink-wal-instance` metadata file identifying the WAL generation.
Consumers should ignore this marker and read only the payload files.

**Restart recovery runs before the Kafka socket accepts connections.**
The materializer discards uncommitted staged files, replays WAL payloads
for decided commits, and publishes each committed generation at most once.
A rename already completed is recognized and finalized. The server
then reconstructs producer state and resolves completed EndTxn outcomes.
An old producer epoch must call `InitProducerID` again before new writes.

An exclusive WAL file lock and an independent filesystem output-root lock
reject concurrent processes attempting to use the same transaction
state/output. A corrupted WAL or committed-directory identity mismatch
stops startup rather than guessing a transaction outcome. The WAL is the
source of truth; staging directories are only a cache.

**Embedded transactional server:** Configure `Config.TransactionStateDir`
to give `pkg/ksink` an exclusively owned durable WAL, or pass an already
opened WAL through `ksink.WithTransactionLog`. An in-memory transactional
coordinator is not supported. For filesystem output, open
`ksink.OpenTransactionLog`, pass it to both
`output.NewTxnFileWriter(pattern, wal)` and
`ksink.WithTransactionLog(wal)`, and close the WAL after stopping the
writer and server. The `Read()` and `AckFunc` event model still
delivers messages before commit; external application side effects
remain the application's responsibility.

**Storage format:** The WAL is the only transaction authority. No
`state.json` snapshot or `.ksink-commit-intent` journal is read or
migrated. On startup, an unfamiliar staging directory stops recovery
rather than being deleted or interpreted as a committed transaction.
Start with a clean WAL/staging directory when switching from older
transactional storage implementations.

**Limits:** Filesystem guarantees require durable file/parent-directory
fsync and atomic same-filesystem directory rename. Crashes before a durable
commit decision abort the in-flight transaction even if Produce was
previously acknowledged; the producer must reinitialize and resend.
Arbitrary application side effects performed through `Read()` are **not**
rolled back, and consumer offsets and non-filesystem outputs are not
included in the WAL's publication protocol. This is not a general Kafka
broker or a blanket exactly-once guarantee. External deletion of published
directories, failed storage, or corrupt complete WAL records fail closed.

**Performance:** Data temporarily exists in the WAL and staged/committed
output; replay and file regeneration add work after a crash. Syncing each
accepted record and periodic checkpoint compaction trade throughput for
durability. The log requires disk-space monitoring and sizing for peak
uncommitted transactions. Large transactions retain record offsets and relative paths in memory,
while payloads are streamed from the WAL during replay.
