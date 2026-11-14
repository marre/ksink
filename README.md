# ksink

A lightweight Kafka-protocol-compatible server library and tool for Go. It accepts produce requests from standard Kafka producers without requiring a full Kafka cluster.

## Features

- Accepts produce requests from any Kafka producer (kafka-console-producer, librdkafka, franz-go, etc.)
- Pull-based API: call `Read` to receive typed events, then acknowledge
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- TLS and mutual TLS (mTLS)
- Topic filtering
- Idempotent producer support
- Durable transactional produce and ordered, committed-only immutable files per topic

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

# Durable transactional output: one immutable, sortable file series per topic
go run ./cmd/ksink --addr :9092 --transactional \
  --output 'data/{topic}/messages-{seq:08}.jsonl' \
  --rotate-size 128MiB --rotate-interval 1m
# data/orders/messages-00000001.jsonl, data/payments/messages-00000001.jsonl, ...
# Each topic has independent Kafka offsets, file numbers and rotation.

# Nontransactional flat-file routing (no WAL/rotation)
go run ./cmd/ksink --addr :9092 --output '{topic}.jsonl' --output-format jsonl
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

### Designed limitation: one partition per topic

ksink intentionally supports **exactly one partition (partition 0) per topic**.
Kafka metadata advertises only partition 0, and direct `Produce` or
`AddPartitionsToTxn` requests for other partition numbers are rejected with
`UNKNOWN_TOPIC_OR_PARTITION`. This is a design choice to keep ksink a small
single-process sink rather than a general-purpose multi-partition Kafka broker.

Multiple topics and transactions spanning multiple topics are supported, each
with partition 0. A single durable transaction decision determines whether
records from **all** participating topics are committed or aborted. Each topic
has its own Kafka offsets and output-file sequence; there is no total
cross-topic order or simultaneous publication of all its output files.
The `{partition}` and `{txnID}` output placeholders are deliberately
unsupported in transactional filesystem mode.

Transactions can overlap and commit in different order from record arrival.
A `read_committed`-style file writer waits behind the earliest unresolved
transaction **within that topic**, then emits only committed records in offset
order. Aborted records and internal Kafka control markers consume offsets but
are not written into output files.

### Transactional Producer Support

The CLI's `--transactional` mode uses the durable shared WAL directly:
accepted Produce records and producer retry state are fsynced together, and
EndTxn success means its COMMIT/ABORT decision and per-topic control-marker
positions are durable. Publication is **asynchronous**: a successful EndTxn
does not mean a file has been rotated and made visible yet.

The embedded `pkg/ksink` API also supports a `Read`/`AckFunc` event
model; when using that API, application code is responsible for its own
transaction-aware side effects. `ksink.WithOrderedTransactionalOutput`
instead uses an offset-aware formatter and journals payloads directly to
the shared WAL; this is what the transactional CLI uses.

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

#### Filesystem and consumer contract

```bash
ksink --transactional \
  --output 'data/{topic}/messages-{seq:08}.jsonl' \
  --rotate-size 128MiB \
  --rotate-interval 1m
```

With no explicit `--output-format`, transactional CLI output uses **JSONL**
(nontransactional output still defaults to binary). The required `{topic}`
placeholder occupies one directory component, and `{seq:08}` supplies a
zero-padded **per-topic file number**, not the Kafka offset. Supported sequence
widths are 6–20. Topic names are encoded injectively, so distinct names never
share an output directory.

The first rotation condition wins. `--rotate-size` is a target size; a
single record can exceed it, but a record is never split. A nonempty,
low-volume file becomes visible after the configured interval; elapsed
time is best-effort and can restart after a crash. Setting one trigger to
`0` disables it, but at least one must remain enabled. Transaction
boundaries do **not** trigger file rotation.

Example:

```text
data/
  orders/
    messages-00000001.jsonl
    messages-00000002.jsonl
  payments/
    messages-00000001.jsonl
  .ksink-kafka-state/
    manifest.json
    segments/
    snapshots/
```

Completed files contain only committed application records, in topic-offset
order. Files can mix committed transactions, and one transaction can span
multiple files. The publisher uses a same-directory temporary file,
`fsync`, a **no-replace** hard-link publish, and directory `fsync` before
durably recording its topic's publication cursor and next file number.
Readers ignore `.ksink-ordered-*.tmp` files and read finished sequence
filenames in lexical order **within each topic**. There is no global
cross-topic order.

**Readers should checkpoint their last processed `(topic, file sequence)`
before deleting or moving completed files.** ksink will never reuse a
filename after a durable publication receipt. If a process crashes in the
small window *after* publishing a file but *before* recording its receipt,
recovery checks an existing final file against WAL payload bytes before
accepting it. If the consumer already processed and removed that file,
ksink cannot distinguish that from never publishing it and might re-create
it. End-to-end exactly-once side effects require reader cooperation.

#### Shared WAL, recovery and retention

One checksummed physical WAL owns all producer identities, epochs, dedup
history, topic/partition-0 offsets, enrolled transaction participants,
durable multi-topic COMMIT/ABORT outcomes, and per-topic file publication
receipts. It has segmented log files, an atomic manifest, and a compacted
checkpoint. Topic log offsets advance for transactional control markers
and aborted messages, even though neither appears in final files.

A restart replays only authoritative WAL segments and snapshots, fences old
producer epochs, preserves durably decided commits and aborts undecided
transactions. Acks for Produce and EndTxn depend on WAL durability, not on
file rotation. Output publication can lag when a transaction remains
unresolved in that topic, but an unrelated topic may continue publishing.

Published payloads and processed control-marker indexes can be reclaimed
once the topic's publication cursor is durably recorded. Compaction retains
absolute per-topic next-offset counters so an old completed file cannot
cause offset or filename reuse after recovery. Pending records remain live
in the WAL, including when another topic is blocked.

The current transaction WAL format is **v3** with manifest **v2**. **No
migration or backwards compatibility is provided** for older/unmerged
transaction WALs or old `committed/<txnID>` directories. Start a new
state/output directory when switching to this layout. An unsupported WAL
manifest fails startup rather than resetting acknowledged history.

#### Intentional limitations

ksink remains a producer-facing sink, not a complete Kafka broker: it does
not offer Kafka Fetch/consumer groups, `sendOffsetsToTransaction`, a
global topic ordering, or an `isolation.level` toggle for filesystem
output. Filesystem output always follows committed-only visibility.

The ordered writer requires a local filesystem supporting atomic hard-link
creation and durable file/directory `fsync`. Some network filesystems do not
provide these guarantees. Disk exhaustion or corruption is an error, not
an invitation to discard acknowledged WAL records. Output readers control
their own retention of finished files. The WAL must be sized for the largest
unresolved transaction and for publication backlog.

An ordered formatted record that produces zero bytes is unsupported: an
empty record cannot create a visible immutable file while preserving its
file-sequence receipt.
