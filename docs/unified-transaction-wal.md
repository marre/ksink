# Unified WAL and ordered committed-file output

Status: implementation architecture for PR #52. The single-WAL design and
ordered filesystem writer are implemented; outstanding reliability gates are
listed at the end. There is **no backwards compatibility requirement** for
the unmerged WAL or transaction-directory layouts.

## Chosen model

ksink is a producer-facing Kafka-protocol sink, **not** a Kafka consumer or
general-purpose broker. Each topic has exactly partition 0. Multiple topics
and multi-topic transactions are supported.

- **One exclusive physical WAL** contains all producer identities, epochs,
  idempotent batch state, accepted application records, transaction
  participants/outcomes, Kafka control-marker offsets, and publication
  receipts. It is the only durable authority.
- Logical per-topic streams have independent Kafka offsets and
  `read_committed`-style stable positions; there is **no** cross-topic
  global offset or file order.
- Output is a separate immutable filename sequence **per topic**. Files
  contain only committed application records, ordered by Kafka topic offset.
  Aborted records and control markers occupy offsets but are not written.
- One materializer service scans stable records for all topics. A blocked
  `orders` transaction does not block independent `payments` publication.
- The transaction outcome is **WAL-durable** before EndTxn acknowledges
  success. Files are published later, under size/time rotation; a
  transaction may span files or a file may mix committed transactions.

```text
data/
  .ksink-kafka-state/
    manifest.json
    segments/
    snapshots/
  orders/
    messages-00000001.jsonl
    messages-00000002.jsonl
  payments/
    messages-00000001.jsonl
```

CLI example:

```bash
ksink --transactional \
  --output 'data/{topic}/messages-{seq:08}.jsonl' \
  --rotate-size 128MiB --rotate-interval 1m
```

With transactional CLI output, JSONL is the default format. The output
pattern requires one entire `{topic}` directory and a
`{seq:NN}` filename placeholder, width 6–20; there is no
`{txnID}`, `{partition}` or transaction directory.

## Durability protocol

1. A single process locks the WAL. It validates the versioned manifest,
   output binding, and framed CRC32C records. A torn active tail can be
   truncated; corrupt complete records and missing sealed segments fail
   closed. Other output roots cannot adopt the same WAL silently.
2. `InitProducerID` persists producer identity/epoch. `AddPartitionsToTxn`
   persists the sorted participant topic set, including later enrollments.
3. Produce assigns topic offsets **before formatting**. A checksummed,
   replay-atomic acceptance boundary contains the formatted bytes and
   producer-sequence snapshot. Complete but unaccepted frames are ignored
   after a crash. Exact retried transactional batches return their
   original base offsets.
4. EndTxn fences new Produce and drains admitted batches. One atomic
   checksummed terminal frame records COMMIT or ABORT with a control-marker
   offset for **every** enrolled topic. This outcome is immutable.
5. On recovery, a durable terminal frame is authoritative even if its
   subsequent coordinator checkpoint was interrupted. Unresolved
   transactions from a dead process are aborted and their old producer
   epochs fenced.
6. For each topic, the stable offset stops at its earliest unresolved
   transactional record. The materializer skips aborted data and
   control-marker positions, emitting committed application payloads only.
7. A selected output batch is written into a same-directory temporary file,
   fsynced, and **published by no-replace hard link**. Parent directories
   are fsynced; the materializer then records a per-topic publication
   receipt and next filename sequence in the shared WAL. The receipt may
   lag the rename.
8. On recovery, if the next final filename already exists but lacks a
   receipt, compare its complete bytes to the next committed WAL prefix
   and only then record the receipt. A mismatched filename is never
   overwritten. If the completed file is absent after a durable receipt,
   the WAL cursor still advances: the file may have been consumed and
   legitimately deleted.
9. WAL payloads and marker indexes below a **durable publication cursor**
   can be reclaimed. Compaction includes a per-topic absolute next-offset
   floor to avoid reusing discarded Kafka offsets. It copies all
   unpublished data (including a stuck topic) into a new checkpoint,
   fsyncs it, swaps the manifest atomically, then cleans old files.

## Rotation and consumption

`--rotate-size` is a target in B/KB/MB/GB/KiB/MiB/GiB and defaults
to **128 MiB**. `--rotate-interval` defaults to **1 minute**;
setting either to 0 disables that trigger. At least one is required.
Whichever condition first becomes eligible causes publication at a
whole-record boundary. Low-volume files are published on the timer even
without a later Produce. The time window is best-effort and may restart
after process restarts.

A consumer ignores `.ksink-ordered-*.tmp`, sorts completed filenames
within each topic, processes each file once, and checkpoints its own
`(topic, sequence)` before removing it. ksink does **not** implement
Kafka Fetch/consumer groups, a read-uncommitted file option, or
consumer-side exactly-once effects.

**Unavoidable ambiguity:** a downstream reader may consume and remove a
final file during the window *after its filesystem publication but before
the WAL receipt is durable*. After a crash ksink might regenerate that
file. A reader checkpoint or acknowledgment protocol is required for
end-to-end exactly-once side effects; WAL durability alone cannot prove
that a missing file was consumed.

## Failure and restart matrix

| Fault | Required outcome |
| --- | --- |
| Torn active WAL frame or incomplete accepted Produce batch | Discard only unaccepted tail; no false ack or offset allocation |
| Missing/corrupt acknowledged WAL segment | Stop; never reinitialize acknowledged history |
| Process death after COMMIT terminal fsync, before coordinator checkpoint | Recover COMMIT plus all participants' marker offsets |
| Earlier unresolved transaction on one topic | Hold later committed records on that topic only |
| Abort marker and aborted data | Consume offsets; skip output bytes |
| Crash before final file name appears | Rebuild it from WAL |
| Crash after final name, before receipt | Verify existing contents then record receipt |
| Conflicting or symlinked final filename | Fail closed; never overwrite |
| Crash after durable receipt and consumer deletes file | Do not reuse file sequence or replay acknowledged range |
| Failure during checkpoint/segment rotation | Replay complete old or new manifest generation, never partial state |
| Another topic blocked during GC | Keep every unpublished payload and each topic's independent offset floor |
| User changes WAL/output format | Reject incompatible version/binding; no automatic migration |

## Status and remaining release gates

- The new ordered CLI and materializer replace the legacy transaction
  directory code; the old layout is not read or migrated.
- Transactional integration, committed/aborted file tests, file rotation,
  WAL compaction and restart tests run through GitHub CI.
- Outstanding before merging: verify durable idempotence for
  **nontransactional producers**; more file-publication failpoints,
  backlog/backpressure and disk-full behavior; validate filesystem
  guarantees on target deployments; independent review of WAL compaction,
  replay and producer fencing. Keep PR #52 **draft** until these gates pass.

See [implementation roadmap](plans/00-ordered-output-roadmap.md) and
[test plan](plans/04-testing-rollout.md).
