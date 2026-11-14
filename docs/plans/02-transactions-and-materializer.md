# Phases 3–4: transaction decisions, read-committed visibility, ordered files

Status: **implemented in PR #52; operational verification remains ongoing**. Ordered materialization and CLI cutover are implemented. Depends on [WAL/offsets](01-wal-and-offsets.md).

## Goal and explicit scope

Replace `.ksink-pending/<txn-instance>/` and `committed/<txn-instance>/` with **per-topic immutable output file sequences**, containing only committed application records in ascending `(topic, partition=0, broker-assigned offset)` order. One materializer service coordinates a separate stable cursor and file writer per topic.

Kafka transaction outcomes are atomic *logically* across multiple topics, but Kafka does not supply a simultaneous globally ordered cross-topic snapshot to independent consumers. Match that: a durable single coordinator decision applies to every participant, but a per-topic file can become available at a different time from another topic's file after the decision. **If an application needs cross-topic all-files-at-once visibility, the final-file reader contract does not provide it**; a cross-topic publication manifest would be a separate feature.

## Shared-WAL coordinator and logical topic-stream protocol

1. **Begin/enroll:** `InitProducerID` establishes fencing identity; `AddPartitionsToTxn` enrolls `topic/0` in shared-WAL coordinator metadata. Persist transaction instance + participants. Only partition 0 is supported.
2. **Produce:** verify producer epoch and producer batch sequence; persist payload, assigned topic offsets and dedup state in the **shared WAL**, indexed by logical topic stream, before responding. Register/validate participants before acknowledging any record they own.
3. **EndTxn:** fence new Produce, wait for accepted in-flight requests, validate all shared-WAL appends are durable; persist **one irreversible COMMIT or ABORT decision** in the same WAL, with marker offsets allocated atomically for each enrolled topic/0. Record Kafka transaction control-marker positions for every participating topic as part of that durable outcome, without exposing those markers as application records. EndTxn success is based on the durable decision **not on output file creation**.
4. **Per-topic visibility:** for each topic, compute a stable offset boundary at the earliest unresolved transaction on that topic. Walk records below the boundary in ascending offsets; emit committed or nontransactional records, skip aborted records, never expose unresolved records. Commit metadata must be validated against the coordinator decision. An unresolved `orders` transaction does not block unrelated `payments` traffic.
5. **Single-WAL recovery:** on restart, replay the authoritative shared-WAL manifest/checkpoint and reconstruct all logical topic streams and transaction decisions. If a COMMIT is durable, process it for *all* participants even if their publication progress differs. An incomplete/undecided enrollment is not a commit. Repair, retry or fail closed rather than invent an outcome.
6. **Publication:** build a bounded temporary file for a single topic, fsync, close, atomic rename to a never-before-used final name, fsync containing directory. Only after that durable finalization may the topic publication cursor advance in durable state. Include expected final filename/sequence, offsets, size and digest in publication intent/receipt metadata.
7. **Crash reconciliation:** on restart, if final file exists but WAL publication receipt is missing, verify immutable name, offset boundaries, byte count and checksum, then idempotently record it. If receipt exists but final file is missing, **do not silently replay or reuse its filename**: it may have been deleted after consumption. Report diagnostics, and preserve the durable cursor. If an expected pending-intent file exists with mismatched bytes, fail closed. A stronger loss-detection guarantee for already-consumed files would require an explicit reader acknowledgement/retention protocol and is out of scope.
8. **GC:** mark shared-WAL payloads obsolete only after the corresponding topic-stream records are safely published or intentionally skipped, including checkpointing that topic's publication position. Never forget older blocked generations when the same transactional ID starts new transactions.

## Transaction ordering example

```text
orders/0:  100 A("created"), 101 B("paid"), 102 A("updated")
payments/0:  50 C("received")
```

- B commits first while A remains open. `orders` stays blocked at 100. C can commit and `payments` may publish offset 50 independently.
- A commits: `orders` publishes 100, 101, 102 (not B first). If A aborts, it skips 100 and 102 and publishes B at 101 once the boundary advances.
- If transaction D touches **both** topics, the coordinator makes one durable COMMIT decision. The corresponding messages become *eligible* in both topics, subject to each topic's local stable position. They need not appear in both sets of files at the same wall-clock moment.

## Cutover from transaction directories

- First introduce durable coordinator decisions and per-topic readers behind interfaces with pure in-memory test consumers. Preserve existing behavior until crash-safe ordered publication passes tests.
- Introduce `OrderedTopicMaterializer` with independent per-topic cursor, rotation state and a single polling worker. Do not hold WAL mutating locks during destination filesystem writes.
- Switch transactional CLI from `NewTxnFileWriter` to the new materializer. `Read()/AckFunc` may still be the producer-facing integration mechanism, but after EndTxn it only needs to persist/confirm decision; publication is independent.
- Remove `internal/output/txnfile*.go` code, per-transaction `.ksink-lock` publication logic, `.ksink-wal-instance` markers, staging/committed directory tests and old README statements when replacement tests cover the new invariants. Retain exclusive state/WAL locks.
- Reject old unmerged state layouts, old `{txnID}` output patterns, and unrecognized output directories explicitly. No silent migration or destructive cleanup.

## Nontransactional input while transactional output is enabled

**Recommended:** treat each accepted nontransactional Produce batch as *already committed* in its topic log. It still needs a topic offset, WAL durability barrier, a replayable dedup policy if idempotent, and the same per-topic stable-prefix/rotation path. Do not put it into a synthetic transaction or require an EndTxn. Reject acks=0 or otherwise document durability semantics if guarantee cannot be stated precisely.

## Failure and end-to-end acceptance criteria

- Cross-topic transaction COMMIT durable before any output; crash after only one topic file finalizes; recovery completes the other topic without duplicate first-topic file.
- A/B/A interleaving and B-first commit on one topic; blocked stable cursor correct.
- A transaction aborts after another is committed; aborted payloads never appear, other committed messages are emitted in offset order.
- Transaction ID reused, older decided output blocked; no lost historical data across WAL compaction or topic-log GC.
- Crash during file write, sync, rename, parent sync, publication checkpoint and GC; each recovery path either reconstructs exact output or stops with clear corruption error.
- Missing output file with durable publication receipt **cannot** trigger blind replay into another file sequence. Since consumers/operators may delete completed files, missing historical files are not by themselves a startup failure. Document that ksink cannot distinguish legitimate deletion from post-publication loss without a consumer acknowledgement protocol.
- A large file may contain several transactions; one transaction can span several files and topics. An open transaction blocking a topic cannot prevent other topic writers from rotating and publishing.
- A consumer sorting only completed file numbers **within a topic** sees exactly one copy of every committed record, no aborted/unresolved record, and ascending topic offsets.
- EndTxn acknowledges a durable WAL decision, not output publication. Any WAL or output write failure terminates the process; on restart, acknowledged data is recovered and published from the WAL. Metrics and bounded-backlog backpressure are out of scope.
- Race, fuzz/codec, crash injection and integration tests pass with real Kafka client producers.

## Exit gate

The ordered materializer fully replaces per-transaction directories, and Kafka-compatible commit/abort responses and per-topic `read_committed` behavior remain recoverable under all tested crash boundaries.

## Crash-window and deletion contract (revalidated)

The shared WAL now stores per-topic `WALTopicProgress` cursors and receipt metadata, with explicit skip advances over aborted data and control markers. Checkpoint recovery preserves the **absolute** cursor and next file number; old receipts can be coalesced. This does **not** make publishing a filesystem rename and recording a WAL receipt one atomic operation. If a process crashes after a final filename appears but before the receipt is durable, it must compare the existing final file with WAL-derived expected bytes and record the receipt idempotently. If a consumer removes that file in the same window, the publisher cannot distinguish “processed and removed” from “never published.” The reader must checkpoint `(topic, fileSeq)` to deduplicate effects; without a reader acknowledgement protocol ksink should not promise end-to-end exactly-once processing. Never overwrite a preexisting final file with different bytes.

**Implementation update (November 2026):** The ordered materializer and transactional CLI cutover have been implemented in PR #52. Output files are published with no-replace hard links, directory sync, and WAL publication receipts; restart reconciliation verifies bytes against the WAL. The desired failure policy is fatal: an output write failure must terminate the process with nonzero status rather than retry indefinitely. The worker stops immediately on the first publication error, and the CLI exits after closing the server. The worker also checks permanent WAL errors. Graceful shutdown joins the worker and force-flushes all stable records, reporting errors rather than pretending output completed. No disk/backlog admission-control or metrics feature is required. Verify fatal I/O error propagation and restart recovery.


## Revised operating policy (November 2026)

- **Fatal I/O:** any WAL or output write/sync/finalization failure must cause nonzero process termination; never acknowledge undurable data. Restart replays durable WAL and reconciles immutable output receipts. This policy is a requirement, not proof that all current code paths already enforce it.
- **No metrics or backpressure feature:** no metrics endpoint, per-topic health metrics, configured disk budget or bounded-backlog admission control is required. Disk exhaustion is a fatal I/O failure.
- **Rotation restart:** a process crash or restart may reset a pending in-memory time-rotation interval. Durable topic offsets, output cursors and file sequence numbers must not reset.


### Nontransactional idempotent producer retention

The ordered coordinator stores each nontransactional producer's epoch and deduplication history in the shared WAL. Successful new Produce batches atomically refresh a durable last-activity timestamp. When the 10,000-entry cap is reached, producers inactive for **more than seven days** can be reclaimed, but only after a replacement coordinator snapshot has been durably appended. Producer IDs are never reused. A Produce request using a reclaimed producer ID is fenced; clients must initialize a new producer ID. Exact retries within the retention window continue to deduplicate. State predating activity timestamps is conservatively retained until its next accepted batch. This is an inactivity-based bounded-retention guarantee, not unlimited historical deduplication.
