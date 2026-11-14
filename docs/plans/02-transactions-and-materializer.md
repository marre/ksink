# Phases 3–4: transaction decisions, read-committed visibility, ordered files

Status: **implementation plan, not yet implemented**. Depends on [WAL/offsets](01-wal-and-offsets.md).

## Goal and explicit scope

Replace `.ksink-pending/<txn-instance>/` and `committed/<txn-instance>/` with **per-topic immutable output file sequences**, containing only committed application records in ascending `(topic, partition=0, broker-assigned offset)` order. One materializer service coordinates a separate stable cursor and file writer per topic.

Kafka transaction outcomes are atomic *logically* across multiple topics, but Kafka does not supply a simultaneous globally ordered cross-topic snapshot to independent consumers. Match that: a durable single coordinator decision applies to every participant, but a per-topic file can become available at a different time from another topic's file after the decision. **If an application needs cross-topic all-files-at-once visibility, the final-file reader contract does not provide it**; a cross-topic publication manifest would be a separate feature.

## Coordinator and topic log protocol

1. **Begin/enroll:** `InitProducerID` establishes fencing identity; `AddPartitionsToTxn` enrolls `topic/0` in the coordinator. Persist transaction instance + participants. No topic other than participant partition 0 is supported.
2. **Produce:** verify producer epoch and producer batch sequence; persist payload, assigned topic offsets and dedup state in that topic's WAL before responding. Register/validate participants before acknowledging any record they own.
3. **EndTxn:** fence new Produce, wait for accepted in-flight requests, validate all participant WAL appends are durable; persist **one irreversible COMMIT or ABORT decision** in coordinator WAL, including the participant set and durable last accepted offsets. EndTxn success is based on the durable decision **not on output file creation**.
4. **Per-topic visibility:** for each topic, compute a stable offset boundary at the earliest unresolved transaction on that topic. Walk records below the boundary in ascending offsets; emit committed or nontransactional records, skip aborted records, never expose unresolved records. Commit metadata must be validated against the coordinator decision. An unresolved `orders` transaction does not block unrelated `payments` traffic.
5. **Cross-log recovery:** on restart, read the coordinator first and validate every referenced topic WAL and its manifest/checkpoint. If a COMMIT is durable, process it for *all* participants even if their local progress differs. An incomplete/undecided enrollment is not a commit. Repair, retry or fail closed rather than invent an outcome.
6. **Publication:** build a bounded temporary file for a single topic, fsync, close, atomic rename to a never-before-used final name, fsync containing directory. Only after that durable finalization may the topic publication cursor advance in durable state. Include expected final filename/sequence, offsets, size and digest in publication intent/receipt metadata.
7. **Crash reconciliation:** on restart, if final file exists but WAL publication receipt is missing, verify immutable name, offset boundaries, byte count and checksum, then idempotently record it. If receipt exists but final file is missing/corrupt, **fail closed**. Never replace an already published file with different contents.
8. **GC:** mark topic WAL payloads obsolete only after all contained records are safely published or intentionally skipped, including checkpointing publication position. Never forget older blocked generations when the same transactional ID starts new transactions.

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
- Introduce `OrderedTopicMaterializer` with independent per-topic cursor, rotation state and one worker per topic or a bounded pooled implementation. Do not hold WAL mutating locks during destination filesystem writes.
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
- Missing output file with durable publication receipt **cannot** trigger blind replay into another file sequence; no silent duplication.
- A large file may contain several transactions; one transaction can span several files and topics. An open transaction blocking a topic cannot prevent other topic writers from rotating and publishing.
- A consumer sorting only completed file numbers **within a topic** sees exactly one copy of every committed record, no aborted/unresolved record, and ascending topic offsets.
- EndTxn succeeds with output filesystem temporarily unavailable once coordinator decision is durable; backlog builds within configured limits, and errors/health are visible. Define and test disk-full/backpressure behavior separately.
- Race, fuzz/codec, crash injection and integration tests pass with real Kafka client producers.

## Exit gate

The ordered materializer fully replaces per-transaction directories, and Kafka-compatible commit/abort responses and per-topic `read_committed` behavior remain recoverable under all tested crash boundaries.
