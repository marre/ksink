# Phase 1: WAL gap closure and durable ordering

Status: **implementation plan**; no functional changes in this document.

## Observed gaps in current PR #52

The current WAL has useful foundations: checksummed binary payload frames, fsynced batched appends, segmented manifest-backed storage, checkpoints and crash failpoints. Retain those.

1. **No authoritative record positions.** `pkg/ksink/handlers.go:parseRecords` obtains `Message.Offset` from a synthetic fetch decode; `ProduceResponseTopicPartition` does not assign a meaningful broker append offset. `WALOutputRecord` stores transaction ID, instance, destination path and formatted bytes, but no global position or topic offset.
2. **Dedup state and records use different operations.** `handleProduce` delivers output, then snapshots producer sequences in another WAL operation. The redesigned flow must assign positions, persist accepted payloads and durable producer-sequence decisions as one logical atomic acknowledgement boundary.
3. **Retention is tied to directory publication.** `TransactionLog.apply` discards output on `published`; the `state` replay path can discard older instance references when a producer starts a new generation. Neither rule is safe when a successful commit waits behind an unresolved earlier record.
4. **Recovery is keyed to the latest producer state.** `RecoveryTransactions()` enumerates current `state.States`, not all committed-but-not-yet-emitted generations. The ordered writer needs persistent outcome/history independent of subsequent transactional.id reuse.
5. **Streaming holds a global WAL lock for filesystem callbacks.** `StreamOutput` holds `TransactionLog.mu` while consuming every payload. The future publisher should not block incoming WAL appends during potentially slow disk writes.
6. **Space and checkpoint behavior need reevaluation.** Current compaction and sealed-segment reclamation assume published output is obsolete. Unresolved old transactions can pin large spans of WAL; no measured backlog/backpressure policy or scaled benchmark exists for the new workload.
7. **The WAL stores serialized output paths and bytes.** Output filenames will be assigned at materialization time and rotation can split records differently. WAL records must become filename-independent logical ordered data, while retaining deterministic formatted bytes and their format/configuration binding.

These are **design gaps / risks identified from code structure**, not all independently demonstrated data-loss bugs.

## Proposed WAL model

Each accepted application record carries:

- `globalSeq` (monotonic uint64 for ordering the one output stream);
- `topic`, fixed `partition=0`, and a broker-assigned, monotonic **topic offset**;
- `transactionInstance` or an explicitly committed nontransactional-batch identity;
- serialized output bytes (or a fully specified deterministic payload representation), plus checksum;
- durable producer ID/epoch and per-topic producer batch signatures required for dedup.

Use metadata frames for `txn-begin` / `txn-commit` / `txn-abort`, durable producer state and publication progress; preserve active/open records through checkpoints. If maintaining precise Kafka topic offsets, account for Kafka control-marker offset semantics separately from the global application-record sequence. Never conflate **Kafka producer sequence**, **topic offset**, **globalSeq**, or future **output file sequence**.

## Implementation steps, in order

1. **Define vNext WAL schema and replay invariants.** Add explicit record/metadata types and version checks. Old unmerged WAL state must be refused rather than implicitly migrated. Define maximum frame/record limits, overflow behavior and invariant validation.
2. **Allocate durable positions atomically with accepted record batches.** Under a serial WAL ordering boundary, reserve global sequence and topic offsets, produce deterministic formatted output, persist payload and dedup/next-offset checkpoint, fsync, then acknowledge Produce. Failed or duplicate batches must not gain fresh positions. Return correct base offsets in Produce responses.
3. **Persist transaction outcomes by immutable instance.** Retain their first/last globalSeq, state and data references until all associated stream positions are durably published or skipped, even when a transactional ID is reused. Do not rely solely on `state.States` or on a `published` per-transaction marker.
4. **Introduce durable materialization progress.** WAL stores an ordered output cursor and finalized-file records (`fileSeq`, globalSeq bounds, path, byte count/digest). Cursor advances **only after** the corresponding immutable file is durably finalized or a wholly aborted span is durably skipped.
5. **Redesign replay/indexing and WAL reclamation.** Recover all pending decisions and indexed records, not just the latest producer generations. Keep any payload needed for unpublished output live. Deletion and compaction require a proof of non-liveness; checkpoint the cursor, decisions, producer state and all live record references together.
6. **Bound memory and contention.** Stream by global position with bounded buffers/read leases or snapshot segment handles; prevent GC of leased segments without holding the write mutex throughout output I/O. Benchmark snapshot frequency and remove unnecessary O(all-producers) full state snapshots per Produce if material.
7. **Define disk exhaustion and error response policy.** Expose backlog bytes/oldest unresolved globalSeq and a configurable fail-closed/backpressure limit; do not allow unbounded WAL growth while the ordered consumer is blocked.

## Acceptance tests / fault matrix

- Two producers interleaved A/B/A; B commits first; replay and output remain globally 0/1/2 after A resolves.
- Two accepted batches concurrent across topics; unique globalSeq, correct topic offsets, persisted across restart.
- Exact duplicate retries and lost Produce acknowledgements do not add records or advance positions. Out-of-order/reused producer sequence rejected; compare with real franz-go clients.
- Abort creates observable gaps but no emitted records. No gaps may appear from failed WAL appends that were nevertheless acknowledged.
- Commit followed by transactional.id reuse **before publication** retains all earlier committed payloads across checkpoint and sealed-segment GC.
- Crash after WAL fsync, before response; after checksum frame; during manifest rotation; during checkpoint and GC; corrupt sealed segment vs torn active tail.
- Compaction with 1 GiB unpublished backlog, slow writer, concurrent ingestion; memory, lock hold time and disk usage measured.
- Fail closed when acknowledged required WAL data disappears or next counters overflow; never reinitialize silently.

## Exit criteria

Ordering is durable at the Produce-ack boundary; all outstanding generations and decisions survive recovery/compaction; no record needed by the ordered publisher is reclaimed; correct producer and record positions can be reconstructed without reading any transaction output directory.
