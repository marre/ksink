# Unified transactional write-ahead log (PR #52 redesign)

Status: implementation design, reviewed against restart, concurrency, and external API failure semantics.

## Requirements

- One authoritative durable history for producer IDs/epochs, per-partition batch sequences, transaction instance IDs, serialized output bytes, and irreversible commit/abort decisions.
- Support Kafka producer protocol, public `Read()` events and `AckFunc`, output formats, `{topic}`/`{partition}`/`{txnID}` routing, atomic committed directories, and nontransactional writers.
- Successful Produce must not precede durability of accepted output bytes **and** the corresponding sequence checkpoint. Successful EndTxn must not precede durable publication and a recorded outcome.
- Process crashes must never cause an irreversible commit to be discarded or a second committed transaction directory to be created.
- No database or third-party storage engine.

## Architecture

1. A single exclusive owner opens a **versioned checksummed append-only WAL** under the transactional output root. Independent exclusive locks protect the WAL directory and the filesystem output root during startup, writes, and recovery. A startup validation binds the WAL to its output pattern to avoid replay to the wrong destination.
2. Records are length-framed and checksummed. Replay verifies complete records, rejects corruption and unknown versions, and truncates only a verifiably torn **trailing** record. Each record is flushed and fsynced before its associated Kafka acknowledgement. A failure makes the owner fail closed.
3. State-changing Kafka requests append coordinator-state checkpoints in the WAL before success responses (future delta encoding can reduce snapshot overhead). Every transaction receives a persisted, collision-resistant instance ID at `AddPartitionsToTxn`.
4. The filesystem writer appends **formatted payload bytes and relative output paths** as WAL data records. Those bytes—not staging files or a mutable output-format configuration—are authoritative during replay. Stage files can be written incrementally as a cache for fast commits but are disposable.
5. After all admitted Produce requests drain, EndTxn appends a durable `COMMIT` decision (or `ABORT`), before publishing or deleting output. An existing decision is immutable. A publish failure can only be retried or recovered; never turned into an abort.
6. The materializer makes a complete private transaction directory from logged bytes, fsyncs its files/directories, publishes with a same-filesystem directory rename, fsyncs parents, then appends a durable publication outcome. A previously published directory is recognized by instance ID. EndTxn records its successful response outcome before sending it.
7. Startup holds the exclusive lock and replays the WAL **before listening**. Incomplete transactions with no decision are discarded. Committed transactions are reconstructed and published idempotently; aborted transactions are cleaned. Only after successful reconciliation may old epochs be fenced and new producers admitted.
8. WAL compaction uses a verified, fsynced checkpoint plus surviving undecided/decided-but-unpublished payloads, swapped atomically. Payload frames are indexed by disk offsets and read one at a time rather than retained in coordinator memory. The log tracks obsolete frame bytes incrementally (including replaced state snapshots and published/aborted payloads). Compaction runs when at least 16 MiB is reclaimable AND reclaimable space is at least 25% of WAL size. A completed commit or abort evaluates the policy immediately; size alone never triggers repeated full rewrites of a large active transaction. Undecided transactions discarded on restart are recorded as aborted to reclaim their WAL bytes. There are no independent coordinator snapshots or output commit-intent markers in the new WAL path.

## Failure review (acceptance criteria)

| Crash/failure point | Required behavior |
| --- | --- |
| Incomplete WAL append | Truncate only the torn tail; reject corrupt complete frames |
| Produce after bytes logged, before sequence checkpoint | No success acknowledgement; no orphan commit; the producer can retry or abort |
| Active transaction with no COMMIT/ABORT | Abort on restart, fence old epoch |
| After durable COMMIT but before directory construction | Rebuild entire output from WAL, publish once |
| During construction or file fsync | Discard and deterministically rebuild disposable staging |
| After directory rename, before parent fsync/ACK | Check existing committed identity, finalize durability barrier without duplicate publication |
| After publication, before EndTxn response | Reuse persisted outcome or reconcile and record it; do not emit the sink commit twice |
| Abort after durable COMMIT | Reject; decision is irreversible |
| WAL corruption or output mismatch | Fail startup closed; never silently discard committed data |
| Concurrent ksink processes on same root | Reject second writer before replay |
| WAL compaction interrupted | Either old valid WAL or complete new checkpoint, never half a checkpoint |
| Concurrent retries/EndTxn | Serialize dedup and drain accepted writes before committing |

## Rejected alternatives / review changes

- **Separate coordinator snapshot + filesystem commit marker:** rejected: independent authoritative decisions can contradict on a crash.
- **WAL contains only metadata:** rejected: a COMMIT could outlive separately staged bytes, preventing recovery.
- **Only construct files during commit:** valid but potentially expensive; retain incremental staging as a disposable cache.
- **Reusing old epoch after restart:** rejected. Epoch must advance on reinitialization, while completed EndTxn response-loss retries may be recognized using recorded outcomes.
- **Advertising generic exactly-once guarantees:** rejected. The guarantees apply to the controlled filesystem materializer, not arbitrary user `Read()` side effects, consumer offsets, HTTP/S3, or malicious alteration of the committed directory.

## Storage format and limits

There is one supported transactional storage model: a shared WAL and
its derived committed directories. In transactional mode the Kafka server
requires `Config.TransactionStateDir` or `WithTransactionLog`; the
filesystem writer requires the same WAL. No standalone commit-intent
journal, alternate in-memory transaction coordinator, automatic
migration from earlier storage layouts, or legacy file-writer
constructor remains.

The WAL must have reliable filesystem sync and rename semantics and
exclusive ownership during startup, writes and recovery. Unrecognized
staging entries fail startup rather than being deleted or silently
recovered. Disk exhaustion and unrecoverable corruption fail closed.
The WAL does not automatically roll back external side effects from
arbitrary `Read()` consumers.

## Verification checklist

- Unit tests for WAL CRC/truncation, replay, compaction, producer state and sequence dedup.
- Recovery tests for every crash boundary with exact output contents and no duplicate directory.
- Integration tests with real Franz-go transactional clients, concurrent retries and startup fencing.
- `go build ./...`, `go vet ./...`, `go test -race -count=1 ./...`, and lint on the PR head.


## Segmented binary WAL implementation plan and crash review

This revision replaces the single `transactions.wal` data file (no storage-format
compatibility required). The WAL is still the **only transactional authority**.
A small `manifest.json` selects physical files, *not* commit outcomes.

- **Files:** `manifest.json` lists an optional compacted snapshot and ordered
  numbered segments in `segments/`. Each segment is immutable after rotation;
  the last segment accepts new frames. A checkpoint is a separately fsynced
  `snapshots/<id>.wal` containing the current producer state, binding, completed
  outcome markers, and all live payload records.
- **Crash-safe rotation:** sync a new empty segment and its directory before
  atomically publishing a fsynced manifest referring to it. Append to the new
  segment only *after* manifest publication; a crash before publication leaves
  an unreferenced empty file, never an acknowledged record.
- **Reclamation:** track live and obsolete frame bytes by file. Remove fully
  obsolete, sealed segments by first atomically publishing a manifest without
  those segments and then deleting them. If mixed live/dead bytes remain above
  the reclaimable threshold, write a fresh checkpoint with the live frames,
  fsync the checkpoint and new append segment, then atomically swap the
  manifest. Only after the manifest is synced may old files be removed.
  Replaying the selected snapshot/segments must never silently accept a missing
  referenced file.
- **Binary output records:** outer length + CRC32C stays; a tagged,
  length-validated binary payload stores transaction ID, instance, relative
  output filename, and raw message bytes without JSON/base64 expansion.
  Compact coordinator snapshots may remain JSON. The frame tag/version
  rejects unknown formats.
- **Batch durable appends:** `AppendOutputs` validates a whole batch under
  the WAL mutex, writes bounded checksummed frames, performs **one fsync**,
  then publishes the in-memory index and returns success. Filesystem staging
  remains disposable; the CLI formats one `MessagesEvent` at a time and calls
  `WriteBatch`. State transitions and COMMIT still use their own durability
  barriers; no acknowledgement may overtake the batch fsync.
- **Concurrency:** recovery, compaction, output streaming and appends hold the
  WAL mutex while reading indexed offsets, so no stream can observe a file
  after the manifest has replaced or deleted it.
- **Failure boundaries:** incomplete append is rolled back to the last fully
  checksummed record; corruption of an acknowledged complete record fails
  closed. Inject failures before/after segment fsync, manifest publication,
  checkpoint fsync, checkpoint publication, and old-file removal. After any
  crash, replay must yield exactly the old durable state or the new one,
  never an acknowledged commit without payloads or duplicated materialization.

The manifest is a physical file-set selector; it cannot itself make a
transaction COMMIT or ABORT. Atomic replacement is required for segment-set
changes. A manifest failure causes startup to fail closed rather than
inventing an empty log. Tested filesystems must provide durable file and
parent-directory fsync plus same-filesystem atomic rename.
