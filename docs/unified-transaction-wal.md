# Unified transactional write-ahead log (PR #52 redesign)

Status: implementation design, reviewed against restart, concurrency, and external API failure semantics.

## Goals and compatibility

- One authoritative durable history for producer IDs/epochs, per-partition batch sequences, transaction instance IDs, serialized output bytes, and irreversible commit/abort decisions.
- Preserve existing Kafka producer protocol, public `Read()` events and `AckFunc`, output formats and `{topic}`/`{partition}`/`{txnID}` routing, atomic committed directories, and nontransactional writers.
- Successful Produce must not precede durability of accepted output bytes **and** the corresponding sequence checkpoint. Successful EndTxn must not precede durable publication and a recorded outcome.
- Process crashes must never cause an irreversible commit to be discarded or a second committed transaction directory to be created.
- No database or third-party storage engine.

## Architecture

1. A single exclusive owner opens a **versioned checksummed append-only WAL** under the transactional output root. One OS lock covers the WAL and its materialized filesystem output. A startup validation binds the WAL to its output pattern to avoid replay to the wrong destination.
2. Records are length-framed and checksummed. Replay verifies complete records, rejects corruption and unknown versions, and truncates only a verifiably torn **trailing** record. Each record is flushed and fsynced before its associated Kafka acknowledgement. A failure makes the owner fail closed.
3. State-changing Kafka requests journal compact producer/epoch/sequence/transaction-state updates in the WAL. Every transaction receives a persisted, collision-resistant instance ID at `AddPartitionsToTxn`.
4. The filesystem writer appends **formatted payload bytes and relative output paths** as WAL data records. Those bytes—not staging files or a mutable output-format configuration—are authoritative during replay. Stage files can be written incrementally as a cache for fast commits but are disposable.
5. After all admitted Produce requests drain, EndTxn appends a durable `COMMIT` decision (or `ABORT`), before publishing or deleting output. An existing decision is immutable. A publish failure can only be retried or recovered; never turned into an abort.
6. The materializer makes a complete private transaction directory from logged bytes, fsyncs its files/directories, publishes with a same-filesystem directory rename, fsyncs parents, then appends a durable publication outcome. A previously published directory is recognized by instance ID. EndTxn records its successful response outcome before sending it.
7. Startup holds the exclusive lock and replays the WAL **before listening**. Incomplete transactions with no decision are discarded. Committed transactions are reconstructed and published idempotently; aborted transactions are cleaned. Only after successful reconciliation may old epochs be fenced and new producers admitted.
8. WAL compaction uses a verified, fsynced checkpoint plus surviving undecided/decided-but-unpublished payloads, swapped atomically. Committed/aborted payloads can be reclaimed after their outcome is durably in the checkpoint. There are no independent coordinator snapshots or output commit-intent markers in the new WAL path.

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

## Migration and limits

The existing committed directory layout from PR #50 remains. The existing PR #51 file-only recovery path remains available for embedders not opting into the unified WAL; the CLI uses the unified path by default and first reconciles any pre-WAL staged directories. The previously proposed `.ksink-kafka-state/state.json` from unmerged PR #52 is intentionally replaced. Old committed directories are never rewritten. The WAL shares the output filesystem and assumes trustworthy local filesystem rename/fsync semantics. Disk exhaustion and unrecoverable corruption fail closed. Compaction and recovery should have dedicated tests.

## Verification checklist

- Unit tests for WAL CRC/truncation, replay, compaction, producer state and sequence dedup.
- Recovery tests for every crash boundary with exact output contents and no duplicate directory.
- Integration tests with real Franz-go transactional clients, concurrent retries and startup fencing.
- `go build ./...`, `go vet ./...`, `go test -race -count=1 ./...`, and lint on the PR head.
