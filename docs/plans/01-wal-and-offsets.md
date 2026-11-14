# Phases 1–2: close WAL gaps and establish per-topic offsets

Status: **plan, not implementation**. Working architecture: one durable data WAL for **partition 0 of each topic**, plus a shared durable transaction coordinator WAL. Keep the one-physical-WAL alternative under review (see [roadmap](00-ordered-output-roadmap.md)).

## Current code facts and risks in PR #52

Strengths worth retaining: checksum-framed binary output payloads, segmented WAL and manifest, fsynced batched append, producer-epoch fencing, deterministic retry signatures, checkpointing and fault injection.

1. **No authoritative topic offset assignment.** `Message.Offset` originates in `parseRecords` from a synthetic fetch decode. `ProduceResponseTopicPartition` does not contain a meaningful assigned base offset. Need per-topic partition-0 offsets durable before Produce acknowledgement.
2. **Producer state and payload are written separately.** `handleProduce` first delivers to filesystem writer (WAL), then calls `persistTxnStateLocked()` to journal dedup sequences. The new append protocol must define crash/response-loss behavior atomically; a duplicate batch must not allocate offsets or append again.
3. **Publisher-dependent GC.** A `published` record makes all transaction output frames obsolete. Coordinator checkpoint replay can discard older instance references on transactional.id reuse. Both are wrong when output is delayed behind earlier unresolved records: retain all unmaterialized committed generations.
4. **Latest-instance-only recovery.** `RecoveryTransactions()` iterates current producer states, not arbitrary outstanding older generations with committed-but-unpublished data.
5. **Output paths are coupled to WAL payloads.** `WALOutputRecord` has `Path` and formatted `Data`; per-topic file names and rotation must be materializer decisions, while the WAL records logical topic + offset + deterministic payload.
6. **WAL read lock across file writes.** `StreamOutput` keeps a global WAL mutex through streaming callbacks; split readers or use safe segment leases/snapshots to avoid blocking Kafka Produce during slow output I/O.
7. **Unresolved backlog pressure.** Large open transactions can pin sealed segments. Current 16 MiB/25% checkpoint triggers don't express per-topic lag, retained bytes or throttling.
8. **Cross-log authority.** Separating topic WALs requires a single durable transaction decision and a recoverable, explicit participant list. Avoid optimistic per-topic commit markers that become partially visible after crashes.

## Proposed durable structure

```text
state/
  coordinator/manifest.json + segments, snapshots    # identities, epochs, participants, final txn decisions
  topics/<sanitized-unique-topic-id>/
    manifest.json + segments, snapshots               # topic records, offsets, producer batch dedup, cursor references
```

Topic storage must avoid collisions of sanitized names (use a reversible encoding or stable escaped ID and a durable original-name mapping). Each topic WAL has monotonically increasing **broker-assigned offset**, stored along with topic, transaction instance (or committed nontransactional batch), deterministic serialized bytes, producer sequence details and CRC. There is **no global record order**.

Coordinator stores producer ID/epoch fencing, transaction instance and participant `{topic, durable end offset / batch IDs}`, and irreversible COMMIT/ABORT. Durable producer batch state can be topic-local, but the coordinator must validate ownership/fencing and retained participant generations.

## Required cross-log durability protocol

1. **Transaction start/enrollment:** persist an immutable instance and registered topic(s) with coordinator authority before a topic can acknowledge its records as eligible for that transaction.
2. **Produce:** validate producer/epoch and topic-local sequence; append records and their assigned offsets and dedup checkpoint **in one topic-WAL fsync barrier**. Return correct Kafka Produce base offset and retain it for identical retries.
3. **Prepare/finalize commit:** stop new Produce for the instance, drain admitted requests, prove all touched topic WALs are fsynced and persist the complete participant/final-offset set. **Only then** fsync the **one global COMMIT decision** and acknowledge EndTxn. The Abort path uses an equally durable decision.
4. **Materializer:** each topic independently scans its stable readable prefix, consulting coordinator decisions; it may publish only records with an authoritative COMMIT or implicit committed nontransactional status. An unresolved earlier offset blocks later offsets **only within that topic**. Aborted records are skipped.
5. **Crash recovery:** load coordinator + all selected topic manifests. Fail closed on any missing acknowledged participant segment. Reconcile in-flight/enrollment, decided transactions and output cursors. Never infer that a missing topic log means an empty transaction.

A process crash between topic WAL append and coordinator decision leaves no visible committed record; replay must not reuse that topic offset incorrectly. Repeated produce requests must have specified lost-response behavior. Avoid needing a second fsync for every Produce unless correctness requires it; if necessary, first define and benchmark the cost.

## Implementation tasks (dependency order)

1. **Schema and ownership:** versioned `CoordinatorState` / `TopicLog` records; validate topic names, unique paths, offsets, producer IDs and participant references. Explicitly reject the old PR #52 WAL v2/transaction directory format; no silent migration.
2. **Topic append and offsets:** one topic WAL per active topic; durable offset assignment, accepted-record batch IDs, signatures and replay, with correct base-offset responses and no duplicate offset consumption. Confirm behavior for nontransactional records and Kafka control-marker offsets.
3. **Participant fencing and decision barrier:** transaction coordination serialized with producer writes and topic enrollment, with COMMIT only after every participant log is durable. A decision must not depend on file output.
4. **Replay and retention:** rebuild all outstanding generations and topic-offset ordered indexes; retain data through producer reuse; reject missing acknowledged data; safe segment GC respecting publisher progress.
5. **Reader API / lock design:** bounded offset-ordered WAL iterators with safe segment lifetimes; keep slow materializer disk writes outside producer/WAL mutexes; measure fairness and blocked startup cases.
6. **Backpressure and observability:** per-topic oldest unstable offset, end offset, emitted offset, blocked transaction, retained bytes and GC backlog, health/errors. Define disk-full policy and bounds.
7. **Checkpoint optimization:** avoid copying unrelated large producer snapshots on every message if benchmarks show cost. Validate CRC, maximum-frame rules, snapshots and generation handling.

## Acceptance tests

- Multiple topics with independent offsets and independent stable boundaries.
- A/B/A interleaving on one topic; B commits before A; outputs remain offset ordered after A resolves.
- One transaction spans `orders` and `payments`; neither topic emits until the **single** coordinator COMMIT is durable; crash at each topic-fsync, coordinator-fsync and decision-response boundary.
- Cross-topic transaction with one missing/corrupt participant WAL fails closed. No false commit and no data loss.
- Exact retry after lost Produce response returns the same topic base offset, no extra records. Out-of-order producer sequence rejected.
- Producer ID reused for new generation while older committed generation remains blocked; all older bytes still recover after checkpoint and GC.
- Crash after manifest creation/rotation/checkpoint/fsync, partial active tail and corrupt sealed segment; deletion guard holds for every topic WAL.
- Large backlog, many topics and concurrent materialization with bounded memory, fair locks and measured fsync rates.
- No code path holds a topic-WAL lock across callback I/O to final output files.

## Gate

Move on only when the topic logs and coordinator can replay a fully accepted multi-topic transaction and producer dedup/offset history **without touching transaction output directories**. CI build, vet, race and lint must pass; fault-injection tests must cover both coordinator and topic journals.
