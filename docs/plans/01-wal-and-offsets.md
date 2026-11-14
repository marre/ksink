# Phases 1–2: close WAL gaps and establish per-topic offsets

Status: **plan with partial implementation underway**, not a completed storage redesign. Chosen architecture: **one shared physical WAL** containing independent logical streams for **partition 0 of each topic**, plus coordinator and transaction-decision frames in that **same WAL**. See [roadmap](00-ordered-output-roadmap.md).

## Current code facts and risks in PR #52

Strengths worth retaining: checksum-framed binary output payloads, segmented WAL and manifest, fsynced batched append, producer-epoch fencing, deterministic retry signatures, checkpointing and fault injection.

1. **No authoritative topic offset assignment.** `Message.Offset` originates in `parseRecords` from a synthetic fetch decode. `ProduceResponseTopicPartition` does not contain a meaningful assigned base offset. Need per-topic partition-0 offsets durable before Produce acknowledgement.
2. **Producer state and payload are written separately.** `handleProduce` first delivers to filesystem writer (WAL), then calls `persistTxnStateLocked()` to journal dedup sequences. The new append protocol must define crash/response-loss behavior atomically; a duplicate batch must not allocate offsets or append again.
   **Separate failure mode:** one fsynced batch of several physical WAL frames is not necessarily a replay-atomic unit. If a process dies after some complete frames but before the last frame / its producer dedup checkpoint, replay must ignore the uncommitted partial batch rather than treating it as accepted data. Specify explicit batch boundaries or a single atomic-acceptance record; test crash at every frame boundary and response-loss retry.
3. **Publisher-dependent GC.** A `published` record makes all transaction output frames obsolete. Coordinator checkpoint replay can discard older instance references on transactional.id reuse. Both are wrong when output is delayed behind earlier unresolved records: retain all unmaterialized committed generations.
4. **Latest-instance-only recovery.** `RecoveryTransactions()` iterates current producer states, not arbitrary outstanding older generations with committed-but-unpublished data.
5. **Output paths are coupled to WAL payloads.** `WALOutputRecord` has `Path` and formatted `Data`; per-topic file names and rotation must be materializer decisions, while the WAL records logical topic + offset + deterministic payload.
6. **WAL read lock across file writes.** `StreamOutput` keeps a global WAL mutex through streaming callbacks; split readers or use safe segment leases/snapshots to avoid blocking Kafka Produce during slow output I/O.
7. **Unresolved backlog pressure.** Large open transactions can pin sealed segments. Current 16 MiB/25% checkpoint triggers don't express per-topic lag, retained bytes or throttling.
8. **Cross-topic authority.** One shared WAL must contain a single durable final transaction decision and the participant topic set; independent per-topic output cursors must not be mistaken for independent COMMIT decisions.

## Proposed durable structure

```text
state/
  manifest.json
  segments/*.wal
  snapshots/*.wal
  # one durable replay order for records, producer state, transaction outcomes
  # and per-topic publication receipts
```

The shared WAL indexes logical streams by **exact topic name**, avoiding collisions from filesystem-safe topic names; the output materializer separately validates collision-free topic directory mappings. Each topic stream has monotonically increasing **broker-assigned offsets**, stored with topic, transaction instance (or committed nontransactional batch), deterministic serialized bytes, producer sequence details and CRC. No cross-topic record order is promised.

In the **same WAL**, coordinator frames store producer ID/epoch fencing, transaction instance and participant `{topic, durable end offset / batch IDs}`, and irreversible COMMIT/ABORT. Logical topic-stream indexes store offsets, producer batch dedup and publication progress. All frames share one physical checksum, manifest and replay domain.

## Kafka offset and idempotency contract

- Assign each topic/partition-0 a monotonic **Kafka log offset**, independently of the producer batch's sequence number and the output file's sequence. Kafka transaction COMMIT/ABORT control markers **also consume topic-log offsets**, although they are never emitted as application records. Preserve those gaps so Produce BaseOffset behaves like Kafka. Replay must distinguish control-marker positions from user payloads.
- A multi-topic EndTxn decision and its per-participant control marker positions are represented by a **single checksummed terminal WAL frame**. This prevents later appends overtaking missing markers, including in crash recovery. The participant set is persisted during `AddPartitionsToTxn`, including subsequent enrollments; repeated registration must be idempotent.
- Extend each producer-batch dedup signature with its **original assigned base offset**. A repeated exact batch must return the same base offset and neither append new payload nor advance the topic offset. The existing five-batch retry window and Kafka sequence wrap rules still apply.
- The output format may include `Message.Offset`. Therefore the server must select Kafka offsets **before formatting**, and persist formatted bytes + producer dedup snapshot under the same fsync/acceptance boundary. Formatter errors must not consume offsets or alter producer state.
- The old `Read()/AckFunc` event path currently delegates formatted payload persistence to the CLI and then writes producer state separately. The future ordered CLI path must deliberately couple formatting and WAL acceptance in one ownership boundary; do not just call the new low-level topic API from the existing writer and claim idempotency.

## Required single-WAL durability protocol

1. **Transaction start/enrollment:** durably persist immutable instance and participant topic(s) in the shared WAL before accepted records can be attributed to that transaction.
2. **Produce:** validate producer/epoch and topic-local sequence, assign topic offsets and persist output payload plus dedup/offset state within **one shared-WAL fsync barrier**. Return correct Kafka Produce base offset and retain it for identical retries.
3. **Commit:** fence new Produce, drain accepted writes, then fsync one authoritative COMMIT/ABORT decision and immutable participant set into the shared WAL. EndTxn can succeed independently of materialization.
4. **Materializer:** each logical topic independently scans its stable readable prefix, consulting shared-WAL decisions. Emit committed or nontransactional records, skip aborted records. Earlier unresolved offsets block **only their own topic**.
5. **Crash recovery:** replay the single selected WAL manifest; reconstruct every logical topic stream, producer dedup state, all outstanding transaction generations and each topic's output cursor. Never assume the last producer epoch's transaction is the only outstanding unpublished generation.

A process crash between record append and the shared WAL commit decision leaves no visible committed record; replay must not reuse that topic offset incorrectly. Repeated produce requests must have specified lost-response behavior. Avoid needing a second fsync for every Produce unless correctness requires it; if necessary, first define and benchmark the cost.

## Implementation tasks (dependency order)

1. **Schema and ownership:** versioned shared-WAL records and in-memory `TopicStream` / `CoordinatorState` indexes; validate topic names, offset counters, producer IDs and participant references. Explicitly reject the old PR #52 WAL v2/transaction directory format; no silent migration.
2. **Logical topic append and offsets:** one physical WAL, per-topic offset counters and indexed records; persist accepted batches, signatures and offset reservations atomically, with explicit replay-visible acceptance markers carrying the producer checkpoint, correct base-offset responses and no duplicate offset consumption. **Format output only after the WAL has selected the authoritative topic offset**; otherwise JSONL / kcat serializers may embed incorrect offsets. Provide an offset-aware formatting callback under the bounded WAL acceptance critical section. Confirm behavior for nontransactional records and Kafka control-marker offsets.
3. **Participant fencing and decision barrier:** transaction coordination serialized with producer writes and topic enrollment; COMMIT follows one shared WAL's existing durable record fsyncs. A decision must not depend on file output.
4. **Replay and retention:** rebuild all outstanding generations and topic-offset ordered indexes; retain data through producer reuse; reject missing acknowledged data; safe shared-segment GC respecting **every topic** publisher cursor.
5. **Reader API / lock design:** bounded offset-ordered WAL iterators with safe segment lifetimes; keep slow materializer disk writes outside producer/WAL mutexes; measure fairness and blocked startup cases. A deterministic in-memory formatter callback during batch acceptance is allowed but may not do network/file I/O or re-enter the WAL.
6. **Backpressure and observability:** per-topic oldest unstable offset, end offset, emitted offset, blocked transaction, retained bytes and GC backlog, plus shared WAL free disk budget and health/errors. Define disk-full policy and bounds.
7. **Checkpoint optimization:** avoid copying unrelated large producer snapshots on every message if benchmarks show cost. Validate CRC, maximum-frame rules, snapshots and generation handling.

## Acceptance tests

- Multiple topics with independent offsets and independent stable boundaries.
- A/B/A interleaving on one topic; B commits before A; outputs remain offset ordered after A resolves.
- One transaction spans `orders` and `payments`; neither topic emits until the **single** coordinator COMMIT is durable; crash at each topic-fsync, coordinator-fsync and decision-response boundary.
- Cross-topic transaction with one missing/corrupt required **shared WAL segment** fails closed. No false commit and no data loss.
- Exact retry after lost Produce response returns the same topic base offset, no extra records. Out-of-order producer sequence rejected. Inject crashes after every *complete* output frame of a multi-record append and before/after its acceptance barrier; no partial replayed batch may be published. Test format failure and offset-dependent JSONL/kcat output as part of the atomic acceptance boundary.
- Producer ID reused for new generation while older committed generation remains blocked; all older bytes still recover after checkpoint and GC.
- Crash after manifest creation/rotation/checkpoint/fsync, partial active tail and corrupt sealed segment; deletion guard holds for the shared WAL.
- Large backlog, many topics and concurrent materialization with bounded memory, fair locks and measured fsync rates.
- No code path holds the shared WAL's write mutex across callback I/O to final output files.

## Gate

Move on only when the shared WAL can replay a fully accepted multi-topic transaction and independent logical topic offset/dedup histories **without touching transaction output directories**. CI build, vet, race and lint must pass; fault-injection tests must cover coordinator decisions, each logical topic stream and their shared replay order.

## Revalidated implementation status

The new shared-WAL record version is **3**, with manifest version **2**; older unmerged WAL state is explicitly rejected. `AddPartitionsToTxn` persists sorted topic participants, and EndTxn allocates their Kafka control-marker offsets with the durable outcome in one WAL record. A read-committed per-topic cursor must skip marker offsets and aborted records, yet preserve their offsets as gaps. Snapshot compaction reconstructs payloads and markers in each topic's offset order. The new publication receipt API is independent per topic but **does not yet reclaim historical payloads**; keep it that way until the immutable file materializer, safe compaction and replay tests establish that all data needed by a blocked topic remains live.

**Outstanding correctness review:** idempotent nontransactional producers still need deduplication equivalent to the transactional producer path, and WAL compaction must eventually encode durable next-offset counters even when older published payload frames are discarded. Neither is a prerequisite for testing an isolated writer, but both are required before the final merge.
