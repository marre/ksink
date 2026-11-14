# Ordered committed output: architecture and dependency roadmap

Status: **proposal / implementation planning** for `marre/ksink` PR #52. No implementation implied by this document.

## Architecture chosen for planning

Use **one physical data WAL per topic** (with exactly **partition 0**), a **small shared transaction-coordinator WAL** for producer identity/epochs and cross-topic COMMIT/ABORT decisions, and **one ordered immutable output-file sequence per topic**. There is **one materializer service**, which tracks independent stable positions for each topic. This models a single Kafka read-committed consumer subscribed to topics, but **does not** implement Kafka Fetch/consumer-group protocols.

```text
state/
  coordinator/manifest.json, segments/*.wal, snapshots/*.wal
  topics/orders/manifest.json, segments/*.wal, snapshots/*.wal
  topics/payments/manifest.json, segments/*.wal, snapshots/*.wal

output/
  orders/messages-00000001.jsonl
  orders/messages-00000002.jsonl
  payments/messages-00000001.jsonl
```

**Reasoning:** Kafka record ordering is per topic-partition; no global cross-topic ordering is promised. A blocked transaction in `orders` should not delay `payments` unless the **same multi-topic transaction** is unresolved for both. File sequences, publication cursors, rotation and data retention are independent per topic.

**Complexity warning:** One WAL per topic **alone** cannot guarantee atomic multi-topic transaction decisions across crashes. The shared coordinator is required unless we intentionally ban multi-topic transactions. The current PR #52 already has one combined coordinator/data WAL; physically splitting it is more complex, not an automatic simplification. The benefit is independent topic state and publication. An alternative would keep **one physical WAL with per-topic logical streams**, providing simpler write/commit durability but less physical isolation. Decide this trade-off before Phase 1 implementation.

## Behavior and limitations

- Every topic advertises and accepts partition 0 only; all other partitions are rejected. Multiple topics and atomic transactions across several topics remain supported.
- Producer acknowledgements mean WAL-durable messages and producer sequence/offset decisions; EndTxn acknowledgement means a WAL-durable final COMMIT/ABORT decision, **not** necessarily completed file output.
- File output contains committed records only, in broker-assigned **per-topic offsets**. Open lower-offset transactions delay later records of **that topic**. Aborts leave permitted gaps. `read_uncommitted` is not supported as a filesystem mode.
- A transaction may occupy multiple output files or multiple topics' file series; do not expose transaction directories. File boundaries are independent of transaction boundaries.
- The one materializer can write/publish separate topic files, but must not invent a cross-topic total order. A single external file reader may subscribe to multiple topic directories but cannot merge them into a Kafka-guaranteed global order.
- Keep existing nontransactional stdout/HTTP/file backends unchanged unless a dedicated compatibility task says otherwise. New transactional output enforces `{topic}` in the path for multiple-topic output; reject `{txnID}` and `{partition}`. **File sequence** is distinct from topic offset and producer batch sequence.
- The unmerged PR #52 WAL v2 and transaction-directory layout are allowed to change without migration, but reopening unsupported old state must fail clearly rather than discard data.

## Implementation order and gates

| Order | Plan | Why first / exit criterion |
|---|---|---|
| **0. Architecture and contracts** | This roadmap | Decide physical per-topic WAL vs logical streams in one WAL, preserve cross-topic transactions, agree EndTxn and output visibility semantics |
| **1. Coordinator correctness / WAL lifecycle** | [01-wal-and-offsets.md](01-wal-and-offsets.md) | Durability contract, transaction generations retained through delayed publication, producer fencing and checksum/fail-closed tests |
| **2. Per-topic WAL and append offsets** | [01-wal-and-offsets.md](01-wal-and-offsets.md) | Exactly one authoritative data log per topic, durable base offsets and idempotent accepted batches, atomic cross-log barriers |
| **3. Transaction resolution and stable-prefix reader** | [02-transactions-and-materializer.md](02-transactions-and-materializer.md) | Commit independent of file materialization, multi-topic decision replay and per-topic read-committed gating |
| **4. Ordered file materializer, remove transaction directories** | [02-transactions-and-materializer.md](02-transactions-and-materializer.md) | Per-topic publication cursor, immutable files and replay without duplication; old staging/commit directory code removed |
| **5. Filename parameters and file rotation** | [03-filenames-and-rotation.md](03-filenames-and-rotation.md) | User-controlled per-topic file patterns, size/time rotation, finalized-file reader contract |
| **6. Verification and merge gate** | [04-testing-rollout.md](04-testing-rollout.md) | Real clients, crash injection, race/load/retention tests, docs and rollout contract verified |

Changes should be delivered as **small reviewable commits or PRs in dependency order**. While PR #52 is unmerged, use its branch for the architectural rewrite; avoid merging the old directory output first. Do not start filename/rotation implementation until WAL retention and stable-prefix publication are demonstrably correct.

## Example: ordering for one topic

```text
orders/0: offset 100 -> txn A, "created"
orders/0: offset 101 -> txn B, "paid"
orders/0: offset 102 -> txn A, "updated"
```

B commits first: no `orders` records at or after 100 are eligible while A is open. A commits: 100, 101 and 102 become eligible in offset order. If A aborts instead, 100 and 102 are skipped and B's 101 becomes eligible when A resolves. `payments/0` has its own offsets, stable position and output-file sequence; its independent committed transactions may be emitted meanwhile.

The output filenames are sorted **within one topic**, not across topics. `{seq}` is the **file number**, not a topic offset.

## Key safety invariants

1. No Produce success unless data and assigned Kafka-compatible positions/dedup result are durable; no EndTxn success unless the irreversible transaction decision is durable.
2. Multi-topic commit is one logical durable outcome; no partial visibility of undecided/aborted transactions after reboot.
3. At most one partition per topic. All records from a topic are read from completed files in ascending topic-offset order, with legitimate gaps for control/aborted records.
4. File finalization and publication-cursor updates survive crashes without double output. A completed filename is immutable and verified before already-needed WAL bytes are reclaimed.
5. Topic WAL payloads stay live through compaction and GC until their output is provably and durably finalized; producer ID reuse must not destroy pending older generations.
6. Slow materialization and unresolved transactions have bounded, observable backlog and an explicit backpressure/fail-closed policy.
7. Existing acknowledged state is never silently reset. Changes in output pattern, codec version or directory layout cannot change replay interpretation.

## Open design decision requiring explicit review

**A. Physical WAL-per-topic + coordinator (this plan's working design):** matches independent per-topic progress but adds cross-log sync/recovery complexity.

**B. One physical segmented WAL with logical per-topic append streams:** simpler durability boundary; still supports per-topic offsets, separate output files and independent stable-prefix cursors, even if log compaction is shared.

The downstream filesystem contract is the same under both. If simplicity dominates, option **B may be preferable**; do not split durable files purely for organizational aesthetics.

See [01](01-wal-and-offsets.md) through [04](04-testing-rollout.md) for detailed acceptance criteria.
