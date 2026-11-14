# Ordered committed output: architecture and dependency roadmap

Status: **proposal / implementation planning** for `marre/ksink` PR #52. No implementation implied by this document.

## Architecture chosen for planning

Use **one shared physical segmented WAL** as the sole durable authority for producer identity/epochs, accepted payloads, topic offsets, transaction participants/decisions and publication receipts. Within it, maintain **independent logical topic streams**, one **partition 0** per topic, with **one ordered immutable output-file sequence per topic**. There is **one materializer service**, which tracks independent stable positions and rotation state for each topic. This models a single Kafka read-committed consumer subscribed to topics, but **does not** implement Kafka Fetch/consumer-group protocols.

```text
state/
  manifest.json
  segments/*.wal
  snapshots/*.wal

output/
  orders/messages-00000001.jsonl
  orders/messages-00000002.jsonl
  payments/messages-00000001.jsonl
```

**Reasoning:** Kafka record ordering is per topic-partition; no global cross-topic ordering is promised. A blocked transaction in `orders` should not delay `payments` unless the **same multi-topic transaction** is unresolved for both. File sequences, publication cursors, rotation and data retention are independent per topic.

**Chosen trade-off:** One physical WAL avoids atomic cross-log coordination and permits a single crash-replay order for transaction decisions and payloads. Offsets, read-committed stable boundaries, file sequences and rotation are **logical per-topic state**; WAL disk retention/compaction are shared. A stuck topic may retain physical WAL segments that also contain other topics' data, but must not block their output publication.

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
| **0. Architecture and contracts** | This roadmap | **Chosen:** one shared physical WAL with independent logical topic streams; multi-topic transactions, durable EndTxn acknowledgements and per-topic output visibility |
| **1. WAL correctness / lifecycle** | [01-wal-and-offsets.md](01-wal-and-offsets.md) | Durable record positions and outcomes in the shared WAL, generation retention through delayed publication, producer fencing and fail-closed tests |
| **2. Logical topic streams and append offsets** | [01-wal-and-offsets.md](01-wal-and-offsets.md) | Independent topic offsets and producer dedup state indexed in one WAL; atomic WAL fsync barrier for multi-topic batches |
| **3. Transaction resolution and stable-prefix reader** | [02-transactions-and-materializer.md](02-transactions-and-materializer.md) | One durable transaction decision, no cross-log protocol, per-topic read-committed gating |
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

## Resolved architecture decision

**Chosen: one shared segmented WAL with per-topic logical streams.** A transaction involving many topics has one durable decision; each topic's records have their own logical offset and output cursor. Physical segment cleanup must respect the oldest unpublished data from **all** topics; this is an accepted storage-efficiency trade-off for simpler crash consistency.

**Defaults chosen:** committed-only output, size rotation target 128 MiB, time rotation 1 minute, output pattern `data/{topic}/messages-{seq:08}.jsonl`, no rotation at transaction boundaries, independent per-topic file counters. Existing nontransactional backends remain supported.

See [01](01-wal-and-offsets.md) through [04](04-testing-rollout.md) for detailed acceptance criteria.
