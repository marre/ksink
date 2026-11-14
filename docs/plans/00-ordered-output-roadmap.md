# Ordered committed output: architecture and implementation roadmap

Status: **proposal / planning only**. Applies to `marre/ksink` PR #52, head `b5b62af` at review time. This plan does not itself implement a new output format.

## Target contract and deliberate limits

- A small Kafka-producer-compatible, single-process sink: **partition 0 only for each topic**; multiple topics remain allowed. No Kafka Fetch consumer protocol or consumer group support.
- One **global ingestion sequence** across accepted records from all topics controls filesystem output order. Kafka-facing **per-topic partition-0 offsets** are a different field and must also be durable. Global order is an explicit ksink choice, not a Kafka cross-topic guarantee.
- One committed-only ordered stream of **immutable completed files** for one external file consumer. This models `read_committed` visibility, not an `isolation.level` consumer-protocol option.
- The WAL is the sole durable authority for accepted records, producer dedup/fencing, transaction outcomes, output publication progress and file identity. Successful Produce means its data and dedup/offset decision are durable. Successful EndTxn means its transaction decision is durable; it need not be published to disk yet.
- No per-transaction staging or committed output directories in the target design. Readers ignore temporary files and sort completed files by a stable, zero-padded file sequence.
- One output file series, even if producers use different topics; preserve topic/offset in an inspectable format (prefer JSONL). The ordered transactional mode does **not** allow `{topic}`, `{partition}`, or `{txnID}` routing in output paths. Keep existing nontransactional backends unchanged.
- No migration is promised for the **unmerged** transaction-WAL/output layout in PR #52. New storage versions must explicitly reject old state, never silently start empty or misread old transaction directories.

## Dependency order (do not reverse)

| Phase | Plan | Prerequisite / deliverable | Gate to next phase |
|---|---|---|---|
| **0. Freeze semantics** | This roadmap | Decide global vs per-topic order, ack contracts, output visibility, file naming and failure model | Review decisions before changing durable format |
| **1. WAL / ordering foundation** | [01-wal-and-offsets.md](01-wal-and-offsets.md) | Atomic ingest with persisted global sequence + per-topic offsets, durable decisions independent of transaction instance reuse, replay and safe retention | Lost-response, interleaving, replay, corruption and GC tests green |
| **2. Transaction coordinator and read-committed eligibility** | [02-transactions-and-materializer.md](02-transactions-and-materializer.md) | EndTxn durability decoupled from publication; global stable prefix; accept/reject policy for nontransactional records; pending transaction recovery | Correct B-before-A commit handling; no early exposure or erroneous abort |
| **3. Ordered materializer, cut over from transaction directories** | [02-transactions-and-materializer.md](02-transactions-and-materializer.md) | Sequential WAL-to-file writer, immutable finalization, durable publication cursor and crash reconciliation; remove old transaction-directory implementation | Restart exactly once, no missing/misordered files, WAL GC safe |
| **4. Filename controls and rotation** | [03-filenames-and-rotation.md](03-filenames-and-rotation.md) | Validated name pattern and size/time rotation, stable file numbering, CLI help and reader documentation | Size/time/boundary/restart tests green |
| **5. Full regression / release gate** | All plans | End-to-end Kafka-client compatibility, fault injection, disk pressure, race/load tests, README refresh; review scope and implementation | Build, vet, race, lint, long-running recovery tests pass |

Implement **one coherent, reviewable stage at a time** on the PR #52 branch, re-running relevant tests before moving on. **Do not merge PR #52 in its current transaction-directory form**; the final merge must reflect the new agreed output contract.

## Example: why ordered output differs from commit order

Accepted global positions: `100 A`, `101 B`, `102 A`, `103 C` (later aborted), `104 D`.

B can commit before A. While A is open, no records at or after global position 100 are eligible. After A commits, 100/A, 101/B and 102/A can be materialized in order. After C aborts, position 103 is skipped and 104/D becomes eligible (provided D committed). A file may contain records from different committed transactions; a transaction may cross file boundaries.

The file consumer sees only finalized files, for example:

```text
output/
  messages-00000000000000000001.jsonl
  messages-00000000000000000002.jsonl
```

Here `{seq}` is the **file number**, not the Kafka offset or global record position. Each file contains data ordered by global record position. Aborted records and transaction markers are not emitted as user records. Gaps in source offsets are legitimate.

## Critical correctness invariants

1. **No success response before durability**: accepted messages + producer dedup state + assigned positions are recoverable; transaction decisions are fsynced before EndTxn success.
2. **No early visibility**: an unresolved lower global record position blocks later records even if their transactions have committed (a stricter global rule than Kafka's per-topic LSO).
3. **No WAL deletion before publication**: all data needed to materialize or reconstruct the next file remains live until the final file is verified durable and its publication progress recorded.
4. **No duplicates across crashes**: finalized immutable filenames, bounds, checksums and the WAL publication cursor reconcile in either crash order.
5. **No guessing**: missing acknowledged WAL files, missing finalized output, mismatched checksums, unsupported versions and unclear output bindings stop startup or publication with an actionable error.
6. **Bounded operations**: WAL backlogs, output backlog, disk usage and materializer memory have limits, status and failure policies.

## Explicit non-goals

Multi-partition topology, distributed brokers, Kafka Fetch/consumer groups, `read_uncommitted` file mode, transaction directory browsing, atomic whole-transaction file appearance, cloud-object-store output, and WAL v2-to-next-format automatic migration.

## External reference

Kafka's own `read_committed` consumers read records in partition-offset order and stop at the earliest unresolved transaction in that partition: https://kafka.apache.org/43/configuration/consumer-configs/. The global cross-topic ordering described here is **ksink-specific**.

## Review questions before implementation

- Confirm that one **global file stream** is preferable to one stream per topic. This roadmap chooses global ordering for a single reader.
- Confirm that committed-only output may **lag a successful EndTxn acknowledgement**. This roadmap chooses yes.
- Confirm that a transaction's records may span multiple immutable files. This roadmap chooses yes.
- Decide the supported behavior for nontransactional Produce while `--transactional` is on; plan 02 recommends committing each such batch atomically into the ordered log.
