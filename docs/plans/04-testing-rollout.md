# Phase 6: verification, operational guardrails and merge readiness

Status: **test/release plan**, not completed. Prerequisite: [ordered materializer](02-transactions-and-materializer.md) and [rotation](03-filenames-and-rotation.md).

## Protocol and semantics matrix

| Workload | Expected |
|---|---|
| Nontransactional Produce to topic/0 | Durable append, assigned topic offsets, immediately eligible; existing nontransactional backend contracts preserved |
| Produce to partition != 0 | `UNKNOWN_TOPIC_OR_PARTITION` (and never writes) |
| Transactional Produce with active instance | Checksummed durable record and topic offset before ack |
| Duplicate produce / response loss | Same durable output and base offset; no duplicate append |
| Out-of-order producer sequence / stale epoch | Error/fenced; no output or offset advancement |
| Multi-topic single transaction | One coordinator COMMIT/ABORT decision; each topic independently obeys own stable offset |
| B commits before earlier A in same topic | B not exposed until earlier A resolves; then offsets sorted |
| Abort | Aborted records omitted, permitted offset gaps, no loss of later committed records |
| Crash/restart with decided but unpublished transaction | Recover from authoritative logs and publish exactly once |
| Many topics + one stuck transaction | Unrelated topics continue emitting completed files |
| Shutdown while buffered file nonempty | Gracefully finalize, or replay/rebuild on crash; no partial file visible |

## Fault injection boundary matrix

- **Coordinator:** after participant enrollment, after record-set validation, before/after durable COMMIT/ABORT state, response lost, epoch fence, timeout abort and restart.
- **Shared WAL / logical topic streams:** frame write, fsync, segment rotation before/after manifest rename, partial active tail, checksum failure in sealed segment, checkpoint/GC with one topic blocked and another published, corrupt required file, limit overflow.
- **Cross-topic transaction:** records from several topics fsynced in one shared WAL, then commit decision fsync; crash before/after the decision; one topic published and another pending; replay consistently recognizes one authoritative decision.
- **Materializer:** read lease vs WAL GC; temp file create, write, sync, close; rename, parent sync, cursor receipt; crash each boundary; topic writer blocked by storage errors while others proceed.
- **Rotation:** size boundary, age boundary without new Produce, restart while active, shutdown flush, filename overflow, path collision, output rename conflict and output deletion after receipt.
- **Corruption and operator errors:** missing manifest, unknown WAL/storage format, changed output path/formatter, swapped topic directories, checksum mismatch, unsupported filesystem, insufficient disk.

Every failpoint must assert one of two outcomes: the exact expected committed-only file stream after restart, **or a fail-closed explicit error**. Never accept silent data loss, duplication, reordering or inconsistent cross-topic transaction outcomes.

## Performance and long-running tests

- go build ./..., go vet ./..., go test -race -count=1 ./..., golangci-lint.
- Real franz-go producer integration including multiple topics, transactions, idempotence and forced network disconnect/retry.
- Fuzz raw WAL codec, topic/path decoding and manifest parser for oversized lengths, integer rollover and invalid state transitions.
- Benchmarks: Produce ack p50/p95/p99, coordinator commit latency, per-topic segment fsync frequency, allocation/lock contention, publisher throughput, file rotation overhead and backlog growth.
- Long-running stress with 10+ concurrent topic streams, large unresolved transactions, forced crashes, concurrent WAL checkpoint and GC, repeated producer generation reuse and slow/failed disk publication.
- Measured disk budget/backpressure and drain time under blocked topic; no unbounded memory growth.
- Verify read-only inspection and recovery diagnostics, low-volume timeout rotation, and clean shutdown.

## Documentation and compatibility gate

- Update README with the designed one-partition limit, **one physical WAL with independent logical topic streams** as the storage model, committed-only semantics, cross-topic visibility caveat, and simple read-final-files example.
- Replace examples referring to `committed/<txn-id>/` and old `{txnID}` patterns with `{topic}` and `{seq}` templates.
- Document filenames and rotation defaults, atomic rename filesystem requirements, background publication/backpressure, WAL retention, recovery, failure states and output-directory deletion hazards.
- Document compatibility: unmerged PR #52 old WAL version is intentionally incompatible; never silently reinitialize acknowledged previous state. Existing nontransactional backends unchanged.
- Ensure no README/CLI claim suggests support for consumer group, Kafka Fetch API or global order across topics.
- Confirm worktree diff and migration/removal checklist has no stale test/command/documentation references.
- Independent correctness review of shared WAL GC, cross-topic single-decision durability, per-topic publication cursor replay and directory path validation.

## Delivery recommendation

Build changes on the unmerged PR #52 branch in the dependency order from [roadmap](00-ordered-output-roadmap.md). Keep each stage separately reviewable and CI green. A failing or uncertain durability invariant blocks later stages. Merge only after complete protocol, output, crash and performance criteria are met; avoid merging the intermediate directory-based WAL layout.
