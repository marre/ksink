# Phase 6: verification, operational guardrails and merge readiness

Status: **active release checklist**. WAL crash tests, race tests, and ordered output tests exist; fatal I/O failure recovery and extended soak remain open. Prerequisite: [ordered materializer](02-transactions-and-materializer.md) and [rotation](03-filenames-and-rotation.md).

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
| Abort | Aborted records omitted, permitted offset gaps for aborted data and transaction control markers, no loss of later committed records |
| Crash/restart with decided but unpublished transaction | Recover from authoritative logs and publish exactly once |
| Many topics + one stuck transaction | Unrelated topics continue emitting completed files |
| Shutdown while buffered file nonempty | Gracefully finalize, or replay/rebuild on crash; no partial file visible |

## Fault injection boundary matrix

- **Coordinator:** after participant enrollment, after record-set validation, before/after durable COMMIT/ABORT state, response lost, epoch fence, timeout abort and restart.
- **Shared WAL / logical topic streams:** frame write, fsync, segment rotation before/after manifest rename, partial active tail, checksum failure in sealed segment, checkpoint/GC with one topic blocked and another published, corrupt required file, limit overflow.
- **Cross-topic transaction:** records from several topics fsynced in one shared WAL, then commit decision fsync; crash before/after the decision; one topic published and another pending; replay consistently recognizes one authoritative decision.
- **Materializer:** read lease vs WAL GC; temp file creation/write/sync, finalization, parent sync and receipt crashes. Output write errors must cause nonzero process termination; restart recovers durable records.
- **Rotation:** size boundary, age boundary without new Produce, restart while active, shutdown flush, filename overflow, path collision, output rename conflict and output deletion after receipt. Verify deleted finalized files are never recreated/reused and the next sequence remains monotonic; no assumption of automatic loss recovery after consumer deletion.
- **Corruption and operator errors:** missing manifest, unknown WAL/storage format, changed output path/formatter, swapped topic directories, checksum mismatch, unsupported filesystem, insufficient disk.

Every failpoint must assert one of two outcomes: the exact expected committed-only file stream after restart, **or a fail-closed explicit error**. Never accept silent data loss, duplication, reordering or inconsistent cross-topic transaction outcomes.

## Performance and long-running tests

- go build ./..., go vet ./..., go test -race -count=1 ./..., golangci-lint.
- Real franz-go producer integration including multiple topics, transactions, idempotence and forced network disconnect/retry.
- Fuzz raw WAL codec, topic/path decoding and manifest parser for oversized lengths, integer rollover and invalid state transitions.
- Benchmarks: Produce ack p50/p95/p99, coordinator commit latency, per-topic segment fsync frequency, allocation/lock contention, publisher throughput, file rotation overhead and backlog growth.
- Long-running stress with 10+ concurrent topic streams, large unresolved transactions, forced crashes, concurrent WAL checkpoint and GC, repeated producer generation reuse and slow/failed disk publication.
- Test disk-full WAL/output errors cause nonzero exit and preserve acknowledged data across restart. No admission-control/backpressure or metrics feature is required.
- Verify read-only inspection and recovery diagnostics, low-volume timeout rotation, and clean shutdown.

## Documentation and compatibility gate

- Update README with the designed one-partition limit, **one physical WAL with independent logical topic streams** as the storage model, committed-only semantics, cross-topic visibility caveat, and simple read-final-files example.
- Replace examples referring to `committed/<txn-id>/` and old `{txnID}` patterns with `{topic}` and `{seq}` templates.
- Document filenames and rotation defaults, atomic rename filesystem requirements, fatal output/WAL write failures, WAL retention, recovery, failure states and output-directory deletion hazards.
- Document compatibility: unmerged PR #52 old WAL version is intentionally incompatible; never silently reinitialize acknowledged previous state. Existing nontransactional backends unchanged.
- Ensure no README/CLI claim suggests support for consumer group, Kafka Fetch API or global order across topics.
- Confirm worktree diff and migration/removal checklist has no stale test/command/documentation references.
- Independent correctness review of shared WAL GC, cross-topic single-decision durability, per-topic publication cursor replay and directory path validation.

## Delivery recommendation

Build changes on the unmerged PR #52 branch in the dependency order from [roadmap](00-ordered-output-roadmap.md). Keep each stage separately reviewable and CI green. A failing or uncertain durability invariant blocks later stages. Merge only after complete protocol, output, crash and performance criteria are met; avoid merging the intermediate directory-based WAL layout.

## Additional mandatory failure tests discovered during implementation

- A rejected `RecordTopicFile` or `SkipTopicOffsets` attempt **must not poison the WAL**; preflight all semantic checks before a checksummed append, and independently revalidate at replay time.
- Crash after multi-topic terminal decision fsync but before coordinator snapshot: recovering must preserve COMMIT and every enrolled topic marker, **never** incorrectly convert it to ABORT.
- Control-marker offsets remain gaps in application output after checkpoint; later records must receive the correct next Kafka offset.
- Crash after file finalization and before publication receipt, with the final file present: reconcile digest then commit receipt. Document the intrinsically ambiguous case where a reader has already consumed and removed that file without a read-side checkpoint/handshake.
- WAL v2 manifest opened with WAL v3 code must fail without modifying acknowledged old state.

## Output supervision verification (November 2026)

The ordered materializer now maintains per-topic publication error state. Its
background supervisor retries a failed topic with capped exponential backoff,
logs failures and recovery, and continues to poll healthy topics. The
`TopicFailures()` snapshot is available to embedding health checks. The
command waits for the worker to exit before forced shutdown flush; a flush
failure is reported as an error, not a successful drain.

Regression coverage includes a pre-existing conflicting final file on one
topic while another publishes, subsequent recovery of the blocked topic, an
oversized single record, and file-number continuity across restart.

**Remaining merge gate:** change the current retrying output worker to terminate on write failure, verify WAL and output error propagation and restart recovery, and finish correctness/fault-injection tests. Disk-space reservation, backlog admission control, metrics and persisted rotation deadlines are explicitly excluded.


## Revised operating policy (November 2026)

- **Fatal I/O:** any WAL or output write/sync/finalization failure must cause nonzero process termination; never acknowledge undurable data. Restart replays durable WAL and reconciles immutable output receipts. This policy is a requirement, not proof that all current code paths already enforce it.
- **No metrics or backpressure feature:** no metrics endpoint, per-topic health metrics, configured disk budget or bounded-backlog admission control is required. Disk exhaustion is a fatal I/O failure.
- **Rotation restart:** a process crash or restart may reset a pending in-memory time-rotation interval. Durable topic offsets, output cursors and file sequence numbers must not reset.
