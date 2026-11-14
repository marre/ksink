# Phase 5: filename controls and ordered output-file rotation

Status: **proposal**. Only start after [transaction/materializer correctness](02-transactions-and-materializer.md) is green.

## Target user experience

```bash
ksink --transactional \
  --output 'data/{topic}/messages-{seq:08}.jsonl' \
  --output-format jsonl \
  --rotate-size 128MiB \
  --rotate-interval 1m
```

Example finalized output:

```text
data/
  orders/messages-00000001.jsonl
  orders/messages-00000002.jsonl
  payments/messages-00000001.jsonl
```

Each topic has **its own independently increasing file number**, while its records retain their durable topic offsets. There is no cross-topic filename ordering guarantee. `{seq}` is the *file number*, **not** the Kafka topic offset, producer sequence or transaction instance.

## Proposed supported options

| Option | Proposed meaning |
|---|---|
| `--output PATH` | Required template for ordered transactional filesystem mode. Includes `{topic}` for topic segregation and `{seq}` for sortable immutable files. |
| `{seq:08}` | Zero-pad file sequence to width 8; validate width (recommend 6–20). Alternatively support `{seq}` with a fixed default width; document exactly one syntax before implementation. |
| `{topic}` | Safe, stable, **collision-free topic path component**. Use reversible encoding or durable mapping, not basename-only sanitization. |
| `--rotate-size SIZE` | Target maximum bytes in an output file, default proposed **128MiB**, 0 disables size-based rotation. Individual record may exceed target; never split a record. |
| `--rotate-interval DURATION` | Maximum wall-clock age of a *nonempty* active file, default proposed **1m**, 0 disables time-based rotation. |
| `--output-format` | Existing JSONL, binary and kcat formatting; format must encode topic/offset if needed and exact bytes remain deterministic across replay. |

**Avoid** date/time placeholders, per-transaction filenames, filename-dependent record offsets, and rotation-per-transaction for v1. Require at least one enabled rotation trigger (size, time or explicit flush on shutdown) so small streams make progress. If both triggers are 0, reject at startup for normal streaming use.

### Rotation semantics

- First trigger wins. Size checks occur at a complete record boundary; one large record can make a file exceed the size target.
- Time checks use wall-clock time for user behavior, but rotation timing is best effort; record an absolute persistent deadline/creation timestamp for a nonempty active file. The worker must rotate even when no further messages arrive. After restart the same timer policy applies.
- **No empty final files.** Once a nonempty file is ready, sync, close and atomic rename it, fsync the parent directory, then checkpoint per-topic publication cursor and next file number. An active file is not visible as finalized and may be rebuilt from WAL after a crash.
- Application records may be from several committed transactions; a transaction can span rotation boundaries. Never rotate solely because a transaction committed.
- Graceful shutdown finalizes a nonempty active file. Force-stop/crash must not expose partial files.
- Completed files are immutable and unique; never replace another file. Use a **no-replace** finalization primitive (not a plain Unix rename that silently overwrites an existing target). If a crash leaves a final name before receipt is checkpointed, validate it against WAL-derived intended contents before recording success. The downstream reader must checkpoint file sequence before deleting/moving a completed file to avoid replayed side effects in the unavoidable rename/receipt crash window.
- Topic file sequences start at 1 (or an explicitly versioned start), persist independently and are never reused across restarts. Define behavior when the selected width no longer fits: **fail safely** rather than wrap or break lexicographic sorting.
- The output root and template are bound to the **shared WAL** identity, while each logical topic retains an independent file sequence and rotation state. Changing template, size, formatter or interval on restart must have documented policy; prefer allowing rotation thresholds to change **only at a clean file boundary**, while rejecting formatter/path changes unless explicit migration is supported.
- Validate path template early: no traversal, collisions, duplicate `{seq}`, platform-reserved names, or symlink escape from the output root. Enforce output and state cannot accidentally overlap.

## Reader contract

1. Enumerate topic subdirectories.
2. For each topic, ignore temporary files; read finalized `{seq}` files in lexical filename order.
3. Do not infer a total order across topics or interpret missing application offsets as corruption (aborts/control markers can create gaps).
4. Process each immutable file at most once by topic + file number; persist the reader's own checkpoint **before deleting or moving a completed file**. ksink cannot provide exactly-once external reader side effects without reader cooperation.
5. Consumers/operators may delete completed files after processing; ksink must **never re-emit a file after its durable publication receipt**, even if its pathname is gone. It cannot distinguish intentional deletion from accidental loss. Document this trade-off plainly. Optional: offer per-file metadata if a reader needs checksum validation, without introducing another non-atomic publication artifact.

## Compatibility

Preserve existing CLI semantics for nontransactional `--output` file/stdout/HTTP. Only ordered transactional file mode has the new template requirements. Reject existing PR #52 `{txnID}` patterns in new mode with an actionable message.

## Acceptance tests

- Single topic: ascending file names, immutable completed contents, independent rotating file numbers after restart.
- Multiple topics: independent counters/timeouts and no cross-topic head-of-line blocking.
- Size rotation at exactly threshold, just below, one record larger than threshold, multi-record batches, binary delimiters, UTF-8 JSONL.
- Time rotation with sparse input and no new records, no empty files, restart just before/after deadline.
- Atomic publish failure/crash before rename, after rename, before/after directory fsync and cursor checkpoint; no duplicates or missing records.
- Reject invalid patterns, collisions/unsafe topics, unsupported `{partition}` / `{txnID}`, overflow, changed binding and unsupported rotation option combinations.
- Full EndTxn and per-topic stable-prefix tests demonstrate rotation does not expose uncommitted records.
- Validate behavior on supported local filesystems; explicitly document that atomic rename and directory fsync assumptions may not hold on some network filesystems.

## Exit gate

A downstream reader needs only a topic directory and its zero-padded sequence files to consume committed ordered records, with documented safety on restart and rotation.
