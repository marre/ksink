# WAL crash-consistency and external history verification

Ksink is a **single-owner WAL, with one Kafka partition (0) per topic and
independent per-topic committed-only output files**. The tests below do not
make it a replicated Kafka broker or prove exactly-once downstream effects.
A successful Produce or EndTxn acknowledgment means the **shared local WAL**
has passed its fsync boundary, not that a corresponding output file or S3
object is already visible.

## WAL v5 frame format (manifest v4)

Each frame is self-identifying, not an unversioned length/CRC tuple:

| Byte offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | Magic `KSWL` |
| 4 | 2 | Envelope version 1, big endian |
| 6 | 2 | Fixed header length 28 |
| 8 | 8 | Monotonically increasing physical frame sequence |
| 16 | 4 | Payload length, max 64 MiB |
| 20 | 4 | CRC32C of the payload |
| 24 | 4 | CRC32C of header bytes 0–23 |

The payload is versioned WAL record metadata or binary output data.
Checkpoint generations use new, greater sequence numbers; WAL replay
requires strictly increasing sequences across referenced snapshots and
segments. Indexed reads validate both checksums. A partially written
unaccepted active tail can be discarded, but corruption in complete
frames or referenced sealed segments fails closed. Header magic, version
and header-integrity violations are not silently treated as valid data.
The manifest version and on-disk WAL version were deliberately bumped;
**no on-disk migration or backward compatibility is provided**.

## Supported linearization points

* **Produce:** acceptance and producer dedup state are one replay-atomic
  WAL batch. Success requires an fsync. A lost response after fsync is
  indeterminate; idempotent retry must not allocate another Kafka offset.
* **EndTxn:** one durable terminal record fixes COMMIT/ABORT and control
  markers for all enrolled topics, with a single fsync boundary.
* **Publication:** ordered immutable final files contain a committed-only
  prefix per topic. Publication receipts and independent file sequences
  are journaled back to the WAL. A crash after publication but before a
  receipt is resolved against the existing final name/bytes.
* **S3 publication:** conditional `If-None-Match: *` plus server-validated
  SHA-256 checksums (full-object or composite multipart) and checksum-mode
  `HeadObject` reconcile existing objects **without downloading them**.
* **Recovery:** after ambiguous write/fsync, the owner must stop
  acknowledging mutations. Startup either reconstructs a valid history
  or rejects corrupted/unsupported storage.

## Safety invariants

1. Successfully acknowledged committed records are not lost after
   restart and final convergence.
2. Abort/undecided payloads never enter committed output.
3. Idempotent retries preserve the original topic offset, without
   duplicates, even after a lost response.
4. Topic offsets and file sequences do not regress or collide. Kafka
   control markers occupy offsets but never appear in final files.
5. A transaction instance has one irreversible outcome, including
   across multi-topic transactions and producer epoch fencing.
6. Each topic's files preserve committed WAL offset order.
7. No final S3 key or local file name is successfully overwritten with
   different bytes after a crash or a competing writer.
8. An EIO, ENOSPC, EDQUOT, short write, fsync or manifest failure poisons
   the current owner; it cannot acknowledge further data.
9. At most one cooperating process holds the same WAL directory lock.
10. Referenced sealed frames and snapshots must never be silently
    discarded on corruption, version mismatch or missing files.

## Deterministic CI coverage

```sh
go test -race -count=1 ./...
go test -count=1 ./pkg/ksink -run 'TestWALJepsen|TestSharedWAL|TestTransactionWAL'
go test -count=1 ./internal/integration -run 'TestOrderedS3MinIOPublicationAndRecovery'
```

The WAL fault matrix injects Linux EIO/ENOSPC/EDQUOT, short successful
writes, errors before and after append fsync, checkpoint creation/write/
fsync/directory sync, segment create/fsync, manifest write/short-write/
rename/parent directory fsync, segment rotation, and garbage collection.
It verifies fail-stop, active-tail repair, and acknowledged-prefix safety
after independent restart. Dedicated tests alter each frame header
integrity field and replay the resulting on-disk corruption. Other
tests exercise byte cuts, post-fsync lost responses, checkpoints,
multiple topics, producer dedup, and owner SIGKILL/lock release.

The pinned MinIO integration exercises actual concurrent conditional
`PutObject` publishers (independent WALs with the same final S3 key)
and racing SHA-256 multipart completions, enforcing a single winner
and rejecting all conflicting publication receipts. Unit tests also
cover lost upload completion responses, checksum absence/mismatch,
corrupted bodies and changing multipart boundary settings.

The injected syscall error is returned **at the real WAL code boundary**,
but is not itself a real kernel EIO/ENOSPC, and does not simulate drive
write caches or unflushed filesystem journal sectors.

## External black-box crash campaign

`TestWALJepsenExternalCampaign` builds and starts the **actual ksink CLI**
as a child process, uses Kafka transactional clients to Produce/Commit/
Abort across two topics, and records invocation/response histories in
fsynced JSONL **outside** the target WAL directory. It kills the server
with SIGKILL (including EndTxn-in-flight), restarts the owner, checks
per-topic Kafka offsets, then runs a no-fault final convergence/flush.
The independent checker rejects ghost, duplicated, out-of-order and
confirmed-aborted payloads and requires every **confirmed committed**
payload to appear. An EndTxn with no successful response is classified
as *indeterminate*; its output is permitted only if it was actually
produced, never conflated with a confirmed abort.

Local reproduction:

```sh
KSINK_JEPSEN_SEED=42 \
KSINK_JEPSEN_CYCLES=10 \
KSINK_JEPSEN_TXNS_PER_CYCLE=10 \
KSINK_JEPSEN_ARTIFACT_DIR=./test-artifacts \
go test -count=1 -timeout=30m -run '^TestWALJepsenExternalCampaign$' ./internal/integration
```

The manual GitHub Actions workflow
[`WAL external crash qualification`](../.github/workflows/wal-jepsen.yml)
offers a small smoke profile or a sharded **qualification** profile:
25 shards × 400 independent kill/restart cycles × 10 transaction
attempts = **10,000 restart cycles and 100,000 transaction attempts**.
It writes per-shard JSONL histories to CI artifacts and limits
concurrent shards to control CI cost. **These counts are configured
targets, not evidence that a qualification workflow has run.**
Do not claim completion until all 25 shards have actually passed and
their histories/reports have been inspected.

## Separate destructive qualifications still needed

* **Real power loss:** filesystem/block-device flush and rename
  durability require controlled hardware, loop-device/dm-flakey,
  or VM disk-fault infrastructure. SIGKILL cannot simulate loss of
  data that the kernel acknowledged as cached.
* **Physical storage exhaustion:** use a restricted filesystem/volume
  to induce actual ENOSPC/EDQUOT, and I/O fault injection to cause
  kernel-level EIO and directory fsync errors.
* **Network and object-store faults:** run against the intended AWS S3
  or S3-compatible production version with injected slow/failed HEAD,
  dropped PUT/CompleteMultipartUpload responses, 409/412 races and
  orphaned MPU cleanup. The pinned MinIO races verify conditional
  semantics but do not represent every object store.
* **Producer-history breadth:** the external CLI campaign validates
  committed-only projection and ambiguous EndTxn with multiple topics.
  Comprehensive Jepsen certification would additionally need
  concurrent overlapping producers, five in-flight batches, response
  partitioning, producer ID fencing under sustained partitions, and
  independent read/write-linearizability analysis.
* **Unsupported contracts:** shared/network WAL filesystems, concurrent
  noncooperating owners, simultaneous cross-topic external visibility,
  Kafka Fetch/consumer groups, sendOffsetsToTransaction, replication
  and exactly-once consumer side effects remain outside this design.

A passing test suite, or even a full configured SIGKILL campaign,
**does not alone establish physical durability guarantees on every
supported filesystem or S3-compatible backend**.
