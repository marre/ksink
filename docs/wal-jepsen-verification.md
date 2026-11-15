# WAL fault testing and consistency model

This document defines what a Jepsen-style checker should test for ksink's
**single-owner, single-partition-per-topic** WAL. It is not a claim that the
system is a replicated database or that a full Jepsen run has passed.

## Supported semantics and linearization points

* **Produce:** successful ordered Produce has durably appended its output
  records and producer-sequence snapshot in one WAL acceptance group. The
  successful `fsync` is the durability boundary. A lost response is
  indeterminate; a correct idempotent retry must not allocate another offset.
* **EndTxn:** one WAL terminal frame establishes the irreversible commit/abort
  outcome and allocates a marker in every enrolled topic. The successful
  `fsync` is the terminal-decision durability boundary.
* **Publication:** output consists of an immutable, per-topic prefix of
  committed application records in ascending WAL offset order. Publication
  receipts and file sequences survive restart. For S3, an existing object
  must be attested against actual body bytes before a receipt is written.
* **Recovery:** a torn active append that has no accepted delimiter may be
  discarded; established sealed records must fail closed on corruption.
  Any ambiguous I/O or `fsync` error must stop future acknowledgements.

The model does **not** promise simultaneously visible files/objects across
different topics, Kafka Fetch/consumer groups, `sendOffsetsToTransaction`,
exactly-once downstream side effects, consensus/replication, or tolerance of
two independent writers or broken storage `fsync` contracts.

## History schema

Capture a durable external history containing:

`client-id, invocation-id, operation, txn-id, producer-id, epoch,
topic, sequence-range, payload-id, invoke-time, complete-time,
response, returned-offset`.

Operations: `init-producer`, `add-topic`, `produce`, `commit`,
`abort`, `kill`, `restart`, `read-output`, `inspect-wal`.

Each payload needs a unique run/client/transaction/operation ID. Treat a
timeout, disconnection, or killed client as **indeterminate**, not as failure
or success. The checker must reconcile indeterminate operations against
subsequent retries and recovered WAL/output state.

## Required safety invariants

1. No successful, committed Produce is missing after recovery and convergence.
2. Aborted and undecided transaction data never appears in committed output.
3. A retained idempotent batch retry returns its original offset without a
   second logical record, including lost acknowledgements.
4. Per-topic offsets never collide or regress; control markers occupy offsets
   but do not appear as application data.
5. Each transaction instance has one irreversible terminal outcome across its
   topic participants, even if its EndTxn response is lost.
6. Output equals the committed-only projection of a stable per-topic WAL
   prefix, in exactly the same offset order.
7. Immutable output names never refer to different bytes, including after
   crash-between-publication-and-receipt.
8. Old producer epochs are fenced after a newer epoch or a server restart.
9. After disk I/O or `fsync` ambiguity, the old owner cannot continue to ack
   mutations; restart either recovers an internally consistent history or
   fails closed on verified corruption.
10. At most one cooperating process owns any WAL at a time.

## Deterministic CI tests

Run:

```sh
go test -race -count=1 ./...
go test -count=1 ./pkg/ksink -run 'TestKafkaCRC|TestWALJepsen'
go test -count=1 ./internal/output -run 'TestS3Ordered'
```

The `TestWALJepsen*` tests exercise process ownership, active-tail byte
cuts, and after-fsync/before-response ambiguity for both Produce and
multi-topic terminal decisions. Kafka CRC tests verify no producer-sequence
or offset consumption on malformed input. S3 recovery tests verify
server-validated SHA-256 HEAD reconciliation (without object downloads), checksum-mismatch and missing-checksum fail-closed errors.

Existing WAL suites additionally cover segment rotation, compaction,
unaccepted acceptance groups, and sealed checksum corruption. All these
tests belong in the normal PR CI, but a passing unit suite is **not** a
substitute for crash-consistency validation under real process and
filesystem failure.

## External fault campaign before declaring Jepsen-ready

Run a black-box Kafka-client history checker using real idempotent and
transactional clients against ksink, capturing the history outside ksink's
WAL. Randomize the following faults:

* SIGKILL at every WAL append, sync, acceptance delimiter, terminal decision,
  manifest switch, compaction checkpoint and publication receipt boundary.
* Response drops after durable Produce/EndTxn and after S3 PUT/MPU completion.
* EIO, short write, ENOSPC, EDQUOT, failed file/parent-directory `fsync`,
  missing segments and injected active-tail/sealed-frame corruption.
* Concurrent writers to the same S3 key, 409/412, checksum HEAD timeouts, lost
  multipart completion responses and orphaned uploads.
* Transactional producers across topics, up to five inflight idempotent
  batches, overlapping commits/aborts, restart/fencing, and owner takeover.
* Local WAL on a dedicated supported filesystem; test power-loss semantics
  separately with block-device failure injection, not just SIGKILL.

A release gate should target **at least 100,000 randomized histories** and
**10,000 kill/restart cycles**, exhaustive defined durable-stage cuts, the
full disk-error matrix, and a final no-fault convergence phase proving the
committed WAL projection equals output. These are proposed external
qualification thresholds, **not executions claimed by this repository**.

## Explicit follow-up work

* Version the WAL frame envelope with magic/version, header integrity and a
  monotonic frame sequence, with unambiguous active-tail repair and
  fail-closed sealed corruption. A storage-format change needs a separate
  migration/compatibility plan and must not be silently shipped as a
  small reliability patch.
* Expand test-only deterministic failpoints to cover file writes, real
  `fsync` failures, manifest `rename` and directory `fsync`, receipt
  appends, and segment deletion; add an external fault-proxy/MinIO race suite.
* Add a standalone linearizability/transaction-history checker and
  black-box fault orchestration. The bounded unit tests above are only
  building blocks of that campaign.
* Treat WAL on network/shared filesystems and concurrent noncooperating
  writers as unsupported until separately qualified.
