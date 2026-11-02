# ADR-0039: Group commit: start the next write transaction during the previous commit's sync

- Status: Proposed (awaiting maintainer approval; no code changed). The
  recommendation is **not to build it now**; see [Decision](#decision).
- Related: issue [#36](https://github.com/qdequele/ZeroDB/issues/36);
  ADR-0004 (write path and commit pipeline), ADR-0019 (durable meta write,
  parked), ADR-0022 (free-page list in the meta page), ADR-0023 (multi-process
  writer lock); divergence [D-027](../LMDB-COMPATIBILITY.md#d-027) (a);
  issue #35 (bounded relaxed durability, drafted separately as ADR-0038);
  issue #146 (CPU cost of small commits).
- Date: 2026-11-02

## Context

### What a durable commit costs today

One write transaction runs at a time. The writer lock (an in-process mutex,
then the cross-process lock of ADR-0023 in a lock-file environment) is taken
at `write_txn()` and held until `commit()` returns (SPEC 04 TXN-6). In the
default, durable mode the commit pipeline (SPEC 04 TXN-61,
`crates/zerodb-core/src/rwtxn/commit.rs`) runs, all under that lock:

| Step | Work | Cost class |
|---|---|---|
| C1 | Save the freed-page list, flush database records | CPU |
| C2 | `pwrite` every dirty page, ascending page number | CPU + page-cache copy |
| C3 | `fdatasync` the data file | device flush |
| C4 | Write the meta page to slot `txnid & 1` | one page-cache write |
| C5 | `fdatasync` again, for the meta page | device flush |
| C6 | Publish the snapshot to readers, store the shared txnid `T` | CPU |

LMDB has the same structure, with one difference ADR-0019 describes: it writes
the meta page through a second descriptor opened `O_DSYNC`, so it issues one
`fdatasync` per commit where ZeroDB issues two. ADR-0019 adopted LMDB's scheme
but its implementation is parked: on the only flushing host we could reach,
removing the second barrier did not change commit latency.

A second writer, in the same process or another, waits for the whole
pipeline, both device flushes included. When many small durable transactions
arrive at once, each pays its own two flushes in turn, and the queue's
latency grows with its length.

### What group commit would change

InnoDB and PostgreSQL release the right to write before the flush, let the
next transactions do their work while it runs, and then make several
transactions durable with one flush. A transaction still reports success
only once its data is durable, and durability records still reach the disk in
commit order. LMDB cannot do this: its writer mutex covers the flush.

Issue #36 proposes the same for ZeroDB: the next write transaction starts its
copy-on-write work from the previous transaction's new in-memory root while
that transaction's flush is still running. When several small transactions
arrive close together, they end up sharing one flush.

### Existing numbers

Nothing in this ADR was measured for it. The figures below are earlier
measurements, cited:

- **Per-commit CPU without fsync:** 5.17 µs for ZeroDB and 4.25 µs for LMDB
  for a one-put commit on Graviton4 (`docs/PERFORMANCE.md`,
  `commit_census`).
- **Durable commits are at parity:** `commit/sync/n1` and `commit/sync/n100`
  run at 0.98× and 1.00× LMDB on Graviton4 (perf ledger, 2026-10-31 runs). The
  device flush dominates both engines.
- **Flush cost depends on the device.** On macOS (`F_FULLFSYNC`, `WRITE_MAP`),
  200 durable one-put commits take 1.53–1.61 s, about 8 ms per commit, against
  LMDB's 4 ms with one flush (ADR-0026). On the EBS io2 Graviton4 host the
  second `fdatasync` was nearly free (ADR-0019's parking note). Under YCSB B
  with fsync on that host, ZeroDB's write p99 was 1.51 ms against LMDB's
  1.10 ms (ADR-0019).
- **Barrier count:** about 2 `fdatasync` per durable commit for ZeroDB and 1
  for LMDB (strace, ADR-0019).

### Who would benefit

Group commit helps only when **several threads (or processes) issue small
durable commits at the same time on one environment**, on a device where a
flush costs much more than the commit's CPU work. A single writer thread gains
nothing: its next transaction cannot begin before its `commit()` returns, and
`commit()` still waits for the flush.

- **Meilisearch's main index environments** (written by `milli`, its indexing
  and search core, and `hannoy`, its vector index): no gain. The indexer writes one large transaction per batch from one thread; two flushes
  per batch are noise.
- **Meilisearch's task queue:** a possible gain. Each API call that enqueues a
  task (a document addition, a settings update) runs
  `IndexScheduler::register` on a blocking-pool thread
  (`tokio::task::spawn_blocking`): one write transaction on the task-queue
  environment, opened with default (durable) flags, then one commit. Under
  concurrent enqueue traffic those commits queue on the writer lock, each
  paying two flushes. The gain is bounded, though. `register` also rewrites
  the queue's status, kind and index bitmaps, whose size grows with the
  queue, so its CPU part is not 5 µs and stays serialized. And the request
  also persists the payload file outside the transaction. How much of an
  enqueue's latency is the two commit flushes has **not been measured**.
- **Other heed users:** services that write small durable transactions from
  many threads (a write per request) are the textbook case. They are not this
  project's first consumers.

Meilisearch would also have to opt in (see [Default](#default-opt-in)),
which is a code change on its side.

### Estimate

A model, not a measurement. Let `c` be one small commit's serialized work
(C1, C2 and the transaction body) and `f` one device flush. With `W` writers
always waiting:

- Today, each commit holds the lock for `c + 2f`, so throughput is
  `1 / (c + 2f)`, and an enqueue that arrives behind `W − 1` others waits about
  `W (c + 2f)`.
- With group commit and groups of `W`, the group's work (`W c`) overlaps the
  previous group's flushes. Those are one data flush plus one meta flush for an
  odd group size, or two for an even one (see the
  [meta rule](#meta-pages-order-and-slots)). Throughput is about
  `W / max(W c, 2f or 3f)`.

| Device flush `f` | Commit work `c` | `W` | Today, commits/s | Group commit, commits/s | Gain |
|---:|---:|---:|---:|---:|---:|
| any | any | 1 | `1/(c+2f)` | same | 1.0× |
| 10 µs (fast NVMe, io2 as measured) | 5 µs | 8 | 40k | 200k (CPU-bound) | ~5× |
| 500 µs (a network block device, assumed) | 5 µs | 8 | ~1.0k | ~5.3k | ~5× |
| 500 µs | 500 µs (a large `register`, assumed) | 8 | ~670 | ~2.0k (CPU-bound) | ~3× |
| 4 ms (macOS full flush) | 5 µs | 8 | ~125 | ~670 | ~5× |

Two things temper these numbers. First, ADR-0019's one-flush-per-commit
already removes one of the two `f`s for every durable commit, single writer
included, at LMDB parity. Measured against ZeroDB with ADR-0019 in place, the
gains shrink to roughly half. Second, the table assumes a steady queue of
`W` writers. Real enqueue traffic is bursty, and group sizes follow it.

## The design (Option B below)

The design below is the one Option B proposes. It is written out so the
review can judge its cost; it is not the recommendation.

### What a caller sees when `commit()` returns

- `commit()` returns `Ok` only once the transaction is **durable** (in the
  durable modes), exactly as today and as in LMDB. No new call is needed to
  wait for durability, and heed's signatures do not change.
- `Err` means what it means today. The transaction may or may not be durable
  (a failed meta flush leaves that unknown, as now), and the environment is
  poisoned (SPEC 06 REC-13).
- The difference a caller can observe: a write transaction can **begin and
  read** a previous transaction's data **before that transaction's `commit()`
  has returned**. If the previous flush then fails, the later transaction's
  `commit()` fails too, even though it did nothing wrong. A caller that acts
  on what it read inside a write transaction before committing (sends a
  reply, writes another file) can act on data that never became durable. LMDB
  never allows this, because the next writer cannot start until the commit
  has finished. Readers (`RoTxn`) are unaffected: they still see only
  durable, published snapshots (SPEC 04 TXN-64).
- Under `NO_SYNC` and `NO_META_SYNC` there is no flush to share, and the
  option changes nothing.

### Pipeline states

Three kinds of transactions exist at once:

- **Durable:** the newest meta on disk whose flush succeeded, `D`. The other
  meta slot holds an older durable snapshot, `P` (today always `D − 1`).
- **Queued:** transactions `D + 1 … M` whose pages are written (C2 done) and
  whose `commit()` is waiting for a flush. They form at most two groups: one
  being flushed, and the next one filling up.
- **Building:** at most one transaction, `M + 1`, holding the writer lock and
  building on `M`'s in-memory state.

### Writer lock: what is released, and when

**In-process lock.** `commit()` runs C1 and C2 under the writer lock, as now,
then puts the transaction on the commit queue and **releases the in-process
writer lock before any flush**. C2 stays under the lock on purpose. A failed
`pwrite` fails the transaction today without poisoning the environment, and
its txnid is reused. Keeping C2 under the lock means no later transaction
ever starts on top of a commit whose pages were not written.

The next writer starts from the tip of the queue (`M`'s roots, database
records, freed-page list, `next_pgno`), not from the published snapshot. It
reads `M`'s pages through the map. `pwrite` has put them in the unified page
cache, which the `MAP_SHARED` map sees before they are durable (ADR-0023 §4,
"byte visibility").

**Cross-process lock.** Another process's writer rebuilds its base from the
meta at the shared txnid `T` (ADR-0023 §4), and `T` names only durable,
published commits. That writer must therefore not start while this process
has queued commits, or it would build on `D` and overwrite what `D + 1 … M`
wrote. So the cross-process lock becomes **owned by the process's pipeline**.
It is taken by the first writer of a pipeline and released by whichever
thread finishes the last queued flush when no writer holds the in-process
lock. Both of its forms (the Linux futex word holding the PID, the macOS
`fcntl` lock) are already releasable from any thread (ADR-0023 §2). Writers
in other processes wait exactly as today, so they neither gain nor lose. A
pipeline-wide cross-process design is Option D, rejected below.

**Fairness.** A process with steady in-process write traffic may never drain
its pipeline, and so may starve other processes' writers. On Linux the futex
word's waiters bit shows that another process is waiting; the next in-process
writer then waits for the pipeline to drain, which releases the lock. macOS's
`fcntl` lock shows no waiters. There the pipeline is bounded by a group count
(open question 4).

### What the building transaction may and may not touch

While queued transactions' pages are still being flushed, the building
transaction:

- **may** read any page, including queued transactions' pages, through the
  map;
- **may** copy-on-write any page of `M`'s trees onto a newly allocated page;
- **may** extend the file past `M`'s `next_pgno`;
- **may** reuse a freed page only under the [reuse rule](#free-page-reuse)
  below;
- **may not** write a meta page, store `T`, publish a snapshot, or seed the
  validated-pages cache for readers. All of that happens in the flush step,
  in commit order;
- **may not** call `Env::resize` or anything else that needs the committed
  state to be final. `resize`, `Env::sync(force)`, a copy that takes the
  writer lock, and closing the environment first wait for the pipeline to
  drain.

Every page the building transaction writes lies either beyond every snapshot
on disk or among pages that rule GC-18, as tightened below, proves
unreachable from both meta slots and from every reader. A crash at any moment
therefore leaves them as unreferenced garbage, as REC-6 already allows for a
crash between C2 and C4.

### Free-page reuse

D-027 (a) records that LMDB never lets transaction `N` reuse pages freed by
transaction `N − 1`, even with no reader open. The maintainer chose LMDB's rule
on 2026-10-20 (the compatibility table still lists it as proposed until it is
implemented). The reason is milli's `PREV_SNAPSHOT` open
(`crates/milli/src/index.rs` in Meilisearch), which rolls an upgrade back to
the older meta. That snapshot must stay intact on disk, so pages it references
must not be overwritten before a newer meta replaces it.

Both meta slots must stay readable at every moment, so the general rule is:

> A page freed by transaction `F` may be reused only if `F ≤ P`, where `P` is
> the **lower txnid of the two meta slots on disk**, in addition to the
> reader gate of SPEC 05 GC-18.

A page freed by `F` is absent from snapshot `F` and from every later
snapshot. If `F ≤ P`, neither slot's tree references it.

- Without a pipeline, `P = N − 2` for writer `N`. The rule is then exactly
  LMDB's (D-027 (a)) and changes nothing beyond that decision.
- With a pipeline, `P` stays at the older durable slot while queued
  transactions advance. No transaction in the pipeline reuses a page that a
  queued transaction freed, nor one freed by `D`. If a flush fails, both `D`
  and `P` are intact.
- The rule protects `PREV_SNAPSHOT` under group commit, as the next section
  requires.

The cost is file growth under bursts: pages freed inside a burst become
reusable only once the burst's metas are durable. That growth is bounded by
the pages a burst frees.

**Interaction with ADR-0022.** Each commit's freed-page list rides in its meta
page. A queued transaction's freed list cannot be reused by its successors,
so each successor carries it into the free-page tree (the path ADR-0022 keeps
for lists that readers pin). That is a tree write per pipelined commit, the
cost ADR-0022 removed. It is small next to a flush, but it shows on the
no-flush ladder if the option is ever enabled without durability. Implementing
D-027 (a) raises the same question in sequential mode; whatever it settles for
"the previous commit's list" should be reused here.

### Meta pages: order and slots

The flush step runs on one thread, the **leader**: the first queued committer
that finds no flush in progress. It takes every queued transaction `D + 1 …
M` as one group and then:

1. Runs **one data flush** (C3) covering every page the group wrote.
2. Writes the meta of `X`, the newest transaction of the group whose txnid
   parity differs from `D`'s (`X = M` when the group size is odd, else
   `M − 1`), into slot `X & 1`. That slot holds `P`, the older snapshot. Then
   it runs the meta flush (C5). The older slot is the only one a meta write
   may overwrite (SPEC 04 TXN-63). The other slot holds `D`, the crash
   fallback, until `X` is durable.
3. Marks `D + 1 … X` durable: stores `T`, publishes `X`'s snapshot (C6) and
   returns `Ok` to their callers.
4. If `X < M`, writes `M`'s meta into slot `M & 1`, which now holds `D` and is
   the older slot since `X` is durable, flushes it, and marks `M` durable.

The intermediate metas `D + 1 … X − 1` are never written. Recovery picks the
newest valid meta (SPEC 06 REC-2) and does not need consecutive txnids in the
two slots. TXN-2's parity argument holds because step 2 always writes the
older slot.

**Ordering guarantees.** Any meta on disk references only data a successful
flush made durable, because a meta is written only after the data flush that
covers it (REC-7, unchanged). A newer meta never overwrites the only durable
one, because step 4 starts after step 2's flush. No transaction is reported
durable before its meta (or a later one) is durable, and none after a failed
flush. So no partly durable chain of commits can stand: everything up to the
last successful meta flush is durable, and nothing after it is reported.

**Visible effect on `PREV_SNAPSHOT`.** After a group, the older slot holds
`D`, the last snapshot of the previous group, not `X − 1`. An environment last
written with group commit therefore rolls back to before the last *group*
under `PREV_SNAPSHOT`, which may be several transactions. That is a
behavior difference from LMDB (for this opt-in option only). Writing every
intermediate meta instead costs one meta flush per transaction (`1 + k`
flushes for a group of `k` instead of `2` or `3`), which keeps most of the
cost the design removes (open question 3).

**Combining with ADR-0019.** With the `O_DSYNC` meta descriptor, steps 2 and 4
each become one synchronized write. The ordering above is unchanged.

### When a flush fails after the next transaction has started

ZeroDB already poisons the environment on any failed barrier (REC-13): every
later `write_txn` or `commit` fails until the environment is reopened. The
design relies on that rule. It is what makes failure handling simple, and
it is a prerequisite: if REC-13 were ever relaxed, aborting a pipeline would
need txnid rollback (TXN-2), which this ADR does not design.

| What fails | Callers in the group | Later transactions | Disk after a reopen |
|---|---|---|---|
| Data flush (step 1) | All of `D + 1 … M` get `Err(Io)` | Queued ones get the poisoned error; the building one gets it at `commit()` (its writes are discarded); new `write_txn` calls get it | `D`: no meta was written |
| First meta flush (step 2) | All of `D + 1 … M` get `Err(Io)` | As above | `D`, or `X` if its meta reached the disk intact. Both are consistent, because `X`'s data was durable. LMDB's scrub, accepted in ADR-0019, rewrites the slot's old bytes so a reopen before power loss does not read back an unacknowledged `X` |
| Second meta flush (step 4) | `D + 1 … X` already got `Ok`; `M` gets `Err(Io)` | As above | `X` or `M`, as in the row above |
| A `pwrite` (C2) | That transaction gets `Err(Io)`, as today; the environment is not poisoned | None started on it, because C2 runs under the writer lock | Unchanged |

Readers keep reading their pinned snapshots throughout, as REC-13 already
states. A poisoned process's pipeline drains (every waiter is woken with its
error) before it releases the cross-process lock, so another process's writer
then rebuilds from `T`, which still names `D` (or `X`).

The first commit of a `PREV_SNAPSHOT` open is durable but not published, and
the next commit reuses its txnid (ADR-0023 amendment 19). It runs outside the
pipeline: the pipeline starts after it.

### Relation to bounded relaxed durability (#35, ADR-0038)

Issue #35 lets `commit()` return before the data is durable, with a bound on
how much a crash may lose. That is the opposite trade: group commit keeps
every acknowledged commit durable and gives back only latency lost to
waiting. The two do not stack:

- An environment uses one or the other. Under a relaxed mode there is no
  per-commit flush to share, so group commit has nothing to do. Enabling both
  should be refused at open.
- They share one rule: **free-page reuse is gated on the older meta slot on
  disk, not on the newest commit.** Under a relaxed mode, recovery falls back
  to the last durable meta, so pages freed since then must not have been
  overwritten. That is the [reuse rule](#free-page-reuse) above, with `P`
  read as "the older durable slot". Whichever ADR is implemented first should
  write it into SPEC 05 GC-18, and the other should cite it.
- Both make metas durable only after the data they reference (REC-7), and both
  poison on a failed barrier (REC-13).
- Both could share the flush machinery. A relaxed mode's background flush is a
  leader with no callers waiting on it.

### Default: opt-in

Group commit is a capability LMDB lacks. It also changes two observable
things: a write transaction can read data not yet durable (above), and
`PREV_SNAPSHOT` can roll back more than one transaction. By the project's
rule it ships behind an explicit option: `group_commit` on the native
`EnvOptions`, and a ZeroDB-only setter on heed's `EnvOpenOptions` in
`heed-zerodb`, like `max_dirty_bytes`. The default stays LMDB's single-writer
commit. The first version refuses the option together with `WRITE_MAP` (open
question 5).

## Options

### Option A — Do nothing (keep LMDB's pipeline)

Keep the writer lock over the whole commit. Implement ADR-0019's one flush per
durable commit first, when a device that really flushes is available to
measure it.

- Faster: no change. ADR-0019 alone removes one flush per durable commit for
  every writer, single writer included, with no new concurrency.
- Reliable: no new protocol.
- Durable: unchanged.
- Secure: unchanged.

### Option B — Pipelined group commit, durable on return, opt-in (the design above)

- Faster: up to about 5× commits per second for `W ≥ 4` concurrent small
  writers when flushes dominate (model above); 1.0× for one writer, which
  includes every Meilisearch index environment. Unmeasured.
- Reliable: a new concurrency protocol at the center of durability. It
  touches the commit pipeline, the writer lock in both layers, the free-page
  gate, meta slot selection and failure propagation. The crash harness
  (ADR-0008) must model a pipeline: faults during a flush while another
  transaction writes, and a failed flush with transactions queued behind it.
- Durable: an acknowledged commit stays durable. New: a write transaction can
  read, and act on, data that never becomes durable; `PREV_SNAPSHOT` rolls
  back a group.
- Secure: no new `unsafe` is needed (a mutex and condition variable for the
  queue, the existing thread-agnostic lock releases).

### Option C — Early return with an explicit wait for durability

`commit()` returns after C2, and `Env::wait_durable(txnid)` (or a returned
token) blocks until the flush. A single writer thread could then overlap its
next transaction with the previous flush.

Rejected. It changes what `Ok` from `commit()` means under heed's API, so
every consumer would need auditing. And a commit that returns before
durability, with a bound on what a crash may lose, is exactly #35's subject.

### Option D — Pipeline across processes

Publish the queue's tip (roots, records, freed list) in the lock file, so a
writer in another process can build on it.

Rejected. It puts not-yet-durable state into shared memory, which ADR-0023
avoided by design ("a dead writer needs no repair"). It needs its own repair
story for a dead leader, and no consumer writes from several processes at
once.

## Decision

**Recommended: Option A now. Keep Option B as the approved shape if a
measurement justifies it later.**

- The project's goal is LMDB-level speed for Meilisearch. Group commit does
  nothing for the index environments, and its effect on the task queue is
  unmeasured and capped by `register`'s own CPU and the payload write.
  Meilisearch would also need a code change to opt in.
- ADR-0019 is the cheaper first step for durable-commit latency. It is LMDB
  parity, needs no new concurrency, and helps every durable commit.
- The design is sound on paper, but it touches every part that crash safety
  rests on. Building it is a large change (the issue is sized `size:L`) for a gain
  no consumer has shown it needs.

**Conditions to reopen and build Option B:**

1. D-027 (a) is implemented, since the reuse rule extends it.
2. ADR-0019 is implemented, or measured again on a device that really flushes.
3. A measurement shows the need: a Meilisearch enqueue load test with at
   least 4 concurrent clients, on a device whose `fdatasync` takes 100 µs or
   more, where the task-queue commit's flushes are at least half of the
   enqueue latency. A `concurrent/commit/sync` rung with `W` writer threads
   (LMDB and ZeroDB) should be added to the ladder at the same time, to give
   the three-column baseline.
4. Issue #35's design has settled, so the shared reuse rule and the "one or
   the other" refusal are written once.

## Consequences

If Option A is approved: no code change. Issue #36 is closed as "not now",
pointing at the conditions above. The reuse rule section of this ADR is
handed to D-027 (a) and #35 as the shared form of the gate.

If Option B is approved later, the implementation must:

- Split the commit pipeline at the lock release after C2. Add the commit
  queue (txnid, freed list, roots, waiters, poisoned state) and the leader
  flush step. Keep C1–C6 in one readable function per ADR-0004, with hooks
  H0–H4 at the same logical points, plus new hooks between a group's data
  flush and each meta write.
- Track `P` (the lower slot txnid) next to the durable txnid and use it in
  the GC-18 gate.
- Move the database-handle publication of ADR-0024, which happens today
  before the lock release, so later writers see handles at release and
  readers see them at durability.
- Seed the validated-pages cache (ADR-0018) for the writer chain at release,
  if the next writer is not to revalidate its predecessor's pages. Under
  poisoning a txnid is never reused once queued, so `(pgno, txnid)` stays
  unique.
- Tests:
  - the crash harness with two and three queued transactions, faults
    injected during each flush;
  - a fault-backend mode that fails the data flush with a transaction
    building behind it (it must get the poisoned error, and the disk must
    recover to `D`);
  - a mode that writes the meta of the wrong parity, to show the harness
    catches it;
  - a differential check that results equal LMDB's for single-threaded use
    with the option on;
  - a loom model of the queue's leader hand-off;
  - the stress test with several writer threads.
- SPEC updates:
  - SPEC 04 TXN-6 (the lock is released before the flush);
  - TXN-61 to TXN-64 (the pipeline split and the leader);
  - LCK-4 (the cross-process lock is owned by the pipeline);
  - SPEC 05 GC-18 (the gate on `P`);
  - SPEC 06 REC-2 and REC-5 (non-consecutive txnids in the slots, the
    `PREV_SNAPSHOT` target);
  - REC-6 (the new cut points);
  - REC-13 (pipeline drain on poison).
- Add a divergence entry in `docs/LMDB-COMPATIBILITY.md` for the opt-in
  option's two observable differences.

## Open questions for human review

1. **Option A now (recommended), or build Option B?** If B, is the opt-in
   default right, given that Meilisearch must then change code to benefit?
2. **Measure first?** Is a Meilisearch enqueue load test on a flushing device
   (condition 3) worth running before closing #36, or close it now and wait
   for a consumer to ask?
3. **Skip intermediate metas** (the design: 2 or 3 flushes per group,
   `PREV_SNAPSHOT` rolls back a group), or **write every meta** (`1 + k`
   flushes, `PREV_SNAPSHOT` rolls back one transaction, as LMDB)? A middle
   way: leave the group's last transaction for the next group when the group
   size is even, so a group always needs exactly one meta flush, at the cost
   of one more round of latency for that transaction.
4. **Cross-process fairness:** drain on Linux when the waiters bit is set, and
   bound the pipeline to a fixed number of groups on macOS, or always drain
   after each group when the lock file shows another process attached?
5. **`WRITE_MAP`:** the data flush is a ranged `msync` of `[0, next_pgno)`
   (ADR-0026), which would also flush pages the building transaction is
   writing in place. That is harmless for crash safety, since those pages are
   unreferenced, but it needs its own review. Refuse the combination in the
   first version, as proposed?
