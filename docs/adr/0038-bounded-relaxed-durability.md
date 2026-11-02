# ADR-0038: Bounded relaxed durability: steady and weak metas, a durable watermark

- Status: Proposed (awaiting maintainer approval; no code changed yet)
- Related: issue #35; issue #36 and ADR-0039 (group commit); ADR-0005 (GC),
  ADR-0008 (crash harness), ADR-0019 (O_DSYNC meta write, parked),
  ADR-0021 (in-place `WRITE_MAP`), ADR-0022 (meta free-list annex),
  ADR-0023 (lock file, multiple processes), ADR-0026 (ranged `msync`);
  SPEC 06 REC-9/REC-10 and its open item 4
- Date: 2026-11-02

## Context

### What a durable commit costs

A default-mode commit pays two barriers (SPEC 04 TXN-61): C3 `fdatasync`s
the data pages, C5 `fdatasync`s the meta page. LMDB pays two as well, or one
through its `O_DSYNC` meta descriptor (ADR-0019, accepted but parked because
it showed no measured gain). Against LMDB, durable commits are at parity:
`commit/sync/*` runs at 0.97–1.03× LMDB's time on a Linux x86-64 server with
SATA storage. The barrier itself is the cost. On AWS Graviton4 a single-put
commit without a barrier costs 5.17 µs in ZeroDB and 4.25 µs in LMDB
(`commit_census`). A barrier costs what the device charges for a cache
flush: from tens of microseconds on local NVMe to about a millisecond on
network block storage (ADR-0019: on EBS io2, YCSB B with fsync showed a
write p99 of 1.51 ms for ZeroDB and 1.10 ms for LMDB). For a tiny commit
the barrier is one to three orders of magnitude larger than the work.

The only way to commit without that cost today is LMDB's relaxed flags,
which heed exposes as `EnvFlags`:

| Flag | Barriers per commit | After a power loss |
|---|---|---|
| default | C3 and C5 | nothing lost |
| `NO_META_SYNC` | C3 only | recent commits lost; the fallback snapshot can be corrupt (below) |
| `NO_SYNC` | none | an unbounded suffix lost; corrupt unless the filesystem keeps write order (REC-11) |
| `MAP_ASYNC` (with `WRITE_MAP`) | asynchronous `msync` | as `NO_SYNC` |

Neither relaxed flag bounds the loss, and neither is corruption-free.

### The reclaim-clobber window

The crash harness found a corruption window in `NO_META_SYNC` (SPEC 06
REC-10 amendment; repro seed 15797139550980166469). Commit `N` returns with
its meta written but not synced. Txn `N+1` may reclaim pages freed by `N`
(SPEC 05 GC-18: a page freed by `F` is reusable once `F ≤ oldest_reader()`,
and `oldest_reader() = N` here). Those pages belong to snapshot `N−1`. If
`N+1` writes them (at C2, or earlier when it spills) and power fails before
meta `N` reaches disk, recovery falls back to meta `N−1`, whose pages now
hold `N+1`'s bytes. LMDB shares this window under `MDB_NOMETASYNC`. SPEC 06
documents it and names a steady-gated reclaim as the fix, which this ADR
designs.

The root cause applies to any scheme that lets a commit return, or a later
txn start, before the previous meta is durable: the reclaim gate counts the
newest *published* snapshot, while crash recovery can only fall back to the
newest *durable* one.

### Prior art: libmdbx

libmdbx, a maintained LMDB fork, has `MDBX_SAFE_NOSYNC` for this purpose.
Its public documentation says:

- `MDBX_SAFE_NOSYNC` syncs lazily, by thresholds it applies itself, and
  after an OS crash or power loss "recent commits may be lost, integrity
  guaranteed". `MDBX_UTTERLY_NOSYNC` is LMDB's `MDB_NOSYNC`.
- The thresholds are a written volume (`mdbx_env_set_syncbytes`,
  `MDBX_opt_sync_bytes`) and a timeout (`mdbx_env_set_syncperiod`,
  `MDBX_opt_sync_period`).
- A commit made without a sync leaves a *weak* checkpoint. A weak checkpoint
  "is ignored on opening next time": transactions after the last non-weak
  (*steady*) checkpoint are rolled back.
- Each meta page stores the boot id of the system that wrote it.
- Processes with different relaxed modes cannot share an environment
  (`MDBX_INCOMPATIBLE`).

Two more details are believed but not verified, because this tree has no
libmdbx source: libmdbx keeps three meta pages, and it trusts a weak meta
written during the current boot. Nothing below depends on either.
Clean-room rule: this ADR describes libmdbx's behavior, not its code.

### Constraints from the current engine

- **Two meta slots**, pages 0 and 1. Txn `N` writes slot `N & 1`, which
  holds `N−2` (SPEC 04 TXN-63). Every meta page is CRC-protected (SPEC 02
  §3.3). The meta header has two reserved fields covered by the CRC and
  ignored on decode: `reserved0` (u16 at offset 18) and the 8-byte variant
  tail (offsets 24–31). The 4-byte `checksum` field at offset 20 is held for
  data-page checksums.
- **Format version 2** (ADR-0022). A meta page lists the pages its own txn
  freed (the free-list *annex*) when they fit. The next writer draws from it
  if `B ≤ oldest_reader()`, where `B` is its base txnid (SPEC 05 GC-31), and
  carries the rest forward under its own txnid (GC-30).
- **In-process readers** never re-read a meta page: they clone the published
  `Arc<Snapshot>` (TXN-10). **Readers in other processes** (ADR-0023) load the
  published txnid `T` from the lock file, `pread` meta slot `T & 1`, check
  that its txnid is `T`, and retry when the copy is torn.
- **The crash harness** (ADR-0008, SPEC 06 §5) models power loss: writes
  issued since the last barrier may be dropped, reordered or torn at
  sector granularity. Process kill is modeled separately.

## What the option promises

A new environment option, off by default, makes most commits *weak*: the
commit is visible to readers when it returns, but no barrier has run.
Periodically a commit, or a background flush, is *steady*: its data and its
meta are durable, and recovery may land on it. The option takes up to three
bounds:

```rust
// zerodb::EnvOpenOptions and heed's EnvOpenOptions (heed-zerodb), next to
// max_dirty_bytes and sequential_writes
pub fn bounded_durability(&mut self, bounds: DurabilityBounds) -> &mut Self;

pub struct DurabilityBounds {
    pub max_commits: Option<u64>,      // weak commits since the last steady one
    pub max_bytes: Option<u64>,        // bytes written by those commits
    pub max_period: Option<Duration>,  // age of the oldest of them
}
```

At least one bound must be set; an empty `DurabilityBounds` fails the open
with `Invalid`. The *unsynced window* is the set of commits made after the
newest steady meta. The promise after **any crash**, process or power loss:

1. **No corruption.** Recovery selects a snapshot that passes the check tool
   (SPEC 06 REC-18.3) in full, with no exemption like `NO_META_SYNC`'s
   "stale fallback".
2. **No loss of anything acknowledged durable.** A commit is acknowledged
   durable once a steady commit or flush at or after it has returned, or once
   `wait_durable(txnid)` has returned `Ok` for it. Recovery never lands
   before such a commit.
3. **Bounded loss.** Only commits in the unsynced window at the moment of
   the crash can be lost, and the window never exceeds the bounds:
   - fewer than `max_commits` weak commits;
   - at most `max_bytes` written by them (data pages, spilled pages and
     their meta writes; a commit that would push the window past the bound
     is made steady instead, so even a single oversized commit is never
     weak);
   - no weak commit older than `max_period` while the environment is open
     in a live process and no write transaction holds the writer lock (see
     the stages: until the background flusher ships, this bound holds at
     commit time only). The flusher needs the writer lock (§9), so a weak
     commit followed by a long write transaction stays in the window until
     that transaction commits: the time bound stretches by the length of
     that transaction. The commit-count and byte bounds are unaffected.
     Open question 9 asks whether to remove this exception.
4. **A process crash loses at most the window, and usually nothing.** When
   the machine did not reboot, the OS page cache holds every completed
   write, and recovery keeps the weak head (see recovery below). The one
   exception is a page size larger than the OS page: a kill can then cut
   short a weak commit's meta write, which overwrites the previous weak
   head in place (Option A). Recovery finds neither valid and lands on the
   steady meta, losing the weak commits since it, including acknowledged
   ones; the loss still stays within the bounds of point 3. With the
   default page size (one OS page) a meta write cannot be torn by a kill,
   and a process crash loses nothing. Power loss or an OS crash can cost
   the window. Process crashes (OOM kills, panics, deploys) are the common
   case.

The window is a *loss* bound, not a *latency* bound: a commit in the
window is visible to every reader at once, as with `NO_SYNC`.

New calls (native API, and in the heed adapter behind the Meilisearch fork
feature of ADR-0027, since heed has no such calls):

- `Env::durable_txnid() -> u64`: the txnid of the newest steady meta.
- `Env::wait_durable(txnid) -> Result<()>`: return once that txnid is
  acknowledged durable, flushing if needed.
- `Env::force_sync()` (existing, `mdb_env_sync(env, 1)`): in this mode,
  flush and return once the current head is steady. `Env::sync(false)`
  does the same.

## Design

### 1. Steady and weak metas on disk

A meta page gains two fields in its reserved, CRC-covered header bytes:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 18 | 2 | `meta_flags` | bit 0 `WEAK`: written without a barrier. Every other bit MUST be 0. |
| 24 | 8 | `boot_tag` | weak metas: a 64-bit hash of the writing system's boot id; 0 when unknown or on a steady meta |

A meta without `WEAK`, which includes every meta written today, is steady.
A steady meta is written only after a barrier that covers every page it
references, and it is itself followed by a barrier before anyone treats it
as steady. These are the two barriers a default commit already pays.

The boot id comes from `/proc/sys/kernel/random/boot_id` on Linux and from
the `kern.bootsessionuuid` sysctl on macOS. If it cannot be read, the tag is
0 and a weak meta is never trusted after a crash. Inside a container the
boot id is the host's, which is correct here because the page cache is the
host kernel's.

### 2. Which slot a commit writes

Rule: **a commit writes the slot that does not hold the newest steady
meta.** Weak commits therefore overwrite the same slot over and over, and
the slot holding the newest steady meta is never written while it holds
it. A steady commit writes the other slot. Once its meta barrier returns,
that slot holds the newest steady meta, and the old one becomes the slot
that gets overwritten.

In default mode every commit is steady, so the rule alternates slots
exactly as `N & 1` does today: same layout, same writes, same parity. The
`PREV_SNAPSHOT` self-resolution (SPEC 06 REC-5) is unchanged, because the
first commit after a `PREV_SNAPSHOT` open still overwrites the abandoned
slot.

### 3. How recovery picks the meta

Validate both slots as today (SPEC 02 §3.2 steps 1–5, with an added check
that undefined `meta_flags` bits are 0). Then:

1. If the higher-txnid valid slot is steady, select it. This is today's
   rule.
2. If it is weak and its `boot_tag` is non-zero and equals the current
   boot's tag, select it: the OS never lost its page cache, so every write
   the weak commit issued is readable. The opener then makes it steady with
   an immediate flush (§6) before the open returns. If that flush's barrier
   fails, the open fails and the env is not used.
3. Otherwise select the steady slot. The slot rule guarantees one exists,
   is intact and was durable: it was written before a barrier and never
   rewritten since. Emit the REC-4 warning, saying that weak commits were
   discarded.

Both slots weak is unreachable after env creation (creation writes two
steady metas, SPEC 02 §3.4). Finding it is `Invalid`, like REC-3.

`PREV_SNAPSHOT` keeps its meaning, "the older of the two valid slots". In a
file left by this mode, that is the newest steady meta when the head is
weak.

### 4. The reclaim gate: the steady snapshot pins like a reader

The gate becomes:

```
oldest = min(writer_txnid − 1, smallest live reader pin, S)
```

where `S` is the txnid of the newest steady meta. A page freed by `F` is
reclaimable only when `F ≤ oldest` (TXN-21 and GC-18 unchanged in form).
The annex draw gate GC-31 uses the same `oldest`. In default mode `S =
writer_txnid − 1` always, so the gate is today's. In the new mode, the
newest steady snapshot acts exactly like a long-lived reader pinned at `S`.
Every existing GC mechanism already handles a parked reader: oldest-first
scans, the annex carry (GC-30), and spill to the GC tree when the annex
overflows (GC-32). No new GC structure is needed.

Why this closes the window. Let `S_r` be the steady meta that recovery
selects in rule 3. Every page `S_r` references was durable when `S_r`'s
meta was written. After that, any writer only writes:

- pages beyond its base's high-water mark, which `S_r` does not reference
  (`last_pg` never decreases across commits; GC-10 shrinks only pages
  extended within the same txn);
- loose pages it allocated itself;
- pages freed by some `F ≤ S'`, where `S'` is the steady txnid when that
  writer drew them. Steady txnids only grow, so `F ≤ S' ≤ S_r`. A page
  freed by `F` is absent from every snapshot `≥ F`, so `S_r` does not
  reference it;
- the meta slot that does not hold `S_r` (§2).

So no write after `S_r` touches anything `S_r` needs, whether the write
reached disk or not. This is SPEC 04 TXN-62 restated against the steady
snapshot instead of the base. It also covers the cases the base-relative
form does not: spills (SPEC 04 §6.3a), and uncommitted or aborted
in-place `WRITE_MAP` writes that the kernel may flush at any time (GC-31's
caveat).

**File growth cost.** A page freed in the window becomes reusable at the
next steady point, not at the next commit. In steady state each page
written by a copy-on-write commit frees about one page, so the file needs
up to one window of extra free space: at most `max_bytes` (or `max_commits`
times the pages one commit rewrites, for example 1,000 single-put commits ×
about 4 pages × 4 KiB ≈ 16 MiB). Pages freed in the window are reused after
the next steady point, so growth stops at one window. Readers pinning old
snapshots cost what they cost today. A cheaper gate is possible (a page
above `S`'s `last_pg` cannot be referenced by `S`, whoever freed it) but
needs a per-page test inside the oldest-first scan. It is left as an open
question.

### 5. The commit pipeline in this mode

A **weak commit** runs C0–C2 as today, skips C3, writes the meta (C4) with
`WEAK` and the boot tag into the slot the slot rule names, skips C5,
publishes (C6), and adds its bytes, its count and (if it is the first in
the window) its timestamp to the window counters.

Before C3, a commit checks whether becoming weak would break a bound:
`max_commits` reached, `max_bytes` exceeded counting this commit's bytes,
or the oldest weak commit older than `max_period`. If so, it runs as a
**steady commit**: C3 (whose barrier also covers every earlier weak write;
`fdatasync` flushes the whole file), C4 without `WEAK`, C5, then advance
`S`, reset the counters and wake waiters. This is the default pipeline,
unchanged.

### 6. Flush (checkpoint)

A flush makes the current head `H` steady without a user transaction. It
is a meta-only commit `H+1` that changes no tree. It needs only the meta
write, because `H`'s pages are covered by the flush's data barrier:

1. Data barrier: `fdatasync`, or under `WRITE_MAP` the ranged `msync` of
   the used prefix plus `fdatasync` (ADR-0026). This may run without the
   writer lock: it covers every write issued before the call.
2. Under the writer lock: if the head is still `H`, write meta `H+1`
   (steady, same roots; `H`'s annex is carried into it exactly as GC-30
   carries a base annex) into the slot the slot rule names, run the meta barrier,
   publish, advance `S`. If the head moved meanwhile, repeat from step 1,
   or let the moved head's own commit be steady when a bound requires it.

A flush that finds the head already steady does nothing. Closing a
writable environment runs a flush, so a clean shutdown leaves no weak
meta. If that flush fails, the next open recovers per §3.

### 7. Barrier failure

As REC-13: a failed barrier poisons the environment in that process. Every
weak commit since `S` is then lost on reopen; this is within the promise,
since none was acknowledged durable. Callers blocked in `wait_durable` for a
txnid above `S` get the I/O error. Readers holding weak snapshots keep
working, but on Linux pages dropped after a writeback error can later read
back their old on-disk bytes. Those readers may then see validation errors.
This exposure exists today under `NO_SYNC`; the ADR does not widen it.

### 8. The lock file and multiple processes

With the lock file (ADR-0023), the state a writer in any process needs is
shared in new header words. This bumps the lock-format version:

- `S`, the steady txnid (`AtomicU64`; stored with `Release` after the
  meta barrier, loaded with `Acquire` by waiters and at writer begin);
- the window counters: weak-commit count, weak bytes, and the
  `CLOCK_MONOTONIC` time of the first weak commit (system-wide on Linux
  and macOS, so comparable across processes of one boot);
- the mode and bounds the first opener chose. A process opening with a
  different durability mode (default, a relaxed LMDB flag, or different
  bounds) fails with `Incompatible`, as libmdbx does. Mixing could be made
  safe, because every writer applies the gate and the slot rule, but the
  crash harness would have to cover every mix.

Readers in other processes stop assuming slot `T & 1`: they `pread` both
slots and take the one whose txnid is `T`. Under the slot rule the weak
slot is rewritten by every commit, not every second commit, so such readers
retry more often after a torn copy. The retry already exists (ADR-0023
point 4). A process that dies leaves its window in the shared counters.
The next writer in any process sees them and makes its commit steady when
a bound is exceeded. The `max_period` bound for that window then waits for
some process to commit or flush (§5). When every process is gone, the next
opener's recovery (§3) handles the window. The lock file's first-opener
reinitialization wipes the counters only when no process has the env open,
which is exactly when recovery runs.

`wait_durable` waits on `S` with the lock file's futex on Linux and a
short poll on macOS, as the writer lock already does.

### 9. The durable watermark and the background flusher

The watermark is `S`: the steady txnid, in memory without a lock file and
in the lock file with one. It is advanced only after a meta barrier, so it
never overstates what recovery will find.

A background flusher thread (stage 3) runs a flush when `max_period`
elapses after the first weak commit, or when a `wait_durable` caller asks.
Several waiters then share one flush: this is group commit from the
caller's side. A caller that commits and then calls `wait_durable` gets
"returns only when durable", while concurrent callers share barriers. The
flusher takes the writer lock only for step 2 of the flush. During a long
indexing write txn it waits, and that txn's own commit applies the bounds.

### 10. Interaction with heed's flags and `WRITE_MAP`

- **`NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC`**: mutually exclusive with this
  option; the open fails with `Invalid`. They keep LMDB's exact behavior
  (LMDB parity). `NO_META_SYNC` could also adopt the steady gate on its own:
  its metas reference durable data, so none need marking weak. That would
  close REC-10's window at the cost of reusing freed pages one commit
  later. It is Option D below and an open question.
- **`WRITE_MAP`**: compatible. A weak commit issues no `msync`. The kernel
  may write map pages back at any time and in any order, including a weak
  meta before its data. This is safe: a weak meta from a lost boot is never
  trusted, the gate keeps every in-map write off the pages `S` references,
  and the steady slot is written only by a steady commit after its data
  barrier. Under in-place `WRITE_MAP` the meta is written into the map like
  any other page. ADR-0026's C3 flushes both meta slots; under this ADR
  that is harmless, since flushing the weak slot persists a weak meta, and
  the steady slot is unchanged.
- **`READ_ONLY`**: never writes. Recovery rule 2 needs a flush, so a
  read-only open of a file with a trusted weak head selects that head
  without flushing. That is safe for reading: the same boot's page cache
  backs it.
- **Environment copies** (`copy_to_file`, raw and compacting) write their
  own steady metas through their own barriers, as today.

### 11. Relation to group commit (issue #36, ADR-0039)

Group commit keeps every commit durable before it returns, and lets txn
`N+1` build on `N` while `N`'s barrier is in flight. That hits this
ADR's root cause: `N+1` may reclaim pages freed by `N` while meta `N` is
not durable. Group commit therefore needs the gate of §4 with `S` defined
as here (the newest meta whose barrier has returned). It does not need weak
metas or the slot rule if it writes metas strictly in order after their
data barriers. The two designs share one definition and one
implementation:

- `S` and its gate, the durable watermark, and `wait_durable`'s wake-up
  belong to whichever lands first, and the other reuses them;
- this ADR adds the weak marker, the slot rule, recovery rule 2 and the
  bounds;
- ADR-0039 adds overlapping a txn with the previous barrier, and aborting
  the dependent chain when a shared barrier fails.

Issue #36 asks that each environment use one of the two. With this design,
"weak commit, then `wait_durable`" gives callers group-commit semantics
without overlapping transactions, so for Meilisearch's task-queue
environment this option may be enough. That is open question 6.

## Options

### Option A — Two slots, the newest steady slot never written (recommended)

As designed above. No page moves, `FIRST_DATA_PGNO` stays 2, and
default-mode behavior is bit-for-bit today's except that the two header
fields are written as 0, which they already are. Cost: in this mode the
weak head is overwritten in place by the next weak commit, so the previous
weak head is no fallback. After power loss weak metas are discarded
anyway. After a process kill the only way to lose the head is a short
`pwrite` of a meta page larger than one OS page (a fatal signal can stop
the kernel's copy between OS pages). Its CRC then fails and recovery lands
on `S`, which is still within the bound. Readers in other processes retry
somewhat more often (§8).

### Option B — Three meta slots

libmdbx's believed layout: each commit writes the slot that is neither the
head nor the newest steady. This keeps the previous head as a fallback,
and the head's slot keeps today's lifetime of two commits. But page 2
becomes a meta slot, `FIRST_DATA_PGNO` moves to 3 and every page number in
every file shifts. That is a format version, a change to the checker,
copy, migration tool and crash harness, and a third meta read at every open
and cross-process decode, all for a property that Option A only lacks in
the multi-OS-page short-write case. Rejected unless review finds a case
where Option A's in-place weak overwrite is unsafe.

### Option C — No new mode; ship group commit only

Keep the LMDB flags as they are and make durable commits cheaper by
sharing barriers (#36). No loss window and no recovery change, but the
gain needs concurrent committers: a single committer still pays two
barriers per commit. Group commit still needs §4's gate (§11), so this
option only drops the weak marker, the slot rule and the bounds.

### Option D — Steady-gated `NO_META_SYNC` (a narrower first step)

Apply §4's gate to `NO_META_SYNC`, with `S` = the newest meta known
durable, which is the previous commit's, made durable by this commit's C3.
Every meta then references durable data, so no weak marker, slot rule or
boot tag is needed, and REC-10's corruption window closes for an existing
LMDB flag. It saves one barrier of two per commit, not both, and its loss
stays unbounded in count and time. It is the smallest change that tests
this ADR's riskiest assumption (the gate) against the harness's known
failing seed. It is recommended as stage 1 below, not as the destination.

## Decision (proposed)

**Option A**, opt-in, staged so the riskiest assumption is tested first
(CONTRIBUTING rule 6):

1. **Stage 1 — the gate.** `oldest = min(…, S)`, with `S` tracked per env.
   Spike it on `NO_META_SYNC` (Option D). Done when the crash harness's
   REC-10 seed and a 10k-cycle `NO_META_SYNC` run pass full REC-18 with
   zero stale fallbacks, `just bench-ab` shows default mode flat, and file
   growth is measured. Whether the gate stays on for `NO_META_SYNC` is a
   separate decision (open question 3).
2. **Stage 2 — weak metas and bounds.** Meta fields, slot rule, recovery
   rules, synchronous bound checks at commit, `durable_txnid`,
   `wait_durable` (flushing in the caller's thread), flush on close, lock
   file words. Format version 3 (open question 1).
3. **Stage 3 — background flusher.** `max_period` enforced while idle;
   waiters share flushes.

The default stays exactly LMDB's behavior. The new mode is a capability
LMDB lacks, behind an explicit option, so LMDB-COMPATIBILITY gains a
capability entry and no behavior difference.

## Expected gain and the Meilisearch paths that would use it

Engine level: a weak commit costs a no-barrier commit (5.17 µs per
single-put commit on Graviton4, 1.17× LMDB's no-sync commit). A steady
commit costs today's durable commit. With `k` commits per steady point, a
commit costs about `c + 2b/k`, where `c` is the commit's own work and `b`
one barrier. On network block storage, where `b` is about half a
millisecond to a millisecond (ADR-0019's EBS io2 numbers), small commits
get about two orders of magnitude cheaper as `k` grows. On local NVMe the
gain is smaller and device-dependent. No end-to-end Meilisearch
measurement exists; the bench plan below defines one.

Where Meilisearch would use it:

- **Task-queue environment (index scheduler)**: the only path with many
  tiny commits. Two kinds of write:
  - *Enqueue*, whose commit acknowledges an API call. A weak enqueue could
    vanish after power loss even though the client received a task id, so
    Meilisearch would need `wait_durable` before replying. Concurrent
    enqueues then share one flush. A lone enqueue gains nothing.
  - *Status changes during processing.* These could stay weak if
    Meilisearch can redo a batch whose final status was lost (open
    question 7). Then they gain the full amount.
- **Index environments (milli, hannoy)**: one large write txn per batch,
  where the barrier is a small part of the commit. Expected gain is
  negligible; they should stay in default mode.

## Crash-harness proof of the bound

The harness (ADR-0008) gains this mode as a durability axis in
`just crash-test-quick` and the long run, with a fake monotonic clock
injected so `max_period` is deterministic.

- **Power loss** (mechanism 2 images, a new boot: the boot tag never
  matches): obligations are full REC-18.1–3 with no stale-fallback
  exemption; the recovered txnid is the newest steady meta at the cut, it
  is at least every acknowledged-durable txnid (strong commits, flushes and
  `wait_durable` returns that completed before the cut), and the lost
  suffix satisfies all three bounds as computed from the journal.
- **Process kill** (mechanism 1, same boot: the image is every issued write,
  plus a short meta write at OS-page boundaries): recovery selects the weak
  head (zero loss), or `S` when the head's meta was cut short.
- **Legal-window encoding** (SPEC 06 §5 amendments): `floor` = the newest
  steady meta the cut can hold, `ceil` = the head. The shape assertion
  `ceil − floor ≤ 1` becomes `ceil − floor ≤ max_commits` for this mode.
- **Mutation self-tests** (ADR-0008 D6.1), each of which must make the
  harness fail within the quick run:
  - `broken_steady_gate`: the gate ignores `S` (today's gate). Expected
    trip: the REC-10 corruption, now outside any exemption.
  - `broken_slot_rule`: a weak commit may overwrite the steady slot.
  - `broken_weak_trust`: weak metas are trusted after a new boot.
  - `broken_bound`: one commit past `max_commits` stays weak.
- **Coverage**: REC-21's ≥ 10k cycles for the new mode, with and without
  `WRITE_MAP` and with the lock file on and off; a loom model of the `S`
  publication and `wait_durable` wake-up (stage 3).

## Scorecard

| | Default mode | New mode |
|---|---|---|
| **Faster** | Unchanged by construction: `min(…, S)` with `S = N−1`, same writes. To be confirmed flat with `just bench-ab`. | Small commits drop from two barriers to `2/k`; per-commit CPU unchanged (#146 is separate). |
| **Reliable** | The selection rule gains one branch that default files never take. The `meta_flags` check makes recovery stricter. | Closes REC-10's corruption window for this mode, proven by the harness and four mutation tests. More states to test: weak, steady, trusted and untrusted. |
| **Durable** | Unchanged: every commit steady, nothing lost. | Bounded, configurable loss after power loss; none after a process crash; acknowledged-durable commits never lost. Strictly better than `NO_SYNC` and `NO_META_SYNC`. |
| **Secure** | No change. | The boot tag is a new on-disk input. A hostile file can only claim a weak meta is trusted, which is no worse than today's acceptance of any CRC-valid meta; geometry checks (REC-1a) still apply. Reading the macOS sysctl needs one `libc::sysctlbyname` call, an `unsafe` policy amendment in `zerodb-io`. The Linux path reads a file. |

## Consequences

- SPEC 02 §3: `meta_flags` and `boot_tag`, the flags check in §3.2 step 5,
  the selection rules of §3 above, format version.
- SPEC 04: TXN-21 (gate includes `S`), TXN-61 (weak and steady pipelines,
  flush), TXN-62 (restated against the steady snapshot), TXN-63 (slot
  rule), TXN-64 (in this mode a weak commit is published before it is
  durable, as under `NO_SYNC`).
- SPEC 05: GC-18 and GC-31 (the gate), GC-30 notes that the steady pin
  delays carried ids like a parked reader.
- SPEC 06: REC-2 selection, REC-9 lattice row, REC-10 note (closed in this
  mode, and in `NO_META_SYNC` if stage 1 is kept), REC-18 obligations, new
  rules from REC-23 on for the promise, recovery rule 2 and the flush. Open
  item 4 resolved.
- SPEC 01 §S6: the new mode is not an LMDB flag; mutual exclusion.
- ADR-0023 / lock file: new header words, lock-format version.
- `zerodb-tools stat` and `check` report each slot's flag and boot tag and
  the recovery choice.
- LMDB-COMPATIBILITY: a capability entry, no behavior difference.

## Bench plan (three columns: LMDB, ZeroDB before, ZeroDB after)

1. `just bench-ab` on the full ladder, default mode: expect flat (stage 1
   and 2 gates).
2. New rungs `commit/bounded/n1_k{10,100,1000}`: single-put commits with
   `max_commits = k`, against LMDB `commit/sync/n1` and `commit/batch/n1`
   (LMDB has no equivalent mode, so it brackets the result), on Graviton4
   with local NVMe and with EBS io2.
3. File growth: `del/churn/reinsert` and an update-heavy YCSB A run under
   the mode, reporting file size against default mode.
4. Meilisearch task-queue replay: concurrent enqueues with `wait_durable`
   before reply, against default mode, on the same two devices.

## Open questions for human review

1. **Format version.** The two fields fit in reserved bytes that today's
   decoder ignores. That means an older binary would accept a weak head
   after power loss. Bump to format version 3 (no migration, as ADR-0022
   did; there are no users yet), or keep version 2 and accept that older
   binaries are unsafe on files this mode wrote? Recommended: version 3.
2. **Same-boot trust** (recovery rule 2). Keep it, so process crashes lose
   nothing, or always roll back to the steady meta as libmdbx's
   documentation describes? Recommended: keep it. Process kills are the
   common failure, and a lost acknowledged task after an OOM kill is what
   users would notice.
3. **Keep the gate on `NO_META_SYNC`** after stage 1? It closes a known
   corruption window in an LMDB flag. The only observable effect is file
   size: freed pages are reused one commit later. Recommended: yes, as a
   documented strengthening, like REC-13's poisoning.
4. **Mixing modes across processes**: refuse with `Incompatible` (proposed)
   or allow it?
5. **Cheaper gate**: exempt pages above `S`'s `last_pg` from the steady pin
   (§4) if the growth measurement shows it matters?
6. **Group commit or this mode for the task-queue environment** (§11):
   decide after both ADRs are reviewed. The shared gate and watermark land
   with whichever is approved first.
7. **Meilisearch side**: can task status changes during processing be lost
   after power loss (the batch is redone), so they can stay weak? This
   needs a Meilisearch maintainer's answer before the consumer bench.
8. **API surface in the heed adapter**: `wait_durable` takes a txnid, and
   heed's `RwTxn::commit` returns none. Add `commit_with_txnid` under the
   fork feature (ADR-0027), or offer only `wait_durable_head()`?
9. **`max_period` during a long write transaction** (point 3). Accept that
   the time bound stretches while a write transaction holds the writer
   lock, or design a flush that does not need the lock: the flusher would
   flush the data pages of the committed weak head and write a steady
   copy of its meta into the steady slot, ordered against the running
   writer's meta write. Recommended: accept it for stages 1 to 3. A long
   indexing transaction ends in a commit that applies the bounds, and the
   loss stays within `max_commits` and `max_bytes`.
10. **Torn weak metas with large pages** (point 4). Accept the loss of the
    weak commits since the steady meta when a kill tears a multi-OS-page
    meta write, or keep the previous weak head as a fallback (Option B's
    third slot, or writing each weak meta to the slot that does not hold
    the head when the steady meta is older)? Recommended: accept it. It
    needs a page size above the OS page, a kill inside one `pwrite`, and
    stays within the bounds.
