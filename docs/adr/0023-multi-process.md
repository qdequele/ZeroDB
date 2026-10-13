# ADR-0023: Multi-process access — lock file, shared reader table, cross-process writer lock

- Status: **Accepted, implemented** (stages 1–3, 2026-10-13). Approved
  2026-10-09 by the maintainer, open questions resolved to the proposed
  defaults (see the end of this ADR); stage-3 amendments 15–18 approved
  2026-10-12. The lock file is on by default. The decision text below is
  reconciled with the amendments; where a sentence was superseded, it says
  so inline. SPEC 04 §12 (`LCK-1` …) states the resulting invariants.
- Related issue: [#70](https://github.com/qdequele/ZeroDB/issues/70); retires
  divergence D-001 and the lock-file half of D-012.
- Date: 2026-10-09

## Context

LMDB lets several OS processes open one environment at once. A companion lock
file (`lock.mdb`) holds a shared-memory reader table, the last committed txnid,
and the writer mutex; a process that dies while holding a reader slot is
detected by PID liveness and its slot reclaimed. heed users get this by default;
`MDB_NOLOCK` turns it off.

ZeroDB is single-process by design (D-001). Every coordination point lives in
process memory:

- **Snapshot selection.** The meta pages are read once, at open (TXN-18). Read
  and write txns take their snapshot from an in-process `SnapshotCell`
  (`Mutex<Arc<Snapshot>>`) and the `commit_point` atomic (TXN-17, TXN-19). A
  reader in another process would never see a commit; a writer in another
  process would start from a stale base — including a stale ADR-0022 annex —
  and corrupt the file.
- **Reader table.** A heap `Box<[CachePadded<AtomicU64>]>`, one word per slot
  holding occupancy and the pinned txnid (ADR-0006, TXN-14..17). No PID, no
  reaping (TXN-16).
- **Writer exclusion.** `WriterLock`, an in-process flag plus condvar
  (TXN-6). It is deliberately thread-agnostic: `RwTxn` is `Send` (heed moves
  write txns across threads) and its drop may run on any thread.
- **Caches and per-process facts.** The ADR-0018 validated-pages cache assumes
  only this process changes the file; `map_size` is fixed per process at open;
  the `EnvAlreadyOpened` registry (TXN-51) and `PREV_SNAPSHOT` exclusivity rely
  on being the only process.
- **No lock file and no flock** anywhere in the engine. heed's `NO_LOCK` and
  `NO_TLS` are accepted and silently dropped by the adapter. `zerodb-tools`
  takes a best-effort `flock` on the data file and refuses a live environment.

The maintainer's decisions (2026-10-09): **full LMDB parity** (any process may
read or write), **on by default** with `NO_LOCK` restoring today's
single-process path, and the **hybrid** writer lock below.

## Options

### Option A — Kernel-released locks everywhere

Writer exclusion by an exclusive `flock`/`fcntl` lock, liveness by per-PID
byte-range locks. Same code on Linux and macOS; the kernel releases every lock
on death, so there is no owner-died recovery. Cost: one or two lock syscalls per
write txn, on the path where `commit/batch/n1` is already 1.60× LMDB.

### Option B — LMDB's Linux design: robust pthread mutex in shared memory

Uncontended writer lock costs no syscall. But a pthread mutex must be unlocked
by the thread that locked it, which contradicts `RwTxn: Send`; recovery needs
`EOWNERDEAD` handling; macOS has no robust mutexes, so a second implementation
(LMDB uses SysV semaphores) is needed.

### Option C — Hybrid (chosen)

A futex word owned by PID on Linux (no syscall when uncontended, releasable from
any thread), an `flock` on macOS, and Option A's byte-range liveness locks on
both.

## Decision

Option C. The design has six parts.

### 1. Lock file and the open protocol

- The lock file sits next to the data file: `lock.mdb` at the heed boundary,
  `zerodb.lock` natively (the same split as ADR-0010). Its layout is ZeroDB's
  own, not byte-compatible with LMDB's (as D-002 for the data file).
- It is mapped `MAP_SHARED`. Layout: a header — magic, lock-format version, page
  size, `max_readers`, the **published txnid word `T`**, the **writer-lock
  word**, a waiters bit — then 128-byte reader slots at offset `256 + i·128`
  (amendment 11), each a `txnid: AtomicU64` at +0 and a 64-bit owner word at
  +8 holding the owner PID and a claim generation (amendment 14). All access
  is by explicit offset over atomics, never by casting the mapping to a
  struct; the slot table is viewed as a slice of the core's `ReaderSlot`
  type, whose layout is fixed and checked at compile time (SPEC 04 LCK-3).
- **First-opener election** (LMDB's technique). On open a process tries an
  exclusive `fcntl` lock on byte 0 of the lock file. If it gets it, no other
  process has the environment open: it (re)initializes the header and wipes
  every slot — which also recovers from any crash — then downgrades to a shared
  lock. Otherwise it waits for a shared lock and validates the header; a
  version, page-size or `max_readers` mismatch fails the open.
  - **Amendment (2026-10-13, stage 3 Task 8):** the `max_readers` part of
    this rule now follows LMDB, as observed. A joiner adopts the lock file's
    reader count whatever it asks for, in both directions
    (`OBS_JOINER_FEWER_MAXREADERS = (0, 200)`,
    `OBS_JOINER_MORE_MAXREADERS = (0, 126)`); only a version or page-size
    mismatch fails the open. The adopted size is what `Env::info` reports.
    The first opener likewise never shrinks the table of a finished lock
    file left by an earlier run: it uses the larger of its own `max_readers`
    and the recorded count, and still wipes every slot
    (`OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS = 200`).
- Within one process, `EnvAlreadyOpened` (TXN-51) is unchanged. The lock file
  coordinates processes, not handles.
- **`NO_LOCK`** (engine: an `EnvOpenOptions` flag; adapter: heed's `NO_LOCK`,
  no longer dropped) creates no lock file and runs exactly today's
  single-process code, at today's cost.
- A read-only environment still needs a lock file, as in LMDB; if it cannot be
  created, open fails unless `NO_LOCK` is set. *(Amended by amendment 17: on
  a read-only filesystem, `EROFS`, a read-only open proceeds without one, as
  LMDB was observed to.)*

### 2. Writer lock (two layers, both thread-agnostic)

1. **In-process:** today's `WriterLock` stays the first gate. Threads of one
   process never contend on the cross-process word.
2. **Cross-process:**
   - **Linux:** the writer-lock word holds the owner's PID. Acquire is
     `CAS 0 → my_pid`; contended waiters set the waiters bit and
     `FUTEX_WAIT` with a timeout. On each timeout a waiter probes the owner's
     liveness (§3); if the owner is dead, `CAS dead_pid → my_pid` takes over.
     Release, from any thread: store 0, then `FUTEX_WAKE` only if the waiters
     bit was set.
   - **macOS:** an exclusive `flock` on a dedicated lock-file descriptor,
     released by the kernel on death and releasable from any thread.
     *(Superseded by amendments 2 and 3: an `fcntl` write lock on byte 1 of
     the one lock-file descriptor.)*
- **Writer death needs no repair.** A commit becomes visible only through the
  atomic meta write and the later store to `T` (§4), so a dead writer leaves at
  most unreferenced pages, as after any crash. The new owner rebuilds its state
  from the map (§4) and trusts nothing a dead writer left in shared memory
  except the reader slots.
- **Poisoning (REC-13) stays per process.** A process whose own commit failed
  mid-way is poisoned; others never saw that commit.

### 3. Shared reader slots, liveness and reaping

*(Reconciled 2026-10-13 with amendments 2, 7, 8, 9, 11, 12, 13 and 14; the
original text of this section is in git history.)*

- **Slot:** 128 bytes at offset `256 + i·128` (amendment 11): a `txnid` word
  and an owner word holding the owner PID (low 32 bits; `0` means "being
  claimed or released, or free — presumed live") and a claim generation
  (high 32 bits) that every claim and every adoption increments
  (amendment 14).
- **Claim:** `CAS txnid FREE → CLAIMED`, then store the owner word
  `(generation + 1, me)`, then pin (§4).
- **Release (owner):** three stores, `txnid = CLAIMED`, then the owner word
  with PID 0 (generation kept), then `txnid = FREE` (amendment 7). It unpins
  before it disowns, so a death at any point leaves either `CLAIMED` with
  the dead PID, which a reaper frees, or `CLAIMED` with PID 0, which pins
  nothing.
- **Liveness:** each process holds an `fcntl` byte-range lock at offset
  `2^32 + pid` in the lock file; "is P alive?" is an `F_GETLK` probe of that
  byte (LMDB's technique). A kernel lock, unlike `kill(pid, 0)`, is immune to
  PID reuse. Both platforms use POSIX record locks (amendment 2), which a
  process loses when *any* of its descriptors to the file closes; the
  implementation keeps one descriptor open for the environment's life and
  refuses a second in-process open of the lock file before opening a
  descriptor. LMDB shares the pitfall.
- **Reap:** a reaper loads every owner word, probes each PID found, and
  adopts a dead process's slot by a CAS of the word it loaded to
  `(generation + 1, reaper_pid)`, then runs the owner's release
  (amendment 8). The owner-word CAS is the single linearization point: two
  reapers cannot both adopt a slot, and the generation makes the CAS fail if
  the slot was freed and re-claimed in between, even under the same PID
  (amendment 14). A process's own-PID stale slots are freed at open
  (amendment 9).
- **When:** at first-opener election (the whole table is wiped); in
  `clear_stale_readers`, which returns the number of slots freed; and when a
  writer takes the writer lock over from a dead owner on Linux. Those are
  the points at which the LMDB fork was observed to reap (amendment 12); no
  other point reaps.
- **Known leak:** a process that dies between its claim CAS and its owner
  store, or between the second and third stores of its release, leaves a
  `CLAIMED` slot with PID 0 that no reaper can attribute. It pins nothing
  (`oldest()` skips `CLAIMED`) and costs one slot until the next
  first-opener wipe. LMDB avoids it by allocating slots under a reader
  mutex; this design accepts one leaked slot per such crash to keep slot
  claims lock-free.
- `oldest()` (the GC gate) is unchanged: it reads only the `txnid` words.
- `reader_list` gains a PID column (amendment 13).

### 4. Snapshot selection and the writer's base

- The writer stores the committed txnid into **`T`** *after* the meta page is
  written — and after its fsync in the durable modes, preserving the ordering
  ADR-0019 relies on.
- **Reader begin** is TXN-17's publish-and-verify against the shared `T`: load
  `T`, store it into the slot, reload `T`, retry until equal. It then uses meta
  slot `T & 1` from the map and checks that its txnid equals `T`.
- **Fast path:** if `T` equals the txnid of the process's cached
  `Arc<Snapshot>`, the cache is reused with no meta decode. A process pays the
  decode only after *another* process committed, so single-process use costs
  about one shared load and one slot store more than today.
- **Writer begin**, after both writer locks: rebuild the base — roots, catalog,
  ADR-0022 annex, `last_pg`, `map_size` — from the meta at `T`, never from the
  in-process cell; the same fast path applies when `T` matches the cache.
- **Byte visibility:** `pwrite` and in-place map writes land in the unified
  page cache, which every process's `MAP_SHARED` map sees on Linux and macOS.
  `msync` is needed only for durability, as today.
- **`NO_LOCK`** keeps the in-process cell and `commit_point` unchanged.

### 5. Per-process state and map growth

- **Map size**, as LMDB (exact codes observed in the oracle): a txn begin whose
  meta has a larger `map_size`, or a `last_pg` beyond this process's map, fails
  with `MapResized`; `Env::resize` adopts the new size. *(Corrected by
  amendment 15: only a `last_pg` beyond the logical map size fails; a larger
  `map_size` alone does not, as observed.)* In-place `WRITE_MAP`
  (ADR-0021) only ever grows the file while other processes may be alive.
- **ADR-0018 validated-pages cache:** it is seeded only from this process's
  *published* commits, keyed by page number and writing txnid. A txnid is unique
  among published commits, and the TXN-2 reuse of a failed commit's txnid
  touches only pages no snapshot references. A page another process rewrote
  therefore carries a new txnid and misses. The cache stays on; if review
  breaks this argument, the fallback is to disable it when a lock file is in
  use.
- **`PREV_SNAPSHOT`** requires winning the first-opener election (no other
  process alive); otherwise open fails, matching LMDB as observed.
- **Process-local and harmless:** comparators, the dbi registry, sequential-write
  overrides, the frame pool, the commit hook, the poisoned flag. Comparators
  must agree across processes — the existing D-014 hazard, not a new one.
- **`zerodb-tools`** (`stat`, `check`, `dump`, hot copy) attach to a live
  environment as ordinary readers; the data-file `flock` guard goes away.

### 6. `unsafe` policy amendment

The maintainer approved (2026-10-09) a new sanctioned `unsafe` category in
`zerodb-io`: **shared-memory synchronization primitives** — the atomic views
over the mapped lock file, the Linux `futex` syscalls and the `fcntl`/`flock`
calls. They are exposed to `zerodb-core` through a safe API, so `zerodb-core`
stays `forbid(unsafe_code)`. AGENTS.md's `unsafe` policy is amended in the same
change as the implementation. Every block carries a `// SAFETY:` comment; every
atomic states its `Ordering` and why, assuming a weak memory model.

## Amendments (stage-1 planning, 2026-10-09)

1. **Shared reader slots move into stage 1.** Without them a writer in one
   process can reuse pages a reader in another process still holds, so
   cross-process tests would be unsound. Stage 1 claims, pins and releases
   shared slots (with the `pid` word); stage 2 adds liveness-based reaping and
   its loom models.
2. **POSIX `fcntl` record locks on both platforms** for the election (byte 0),
   liveness (byte `LIVENESS_BASE + pid`) and the macOS writer lock (byte 1),
   instead of OFD locks on Linux. One code path, LMDB's own choice, atomic
   exclusive→shared conversion. The per-process pitfall (closing any descriptor
   drops every lock) is closed by refusing a second in-process open of the same
   lock file *before* opening a descriptor.
3. **macOS writer lock is an `fcntl` write lock on byte 1**, not `flock`: on BSD
   systems `flock` and `fcntl` locks on one file may interact. POSIX locks are
   per process, so any thread may release it; the in-process writer lock
   already serializes threads.
4. **Meta reads use `pread`** (`Backing::read_meta_copy`), not a slice of the
   map: another process may be rewriting the slot, and a kernel copy avoids a
   data race on borrowed memory. A torn copy fails its CRC and is retried.
5. **A joining process seeds its snapshot cache from `T`**, never from the
   newest valid meta: a meta written but not yet published (or whose commit then
   failed) must never enter the cache.
6. **Stage 1 is opt-in** (`EnvOpenOptions::lock_file(true)`, default off) so the
   existing suite is untouched; stage 3 flips the default and updates the two
   tests that assert the single-process behavior.

## Amendments (stage-2 planning, 2026-10-11)

7. **Release unpins before it disowns.** The owner's release is now three
   stores: `txnid = CLAIMED`, then `pid = 0`, then `txnid = FREE`. With the
   §3 order (`pid = 0`, then `FREE`), a process dying between the two stores
   left a *pinned* txnid with `pid == 0`. No reaper could attribute it, and it
   held the GC gate back until the next first-opener wipe. Now a death at any
   point leaves one of two states. `CLAIMED` with the dead PID is reapable.
   `CLAIMED` with `pid == 0` is the known leak, which pins nothing.
   The local (single-process) table keeps its one-store release.
   *Leak cost, restated:* a death between the second and third stores, like
   a death between a claim's CAS and its owner store, costs one reader slot
   until the next first-opener wipe; it never holds back the GC gate.
8. **A reaper adopts, then releases.** Reaping a dead process P's slot is
   `CAS pid P → reaper_pid` (the single linearization point of §3), then the
   owner's release sequence (amendment 7). A reaper that dies mid-reap leaves
   a slot carrying its own PID, which the next reaper frees.
9. **Own-PID stale slots are freed at open.** A slot carrying this process's
   PID before this process has claimed anything was left by a dead process
   with the same PID. The liveness probe cannot see that process is dead,
   because this process now holds that PID's liveness lock. A joining process
   frees every such slot during open, before its first claim. It uses
   `CAS pid P → 0` (which excludes a concurrent reaper's adoption), then
   `txnid = FREE`. It cannot first store `txnid = CLAIMED`, as amendment 7's
   release does: until the CAS succeeds this process does not own the slot.
   A concurrent reaper may already have adopted and freed it, and another
   process re-claimed it, so a store to its `txnid` before the CAS could unpin
   a live reader. Dying between those two stores leaks one pinned slot until
   the next wipe; the window is a single store, accepted and documented.
10. **Joiner re-election and magic-last.** The initializer stores the magic in
    `finish_open`, after `T` is seeded, so a joiner that sees the magic also
    sees a seeded `T`. After a joiner is granted the shared election lock, it
    tries the exclusive lock once more. POSIX converts its shared lock in
    place, and the upgrade succeeds only when no other process holds the
    byte, which means every process exited while it waited. If the upgrade
    succeeds, the joiner initializes. A joiner that loses the upgrade and
    finds no magic drops its lock and retries the election, a bounded number
    of times, and exhausting the retries fails the open. This happens when
    an initializer died before finishing while another joiner also holds the
    lock. The magic is stored with Release and loaded with Acquire. A magic
    found by a joiner that lost the upgrade may also be left over from a
    previous run: an initializer may have died before truncating, or every
    process may have exited while this joiner waited. Joiners that collide
    then adopt the previous run's table. That is the same state as joining a
    table whose processes all exited, which the protocol tolerates: `T` is
    the last published value, dead PIDs are reapable, and a stale writer word
    is taken over.
11. **Slot table at offset 256, lock-format version 2.** Slot 0 moves off the
    writer word's 128-byte line pair. The version bump makes a stage-1 lock
    file of a still-running process fail validation instead of being misread.
12. **Reap points, as observed in the oracle.** The table below is observed
    by `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs`, which
    SIGKILLs a reader process and then checks whether `mdb_reader_list` still
    shows its slot. Results are identical on macOS aarch64 and Linux x86-64
    for every scenario run on both, and stable over repeated runs. ZeroDB reaps at exactly these points:

    | Event after a reader process died | LMDB reaps? |
    |---|---|
    | Read transaction begin | No |
    | Write transaction commit (the scenario begins a write txn and commits it, so it covers the begin too) | No |
    | Another process opening and closing the environment | No |
    | Reader table full (`ReadersFull`) | No: the error is returned, no reap-and-retry |
    | Writer takeover after the previous writer died (Linux; the robust writer mutex reports the dead owner) | Yes |
    | First opener of the environment | Yes: the lock table is reinitialized |
    | Explicit `mdb_reader_check` | Yes |

    The writer-takeover row is not observed on macOS; the takeover scenario
    runs only on Linux. `mdb_reader_check` counts slots, not processes: a
    process that died holding two reader slots reports 2.

13. **`reader_list` reports the owner PID** (`ReaderEntry::pid`). It is
    `None` on the default path, and for a slot being claimed or released.
14. **Claim generation in the owner word (closes a PID-reuse ABA).** The
    slot's owner field is a 64-bit word: the owner PID in the low 32 bits,
    a claim generation in the high 32 bits, which every claim and every
    adoption increments.
    Without it, a reaper that read a dead PID P could adopt the slot after a
    new process with the same PID P freed it (amendment 9) and re-claimed
    it, because the word would read P again; the reaper would then free a
    live reader's slot. With the generation, any re-claim changes the word
    and the reaper's CAS fails. LMDB avoids the same race by re-checking the
    PID under its reader mutex; ZeroDB's slot claims are lock-free, so the
    check moves into the word itself. Wrap-around needs 2³² claims of one
    slot while one reaper is paused between its load and its CAS.
    A reaper also loads every owner word *before* it probes the PIDs it
    found, and adopts only by a CAS against the word it loaded: a process
    holds its liveness lock from open until after its last slot is released,
    so a probe that finds PID P dead proves every word carrying P that was
    loaded earlier is still a dead process's, unless the word has changed —
    which the generation makes the CAS detect. Probing first and loading
    later would let a new process with a reused PID be judged by a stale
    verdict.

## Amendments (stage-3 planning, 2026-10-12)

Status: **Approved (2026-10-12, maintainer)**: all twelve decisions at the end of
amendment 18 are accepted as proposed. The LMDB behavior cited here is pinned by the
`OBS_*` constants of `crates/zerodb-oracle/tests/lmdb_stage3_observation.rs`.
Each value was observed on both macOS aarch64 and Linux aarch64. The Linux
results come from a privileged container (`--privileged`), which allowed the
read-only bind mount and dropping to `nobody`. Without those privileges the
read-only-filesystem and run-as-`nobody` cases print `SKIPPED: <reason>` and
record nothing. The choices that need a decision are collected at the end of
amendment 18.

Terms used below:
- **Mapping:** one `mmap` of the data file.
- **Logical map size:** the `map_size` this process enforces (`MapFull`,
  `MapResized`, `Env::info`). A mapping may be longer than the logical map
  size, never shorter.
- **The default flip:** amendment 6 made the lock file opt-in for stages 1
  and 2. Stage 3 turns it on by default.

15. **Map growth: `MapResized`, `Env::resize`, and grow-only mappings.**
    Approved 2026-10-12.

    *Observed LMDB behavior (binding, rule 1).*

    | Case | LMDB | Constant |
    |---|---|---|
    | Read begin after another process committed past this process's map | `MDB_MAP_RESIZED` | `OBS_READ_BEGIN_SMALL_MAP` |
    | Write begin, same case | `MDB_MAP_RESIZED` | `OBS_WRITE_BEGIN_SMALL_MAP` |
    | The meta's `map_size` is larger, but `last_pg` still fits this map | begin succeeds | `OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES = false` |
    | `mdb_env_set_mapsize(0)` | adopts the meta's size (8 MiB in the test) | `OBS_SET_MAPSIZE_ZERO_ADOPTS` |
    | `mdb_env_set_mapsize` while this env has a write txn | `EINVAL` | `OBS_SET_MAPSIZE_WITH_WRITE_TXN` |
    | A process with a smaller map commits | the meta's size is not lowered | `OBS_COMMIT_PERSISTS_OWN_MAPSIZE = false` |
    | Open with a map smaller than the meta's, and the data fits | keeps the smaller map | `OBS_OPEN_SMALLER_THAN_META` |
    | Open with a map smaller than `(last_pg + 1) * page_size` | rounds up to exactly that minimum | `OBS_OPEN_BELOW_LAST_PG = "minsize"` |

    This corrects §5: a begin fails with `MapResized` only when the
    snapshot's `last_pg` lies beyond this process's logical map size. A larger
    `map_size` in the meta alone does not fail a begin.

    *Open-time parity fix.* Today ZeroDB keeps a requested map below
    `last_pg`. Observed: 8 MiB written, a reopen with 1 MiB reports
    `map_size` 1 MiB and serves reads through the mapping, which covers the
    file. The open in `zerodb_io::open_or_create_with_advice` now sets the
    logical map size to `max(requested or persisted, (last_pg + 1) *
    page_size)`, as LMDB does.

    The same `MAX_MAP_SIZE` cap as `resize` (below) applies at open, before
    any `set_len` or `mmap`. A persisted meta `map_size` above it is corrupt
    and fails the open with `Invalid`. A requested size above it fails with
    `Io(InvalidInput)`.

    *Where the logical size lives.* A published `MapView` holds two values:
    a reference to a mapping and the logical map size. One `Acquire` load of
    the current view gives a consistent pair. `EnvInner::map_size` stops
    being a separate field. This closes the race of a separately stored
    size: a reader that saw a new, larger size could otherwise read through
    an older, shorter mapping.

    *The begin check.* The engine gains `MdbError::MapResized`. The engine's
    error converts to the heed adapter's existing `MdbError::MapResized`.
    `check_snapshot_backed` loads the view once, then:
    - `(last_pg + 1) * page_size > view.map_size` → `MapResized`. This
      compares against the logical size, as LMDB compares against its
      `me_maxpg`.
    - `> view.bytes.len()` → `Invalid`. This is a real check, not a debug
      assertion.
    - beyond the file's real length → `Invalid`, because that is corruption
      (SPEC 06 REC-1a).

    The check runs on every snapshot decoded after open: read begin in a
    lock-file environment (`pin_shared` → `snapshot_at`) and the writer's
    base (`current_snapshot`). Both already release the slot or the writer
    locks on error.

    Two further rules:
    - A write begin also checks its base against the view, even when the base
      came from the cache. A write txn never starts with a base outside its
      logical size, which would otherwise make every allocation `MapFull`.
    - The cached-snapshot fast path of a read begin needs no check. The
      invariants below cover it, and the read begin gains no atomic.

    *Invariants.*
    1. Mapping lengths never decrease, in the publication order of views.
       Views are published one at a time (under the writer lock), and a new
       mapping is created only when it is longer than the current one. By
       read-read coherence, a thread's later load never returns an earlier
       view. So every mapping is at least as long as every snapshot that has
       ever passed a check.
    2. The logical size is never set below the newest published snapshot.
       `resize` holds the cross-process writer lock while it reads `T` and
       publishes (see below), so no commit lands in between. Every cached
       snapshot is at or below that `T`.

    *`Snapshot::map_size` and commits.* `Snapshot` gains the meta's
    `map_size`. A commit writes `max(base.map_size, the logical map size)`
    instead of its own size, so it never lowers the persisted size.

    *Engine API.* `Env::resize(&self, new_size: u64) -> Result<()>`:
    1. *Try* the in-process writer lock. If a write txn of this env is live,
       return `Error::Io` with raw OS error `EINVAL`. heed callers then see
       `Error::Io(os error 22)`, as with heed.
    2. In a lock-file environment, take the cross-process writer lock. This
       waits like a write begin, and it is the deliberate difference from
       LMDB, which takes no lock here (decision 3): its `mdb_env_set_mapsize`
       returns while another process holds a write txn, also `MDB_RDONLY`
       (observed 2026-10-13, `OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`,
       `OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER_RDONLY`). Without a lock file, this
       step does nothing.
    3. Read the newest snapshot: at `T` with `pread`, without the map check,
       or the process-local cell without a lock file.
    4. Compute `size = if new_size == 0 { snapshot.map_size } else { new_size }`,
       then round it up to `(last_pg + 1) * page_size`, computed with checked
       arithmetic: an overflow is `Invalid`. The result is never 0, since it
       is at least two pages.
    5. Refuse a size above `MAX_MAP_SIZE` (2^47 bytes, 128 TiB, the x86-64
       user address space): a meta-supplied size with `Invalid` (the meta is
       corrupt), a caller-supplied size with `Io(InvalidInput)`. This keeps a
       corrupt meta from causing a huge `set_len` under `WRITE_MAP`.
    6. Call `Backing::grow_map(size)`, which publishes the new view.
    7. Release the locks.

    Live **read** txns do not block `resize`. LMDB returns success in that
    case too: it does not check readers, and the caller's promise is what
    keeps them valid. The grow-only mappings keep them valid without that
    promise.

    *Adapter.* `unsafe fn resize(&self, new_size: usize) -> heed::Result<()>`
    mirrors heed 0.22.1 `Env::resize`. A size that is not a multiple of the
    OS page size fails with heed's `Io(InvalidInput)` and its exact message,
    using the adapter's existing `sysconf`. Otherwise it calls the engine. It
    stays `unsafe` only for heed's signature: the engine enforces what heed
    asks the caller to promise.

    *Mechanism: grow-only mappings, kept until close.*
    - `zerodb-io`'s `MmapBacking` and `WriteMapBacking` hold a
      `MapRegions` cell instead of one map:
      - `current: AtomicPtr<MapView>`;
      - `owned: Mutex<Owned>`, which records every mapping and every view
        ever created, as raw pointers from `Box::into_raw`;
      - every one of them is freed by `Box::from_raw` in `Drop` only.
    - `Backing::view(&self) -> MapView<'_>` returns `(bytes, map_size)` from
      one `Acquire` load. `Backing::bytes()` is `view().bytes`.
    - `Backing::grow_map(&self, map_size: u64) -> io::Result<()>`:
      - If `map_size` exceeds the current mapping's length, it creates a new
        mapping of `max(map_size, 2 × current length)` bytes. If that `mmap`
        fails with `ENOMEM`, it retries with exactly `map_size`. A second
        failure returns `Io`, never undefined behavior.
      - Under `WRITE_MAP` it first grows the file (amendment 15's file rule,
        below).
      - Either way it creates a new `MapView` with the logical size and
        publishes it with a `Release` store.
    - The trait default returns `Unsupported`. The fault backend forwards.
      The heap test backings (`zerodb-io`'s `TestWriteMap`, the core's
      `VecBacking`) keep the default; `MapRegions` itself has a heap
      mapping variant, which its miri test uses. *(Corrected 2026-10-13: this
      bullet said the heap test backing implements `grow_map`.)*

    *Address-space bound.* Each new mapping is at least twice the previous
    one, so all earlier mappings together are shorter than the newest one.
    Total retained address space is below 2 × the largest mapping. The only
    exception is when the `ENOMEM` fallback was taken. A view itself is a few
    bytes, one per `resize`. The logical map size stays exactly what was
    requested or adopted.

    *Soundness.*
    - Every `&[u8]` the engine lends comes from `Backing::bytes(&self)`.
    - A mapping is freed only in `Drop`, which needs `&mut self` and so runs
      after every borrow has ended.
    - Pointers produced by `Box::into_raw` are never moved or re-boxed before
      `Drop`, so references derived from them stay valid under Stacked
      Borrows.
    - So no `resize` can invalidate a borrow, whether it is held by a read
      txn, by the writer's cached whole-map view, or by a caller of the
      public `EnvInner::backing_bytes`. No count of live txns is involved.

    Two ordering edges carry it:
    - The `Release` store of `current` after the view and its mapping are
      fully built, paired with the `Acquire` load in `view()`. This is
      message passing, and it gives a consistent `(bytes, map_size)`.
    - Invariant 1, which makes any later load see a mapping at least as long.

    The logical size is never stored on its own, so no load of it can run
    ahead of its mapping.

    *Bytes seen through two mappings.* All mappings are `MAP_SHARED` maps of
    one file, so they show the same page-cache pages. Writes (`pwrite`, or the
    in-place `WRITE_MAP` broker through the current mapping) still target
    only pages no live snapshot references (TXN-62). The argument is the one
    for two processes (§4).

    Durability also covers old mappings:
    - A synchronous `Env::sync` ends with `fdatasync` of the data descriptor
      (`WriteMapBacking::sync` and the default backing). Pages dirtied
      through a mapping that was replaced are therefore flushed too.
    - `MAP_ASYNC` stays relaxed (REC-9).

    *File growth under `WRITE_MAP`.* The file grows only under the
    cross-process writer lock. A check-then-`set_len` without it can shrink
    the file under another process's mapping: A reads 8 MiB, B grows the file
    to 32 MiB, then A sets 16 MiB, and B gets `SIGBUS`.

    - **In `resize`**, both writer locks are held from step 2, taken through
      the core's ordinary path, which reaps dead readers after a dead-owner
      takeover (amendment 12). `grow_map` re-reads the file length (`fstat`)
      and calls `set_len(map_size)` only if the file is shorter.
    - **At open**, today `map_backing` calls `set_len` unguarded. Instead:
      - an *initializer*, which won the first-opener election and is the only
        process, grows the file as today;
      - a *joiner* opening with `WRITE_MAP` does **not** grow the file at
        open. It maps the logical size, which may extend past the file's
        end.
      - Without a lock file, the single-process path is unchanged.
    - **At a write-txn begin**, after both writer locks are held (and after
      the takeover reap, if any), a `WRITE_MAP` env grows the file to its
      logical map size if it is shorter: an `fstat` re-read, then `set_len`.

    Why a joiner's mapping past the file's end is safe:
    - A mapped page past the end of the file faults only when accessed.
    - Readers touch only pages up to their snapshot's `last_pg`. Those lie
      inside the file: the writer that committed them extended the file
      under the writer lock, through its own begin-time growth or by
      `pwrite`, before publishing `T`.
    - In-place `WRITE_MAP` writes (the ADR-0021 broker and `write_at`) happen
      only inside a write txn, after that txn's begin-time growth to the
      logical size. Every page allocated below the logical size (`MapFull`)
      is therefore inside the file.

    This also keeps every call of the cross-process writer lock inside the
    in-process writer lock. The Linux same-PID-stale takeover in
    `LockFile::lock_writer` keeps its stated reason unchanged: no other
    thread of this process can own the cross-process lock while the caller
    holds the in-process one.

    With this, §5's "`WRITE_MAP` only ever grows the file while other
    processes may be alive" holds.

    *Checking it.*
    - No new cross-thread protocol enters `zerodb-core`. The existing loom
      models stand.
    - `MapRegions` is checked by a miri test over heap mappings, which
      `zerodb-io`'s test backing already provides. Miri's data-race detector
      reports a weakened (`Relaxed`) load as a race.

    *`unsafe`.* It is all in `zerodb-io`, in the mmap-access category, each
    instance with a `// SAFETY:` comment:
    - the dereference of the view pointer loaded from `current` in `view()`,
      and of that view's `mapping` pointer;
    - in `publish`, the dereference of the current view (and its `mapping`)
      to reuse the mapping when no new one is added;
    - the `Box::from_raw` calls in `Drop for MapRegions`;
    - `unsafe impl Send for MapRegions` and `unsafe impl Sync for MapRegions`.
      Raw pointers are neither `Send` nor `Sync`. These are sound because
      they point to uniquely owned boxes of `Send + Sync` mapping types,
      mutated only under the mutex.

    `zerodb-core` gets no new `unsafe`, and `Env::resize` is safe. The
    AGENTS.md bullet "mmap access and page casting (`zerodb-core::page`,
    `zerodb-io`)" gains: "including the grow-only mapping cell
    (`zerodb-io::mmap::MapRegions`): an `AtomicPtr` to views and mappings
    freed only on drop, with its `Send`/`Sync` impls (ADR-0023 amendment
    15)."

    *Candidates measured.* Per-op time in a focused loop, median ns/op of 7
    interleaved rounds. Each run did 10M iterations, except 2M for
    `rw_empty_commit` with the lock file on.
    - `ro_begin_abort`: `read_txn` and drop.
    - `rw_empty_commit`: `write_txn` and an empty `commit`, with `NO_SYNC`.
    - `get`: one `get` in a held read txn.

    | Case | base | Flag | `Arc` per txn | `Arc` in `Snapshot` | **Grow-only** |
    |---|---|---|---|---|---|
    | `ro_begin_abort`, no lock file | 27.04 | 27.79 | 35.12 | 26.86 | **27.43** |
    | `ro_begin_abort`, lock file | 29.12 | 29.43 | 36.99 | 28.41 | **28.60** |
    | `rw_empty_commit`, no lock file | 78.02 | 78.61 | 77.69 | 77.60 | **77.61** |
    | `rw_empty_commit`, lock file | 948.81 | 939.99 | 937.33 | 942.74 | **938.10** |
    | `get`, no lock file | 71.83 | 71.45 | 68.66 | 65.86 | **70.42** |
    | 8 threads `ro_begin_abort`, no lock file | 107.39 | 98.95 | 146.39 | 95.32 | **98.33** |
    | 8 threads `ro_begin_abort`, lock file | 118.61 | 121.55 | 164.45 | 116.80 | **117.67** |

    - Every difference except the per-txn `Arc`'s is within run-to-run and
      code-layout noise. The `Arc`-in-`Snapshot` variant leaves `get`
      untouched and still moved it by 6 ns, which sizes that noise.
    - The per-txn `Arc` costs 8 ns per begin, and 39 to 46 ns under 8
      threads: a mutex and a shared refcount line.
    - The spike put the grow-only variant's single atomic load in
      `MmapBacking::bytes`. The `MapView` design keeps it to one load per
      `bytes()`/`view()` call, so these figures carry over.
    - These numbers are **macOS aarch64 (Apple M1 Pro) only**. They support a
      design choice, not a performance claim. Under rule 7 the x86-64 and
      Graviton three-column numbers belong in the implementation PR. On
      generic linux-aarch64 the `Acquire` load is `LDAR`, unmeasured here.

    *Candidates rejected.*
    - **A remap-in-progress flag**, LMDB's model with the caller's promise
      enforced. A read begin re-checks a `SeqCst` flag after its slot pin, and
      `resize` sets the flag and then scans this process's slots. It is
      equally cheap, but:
      - it unmaps while `&self` borrows exist, so it needs `unsafe` in
        `zerodb-core`;
      - it is sound only if every map borrow is bracketed by a slot, which
        the public `backing_bytes` breaks;
      - `resize` fails or spins behind any long-lived reader;
      - its `SeqCst` store-then-load proof is not checkable by loom
        (ADR-0006 R1).
    - **An `Arc` of the mapping per txn:** 8 to 46 ns per begin, as measured.
    - **The mapping's `Arc` inside `Snapshot`:** free per begin, and it
      unmaps promptly. But every map borrow would have to become
      txn-lifetime, which reworks the writer's whole-map view and the
      `WRITE_MAP` broker lifetimes (ADR-0021). It is the fallback if address
      space ever matters more than the bound above.

    *SPEC.*
    - SPEC 02 §8: map size, `resize`, grow-only mappings, the open-time
      rounding, the persisted size as a max.
    - SPEC 04: txn begin returns `MapResized`.
    - SPEC 06 REC-1a: a short map is `MapResized`; a short mapping or file is
      `Invalid`.
    - ADR-0004 D4 ("map once, no remap") is amended by reference: a mapping
      is never remapped in place; a larger one is added.

16. **Lock-file name derivation and the file guards.** Approved 2026-10-12.
    - The default lock name is derived from the data-file name: `zerodb.dat`
      → `zerodb.lock`, `data.mdb` → `lock.mdb`, and any other name
      `<data_file_name>-lock` (LMDB's `MDB_NOSUBDIR` convention). An explicit
      `lock_file_name` still overrides it.
    - **This reverses resolved open question 1 for non-default data names
      only.** `zerodb.dat` keeps `zerodb.lock`. Today every native env uses
      `zerodb.lock` whatever its data name, so two native envs with different
      data names in one directory share one lock file, unnoticed.
    - The adapter's explicit `lock.mdb` goes away, because derivation gives
      the same name.
    - `zerodb-tools` probes for `zerodb.dat` and `data.mdb` and opens with
      the name it found, so it derives the same lock file as the process it
      attaches to.
    - The explicit-name checks stay: the name differs from the data name and
      is a single path component.
    - **Same-file guard.** `LockFile::open` also receives the data-file path.
      Before anything is truncated, every opener compares the lock
      descriptor's `fstat` `(st_dev, st_ino)` with a `stat` of the data path.
      If they are the same file, the open fails with `Io(InvalidInput)`.
      This catches case-insensitive and normalizing filesystems (default
      APFS: `ZERODB.DAT` and `zerodb.dat` are one file) and hard links.
      - To know whether this open created the lock file, the open tries
        `create_new` first and falls back to opening the existing file.
      - On refusal, the lock file is unlinked only if this open created it
        and it is still empty. Otherwise the name now designates the data
        file, which must not be removed.
    - **Foreign-file guard.** The initializer refuses to truncate a non-empty
      file that does not carry a lock magic, with `Io(InvalidInput)`: the
      final magic `ZDBLOCK1`, or the initializing magic `ZDBLOCK0`
      (`LOCK_MAGIC_INIT`). The one exception is an all-zero header (below),
      accepted only at a length an initializer can leave: 8 bytes, or
      `256 + 128·k` for `k ≥ 1`. This refuses every *initialized* data file,
      including another env's data file whose name equals this env's derived
      lock name (data `a` derives `a-lock`, while another env uses `a-lock`
      as its data name). It does not refuse every foreign file: a file with
      a zero header at one of those lengths is accepted and overwritten.
      *(Reworded 2026-10-13: this bullet said the guard covers every foreign
      file.)*
      - A data file's first bytes are often zero, so "zero" cannot mean
        "ours". The initializer therefore truncates to 0 and then writes the
        initializing magic `LOCK_MAGIC_INIT` (`ZDBLOCK0`) with `pwrite` at
        the magic offset, before `set_len` and before the final magic
        (amendment 10).
      - A *process* crash at any point leaves an empty file, or one carrying
        the initializing or final magic. All three are accepted.
      - Nothing fsyncs the lock file, so an *OS* crash or power loss can
        keep the new length and lose the block holding the magic. The file
        then reads as zeros with a non-zero length. The initializer
        therefore also accepts a file whose whole header (the first 256
        bytes, up to the slot table, or the whole file if shorter) is zero,
        as never initialized, if its length is one an initializer can leave
        (8, or `256 + 128·k`, `k ≥ 1`).
      - An initialized data file can never match. A ZeroDB data file's page
        0 is a meta page with the magic `ZDB1` at byte 32, and an LMDB data
        file's page 0 carries `0xBEEFC0DE` at byte 16. Both lie inside the
        first 256 bytes and are non-zero.
      - Joiners keep treating anything but the final magic as unfinished.
      - **Residual window.** A data file is created under its final name
        (`create_env_file`: `O_EXCL`, then `set_len` to two pages, then the
        two meta writes and an fsync). Between the create and the first meta
        write it is empty or all zero. If, in that window, another env's
        initializer opens the same file as its lock file (only possible with
        the naming collision above), the guard accepts it, and both envs
        fail. The window is a few syscalls, and the derived names already
        narrow the case to a deliberately colliding data name. Closing it
        would need the data file to be built under a temporary name and
        renamed, which is a new protocol, not proposed here.
    - SPEC 02 §8 (naming) and `docs/COMPATIBILITY.md` record the rule.

17. **`NO_LOCK` and read-only.** Approved 2026-10-12.
    - The engine's opt-out is `EnvOpenOptions::lock_file(false)`. The default
      becomes `true` (the default flip), gated as below.
    - heed's `NO_LOCK` maps to `lock_file(false)`. The adapter's flag
      translation stops dropping it.
    - The adapter's `Env::flags()` reports `NO_LOCK` for an env without a
      lock file. The engine exposes whether a lock file is in use.
    - The adapter's `lock_file()` setter is removed. It exists only on this
      branch, so nothing that shipped breaks.
    - `NO_TLS` stays accepted and vestigial.
    - `NO_LOCK` creates no lock file (`OBS_NOLOCK_CREATES_LOCK_FILE = false`)
      and runs today's single-process path. `EnvAlreadyOpened` (TXN-51) and
      the in-process writer lock still apply.
    - **Gate for the default flip (rule 7).** The flip lands only after the
      stage-3 three-column bench: LMDB, ZeroDB on `main`, and ZeroDB after
      the flip, with the lock file on and with `NO_LOCK`. It runs on Linux
      x86-64, and on Graviton when a host is available. On macOS,
      `rw_empty_commit` costs 938 ns with the lock file against 78 ns
      without. That gap is macOS's `fcntl` writer lock: two syscalls per
      write txn, by design (§2). It is not a measure of the Linux futex
      path, which the gate measures.
    - **READ_ONLY with a lock file**, as observed:

      | Case | LMDB | Constant |
      |---|---|---|
      | Read-only open while another process holds a write txn | joins the lock file and reads | `OBS_RDONLY_EXISTING_LOCK = 0` |
      | `lock.mdb` exists but this process cannot write it | `EACCES` | `OBS_RDONLY_LOCK_UNWRITABLE` |
      | No `lock.mdb`, and the directory is not writable | `EACCES`, nothing created | `OBS_RDONLY_LOCK_DIR_UNWRITABLE` |
      | No `lock.mdb`, on a read-only filesystem | opens without a lock file and serves read txns | `OBS_RDONLY_LOCK_READONLY_FS = 0` |

      - LMDB opens the lock file read-write even for a read-only env, and
        ZeroDB already does (`LockFile::open`). The first three rows
        already hold.
      - ZeroDB mirrors the fourth. A read-only open whose lock-file create
        fails with `EROFS` continues on the single-process path, with no
        lock file, as if `NO_LOCK` were set. Every other lock-file error
        fails the open, as `Io`.
      - The fourth row was made with a read-only disk image (`hdiutil`) on
        macOS, and with a read-only bind mount in the privileged Linux
        container. The errno LMDB reacted to is inferred from the setup, not
        printed.
      - A read-only process may win the election and initialize the lock
        file. It seeds `T` from the meta and never writes the data file or
        takes the writer lock.
    - SPEC 01 `MDB_NOLOCK` row (WON'T becomes MUST), SPEC 00 flag rows,
      DIVERGENCES D-016 (it says `NO_LOCK` is an accepted no-op) and
      `docs/COMPATIBILITY.md` are updated.

18. **Tools attach as readers.** Approved 2026-10-12.
    - `stat`, `dump` and `check` open the environment `READ_ONLY` through
      `EnvOpenOptions`, with the data name the tool found and the lock name
      derived from it (amendment 16).
      - They take a reader slot and pin `T` like any reader.
      - On read-only media they run without a lock file (amendment 17).
      - A long `dump` or `check` pins its snapshot, so the file grows while
        it runs, exactly as with `mdb_dump` holding a read txn.
    - The data-file `flock` in `zerodb_tools::lock::acquire_existing` is no
      longer taken by `stat`, `dump` or `check`.
    - `check` today reads the raw file, which is a torn image on a live env.
      Attached, it walks the pinned snapshot: its roots, its free tree and
      its `last_pg`, through the txn's map view. It does not use the image's
      meta slots, and it never reads the content of pages that are free in
      that snapshot.
    - If the open fails as `Invalid` (a corrupt file, which is when `check`
      matters most), `check` falls back to the raw-image walker, while
      holding the env exclusively:
      - A new engine API,
        `zerodb::ExclusiveLock::acquire(dir, &EnvOpenOptions) -> Result<ExclusiveLock>`,
        opens the env's lock file through the non-blocking
        `LockFile::try_open_exclusive`, which tries the election once and
        never enters the joiner path. It succeeds only if this process wins
        the first-opener election, and returns an "env in use" error
        (`Io(WouldBlock)`) otherwise. It never calls `finish_open`, so the
        election byte stays write-locked until the guard drops.
      - The tools size the guard, and their attached open, from the reader
        count the lock file records, read first with
        `zerodb_io::lockfile::published_max_readers`. A released guard
        leaves the initializing magic, which records no count, so the next
        sole opener uses the count it asks for.
      - Every other opener waits in the election meanwhile. Dropping the
        guard leaves the initializing magic, which the next opener accepts
        (amendment 16).
      - It is the engine's own lock-file descriptor, the only one. The
        in-process registry refuses a second open of that lock file before
        any descriptor exists (amendment 2), and the tool opens no `Env`
        while it holds the guard. So POSIX's per-process record locks cannot
        be dropped or shadowed by another descriptor. The walker reads the
        *data* file, whose descriptors do not affect lock-file locks.
      - If no lock file can exist (read-only media, amendment 17), the raw
        walk runs and says it ran offline without a liveness guarantee, as
        LMDB tools would.
    - A `MapResized` at begin is answered with `resize(0)` and one retry.
    - `load` and `migrate-from-lmdb` keep refusing a non-empty destination
      (`refuse_existing_env`). Their destination `flock` excludes another
      tool but not a ZeroDB process, so it is replaced:
      - They open the destination through the engine, with the lock file
        on, and refuse unless this process won the first-opener election.
        This uses a new engine API, `Env::opened_exclusively() -> bool`,
        backed by `SharedRegion::is_initializer`, which is `true` without a
        lock file.
      - They hold that `Env` for the whole run and write through ordinary
        write txns. `migrate` already does. `load` moves from streaming raw
        pages into the data file to write txns with sequential writes
        (ADR-0015), as LMDB's `mdb_load` uses txns. Streaming raw pages under
        an open env would truncate and rewrite a file another process may
        have mapped.
      - Another process can still join after the check, as with `mdb_load`.
        A joining reader sees each commit through `T` and is harmless to a
        fresh load. A joining writer interleaves its own txns, as with LMDB.
      - The AGENTS.md `libc::flock` exception and `zerodb_tools::lock` are
        retired.
    - `crates/zerodb-tools/tests/live_env_refusal.rs` changes behavior.
      `tools_refuse_a_locked_env` is rewritten to hold a live env with an
      open write txn in a child process and assert that the tools attach and
      read its committed data. The PR calls this out under rule 2.
      `tools_refuse_a_missing_env` stays.
    - The "tools run offline" text in the `zerodb-tools` crate docs and usage
      string, `docs/TOOLS.md`, and AGENTS.md change with it.

    **Decisions** (approved 2026-10-12 by the maintainer, all as proposed):

    1. **Remap mechanism:** grow-only mappings kept until close.
       *Alternative:* the mapping's `Arc` in `Snapshot`, which unmaps
       promptly but means a large rework of the write path.
    2. **Geometric mappings:** each new mapping is at least 2 × the previous
       one, bounding retained address space below 2 × the largest.
       *Alternative:* map exactly the requested size, with unbounded
       retention.
    3. **`resize` takes the cross-process writer lock** with a lock file, so
       no commit lands while it rounds up. *Alternative:* LMDB's lock-free
       `set_mapsize`, which relies on the write-begin check and gives up
       invariant 2 for cached read snapshots.
    4. **The data file grows only under both writer locks**, in `resize`
       and at a `WRITE_MAP` write-txn begin. A `WRITE_MAP` joiner never
       grows it at open. *Alternative:* refuse `WRITE_MAP` growth while
       other processes are alive.
    5. **`resize` with live read txns succeeds**, as LMDB's return code
       does. *Alternative:* refuse while any in-process txn is live.
    6. **`MAX_MAP_SIZE` = 2^47**, for `resize` and at open. *Alternative:*
       no cap; only an `mmap` failure stops it.
    7. **Derived lock names** for non-default data names, reversing open
       question 1 there. *Alternative:* keep `zerodb.lock` and refuse a
       second data name in one directory.
    8. **Foreign-file guard** with an initializing magic, which also accepts
       an all-zero header. The creation window stays open. *Alternative:*
       the same-file guard only.
    9. **Read-only media:** `EROFS` on the lock file proceeds without a lock
       file, mirroring LMDB. *Alternative:* fail the open.
    10. **`check`'s raw fallback runs only under `ExclusiveLock`**, which
        holds the election byte for the whole walk. *Alternative:* warn and
        walk anyway.
    11. **`load` and `migrate` open the destination through the engine**,
        refuse unless `Env::opened_exclusively()`, and write through txns.
        `load` drops raw streaming, which retires the `flock`.
        *Alternative:* keep `load`'s raw streaming under `ExclusiveLock` for
        the whole build. That is faster, and every opener waits, but the
        tool would hold no `Env`.
    12. **The default flip is gated** on the Linux three-column bench.
        *Alternative:* flip now and measure in the PR.

## Consequences

### Staging (rule 6: the riskiest assumption first)

0. This ADR, approved before any code.
1. **Riskiest assumption** — lock file, first-opener election, the futex/`flock`
   writer lock, the shared `T` and the meta re-read with its fast path. It must
   show (a) cross-process visibility and writer handoff in two-process smoke
   tests and (b) acceptable single-process cost on the three-column ladder
   (`txn/begin`, `get/*`, `commit/batch/n1`; LMDB / ZeroDB before / after, in
   default and `NO_LOCK` mode). Report and stop before stage 2.
2. Shared reader slots with PID, liveness locks, reaping, loom models.
3. Parity and docs: `MapResized`, `PREV_SNAPSHOT`, tools, adapter flags, the
   full oracle suite; SPEC, DIVERGENCES, COMPATIBILITY, CHANGELOG.

### Tests

- **Oracle (multi-process differential):** built on the crash harness's
  child-process pattern, each scenario on both engines — cross-process commit
  visibility; a foreign reader holding back page reuse; a SIGKILLed reader, then
  `clear_stale_readers`' count; a SIGKILLed writer and takeover; `MapResized`;
  `NO_LOCK`. The "no `lock.mdb`" assertion in
  `env_lifecycle_differential.rs` flips to "present".
- **loom:** claim / release / reap against the `oldest()` scan; publish-and-
  verify against `T`. Per ADR-0006 R1, StoreLoad edges are SeqCst with a
  written proof, since loom cannot detect weakened SeqCst accesses.
- **Crash harness:** SIGKILL a writer process mid-commit while a reader process
  is live, then reopen.
- **Stress:** two processes, each with readers and a writer, 180 s on x86-64 and
  aarch64.
- **miri:** all non-mmap logic.
- **Changed test:** `crates/zerodb-tools/tests/live_env_refusal.rs` asserts that
  tools refuse a live environment; this ADR changes that behavior, so the test
  is rewritten to assert attachment. Called out in the PR (rule 2).

### SPEC and divergences

Rules amended (never renumbered; a rule that no longer holds is retired
explicitly): SPEC 04 §0, §2, TXN-6, TXN-7, TXN-9, §4, TXN-14..TXN-22 (TXN-18a
included), TXN-38, TXN-45b, TXN-50, TXN-51, TXN-53, TXN-56; SPEC 00 (lock-file
row, `clear_stale_readers`, `reader_list`, `NO_LOCK`); SPEC 01 (`MDB_NOLOCK`,
`PREV_SNAPSHOT` exclusivity); SPEC 02 ("no lock file is ever created"); SPEC 06
(REC-1a, recovery and readers). D-001 is retired; D-012's lock-file note is
updated. New invariants for the lock file get fresh IDs.

### Costs and risks

- Default single-process use pays one shared load and one slot store per txn
  begin, plus one uncontended CAS per write txn on Linux (a `flock` pair on
  macOS). Stage 1's bench decides whether that is acceptable.
- The macOS POSIX-lock pitfall (§3) and Linux-only futex give two writer-lock
  implementations to test on CI.
- A bug here corrupts data rather than crashing — the reason for stage-1-first,
  loom, crash and stress coverage before the default flips.

## Open questions for human review

Resolved 2026-10-09 (maintainer approval of the proposed defaults):

1. The native lock-file name is `zerodb.lock`.
2. `ReadersFull` does exactly what LMDB is observed to do; no reap-and-retry
   unless the oracle shows LMDB does one.
3. Stage 1 stays on a branch and does not merge to `main` until stage 2's reader
   reaping is in, so the default never ships without it.
