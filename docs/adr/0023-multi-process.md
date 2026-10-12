# ADR-0023: Multi-process access — lock file, shared reader table, cross-process writer lock

- Status: **Approved (2026-10-09, maintainer)** — open questions resolved to
  the proposed defaults (see the end of this ADR). Implementation proceeds by
  stage; stage 1 reports before stage 2 starts.
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
  word**, a waiters bit — then cache-line-padded reader slots of
  `{txnid: AtomicU64, pid: AtomicU32}`. All access is by explicit offset over
  atomics; no `#[repr(C)]` casts.
- **First-opener election** (LMDB's technique). On open a process tries an
  exclusive `fcntl` lock on byte 0 of the lock file. If it gets it, no other
  process has the environment open: it (re)initializes the header and wipes
  every slot — which also recovers from any crash — then downgrades to a shared
  lock. Otherwise it waits for a shared lock and validates the header; a
  version, page-size or `max_readers` mismatch fails the open.
- Within one process, `EnvAlreadyOpened` (TXN-51) is unchanged. The lock file
  coordinates processes, not handles.
- **`NO_LOCK`** (engine: an `EnvOpenOptions` flag; adapter: heed's `NO_LOCK`,
  no longer dropped) creates no lock file and runs exactly today's
  single-process code, at today's cost.
- A read-only environment still needs a lock file, as in LMDB; if it cannot be
  created, open fails unless `NO_LOCK` is set.

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
- **Writer death needs no repair.** A commit becomes visible only through the
  atomic meta write and the later store to `T` (§4), so a dead writer leaves at
  most unreferenced pages, as after any crash. The new owner rebuilds its state
  from the map (§4) and trusts nothing a dead writer left in shared memory
  except the reader slots.
- **Poisoning (REC-13) stays per process.** A process whose own commit failed
  mid-way is poisoned; others never saw that commit.

### 3. Shared reader slots, liveness and reaping

- **Claim:** `CAS txnid FREE → CLAIMED`, then store `pid = me`, then pin (§4).
  `pid == 0` means "being claimed or released — presumed live".
- **Release (owner):** store `pid = 0`, *then* `txnid = FREE`. This order keeps
  a reaper from mistaking a newly claimed slot for a dead process's slot.
- **Liveness:** each process holds an `fcntl` byte-range lock at offset = its
  PID in the lock file; "is P alive?" is an `F_GETLK` probe on byte P (LMDB's
  technique). A kernel lock, unlike `kill(pid, 0)`, is immune to PID reuse.
  Linux uses OFD locks. macOS has only POSIX record locks, which a process loses
  when *any* of its descriptors to the file closes; the implementation keeps one
  descriptor open for the environment's life, and the SPEC records the pitfall
  (LMDB shares it).
- **Reap:** a reaper claims a dead process's slot with `CAS pid P → 0`, then
  stores `txnid = FREE`. The `pid` CAS is the single linearization point: two
  reapers cannot both free a slot, and no reaper can free a slot a live process
  re-claimed.
- **When:** at first-opener election (the whole table is wiped); in
  `clear_stale_readers` (today it returns 0; it will return the reaped count);
  and at whatever other points LMDB reaps, as observed in the oracle — not
  guessed (rule 1).
- **Known leak window:** a process that dies between its claim CAS and its
  `pid` store leaves a `CLAIMED` slot with `pid == 0` that no reaper can
  attribute. It pins nothing (`oldest()` skips `CLAIMED`) and costs one slot
  until the next first-opener wipe. LMDB avoids it by allocating slots under a
  reader mutex; this design accepts one leaked slot per such crash to keep slot
  claims lock-free.
- `oldest()` (the GC gate) is unchanged: it reads only the `txnid` words.
- `reader_list` gains a PID column.

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
  with `MapResized`; `Env::resize` adopts the new size. In-place `WRITE_MAP`
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
   `txnid = FREE`. Dying between those two stores leaks one pinned slot until
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
    | Write transaction commit | No |
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

Status: **Proposed, awaiting maintainer approval** (AGENTS.md rule 5). No
implementation lands before approval. LMDB behavior cited here is observed by
`crates/zerodb-oracle/tests/lmdb_stage3_observation.rs` (the `OBS_*`
constants).

15. **Map growth: `MapResized`, `Env::resize`, and a grow-only remap.**
    Proposed, awaiting maintainer approval.

    *Observed LMDB behavior (binding, rule 1).*

    | Case | LMDB | Constant |
    |---|---|---|
    | Read begin after another process committed past this process's map | `MDB_MAP_RESIZED` | `OBS_READ_BEGIN_SMALL_MAP` |
    | Write begin, same case | `MDB_MAP_RESIZED` | `OBS_WRITE_BEGIN_SMALL_MAP` |
    | The meta's `map_size` is larger, but `last_pg` still fits this map | begin succeeds | `OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES = false` |
    | `mdb_env_set_mapsize(0)` | adopts the meta's size (8 MiB in the test) | `OBS_SET_MAPSIZE_ZERO_ADOPTS` |
    | `mdb_env_set_mapsize` while this env has a write txn | `EINVAL` | `OBS_SET_MAPSIZE_WITH_WRITE_TXN` |
    | A process with a smaller map commits | the meta's size is not lowered | `OBS_COMMIT_PERSISTS_OWN_MAPSIZE = false` |
    | Open with a map smaller than the meta's (the data fits) | keeps the smaller map | `OBS_OPEN_SMALLER_THAN_META` |

    This corrects §5. A begin fails with `MapResized` only when the
    snapshot's `last_pg` lies beyond this process's map. A larger `map_size`
    in the meta alone does not fail a begin.

    *The begin check.* `MdbError::MapResized` is added to the engine
    (`crates/zerodb-core/src/error.rs:37` has none today); the adapter's
    variant (`crates/heed-zerodb/src/error.rs:117`) maps to it. In
    `check_snapshot_backed` (`crates/zerodb-core/src/env.rs:2223`), a snapshot
    with `(last_pg + 1) * page_size > map_size` returns `MapResized` instead
    of `Invalid`. The comparison is against `map_size`, as LMDB compares
    against its `me_maxpg`. A `last_pg` beyond the real file length stays
    `Invalid`, because that is corruption (SPEC 06 REC-1a). The check already
    runs on every decoded snapshot: read begin in a lock-file env
    (`pin_shared` → `snapshot_at`, `env.rs:893`, `:927`) and the writer's
    base (`current_snapshot`, `env.rs:951`). Both paths already release the
    slot or the writer locks on error. The cached-snapshot fast path needs no
    check: `resize` never sets `map_size` below the newest snapshot's
    `(last_pg + 1) * page_size`. The begin path gains no new atomic.

    *`Snapshot::map_size` and commits.* `Snapshot` gains the meta's
    `map_size`. A commit writes `max(base.map_size, this process's map_size)`
    instead of its own size (`crates/zerodb-core/src/rwtxn.rs:4360`), so it
    never lowers the persisted size.

    *Engine API.* `Env::resize(&self, new_size: u64) -> Result<()>`:
    - `new_size == 0` adopts the newest snapshot's meta `map_size`. In a
      lock-file env that is the snapshot at `T`, decoded with `pread`
      (amendment 4) and without the map check.
    - The result is rounded up to `(last_pg + 1) * page_size` of that
      snapshot, as LMDB rounds up to its minimum.
    - It is refused with `Error::Io` carrying raw OS error `EINVAL` while a
      write txn of this env is live. The in-process writer lock is
      *try*-acquired, not waited for. heed callers then see
      `Error::Io(os error 22)`, as with heed.
    - Live **read** txns do not block it. LMDB returns success in that case
      too: it does not check readers, and the caller's promise is what keeps
      them valid. The mechanism below keeps them valid without that promise.
    - It does not take the cross-process writer lock. A remap is
      process-local.

    *Adapter.* `unsafe fn resize(&self, new_size: usize) -> heed::Result<()>`
    mirrors heed 0.22.1 (`heed-0.22.1/src/envs/env.rs:647`). A size that is
    not a multiple of the OS page size fails with heed's `Io(InvalidInput)`
    and its exact message (the existing `sysconf` gives the OS page size).
    Otherwise it calls the engine. It stays `unsafe` only to keep heed's
    signature. The engine enforces what heed asks the caller to promise.

    *Remap mechanism: a grow-only map with retired regions (candidate C).*
    The backing keeps every region it has mapped until the env closes:

    - `zerodb-io` replaces the single map in `MmapBacking` and
      `WriteMapBacking` (`crates/zerodb-io/src/lib.rs:43`, `:122`) with a
      `MapRegions` cell. It holds
      `current: AtomicPtr<Region>` and
      `owned: Mutex<Vec<Box<Region>>>`, where every region ever mapped is
      owned by `owned` and is freed only by `Drop`.
    - `Backing::bytes()` keeps its signature. It does an `Acquire` load of
      `current` and returns the region's bytes, borrowed from `&self`.
    - A new safe trait method `Backing::grow_map(&self, len: usize) ->
      io::Result<()>` maps a new region of `len` bytes and pushes its `Box`
      to `owned` under the mutex. It then publishes it with a `Release`
      store to `current`. Under `WRITE_MAP` it first `set_len`s the file to
      `len` when the file is shorter, so the file only grows (§5). A `len`
      that is not larger than the current region does nothing: the map never
      shrinks, and `map_size` alone records a smaller size.
    - The trait default returns `Unsupported`; the heap test backing and the
      fault backend forward or refuse.

    *Soundness argument.* Every `&[u8]` the engine lends is derived from
    `Backing::bytes(&self)`. The region it points into is owned by `owned`
    and freed only in `Drop`, which needs `&mut self` and so runs after
    every borrow of the backing has ended. A `Box`'s heap address does not
    move when the `Vec` reallocates. So no remap can invalidate any borrow,
    whether it is held by a read txn, by the writer's cached `&'env [u8]`
    (`rwtxn.rs:829`, `:1814`), or by a caller of `EnvInner::backing_bytes`
    (`env.rs:1300`, public). This is pure ownership: no runtime handshake,
    no count of live txns. The one ordering edge is the `Release` store of
    `current` after the region is fully built, paired with the `Acquire`
    load in `bytes()` (message passing). It ensures a thread that sees the
    new pointer also sees the region's `(ptr, len)`.

    *Bytes seen through two regions.* Each region is a `MAP_SHARED` map of
    the same file, so they all show the same page-cache pages. The bytes a
    reader sees do not depend on the region it reads through. Writes
    (`pwrite`, or the in-place `WRITE_MAP` broker through the current
    region) still target only pages no live snapshot references (TXN-62).
    The argument is unchanged from two processes mapping one file (§4).

    *Checking it.* No new cross-thread protocol enters `zerodb-core`, so the
    existing loom models stand and none is added. The publication is checked
    by a miri test of `MapRegions` over heap regions. Miri's data-race
    detector reports a weakened (`Relaxed`) load as a race on the region's
    fields.

    *`unsafe`.* One new block in `zerodb-io`: dereferencing the
    `AtomicPtr<Region>` in `bytes()` (and in the `WRITE_MAP` write paths).
    SAFETY: the pointer is always one of the `Box`es in `owned`, published
    by `Release` after construction, and never freed before `Drop`.
    `zerodb-core` gets no new `unsafe`; `Env::resize` is a safe function. The
    AGENTS.md list item "mmap access and page casting (`zerodb-core::page`,
    `zerodb-io`)" gains: "including the grow-only map cell
    (`zerodb-io::mmap::MapRegions`): an `AtomicPtr` to a region that is
    freed only on drop (ADR-0023 amendment 15)."

    *Other `map_size` users.* `EnvInner::map_size` becomes an `AtomicU64`
    (`env.rs:577`). `resize` stores it under the in-process writer lock, after
    `grow_map` returns. So the region is always at least `map_size`, and a
    write txn reads a stable value: `MapFull` (`rwtxn.rs:1291`, `:1316`,
    `env.rs:1323`). `Env::info().map_size` reports it.

    *Measured cost.* The spike measured per-op time in a focused loop:
    `read_txn` + drop for `ro_begin_abort`, `write_txn` + empty `commit`
    with `NO_SYNC` for `rw_empty_commit`, and one `get` in a held read txn.
    It ran 10M iterations (2M for the lock-file `rw_empty_commit`),
    interleaved, in 7 rounds, with the lock file off and on. The table gives
    the median ns/op.

    | Case | base | A: flag | B1: `Arc` per txn | B2: `Arc` in `Snapshot` | **C: grow-only** |
    |---|---|---|---|---|---|
    | `ro_begin_abort`, no lock | 27.04 | 27.79 | 35.12 | 26.86 | **27.43** |
    | `ro_begin_abort`, lock | 29.12 | 29.43 | 36.99 | 28.41 | **28.60** |
    | `rw_empty_commit`, no lock | 78.02 | 78.61 | 77.69 | 77.60 | **77.61** |
    | `rw_empty_commit`, lock | 948.81 | 939.99 | 937.33 | 942.74 | **938.10** |
    | `get`, no lock | 71.83 | 71.45 | 68.66 | 65.86 | **70.42** |
    | 8 threads `ro_begin_abort`, no lock | 107.39 | 98.95 | 146.39 | 95.32 | **98.33** |
    | 8 threads `ro_begin_abort`, lock | 118.61 | 121.55 | 164.45 | 116.80 | **117.67** |

    Every difference except B1's is within run-to-run and code-layout noise.
    B2 does not touch the `get` path, yet it moved `get` by 6 ns, which sizes
    that noise. B1 costs 8 ns per single-threaded begin and 39 to 46 ns per
    begin under 8 threads: the mutex and the shared refcount line. These
    numbers are **macOS aarch64 (Apple M1 Pro)**. The x86-64 confirmation on
    the bench server is pending: its host key changed and is not yet
    re-verified. The linux-aarch64 (Graviton) run is pending too. There, an
    `Acquire` load compiles to `LDAR` rather than `LDAPR` unless the target
    enables `rcpc`.

    *Candidates rejected.*
    - **A: generation flag and quiescent remap**, LMDB's model with the
      caller's promise enforced. The read begin re-checks a `remapping` flag
      (`SeqCst`) after its slot pin, and `resize` sets it and then scans for
      this process's slots: a Dekker pair. Its cost is equally negligible.
      But the remap must unmap a region while `&self` borrows exist, so:
      - it needs an `unsafe` call from `zerodb-core`, a new allowlist entry;
      - it is sound only if every map borrow is bracketed by a slot or the
        writer lock, which the public `EnvInner::backing_bytes` already
        breaks (the oracle's crash verifier uses it);
      - `resize` must fail or spin while any in-process reader lives, and
        Meilisearch keeps long-lived and `'static` read txns;
      - its `SeqCst` StoreLoad proof cannot be checked by loom (ADR-0006 R1).
    - **B1: an `Arc` of the mapping cloned per txn.** It costs 8 to 46 ns
      per begin, as measured above.
    - **B2: the mapping's `Arc` carried in the `Snapshot`.** It adds no
      atomic per begin, and it frees old regions as soon as their last
      snapshot drops. But every map borrow would have to come from a
      txn-held `Arc` instead of `&'env EnvInner`. That means reworking the
      writer's `&'env [u8]` view, `whole_map`, and the `WRITE_MAP` broker
      lifetimes (ADR-0021), plus the public `backing_bytes`. It is a large,
      risky change to buy prompt `munmap`. It stays the follow-up if address
      space retention matters (below).

    *Cost of C.* Retired regions keep their virtual address space, and the
    page-table entries of pages touched through them, until the env closes.
    Only a `resize` that grows past the current region adds a region.
    Meilisearch does not resize a live env (it closes and reopens).
    Multi-process users resize after a `MapResized`, which is rare when map
    sizes are generous, as LMDB advises. The retained total is the sum of
    the superseded region sizes. SPEC 02 §8 documents it.

    *SPEC.* SPEC 02 §8 (map size: `resize`, grow-only map, persisted size =
    max), SPEC 04 (txn begin returns `MapResized`), SPEC 06 REC-1a (the
    per-snapshot check returns `MapResized` for a short map and `Invalid`
    for a short file). ADR-0004 D4 ("map once, no remap") is amended by
    reference: still one region per size, never remapped in place.

16. **Lock-file name derivation and the same-file guard.** Proposed,
    awaiting maintainer approval.
    - The default lock name is derived from the data-file name: `zerodb.dat`
      → `zerodb.lock`, `data.mdb` → `lock.mdb`, and any other name
      `<data_file_name>-lock` (LMDB's `MDB_NOSUBDIR` convention). An explicit
      `lock_file_name` still overrides it.
    - Today the native name is the constant `zerodb.lock` whatever the data
      name (`crates/zerodb/src/lib.rs:85`, default at `:230`), so two native
      envs with different data names in one directory share one lock file,
      unnoticed. The adapter forces `lock.mdb`
      (`crates/heed-zerodb/src/env.rs:315`), and that line goes away:
      derivation gives the same name.
    - `zerodb-tools` opens with the data name its two-name probe resolved, so
      it derives the same lock file as the process it attaches to
      (amendment 18).
    - The explicit-name check `lock_file_name == data_file_name`
      (`lib.rs:521`) stays, and so does the single-component check.
    - **Same-file guard.** `LockFile::open` (`crates/zerodb-io/src/lockfile.rs:264`)
      also receives the data-file path. Before the initializer truncates
      (`set_len(0)`, `lockfile.rs:341`), every opener compares the lock
      descriptor's `fstat` `(st_dev, st_ino)` with a `stat` of the data path.
      If the data file exists and is the same file, the open fails with
      `Io(InvalidInput)` before anything is written. This catches
      case-insensitive and normalizing filesystems (default APFS), where
      `ZERODB.DAT` and `zerodb.dat` are one file, and hard links. The
      in-process registry already keys by `(dev, ino)` (`lockfile.rs:132-231`).
    - Residual risk, not covered: env 1 with data `a` derives lock `a-lock`,
      and env 2 in the same directory uses `a-lock` as its *data* name.
      Neither env's own guard sees it. An optional extra guard could refuse
      to truncate a file whose first page validates as a ZeroDB meta page.
      That is for the maintainer to decide.
    - SPEC 02 §8 (naming) and `docs/COMPATIBILITY.md` record the rule.

17. **`NO_LOCK` and read-only.** Proposed, awaiting maintainer approval.
    - The engine's opt-out is `EnvOpenOptions::lock_file(false)`. The default
      flips to `true` (amendment 6's stage-3 flip): the field default at
      `crates/zerodb/src/lib.rs:229` and the setter at `:408`.
    - heed's `NO_LOCK` maps to `lock_file(false)`. `zerodb_env_flags`
      (`crates/heed-zerodb/src/env.rs:328`) stops dropping it.
    - The adapter's `Env::flags()` (`env.rs:415`) reports `NO_LOCK` for an env
      opened without a lock file. The engine exposes whether a lock file is
      in use (`EnvInner::shared.is_some()`).
    - The adapter's `lock_file()` setter (`env.rs:217`, field `:81`, `:104`,
      `:121`) is removed. It exists only on this branch (`git show main:`
      has none), so nothing that shipped breaks. `NO_TLS` stays accepted and
      vestigial.
    - `NO_LOCK` creates no lock file (`OBS_NOLOCK_CREATES_LOCK_FILE = false`)
      and runs today's single-process path. `EnvAlreadyOpened` (TXN-51) and
      the in-process writer lock still apply.
    - **READ_ONLY with a lock file**, as observed:
      - A read-only open joins a live environment's lock file and reads,
        while another process holds a write txn
        (`OBS_RDONLY_EXISTING_LOCK = 0`).
      - A lock file this process cannot write fails the open with `EACCES`
        (`OBS_RDONLY_LOCK_UNWRITABLE`). LMDB opens the lock file read-write
        even for a read-only env, and the engine already does too
        (`lockfile.rs:270-275`). The `Io` error propagates
        (`crates/zerodb/src/lib.rs:554`), so heed callers see `Io(os error 13)`.
      - A read-only process may win the election and initialize the lock
        file. It seeds `T` from the meta and never writes the data file or
        takes the writer lock.
      - Not observed yet: a read-only open in a directory where `lock.mdb`
        does not exist and cannot be created. Its error code is observed
        (rule 1) before that path is implemented.
    - SPEC 01 `MDB_NOLOCK` row (WON'T becomes MUST), SPEC 00 flag rows,
      DIVERGENCES D-016 (it says `NO_LOCK` is an accepted no-op) and
      `docs/COMPATIBILITY.md` are updated.

18. **Tools attach as readers.** Proposed, awaiting maintainer approval.
    - `stat`, `dump` and `check` open the environment `READ_ONLY` through
      `EnvOpenOptions`, with the data name resolved by the two-name probe and
      the lock name derived from it (amendment 16). They take a reader slot
      and pin `T` like any reader, and never block or disturb a live writer.
    - The data-file `flock` (`crates/zerodb-tools/src/lock.rs`,
      `acquire_existing`) is no longer taken by `stat` (`commands.rs:29`),
      `dump` (`:139`) or `check` (`:324`).
    - `check` today reads the raw file (`std::fs::read`, `commands.rs:340`),
      which is a torn image on a live env. Attached, it walks the pinned
      snapshot instead: its roots, its free tree and its `last_pg`, through
      the txn's map view. It does not use the meta slots in the image, which
      another process may rewrite, and it never reads the content of pages
      that are free in that snapshot. Opening a corrupt file fails as
      `Invalid`. Then `check` falls back to the raw-image walker, which is
      `check`'s main use, and prints that the result assumes no live writer.
    - A `MapResized` at begin (another process grew the map) is answered by
      `resize(0)` and one retry.
    - `load` (`commands.rs:160`) and `migrate-from-lmdb`
      (`migrate.rs:65`) keep refusing a non-empty destination
      (`refuse_existing_env`, `commands.rs:288`), and keep their destination
      `flock` so two loads cannot race into one empty directory. The AGENTS.md
      `libc::flock` exception narrows to that use.
    - `crates/zerodb-tools/tests/live_env_refusal.rs` changes behavior.
      `tools_refuse_a_locked_env` is rewritten to hold a live env with an
      open write txn in a child process and assert that the tools attach and
      read its committed data. The PR calls this out under rule 2.
      `tools_refuse_a_missing_env` stays.
    - The "offline" docs change: `zerodb-tools/src/lib.rs:16-24` and `:169`,
      `docs/TOOLS.md`, and the AGENTS.md `zerodb-tools` line.

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
