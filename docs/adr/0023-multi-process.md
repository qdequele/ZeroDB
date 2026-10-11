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
    of times. This happens when an initializer died before finishing while
    another joiner also holds the lock.
11. **Slot table at offset 256, lock-format version 2.** Slot 0 moves off the
    writer word's 128-byte line pair. The version bump makes a stage-1 lock
    file of a still-running process fail validation instead of being misread.
12. **Reap points, as observed in the oracle.** The table below is observed
    by `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs`, which
    SIGKILLs a reader process and then checks whether `mdb_reader_list` still
    shows its slot. Results are identical on macOS aarch64 and Linux x86-64
    and stable over repeated runs. ZeroDB reaps at exactly these points:

    | Event after a reader process died | LMDB reaps? |
    |---|---|
    | Read transaction begin | No |
    | Write transaction commit | No |
    | Another process opening and closing the environment | No |
    | Reader table full (`ReadersFull`) | No: the error is returned, no reap-and-retry |
    | Writer takeover after the previous writer died (Linux; the robust writer mutex reports the dead owner) | Yes |
    | First opener of the environment | Yes: the lock table is reinitialized |
    | Explicit `mdb_reader_check` | Yes |

    On macOS the fork's writer mutex is not robust, so a dead writer blocks
    the next one and there is no takeover point. `mdb_reader_check` counts
    slots, not processes: a process that died holding two reader slots
    reports 2.

13. **`reader_list` reports the owner PID** (`ReaderEntry::pid`). It is
    `None` on the default path, and for a slot being claimed or released.
14. **Claim generation in the owner word (closes a PID-reuse ABA).** The
    slot's owner field is a 64-bit word: the owner PID in the low 32 bits,
    a claim generation in the high 32 bits, which every claim increments.
    Without it, a reaper that read a dead PID P could adopt the slot after a
    new process with the same PID P freed it (amendment 9) and re-claimed
    it, because the word would read P again; the reaper would then free a
    live reader's slot. With the generation, any re-claim changes the word
    and the reaper's CAS fails. LMDB avoids the same race by re-checking the
    PID under its reader mutex; ZeroDB's slot claims are lock-free, so the
    check moves into the word itself. Wrap-around needs 2³² claims of one
    slot while one reaper is paused between its load and its CAS.

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
