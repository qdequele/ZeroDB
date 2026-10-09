# ADR-0023: Multi-process access — lock file, shared reader table, cross-process writer lock

- Status: **Draft** — design agreed with the maintainer in conversation
  (2026-10-09); this written ADR awaits approval before any implementation
  (CLAUDE.md rules 5 and 6).
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

1. The native lock-file name (`zerodb.lock` proposed).
2. Should `ReadersFull` trigger one reap-and-retry? LMDB's behavior is to be
   observed first; the default is to match it exactly.
3. Whether stage 1 lands on `main` behind `NO_LOCK`-by-default until stage 2
   (so the default never ships without reader reaping), or stays on a branch
   until stage 3.
