# SPEC 04 — Transactions & MVCC

Revised 2026-10-05 — docs sweep: snapshot-object field list brought up to the
shipped shape (`last_pg`, free-list annex — ADR-0022), dirty-frame pool noted
(commit 10c98a1).

Revised 2026-10-13 — multi-process access (ADR-0023, stages 1–3): the lock
file is on by default, D-001 is retired, and the rules below that assumed a
single process are amended in place (§0, §2, TXN-6/7/9, §4, TXN-14/16/17/18/19,
TXN-38, TXN-45b, TXN-50/51/53, TXN-56, TXN-66). The lock file's own invariants
are the new §12 (`LCK-1` …).

Status: **DONE** — 2026-07-15 (milestone 0.4). Behavioral source of truth for the
transaction lifecycle (M1.4), the reader table and concurrency (M1.8), nested
read transactions (M1.9), and the write-txn value-borrow contract that every
write-path change (M1.4/1.10) must uphold. Formats are in
[SPEC 02](02-pages.md); tree algorithms in [SPEC 03](03-btree.md); free-page
reclamation in [SPEC 05](05-gc.md); the crash/durability guarantees that the
commit pipeline defined here must satisfy are in [SPEC 06](06-recovery.md).

Clean-room note: the LMDB fork (`mdb.master.nested-rtxns`, SPEC 00 pin) was read
to understand the *algorithms* — reader-slot claiming, `mdb_find_oldest`, the
nested-read patch (ITS#10395), and the commit sequence (AGENTS.md rule 4). The
protocols below are ZeroDB's own design; where a rule mirrors an LMDB idea it is
called out, but no C is transliterated. Binding decisions (not relitigated):
several processes may open one environment through a shared lock file, on by
default (**ADR-0023**; it retired the founding single-process decision D-001),
and `NO_LOCK` (`EnvOpenOptions::lock_file(false)`) keeps the single-process
path with an in-process reader table; nested WRITE txns
unsupported (**D-003**); nested READ txns over the active write txn are a
hot-path MUST with see-uncommitted semantics (M1.9); meta CRC + double-buffer
per SPEC 02 §3; commit ordering encoded in one function with crash hooks
(M1.4).

Normative rules are numbered **TXN-n** so tests and the check tool can cite them.

---

## §0 — Model and vocabulary

- A **transaction** is a consistent view of the database plus (for a write txn)
  a set of pending page mutations. Two kinds exist: a **read txn** (`RoTxn`,
  read-only snapshot) and a **write txn** (`RwTxn`, the single writer).
- A **snapshot** is `(root set, txnid)`: the `main_db`/`free_db` roots plus all
  named-DB roots reachable from a chosen meta, pinned at a fixed `txnid`. A read
  txn reads exactly one snapshot for its whole life; nothing it observes changes.
- **txnid** is a strictly monotonic `u64` (SPEC 02 §2 header stamp; §3 meta
  `txnid`). It only ever increases; ZeroDB does not wrap it (at 1 commit/µs a
  `u64` lasts ~584,000 years).
- **One handle per process, one writer across processes.** There is exactly
  one `Env` per path per process (registry, §7) and at most one live write txn
  per environment at a time, across every process that has it open (§2).
  *(Amended 2026-10-13, ADR-0023; this bullet used to say "single process, no
  lock file", D-001, now retired.)* By default the environment is
  coordinated through a **lock file** next to the data file (§12): it holds
  the published txnid `T`, the reader slots (§4) and the cross-process writer
  lock, so any process may read or write. With `NO_LOCK`
  (`EnvOpenOptions::lock_file(false)`, heed's `NO_LOCK`), or for a
  `READ_ONLY` open on read-only media, there is no lock file: the reader table
  is process memory, and the caller must keep other processes out, as with
  LMDB's `MDB_NOLOCK`. There is no robust mutex in either mode (§2).
- **Dirty page / dirty set.** A write txn's private, not-yet-committed page
  copies. Storage and stability rules are §6 (the value-borrow contract).
- **WithoutTls is the only real mode** (SPEC 00 rows 2/29, SPEC 01 §S7
  `MDB_NOTLS`): reader slots bind to the txn object, not a thread-local, so
  `RoTxn: Send`. The WithTls path is a thin shim (SPEC 00 second table) and adds
  no new semantics here.

---

## §1 — txnid assignment and the meta relationship

- **TXN-1** — At env open the engine selects the live meta (SPEC 02 §3.2) and
  records `last_committed_txnid = meta.txnid`. This is the id of the newest
  durable snapshot.
- **TXN-2** — A new write txn is assigned `writer_txnid = last_committed_txnid +
  1` at `begin` — always, so **an aborted txn's id is reused** by the next
  writer. It is not published to any meta until commit, and reuse is sound
  precisely because no page or meta ever recorded the aborted id. Reuse is not
  merely permitted but **required** by the slot-parity scheme: commit `N`
  writes slot `N & 1`, and that slot is the *older* slot (TXN-63) only when
  committed ids are consecutive — a non-reusing counter would let a post-abort
  commit share parity with the *live* snapshot's slot and overwrite the crash
  fallback. *(Amended 2026-07-16, M1.4: an earlier revision of this rule
  described a never-reusing monotone counter as the assumed implementation;
  that contradicted TXN-63/TXN-67 and is retired — ADR-0004, module note in
  `zerodb-core::rwtxn`.)* The same mechanism serves the `PREV_SNAPSHOT`
  rollback (TXN-67): reopening on `older_txnid` makes the first commit
  `older_txnid + 1` — the id the now-abandoned newer branch used — whose
  same-parity meta write supersedes the abandoned slot in place (SPEC 06
  REC-5), on a branch that has been discarded.
- **TXN-3** — Every page a write txn writes is stamped `page.txnid =
  writer_txnid` (SPEC 02 §2). On commit, the meta slot `writer_txnid & 1`
  (SPEC 02 §3) is written with `txnid = writer_txnid`; that meta becomes the new
  live snapshot (SPEC 06 makes it durable). INV-20 (SPEC 03 §11) then holds:
  every reachable page's stamp `≤` live meta txnid.
- **TXN-4** — A read txn takes `snapshot_txnid = last_committed_txnid` *as
  observed at the instant it pins its slot* (§3), and reads that meta's roots.
  Two read txns opened around a commit may see different snapshots; each is
  internally consistent for its whole life.
- **TXN-5** — `RoTxn::id()` / `RwTxn::id()` (SPEC 00 second table, SHOULD) report
  `snapshot_txnid` / `writer_txnid` respectively. Not used by any Phase-1
  consumer but the ids exist internally regardless (page stamps).

---

## §2 — Single-writer protocol (two-layer writer lock)

- **TXN-6** — `Env::write_txn()` acquires the env's in-process **writer lock**
  (a field on the shared `EnvInner`, §7). The guard is held for the entire life
  of the `RwTxn` and released on commit or abort.
  **Two layers (amended 2026-10-13, ADR-0023 §2).** In a lock-file
  environment the guard then also takes the **cross-process writer lock**
  (LCK-4), always *inside* the in-process one, so threads of one process never
  contend on the cross-process lock. The guard's drop releases the
  cross-process lock first, then the in-process one; both releases are
  thread-agnostic, so `RwTxn: Send` holds in both layers. Without a lock file
  the in-process lock alone serialises writers, as before: the original text
  ("there is no lock file and no cross-process coordination", D-001) now
  describes only that mode.
  **Lock construction (amended 2026-09-09, security review M1):** the lock is
  a **thread-agnostic** `occupied: Mutex<bool>` flag plus a `Condvar`, NOT a
  `Mutex<()>` whose `MutexGuard` lives inside the txn. `RwTxn` is `Send` (and
  heed's `WithoutTls` mode moves write txns across threads), so the release
  may run on a different thread than the acquire — unlocking a
  `std::sync::MutexGuard` from a foreign thread is forbidden by std
  (`MutexGuard: !Send`; the underlying lock may abort on macOS). Acquire
  waits on the condvar until `occupied` is false, then sets it; the guard's
  drop takes the flag mutex briefly *on whatever thread drops it*, clears the
  flag, and notifies — **only if a waiter is parked** (amended 2026-09-25): the
  flag mutex also holds a waiter count, incremented before `wait` and
  decremented after it, so a release that reads zero under the mutex cannot
  miss a thread about to park. On Linux an unconditional `notify_one` is a
  `futex_wake` system call per write txn even when uncontended. Writer-panic
  policy is unchanged: the flag mutex guards
  no txn data (the dirty set died with the unwound `RwTxn`, TXN-60), so a
  poisoned flag mutex is recovered (`PoisonError::into_inner`) rather than
  propagated.
- **TXN-7** — A second `write_txn()` on the same env **blocks** until the current
  writer finishes (mutex contention), matching LMDB's single-writer serialisation
  (LMDB blocks on `me_wmutex`). It does not error. (Consumers hold a write txn
  briefly or structure their own scheduling; ZeroDB does not add a try-lock
  Phase-1 API.)
  **Cross-process wait (amended 2026-10-13, ADR-0023 §2).** In a lock-file
  environment a writer in another process blocks this one the same way, after
  it has the in-process lock: on Linux it sleeps on the writer word's futex
  and, every 100 ms, checks whether the owner process is still alive (LCK-4),
  taking the lock over from a dead owner; on macOS it blocks in a POSIX
  record lock (`F_SETLKW`), which the kernel releases when its owner dies.
  It never errors because another process holds the lock; only an I/O error
  of the lock call or of the takeover's reader reap (LCK-7) fails the begin.
- **TXN-8** — On an env opened read-only (`EnvFlags::READ_ONLY`, SPEC 01 Table 1,
  SHOULD/M1.10), `write_txn()` returns the error mapping of LMDB `EACCES`
  (heed `MdbError` path; §8 error table). No write mutex is taken.
- **TXN-9** — Readers never take the write mutex and never block on it; the
  writer never blocks on readers (it computes the oldest reader lock-free, §4).
  This is the core concurrency guarantee (M1.8): *readers never block, the
  writer never blocks readers.* The published-snapshot **cell** is not the
  write mutex: readers may briefly contend on its bounded O(1) pointer
  operations (one swap per commit, one clone per pin) but never on any part
  of the writer's *transaction* — see TXN-18 as amended (ratified 2026-07-16,
  ADR-0006 Option B) for the precise scope of this guarantee.
  **Lock-file note (2026-10-13, ADR-0023 §4).** The guarantee holds across
  processes. A reader in any process pins against the shared published txnid
  `T` (TXN-17) and claims a slot lock-free (§4.2); it never takes either
  writer lock. A reader that finds `T` ahead of its cached snapshot decodes
  that commit's meta from a `pread` copy (TXN-18), a bounded amount of I/O
  that waits on no writer.

---

## §3 — Read-txn snapshot semantics (pin against the reader table)

A read txn must **pin** its snapshot so the writer's page reclamation (SPEC 05)
never frees a page the reader can still reach. "Pin" is concrete: publish the
snapshot txnid into a reader-table slot such that the writer's oldest-reader scan
(§4) is guaranteed to observe it before the writer could reclaim any page that
snapshot references.

- **TXN-10** — `Env::read_txn()` performs, in order:
  1. **Claim a slot** (§4.2): find a free slot and reserve it.
  2. **Publish-and-verify the snapshot** (the pin, §4.3): repeatedly read the
     **published-snapshot object**'s txnid (TXN-18), publish it into the slot, and
     re-read until the two agree. The agreed value is `snapshot_txnid`.
  3. **Clone the roots from the published-snapshot object, never from the durable
     meta page.** The reader `Arc`-clones the current published snapshot (TXN-18)
     — an immutable `(txnid, roots)` value — and takes its `main_db`/`free_db`
     roots (named-DB roots resolve lazily on first `open_database`, also from this
     object's catalog view). It **must not** re-read a meta *page* after open: the
     meta slot the pinned txnid lived in can be overwritten by a later commit
     (`t & 1` is rewritten by commit `t+2`, TXN-63), so reading the durable meta
     page here would race. The `Arc` keeps the `(txnid, roots)` alive for the
     reader's entire life independent of what the meta slots later become.
  A read txn holds no page locks and copies no pages: it borrows tree data
  directly from the read-only mmap (SPEC 03 §3), valid for the txn's life because
  the pin prevents reclamation (§6) and the published snapshot fixes its roots.
- **TXN-11** — The snapshot is immutable: no operation on a `RoTxn` changes what
  it observes. Concurrent commits are invisible to it. `get`/cursor ops read the
  pinned roots only.
- **TXN-12** — Dropping a `RoTxn` releases its slot (§4.4). After release the
  writer may reclaim pages that only this snapshot pinned. A `'static`
  env-owning read txn (`static_read_txn`, §7) additionally holds an `Env` handle
  and thus keeps the env from closing until dropped.
- **TXN-13** — `RoTxn` is **`Send`** (WithoutTls): a slot is owned by the txn
  object, not a thread, so the txn (and any `&[u8]` it lends) may move to another
  thread (rayon/async). It is **not `Sync`** for mutation purposes; heed's
  borrow model governs sharing. (SPEC 00 row 29.)

---

## §4 — The reader table (lock-free read path)

The reader table is the only place a reader and the writer communicate. In a
lock-file environment (the default, ADR-0023) it is the slot table of the
shared lock file, mapped by every process that has the environment open
(§12); without a lock file it is an array in this process's memory. Both
run the same protocol below. *(Amended 2026-10-13: this section used to
describe an in-process array only, D-001.)* Its job: let the writer
compute the **oldest live snapshot** without blocking readers, and let readers
claim/release a pinned snapshot without blocking the writer.

### §4.1 — Layout and sizing

- **TXN-14** — The table is a fixed-size array of `max_readers` **slots**, sized
  at env open from `EnvOpenOptions::max_readers` (SPEC 00 row 5; Meilisearch sets
  1024). When the caller does **not** set it, the default is **126** (LMDB
  parity: `DEFAULT_READERS`), not an arbitrary number — a differential detail the
  oracle can probe. It is allocated once and never resized during the env's life
  (matching LMDB, whose reader count is fixed at open). With a lock file
  (ADR-0023) the table lives in it and the first opener sizes it; a process
  that joins adopts that size whatever its own `max_readers` says, as an LMDB
  joiner does (`OBS_JOINER_FEWER_MAXREADERS`, `OBS_JOINER_MORE_MAXREADERS`;
  ADR-0023 §1 as amended). A first opener that finds a finished lock file
  from an earlier run never shrinks its table: the size is the larger of its
  `max_readers` and the recorded count, as LMDB
  (`OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS`). Because the slots are
  allocated **eagerly** (one 128-byte slot each, in process memory or in the
  lock file, whose length is `256 + 128 · max_readers` bytes), `max_readers` is
  bounded at open: values above `MAX_READERS_LIMIT` (2^20) are rejected with
  `Io(InvalidInput)` rather than aborting on allocation failure
  (added 2026-09-09, security review M6; `max_dbs` has the same bound,
  `MAX_DBS_LIMIT` = 2^20, for the comparator-registry slots — SPEC 00
  rows 4/5). The bound is checked before the lock file is created
  (amended 2026-10-13).
- Each **slot** is cache-line padded (≥ 64 bytes, aligned) to avoid false
  sharing between a reader touching its slot and the writer scanning the array.
  *(Amended 2026-10-13, ADR-0023 §3.)* Both modes use one slot type,
  `zerodb_core::shared::ReaderSlot`: 128 bytes, 64-byte aligned, a txnid word
  at offset 0 and an owner word at offset 8, the rest atomic padding. The
  lock file stores its slots with exactly that stride (LCK-3). The txnid
  word is the one load-bearing field of the pin protocol:

  | Field | Type | Meaning |
  |-------|------|---------|
  | `txnid` | `AtomicU64` | The pinned snapshot txnid, or a sentinel. This single field encodes both occupancy and the pinned value. |

  Sentinels (values a real txnid can never take):
  - `RDR_FREE = u64::MAX` — slot unoccupied.
  - `RDR_CLAIMED = u64::MAX − 1` — slot reserved by a reader that has not yet
    finished publishing a real snapshot txnid (transient, §4.3).

  A real `snapshot_txnid` is always `< RDR_CLAIMED` (TXN-1: ids start at 1 and
  grow by 1/commit; the sentinel band at the top of `u64` is unreachable) —
  and, against a hostile meta, *enforced*: open refuses `txnid >
  MAX_COMMITTED_TXNID = RDR_CLAIMED - 2^32` (SPEC 06 REC-1a) and `write_txn`
  refuses to start once `base.txnid >= MAX_COMMITTED_TXNID`, so the 2^32 margin
  can never be consumed by commits.
  Occupancy is encoded in `txnid` alone, so the pin protocol needs no
  `pid`/`tid` field (LMDB's `mr_pid`/`mr_tid`). In a lock-file environment
  each slot's 64-bit owner word holds the owner PID (low 32 bits) and a claim
  generation (high 32 bits) (ADR-0023 amendments 7 and 14), which only
  reaping (LCK-7) and `reader_list` read. Without a lock file the owner word
  stays 0.

### §4.2 — Claim (lock-free)

- **TXN-15** — To claim a slot a reader scans the array and, for each slot,
  attempts `compare_exchange(RDR_FREE → RDR_CLAIMED)`:
  - **CAS success ordering**: `Acquire` on success (so subsequent reads of the
    meta happen-after the claim), `Relaxed` on failure (a lost race carries no
    data dependency; just try the next slot). Justification: the claim publishes
    nothing another thread must observe yet — only the later publish (§4.3) is
    cross-thread-ordered — so `Acquire`/`Relaxed` suffices here and avoids a full
    fence on the common (fast) claim.
  - The first successful CAS gives the reader exclusive ownership of that slot.
- **TXN-16** — If **no** slot is free (every CAS failed after a full scan), the
  read txn fails with the LMDB `MDB_READERS_FULL` mapping (heed
  `MdbError::ReadersFull`; §8). This is the observable overflow behavior and
  matches LMDB (`mti_numreaders == env->me_maxreaders → MDB_READERS_FULL`).
  Without a lock file there are no *stale* cross-process slots to reap first
  (LMDB's `mdb_reader_check`), so the error is raised immediately. With a lock
  file (ADR-0023) dead processes' slots can be stale, and the error is still
  raised immediately without reaping, as the LMDB fork does (ADR-0023
  amendment 12, observed); `Env::clear_stale_readers` frees them (LCK-7).

### §4.3 — Publish-and-verify (the pin) and its memory-ordering correctness

The reader now owns a `RDR_CLAIMED` slot but has not yet pinned a real snapshot.
The pin must be correct against a *concurrent committing writer* on a weakly
ordered machine (ARM; AGENTS.md). The hazard is a classic **store→load
reordering**:

```
reader:  store slot.txnid = T ;  load commit_point       (wants: writer sees my T)
writer:  store commit_point = N ; load every slot.txnid  (wants: reader sees new N)
```

If either store→load pair is allowed to reorder, the reader could pin an old `T`
while the writer, not yet seeing `T`, reclaims pages that snapshot `T` needs.
StoreLoad is the one reordering that neither `Acquire` nor `Release` prevents —
only a full fence / `SeqCst` does.

- **TXN-17** — **Reader publish-and-verify loop:**

  ```
  loop:
      t = commit_point.load(SeqCst)        # observe current commit point (TXN-18)
      slot.txnid.store(t, SeqCst)          # publish my pin
      if commit_point.load(SeqCst) == t:   # writer did not advance meanwhile
          break                            # pinned at t
      # else a commit landed between the two loads; retry with the newer t
  snapshot_txnid = t
  ```

  Both the publishing `store` and the two `commit_point` `load`s use
  **`SeqCst`**. Justification (ARM weak model): `SeqCst` places the reader's
  `store slot` and its following `load commit_point` in the single total order
  that `SeqCst` guarantees, and pairs with the writer's `SeqCst` store/scan
  (TXN-19) so the two store→load sequences cannot both "miss" each other. The
  retry loop additionally guarantees liveness: it only spins while a commit is
  actively racing, and each commit strictly advances `commit_point`, so it
  terminates. After the loop the reader clones the published-snapshot object
  (TXN-10 step 3); if that object's own txnid is `> t` (a commit landed between
  the verify and the clone), the reader adopts the newer object and re-stores its
  txnid into the slot — monotone and still a validly pinned, newer snapshot.
  **Lock-file environments (amended 2026-10-13, ADR-0023 §4).** The commit
  point is the shared published txnid `T` in the lock file, not the
  process-local `commit_point`: the loop runs unchanged against `T`
  (`ReaderTable::pin_against`), with the same `SeqCst` accesses, and pairs with
  the committing writer's `SeqCst` store of `T` in whichever process commits.
  The verified `t` is then resolved to a snapshot (TXN-18 below); if meta slot
  `t & 1` has already moved on, the loop re-reads `T` and pins again, the slot
  keeping its last pin meanwhile (conservative for the GC gate, TXN-20). The
  tail's adopt-newer step does not apply: the snapshot is the one for `t`.
- **TXN-18** — **The published-snapshot object.** *(Amended 2026-07-16,
  ratified — maintainer, standing directive, ADR-0006 Option B: the original
  text required an ArcSwap-style lock-free cell "in the reader-table module
  where `unsafe` is sanctioned"; the ratified implementation keeps
  `zerodb-core` `forbid(unsafe_code)` and scopes the no-blocking guarantee to
  its intent — never blocking on the write* ***transaction*** *— with
  LMDB-NOTLS read-open parity as the bar.)* `EnvInner` holds a
  **published-snapshot cell**: the current `Arc<Snapshot>` plus a mirroring
  `commit_point: AtomicU64` equal to the object's txnid. `Snapshot` is an
  **immutable** value `{ txnid, last_pg, main_db, free_db, free-list annex }`
  (the roots and DBRecords of one committed state; the main record doubles as
  the named-DB catalog view). `last_pg` is the snapshot's committed high-water
  — the bound every committed-map resolution enforces (TXN-38) — and the
  free-list annex is the meta's `fl_ids` (format v2, ADR-0022 / SPEC 05 §2a),
  carried **in the snapshot** because the meta slot `txnid & 1` is overwritten
  by txn `txnid + 2` even while this snapshot stays pinned (TXN-63), so holders
  (the next writer's GC-30 carry, `free_page_count`, `copy`) must never re-read
  the slot. Rules:
  - The writer builds a fresh `Arc<Snapshot>` at commit step **C6** (SPEC 04 §9),
    under the write mutex, and publishes it in **this order**: (1) swap the new
    `Arc<Snapshot>` into the cell, then (2) `commit_point.store(writer_txnid,
    SeqCst)` (TXN-19). Because the object is swapped *before* `commit_point` is
    advanced, any reader that observes the new `commit_point` also observes the
    matching (or newer) snapshot object — closing the pin↔roots race.
  - Readers load-and-clone the `Arc` **without ever blocking on the write
    transaction** (TXN-9). The cell may be lock-free or a mutex whose critical
    sections are all bounded O(1) pointer operations (the writer's single swap
    at C6; a reader's clone), never held across I/O, allocation, tree work, or
    any other writer step — so the worst reader wait is another thread's
    pointer op, independent of write-txn duration. (Implementation: ADR-0006
    Option B — the M1.4 `Mutex<Arc<Snapshot>>` cell is thereby ratified as the
    M1.8 cell; the lock-free pin protocol lives entirely in the slot and
    `commit_point` atomics, TXN-17/19/20.) The clone keeps the
    `(txnid, roots)` alive for the reader's life regardless of later commits.
  - The **durable meta page is read only once, at env open** (SPEC 02 §3.2), to
    seed the first `Arc<Snapshot>`. Steady-state reads *never* touch a meta page;
    they use the published object. This is what removes the "read the meta slot
    after open" race entirely. *(This bullet holds without a lock file only.)*
  - **Meta re-read at `T` (amended 2026-10-13, ADR-0023 §4 and amendments 4
    and 5).** In a lock-file environment another process may commit, so a
    meta *is* read after open: when a reader's verified `t`, or a writer's
    base (TXN-56), differs from the txnid of this process's cached snapshot,
    the engine decodes meta slot `t & 1` from a `pread` copy
    (`Backing::read_meta_copy`), never from a slice of the map, because
    another process may be rewriting that slot. A copy that fails its CRC, or
    whose txnid is not `t`, means the slot moved on (`t + 2` overwrote it)
    or was torn; the caller re-reads `T` and retries, a bounded number of
    times, then fails with `MdbError::Invalid`. The decoded snapshot is
    checked against this process's map and the file (SPEC 06 REC-1a; a
    snapshot past the logical map size is `MdbError::MapResized`, TXN-38) and
    then installed in the cell if it is newer than the cached one. When `t`
    equals the cached snapshot's txnid, the cache is used with no decode and
    no check (the single-process fast path). A process that *joins* an
    environment others have open seeds its first snapshot from the meta at
    `T`, never from the newest valid meta, which may belong to a commit that
    is still in flight or that failed.
  - `Env::info()` and `Env::stat()` follow `T` the same way, as LMDB's do
    (`OBS_INFO_SEES_OTHER_COMMIT`, `OBS_STAT_SEES_OTHER_COMMIT`); SPEC 02 §8
    states their fallback.

### §4.4 — Release

- **TXN-18a** — Dropping a read txn stores `slot.txnid = RDR_FREE` with
  **`Release`** ordering. Justification: `Release` ensures the reader's prior
  reads (page dereferences) are complete before the slot is marked reusable;
  pairing with a later claimer's `Acquire` CAS (§4.2) it hands the slot over
  cleanly. A full fence is unnecessary — releasing a slot only needs to not be
  reordered *before* the reader's own accesses, which `Release` provides.
  In a lock-file environment the release is three `Release` stores instead:
  `txnid = RDR_CLAIMED`, then the owner PID cleared in the owner word, then
  `txnid = RDR_FREE` (ADR-0023 amendment 7).
- **TXN-73 (reader count in a lock-file env)** — `EnvInfo::num_readers` is
  LMDB's `mti_numreaders`: the high-water mark of reader slots used by any
  process since the lock file was initialized, never decreasing
  (`OBS_NUMREADERS_OTHER_PROCESS`). The lock file holds no counter for it.
  It is derived from the slots: one past the highest slot whose txnid is not
  `RDR_FREE` or whose owner generation is not 0, or this process's own
  high-water mark if that is larger. Each txnid is loaded with `Acquire`
  before the owner word, so a `RDR_FREE` written by a release (TXN-18a)
  shows the generation stored before it. The derivation relies on three
  invariants, which any change to claiming or slot initialization must keep:
  1. claiming is first-fit (TXN-15, no scan hint or slot affinity), so the
     slots ever claimed are a prefix of the table;
  2. a claimed slot's generation is never 0, because every claim and every
     reaper adoption increments it and a release keeps it (ADR-0023
     amendment 14). The exception is the wrap after 2^32 claims of one
     slot: if the highest used slot has just wrapped and is free, the count
     is one short until it is claimed again;
  3. the initializer wipes every slot to `RDR_FREE` with owner word 0, which
     is LMDB's reset of `mti_numreaders` to 0.

  Without a lock file the count is this process's own high-water mark.

### §4.5 — Oldest-reader computation (writer side) and its correctness

- **TXN-19** — The writer publishes its new commit point at step C6 (SPEC 04 §9),
  **after** the durable meta is written and before it releases the write mutex, in
  two ordered steps: (1) swap the new `Arc<Snapshot>` into the published cell
  (TXN-18), then (2) `commit_point.store(writer_txnid, SeqCst)`. Step (2) is the
  moment new readers begin to see the new snapshot, and is the writer half of the
  pairing in TXN-17. The object-before-counter order guarantees a reader that
  sees the new counter can load the matching roots.
  **Lock-file environments (amended 2026-10-13, ADR-0023 §4).** After step
  (2) the writer also stores `writer_txnid` into the shared `T` (`SeqCst`),
  still holding both writer locks. That store is the moment other processes
  see the commit; it follows the meta write, and its fsync in the durable
  modes (C4/C5), so no process can pin a commit before it is durable. Only
  the holder of the cross-process writer lock stores `T` (besides the lock
  file's initializer, which seeds it once at open, LCK-2). On Linux the
  writer word's `Release` swap at unlock and the next owner's `Acquire` CAS
  order one writer's `T` store and meta write before the next writer's begin;
  on macOS the writer lock is a kernel record lock, whose unlock and lock
  system calls separate the two writers. The oldest-reader
  scan (TXN-20) reads the shared slots with the same `SeqCst` loads.
- **TXN-20** — `oldest_reader()` returns the minimum pinned snapshot the GC must
  respect:

  ```
  oldest = writer_txnid - 1          # no reader can be newer than last committed
  for slot in table:
      v = slot.txnid.load(SeqCst)
      if v == RDR_FREE or v == RDR_CLAIMED:
          continue                   # unoccupied, or mid-pin (see below)
      if v < oldest:
          oldest = v
  return oldest
  ```

  Each slot load is **`SeqCst`**, pairing with TXN-17's publish. Correctness has
  two cases; let the writer be in txn `N`, so `oldest` starts at `N − 1` and GC
  reclaims only pages freed by `F ≤ oldest` (TXN-21, SPEC 05 GC-18).

  1. **Already-pinned readers (real txnid `v`, not a sentinel) are never missed.**
     The reader published `v` with a `SeqCst` store (TXN-17) and the writer reads
     the slot with a `SeqCst` load; both participate in the single `SeqCst` total
     order, and the writer additionally holds the write mutex (the only path that
     advances `commit_point`). So any pin that became visible before the writer's
     scan is seen and folded into `oldest`; the writer cannot reclaim a page
     `F ≤ v` that reader `v` could reach.
  2. **Skipping a `RDR_CLAIMED` slot is safe.** A reader in the `RDR_CLAIMED`
     window has not pinned yet. When it does (TXN-17), it loads `commit_point`,
     which — because the writer only advances it under the mutex and it is
     currently `N − 1` — is observed as `≥ N − 1`; its verify then confirms no
     commit raced. So a skipped mid-pin reader ends up pinned at a snapshot
     `≥ N − 1`. Reclaiming pages freed by `F ≤ N − 1` is safe against any snapshot
     `≥ N − 1`: a page freed by `F` left `F`'s tree and every later tree
     (`≥ F`), so no snapshot `≥ N − 1 ≥ F` references it. The mid-pin reader
     therefore cannot be reading a page this writer reclaims. This is the whole
     correctness argument for the lock-free pin.

  **Release-vs-SeqCst on the release path (TXN-18a) is safe.** Because a reader
  releases its slot with a `Release` store of `RDR_FREE` (not `SeqCst`), the
  writer's `SeqCst` scan may still read the slot's *old* (non-`FREE`) txnid for a
  reader that has just released. This is **conservative**, never unsafe: the
  writer treats a departed reader as still live and merely *holds back*
  reclaiming pages it could otherwise reuse. A stale read can only make `oldest`
  smaller (more cautious), never larger, so it can never authorize reclaiming a
  page a live reader needs. The next `oldest_reader()` (next allocation or next
  txn, TXN-22) observes the freed slot and catches up.
- **TXN-21** — GC reuse (SPEC 05) is gated by `oldest_reader()`: a page freed by
  txn `F` is reclaimable only when `F ≤ oldest_reader()` (SPEC 05 GC-18).
  `oldest_reader()` = `min(smallest live reader table pin, writer_txnid − 1)`,
  with the reader term computed by the TXN-20 SeqCst table scan. *(History:
  pre-M1.8 the reader term came from an **interim mutexed reader registry** —
  ADR-0005 OQ1, approved 2026-07-16, itself superseding an earlier "no
  readers ⇒ `writer_txnid − 1`" placeholder. The M1.8 lock-free reader table
  replaced the registry wholesale — ADR-0006; the gate expression, its only
  consumer, is unchanged.)* Debug builds additionally re-scan the table at
  every GC hand-out and assert `F ≤` every live pin (the M1.8 shadow
  check).
- **TXN-22** — The writer recomputes `oldest_reader()` at most once per
  allocation attempt and may cache it for the duration of a single
  `mdb_page_alloc`-equivalent (SPEC 05); caching only ever makes `oldest` *more*
  conservative (a reader that releases after the cache read is simply not
  reclaimed-against this round), so it is always safe. It must recompute in a
  fresh write txn. *(Implementation choice, ratified 2026-07-16 with ADR-0006
  decision 6: the cache spans the whole write txn — computed by the first GC
  draw, dropped with the `RwTxn`. Sound by the same conservativeness argument;
  a reader that pins mid-txn pins `≥ commit_point = writer_txnid − 1 ≥` the
  cache, so the cache never overshoots a new pin. No refresh-on-gated-miss in
  Phase 1.)*

### §4.6 — Slot lifecycle for the three read-txn shapes

- **TXN-23** — **Plain `RoTxn<'env>`** (SPEC 00 row 14): claims a slot at begin
  (§4.2/4.3), releases at drop (§4.4). Borrows the env by lifetime; cannot
  outlive it.
- **TXN-24** — **`'static` env-owning `RoTxn<'static>`** via
  `Env::static_read_txn()` (SPEC 00 row 15): claims a slot **and** clones an
  `Env` handle (bumps the refcount, §7) which it owns. Releasing the slot and
  dropping the handle both happen at drop; the held handle keeps the env open
  (blocks close, §7) until then. Send, handed to async handlers.
- **TXN-25** — **Nested read txn** (SPEC 00 row 16, §5): **does not** claim a
  reader-table slot. It shares the *write* txn's snapshot (the writer's
  in-progress state), and the write txn already gates GC via `writer_txnid − 1`
  (there is nothing older to protect that the writer isn't already protecting).
  Adding a slot would be redundant and would wrongly pin `writer_txnid` as if
  committed. See §5 for its distinct lifecycle.

---

## §5 — Nested read transactions over a write txn (M1.9, fork semantics)

The Meilisearch fork (ITS#10395, SPEC 00 §A, SPEC 01 §S9) lets a **read-only**
txn be parented to the **active write** txn and see its **uncommitted** state.
milli (5 sites) and hannoy (1 site) open `N = rayon_threads(+1)` of these and
fan them to workers for parallel reads while the write txn is paused. This is a
hot-path MUST. Nested **write** txns remain unsupported (D-003, TXN-40).

### §5.1 — What a nested reader is and sees

- **TXN-26** — `RwTxn::nested_read_txn()` / `Env::nested_read_txn(&wtxn)` opens a
  read-only child of the active write txn. Its snapshot is **the write txn's
  current in-progress view**: `snapshot_txnid = writer_txnid`, and its roots are
  the write txn's *working* roots (the COW roots the writer has produced so far,
  §SPEC 03 §5), **not** the last committed meta. It therefore observes every
  put/del the writer has performed **before the nested reader was opened**.
  - *Implementation note (M1.9, ADR-0007 D2): the child **delegates live** to
    the parent's `TxnRead` rather than copying roots at open — observably
    equivalent, because mutation is impossible from the first child's open to
    the last child's drop (TXN-29/30), so the parent's roots/open-table/dirty
    set are constant across every child's lifetime and all children of one
    paused window see the identical state.*
- **TXN-27** — A nested reader reads through the writer's **dirty set** for any
  page the writer has copied, and through the read-only mmap for pages the writer
  has not touched. Reading a dirty page is subject to the value-borrow contract
  (§6): the nested reader borrows `&[u8]` out of the writer's dirty-page storage.
- **TXN-28** — Multiple nested readers may be live **concurrently** (arbitrarily
  many, matching the fork's `mt_rdonly_child_count`). Each is independent and
  `Send` (WithoutTls), so each can move to a distinct rayon worker.

### §5.2 — The derived safety contract (writer paused while children live)

The fork's C does not itself pause the writer; Meilisearch's usage always
quiesces the writer while the nested readers run. ZeroDB must **enforce**
quiescence, because Rust aliasing + the value-borrow contract make concurrent
writer mutation unsound: a nested reader holds `&[u8]` into a dirty page while a
worker on another thread reads it; if the writer split/freed/reallocated that
page concurrently, the borrow would dangle and the read would race.

- **TXN-29** — **While any nested read txn is live, the parent write txn MUST NOT
  mutate.** No `put`, `del`, `put_current`, `del_current`, `clear`, page
  allocation, or COW may occur on the parent between the opening of the first
  nested reader and the drop of the last. The dirty set is **frozen** for that
  window.
  - **Runtime guard.** Every mutating `RwTxn` op (each `&mut self` method: `put`,
    `put_with_flags`, `put_reserved`, `del`, `delete_range`, `clear`,
    `put_current`, `del_current`) asserts `child_count == 0` on entry, in **both**
    debug and release builds. If a nested reader is somehow live (an `unsafe`/FFI
    path that bypassed the borrow checker), the op returns
    **`MdbError::BadTxn`** (ADR-0007 Q4, ratified 2026-07-16 — LMDB's own
    `MDB_BAD_TXN` is "transaction … has a child", the parity-adjacent choice)
    rather than mutating frozen-but-aliased pages. This backstops TXN-30's
    compile-time guarantee. *(M1.9 implementation note: the guard lives in the
    single `guard_ok` funnel every mutating entry passes through, plus
    `ensure_open` — which mutates the open-table children read via
    `record_for` — and commit C0.)* This quiescence is a **zerodb** soundness
    rule the fork does not itself impose — **D-005 (APPROVED)** in
    `docs/DIVERGENCES.md` (not observable to any consumer, which always pause
    the writer; see §10 conflict block).
- **TXN-30** — Enforcement is primarily **compile-time**: `nested_read_txn(&wtxn)`
  borrows the `RwTxn` **immutably** (`&self`). All mutating `RwTxn` ops take
  `&mut self` (heed's model: `get` = `&Txn`, `put` = `&mut Txn`). While any
  nested reader (holding a shared `&wtxn` borrow, directly or captured in a
  rayon closure) is alive, the borrow checker forbids a `&mut wtxn` call. The
  fan-out pattern joins all workers (ending the shared borrows) before the writer
  resumes; the join is where the borrow is released.
- **TXN-31** — For `Send` fan-out the nested reader carries a lifetime `'t` tied
  to `&'t RwTxn`; it cannot outlive the write txn (compile-time). A runtime
  **live-child counter** on the write txn (incremented at open, decremented at
  drop) additionally guards the internal invariant and lets `commit`/`abort`
  assert `child_count == 0` (TXN-33), catching any `unsafe`/FFI-boundary misuse
  that bypassed the borrow checker. The counter is an **`AtomicUsize`**
  (ADR-0007 D3, ratified 2026-07-16; the `Cell` alternative in the original
  text is struck — a `Cell` would destroy `RwTxn: Sync`, which the `Send`
  child derivation depends on, and children really do decrement from worker
  threads at all six consumer call sites): open `fetch_add(1, Relaxed)` (the
  opener holds `&RwTxn`; the child reaches its worker through the spawn/send
  happens-before edge), drop `fetch_sub(1, Release)`, every writer-side check
  `load(Acquire)` — so all child reads happen-before the writer mutates or
  commits. Loom-checked as **L6** (`zerodb-core/src/nested.rs`); note the L6
  mutation-check record there: an all-`Relaxed` weakening is load-buffering-
  shaped and NOT loom-detectable — the `Release`/`Acquire` pair is normative
  per this clause, guarded by the per-site comments, not by loom.
- **TXN-32** — Because the writer is frozen (TXN-29), a nested reader needs **no
  reader-table slot** (TXN-25): the writer performs no allocation while children
  live, so there is nothing for a slot to gate. GC sees only `writer_txnid − 1`
  as before.

### §5.3 — Lifecycle and restrictions

- **TXN-33** — `RwTxn::commit()` / `abort()` require `child_count == 0` (all
  nested readers dropped). Because of TXN-30 this is normally guaranteed by the
  borrow checker; the **commit** path carries the runtime check (C0, TXN-58) as
  defense-in-depth. The **abort/drop** path deliberately carries no runtime
  check *(amended 2026-07-16, M1.9 — ratified under the maintainer's standing
  directive; ADR-0007 post-implementation notes)*: a live child at parent-drop
  is unrepresentable in safe Rust, and a nonzero count at drop can only mean
  `mem::forget(child)` — which consumed the child, aliases nothing, and is
  harmless; a drop-side assert would false-positive on exactly that case while
  the FFI-misuse case it could catch is already covered by the per-`&mut`-op
  TXN-29 guard. A nested reader thus **must not outlive its parent** — enforced
  by lifetime, asserted by counter at commit and on every mutation.
- **TXN-34** — A nested reader is strictly read-only; it exposes the `RoTxn`
  read API only (`get`, cursors, `len`, …). Any attempt to obtain a write txn or
  nested write txn from it is a type error — no such API exists (TXN-40 as
  amended, D-003).
- **TXN-35** — Opening a nested reader while the env is in `WRITE_MAP` mode
  (SPEC 01 §S7) is permitted: the nested reader reads dirty bytes straight from
  the writable map instead of a heap page (§6.4), same borrow contract. (The fork
  blocks only writemap nested *write* children; read children are fine.)
- **TXN-36** — Oracle parity target (M1.9): randomized
  write-then-open-nested-read-then-read sequences, including reads of uncommitted
  state, and replays of the milli/hannoy fan-out, must match the fork observed
  through heed. A nested-**write** attempt is **unrepresentable in the API**
  (TXN-40 as amended): the D-003 acceptance is carried by API absence — a
  stronger form of "clean error" — not by a runtime test.

### §5.4 — Relationship to Phase 3.8 snapshot()

Phase 3.8's `RwTxn::snapshot()` (a *last-committed* view, not the in-progress
one) is the cleaner sibling and is designed against the same 6 call sites, but it
is **not** a Phase-1 substitute: it would change what the workers observe
(committed vs uncommitted). Phase 1 implements the fork's uncommitted-view
semantics exactly (TXN-26). No Phase-3.8 seam is opened here beyond leaving the
6 call sites documented.

---

## §6 — The write-txn value-borrow contract (Rust soundness)

This is the likeliest soundness bug outside the reader table (M0.4/1.4). A
`get` during a write txn may return bytes from a **dirty page in heap memory**
(default mode) or from the **writable mmap** (WRITE_MAP) — not the read-only
map. The contract below defines exactly when a borrow is valid and how the
dirty-page store must be built so it is.

### §6.1 — Where borrowed bytes come from

- **TXN-37** — `RoTxn::get` / cursor reads borrow `&'txn [u8]` from the
  **read-only mmap**. Validity: the whole txn life, because (a) the mmap is never
  unmapped or moved while any txn is live (§7 close is deferred), and (b) the
  reader's pin (§3) stops the writer reclaiming the pages. Nothing a `RoTxn` can
  do invalidates its own read borrows.
- **TXN-38** — `RwTxn::get` / cursor reads return bytes from **whichever storage
  currently backs the page**:
  - a page the write txn has **not** touched → borrowed from the read-only mmap
    (as TXN-37);
  - a page the write txn **has** dirtied (COW copy or freshly allocated) →
    borrowed from the **dirty-page store** (heap, §6.3) or the **writable mmap**
    (WRITE_MAP, §6.4).
  A caller cannot tell which; the borrow lifetime rules (§6.2) are identical for
  both so the distinction is invisible and safe.
  **High-water bound (added 2026-09-09, security review H1):** every
  committed-map resolution — a reader's, or the writer's map fallback — refuses
  a pgno above the pinned snapshot's `last_pg` with a typed error
  (`PageError::PageOutOfBounds`), and clamps multi-page (overflow-run) slices
  at that bound. The mapping covers the full `map_size` (ADR-0004 D4), so map
  bytes past the real file end are unbacked: a corrupt/hostile reference there
  must fail typed, not SIGBUS (SPEC 06 REC-14). Dirty frames are exempt — a
  writer legitimately allocates pages beyond its base snapshot's `last_pg`,
  and those resolve from the dirty store before the bound is consulted.
  **Page-validation policy (added 2026-09-28, ADR-0014):** by default a
  map-sourced tree page is fully validated (every cell) the first time a txn
  views it, and re-wrapped without checks on later views through the
  txn-scoped validated-pages memo. An env opened with the trusting
  `FileTrust` policy skips the cell walk for map pages: on a memo miss they
  take the O(1) header checks dirty frames take, then enter the memo (page type, reserved fields,
  free-space bounds). The high-water bound above and the free-list id checks
  (SPEC 05 GC-18) apply under both policies. Under the trusting policy an
  overflow value is sliced from its head page without reading the run's header
  (ADR-0014 amendment, 2026-09-29); the slice stays bounded by the high-water. Under the
  trusting policy a corrupt page is undefined behaviour, as in LMDB; the
  policy is the caller's `unsafe` contract (SPEC 00, `file_trust`).
  **Cross-txn validation cache (added 2026-09-29, ADR-0018):** under the
  validating policy a txn-memo miss on a map-sourced leaf or branch first
  probes an env-wide cache keyed by **(pgno, page kind, header txnid stamp)**.
  An exact hit takes the zero-check view; a miss runs the full cell walk and,
  on success, records that key — **readers only**. Either way the page enters
  the txn's memo. Plain read txns use it, borrowing it from the env (opening
  a txn touches no shared refcount). Write txns — and the nested read txns
  that share their parent writer's memo — **probe the cache but never publish
  from the miss arm** (ADR-0018 amendment, 2026-10-01): a writer's miss-arm
  publish could record a non-final image of its own spilled page (TXN-69
  rewrites a spilled pgno in place under the same stamp), and an aborted
  writer's publishes would survive into its reused txnid (TXN-2). A writer
  probe cannot false-hit, because the cache holds only successfully committed
  stamps, all strictly below the live writer's txnid. Env-owning
  `static_read_txn`s do not use the cache, and dirty frames never do.
  **Commit seeds the cache** (same amendment): after commit step C5 — the
  commit is irrevocable and the txnid consumed forever — and before C6, the
  committer publishes `(pgno, kind, txnid)` for every leaf or branch frame of
  the committed dirty set — written at C2, or already realized **in place**
  in the map under TXN-45b (§6.4), where C2 writes nothing for it
  (engine-authored, stamped with the committing txnid, final either way;
  amended 2026-10-02, ADR-0021 m1). Failed
  or aborted commits publish nothing (their txnid is reused, TXN-2); pages
  still spilled at commit are not published (their mid-txn image was
  rewritable in place) and revalidate on first view; trusted-mode envs skip
  the seeding. Soundness
  rests on the **page-version identity**: ZeroDB never exposes two byte images
  of one pgno under the same stamp, because every page a commit writes
  carries the committing txnid (COW copies are restamped, fresh pages are
  initialized with it), each pgno is written at most once per commit, a
  pgno is only reused by a later txn, and aborted or failed commits leave no
  page reachable from a published snapshot. A reused page therefore carries a
  newer stamp and misses; no invalidation is needed. The cache starts empty
  at open, so a corrupt or hostile file is still validated on its first view.
  **Several processes (amended 2026-10-13, ADR-0023 §5).** The cache is per
  process and stays on in a lock-file environment. Its entries come from
  this process's own committed dirty sets and from its readers' successful
  validations of pages reachable from a published snapshot, which may be
  another process's commit. The page-version identity above holds across
  processes, because every process commits under the same rules and only
  one writes at a time (TXN-6): a page another process rewrote was reused by
  a later txn and carries that txn's newer stamp, so it misses. A failed
  commit's txnid is reused by the next writer, in any process, but the
  failed commit published no `T`, so no reader in any process reached its
  pages, and no cache holds its stamps. The original text said the cache
  assumes the file changes only through this process's commits (D-001);
  it now assumes the file changes only through ZeroDB commits.
  The table is fixed-size (allocated in chunks as publishes land) and
  direct-mapped by pgno; each slot is a seqlock, so a
  lookup racing a publish or a collision only misses (loom model
  `loom_stamp_cache_never_mixes_publishes`). Tests:
  `zerodb/tests/page_version_identity.rs` (the identity over churn with
  overflow values, named DBs, aborts and delayed reuse; short read txns
  against a model), `stamps::tests`,
  `btree::tests::stamp_cache_serves_the_exact_page_version_across_txns`.
  **Snapshots past this process's map (added 2026-10-13, ADR-0023 amendment
  15).** In a lock-file environment another process with a larger map may
  commit pages beyond this process's logical map size. A read or write txn
  begin whose snapshot needs `(last_pg + 1) · page_size` bytes beyond the
  logical map size fails with `MdbError::MapResized` before any page is
  resolved, as LMDB's begin returns `MDB_MAP_RESIZED`
  (`OBS_READ_BEGIN_SMALL_MAP`, `OBS_WRITE_BEGIN_SMALL_MAP`); a larger
  `map_size` in the meta alone is not an error
  (`OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES = false`). The caller answers with
  `Env::resize(0)` and begins again. So a committed-map resolution never
  needs a page outside this process's mapping. The check is SPEC 06 REC-1a's;
  a write begin applies it to its base even when the base came from the
  cache. The read fast path (the cached snapshot is current) needs no check:
  mappings never shrink, and `resize` never sets the logical size below the
  newest published snapshot, so every cached snapshot still fits.

### §6.2 — Which operations invalidate which borrows

- **TXN-39** — A borrow `&'txn [u8]` returned by a read on a `RwTxn` (or by a
  nested reader on that txn, §5) is valid until the **next mutating operation on
  that write txn**, and no longer. Mutating operations are exactly those taking
  `&mut RwTxn`: `put`, `put_with_flags`, `put_reserved`, `del`, `delete_range`,
  `clear`, and the write-cursor ops `put_current`/`del_current`. Any one of them
  may split, merge, COW, reallocate, or free pages (SPEC 03 §5–§10), which can
  move or invalidate the bytes behind an outstanding borrow.
  - **heed's type model already enforces this at compile time.** `get`/cursor
    reads borrow `&self`; mutations take `&mut self`. The borrow checker forbids
    holding a read borrow across a mutation of the same txn. So a well-typed
    consumer *cannot* observe a dangling borrow; the contract restates what the
    types guarantee and pins the obligation on the engine to *honor* it
    (i.e. never move dirty bytes *within* a single `&self` borrow, §6.3).
  - The write-cursor ops are `unsafe` in heed precisely because they can be
    called while an FFI-style raw pointer into the entry is held; ZeroDB's safe
    surface upholds TXN-39, and the `unsafe` ops carry the SPEC 03 §7 rule "no
    live borrow of the current entry may span the call."

### §6.3 — Dirty-page store: the stability guarantee (default heap mode)

- **TXN-41** — The dirty-page store MUST guarantee that a page's backing memory
  **never moves or is reallocated while a borrow into it is live**. Concretely,
  the store is **not** a `Vec<u8>` / growable arena that can reallocate. It is a
  collection of individually stable frames indexed by pgno through a map
  (`HashMap<pgno, FrameHandle>` or a radix/BTree index). Two frame shapes:
  - **Tree pages** (leaf/branch/GC) each use a fixed **`psize`-byte** frame (e.g.
    `Box<[u8; psize]>` or a page-frame pool of boxed frames that are handed out
    but never moved). One frame = one page.
  - **Overflow runs** use a single **contiguous `N * psize`-byte** frame for the
    whole `N`-page run (`N = ovf_pages`, SPEC 02 §5), **not** `N` separate
    per-page frames. Contiguity is mandatory: BIGDATA values are read zero-copy as
    one `dsize`-byte slice spanning the run (SPEC 03 §3), and `put_reserved`
    (TXN-47) hands the caller one contiguous `&mut [u8]` into a run — both require
    the run's bytes to be adjacent in memory. The run frame is stable and never
    moved for the txn's life, exactly like a tree frame.

  Adding a new dirty page or run allocates a **new** frame; it never disturbs the
  address of any existing frame. The per-`psize` rule (above) governs tree pages;
  overflow runs are the contiguous-frame exception.
  *(Realization note — frame pool, commit 10c98a1, PERF-GAP B3/B12; LMDB's
  `me_dpages`.) One-page frames are recycled: within a txn a freed/spilled
  frame parks on a bounded spare list, and at txn end up to `SPARE_CAP` (256)
  spares ride back to an env-level pool that seeds the next write txn's store.
  Reuse happens only at `&mut` boundaries within a txn (TXN-43) and across txn
  ends otherwise, so the stability guarantee above is untouched; run frames and
  in-map frames (TXN-45b) are never pooled.*
- **TXN-42** — COW (SPEC 03 §5) allocates a fresh frame, `memcpy`s the source
  page into it, and inserts it; it never edits a frame's backing identity.
  Editing an already-dirty page (SPEC 03 §5 rule 2) writes in place within its
  existing frame — allowed under TXN-39 only because such an edit is itself a
  `&mut` op, so no read borrow is outstanding.
- **TXN-43** — A page freed **within** the txn keeps its frame allocated until
  the txn ends (or until a later `&mut` op legitimately reuses it via the
  loose-page path, SPEC 05 GC-8). Because reuse happens only at a `&mut`
  boundary (no borrow outstanding, TXN-39), reusing a freed frame for a new page
  is sound. The store must not `Drop` a frame while a `&self`-scoped borrow into
  it could still be live — which TXN-39 already precludes. Spilling (§6.3a)
  also releases frames, likewise only at a `&mut` boundary.
- **TXN-44** — Growth of the *index* (the pgno→frame map) may reallocate the map
  itself; that is fine — the map stores handles/pointers to frames, and moving a
  pointer does not move the pointee. Only frame *contents* addresses are
  load-bearing for borrows.

### §6.3a — Bounded dirty memory: spilling (ADR-0017, added 2026-09-30)

A write txn's dirty set is bounded, as LMDB bounds it (`mdb_page_spill`): when
it grows past the env's **dirty limit**, part of it is written to the file
early and its frames are released. (Under in-place WRITE_MAP, TXN-45b, the
same contract holds but the "write" is a no-op per in-map frame — see the
spilling bullet there.)

- **TXN-68 (limit, trigger)** — The dirty limit is counted in pages (an
  overflow run counts its `N` pages). Default: LMDB's `MDB_IDL_UM_MAX` =
  131,072 pages; `EnvOpenOptions::max_dirty_bytes` (ZeroDB extension, SPEC 00)
  sets it to `max(bytes / psize, 128)`. The check runs where no descent path,
  view or borrow into a frame is live: at the start of every page-mutating
  `Database` / `RwCursor` call and of `create_database` (after the TXN-29 child
  guard), and at the top of each leaf step of `delete_range` (which keeps no
  path across steps). Commit-internal writes (C1a/C1) never spill: commit
  writes every frame anyway. At each check, if `dirty_pages + NEED > limit`
  (`NEED` = 64, a fixed estimate of one op's worst-case touches, as LMDB's
  `need` estimate), the txn spills. An op may still overshoot the limit by its own pages (a large
  value's run); the next op's check brings it back.
- **TXN-69 (what is spilled)** — At least `max(NEED, limit / 8)` pages (LMDB's
  1/8 rule), taken from the dirty frames with the **highest** pgnos first (LMDB
  flushes its pgno-sorted dirty list from the tail), skipping each open tree's
  root page (main, GC, open named DBs) and every page a rightmost-leaf finger
  names (SPEC 03 §6.6) — the pages about to be touched again. The chosen
  frames are written at their pgnos with the commit's page writer (same
  batched writes as C2, no fsync), then removed from the store and their pgnos
  recorded as **spilled** (with their page count). A failed write marks the
  txn errored (only abort remains); frames leave the store only after their
  write succeeded.
- **TXN-70 (what may be written)** — A spilled page is a dirty page, so it
  satisfies TXN-62: it lies past the committed high-water or was reclaimed
  under the oldest-reader gate, and no live snapshot references it. Spilling
  changes when those pages reach the file, not which pages do. The meta is
  written only at commit (C4); a crash or an abort after a spill leaves the
  spilled pages as unreferenced bytes (SPEC 06 REC-6 H0 as amended). Nothing is
  undone on abort; the file may keep a grown, unreferenced tail, as with LMDB.
- **TXN-71 (reading a spilled page)** — The writer's source resolves a spilled
  pgno from the map, through the ordinary map path: its TXN-38 high-water bound
  is the committed high-water **raised past the highest page any spill of this
  txn has written** (`read_high`). The spill wrote that page, so the file backs
  every page up to the bound (never-written pages in between read as zeros),
  and a stray reference into that range fails with a typed validation error,
  never a fault (under `WRITE_MAP` the whole map is backed anyway). A spilled
  page's map bytes change when a later spill rewrites it — and, under the
  in-place realization (TXN-45b, ADR-0021), at the in-place stores of this
  txn's own dirty frames (allocation/edit time) — and **every spill resets
  the writer's validated-pages memo**, so a memo entry never outlives the
  bytes it vouched for. **Writer view re-derivation (amended 2026-10-02,
  ADR-0021 B2/M1):** the writer's whole-map `&[u8]` read view is re-derived
  from the backing at every spill — a view predating a page's write must
  never serve it (under Stacked/Tree Borrows the write invalidates the stale
  borrow at exactly the written locations) — and under the in-place
  realization the view is not cached at all but borrowed lazily per access,
  like a reader's (TXN-45b writes happen mid-txn, and under Stacked Borrows
  even *copying* a stale whole-map reference after one is undefined behavior
  at the written locations; caught by miri on the heap-backed test map,
  `zerodb-core/tests/writemap_in_place_miri.rs`). The resolution hot path is
  unchanged
  (PERF-GAP B13: an extra spilled-page lookup inlined into every descent cost
  read rungs 3–13 %). Nested read children read through the same source; no
  spill runs while one is live (TXN-29 guard precedes the check).
- **TXN-72 (writing a spilled page again)** — Touching a spilled tree page
  (COW first-touch, SPEC 03 §5.1) copies its bytes from the map into a fresh
  frame **at the same pgno** and clears its spilled mark (LMDB
  `mdb_page_unspill`); the page already carries this txn's id, so there is no
  new pgno, no parent-pointer rewrite and no free. Freeing a spilled page or run
  clears its mark and classifies it as loose, like any page this txn
  allocated. Commit C2 writes the remaining frames only: spilled pages are
  already on the file with their final bytes. A txn whose only remaining change
  is spilled pages is not "unchanged" (the commit proceeds).

### §6.4 — WRITE_MAP variant of the contract

- **TXN-45** — Under `EnvFlags::WRITE_MAP` (SPEC 01 §S7) dirty bytes live in the
  **writable mmap** at the page's on-disk address, not in a heap frame. The
  borrow contract is **identical from the borrow checker's view** (TXN-39), but
  the backing is the map:
  - The mmap base address MUST NOT change while any borrow is live. Map growth
    (remap to a larger size on file growth) is a mutating event and may only
    happen at a `&mut` boundary; it invalidates all outstanding borrows exactly
    like any other mutation (TXN-39), which is sound because none can be live
    across a `&mut` call.
  - `get` on a dirtied page returns a slice straight into the writable map
    (zero-copy, as the fork does). No heap frame is involved; there is nothing to
    keep stable beyond "do not remap mid-borrow."
- **TXN-46** — The engine MUST treat writemap and heap-mode dirty storage
  **uniformly** at the API/borrow level (SPEC 01 §S7): the same `get` signature,
  the same TXN-39 invalidation points. Only the internal backing differs.

- **TXN-45a — Phase-1 realization (amended 2026-07-16, M1.10).** The
  implementation realizes `WRITE_MAP` as a **commit-time write strategy**, not
  live-map mutation during the txn: dirty bytes live in the heap dirty-page
  store (§6.3) for the whole write txn — identical to the default mode — and are
  copied into the writable map at commit **C2** (`WriteMapBacking::write_at_page`
  = `memcpy` into the map), then made durable by `msync` at C3/C5 (SPEC 06
  REC-12). Rationale and consequences:
  - **Observably identical** to the fork's live-map writes through the heed /
    oracle surface: same `put`/`get`/`put_reserved`/iteration results, same
    durability. `env_writemap_*` differential tests (M1.10) confirm parity.
  - **Preserves every invariant unchanged:** the value-borrow contract (§6.3),
    nested-reader reads of dirty pages (§5), and abort-by-drop all operate on the
    heap store exactly as the default mode — so `WRITE_MAP` needs **no map
    `unsafe` in `zerodb-core`** (the writable-mmap `unsafe` is confined to
    `zerodb-io`, per the AGENTS.md unsafe policy) and the M1.4 miri coverage
    (§6.6 / TXN-49) covers writemap's during-txn path for free.
  - The writable map covers the full `map_size` and the file is `set_len` to
    `map_size` at open (matching the fork's `ftruncate`-to-mapsize under
    writemap) so a store to any mapped page never faults past EOF (ADR-0004 D4).
    *(Amended by ADR-0023 amendment 15, decision 4.)* The file only ever grows,
    and only under both writer locks while other processes may be alive: at
    open only when this process is alone (no lock file, or the lock file's
    initializer); in `Env::resize`; and at every write-txn begin, after both
    writer locks are taken, which grows the file to this process's logical
    `map_size` when it is shorter (an `fstat` re-read, then `set_len`; one
    cached comparison when it is already long enough). A joining process maps
    its logical size even past the file's end; it writes through the map only
    inside a write txn, after that begin-time growth, and readers touch only
    pages a committed snapshot references, which lie inside the file.
  - True zero-copy **live-map mutation during the txn** is the TXN-45b
    realization below (ADR-0021). TXN-45a remains the realization for every
    backing that does not broker map slices: the default mode, the miri test
    backing, and the fault-injection backend (whose crash journal records
    fd-path writes only). Neither realization is a `docs/DIVERGENCES.md`
    entry — both are observably identical to the fork through the heed /
    oracle surface.

- **TXN-45b — in-place realization (ADR-0021, accepted 2026-10-02; spike, then production hardening, merged PR #88).**
  When the backing brokers mutable map slices (`Backing::dirty_in_map`,
  implemented by the writable-map backing only), the write txn realizes every
  dirty frame **in the writable map at the frame's own page number**: COW
  first-touch copies the source page straight into the map at the fresh pgno,
  new tree pages and overflow runs are formatted in the map at their final
  offsets, and in-frame edits (TXN-42 rule 2) mutate the map bytes directly.
  Commit C2 therefore writes nothing for these frames; C3's `msync` is the
  barrier that makes the in-place stores durable, and C4/C5 are unchanged.
  Rules and consequences:
  - **Reader safety is TXN-62's, applied earlier.** Every in-place store
    targets a pgno this txn **allocated** (beyond the committed high-water, or
    GC-reclaimed under the oldest-reader gate), which no live snapshot — and
    no nested read child of an older state — references. This is the same
    invariant spilling relies on (TXN-70); in-place merely moves the write
    from C2/spill time to allocation/edit time. Crash at any point before C4:
    the live meta `N-1` references none of the written pages, so the
    recovered store is byte-equivalent to `N-1` (SPEC 06 REC-6 H0/H1 hold with
    "nothing written" relaxed to "only unreferenced pages written", exactly as
    §6.3a already relaxed it for spills).
  - **The borrow contract (TXN-39/41/42/46) is unchanged.** A map frame's
    address is stable for the txn's life: a mapping is never remapped in
    place, and `Env::resize`, which adds a longer mapping beside it, refuses
    to run while a write txn of the env is live (ADR-0023 amendment 15;
    within a txn, growth past the map is `MapFull`). Earlier mappings stay
    mapped until the env closes. Shared views of a dirty
    frame are tied to `&txn`, mutable views to `&mut txn`, exactly as heap
    frames are. The brokered `&mut` slices are created transiently inside
    `&mut` ops of the dirty store and never stored.
  - **Abort = don't advance the meta.** An aborted (or crashed) txn leaves
    its in-place stores as unreferenced bytes at TXN-62 pgnos: free-list
    pages whose *contents* no committed tree references. The next txn
    re-draws them from the GC as usual (their prior contents never mattered
    for a free page). Nothing is unwound; the file may keep grown,
    garbage-filled unreferenced pages — LMDB's own `MDB_WRITEMAP` abort
    behavior, and the same end state §6.3a already sanctions after an
    aborted spill.
  - **Spilling (§6.3a) degenerates to bookkeeping.** The dirty limit and the
    TXN-68..72 observable contract stand, but a "spill" of an in-map frame
    writes nothing (the bytes already sit at their final offset): the frame
    is released from tracking and the pgno marked spilled; unspilling
    re-tracks the same map region with no copy (TXN-72's "copies its bytes
    from the map into a fresh frame at the same pgno" is satisfied
    degenerately — the frame *is* the map region). The kernel, not the
    engine, bounds in-map dirty memory (dirty file-backed pages are evictable
    page cache).
  - **The validated-pages memo (TXN-38/71) stays sound** because a map page's
    bytes now change during the txn **only at dirty pgnos**, and dirty pgnos
    are resolved from the dirty store before the memo is consulted; spills
    still reset the memo (TXN-71).
  - **The split scratch copy.** Re-packing a page at its own pgno while
    reading its pre-split image (the general split) copies the in-map frame
    out to a heap scratch first — the map region cannot be both source and
    destination. One page copy per general split, as LMDB's split makes under
    `MDB_WRITEMAP`.
  - **unsafe containment (AGENTS.md policy; hardened 2026-10-02, ADR-0021
    B1).** The broker is an **`unsafe fn`** end to end
    (`MmapWritable::slice_mut` → `Backing::map_dirty_page`): no safe
    function mints `&mut [u8]` from `&self`. The sole sanctioned caller is
    `zerodb-core::dirty::map_mut`, whose `SAFETY` block discharges the
    contract — single writer (TXN-6), TXN-62 target, exactly one live
    `&mut` per region tied to `&mut DirtyStore` and never stored, and the
    whole-map read view re-derived per spill / borrowed per access
    (TXN-71 as amended).
  - **The writer's whole-map view is lazy.** Because in-place stores mutate
    map bytes mid-txn, the write txn holds **no** cached whole-map `&[u8]`
    in this mode; `get`/descent resolutions borrow the view per access, as
    readers do (TXN-71 as amended; ADR-0021 M1 — under Stacked Borrows a
    cached reference field is re-asserted on every move of the txn, which
    is UB at in-place-written locations).
  - **Release-mode TXN-62 guard (ADR-0021 M2).** Every pgno the allocator
    returns in this mode is checked `pgno > committed_last_pg ∨ pgno ∈
    reclaimed` (each page of a run) **in release builds**, failing with a
    typed error that marks the txn errored — an allocator bug must surface
    as an error, never clobber committed data through the map. (The default
    mode keeps the C2-time `debug_assert`, whose failure there corrupts
    nothing committed until C2 and is caught by the crash/image harness.)
  - **Crash coverage (ADR-0021 B3).** The fault-injection backend forwards
    the broker and journals the brokered regions (bytes resolved at `sync`
    seal points and at capture), so the image mechanism's torn/dropped/
    reordered fates exercise the in-place realization — SIGKILL alone
    cannot (the page cache survives it). Under `MAP_ASYNC` each flush seals
    that commit's final region bytes, preserving the per-commit versions
    the ordered-writeback sub-model (SPEC 06 REC-11) compares against.

### §6.5 — put_reserved / ReservedSpace rules

- **TXN-47** — `Database::put_reserved(txn, key, len, f)` (SPEC 00 row 35,
  SPEC 01 §S3, `MDB_RESERVE`) allocates the value slot inside the dirty leaf (or
  overflow head) and hands the closure `f` a `&mut ReservedSpace` — a mutable
  slice **into the dirty page** (heap frame or writable map per mode). Rules:
  - The closure MUST fully write the reserved bytes before it returns.
  - The reserved slice is valid only until the **next** op on the txn (TXN-39);
    it MUST NOT be stashed and used after any subsequent mutation that could
    move/split/spill/free the page.
  - The engine MUST NOT zero the reserved region (SPEC 01 §S3: parity with
    writemap peek; the caller sees uninitialized-but-owned space). This differs
    from a normal `put`, which copies the caller's value.
- **TXN-48** — After the write txn commits or aborts, any `ReservedSpace`-derived
  pointer is invalid (the frame/map page may be reused or the txn's frames
  dropped). This mirrors SPEC 01 §S3's "after commit/abort the pointer is
  invalid."

### §6.6 — miri obligations (M1.4)

- **TXN-49** — `cargo miri test -p zerodb-core` MUST exercise, on the **heap
  dirty-page store** (writemap uses real mmap and is not miri-able — a heap-backed
  shadow store stands in under `cfg(miri)`):
  1. **get-then-put**: obtain `v = get(k)`, then `put(k2, …)`; assert the earlier
     `v` was consumed *before* the put (compile-time), and that a *cloned* copy
     of `v` survives — proving no aliasing UB in the COW/allocation path.
  2. **reserve-then-mutate**: `put_reserved` fills the slot, then a subsequent
     `put` triggers a split; assert no read of the stale reserved pointer (the
     test must not retain it) and that the frame reuse path is sound.
  3. **nested-reader-reads-dirty** (§5): open a nested reader, read a dirty page
     it borrows, drop it, then resume the writer; assert the borrow did not span
     a mutation and no frame moved under it.
  4. **many dirty pages / frame stability**: dirty enough pages to exercise the
     pgno→frame index growth (TXN-44) and assert existing frame addresses are
     unchanged across the growth.

---

## §7 — Env handle model, Clone, close, registry (SPEC 00 rows 23–26)

- **TXN-50** — `Env` is a cheap, cloneable handle wrapping `Arc<EnvInner>`
  (refcounted shared ownership; SPEC 00 row 25). `EnvInner` owns the mmap, the
  data-file fd, the write mutex (§2), the reader table (§4), the
  published-snapshot cell (the atomically-swappable `Arc<Snapshot>`, TXN-18) and
  its mirroring in-memory `commit_point` atomic (§4.3), and the process-registry
  back-reference. `Clone` bumps the refcount; it does not reopen the file.
  **Lock region (amended 2026-10-13, ADR-0023).** In a lock-file environment
  `EnvInner` also owns the **lock region**: the mapped lock file and its one
  descriptor, through which it reaches the published txnid `T`, the shared
  reader slots and the cross-process writer lock (§12). The reader table
  holds the only other reference to it. Without a lock file there is none.
- **TXN-51** — **Same-process registry** (SPEC 00 row 26): a process-global map
  `path → Weak<EnvInner>` guards uniqueness. `open` on a path whose `Weak` still
  upgrades returns the LMDB `EnvAlreadyOpened` mapping (heed
  `Error::EnvAlreadyOpened`; §8). `Index::rollback` (drop-then-reopen-with-
  PREV_SNAPSHOT) and the IndexMap LRU depend on this exact behavior. The registry
  entry is cleared when the last handle drops (close, TXN-53).
  **Lock-file registry (amended 2026-10-13, ADR-0023 amendment 2).** A second,
  process-global registry, in `zerodb-io`, records every lock file this
  process has open, keyed by the file's identity (`st_dev`, `st_ino`), or by
  its canonical path while it does not exist yet. A second open of the same
  lock file in this process, under any name (another spelling, a symbolic or
  hard link), is refused with `EnvAlreadyOpened` *before* a descriptor is
  opened. POSIX record locks belong to the process, so closing a second
  descriptor of the file would silently drop the first handle's election and
  liveness locks. The lock file coordinates processes; this rule and the path
  registry above keep one handle per environment within a process.
- **TXN-52** — `Env::prepare_for_closing()` consumes the public `Env`, returns an
  `EnvClosingEvent` (SPEC 00 rows 23/24), and marks the env *closing*. The
  underlying `EnvInner` is **not** torn down until the last strong reference
  drops — including every outstanding `RoTxn`/`RwTxn` (each holds what it needs to
  stay valid) and every `static_read_txn` handle (TXN-24). This is the observable
  "close blocks until the last txn drops" contract.
- **TXN-53** — Actual teardown (munmap, close fd, clear the registry slot) runs
  in the `Drop` of the last `EnvInner` reference. `EnvClosingEvent::wait()` /
  `wait_timeout(dur)` block until that teardown has run (a `SignalEvent` fired
  from the teardown path). The IndexMap waits on this before reopening a path on
  resize/delete.
  **Lock-file teardown (amended 2026-10-13, ADR-0023).** After the data file's
  mappings and descriptor are released, and still before the close event
  fires, the teardown drops the lock region: the lock file is unmapped, its
  descriptor closed, which releases this process's record locks (its shared
  election lock and its liveness lock), and only then is its entry in the
  lock-file registry removed. A waiter woken by the event can therefore
  reopen the environment at once. Every reader slot of this process is
  already released by then, because every read txn holds the env alive
  (TXN-52).
- **TXN-54** — Because close is deferred to the last reference, a read borrow
  `&[u8]` handed to another thread via a `Send` `RoTxn` (TXN-13) can never
  outlive the mmap: the txn keeps the env alive, and the borrow is tied to the
  txn. `static_read_txn` makes this explicit by owning an `Env` clone.
- **TXN-55** — `Env::try_clone_inner_file()` (SPEC 00 row 22) `dup()`s the single
  data-file fd for raw S3/snapshot streaming under a held write txn; it requires
  the env to be a single regular data file (SPEC 02 §8) and does not affect the
  refcount/close model beyond the returned `File`'s own lifetime.

---

## §8 — Write-txn lifecycle: begin / mutate / commit / abort

- **TXN-56** — **begin** (`Env::write_txn`): acquire the write mutex (TXN-6),
  read the live meta, set `writer_txnid = last_committed_txnid + 1` (TXN-2),
  initialise an empty dirty set and freed-page list, snapshot the working
  `DBRecord`s (roots/stats) from the meta for in-txn mutation. On a read-only env,
  fail per TXN-8.
  **Base at `T` (amended 2026-10-13, ADR-0023 §4 and amendment 15).** In a
  lock-file environment "the live meta" is the snapshot published as `T`,
  read *after both writer locks are held* (TXN-6), never the process-local
  cell: another process may have committed since this process last looked.
  It is the cached snapshot when `T` matches its txnid, else decoded at `T`
  (TXN-18). The begin then runs, in order: the takeover reap, when the
  cross-process lock was taken over from a dead owner (LCK-7); under
  `WRITE_MAP`, growing the data file to this process's logical map size
  (§6.4); reading the base at `T`; and checking the base against the map view
  the txn will read through (`MdbError::MapResized` past the logical size,
  TXN-38). `writer_txnid` is the base's txnid + 1. Every failure releases both
  writer locks. Without a lock file the base is the process-local cell, as
  before.
- **TXN-57** — **mutate**: `put`/`del`/cursor writes run SPEC 03 §5–§10 against
  the dirty set (COW top-down, TXN-42), record freed pages (SPEC 05), and update
  working `DBRecord` stats. Reads interleave under §6. Nested readers may be
  opened only in the frozen windows §5 permits.
- **TXN-58** — **commit** (`RwTxn::commit`): assert `child_count == 0` (TXN-33),
  then run the commit pipeline (§9), which hands off to SPEC 06 for durability.
  On success the new meta is live (TXN-3/TXN-19) and the write mutex is released.
- **TXN-59** — **abort** (`RwTxn::abort`, also used as a pure write-lock release,
  SPEC 00 row 28): drop the dirty set and freed-page list, discard the working
  `DBRecord`s, make **no** disk change (the live meta is untouched), release the
  write mutex. txnid is not consumed on disk (TXN-2). Named-DB creates performed
  in the txn vanish (SPEC 00 row 62 "create-in-txn-then-abort"; M1.6).
- **TXN-60** — A panic while a `RwTxn` is live is an implicit abort (the guard's
  `Drop` releases the mutex and drops the dirty set); the env's durable state is
  the last committed meta. If a commit-pipeline fsync failed, the env is poisoned
  instead (SPEC 06 REC-13), and both commit and future writes error.

### §8.1 — Error taxonomy (must map 1:1 to heed, SPEC 00 row 54)

| Condition | ZeroDB error → heed | Rule |
|-----------|---------------------|------|
| write txn on RDONLY env | `MdbError` (LMDB `EACCES`) | TXN-8 |
| reader slots exhausted | `MdbError::ReadersFull` | TXN-16 |
| second open of live path | `Error::EnvAlreadyOpened` | TXN-51 |
| allocation exceeds map_size | `MdbError::MapFull` | SPEC 02 §8, SPEC 05 GC-17 |
| txn begin: snapshot beyond this env's map_size (another process grew the data) | `MdbError::MapResized` | SPEC 02 §8, ADR-0023 amendment 15 |
| nested write txn attempt | unsupported (D-003) | TXN-40 |
| fsync failed (poisoned) | `Error::Io` / poisoned | SPEC 06 REC-13 |

---

## §9 — Commit pipeline (one function, crash hooks between steps)

The commit ordering is the crash-safety invariant (M1.4). It is encoded in
**one** function with **crash-injection hooks** between each numbered step
(M1.11 kills / tears at each hook). SPEC 06 defines the invariant guaranteed at
every hook; this section defines the steps and their ordering. Both write modes
(heap+pwrite default, and WRITE_MAP) share this ordering; only the primitive
(`pwrite`+`fdatasync` vs `msync`) differs (SPEC 06 REC-8/REC-9).

- **TXN-61** — Commit steps, in order (target meta slot = `writer_txnid & 1`):

  | # | Step | Hook after | What is on disk if we crash here |
  |---|------|-----------|----------------------------------|
  | C0 | Assert `child_count == 0`; compute freed-page list. | — | last committed meta `N−1` (unchanged). |
  | C1a | **catalog write-back** (M1.6, SPEC 02 §6.1): write each dirty named-DB working record back into the main tree as its `F_SUBDATA` catalog entry. Before `freelist_save` (matches LMDB's sub-DB flush order) so the pages this COWs/frees are captured by C1. | — | `N−1` (all changes still in the dirty set). |
  | C1 | **freelist_save** (SPEC 05 §4): apply drain rewrites to the GC DB (dirtying GC pages into the dirty set, loop-until-stable, SPEC 05 GC-11) and place this txn's freed set — into the meta free-list annex when it fits (format v2, SPEC 05 §2a GC-32: stashed for C4, no tree write), else into the GC DB under `BE(N)`. | H0 | `N−1` (all changes still in dirty set, nothing written). |
  | C2 | **write dirty pages** to their pgnos (`pwrite` each dirty frame; memcpy-into-map under heap-staged WRITE_MAP, TXN-45a; a no-op per in-map frame under in-place WRITE_MAP, TXN-45b — those bytes were stored at allocation/edit time, under the same TXN-62 invariant). Not yet durable. | **H1** | `N−1` live; new pages sit in free/beyond-HWM slots the `N−1` tree does not reference (TXN-62). Partial/torn data pages are unreferenced garbage. |
  | C3 | **fsync(data)** — flush all data pages (skipped under `NO_SYNC`/`MAP_ASYNC`, SPEC 06 REC-6). | **H2** | `N−1` live; txn `N`'s data fully durable but unreferenced (no meta points at it). |
  | C4 | **write meta** to slot `N&1` (the *older* slot), with `txnid = writer_txnid`, the C1-stashed free-list annex (format v2, SPEC 02 §3) and a fresh CRC covering both (SPEC 02 §3.3). Not yet durable. | **H3** | Either `N−1` (meta `N` not yet reached disk, or reached but torn → CRC rejects it → older slot wins) or `N` (meta reached disk intact). Never a torn meta accepted. |
  | C5 | **fsync(meta)** — flush the meta page (skipped under `NO_META_SYNC`/`NO_SYNC`: the meta is written but not fsynced this commit, SPEC 06 REC-6/REC-7/REC-9). | **H4** | txn `N` durable and live. |
  | C6 | Publish the new snapshot (TXN-18/TXN-19): swap the `Arc<Snapshot>`, then `commit_point.store(writer_txnid, SeqCst)`; update `last_committed_txnid`; release the write mutex. | — | commit complete; new readers see `N`. |

- **TXN-62** — **Page-reuse crash-safety invariant.** C2 may only write to pages
  that the currently-live meta (`N−1`) does **not** reference: freshly allocated
  pages beyond `last_pg`, or GC-reclaimed pages that are safe by the oldest-reader
  gate (SPEC 05 GC-18). The gate guarantees a reused page was freed by a txn `≤
  oldest_reader ≤ N−1` and is therefore absent from `N−1`'s tree — so overwriting
  it cannot corrupt the crash-fallback snapshot. This invariant is what makes H1
  and H2 safe (SPEC 06 REC-2/REC-3).
- **TXN-63** — Meta slot targeting: writing slot `N&1` overwrites the slot holding
  `N−2`; the intact fallback is slot `(N−1)&1` holding `N−1`. This is why the
  double buffer, not a single meta, is mandatory (SPEC 02 §3): the last committed
  snapshot survives the entire pipeline (SPEC 06 REC-2).
- **TXN-64** — `writer_txnid` is published as the in-memory commit point (C6)
  **only after** C5, so no reader can pin `N` before it is durable. This closes
  the loop with the reader pin (TXN-17): a reader either pins `≤ N−1` (and the
  writer respects it in GC) or, after C6, pins `N` (durable).

---

## §10 — PREV_SNAPSHOT at the txn layer (SPEC 01 §S5)

- **TXN-65** — Opening an env with `EnvFlags::PREV_SNAPSHOT` selects the
  **older** valid meta at open (SPEC 02 §3.2 `(txnid[0] < txnid[1]) XOR
  prev_snapshot`, restricted to CRC-valid slots). The txn layer then treats that
  older snapshot as `last_committed_txnid` for every subsequent txn: read txns
  see the older roots; the first write txn is `older_txnid + 1`.
- **TXN-66** — **Exclusivity**: a PREV_SNAPSHOT open requires the env not already
  be open in-process (TXN-51 already enforces one handle per path; a PREV_SNAPSHOT
  open while another handle is live is refused, matching the fork's `EAGAIN`
  requirement, SPEC 01 §S5).
  **Other processes (amended 2026-10-13, ADR-0023 §5).** In a lock-file
  environment a `PREV_SNAPSHOT` open must win the first-opener election
  (LCK-1), that is, no other process has the environment open. A process
  that joins instead fails the open with `Io` and raw OS error `EAGAIN`,
  after releasing the lock file and before opening the data file, as LMDB
  does (`OBS_PREV_SNAPSHOT_JOINER = EAGAIN`). Without a lock file the caller
  keeps other processes out. A `PREV_SNAPSHOT` open takes its map geometry
  from the older meta, the one it selects: a map below that meta's data
  rounds up to its `(last_pg + 1) · page_size`, and an unset map size adopts
  that meta's `map_size`, as LMDB does (`OBS_PREV_SNAPSHOT_MINSIZE_FROM`,
  `OBS_PREV_SNAPSHOT_ZERO_MAPSIZE_FROM`, both `"prev"`; observed on macOS
  only). The original closing sentence ("under D-001 this is the natural
  single-handle case") is retired.
  **The election is held until the first commit (amended 2026-10-13,
  ADR-0023 amendment 19).** The `PREV_SNAPSHOT` initializer does not finish
  the election at the end of its open (LCK-1, LCK-2): byte 0 stays
  exclusive, with the initializing magic, until its first commit that runs
  the commit pipeline, which finishes it after the meta write and its fsync
  (TXN-67). An unchanged commit writes nothing and keeps it. Until then
  every other opener waits in the election: an ordinary open then joins and
  reads the older snapshot; a second writable `PREV_SNAPSHOT` open joins and
  then fails with `EAGAIN` as above (`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`,
  `OBS_PREV_SNAPSHOT_EMPTY_COMMIT_KEEPS_LOCK`,
  `OBS_PREV_SNAPSHOT_SECOND_PREV_OPENER`). A `READ_ONLY` `PREV_SNAPSHOT`
  open never commits, so it holds the election until it closes
  (`OBS_PREV_SNAPSHOT_READ_ONLY_OPENER`). If the initializer closes or dies
  first, a waiting process wins the election and re-initializes the lock
  file from the newest meta; LMDB differs there (D-025, approved).
  A `READ_ONLY` `PREV_SNAPSHOT` open that joins a running environment also
  fails with `EAGAIN`. LMDB admits it, and after its open and close the
  shared txnid is back at the older meta for every process
  (`OBS_PREV_SNAPSHOT_READ_ONLY_JOINER`); ZeroDB never moves `T` backwards
  (TXN-17), D-025 (approved).
- **TXN-67** — **Auto-clear on first commit** (SPEC 01 §S5, milli
  `Index::rollback`): the first `commit()` on a PREV_SNAPSHOT env is txn
  `older_txnid + 1` (TXN-2) and writes slot `(older_txnid + 1) & 1`. **This slot
  is exactly the abandoned newer slot** — because the newer snapshot was
  `older_txnid + 1` (the two metas always differ by one commit), it lived in slot
  `(older_txnid + 1) & 1`, the *same parity*. So the first post-rollback commit
  **overwrites the abandoned newer meta in place**, with no separate invalidation
  step: afterwards slot `older&1` holds `older_txnid` and slot `(older+1)&1` holds
  the fresh commit, so normal higher-txnid selection resumes and the env is no
  longer pinned to the old meta. This **self-resolution** is confirmed by the
  oracle (`flag_semantics.rs::env_prevsnapshot_opens_older_meta`: after a
  prev-snapshot commit and a normal reopen, the rolled-back-then-committed state
  is live and txn B's abandoned meta is gone). The observable contract is milli's:
  "open older, commit once to make it live, then normal." (See §10 / SPEC 06 REC-5
  for the one residual, cosmetic open question.)
  **With a lock file (amended 2026-10-13, ADR-0023 amendment 19).** The
  first commit is durable but **not published**, as LMDB's: `T` stays
  `older_txnid`, the published-snapshot cell keeps the older snapshot and the
  validated-pages cache (TXN-38) is not seeded from it. Every new read txn,
  in this process or in one that joins, therefore pins `older_txnid`; the
  next write txn begins from `T` (ADR-0023 §4), so it is again
  `older_txnid + 1`, built on the older snapshot, and overwrites the first
  commit's meta slot, which drops the first commit's data. That commit
  publishes `T` as usual. In this process, `Env::info`, `Env::stat`,
  `Env::txnid` and `Env::resize` use the unpublished commit while it is newer
  than the snapshot at `T`, as LMDB reports the newest meta on disk; a
  process that joins after the first commit keeps the newest meta it selected
  at open for `Env::info`, `Env::stat` and `Env::resize` while it is newer
  than `T` (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`); its
  `Env::txnid` and its read txns stay at `T`
  (`OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW`, `OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`,
  `OBS_PREV_SNAPSHOT_SECOND_COMMIT_KEEPS_FIRST = false`). A fresh open after
  every process has closed re-initializes the lock file and serves the
  first commit (`OBS_PREV_SNAPSHOT_ONE_COMMIT_THEN_REOPEN`), so milli's
  "commit once, close, reopen" still makes the rollback live. A crash during
  the second commit can leave the first commit's meta naming pages the
  second commit rewrote: LMDB's second commit writes its root to the page
  the first commit's meta names
  (`OBS_PREV_SNAPSHOT_SECOND_COMMIT_REWRITES_FIRST_ROOT = true`), and ZeroDB
  builds on the same older free space; that it reuses the same page number
  is inferred from that observation, not observed in ZeroDB (ADR-0023
  amendment 19). A process reports only a newer meta it found at open: a
  meta written after its open and never published (a writer that died
  between its meta write and its `T` store) is not reported by its
  `Env::info` or `Env::stat` until the next commit publishes, while LMDB
  re-reads the metas on every call. Without a lock file the first commit is
  published at once (`OBS_PREV_SNAPSHOT_NOLOCK_COMMITS`).

> **Conflicts for human review (SPEC 04):**
> 1. **PREV_SNAPSHOT abandoned slot — largely self-resolving (TXN-67, REC-5).**
>    The reviewer's observation stands: because the abandoned newer snapshot and
>    the first post-rollback commit share slot parity (both are `older_txnid + 1`),
>    the commit overwrites the abandoned meta *in place*, and no explicit
>    invalidation is needed — the oracle confirms it. The only **residual,
>    cosmetic** open question is whether reusing txnid `older_txnid + 1` (equal to
>    the abandoned commit's txnid) for the new commit is acceptable given it orphans
>    the abandoned commit's beyond-high-water pages (a bounded space leak milli's
>    rollback already tolerates, LMDB parity). This needs no SPEC 02 format change;
>    flagged for maintainer ratification only.
> 2. **Writer-quiescence rule TXN-29 (D-005 APPROVED, maintainer 2026-07-16).**
>    The fork technically permits a writer to mutate while nested read children
>    are live; ZeroDB forbids it for Rust soundness and adds a runtime guard
>    (TXN-29, `MdbError::BadTxn`). This is **not** observable to any consumer
>    (all six call sites pause the writer); the oracle keeps it unobservable by
>    classifying writes-under-a-child as the symmetric
>    `Skip::WriteBlockedByNested` **before either engine runs**.

---

## §11 — Cross-reference index

| Concern | Rule(s) | Interlocks with |
|---------|---------|-----------------|
| txnid ↔ meta | TXN-1..5, TXN-63 | SPEC 02 §2/§3, INV-20 |
| single writer | TXN-6..9 | SPEC 01 Table 1 RDONLY |
| read snapshot pin | TXN-10..13 | SPEC 05 GC-18 |
| reader table | TXN-14..25 | M1.8; loom suite L1–L5 (`zerodb-core/src/readers.rs`, `just loom`); stress gate (`zerodb/tests/reader_stress.rs`, `just stress`) |
| memory ordering | TXN-15/17/19/20 | AGENTS.md (ARM), SPEC 06 |
| nested read txn | TXN-26..36 | SPEC 00 row 16, SPEC 01 §S9; loom L6 (`zerodb-core/src/nested.rs`); fan-out gate (`zerodb/tests/nested_fanout.rs`); differential (`zerodb-oracle/tests/nested_read_differential.rs`) |
| value-borrow contract | TXN-37..49 | SPEC 03 §3/§5/§7, SPEC 01 §S3/§S7 |
| env clone/close/registry | TXN-50..55 | SPEC 00 rows 23–26 |
| commit pipeline | TXN-61..64 | SPEC 06 REC-1..12 |
| PREV_SNAPSHOT | TXN-65..67 | SPEC 01 §S5, SPEC 02 §3.2, SPEC 06 REC-5 |
| lock file (multi-process) | LCK-1..11 (§12); TXN-73 (§4.4); TXN-6/7, TXN-17..19, TXN-51/53, TXN-56, TXN-66 as amended | ADR-0023; SPEC 02 §8; SPEC 06 REC-1a; loom models in `zerodb-core/src/readers.rs`; `zerodb/tests/multi_process*.rs`; `zerodb-oracle/tests/multi_process_differential.rs`, `lmdb_reaping_observation.rs`, `lmdb_stage3_observation.rs` |

**Rule count: TXN-1 … TXN-67 (67 normative rules; §4.4 adds sub-rule TXN-18a for
slot release), plus the later TXN-68 … TXN-72 (§6.3a), TXN-73 (§4.4, added
2026-10-13) and LCK-1 … LCK-11 (§12, added 2026-10-13).** TXN-40 (nested write unsupported, D-003) is referenced from
§5/§8 and defined here:

- **TXN-40** — A nested **write** txn is **unsupported** (D-003) and, as
  amended by ADR-0007 Q2/Q3 (ratified 2026-07-16), **unrepresentable in the
  public API**: zerodb exposes **no** nested-write entry point at all — no
  `Env::nested_write_txn`, no `RwTxn::nested` — which is a strictly stronger
  form of the original "clean error" clause (nothing to call, so never a
  panic and never partial state). The heed-zerodb adapter (M1.13) likewise
  surfaces no nested-write entry point and no error variant is needed. heed
  itself keeps only `RwTxn::nested` `pub(crate)`: its `Env::nested_write_txn`
  is public (heed 0.22.1 and Meilisearch's fork), so a consumer calling it
  does not compile against the adapter (issue #71; corrected 2026-10-17, the
  ratified text said heed exposed no entry point). Should any future
  surface be forced to exist (e.g. an FFI shim), it must return a clean
  documented "unsupported" error mapped through the heed taxonomy (§8.1).
  Zero consumer call sites (SPEC 00 §A). The 1.14 heed-suite gate excludes
  nested-write tests.
  - *Pre-amendment text (superseded): "it returns a clean, documented
    'unsupported' error (mapped through the heed error taxonomy, §8.1), never
    a panic and never partial state."*

---

## §12 — The lock file (multi-process access, ADR-0023)

Added 2026-10-13. An environment opened with a lock file (the default; §0) is
coordinated through one file next to the data file, mapped `MAP_SHARED` by
every process that has the environment open. This section states its
invariants; the rules in §2, §4 and §7–§10 say how transactions use it. The
design follows LMDB's lock file in technique (a first-opener election, PID
liveness by record locks, a shared reader table), not in byte layout: the
file is ZeroDB's own format, never LMDB's. Normative rules are numbered
**LCK-n**. Implementation: `zerodb-io::lockfile` (the file, its record locks
and the futex), `zerodb-core::shared` (the safe interface the core sees) and
`zerodb-core::readers` (the slot protocol).

- **LCK-1 — First-opener election (ADR-0023 §1, amendments 2 and 10).** Every
  opener takes POSIX `fcntl` record locks, which belong to the process. On
  open it tries an exclusive (write) lock on byte 0 without waiting. If it
  gets it, no other process has the environment open: it is the
  **initializer** and builds the file (LCK-2), keeping byte 0 exclusive until
  the environment is fully open, then converts the lock to a shared one in
  place. A `PREV_SNAPSHOT` initializer keeps it exclusive until its first
  commit instead (TXN-66, ADR-0023 amendment 19). Otherwise it is a
  **joiner**: it waits for a shared lock on byte 0,
  which it holds until it closes. Once granted, it tries the exclusive lock
  once more; that succeeds only when every other process has exited
  meanwhile, and the joiner then initializes instead. A joiner that loses
  that upgrade and finds no final magic (LCK-2) drops its lock and runs the
  election again, at most 100 times with a short PID-staggered sleep, then
  fails the open with `Io(TimedOut)`. A joiner that loses the upgrade but
  finds a final magic left by a previous run adopts that run's table, which
  is the same state as joining a table whose processes all exited.
  `zerodb::ExclusiveLock` runs the election without waiting and never
  finishes it, so every other opener waits until the guard drops (used by
  `zerodb-tools check` on a file that does not open, ADR-0023 amendment 18).
- **LCK-2 — Initialization and the magic (ADR-0023 amendments 10, 16 and
  20).** The initializer rebuilds the file in place and never truncates it to
  zero. It writes the initializing magic `ZDBLOCK0` at offset 0 with
  `pwrite`; sets the file's length to `256 + 128·slots` only when the length
  differs (before the next write, so that write never extends the file);
  writes zeros over the rest of the header (bytes 8–255), so no header field
  survives from a previous life; then wipes every reader slot (txnid
  `RDR_FREE`, owner word 0) and stores the format version and the slot count.
  The unused bytes inside a slot are not rewritten, and nothing reads them.
  The engine then binds the data page size and seeds `T` from the meta it
  selected. Only then is the final magic `ZDBLOCK1` stored,
  with `Release`, and byte 0 downgraded: at the end of the open, or, for a
  `PREV_SNAPSHOT` open, at its first commit (TXN-66). A joiner loads the magic with
  `Acquire`, so a joiner that sees the final magic also sees every header
  field and a seeded `T`. A joiner refuses the open with `Io(InvalidInput)`
  when the format version differs, when the recorded slot count is 0 or
  above `MAX_READERS_LIMIT`, or when the file is shorter than that count
  needs, and with `Io(InvalidData)` when the bound page size differs from the
  data file's.
  - **Reader count.** A joiner adopts the recorded slot count whatever its
    own `max_readers` asks (TXN-14). An initializer that finds a finished
    file from an earlier run uses the larger of its `max_readers` and the
    recorded count, and still wipes every slot. A file left with only the
    initializing magic (an initializer that died, or a released
    `ExclusiveLock`) records no count, so the next initializer uses its own.
- **LCK-3 — Layout (ADR-0023 §1, amendments 11 and 14).** All fields are
  little-endian and reached only through atomics at explicit offsets, never
  through a cast of the mapping to a struct. The mapping is page-aligned.

  | Offset | Field |
  |---|---|
  | 0 | magic `u64`: `ZDBLOCK0` while being built, `ZDBLOCK1` once published |
  | 8 | lock-format version `u32`, currently 2 |
  | 12 | data page size `u32`, 0 until bound |
  | 16 | slot count `u32` (`max_readers`) |
  | 64 | published txnid `T` `u64`, on its own cache line |
  | 128 | Linux writer word `u32`: owner PID, bit 31 = waiters |
  | 136–255 | unused, keeping slot 0 off the writer word's 128-byte line pair |
  | 256 + 128·i | reader slot `i` (128 bytes, `ReaderSlot`, TXN-14) |

  The file is `256 + 128 · max_readers` bytes. A slot holds its txnid word
  at +0 and its **owner word** at +8: the owner PID in the low 32 bits (0 =
  no owner: free, being claimed or being released, presumed live) and a
  claim generation in the high 32 bits. Every write that stores a non-zero
  PID in the word (a claim, or a reaper's adoption) increments the
  generation; a release stores PID 0 and keeps the generation. Record-lock
  offsets are separate from the mapped bytes: byte 0 is the election, byte 1
  the macOS writer lock, and byte `2^32 + pid` each process's liveness lock.
- **LCK-4 — Cross-process writer lock (ADR-0023 §2, amendment 3).** Taken only
  while holding the in-process writer lock (TXN-6), by a write-txn begin and
  by `Env::resize`.
  - *Linux:* acquire is `CAS 0 → my_pid` (`Acquire`). A contended waiter sets
    the waiters bit and sleeps on the word's futex for at most 100 ms; after
    each timeout it probes the owner's liveness (LCK-5) and, if the owner is
    dead, takes the lock over by a CAS from the dead PID to its own, keeping
    the waiters bit. A word naming this process's own PID cannot be held by
    another thread of this process (they are excluded by the in-process lock),
    so it was left by a dead process with the same PID and is taken over at
    once. Release swaps in 0 (`Release`) and wakes the waiters only if the
    bit was set; it may run on any thread.
  - *macOS:* an exclusive `fcntl` lock on byte 1, waited for with `F_SETLKW`
    and released with `F_SETLK`; the kernel releases it if the process dies.
    A failed release aborts the process rather than leaving other processes
    blocked. A takeover from a dead process cannot be told apart from an
    ordinary acquisition.
  - A writer that dies holding the lock needs no repair: its commit is
    visible only through the meta write and the later store of `T`, so it
    leaves at most unreferenced pages, as after a crash. The next owner
    rebuilds its base from the meta at `T` (TXN-56).
- **LCK-5 — Liveness (ADR-0023 §3, amendment 2).** Each process holds an
  exclusive record lock on byte `2^32 + pid` from its open until its
  environment closes, which is after its last reader slot is released.
  "Is process P alive?" is an `F_GETLK` probe of that byte. A kernel record
  lock, unlike `kill(pid, 0)`, is immune to PID reuse: a recycled PID holds
  the lock only if a live process with that PID has the environment open. A
  probe of this process's own PID always answers alive, because `F_GETLK`
  never reports the caller's own locks. POSIX drops all of a process's
  record locks on a file when *any* of its descriptors to that file closes,
  so a process keeps exactly one descriptor per lock file (TXN-51, the
  lock-file registry).
- **LCK-6 — Slot claim and the three-store release (ADR-0023 §3, amendments
  7 and 13).** A claim is the TXN-15 CAS `RDR_FREE → RDR_CLAIMED`
  (`Acquire`), then the owner word `(generation + 1, my_pid)` (`Release`),
  then the TXN-17 pin. The owner's release is three `Release` stores:
  `txnid = RDR_CLAIMED` (unpins), then the owner word with PID 0 and the
  generation kept (disowns), then `txnid = RDR_FREE` (hands the slot over).
  A death at any point therefore leaves either `RDR_CLAIMED` with the dead
  PID, which a reaper frees, or `RDR_CLAIMED` with PID 0, which pins nothing
  (`oldest()` skips `RDR_CLAIMED`). The second state, and a death between
  the claim CAS and the owner store, is the **known leak**: one slot no
  reaper can attribute, lost until the next initializer wipes the table.
  ZeroDB accepts it to keep claims lock-free; LMDB avoids it by allocating
  slots under a reader mutex. `reader_list` reports a slot's PID, or `None`
  while it has none.
- **LCK-7 — Reaping (ADR-0023 §3, amendments 8, 12 and 14).** A reaper frees
  the slots of dead processes:
  1. it loads every owner word (`Acquire`), keeping those with a non-zero PID
     other than its own;
  2. it probes each distinct PID once (LCK-5);
  3. for each slot whose PID was judged dead, it adopts the slot by a CAS
     of the owner word it loaded in step 1 to `(generation + 1, reaper_pid)`
     (`AcqRel`), and on success runs the owner's release (LCK-6).

  Two invariants make the adoption safe:
  - **A — the owner word is ABA-free.** A word with a non-zero PID never
    recurs, because every write of a non-zero PID increments the
    generation; a word with PID 0 is never a CAS target. Wrap-around needs
    2^32 claims of one slot while one reaper is paused, which is accepted.
  - **B — load before probe.** A slot is adopted only by a CAS against a
    word loaded *before* the probe that judged its PID dead. Since a process
    holds its liveness lock until after its last slot is released (LCK-5), a
    probe that finds P dead proves that every word carrying P loaded earlier
    still belongs to a dead process, unless the word changed, which A makes
    the CAS detect. The `Acquire` loads keep the step-1 loads ahead of the
    probes on a weakly ordered machine.

  A reaper that dies after its CAS leaves a slot carrying its own PID, which
  the next reaper frees. Reaping counts slots, not processes. **Reap
  points**, as observed in the LMDB fork (amendment 12,
  `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs`): an explicit
  `Env::clear_stale_readers` (`mdb_reader_check`); a writer that took the
  writer lock over from a dead owner (Linux; LCK-4), whose reap failure
  fails the write-txn begin and releases both locks; and the initializer,
  which wipes the whole table. Nothing else reaps: not a read-txn begin, a
  commit, a joining open, nor a full reader table (TXN-16).
- **LCK-8 — Own-PID stale slots at join (ADR-0023 amendment 9).** A joiner
  frees, during open and before its first claim, every slot whose owner word
  carries its own PID: a dead process with the same PID left them, and the
  liveness probe cannot tell them apart from this process's own slots. Each
  is a CAS of the word to `(same generation, PID 0)` (`AcqRel`), which makes
  a concurrent reaper's pending adoption fail, then `txnid = RDR_FREE`
  (`Release`). Dying between the two stores leaks one *pinned* slot until
  the next initializer; the window is a single store, accepted.
- **LCK-9 — Name (ADR-0023 amendment 16).** The lock file sits in the
  environment directory. Its default name is derived from the data-file
  name: `zerodb.dat` → `zerodb.lock`, `data.mdb` → `lock.mdb`, any other
  name `<data_file_name>-lock`; `EnvOpenOptions::lock_file_name` overrides it
  with a single path component that differs from the data-file name.
  Distinct data names in one directory therefore never share a lock file.
  SPEC 02 §8 states the rule and the two file guards below.
- **LCK-10 — File guards (ADR-0023 amendment 16).** Before anything is
  written, every opener refuses a lock file that is the data file under
  another name (same `st_dev` and `st_ino`), and the initializer refuses a
  non-empty file that carries neither magic and is not an all-zero header
  (the first 256 bytes, or the whole file if shorter) at a length an
  initializer can leave (8, or `256 + 128·k` for `k ≥ 1`). Both refusals are
  `Io(InvalidInput)` and modify neither file. SPEC 02 §8 gives the details
  and the accepted residual window.
- **LCK-11 — Without a lock file (ADR-0023 amendment 17).** `NO_LOCK`
  (`lock_file(false)`) creates and opens no lock file
  (`OBS_NOLOCK_CREATES_LOCK_FILE = false`), and so does a `READ_ONLY` open
  whose lock file cannot be created or opened for writing on a read-only
  filesystem (`EROFS`; `OBS_RDONLY_LOCK_READONLY_FS = 0`). The environment
  then runs the single-process path: the in-process reader table and
  published-snapshot cell, and the in-process writer lock only. Every other
  lock-file error fails the open, `READ_ONLY` included; a `READ_ONLY` open
  opens the lock file read-write, as LMDB does, and may itself be the
  initializer, which seeds `T` but never writes the data file.

### §12.1 — Documented limitations

These are stated limits of the current design, not rules. None is observed
against LMDB unless it says so.

- **`fork()` after open is unsupported.** The lock region records the PID of
  the process that opened it, and POSIX record locks are not inherited by a
  child process: a child using the parent's `Env` would claim slots and
  writer locks under the parent's PID without holding its liveness lock.
  Open the environment in the process that uses it.
- **PID namespaces are unsupported.** Owner words, the writer word and the
  liveness-lock offsets use PIDs as each process sees them. Processes in
  different PID namespaces sharing one lock file can name unrelated processes
  by one PID. Not tested.
- **macOS writer takeover does not reap.** On macOS the writer lock is a
  record lock that reports no previous owner (LCK-4), so a writer that
  follows a dead writer does not free dead readers' slots; only
  `clear_stale_readers` and the next initializer do. LMDB's writer takeover
  was observed on Linux only (amendment 12); its macOS behavior is
  unobserved.
- **`Env::sync` takes only the in-process writer lock.** A sync in one
  process serializes with this process's commits, not with a commit in
  another process. Whether LMDB's `mdb_env_sync` takes its writer mutex is
  unobserved.
- **`PREV_SNAPSHOT`: what is not mirrored.** The election hold, the
  unpublished first commit and the joiner's `info`/`stat` follow LMDB
  (TXN-66, TXN-67; ADR-0023 amendment 19; `multi_process_differential.rs::
  join_before_prev_snapshot_first_commit_vs_lmdb` and its neighbors). Two
  cases differ, docs/DIVERGENCES.md D-025 (approved). If the opener closes
  or dies before its first commit, a waiting ZeroDB process becomes the
  first opener and serves the newest meta; LMDB's waiter joins the lock
  file as the opener left it and reads the older meta, or, on macOS after a
  clean close, fails with `EINVAL`
  (`OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT`,
  `OBS_PREV_SNAPSHOT_OPENER_DIES_BEFORE_COMMIT`,
  `OBS_PREV_SNAPSHOT_READ_ONLY_OPENER`). And a `READ_ONLY` `PREV_SNAPSHOT`
  open that joins a running environment fails with `EAGAIN`, while LMDB's
  opens and rolls every process back to the older meta
  (`OBS_PREV_SNAPSHOT_READ_ONLY_JOINER`).
- **`Env::resize` waits for the cross-process writer lock**, also on a
  `READ_ONLY` environment (its lock file is open read-write, so the lock is
  available to it). LMDB takes no lock: its `mdb_env_set_mapsize` returns
  while another process holds a write txn, also on an `MDB_RDONLY` env
  (`OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`,
  `OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER_RDONLY`). The wait is deliberate
  (ADR-0023 decision 3). See `docs/DIVERGENCES.md` (D-023).
