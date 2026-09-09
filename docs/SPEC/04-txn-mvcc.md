# SPEC 04 — Transactions & MVCC

Status: **DONE** — 2026-07-15 (milestone 0.4). Behavioral source of truth for the
transaction lifecycle (M1.4), the reader table and concurrency (M1.8), nested
read transactions (M1.9), and the write-txn value-borrow contract that every
write-path change (M1.4/1.10) must uphold. Formats are in
[SPEC 02](02-pages.md); tree algorithms in [SPEC 03](03-btree.md); free-page
reclamation in [SPEC 05](05-gc.md); the crash/durability guarantees that the
commit pipeline defined here must satisfy are in [SPEC 06](06-recovery.md).

Clean-room note: the LMDB fork (`mdb.master.nested-rtxns`, SPEC 00 pin) was read
to understand the *algorithms* — reader-slot claiming, `mdb_find_oldest`, the
nested-read patch (ITS#10395), and the commit sequence (CLAUDE.md rule 4). The
protocols below are ZeroDB's own design; where a rule mirrors an LMDB idea it is
called out, but no C is transliterated. Binding decisions (not relitigated):
single-process, in-process reader table only (**D-001**); nested WRITE txns
unsupported (**D-003**); nested READ txns over the active write txn are a
hot-path MUST with see-uncommitted semantics (M1.9); meta CRC + double-buffer
per SPEC 02 §3; commit ordering encoded in one function with crash hooks
(PLAN 1.4).

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
- **Single process, single writer.** There is exactly one `Env` per path per
  process (registry, §7) and at most one live write txn per env at a time
  (§2). No lock file, no cross-process readers, no robust mutex (D-001).
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

## §2 — Single-writer protocol (no lock file)

- **TXN-6** — `Env::write_txn()` acquires the env's in-process **writer lock**
  (a field on the shared `EnvInner`, §7). The guard is held for the entire life
  of the `RwTxn` and released on commit or abort. There is **no** lock file and
  **no** cross-process coordination (D-001); the lock serialises only the
  threads of this process, which is the whole world under D-001.
  **Lock construction (amended 2026-09-09, security review M1):** the lock is
  a **thread-agnostic** `occupied: Mutex<bool>` flag plus a `Condvar`, NOT a
  `Mutex<()>` whose `MutexGuard` lives inside the txn. `RwTxn` is `Send` (and
  heed's `WithoutTls` mode moves write txns across threads), so the release
  may run on a different thread than the acquire — unlocking a
  `std::sync::MutexGuard` from a foreign thread is forbidden by std
  (`MutexGuard: !Send`; the underlying lock may abort on macOS). Acquire
  waits on the condvar until `occupied` is false, then sets it; the guard's
  drop takes the flag mutex briefly *on whatever thread drops it*, clears the
  flag, and notifies. Writer-panic policy is unchanged: the flag mutex guards
  no txn data (the dirty set died with the unwound `RwTxn`, TXN-60), so a
  poisoned flag mutex is recovered (`PoisonError::into_inner`) rather than
  propagated.
- **TXN-7** — A second `write_txn()` on the same env **blocks** until the current
  writer finishes (mutex contention), matching LMDB's single-writer serialisation
  (LMDB blocks on `me_wmutex`). It does not error. (Consumers hold a write txn
  briefly or structure their own scheduling; ZeroDB does not add a try-lock
  Phase-1 API.)
- **TXN-8** — On an env opened read-only (`EnvFlags::READ_ONLY`, SPEC 01 Table 1,
  SHOULD/M1.10), `write_txn()` returns the error mapping of LMDB `EACCES`
  (heed `MdbError` path; §8 error table). No write mutex is taken.
- **TXN-9** — Readers never take the write mutex and never block on it; the
  writer never blocks on readers (it computes the oldest reader lock-free, §4).
  This is the core concurrency guarantee (PLAN 1.8): *readers never block, the
  writer never blocks readers.* The published-snapshot **cell** is not the
  write mutex: readers may briefly contend on its bounded O(1) pointer
  operations (one swap per commit, one clone per pin) but never on any part
  of the writer's *transaction* — see TXN-18 as amended (ratified 2026-07-16,
  ADR-0006 Option B) for the precise scope of this guarantee.

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

## §4 — The reader table (in-process, lock-free read path)

The reader table is the only place a reader and the writer communicate. It is an
in-process array (D-001: no shared-memory lock file). Its job: let the writer
compute the **oldest live snapshot** without blocking readers, and let readers
claim/release a pinned snapshot without blocking the writer.

### §4.1 — Layout and sizing

- **TXN-14** — The table is a fixed-size array of `max_readers` **slots**, sized
  at env open from `EnvOpenOptions::max_readers` (SPEC 00 row 5; Meilisearch sets
  1024). When the caller does **not** set it, the default is **126** (LMDB
  parity: `DEFAULT_READERS`), not an arbitrary number — a differential detail the
  oracle can probe. It is allocated once and never resized during the env's life
  (matching LMDB, whose reader count is fixed at open). Because the slots are
  allocated **eagerly** (one cache-padded word per slot), `max_readers` is
  bounded at open: values above `MAX_READERS_LIMIT` (2^20) are rejected with
  `Io(InvalidInput)` rather than aborting on allocation failure
  (added 2026-09-09, security review M6; `max_dbs` has the same bound,
  `MAX_DBS_LIMIT` = 2^20, for the comparator-registry slots — SPEC 00
  rows 4/5).
- Each **slot** is cache-line padded (≥ 64 bytes, aligned) to avoid false
  sharing between a reader touching its slot and the writer scanning the array.
  A slot carries exactly one load-bearing atomic field:

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
  Because occupancy is encoded in `txnid` alone, no separate `pid`/`tid` fields
  are needed (D-001 deletes LMDB's `mr_pid`/`mr_tid`).

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
  matches LMDB (`mti_numreaders == env->me_maxreaders → MDB_READERS_FULL`). Under
  D-001 there are no *stale* cross-process slots to reap first (LMDB's
  `mdb_reader_check`), so the error is raised immediately; Phase 2.2 adds reader
  introspection, not a reaping path.

### §4.3 — Publish-and-verify (the pin) and its memory-ordering correctness

The reader now owns a `RDR_CLAIMED` slot but has not yet pinned a real snapshot.
The pin must be correct against a *concurrent committing writer* on a weakly
ordered machine (ARM; CLAUDE.md). The hazard is a classic **store→load
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
- **TXN-18** — **The published-snapshot object.** *(Amended 2026-07-16,
  ratified — Quentin, standing directive, ADR-0006 Option B: the original
  text required an ArcSwap-style lock-free cell "in the reader-table module
  where `unsafe` is sanctioned"; the ratified implementation keeps
  `zerodb-core` `forbid(unsafe_code)` and scopes the no-blocking guarantee to
  its intent — never blocking on the write* ***transaction*** *— with
  LMDB-NOTLS read-open parity as the bar.)* `EnvInner` holds a
  **published-snapshot cell**: the current `Arc<Snapshot>` plus a mirroring
  `commit_point: AtomicU64` equal to the object's txnid. `Snapshot` is an
  **immutable** value `{ txnid, main_db, free_db, catalog view }` (the roots and
  DBRecords of one committed state). Rules:
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
    after open" race entirely.

### §4.4 — Release

- **TXN-18a** — Dropping a read txn stores `slot.txnid = RDR_FREE` with
  **`Release`** ordering. Justification: `Release` ensures the reader's prior
  reads (page dereferences) are complete before the slot is marked reusable;
  pairing with a later claimer's `Acquire` CAS (§4.2) it hands the slot over
  cleanly. A full fence is unnecessary — releasing a slot only needs to not be
  reordered *before* the reader's own accesses, which `Release` provides.

### §4.5 — Oldest-reader computation (writer side) and its correctness

- **TXN-19** — The writer publishes its new commit point at step C6 (SPEC 04 §9),
  **after** the durable meta is written and before it releases the write mutex, in
  two ordered steps: (1) swap the new `Arc<Snapshot>` into the published cell
  (TXN-18), then (2) `commit_point.store(writer_txnid, SeqCst)`. Step (2) is the
  moment new readers begin to see the new snapshot, and is the writer half of the
  pairing in TXN-17. The object-before-counter order guarantees a reader that
  sees the new counter can load the matching roots.
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
  every GC hand-out and assert `F ≤` every live pin (the PLAN §1.8 shadow
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
  check *(amended 2026-07-16, M1.9 — ratified, session lead under standing
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
- **TXN-36** — Oracle parity target (PLAN 1.9): randomized
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

This is the likeliest soundness bug outside the reader table (PLAN 0.4/1.4). A
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
  it could still be live — which TXN-39 already precludes.
- **TXN-44** — Growth of the *index* (the pgno→frame map) may reallocate the map
  itself; that is fine — the map stores handles/pointers to frames, and moving a
  pointer does not move the pointee. Only frame *contents* addresses are
  load-bearing for borrows.

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
    `zerodb-io`, per the CLAUDE.md unsafe policy) and the M1.4 miri coverage
    (§6.6 / TXN-49) covers writemap's during-txn path for free.
  - The writable map covers the full `map_size` and the file is `set_len` to
    `map_size` at open (matching the fork's `ftruncate`-to-mapsize under
    writemap) so a store to any mapped page never faults past EOF (ADR-0004 D4).
  - True zero-copy **live-map mutation during the txn** (writing COW copies
    straight into the map, `get` returning a slice into the map) is a Phase-3
    optimization: it needs a `zerodb-io`-brokered map-slice API (to keep the map
    `unsafe` out of `zerodb-core`) and a bench to justify it. The full-generality
    wording of TXN-45 (bytes "live in the writable map") is the Phase-3 target;
    Phase-1 satisfies the observable contract via the commit-time copy. This is
    **not** a `docs/DIVERGENCES.md` entry — it produces no observable divergence
    from the fork.

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

### §6.6 — miri obligations (PLAN 1.4)

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
- **TXN-51** — **Same-process registry** (SPEC 00 row 26): a process-global map
  `path → Weak<EnvInner>` guards uniqueness. `open` on a path whose `Weak` still
  upgrades returns the LMDB `EnvAlreadyOpened` mapping (heed
  `Error::EnvAlreadyOpened`; §8). `Index::rollback` (drop-then-reopen-with-
  PREV_SNAPSHOT) and the IndexMap LRU depend on this exact behavior. The registry
  entry is cleared when the last handle drops (close, TXN-53).
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
| nested write txn attempt | unsupported (D-003) | TXN-40 |
| fsync failed (poisoned) | `Error::Io` / poisoned | SPEC 06 REC-13 |

---

## §9 — Commit pipeline (one function, crash hooks between steps)

The commit ordering is the crash-safety invariant (PLAN 1.4). It is encoded in
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
  | C1 | **freelist_save** (SPEC 05 §4): write this txn's freed pages into the GC DB, dirtying GC pages into the dirty set (loop-until-stable, SPEC 05 GC-11). | H0 | `N−1` (all changes still in dirty set, nothing written). |
  | C2 | **write dirty pages** to their pgnos (`pwrite` each dirty frame / `msync` region under WRITE_MAP). Not yet durable. | **H1** | `N−1` live; new pages sit in free/beyond-HWM slots the `N−1` tree does not reference (TXN-62). Partial/torn data pages are unreferenced garbage. |
  | C3 | **fsync(data)** — flush all data pages (skipped under `NO_SYNC`/`MAP_ASYNC`, SPEC 06 REC-6). | **H2** | `N−1` live; txn `N`'s data fully durable but unreferenced (no meta points at it). |
  | C4 | **write meta** to slot `N&1` (the *older* slot), with `txnid = writer_txnid` and a fresh CRC (SPEC 02 §3.3). Not yet durable. | **H3** | Either `N−1` (meta `N` not yet reached disk, or reached but torn → CRC rejects it → older slot wins) or `N` (meta reached disk intact). Never a torn meta accepted. |
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
  requirement, SPEC 01 §S5). Under D-001 this is the natural single-handle case.
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
> 2. **Writer-quiescence rule TXN-29 (D-005 APPROVED, Quentin 2026-07-16).**
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
| reader table | TXN-14..25 | PLAN 1.8; loom suite L1–L5 (`zerodb-core/src/readers.rs`, `just loom`); stress gate (`zerodb/tests/reader_stress.rs`, `just stress`) |
| memory ordering | TXN-15/17/19/20 | CLAUDE.md (ARM), SPEC 06 |
| nested read txn | TXN-26..36 | SPEC 00 row 16, SPEC 01 §S9; loom L6 (`zerodb-core/src/nested.rs`); fan-out gate (`zerodb/tests/nested_fanout.rs`); differential (`zerodb-oracle/tests/nested_read_differential.rs`) |
| value-borrow contract | TXN-37..49 | SPEC 03 §3/§5/§7, SPEC 01 §S3/§S7 |
| env clone/close/registry | TXN-50..55 | SPEC 00 rows 23–26 |
| commit pipeline | TXN-61..64 | SPEC 06 REC-1..12 |
| PREV_SNAPSHOT | TXN-65..67 | SPEC 01 §S5, SPEC 02 §3.2, SPEC 06 REC-5 |

**Rule count: TXN-1 … TXN-67 (67 normative rules; §4.4 adds sub-rule TXN-18a for
slot release).** TXN-40 (nested write unsupported, D-003) is referenced from
§5/§8 and defined here:

- **TXN-40** — A nested **write** txn is **unsupported** (D-003) and, as
  amended by ADR-0007 Q2/Q3 (ratified 2026-07-16), **unrepresentable in the
  public API**: zerodb exposes **no** nested-write entry point at all — no
  `Env::nested_write_txn`, no `RwTxn::nested` — which is a strictly stronger
  form of the original "clean error" clause (nothing to call, so never a
  panic and never partial state). heed itself keeps `RwTxn::nested`
  `pub(crate)`, so the heed-zerodb adapter (M1.13) likewise surfaces no
  nested-write entry point and no error variant is needed. Should any future
  surface be forced to exist (e.g. an FFI shim), it must return a clean
  documented "unsupported" error mapped through the heed taxonomy (§8.1).
  Zero consumer call sites (SPEC 00 §A). The 1.14 heed-suite gate excludes
  nested-write tests.
  - *Pre-amendment text (superseded): "it returns a clean, documented
    'unsupported' error (mapped through the heed error taxonomy, §8.1), never
    a panic and never partial state."*
