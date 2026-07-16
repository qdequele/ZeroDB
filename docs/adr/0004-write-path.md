# ADR-0004: Write path — single-writer RwTxn, COW dirty store, commit pipeline

- Status: Approved — Quentin, 2026-07-16 (relayed via session lead; standing
  directive "continue until finished"). OQ1–OQ5 answered; resolutions recorded
  at the end of this document.
- Milestone: 1.4
- Date: 2026-07-16

## Context

M1.4 implements the first mutating code path: `Env::write_txn() -> RwTxn`,
put/del/clear/put_reserved, write cursors (`put_current`/`del_current` for the
`iter_mut` passes every consumer uses), COW with split/merge/rebalance, and the
commit pipeline. The behavioral contract is already fixed by the SPECs — this
ADR decides the *concrete structures* and *seams*, not semantics:

- **SPEC 04 §6 (TXN-37..49)** — the value-borrow contract: dirty-page frames
  must never move/reallocate while a `&'txn [u8]` borrow is live; overflow runs
  must be one contiguous `N × psize` frame; `put_reserved` hands out a `&mut`
  slice into a dirty page and must not zero it (TXN-47).
- **SPEC 04 §9 (TXN-61..64)** — commit steps C0–C6 with crash hooks H0–H4 in
  one function; meta slot `N & 1`; fsync(data) strictly before the meta write
  (REC-7); publish order = swap `Arc<Snapshot>` then `commit_point.store(SeqCst)`
  (TXN-18/19); no reader can pin `N` before C5 (TXN-64).
- **SPEC 06 §2 (REC-6)** — the invariant at every hook: crash at H0/H1/H2
  recovers to `N−1`; H3 recovers to `N−1` or `N` (torn meta rejected by CRC,
  intact meta safe because data was fsynced at C3); H4 recovers to `N`.
- **SPEC 03 §5–§10** — COW rules, insert/split (§6.4 median-plus-fit-adjust,
  §6.3 APPEND last-key-compare), write cursors §7, alloc §8, root grow/shrink
  §9, delete rebalance §10. These are transcribed, not re-derived.
- **SPEC 05 (M1.4 seam only)** — allocation = loose pages (GC-7/8) + file
  extend (GC-15/16 `next_pgno`, GC-17 MapFull); **no GC-DB draws** and
  `freelist_save` is a stub at the C1 hook (GC proper is M1.5).

Existing state (M1.1–M1.3): page codecs (`zerodb-core::page`), env open/meta
selection over a `Backing` trait, read-only btree + cursors over `&[u8]`
(whole-map slice), `RoTxn`/`Database` read API, oracle differential with the
IterMut ops gated off and `ZerodbEngine` faking commits by rebuilding the world
through the bulk builder. `zerodb-core` is `#![forbid(unsafe_code)]` and must
stay so (the dirty store below is pure safe Rust).

Prior art consulted clean-room (algorithms only, no transliteration): LMDB fork
`mdb_page_touch`/`mdb_page_alloc`/`mdb_txn_commit`/`mdb_env_write_meta` (sorted
dirty list `mt_u.dirty_list` of malloc'd pages, `me_wmutex`, fdatasync-then-meta
ordering); libmdbx `dpl` (sorted dirty-page vector with spill thresholds, which
we deliberately do not copy — spilling is a large-txn optimization out of
Phase-1 scope).

---

## D1 — Dirty-page store

**Options.**

- **A — `HashMap<u64 /*pgno*/, DirtyPage>` with boxed frames** (chosen):
  `enum DirtyPage { Tree(Box<[u8]>), OverflowRun(Box<[u8]>) }` — a tree frame
  is exactly `psize` bytes; an overflow run is one contiguous `n_pages × psize`
  allocation keyed by its **head** pgno (interior pgnos of a run get no map
  entry; lookups of an interior page cannot occur — runs are only ever read
  through their head, SPEC 02 §5).
- **B — page-frame pool/arena** (LMDB-ish malloc pool, libmdbx-ish dpl): a slab
  of `psize` frames with a free list, plus a sorted `(pgno, frame_idx)` vector.
  Faster alloc and natively sorted for commit write-out, but more code, an
  unsafe-free implementation is awkward (self-referential indices), and the
  performance win is a Phase-3 concern.
- **C — `BTreeMap<u64, Box<[u8]>>`**: natively pgno-sorted iteration for C2,
  but O(log n) on the *hot* path (every page resolution during tree ops checks
  the dirty store first). Commit-time sorting is the rare event; lookups are not.

**Decision: A.** `Box<[u8]>` gives TXN-41 for free: a boxed slice never
reallocates, and rehashing/growing the `HashMap` moves only the `Box` handle
(a pointer), never the pointee — exactly TXN-44's "the index may move, frame
contents may not". Contiguity for overflow runs (TXN-41) is the single
`Box<[u8]>` of `n × psize` bytes, so BIGDATA reads and `put_reserved` (TXN-47)
each hand out one straight slice. At C2 the pgnos are collected and sorted once
(`O(D log D)`) for ascending-pgno write-out. Frames freed within the txn keep
their allocation until txn end or a legitimate `&mut`-boundary reuse (TXN-43);
in practice M1.4 never drops a frame mid-txn — loose-page reuse rebinds an
existing frame to a new pgno (GC-8), which is a map remove+insert of the same
`Box`, address-stable.

**Aliasing story (why this is sound safe Rust).** `RwTxn` owns the store.
Reads go through `&self` and return `&'txn [u8]` reborrows *into* a boxed frame
(or into the mmap for untouched pages — TXN-38; the caller cannot tell, and
does not need to). Mutations take `&mut self`. The borrow checker therefore
enforces TXN-39 exactly (heed's own model: `get` = `&Txn`, `put` = `&mut Txn`):
no read borrow can span a mutation, so in-place edits of already-dirty frames
(TXN-42) and loose-frame reuse (TXN-43) can never alias a live borrow. No
`unsafe`, no raw pointers; `zerodb-core` keeps `#![forbid(unsafe_code)]`, and
the TXN-49 miri suite exercises get-then-put, reserve-then-mutate, and
frame-stability-across-index-growth on this store directly.

## D2 — Write-txn API shape

**Options.**

- **A — separate read implementation duplicated on `RwTxn`**: re-implement
  get/cursor logic against the dirty store. Rejected: two btrees to keep in
  sync; M1.9's nested readers would need a third.
- **B — a page-resolution trait threaded through the btree** (chosen).

**Decision: B.** Introduce in `zerodb-core::btree` a small trait (name
indicative):

```rust
pub trait PageSource {
    fn page(&self, pgno: u64) -> Result<&[u8]>;      // psize bytes
    fn overflow(&self, head: u64, n: u64) -> Result<&[u8]>; // n*psize, contiguous
}
```

`Tree`/`Cursor` (M1.3, currently over a raw `&[u8]` map slice) become generic
over `S: PageSource`. `RoTxn`'s source resolves from the mmap; `RwTxn`'s source
checks the dirty store **first**, then falls back to the map. This is also the
exact seam M1.9 needs (a nested reader is "the writer's source, read-only") and
M1.10 needs (WRITE_MAP swaps the backing, TXN-46). The M1.3 read code paths are
refactored, not rewritten; the read differential re-run proves no regression.

**`RwTxn` shape** (`zerodb-core::rwtxn`, re-exported through `zerodb`):

```rust
pub struct RwTxn<'env> {
    env: &'env EnvInner,
    _write_guard: MutexGuard<'env, ()>,   // TXN-6: held for the txn's life
    txnid: u64,                            // last_committed + 1 (TXN-2/56)
    dirty: DirtyStore,                     // D1
    freed_pgs: Vec<u64>,                   // GC-6 (recorded; stub-saved, D6)
    loose_pgs: Vec<u64>,                   // GC-7/8
    next_pgno: u64,                        // GC-15; meta persists last_pg
    main_db: DBRecord,                     // working roots/stats (TXN-56)
    free_db: DBRecord,
}
```

Surface on `Database` (heed-shaped, SPEC 00 rows 31–38): `put`,
`put_with_flags`, `put_reserved(key, len, f)` (closure over a `ReservedSpace`
wrapping `&mut [u8]` into the dirty frame, not zeroed — TXN-47), `delete`,
`delete_range`, `clear` — all `&mut RwTxn`; every read op from M1.3 also
accepts an `RwTxn` via the `PageSource` genericity. `RwTxn::commit()` (D3),
`abort()`, and `Drop` = implicit abort (TXN-59/60: drop dirty set, no disk
change, release mutex).

**Write cursor**: `RwCursor<'a>` created from `&'a mut RwTxn` (so exactly one
exists at a time — see D5 on cursor fix-up). `get_current`/movement take
`&self`-of-cursor and return borrows tied to that borrow; `put_current`,
`put_current_with_flags` (the `_with_options` codec swap is heed-adapter
sugar over it), `del_current` take `&mut self`-of-cursor. This reproduces
heed's borrow discipline one level up: yielded `&[u8]` cannot span a
`*_current` call. `iter_mut`/`prefix_iter_mut`/`rev_*` in the public crate are
adapters over `RwCursor` exactly as `RoRange` wraps the read cursor.

`Err(KeyExist)` from NO_OVERWRITE carries enough to expose the existing value
(SPEC 01 §S2): the engine method returns the conflicting value as a borrow in
the error path (`Result<PutOutcome<'txn>>` internally; the heed adapter maps it
onto heed's signature at M1.13).

## D3 — Commit pipeline: one function, injectable hooks, fsync choice

**One function.** `RwTxn::commit()` calls a single
`commit_pipeline(&mut self) -> Result<()>` in `zerodb-core` containing the
C0–C6 steps of TXN-61 in textual order, with the hook invocation between each
step. No step is factored into a place where reordering is possible without
touching this one function. Crash-safety restated per cut (REC-6): after C1
nothing is on disk (recover `N−1`); after C2 only pages the live `N−1` meta
does not reference were written (TXN-62 — in M1.4 trivially true: every written
pgno is beyond the old `last_pg` or loose, D6) so torn data is unreferenced
garbage (recover `N−1`); after C3 txn data is durable but unreferenced (recover
`N−1`); after C4 the meta is single-page, CRC-guarded — torn ⇒ rejected ⇒
`N−1`, intact ⇒ `N` is safe *because C3 already ran* (REC-7); after C5 `N` is
durable; C6 publishes in-memory only.

**Hook mechanism — options.**

- **A — `#[cfg(any(test, fuzz))]` hook trait**: zero release cost, but the
  crash-tested pipeline is *not the shipped pipeline* (different code under
  cfg), and M1.11's child-process kill harness (REC-17) runs real binaries that
  would need the cfg propagated across crates.
- **B — always-compiled hook field** (chosen): `EnvInner` carries
  `commit_hooks: Option<Arc<dyn CommitHook>>` (a `#[doc(hidden)]` setter;
  `trait CommitHook: Send + Sync { fn at(&self, h: HookPoint); }`,
  `enum HookPoint { H0, H1, H2, H3, H4 }`). Default `None`; the pipeline does
  `if let Some(h) = ... { h.at(H1) }` between steps. Cost: five branches per
  commit, adjacent to two fsyncs — unmeasurable.

**Decision: B.** Tested code = shipped code; M1.11's harness (both mechanisms)
and M1.4's own smoke test install a hook that panics/aborts/SIGKILLs at a
chosen point with no feature-matrix divergence.

**fsync primitive.** C3 and C5 use `File::sync_data()` on the single data-file
fd. Justification: on Linux (primary target) this is `fdatasync`, which flushes
data *and* the file-size metadata required to read that data back — sufficient
even for commits that grew the file (REC-14/GC-28), and it skips the mtime
flush `sync_all` would add per fsync, twice per commit. On macOS (dev only)
Rust maps `sync_data` to `fcntl(F_BARRIERFSYNC)` — an ordering barrier rather
than a full flush; ordering is what REC-7 needs, and macOS is not a durability
target (flagged as open question OQ3). `NO_SYNC`/`NO_META_SYNC` routing is
M1.10; M1.4 always runs both syncs.

**Meta write (C4).** A positioned `write_page` of the full `psize` meta page to
slot `writer_txnid & 1` through the fd (never through the read-only map), body
per SPEC 02 §3 with `last_pg = next_pgno − 1` (GC-15), both txnid copies, fresh
CRC32C over `[0,168)`, zeroed tail. C5 = `sync_data` again.

**Publish (C6) and the snapshot cell.** M1.4 must introduce the
published-snapshot object *now*: after the first real commit, new `RoTxn`s must
see the new roots, and they must never re-read a durable meta page (TXN-10.3 —
slot `t & 1` gets overwritten by commit `t+2`). Full TXN-18 specifies a
lock-free ArcSwap-style cell, but that belongs with the reader table (M1.8,
loom-tested, the one sanctioned unsafe zone). **M1.4 placeholder:** `EnvInner`
holds `Mutex<Arc<Snapshot>>` (`Snapshot { txnid, main_db, free_db }`, immutable)
plus `commit_point: AtomicU64`. C6 does, in TXN-19 order: (1) swap the new
`Arc<Snapshot>` under the mutex, (2) `commit_point.store(writer_txnid, SeqCst)`
— SeqCst per TXN-17/19's StoreLoad pairing; the ordering discipline is real
from day one even though M1.4 readers also take the (nanosecond-scoped) mutex
to clone. TXN-9's "readers never block" is formally deferred to M1.8, which
replaces only the cell's *implementation*, not the publish order. Safe code
only; no new dependency (`arc-swap` stays off the allowlist). `RoTxn::read_txn`
switches from `EnvInner::meta()` (open-time, now stale-able) to cloning the
snapshot cell.

## D4 — COW mechanics, allocation, and the map-growth story

**COW (SPEC 03 §5, cited not re-derived).** First touch of a committed page:
`allocate(1)` a new pgno, memcpy the source (map or frame) into a fresh boxed
frame, stamp `txnid`, insert into the dirty store, push the old pgno onto
`freed_pgs` (GC-6); parent-chain dirtying is top-down along the descent path
(§5.3), ending in the working `DBRecord.root`. Already-dirty pages
(`dirty.contains(pgno)`) are edited in place (§5.2) — sound because every edit
is behind `&mut` (TXN-42). Overflow values are COW'd as whole runs (§5.5): a
BIGDATA rewrite frees the whole old run and allocates a new one.

**Allocation (M1.4 subset of GC-16).**

```
allocate(n):
    if n == 1 && let Some(p) = loose_pgs.pop(): return p        # GC-8 fast path
    # NO GC-DB draw in M1.4 (M1.5)
    if next_pgno + n > map_size/psize: return MapFull            # GC-17 predicate
    p = next_pgno; next_pgno += n; return p                      # extend
```

Loose tracking is trivial (a page is loose iff its pgno was minted by this
txn's `allocate`, i.e. `> old last_pg`, or already loose, and then freed), and
the GC-10 trailing shrink (`next_pgno` rollback for never-written tail loose
pages) is included in M1.4 because it only touches `next_pgno`/`loose_pgs`, not
the GC DB — it keeps file-size parity with LMDB honest from the start.

**Map growth — options.**

- **A — map exactly the file length; remap on growth**: requires a
  map-generation scheme (readers hold `Arc<Mmap>` of the old generation) and a
  remap point that cannot invalidate outstanding `&[u8]` — heavy machinery that
  TXN-37 ("the mmap is never unmapped or moved while any txn is live") exists
  to avoid.
- **B — map the full `map_size` once at open; never remap** (chosen; LMDB's own
  scheme). `mmap` of length > file length is valid; pages wholly beyond EOF
  SIGBUS *only if touched*. No read path can touch them: readers only follow
  pointers from a committed snapshot, and REC-14/GC-28 guarantee every page a
  durable meta references was written (extending the file via `pwrite`) and
  fsynced before that meta existed. The writer never reads its own new pages
  from the map at all — the dirty store is authoritative for every page the txn
  has touched (TXN-38), which is precisely how a growing writer reads pages the
  file does not yet contain.

**Decision: B.** Concretely: `open_or_create` maps
`max(map_size, file_len)` rounded up to the OS page size (never assume 4 KiB —
ARM 64K pages), instead of M1.3's file-length map. Since `map_size` is fixed
for the env's life in Phase 1 (auto-geometry is 3.2), the base address is fixed
for the env's life: TXN-37 holds trivially and there is **no remap event in
Phase 1 at all**. Interaction with D-006 noted in OQ2. `zerodb-io` gains
`sync_data` plumbing and beyond-EOF mapping; its single `// SAFETY:` comment is
extended to state the beyond-EOF access rule above.

## D5 — Split/rebalance/flags: transcribe SPEC 03, plus the one new decision

The algorithms are **not redecided here**; the implementation transcribes, with
each function citing its section: insert + leaf split §6.2, split point =
median-then-fit-adjust with the `s`-belongs-to-right tie-break §6.4 (feasibility
`used = Σ(cell+2) ≤ psize − HEADER_SIZE` on both sides; termination argument as
written), branch split rises-and-removes the median §6.5, APPEND = last-key
compare only with the force-`s = nkeys` append split §6.3 (equal-to-last ⇒
`KeyExist`; cursor position irrelevant even via `put_current` — oracle-pinned,
§7), replace-in-place same-size else delete+reinsert §6.1, delete rebalance
with borrow-else-merge, left-absorbs-right, cascading, 25 % trigger vs
`min_keys` guarantee §10/INV-8, root grow/shrink §9, key validation per the
§2.1 table (writes reject empty and >511 up front; reads reject only empty).
Put flags per SPEC 01: §S1 APPEND, §S2 NO_OVERWRITE returns the existing value
with `KeyExist`, §S3 RESERVE returns the write handle and never zeroes.

**The one M1.4-specific decision — cursor fix-up scope (SPEC 03 §5.4).** LMDB
tracks and repairs *all* sibling cursors after a split/merge because C allows
many live cursors in one write txn. Under D2's borrow model, **at most one
cursor can exist across a mutation** — mutations reach the tree either through
`&mut RwTxn` (no cursor can be borrowed simultaneously) or through the single
`RwCursor` holding `&mut RwTxn` exclusively. Fix-up therefore reduces to
repairing the *acting cursor's own* `page[]/ki[]` stack after its own
split/merge/COW (so `del_current`-then-`next` lands on the successor, §7).
Multi-cursor tracking infrastructure is deliberately **not built**; if Phase 2
ever exposes concurrent write cursors, that is a new ADR. This is a
simplification the type system makes sound, not a divergence — observable
behavior (oracle: interleaved `iter_mut` mutate-and-continue sequences) must
still match LMDB, and the differential gate proves it.

## D6 — Scope fence and oracle plan

**M1.4 does NOT include** (each has its own milestone/ADR):

- GC-DB reads or draws, `freelist_save` proper, drains (M1.5). C1 in the
  pipeline calls `freelist_save(txn)` which in M1.4 is a **stub**: it performs
  the GC-10 trailing-loose shrink and otherwise returns without writing the GC
  DB. `freed_pgs` is faithfully accumulated so M1.5 changes only the stub body.
  **Consequence, stated honestly:** pages freed by M1.4 commits are leaked on
  disk (neither reachable nor GC-listed — INV-10 is violated by M1.4-era
  images until M1.5). The M1.4 invariant walk therefore checks the tree
  invariants (INV-1..9, 11–13, 16–21) and *not* reachability-xor-freeness;
  file-size parity bands are M1.5 acceptance, not M1.4.
- Reader table (M1.8): `oldest_reader()` degenerates to `writer_txnid − 1`
  (TXN-21) — moot anyway with no GC draws. The snapshot cell ships as the D3
  placeholder.
- Nested read txns (M1.9), nested writes (D-003, TXN-40).
- WRITE_MAP, durability flags NO_SYNC/NO_META_SYNC/MAP_ASYNC, RDONLY-env write
  rejection matrix (M1.10) — M1.4 is heap-frames + pwrite + full sync only.
- Named DBs/catalog (M1.6): main DB only, as in M1.3.
- The full crash harness (M1.11) — but the hooks it needs ship now (D3).

**Oracle plan.** `ZerodbEngine` drops rebuild-world-on-commit: `BeginRw` opens
a real `RwTxn`, mutations hit the real write path, `Commit` runs the real
pipeline, `Abort` drops. The `IterMut*` ops (`put_current`, `del_current`,
mutate-while-iterating sequences) flip on in `driver::classify` +
`implements()`, symmetrically with `LmdbEngine`. The existing read surface
stays differential, so the whole M1.4 surface — writes, write-cursor ops,
in-txn reads through dirty frames, commit/abort visibility, reopen — is
covered by `diff_ops` fuzz + a new write-workload proptest (values 0 B–16 MB
per PLAN; the harness's 64 MiB map and 1/16 large-value bias from M1.3 get a
dedicated large-value differential test so 16 MB runs are exercised without
starving throughput). After every fuzz/proptest case: run the invariant walk
(scoped per the fence above) on the committed image. The M1.2 FORK-1 guard
(`KnownForkBug` skip for APPEND-after-clear) stays in place and symmetric.

## D7 — Risk register (top 3 traps and the test that guards each)

1. **Borrow-contract violation via frame movement** — e.g. an overflow run
   stored as per-page frames (breaking TXN-41 contiguity), or a "clever" arena
   that reallocates. Guard: the TXN-49 miri suite (get-then-put,
   reserve-then-split, frame-address stability across dirty-index growth,
   multi-page overflow readback through a dirty run), running under
   `cargo miri test -p zerodb-core` since the store is I/O-free safe code; plus
   the read differential re-run over in-txn reads.
2. **Split off-by-one vs free-space accounting** — `used(L/R) ≤ C` feasibility
   vs the page's `upper − lower ≥ cell + 2` insert check disagreeing at the
   boundary (the classic +2-pointer error), or the §6.4 tie-break landing the
   boundary cell on the wrong side. Guard: deterministic boundary tests at
   psize 4096 *and* 65536 that construct exact-fit pages (extending M1.1's
   page-full off-by-one suite to the split path), a proptest asserting
   post-split INV-9 + both-sides-fit + separator correctness for arbitrary
   `(nkeys, newindx, sizes)`, and page-count tracking in the differential.
3. **Commit-pipeline ordering drift** — a refactor that lets the meta write
   precede the data fsync, publishes the snapshot before C5 (TXN-64), or writes
   a pgno the live meta references (TXN-62). Guard: an M1.4 hook smoke test
   (before M1.11): install a `CommitHook` that kills a child process at each of
   H0..H4 in turn, reopen, assert the REC-6 row (recovered txnid ∈ {N−1, N} per
   hook, tree walk clean); plus a debug assertion in C2 that every written pgno
   is `> old last_pg` or was loose (the M1.4 form of TXN-62).

(Runner-up, watched but not top-3: cursor self-fix-up after `put_current`
triggers a split — covered by the IterMut differential ops, which is why they
flip on in this milestone and not later.)

## Consequences

- `zerodb-core` gains `rwtxn` (dirty store, allocation, commit pipeline) and a
  `PageSource` refactor of `btree`; stays `forbid(unsafe_code)`, miri-clean on
  the whole write path (I/O reaches it via `Backing`/write-backend seams).
- `zerodb-io` gains `sync_data` helpers and maps `map_size` (not file length)
  — the one unsafe block's SAFETY comment is extended for beyond-EOF mapping.
- `EnvInner` gains: write mutex, snapshot cell (placeholder) + `commit_point`,
  `commit_hooks`. `EnvInner::meta()` open-time snapshot is demoted to seeding
  the cell (TXN-18 "meta page read once, at open").
- M1.5 lands inside the `freelist_save` stub and `allocate` step 2; M1.8
  replaces the snapshot-cell internals; M1.9 reuses `PageSource`; M1.10 swaps
  the frame backing; M1.11 consumes the hooks. No format change; SPEC 02 §3's
  interface note (`next_pgno` in memory, `last_pg` persisted) is honored as
  GC-15 already fixed.
- SPEC updates in the same change if implementation clarifies behavior
  (CLAUDE.md rule 3): expected touch points are SPEC 03 §5.4 (single-cursor
  fix-up scope) and SPEC 04 §6 (if miri forces any frame-lifetime refinement).

## Open questions — RESOLVED (Quentin, 2026-07-16)

1. **OQ1 — snapshot-cell placeholder (D3): APPROVED.** `Mutex<Arc<Snapshot>>`
   with the TXN-19 publish order ships in M1.4; TXN-9's lock-free guarantee
   lands with the reader table in M1.8. Correctness of the publish order is
   what matters now.
2. **OQ2 — map full `map_size` at open (D4/B): APPROVED.** Resolve D-006 as
   proposed: round the **mapping length** up to the OS-page multiple
   internally; never change the reported/persisted `map_size` or the GC-17
   MapFull predicate. D-006 flipped to APPROVED in `docs/DIVERGENCES.md`
   citing this ADR.
3. **OQ3 — macOS fsync (D3 amendment): use std's `sync_data`/`sync_all`
   semantics as-is.** No F_BARRIERFSYNC special-casing: the pipeline calls
   `File::sync_data()`, which is `fdatasync` on Linux (primary target) and
   std's full-flush path on macOS (correct, slower, fine for a dev platform).
   Recorded as the D3 amendment; the earlier F_BARRIERFSYNC discussion in D3
   is superseded by this resolution.
4. **OQ4 — M1.4→M1.5 freed-page leak window (D6): ACCEPTED.** Explicitly
   scoped; INV-10 is excluded from the invariant walk on M1.4-era images;
   M1.5 is the very next milestone and lands inside the `freelist_save` stub.
5. **OQ5 — always-compiled commit hooks (D3/B): APPROVED.** One predictable
   branch per hook site is noise; tested code = shipped code wins.
