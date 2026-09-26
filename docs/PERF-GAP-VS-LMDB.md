# Performance inventory: every time & memory cost vs LMDB

Status: full inventory, 2026-07-21 (second, deeper pass — supersedes the
first "LMDB techniques" listing; that pass compared against LMDB's design
choices, this one also sweeps zerodb's own hot paths for every copy,
allocation, redundant computation, lock, and RAM sink).

**Measuring these items.** `just bench` runs a microbench *ladder* built so
that each rung isolates one of the mechanisms inventoried below; `just
bench-report` prints the per-rung LMDB-vs-ZeroDB ratio and each rung's delta
against its family's base rung. [`BENCH-MAP.md`](BENCH-MAP.md) maps rungs to the
A/B/C items on this page — start there before picking a lever, since two blind
picks were falsified during the July 2026 campaign.

Every claim is cited. LMDB side: the vendored fork (`lmdb-master-sys 0.2.6`,
`liblmdb/mdb.c` — the tree the oracle links; reading it for techniques is
permitted by CLAUDE.md rule 4, nothing is transliterated). ZeroDB side:
file:line in this repo at the time of writing.

**Unsafe policy note.** CLAUDE.md already sanctions unsafe in
`zerodb-core::page` and `zerodb-io` ("mmap access and page casting") and
prescribes the mechanism ("explicit offsets + `read_unaligned`"). Items marked
*unsafe (sanctioned)* are permitted today and simply not taken — Phase 1 chose
`#![forbid(unsafe_code)]` in the page codec as a correctness-first default.

## Top of the stack — what one `get` on a named DB actually costs

Reads through the adapter on a named DB (how milli and hannoy do ~all reads):

1. adapter enum dispatch + key-size boundary checks — cheap.
2. `RoTxn::record_for` (`rotxn.rs:211`) → `EnvInner::named_name`
   (`env.rs:632`): **registry mutex lock + `Box<[u8]>` clone of the DB name**
   (heap alloc + memcpy) — *per read op*.
3. `resolve_named_record` (`rotxn.rs:92`): **a full catalog descent of the
   main tree** to find the DB's record — *per read op*.
4. The real descent — where each level pays:
   - `PageRef::new` (`page/header.rs:100`): `validate_page_size` re-check +
     header parse, per page load;
   - `BranchRef::new` / `LeafRef::new` (`page/tree.rs:509` / `:161`):
     **every cell on the page validated**, O(num_keys) per page;
   - byte-copy field reads (`page/raw.rs:19`): `u16::from_le_bytes([buf[o],
     buf[o+1]])` — bounds-checked indexing + stack array per field.
5. `Tree::get` (`btree.rs:180`) allocates a fresh `Cursor` whose stack `Vec`
   heap-allocates on first push — one alloc per get.

LMDB's equivalent: `mt_dbs[dbi]` array index, one descent of raw pointer
derefs (`NODEPTR`, mdb.c:1159), `memcmp`. **The named-DB resolution alone
(items 2–3) roughly doubles the tree work per read; validation (item 4)
multiplies the rest.** Together this accounts for the measured get ≈ 5.6×.

---

## A. Read path (time, per operation)

### A1. Named-DB record re-resolved on EVERY read op — **DONE 2026-07-21 (37eb96e)**
**Done:** per-txn `named_memo` on `RoTxn` (a `Mutex<Vec<(dbi, DBRecord)>>` —
uncontended, `Sync`-preserving; soundness: the pinned snapshot's catalog and
the append-only dbi registry are both immutable for the txn's life). RwTxn and
nested txns were already covered by the open-table. Part of the batch that
took hannoy indexing 5–6× → 1.3–1.44×. Original analysis kept below.
**Amendment 2026-09-25: lock-free.** The memo's mutex was still taken on every
read. LMDB indexes a per-txn array (`txn->mt_dbs[dbi]`, sized `me_maxdbs`,
filled lazily through `DB_STALE`). `RoTxn` now does the same: a dbi-indexed
table of write-once `OnceLock<DBRecord>` slots, allocated on the first named
access and sized `min(max_dbs, 256)`. A hit is one `Acquire` load, so milli's
rayon workers sharing a `RoTxn` no longer contend. The mutex `Vec` remains
only for dbis ≥ 256. Bench server (x86-64, 4 KiB): `scan/meta/len` 222 → 92 µs
(**2.46× → 1.01×**), `scan/edge/first_last` 2.38× → 2.13–2.22× (0.89–0.90 of
before). `get/access/hot` is flat, so the rest of the per-get gap is not
handle resolution.
`Database::get/len/is_empty/iter…` on a `RoTxn` all call `record_for`
(`rotxn.rs:501,521,548,557`) which, for a named DB, does the mutex + name
clone + full catalog descent *every time* (`rotxn.rs:211-216`, comment at
`:193` says "borrowed lazily from the env on each access (rather than
cached)"). LMDB resolves a dbi once into `txn->mt_dbs[dbi]`.
**Fix shape:** cache the resolved `DBRecord` per (txn, dbi) — a RoTxn is an
immutable snapshot, so the record cannot change during its life; a small
`Cell`-based memo (same soundness argument as the cursor leaf cache) removes
the catalog descent, the mutex, and the name clone from every read.
No unsafe. Effort: low. **This also multiplies A2–A4** (the catalog descent
pays validation too), so it must land first or it hides the others' wins.

### A2. Eager O(num_keys) page validation on every view construction — **(a) DONE 2026-07-21 (37eb96e; lock-free via A8); (b) open, issue [#21](https://github.com/qdequele/ZeroDB/issues/21)**
**Done (a):** txn-scoped `ValidatedPages` memo — disk bytes validate exactly
once per txn at first map entry; every later view is `new_prevalidated`
(O(1)) or, since A8, `new_trusted` (two raw header reads). Engine-authored
dirty frames trusted by construction (batch 3, same commit).
**Open (b):** the lazy/checked-per-access variant (validate O(log K) per
descent instead of O(K) at first touch) — tracked as issue #21 (`needs-adr`:
it bundles an opt-in trusted-file mode). Superseded in practice by (a); re-open
only if a profile names first-visit validation. Original analysis kept below.
`LeafRef::new`/`BranchRef::new` walk every cell (`page/tree.rs:161-179`,
`:509+`). Cursor iteration no longer repays it (leaf memoized — scan went
29× → 2.18×), but **every descent still does**: root + each branch + leaf,
for gets AND puts (the write path descends through the same code —
`rwtxn.rs` search paths build the same views).
**Fix shape:** (a) per-txn memo of validated upper pages (root re-validated on
*every* op today), or (b) the principled fix: make `key(i)`/`value(i)`
checked (they slice unchecked at `page/tree.rs:205-227`, which is *why* the
eager pass exists) and validate lazily per access — O(log K) instead of O(K)
per page. No unsafe. Effort: (a) low, (b) medium.

### A3. Byte-copy field reads instead of pointer reads — **DONE 2026-07-22 (7848e30)**
(Issue [#20](https://github.com/qdequele/ZeroDB/issues/20) was filed for this
after the work landed — safe to close against 7848e30.)
Was: every u16/u32/u64 field read = bounds-checked indexing into a stack
array + `from_le_bytes` (a u64 = 8 checked indexes + panic paths); every
field of every node of every page. LMDB: direct struct access through
`NODEPTR`, zero copies.
**Done:** `read_*_unchecked` tier in `page/raw.rs` — explicit offsets +
`ptr::read_unaligned`, exactly the pattern the unsafe policy prescribes, in
its sanctioned home. Crate went `forbid(unsafe_code)` →
`deny` + a single `#[allow]` scoped to `mod raw` (plus per-fn allows at the
call sites); every block SAFETY-commented against one contract: view
construction proves (full walk) or inherits (kind-tagged memo hit /
engine-authored dirty frame — batch 3/A8) that all cells lie in bounds.
Converted: `LeafRef`/`BranchRef` `cell_abs`/`node_flags`/`key`/`value`/
`child_pgno` + `leaf_lookup` (the profile's hottest descent loop). Public
accessors now **hard-assert** `i < num_keys` (one predictable branch —
strictly stricter than before, where a bad index could silently read a
garbage in-bounds offset) and do unchecked reads behind it; internal loops
ride the binary-search invariant. `debug_assert!`s keep every contract loud
in test/fuzz builds; miri referees the whole corpus.

### A4. `validate_page_size` re-run on every page load — **DONE 2026-07-21 (37eb96e)**
Was: `PageRef::new` (`page/header.rs:101`) re-validated an env-immutable value
on the hottest path in the engine.
**Done:** `PageRef::new_trusted_psize` — trusts only `psize` (validated once at
env open), keeps every per-buffer check; `btree::load_page` uses it.

### A5. Cursor caches only the leaf; branch levels re-resolved
`Cursor.stack` holds `(pgno, ki)` only; ascend/descend re-load branches
(`btree.rs`). Post-A8 the *revalidation* is gone (memo hit → trusted view, two
raw header reads), so what remains per level is the page lookup + header
parse. LMDB keeps a resolved `MDB_page*` per level (`mc_pg[CURSOR_STACK]`,
mdb.c:1470). Extend the proven leaf-memo pattern per level. No unsafe.
Effort: low. No tracking issue (this ledger is its home); profile-gated.

### A6. Heap allocation per get / per cursor — **DONE 2026-07-22** — issue [#19](https://github.com/qdequele/ZeroDB/issues/19)
Was: `Tree::get` built a `Cursor` with a heap `Vec` stack per call — one
malloc + free per get (the `grow_one` frame in the hannoy-build call tree;
allocator traffic also contended across rayon workers). LMDB cursors live on
the C stack.
**Done:** `PathStack` — inline `[(u64, usize); 32]` + len (LMDB's
`CURSOR_STACK` bound; SPEC 03 §4 "Path bound"), `Vec`-shaped API, overflow
fails typed (`depth_exceeded`), park/resume moves it by value. Cursor
construction is now allocation-free. **In the same batch** (all three named
by the same profile): the descent now resolves each page's bytes **once**
per level (`node_view` — was twice: type dispatch + view construction, each
a dirty-store probe inside a write txn), `Tree::get` reuses the leaf view
the search just cached (was a third resolution), and the rwtxn
`search_path`/`rightmost_path` descents got the same single-resolution
treatment. Referee numbers pending a quiet machine — the first post-batch
hannoy run was discarded as contaminated (interactive load; 8–32 s tail
samples on both sides); correctness gates all green (fmt/clippy/81 suites/
miri/37,891-run differential fuzz 0 divergences/crash 212 cycles).

### A7. Custom-comparator vtable call per key comparison — issue [#14](https://github.com/qdequele/ZeroDB/issues/14)
`KeyCmp::Custom(&dyn Comparator)` (`cmp.rs:160-172`) — indirect call per
comparison on custom-comparator DBs (milli sets one). LMDB uses a plain fn
pointer. Monomorphize the search over the comparator. Effort: low.
(Not to be confused with the memo work stamped below — the old task list
briefly used "A7" for what this ledger numbers A8.)

### A8. Memo probe cost: `Mutex<HashSet>` + SipHash per page view — **DONE 2026-07-21**
Found by profiling, not inspection: with B1/B6 landed, the milli indexing
referee did NOT move (2.68× → 2.81× same-run) — `sample` profiles of both
backends showed milli indexing is **get-bound on both**, and the #2 zerodb
frame was `ValidatedPages::contains` itself: the A2 memo's `Mutex<HashSet>`
probe (lock + SipHash + probe per page view, contended — milli shares one
`RoTxn` across rayon workers). LMDB's equivalent cost is zero.
**Done:** (1) the memo is now an insert-only **lock-free** open-addressed set
of `AtomicU64` slots in geometrically growing `OnceLock`-published levels
(never rehashed; splitmix64 mix; ½ load-factor gate; saturation degrades to
revalidation — the memo can never affect correctness, so every race outcome
is at worst a miss). Orderings: `Release` slot publication pairs with
`Acquire` probes ("validation completed" happens-before any trusting reader —
ARM-safe); justified inline per the atomics policy. `std::sync` directly, not
the loom shim — ADR-0006 scopes the shim to the reader table/nested counter,
and the memo's contract is advisory; a native+miri 4-thread stress test
(kind-tag separation, level growth, races) rides the gate instead.
(2) The memo key now **tags the page kind** (bit 63: leaf/branch), so a hit
hands out `LeafRef::new_trusted`/`BranchRef::new_trusted` — two raw header
reads, zero checks (the old `new_prevalidated` hit path still re-parsed and
re-checked type/reserved-tail/bounds every view; a kind-mismatched lookup now
just misses and revalidates loudly). Dirty-frame views keep the O(1)
`new_prevalidated` checks (batch-3 trust unchanged).
(3) **The real milli lever, found only by reading the write-phase call
tree** (91% of the main thread inside `write_to_db`; `LeafRef::new` ≈ 2,100
of 3,570 samples): `LeafMut::from_valid` / `BranchMut::from_valid` — doc'd
"wrap an already-validated page" — silently re-ran the **full O(`num_keys`)
cell walk on every mutation** (every put wraps its target leaf; parent-chain
touches wrap branches), a batch-3 miss hiding inside the mutable wrapper,
made 4× worse by 16 K parity pages. All 22 call sites audited: every one
passes `self.dirty.bytes_mut(..)` frames (engine-authored by the batch-3
argument) — now O(1) `new_prevalidated`, matching the read-side dirty path.

## B. Write path (time, per operation)

### B1. `RwCursor`: full re-seek + 3 heap copies per step — **DONE 2026-07-21**
Was: `RwCursor` tracked position **by key**: each `next()` ran a fresh descent
(`set_range(k)`), yielded the pair as **owned** `(k.to_vec(), v.to_vec())`,
then cloned the key a third time into `CurPos::At(k.clone())` — and the
adapter's `RwGuts` didn't even use it: every `iter_mut` step was its own
`db.get_greater_than(txn, last)` descent plus a `last = k.to_vec()`.
**Done:** the engine `RwCursor` parks/resumes the read cursor's root-to-leaf
stack (`btree.rs` `SavedCursor`; the stack `Vec`'s allocation is moved in/out,
not reallocated) — an advance is an amortized O(1) stack step (LMDB's
`mc_pg[]`/`mc_ki[]`), yields are lending borrows of the txn's frames (safe at
the engine level; two-phase position-then-materialize through the txn's
validated-pages memo), and the adapter's `RwGuts` now owns one engine cursor
(seeks for all six bound forms + `move_next`/`move_prev`), erasing lifetimes
once at construction (the sanctioned M1.13 clause). Per-step allocations:
zero. **Residual (deliberate):** a mutation (`put_current`/`del_current`/
`put`) drops the parked stack and records the key; the next advance re-seeks
once — LMDB's in-place cursor fix-up avoids that descent. Costs one descent
per *mutation* (was: one per *step* + one per mutation). Revisit only if a
consumer bench still shows it (put_tree path adoption is the escalation);
tracked as issue [#7](https://github.com/qdequele/ZeroDB/issues/7).
`put_reserved`'s inline re-locate second descent has the same shape (noted
below).

### B2. Per-insert owned-cell alloc; splits materialize the whole page — **DONE 2026-07-21 (3c6a064)**
**Done:** end-insert/append split takes a fast path (fresh right page, insert
directly, no copy — LMDB's `newindx == n_old` bias); the general split packs
both frames from cells **borrowed** out of the old frame (removed from the
dirty store as an address-stable `Box`) + the caller's new cell — no
`OwnedLeafCell` materialization. Plain puts insert straight into the frame
(`NewLeafVal`). Original analysis kept below.
Every put builds an `OwnedLeafCell` (heap `Vec`s for key/value) for the new
cell (`rwtxn.rs:1368,1409`); a split calls `extract_leaf_cells` — **every
cell of the page copied into a `Vec<OwnedLeafCell>`** (`rwtxn.rs:1424-1439`)
— plus a `sizes: Vec` collect (`:1526`) and a full page re-encode
(`write_leaf_frame`, `:1462`). LMDB splits by memmoving node pointers between
the two mapped pages. Amortized: ~2 allocs + a page copy per K inserts on top
of B1/B3. No unsafe. Effort: medium.

### B3. Dirty store: HashMap + `Box` per page + commit-time sort + zeroing — **PARKED 2026-07-22 (profile says no)** — issues [#4](https://github.com/qdequele/ZeroDB/issues/4), [#5](https://github.com/qdequele/ZeroDB/issues/5)
Verdict from the post-A8 milli write-phase call tree (the referee that found
`from_valid`): at 1.19× end-to-end, the dirty-store probe does not clear a
60-sample bar in an 8 s window where `search_path` holds ~1,760 — the arena
redesign would chase <5 % while risking the TXN-41 frame-address-stability
contract (the `Box` is load-bearing; B2's split path moreover now *takes
ownership* of frames via `DirtyStore::remove`). Re-open only if a future
profile (EBS commit path included) names it. Original analysis kept below.
`DirtyStore { frames: HashMap<u64, Box<[u8]>> }` (`dirty.rs:31-33`): hash
lookup on every page touch (every read *inside a write txn* goes through
`Source::Writer` → `dirty.bytes(pgno)` first — `btree.rs:75`), a heap alloc
per dirty page (frames zeroed on allocation — no `MDB_NOMEMINIT` analogue),
and `sorted_pgnos()` allocates + sorts a fresh `Vec` **per commit**
(`dirty.rs:109`). LMDB: sorted `MDB_ID2L` array (mdb.c:1345) + a page-buffer
reuse pool (`me_dpages`, mdb.c:1567, 2076-2118) — steady-state zero allocator
traffic, ordered iteration for free.
**Note:** the `Box` is load-bearing for TXN-41 frame-address stability; an
arena/pool preserves that. No unsafe. Effort: medium.
**Amendment 2026-07-22:** the *hash-cost* component of this entry (SipHash
per probe, and two probes per descent level) was eliminated separately
without touching the parked arena/TXN-41 design: issue #9 (splitmix64
`PgnoHasher` on the frames map, the GC `reclaimed` set, and the check
walker's maps) + the `node_view` single-resolution descent (see A6). The
parked scope is now only the allocation/pooling redesign itself.

### B4. One `pwrite` syscall per dirty page at commit; no coalescing — **DONE 2026-07-21 (3c6a064); EBS validation PENDING**
Was: `for pgno in sorted_pgnos { backing.write_at_page(...) }` — one syscall
per dirty page. LMDB's `mdb_page_flush` coalesces contiguous runs into
`iovec[MDB_COMMIT_PAGES]` batches (mdb.c:1613-1617) flushed via `pwritev`.
**Done:** the C2 flush batches contiguous pgnos and `zerodb-io` issues one
`pwritev` per chunk of up to `MAX_IOV` frames (`file.rs`). **Caveat:** the win
this exists for is Graviton + EBS gp3, where per-syscall latency dominates —
the laptop page cache hides it, so it is implemented but *unmeasured on
target*. Same hardware gate as the ~1.8× durable-commit number.

### B5. WRITEMAP copies at commit instead of mutating the map — *unsafe (sanctioned)* — issue [#13](https://github.com/qdequele/ZeroDB/issues/13)
M1.10 implemented WRITE_MAP as commit-time copy (heap dirty store → map at
C2) to keep the value-borrow contract, nested-reader dirty reads, and
abort-by-drop identical. Costs a full memcpy of every dirty page per commit +
the B3 allocation. LMDB with `MDB_WRITEMAP` mutates mapped pages in place and
skips the write entirely (mdb_page_flush early-out). Already flagged in
PROGRESS as "true zero-copy live-map mutation deferred to Phase 3". Effort:
high (abort semantics + borrow contract redesign). Do last, if at all.

### B6. Adapter `ReservedSpace` = alloc + zero + copy — **DONE 2026-07-21**
Was: heed's `MDB_RESERVE` hands the caller a pointer *into the page*; the
adapter handed a **zero-initialized heap buffer** the caller filled and the
engine then copied into the dirty frame — alloc + memset + extra memcpy per
`put_reserved` (milli serializes documents and roaring bitmaps this way).
**Done:** `Database::put_reserved` builds the `ReservedSpace` over the
engine's in-frame slot inside the native fill closure — no buffer, no copy;
only the *unwritten tail* is zeroed (usually 0 bytes — milli fills fully),
preserving the shipped zero-tail contract. Fork-pinned semantics change that
came with it: a failing closure now leaves the entry in place and errors as
`Io` (LMDB cannot un-put a reserve; the pre-B6 adapter wrote nothing and
returned `Encoding` — a real divergence, pinned by the oracle's
`put_reserved_failing_closure_leaves_entry_parity`). **Residuals** (tracked
as issue [#10](https://github.com/qdequele/ZeroDB/issues/10)):
- ~~second descent~~ **RESOLVED 2026-07-22:** `ReserveLoc::Inline` now
  carries the settled `(leaf, slot)` from every no-split insert arm (the
  common case — one descent per `put_reserved`, also taken by the GC's
  per-commit PIL writer); a split returns `None` and re-locates by key.
  Debug builds cross-check the carried position against a real search on
  every use, so the whole test battery referees the proof.
- The *cursor* reserved put (`put_current_reserved_with_flags`) keeps its
  heap-buffer emulation: no consumer calls it, and exact parity needs an
  engine-level cursor reserve — see **D-015** (its closure/fill semantics
  measurably diverge from the fork; recorded, pending maintainer decision
  rather than built unilaterally).

### B7. Freelist bookkeeping allocation churn — issue [#29](https://github.com/qdequele/ZeroDB/issues/29)
`drains: BTreeMap<u64, Vec<u64>>` + `reclaimed: HashSet` + PIL decode `Vec`
per GC entry touched (`rwtxn.rs:924-998`); `freelist_save` fixed-point loop
per commit. LMDB: sorted `MDB_IDL` arrays manipulated in place. Effort:
medium. Only matters under GC churn; measure before redesigning.

**Amendment 2026-09-26: the front draw is O(1).** Every single-page GC draw
takes the entry's smallest id (GC-19), and `Vec::drain(0..1)` memmoved the
whole remainder each time, so consuming an entry of `L` ids cost O(L²). A
drain entry is now the decoded ids plus a consumed-prefix offset: a front draw
advances the offset, and a mid-entry run removal (rare) still memmoves. The
rewritten free-list value at commit is exactly the live remainder, so the file
is byte-identical. LMDB avoids the same cost by keeping its reclaimed list
reverse-sorted and cutting from the tail. The draw arms (`gc_reclaim`,
`save_pool_draw`, `loose_run`) are kept `#[inline(never)]` so `allocate`'s hot
shape (the loose pop) does not move; the first try without that was reverted
because it re-laid-out neighbouring write code (B13 point 3). Bench server,
x86-64, 4 KiB: `put/gc/drain_big` 2.21× → 1.68× (CGU16) and 2.23× → 1.70×
(CGU1); the other 17 `put/*` and `del/*` rungs flat in both builds.

### B8. Delete-time rebalance moves entries cell-by-cell — **DIAGNOSED + CONSUMER-CONFIRMED 2026-09-10, open**
The largest steady-state gap the microbench ladder finds, and the only cluster
in this inventory that was never predicted from reading `mdb.c`. On the first
ladder run (`benches/results/2026-09-10-engine-ladder-macos.md`, Apple M1 Pro,
16 K pages both sides):

| rung | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| `del/bulk/half` (per-key, tree stays populated) | 11.3 ms | 22.9 ms | **2.02×** |
| `del/bulk/all` (per-key, down to empty) | 20.5 ms | 40.4 ms | **1.97×** |
| `del/range/half` (one `delete_range` cursor walk) | 8.7 ms | 23.7 ms | **2.74×** |
| `del/churn/reinsert` (delete + re-insert, 3 rounds) | 13.1 ms | 21.5 ms | 1.64× |

**Ruled out by the ladder.** `churn/reinsert` is the *least* affected rung, so
this is not freelist/GC churn (that is B7, and it is not what fires). Commit
write-out is not it either: deleting with the commit dropped instead of taken
costs the same (21.2 ms vs 21.3 ms). The read side is not it: the range **scan**
that `delete_range` performs first is *faster* on ZeroDB (1.61 ms vs 2.12 ms).

**Diagnosis (2026-09-10).** Ablation on 50 K entries / 16 K pages, deleting the
first 25 K keys, best of 5, per operation:

| step | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| same-size overwrite — descent + COW only, no cell churn | 274 ns | 393 ns | 1.43× |
| delete — the same, plus cell removal and rebalance | 390 ns | 864 ns | 2.22× |
| **difference = the removal + rebalance step** | **116 ns** | **471 ns** | **4.1×** |

So the shared descent/COW path costs ZeroDB +119 ns (secondary), and the
**removal + rebalance step costs +355 ns — 75 % of the whole gap.**

**The mechanism, counted.** Instrumenting the page primitives: 25 000 logical
deletes issue **63 542 `remove_cell` calls (2.54 per delete)** and **44 041
`insert_pointer` calls (1.76 per delete)**. A delete needs one removal and zero
insertions. The surplus is `rebalance` → `borrow_entry`, which round-trips each
moved entry through a full `remove_cell` + `insert_cell` page rewrite, and fires
on nearly every delete once a leaf sits at the threshold (a borrow moves exactly
one entry, so the next delete drops the page under 25 % again). Each of those
primitives runs the O(`num_keys`) pointer-adjust loop: **4 990 806 iterations
for 25 000 deletes ≈ 200 per delete** at 78.5 keys/page average. LMDB performs
the same *algorithm* — `mdb_node_move` then `mdb_node_del`, identical
thresholds (`FILL_THRESHOLD` 250 ‰, `minkeys` 1 leaf / 2 branch, verified
against `mdb.c`) — but moves node bytes page-to-page, where `borrow_entry`
materializes an `OwnedLeafCell` (`key.to_vec()` + `val.to_vec()`) in between.

Suppressing rebalance confirms it: deleting every 8th key instead (fill never
approaches the threshold) drops ZeroDB to **1.00 `remove_cell` and 0.00
`insert_pointer` per delete**, and the ratio from 2.18× to 1.47×.

**Falsified — do not retry.** Converting `remove_cell`'s pointer-adjust loop to
a fused single pass over a bounds-narrowed slice (LMDB's loop shape) made it
**slower**: 23.8 ms vs 21.3 ms. Per-element indexing loses to `copy_within`.
The cost is the *number* of cell operations, not the per-element checking, so
the lever is doing fewer of them — e.g. moving several entries per borrow, or a
`move_entry` primitive that writes the destination cell directly from the source
page without the owned round-trip. Both are real design changes: **ADR first.**

**Confirmed at consumer level, same day.** Meilisearch v1.53.1,
`settings-add-remove-filters` on 150 k documents, `ROUNDS=2` alternated
(`benches/results/2026-09-10-meilisearch-delete-heavy-macos.md`): the delete
phase — `apply_index_operation` **self** time, which is where the
un-instrumented `delete_old_fid_from_facet_databases` lands — goes
332.8 ms → 805.1 ms = **2.42×, +472 ms**, while `write_db::all` is **0.96×**
and `extract` **0.96×** (ZeroDB faster). The 2.42× lands inside this rung's
1.97–2.74× microbench band: the ladder predicted the consumer number. Bounded
blast radius — 1.07× end-to-end on the workload built to trigger it — but
linear in entries deleted, so a 10 M-document index pays proportionally.
The 2026-09-09 insert-only bench could not have caught this: the path fires
only on settings changes that *remove* an attribute.

**Reproduce:** `just bench del` for the rung;
`WORKLOADS="workloads/settings-add-remove-filters.json" ROUNDS=2
scripts/consumer.sh bench` for the consumer figure.
[`BENCH-MAP.md`](BENCH-MAP.md) says what each rung isolates. Effort: medium.
No tracking issue yet.

### B8a. `RwCursor::del_current` re-descends twice per entry — **DONE 2026-09-10**
Separate from B8 and additive with it. Deleting a 25 K span three ways:

| path | LMDB | ZeroDB |
|---|---:|---:|
| `Database::delete` per key | 9.54 ms | 21.3 ms |
| `Database::delete_range` (the public API) | 6.85 ms | 22.2 ms |
| `range_mut` + `del_current` (the cursor loop) | 6.87 ms | 29.2 ms (**4.2×**) |

LMDB's cursor delete is *cheaper* than its point delete (6.87 vs 9.54 ms) — the
cursor keeps its path and `mdb_cursor_del` sets `C_DEL` so the following `NEXT`
resumes in place. ZeroDB's is *dearer* than its own point delete (29.2 vs
21.3 ms): `del_current` (`rwtxn.rs`) clones the current key, calls
`delete_tree` — a fresh root descent — then sets `saved = None` and
`pos = AfterDelete(key)`, so the following `move_next` does `set_range(k)`, a
**second** full descent. Two descents and a key clone per entry, where LMDB does
neither.

`Database::delete_range` sidesteps this by collecting every key into a `Vec`
first and issuing point deletes, which is why it tracks the point-delete number
rather than the cursor one — at the cost of materialising the whole key set.

**This is not a hypothetical API.** `del_current` appears at **nine call sites
in milli, four of them in the current indexer** (`update/new/indexer/mod.rs`) —
`IndexingStep::DeletingFromAllFilters`, `delete_old_fid_word_count_docids`,
`words_prefix_docids`, `facet/new_incremental`, and
`post_processing::prefix::delete_prefixes`, all written as the idiomatic
`prefix_iter_mut` + `del_current` loop. Measured on the real server
(`benches/results/2026-09-10-meilisearch-delete-heavy-macos.md`),
`post_processing::prefix::delete_prefixes` is **3.63×** (3.9 → 14.1 ms) and
reproduces at 3.65× / 3.60× across both alternated rounds — against the 4.2×
microbench figure.

Fixing this is the `C_DEL`-equivalent: let `del_current` leave the parked stack
valid at the successor instead of discarding it.

**DONE 2026-09-10.** `del_current` now (1) deletes at the path the cursor is
already holding — the new `RwTxn::delete_at_path`, so the delete costs no
descent of its own — and (2) parks on the slot the delete vacated, which now
holds the successor, so a following `next` *settles* there
(`Cursor::settle`, LMDB's `C_DEL` shape) instead of re-seeking. `rebalance`
now reports whether it changed the tree's shape; on a borrow/merge/root-shrink
the path is discarded and the old re-seek runs, which §5.4a sanctions as a
correct repair. Parking on the vacated slot rather than one before it is what
makes it fire on milli's drain, where the deleted index is always 0.

The vacated slot stays invisible to everything but `next`: `current_key`
reports `None` there, so a second `del_current` or a `put_current` remains the
no-op it was — otherwise it would have silently deleted the *successor* — and
`prev` discards the path and re-derives, exactly as before.

**Three-column referee** (`just bench del`, Apple M1 Pro, 16 K pages, medians;
the `del/cursor/drain` rung was added with the fix — the pre-existing `del/*`
rungs all reach the tree *by key*, so none of them could show this at all):

| rung | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| **`del/cursor/drain`** (`range_mut` + `del_current`) | 9.22 ms | 31.19 ms (**3.38×**) | **25.35 ms (2.46×)** |
| `del/range/half` — control, by key | 9.46 ms | 24.31 ms | 25.12 ms |
| `del/bulk/half` — control, by key | 12.16 ms | 24.75 ms | 23.58 ms |
| `del/bulk/all` — control, by key | 21.48 ms | 42.56 ms | 42.88 ms |

The by-key controls do not move, which is the check that the change is confined
to the cursor. The result to read is the *relationship*: before, draining
through the cursor cost **28 % more** than the same span deleted by key
(31.19 vs 24.31 ms) — LMDB's cursor drain is *cheaper* than its by-key delete.
After, the two are at parity (25.35 vs 25.12 ms). **The cursor-specific penalty
is gone**; the residual 2.46× is B8, which every delete path pays alike, and
which is now the only thing left in this cluster.

Not yet re-measured at consumer level: the `delete_prefixes` 3.63× should fall
toward the by-key ratio, but that claim needs another
`scripts/consumer.sh bench` run before it is made.

**Spec amendment landed 2026-09-10 — this is what unblocked it.** The
blocker was not §7 but SPEC 03 §5 rule 4, which *mandated* the slow mechanism
("the M1.4 write cursor tracks its position **by key** and re-seeks after each
of its own mutations"). New **§5.4a** demotes that to one permitted mechanism
and states the actual requirement — the cursor's *logical* position must be
unchanged, by any means — with a table of the structural events (COW, cell
shift, borrow, merge, root shrink) at which a retained path must be repaired or
discarded. §7 gains the observed position contract as a table, pinned by
`crates/zerodb-oracle/tests/cursor_delete_position.rs` (7 differential cases,
including drains that force merges mid-walk). That test is the guard: it was
written *before* any optimization, so it cannot be tuned to fit one.

Gate: fmt / clippy `-D warnings` / `cargo test --workspace` 543 pass /
`miri` 154 pass, no UB / `fuzz-quick` 72,532 diff_ops runs, 0 divergences /
`crash-test-quick` 215 cycles, 0 violations.



### B9. Env creation costs two `fsync`s — **BY DESIGN, recorded 2026-09-10**
`env/open/create` is 10.85× (LMDB 0.51 ms, ZeroDB 5.5 ms per env). Fully
explained: `create_env_file` (`zerodb-io/src/file.rs:245-250`) does
`file.sync_all()` and then `fsync_parent_dir`, unconditionally and regardless of
`NO_SYNC`; LMDB does neither. On macOS Rust's `sync_all` is `F_FULLFSYNC`
(~2.5 ms each on a laptop SSD), which accounts for the entire gap.

This is the durability guarantee added by issue #46 — without the directory
fsync a crash just after creation can lose the *name* of an already-durable
file — and it is paid **once per environment lifetime**. `env/open/reopen`, the
same path minus creation, is **0.41×**. Listed here only so the ratio is not
re-derived as a regression; no action.

### B10. Copy / compaction — **MEASURED 2026-09-10** (extends C1's residual)
`maint/copy/raw` 2.77× (LMDB 7.4 ms, ZeroDB 20.5 ms) and
`maint/copy/compact` 4.72× (6.3 ms vs 29.6 ms). Meilisearch calls this on every
snapshot, so it is user-visible latency, not an internal detail. The
`compact ÷ raw` step (+1.95 ratio) puts the cost in the rebuild rather than the
I/O. `copy_raw` (`CompactionOption::Disabled`) is also still the buffered path
flagged in the 2026-09-09 review — a 1× env image in RAM plus one `fs::write` —
which C1's streaming work never reached. Reproduce with `just bench maint`.

### B11. Writer slot: a `futex_wake` system call on every write-txn release — **DONE 2026-09-25**
`WriterGuard::drop` (`env.rs`) called `Condvar::notify_one()` unconditionally.
On Linux, std's futex `Condvar` turns every notify into a `futex_wake` system
call, even with nobody waiting. That is one syscall per write txn, on a CPU
whose page-table-isolation mitigation makes each syscall expensive. LMDB's
writer lock on Linux is a pthread mutex (`me_wmutex`, `LOCK_MUTEX0` →
`pthread_mutex_lock`, mdb.c:466): uncontended lock and unlock stay in user
space, and the kernel is entered only under contention. **Fix:** a waiter
count lives under the flag mutex, and the release notifies only when it is
non-zero (SPEC 04 TXN-6 amended). On macOS, LMDB is built with
`MDB_USE_POSIX_SEM` (named semaphores, mdb.c:408), which always syscall.
That is why the laptop ladder showed ZeroDB 8× *faster* on empty commits, and
why this cost never appeared there.
Bench server (x86-64, 4 KiB, turbo off), `perf trace -s` over 3 s of
`env/txn/rw_empty_commit`: ZeroDB **1,401,099 → 31** `futex` calls (LMDB: 31
in the same binary). 5 interleaved rounds: 456 → 168 µs, **5.93× → 2.19×** vs
LMDB, every round 436–460 → 163–174 µs. `bench-ab` itself returned `invalid`
(LMDB drift 1.032 against a 0.03 limit), and Quentin approved the keep on the
deterministic count (chat, 2026-09-25). `commit/batch/*` and
`env/txn/ro_begin_abort` flat. The remaining 2.19× on this rung is the rest
of ZeroDB's begin/commit path (`write_txn`, `RwTxn` drop glue,
`SnapshotCell::clone_snapshot` in the profile).

### B12. Single-put commit: +8.4 µs per commit, attributed — **MEASURED 2026-09-25**
Bench server (x86-64, 4 KiB, turbo off), `examples/commit_census.rs`:
20,000 single-put commits of 256-byte values into a fresh named DB, NO_SYNC,
nothing else in the process. Phases are timed with `Instant`; `strace -c`,
`perf stat` and a DWARF `perf record` are taken over the same binary.

| per commit | LMDB | ZeroDB |
|---|---:|---:|
| total | 7.6 µs | 16.0 µs |
| begin / put / commit | 0.12 / 1.1 / 6.4 µs | 0.17 / **4.45** / **11.35** µs |
| write syscalls | 5.5 | 4.3 |
| bytes written | 20.3 KB (4.96 pages) | 24.3 KB (5.93 pages) |
| instructions | 22.9k | **57.5k** |

The gap is CPU work, not I/O; about 89 % of it is attributed:

- **Copy-on-write of the path, +2.3 µs** (`touch_path`: 3.0 µs vs
  `mdb_cursor_touch` 0.7 µs). `RwTxn::touch` heap-allocates a fresh 4 KiB `Box`
  and copies the **whole** page. LMDB reuses a frame from `me_dpages` and
  `mdb_page_copy` copies only the header/pointer area and the used heap,
  skipping the free gap.
- **Descent, +1.5 µs** (`search_path` 2.1 µs vs `mdb_page_search` 0.56 µs).
  Most of it is `node_view` → `BranchRef::new`/`LeafRef::new`: full per-cell
  validation of every map page the first time a write txn touches it.
- **Commit CPU, +3.1 µs.** The free-list save (`put_pil`: a second tree put,
  plus its own touch, per commit) is ~1.6 µs; `allocate` ~1.0 µs; then meta
  encoding and the rest.
- **Kernel write path, +1.1 µs**: roughly one extra 4 KiB page per commit.

`begin` differs by only 0.05 µs. Levers, in LMDB's shape: used-portion COW
copy plus frame reuse (reopens B3's frame pool, now with evidence); a cheaper
free-list save; and lazy first-touch validation (A2(b), ADR). The
`commit/batch/*` rungs no longer time the fixture drop (2026-09-25 harness
fix), so their pre-fix numbers are not comparable.

### B13. Hot paths sit above LLVM's inlining threshold — **MEASURED 2026-09-26**
Bench server (x86-64, 4 KiB, turbo off). The same commit is built four ways,
each in its own target dir, and measured in two passes (forward, then reverse
order; pass-to-pass spread 0.3–1.3 %). LMDB's C core is compiled by gcc and
ignores these flags, so it is the control (drift 0.97–1.0 except under fat LTO).

| rung | V0 default (CGU16) | V1 CGU1 (Meilisearch's release profile) | V2 CGU1 + `-C llvm-args=-inline-threshold=1000` | ZeroDB time V2 vs V0 |
|---|---:|---:|---:|---:|
| `get/access/hot` | 1.22× | 1.40× | 1.08× | −14 % |
| `get/db/named` | 1.45× | 1.55× | 1.26× | −15 % |
| `seek/ge/*` | 1.39–1.46× | 1.45–1.55× | 1.20–1.25× | −13 to −16 % |
| `scan/edge/first_last` | 2.15× | 2.19× | 1.41× | −38 % |
| `scan/range/*`, `scan/prefix/bucket` | 1.94–2.82× | 2.16–2.85× | 1.58–2.09× | −25 to −26 % |
| `scan/full/*` | 1.84–1.96× | 1.79–2.04× | 1.50–1.53× | −18 to −21 % |
| `put/order/seq` | 1.16× | 1.23× | 1.05× | −10 % |
| `put/val/v256` | 1.19× | 1.26× | 1.08× | −11 % |
| `commit/batch/n1` | 2.17× | 2.12× | 2.06× | −8 % |

(Cells are ZeroDB ÷ LMDB.) Adding fat LTO to V2 (V3) also speeds up heed's Rust
wrapper around LMDB (scan LMDB drift 0.75–0.86), so it does not separate the
engines and is omitted.

Three consequences:
1. With no code change, 8–38 % of ZeroDB's time on the hot paths comes from
   hot functions LLVM declines to inline at its default threshold.
2. CGU1, Meilisearch's build, is ZeroDB's **worst** case: slower than CGU16 on
   most read rungs. Before/after runs meant to speak for Meilisearch must also
   run at `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`. hannoy builds at the default
   16.
3. It explains the reverted GC head offset (ledger, 2026-09-25). Functions at
   the threshold change shape when a neighbour changes, which moved unrelated
   write rungs by 1–9 %. The retry with the draw arms kept out of line landed
   (B7 amendment).
4. A one-round breadth run can still flag identical code. On the B7 retry,
   five read rungs showed +5–8 % at CGU1 while `Tree::get`, `Cursor::search`,
   `Cursor::next` and the adapter `get` disassembled to identical instructions
   in both builds; only their 64-byte alignment moved. Three rounds on those
   rungs were flat (0.999–1.010). Confirm every one-round regression with
   three rounds before reverting.

**Amendment 2026-09-26: lever landed (source-level, no LLVM flag).** The
first try, `#[cold]` arms plus `#[inline]` hints, was flat and was reverted:
`#[inline]` only makes a function eligible, and LLVM's cost model still
declined. An `nm` diff of the gate builds (CGU1 vs CGU1 + threshold 1000)
named what LLVM inlines only at the raised threshold. Forcing exactly those
with `#[inline(always)]` (`bytes_from_classified`, `ValidatedPages::contains`,
`node_view`, `Cursor::leaf_at`, `BranchRef::child_index_with`,
`leaf_cell_len`, `read_and_check_bounds`, `OverflowRef::new`; `#[inline]` on
heed-zerodb `Database::get`) gives, at CGU1 (Meilisearch's build):
`get/access/hot` 1.37× → 1.11×, `get/db/named` 1.56× → 1.33×,
`scan/edge/first_last` 2.22× → 1.56×, `seek/ge/rand` 1.46× → 1.27×,
`put/order/seq` 1.23× → 1.10×, `put/api/reserved` 1.33× → 1.18×; deletes
10–12 % faster, `mixed/rw/8dbs` 1.20× → 1.10×. It improved 37 rungs at CGU1
and 31 at CGU16, and regressed none in either build (bench server).

Lever: consumers will not set an LLVM flag, so the fix is at the source level.
Shrink the hot bodies below the default threshold: `#[cold]` /
`#[inline(never)]` on the memo-miss, full-validation, error and
multi-page-allocation arms, and `#[inline]` on the small hot helpers. Judge it
by how much of V2's gain the default build keeps, at both CGU1 and CGU16.

## C. RAM (peak memory)

### C1. Compaction / `copy_to_file(Enabled)` / `load`: ~2× env size in RAM — **DONE 2026-07-22 (compaction path)**
**Done:** push-driven streaming rebuild — `PageSink` trait in core (core keeps
its no-I/O policy; the positioned-write `FileSink` lives in `zerodb::copy`),
`TreeStream` (the batch packer's greedy fill + last-two rebalance, driven one
entry at a time; one leaf scratch + one overflow scratch + ≤ 2 child lists
per branch level = **O(depth × psize)** peak), `EnvStream`/`MainStream`
(named DBs → catalog records merge-interleaved into the main push stream),
and `copy_to_file(Enabled)` rewritten onto a borrowed cursor walk
(`for_each_entry_flagged`) — no entry Vecs, no whole-image buffer. `path` is
touched only by an atomic rename of a sibling temp file after the last
progress callback, so the documented panic contract holds verbatim and even
a mid-copy process kill cannot leave a half-written destination (the old
single `fs::write` could). Referee: `stream_builder_matches_batch_builder`
(identical logical content INCLUDING followed catalogs, identical page
economy — same page counts per kind, same depth; layout order legitimately
differs) + the existing copy differentials/round-trips/tools acceptance.
**Residual resolved 2026-07-22:** `zerodb-tools load` now streams straight
into the data file (a tools-side `PageSink`; the whole-image `Vec` and its
`fs::write` are gone — the load half of issue
[#63](https://github.com/qdequele/ZeroDB/issues/63); #63 stays open for its
double-buffered-writer idea). Two bounds remain by choice: the dump-text
parse is still in-memory (input side), and the post-load invariant check
reads the file back (same `fs::read` the `check` subcommand uses) — peak is
now max(parse, check) instead of parse + image. Correction to the original
note: `migrate-from-lmdb` never used the batch builder — it streams through
batched write txns. Original analysis kept below.
`collect_entries_flagged` copies **every key and value in the DB into owned
`Vec`s** (`rotxn.rs:416-424`), then `build_multi_db_image` materializes **the
entire output env image in a second `Vec<u8>`** (`builder.rs`). Peak ≈ live
data + full image ≈ **2× env size** (+ allocator overhead) for the exact
operations Meilisearch runs against production indexes (snapshot compaction,
`process_batch.rs`). LMDB's `mdb_env_copyfd2` streams with a bounded buffer.
**Fix shape:** stream the rebuild — walk the source cursor while packing
leaves incrementally into a bounded write buffer (the PLAN §3.4 note already
reframed bulk-load as exactly this tooling win). No unsafe. Effort: medium.
This is the difference between "compaction works on a 100 GB index" and OOM.

### C2. Write-txn RAM = full copy of every touched page
Inherent to the heap dirty store (B3/B5): a write txn holds `Box` copies of
every touched page + overflow run. LMDB WRITEMAP holds zero (mutates the
map); LMDB default holds the same order but pool-recycled. Bounded by txn
size; milli's indexing txns are large. Mitigations = B5 (issue #13), frame
pooling (B3, parked), or dirty-page **spilling** — LMDB's `mdb_page_spill`
analogue, tracked as issue
[#3](https://github.com/qdequele/ZeroDB/issues/3).

### C3. Env-image builder for `load`/`migrate` (same as C1's second half) — **DONE 2026-07-22**
Was: `build_single/multi_db_image` return the whole file as `Vec<u8>` — fine
for tests, wrong for tools at scale.
**Done:** `load` streams (see the C1 residual note above); `migrate` never
had the problem (batched write txns). The batch `build_*_image` wrappers
remain for tests and the stream-vs-batch differential referee.

## D. Deliberate — keep, do not "optimize"

- **Meta CRC32C** (ADR-0002): one CRC per commit/meta-read, not a hot path;
  the crash harness's torn-write detection depends on it. LMDB has nothing
  equivalent. Keep.
- **Corrupt-page → typed error, never UB/panic**: the reason validation
  exists. A2(b) keeps the guarantee at O(log K) instead of O(K) — removing
  validation outright is not on the table.

## State of play (2026-07-22 — supersedes the original sequencing)

The original sequencing ran to completion: A1, A4, A2(a), B1, B6, B2, B4,
A8, A3, C1 are **DONE** (stamps above, each behind the full gate + referee);
B3 is **PARKED** on profile evidence. Scoreboard at this point (alternated
same-run medians vs the fork, matched 16 K geometry, Apple M-series): milli
end-to-end indexing 2.68× → **1.12×**; hannoy search **0.95×** (faster, all
dims); hannoy build 1.27–1.49×; micro get/scan/overflow ≈ parity; on-disk
17 % denser; compaction peak RAM 2× env → ~100 KB.

What remains, and its gate:

- **Profile-gated micro levers:** A5 (branch-level cursor cache), A7 (#14
  comparator monomorphization), plus the B1 residual (#7) and the cursor half
  of B6 (#10 / D-015). A6 (#19) landed 2026-07-22. Implement only what a
  consumer call-tree names — two blind picks were falsified this campaign.
- **Hardware-gated validation:** B4 is built but unmeasured on its target
  (Graviton + EBS); same run validates the durable-commit path (~1.8× on
  laptop, unknown on EBS).
- **ADR-gated:** B5 (#13, WRITEMAP in-place — last, if at all), A2(b) (#21,
  lazy validation + trusted-file mode).
- **Measure-first:** B7 (#29) — only matters under GC churn.
- **Inherent, mitigations tracked:** C2 → spilling (#3), B5 (#13), or B3
  pooling (parked).
- **Tooling residual:** C3 landed 2026-07-22 (`load` streams; #63's load
  half). Still buffered: `copy_raw` (`CompactionOption::Disabled`, 1× env in
  RAM + single `fs::write`), and — until 2026-09-09 — the adapter's
  `heed::Env::copy_to_file`, which `read_to_end`'d the staged copy before
  writing it (now `io::copy` through a private staging dir).

The broader Phase-3 technique backlog (beyond this inventory's LMDB-parity
scope) lives in the GitHub issues, filed 2026-07-22: prefetch/madvise, io_uring,
durability tiers, page-layout evolutions, consumer-API batching.

## Already done

- **Cursor leaf memoization** (2026-07-21): scan 29× → **2.18×** (zerodb
  −92.6%, LMDB control flat, p<0.05); gate green (81 suites / 494 tests).
  `get` unchanged, as predicted — descents don't revisit pages; that's what
  A1/A2 are for.
