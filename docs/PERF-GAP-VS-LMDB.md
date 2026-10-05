# Performance inventory: every time & memory cost vs LMDB

Status: full inventory, 2026-07-21 (second, deeper pass — supersedes the
first "LMDB techniques" listing; that pass compared against LMDB's design
choices, this one also sweeps zerodb's own hot paths for every copy,
allocation, redundant computation, lock, and RAM sink). Items B8–B24 were
added as later measurements found them; status markers are current as of
2026-10-05 (see "State of play" at the end).

**Measuring these items.** `just bench` runs a microbench *ladder* built so
that each rung isolates one of the mechanisms inventoried below; `just
bench-report` prints the per-rung LMDB-vs-ZeroDB ratio and each rung's delta
against its family's base rung. [`BENCH-MAP.md`](BENCH-MAP.md) maps rungs to the
A/B/C items on this page — start there before picking a lever, since two blind
picks were falsified during the July 2026 campaign.

Every claim is cited. LMDB side: the vendored fork (`lmdb-master-sys 0.2.6`,
`liblmdb/mdb.c` — the tree the oracle links; reading it for techniques is
permitted by AGENTS.md rule 4, nothing is transliterated). ZeroDB side:
file:line in this repo at the time of writing — those line numbers have
drifted since; search by the function name.

**Unsafe policy note.** AGENTS.md already sanctions unsafe in
`zerodb-core::page` and `zerodb-io` ("mmap access and page casting") and
prescribes the mechanism ("explicit offsets + `read_unaligned`"). Items marked
*unsafe (sanctioned)* were permitted but not taken when this was written —
Phase 1 chose `#![forbid(unsafe_code)]` in the page codec as a
correctness-first default. A3 (2026-07-22) took it: `zerodb-core` is now
`deny(unsafe_code)` with scoped `#[allow]`s (`page::raw` and its call sites;
since ADR-0021 also the WRITE_MAP map-slice calls in `dirty`).

## Top of the stack — what one `get` on a named DB actually costs

*July 2026 baseline, kept as the starting point. Since then items 2–3 were
removed by A1, item 5 by A6 and B17, and item 4 reduced to a once-per-page
validation shared across txns (A2/A3/A4/A8, ADR-0018). On the bench server
`get/access/hot` is now 0.77–0.80× LMDB (B19).*

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

### A2. Eager O(num_keys) page validation on every view construction — **(a) DONE 2026-07-21 (37eb96e; lock-free via A8); (b) parked (ADR-0016); (c) DONE 2026-09-30 (ADR-0018)**
**Done (c), 2026-09-30:** the txn-scoped memo left short read txns
re-validating the same hot pages on every txn (a one-get read txn over a
1M-key tree: 1,984 ns vs LMDB's 649 ns, 59 % in the cell walk). Read txns now
share an env-wide cache keyed by (pgno, kind, header txnid stamp): one txn's
validation serves the next, and a reused page misses on its newer stamp
(ADR-0018, SPEC 04 TXN-38). Result: that read txn **1,984 → 1,008 ns**;
`get/*` 1.15× → 0.94–0.96×, `seek/ge/*` → 0.95×, `get/size/n1m` 1.48× →
1.08×, `scan/range/*` 1.70× → 1.33×; writes, memo-hit and per-txn rungs flat
in both builds (`benches/results/2026-09-30-validation-cache.md`).
**Done (a):** txn-scoped `ValidatedPages` memo — disk bytes validate exactly
once per txn at first map entry; every later view is `new_prevalidated`
(O(1)) or, since A8, `new_trusted` (two raw header reads). Engine-authored
dirty frames trusted by construction (batch 3, same commit).
**Parked (b):** the lazy/checked-per-access variant (validate O(log K) per
descent instead of O(K) at first touch), issue #21. Its two halves were split
into ADRs: the opt-in trusted-file mode landed (ADR-0014, see section D), and
lazy validation was measured over four variants and not adopted (ADR-0016,
parked 2026-09-29: 1M-key tree −23 %, but seq/miss +11–23 %). The env-wide
cache (c) then removed most of the repeat cost lazy validation targeted.
Re-open only if a profile names first-visit validation. Original analysis
kept below.
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
**Tried 2026-09-27, reverted:** one cached parent `BranchRef` in `Cursor`
(the scan half of this item) gained a consistent 2–4 % on the `scan/*` rungs
at CGU1, under the noise bar, so the verdict was flat: a branch re-resolution
is already a memo hit (perf ledger). Still open.

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
`KeyCmp::Custom(&dyn Comparator)` (`cmp.rs`) — indirect call per
comparison on custom-comparator DBs (milli sets one). LMDB uses a plain fn
pointer. Monomorphize the search over the comparator. Effort: low. Open;
the default ordering's compare was sped up separately (B19), this item is
the custom-comparator path only.
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
tracked as issue [#7](https://github.com/qdequele/ZeroDB/issues/7). The
`del_current` half of this residual was since mostly removed (B8a: the cursor
keeps its path across a delete that does not rebalance; B22: also across the
leftmost-pairing rebalances); `put_current` and cursor `put` still
drop the parked stack and re-seek once.
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
**Amendment 2026-09-27/30:** the frame-reuse half of that redesign landed as
B18 (a bounded spare-frame pool, LMDB's `me_dpages`; frames stay individually
boxed, so TXN-41 is untouched), and dirty memory is bounded by spilling (C2,
ADR-0017). What stays parked is the `HashMap` → sorted-array swap (issue #5)
and the per-commit sort; issue #4 (per-page allocation and zero-fill) is
partly addressed by B18.

### B4. One `pwrite` syscall per dirty page at commit; no coalescing — **DONE 2026-07-21 (3c6a064); EBS validation PENDING**
Was: `for pgno in sorted_pgnos { backing.write_at_page(...) }` — one syscall
per dirty page. LMDB's `mdb_page_flush` coalesces contiguous runs into
`iovec[MDB_COMMIT_PAGES]` batches (mdb.c:1613-1617) flushed via `pwritev`.
**Done:** the C2 flush batches contiguous pgnos and `zerodb-io` issues one
`pwritev` per chunk of up to `MAX_IOV` frames (`file.rs`). **Caveat:** the win
this exists for is Graviton + EBS gp3, where per-syscall latency dominates —
the laptop page cache hides it, so it is implemented but *unmeasured on
target*. Same hardware gate as the ~1.8× durable-commit number.
**Update 2026-10-05:** that ~1.8× was a macOS laptop figure
(`benches/results/2026-09-10-engine-ladder-macos.md`). On Linux, durable
commits are at parity: `commit/sync/*` 0.97–1.03× on the x86-64 bench server
(`2026-09-28-evening-bench-server-linux-x86.md`), and on durable YCSB B on
Graviton4 local NVMe ZeroDB does 258k ops/s against LMDB's 238k after
ADR-0022 (`2026-10-05-meta-annex-real-case.md`). An EBS gp3 run is still
owed.

### B5. WRITEMAP copies at commit instead of mutating the map — **DONE 2026-10-05 (ADR-0021, PR #88, opt-in under `WRITE_MAP`)** — issue [#13](https://github.com/qdequele/ZeroDB/issues/13)
**Done:** under `WRITE_MAP`, dirty pages are now realized in the writable map
at their final offset (COW copy, new page, overflow run), with no heap staging
and no commit write-back; abort leaves them unreferenced by not advancing the
meta (SPEC 04 TXN-45b). The default heap-staged path is unchanged. Merged via
PR #88 (8b066a6) after the production pass (unsafe-policy amendment for the
`dirty` map-slice call, map-aware crash backend, miri test backing, release
guard on the brokered path; ADR-0021 "Production hardening pass"). Spike A/B
(`commit_census`, macOS arm64, NO_SYNC, 20k single-put commits): total
29.8–43.5 µs → 8.6–11.1 µs per commit. On Graviton4 NVMe YCSB no-sync,
ZeroDB-`WRITE_MAP` still trails LMDB-`WRITE_MAP` (A 408k vs 545k ops/s, B
847k vs 1,199k; `benches/results/2026-10-05-meta-annex-real-case.md`).
**Not done** (issue #13 is still open): prefault/`fallocate` of the map, a
ranged `msync` of only the changed pages (issue #45), the heap-staged vs
in-place A/B on Graviton, and ADR-0021's open questions (sub-flag or not,
whether to advise Meilisearch to set `WRITE_MAP`). Original analysis kept
below.
M1.10 implemented WRITE_MAP as commit-time copy (heap dirty store → map at
C2) to keep the value-borrow contract, nested-reader dirty reads, and
abort-by-drop identical. Costs a full memcpy of every dirty page per commit +
the B3 allocation. LMDB with `MDB_WRITEMAP` mutates mapped pages in place and
skips the write entirely (mdb_page_flush early-out). Already flagged in
PROGRESS as "true zero-copy live-map mutation deferred to Phase 3". Effort:
high (abort semantics + borrow contract redesign). ~~Do last, if at all.~~
(Done, see above.)

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

### B7. Freelist bookkeeping allocation churn — **open, measure-first; front draw DONE 2026-09-26; per-commit save moved to the meta page 2026-10-05 (ADR-0022)** — issue [#29](https://github.com/qdequele/ZeroDB/issues/29)
`drains: BTreeMap<u64, Vec<u64>>` + `reclaimed: HashSet` + PIL decode `Vec`
per GC entry touched (`RwTxn::gc_reclaim` and the `RwTxn` free-list fields);
`freelist_save` fixed-point loop per commit. LMDB: sorted `MDB_IDL` arrays
manipulated in place. Effort: medium. Only matters under GC churn; measure
before redesigning. (Since written: a drain entry is now a `Drain` — decoded
ids plus a consumed-prefix offset, amendment below — and `reclaimed` uses the
splitmix64 `PgnoHasher`, issue #9.)

**Amendment 2026-10-05: the common-case save left the GC tree.** ADR-0022
(PR #89, on-disk `FORMAT_VERSION` 2) writes a commit's freed-page list into
its own meta page, under the meta CRC: when the list fits the annex cap
(`(psize − 176) / 8` ids) the per-commit GC-tree put of that list, and its
COW touch of the GC leaf, are gone; drain rewrites (GC-20) still go to the
tree. `commit/batch/n1` 2.04× → 1.60× LMDB (bench server, CGU1, 5 rounds).
This is a deliberate non-LMDB lever: LMDB keeps its freelist in `FREE_DBI`
and pays the tree ops on every commit.

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

### B8. Delete-time rebalance moves entries cell-by-cell — **DIAGNOSED + CONSUMER-CONFIRMED 2026-09-10, open (narrowed by B15/B16/B22/B23)**
*Status 2026-10-05:* the rebalance lever itself (fewer cell operations per
borrow, or a direct `move_entry`) is not done: `borrow_entry` still
materializes an `OwnedLeafCell`. Shared-path levers narrowed the per-key
rungs, and `delete_range` left this path (B23). Bench server ladder,
2026-09-28 evening (x86-64, CGU16, default options): `del/bulk/half` 1.75×,
`del/bulk/all` 1.75×, `del/churn/reinsert` 1.40×, `del/cursor/drain` 2.72×,
`del/range/half` 0.95×. The macOS figures below are the original diagnosis.

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
(Until B23, which replaced that with one leaf-granular pass.)

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
which is now the only thing left in this cluster. (B22 later found one more
cursor-only cost: the path was still discarded on every delete that
rebalanced.)

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
explained: `create_env_file` (`zerodb-io/src/file.rs`) does
`file.sync_all()` and then `fsync_parent_dir`, unconditionally and regardless of
`NO_SYNC`; LMDB does neither. On macOS Rust's `sync_all` is `F_FULLFSYNC`
(~2.5 ms each on a laptop SSD), which accounts for the entire gap.

This is the durability guarantee added by issue #46 — without the directory
fsync a crash just after creation can lose the *name* of an already-durable
file — and it is paid **once per environment lifetime**. `env/open/reopen`, the
same path minus creation, is **0.41×**. Listed here only so the ratio is not
re-derived as a regression; no action.

### B10. Copy / compaction — **MEASURED 2026-09-10** (extends C1's residual); **superseded by [B24](#b24-env-copies-wrote-every-byte-two-or-three-times--done-2026-09-28)**
*Pre-B24 baseline, kept for history: the buffered raw copy described here is
gone. B24 (2026-09-28) streams both modes: `maint/copy/raw` 2.53× → 0.93× and
`maint/copy/compact` 3.31× → 0.98× on the bench server.*

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
(LMDB drift 1.032 against a 0.03 limit), and the maintainer approved the keep
on the deterministic count (2026-09-25). `commit/batch/*` and
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

**Lever status 2026-10-05:** frame reuse landed (B18, `commit/batch/n1`
2.05× → 1.93×); the free-list save moved into the meta page (ADR-0022, see
B7: `commit/batch/n1` 2.04× → 1.60×); lazy first-touch validation was
measured and parked (ADR-0016), while the env-wide validation cache landed
(ADR-0018, seeded at commit; writers probe it but never publish). The
used-portion COW copy was tried and falsified (amendment below): `touch` keeps
copying the whole page. Under `WRITE_MAP`, B5 removes the heap COW frame and
the commit write-back altogether.

**Amendment 2026-10-02 — used-portion COW copy is FALSIFIED, do not retry.**
The census item "`touch` copies the whole page" was implemented as a used-portion
copy (`cow_gap` skips the free gap on recycled frames, LMDB `mdb_page_copy`
shape; local branch `qdequele/perf-b12-used-portion-cow`, green gate + clean
spec-review, never merged). Three-column A/B on the bench server (x86-64, 3
rounds): `commit/batch/n1` 1.95× → **1.93×** (after÷before 0.997 ±0.031), all
17 commit/put/mixed rungs flat, 0 improved. **Reverted.** B18's dirty-frame pool
had already removed the *allocation* cost the census attributed to `touch`; the
remaining memcpy (≤ one page) is below the 3 % noise floor, and skipping the gap
leaks stale cross-DB/aborted bytes into on-disk gaps for no measured gain. The
per-op commit gap is the OTHER census items — free-list save (since moved into
the meta page, ADR-0022), `allocate`, meta encode — plus validation, not the
copy. (Recorded on 2026-10-02 in commit 279ceba on an unmerged branch; carried
here 2026-10-05.)

### B13. Hot paths sit above LLVM's inlining threshold — **MEASURED 2026-09-26; source-level lever DONE (eb450ac)**
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

*Original lever statement (before the amendment above; its `#[cold]` half was
the reverted first try):* consumers will not set an LLVM flag, so the fix is
at the source level. Shrink the hot bodies below the default threshold: `#[cold]` /
`#[inline(never)]` on the memo-miss, full-validation, error and
multi-page-allocation arms, and `#[inline]` on the small hot helpers. Judge it
by how much of V2's gain the default build keeps, at both CGU1 and CGU16.

### B14. Write txn's named-DB table was a SipHash map — **DONE 2026-09-26**
`RwTxn.open` was a `HashMap<u32, NamedTree>` keyed by dbi with the default
SipHash hasher, and a named put probes it several times (`ensure_open`,
`record`, `record_mut`). A `put/val/v8` profile on the bench server put
`hash_one::<&u32>` plus `DefaultHasher::write` at 7.5 % of ZeroDB's samples.
LMDB keeps the same state in `txn->mt_dbs[dbi]`, an array read by index on
every cursor init (`mdb_cursor_init`). The table is now indexed by dbi
(`OpenTable`, a `Vec<Option<NamedTree>>`); dbi indices are small and
append-only, so it grows only to the highest dbi the txn touched.

Bench server, x86-64, 4 KiB (bench-ab, 3 rounds):

| rung | CGU16 | CGU1 |
|---|---|---|
| `put/val/v8` | 1.28× → 1.09× | 1.28× → 1.10× |
| `put/order/seq` | 1.04× → 0.95× | 1.03× → 0.96× |
| `put/api/plain` | 1.06× → 0.97× | 1.05× → 0.97× |
| `put/api/reserved` | 1.16× → 1.07× | 1.15× → 1.04× |
| `put/order/append` | 1.47× → 1.32× | 1.45× → 1.30× |

No `put/*` rung regressed in either build. Breadth at CGU1 (1 round):
`commit/batch/n10k` 1.16× → 1.02×, `mixed/rw/8dbs` 1.11× → 1.05×, `del/*` 4–7 %
faster; the one flag (`del/clear/all`) was flat over 3 rounds (0.984).

### B15. A fresh descent-path `Vec` per put and delete — **DONE 2026-09-26**
`search_path` built a new `Vec<(u64, usize)>` for every put and delete: an
allocation, a growth and a free per op (~3 % of `put/val/v8` samples). LMDB's
cursor stack is allocated once and reused. The write txn now keeps one path
buffer (`RwTxn::path_buf`), taken for each put/delete and put back after, so
only the first op allocates. The first try, the read path's inline 32-frame
`PathStack`, was reverted: zero-filling and returning a 520-byte struct per op
cost more than the 2–3-frame `Vec` (put/val/v8 +8 % at CGU16).

Bench server, x86-64, 4 KiB (bench-ab, 3 rounds), on top of B14:

| rung | CGU16 | CGU1 |
|---|---|---|
| `put/val/v8` | 1.08× → 1.00× | 1.11× → 0.96× |
| `put/order/seq` | 0.95× → 0.92× | 0.94× → 0.90× |
| `put/api/plain` | 0.96× → 0.93× | 0.96× → 0.92× |
| `del/bulk/half` | 2.25× → 2.08× | flat |

No `put/*` or `del/*` rung regressed in either build; breadth at CGU1 had
`commit/batch/n10k` 1.03× → 0.99× and nothing slower.

**Amendment 2026-09-26: APPEND.** The APPEND check copied the tree's last key
into a fresh `Vec` (`rightmost_path`) to compare it once. It now compares in
place in the leaf under the tree's ordering, as LMDB's APPEND check does, and
reuses the same path buffer. `put/order/append` 1.32× → 1.19× (CGU16) and
1.30× → 1.16× (CGU1); the other 11 `put/*` rungs flat.

### B16. Page views decoded the whole header to read its flags — **DONE 2026-09-27**
`LeafRef::new_prevalidated`, `BranchRef::new_prevalidated` and `OverflowRef::new`
called `CommonHeader::read`, which also loads `pgno` and `txnid`. The loads are
bounds-checked, so LLVM keeps them although nothing uses the values: this is
the out-of-line `read_u64` at 15 % of hannoy's search profile and 5.7 % of
`put/val/v8`. They now read only the flags field (`header::read_flags`), as
LMDB tests `IS_LEAF(mp)` on the one field. Every check is unchanged.

The write path gains most, because its descent builds a view per level on
every put and delete. Bench server, x86-64, 4 KiB (bench-ab, 3 rounds):

| rung | CGU16 | CGU1 |
|---|---|---|
| `del/bulk/half` | 2.14× → 1.83× | 2.07× → 1.76× |
| `del/range/half` | 3.18× → 2.68× | 3.14× → 2.65× |
| `del/cursor/drain` | 3.61× → 3.05× | 3.50× → 2.99× |
| `put/val/v8` | 1.00× → 0.89× | 0.98× → 0.87× |
| `put/api/reserved` | 1.04× → 0.96× | 1.05× → 0.94× |
| `put/order/append` | 1.27× → 1.05× | 1.16× → 1.05× |

`put/*` + `del/*`: 14 / 12 improved, 0 regressed. Reads: `scan/range/*` and
`scan/prefix/bucket` 3–5 % faster in both builds. One flag, `scan/meta/len`
+3.4 % at CGU16 (reproduced), runs byte-identical engine code at the same
address in both builds (`Database::len` inlines its engine work); it is
harness layout, not this change (see B13 point 4).

### B17. Point `get` built a full cursor — **DONE 2026-09-27**
`Tree::get` (and the catalog lookup) opened a `Cursor`, zero-filling its
32-frame `PathStack`, descended through `Cursor::search`, then compared the
found key a second time (`cmp.eq`) although the leaf's binary search had
already decided exactness. A point lookup needs no path: `Tree::find_exact`
now walks root to leaf with the same `node_view` loop and the same level bound
and error as `Cursor::search` (`depth + 2` levels, capped at `CURSOR_STACK`),
and takes exactness from `lookup_with` (`Ok` = exact). LMDB's
`mdb_node_search` reports exactness the same way, and `mdb_cursor_init` only
resets the depth.

Bench server, x86-64, 4 KiB (bench-ab, 3 rounds):

| rung | CGU16 | CGU1 |
|---|---|---|
| `get/access/hot` | 1.11× → 0.94× | 1.12× → 0.93× |
| `get/access/miss` | 1.33× → 1.12× | 1.35× → 1.13× |
| `get/val/v8` | 1.16× → 1.02× | 1.21× → 1.04× |
| `get/db/named` | 1.30× → 1.23× | 1.35× → 1.24× |
| `get/size/n1k` | 1.17× → 1.03× | 1.21× → 1.01× |

CGU1: 20 improved, 0 regressed. CGU16: 20 improved; three scan rungs flagged
(`scan/full/fwd` +4.1 %, `scan/full/rev` +4.4 %, `scan/prefix/bucket` +3.7 %),
but every cursor function they run (`next`, `prev`, `ascend_next`,
`set_range`, `search`, `first`, `last`) disassembles identically in both
builds, shifted by 80 bytes: layout, not this change (B13 point 4).

### B18. A fresh 4 KiB frame per touched page — **DONE 2026-09-27 (maintainer override)**
Every first-touch COW copied the page into a new `Box` and every new page
allocated a zeroed one; freed and committed frames went back to the allocator
(4 KiB is above glibc's tcache size). LMDB reuses dirty-page buffers through
`me_dpages`, within and across write txns. `DirtyStore` now keeps a spare list
of one-page frames (freed frames go there; `touch` copies into one, new pages
zero-fill one), and the spares ride across txns in the writer slot's state,
under the writer lock's own mutex as LMDB keeps `me_dpages` under its writer
mutex, so the hand-over costs no lock of its own. Capped at 256 frames (1 MiB
at 4 KiB): LMDB's pool is bounded because it spills dirty pages, ZeroDB had
no spill then (spilling landed 2026-09-30, C2). Frames stay individually
boxed (TXN-41), and every reuse overwrites or
zero-fills the frame, so the file bytes are unchanged.

Bench server, x86-64, 4 KiB, CGU1 (bench-ab, 3 rounds, on top of B17):

| rung | before | after |
|---|---|---|
| `put/gc/drain_big` | 1.58× | 1.17× |
| `commit/batch/n1` | 2.05× | 1.93× |
| `put/val/v4k` | 1.04× | 1.00× |
| `env/txn/rw_empty_commit` | 2.19× | 2.41× |

The empty-write-txn cost (~+10–15 % at CGU1, flat at CGU16; three designs
tried, the last with zero extra lock operations) is accepted by the maintainer
for the gains on page-heavy writes; Meilisearch does not run empty write txns
in a hot loop.

### B19. Default key compare called `memcmp` on every probe — **DONE 2026-09-27 (maintainer override)**
With the default ordering every binary-search probe (`leaf_lookup`,
`BranchRef::child_index_with`) went through `a.cmp(b)`, a `memcmp` call. For
two slices of equal length 8 or 4, big-endian integer order is exactly memcmp
order, so `KeyCmp::Default` now compares those as one `u64`/`u32` each
(Masstree-style key slices); every other length pair, and custom comparators,
are unchanged. Proptests check it against slice order for all length pairs
0..=16.

Bench server, x86-64, 4 KiB (bench-ab; CGU1 3 rounds, CGU16 1 round):

| rung | CGU16 | CGU1 |
|---|---|---|
| `get/access/hot` | 1.00× → 0.80× | 0.93× → 0.77× |
| `get/val/v8` | 1.07× → 0.93× | 1.02× → 0.91× |
| `put/order/seq` | 0.90× → 0.86× | 0.92× → 0.86× |
| `seek/ge/rand` | 1.27× → 1.19× | 1.19× → 1.11× |
| `get/key/k128` | 1.21× → 1.26× | 1.20× → 1.26× |

25 / 20 rungs faster. Two caveats: most ladder rungs use 8-byte keys, so the
gain is overstated for string keys; and long keys pay the length dispatch
(`get/key/k128` +4–5 % in both builds). Kept by the maintainer: Meilisearch's
hot trees key on 4-byte big-endian document ids and hannoy's on item ids, which
take the fast path.

### B20. `non_free_pages_size` decoded and validated every free-list id — **DONE 2026-09-27 (first step)**
`free_page_count` (behind heed's `non_free_pages_size` / `used_size`) walked
every GC entry and ran `pil_decode` (a `Vec<u64>` per entry) plus
`validate_pil_ids`, only to take the length. Meilisearch's index-scheduler
calls it before every register write txn and `IndexStats` after every batch.
It now sums each entry's 8-byte count prefix (`pil_count`), which keeps the
length/shape check (a torn value still errors `Invalid`) but no longer decodes
or range-checks the ids; they stay fully validated where they are drawn for
reuse (`gc_reclaim`) and by `check`. SPEC 05 GC-23 states this.

New rung `env/stat/non_free` (1M keys deleted across 1,000 commits under a
pinned reader, ~1k GC entries). Bench server, x86-64, 4 KiB: 317 µs → 170 µs
(0.537, CGU1 3 rounds; 0.524 CGU16) — the same ratio in the screen and both
builds; the CGU1 verdict reads `invalid` only from LMDB drift on its 0.35 µs
side. **Still ~480× LMDB:** heed over LMDB derives the figure from `mdb_stat`
page counts per DB and never reads the free list (O(#DBs)). Matching that is
the next step and needs a SPEC GC-23/24 amendment.

*Second step, 2026-09-29 (SPEC GC-23/24 amended with maintainer approval):*
`non_free_pages_size` now uses heed's definition — the main DB's and every
named DB's branch + leaf + overflow pages times the page size, read from the
catalog records in one pass over the main DB's entries — and no longer walks
the free list or reads the file length. That also fixes `WRITE_MAP`, where
the file spans the whole map and the old `real_disk_size − free × psize`
reported almost all of it as used. `env/stat/non_free` 169 µs → **148 ns**
(467× → **0.41×** LMDB, CGU1 3 rounds). `free_page_count` stays for tools and
`check`.

### B21. `clear` read every leaf of the tree — **DONE 2026-09-27**
`clear_tree` → `collect_tree` loaded every page, leaves included, to collect
the pgnos to free. LMDB's `mdb_drop0` walks only to the lowest branch level
when the DB has no overflow pages and frees the leaf pgnos from the child
pointers without reading the leaves. `clear_tree` now does the same when the
working record's `overflow_pages == 0` (`collect_tree_skip_leaves`); otherwise
the full walk is unchanged. Every unread pgno must be dirty in this txn or lie
in `[FIRST_DATA_PGNO, committed last_pg]`, else `Invalid`; collection happens
before any free, so a crafted pointer never reaches the free list, and an
aliased child refuses the clear. SPEC 02 §6.1 states the rule.

Bench server, x86-64, 4 KiB (bench-ab): `del/clear/all` 12.73× → 1.78× (CGU1,
3 rounds, 0.141) and 12.15× → 1.88× (CGU16, 0.155). The per-key delete rungs
(`del/bulk/*`, `del/range/half`, `del/churn`) read +4–7 % in both builds, but
every function on their path (`delete_tree`, `delete_at_path`, `rebalance`,
`merge_pages`, `borrow_entry`, `free_page`, `touch`, `touch_path`,
`search_path`, `update_parent_key`) disassembles identically; only
`clear_tree`/`collect_tree` changed: layout (B13 point 4).

### B22. A cursor delete that rebalanced threw away the cursor's path — **DONE 2026-09-27 (leftmost pairing)**
After B8a, `RwCursor::del_current` still discarded its parked path whenever the
delete borrowed or merged (~86 % of front-to-back drain deletes at 4 KiB), so
the next step re-descended from the root. LMDB's `mdb_cursor_del0` repairs
the cursor after `mdb_rebalance`. `rebalance` now reports a `PathFate`
(Unchanged / Kept / KeptShrunk / Invalidated) and `del_current` keeps the
path when it can. Repaired: the leftmost pairing (`pki == 0`, always with the
right sibling) — borrow from right, merge with right and its cascade, root
shrink to the surviving child (frame 0 popped). Everything else, including any
ancestor split, the from-left pairing and a hostile empty sibling, falls back
to today's re-descent. In debug builds every kept path is re-checked against a
fresh root-to-leaf search. SPEC 03 §5.4a carries the per-level repair table.

Bench server, x86-64, 4 KiB: `del/cursor/drain` 2.91× → 2.63× (CGU1, 3 rounds,
0.889) and 2.95× → 2.69× (CGU16, 1 round, 0.914). A first CGU1 run flagged
`del/bulk/half` / `del/range/half` +3.5–5 % under LMDB drift; the re-run read
them flat (+1.5–3.2 %, within ±4–5 %).

### B23. `delete_range` collected every key, then point-deleted each — **DONE 2026-09-27 (beyond LMDB)**
`Database::delete_range` copied every covered key into a `Vec<Vec<u8>>`
(~50–60 MB for a 90 % delete of 1M keys) and called `delete_tree` per key: a
root-to-leaf descent and possible rebalance each. heed over LMDB implements it
as `range_mut` + one `mdb_cursor_del` per entry; LMDB has no bulk range-delete
primitive, so this lever deliberately goes past it. The walk now splices each
covered leaf's cells out in one pass (`LeafMut::remove_span`), frees covered
overflow runs, and rebalances each touched leaf once; the range-start leaf is
rebalanced once at the end, and on this path an emptied leaf merges instead of
being borrowed into (the outcome of LMDB's own empty-page unlink). Point and
cursor deletes keep the old policy. SPEC 03 describes the walk.

Bench server, x86-64, 4 KiB: `del/range/half` 2.63× → **0.92×** (CGU1, 3 rounds,
0.351) and 2.58× → 0.97× (CGU16, 1 round, 0.375); the other 17 `put/*` and
`del/*` rungs flat in both builds. The walk is leaf-granular; unlinking whole
covered subtrees from their parent in one step (WiredTiger-style fast
truncate) is issue [#12](https://github.com/qdequele/ZeroDB/issues/12), still
open (its text predates B23).

### B24. Env copies wrote every byte two or three times — **DONE 2026-09-28**
heed's `Env::copy_to_file(&mut File)` (what the ladder's `maint/copy/*`
calls) staged the copy in a private directory, then `io::copy`'d it into the
caller's file: every byte written twice and read back once. The raw mode also
assembled the whole image in a zeroed `Vec` before writing it, and the
compacting mode's sink issued one `pwrite` per page. LMDB's
`mdb_env_copyfd2` writes into the caller's descriptor directly, the raw mode
straight from its map and the compacting mode through a 1 MiB buffer
(`MDB_WBUF`). Now the same: `CopyToFile::copy_to_open_file` writes into the
handle from its current position (the adapter's `copy_to_file` calls it),
the raw copy writes the map in large positioned chunks with no in-memory
image, and the compacting sink gathers runs of consecutive pages into a
1 MiB buffer. The path-based copy streams both modes into a sibling staging
file renamed over the destination, so the panic contract (no callback after
the first destination byte) holds unchanged.

Bench server, x86-64, 4 KiB: `maint/copy/raw` 2.53× → **0.93×** (CGU1,
3 rounds, 0.370) and → 0.95× (CGU16, 1 round, 0.374); `maint/copy/compact`
3.31× → **1.51×** (0.456) and 3.34× → 1.51× (0.451).

*Second step (same day):* the profile then put ~31 % of the compacting copy's
samples in `pwrite` (ext4 page-cache work) against ~13 % for the walk and
re-pack. LMDB overlaps the two: `mdb_env_copyfd1` hands full buffers to a
writer thread. The compacting copy now does the same: a scoped writer thread
lands each full 1 MiB buffer while the walk packs the next (at most three
buffers exist; the first write error stops the writer and is the error
returned). `maint/copy/compact` 1.51× → **0.98×** (CGU1, 3 rounds, 0.651) and
1.49× → 0.99× (CGU16, 1 round, 0.668). The walk still re-packs every entry
where LMDB copies pages; at parity that is no longer worth a new algorithm.

## C. RAM (peak memory)

### C1. Compaction / `copy_to_file(Enabled)` / `load`: ~2× env size in RAM — **DONE 2026-07-22 (compaction path; `load` output side too); `load` input + check residual open (#63)**
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
`fs::write` are gone). Two bounds remain: the dump-text parse is still
in-memory (input side), and the post-load invariant check reads the file
back (same `fs::read` the `check` subcommand uses) — peak is now
max(parse, check) instead of parse + image. Those two are what issue
[#63](https://github.com/qdequele/ZeroDB/issues/63) now tracks ("Bound peak
memory in zerodb-tools load: stream the dump parse and the post-load check",
with an optional writer thread for `load`); the compacting copy's writer
thread already landed (B24, 7dc3d1c). Correction to the original
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

### C2. Write-txn RAM = full copy of every touched page — **DONE 2026-09-30 (spilling, ADR-0017)**
**Done:** a write txn past its dirty limit (LMDB's 131,072 pages by default,
`max_dirty_bytes` to set it) writes its highest-numbered dirty pages to the
file and drops them from memory, as `mdb_page_spill` does (SPEC 04 §6.3a).
rust-storage-bench, 10M items, 2 GB cap: the unsorted YCSB C load no longer
dies at the cap (539 MiB anonymous peak vs LMDB's 520 MiB); with the memory
back for the page cache, YCSB A 0.58× → 0.76× LMDB and YCSB B 0.31× → 0.69×
(`benches/results/2026-09-30-bounded-dirty-memory.md`). Engine ladder flat.
Issue #3 is still open. Of the other mitigations named below, the frame pool
landed as B18 and in-place `WRITE_MAP` as B5 (under `WRITE_MAP` the dirty
set lives in the map, so spilling only does bookkeeping there). Original
analysis kept below.
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
**Done:** `load` streams its output (see the C1 residual note above; the
input parse and post-load check are issue #63); `migrate` never had the
problem (batched write txns). The batch `build_*_image` wrappers remain for
tests and the stream-vs-batch differential referee.

## D. Deliberate — keep, do not "optimize"

- **Meta CRC32C** (ADR-0002): one CRC per commit/meta-read, not a hot path;
  the crash harness's torn-write detection depends on it. LMDB has nothing
  equivalent. Keep.
- **Corrupt-page → typed error, never UB/panic**: the reason validation
  exists. A2(b) keeps the guarantee at O(log K) instead of O(K) — removing
  validation outright is not on the table **by default**. *Amended
  2026-09-28 (ADR-0014):* an env may opt out through the `unsafe`
  `FileTrust::trust_contents()` open policy, which reads map pages as LMDB
  does; the default stays validating.

## State of play (updated 2026-10-05)

*As of 2026-07-22 (supersedes the original sequencing):* the original
sequencing ran to completion: A1, A4, A2(a), B1, B6, B2, B4, A8, A3, C1 are
**DONE** (stamps above, each behind the full gate + referee); B3 is
**PARKED** on profile evidence. Scoreboard at that point (alternated
same-run medians vs the fork, matched 16 K geometry, Apple M-series): milli
end-to-end indexing 2.68× → **1.12×**; hannoy search **0.95×** (faster, all
dims); hannoy build 1.27–1.49×; micro get/scan/overflow ≈ parity; on-disk
17 % denser; compaction peak RAM 2× env → ~100 KB.

*Since then:* A6, A2(c) (ADR-0018), B5 (ADR-0021, opt-in `WRITE_MAP`), B8a,
B11, B13–B24 and C2 (spilling, ADR-0017) are **DONE** (C3 landed
2026-07-22 with C1); B7's front draw and per-commit save (ADR-0022) landed;
A2(b)'s lazy-validation half is
**PARKED** (ADR-0016) and its trusted-file half shipped as an opt-in
(ADR-0014). Bench-server scoreboard, 2026-09-28 evening (x86-64, default
options; `benches/results/2026-09-28-evening-bench-server-linux-x86.md`):
Meilisearch indexing 0.99×, search 0.95×; hannoy build 0.84–0.89×, search
0.95×; durable commits at parity.

What remains, and its gate:

- **Delete path:** B8, the rebalance itself (`del/bulk/*` 1.75×,
  `del/cursor/drain` 2.72× — B22 repaired only the leftmost pairing). Design
  change, ADR first; no tracking issue yet.
- **Commit constants:** the rest of B12 (`allocate`, meta encode; the
  used-portion COW copy was falsified 2026-10-02); `commit/batch/n1` is 1.60× after ADR-0022 and
  `env/txn/rw_empty_commit` 2.4–2.5× (B11, B18).
- **Profile-gated micro levers:** A5 (branch-level cursor cache; one try read
  flat), A7 (#14 comparator monomorphization), the B1 residual (#7:
  `put_current` still re-seeks) and the cursor half of B6 (#10 / D-015).
  Implement only what a consumer call-tree names — two blind picks were
  falsified in the July campaign.
- **Hardware-gated validation:** B4 on its target (Graviton + EBS gp3) is
  still owed. The durable-commit path is at parity on Linux (bench server
  SATA, Graviton4 NVMe); the old ~1.8× was a macOS laptop figure.
- **B5 follow-ups:** ranged `msync` (#45), prefault/`fallocate` of the map,
  the Graviton A/B, and ADR-0021's open questions.
- **Parked:** A2(b) lazy validation (ADR-0016; issue #21 still open), the B3
  dirty-store arena / sorted map (#5; #4 partly covered by B18).
- **Measure-first:** the rest of B7 (#29) — only matters under GC churn.
- **Tooling residual:** `zerodb-tools load` still parses the whole dump in
  memory and reads the built file back for its check (#63). Env copies are
  at parity and stream (B24).

The broader Phase-3 technique backlog (beyond this inventory's LMDB-parity
scope) lives in the GitHub issues, filed 2026-07-22: prefetch/madvise, io_uring,
durability tiers, page-layout evolutions, consumer-API batching.

## Already done

- **Cursor leaf memoization** (2026-07-21): scan 29× → **2.18×** (zerodb
  −92.6%, LMDB control flat, p<0.05); gate green (81 suites / 494 tests).
  `get` unchanged, as predicted — descents don't revisit pages; that's what
  A1/A2 are for.
