# Performance inventory: every time & memory cost vs LMDB

Status: full inventory, 2026-07-21 (second, deeper pass — supersedes the
first "LMDB techniques" listing; that pass compared against LMDB's design
choices, this one also sweeps zerodb's own hot paths for every copy,
allocation, redundant computation, lock, and RAM sink).

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

### A1. Named-DB record re-resolved on EVERY read op — **new, likely #1**
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

### A2. Eager O(num_keys) page validation on every view construction
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

### A3. Byte-copy field reads instead of pointer reads — **DONE 2026-07-22**
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

### A4. `validate_page_size` re-run on every page load — free fix
`PageRef::new` (`page/header.rs:101`) re-validates an env-immutable value on
the hottest path in the engine. Hoist to open. Effort: trivial.

### A5. Cursor caches only the leaf; branch levels re-resolved
`Cursor.stack` holds `(pgno, ki)` only; ascend/descend re-load + re-validate
branches (`btree.rs:393,414`). LMDB keeps a resolved `MDB_page*` per level
(`mc_pg[CURSOR_STACK]`, mdb.c:1470). Extend the proven leaf-memo pattern per
level. No unsafe. Effort: low.

### A6. Heap allocation per get / per cursor
`Tree::get` builds a `Cursor` with a heap `Vec` stack per call
(`btree.rs:180-182`). LMDB cursors live on the C stack. Fix: inline
`[(u64, u16); MAX_DEPTH]` array (depth is bounded), or a reusable per-txn
cursor. No unsafe. Effort: low.

### A7. Custom-comparator vtable call per key comparison
`KeyCmp::Custom(&dyn Comparator)` (`cmp.rs:160-172`) — indirect call per
comparison on custom-comparator DBs (milli sets one). LMDB uses a plain fn
pointer. Monomorphize the search over the comparator. Effort: low.

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
consumer bench still shows it (put_tree path adoption is the escalation).
`put_reserved`'s inline re-locate second descent has the same shape (noted
below).

### B2. Per-insert owned-cell alloc; splits materialize the whole page
Every put builds an `OwnedLeafCell` (heap `Vec`s for key/value) for the new
cell (`rwtxn.rs:1368,1409`); a split calls `extract_leaf_cells` — **every
cell of the page copied into a `Vec<OwnedLeafCell>`** (`rwtxn.rs:1424-1439`)
— plus a `sizes: Vec` collect (`:1526`) and a full page re-encode
(`write_leaf_frame`, `:1462`). LMDB splits by memmoving node pointers between
the two mapped pages. Amortized: ~2 allocs + a page copy per K inserts on top
of B1/B3. No unsafe. Effort: medium.

### B3. Dirty store: HashMap + `Box` per page + commit-time sort + zeroing — **PARKED 2026-07-22 (profile says no)**
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

### B4. One `pwrite` syscall per dirty page at commit; no coalescing
`rwtxn.rs:2510`: `for pgno in sorted_pgnos { backing.write_at_page(...) }`.
LMDB's `mdb_page_flush` coalesces contiguous runs into `iovec[MDB_COMMIT_PAGES]`
batches (mdb.c:1613-1617) flushed via `pwritev`. Hidden by the laptop page
cache; **dominant on the real target (Graviton + EBS gp3)** — the current
bench cannot see it. No unsafe. Effort: medium (zerodb-io gains a vectored
write; the C2 loop batches contiguous pgnos).

### B5. WRITEMAP copies at commit instead of mutating the map — *unsafe (sanctioned)*
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
`put_reserved_failing_closure_leaves_entry_parity`). **Residuals:** the
engine's inline-reserve path still re-locates the settled cell with a second
descent (`rwtxn.rs` `put_reserved` → `search_path`) — same shape as the B1
mutation residual; and the *cursor* reserved put
(`put_current_reserved_with_flags`) still goes through a heap buffer (rare
path; wire it to the slot fill if a consumer profile ever shows it).

### B7. Freelist bookkeeping allocation churn
`drains: BTreeMap<u64, Vec<u64>>` + `reclaimed: HashSet` + PIL decode `Vec`
per GC entry touched (`rwtxn.rs:924-998`); `freelist_save` fixed-point loop
per commit. LMDB: sorted `MDB_IDL` arrays manipulated in place. Effort:
medium. Only matters under GC churn; measure before redesigning.

## C. RAM (peak memory)

### C1. Compaction / `copy_to_file(Enabled)` / `load`: ~2× env size in RAM — **new, quantified**
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
size; milli's indexing txns are large. Mitigation = B5, or frame pooling (B3).

### C3. Env-image builder for `load`/`migrate` (same as C1's second half)
`build_single/multi_db_image` return the whole file as `Vec<u8>` — fine for
tests, wrong for tools at scale. Covered by the C1 streaming fix.

## D. Deliberate — keep, do not "optimize"

- **Meta CRC32C** (ADR-0002): one CRC per commit/meta-read, not a hot path;
  the crash harness's torn-write detection depends on it. LMDB has nothing
  equivalent. Keep.
- **Corrupt-page → typed error, never UB/panic**: the reason validation
  exists. A2(b) keeps the guarantee at O(log K) instead of O(K) — removing
  validation outright is not on the table.

## Sequencing (dependency-aware)

1. **A1** named-record memo per txn — unlocks true read cost; everything
   read-side is currently double-counted through the catalog descent.
2. **A4** hoist `validate_page_size` (trivial) + **A5/A6** cursor/branch memo
   + stack-array cursor (the proven pattern).
3. **A2** lazy/checked validation (the big remaining read multiplier).
4. **B1** RwCursor page-position (milli's mutation loops) + **B6** reserve
   into the frame (milli's put path).
5. **B3/B4** dirty-store arena + `pwritev` coalescing (commit cost; the EBS
   win invisible on the laptop).
6. **C1** streaming compaction (RAM ceiling, tooling-critical).
7. **A3** `read_unaligned` codec (cross-cutting constant factor) — after the
   structural fixes so its effect is measurable in isolation.
8. **B5** WRITEMAP in-place — only with an ADR; highest blast radius.

## Already done

- **Cursor leaf memoization** (2026-07-21): scan 29× → **2.18×** (zerodb
  −92.6%, LMDB control flat, p<0.05); gate green (81 suites / 494 tests).
  `get` unchanged, as predicted — descents don't revisit pages; that's what
  A1/A2 are for.
