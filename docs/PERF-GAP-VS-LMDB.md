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

### A3. Byte-copy field reads instead of pointer reads — *unsafe (sanctioned)*
`page/raw.rs:19-33`: every u16/u32/u64 field read = bounds-checked indexing
into a stack array + `from_le_bytes`; every field of every node of every page.
LMDB: direct struct access through `NODEPTR` (mdb.c:1159-1168), zero copies.
**Fix shape:** `read_unaligned` at explicit offsets in `zerodb-core::page` —
exactly what the policy prescribes. Must stay miri-clean. Effort: medium.

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

## B. Write path (time, per operation)

### B1. `RwCursor`: full re-seek + 3 heap copies per step — **new; milli's hot API**
`RwCursor` tracks position **by key** (`rwtxn.rs:2766-2774`): each `next()`
runs a fresh descent (`set_range(k)`), yields the pair as **owned**
`(k.to_vec(), v.to_vec())`, then clones the key a third time into
`CurPos::At(k.clone())` (`rwtxn.rs` next/put_current/del_current). So milli's
`iter_mut`/`put_current`/`del_current` loops — sharding, vector db, facet
level 0, all of `write_from_bbqueue`'s delete path — pay a tree descent plus
three allocations and a full value copy *per entry*. LMDB: pointer bump in
`mc_pg[top]`, zero copies.
**Fix shape:** page-position tracking with explicit invalidation on structural
change (the M1.4 comment even anticipates the heed adapter "revisits zero-copy
yields at M1.13" — it never did). No unsafe. Effort: medium (must keep the
re-seek fallback for splits/merges).

### B2. Per-insert owned-cell alloc; splits materialize the whole page
Every put builds an `OwnedLeafCell` (heap `Vec`s for key/value) for the new
cell (`rwtxn.rs:1368,1409`); a split calls `extract_leaf_cells` — **every
cell of the page copied into a `Vec<OwnedLeafCell>`** (`rwtxn.rs:1424-1439`)
— plus a `sizes: Vec` collect (`:1526`) and a full page re-encode
(`write_leaf_frame`, `:1462`). LMDB splits by memmoving node pointers between
the two mapped pages. Amortized: ~2 allocs + a page copy per K inserts on top
of B1/B3. No unsafe. Effort: medium.

### B3. Dirty store: HashMap + `Box` per page + commit-time sort + zeroing
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

### B6. Adapter `ReservedSpace` = alloc + zero + copy — **new; milli's put path**
heed's `MDB_RESERVE` hands the caller a pointer *into the page*; the adapter
hands a **zero-initialized heap buffer** (`reserved_space.rs:1-17`) that the
caller fills and the engine then copies into the dirty frame. Every
`put_reserved` (milli serializes documents and roaring bitmaps this way) pays
alloc + memset + extra memcpy. Fix: reserve directly in the dirty frame (the
frame is heap memory the engine owns — no unsafe needed for a first version
that returns `&mut [u8]` into the frame). Effort: medium (API threading).

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
