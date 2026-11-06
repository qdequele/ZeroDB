# ADR-0041: A new tree page is not zero-filled when its memory is reused, as LMDB

- Status: Proposed (awaiting maintainer approval; no code changed yet)
- Related: issue #148, ADR-0030 (the free gap may hold earlier bytes),
  ADR-0021 (in-place `WRITE_MAP`), SPEC 04 TXN-41 and TXN-47
- Date: 2026-11-02

## Context

Every new tree page a write transaction creates (both halves of a leaf
split, the new page of a branch split, a new root, the first leaf of an empty
tree) gets its memory from `DirtyStore::insert_tree_frame`, whose contract is
that the frame comes back all zero. Its doc comment says callers rely on it,
and SPEC 03 §5 rule 1 restates the heap half ("a freshly allocated heap frame
is always filled whole"). The memory comes from one of three places:

| mode | memory | zeroing today |
|---|---|---|
| heap | a fresh allocation | `vec![0; psize]` |
| heap | a reused spare frame (up to 256 carried between write txns, TXN-41) | `fill(0)` |
| in-place `WRITE_MAP` | the map page at the new page number | `fill(0)` |

**What LMDB does** (`mdb_page_malloc` and its `WRITE_MAP` path, read for the
technique only): a page taken from the environment's spare list (`me_dpages`)
is handed out as it is; a fresh `malloc` is zeroed after the page header
unless `MDB_NOMEMINIT` is set; under `MDB_WRITEMAP` the new page is the map
page itself and is not zeroed. So LMDB zeroes exactly where ZeroDB's
allocator must anyway (fresh memory), and skips the two cases where ZeroDB
pays a `memset` of a whole page.

**Who reads the zeros.** Nothing that a reader, the checker or recovery
interprets:

- `init_tree_page` writes all 32 header bytes (page number, txnid, flags, the
  reserved and checksum fields, `lower`, `upper`, `leaf2_ksize`,
  `reserved1`), and the inserts write every pointer and cell.
- The page views read only `[0, HEADER + lower)` and `[HEADER + upper,
  psize)`. ADR-0030 established that nothing reads the free gap between
  them; `remove_cell` and copy-on-write copies already leave earlier bytes
  there.
- The one engine path that relies on the zeros is a split `MDB_RESERVE`:
  `NewLeafVal::ZeroReserve` packs the reserved value slot into a fresh frame
  and counts on the frame to make it zero. A reserve that does not split
  already gets whatever the gap held: SPEC 04 TXN-47 says the engine must not
  zero a reserved region, the native `put_reserved` requires the closure to
  write all of it, and the heed adapter zeroes any unwritten tail
  (`heed-zerodb/src/reserved_space.rs`).
- One unit test encodes the contract:
  `dirty::tests::discarded_one_page_frame_is_reused_and_zeroed`.

**Measured cost** (macOS laptop, Apple Silicon, 16 KiB pages, a scratch driver
timing 50,000 `APPEND` puts of 256-byte values in one write transaction, loop
time only, minimum of 31 repetitions per run, 5 interleaved runs, a heavily
loaded machine, so indicative only). "Skip" is a throwaway build without the
two `fill(0)` calls:

| loop | zero-filled | skip | skip ÷ zero-filled |
|---|---:|---:|---:|
| `WRITE_MAP` APPEND | 81.6 ns/entry | 73.6 ns/entry | 0.90 (all 5 runs agree) |
| heap APPEND | 113.1 ns/entry | 109.4 ns/entry | 0.97 (within noise) |
| `WRITE_MAP` ascending plain put | 140.6 ns/entry | 136.9 ns/entry | 0.97 |

The heap rows barely move because only reused spare frames are affected: a
large load runs out of spares after 256 new pages and the rest are fresh
allocations, zeroed by both engines. The `put/order/append` ladder rung opens
a fresh environment per iteration, so in heap mode it has no spares at all;
under `ZERODB_BENCH_WRITE_MAP=1` it is 1.38× LMDB on Graviton4 (October 31
run), and this is one of the costs in it.

## Options

### Option A — keep zero-filling every new tree page

No change.

- Bytes on disk: the free gap of a new page is zero until its cells are
  deleted or it is copied.
- Speed: the `WRITE_MAP` append and split-heavy loads keep paying one page
  `memset` per new page that LMDB does not.

### Option B — zero only fresh allocations, and the split `MDB_RESERVE` slot

`insert_tree_frame` zeroes a fresh heap allocation (as today: no allocation
ever exposes uninitialised memory) and hands out a reused spare frame or a
`WRITE_MAP` map page as it is. The caller formats the page with
`LeafMut::init` / `BranchMut::init` as today. A split `MDB_RESERVE` zeroes its
own `dsize`-byte value slot right after placing it, so every observable value
is what it is today.

- Faster: the measurement above, about 10 % of a `WRITE_MAP` APPEND loop at
  16 KiB pages; the heap gain is limited to the spare frames. At 4 KiB pages
  the `memset` is a quarter of the size but a split is four times as
  frequent per byte loaded, so the share should be similar; Graviton is the
  referee.
- Bytes on disk: the free gap of a new page holds the earlier bytes of its
  memory, which are always this environment's own: a spare frame last held a
  page of this environment, and a map page at a fresh page number last held a
  freed page of this file (a TXN-62 page no live snapshot references). This
  is the exposure ADR-0030's resolution accepted for copied pages.
- Determinism: a given sequence of operations from open still writes the
  same file in each mode; heap and `WRITE_MAP` files may differ in gap bytes,
  which ADR-0030 already allows.
- Crash safety, format, recovery: unchanged. No on-disk structure, ordering,
  checksum or recovery rule changes, and no reader, checker or recovery path
  reads a gap.
- ARM: none; it removes a `memset`.
- LMDB parity: matches LMDB's choice on reused and `WRITE_MAP` pages.

### Option C — as B, but leave the split `MDB_RESERVE` slot unzeroed

The same as B, without the slot `memset`: a split reserve then gets stale
bytes, like a reserve that does not split today.

- Within the letter of TXN-47 and the native contract (the closure writes
  everything), and invisible through heed (the adapter zeroes unwritten
  tails).
- But a native caller that breaks the contract would see this environment's
  stale bytes in a value where today it sees zeros, and the saving (one small
  `memset` on a reserve that splits) is negligible.

### Option D — skip zeroing under `WRITE_MAP` only

Keep the heap spare path zeroed; change only the in-map page.

- Takes most of the measured gain with the smallest change.
- Leaves heap and in-map modes with different rules for the same frame
  contract, for a heap saving that is small but free.

## Decision

Pending. Recommended: **Option B**. It copies LMDB's technique for the path,
keeps every observable value identical (the split reserve slot stays zero),
keeps fresh allocations zeroed, and rests on the same "nothing reads a gap"
property ADR-0030 was accepted on.

## Consequences

If B is accepted:

- **Spike first (CONTRIBUTING rule 6).** The scratch build above, plus the
  reserve-slot zeroing, measured with `just bench-ab` on a quiet host:
  `^(put/order|put/val|put/api|commit/batch)` in heap mode and again with
  `ZERODB_BENCH_WRITE_MAP=1`, three columns (LMDB, before, after), laptop and
  Graviton4. Keep it only if `WRITE_MAP` `put/order/append` improves and
  nothing regresses.
- **Code.** `DirtyStore::insert_tree_frame` (the spare and in-map branches)
  and its doc contract; `DirtyStore::reclaimable_frames` doc ("every reuse
  zero-fills or fully overwrites"); `NewLeafVal::ZeroReserve` and the two
  split sites in `rwtxn/insert.rs` that place it, which zero the slot
  explicitly; their comments citing TXN-47.
- **Tests.**
  - `discarded_one_page_frame_is_reused_and_zeroed` is rewritten with this
    ADR's approval to assert the new contract: a reused frame comes back
    from the pool, and after `init` its header and bounds are a valid empty
    page.
  - New: a `MDB_RESERVE` that splits leaves an all-zero value in heap and
    in-place `WRITE_MAP` modes when the closure writes nothing; a load over
    recycled spares and map pages passes `zerodb-tools check`.
  - Unchanged and must pass: `rightmost_finger.rs` (file identity across two
    runs of one mode), `writemap_in_place.rs`, the differential fuzz and the
    crash smoke.
- **SPEC.** SPEC 04 §6.3 (TXN-41's realization note: a reused spare frame is
  handed out unzeroed) and §6.5 (TXN-47: the split reserve slot is zeroed by
  the split); SPEC 03 §5 rule 1 is unchanged (a fresh heap allocation is
  still filled whole).

## Open questions for human review

1. Is it acceptable that a new page's free gap carries this environment's
   earlier bytes, as ADR-0030 accepted for copied pages?
2. Should the split `MDB_RESERVE` slot stay zero (B), or follow TXN-47's
   unzeroed rule like a reserve that does not split (C)?
3. Overflow runs: `insert_run_frame` also zeroes a whole run, under
   `WRITE_MAP` too, where LMDB leaves the map pages as they are. Out of scope
   here (a value's payload overwrites most of the run); worth its own
   measurement?
