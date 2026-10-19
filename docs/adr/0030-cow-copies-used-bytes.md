# ADR-0030: A copy-on-write copies only a page's used bytes, as LMDB

- Status: **Accepted (Option A, maintainer, 2026-10-19), implemented.** It
  changes which bytes a committed page carries in its free gap (never read,
  but written to disk), so it was decided before it was implemented
  (AGENTS.md rule 5). A spike measured it first (rule 6).
- Related issue: none yet. Background:
  [`benches/results/2026-10-19-crud-scan-count-and-batch-delete.md`](../../benches/results/2026-10-19-crud-scan-count-and-batch-delete.md).
- Date: 2026-10-19

## Context

crud-bench's `batch_delete_1000` (1,000 random-key deletes per write
transaction on a 500k-entry tree, about 1.2 KB values, 16 KiB pages,
`WRITE_MAP | MAP_ASYNC | NO_SYNC`) runs at about 0.7–0.8× LMDB's throughput.
A repro of the same shape (load, 250 × 1,000 creates and updates, then 250 ×
1,000 timed deletes) gives LMDB ~2.2 µs and ZeroDB ~2.7 µs per delete.

A `sample` profile of ZeroDB's delete phase:

| frame | share |
|---|---:|
| `RwTxn::touch` → `DirtyStore::insert_copy` → `memmove` (the 16 KiB copy-on-write copy) | 37 % |
| `search_path_into` (descent; branch binary-search key compares, cache misses) | 40 % |
| `LeafMut::remove` → `memmove` (cell-heap compaction) | 9 % |
| commit | 3 % |

The descent's cost is the same kind of cost LMDB pays in `mdb_node_search`,
and read transactions already run batched point reads at parity
(`batch_read_1000` 0.97–1.15× LMDB), so the descent is not the gap. The copy
is where the two engines do different work.

**What LMDB does** (`mdb_page_touch` → `mdb_page_copy`, read for its
technique only): when it copies a page for copy-on-write it copies the header
and pointer array (`[0, lower)`) and the cell heap (`[upper, page end)`) and
skips the free gap between them. Under `WRITE_MAP` the new page is the map
page itself, so the gap keeps whatever that file page held before.

**What ZeroDB does** (SPEC 04 §6, `DirtyStore::insert_copy`): copies all
`psize` bytes, in heap mode into a pooled frame and under in-place
`WRITE_MAP` (ADR-0021) into the map page at the fresh page number. With ~10
entries of ~1.2 KB per 16 KiB leaf, the gap is often 2–4 KiB of each copy.

ZeroDB already writes stale bytes into gaps: `remove_cell` slides the cell
heap up without zeroing the vacated bytes, and `NO_MEM_INIT` is an accepted
no-op. So a page's gap already holds leftovers from its own deleted cells.
Nothing reads a gap: the page views read only `[0, lower)` and
`[upper, end)`, and `check_image` does not inspect it.

## Options

### Option A — copy the used bytes only (LMDB's technique)

`insert_copy` (and `unspill_copy` through it) copies `[0, HEADER + lower)`
and `[HEADER + upper, psize)` when the source's bounds are well formed, and
the whole page otherwise (so a corrupt header never makes the copy read out
of bounds). A freshly allocated heap frame (no pooled spare) still copies
the whole page, so a new allocation never exposes uninitialised memory.

- Faster: the spike (a scratch build of this option over the count-scan fix)
  measured ~2.43 vs ~2.73 µs per delete in the repro, 5 interleaved rounds
  on a busy Mac (load 8–13; one outlier round dropped), about 11 % on the
  delete, close to LMDB's ~2.2–2.4 µs.
- Bytes on disk: the gap of a copied page holds the previous contents of its
  new page number (under `WRITE_MAP`) or of a recycled frame of the same
  transaction (heap mode), instead of the source page's gap. Both are bytes
  this file already held. Files written by the same sequence of operations
  stay identical run to run, but are no longer identical between heap mode
  and in-place `WRITE_MAP` in their gaps.
- Crash safety and format: unchanged. No reader, checker or recovery path
  reads gap bytes.
- ARM: none; plain copies.

### Option B — keep copying the whole page

No change. The delete gap stays ~10 % wider than it needs to.

### Option C — copy the used bytes and zero the gap

Same speed for the copy, plus a `memset` of the gap. It makes gaps
deterministic (all zero) but adds back about half the bytes Option A saves,
and is more than ZeroDB does today (`remove_cell` leaves stale bytes).

## Decision

**Option A**, as LMDB.

## Consequences

- Tests: a test that a copied page's live region is byte-identical to its
  source's for leaves and branches, in heap and in-place `WRITE_MAP` modes;
  the whole suite (in particular `writemap_in_place.rs`, which compares
  results, and `rightmost_finger.rs`, which compares files across two runs of
  one mode) must pass unchanged; differential fuzz and crash smoke.
- SPEC 04 §6 (dirty frames) gains a sentence that a copy-on-write copy
  carries the source's used bytes and not its gap.
- Bench: a ladder rung for random batched deletes on a larger-than-cache
  tree (the shape no rung covers yet), plus crud-bench, three columns.

## Open questions for human review

1. Is it acceptable that a page's free gap carries bytes from an earlier
   page at that page number rather than its source's gap, given gaps
   already carry stale cell bytes today?
2. Should heap mode and in-place `WRITE_MAP` keep producing byte-identical
   files (which would need Option C, or a full copy in one of the two
   modes)?

## Resolution (2026-10-19)

The maintainer chose Option A ("A", 2026-10-19), after the trade-offs above
were laid out. That settles both open questions:

1. Yes: a page's free gap may carry bytes from an earlier page at its page
   number. Gaps already carry stale cell bytes, and nothing reads them.
2. No: heap mode and in-place `WRITE_MAP` need not write byte-identical files.
   Their live page contents stay identical, and each mode stays deterministic
   for a given sequence of operations.

## Measured after implementation (2026-10-19)

Graviton4 `c8g.xlarge`, 4 KiB pages: `^(del|put|commit/batch)/` flat (0
regressed, `commit/batch/n1` 0.960), the batch-delete repro within 3 %. At
4 KiB a leaf of ~1.2 KB values has little gap to skip, so the gain is the Mac
spike's ~11 % at 16 KiB pages only. Details:
`benches/results/2026-10-19-crud-scan-count-and-batch-delete.md`.
