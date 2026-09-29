# ADR-0017: Bounded dirty-page memory in large write transactions (spilling)

- Status: Draft — needs maintainer approval (commit-path change)
- Milestone: Phase 3 (performance / memory), PERF-GAP C2, issue #3
- Date: 2026-09-29

## Context

A ZeroDB write txn keeps every page it touches as a heap frame until commit
(the dirty store, SPEC 04 §6.3). Memory therefore grows with the txn: the
rust-storage-bench YCSB load (one txn of 10M × 128-byte puts) peaked at
**1.7 GB** of anonymous memory, against **~540 MiB** for LMDB on the same load
(`benches/results/2026-09-29-ycsb-bigger-than-ram.md`). Meilisearch indexes in
single large write txns (autobatching up to min(max_indexing_memory / 2,
10 GiB) of payload), so its peak memory during indexing is the indexer's own
budget **plus** the whole dirty set. Under glibc the freed frames also stay in
the process after commit; Meilisearch uses mimalloc, which returns them, so
the steady state is not affected — the peak is.

LMDB bounds this with `mdb_page_spill` (mdb.c:2350): when the txn's dirty room
(`MDB_IDL_UM_MAX` = 131,071 pages by default) runs low, it writes **1/8 of the
dirty pages** to the file early, keeping root pages and pages of active
cursors, and records their pgnos in a spill list. A spilled page is read back
from the map; if the txn writes it again, `mdb_page_unspill` makes it dirty
again. The meta page is not written, so a crash leaves the spilled pages as
unreferenced bytes in free or unused space.

## Options

### Option A — LMDB-style spill (proposed)
When the dirty store exceeds a threshold (default: LMDB's 131,071 pages;
configurable), write the oldest 1/8 of non-root, non-cursor dirty frames to
their pgnos with the commit's page writer (`pwritev`, no fsync), drop the
frames, and record the pgnos in a spill set. Reads of a spilled pgno in this
txn resolve from the map; a write to it copies it back into a frame.
- **Crash safety:** unchanged in principle — spilled pages are fresh
  (beyond the committed high-water) or reclaimed free pages no live snapshot
  reaches (GC gate, SPEC 05), and the meta is written only at commit (SPEC 06
  REC-7). The crash harness must cover spills mid-txn and aborts after spills.
- **Read path:** the writer's `Source` must resolve spilled pgnos above the
  base snapshot's high-water from the map (today refused by the TXN-38
  bound); the validated-pages memo must not treat a spilled page's map bytes
  as immutable across a later re-dirty and re-spill.
- **Abort:** spilled pages were written to free/unused space only, so abort
  needs no undo (their pgnos return to the free set as today).
- **Nested read txns:** readers of a spilling writer only exist while the
  writer is frozen (TXN-29/30), so no spill happens under them.
- **Cost:** extra writes for pages re-dirtied after a spill (LMDB's 1/8 rule
  exists to keep this low); no cost below the threshold.

### Option B — Always write through a writable map (WRITE_MAP-like)
Dirty pages live in the map itself, as LMDB's `MDB_WRITEMAP`. Zero heap
frames, but COW then needs a free page to copy into first and mutations are
visible in the file before commit; ZeroDB's WRITE_MAP currently buffers until
commit (TXN-45a) precisely to keep the no-UB borrow rules simple. Larger
change to the borrow and crash model.

### Option C — Refuse past a cap (`TXN_FULL`)
Bounded, but pushes the problem to the caller; Meilisearch does not expect
`MDB_TXN_FULL` from LMDB, which spills instead.

### Option D — Return memory at commit only
Allocate frames from large chunks and release them when the txn ends. Fixes
glibc retention (not Meilisearch's case) but not the peak.

## Decision (proposed)

Option A, default threshold = LMDB's, behind the full gate (crash-test,
stress, fuzz) and a memory rung: a load txn larger than the threshold must
stay near the threshold's memory, with commit and read results identical.

## Open questions for human review

1. Default threshold: LMDB's 131k pages (512 MiB at 4 KiB, 2 GiB at 16 KiB),
   or a byte budget that does not scale with the page size?
2. Expose it as an env option (e.g. `max_dirty_bytes`)?
3. Land before or after measuring Meilisearch's peak indexing memory on
   hackernews with both engines (to size the benefit on the real consumer)?
