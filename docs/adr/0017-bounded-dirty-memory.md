# ADR-0017: Bounded dirty-page memory in large write transactions (spilling)

- Status: Accepted (approved by the maintainer, 2026-09-30, with the goal of LMDB-level memory usage)
- Implementation note (2026-10-05): implemented and kept — 629e945 (`benches/results/perf-ledger.jsonl`). Under in-place `WRITE_MAP` (ADR-0021) spilling reduces to bookkeeping.
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

## Decision

Option A, LMDB-style spill, specified in SPEC 04 §6.3a (TXN-68..72), with
SPEC 06 REC-6 H0 amended. Answers to the review questions (2026-09-30,
following the recommendation the maintainer approved):

1. **Default threshold:** LMDB's — 131,072 dirty pages, counted in pages as
   LMDB counts them (512 MiB at 4 KiB pages; 2 GiB at 16 KiB, as with LMDB).
2. **Option:** yes, opt-in: `EnvOpenOptions::max_dirty_bytes` (zerodb and
   heed-zerodb), limit = `max(bytes / psize, 128)` pages. Default unset =
   LMDB's. Divergence D-021.
3. **Order:** implement now; the acceptance test is the rust-storage-bench
   YCSB C load (10M unsorted keys in one txn under a 2 GB cap), which LMDB
   completes and ZeroDB could not (killed at 2.09 GB, 2026-09-30). Meilisearch's
   peak indexing memory is measured afterwards to size the consumer benefit.

Design points settled by the spec:

- Trigger at the start of every mutating entry (`ensure_open`, after the
  child guard), when nothing borrows a frame: `dirty_pages + 64 > limit`.
- Spill at least `max(64, limit / 8)` pages, highest pgnos first, keeping
  tree roots and finger pages; write with the commit's batched page writer.
  LMDB also keeps the pages of every open cursor (`P_KEEP`); ZeroDB's
  cursors hold no frames between calls (they re-resolve by pgno), so a
  spilled page on a cursor's path is simply brought back on its next write.
  Results are identical; only the number of re-reads can differ.
- Spilled pages are read back from the map through the ordinary resolution
  path, whose bound is raised past the highest page any spill wrote (the
  file backs everything below it); every spill resets the writer's
  validated-pages memo, since a spill is the only time a spilled page's map
  bytes change. A touch brings a spilled page back into a frame at the same
  pgno (`mdb_page_unspill`). A first version resolved spilled pages through a
  separate lookup inside the force-inlined resolution function instead; it
  grew every inlined descent and cost read-only rungs 3–13 %
  (`scan/edge/first_last`, `get/db/named_x8`, `scan/full/rev`) in the
  codegen-units=1 build, so the hot path is now byte-identical to before.
- Crash and abort: TXN-62's writable set is unchanged; only timing moves.
  Under `NO_META_SYNC` that timing widens the known reclaim-clobber window
  (SPEC 06 REC-10 amendment) from "C2 to C3" to "first spill to C3", as with
  LMDB; the default mode stays immune. Harness, one seed, same workload:
  5 stale fallbacks without spilling, 48 with it, 0 violations.

## Acceptance

- YCSB C (10M × 128 B, 2 GB cap) completes; peak anonymous memory during the
  load near the limit (≈ LMDB's ~540 MiB) instead of > 2 GB.
- Results and committed files byte-identical to an unbounded run (differential
  test with a tiny limit against the default).
- Crash harness with spill-heavy cycles (small limit) green; the full gate.
- The engine ladder flat (no spill below the limit: one comparison per op).
