# ADR-0026: Ranged `msync` for `WRITE_MAP` commits

- Status: **Proposed** (2026-10-17). Implemented on the branch
  `qdequele/writemap-ranged-msync` for review; AGENTS.md rule 5 requires
  maintainer approval of this ADR before the change merges.
- Related issue: #45 (follow-up list: #13)
- Date: 2026-10-17

## Context

Under `WRITE_MAP` a write txn stores its dirty pages directly in the
writable map (ADR-0021, SPEC 04 TXN-45b). The commit barriers C3 (data) and
C5 (meta) are `msync`s instead of `fdatasync` (SPEC 06 REC-12). Until this
change each synchronous barrier was `MmapWritable::flush`, an
`msync(MS_SYNC)` of the **whole map**, followed by an `fdatasync` of the data
file on every platform. Issue #45 asks for an `msync` of only the pages the
commit changed, on the grounds that a whole-map `msync` scans every mapped
page.

What LMDB does (the Meilisearch fork, `mdb.c`, read for the algorithm only):

- `mdb_page_flush` writes nothing under `MDB_WRITEMAP` (the pages are already
  in the map); it only clears dirty flags.
- `mdb_txn_commit` then calls `mdb_env_sync0(env, 0, txn->mt_next_pgno)`.
  Under `MDB_WRITEMAP` that is one `msync` of `[0, next_pgno * psize)`: the
  used prefix of the file, both meta pages included, not the whole map. It
  uses `MS_ASYNC` under `MDB_MAPASYNC`. It adds an `fdatasync` only on
  macOS and Windows.
- `mdb_env_write_meta` under `MDB_WRITEMAP` stores the meta fields into the
  mapped meta page, then (unless `NOMETASYNC`/`NOSYNC`) `msync`s only that
  meta page, with its start rounded down to the OS page. No `fdatasync`.
- `mdb_env_sync` (the explicit sync) flushes `[0, last_pg + 1)`.

So LMDB already avoids the whole map: its data barrier covers the used
prefix, and its meta barrier covers one page.

How much the whole-map `msync` costs depends on the kernel. On Linux,
`msync(MS_SYNC)` on a shared file mapping is an fsync of the matching file
range (`vfs_fsync_range` in `mm/msync.c`). It does not walk page tables, and
writeback visits only pages tagged dirty, so its cost tracks the dirty pages,
not the map size. Our `fdatasync` then follows anyway. On Linux, then, a
narrower `msync` changes little; the issue's "tens of milliseconds on a
100 GB map" is not expected there. macOS implements `msync` through the VM
object and may cost more over a large range. No numbers either way yet
(rule 7).

## Options

### Option A — keep the whole-map `msync` (status quo)

Simplest. Durability is trivially a superset. Leaves the macOS cost and
whatever Linux per-VMA overhead exists.

### Option B — LMDB's used prefix

C3 flushes `[0, next_pgno)`, C5 the meta page. Copies LMDB exactly
(rule 7: speedups copy LMDB's technique). Covers pages earlier commits left
unflushed (another process's `NO_SYNC` commits, for instance) without
tracking them. On a large database the range is still as large as the file.

### Option C — exactly the pages this commit wrote (issue #45)

C3 flushes every page the txn wrote (its dirty frames, overflow runs at full
length, and its spilled pages) plus both meta slots. C5 flushes the meta
slot C4 wrote. Range starts are rounded down to the OS page; ranges closer
than 64 KiB are merged. The `fdatasync` after a synchronous barrier stays.
`Env::sync` keeps the whole-map flush.

Completeness argument. Under fixed per-env durability flags, every page a
durable meta `N` references was written either by txn `N` (so it is in `N`'s
frame or spilled set at C3: a dirty page leaves both sets only when it is
freed, and a freed page is referenced by no meta, TXN-62) or by an earlier
txn whose own C3 made it durable. Both meta slots in C3 keep LMDB's
behavior for `NO_META_SYNC`: the previous commit's meta becomes durable at
this commit's C3, which bounds the REC-10 window as before.

Residual difference from LMDB. If two processes open the same environment
with different durability flags, a durable commit by one can reference pages
the other committed under `NO_SYNC`/`MAP_ASYNC` and never flushed. LMDB's
used-prefix `msync` covers them. Option C covers them only through the
`fdatasync`: on Linux it writes back every dirty page of the file; on macOS
it is not guaranteed to flush pages dirtied through another process's map.

## Decision (proposed)

Option C, as implemented on the branch, because it is what #45 asks for and
the narrowest flush. The `fdatasync` is unchanged, so on Linux durability is
unchanged in practice. The approver should weigh Option B: it is LMDB's own
technique and has no cross-process residual.

## Consequences

- New `Backing` methods `ranged_sync()` (default `false`) and
  `sync_pages(async, psize, ranges)` (default: `sync`, a superset). Only
  `WriteMapBacking` and the fault wrapper override them. The default
  backing's path is unchanged and builds no ranges.
- New `MmapWritable::flush_range` over memmap2's
  `flush_range`/`flush_async_range`. No new `unsafe`.
- The fault backing models a ranged barrier strictly: it folds only the
  journaled bytes inside the exact requested page ranges, leaving the rest
  pending. The crash harness gets a tripwire that fails a `WRITE_MAP` cycle
  whose synchronous barriers were never ranged. Under this model, dropping
  spilled pages from the ranges gave 65 violations in 3,000 `WRITE_MAP`
  image cycles; the shipped ranges gave none.
- SPEC 06 REC-12, SPEC 01 (`MDB_WRITEMAP` row, §S7) and SPEC 04 TXN-45b are
  updated.
- Performance: not measured yet. Only durable `WRITE_MAP` commits run the
  changed barriers (`NO_SYNC` skips them). The criterion ladder has durable
  commit rungs (`commit/sync/n1`, `commit/sync/n100`) but no `WRITE_MAP`
  switch (its `ZERODB_BENCH_*` options are `TIER`, `TRUST_FILE`,
  `SEQUENTIAL_WRITES` and `NO_LOCK`). `WRITE_MAP` has been measured through
  rust-storage-bench's durable YCSB `*-wm` runs. Measuring this change
  needs either those runs, on a map much larger than the data, or a
  `WRITE_MAP` switch on the `commit/sync/*` rungs.

## Open questions for human review

1. Option B or C? (See the cross-process residual.)
2. Should Linux drop the `fdatasync` after a synchronous `msync`, as LMDB
   does? That would halve the device flushes per durable `WRITE_MAP`
   commit. It is a separate durability change and would make Option C's
   ranges load-bearing on Linux.
3. Is a measured win on macOS alone, a development platform, enough to merge
   this?
