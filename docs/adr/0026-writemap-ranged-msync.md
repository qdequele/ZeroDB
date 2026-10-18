# ADR-0026: Ranged `msync` for `WRITE_MAP` commits

- Status: **Accepted (Option B, maintainer, 2026-10-17)**. First proposed
  with Option C; reworked to Option B before merge.
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

## Decision

**Option B**, LMDB's technique (approved by the maintainer, 2026-10-17):

- **C3** `msync`s the used prefix `[0, next_pgno)`, with `next_pgno` the
  txn's high-water at commit. The prefix holds every page any snapshot up to
  this commit can reference: this txn's frames and spilled pages, both meta
  slots, and pages earlier commits wrote and never flushed (`NO_SYNC`, in
  this process or another). Flushing the other meta slot keeps the REC-10
  window under `NO_META_SYNC` bounded, as before.
- **C5** `msync`s the meta slot C4 wrote.
- The range start is rounded down to the OS page (POSIX `msync`).
- The `fdatasync` after a synchronous barrier **stays on every platform**.
  Dropping it on Linux, as LMDB does, would be a separate durability change
  with its own ADR.
- `Env::sync` / `force_sync` keep the whole-map flush.

Why B over C: B is LMDB's own technique (rule 7), it needs no per-commit
range building, and it has no cross-process residual. On a database that
fills most of its map, B flushes about as much as the whole-map `msync`.
The gain is on maps much larger than the data, which is how Meilisearch
sizes its maps.

Answers to the review questions:

1. Option B.
2. Keep the `fdatasync` on Linux.
3. A win measured on macOS only is enough to merge. Linux is not expected to
   move (see Context).

## Consequences

- New `Backing` methods `ranged_sync()` (default `false`) and
  `sync_pages(async, psize, ranges)` (default: `sync`, a superset). Only
  `WriteMapBacking` and the fault wrapper override them. The default
  backing's path is unchanged.
- New `MmapWritable::flush_range` over memmap2's
  `flush_range`/`flush_async_range`. No new `unsafe`.
- The fault backing models a ranged barrier strictly: it folds only the
  journaled bytes inside the exact requested page ranges, leaving the rest
  pending. The crash harness gets a tripwire that fails a `WRITE_MAP` cycle
  whose synchronous barriers were never ranged. The ranged-barrier tests
  catch a shortened prefix: leaving out the meta slots, the last page, or
  the upper half of the prefix each fails them.
- The bench ladder gets a `ZERODB_BENCH_WRITE_MAP=1` switch, which opens
  both engines' envs with `WRITE_MAP`.
- SPEC 06 REC-12, SPEC 01 (`MDB_WRITEMAP` row, §S7) and SPEC 04 TXN-45b are
  updated.
- Performance: see below.

## Performance

`scripts/bench-ab.sh` with `ZERODB_BENCH_WRITE_MAP=1`, 3 interleaved
rounds, Apple M-series Mac. Before = `0d26fdf` (whole-map `msync`), after =
this change. `commit/sync/n1_map16g` is `commit/sync/n1` on a 16 GiB map.

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | verdict |
|---|---:|---:|---:|---:|---:|---|
| `commit/sync/n1` | 801.7 ms | 1.607 s | 1.530 s | 2.00× → 1.91× | 0.952 (±0.070) | flat |
| `commit/sync/n100` | 843.0 ms | 1.564 s | 1.572 s | 1.85× → 1.87× | 1.006 (±0.085) | flat |
| `commit/sync/n1_map16g` | 793.4 ms | 1.605 s | 1.486 s | 2.02× → 1.87× | 0.986 (±0.085) | flat |

Every rung is flat. The whole-map `msync` did not cost more on a 16 GiB map
than on a 1 GiB one (1.605 s against 1.607 s before the change), so on
macOS the cost #45 describes is not visible at these sizes, and Linux was
not expected to move. The change is kept for LMDB parity of the barrier
ranges, with no measured speedup.

The 2× gap to LMDB on these rungs is elsewhere. On macOS each durable
ZeroDB `WRITE_MAP` commit issues two `fdatasync`s (after C3 and after C5);
LMDB issues one, after its data `msync`, and none after the meta `msync`.
That is the separate durability question this ADR leaves open.
