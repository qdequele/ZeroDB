# ADR-0036: Grow the memory map on demand, behind an option

- Status: Proposed (awaiting maintainer approval; no code changed)
- Related: issue #33; ADR-0004 (mapping strategy), ADR-0017 (spilling),
  ADR-0021 (in-place `WRITE_MAP`), ADR-0023 (multi-process access,
  amendment 15: `resize` and grow-only mappings), ADR-0026 (ranged `msync`);
  divergences D-023 and D-024
- Date: 2026-11-02

## Context

LMDB fixes a maximum database size, `map_size`. An allocation past it fails
the write with `MDB_MAP_FULL` (`MdbError::MapFull`), and the transaction must
be aborted. ZeroDB matches this (SPEC 02 §8, SPEC 05 GC-17).

### What `map_size` controls in ZeroDB today

One number does three jobs:

1. **The `MapFull` ceiling.** `RwTxn::allocate` compares the run's end with
   `map_size / page_size` (`crates/zerodb-core/src/rwtxn/alloc.rs`).
2. **The address space mapped.** At open the data file is mapped over
   `max(file length, map_size)` (`map_backing` in
   `crates/zerodb-io/src/lib.rs`).
3. **The data-file length under `WRITE_MAP`.** The file is `set_len` to
   `map_size` at open, and a joining process grows it to its own map size at
   its first write-txn begin (D-024), so no store through the map faults.

Without `WRITE_MAP` the file already grows on demand: commits extend it with
`pwrite`, and the read mapping simply covers bytes past the end of the file
that no snapshot references (SPEC 06 REC-14). So for the default backing,
"growing the map" only means growing the mapping; for `WRITE_MAP` it also
means growing the file.

### What exists already

`Env::resize` (heed's `unsafe fn resize`, LMDB's `mdb_env_set_mapsize`)
changes `map_size` on an open environment (ADR-0023 amendment 15, SPEC 02 §8):

- a mapping is never remapped in place. A larger size adds a new mapping of
  `max(new size, 2 × current length)` and publishes it with the new logical
  size through `MapRegions` (`crates/zerodb-io/src/mmap.rs`); older mappings
  stay mapped until the environment closes, so every live read transaction
  and every borrowed value survives, and the retained address space stays
  below twice the newest mapping;
- another process learns the new size from the committed meta: a txn begin
  whose snapshot lies past its logical size fails with `MdbError::MapResized`,
  and `resize(0)` adopts the recorded size;
- a live write transaction of the same environment makes `resize` fail with
  `EINVAL` (`OBS_SET_MAPSIZE_WITH_WRITE_TXN`, LMDB parity), so a transaction
  that hits `MapFull` cannot grow and continue.

### How Meilisearch sizes and resizes its maps

Read from the Meilisearch source at v1.54.1-62 (`9d3365c81`, 2026-10-08), the
version the consumer gate runs:

- **Every index opens with a 2 TiB `map_size`** (`INDEX_SIZE`,
  `crates/meilisearch/src/option.rs`). The `max_index_size` option is
  `#[clap(skip)]`: users cannot change it. The task database opens at 20 GiB
  (`TASK_DB_SIZE`) and refuses new tasks once 40% of `info().map_size` is used
  (`NoSpaceLeftInTaskQueue`, `crates/index-scheduler/src/lib.rs`).
- **The 2 TiB per index is paid in address space up front.** At start
  `IndexScheduler::index_budget` probes how much it can map by opening the
  task environment with an 80 TiB `map_size` (6 TiB on Windows), falling back
  to a binary search when that open fails, then divides the budget by the
  per-index map size to decide how many indexes can be open at once (at most
  20). When fewer than two 2 TiB indexes fit, one index gets 40% of the
  budget instead.
- **`MapFull` throws the batch away.** The scheduler maps `MdbError::MapFull`
  to `MaxDatabaseSizeReached`; `IndexMapper::resize_index` closes the index's
  environment and reopens it with `map_size + 10 GiB` (`index_growth_amount`),
  and the tick runs the whole batch again
  (`crates/index-scheduler/src/scheduler/mod.rs`). Meilisearch never calls
  heed's `resize`.
- With `--experimental-reduce-indexing-memory-usage` indexes open with
  `WRITE_MAP`, so each index's data file is set to 2 TiB at open (sparse where
  the filesystem supports it), as LMDB does.

So Meilisearch already reserves a large range per index, in effect option B
below. `MapFull` reaches it in two cases only: an index larger than 2 TiB, or
a host where the address-space probe fails (a kernel with a small user address
space, such as aarch64 builds configured for 39-bit addresses, an
`RLIMIT_AS` limit, or a VSZ-capped container) and every index gets a smaller
fixed share. In both cases the fix that helps is mapping address space in
proportion to the data, not to a guess made at open.

### Constraints

- The default stays LMDB's: fixed size, `MapFull` at `map_size`, `MapResized`
  across processes, `resize` refused under a live write txn. New behavior is
  opt-in (`CONTRIBUTING.md`).
- The aliasing contract of `crates/zerodb-io/src/mmap.rs` must keep holding:
  the file outlives every mapping, nothing faults past the end of the file,
  one writer writes only unshared pages, and no live borrow is rewritten.
  Today it also says "no process truncates the file", which shrinking would
  break.
- No on-disk format change unless an option needs one.

## Options

Each option is described by four questions: how another process learns the
size (the lock file and reader table), what growing costs while pages are
staged, spilled or written in place (`WRITE_MAP`), what the heed API looks
like, and how it is tested. Linux and macOS differences are listed where they
matter.

### Option A — libmdbx-style geometry, growing inside the write transaction

libmdbx, an LMDB fork, replaces the fixed size with
`set_geometry(lower, now, upper, growth_step, shrink_threshold, page_size)`:
the file and the map start at `now`, grow by `growth_step` when an allocation
reaches the end, never pass `upper` (`MDBX_MAP_FULL` there), and shrink at
commit when the free space at the end of the file passes `shrink_threshold`.

In ZeroDB terms this separates job 1 (the ceiling, `upper`) from jobs 2 and 3
(the mapped length and the `WRITE_MAP` file length, starting at `now`):

- **Allocation.** `allocate` checks the ceiling as today, against `upper`.
  When the run would end past the *mapped* length, it grows on a cold path:
  under `WRITE_MAP`, `set_len` the file first (sparse), then add a mapping
  through the existing `grow_regions` (geometric: at least twice the previous
  length, so `log2(upper / now)` mappings over the life of the environment)
  and publish it. The transaction already holds both writer locks, which is
  what `publish` requires, so this is `resize` without the `EINVAL` check, run
  by the owner of the lock instead of refused because of it.
- **Staged pages (heap modes).** Nothing moves: dirty pages are addressed by
  page number. Only the writer's cached whole-map view (`RwTxn::bytes`) is
  stale, and it must be re-derived after a growth exactly as after a spill
  (TXN-71). The default backing does not even need the mapping before it is
  read: a spill writes with `pwrite` (which extends the file), and the mapping
  must cover the spilled pages only before they are read back, and the whole
  snapshot only before commit publishes it. So without `WRITE_MAP` growth can
  wait until the first spill past the mapping, or until commit.
- **In-place `WRITE_MAP` (ADR-0021).** Dirty pages live in the file. A page
  written through the old mapping is the same page-cache page the new mapping
  shows (`MAP_SHARED` of one file on both systems), and the dirty store keeps
  no stored slices (it re-derives them through the broker, which reads the
  current view), so nothing is copied. Two points need care: the aliasing
  contract must be stated per file page across every mapping, not per
  mapping; and commit's ranged `msync` (ADR-0026) goes through the newest
  mapping while some stores went through an older one. On Linux `msync` with
  `MS_SYNC` flushes the file range whichever mapping dirtied it; on macOS this
  is the assumption the spike must check first.
- **Cost of a growth.** One `mmap` (lazy, no page touched) and, under
  `WRITE_MAP`, one `ftruncate`. No staged or spilled page is copied. The
  real cost is afterwards: transactions that begin on the new mapping fault
  their hot pages into fresh page tables once (minor faults, no I/O). With
  geometric growth that happens about `log2(upper / now)` times per process.
  The steady state pays nothing: the hot path keeps one comparison, against
  the mapped length instead of the ceiling.
- **Cross-process.** Nothing new in the lock file or the reader table. The
  commit persists `max(base meta map_size, own map_size)` as today, so the
  meta keeps recording the ceiling. A process whose mapping is shorter than
  another process's commit can extend its mapping at txn begin instead of
  returning `MapResized` when it runs with the option: the logical ceiling is
  unchanged and no borrow is invalidated. That begin-time growth must keep
  publications monotonic without the writer lock (a reader may race the
  writer's own growth), which is a change to the `MapRegions` protocol and
  needs its own miri test. A process opened without the option keeps
  `MapResized` and `resize(0)`, unchanged. A `WRITE_MAP` joiner grows the file
  to its mapped length at write begin (D-024), not to the ceiling.
- **Shrinking.** Truncating the file at commit needs three things this design
  does not have. (1) Only pages that no live snapshot in any process
  references, and that neither meta slot references, can be cut: a crash that
  falls back to the older meta must find every page it names, and an older
  snapshot still has its larger `last_pg`. So the truncation lags the commit
  that freed the tail until the oldest reader and both meta slots are past it.
  (2) "The file only grows" is relied on by D-024 and by
  `WriteMapBacking::file_len`, a cached lower bound of the file length; a
  `WRITE_MAP` process in another process that stores past a new end of file
  gets `SIGBUS`, so every process must learn of the shrink before its next
  write, which means a file-length generation in the lock file (a lock-file
  layout change). (3) The mmap aliasing contract must be rewritten. LMDB never
  shrinks the file, and Meilisearch already reclaims space by compaction
  (`copy_to_path` with `CompactionOption::Enabled`).
- **heed API.** heed 0.22.1 has no geometry call; the adapter adds one beside
  its other extensions (`file_trust`, `sequential_writes`,
  `max_dirty_bytes`). Two shapes:
  - *A-full:* `EnvOpenOptions::geometry(Geometry { lower, now, upper,
    growth_step, shrink_threshold })`, as libmdbx;
  - *A-min:* keep `map_size` as the ceiling, as LMDB defines it, and add one
    method, `EnvOpenOptions::initial_map_size(bytes)`: map, and under
    `WRITE_MAP` preallocate, only `bytes` at open, and grow on demand up to
    `map_size`. `info().map_size` keeps reporting the ceiling, which is what
    Meilisearch compares usage against (the task queue's 40% rule, the
    `+10 GiB` reopen). `resize` keeps changing the ceiling.
- **Testing.**
  - *Unit:* an allocation crossing the mapped length grows and succeeds;
    `MapFull` at exactly the ceiling (SPEC 02 §8's worked boundary, with the
    ceiling in place of `map_size`); a failed growth (`mmap` `ENOMEM`,
    `set_len` error) fails the write with an error and leaves the
    transaction abortable.
  - *Differential:* the oracle runs an operation sequence on LMDB with
    `map_size = upper` and on ZeroDB with `initial_map_size` small and
    `map_size = upper`. Results, contents and the `MapFull` outcome must
    match. The existing `MapFull`, `resize` and `MapResized` differentials
    run unchanged for the default.
  - *Multi-process:* a writer grows mid-transaction while another process
    holds a read transaction and borrowed values across several growths; a
    plain process sees `MapResized` and recovers with `resize(0)`; a process
    with the option extends at begin; a `WRITE_MAP` joiner writes after
    another process grew the file.
  - *Crash harness:* crash points after the `set_len`, after a spill past the
    old mapping, between the page writes and the meta, and after the meta.
    Recovery opens the previous or the new snapshot, never a mix; a file
    longer than the meta's `last_pg` is already legal (REC-14). The
    fault-injection backend (`crates/zerodb-io/src/fault.rs`) already
    implements `grow_map`, and gains injected `mmap` and `set_len` failures.
  - *Stress:* the reader/writer stress test with a tiny initial size, so it
    crosses many growths.

### Option B — reserve a large virtual range at open; `map_size` is a quota

Map (or reserve) a large range once at open, and treat `map_size` as a quota
inside it, so growing is publishing a larger number. Two forms:

- **B1, one large file mapping.** Map `reservation` bytes of the file at open
  (already what happens when `map_size` is large: Meilisearch's 2 TiB). A
  shared file mapping is not charged to the commit limit on Linux, so
  `MAP_NORESERVE` adds nothing to it; the cost is address space only.
  Growth inside the write transaction is `publish(None, new_size)`: no new
  mapping, no page-table warm-up, and in heap modes not even a view
  re-derivation, because the mapping does not change.
- **B2, an anonymous reservation filled with file chunks.** Reserve the range
  with an anonymous `PROT_NONE` mapping (`MAP_NORESERVE` on Linux; private
  `PROT_NONE` memory is not charged either way), then map the file over its
  prefix with `MAP_FIXED`, one chunk per growth, so the map stays one
  contiguous range and old chunks keep their page tables. `memmap2` cannot
  do this; it needs raw `libc::mmap` with `MAP_FIXED`, a new `unsafe` surface
  that the `unsafe` policy must list, and that miri cannot check.

Both forms share A's file handling under `WRITE_MAP` (`set_len` by step), and
A's cross-process story (the meta records the quota; a smaller process gets
`MapResized`).

- **What it costs.** The virtual size is the reservation, whatever the data
  size: it shows in VSZ-based monitoring, counts against `RLIMIT_AS`, and
  limits how many environments one process can hold, which is exactly
  Meilisearch's index budget today. It needs a 64-bit address space large
  enough for the reservation. On Linux, file-backed shared mappings are left
  out of core dumps by default (`coredump_filter`), so dumps are not the
  problem the issue feared. macOS has no `mremap` and documents no
  `MAP_NORESERVE`; B2's `MAP_FIXED` over its own reservation works there, but
  the largest reservation macOS grants must be measured, not assumed.
- **What it does not fix.** Where Meilisearch actually meets `MapFull` (an
  index over its reservation, or a host that cannot reserve much), B cannot
  reserve more either. It removes the remap cost of growth, not the guess.
- **heed API.** `EnvOpenOptions::map_reservation(bytes)` beside `map_size`
  (the quota).
- **Testing.** As A; B2 adds tests that the reservation is never
  dereferenced past the mapped chunks and that `msync` ranges never cross an
  unmapped chunk (`msync` fails with `ENOMEM` there).

### Option C — Meilisearch calls `resize` instead of reopening

No engine change: on `MapFull`, Meilisearch aborts, calls `resize` (larger)
on the open environment and re-runs the batch. It saves the close and reopen
(including waiting for every reader to finish), not the re-run, and it needs
a Meilisearch change. Not a fix for issue #33; listed because it is the
smallest step.

### Option D — Grow before the write transaction begins

A write-txn begin checks the headroom (`map_size − (last_pg + 1) × page_size`)
and grows when it is below a threshold, outside any transaction, so `resize`
keeps its `EINVAL` rule. A single transaction larger than the headroom still
hits `MapFull`, and a Meilisearch indexing batch is one transaction of
unbounded size, so this does not remove the re-run. Rejected.

### Scorecard

| | Faster | Reliable | Durable | Secure |
|---|---|---|---|---|
| **A-min** (initial size + grow inside the txn, ceiling = `map_size`, no shrink) | Same steady state (one comparison on the hot path); a `MapFull` batch re-run (minutes) becomes a growth (one `mmap`, plus `ftruncate` under `WRITE_MAP`); `log2(upper / initial)` page-table warm-ups per process | Better: address space tracks the data (at most twice the newest mapping), so a small budget is spent on data, not reservations; no 2 TiB sparse file under `WRITE_MAP`. New code on the allocation slow path and a new growth point inside the txn | Same: no new write ordering; a file longer than the meta is already legal | Same: bounded by `map_size` and `MAX_MAP_SIZE` |
| **A-full** with shrinking | As A-min, plus file truncation at commit | Smaller files without a copy, but lifts "the file only grows", which several invariants rely on | Worse until proven: the truncation must lag both meta slots and every reader, across processes | A truncation bug becomes `SIGBUS` in another process |
| **B1** (one large mapping, quota) | No remap ever: growth is a number | Virtual size is the reservation; does not help hosts that cannot reserve | Same | Same |
| **B2** (`PROT_NONE` reservation + `MAP_FIXED` chunks) | No remap, old chunks keep page tables | As B1 | Same | Worse: new raw `mmap`/`MAP_FIXED` `unsafe`, outside miri |
| **C** (Meilisearch calls `resize`) | Saves the reopen, not the re-run | Same | Same | Same |
| **D** (grow at begin) | Saves the re-run only for transactions smaller than the threshold | Partial | Same | Same |

## Decision

**Proposed: Option A-min, without shrinking.**

- `EnvOpenOptions::initial_map_size(bytes)` (native and heed adapter) opts in.
  `map_size` stays the ceiling, persisted and reported as today. Without the
  option nothing changes: same code path, same `MapFull`, `MapResized`,
  `resize` and file-length behavior as LMDB.
- With it, the mapping (and under `WRITE_MAP` the file) starts at
  `max(initial size, file length)` and grows inside the write transaction,
  geometrically through the existing grow-only mappings, up to `map_size`.
  `MapFull` is returned only at the ceiling. Without `WRITE_MAP` the growth
  is deferred to the first read of a spilled page past the mapping, or to
  commit.
- An environment opened with the option extends its mapping at txn begin
  when another process committed past it, instead of returning `MapResized`;
  it still returns `MapResized` when the snapshot lies past its ceiling.
- Setting the initial size equal to `map_size` gives B1 exactly, so a caller
  that prefers no remaps keeps that choice. B2 is left for later, and only if
  measurement shows the page-table warm-up after a growth matters.
- Shrinking is out of scope; it would need its own ADR with a lock-file
  change.

Why: it is the only option that helps the hosts where Meilisearch meets
`MapFull` (address space in proportion to the data), it reuses the mapping
mechanism that already keeps every borrow valid, it changes neither the
on-disk format nor the lock file, and the default is untouched.

Following `CONTRIBUTING.md` rule 6 (no consumer calls this yet), the
implementation starts with a spike on the riskiest assumption: in-place
`WRITE_MAP` pages stored through an older mapping, flushed by a ranged
`msync` through a newer one, read back after a crash, on macOS and Linux,
together with the heap-mode view re-derivation after a growth.

## Consequences

- **SPEC.** 02 §8: separate the ceiling from the mapped length; describe
  growth inside the write transaction and the begin-time extension. 04: a
  growth re-derives the writer's cached whole-map view (next to TXN-71), and
  the `WRITE_MAP` begin grows the file to the mapped length, not the ceiling.
  05 GC-17: `MapFull` at the ceiling. Rule IDs are added, never renumbered.
- **`mmap.rs` aliasing contract:** restated per file page across all
  mappings; `MapRegions::publish` gains a monotonic rule if a reader may
  publish.
- **Compatibility doc:** a new row for the option; D-024 amended (a
  `WRITE_MAP` joiner grows the file to its mapped length under the option).
  The default rows are unchanged.
- **Meilisearch** gains nothing until it opts in: open indexes with
  `initial_map_size` (and possibly a larger ceiling), and count mapped bytes
  rather than ceilings in its index budget. That is a Meilisearch change, made
  only through the heed adapter's extension methods.
- **Measurement gate**, three columns (LMDB, ZeroDB before, ZeroDB after):
  the standard write ladder with the option off (must be unchanged) and on;
  a custom case loading from a small initial size to several GiB, against
  LMDB opened at the final size, counting growths and timing the first reads
  after each; the Meilisearch and hannoy consumer suites with the option on.
- **Tests:** as listed under Option A.

## Open questions for the maintainer

1. **Worth doing now?** Meilisearch already reserves 2 TiB per index, so on
   ordinary 64-bit hosts it rarely sees `MapFull`; the win is on
   address-space-limited hosts and for `WRITE_MAP` files, and it needs a
   Meilisearch change to show. Is that enough, or should this wait for a
   consumer to ask?
2. **A-min or A-full API?** One `initial_map_size` method keeps `map_size`'s
   LMDB meaning; a libmdbx-shaped `geometry` call is familiar to libmdbx
   users but carries `growth_step` and `shrink_threshold`, which this design
   would not honor (growth is geometric, no shrink).
3. **Begin-time extension** instead of `MapResized` under the option: accept
   the `MapRegions` change (a reader may publish a longer mapping), or keep
   `MapResized` and let the caller call `resize(0)`?
4. **Growth failure:** when the `mmap` or `set_len` of a growth fails below
   the ceiling, return the I/O error (recommended: the caller can tell an
   address-space or disk problem from a full database) or `MapFull`, as an
   LMDB caller would expect?
5. **Shrinking:** confirm it stays out of scope, with compaction as the way
   to give space back.
