# ADR-0036: Grow the memory map on demand, behind an option

- Status: Accepted (Option A-min, without shrinking; approved by the
  maintainer on 2026-11-02); implemented 2026-11-02 (see Implementation),
  measurement on a quiet Linux host owed
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

**Accepted (2026-11-02): Option A-min, without shrinking**, as written below.

- `EnvOpenOptions::initial_map_size(bytes)` (native and heed adapter) opts in.
  `map_size` stays the ceiling and is reported as today; the meta records
  the size grown so far (amended by the spike, point 1). Without the
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

## Spike results (2026-11-02)

### The riskiest assumption holds on both systems

`crates/zerodb-io/src/mmap.rs`, test
`a_flush_through_a_newer_mapping_writes_back_older_stores`: two writable
mappings of one file, the older one shorter, as after a growth. A store
through the older mapping is followed by one of three flushes, and the test
checks that the process's disk-write counter moved by at least one OS page
(`/proc/self/io` on Linux, `proc_pid_rusage` on macOS) and, on macOS, that
`mincore` reports the page dirty after the store and clean after the flush.

| Flush after a store through the older mapping | macOS (Apple silicon, APFS) | Linux (aarch64 VM, overlay on its disk) |
|---|---|---|
| `msync` of the range through the older mapping (control) | written back | written back |
| `msync` of the range through the newer mapping | written back | written back |
| `fdatasync` of the descriptor alone | written back | written back |

A synchronous `WRITE_MAP` commit does both of the last two (a ranged `msync`
through the newest mapping, then `fdatasync`), so a page stored in place
before a growth is made durable by the commit after it. What the test shows
is writeback to the device, not survival of a power cut, which needs real
hardware; the crash harness below covers the engine's ordering. The probe
adds test-only `unsafe` calls in `zerodb-io` (`sysconf`, `mincore`,
`proc_pid_rusage`), which the `unsafe` policy must list if the test is kept.

### The engine change is small

The prototype touches no mapping code, no lock file, no reader table and
no on-disk format:

- `EnvOpenOptions::initial_map_size` on the native and heed builders. Open
  maps the initial size (or what the data needs) and records `map_size` as
  the ceiling on the environment.
- `RwTxn::allocate`, where it returned `MapFull`, now calls a cold
  `grow_map_for`: past the ceiling it is still `MapFull`; otherwise it grows
  the logical size to at least twice its value, capped at the ceiling,
  through the backing's existing `grow_map` (the file first under
  `WRITE_MAP`, then a new mapping), and in heap modes re-derives the writer's
  cached view, as after a spill. A failed `mmap` or `set_len` returns the
  I/O error (question 4's recommendation).
- `Env::info` reports the ceiling; `Env::resize` with a non-zero size sets
  the ceiling under the option.

### What the spike changes in the design

1. **The mapping keeps covering the logical size.** The Decision describes
   a logical size equal to the ceiling with a shorter mapping. The spike
   instead grows the logical size itself and keeps the ceiling beside it, so
   the existing invariant "the published mapping covers the logical size"
   still holds and nothing that reads the view changed. Consequence: a commit
   persists the size grown so far, not the ceiling (the existing rule,
   `max(base meta map_size, own map_size)`), so a plain process that adopts
   it with `resize(0)` maps what the data needs. Approved by the maintainer
   on 2026-11-02: this replaces the Decision's "persisted as today" and the
   Option A remark that the meta keeps recording the ceiling.
2. **The free-list save must not see the growth.** Its top-of-file
   completion (`loose_run`) checked the logical size; with growth on it chose
   a different run than a fixed map would. It now checks the ceiling and the
   caller grows the map to cover the extension. Found by the differential
   test below. *(Moot since on-disk format 3, ADR-0037: the free-list save
   asks only for single pages, and `loose_run` is gone.)*
3. **Growth at allocation in every mode is correct.** Deferring it to the
   first spill read or to commit without `WRITE_MAP` remains an optimisation;
   the spike did not need it.
4. **Begin-time extension was not built.** Another process still gets
   `MapResized` and recovers with `resize(0)`, as the two-process tests show;
   with point 1 that is LMDB's behaviour unchanged. *(Built in the
   implementation, as the Decision says.)*

### Tests run

- `crates/zerodb/tests/map_growth.rs`, 8 tests, on macOS:
  - a seeded workload of three 8,000-op transactions, with the smallest
    dirty limit so pages spill past the old mapping mid-txn, gives the same
    contents and the same high-water page as a fixed map of the ceiling's
    size, in heap, in-place `WRITE_MAP` and durable `WRITE_MAP` modes, before
    and after reopen with and without the option; a value borrowed before
    any growth is intact after all of them; under `WRITE_MAP` the file stays
    far below the ceiling, while the fixed map's file is the full ceiling;
  - `MapFull` comes at the same put, with the same high-water page, as with
    a fixed map of the ceiling, with and without `WRITE_MAP`;
  - two processes: a plain reader keeps its borrow while the other process
    grows the map, then gets `MapResized` and follows with `resize(0)`; a
    second `WRITE_MAP` process with the option joins after the first grew
    the file, adopts the size and grows the map and the file again.
- Crash harness, with growth on a third of cycles (the cycle spec draws it
  last, so other seeds keep their earlier fields): 3,086 image-cut states,
  927 states from mixed image and SIGKILL cycles, and 157 multi-process
  cycles, all verified, none abandoned. A new summary line counts image cuts
  whose map grew before the cut: 35 cuts (about 420 crash states) in the
  image run.
- `cargo test --workspace` on macOS, and `cargo clippy -D warnings` on
  macOS and for `x86_64-unknown-linux-gnu`. Not run: miri, loom, fuzzing,
  the stress test and the consumer gate.

Not measured: performance. The steady-state path is unchanged (the extra
code runs only where `MapFull` was returned), but the three-column gate in
Consequences still applies to the staged implementation.

## Implementation (2026-11-02)

Built on the spike, after merging main (on-disk format 3):

- **Begin-time following.** A read or write begin of an environment with
  the option whose snapshot lies past its logical size but within its
  ceiling raises the logical size instead of returning `MapResized`
  (`EnvInner::follow_growth`, from the decoded-snapshot check and the
  write-begin base check). The raise goes through a new
  `Backing::extend_map`, not `grow_map`: it holds no writer lock, so it
  must never `set_len` the file (an `ftruncate` racing the writer's could
  shorten the file), and it needs none, since a committed snapshot lies
  inside the file. A `WRITE_MAP` write begin still grows the file to the
  logical size under both writer locks before anything is stored.
- **Monotonic publication.** Every decision to publish a view, and its
  `mmap`, now runs under the regions mutex (`MapRegions::update`), and a
  raise never lowers the logical size, so a reader's raise is serialized
  with a writer's growth and never publishes over a newer view. Tested
  under miri with a raising thread, a growing thread and a watcher.
- **`resize` under the option:** a non-zero size sets the ceiling (the
  logical size keeps what has been grown, within it); `0` adopts the meta's
  size and raises the ceiling to it if it is lower.
- **Growth failure (question 4):** the I/O error of the `mmap` or file
  growth, as recommended, with the txn left as after a `MapFull`
  (abortable, the environment not poisoned). Awaiting confirmation.
- **Not built, by choice:** deferring growth to the first spill read or to
  commit without `WRITE_MAP`. With the spike's design (point 1) the mapping
  must cover the logical size at all times, and a growth costs one lazy
  `mmap` call, so deferring would break an invariant to save nothing
  measurable. This replaces the Decision's sentence about deferral.
- **Docs:** SPEC 02 §8 (map growth bullet, `resize`, mapping strategy,
  `MapFull`), SPEC 04 TXN-74 (new rule) and the begin checks, SPEC 05 GC-17,
  the `mmap.rs` aliasing contract (stated per file page across mappings),
  `docs/LMDB-COMPATIBILITY.md` (option row, D-028), `docs/USING.md`. D-024
  needs no change: a `WRITE_MAP` joiner grows the file to its logical size,
  which is its map size.

### Checks run for the implementation

- `cargo test --workspace` (191 test binaries) on macOS; the growth tests
  (`map_growth.rs`, 10 tests; the `zerodb-io` mapping and flush tests; the
  growth-failure test; the heed adapter test; the growth stress for 60 s)
  also on Linux (aarch64 VM).
- New tests: a process with the option follows another's growth without
  `MapResized`, in read and write begins, with and without `WRITE_MAP`;
  `resize` sets the ceiling and `resize(0)` keeps a higher one; a failed
  growth returns the I/O error and leaves the env usable; raises never lower
  the logical size, alone and concurrently with growths (miri); the heed
  adapter's option survives the TLS retag and `MapFull` comes at the
  ceiling; a reader/writer stress variant whose in-place `WRITE_MAP` writer
  starts from a 64 KiB map.
- Crash harness: 3,094 image-cut states (38 cuts after a growth), 934 mixed
  image and SIGKILL states, 157 multi-process cycles; all verified, none
  abandoned.
- `just stress` at 180 s (the four reader/writer variants, including the
  growth one, and the two-process stress), miri on `zerodb-core` and on the
  `MapRegions` tests, `just fuzz-quick` (the four targets for their full
  time, no crash), clippy `-D warnings` on macOS, for
  `x86_64-unknown-linux-gnu` and with heed's `unique-txns` feature. Loom was
  not run: the reader table is unchanged.

### Performance

Option off, `scripts/bench-ab.sh '^(put|commit/batch|env/txn)/'`, three
interleaved rounds on the development Mac (Apple silicon, shared with other
builds), before = main at `ec84097`: 22 rungs flat, 0 regressed, 2
unreliable (LMDB drift above 3 %).

Option on, the same binary with `ZERODB_BENCH_INITIAL_MAP=65536` against
the option off, `'^(put|commit/batch)/'`: every iteration builds a fresh
environment from a 64 KiB map, so it pays every growth (about nine) inside
the timed region, the worst case. With heap staging, flat (16 flat, 5
unreliable). Under `WRITE_MAP`, three rungs slower: `put/gc/ovf_across`
1.67× (about 0.7 ms per iteration), `put/val/v4k` 1.21× (about 4 ms),
`put/api/plain` 1.04×. A `WRITE_MAP` growth is a file extension, a new
writable mapping, and page faults again on pages already touched through the
old mapping. Once the map has reached its working size nothing differs.

Tables: `benches/results/2026-11-03-map-growth-macos.md`; both runs are in
the performance ledger. The quiet-host gate (Linux, and a load that grows to
several GiB against LMDB opened at the final size) is still owed.

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

The maintainer approved the Decision section as written on 2026-11-02, which
settles questions 1, 2, 3 and 5: build it now, the one-method A-min API,
extension at txn begin under the option, and no shrinking. Question 4 is
implemented as recommended (the I/O error) and awaits confirmation.
Question 6 comes from the implementation's measurements.

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
6. **A minimum growth step?** Growth doubles the logical size, so a map
   started at 64 KiB grows about nine times before it reaches tens of MiB,
   and under `WRITE_MAP` each growth costs a file extension, a new mapping
   and page faults again (measured above). A floor on the step (libmdbx's
   `growth_step`; for example 1/64 of the ceiling, or a fixed 16 MiB) would
   cut early growths to one or two at no address-space cost worth counting.
   Add it, or leave the initial size to the caller?
