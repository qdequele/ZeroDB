# Changelog

All notable changes to ZeroDB. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the release workflow publishes the section matching the tag as the GitHub release notes
(see `docs/RELEASING.md`). Versioning policy: ADR-0013.

## [Unreleased]

### `WRITE_MAP` bulk writes at LMDB speed (issue #13)

Under `WRITE_MAP`, every step of a write transaction's tree descent asked the
dirty-page hash index whether the page was dirty. On a large write
transaction those probes missed the cache, which cost 15–17 % of the cycles of
a bulk insert. Once a transaction holds 64 dirty pages, the dirty store now
also keeps two bits per page number. A dirty tree page is then found with one
load and read at its own offset in the map, as LMDB reads a dirty page under
`MDB_WRITEMAP`. Validation is unchanged. On Graviton4 with `WRITE_MAP`,
`put/order/seq` went from 1.12× to 0.99× LMDB, `commit/batch/n10k` from 1.11×
to 1.00× and `put/api/reserved` from 1.30× to 1.12×. 7 of 11 ladder rungs got
faster and none slower. A control run of 9 of those rungs without `WRITE_MAP`
(all but `put/api/*`) is unchanged.
Details: `benches/results/2026-10-31-writemap-dirty-bits-graviton.md`. New
example `writemap_bulk_census` (`zerodb-oracle`).

### Write transactions reuse the previous one's containers (#8)

Every write transaction built its working containers empty: the dirty-page
and spilled-page indexes, the freed, loose and reclaimed page sets, the
commit's sorted page list, the open-table slots and the descent buffers. A
small commit spent much of its CPU growing and freeing them. They now live in
a box that rides the env's writer slot from one write transaction to the
next, used in place, as LMDB keeps its single write transaction (`me_txn0`)
per env. Hash indexes keep capacity for about 128 entries and vectors 4,096,
so one large transaction does not make later small ones walk an oversized
table. A write transaction also starts from the snapshot the previous one
published when nothing has committed since, instead of taking the snapshot
cell's lock, and drains the base meta page's free-list annex in place
instead of copying it. Nothing observable changes. A single-put commit on a
named database makes 5 heap allocations instead of 19, and an empty write
transaction none instead of one. On Graviton4 a single-put no-sync commit
takes 5.17 µs instead of 5.51 (LMDB: 4.25) and 5.74k user cycles instead of
6.68k; `commit/batch/n1` went from 1.26× to 1.17× LMDB, no rung regressed.
Details: `benches/results/2026-10-31-writer-carry-graviton.md`. New test
`crates/zerodb/tests/write_txn_allocations.rs`. In `zerodb-core`,
`DirtyStore::sorted_pgnos` is replaced by `sorted_pgnos_into`, which fills a
buffer the caller reuses.
### Faster cursor deletes and `clear` (issue #137)

Four changes to the delete path, each taken from LMDB's technique for it; the
tree shape and the file are unchanged.

- The write cursor steps inside its leaf in place after a delete, as
  `mdb_cursor_next`/`mdb_cursor_prev` do: it reads the next slot from the path
  it keeps instead of rebuilding a cursor around it, and no longer allocates a
  copy of each deleted key.
- A delete that borrows from or merges with its **left** sibling keeps the
  cursor's path (LMDB's `mdb_node_move` and `mdb_page_merge` cursor fix-ups)
  instead of re-descending, and `prev` after a delete steps back from the kept
  path. Backward drains (`rev_range_mut` + `del_current`) used to re-descend on
  every step. SPEC 03 §5.4a.
- Removing a cell fixes the page's cell pointers in one pass, as
  `mdb_node_del` does.
- `clear` frees a page of the base snapshot with a list append (it cannot have
  a dirty frame), reserves its page list per branch (`mdb_midl_need`), and
  checks the collected pages for duplicates with a bitmap instead of a sort.

On Graviton4 (ZeroDB time ÷ LMDB time): `del/cursor/drain` 2.41× → 1.66×,
`del/cursor/rev` 3.06× → 1.50×, `del/clear/all` 1.73× → 1.43×, `del/bulk/half`
1.41× → 1.35×; cursor rewrites (`put/cursor/rewrite`) 2.12× → 1.75×, and no
put or commit rung slowed. New ladder rung `del/cursor/rev` (the backward
drain). Details: `benches/results/2026-10-31-delete-path-cursor.md`.

### The validated-pages cache covers the whole file (ADR-0018 amendment)

The env-wide cache of validated page versions had 64 Ki slots, so on a file of
more than 64 Ki pages most random reads found their leaf's slot taken by
another page and checked the leaf's cells again. Each page now has its own
slot, up to 2^23 pages (32 GiB at 4 KiB pages, 128 GiB at 16 KiB), allocated
as pages are first committed or validated: 8 bytes per page, 64 MiB per
environment at most.
A slot is one atomic word instead of a seqlock over three, so a lookup is one
load and a publish one store. Files and the public API are unchanged.
On Graviton4, random gets on a 10M-key file went from 1.32–1.36 µs to
1.23–1.24 µs p50 (LMDB 0.96–1.08 µs) and `scan/full/big` from 27.9 ms to
24.3 ms; the cache holds 2.5 MiB for that 1.33 GB file.

### Write-cursor puts keep their position across splits; cheaper cursor steps and APPEND puts (#138)

A put through the write cursor (`put_current`, or heed's `put_current` on a
`RwIter`/`RwRange`) that splits its leaf used to drop the cursor's
root-to-leaf path, and the next step searched from the root again. The path
now follows the entry to whichever half holds it, through branch splits and a
root grow, as LMDB fixes up its cursor after a page split. The write cursor
also steps on its parked path in place instead of copying it in and out per
step, yields the entry the step landed on without looking the page up a
second time, and rewrites a same-size value on a page the transaction already
owns in one page access, with no copy-on-write pass over the path (LMDB's
`MDB_CURRENT` put on a dirty page). `APPEND` puts no longer view the
rightmost leaf twice and skip the copy-on-write pass when the whole right
spine is already the transaction's. Behaviour and files are unchanged.
On Graviton4, `put/cursor/rewrite` went from 2.50× to 1.37× LMDB's time,
`put/order/append` from 1.26× to 1.21× and `del/cursor/drain` from 2.62× to
2.15×; without commit costs, the cursor rewrite loop went from 4.1× to 2.1×
LMDB's CPU time per entry. Details:
`benches/results/2026-10-31-write-cursor-split-append-graviton.md`. New example
`crates/zerodb-oracle/examples/cursor_put_census.rs`; the differential
fuzzer's cursor-put walk gains a `grow` mode that splits on most puts.

### Raw environment copies read the data file: a clone on APFS, `copy_file_range` on Linux (ADR-0032)

A non-compacting copy (`copy_to_file` with `CompactionOption::Disabled`) wrote
every page from the memory map. It now takes the pages from the data file while
its read transaction pins the snapshot, then writes the snapshot's two meta
pages over the copy's. On macOS, a copy to a path clones the file (an APFS
clone shares the file's blocks) and trims it to the snapshot; a copy with a
progress callback, a copy into an open file, and a `WRITE_MAP` env still stream
from the map. On Linux, when the copy lands on the data file's own XFS, btrfs
or bcachefs filesystem, both forms clone the snapshot's bytes with
`copy_file_range`; on other filesystems (ext4, where the in-kernel copy measured
slower) they still stream. Writers are not paused, the output is byte-identical
to before, and nothing is synced. A 3.2 GiB raw copy to a path on APFS went
from 20 s to 0.19 s (LMDB: 20–23 s).
New example `crates/zerodb-oracle/examples/raw_copy_bench.rs`.

### Hardware CRC32C for the meta checksum on aarch64 (ADR-0031)

On aarch64 CPUs with the `crc` feature (every Graviton and Apple M-series
core), the meta-page checksum uses the ARMv8 `crc32c` instructions, detected
at run time. Other CPUs keep the slicing-by-8 table. The checksum value is
the same, so files are unchanged. A full 16 KiB annex is checksummed in
0.75 µs on Graviton4 and 2.4 µs on Apple M-series, against 9–10 µs for the
table. Commits that free many pages gain the most: one delete per
transaction on a large tree costs about 0.77× as much as on the table path
(Mac). A new criterion rung, `del/random/n1`, measures this case.
Details: `benches/results/2026-10-19-meta-crc-hardware.md` and
`-graviton.md`.

### Copy-on-write copies only a page's used bytes (ADR-0030)

When a write transaction first modifies a page, ZeroDB copied all of it. It now
copies the header with the pointer array and the cell area and skips the free
gap between them, as LMDB's `mdb_page_copy` does. The gap of a copied page keeps
whatever its new page held before; nothing reads it, and ZeroDB already left
stale bytes there when a cell was removed. The on-disk format and recovery are
unchanged. Files written in heap mode and in in-place `WRITE_MAP` mode can now
differ in those gap bytes. Batch deletes at 16 KiB pages gain about 11 %; at
4 KiB pages (Graviton4) the change is flat, with nothing regressed. New
benchmark rung `del/batch/k1000`.

### Faster full scans over large trees under the trusting policy

A full scan (`iter().count()`, a `for` loop over `iter`) over a tree much larger
than the CPU caches ran at about half LMDB's speed. Two causes, both fixed:

- Under the trusting file policy (ADR-0014), every leaf a scan visited was
  recorded in the read transaction's validated-pages memo. A scan sees each leaf
  once, so the memo grew to megabytes and every probe missed through all its
  levels. Trusted leaves are no longer recorded; branches still are (ADR-0014
  amendment 2026-10-19). The header checks on each leaf view are unchanged, and
  the default validating policy is untouched.
- A cursor step within a leaf now reads the entry from the leaf view it already
  holds, as LMDB's `mdb_cursor_next` does, instead of re-deriving the position.

In a crud-bench-shaped repro (500k entries, ~0.9 GB), a full count went from
0.57× to about 1.0× LMDB. On Graviton4, full scans take 1.45× LMDB's time
(was 2.25×) under the default policy and about LMDB's time (1.02–1.06×, was
~2.0×) under the trusting one; range scans and point gets got faster too, and no rung regressed. New
benchmark rung `scan/full/big`. Numbers:
`benches/results/2026-10-19-crud-scan-count-and-batch-delete.md`.
### A deleted environment no longer blocks opening an unrelated one (#123)

ZeroDB refuses a second open of a lock file this process already has open,
recognizing the file by its inode number. When an environment's directory
was deleted while the environment was open, closing it freed the lock
file's inode before its entry left that registry. On filesystems that reuse
inode numbers at once (ext4, for one), a lock file created by another thread
in that brief window got the same number and its open failed with
`EnvAlreadyOpened`, although its directory was brand new. The descriptor now
closes and the entry is removed under the registry's lock, so no open can see
the stale entry. Reproduced on Linux ext4 (two false refusals in 30 s of
concurrent opens; none in 60 s with the fix).

### `zerodb-tools load` streams its input (#63)

`load` used to read the whole dump into memory, parse every record into
owned buffers and sort each database there, so its peak memory grew with the
dump. It now reads the dump a line at a time, twice: a first pass rejects a
malformed dump (or an over-long key) before the destination is created, the
second writes each database in dump order in 64 MiB write txns. A dump
piped in (`load /dev/stdin <dir>`) is copied during the first pass to an
unlinked spool file in the destination directory. Peak memory is about one batch
plus its dirty pages, whatever the dump's size. The dump format and the
command line are unchanged.

- Generated dumps of 0.7 GB and 1.4 GB (macOS aarch64, `/usr/bin/time -l`
  peak memory footprint): 1,212 MiB → 156 MiB and 2,420 MiB → 157 MiB.
  LMDB's `mdb_load` (Meilisearch fork) on the same 0.7 GB dump: ~1 MiB,
  since it commits every 100 records.
- Writing the main database before the named ones keeps its keys on the
  append path, so the loaded file of the 0.7 GB dump is 379 MiB instead of
  619 MiB (half-full pages before), close to LMDB's 367 MiB.
- In a hand-edited dump, records out of order still land in key order, and
  of several records with the same key in a database the first is kept, as
  before; this now also holds across repeated blocks of the same database.

### `check` walks large environments on several threads (#65)

`zerodb-tools check`, the check after `zerodb-tools load`, and
`check::check_image` / `check::check_snapshot` walked every tree on one
thread and kept the reached pages in a hash set. The reached pages are now a
bitmap (one bit per page instead of a hash-set entry), and an environment of
at least 16 384 pages is walked on up to 16 threads: each record's root
children and each named database are independent jobs. A parallel walk that
finds anything is redone on one thread, so the findings and their order
never depend on the thread count. Pages listed free more than
once (INV-24) are now reported in ascending page order rather than hash
order.

- 2.1 GB environment, Apple M-series (8 cores, busy host, indicative):
  `zerodb-tools check` 0.39-0.47 s -> 0.16-0.20 s with the page cache warm;
  the walk alone 238 ms -> 205 ms on one thread, 60 ms on eight. LMDB has no
  equivalent checker.

### Short read transactions reuse their memory (#25)

A read transaction that reads one key used to allocate, zero and free two
per-transaction structures: the first 8 KiB level of the validated-pages
memo and the named-database record table. Both are now cleared when the
transaction drops and handed to the next one opened on the same thread (one
of each is kept per thread). The memo clears exactly the slots it wrote; no
entry is ever carried from one transaction to the next. heed 0.22.1 exposes
no `mdb_txn_reset` / `mdb_txn_renew`, so the reuse is internal and needs no
API change.

- New rung `env/txn/ro_get_one` (begin, one named-DB `get`, drop). Memo
  reuse alone, Apple M-series: 0.57x -> 0.42x LMDB's time (after / before
  0.735); `env/txn/ro_begin_abort` flat.

### Multi-page values reuse free pages freed by different transactions (ADR-0029, #29)

A value larger than a page needs a run of contiguous free pages. ZeroDB
looked for that run inside one freed-page record at a time (one record per
freeing transaction), so a run whose pages were freed by two transactions
was missed and the file grew by the whole value. When no single record holds
the run, ZeroDB now merges the records the reuse gate admits, with the meta
page's freed list, and searches the merged list, as LMDB merges the records
it reads into its free list. The merged list is kept for the transaction;
the records and how they are saved are unchanged.

- `free_run_churn` (macOS aarch64, 16 KiB pages; 2,000 rewrites of mixed
  inline and 2–16 page values per commit): file size vs LMDB 1.64× → 1.06×
  after 200 commits, 2.43× → 1.10× after 400. Small-commit and inline-only
  controls unchanged (0.98×, 0.80×).
- `lmdb_free_run_observation.rs` gains two cases where the run spans records
  and is still whole; ZeroDB now places the value where LMDB does. Divergence
  D-027 (b) is resolved; (a), reuse one transaction sooner than LMDB, stays
  proposed.
- New tests: reuse, reader gate and a crash at every commit step for a run
  drawn from two records (`crates/zerodb/tests/gc_reclaim.rs`), and a
  property test (`free_run_cross_record_proptest.rs`). New bench rungs
  `put/gc/ovf_across` and `put/gc/ovf_miss`.
- Commit no longer panics with "freelist_save failed to reach a fixed point"
  when the committing transaction's own freed-page record sits at an
  overflow page-count boundary: the save could shrink the record by a page,
  grow it back by listing that page, and repeat once per page of the meta
  page's freed list. Once the record has shrunk during a save, growing it
  now takes pages from outside the set it lists (SPEC 05 GC-13 (v)). Found
  by the property test on Linux CI; the shrunk case is kept as a fixed test.

### `skip` crosses whole leaves instead of stepping each entry

The read iterators (`iter`, `rev_iter`, `range`, `rev_range`, `prefix_iter`,
`rev_prefix_iter`) override `Iterator::nth`, which `Iterator::skip` calls. A
skip now moves through a leaf by its entry count and hops to the next leaf,
without decoding, bound-testing or reading the value of the entries it
passes; only the landing entry is read. The results are the same as `n` calls
to `next`, which the new differential test checks against LMDB. LMDB's C
cursor moves one entry per call, so heed over LMDB cannot skip faster than
that.

- New cursor operations `Cursor::advance_by` and `Cursor::retreat_by` in
  `zerodb-core` (SPEC 03 §4).
- A paged listing such as `iter.skip(5000).take(100)` runs several times
  faster; the numbers are in the pull request. A plain `count()` is
  unchanged: std routes it through `try_fold`, which never calls `nth`.
- Under corruption only: a damaged overflow page behind a skipped entry is no
  longer noticed, and a page error met while crossing leaves is returned as
  the item rather than ending the iterator silently.
- New benchmark rungs `scan/skip_take/fwd` and `scan/skip_take/rev`.

### Faster deletes: rebalance moves cells page to page (ADR-0028)

When a delete leaves a page under a quarter full, ZeroDB borrows an entry
from a neighbor page or merges the two. It used to decode each moved entry
into owned buffers, re-encode it, and rewrite the parent's separator by
removing and re-inserting it. It now copies the entry's encoded bytes
directly and rewrites the separator in place, as LMDB does. The resulting
tree is identical (same pages, same page numbers, same `stat`); only the
work to get there shrinks.

- Criterion ladder (Apple M-series, busy host): `del/bulk/half` 2.46× →
  1.30× LMDB's time, `del/cursor/drain` 2.59× → 1.50×, `del/churn/reinsert`
  1.67× → 1.21×, `del/range/half` 0.67× → 0.24×.
- crud-bench deletes (one delete per transaction) gain ~10–15 %: that
  workload is bound by commit, not by the rebalance.
- SPEC 03 §10.1 numbers the rebalance rules (REB-1..REB-5). New golden test
  `rebalance_logical_image` pins the tree shape deletes leave behind.

### Less CPU per commit for the meta page (#8)

- The meta-page CRC32C uses slicing-by-8 tables instead of one lookup per
  byte, with bit-identical results (about 5x faster on the ~200 bytes it
  covers).
- A commit encodes its meta page into a zeroed buffer the environment keeps
  between commits, instead of allocating and zeroing a fresh page each time.
  The bytes written to disk are unchanged.
- One-key no-sync commit on macOS arm64: user CPU 4.17 µs → 3.45 µs per
  commit (LMDB 1.50 µs). Wall time `commit/batch/n1` 0.95–0.97× of before.
  Empty write transactions are unchanged.

### Meilisearch v1.55's heed fork (ADR-0027, #102)

Meilisearch v1.55 builds against its own heed fork, whose API differs from
heed 0.22.1's: opening and creating databases goes through *unique*
transactions, which hold a per-environment mutex. `heed-zerodb` gains a
`unique-txns` feature with that API, and `crates/heed-shim-unique-txns` is
the matching patch target. Without the feature nothing changes.

- New with the feature: `UniqueRwTxn`, `UniqueRoTxn`, `AsUniqueTxnRef`,
  `Env::unique_write_txn`, `unique_read_txn` and their `try_` forms,
  `EnvStat::non_free_page_size`, `DatabaseStat::non_free_page_size`.
- Changed with the feature: `open_database` and `DatabaseOpenOptions::open`
  take a unique transaction; `create_database` and
  `DatabaseOpenOptions::create` take `&mut UniqueRwTxn`.
- Removed with the feature, as in the fork: `RoTxn::commit` (now
  `UniqueRoTxn::commit`) and `Env::non_free_pages_size`.
- `scripts/consumer.sh` picks the shim from the consumer's lock file;
  `HEED_SHIM` overrides it. Its default `MEILISEARCH_REF` moves from v1.53.1
  to v1.55.0.
- Meilisearch v1.55.0 on ZeroDB (macOS arm64): milli and index-scheduler
  compile with no C LMDB in their trees, and their suites pass, 447 tests
  with 0 failures (5 ignored by Meilisearch). Two HuggingFace embedder tests
  fail when run in parallel, on a lock while downloading the model, and pass
  when run alone.

### Database handles follow LMDB's lifetime (ADR-0024)

A database handle opened in a transaction that ends without committing, or
whose database was removed, is now refused, as LMDB refuses it. Operations
through such a handle return `EINVAL` (`Error::Io`, OS error 22), or
`MDB_BAD_DBI` for a read transaction that began while the handle was still
valid. Before, ZeroDB kept serving the handle, and a write through a handle
from an aborted `create_database` re-created and committed that database.

- Read transactions, and nested read transactions, gain `commit`: as in LMDB,
  committing one keeps the databases it opened, and dropping it closes them.
- Freed handle slots are reused, and `max_dbs` counts the databases open now,
  not every name ever opened.
- Two cases are stricter than LMDB (divergence D-026): a handle whose slot
  was reused by another database is refused instead of reading that
  database, and two transactions opening one database for the first time
  share its handle.
- New error variant `MdbError::BadDbi`.

### Branch pages packed on ascending inserts

#### Changed

- **A full branch page that takes a new last child now splits with two
  children on the new page** and keeps all the others, instead of splitting
  near the middle — the split point LMDB uses (SPEC 03 §6.4). Ascending
  loads (sorted bulk loads, increasing ids) leave branch pages nearly full
  rather than about half full: at 4 KiB pages, 20 000 ascending 500-byte
  keys now build 480 branch pages and a 6-level tree, against 949 pages and
  7 levels before.
- **A plain put at the end of a full leaf now moves the old last entry to the
  new page with the new one**, as LMDB does; only an `APPEND` put starts the
  new page with the new entry alone. Before, every end insert did that, so
  ZeroDB's leaves on ascending plain puts held one entry more than LMDB's
  (4 KiB pages, 20 000 ascending 500-byte entries: 2858 leaves, LMDB 3334).
  At 16 KiB pages both engines now build exactly the leaf count this rule
  predicts. Files built by ascending plain puts grow
  accordingly; `APPEND` loads do not change.

### Multi-process access (ADR-0023)

Several processes can now open one environment at the same time and read or
write it, as with LMDB. The environment's lock file is **on by default**. This
retires divergence D-001 (single-process only).

#### Changed

- **A lock file is created next to the data file by default**: `lock.mdb`
  through the heed adapter (as LMDB), `zerodb.lock` for the native
  `zerodb.dat`. It holds the shared reader table, the last published txnid
  and the cross-process writer lock. Its format is ZeroDB's own, not LMDB's.
  The writer lock is a futex word on Linux and an `fcntl` record lock on
  macOS; process liveness is an `fcntl` record lock per process, so a
  reused PID is never mistaken for a dead process.
- **`NO_LOCK` is honored** by the heed adapter (it was silently dropped): it
  creates no lock file and keeps the single-process mode, and `Env::flags()`
  reports it. The native opt-out is `EnvOpenOptions::lock_file(false)`. The
  adapter's own `lock_file(bool)` setter, which never shipped in a release,
  is removed: `NO_LOCK` is the opt-out.
- A **`READ_ONLY` open on a read-only filesystem** runs without a lock file
  and serves read transactions, as LMDB does. Any other lock-file error, such
  as `EACCES` in a directory the process cannot write, fails the open.
- **Lock-file name derivation**: the default name follows the data-file name,
  `zerodb.dat` → `zerodb.lock`, `data.mdb` → `lock.mdb`, any other name
  `<name>-lock`; `lock_file_name` overrides it. The open refuses, without
  modifying anything, a lock file that is the data file under another name
  (a hard link, or a case variant on a case-insensitive filesystem) and a
  non-empty file that is not a ZeroDB lock file, such as another env's data
  file or a stale LMDB `lock.mdb`.
- **`max_readers` and `max_dbs` limits are checked before the lock file is
  created**, so an out-of-range value leaves nothing behind. A process that
  joins a running env adopts the lock file's reader count whatever it asks
  for, and a first opener never shrinks the count an earlier run recorded,
  as LMDB.
- **`Env::clear_stale_readers` returns a count**: in a lock-file env it frees
  the reader slots of processes that died holding them and returns how many
  slots it freed, as `mdb_reader_check` counts. It returns 0 with `NO_LOCK`.
  As in LMDB, a writer that takes the writer lock over from a dead process
  (Linux) also frees them; nothing else reaps.
- **`ReaderEntry::pid`**: `Env::reader_list()` reports each slot's owner PID
  in a lock-file env (`None` with `NO_LOCK`). This adds a public field to a
  struct without `#[non_exhaustive]`, so code that builds a `ReaderEntry` with
  a struct literal or destructures it exhaustively must add the field.
- `Env::info()` and `Env::stat()` include other processes' commits as soon as
  they are published, as LMDB's do. `info().num_readers` (heed's
  `number_of_readers`) counts other processes' readers too: as LMDB's
  `me_numreaders`, it is the high-water mark of reader slots used by any
  process since the lock file was initialized, and never decreases.
- A `PREV_SNAPSHOT` open fails with `EAGAIN` while another process has the
  env open, as LMDB; the geometry of such an open comes from the older meta
  it selects.
- **A `PREV_SNAPSHOT` open with a lock file behaves as LMDB's** (ADR-0023
  amendment 19): other processes' opens wait until its first commit, and
  that commit is durable but not published. Read txns, in this process and
  in processes that join, keep the older snapshot; the second write txn
  reuses the first commit's txnid, starts from the older snapshot and drops
  the first commit's changes. A fresh open after the env is closed serves
  the first commit, so milli's rollback (one commit, then close) is
  unchanged. Without a lock file the first commit is published at once, as
  before. A process that joins after the first commit reports that commit
  in `Env::info`/`Env::stat`, as LMDB. Two cases differ from LMDB (D-025,
  approved): an opener that ends before its first commit, and a `READ_ONLY`
  `PREV_SNAPSHOT` open joining a running env, which ZeroDB refuses with
  `EAGAIN` while LMDB rolls every process back.
- **`zerodb-tools` attach to live environments**: `stat`, `dump` and `check`
  open the env read-only through its lock file and read one pinned snapshot
  while other processes keep writing, as `mdb_stat` and `mdb_dump` do.
  `check` falls back to walking the raw file only when the env does not
  open, and only while holding its lock file exclusively. `load` and
  `migrate-from-lmdb` refuse a destination another process has open, and
  `load` now writes through ordinary write transactions. The data-file
  `flock` the tools used to take is gone.

#### Added

- **`Env::resize`** (heed's `unsafe fn resize`), as `mdb_env_set_mapsize` on
  an open env: `0` adopts the size another process recorded, a smaller size
  rounds up to the data, and a live write txn gets `EINVAL`. Live read
  transactions and the values they borrowed stay valid: a resize adds a
  longer mapping and keeps the old ones until the env closes. In a lock-file
  env it waits for the cross-process writer lock (an approved divergence, D-023:
  LMDB's `mdb_env_set_mapsize` takes no lock and returns while another
  process holds a write txn, `OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`).
- **`MdbError::MapResized`** in the engine, returned at transaction begin
  when another process committed past this process's map size (the heed
  adapter's existing variant of that name now occurs); answer with `resize(0)` and begin
  again. Opening with a map below the data now rounds the map up to it, as
  LMDB does. A map size above 2^47 bytes is refused.
- Native API: `EnvOpenOptions::lock_file`/`lock_file_name` and their getters,
  `derived_lock_file_name`, `Env::has_lock_file`, `Env::opened_exclusively`
  and `ExclusiveLock`.

#### Removed

- The `crossbeam-utils` dependency: reader slots use their own padded type,
  shared with the lock file's layout.

#### Performance

- **The lock file has a cost**, measured on Graviton4 (EBS io2, ext4;
  `benches/results/2026-10-13-multi-process-stage3-graviton.md`) against
  0.2.0: +29.8 ns per write commit, +6.8 ns per read txn begin/abort, +7 ns
  per `Env::stat`. With `NO_LOCK` (`lock_file(false)` natively) every rung
  is flat.
- **Opening an env with the lock file is at parity with LMDB.** The first
  opener of an env now rebuilds the lock file in place instead of truncating
  it to zero, as LMDB does (ADR-0023 amendment 20, issue #94). Truncating
  made ext4 free and reallocate the file's blocks on every open. On Graviton4
  (EBS gp3, ext4; `benches/results/2026-10-15-lock-file-in-place-graviton.md`),
  env reopen went from 1016 µs to 36.4 µs per open (LMDB 37.8 µs), and env
  create got 1.13 ms faster. Before this, the lock-file default had cost
  +155 µs per create and 17 → 347 µs per reopen on the io2 host of the
  stage-3 ladder.

#### Known limits

- Using an `Env` in a child after `fork()`, and sharing an env across PID
  namespaces, are unsupported. On macOS a writer that follows a dead writer
  does not free dead readers' slots (call `clear_stale_readers`).
  `Env::sync` serializes only with this process's commits. Details: SPEC 04
  §12.1.

### Write-cursor puts keep their position (#7)

#### Changed

- **A put through the write cursor of the key it sits on now writes at the
  cursor's position**, as LMDB's `MDB_CURRENT` put does: no search from the
  root for the put, and none for the next step, which resumes from the kept
  path. This covers `RwCursor::put_current` and heed's `put_current` on
  `iter_mut` / `range_mut` / `prefix_iter_mut` and their reverse forms. A
  put that splits its leaf, a put with flags, or a put of another key still
  takes the keyed path and re-seeks once (SPEC 03 §5.4b). The new
  `put/cursor/rewrite` benchmark (10 000 same-size rewrites through
  `range_mut` + `put_current`) went from 5.47× to 2.49× LMDB's time on
  macOS arm64 (ZeroDB 5.33 ms → 2.43 ms).

### `WRITE_MAP` commits flush the used prefix, as LMDB (#45, ADR-0026)

#### Changed

- A durable `WRITE_MAP` commit no longer `msync`s the whole map. Its data
  barrier flushes the used part of the file, `[0, next_pgno)`, both meta
  pages included, and its meta barrier flushes the meta page it wrote: the
  ranges LMDB flushes. The `fdatasync` that follows a synchronous barrier is
  unchanged, and `Env::sync` / `force_sync` still flush the whole map.
  Measured flat on macOS, including on a 16 GiB map. SPEC 06 REC-12.

## [0.2.0] - 2026-10-05

First published release. **On-disk `format_version` 2.** Files are not LMDB
files, and files written by development builds before 2026-10-05
(`format_version` 1) are refused at open: migrate with `zerodb-tools dump` built
from the older tree, then `zerodb-tools load` from this release. LMDB
environments migrate with `zerodb-tools migrate-from-lmdb` (an opt-in build
feature that links C LMDB).

### What ZeroDB is

A pure-Rust, transactional, memory-mapped key-value engine with LMDB's
architecture: a single writer, lock-free MVCC readers, copy-on-write B+trees,
double-buffered CRC32C meta pages and reader-gated free-page reuse. It ships
behind a 1:1 re-implementation of the API of heed 0.22.1 (the Rust LMDB wrapper
Meilisearch uses), and every behavior is checked against the LMDB fork
Meilisearch runs.

- **Use the engine directly:** `zerodb = "0.2"` from crates.io (with
  `zerodb-core` and `zerodb-io`). `zerodb-tools` (`stat`, `dump`, `load`,
  `check`, `migrate-from-lmdb`) installs with `cargo install zerodb-tools` or
  from the binaries attached to this release (linux x86-64, linux aarch64,
  macOS aarch64).
- **Drop it under a heed consumer:** `[patch.crates-io] heed = { git =
  "https://github.com/qdequele/ZeroDB", tag = "v0.2.0" }`. The heed adapter is
  git-only because cargo needs a crate named `heed` for the patch. Meilisearch
  and hannoy build against it with no source changes.

The full coverage matrix of heed items and LMDB features is
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md); intentional differences from
LMDB are in [`docs/DIVERGENCES.md`](docs/DIVERGENCES.md).

### Features

- The heed 0.22.1 surface, including nested read transactions inside a write
  transaction (the LMDB-fork feature milli depends on), `PREV_SNAPSHOT`,
  read-only environments, `copy_to_file`/`copy_to_path` with and without
  compaction, and the `NO_SYNC`/`NO_META_SYNC`/`MAP_ASYNC` durability modes.
- **In-place `WRITE_MAP`**: a write transaction's dirty pages live directly in
  the writable map, as with LMDB's `MDB_WRITEMAP` — no heap staging and no copy
  at commit (ADR-0021).
- **Bounded write-transaction memory**: past a dirty-page limit
  (`EnvOpenOptions::max_dirty_bytes`, by default LMDB's 131,072 pages) a write
  transaction spills dirty pages to the file, as LMDB does (ADR-0017).
- `NO_READ_AHEAD` is honored as in LMDB (`madvise(MADV_RANDOM)` on the map).
- Opt-in, default off: trusted-file mode (`FileTrust::trust_contents()`), which
  skips read-path page validation as LMDB does (ADR-0014), and a
  sequential-writes fast path with a per-database override (ADR-0015).
- Extensions beyond heed: `Env::sync(force)`, `Env::reader_list()`,
  `Env::live_readers()`, `EnvOpenOptions::page_size` (4 KiB–64 KiB, chosen at
  creation), `copy_to_path_with_progress`, safe custom key comparators on named
  databases, and compaction that streams with memory bounded by tree depth ×
  page size.
- `zerodb-tools`: `stat`, `dump` (mdb_dump-shaped logical format), `load`,
  `check` (invariant checker), `migrate-from-lmdb`.

### Performance

ZeroDB runs its consumers at LMDB-level performance (ratios are ZeroDB time ÷
LMDB time; below 1 is faster):

- **Meilisearch v1.53.1**, on this release's code (2026-10-05, x86-64 bench
  server): movies indexing 1.01×, incremental hackernews additions 0.99×, movies
  search 0.94×.
  [`benches/results/2026-10-05-meta-annex-real-case.md`](benches/results/2026-10-05-meta-annex-real-case.md)
- **hannoy v0.1.7-nested-rtxns** (2026-09-28): HNSW build 0.84–0.89×, search
  0.95×.
  [`benches/results/2026-09-28-evening-bench-server-linux-x86.md`](benches/results/2026-09-28-evening-bench-server-linux-x86.md)
- **YCSB** (rust-storage-bench, Graviton4 NVMe): durable YCSB B at 1.08× LMDB's
  throughput; with `WRITE_MAP`, no-sync YCSB at 1.6–1.7× default LMDB's
  throughput, still behind LMDB's own `WRITE_MAP`. The cross-engine results are
  in [`benches/results/2026-09-30-public-suite-nvme.md`](benches/results/2026-09-30-public-suite-nvme.md).

Where ZeroDB stands per area, which LMDB techniques closed each gap, and what
remains: [`docs/PERF-GAP-VS-LMDB.md`](docs/PERF-GAP-VS-LMDB.md). Every
performance experiment, kept or reverted, is a line in
[`benches/results/perf-ledger.jsonl`](benches/results/perf-ledger.jsonl).

### Verification

- **Differential testing** against the exact LMDB fork Meilisearch ships: every
  PR runs the workspace suite (653 tests at this release), a 10-minute
  differential fuzz on x86-64 and aarch64, `miri` on the core, `loom` on the
  reader table and a crash-injection smoke; nightly runs add a 4-hour fuzz,
  10,000 crash cycles and a reader/writer stress test.
- **Consumers**: Meilisearch v1.53.1 and hannoy run unmodified on this release
  (the benchmarks above). Their own test suites (milli 271 lib + 92 integration
  tests, index-scheduler 80 tests, hannoy) last passed on ZeroDB on 2026-09-09,
  before spilling, in-place `WRITE_MAP` and `format_version` 2; they were not
  re-run for this release.
- **Hostile or corrupt files** fail with typed errors instead of crashing: open
  refuses page references beyond the file, the read path refuses pages above
  the snapshot's high-water mark instead of `SIGBUS`, the free list is validated
  before reuse, and files are created `0600` and never through a planted
  symlink. A fuzz target feeds arbitrary bytes to `open`. Scope:
  [`SECURITY.md`](SECURITY.md).

### Requirements

- **MSRV 1.98** (the toolchain Meilisearch pins). Developing ZeroDB itself
  (oracle, benches, fuzzers) needs a current stable toolchain.
- **Platforms**: linux-aarch64 (primary), linux-x86_64, macOS aarch64.

### Known gaps

- Not supported: `DUPSORT`/`DUPFIXED` and the other `DatabaseFlags`, nested
  *write* transactions, multi-process access (no lock file), `Env::resize`,
  `Env::set_flags`, `get_or_put*`, `MDB_NOSUBDIR` (refused at open),
  encryption. Each is a documented entry in `docs/DIVERGENCES.md` or a row in
  `docs/COMPATIBILITY.md`; none is used by Meilisearch or hannoy.
- Not yet run: the 24-hour fuzz soak and a 64 KiB-page kernel.
- heed's `WithTls` read transactions compile but are not thread-pinned;
  `clear_stale_readers` returns 0 (correct for a single-process engine).

[Unreleased]: https://github.com/qdequele/ZeroDB/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/qdequele/ZeroDB/releases/tag/v0.2.0
