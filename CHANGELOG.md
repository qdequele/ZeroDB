# Changelog

All notable changes to ZeroDB. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the release workflow publishes the section matching the tag as the GitHub release notes
(see `docs/maintainers/RELEASING.md`). Versioning policy: ADR-0013.

## [Unreleased]

### Added

- **Multi-process access** (ADR-0023). Several processes can open one
  environment and read or write it, as with LMDB. A lock file is created next
  to the data file by default: `lock.mdb` through heed, `zerodb.lock`
  natively. Its format is ZeroDB's own, not LMDB's. `NO_LOCK` (natively
  `EnvOpenOptions::lock_file(false)`) keeps the single-process mode. This
  retires divergence D-001.
- **`Env::resize`** (heed's `unsafe fn resize`), as `mdb_env_set_mapsize`:
  `0` adopts the size another process recorded. Live read transactions and
  their borrowed values stay valid across a resize. In a lock-file env it waits
  for the cross-process writer lock (D-023).
- **`MdbError::MapResized`**, returned at transaction begin when another
  process has committed past this process's map. Call `resize(0)` and begin
  again.
- **Meilisearch v1.55's heed fork** (ADR-0027): the heed adapter's
  `unique-txns` feature gives the fork's API (`UniqueRwTxn`, `UniqueRoTxn`, `AsUniqueTxnRef`,
  `Env::unique_write_txn` and friends, `non_free_page_size`). Without the
  feature nothing changes. Meilisearch v1.55.0's milli and index-scheduler
  suites pass on it (447 tests).
- Read transactions and nested read transactions gain `commit`: as in LMDB,
  committing one keeps the databases it opened.
- `ReaderEntry::pid`: `Env::reader_list()` reports each slot's owner process.
  Code that builds or exhaustively destructures `ReaderEntry` must add the
  field.
- Native API: `EnvOpenOptions::lock_file` and `lock_file_name` with their
  getters, `derived_lock_file_name`, `Env::has_lock_file`,
  `Env::opened_exclusively` and `ExclusiveLock`.
- `zerodb-core`: `Cursor::advance_by` and `Cursor::retreat_by`.
- New error variant `MdbError::BadDbi`.
- A runnable example, `cargo run -p zerodb --example basic`.
- `zerodb::EnvFlags::from_bits_truncate`.

### Changed

- **The heed adapter is the `[patch]` target itself.** `crates/heed-zerodb`'s
  package is now named `heed` (version 0.22.1), and the `crates/heed-shim` and
  `crates/heed-shim-unique-txns` re-export crates are removed. The one-line
  `[patch.crates-io] heed = { git = "…ZeroDB" }` gives heed 0.22.1's API. A
  consumer on Meilisearch's heed fork adds `"unique-txns"` to the features of
  one of its own `heed` dependencies, since a `[patch]` cannot enable features
  (docs/USING.md). In cargo commands, select the adapter with `-p heed`.
- **Database handles follow LMDB's lifetime** (ADR-0024). A handle opened in
  a transaction that ends without committing, or whose database was removed,
  is refused with `EINVAL`, or `MDB_BAD_DBI` for a read transaction that began
  while it was valid. Before, a write through a handle from an aborted
  `create_database` re-created the database. Freed slots are reused, and
  `max_dbs` counts the databases open now. Two cases are stricter than LMDB
  (D-026).
- **`zerodb-tools` work on live environments.** `stat`, `dump` and `check`
  attach read-only through the lock file and read one pinned snapshot while
  other processes write. `check` walks the raw file only when the env does not
  open, and only while it holds the lock file exclusively. `load` and
  `migrate-from-lmdb` refuse a destination another process has open.
- **`zerodb-tools load` streams its input** (#63): memory stays at about one
  64 MiB batch whatever the dump's size (a 1.4 GB dump: 2,420 → 157 MiB), and
  the loaded file is denser (379 instead of 619 MiB for a 0.7 GB dump; LMDB
  367).
- **`zerodb-tools check` walks large environments on up to 16 threads**
  (#65); findings and their order do not depend on the thread count.
- **Multi-page values reuse runs freed by different transactions** (ADR-0029):
  under churn, files stay within 1.06–1.10× of LMDB's size instead of growing
  to 1.6–2.4×. Divergence D-027 (b) is resolved.
- **Branch and leaf pages split at the end on ascending inserts**, as LMDB
  does, so ascending loads build LMDB's page counts (20,000 ascending keys at
  4 KiB: 480 branch pages instead of 949).
- `Env::clear_stale_readers` frees the slots of dead processes and returns how
  many, as `mdb_reader_check` does.
- `Env::info()` and `Env::stat()` include other processes' commits, and
  `info().num_readers` counts their readers, as LMDB's high-water mark does.
- `PREV_SNAPSHOT` with a lock file behaves as in LMDB: other processes wait
  for the opener's first commit, which is durable but not published. A
  writable `PREV_SNAPSHOT` open fails with `EAGAIN` while another process has
  the env open. Two cases differ (D-025).
- `NO_LOCK` is honored by the heed adapter (it was ignored), and
  `Env::flags()` reports it.
- A `READ_ONLY` open on read-only media runs without a lock file.
- `max_readers` and `max_dbs` are checked before the lock file is created,
  and a joining process adopts the lock file's reader count.
- Opening with a map smaller than the data rounds it up, as LMDB does; a map
  above 2^47 bytes is refused.
- A durable `WRITE_MAP` commit flushes only the used part of the file, as
  LMDB does (ADR-0026).
- Copy-on-write copies only a page's used bytes, as `mdb_page_copy` does
  (ADR-0030). The format is unchanged, but files written in heap and in-place
  `WRITE_MAP` modes can now differ in unused bytes.
- Raw (non-compacting) copies read the data file: an APFS clone on macOS and
  `copy_file_range` on Linux XFS, btrfs and bcachefs (ADR-0032). A 3.2 GiB
  copy on APFS went from 20 s to 0.19 s. The output is unchanged.
- `zerodb-core`: `DirtyStore::sorted_pgnos` is replaced by
  `sorted_pgnos_into`, which fills a caller's buffer.
- `zerodb-tools dump` streams each database to its output instead of
  holding the whole environment in memory, and `migrate-from-lmdb` commits
  about every 64 MiB and grows the map when it fills, as `load` does.
- `zerodb-tools` rejects an option with a missing or unparseable value (such
  as `--page-size abc`) instead of treating it as a path.

### Performance

Ratios are ZeroDB time ÷ LMDB time on Graviton4 unless stated; details in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

- `WRITE_MAP` bulk writes find dirty pages with two bits per page instead of
  a hash probe, and read them at their own offset in the map, as LMDB does
  under `MDB_WRITEMAP` (#13). With `WRITE_MAP`: `put/order/seq` 1.12× →
  0.99×, `commit/batch/n10k` 1.11× → 1.00×, `put/api/reserved` 1.30× → 1.12×;
  nothing changes without `WRITE_MAP`.
- Write transactions reuse the previous one's containers, as LMDB's
  `me_txn0` (#8): 5 heap allocations per single-put commit instead of 19.
  `commit/batch/n1` 1.26× → 1.17×.
- Cursor deletes step in place and keep their path across left-sibling
  rebalances (#137): `del/cursor/drain` 2.41× → 1.66×, `del/cursor/rev`
  3.06× → 1.50×, `del/clear/all` 1.73× → 1.43×.
- Rebalances move cells byte for byte and rewrite separators in place
  (ADR-0028): `del/bulk/half` 2.46× → 1.30× (laptop).
- Write-cursor puts keep their position, including across splits (#7, #138):
  `put/cursor/rewrite` 2.50× → 1.37×; `APPEND` puts 1.26× → 1.21×.
- The validated-pages cache has one slot per page of the file: random gets on
  a 10M-key file 1.32 → 1.23 µs p50.
- Full scans read entries from the leaf in hand, and trusted mode no longer
  records leaves: `scan/full/fwd` 2.25× → 1.45×, and about 1.0× in trusted
  mode.
- `skip` crosses whole leaves without decoding the entries it passes.
- Short read transactions reuse their memory on the same thread (#25).
- The meta checksum uses the ARMv8 `crc32c` instructions on aarch64
  (ADR-0031), elsewhere slicing-by-8 tables.
- The lock file costs about +30 ns per write commit and +7 ns per read txn;
  reopening an env is at parity with LMDB (36.4 vs 37.8 µs), since the lock
  file is rebuilt in place.

### Fixed

- heed adapter: `Database::put_reserved` returns `Io(UnexpectedEof)` when the
  closure writes fewer bytes than it reserved, as heed does. It returned `Ok`
  with a zeroed tail.
- A commit no longer panics with "freelist_save failed to reach a fixed
  point" when the committing transaction's freed-page record sits at an
  overflow page-count boundary.
- An environment whose directory was deleted while open no longer makes an
  unrelated open fail with `EnvAlreadyOpened` on filesystems that reuse inode
  numbers at once (#123).
- Under corruption, a page error met while `skip` crosses leaves is returned
  as an item instead of ending the iterator silently.

### Removed

- Re-exports from `zerodb` that were internals: `build_multi_db_image`,
  `EnvStream`, `NamedDbData`, `PageSink`, `DEFAULT_FILL_PERMILLE`, `KeyCmp`
  and `FIRST_DATA_PGNO`; and the `EnvOpenOptions` getters
  `get_sequential_writes`, `get_max_dirty_bytes`, `get_max_dbs` and
  `get_max_readers`.
- `zerodb-io` exports only `probe_page_size` among its file helpers; `Mmap`,
  `MmapWritable` and the page read and write helpers are now private.
- The `crossbeam-utils` dependency.
- The heed adapter's unreleased `lock_file(bool)` setter: `NO_LOCK` is the
  opt-out.

### Known limits

- Using an `Env` in a child after `fork()`, and sharing an environment across
  PID namespaces, are unsupported. `Env::sync` only serializes with this
  process's commits. On macOS, a writer that takes over from a dead writer
  does not free dead readers' slots; call `clear_stale_readers` (SPEC 04
  §12.1).

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
[`docs/LMDB-COMPATIBILITY.md`](docs/LMDB-COMPATIBILITY.md); intentional differences from
LMDB are in [`docs/LMDB-COMPATIBILITY.md#behavior-differences`](docs/LMDB-COMPATIBILITY.md#behavior-differences).

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
  search 0.94× (2026-10-05 run).
- **hannoy v0.1.7-nested-rtxns** (2026-09-28): HNSW build 0.84–0.89×, search
  0.95× (2026-09-28 run).
- **YCSB** (rust-storage-bench, Graviton4 NVMe): durable YCSB B at 1.08× LMDB's
  throughput; with `WRITE_MAP`, no-sync YCSB at 1.6–1.7× default LMDB's
  throughput, still behind LMDB's own `WRITE_MAP`. The cross-engine results are
  from the 2026-09-30 public-suite run.

Where ZeroDB stands per area, which LMDB techniques closed each gap, and what
remains: [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md). Every
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
  encryption. Each is a documented entry in `docs/LMDB-COMPATIBILITY.md#behavior-differences` or a row in
  `docs/LMDB-COMPATIBILITY.md`; none is used by Meilisearch or hannoy.
- Not yet run: the 24-hour fuzz soak and a 64 KiB-page kernel.
- heed's `WithTls` read transactions compile but are not thread-pinned;
  `clear_stale_readers` returns 0 (correct for a single-process engine).

[Unreleased]: https://github.com/qdequele/ZeroDB/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/qdequele/ZeroDB/releases/tag/v0.2.0
