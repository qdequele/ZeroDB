# Changelog

All notable changes to ZeroDB. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the release workflow publishes the section matching the tag as the GitHub release notes
(see `docs/RELEASING.md`). Versioning policy: ADR-0013.

## [Unreleased]

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
