# Changelog

All notable changes to ZeroDB. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the release workflow publishes the section matching the tag as the GitHub release notes
(see `docs/RELEASING.md`). Versioning policy: ADR-0013.

## [Unreleased]

## [0.1.0] - 2026-09-09

First tagged release. **On-disk `format_version` 1.** Not compatible with LMDB
files (migration is logical: `zerodb-tools migrate-from-lmdb`, or `dump` on
LMDB and `load` here). Files written by this release open unchanged in every
later release that keeps `format_version` 1.

### What ZeroDB is at 0.1

A pure-Rust, transactional, memory-mapped key-value engine with LMDB's
architecture (single writer, lock-free MVCC readers, copy-on-write B+tree,
double-buffered CRC32C meta pages, reader-gated free-page GC) behind a 1:1
re-implementation of heed 0.22.1's API. Meilisearch and hannoy build against it
with **zero source changes** through `[patch.crates-io] heed = { git =
"https://github.com/qdequele/ZeroDB", tag = "v0.1.0" }`. The engine crates
`zerodb`, `zerodb-core`, `zerodb-io` and `zerodb-tools` are published on
crates.io at this version (the heed adapter is git-only: cargo needs a crate
*named* `heed` for the patch). The full coverage matrix of heed items and LMDB
features is `docs/COMPATIBILITY.md`.

### Verified on this release

- **Meilisearch v1.53.1** (heed 0.22.1 / hannoy 0.1.3 / arroy 0.6.4): milli
  271 lib + 92 integration tests and index-scheduler 80 tests pass on ZeroDB;
  the C LMDB fork is absent from the dependency tree. Meilisearch's own
  end-to-end benchmark runner on two release builds of the server, movies
  workloads, 10 runs each, Apple M1 Pro: indexing 1.00× LMDB (the
  storage-touching phase 0.99×), search 1.03× (noise band).
  `benches/results/2026-09-09-meilisearch-v1.53.1-movies-macos.md`.
- **hannoy v0.1.7-nested-rtxns**: HNSW build 1.01–1.14× LMDB, search
  0.90–1.07×, one round. `benches/results/2026-09-09-hannoy-v0.1.7-macos.md`.
- **Differential oracle** against the exact LMDB fork Meilisearch ships: 157
  differential tests plus a 10-minute fuzz run (68k op sequences) with zero
  divergences; crash-injection harness 200 cycles clean across all five
  durability modes; `miri` clean on the core; `loom` on the reader table.

### Added

- The heed 0.22.1 surface over ZeroDB (`heed-zerodb`, consumed through the
  `heed`-named shim), including nested read transactions inside a write
  transaction (the fork feature milli depends on), `PREV_SNAPSHOT`, `WRITE_MAP`,
  the `NO_SYNC`/`NO_META_SYNC`/`MAP_ASYNC` durability modes, read-only
  environments, `copy_to_file`/`copy_to_path` with and without compaction.
- ZeroDB extensions beyond heed: `Env::sync(force)`, `Env::reader_list()`,
  `Env::live_readers()`, `EnvOpenOptions::page_size` (4 K–64 K, creation-time;
  default = OS page size through the adapter), `copy_to_path_with_progress`,
  safe custom key comparators on named databases, streamed compaction with
  O(tree depth × page size) peak memory.
- `zerodb-tools`: `stat`, `dump` (mdb_dump-shaped logical format), `load`,
  `check` (invariant checker), `migrate-from-lmdb` (opt-in feature, links C
  LMDB), `--version`. Prebuilt binaries for linux x86-64, linux aarch64 and
  macOS aarch64 are attached to the release.
- Consumer gate tooling: `scripts/consumer.sh` (Meilisearch drop-in check,
  test suites, LMDB-vs-ZeroDB benchmark on Meilisearch's own workloads) and
  `scripts/hannoy.sh`; `docs/CONSUMER-GATE.md`.
- CI on x86-64 and aarch64: fmt, clippy, tests, MSRV, miri, loom, 10-minute
  differential fuzz, crash smoke; nightly stress, 10k crash cycles, 4 h fuzz.

### Fixed (since the July 2026 development snapshot)

- `range_mut`/`rev_range_mut` tested their bound with memcmp on a
  custom-comparator database; `prefix_iter_mut("")` scanned the whole database
  instead of returning `BadValSize`; heed's `copy_to_file` buffered the whole
  copy in memory and staged under a predictable temp name.
- A `Database` handle used with a transaction from another environment now
  panics with heed's message instead of silently reading the wrong file.
- `MdbError` messages are LMDB's `mdb_strerror` text, as heed prints them.
- Range and prefix iterators carry heed's comparator type parameter;
  `DatabaseOpenOptions` is `Copy`/`Debug` with a public `new`; `EnvOpenOptions`
  is `Debug`/`PartialEq`/`Eq`; flag types have `from_bits`.
- **Soundness:** the write transaction held a std `MutexGuard` while being
  `Send`, so dropping or committing it on another thread unlocked a mutex from
  a foreign thread. The writer lock is now a thread-agnostic occupied flag;
  `RwTxn: Send` is genuinely sound (milli moves the transaction across rayon
  threads).
- **Hostile or corrupt files** now fail with typed errors instead of crashing
  (pre-release security review): open refuses page references beyond the file
  and a txnid near the reader-table sentinel band; the read path refuses pages
  above the snapshot's high-water instead of `SIGBUS`; zero-child branch pages
  are rejected at decode; the GC free list is validated before reuse; the
  offline checker uses checked arithmetic, a bounded depth and a seeded hasher;
  `max_readers`/`max_dbs` are bounded; files are created `0600` and never
  through a planted symlink. A new fuzz target feeds arbitrary bytes to `open`.
  Details: `SECURITY.md`, D-017/D-018.

### Known gaps (deliberate, documented)

- Not supported: `DUPSORT`/`DUPFIXED` and the other `DatabaseFlags`, nested
  *write* transactions, multi-process access (no lock file), `Env::resize`,
  `Env::set_flags`, `get_or_put*`, `MDB_NOSUBDIR` (refused at open),
  encryption. Each is a signed-off entry in `docs/DIVERGENCES.md` or a row in
  `docs/COMPATIBILITY.md`. None is used by Meilisearch or hannoy.
- Not yet run: the 24-hour fuzz soak, a Graviton (4 K and 64 K page kernel)
  benchmark, EBS validation of the vectored commit path. All laptop numbers are
  indicative.
- `heed`'s `WithTls` read transactions compile but are not thread-pinned;
  `clear_stale_readers` returns 0 (correct in a single-process engine).

[Unreleased]: https://github.com/qdequele/ZeroDB/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/qdequele/ZeroDB/releases/tag/v0.1.0
