# AGENTS.md

Instructions for AI coding agents working in this repository. Humans and
agents follow the same rules: they are in
[CONTRIBUTING.md](CONTRIBUTING.md), which you must read before your first
change. That includes the `unsafe` policy, the checks to run, the dependency
allowlist and the style rules. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
explains how the engine works.

## What this project is

ZeroDB is a transactional, memory-mapped embedded key-value engine written in
pure Rust. It is a drop-in replacement for LMDB **at the API of `heed`** (the
Rust LMDB wrapper Meilisearch uses), not at the file-format level. Its first
consumers are Meilisearch's indexing and search core (`milli`) and its HNSW
vector index (`hannoy`). Default behavior matches LMDB; capabilities LMDB lacks
ship behind explicit options.

## Points agents most often get wrong

- **Never guess LMDB's behavior.** Write a differential test in
  `crates/zerodb-oracle` and observe it. Never change an oracle expectation, or
  delete, weaken or `#[ignore]` a test, to make ZeroDB pass: report it instead.
- **Read the SPEC section before changing an area**, and update it in the same
  change when behavior changes. Never renumber a rule ID.
- **Format, durability, GC, reader-table and concurrency changes need an
  approved ADR before the implementation.** Do not start one on your own
  initiative.
- **Performance claims need a three-column comparison** (LMDB, ZeroDB before,
  ZeroDB after), from `just bench-ab`.
- **Run the full check list** in CONTRIBUTING.md before calling a change done,
  including the `x86_64-unknown-linux-gnu` clippy run on macOS.
- **Comments are for outside readers.** State what code does or why, never the
  history of the change, benchmark numbers, issue numbers or internal
  shorthand.

## Repo map

- `crates/zerodb-core`: pages, B+trees, transactions, free-page GC, the reader
  table. No I/O policy.
- `crates/zerodb-io`: mmap reads, `pwrite`/`pwritev` commit writes, fsync, the
  lock file, and the fault-injection backend used by the crash tests.
- `crates/zerodb`: the public engine API (heed-shaped).
- `crates/heed-zerodb`: the heed 0.22.1 API over the engine. Its package is
  named `heed`, so it is the `[patch.crates-io]` target itself; select it with
  `-p heed`, and depend on it as `heed_zerodb = { package = "heed", path = … }`
  (the oracle also links the real crates.io `heed`).
- `crates/zerodb-tools`: `stat`, `dump`, `load`, `check`, `migrate-from-lmdb`.
  The optional `migrate-lmdb` feature links C LMDB through heed.
- `crates/zerodb-oracle`: the differential harness against C LMDB, the crash
  harness, and the criterion benchmark ladder (`benches/engine_comparison/`).
- `fuzz/`: differential and decoder fuzz targets.
- `benches/results/`: benchmark reports and `perf-ledger.jsonl`, the record of
  every performance experiment, kept or reverted.
- `scripts/`: benchmark A/B, profiling and consumer-suite runners.
- `docs/SPEC/`: the format and algorithm spec. `docs/adr/`: design decisions,
  indexed in `docs/DECISIONS.md`. `docs/LMDB-COMPATIBILITY.md`: heed coverage
  and every approved behavior difference from LMDB. `docs/PERFORMANCE.md`: the
  comparison with LMDB.

Planned and open work lives in GitHub issues.
