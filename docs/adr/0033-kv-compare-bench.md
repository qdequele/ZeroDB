# ADR-0033: A standalone benchmark crate comparing ZeroDB with other key-value stores

- Status: Proposed
- Related issue: —
- Date: 2026-10-27

## Context

The README compares ZeroDB with other embedded key-value stores using the
five workloads of the *fjall 3* article, run through the external
`rust-storage-bench` harness with a `zerodb` backend added on the bench host.
That setup has three problems:

- The harness copy with the `zerodb` backend lived only on a bench host that
  has since been terminated; the comparison cannot be rerun from this
  repository.
- Its workloads are blends (for example "50% reads, 50% updates", or a social
  feed). A reader cannot map a blended number to their own use, and the
  README results were found hard to understand.
- It runs one thread, so it never measures what most distinguishes an
  LMDB-style engine: readers that scale across cores and are not slowed by a
  concurrent writer. It also does not measure write amplification, a range
  read, or throughput over time.

Measuring the other engines needs their crates — `rocksdb`, `fjall`, `redb`,
`rusqlite` — plus `heed` for LMDB. None is on the dependency allowlist
(AGENTS.md), and `rocksdb` and `rusqlite` build large C/C++ libraries.

## Options

### Option A — keep using rust-storage-bench

Rebuild the `zerodb` backend in a fork of the external harness. No new
dependency here, but the workloads stay blended and single-threaded, and the
comparison lives outside this repository.

### Option B — a benchmark inside the workspace

Add the engines as dev-dependencies of `zerodb-oracle`. Every workspace build,
`cargo test --workspace` and CI would compile RocksDB and SQLite, and the
allowlist would grow by five crates that only one benchmark uses.

### Option C — a standalone crate outside the workspace

`benches/kv-compare/` with its own `[workspace]` table and lock file. It
depends on `heed-zerodb` by path and on the other engines by exact version.
Nothing in the ZeroDB workspace depends on it, so the workspace build, its
lock file, CI and the allowlist are unchanged. It is built only on the bench
host, or by hand.

## Decision

Option C (proposed). The new dependencies are confined to a crate that ships
nothing and is never published (`publish = false`): `heed =0.22.1` (the
Meilisearch LMDB fork, as in the oracle), `fjall =3.1.13`, `redb =4.4.0`,
`rocksdb =0.25.0`, `rusqlite =0.41.0` (bundled SQLite), and `libc` (already
allowlisted). Exact pins keep a published result tied to the engine versions
it measured.

The crate measures single operations rather than blends (read one key, read
a range, durable single write, batched write, bulk load) on a data set that
fits in memory and one that does not; read scaling over threads, with and
without a concurrent writer; disk space, bytes written to the device, and
memory; and insert throughput over 30 minutes. Each engine runs with its
documented defaults, except the cache size and the commit durability, which
are set equal (`benches/kv-compare/README.md`).

## Consequences

- The comparison is reproducible from this repository: `run.sh` on a Linux
  host, `report.py` for tables and charts.
- `unsafe` in the crate: the two `heed` opens (the same contract as
  `zerodb-oracle`), the `FileTrust::trust_contents` call for the
  trusted-file engine, one `getrusage` call, and one `sync` call before reading
  the device write counter. This ADR extends the
  `unsafe` policy to cover them.
- A RocksDB build on macOS needs `libclang` for its bindings
  (`LIBCLANG_PATH`); the crate README says how.
- `cargo fmt` and `cargo clippy` at the workspace root do not reach the crate;
  it is linted on its own.

## Open questions for human review

1. Approve the five engine dependencies, confined to `benches/kv-compare/`.
2. Approve the `unsafe` listed under Consequences.
3. Should CI build the crate (catching API drift in `heed-zerodb`), at the
   cost of compiling RocksDB and SQLite on every run?
