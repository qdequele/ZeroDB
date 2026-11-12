# ZeroDB

**A pure-Rust embedded key-value store, built on LMDB's design.**

ZeroDB is transactional, memory-mapped and fully ACID. It reimplements LMDB's
architecture in Rust, with no C code and no LMDB linked: one writer, readers
that never wait on the writer, copy-on-write B+trees and durable meta pages.
It speaks the [heed](https://github.com/meilisearch/heed) 0.22 API, so you can
use it directly or put it under an existing heed user without source changes
(the manifest changes are in [docs/USING.md](docs/USING.md#as-a-heed-replacement)).
Its behavior is defined by tests that run the same operations on the LMDB
build Meilisearch ships and compare the results.

<!-- BEGIN main-vs-release note: delete this block at the next release. -->
> **These docs describe `main`, which is ahead of the 0.2.0 release.** The
> `0.2` crates and the `v0.2.0` tag differ from what is written here:
>
> | | 0.2.0 | `main` |
> |---|---|---|
> | `EnvOpenOptions::open` (native API) | a safe `fn` | an `unsafe fn`, with heed's contract |
> | Pages read from the file | validated by default; trusting them is opt-in | trusted by default, as in LMDB; `FileTrust::VALIDATE` is opt-in |
> | Processes per environment | one (no lock file) | several (lock file on by default) |
> | On-disk format version | 2 | 3; a version 2 file is refused at open, so dump it with the 0.2.0 `zerodb-tools` and `load` it |
> | `unique-txns` feature (Meilisearch 1.55 and later) | absent | present |
> | `Env::resize` | absent | present |
>
> To build against `main`, pin a commit of it
> (`git ls-remote https://github.com/qdequele/ZeroDB main` prints the current
> one):
>
> ```toml
> zerodb = { git = "https://github.com/qdequele/ZeroDB", rev = "<commit>" }
>
> [patch.crates-io]
> heed = { git = "https://github.com/qdequele/ZeroDB", rev = "<commit>" }
> ```
<!-- END main-vs-release note -->

The 0.2.0 release:

```toml
# Use the engine directly:
zerodb = "0.2"

# …or replace LMDB under a heed user, with no source changes:
[patch.crates-io]
heed = { git = "https://github.com/qdequele/ZeroDB", tag = "v0.2.0" }
```

The example below is written for `main`:

```rust
use zerodb::EnvOpenOptions;

// SAFETY: only ZeroDB writes this environment's files (the contract of
// `open`, as in heed and LMDB).
let env = unsafe { EnvOpenOptions::new().map_size(10 * 1024 * 1024).max_dbs(4).open("books.db")? };

let mut wtxn = env.write_txn()?;
let db = env.create_database(&mut wtxn, Some(&b"books"[..]))?;
db.put(&mut wtxn, b"1984", b"Orwell")?;
wtxn.commit()?;

let rtxn = env.read_txn()?;             // a snapshot; never waits on the writer
assert_eq!(db.get(&rtxn, b"1984")?, Some(&b"Orwell"[..]));
```

A runnable version is in [`crates/zerodb/examples/basic.rs`](crates/zerodb/examples/basic.rs).

## Why ZeroDB

- **LMDB's design in Rust.** Same architecture and semantics, and no C in your
  build. `unsafe` code is confined to a listed set of places (mmap access,
  page reads, shared-memory locks, the `WRITE_MAP` path, and what heed's API
  shape forces), and each `unsafe` block states the invariants it relies on.
  It has not had an independent audit; the counts are in
  [docs/PROVENANCE.md](docs/PROVENANCE.md#what-has-not-been-verified).
- **Tested against LMDB, not against expectations.** A differential harness
  runs the same operation sequences on LMDB and on ZeroDB and compares every
  result. A differential fuzzer generates such sequences in CI, on every pull
  request and every night (about a million sequences per architecture in
  each nightly run). It found a crash in the LMDB build it tests
  against, which is Meilisearch's LMDB fork; the crash has not been reported
  upstream yet ([details](docs/UPSTREAM-BUGS.md)). Crash recovery is tested
  with fault injection and real process kills, and the reader protocol is
  model-checked with loom. What these tests do not cover is listed under
  [Provenance](#provenance).
- **A heed drop-in.** Meilisearch and hannoy build and pass their test suites
  on it with no source changes. Meilisearch 1.55 and later needs manifest
  changes, described in [docs/USING.md](docs/USING.md#as-a-heed-replacement).
  Speed is in [Performance](#performance).
- **Validation when you need it.** Pages are trusted by default, as LMDB
  trusts them. With `FileTrust::VALIDATE`, every page read from the file is
  validated, so a corrupt file returns an error instead of undefined behavior.
  Each meta page carries a checksum either way.
- **A few things LMDB lacks:** nested read transactions inside a write
  transaction, a reader list, compaction with bounded memory and an atomic
  destination, and opt-in fast paths for sequential writes.

## Status

**Version 0.2** is the first published release ([release notes](CHANGELOG.md),
[crates.io](https://crates.io/crates/zerodb), prebuilt `zerodb-tools` on the
[releases page](https://github.com/qdequele/ZeroDB/releases)).

ZeroDB matches LMDB for the heed surface Meilisearch and hannoy use. Both
build on it with no source changes. Meilisearch v1.53.1 needs one `[patch]`
line; Meilisearch v1.55 and later also needs the `unique-txns` feature and a
second `[patch]` table ([docs/USING.md](docs/USING.md#as-a-heed-replacement)).
hannoy is tested at its `v0.1.7-nested-rtxns` tag.

The consumer test suites are run by hand, not in CI. The last recorded passes
are Meilisearch v1.55.0's milli and index-scheduler suites on 2026-10-18, and
Meilisearch v1.53.1's and hannoy's suites on 2026-09-09. They were not re-run
for the 0.2.0 release, and no later run is recorded.

ZeroDB is not yet production-hardened: the 24-hour fuzz soak and a run on a
64 KiB-page kernel are still open. [Provenance](#provenance) lists what has
and has not been verified.

**Not supported:**

- `DUP_SORT`, `DUP_FIXED`, `INTEGER_KEY` and the other `DatabaseFlags`
  (`key_comparator` covers reverse and integer key orders).
- Nested **write** transactions (nested read transactions are supported).
- `NO_SUB_DIR`: every environment is a directory.
- Changing flags at runtime (`set_flags`), the `get_or_put` family, and
  `copy_to_fd`.
- LMDB data files: ZeroDB has its own format, so migrate with `zerodb-tools`.
- Keys longer than 511 bytes, as in LMDB.

The full list, item by item, is in
[docs/LMDB-COMPATIBILITY.md](docs/LMDB-COMPATIBILITY.md).

## How it works

- One data file, mapped into memory and read with no copies; pages are 4 to
  64 KiB.
- Every database is a copy-on-write B+tree: a write never touches a page that
  a committed snapshot can see.
- One writer at a time; any number of readers, each pinned to a snapshot
  through a reader table, shared across processes by a lock file. A reader
  never waits on a write transaction. Beginning a read transaction claims a
  reader slot with atomic operations and takes one short mutex to pin the
  current snapshot.
- A commit writes the new pages, flushes them, then writes one of two meta
  pages, so a crash always leaves the previous commit intact.
- Freed pages are reused only when no reader can still see them.

More in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), which also has a
row-by-row table of
[how ZeroDB differs from LMDB](docs/ARCHITECTURE.md#zerodb-and-lmdb).

## Performance

The goal is LMDB's speed on the workloads Meilisearch and hannoy run. The
figures below are ZeroDB time ÷ LMDB time (below 1.00, ZeroDB is faster), on
current `main` with default options, measured on an AWS Graviton4 instance
with an EBS gp3 volume
([report](benches/results/2026-11-12-ladder-meilisearch-graviton-gp3.md)):

- **Meilisearch v1.55.0:** indexing 0.98×, search 1.00×.
- **Durable commits:** 0.98–1.00×.
- **Faster:** point reads (0.62–0.95×), seeks (0.80–0.97×), range scans
  (0.77–0.78×), sequential and random puts (0.93–0.94×).
- **At parity:** full scans (1.01–1.03×).
- **Slower:** a one-put commit without sync (1.15×), write-cursor rewrites
  (1.26×), per-key deletes (1.27–1.28×), deletes through the write cursor
  (1.40–1.55×), an empty write transaction (3.49×), and creating an
  environment (27.8×: 61 ms against 2.2 ms, paid once per environment).
- **With page validation on** (`FileTrust::VALIDATE`): full scans 1.43–1.57×
  and reads of overflow values 1.31–1.33×; little change elsewhere.

hannoy and the larger Meilisearch workloads were last measured before the
trusted-pages default (builds and searches at 0.83–0.97×) and have not been
re-run on this setup.
Three suites compare ZeroDB with LMDB:

- **Operation by operation:** `just bench` runs a criterion ladder where
  adjacent steps differ by one mechanism, so a gap points at its cause.
- **Against other embedded stores:** [`benches/kv-compare`](benches/kv-compare/README.md)
  runs ZeroDB, LMDB, fjall, RocksDB, redb and SQLite on the same YCSB-style
  workloads, in and out of memory.
- **In real consumers:** Meilisearch and hannoy built once on LMDB and once on
  ZeroDB ([docs/maintainers/CONSUMER-GATE.md](docs/maintainers/CONSUMER-GATE.md)).

[docs/PERFORMANCE.md](docs/PERFORMANCE.md) explains the method and lists the
known gaps.

## Provenance

How this project was built matters for how far to trust it.

- **The code was written by AI coding agents** (Anthropic's Claude), directed
  and reviewed by the maintainer. The agents follow the same rules as human
  contributors ([CONTRIBUTING.md](CONTRIBUTING.md)).
- **Behavior is defined by observation, not by the authors' judgment.** What
  LMDB does is learned by running a differential test against the LMDB build
  Meilisearch ships. A difference is a bug unless it is listed in
  [docs/LMDB-COMPATIBILITY.md](docs/LMDB-COMPATIBILITY.md#behavior-differences).
- **Design decisions are written down** as [ADRs](docs/DECISIONS.md). Format,
  durability, free-page and concurrency changes need the maintainer's approval
  before the implementation. Four implemented ADRs have no recorded approval;
  the index marks them.
- **Clean room.** LMDB's C source was read to understand algorithms. It was
  not transliterated.

Verified so far: differential tests and fuzzing against LMDB, crash tests
with injected write faults and real process kills, loom model checks of the
reader table, and miri on the core crate.

Not verified so far: there has been no independent audit of the `unsafe`
code, no I/O-error injection, no power-loss test on a real device, and no run
on a 64 KiB-page kernel. The differential tests do not cover every operation.

The full account, with numbers, is in
[docs/PROVENANCE.md](docs/PROVENANCE.md).

## Documentation

| | |
|---|---|
| [docs/USING.md](docs/USING.md) | Getting started, options, durability, multiple processes |
| [docs/LMDB-COMPATIBILITY.md](docs/LMDB-COMPATIBILITY.md) | Every heed item and LMDB feature, and every behavior difference |
| [docs/TOOLS.md](docs/TOOLS.md) | `zerodb-tools`: `stat`, `dump`, `load`, `check`, `migrate-from-lmdb` |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the engine works, how it differs from LMDB, and how it is verified |
| [docs/PROVENANCE.md](docs/PROVENANCE.md) | How the project was built, what has been verified and what has not |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Comparison with LMDB, and the benchmark ladder |
| [docs/SPEC/](docs/SPEC/) | The on-disk format and algorithm specification |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Architecture decision records |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to build, test and send a change |

## License

Apache-2.0 or MIT, at your option
([LICENSE-APACHE](LICENSE-APACHE) / [LICENSE-MIT](LICENSE-MIT)).
