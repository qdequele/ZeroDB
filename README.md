# ZeroDB

**A pure-Rust embedded key-value store, built on LMDB's design.**

ZeroDB is transactional, memory-mapped and fully ACID. It reimplements LMDB's
architecture in Rust, with no C code and no LMDB linked: one writer, readers
that never block, copy-on-write B+trees and durable meta pages. It speaks the
[heed](https://github.com/meilisearch/heed) 0.22 API, so you can use it
directly or put it under an existing heed user without code changes. Every
operation is tested against real LMDB.

```toml
# Use the engine directly:
zerodb = "0.2"

# …or replace LMDB under a heed user, with no code changes:
[patch.crates-io]
heed = { git = "https://github.com/qdequele/ZeroDB", tag = "v0.2.0" }
```

```rust
use zerodb::EnvOpenOptions;

let env = EnvOpenOptions::new().map_size(10 * 1024 * 1024).max_dbs(4).open("books.db")?;

let mut wtxn = env.write_txn()?;
let db = env.create_database(&mut wtxn, Some(&b"books"[..]))?;
db.put(&mut wtxn, b"1984", b"Orwell")?;
wtxn.commit()?;

let rtxn = env.read_txn()?;             // a snapshot; never blocks the writer
assert_eq!(db.get(&rtxn, b"1984")?, Some(&b"Orwell"[..]));
```

A runnable version is in [`crates/zerodb/examples/basic.rs`](crates/zerodb/examples/basic.rs).

## Why ZeroDB

- **LMDB's design in Rust.** Same architecture and semantics, and no C in your
  build. `unsafe` code is limited to a few audited places (mmap, page reads,
  shared-memory locks), each with a written safety contract.
- **Tested against LMDB, not against expectations.** A differential harness
  runs every operation on LMDB and on ZeroDB and compares the results. A
  differential fuzzer has driven hundreds of thousands of operation sequences
  through it, and on the way found a crash in the LMDB build it tests against
  ([details](docs/UPSTREAM-BUGS.md)). Crash recovery is tested with fault
  injection and real process kills, and the reader protocol is model-checked
  with loom.
- **A heed drop-in.** Meilisearch and hannoy run on it unmodified, at LMDB's
  speed.
- **Safer by default.** Every page read from the file is validated, so a
  corrupt file returns an error instead of undefined behavior, and each meta
  page carries a checksum.
- **A few things LMDB lacks:** nested read transactions inside a write
  transaction, a reader list, compaction with bounded memory and an atomic
  destination, and opt-in fast paths for sequential writes.

## Status

**Version 0.2** is the first published release ([release notes](CHANGELOG.md),
[crates.io](https://crates.io/crates/zerodb), prebuilt `zerodb-tools` on the
[releases page](https://github.com/qdequele/ZeroDB/releases)).

ZeroDB matches LMDB for the heed surface Meilisearch and hannoy use, and both
run their test suites on it unmodified. It is not yet production-hardened: the
24-hour fuzz soak and a run on 64 KiB-page kernels are still open.

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
  through a lock-free reader table, shared across processes by a lock file.
- A commit writes the new pages, flushes them, then writes one of two meta
  pages, so a crash always leaves the previous commit intact.
- Freed pages are reused only when no reader can still see them.

More in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Performance

**In Meilisearch, ZeroDB runs at LMDB's speed.** On AWS Graviton4, indexing
the movies dataset takes 2 % longer and search is 1 % faster; hannoy builds its
vector index 3–6 % faster and searches 6–17 % faster
([Graviton4 run](benches/results/2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md);
an earlier [x86-64 run](benches/results/archive/2026-10-05-meta-annex-real-case.md)
has the same picture). The remaining gaps are in micro-operations those
consumers use little, such as full scans and empty commits; see
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

**Against other embedded stores**, no engine wins everything. LMDB and ZeroDB
answer reads fastest, and their readers stay fast while another thread writes.
fjall and RocksDB, which are LSM trees, write and load much faster and use less
disk, but pause writes now and then to reorganize their files.

Thousands of operations per second, 10 M records of 16-byte keys and 100-byte
values, data in memory, AWS Graviton4 (higher is better, best in **bold**):

| | ZeroDB | LMDB | fjall | RocksDB | redb | SQLite |
|---|---:|---:|---:|---:|---:|---:|
| Read one key | 756 | **844** | 240 | 344 | 542 | 290 |
| Read, 8 threads, while one thread writes | 1,900 | **2,400** | 346 | 408 | 612 | 411 |
| Write one key, flushed to disk | 5.2 | 3.8 | 15.2 | 11.2 | 3.8 | **16.2** |
| Write 1,000 keys at a time | 64 | 75 | 280 | **504** | 65 | 27 |
| Load the data set | 141 | 176 | 409 | **600** | 146 | 19 |

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/mixed-threads-small.dark.svg"><img alt="Reads per second while one thread writes, data in memory, by number of reader threads" src="benches/results/2026-10-27-kv-compare/charts/mixed-threads-small.light.svg"></picture>

Against LMDB, ZeroDB is 10–20 % slower on reads that fit in memory, equal on
reads larger than memory, faster on writes flushed to disk, and 15–20 % slower
at loading. With the opt-in that skips page validation (LMDB never validates),
it reads one key at 870k per second, ahead of LMDB. The data-larger-than-memory
results, latencies, disk use, a 30-minute stability test and the method are in
the [full report](benches/results/2026-10-27-kv-compare/report.md); the harness
is [`benches/kv-compare`](benches/kv-compare/README.md).

## Documentation

| | |
|---|---|
| [docs/USING.md](docs/USING.md) | Getting started, options, durability, multiple processes |
| [docs/LMDB-COMPATIBILITY.md](docs/LMDB-COMPATIBILITY.md) | Every heed item and LMDB feature, and every behavior difference |
| [docs/TOOLS.md](docs/TOOLS.md) | `zerodb-tools`: `stat`, `dump`, `load`, `check`, `migrate-from-lmdb` |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the engine works and how it is verified |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Comparison with LMDB, and the benchmark ladder |
| [docs/SPEC/](docs/SPEC/) | The on-disk format and algorithm specification |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Architecture decision records |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to build, test and send a change |

## License

Apache-2.0 or MIT, at your option
([LICENSE-APACHE](LICENSE-APACHE) / [LICENSE-MIT](LICENSE-MIT)).
