# ZeroDB

**A pure-Rust embedded key-value store, built on LMDB's design.**

Transactional, memory-mapped, fully ACID — LMDB's architecture (single writer,
lock-free MVCC readers, copy-on-write B+trees, durable meta pages) reimplemented
in safe Rust. No C, no `libc` build dance, no LMDB linked. It speaks the
[heed](https://github.com/meilisearch/heed) 0.22 API, so you can use it on its
own or drop it under any heed/LMDB consumer — and every operation is verified
against real LMDB.

```toml
# Use the engine directly:
zerodb = { git = "https://github.com/qdequele/ZeroDB" }

# …or drop it under a heed/LMDB consumer — one line, no code changes:
[patch.crates-io]
heed = { git = "https://github.com/qdequele/ZeroDB" }
```

```rust
use zerodb::EnvOpenOptions;

let env = EnvOpenOptions::new().map_size(10 * 1024 * 1024).max_dbs(4).open("books.db")?;

let mut wtxn = env.write_txn()?;
let db = env.create_database(&mut wtxn, Some(&b"books"[..]))?;
db.put(&mut wtxn, b"1984", b"Orwell")?;
wtxn.commit()?;

let rtxn = env.read_txn()?;             // lock-free MVCC snapshot
assert_eq!(db.get(&rtxn, b"1984")?, Some(&b"Orwell"[..]));
```

## What you get

- **LMDB in safe Rust** — same architecture and semantics, no C in your build;
  `unsafe` is confined to a few audited spots (mmap, page casts), each with a
  `SAFETY:` contract.
- **Verified, not hoped** — every operation runs against the real LMDB fork
  side-by-side and is diffed, and a differential fuzzer has driven hundreds of
  thousands of op sequences with zero unresolved divergences (it even found a bug
  [in LMDB itself](docs/UPSTREAM-BUGS.md)). A fault-injection harness checks crash
  recovery; the MVCC protocol is model-checked under loom.
- **Drop-in for heed** — Meilisearch and hannoy run unmodified, at LMDB-level
  performance.
- **Beyond LMDB** — nested read transactions inside a write transaction, reader
  introspection, streamed compaction with an atomic destination.

## Status

**v0.1.** LMDB parity is complete and verified; Meilisearch v1.53.1 and hannoy
pass their full test suites on it unmodified. Not yet production-hardened on the
target hardware (the 24 h fuzz soak and Graviton 4K/64K bench are open) — see
[`CHANGELOG.md`](CHANGELOG.md).

Three things to know: the data file is **not** an LMDB file (own format; migrate
with `zerodb-tools`), it is **one process per environment**, and keys are ≤ 511
bytes. Full list: [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

## Performance

On its real consumers, ZeroDB runs at **LMDB-level performance**: Meilisearch
indexing 1.01× (movies) and 0.99× (incremental hackernews additions), search
0.94× — time relative to LMDB, lower is better; hannoy search 0.95×.

**ZeroDB vs LMDB, current tree** — YCSB on a Graviton4 with local NVMe,
10 M × 128 B, kops/s (higher is better). `WRITE_MAP` is opt-in on both engines
(LMDB's `MDB_WRITEMAP`, ZeroDB's in-place `WRITE_MAP`):

| Workload | LMDB | zerodb | LMDB `WRITE_MAP` | zerodb `WRITE_MAP` |
|---|---:|---:|---:|---:|
| YCSB A (no-sync, 2 GiB cap) | 256 | 211 | 545 | 408 |
| YCSB B (no-sync, 2 GiB cap) | 490 | 355 | 1,199 | 847 |
| YCSB B (fsync) | 238 | **258** | — | — |

Durable writes are ahead of LMDB (1.08×); no-sync writes trail it by 18–27%, and
`WRITE_MAP` makes ZeroDB 1.6–1.7× faster than default LMDB, still behind LMDB's
own `WRITE_MAP`. Details:
[`benches/results/2026-10-05-meta-annex-real-case.md`](benches/results/2026-10-05-meta-annex-real-case.md).
_(2026-10-05, 2 reps.)_

**Against the field** — the
[rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench) suite
behind the *fjall 3* article, on a Graviton4 with local NVMe — throughput
in kops/s (higher is better; per-row winner in **bold**). This run predates
in-place `WRITE_MAP` and the meta free-list annex, and uses a different setup
(8 GiB cap, 2 GiB cache, 100 B values), so its numbers don't compare with the
table above:

| Workload | LMDB | zerodb | fjall 3 | rocksdb | redb | sqlite |
|---|---:|---:|---:|---:|---:|---:|
| YCSB A (no-sync) | **223** | 111 | 187 | 139 | 67 | 32 |
| YCSB B (fsync) | 112 | 112 | **148** | 89 | 68 | 130 |
| 4 KB values | 46 | 51 | **61** | 43 | 12 | 15 |
| feed | **55** | 42 | 37 | 31 | 16 | 38 |
| 100 M keys | **93** | 79 | 76 | 65 | 15 | 36 |

In that run ZeroDB tracked LMDB closely — at parity on durable writes (YCSB B), a little ahead
on 4 KB values, behind on the write-heavy no-sync mix (its weakest path). The LSM
engines (fjall, rocksdb) take the raw write-throughput rows; the B-trees (LMDB and
ZeroDB) keep read p99 in microseconds where the LSMs run to hundreds. Per-engine
latency, RSS and disk tables are in
[`benches/results/2026-09-30-public-suite-nvme.md`](benches/results/2026-09-30-public-suite-nvme.md);
the lever-by-lever LMDB campaign is in
[`docs/PERF-GAP-VS-LMDB.md`](docs/PERF-GAP-VS-LMDB.md).
_(2026-09-30, indicative — 2 reps, feed and 100 M-key runs partial.)_

## More

| | |
|---|---|
| [`docs/SPEC/`](docs/SPEC/) | On-disk format & algorithm spec — the source of truth |
| [`docs/DECISIONS.md`](docs/DECISIONS.md) | Architecture decision records |
| [`docs/TOOLS.md`](docs/TOOLS.md) | `zerodb-tools`: stat / dump / load / check / migrate-from-lmdb |
| [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) | Per-item heed coverage and every difference vs LMDB |
| [`CLAUDE.md`](CLAUDE.md) | How it's built and verified: spec-first, differential-tested, full gate per change |

Build and test: `cargo test --workspace`, `just fuzz-quick` (differential fuzz vs
real LMDB), `just bench` (the LMDB-vs-zerodb microbench ladder).

## License

Apache-2.0 or MIT, at your option
([LICENSE-APACHE](LICENSE-APACHE) / [LICENSE-MIT](LICENSE-MIT)).
