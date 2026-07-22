# ZeroDB

**A pure-Rust, transactional, memory-mapped key-value engine — a drop-in
replacement for LMDB at the [heed](https://github.com/meilisearch/heed) API
level, built for [Meilisearch](https://github.com/meilisearch/meilisearch) and
[hannoy](https://github.com/nnethercott/hannoy).**

No C, no `libc` build dance, no LMDB. Same heed API, same semantics — verified
against the exact LMDB fork Meilisearch ships, operation by operation.

```toml
# In a heed 0.22 consumer (milli, hannoy, …) — one line to switch engines:
[patch.crates-io]
heed = { path = "…/zerodb/crates/heed-shim" }
```

> **Status: experimental.** Phase 1 (strict LMDB parity) and Phase 2 (the LMDB
> features heed hides) are complete; Phase 3 (performance & workload features)
> is in progress. Not production-ready — see [Pending](#whats-pending).

---

## Why

Meilisearch's storage is LMDB via heed — mature and fast, but C: unsafe FFI at
the boundary, a fork to maintain (`mdb.master.nested-rtxns`), no way to evolve
the engine for search workloads. ZeroDB reimplements the engine in Rust with
LMDB's architecture (single writer, MVCC readers over a memory map,
copy-on-write B+trees, two durable meta slots) and heed's exact API on top —
so milli and hannoy run **unmodified**.

The bet: get to behavioral parity first with brutal verification, then use
memory safety and workload knowledge to go past LMDB where it matters.

## How it's verified

The correctness story is the point of this project. Every change passes:

| Referee | What it does |
|---|---|
| **Differential oracle** | Runs every operation against **the actual LMDB fork Meilisearch ships** (`mdb.master.nested-rtxns` via `lmdb-master-sys 0.2.6`, linked side-by-side in one binary) and diffs results — including error *kinds*, flag semantics, cursor edge cases, nested read txns, stat output. Never stock LMDB, never guessed semantics. |
| **Differential fuzzing** | `cargo-fuzz` drives random op sequences through both engines (native API **and** through the heed adapter). Hundreds of thousands of executions to date, zero unresolved divergences. It has found real bugs — including a SEGV **in the LMDB fork itself** ([`docs/UPSTREAM-BUGS.md`](docs/UPSTREAM-BUGS.md)). |
| **Crash-injection harness** | A fault-injecting write backend kills the engine at every write/fsync boundary and verifies recovery invariants (no committed data lost, no torn state observed) — ADR-0008. |
| **miri** | The entire engine core (mmap-free by design) runs under miri — including the lock-free reader table, the validated-pages memo, and the `unsafe` unaligned readers. |
| **loom** | The MVCC reader-table protocol (pin/publish/GC-gate) is model-checked under loom — ADR-0006. |
| **Consumer suites** | milli's and hannoy's own test suites pass on the zerodb backend via the shim. |
| **Sanctioned divergences** | Anything that deliberately differs from the fork is a signed-off entry in [`docs/DIVERGENCES.md`](docs/DIVERGENCES.md) — nothing diverges silently. |

`unsafe` is confined to four audited locations (mmap, page casting, the
reader table, the adapter's heed-shaped boundary), every block carries a
`SAFETY:` contract, and the whole policy is written down in
[`CLAUDE.md`](CLAUDE.md).

## Performance

Measured end-to-end on the real consumers, alternated same-run medians vs the
LMDB fork, matched 16 K page geometry (Apple M-series; NVMe/EBS validation
pending):

| Workload | zerodb vs LMDB |
|---|---|
| **hannoy vector search** (DIM 512/768/1536) | **0.95× — faster** |
| **Meilisearch (milli) indexing**, 30 k docs end-to-end | **1.12×** |
| hannoy graph build | 1.27–1.49× |
| Point get / full scan / overflow values (microbench) | ≈ parity |
| Sequential put (microbench) | ~1.25× |
| Commit (durable, laptop) | ~1.8× — the vectored-write path targets EBS, unmeasured there yet |
| **On-disk size** (same milli index) | **0.83× — 17 % denser** |
| Compaction peak memory | **O(tree depth × page size)** (~100 KB) vs ~2× env size |

The whole optimization campaign is documented lever-by-lever — each with its
profile evidence, soundness argument, and referee run — in
[`docs/PERF-GAP-VS-LMDB.md`](docs/PERF-GAP-VS-LMDB.md), and chronologically in
[`PROGRESS.md`](PROGRESS.md).

## Architecture

```
crates/
├── zerodb-core     # pages, B+tree, txns/MVCC, GC, reader table — no I/O, miri-clean
├── zerodb-io       # mmap + pwrite/pwritev backends, fsync strategies, fault injection
├── zerodb          # public engine API (heed-shaped) + copy/compaction
├── heed-zerodb     # the heed 0.22 adapter: 1:1 API surface over zerodb
├── heed-shim       # a crate literally named `heed` re-exporting heed-zerodb
│                   #   → the [patch.crates-io] target consumers point at
├── zerodb-tools    # stat / dump / load / check / migrate-from-lmdb  → docs/TOOLS.md
└── zerodb-oracle   # the differential harness: links REAL LMDB + zerodb in one binary
fuzz/               # differential fuzz targets (cargo-fuzz)
```

Engine shape (LMDB's, deliberately): single write transaction, any number of
lock-free MVCC readers pinned to published snapshots, copy-on-write B+trees,
free-page recycling with a reader-gated GC, double-buffered meta pages, page
size chosen at creation (4 K–64 K, OS-page default). Plus what LMDB can't
give you: nested read transactions *inside* a write transaction (the fork
feature milli depends on), reader introspection, copy progress callbacks,
streamed compaction with an atomic destination.

## Documentation

| | |
|---|---|
| [`docs/SPEC/`](docs/SPEC/) | The on-disk format & algorithm spec — **source of truth**, 7 volumes (API surface, flags, pages, B+tree, txn/MVCC, GC, recovery) |
| [`docs/adr/`](docs/adr/) | 12 architecture decision records, indexed in [`docs/DECISIONS.md`](docs/DECISIONS.md) |
| [`docs/DIVERGENCES.md`](docs/DIVERGENCES.md) | Every sanctioned behavior difference vs the fork |
| [`docs/PERF-GAP-VS-LMDB.md`](docs/PERF-GAP-VS-LMDB.md) | The performance ledger: every LMDB trick, taken or deliberately not |
| [`docs/UPSTREAM-BUGS.md`](docs/UPSTREAM-BUGS.md) | Bugs found **in LMDB itself** by the differential fuzzer |
| [`docs/TOOLS.md`](docs/TOOLS.md) | The `zerodb-tools` manual |
| [`PLAN.md`](PLAN.md) | The milestone roadmap (Phases 0–3) |
| [`PROGRESS.md`](PROGRESS.md) | The append-only engineering log — every milestone, gate result, and bench, verbatim |

## Development method

This engine is built spec-first and AI-assisted under a written law
([`CLAUDE.md`](CLAUDE.md)): the spec wins over code, every LMDB semantics
question is answered by a differential test (never a guess), no test is ever
weakened to pass, and every change lands through the full gate:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core
just fuzz-quick          # 10-minute differential fuzz
just crash-test-quick    # crash-consistency harness
```

The commit history is the audit trail: each perf commit carries its gate
results and referee numbers in the message.

## Trying it

```bash
cargo test --workspace          # the whole battery minus fuzz/crash
just fuzz-quick                 # differential fuzz vs real LMDB (10 min)
cargo bench -p zerodb-oracle    # dual-backend microbench, LMDB vs zerodb
```

To run a heed consumer on zerodb, add the `[patch.crates-io]` above — ready-made
patches for milli and hannoy are in [`docs/patches/`](docs/patches/).

## What's pending

- Production-target validation: Graviton + EBS runs (the vectored commit path
  is built for exactly that), 24 h fuzz soak, CI.
- Three ADRs / divergence entries awaiting human sign-off.
- `DUPSORT` is parked by design (no consumer uses it — ADR-0011).
- Not published to crates.io; API may still move.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
