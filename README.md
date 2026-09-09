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
heed = { git = "https://github.com/qdequele/ZeroDB", tag = "v0.1.0" }   # or path = "…/zerodb/crates/heed-shim"
```

Before you rely on it, know four things about the files: the data file is
**not an LMDB file** (own `ZDB1` format, migrate with `zerodb-tools`); an
adapter env directory contains exactly one file, `data.mdb`, and **no
`lock.mdb`**; **one process per environment** (no cross-process locking); keys
are at most **511 bytes**, as in LMDB. The full list is in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

> **Status: 0.1, experimental.** Phase 1 (strict LMDB parity) is implemented and
> Meilisearch v1.53.1 and hannoy build and pass their test suites on it with zero
> source changes; the remaining Phase 1 exit criteria (24 h fuzz soak, Graviton
> 4K/64K bench) are still open. Phase 2 (the LMDB features heed hides) is
> implemented except `DUPSORT` (parked) and three deferred `SHOULD` items. Phase 3
> has not started. Not production-ready — see [Pending](#whats-pending),
> [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) for every heed item's status,
> and [`CHANGELOG.md`](CHANGELOG.md).

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

`unsafe` is confined to a handful of audited locations (mmap, page casting,
the adapter's heed-shaped boundary, the oracle's LMDB FFI, and one `flock`
in the tools crate — the lock-free reader table needs none), every block
carries a `SAFETY:` contract, and the whole policy is written down in
[`CLAUDE.md`](CLAUDE.md).

## Performance

Measured on the real consumers vs the LMDB fork (Apple M-series laptop;
Graviton + EBS validation pending). Each row is dated: the July rows come from
the in-repo perf campaign (alternated same-run medians, matched 16 K page
geometry); the September row is Meilisearch's own end-to-end benchmark runner
on two release builds of the real server (`scripts/consumer.sh bench`,
[`benches/results/`](benches/results/)).

| Workload | zerodb vs LMDB | Measured |
|---|---|---|
| **Meilisearch v1.53.1 indexing**, movies workload, whole pipeline (10 runs) | **1.00×** — `write_db` phase 0.99× | 2026-09-09 |
| **Meilisearch v1.53.1 search**, movies workload (10 runs) | 1.03× — inside noise | 2026-09-09 |
| **hannoy vector search** (DIM 512/768/1536) | **0.95× — faster** | 2026-07-22 |
| Meilisearch (milli) indexing, 30 k docs, in-repo harness | 1.12× | 2026-07-22 |
| hannoy graph build | 1.27–1.49× | 2026-07-22 |
| Point get / full scan / overflow values (microbench) | ≈ parity | 2026-07-22 |
| Sequential put (microbench) | ~1.25× | 2026-07-22 |
| Commit (durable, laptop) | ~1.8× — the vectored-write path targets EBS, unmeasured there yet | 2026-07-22 |
| **On-disk size** (same milli index) | **0.83× — 17 % denser** | 2026-07-22 |
| Compaction peak memory | **O(tree depth × page size)** (~100 KB) vs ~2× env size | 2026-07-22 |

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
size chosen at creation (4 K–64 K; 4 K native default, the OS page size
through the heed adapter, matching LMDB). Plus what LMDB can't
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

To run a heed consumer on zerodb, add the `[patch.crates-io]` above. For
Meilisearch specifically, `scripts/consumer.sh` does it for you — compile
check, the milli and index-scheduler suites, and a Meilisearch-level LMDB vs
ZeroDB benchmark on the same workloads — see
[`docs/CONSUMER-GATE.md`](docs/CONSUMER-GATE.md).

## What's pending

- Production-target validation: Graviton + EBS runs (the vectored commit path
  is built for exactly that), the 24 h fuzz soak, a 64K-page kernel in CI.
- Human sign-off pending on ADR-0009 and ADR-0012 (draft) and on divergences
  D-011, D-013, D-014, D-015 (`docs/DIVERGENCES.md`).
- Three write-iterator parity fixes landed 2026-09-09 (`range_mut` bounds under
  a custom comparator, `prefix_iter_mut("")`, streamed `copy_to_file`) are
  pinned by adapter tests but not yet by the oracle — see PROGRESS.md.
- `DUPSORT` is parked by design (no consumer uses it — ADR-0011).
- Releases are git tags with GitHub release notes and prebuilt `zerodb-tools`
  binaries; nothing is on crates.io during 0.x (ADR-0013). Consumers pin a tag
  through the `[patch.crates-io]` above. ZeroDB-only extension APIs may still
  move between minor versions; heed-mirrored signatures never do.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
