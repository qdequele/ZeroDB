# ZeroDB documentation

## For users

| Document | What it covers |
|---|---|
| [USING.md](USING.md) | Getting started, durability options, performance options, multiple processes, build settings |
| [LMDB-COMPATIBILITY.md](LMDB-COMPATIBILITY.md) | Every heed 0.22.1 item and LMDB feature with its status, and every behavior difference from LMDB (`D-001` …) |
| [TOOLS.md](TOOLS.md) | `zerodb-tools`: `stat`, `dump`, `load`, `check`, `migrate-from-lmdb` |
| [PERFORMANCE.md](PERFORMANCE.md) | How ZeroDB compares with LMDB, and why |

## For contributors

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | How the engine works and how it is verified; start here |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | The project rules, the `unsafe` policy and the checks to run |
| [SPEC/](SPEC/) | The reference for formats and algorithms, with the rule IDs (`TXN-61`, `GC-18` …) the code cites |
| [DECISIONS.md](DECISIONS.md) | Index of the architecture decision records in [adr/](adr/) |
| [PERFORMANCE.md#benchmark-ladder](PERFORMANCE.md#benchmark-ladder) | The LMDB-versus-ZeroDB benchmark ladder and how to judge a change |
| [UPSTREAM-BUGS.md](UPSTREAM-BUGS.md) | Bugs the differential fuzzer found in the LMDB reference itself |

The SPEC has seven volumes: `00` the heed API contract, `01` LMDB flags and
semantics, `02` the page and file format, `03` B+trees and cursors, `04`
transactions and MVCC, `05` free-page reuse, and `06` durability and recovery.

## For maintainers

| Document | What it covers |
|---|---|
| [maintainers/RELEASING.md](maintainers/RELEASING.md) | How a version is cut |
| [maintainers/CONSUMER-GATE.md](maintainers/CONSUMER-GATE.md) | Running Meilisearch's and hannoy's test suites and benchmarks on ZeroDB |

Benchmark reports are in [`benches/results/`](../benches/results/README.md),
and release history in [CHANGELOG.md](../CHANGELOG.md).
