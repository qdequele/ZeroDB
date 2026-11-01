# Benchmark results

Each report records one run: the machine, the build, the commands and the raw
numbers. Ratios are **ZeroDB time ÷ LMDB time**, so below 1.00 means ZeroDB is
faster. [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md) summarizes them.

## Current reports

| Report | What it measures |
|---|---|
| [2026-10-27-kv-compare](2026-10-27-kv-compare/report.md) | ZeroDB, LMDB, fjall, RocksDB, redb and SQLite on the same workloads, in and out of memory, on Graviton4 (the README's comparison) |
| [2026-10-15-meilisearch-v1.53.1-graviton-xlarge](2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md) | Meilisearch and hannoy built on LMDB and on ZeroDB, on Graviton4 |
| [2026-10-20-public-suite-graviton](2026-10-20-public-suite-graviton.md) | A public multi-engine YCSB suite on Graviton4 NVMe |

## Archive

Dated reports from the development history are in [archive/](archive/). Each
records the change it measured (an ADR or an issue), so later work can be
compared with it. Some were run with helper scripts that are not in this
repository; their numbers stand, but they cannot be rerun as written.

| Topic | Reports |
|---|---|
| Benchmark ladder, full runs | [2026-09-10 (macOS)](archive/2026-09-10-engine-ladder-macos.md), [2026-09-25 (x86-64)](archive/2026-09-25-bench-server-linux-x86.md), [2026-09-28](archive/2026-09-28-bench-server-linux-x86.md), [2026-09-28 evening](archive/2026-09-28-evening-bench-server-linux-x86.md), [2026-10-13 (Graviton4)](archive/2026-10-13-multi-process-stage3-graviton.md), [2026-10-31 build profiles (Graviton4)](archive/2026-10-31-build-profiles-and-scans-graviton.md) |
| Meilisearch and hannoy | [hannoy, macOS](archive/2026-09-09-hannoy-v0.1.7-macos.md), [movies, macOS](archive/2026-09-09-meilisearch-v1.53.1-movies-macos.md), [delete-heavy, macOS](archive/2026-09-10-meilisearch-delete-heavy-macos.md), [hackernews merge step, macOS](archive/2026-10-16-hackernews-merge-step-mac.md) |
| YCSB and memory | [bigger than RAM](archive/2026-09-29-ycsb-bigger-than-ram.md), [YCSB A/B/C matrix](archive/2026-09-30-ycsb-matrix.md), [spilling (ADR-0017)](archive/2026-09-30-bounded-dirty-memory.md), [public suite, 2026-09-30](archive/2026-09-30-public-suite-nvme.md) |
| Reads | [validated-pages cache (ADR-0018)](archive/2026-09-30-validation-cache.md), [cache sized to the file](archive/2026-10-31-stamp-cache-graviton.md), [scans and batch deletes](archive/2026-10-19-crud-scan-count-and-batch-delete.md) |
| Writes and commits | [meta free-list annex (ADR-0022)](archive/2026-10-05-meta-annex-real-case.md), [meta CRC: slicing](archive/2026-10-19-meta-crc-slicing.md), [hardware CRC (ADR-0031)](archive/2026-10-19-meta-crc-hardware.md), [CRC on Graviton](archive/2026-10-19-meta-crc-graviton.md), [write-cursor splits and APPEND](archive/2026-10-31-write-cursor-split-append-graviton.md), [writer state reuse](archive/2026-10-31-writer-carry-graviton.md), [`WRITE_MAP` dirty-page bits](archive/2026-10-31-writemap-dirty-bits-graviton.md) |
| Deletes | [rebalance moves cells (ADR-0028)](archive/2026-10-18-delete-rebalance-node-move-mac.md), [delete path and cursor](archive/2026-10-31-delete-path-cursor.md) |
| Multi-process (ADR-0023) | [lock file, stage 1](archive/2026-10-11-multi-process-stage1.md), [lock file rebuilt in place](archive/2026-10-15-lock-file-in-place-graviton.md) |

## The perf ledger

[`perf-ledger.jsonl`](perf-ledger.jsonl) has one line per performance
experiment, kept or reverted, with its hypothesis, the rungs it targeted and
the measured result. `just perf-ledger show` prints it.
