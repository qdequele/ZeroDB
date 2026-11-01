# Benchmark results

The benchmark reports are being re-run on a quiet AWS Graviton4 host and will
be published here, one Markdown file per run: the machine, the build, the
commands and the raw numbers. Ratios are **ZeroDB time ÷ LMDB time**, so below
1.00 means ZeroDB is faster. [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md)
explains the method and summarizes the results.

Earlier reports from the development history were removed from the tree; they
are still in git history (for example at commit `e0fd466`).

## Running the benchmarks

| What | How |
|---|---|
| ZeroDB against LMDB, operation by operation | `just bench` (the criterion ladder in `crates/zerodb-oracle/benches/engine_comparison/`) |
| ZeroDB before and after a change, with LMDB as the reference | `just bench-ab` |
| ZeroDB against other embedded stores (LMDB, fjall, RocksDB, redb, SQLite) | [`benches/kv-compare`](../kv-compare/README.md) |
| Meilisearch and hannoy built on LMDB and on ZeroDB | [docs/maintainers/CONSUMER-GATE.md](../../docs/maintainers/CONSUMER-GATE.md) |

## Performance ledger

[`perf-ledger.jsonl`](perf-ledger.jsonl) records every performance experiment,
kept or reverted, one JSON object per line, so a disproved idea is not tried
again. `just perf-ledger` shows it. Older entries name report files that are no
longer in the tree; their numbers are in the entries themselves.
