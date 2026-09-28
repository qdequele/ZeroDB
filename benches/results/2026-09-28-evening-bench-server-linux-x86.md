# LMDB fork vs ZeroDB on the bench server, opt-in modes included — 2026-09-28 (evening)

The third full run on the dedicated bench server, after the day's work: single-write env copies (PERF-GAP B24), the opt-in trusted-file policy (ADR-0014) and the opt-in sequential-writes option (ADR-0015). Same machine and settings as [the morning run](2026-09-28-bench-server-linux-x86.md). The reading guide is [`docs/BENCH-MAP.md`](../../docs/BENCH-MAP.md).

**Ratio = ZeroDB ÷ LMDB; below 1.00 means ZeroDB is faster.**

| | |
|---|---|
| Machine | Intel Xeon E3-1230 v2 (4C/8T), 31 GB, 2 × SATA SSD in mdraid RAID 1 |
| OS | Debian 12, Linux 6.1, 4 KiB pages |
| Bench mode | performance governor, turbo off (3.3 GHz), apt timers paused, idle |
| Build | criterion bench profile, default codegen units (16) |
| Tree | `e55b2c1` |
| Consumers | Meilisearch v1.53.1, hannoy v0.1.7-nested-rtxns |

## Consumers (end to end, default options)

Meilisearch and hannoy open ZeroDB through the unmodified heed surface, so both opt-in modes are off here.

| workload | 2026-09-25 | 2026-09-28 morning | 2026-09-28 evening |
|---|---:|---:|---:|
| Meilisearch indexing, movies.json (total self time, 20 runs) | 1.08× | 0.96× | **0.99×** |
| Meilisearch search, search-movies.json | 0.96× | 0.97× | **0.95×** |
| hannoy build_hnsw 512 / 768 / 1536 | 1.11–1.14× | 0.84× / 0.85× / 0.89× | **0.84× / 0.84× / 0.89×** |
| hannoy search_hnsw 512 / 768 / 1536 | — | 0.96× / 0.96× / 0.98× | **0.95× / 0.95× / 0.95×** |

Meilisearch indexing is at parity: the two runs today read 0.96× and 0.99× on the mean, and their fastest runs 1.02× and 0.99×. None of today's levers touches that path with the options off, so the difference is run-to-run spread. Inside indexing, the merge spans read 1.03× and `docids_extract` 0.96×.

### Larger data: Meilisearch hackernews (1M documents)

The hackernews workloads from Meilisearch's own bench suite: 1M documents indexed in ten batches of 100k (3 runs × 2 rounds per engine), and the matching search workload. About 30× the movies dataset. Meilisearch's `xtask bench` started each run without waiting for the previous server to exit, so the next run could find port 7700 taken; the bench clone was patched to wait (identical for both engines).

| workload | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| indexing, total self time (6 runs) | 23.477 s | 23.291 s | **0.99×** |
| indexing, `::meta::total` (median inclusive) | 16.449 s | 17.077 s | 1.04× |
| └ `indexing::scheduler::commit` | 4.340 s | 5.356 s | **1.23×** |
| └ `indexing::write_db::all` | 8.340 s | 8.134 s | 0.98× |
| └ `indexing::documents::extract::docids_extract` | 2.292 s | 1.521 s | 0.66× |
| search, total self time (6 runs) | 33.2 ms | 33.4 ms | **1.01×** |
| └ `search::sort::next_bucket` | 5.1 ms | 6.2 ms | 1.21× |

The commit span read 1.23×, but it did not reproduce in two follow-up checks:

- **Direct replay** of the same indexing workload against each engine's Meilisearch binary (two runs each; the last nine uploads autobatch into one 900k-document txn): 122.1 / 123.8 s for LMDB against 124.3 / 123.2 s for ZeroDB (1.00–1.02×). ZeroDB wrote 6.65–6.69 GB against 6.58–6.62 GB (+1 %) with 489k write syscalls against 768k–882k, and its index file ended at 4.00–4.04 GB against 3.93–3.94 GB (+2–3 %).
- **`examples/big_commit_census.rs`** (10 durable batches of 200k random-key puts over 8 DBs, Meilisearch-like value sizes): commit 18.3–18.7 s against 17.9 s (~1.03×), dominated by `pwritev` and `fdatasync`; the puts themselves are 1.14× (the descent into large trees).

So at 1M documents indexing is at parity end to end; the 1.23× span is most likely spread on an fsync-bound span on this SATA RAID 1. Search is at parity; the facet sort buckets (range scans over the facet trees) are 1.21×.

## Engine ladder (long tier)

Three ladder runs of the same binary, back to back: default options, every ZeroDB env opened with the trusted-file policy (`ZERODB_BENCH_TRUST_FILE=1`), and every ZeroDB env with sequential writes on (`ZERODB_BENCH_SEQUENTIAL_WRITES=1`). Each column is its own run's ZeroDB ÷ LMDB; LMDB and ZeroDB times are from the default run. These are single runs, not interleaved A/Bs: a difference under ~3 % between columns is noise. The measured A/Bs are in the perf ledger and in ADR-0014 / ADR-0015.

| rung | LMDB | ZeroDB | default | trusted | sequential writes | morning (default) |
|---|---:|---:|---:|---:|---:|---:|
| `commit/batch/n1` | 13.288 ms | 26.301 ms | **1.98×** | 2.01× | 2.09× | 2.04× |
| `commit/batch/n100` | 12.389 ms | 13.378 ms | **1.08×** | 1.08× | 1.05× | 1.08× |
| `commit/batch/n10k` | 25.930 ms | 23.376 ms | **0.90×** | 0.90× | 0.80× | 0.88× |
| `commit/sync/n1` | 300.635 ms | 291.095 ms | **0.97× *(noise)*** | 0.94× *(noise)* | 0.93× *(noise)* | 0.98× *(noise)* |
| `commit/sync/n100` | 558.306 ms | 577.196 ms | **1.03× *(noise)*** | 0.84× *(noise)* | 0.89× *(noise)* | 1.03× *(noise)* |
| `concurrent/writer/r0` | 4.102 ms | 4.729 ms | **1.15×** | 1.07× | 1.15× | 1.10× |
| `concurrent/writer/r1` | 4.795 ms | 5.670 ms | **1.18×** | 1.10× | 1.18× | 1.11× |
| `concurrent/writer/r4` | 7.601 ms | 8.966 ms | **1.18×** | 1.13× | 1.22× | 1.16× |
| `del/bulk/all` | 21.288 ms | 37.310 ms | **1.75×** | 1.71× | 1.74× | 1.68× |
| `del/bulk/half` | 10.825 ms | 18.925 ms | **1.75×** | 1.71× | 1.73× | 1.63× |
| `del/churn/reinsert` | 14.629 ms | 20.486 ms | **1.40×** | 1.39× | 1.43× | 1.37× |
| `del/clear/all` | 197.418 µs | 362.892 µs | **1.84×** | 1.79× | 1.90× | 1.78× |
| `del/cursor/drain` | 8.093 ms | 21.979 ms | **2.72×** | 2.72× | 2.66× | 2.63× |
| `del/range/half` | 8.109 ms | 7.712 ms | **0.95×** | 0.90× | 0.97× | 0.97× |
| `env/open/create` | 4.535 ms | 38.773 ms | **8.55×** | 8.87× | 8.83× | 8.48× |
| `env/open/reopen` | 2.430 ms | 817.940 µs | **0.34×** | 0.31× | 0.34× | 0.31× |
| `env/stat/non_free` | 366.1 ns | 177.057 µs | **483.63×** | 477.07× | 477.95× | 483.99× |
| `env/txn/ro_begin_abort` | 1.427 ms | 805.389 µs | **0.56×** | 0.60× | 0.53× | 0.55× |
| `env/txn/rw_empty_commit` | 73.302 µs | 180.219 µs | **2.46×** | 2.13× | 2.29× | 2.34× |
| `get/access/hot` | 1.726 ms | 1.418 ms | **0.82×** | 0.82× | 0.82× | 0.81× |
| `get/access/miss` | 1.259 ms | 1.239 ms | **0.98×** | 0.99× | 0.99× *(noise)* | 0.98× |
| `get/access/rand` | 3.528 ms | 4.037 ms | **1.14×** | 1.01× | 1.08× | 1.17× |
| `get/access/seq` | 1.828 ms | 1.744 ms | **0.95×** | 0.92× | 0.96× | 0.94× |
| `get/db/named` | 3.501 ms | 4.051 ms | **1.16×** | 0.97× *(noise)* | 1.07× | 1.09× |
| `get/db/named_x8` | 3.211 ms | 3.916 ms | **1.22×** | 0.99× *(noise)* | 1.14× | 1.24× |
| `get/db/root` | 3.516 ms | 4.024 ms | **1.14×** | 0.91× | 1.15× | 1.15× |
| `get/key/k128` | 4.783 ms | 6.003 ms | **1.26×** | 1.13× | 1.26× | 1.26× |
| `get/key/k32` | 3.789 ms | 5.052 ms | **1.33×** | 1.17× | 1.33× | 1.31× |
| `get/key/k8` | 3.550 ms | 4.122 ms | **1.16×** | 0.97× | 1.16× | 1.12× |
| `get/size/n1k` | 1.712 ms | 1.660 ms | **0.97×** | 0.96× | 0.98× | 0.95× |
| `get/size/n1m` | 5.514 ms | 8.095 ms | **1.47×** | 1.02× *(noise)* | 1.46× | 1.48× |
| `get/size/n50k` | 3.510 ms | 4.038 ms | **1.15×** | 0.99× | 1.14× | 1.15× |
| `get/val/v256` | 2.507 ms | 2.465 ms | **0.98×** | 0.95× | 0.99× | 0.99× |
| `get/val/v2page` | 2.262 ms | 2.980 ms | **1.32×** | 1.28× | 1.30× | 1.26× |
| `get/val/v2page_touch` | 3.191 ms | 3.752 ms | **1.18×** | 1.15× | 1.17× | 1.15× |
| `get/val/v4k` | 2.226 ms | 2.967 ms | **1.33×** | 1.31× | 1.32× | 1.30× |
| `get/val/v4k_touch` | 3.160 ms | 3.713 ms | **1.18×** | 1.16× | 1.17× | 1.17× |
| `get/val/v8` | 2.187 ms | 2.058 ms | **0.94×** | 0.91× | 0.94× | 0.93× |
| `get/val/v8_touch` | 2.164 ms | 2.059 ms | **0.95×** | 0.92× | 0.95× | 0.93× |
| `maint/copy/compact` | 10.369 ms | 10.273 ms | **0.99×** | 0.98× | 0.99× | 3.42× |
| `maint/copy/raw` | 9.566 ms | 9.035 ms | **0.94×** | 0.94× | 0.94× | 2.51× |
| `mixed/rw/8dbs` | 60.139 ms | 55.200 ms | **0.92×** | 0.92× | 0.95× | 0.89× |
| `put/api/plain` | 27.647 ms | 23.874 ms | **0.86×** | 0.86× | 0.77× | 0.85× |
| `put/api/reserved` | 27.706 ms | 26.470 ms | **0.96×** | 0.95× | 0.85× | 0.93× |
| `put/gc/drain_big` | 30.154 ms | 34.360 ms | **1.14×** | 1.08× | 1.14× | 1.16× |
| `put/order/append` | 19.101 ms | 22.019 ms | **1.15×** | 1.15× | 1.10× | 1.11× |
| `put/order/rand` | 48.513 ms | 45.319 ms | **0.93×** | 0.94× | 0.97× | 0.98× |
| `put/order/seq` | 27.555 ms | 23.843 ms | **0.87×** | 0.86× | 0.77× | 0.85× |
| `put/over/grow` | 17.038 ms | 20.723 ms | **1.22×** | 1.22× | 1.22× | 1.22× |
| `put/over/same_size` | 5.472 ms | 5.597 ms | **1.02×** | 0.98× | 1.05× | 1.02× |
| `put/val/v256` | 4.982 ms | 4.379 ms | **0.88×** | 0.87× | 0.79× | 0.86× |
| `put/val/v2page` | 17.235 ms | 17.503 ms | **1.02×** | 1.02× | 0.99× | 1.02× |
| `put/val/v4k` | 60.125 ms | 59.800 ms | **0.99×** | 1.00× | 1.00× | 1.00× |
| `put/val/v8` | 2.539 ms | 2.099 ms | **0.83×** | 0.83× | 0.66× | 0.78× |
| `scan/edge/first_last` | 1.408 ms | 2.338 ms | **1.66×** | 1.59× | 1.65× | 1.70× |
| `scan/full/fwd` | 1.504 ms | 2.524 ms | **1.68×** | 1.55× | 1.82× | 1.73× |
| `scan/full/rev` | 1.483 ms | 2.501 ms | **1.69×** | 1.56× | 1.73× | 1.72× |
| `scan/meta/len` | 91.375 µs | 91.463 µs | **1.00×** | 1.00× *(noise)* | 1.00× *(noise)* | 1.07× |
| `scan/prefix/bucket` | 5.366 µs | 11.904 µs | **2.22×** | 1.87× | 2.47× | 2.46× |
| `scan/range/10pct` | 127.282 µs | 190.926 µs | **1.50×** | 1.19× | 1.50× | 1.58× |
| `scan/range/1pct` | 12.860 µs | 20.334 µs | **1.58×** | 1.21× | 1.57× | 1.64× |
| `seek/ge/gap` | 4.020 ms | 5.299 ms | **1.32×** | 1.14× | 1.38× | 1.33× |
| `seek/ge/rand` | 3.782 ms | 4.535 ms | **1.20×** | 0.96× | 1.21× | 1.21× |
| `seek/ge/seq` | 2.725 ms | 3.368 ms | **1.24×** | 0.98× | 1.25× | 1.23× |

## What changed since the morning

- **Env copies:** `maint/copy/raw` 2.51× → **0.94×**, `maint/copy/compact` 3.42× → **0.99×** (B24: straight from the map, a 1 MiB run buffer, a writer thread as in LMDB's compacting copy).
- **Trusted mode** (opt-in) reaches parity or better on random point reads (`get/access/rand` 1.01×, `get/db/*` 0.91–0.99×, `get/key/k8` 0.97×, `get/size/n50k` 0.99×) and on seeks (0.96–1.14×). The largest gain is on the biggest tree: `get/size/n1m` 1.47× → **1.02×**. Range scans go 1.50–1.58× → 1.19–1.21×.
- **Sequential writes** (opt-in): `put/val/v8` 0.83× → **0.66×**, `put/order/seq` and `put/api/plain` 0.86–0.87× → 0.77×, `commit/batch/n10k` 0.90× → 0.80×; random and mixed writes 3–4 % slower, as ADR-0015 measured.

## Largest gaps left (default options)

- `env/stat/non_free` ~480×: needs the SPEC GC-23/24 amendment to count pages per database, as heed does.
- `env/open/create` 8.5×: by design (two fsyncs, PERF-GAP B9).
- `del/cursor/drain` 2.7×, `commit/batch/n1` 2.0×, `env/txn/rw_empty_commit` 2.5×: commit and delete-path constants.
- `scan/*` 1.5–2.2× (1.2–1.9× trusted), `del/bulk/*` 1.75×, overflow-value reads `get/val/v2page`/`v4k` 1.3× in both modes.
- End to end at 1M documents: random-key puts into large trees (1.14× in the commit census) and facet range scans (1.21× in hackernews search).
