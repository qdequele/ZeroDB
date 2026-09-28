# LMDB fork vs ZeroDB on the bench server after the perf loop — 2026-09-28

The second full run on the dedicated bench server, after the LMDB-parity perf loop of 2026-09-25..28 (every kept lever is in `benches/results/perf-ledger.jsonl` and in `docs/PERF-GAP-VS-LMDB.md` B11–B23). Same machine and settings as [2026-09-25](2026-09-25-bench-server-linux-x86.md). The reading guide is [`docs/BENCH-MAP.md`](../../docs/BENCH-MAP.md).

**Ratio = ZeroDB ÷ LMDB; below 1.00 means ZeroDB is faster.**

| | |
|---|---|
| Machine | Intel Xeon E3-1230 v2 (4C/8T), 31 GB, 2 × SATA SSD in mdraid RAID 1 |
| OS | Debian 12, Linux 6.1, 4 KiB pages |
| Bench mode | performance governor, turbo off (3.3 GHz), apt timers paused, idle |
| Build | criterion bench profile, default codegen units (16) |
| Tree | `02277b7` |
| Consumers | Meilisearch v1.53.1, hannoy v0.1.7-nested-rtxns |

## Consumers (end to end)

| workload | 2026-09-25 | 2026-09-28 |
|---|---:|---:|
| Meilisearch indexing, movies.json (total self time, 20 runs) | 1.08× | **0.96×** |
| Meilisearch search, search-movies.json | 0.96× | **0.97×** |
| hannoy build_hnsw 512 / 768 / 1536 | 1.11–1.14× | **0.84× / 0.85× / 0.89×** |
| hannoy search_hnsw 512 / 768 / 1536 | — | **0.96× / 0.96× / 0.98×** |

ZeroDB is now at or below LMDB end to end on both consumers. Inside Meilisearch indexing, the merge spans still read 1.06–1.07× while `docids_extract` reads 0.52×.

## Engine ladder (long tier)

Caveat: the 2026-09-25 `put/*`, `del/*`, `commit/*` and `maint/*` rungs still timed fixture teardown (fixed since; see BENCH-MAP), so those families are not strictly comparable. Read and `env/*` rungs are.

| rung | LMDB | ZeroDB | ratio | 2026-09-25 |
|---|---:|---:|---:|---:|
| `commit/batch/n10k` | 26.583 ms | 23.383 ms | **0.88×** | 1.17× |
| `commit/batch/n1` | 13.290 ms | 27.056 ms | **2.04×** | 2.22× |
| `commit/batch/n100` | 12.603 ms | 13.657 ms | **1.08×** | 1.34× |
| `commit/sync/n100` | 531.911 ms | 548.789 ms | **1.03×** *(noise)* | 0.89× |
| `commit/sync/n1` | 279.588 ms | 274.669 ms | **0.98×** *(noise)* | 1.13× |
| `concurrent/writer/r0` | 4.171 ms | 4.587 ms | **1.10×** | — |
| `concurrent/writer/r1` | 4.913 ms | 5.465 ms | **1.11×** | 1.13× |
| `concurrent/writer/r4` | 7.686 ms | 8.883 ms | **1.16×** | 1.46× |
| `del/bulk/half` | 11.123 ms | 18.109 ms | **1.63×** | 2.05× |
| `del/bulk/all` | 21.899 ms | 36.876 ms | **1.68×** | 2.23× |
| `del/churn/reinsert` | 14.577 ms | 19.997 ms | **1.37×** | 1.68× |
| `del/clear/all` | 204.224 µs | 363.944 µs | **1.78×** | 1.92× |
| `del/cursor/drain` | 7.998 ms | 21.033 ms | **2.63×** | 3.17× |
| `del/range/half` | 7.960 ms | 7.732 ms | **0.97×** | 2.86× |
| `env/open/reopen` | 2.380 ms | 748.858 µs | **0.31×** | 0.31× |
| `env/open/create` | 4.534 ms | 38.453 ms | **8.48×** | 9.11× |
| `env/stat/non_free` | 366.0 ns | 177.119 µs | **483.99×** | — |
| `env/txn/ro_begin_abort` | 1.474 ms | 806.392 µs | **0.55×** | 0.53× |
| `env/txn/rw_empty_commit` | 75.746 µs | 177.296 µs | **2.34×** | 5.88× |
| `get/access/hot` | 1.725 ms | 1.400 ms | **0.81×** | 1.29× |
| `get/access/miss` | 1.258 ms | 1.227 ms | **0.98×** | 1.74× |
| `get/access/rand` | 3.598 ms | 4.212 ms | **1.17×** | 1.53× |
| `get/access/seq` | 1.844 ms | 1.734 ms | **0.94×** | 1.47× |
| `get/db/root` | 3.516 ms | 4.030 ms | **1.15×** | 1.42× |
| `get/db/named` | 3.775 ms | 4.132 ms | **1.09×** | 1.49× |
| `get/db/named_x8` | 3.224 ms | 3.998 ms | **1.24×** | 1.59× |
| `get/key/k8` | 3.700 ms | 4.136 ms | **1.12×** | 1.53× |
| `get/key/k128` | 4.743 ms | 5.986 ms | **1.26×** | 1.45× |
| `get/key/k32` | 3.858 ms | 5.043 ms | **1.31×** | 1.53× |
| `get/size/n1k` | 1.745 ms | 1.657 ms | **0.95×** | 1.34× |
| `get/size/n1m` | 5.513 ms | 8.152 ms | **1.48×** | 1.84× |
| `get/size/n50k` | 3.525 ms | 4.063 ms | **1.15×** | 1.45× |
| `get/val/v8` | 2.217 ms | 2.055 ms | **0.93×** | 1.32× |
| `get/val/v256` | 2.497 ms | 2.461 ms | **0.99×** | 1.40× |
| `get/val/v2page` | 2.314 ms | 2.918 ms | **1.26×** | 1.68× |
| `get/val/v4k` | 2.246 ms | 2.910 ms | **1.30×** | 1.72× |
| `get/val/v8_touch` | 2.199 ms | 2.056 ms | **0.93×** | — |
| `get/val/v2page_touch` | 3.248 ms | 3.745 ms | **1.15×** | — |
| `get/val/v4k_touch` | 3.168 ms | 3.699 ms | **1.17×** | — |
| `maint/copy/raw` | 9.580 ms | 24.004 ms | **2.51×** | 2.18× |
| `maint/copy/compact` | 10.361 ms | 35.477 ms | **3.42×** | 2.98× |
| `mixed/rw/8dbs` | 60.293 ms | 53.906 ms | **0.89×** | 1.13× |
| `put/api/plain` | 28.284 ms | 23.925 ms | **0.85×** | 1.12× |
| `put/api/reserved` | 28.301 ms | 26.454 ms | **0.93×** | 1.22× |
| `put/gc/drain_big` | 30.070 ms | 34.817 ms | **1.16×** | — |
| `put/order/append` | 19.555 ms | 21.769 ms | **1.11×** | 1.57× |
| `put/order/rand` | 47.689 ms | 46.648 ms | **0.98×** | 1.10× |
| `put/order/seq` | 28.334 ms | 24.093 ms | **0.85×** | 1.12× |
| `put/over/same_size` | 5.495 ms | 5.616 ms | **1.02×** | 1.11× |
| `put/over/grow` | 17.273 ms | 21.100 ms | **1.22×** | 1.27× |
| `put/val/v8` | 2.676 ms | 2.081 ms | **0.78×** | 1.33× |
| `put/val/v256` | 5.124 ms | 4.399 ms | **0.86×** | 1.15× |
| `put/val/v2page` | 17.336 ms | 17.707 ms | **1.02×** | 1.03× |
| `put/val/v4k` | 59.666 ms | 59.958 ms | **1.00×** | 1.02× |
| `scan/edge/first_last` | 1.418 ms | 2.408 ms | **1.70×** | 2.36× |
| `scan/full/fwd` | 1.464 ms | 2.536 ms | **1.73×** | 1.92× |
| `scan/full/rev` | 1.458 ms | 2.513 ms | **1.72×** | 1.83× |
| `scan/meta/len` | 91.392 µs | 97.517 µs | **1.07×** | 1.99× |
| `scan/prefix/bucket` | 4.841 µs | 11.896 µs | **2.46×** | 2.80× |
| `scan/range/1pct` | 12.412 µs | 20.318 µs | **1.64×** | 1.87× |
| `scan/range/10pct` | 121.127 µs | 190.983 µs | **1.58×** | 1.80× |
| `seek/ge/seq` | 2.738 ms | 3.378 ms | **1.23×** | 1.51× |
| `seek/ge/gap` | 4.306 ms | 5.712 ms | **1.33×** | 1.44× |
| `seek/ge/rand` | 4.019 ms | 4.844 ms | **1.21×** | 1.43× |

## Largest gaps left

- `env/stat/non_free` ~480×: ZeroDB walks the free list (now only the count prefixes, B20); heed over LMDB derives the figure from per-DB page counts. Matching that needs a SPEC GC-23/24 amendment.
- `env/open/create` 8.5×: by design (two fsyncs, PERF-GAP B9).
- `maint/copy/*` 2.5–3.4×: untouched by the loop.
- `del/cursor/drain` 2.63× and `commit/batch/n1` 2.04×: partially addressed (B22, B18); the from-left path repair and the used-portion COW copy remain.
- `scan/*` 1.6–2.5×: the cached-parent lever did not clear the bar (ledger).

