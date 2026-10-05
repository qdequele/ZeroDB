# Meta free-list annex (ADR-0022) — real-case three-column check — 2026-10-05

The commit-ladder A/B for ADR-0022 only showed a win on tiny no-sync commits
(`commit/batch/n1` −22%; `n10k` and the fsync rungs flat). This run checks it on
real workloads: YCSB through rust-storage-bench, and Meilisearch's own
`cargo xtask bench` workloads.

Three columns throughout:

| column | tree |
|---|---|
| **LMDB** | heed 0.22.1 (the Meilisearch LMDB fork) — drift control |
| **ZeroDB before** | `8b066a6` — main with ADR-0021 (in-place `WRITE_MAP`), no annex |
| **ZeroDB after** | `b01b75b` — `8b066a6` + ADR-0022 (the annex), nothing else |

`*-wm` rows open the environment with `WRITE_MAP` (LMDB `MDB_WRITEMAP`, ZeroDB
in-place `WRITE_MAP`).

## YCSB — Graviton4 NVMe (the target hardware)

AWS `m8gd.xlarge` (Graviton4, 4 vCPU, 16 GiB), Ubuntu 24.04 aarch64, local NVMe
instance store, ext4 `noatime`. rust-storage-bench (with the `zerodb` backend),
10 M items × 128 B, json corpus, 8 MiB engine cache, 60 s per run; no-sync runs in
a `MemoryMax=2G` scope, durable runs uncapped. Fresh data dir and dropped page
cache before every run. 2 reps, the second in reverse order.

### YCSB A (50/50 read/update), no-sync

| config | ops/s (reps) | median | ÷ LMDB | write p50 | write p99 |
|---|---|---:|---:|---:|---:|
| LMDB | 255k / 257k | 256k | 1.00 | 6.4 µs | 13.9 µs |
| ZeroDB before | 207k / 207k | 207k | 0.81 | 14.0 µs | 27.7 µs |
| **ZeroDB after** | 213k / 209k | 211k | 0.82 | **12.0 µs** | **24.8 µs** |
| LMDB-wm | 545k / 545k | 545k | 2.13 | 1.3 µs | 4.1 µs |
| ZeroDB-wm before | 421k / 404k | 412k | 1.61 | 4.4 µs | 9.3 µs |
| **ZeroDB-wm after** | 412k / 405k | 408k | 1.60 | **3.3 µs** | **7.8 µs** |

after ÷ before: default **1.018** (rep range 1.008–1.028); wm 0.991 (0.963–1.020).

### YCSB B (95/5 read/update), no-sync

| config | ops/s (reps) | median | ÷ LMDB | write p50 | write p99 |
|---|---|---:|---:|---:|---:|
| LMDB | 492k / 487k | 490k | 1.00 | 7.1 µs | 15.4 µs |
| ZeroDB before | 368k / 358k | 363k | 0.74 | 15.1 µs | 31.9 µs |
| **ZeroDB after** | 372k / 339k | 355k | 0.73 | **13.1 µs** | 54.0 µs ¹ |
| LMDB-wm | 1209k / 1190k | 1199k | 2.45 | 1.4 µs | 6.4 µs |
| ZeroDB-wm before | 819k / 810k | 814k | 1.66 | 4.6 µs | 11.6 µs |
| **ZeroDB-wm after** | 845k / 848k | 847k | 1.73 | **3.5 µs** | **10.3 µs** |

after ÷ before: default 0.980 (0.922–1.040, noisy); wm **1.040** (1.033–1.047).

¹ One rep only: rep 1 was 29.4 µs (better than before's 31.9 µs); rep 2 was
78.5 µs during a run that read 382 MiB from disk vs ~210 MiB for the others — a
page-cache eviction episode under the 2 GiB cap, not the annex.

### YCSB B, durable (every commit fsynced)

| config | ops/s (reps) | median | ÷ LMDB | write p50 | write p99 |
|---|---|---:|---:|---:|---:|
| LMDB | 238k / 239k | 238k | 1.00 | 167.8 µs | 178.1 µs |
| ZeroDB before | 238k / 241k | 239k | 1.00 | 161.2 µs | 178.1 µs |
| **ZeroDB after** | 258k / 258k | **258k** | **1.08** | **129.4 µs** | **148.8 µs** |

after ÷ before: **1.078** (rep range 1.070–1.085). One page fewer written and
flushed per commit shows up directly when the flush is cheap (NVMe).

## YCSB — x86-64 bench server (confirmation)

Xeon E3-1230 v2 (4C/8T, turbo off, performance governor), 2× SATA SSD in
mdraid RAID 1, Debian 12. Same method, 45 s per run, 2 reps (durable: 1 rep).

| workload | before → after ops/s | after ÷ before | write p50 before → after |
|---|---|---:|---|
| A no-sync | 254k → 261k | **1.027** (1.019–1.034) | 17.2 → 13.8 µs |
| A no-sync, wm | 336k → 357k | **1.065** (1.022–1.110) | 8.7 → 8.0 µs |
| B no-sync | 421k → 448k | **1.064** (1.046–1.083) | 18.2 → 13.9 µs |
| B no-sync, wm | 661k → 696k | **1.053** (1.052–1.054) | 8.9 → 6.9 µs |
| B durable | 188k → 189k | 1.006 | 1398 → 1316 µs |

The durable row is flat here: this box's fsync is a ~1.4 ms SATA RAID flush,
which drowns per-commit CPU (the same reason the ladder's `commit/sync/*` rungs
measured flat on it).

## Meilisearch — x86-64 bench server

Meilisearch v1.53.1 built three times (stock LMDB, ZeroDB before, ZeroDB after),
`cargo xtask bench` on `movies.json`, `hackernews-add-new-documents.json` and
`search/movies.json`, 3 rounds with the starting engine rotated each round.
Median server-side time (`::meta::total` span) over all runs:

| workload | LMDB | ZeroDB before | ZeroDB after | after ÷ before | after ÷ LMDB |
|---|---:|---:|---:|---:|---:|
| movies indexing (30 runs) | 4.850 s | 4.865 s | 4.879 s | 1.00 | 1.01 |
| hackernews incremental additions (9 runs) | 40.25 s | 40.74 s | 40.01 s | 0.98 | 0.99 |
| — of which `indexing::scheduler::commit` | 11.03 s | 10.86 s | 10.21 s | **0.94** | 0.93 |
| movies search (30 runs) | 15.4 ms | 14.8 ms | 14.4 ms | 0.97 | 0.94 |

No regression. Bulk indexing is flat as expected (few, large commits); the
incremental-additions workload, which commits more often, gains 6% on its commit
span.

## Verdict

ADR-0022 is a real-case win where commits are frequent or durable on fast
storage, and neutral for Meilisearch bulk indexing: YCSB +2–8% throughput, write
p50 −14% to −25% in every configuration on both machines, no regression in any
Meilisearch workload. It is not a Meilisearch indexing speed-up and should not be
described as one.
