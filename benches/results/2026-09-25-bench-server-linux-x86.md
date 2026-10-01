# LMDB fork vs ZeroDB on a quiet Linux x86-64 server — 2026-09-25

The first run on the dedicated bench server. It covers the full comparison
ladder (long tier), a before/after A/B of the staged B8a change, and both
consumer benches, Meilisearch and hannoy. The reading guide is
[`docs/BENCH-MAP.md`](../../docs/BENCH-MAP.md).

**Ratio = ZeroDB ÷ LMDB; above 1.00 means ZeroDB is slower.**

| | |
|---|---|
| Machine | Intel Xeon E3-1230 v2 (Ivy Bridge, 4C/8T), 31 GB, 2 × SATA SSD in mdraid RAID 1 |
| OS | Debian 12, Linux 6.1, **4 KiB pages** (both engines pinned to them) |
| Bench mode | `performance` governor, turbo off (fixed 3.3 GHz), apt timers paused, machine otherwise idle |
| Toolchain | rustc 1.98.1, `--release` (criterion bench profile) |
| Tree | `a8faeb7` + the staged working tree (the ladder harness and B8a) |
| Consumers | Meilisearch v1.53.1, hannoy v0.1.7-nested-rtxns |
| Scripts | `_side_projects/benches-server/run.sh` (`ab ladder consumer hannoy`) |

## What this run says

**The macOS "read path at parity" picture does not survive Linux at 4 KiB.**
On the 2026-09-10 laptop run (16 KiB pages), `get/*` sat at 0.93–1.21× and
`seek/*` at 0.89–0.93×. Here `get/*` is **1.29–1.84×**, `seek/*` **1.43–1.51×**
and `scan/*` **1.80–2.80×**. The run cannot separate the two changes. The page
size quadruples the page loads per descent, and ZeroDB's per-page costs
(header parse, view construction, validation memo) scale with page loads; the
CPU is a 2012 Ivy Bridge. 4 KiB is the common Linux page size, though, so these
are the more representative numbers for most Meilisearch deployments.
`get/size/n1m` at 1.84× against `n50k` at 1.45× says the gap grows with tree
depth, which points at per-level descent cost (PERF-GAP A5).

**Transaction setup flips.** `env/txn/rw_empty_commit` is **5.88×** (LMDB
75 µs, ZeroDB 439 µs), where macOS had it at 0.12×. That macOS "win" was LMDB
being slow on macOS (603 µs); ZeroDB's empty commit, by contrast, got **6×
slower** on Linux (75 µs → 439 µs), so something Linux-specific sits on its
commit path. `commit/batch/n1` (2.22×) is the same cost seen through one put.
`ro_begin_abort` is 0.53×, still ZeroDB-faster, but not the macOS 0.04×.

**Durable commits are at parity here.** `commit/sync/*` is 0.89–1.13×, both
marked noise. On a local SATA SSD behind mdraid, the barrier dominates both
engines alike. That is still not the EBS answer.

**Deletion is the same finding as on macOS**, just as large: `del/*`
1.68–3.17×, with `del/range/half` at 2.86× and `del/bulk/*` at 2.05–2.23×.

## Consumers: close to parity end to end

The micro gaps mostly wash out at the consumer level:

| workload | LMDB | ZeroDB | ratio | macOS 09-09/09-10 |
|---|---:|---:|---:|---:|
| Meilisearch `movies.json` indexing (total self time, median of 20) | 6.949 s | 7.479 s | **1.08×** | 1.00× |
| … `indexing::write_db::all` | 3.755 s | 4.104 s | 1.09× | 0.99× |
| … `indexing::documents::extract` | 3.031 s | 3.112 s | 1.03× | 0.99× |
| Meilisearch `search/movies.json` (median of 20) | 14.6 ms | 14.0 ms | **0.96×** | — |
| … `search::bucket_sort::bucket_sort` | 3.9 ms | 3.5 ms | 0.90× | — |
| hannoy `build_hnsw` 512 / 768 / 1536 | 1.58 / 1.76 / 2.35 s | 1.80 / 1.99 / 2.59 s | **1.14 / 1.13 / 1.11×** | 1.01–1.14× |
| hannoy `search_hnsw` 512 / 768 / 1536 | 1.75 / 1.87 / 2.49 ms | 1.98 / 2.14 / 2.79 ms | **1.13 / 1.14 / 1.12×** | 0.90–1.07× |

Meilisearch search is **faster** on ZeroDB. Indexing is 8 % behind, and that
gap sits in `write_db` and the merges, which is the write and delete path the
ladder names. hannoy is 11–14 % behind on both build and search.

## Before/after: B8a (`RwCursor::del_current` without the double re-descent)

`bench-ab '^del/'`, `BASE=HEAD` (a8faeb7) against the staged tree, 3
interleaved rounds. **Verdict: improved.**

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `del/bulk/all` | 24.609 ms | 53.339 ms | 53.318 ms | 2.17× → **2.17×** | 0.993 (±0.035) | 1.013 | flat |
| `del/bulk/half` | 13.940 ms | 27.872 ms | 27.896 ms | 2.00× → **2.00×** | 1.001 (±0.043) | 1.016 | flat |
| `del/churn/reinsert` | 18.184 ms | 30.210 ms | 29.602 ms | 1.66× → **1.63×** | 0.976 (±0.030) | 1.011 | flat |
| `del/clear/all` | 2.907 ms | 4.806 ms | 4.806 ms | 1.65× → **1.65×** | 0.993 (±0.030) | 1.012 | flat |
| `del/cursor/drain` 🎯 | 11.001 ms | 41.949 ms | 34.272 ms | 3.81× → **3.12×** | 0.805 (±0.050) | 1.014 | ✅ faster |
| `del/range/half` | 10.913 ms | 30.129 ms | 30.060 ms | 2.76× → **2.75×** | 0.992 (±0.030) | 0.996 | flat |

The targeted rung is 19.5 % faster, and nothing else moved beyond ±3 %. LMDB
drifted at most 1.6 % on every rung. `cursor/drain ÷ range/half` goes from
1.39 to **1.14** on this machine. BENCH-MAP's laptop note records 1.28 → 1.01,
so here the cursor path keeps a 14 % overhead that the laptop did not show.
(The "load 5.0" warning in that run was the two builds still in the 1-minute
average; `bench-ab` now waits for the load to settle before judging.)

## Where to point the perf loop next, from this run

1. `env/txn/rw_empty_commit` 5.88× / `commit/batch/n1` 2.22×. **Diagnosed
   the same day:** `WriterGuard::drop` (`crates/zerodb-core/src/env.rs:258`)
   calls `Condvar::notify_one()` unconditionally, even with no waiter. On Linux,
   std's futex-based `Condvar` turns every notify into a `futex_wake` system
   call; on macOS a pthread signal with no waiters stays in user space, which
   is why the laptop never showed it. `perf trace -s` over 3 s of the rung:
   ZeroDB **1,386,098 `futex` calls**, LMDB **100**. This CPU runs with the
   page-table-isolation mitigation (`Mitigation: PTI`), which makes each system
   call dear. `perf record` puts ~30 % of the rung in syscall entry/exit. The
   candidate fix: count waiters in the flag mutex's state and notify only when
   one exists. Mutual exclusion is unchanged. It still needs its own
   `bench-ab` and a `just loom` / stress pass before it is claimed.
2. The 4 KiB read path: `get/size/n1m` 1.84× and `scan/*` 1.8–2.8×.
   Per-level descent cost (A5) and per-page view construction. This is what
   hannoy search and Meilisearch `write_db` reads pay.
3. `del/*`, still 2–3×. The remaining lever is the B8 rebalance redesign,
   which is ADR-gated (see the ledger).

## Full ladder

| rung | LMDB | ZeroDB | ratio | vs family base |
|---|---:|---:|---:|---:|
| `commit/batch/n10k` | 27.811 ms | 32.656 ms | **1.17×** | +0.00 |
| `commit/batch/n1` | 13.429 ms | 29.800 ms | **2.22×** | +1.04 ⚠ |
| `commit/batch/n100` | 13.149 ms | 17.651 ms | **1.34×** | +0.17 ⚠ |
| `commit/sync/n100` | 576.548 ms | 512.048 ms | **0.89×** *(noise)* | +0.00 |
| `commit/sync/n1` | 295.646 ms | 333.851 ms | **1.13×** *(noise)* | +0.24 ⚠ |
| `concurrent/writer/r1` | 8.768 ms | 9.877 ms | **1.13×** | +0.00 |
| `concurrent/writer/r4` | 10.830 ms | 15.824 ms | **1.46×** | +0.33 ⚠ |
| `del/bulk/half` | 13.486 ms | 27.634 ms | **2.05×** | +0.00 |
| `del/bulk/all` | 23.881 ms | 53.175 ms | **2.23×** | +0.18 ⚠ |
| `del/churn/reinsert` | 17.563 ms | 29.518 ms | **1.68×** | +0.00 |
| `del/clear/all` | 2.475 ms | 4.757 ms | **1.92×** | +0.00 |
| `del/cursor/drain` | 10.547 ms | 33.470 ms | **3.17×** | +0.00 |
| `del/range/half` | 10.466 ms | 29.882 ms | **2.86×** | +0.00 |
| `env/open/reopen` | 2.353 ms | 732.078 µs | **0.31×** | +0.00 |
| `env/open/create` | 4.419 ms | 40.244 ms | **9.11×** | +8.80 ⚠ |
| `env/txn/ro_begin_abort` | 1.480 ms | 788.489 µs | **0.53×** | +0.00 |
| `env/txn/rw_empty_commit` | 74.698 µs | 439.206 µs | **5.88×** | +5.35 ⚠ |
| `get/access/hot` | 1.807 ms | 2.330 ms | **1.29×** | +0.00 |
| `get/access/miss` | 1.294 ms | 2.258 ms | **1.74×** | +0.46 ⚠ |
| `get/access/rand` | 3.550 ms | 5.421 ms | **1.53×** | +0.24 ⚠ |
| `get/access/seq` | 1.894 ms | 2.774 ms | **1.47×** | +0.18 ⚠ |
| `get/db/root` | 3.794 ms | 5.392 ms | **1.42×** | +0.00 |
| `get/db/named` | 3.633 ms | 5.415 ms | **1.49×** | +0.07 |
| `get/db/named_x8` | 3.262 ms | 5.196 ms | **1.59×** | +0.17 ⚠ |
| `get/key/k8` | 3.594 ms | 5.496 ms | **1.53×** | +0.00 |
| `get/key/k128` | 4.821 ms | 6.972 ms | **1.45×** | -0.08 |
| `get/key/k32` | 3.877 ms | 5.932 ms | **1.53×** | +0.00 |
| `get/size/n1k` | 1.782 ms | 2.383 ms | **1.34×** | +0.00 |
| `get/size/n1m` | 5.541 ms | 10.205 ms | **1.84×** | +0.50 ⚠ |
| `get/size/n50k` | 3.730 ms | 5.415 ms | **1.45×** | +0.11 |
| `get/val/v8` | 2.220 ms | 2.932 ms | **1.32×** | +0.00 |
| `get/val/v256` | 2.551 ms | 3.571 ms | **1.40×** | +0.08 |
| `get/val/v2page` | 2.322 ms | 3.910 ms | **1.68×** | +0.36 ⚠ |
| `get/val/v4k` | 2.259 ms | 3.897 ms | **1.72×** | +0.40 ⚠ |
| `maint/copy/raw` | 11.265 ms | 24.574 ms | **2.18×** | +0.00 |
| `maint/copy/compact` | 12.322 ms | 36.741 ms | **2.98×** | +0.80 ⚠ |
| `mixed/rw/8dbs` | 63.008 ms | 71.479 ms | **1.13×** | +0.00 |
| `put/api/plain` | 29.647 ms | 33.280 ms | **1.12×** | +0.00 |
| `put/api/reserved` | 29.806 ms | 36.216 ms | **1.22×** | +0.09 |
| `put/order/append` | 21.203 ms | 33.332 ms | **1.57×** | +0.00 |
| `put/order/rand` | 51.931 ms | 57.220 ms | **1.10×** | -0.47 ⚠ |
| `put/order/seq` | 29.900 ms | 33.480 ms | **1.12×** | -0.45 ⚠ |
| `put/over/same_size` | 8.469 ms | 9.361 ms | **1.11×** | +0.00 |
| `put/over/grow` | 20.952 ms | 26.683 ms | **1.27×** | +0.17 ⚠ |
| `put/val/v8` | 2.665 ms | 3.553 ms | **1.33×** | +0.00 |
| `put/val/v256` | 5.409 ms | 6.244 ms | **1.15×** | -0.18 ⚠ |
| `put/val/v2page` | 19.763 ms | 20.351 ms | **1.03×** | -0.30 ⚠ |
| `put/val/v4k` | 68.305 ms | 69.954 ms | **1.02×** | -0.31 ⚠ |
| `scan/edge/first_last` | 1.410 ms | 3.334 ms | **2.36×** | +0.00 |
| `scan/full/fwd` | 1.462 ms | 2.801 ms | **1.92×** | +0.00 |
| `scan/full/rev` | 1.439 ms | 2.630 ms | **1.83×** | -0.09 |
| `scan/meta/len` | 111.419 µs | 222.113 µs | **1.99×** | +0.00 |
| `scan/prefix/bucket` | 4.843 µs | 13.547 µs | **2.80×** | +0.00 |
| `scan/range/1pct` | 13.179 µs | 24.688 µs | **1.87×** | +0.00 |
| `scan/range/10pct` | 129.961 µs | 234.262 µs | **1.80×** | -0.07 |
| `seek/ge/seq` | 2.799 ms | 4.238 ms | **1.51×** | +0.00 |
| `seek/ge/gap` | 4.016 ms | 5.803 ms | **1.44×** | -0.07 |
| `seek/ge/rand` | 3.850 ms | 5.513 ms | **1.43×** | -0.08 |
