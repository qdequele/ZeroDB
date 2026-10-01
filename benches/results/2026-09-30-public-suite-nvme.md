# Public KV-store suite — ZeroDB vs the field (NVMe Graviton) — 2026-09-30

The five benchmarks from the [*fjall 3* article](https://fjall-rs.github.io/post/fjall-3/)
on [rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench), with a
`zerodb` backend added next to the article's engines. `heed` is the Meilisearch
LMDB fork (0.22.1); `zerodb` / `zerodb-trusted` are `heed-zerodb` in the default
and `FileTrust::trust_contents()` (ADR-0014) policies.

| | |
|---|---|
| Machine | AWS Graviton4 `m8gd.xlarge` (Neoverse-V2, 4 vCPU, 15 GiB), Ubuntu 24.04 aarch64, kernel 7.0 |
| Disk | local NVMe instance store, ext4 `noatime` |
| Method | each run in its own `systemd-run --scope` with `MemoryMax=8G MemorySwapMax=0`; 2 GiB engine cache; fresh data dir and dropped page cache before each run; 300 s after the load; engine order rotated per repetition |
| Durability | `b_sync` is `--fsync` (every commit durable); the rest are eventually durable (LMDB/ZeroDB `NO_SYNC`, redb `None`, SQLite `NORMAL`) |
| Tree | ZeroDB `2fc95c1`; rust-storage-bench `f5e87f8` |

**Reps: 2 for `a_nosync` / `b_sync` / `blob4k`; 1 for `feed`; `medium` partial
(the run was interrupted after rep 2's load).** Numbers are medians over the reps
present and should be read as **indicative**, not final — a complete 3-rep run is
the follow-up.

Throughput counts the run phase only (first sampled read to the last sample).
Latencies are the harness's end-of-run histograms; disk is the data-dir size at
exit; RSS is the run's peak.

## a_nosync — YCSB A (50/50 read/update), 10 M × 100 B, no sync

| engine | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heed (LMDB) | 223.4 | 1.00 | 7.19 | 10.51 | 0.97 | 1.30 | 1,782 | 1,229 |
| zerodb | 111.1 | 0.50 | 14.92 | 26.38 | 1.72 | 2.23 | 1,756 | 1,238 |
| zerodb-trusted | 119.6 | 0.54 | 13.91 | 25.10 | 1.22 | 1.57 | 1,754 | 1,238 |
| fjall 3 | 187.3 | 0.84 | 3.57 | 7.48 | 5.27 | 11.62 | 2,461 | 1,694 |
| redb | 67.3 | 0.30 | 21.81 | 38.19 | 2.47 | 5.94 | 3,754 | 4,112 |
| rocksdb | 139.0 | 0.62 | 3.46 | 8.78 | 9.80 | 20.96 | 2,372 | 1,481 |
| sqlite | 32.3 | 0.14 | 30.96 | 84.15 | 6.31 | 9.52 | 50 | 2,708 |

## b_sync — YCSB B (95/5 read/update), 10 M × 100 B, **fsync**

| engine | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heed (LMDB) | 112.1 | 1.00 | 161.19 | 178.14 | 1.02 | 1.35 | 1,782 | 1,229 |
| zerodb | 112.0 | 1.00 | 154.87 | 178.14 | 1.41 | 2.04 | 1,756 | 1,238 |
| zerodb-trusted | 112.0 | 1.00 | 158.00 | 178.14 | 1.15 | 1.59 | 1,754 | 1,238 |
| fjall 3 | 148.1 | 1.32 | 28.86 | 111.34 | 4.27 | 6.50 | 1,689 | 1,389 |
| redb | 68.4 | 0.61 | 255.34 | 299.65 | 2.00 | 2.57 | 3,293 | 2,056 |
| rocksdb | 89.1 | 0.79 | 74.64 | 120.62 | 7.33 | 14.05 | 2,051 | 1,238 |
| sqlite | 129.9 | 1.16 | 31.27 | 90.25 | 5.65 | 7.63 | 50 | 2,708 |

## blob4k — YCSB B, 2.5 M × 4096 B, no sync

| engine | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heed (LMDB) | 46.5 | 1.00 | 80.04 | 189.16 | 0.40 | 1.39 | 7,828 | 19,613 |
| zerodb | 51.4 | 1.10 | 28.87 | 110.25 | 0.95 | 82.49 | 7,631 | 19,615 |
| zerodb-trusted | 52.0 | 1.12 | 30.05 | 111.34 | 0.64 | 1.67 | 7,632 | 19,615 |
| fjall 3 | 61.1 | 1.31 | 9.23 | 21.82 | 2.67 | 178.10 | 3,078 | 13,717 |
| redb | 12.3 | 0.26 | 231.23 | 1,011.00 | 1.92 | 645.37 | 4,742 | 24,576 |
| rocksdb | 42.7 | 0.92 | 12.84 | 27.46 | 6.07 | 271.19 | 2,607 | 11,344 |
| sqlite | 15.1 | 0.33 | 195.01 | 1,245.95 | 22.93 | 585.46 | 68 | 22,409 |

## feed — 8 M users × 50 posts × 100 B (1 rep)

| engine | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heed (LMDB) | 54.6 | 1.00 | 11.73 | 167.77 | 1.44 | 154.87 | 7,858 | 62,400 |
| zerodb | 42.4 | 0.78 | 23.16 | 255.34 | 2.10 | 231.04 | 7,872 | 62,455 |
| zerodb-trusted | 46.6 | 0.85 | 21.81 | 189.16 | 1.65 | 158.00 | 7,870 | 62,598 |
| fjall 3 | 37.2 | 0.68 | 5.49 | 13.77 | 6.19 | 255.34 | 2,881 | 40,366 |
| redb | 15.7 | 0.29 | 54.74 | 615.63 | 16.49 | 546.01 | 4,735 | 69,632 |
| rocksdb | 31.0 | 0.57 | 5.94 | 16.49 | 11.50 | 318.18 | 3,118 | 38,734 |
| sqlite | 38.4 | 0.70 | 47.59 | 344.68 | 6.44 | 250.29 | 204 | 129,970 |

## medium — YCSB B, 100 M × 200 B, no sync (**partial run**)

| engine | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| heed (LMDB) | 92.6 | 1.00 | 78.46 | 185.42 | 0.66 | 80.04 | 8,036 | 21,841 |
| zerodb | 79.3 | 0.86 | 88.46 | 193.14 | 0.99 | 81.66 | 8,016 | 23,297 |
| zerodb-trusted | 75.8 | 0.82 | 86.71 | 260.55 | 0.89 | 105.90 | 8,014 | 23,297 |
| fjall 3 | 76.3 | 0.82 | 4.92 | 12.22 | 2.75 | 182.37 | 2,389 | 20,833 |
| redb | 14.5 | 0.16 | 271.19 | 597.46 | 1.85 | 460.67 | 4,709 | 24,576 |
| rocksdb | 65.0 | 0.70 | 6.13 | 16.66 | 3.64 | 234.31 | 2,824 | 20,835 |
| sqlite | 35.9 | 0.39 | 151.80 | 546.01 | 4.77 | 171.16 | 101 | 49,211 |

## Reading it

- **ZeroDB tracks LMDB.** At parity on durable writes (`b_sync` 1.00×), a little
  ahead on 4 KB values (`blob4k` 1.10–1.12×), and behind on the write-heavy
  no-sync mix (`a_nosync` 0.50×) — holding a write transaction's dirty pages in
  memory is its weakest path. On the bigger sets it is 0.78–0.86×.
- **B-tree vs LSM.** The LSM engines (fjall, rocksdb) win raw write throughput and
  write p50 (single-digit µs); the B-trees (LMDB, ZeroDB) keep **read p99 in the
  low µs** on the in-memory sets where the LSMs are 10–20 µs, and far lower RSS on
  the small-value workloads.
- **Trusted-file mode** (ADR-0014) mainly helps read tails: `blob4k` read p99
  82.5 µs → 1.67 µs (it skips per-page validation on the read path).

Reproduce with `benches-server/public-suite.sh` and summarize with
`benches-server/suite_summary.py`.
