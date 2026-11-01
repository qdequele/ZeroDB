# Public KV-store suite — ZeroDB vs the field (NVMe Graviton) — 2026-10-20

The five benchmarks from the [*fjall 3* article](https://fjall-rs.github.io/post/fjall-3/)
on [rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench), with a
`zerodb` backend added next to the article's engines. This is the complete
three-repetition run that the [2026-09-30 report](archive/2026-09-30-public-suite-nvme.md)
left as a follow-up.

`heed` is Meilisearch's LMDB fork through heed 0.22.1 (`lmdb-master-sys` 0.2.6).
`zerodb` and `zerodb-trusted` are `heed-zerodb` with the default page-validation
policy and with `FileTrust::trust_contents()` (ADR-0014).

| | |
|---|---|
| Machine | AWS Graviton4 `m8gd.xlarge` (Neoverse-V2, 4 vCPU, 15 GiB), Ubuntu 24.04 aarch64, kernel 7.0.0-1014-aws |
| Disk | local NVMe instance store, ext4 `noatime` |
| Method | each run in its own `systemd-run --scope` with `MemoryMax=8G MemorySwapMax=0`; 2 GiB engine cache (`--cache-size`, ignored by LMDB and ZeroDB, which use the page cache); fresh data dir and dropped page cache before each run; 300 s after the load; 1 thread; engine order rotated per repetition |
| Durability | `b_sync` is `--fsync` (every commit durable); the others are eventually durable (LMDB/ZeroDB `NO_SYNC`, redb `None`, SQLite `NORMAL`) |
| Runs | 3 repetitions × 5 benchmarks × 7 engines = 105 runs, all exit 0, no OOM kill; 04:52–18:28 UTC |
| Tree | ZeroDB `2013c13`; rust-storage-bench `v1` branch `96b8846` plus a local commit `573a083` (adds the `zerodb` backend, bumps heed 0.20.5 → 0.22.1) |
| Build | release profile as upstream (no LTO, 16 codegen units), `mimalloc_v3` allocator, rustc 1.99.1 |
| Runner | an external runner (3 rounds, 300 s, 5M keys; not in this repo) |

Numbers are medians over the three repetitions. Throughput counts the run
phase only (first sampled read to the last sample). Latencies are the harness's
end-of-run histograms; disk is the data-dir size at exit; RSS is the run's peak;
load is the time to load the data set before the 300 s run.

## How the `zerodb` backend is built

The backend is a word-for-word copy of the harness's heed backend with only the
crate path changed: same environment flags (`NO_SYNC | NO_READ_AHEAD`, or
`NO_READ_AHEAD` under `--fsync`), same map size, one unnamed database, the same
transaction pattern for every operation. `heed` and `heed-zerodb` link into the
same binary. One forced adaptation applies to both: heed 0.22 refuses a map size
that is not a multiple of the OS page size, so the article's 10^12 bytes is
rounded down to a 64 KiB multiple.

## Differences from the article and from 2026-09-30

- The harness commit used on 2026-09-30 (`f5e87f8`) is no longer in the upstream
  repository; this run uses the current `v1` branch.
- The allocator is mimalloc v3; the article's scripts used snmalloc. This
  applies to every engine alike.
- `feed` uses 5,000,000 users (the runner's default); 2026-09-30 used 8,000,000,
  so the two `feed` tables do not compare.
- The release profile has no LTO and 16 codegen units, as upstream. LMDB is a
  single C file compiled at `-O3`; ZeroDB spans several crates and only inlines
  generic or `#[inline]` code across them, so this setting may favour LMDB over
  every Rust engine.

## a_nosync

| engine | reps | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB | load s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| heed | 3 | 224.7 | 1.00 | 7.12 | 10.41 | 0.94 | 1.22 | 1,758 | 1,229 | 6 |
| zerodb | 3 | 134.2 | 0.60 | 11.97 | 24.11 | 1.33 | 1.86 | 1,807 | 1,229 | 6 |
| zerodb-trusted | 3 | 141.5 | 0.63 | 11.27 | 22.70 | 1.09 | 1.41 | 1,807 | 1,229 | 6 |
| fjall3 | 3 | 177.6 | 0.79 | 3.75 | 7.71 | 5.60 | 12.46 | 2,592 | 1,769 | 12 |
| redb | 3 | 53.4 | 0.24 | 25.60 | 43.93 | 2.89 | 5.17 | 2,503 | 4,112 | 18 |
| rocksdb | 3 | 139.2 | 0.62 | 3.53 | 8.69 | 9.80 | 20.54 | 2,348 | 1,510 | 12 |
| sqlite | 3 | 32.4 | 0.14 | 30.04 | 84.99 | 6.19 | 9.80 | 20 | 2,708 | 24 |

## b_sync

| engine | reps | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB | load s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| heed | 3 | 112.3 | 1.00 | 161.19 | 178.14 | 1.00 | 1.35 | 1,758 | 1,229 | 12 |
| zerodb | 3 | 135.1 | 1.20 | 121.82 | 148.80 | 1.33 | 1.90 | 1,807 | 1,229 | 12 |
| zerodb-trusted | 3 | 135.1 | 1.20 | 129.36 | 148.80 | 1.06 | 1.44 | 1,807 | 1,229 | 12 |
| fjall3 | 3 | 135.2 | 1.20 | 31.27 | 110.23 | 4.87 | 7.56 | 1,655 | 1,332 | 12 |
| redb | 3 | 68.5 | 0.61 | 250.29 | 299.65 | 2.28 | 2.95 | 1,688 | 2,056 | 18 |
| rocksdb | 3 | 88.9 | 0.79 | 75.38 | 119.41 | 7.41 | 14.33 | 2,028 | 1,238 | 12 |
| sqlite | 3 | 133.1 | 1.19 | 30.65 | 88.46 | 5.49 | 7.41 | 21 | 2,708 | 24 |

## blob4k

| engine | reps | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB | load s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| heed | 3 | 47.5 | 1.00 | 80.04 | 167.77 | 0.42 | 1.38 | 7,828 | 19,613 | 90 |
| zerodb | 3 | 53.3 | 1.12 | 25.60 | 108.05 | 0.85 | 81.66 | 7,516 | 19,616 | 90 |
| zerodb-trusted | 3 | 54.0 | 1.14 | 24.11 | 108.05 | 0.56 | 1.75 | 7,514 | 19,616 | 90 |
| fjall3 | 3 | 65.4 | 1.38 | 9.61 | 22.25 | 2.89 | 101.76 | 3,152 | 13,354 | 48 |
| redb | 3 | 15.9 | 0.33 | 213.28 | 882.40 | 2.10 | 535.20 | 3,035 | 24,576 | 195 |
| rocksdb | 3 | 46.8 | 0.98 | 12.97 | 27.18 | 5.94 | 196.88 | 2,643 | 11,231 | 126 |
| sqlite | 3 | 17.4 | 0.37 | 185.42 | 751.93 | 22.25 | 438.18 | 44 | 22,409 | 168 |

## feed

| engine | reps | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB | load s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| heed | 3 | 65.3 | 1.00 | 11.27 | 161.19 | 1.35 | 145.85 | 7,951 | 40,144 | 270 |
| zerodb | 3 | 58.8 | 0.90 | 16.82 | 171.16 | 1.75 | 148.80 | 7,924 | 39,992 | 234 |
| zerodb-trusted | 3 | 60.1 | 0.92 | 16.49 | 171.16 | 1.50 | 148.80 | 7,924 | 40,030 | 228 |
| fjall3 | 3 | 43.8 | 0.67 | 5.60 | 13.77 | 6.44 | 108.05 | 2,642 | 25,456 | 318 |
| redb | 3 | 14.7 | 0.23 | 48.55 | 568.29 | 12.71 | 524.60 | 2,890 | 45,056 | 656 |
| rocksdb | 3 | 39.4 | 0.60 | 5.83 | 15.53 | 10.83 | 171.16 | 2,823 | 24,294 | 414 |
| sqlite | 3 | 49.5 | 0.76 | 35.25 | 231.04 | 5.94 | 161.19 | 118 | 81,410 | 1,068 |

## medium

| engine | reps | kops/s | ÷ LMDB | write p50 µs | write p99 µs | read p50 µs | read p99 µs | peak RSS MiB | disk MiB | load s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| heed | 3 | 93.9 | 1.00 | 78.46 | 158.00 | 0.66 | 80.04 | 8,035 | 21,841 | 102 |
| zerodb | 3 | 83.7 | 0.89 | 83.31 | 164.45 | 0.89 | 81.66 | 8,032 | 23,139 | 108 |
| zerodb-trusted | 3 | 85.9 | 0.92 | 83.31 | 164.45 | 0.79 | 81.66 | 8,031 | 23,139 | 108 |
| fjall3 | 3 | 83.8 | 0.89 | 5.17 | 12.71 | 3.01 | 97.77 | 2,444 | 20,784 | 96 |
| redb | 3 | 16.8 | 0.18 | 231.04 | 546.01 | 2.06 | 447.03 | 2,867 | 28,672 | 289 |
| rocksdb | 3 | 75.2 | 0.80 | 6.06 | 16.16 | 3.91 | 110.23 | 2,783 | 20,866 | 270 |
| sqlite | 3 | 38.2 | 0.41 | 151.80 | 293.72 | 4.68 | 161.19 | 77 | 49,211 | 456 |


## Reading

- **ZeroDB ÷ LMDB, against 2026-09-30:** `a_nosync` 0.50 → 0.60, `b_sync`
  1.00 → 1.20, `blob4k` 1.10 → 1.12, `medium` 0.86 → 0.89 (the earlier run was
  partial). `feed` is 0.90 but used a smaller data set than before.
- **Synchronous commits (`b_sync`):** ZeroDB's write p50 is 122 µs against
  LMDB's 161 µs, which gives the same throughput as fjall 3 and SQLite.
- **Small no-sync updates (`a_nosync`) remain the largest gap:** 0.60× LMDB,
  with write p50 12 µs against 7 µs. This is per-commit CPU, as the
  engine_comparison ladder's commit rungs also show.
- **Trusted-file mode** adds 0–5 % throughput and mostly cuts read latency
  (`blob4k` read p99 82 µs → 1.75 µs).
- **Disk:** equal to LMDB's on the 10 M-key sets and `blob4k`; `medium` is
  1.06× LMDB's (23,139 against 21,841 MiB).

Raw per-run output (`.jsonl`, `.log`, `.du`, `STATUS`, `config.txt`) was copied
off the host before it was terminated; it is not committed.
