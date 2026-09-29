# YCSB bigger than RAM — LMDB vs ZeroDB (rust-storage-bench) — 2026-09-29

Phase D, first pass. [rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench) (branch `v1`, the harness behind the fjall 3 post), with a `zerodb` backend added next to its `heed` one (both heed 0.22.1-shaped; `heed` = the Meilisearch LMDB fork, `zerodb` = `heed-zerodb`). The author's YCSB configuration (`scripts/ycsb.nu`): 10M items, 128-byte JSON values, Zipfian keys, 60 s per engine, 8 MiB engine cache. Each engine runs in its own `systemd-run --scope -p MemoryMax=2G -p MemorySwapMax=0`, so the ~1.5 GB database plus its working set does not fit.

**How this harness uses the engines:** every point read is its own read txn and every write its own write txn + commit (`NO_SYNC`, since the runs are without `--fsync`), and the heed backend opens the env with `NO_READ_AHEAD` (LMDB: `madvise(MADV_RANDOM)`).

| | |
|---|---|
| Machine | Intel Xeon E3-1230 v2 (4C/8T), 31 GB, 2 × SATA SSD in mdraid RAID 1 |
| OS | Debian 12, Linux 6.1, 4 KiB pages, cgroup v2 |
| Bench mode | performance governor, turbo off, page cache dropped before each engine |
| Tree | `93b2740` (ZeroDB); rust-storage-bench `v1` + zerodb backend |

## First run: ZeroDB ignored `NO_READ_AHEAD`

| engine | YCSB A ops/s | YCSB B ops/s | read p99 (A / B) | disk read in 60 s (A / B) |
|---|---:|---:|---:|---:|
| LMDB (heed 0.22.1) | 293,392 | 644,415 | 1.7 / 1.5 µs | 101 / 268 MiB |
| ZeroDB | 143,281 | 147,449 | 1.4 / 1.4 ms | 9.8 / 10.4 GB |
| ZeroDB trusted | 145,358 | 147,867 | 1.4 / 1.4 ms | 10.0 / 10.9 GB |
| fjall 3.1.2 | 152,734 | 144,713 | 0.41 / 0.40 ms | 12 / 12 MiB |
| redb 4.1 | 136,381 | 183,542 | 0.98 ms / 35 µs | 3.9 GB / 12 MiB |

ZeroDB accepted `NO_READ_AHEAD` as a no-op, so every page fault read a whole readahead window: the 2 GB cap filled with pages nobody asked for and evicted the working set — ~10 GB read from disk in 60 s for a 1.5 GB database. Fixed in `93b2740`: the flag now advises the map `MADV_RANDOM`, as LMDB does.

## After the fix

| engine | ops/s | read p50 | read p99 | write p50 | write p99 | peak RSS | disk read | disk written | write amp |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **YCSB A** (50 % reads / 50 % updates) | | | | | | | | | |
| LMDB | 291,254 | 713 ns | 1.8 µs | 8.5 µs | 14.0 µs | 2,000 MiB | 100 MiB | 5,820 MiB | 2.82 |
| ZeroDB | 167,300 | 2.8 µs | 209 µs | 18.2 µs | 236 µs | 2,027 MiB | 863 MiB | 3,208 MiB | 2.10 |
| ZeroDB trusted | 171,258 | 1.3 µs | 205 µs | 16.2 µs | 231 µs | 2,028 MiB | 908 MiB | 3,275 MiB | 2.14 |
| **YCSB B** (95 % reads / 5 % updates) | | | | | | | | | |
| LMDB | 607,258 | 550 ns | 1.5 µs | 8.9 µs | 17.2 µs | 2,004 MiB | 270 MiB | 3,107 MiB | 1.95 |
| ZeroDB | 190,165 | 2.3 µs | 129 µs | 21.4 µs | 155 µs | 2,034 MiB | 1,451 MiB | 1,858 MiB | 1.33 |
| ZeroDB trusted | 197,312 | 963 ns | 122 µs | 20.5 µs | 155 µs | 2,033 MiB | 1,651 MiB | 1,932 MiB | 1.38 |

`MADV_RANDOM` cut ZeroDB's disk reads 7–11× and its p99 ~7×, and raised throughput 17 % (A) and 29 % (B). ZeroDB is still at 57 % (A) and 31 % (B) of LMDB:

1. **Per-operation transactions.** ZeroDB's read p50 is 2.3–2.8 µs against 0.55–0.71 µs and its write p50 18–21 µs against 8.5–8.9 µs. Every read txn starts with an empty validated-pages memo, so each get fully validates every page on its path (trusted mode halves the read p50), and a one-put commit is ~2× LMDB (`commit/batch/n1` on the ladder).
2. **Memory pressure.** Both engines sit at the 2 GB cap, yet ZeroDB reads 5–9× more from disk (0.9–1.7 GB vs 0.1–0.3 GB): its p99 is a major fault. Either ZeroDB touches more pages per operation or keeps less of the working set resident; not yet diagnosed.

Meilisearch does not set `NO_READ_AHEAD` and keeps one read txn per request, so neither the thrash nor the per-op txn cost applies to it as-is; hannoy search does not set it either.
