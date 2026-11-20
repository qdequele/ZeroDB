# Cold-cache reads on Graviton4 with EBS gp3: LMDB, ZeroDB, and 16 KiB pages

- Date: 2026-11-12
- Question: when the data set is several times larger than memory and nothing
  is cached, how do ZeroDB's reads compare with LMDB's on EBS, and do a
  larger database page or kernel read-ahead help?
- Host: AWS `c8g.xlarge` (Graviton4, Neoverse-V2, 4 vCPU, 8 GiB), eu-west-1,
  Ubuntu 24.04, Linux 7.0.0-1014-aws, 4 KiB OS pages. Launched for this run.
- Storage: one EBS gp3 volume at its baseline (3,000 IOPS, 125 MiB/s), ext4.
- Rust 1.99.1.
- Commit: `8932752`, plus the cold-cache harness in `benches/kv-compare`
  (`cold.sh`, `--page-kib`, `--read-ahead`, disk-read counting, and readers
  that read every value byte).
- LMDB: the Meilisearch fork vendored by `lmdb-master-sys 0.2.6`, through
  heed 0.22.1.

## Method

```sh
cd benches/kv-compare && cargo build --release
DATA=/home/ubuntu/kvc ./cold.sh
python3 cold-report.py results/cold-<run>/results.jsonl
```

Three variants: LMDB, ZeroDB at its default page size (4 KiB here), and
ZeroDB with 16 KiB pages. Two data sets, loaded in random key order with
16-byte keys:

- `small`: 30,000,000 records with 100-byte values, 5.3 to 5.4 GiB on disk.
- `large`: 1,500,000 records with 3,000-byte values, 5.8 to 6.5 GiB on disk.
  A 4 KiB page stores such a value in an overflow page; a 16 KiB page stores
  it inline.

Each measurement is one process that runs for 60 seconds in a cgroup limited
to 1 GiB of memory with no swap, started right after `sync` and
`echo 3 > /proc/sys/vm/drop_caches`, with no warm-up. `get` reads random
existing keys; `range` reads 100 consecutive entries from a random key. Each
read is its own read transaction and reads every byte of the value.
"Read-ahead off" opens the environment with `NO_READ_AHEAD`; "on" opens it
without. "Disk reads per read" and "KiB read per read" come from the block
device's counters (`/sys/block/nvme0n1/stat`) over the measurement.

Each cell is one 60-second run. The eight-thread `get` rows without
read-ahead sit at the volume's 3,000 IOPS: they measure the disk.

## Summary

- At the same page size ZeroDB and LMDB are equal: within 3.5 % on all twelve
  pairs of rows, with the same disk reads per read.
- For random gets the best setting is 4 KiB pages without read-ahead: one
  disk read per get, about 1,500 to 1,600 gets/s on one thread.
- Read-ahead halves random gets on 4 KiB pages for both engines: each get
  reads 140 to 165 KiB from disk instead of 4 to 5 KiB.
- 16 KiB pages without read-ahead cost about three disk reads per get, because
  the kernel reads a 16 KiB database page as separate 4 KiB pages: 2.6 to
  2.8 times fewer gets/s than 4 KiB pages.
- 16 KiB pages with read-ahead cost one disk read per get (about 1,130 to
  1,180 gets/s on one thread) and give the fastest range scans: 563 scans/s
  on `small` against 306 for the best 4 KiB setting, and 42 against 18 on
  `large`.

## Results

| data set | engine | page | read-ahead | phase | threads | reads/s | p50 | p99 | disk reads per read | KiB read per read | file GiB |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| small | lmdb | default | off | get | 1 | 1622 | 565 µs | 1.20 ms | 1.11 | 4.45 | 5.41 |
| small | lmdb | default | off | get | 8 | 3011 | 2.65 ms | 5.70 ms | 1.01 | 4.09 | 5.41 |
| small | lmdb | default | off | range | 1 | 306 | 3.38 ms | 4.65 ms | 5.90 | 23.65 | 5.41 |
| small | lmdb | default | on | get | 1 | 770 | 1.06 ms | 2.65 ms | 1.52 | 159.48 | 5.41 |
| small | lmdb | default | on | get | 8 | 818 | 8.06 ms | 18.09 ms | 1.50 | 159.21 | 5.41 |
| small | lmdb | default | on | range | 1 | 191 | 5.31 ms | 8.06 ms | 5.93 | 651.54 | 5.41 |
| small | zerodb | default | off | get | 1 | 1566 | 582 µs | 1.20 ms | 1.14 | 4.77 | 5.35 |
| small | zerodb | default | off | get | 8 | 2986 | 2.65 ms | 5.70 ms | 1.02 | 4.09 | 5.35 |
| small | zerodb | default | off | range | 1 | 304 | 3.38 ms | 4.65 ms | 5.95 | 24.16 | 5.35 |
| small | zerodb | default | on | get | 1 | 747 | 1.10 ms | 2.52 ms | 1.50 | 164.17 | 5.35 |
| small | zerodb | default | on | get | 8 | 809 | 8.06 ms | 18.09 ms | 1.54 | 160.84 | 5.35 |
| small | zerodb | default | on | range | 1 | 190 | 5.18 ms | 8.06 ms | 5.80 | 652.20 | 5.35 |
| small | zerodb | 16 KiB | off | get | 1 | 586 | 1.69 ms | 3.44 ms | 3.06 | 12.77 | 5.32 |
| small | zerodb | 16 KiB | off | get | 8 | 1058 | 7.93 ms | 14.55 ms | 2.88 | 11.56 | 5.32 |
| small | zerodb | 16 KiB | off | range | 1 | 238 | 4.39 ms | 6.88 ms | 7.65 | 31.95 | 5.32 |
| small | zerodb | 16 KiB | on | get | 1 | 1184 | 991 µs | 1.98 ms | 0.94 | 104.61 | 5.32 |
| small | zerodb | 16 KiB | on | get | 8 | 1243 | 7.54 ms | 15.34 ms | 0.96 | 104.79 | 5.32 |
| small | zerodb | 16 KiB | on | range | 1 | 563 | 1.92 ms | 3.31 ms | 1.98 | 221.93 | 5.32 |
| large | lmdb | default | off | get | 1 | 1519 | 565 µs | 1.23 ms | 1.17 | 4.68 | 5.85 |
| large | lmdb | default | off | get | 8 | 2912 | 2.65 ms | 5.70 ms | 1.05 | 4.23 | 5.85 |
| large | lmdb | default | off | range | 1 | 18 | 57.15 ms | 59.24 ms | 98.93 | 396.28 | 5.85 |
| large | lmdb | default | on | get | 1 | 846 | 1.04 ms | 2.20 ms | 1.52 | 143.17 | 5.85 |
| large | lmdb | default | on | get | 8 | 917 | 7.67 ms | 17.56 ms | 1.52 | 141.95 | 5.85 |
| large | lmdb | default | on | range | 1 | 12 | 84.93 ms | 99.61 ms | 94.96 | 10554.97 | 5.85 |
| large | zerodb | default | off | get | 1 | 1515 | 582 µs | 1.20 ms | 1.18 | 4.92 | 5.82 |
| large | zerodb | default | off | get | 8 | 2910 | 2.65 ms | 5.70 ms | 1.05 | 4.20 | 5.82 |
| large | zerodb | default | off | range | 1 | 18 | 57.15 ms | 58.20 ms | 99.72 | 416.19 | 5.82 |
| large | zerodb | default | on | get | 1 | 876 | 1.02 ms | 2.20 ms | 1.52 | 137.98 | 5.82 |
| large | zerodb | default | on | get | 8 | 935 | 7.67 ms | 17.04 ms | 1.57 | 139.26 | 5.82 |
| large | zerodb | default | on | range | 1 | 12 | 84.93 ms | 97.52 ms | 93.85 | 10528.84 | 5.82 |
| large | zerodb | 16 KiB | off | get | 1 | 578 | 1.69 ms | 3.90 ms | 3.07 | 12.33 | 6.48 |
| large | zerodb | 16 KiB | off | get | 8 | 1039 | 8.06 ms | 13.24 ms | 2.94 | 11.84 | 6.48 |
| large | zerodb | 16 KiB | off | range | 1 | 17 | 63.44 ms | 70.25 ms | 109.10 | 437.23 | 6.48 |
| large | zerodb | 16 KiB | on | get | 1 | 1128 | 991 µs | 2.02 ms | 0.96 | 110.04 | 6.48 |
| large | zerodb | 16 KiB | on | get | 8 | 1187 | 7.67 ms | 15.07 ms | 0.98 | 109.73 | 6.48 |
| large | zerodb | 16 KiB | on | range | 1 | 42 | 24.38 ms | 29.62 ms | 26.16 | 2983.02 | 6.48 |

## Limits

- One run per cell, on one instance. The differences called out above are
  1.8 times or more; the differences between LMDB and ZeroDB at 4 KiB are
  within what a second run could move.
- The 1 GiB limit holds the branch pages of every variant, so a get reads
  about one leaf (and, on `large` with 4 KiB pages, mostly one overflow page
  while the leaves stay cached). A smaller limit or a larger data set would
  add branch-page reads.
- The data sets were loaded in random key order and never updated or
  deleted from; a file fragmented by a long write history may scan
  differently.
- `cold.sh` at this commit raises the kernel's dirty-page limits during the
  loads. For this run they were raised by hand, after the first LMDB load
  had started; load times were not kept.
