# Ladder and Meilisearch on Graviton4 with EBS gp3: default and validating

- Date: 2026-11-12
- Question: on the target hardware, how does current `main` compare with
  LMDB under its default (pages trusted, ADR-0034) and with
  `FileTrust::VALIDATE`? Earlier reports predate the trusted default, and
  durable commits on gp3 had not been measured.
- Host: AWS `c8g.xlarge` (Graviton4, Neoverse-V2, 4 vCPU, 8 GiB), eu-west-1,
  Ubuntu 24.04, Linux 7.0.0-1014-aws, 4 KiB OS and database pages. Launched
  for this run. Load average 1.1 to 1.2 during the ladder.
- Storage: one 60 GiB EBS gp3 volume at its baseline (3,000 IOPS, 125 MiB/s),
  ext4. The benchmark environments live in `/tmp` on that volume.
- Rust 1.99.1, `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1` (how Meilisearch builds).
- Commit: `d70dbe9` (`main`), plus the `ZERODB_BENCH_VALIDATE` knob in the
  bench harness.
- LMDB: the Meilisearch fork vendored by `lmdb-master-sys 0.2.6`, through
  heed 0.22.1.

## Method

The whole ladder ran twice, one pass after the other:

```sh
cargo bench -p zerodb-oracle --bench engine_comparison -- '^'
ZERODB_BENCH_VALIDATE=1 cargo bench -p zerodb-oracle --bench engine_comparison -- '^'
```

Each pass measures LMDB and ZeroDB, so each ratio compares two engines from
the same pass. The passes were not interleaved. "LMDB drift" is LMDB's time
in the second pass divided by its time in the first: where it is more than
3 % from 1.000, the difference between the two ZeroDB columns is within the
noise of the machine. That is the case for 13 rungs, most of them `get/*`,
where LMDB ran up to 9 % faster in the second pass. The commit, put, delete
and scan rungs drifted by less than 2 %.

Ratios are ZeroDB time ÷ LMDB time; below 1.00, ZeroDB is faster.

## Summary

- Durable commits on gp3 are at parity: `commit/sync/*` 0.98 to 1.00.
- Point reads and seeks are faster than LMDB (0.62 to 0.97), range scans
  0.77 to 0.78, and full scans are at parity (1.01 to 1.03).
- Sequential and random puts are 0.93 to 0.94. A single-put no-sync commit is
  1.15, the write cursor 1.26, per-key deletes 1.27 to 1.28, cursor drains
  1.40 to 1.55, and an empty write transaction 3.49.
- Creating an environment is 27.8 times LMDB's time (60.9 ms against
  2.2 ms). ZeroDB syncs the new file and its directory, and on gp3 those
  syncs cost far more than on local NVMe, where earlier runs measured about
  10 times.
- Validation costs little outside two families: full scans go from parity to
  1.43 to 1.57, and reads of overflow values from 0.77 to 1.31 to 1.33. Most
  other rungs move by 0.05 or less.
- Meilisearch v1.55.0 on the movies workloads: indexing 0.98, search 1.00.

## Ladder

| rung | LMDB | ZeroDB default | ZeroDB validating | LMDB drift |
|---|---:|---:|---:|---:|
| `commit/batch/n10k` | 14.2 ms | 0.93× | 0.94× | 0.991 |
| `commit/batch/n1` | 7.28 ms | 1.15× | 1.18× | 1.005 |
| `commit/batch/n100` | 6.24 ms | 1.01× | 1.01× | 1.007 |
| `commit/sync/n100` | 778 ms | 1.00× | 0.99× | 0.997 |
| `commit/sync/n1` | 395 ms | 0.98× | 0.98× | 1.001 |
| `commit/sync/n1_map16g` | 396 ms | 0.98× | 0.98× | 1.001 |
| `del/batch/k1000` | 32.6 ms | 1.06× | 1.08× | 1.004 |
| `del/bulk/half` | 7.12 ms | 1.27× | 1.29× | 0.999 |
| `del/bulk/all` | 14 ms | 1.28× | 1.29× | 0.995 |
| `del/churn/reinsert` | 9.45 ms | 1.22× | 1.23× | 1.003 |
| `del/clear/all` | 118 µs | 1.50× | 1.50× | 1.003 |
| `del/cursor/drain` | 5.26 ms | 1.55× | 1.57× | 0.999 |
| `del/cursor/rev` | 5.42 ms | 1.40× | 1.40× | 0.993 |
| `del/random/n1` | 934 ms | 1.18× | 1.21× | 0.996 |
| `del/range/half` | 5.23 ms | 0.54× | 0.55× | 0.994 |
| `env/open/reopen` | 759 µs | 0.91× | 0.92× | 0.997 |
| `env/open/create` | 2.19 ms | 27.79× | 27.68× | 0.998 |
| `env/stat/non_free` | 235 ns | 0.45× | 0.46× | 0.983 |
| `env/txn/ro_begin_abort` | 960 µs | 0.72× | 0.69× | 1.061 |
| `env/txn/ro_get_one` | 4.98 ms | 1.36× | 1.45× | 1.006 |
| `env/txn/rw_empty_commit` | 54.7 µs | 3.49× | 3.34× | 1.019 |
| `get/access/hot` | 1.41 ms | 0.62× | 0.66× | 0.906 |
| `get/access/miss` | 890 µs | 0.85× | 0.86× | 0.962 |
| `get/access/rand` | 3.63 ms | 0.83× | 0.86× | 1.010 |
| `get/access/seq` | 1.49 ms | 0.77× | 0.82× | 0.956 |
| `get/db/root` | 3.6 ms | 0.81× | 0.87× | 0.996 |
| `get/db/named` | 3.58 ms | 0.85× | 0.89× | 0.998 |
| `get/db/named_x8` | 3.29 ms | 0.82× | 0.90× | 1.006 |
| `get/key/k8` | 3.73 ms | 0.84× | 0.90× | 0.955 |
| `get/key/k128` | 4.92 ms | 0.95× | 1.02× | 0.983 |
| `get/key/k32` | 3.93 ms | 0.93× | 1.00× | 0.981 |
| `get/size/n1k` | 1.42 ms | 0.81× | 0.84× | 0.957 |
| `get/size/n50k` | 3.63 ms | 0.85× | 0.89× | 0.966 |
| `get/val/v8` | 1.91 ms | 0.74× | 0.80× | 0.930 |
| `get/val/v256` | 2.6 ms | 0.75× | 0.76× | 0.972 |
| `get/val/v2page` | 1.96 ms | 0.77× | 1.33× | 0.930 |
| `get/val/v4k` | 1.94 ms | 0.78× | 1.31× | 0.932 |
| `get/val/v8_touch` | 1.92 ms | 0.75× | 0.80× | 0.926 |
| `get/val/v2page_touch` | 3.12 ms | 0.89× | 0.90× | 0.978 |
| `get/val/v4k_touch` | 3.08 ms | 0.89× | 0.91× | 0.971 |
| `maint/copy/raw` | 2.56 ms | 1.12× | 1.16× | 0.986 |
| `maint/copy/compact` | 2.65 ms | 1.20× | 1.32× | 1.008 |
| `mixed/rw/8dbs` | 46.9 ms | 0.95× | 0.95× | 0.997 |
| `put/api/plain` | 14.7 ms | 0.93× | 0.94× | 1.001 |
| `put/api/reserved` | 14.8 ms | 1.04× | 1.04× | 1.006 |
| `put/cursor/rewrite` | 1.56 ms | 1.26× | 1.30× | 1.013 |
| `put/cursor/rewrite_reserved` | 1.8 ms | 1.27× | 1.31× | 1.003 |
| `put/cursor/rewrite_reserved_ovf` | 33.5 ms | 1.06× | 1.06× | 1.004 |
| `put/gc/drain_big` | 23.3 ms | 1.22× | 1.25× | 1.000 |
| `put/gc/ovf_across` | 599 µs | 1.42× | 1.48× | 0.989 |
| `put/gc/ovf_miss` | 1.25 ms | 1.05× | 1.06× | 0.991 |
| `put/order/append` | 9.23 ms | 1.18× | 1.19× | 0.996 |
| `put/order/rand` | 38.3 ms | 0.94× | 0.96× | 0.980 |
| `put/order/seq` | 14.8 ms | 0.93× | 0.93× | 1.003 |
| `put/over/same_size` | 2.98 ms | 1.05× | 1.06× | 1.002 |
| `put/over/grow` | 8.85 ms | 1.09× | 1.12× | 0.993 |
| `put/val/v8` | 1.44 ms | 0.95× | 0.95× | 0.994 |
| `put/val/v256` | 2.71 ms | 0.96× | 0.96× | 0.995 |
| `put/val/v2page` | 6.65 ms | 1.09× | 1.08× | 1.007 |
| `put/val/v2page_abort` | 1.49 ms | 1.12× | 1.12× | 1.001 |
| `put/val/v4k` | 23.8 ms | 1.09× | 1.09× | 1.001 |
| `scan/edge/first_last` | 982 µs | 1.07× | 1.08× | 1.003 |
| `scan/full/fwd` | 488 µs | 1.03× | 1.43× | 1.002 |
| `scan/full/big` | 17.5 ms | 1.01× | 1.57× | 0.986 |
| `scan/full/rev` | 497 µs | 1.01× | 1.46× | 0.988 |
| `scan/meta/len` | 43.4 µs | 1.45× | 1.40× | 1.030 |
| `scan/prefix/bucket` | 3.37 µs | 1.11× | 1.28× | 1.002 |
| `scan/range/1pct` | 7.65 µs | 0.78× | 0.93× | 1.011 |
| `scan/range/10pct` | 69 µs | 0.77× | 0.91× | 1.003 |
| `scan/skip_take/fwd` | 49.9 µs | 0.25× | 0.48× | 0.993 |
| `scan/skip_take/rev` | 49.9 µs | 0.25× | 0.48× | 1.005 |
| `seek/ge/seq` | 1.91 ms | 0.80× | 0.95× | 1.014 |
| `seek/ge/gap` | 4.06 ms | 0.97× | 1.03× | 0.980 |
| `seek/ge/rand` | 3.88 ms | 0.84× | 0.89× | 0.962 |

## Meilisearch v1.55.0

`ROUNDS=2 scripts/consumer.sh bench`: Meilisearch built once on LMDB and once
on ZeroDB (default options), the `movies.json` and `search/movies.json`
workloads, 20 runs per engine, engine order alternated between rounds.

| workload | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| `movies.json` indexing, total self time | 6.830 s | 6.666 s | 0.98× |
| … `indexing::scheduler::process_batch` | 4.339 s | 4.355 s | 1.00× |
| … `indexing::write_db::all` | 3.893 s | 3.881 s | 1.00× |
| … `indexing::documents::extract` | 3.762 s | 3.796 s | 1.01× |
| … `indexing::scheduler::commit` | 274.4 ms | 320.5 ms | 1.17× |
| `search/movies.json`, total self time | 9.1 ms | 9.1 ms | 1.00× |
| … `search::main::execute_search` | 3.4 ms | 3.4 ms | 1.00× |

Span rows are the median inclusive time over the runs.

## Limits

- One host, one volume type at its baseline settings, one run. gp3 with
  provisioned IOPS, io2 and larger instances were not measured.
- The two ladder passes were sequential, so the validating column is
  indicative on the rungs whose LMDB drift exceeds 3 %.
- The Meilisearch comparison used the default (trusting) policy only, and the
  small movies workloads; the larger hackernews workloads were not run.
