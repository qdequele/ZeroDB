# Env-wide cache of validated page versions (ADR-0018) — bench server, Linux x86-64

_2026-09-30. Base `df50c1a` vs the ADR-0018 change, LMDB (heed 0.22.1) as the
drift control. Ratios are ZeroDB time ÷ LMDB time (lower is better)._

## What changed

Under the default (validating) policy, a read txn used to walk every cell of
each map page the first time **that txn** saw it. A workload of short read
txns re-validated the same hot pages on every txn. Read txns now share an
env-wide cache keyed by (pgno, page kind, header txnid stamp): a page version
validated by one txn is taken with the zero-check view by the next. A reused
page carries a newer stamp and misses. Write txns and `static_read_txn` do
not use it. Design and soundness argument: ADR-0018; spec: SPEC 04 TXN-38.

## Engine ladder, Meilisearch build (codegen-units=1), 3 rounds

**24 faster, 0 slower, 32 flat, 2 unreliable (LMDB drift > 3 %).**

| rung | before | after | after ÷ before |
|---|---:|---:|---:|
| `get/db/named` | 1.15× | **0.95×** | 0.819 |
| `get/db/root` | 1.15× | **0.94×** | 0.817 |
| `get/key/k8` | 1.16× | **0.95×** | 0.814 |
| `get/access/rand` | 1.16× | **0.94×** | 0.814 |
| `get/size/n50k` | 1.16× | **0.96×** | 0.828 |
| `seek/ge/rand` | 1.15× | **0.95×** | 0.833 |
| `seek/ge/seq` | 1.19× | **0.95×** | 0.791 |
| `scan/full/fwd` | 1.71× | **1.49×** | 0.873 |
| `scan/range/1pct` | 1.70× | **1.33×** | 0.780 |
| `scan/prefix/bucket` | 2.28× | **1.76×** | 0.772 |

Long tier, 1 round: `get/size/n1m` (1M-key tree) **1.48× → 1.08×** (0.733).

Flat, as required: memo-hit rungs (`get/access/hot`, `get/access/miss`,
`get/size/n1k`), every `put/*`, `commit/*`, `del/*` and `maint/*` rung, and
the per-txn rungs (`env/txn/ro_begin_abort`, `env/txn/rw_empty_commit`,
`env/stat/non_free`, `env/open/reopen`).

## Engine ladder, hannoy build (codegen-units=16)

1 round of the full ladder: 25 faster; `commit/sync/n100` and
`env/open/reopen` flagged, both flat on a 3-round re-run (1.032 ± 0.143;
`commit/sync` unreliable, fsync-bound).

| rung | before | after | after ÷ before |
|---|---:|---:|---:|
| `get/db/named` | 1.15× | **0.95×** | 0.825 |
| `get/access/rand` | 1.15× | **0.94×** | 0.822 |
| `seek/ge/seq` | 1.24× | **0.99×** | 0.795 |
| `scan/full/fwd` | 1.75× | **1.61×** | 0.920 |
| `scan/range/1pct` | 1.58× | **1.24×** | 0.781 |

## One operation per transaction (`short_txn_census`, 1M keys, 200k ops, 3 runs)

| per op | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| read txn + get | 649 ns | 1,984 ns (3.06×) | **1,008 ns (1.55×)** |
| write txn + put + commit | 5,949 ns | 14,924 ns | 15,340 ns (not using the cache; run spread 14.9–15.6 µs) |

## YCSB B, bigger than RAM (rust-storage-bench, 10M × 128 B, 2 GB cap, 60 s)

| engine | ops/s | read p50 | read p99 | disk read | peak RSS |
|---|---:|---:|---:|---:|---:|
| LMDB | 627,633 | 518 ns | 1.5 µs | 262 MiB | 2,004 MiB |
| ZeroDB before | 191,763 | 2.3 µs | 124 µs | 1,492 MiB | 2,033 MiB |
| ZeroDB after | 194,720 | **1.1 µs** | 129 µs | 1,577 MiB | 2,034 MiB |

The median read halves, but throughput moves 1.5 %: under the cap this
workload is bound by disk reads (p99 is I/O latency). ZeroDB reads ~6× more
from disk than LMDB because a large write txn's dirty pages peak at ~1.7 GB
and glibc keeps them after commit, leaving less memory for the page cache
(2026-09-29 diagnosis). That is ADR-0017's subject, not this cache's.

## Shapes measured and dropped on the way

Seven intermediate designs were measured before this one; each is listed with
its cost in ADR-0018 §Implementation. The two that shaped the final design:
writers consulting the cache (`put/val/v8` +12 %), and hashed slots (a full
scan paid one random table line and page per leaf: `scan/full/*` +5 %, perf
showing −15 % instructions but +17 % L1 misses). Slots are now indexed by
pgno, so a scan walks the table almost sequentially.

## Correctness

Local gate (fmt, clippy, test, miri, fuzz-quick) green; all loom models pass,
including the new `loom_stamp_cache_never_mixes_publishes` (exhaustive; with
the reader's sequence re-check removed it finds a mixed entry in under a
second). `zerodb/tests/page_version_identity.rs` checks that no (pgno, stamp)
pair ever appears with two byte images; with COW restamping disabled it
fails.
