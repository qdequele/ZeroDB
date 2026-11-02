# Bounded free-page records (ADR-0037), macOS laptop

- **Change:** ADR-0037 Option C. The free-page tree is keyed `(txnid, seq)`,
  and every record holds at most `K` ids (1,017 at 16 KiB pages) inline.
  On-disk format 3.
- **Machine:** Apple M1 Pro, 16 GiB, macOS 27.0.1, 16 KiB OS pages (both
  engines at 16 KiB), rustc 1.99.0. Other builds ran on the machine
  throughout; the load average is given with each run. Every number here is
  indicative. A verdict at the 3 % level needs a quiet Linux host.
- **Before:** `65cd4e2` (main `3c4f37f` plus the ADR). **After:** the
  implementation branch. LMDB is heed 0.22.1 over the Meilisearch fork, in the
  same binary as each side.

## Small commits after one large delete (`freelist_big_record`)

`freelist_big_record 500 0 2000 0 6000 24000 96000 192000`: `NO_SYNC`, 2,000
empty write txns first, then one txn clears `pages` overflow pages, then 500
single-put commits. Three rounds of before then after, load average 8 to 9.
The value is the median over the rounds of each round's median of commits 3
to 500, in µs. The LMDB column is the median of its six runs.

| Pages freed | LMDB | ZeroDB before | ZeroDB after | before ÷ LMDB | after ÷ LMDB |
|--:|--:|--:|--:|--:|--:|
| 0 (control) | 8.9 | 8.9 | 8.9 | 1.00 | 1.00 |
| 6,000 | 17.1 | 32.5 | 21.8 | 1.90 | 1.27 |
| 24,000 | 17.3 | 78.3 | 21.7 | 4.53 | 1.26 |
| 96,000 | 21.4 | 274.1 | 25.2 | 12.8 | 1.18 |
| 192,000 | 22.5 | 512.8 | 25.3 | 22.8 | 1.12 |

The first two commits after the delete (medians, µs) and the delete itself
(ms):

| Pages | LMDB c1 / c2 | before c1 / c2 | after c1 / c2 | delete LMDB / before / after |
|--:|--:|--:|--:|--:|
| 96,000 | 219 / 236 | 601 / 452 | 155 / 28 | 202 / 2.8 / 3.9 |
| 192,000 | 266 / 937 | 1,152 / 639 | 222 / 27 | 4,037 / 12.2 / 17.9 |

The rounds above ran before the run-search memo was added (see
`free_run_churn` below). The final code re-run (after side only, three rounds,
load average 28 to 42, so both engines are slower) gave a ZeroDB p50 of
11.9 / 29.2 / 28.8 / 29.2 / 37.2 µs for the five sizes, against LMDB's
11.7 / 20.5 / 21.4 / 30.9 / 35.2 µs in the same runs. The cost no longer
grows with the size of the delete.

**Durable commits** (default flush, `freelist_big_record 100 1 2000 0 24000
96000`, one round each, p50 in ms):

| Pages freed | LMDB | ZeroDB before | ZeroDB after |
|--:|--:|--:|--:|
| 0 | 4.0 | 8.0 | 7.7 |
| 24,000 | 4.0 | 8.0 | 8.0 |
| 96,000 | 4.0 | 9.1 | 8.0 |

After the change, a large delete adds nothing to a durable commit. The
remaining 2× is the second flush per commit on macOS, and it is the same with
no delete.

## Multi-page value churn (`free_run_churn`)

The defaults: 20,000 keys, 200 cycles, 2,000 rewrites per cycle, values of up
to 16 pages. Default flush. The table gives ZeroDB time ÷ LMDB time, each run
in its own process, with before and after alternating; load average 4 to 6.

| | runs | ZeroDB ÷ LMDB time | file pages ZeroDB ÷ LMDB |
|---|---|---|--:|
| before | 4 | 0.94, 1.15, 1.10, 0.96 | 1.063 |
| after, first version | 6 | 1.16, 1.32, 1.34, 1.38, 1.14, 1.15 | 1.059 |
| after, final | 2 | 0.99, 0.91 | 1.059 |

The first version regressed this workload. A profile showed the per-record
run search (`gc_reclaim`) walking from the first gated record on every
allocation: it re-decoded every record it had not drawn from, and it crossed
a leaf (and re-validated branch pages) every two records. One freeing txn is
now many records, so that walk grew. The final version keeps two per-txn
facts that hold because records only lose ids during a txn's operations: the
shortest run already missed in every record, and, per run length, the record
the last walk drew from, where the next walk starts. The picks do not change:
the file sizes are identical to the first version's.

## Free-page churn file size (`gc_churn_parity`)

`cargo test -p zerodb-oracle --test differential gc_churn_parity --
--nocapture`. Steady-state file size ÷ LMDB's (deterministic):

| workload | before | after |
|---|--:|--:|
| general | 0.677 (3,604,480 B) | 0.678 (3,608,576 B, one page more) |
| no overflow | 0.741 | 0.741 (identical) |

## Criterion rungs (`just bench-ab`)

`BASE=65cd4e2 ROUNDS=3 scripts/bench-ab.sh '^(put/gc/drain_big|commit/batch/)'`.

First version, load average 26 at the start, LMDB drift within 1.4 %:

| rung | LMDB | before | after | after ÷ before |
|---|--:|--:|--:|--:|
| `commit/batch/n1` | 18.594 ms | 19.352 ms | 18.779 ms | 0.982 (±0.030) |
| `commit/batch/n100` | 7.410 ms | 7.182 ms | 7.199 ms | 0.989 (±0.033) |
| `commit/batch/n10k` | 11.956 ms | 9.958 ms | 9.929 ms | 0.997 (±0.030) |
| `put/gc/drain_big` | 29.111 ms | 25.749 ms | 25.647 ms | 1.003 (±0.075) |

Final version: invalid twice, because LMDB drifted 4 to 10 %. The three-round
run (load average 19) gave after ÷ before 1.02 / 1.06 / 1.03 / 1.28 for the
four rungs, with intervals of ±0.11 to ±0.28. A five-round run of
`put/gc/drain_big` alone (load average 5) gave 1.04 (±0.33). Owed: a verdict
on a quiet host.
