# YCSB A/B/C matrix, bigger than RAM — bench server, Linux x86-64

_2026-09-30. rust-storage-bench (v1 + a ZeroDB backend), 10M items × 128 B
(JSON corpus), 2 GB memory cap (systemd scope, no swap), 60 s per run, one
run per engine, fresh data dir and dropped page cache before each. ZeroDB at
d8a2f79 (ADR-0018 included); LMDB = heed 0.22.1. System allocator (glibc)._

**Sync modes.** "no sync": LMDB and ZeroDB open with `NO_SYNC`, the others
commit without fsync. "sync" (`--sync`): every write commit is durable. The
harness itself notes LMDB's `NO_SYNC` is not quite the other engines' "no
sync commit".

## Result in one line

ZeroDB **could not run YCSB C at all**: its load was killed by the 2 GB cap,
in both modes and both validation policies. On A and B it trails LMDB by
1.7–3.3× without sync and is within 5 % of it with sync.

## YCSB C: ZeroDB killed by the memory cap during the load

C loads 10M items through one write txn in **unsorted** key order (`put`,
not `APPEND`; base64 keys), which builds a 2.1 GB tree (LMDB) instead of A/B's
1.5 GB. ZeroDB keeps every dirty page of a write txn on the heap until commit
(SPEC 04 §6.3); the kernel's memory-cgroup OOM killer stopped it at 2.09 GB
anonymous RSS, before the first measured operation. LMDB spills dirty pages to
the file once a txn holds ~131k of them (`mdb_page_spill`) and stays under the
cap. This is ADR-0017's subject (bounded dirty memory), now a hard failure
rather than a slowdown.

| engine | no sync ops/s | sync ops/s | read p50 | read p99 |
|---|---:|---:|---:|---:|
| LMDB | 307,318 | 307,876 | 550 ns | 112.5 µs |
| canopydb | 171,079 | 170,184 | 1.7 µs | 110.2 µs |
| redb | 142,786 | 140,941 | 2.5 µs | 151.8 µs |
| fjall 3 | 108,214 | 108,557 | 3.3 µs | 213.3 µs |
| ZeroDB | killed (load) | killed (load) | — | — |
| ZeroDB trusted | killed (load) | killed (load) | — | — |

(C is read-only after the load, so sync mode does not change it.)

## YCSB A (50 % reads, 50 % updates)

| engine | no sync ops/s | ÷ LMDB | sync ops/s | ÷ LMDB |
|---|---:|---:|---:|---:|
| LMDB | 286,275 | 1.00 | 145,922 | 1.00 |
| ZeroDB trusted | 170,437 | 0.60 | 139,861 | 0.96 |
| ZeroDB | 166,975 | 0.58 | 139,855 | 0.96 |
| fjall 3 | 151,157 | 0.53 | 138,847 | 0.95 |
| redb | 134,685 | 0.47 | 118,739 | 0.81 |
| canopydb | 132,538 | 0.46 | 116,598 | 0.80 |

| engine (no sync) | read p50 | read p99 | write p50 | write p99 | peak RSS | disk read |
|---|---:|---:|---:|---:|---:|---:|
| LMDB | 699 ns | 1.8 µs | 8.4 µs | 15.5 µs | 2,000 MiB | 103 MiB |
| ZeroDB | 2.6 µs | 204.9 µs | 18.2 µs | 235.7 µs | 2,027 MiB | 852 MiB |
| ZeroDB trusted | 1.4 µs | 200.9 µs | 16.8 µs | 231.0 µs | 2,028 MiB | 884 MiB |

## YCSB B (95 % reads, 5 % updates)

| engine | no sync ops/s | ÷ LMDB | sync ops/s | ÷ LMDB |
|---|---:|---:|---:|---:|
| LMDB | 632,941 | 1.00 | 154,921 | 1.00 |
| ZeroDB trusted | 196,853 | 0.31 | 147,264 | 0.95 |
| ZeroDB | 194,938 | 0.31 | 147,119 | 0.95 |
| redb | 182,422 | 0.29 | 126,387 | 0.82 |
| canopydb | 174,420 | 0.28 | 125,063 | 0.81 |
| fjall 3 | 144,604 | 0.23 | 143,315 | 0.93 |

| engine (no sync) | read p50 | read p99 | write p50 | write p99 | peak RSS | disk read |
|---|---:|---:|---:|---:|---:|---:|
| LMDB | 518 ns | 1.4 µs | 9.0 µs | 16.8 µs | 2,003 MiB | 267 MiB |
| ZeroDB | 1.1 µs | 121.8 µs | 21.8 µs | 154.9 µs | 2,034 MiB | 1,591 MiB |
| ZeroDB trusted | 925 ns | 124.3 µs | 20.1 µs | 154.9 µs | 2,033 MiB | 1,641 MiB |

## Reading

- **Without sync**, ZeroDB's gap to LMDB is disk reads: 6–8× more read from
  disk, and a read p99 of 120–205 µs (I/O latency) against LMDB's 1.4–1.8 µs.
  The load txn's dirty pages (~1.7 GB peak) stay in the process under glibc,
  leaving less of the 2 GB for the page cache (2026-09-29 diagnosis). Same
  cause as the C failure: ADR-0017.
- **With sync**, fsync dominates every engine's write path and ZeroDB is at
  0.95–0.96× LMDB, ahead of fjall 3, redb and canopydb on A and B.
- **Validation policy** hardly matters here (trusted within 1–2 %): with
  ADR-0018 the default policy's point reads are already close to the trusted
  path (B read p50 1.1 µs vs 925 ns), and I/O dominates the rest.
- Against the other engines, ZeroDB is first or second behind LMDB on A and B
  in both modes.

Raw JSONL, per-task HTML reports and logs are on the bench server under
`results/ycsb-matrix-20260930T061358Z/`; the script is `ycsb-matrix.sh` in
the bench-server scripts.
