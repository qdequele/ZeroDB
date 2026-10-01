# Bounded dirty memory: spilling (ADR-0017) — bench server, Linux x86-64

_2026-09-30. Base `2753faa` vs the ADR-0017 change. LMDB = heed 0.22.1.
System allocator (glibc)._

## What changed

A ZeroDB write txn used to keep every page it touched in memory until
commit. A txn larger than RAM could not finish, and after a large txn glibc
kept that memory, leaving less for the page cache. Now, past the dirty limit
(LMDB's 131,072 pages by default, 512 MiB at 4 KiB pages), the txn writes its
highest-numbered dirty pages to the file and releases them, as LMDB's
`mdb_page_spill` does. A page the txn writes again is read back in place.
Results and committed files do not depend on it. Design: ADR-0017; spec:
SPEC 04 §6.3a (TXN-68..72); option `EnvOpenOptions::max_dirty_bytes` (D-021).

## Acceptance: rust-storage-bench, 10M × 128 B, 2 GB cap, 60 s

Same method as `2026-09-30-ycsb-matrix.md` (no sync), with the cgroup's
anonymous memory sampled every 5 s.

| workload | engine | ops/s | ÷ LMDB | read p50 | read p99 | peak anon | disk read |
|---|---|---:|---:|---:|---:|---:|---:|
| YCSB C | LMDB | 310,614 | 1.00 | 573 ns | 112.5 µs | 520 MiB | 1,559 MiB |
| YCSB C | ZeroDB before | killed by the cap during the load | — | — | — | > 2,090 MiB | — |
| YCSB C | **ZeroDB after** | **268,758** | **0.87** | 1.0 µs | 112.5 µs | **539 MiB** | 1,472 MiB |
| YCSB A | LMDB | 291,606 | 1.00 | 699 ns | 1.9 µs | 519 MiB | 98 MiB |
| YCSB A | ZeroDB before | 166,975 | 0.58 | 2.6 µs | 204.9 µs | ~1.6 GB | 852 MiB |
| YCSB A | **ZeroDB after** | **221,456** | **0.76** | 2.3 µs | **4.1 µs** | **532 MiB** | 80 MiB |
| YCSB B | LMDB | 628,080 | 1.00 | 508 ns | 1.4 µs | 519 MiB | 266 MiB |
| YCSB B | ZeroDB before | 194,938 | 0.31 | 1.1 µs | 121.8 µs | ~1.6 GB | 1,591 MiB |
| YCSB B | **ZeroDB after** | **433,813** | **0.69** | 963 ns | **3.1 µs** | **534 MiB** | 263 MiB |

("Before" rows: the 2026-09-30 matrix at `2753faa`, and the 2026-09-29
diagnosis for the anonymous peak.)

The load's dirty set now stays near the limit, as LMDB's does, so the rest of
the 2 GB serves as page cache: disk reads fall to LMDB's level and read p99
drops from ~120–200 µs (I/O) to a few µs.

YCSB C's file is 2,322 MiB against LMDB's 2,155 MiB. That is not spilling —
the same 1M-key unsorted load gives a byte-identical page layout with and
without it (229 MiB, same high-water, 0 free pages, after 109 spills) — but
ZeroDB's page fill under random-order inserts, a separate item.

## Engine ladder: nothing below the limit changes

- **codegen-units=1, 3 rounds, full ladder:** flat except rungs later shown
  to be noise or fixed: `del/cursor/drain` / `del/range/half` flat on a
  3-round re-run (0.994 / 1.012); `del/clear/all` +5 % fixed (committed
  leaves now answer on the range test before the spilled-set lookup, 1.001);
  a page-count division on every freed page replaced by a shift (`del/*`
  0.968–1.028). `scan/meta/len` (reads only the DB record, no changed code)
  lands on discrete values across builds — 64.1, 70.2, 76.2 µs — and is
  1.000 on the final build.
- **codegen-units=16:** 1 round of the full ladder, flagged rungs re-run with
  3 rounds: all flat (`del/*` 0.963–1.006) except `scan/meta/len` +3.2 %
  (94.48 → 97.52 µs), the same alignment step as above.
- A first version resolved spilled pages through an extra lookup inside the
  force-inlined page resolution; it grew every descent and cost read-only
  rungs 3–13 % (`scan/edge/first_last` +13 %). The final version leaves that
  hot path byte-identical: spilled pages resolve through the ordinary map
  path under a raised read bound, and each spill resets the writer's memo
  (SPEC 04 TXN-71).

## Correctness

- `zerodb/tests/dirty_spill.rs` (7): identical results under a tiny limit and
  the default (heap and `WRITE_MAP`, across reopen); dirty set bounded through
  a 60k-put txn; abort after spilling; `clear`/`drop_db`/`put_reserved`/
  `create_database` in a spilled txn; no spill under the limit. Mutation
  checks: skipping the spill's write crashes the suite; removing the
  spilled-leaf arm of `clear`'s collection fails it.
- Crash harness: tiny limits and a bulk phase on 1/3 of cycles; 10,035 cycles
  verified, 0 violations, 3,114 spilling txns under cuts (431 image, 2,683
  SIGKILL). Under `NO_META_SYNC` spilling widens the known reclaim-clobber
  window (SPEC 06 REC-10), as with LMDB: one seed, same workload, 5 stale
  fallbacks without spilling vs 48 with it; the default mode is immune.
- `just stress` 180 s with a new spilling-writer variant: 2,702 commits, all
  spilled, 6,062 reader double-walks clean.
- Local gate (fmt, clippy, test, miri, fuzz-quick) green; spec review found no
  blocker (its findings are fixed).
