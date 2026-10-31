# Validated-pages cache sized to the file: the Graviton gate — 2026-10-31

- **Question:** the env-wide validated-pages cache (ADR-0018) had 64 Ki
  slots. On a 10M-key file of ~325k pages most random reads found their
  leaf's slot taken by another page and walked the leaf's cells again
  (`validate_leaf_miss` + `validate_branch_miss` 5.8 % of cycles in the
  2026-10-29 Graviton4 profile). Does giving every page its own slot (one
  8-byte word per page, mapped by pgno up to 2^23 pages; ADR-0018 amendment
  2026-10-31) remove that cost, and at what memory and ladder cost?
- **ZeroDB:** before = `3764c8c` (64 Ki seqlock slots), after = `4a806e9`
  (one-word slots). The trusted column is the branch tip `6db6c07` opened
  with `FileTrust::trust_contents` (ADR-0014): no validation and no cache,
  the floor for what removing the walk can give.
- **LMDB:** the Meilisearch fork through heed 0.22.1.
- **Host:** AWS c8g.xlarge, Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB, Ubuntu
  24.04, kernel 7.0.0-1014-aws, 4 KiB OS pages, 60 GiB gp3 root, rustc
  1.99.1, workspace release profile. Launched for this run and shut down
  after it. Nothing else ran on it.

## Census: `ycsb_a_census`, 10M keys

`crates/zerodb-oracle/examples/ycsb_a_census.rs`: the public suite's YCSB A
shape (10M ascending 16-byte big-endian `u128` keys loaded in one write
txn, 100-byte values, `NO_SYNC | NO_READ_AHEAD`, default 4 KiB pages), the
data file read once through the page cache, then 30 s of uniformly random
operations with one txn per operation. Page cache dropped before each run;
two reps, engine order reversed in the second. Data file 1.33 GB for both
engines.

| p50, rep 1 / rep 2 | LMDB | ZeroDB before | ZeroDB after | ZeroDB trusted |
|---|---:|---:|---:|---:|
| `read`: get | 961 / 1,075 ns | 1,323 / 1,363 ns | **1,232 / 1,236 ns** | 1,119 / 1,127 ns |
| `mix`: get | 1,127 / 1,133 ns | 1,571 / 1,512 ns | **1,390 / 1,388 ns** | 1,273 / 1,279 ns |
| `mix`: update | 7.9 / 7.7 µs | 14.1 / 13.0 µs | **12.2 / 12.7 µs** | 12.8 / 12.5 µs |
| `read`: get p99 | 1,278 / 1,491 ns | 1,896 / 1,892 ns | **1,593 / 1,610 ns** | 1,508 / 1,517 ns |

Read p50 is 7–9 % lower and p99 15 % lower. The read gap to the trusted
floor halves: 204–236 ns before, 109–113 ns after. In `mix` the gap to
trusted goes from 233–298 ns to 109–117 ns. Update p50 does not get worse
(12.2–12.7 µs against 13.0–14.1 µs).

`mix` throughput is not comparable between runs: as in the 2026-10-29
profile, the kernel's dirty-page throttling on gp3 decides it and varies run
to run (ZeroDB-before rep 1 ran at 4.9k ops/s with 9 ms update p99). The
p50s and the `read` runs, which write nothing, are the stable figures.

### Where a read's cycles go (`perf stat`, `read`, rep 1, 25 s)

| per get | LMDB | before | after |
|---|---:|---:|---:|
| instructions | 3,652 | 6,037 | **4,155** |
| cycles | 2,804 | 3,869 | **3,583** |
| cache misses | 38.0 | 48.8 | **44.2** |
| dTLB load misses | 4.3 | 7.1 | **6.7** |

The cell walk is gone: instructions per get drop 31 % and land within 14 %
of LMDB's. Cycles drop only 7 %, because a random get is bound by memory,
not instructions. What separates "after" from trusted (~110 ns) is the
probe itself: one load into a 2.5 MiB table at a random slot, typically an
L2 miss and often a TLB miss with 4 KiB pages. The remaining gap to LMDB
is memory stalls beyond the cache, not validation.

## Memory

8 bytes per page in use, in 32 KiB chunks allocated where publishes land,
plus 1.5 KiB per directory block of 256 Ki pages and a 768-byte top array
made at open. This file has 324,841 pages: 80 chunks and 2 directory
blocks, **2.50 MiB** if every chunk is touched (all leaves are, under
uniform random reads), against 2.0 MiB for the 64 Ki-slot table, which held
only a fifth of the pages. The ceiling is 64 MiB per env (files of 2^23
pages and more).

## Ladder: `scripts/bench-ab.sh '^(get/|scan/|seek/|env/)'`, 3 rounds

Verdict **improved**: 3 faster, 0 regressed, 34 flat, 1 unreliable (LMDB
drift > 3 %).

| rung | LMDB | before | after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---|
| `scan/full/big` | 17.091 ms | 27.877 ms | 24.251 ms | 0.870 (±0.038) | 1.010 | faster |
| `get/db/named` | 3.308 ms | 3.183 ms | 2.920 ms | 0.928 (±0.040) | 0.978 | faster |
| `env/txn/ro_get_one` | 4.827 ms | 7.096 ms | 6.812 ms | 0.960 (±0.030) | 0.996 | faster |
| `get/db/root` | 3.307 ms | 3.084 ms | 2.975 ms | 0.922 (±0.136) | 1.036 | drift |
| `get/access/rand` | 3.324 ms | 3.157 ms | 3.070 ms | 0.972 (±0.030) | 0.990 | flat |
| `env/open/reopen` | 807.135 µs | 726.306 µs | 712.426 µs | 0.983 (±0.030) | 1.015 | flat |
| `env/txn/ro_begin_abort` | 1.034 ms | 778.177 µs | 788.481 µs | 1.014 (±0.030) | 1.028 | flat |
| `scan/full/fwd` | 480.828 µs | 699.458 µs | 689.696 µs | 0.984 (±0.030) | 1.003 | flat |
| `scan/full/rev` | 483.404 µs | 678.908 µs | 684.417 µs | 1.009 (±0.030) | 1.000 | flat |

The other 29 rungs are flat. `scan/full/big` gains because its file
outgrows 64 Ki pages, and because eight slots now share a cache line where
two did. `env/open/reopen` and `env/txn/ro_begin_abort`, the at-open and
per-txn costs, are flat.

## Checks

On the branch: `cargo fmt`, `cargo clippy --workspace --all-targets` and the
Linux-target clippy, `cargo test --workspace`, `cargo +nightly miri test -p
zerodb-core`, `just loom` (with an isolated cargo home; the rewritten
`loom_stamp_cache_never_mixes_publishes` passes), the crash smoke and
`fuzz-quick` all pass on macOS aarch64.
