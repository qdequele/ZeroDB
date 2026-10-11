# Multi-process lock file (ADR-0023) stage 1: single-process cost and Linux runs (2026-10-11)

Stage 1 of ADR-0023 tests the riskiest assumption first. That assumption has two
parts:

1. **Cross-process visibility and handoff work.** A commit in one process is seen
   by another, a foreign reader holds back page reuse, writers in two processes
   exclude each other, and a killed writer's lock is taken over. This must hold on
   Linux (futex writer lock) and on macOS (`fcntl` writer lock).
2. **The single-process cost is acceptable.** `env/txn/*` and `get/*` should stay
   within noise, and `commit/batch/n1` within a few percent.

The lock file is opt-in (`EnvOpenOptions::lock_file(true)`, off by default), so
this run also checks that the default path did not regress.

## Verdict

- **Part 1 holds.** All 7 two-process tests pass 5 times out of 5 on Linux
  x86-64, and they pass on macOS aarch64. The Linux futex path compiles with
  clippy clean.
- **Part 2 holds for reads only.** Every `get/*` rung is flat in both modes.
  The transaction rungs are not within noise:
  - With the lock file on, `env/txn/ro_begin_abort` is +23% (+19 ns per read
    txn), `env/txn/rw_empty_commit` is +19% (+40 ns per write txn) and
    `commit/batch/n1` is +5.4% (+0.5 µs per commit). That is above "a few
    percent".
  - **The default path (lock file off) also regressed:** `ro_begin_abort` is
    +4.5% and `rw_empty_commit` is +9.4%. `commit/batch/n1` (+2.1%) is within
    noise.
  - Each of these regressions shows in all 5 rounds, and LMDB stays put, so it
    is real (per-round numbers below).

  ZeroDB still beats LMDB on read-txn begin/abort in both modes (0.61× → 0.75×
  with the lock file on). The empty-commit gap to LMDB grows from 2.78× to 3.32×.

## Setup

| | |
|---|---|
| Host | bench server: Xeon E3-1230 v2 (4C/8T), turbo off, `performance` governor, Debian 12, kernel 6.1, x86-64, 4 KiB pages |
| Toolchain | rustc 1.98.1 |
| Build | criterion bench profile, `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1` (how Meilisearch builds), as for ADR-0022 |
| LMDB | heed 0.22.1 (the Meilisearch LMDB fork), measured in every run as the drift control |
| ZeroDB before | `7002d6e` (main, the branch's merge base) |
| ZeroDB after | `ca27d00` (`qdequele/adr-0023-multi-process`, tasks 1–7) |
| Rungs | `env/txn/*`, `get/*`, `commit/batch/n1`, `commit/batch/n100`, `commit/sync/*` |
| Rounds | 5 interleaved rounds; each round rotates the starting side |

**Why bench-ab was not used as-is.** `scripts/bench-ab.sh` copies the working
tree's bench harness onto the base tree. The branch's harness calls
`EnvOpenOptions::lock_file`, which `main` does not have, so the "before" side
fails to compile. The two harnesses differ only in that 6-line lock-file axis
(`ZERODB_BENCH_LOCK_FILE`), so the run used a three-sided driver instead:

- "before" is built from `main` with its own harness, in its own target dir.
- "after" is the branch build, run twice per round: once with
  `ZERODB_BENCH_LOCK_FILE=1` (lock on) and once without it (lock off).
- The rounds are fed to `scripts/bench-ab.py` unchanged, once per after-variant.
  Both variants are compared against the same "before" measurements.

**Checks that the axis really took effect:**

- The "before" binary has 0 `zerodb_io::lockfile` symbols. The "after" binary
  has 24.
- Under `strace`, the after binary with `ZERODB_BENCH_LOCK_FILE=1` opens
  `lock.mdb` (mode 0666, the heed adapter's lock-file name) in each ZeroDB env
  directory. Without the variable it opens none. The only other `lock.mdb` opens
  are LMDB's own, with mode 0600.

## Results

`÷ LMDB` is ZeroDB time divided by LMDB time (below 1 means faster than LMDB).
`on ÷ before` and `off ÷ before` are the median over rounds of after-time divided
by before-time, followed by the noise threshold and bench-ab's per-rung state.
"drift" means LMDB moved more than 3% between halves.

| rung | LMDB | ZeroDB before | after, lock on | after, lock off | ÷ LMDB: before → on / off | on ÷ before | off ÷ before |
|---|---:|---:|---:|---:|---:|---:|---:|
| `commit/batch/n1` | 13.476 ms | 19.732 ms | 20.776 ms | 20.122 ms | 1.46× → 1.54× / 1.49× | 1.054 ±0.030 (**slower**) | 1.021 ±0.032 (flat) |
| `commit/batch/n100` | 12.300 ms | 12.462 ms | 12.540 ms | 12.421 ms | 1.01× → 1.02× / 1.01× | 1.005 ±0.055 (flat) | 0.998 ±0.048 (flat) |
| `commit/sync/n1` | 303.613 ms | 275.882 ms | 283.348 ms | 281.391 ms | 0.91× → 0.93× / 0.93× | 1.014 ±0.077 (drift) | 0.966 ±0.122 (drift) |
| `commit/sync/n100` | 580.661 ms | 571.121 ms | 570.465 ms | 573.607 ms | 0.98× → 0.98× / 0.98× | 1.023 ±0.116 (flat) | 1.013 ±0.138 (flat) |
| `env/txn/ro_begin_abort` | 1.353 ms | 825.7 µs | 1.013 ms | 863.1 µs | 0.61× → 0.75× / 0.69× | 1.227 ±0.030 (drift¹) | 1.045 ±0.030 (**slower**) |
| `env/txn/rw_empty_commit` | 74.6 µs | 207.8 µs | 247.9 µs | 227.2 µs | 2.78× → 3.32× / 3.05× | 1.194 ±0.068 (**slower**) | 1.094 ±0.035 (**slower**) |
| `get/access/hot` | 1.802 ms | 1.379 ms | 1.413 ms | 1.399 ms | 0.76× → 0.78× / 0.78× | 1.027 ±0.030 (flat) | 1.017 ±0.030 (flat) |
| `get/access/miss` | 1.266 ms | 1.226 ms | 1.245 ms | 1.251 ms | 0.97× → 0.98× / 0.99× | 1.015 ±0.030 (flat) | 1.021 ±0.030 (flat) |
| `get/access/rand` | 3.567 ms | 3.354 ms | 3.369 ms | 3.369 ms | 0.94× → 0.94× / 0.94× | 1.008 ±0.061 (flat) | 1.004 ±0.034 (flat) |
| `get/access/seq` | 1.872 ms | 1.663 ms | 1.681 ms | 1.683 ms | 0.89× → 0.90× / 0.90× | 1.010 ±0.030 (flat) | 1.012 ±0.030 (flat) |
| `get/db/named` | 3.541 ms | 3.345 ms | 3.365 ms | 3.372 ms | 0.94× → 0.95× / 0.95× | 1.006 ±0.047 (flat) | 1.012 ±0.030 (flat) |
| `get/db/named_x8` | 3.270 ms | 3.247 ms | 3.228 ms | 3.243 ms | 0.99× → 0.99× / 0.99× | 0.993 ±0.062 (flat) | 0.994 ±0.062 (flat) |
| `get/db/root` | 3.542 ms | 3.321 ms | 3.333 ms | 3.335 ms | 0.94× → 0.94× / 0.94× | 1.005 ±0.030 (flat) | 1.003 ±0.030 (flat) |
| `get/key/k128` | 4.810 ms | 5.455 ms | 5.494 ms | 5.496 ms | 1.13× → 1.14× / 1.14× | 1.007 ±0.030 (flat) | 1.007 ±0.030 (flat) |
| `get/key/k32` | 3.865 ms | 4.408 ms | 4.402 ms | 4.417 ms | 1.14× → 1.14× / 1.14× | 0.998 ±0.030 (flat) | 0.997 ±0.030 (flat) |
| `get/key/k8` | 3.634 ms | 3.480 ms | 3.424 ms | 3.429 ms | 0.96× → 0.94× / 0.95× | 0.995 ±0.040 (flat) | 0.979 ±0.047 (flat) |
| `get/size/n1k` | 1.771 ms | 1.639 ms | 1.635 ms | 1.638 ms | 0.93× → 0.92× / 0.92× | 0.994 ±0.030 (flat) | 0.999 ±0.030 (flat) |
| `get/size/n50k` | 3.546 ms | 3.424 ms | 3.381 ms | 3.362 ms | 0.97× → 0.95× / 0.95× | 1.002 ±0.052 (flat) | 0.990 ±0.038 (flat) |
| `get/val/v256` | 2.540 ms | 2.378 ms | 2.382 ms | 2.378 ms | 0.94× → 0.94× / 0.94× | 0.997 ±0.030 (flat) | 1.000 ±0.030 (flat) |
| `get/val/v2page` | 2.302 ms | 2.885 ms | 2.888 ms | 2.887 ms | 1.25× → 1.25× / 1.25× | 1.001 ±0.030 (flat) | 1.001 ±0.030 (flat) |
| `get/val/v2page_touch` | 3.229 ms | 3.698 ms | 3.684 ms | 3.678 ms | 1.15× → 1.14× / 1.14× | 0.999 ±0.030 (flat) | 1.000 ±0.030 (flat) |
| `get/val/v4k` | 2.241 ms | 2.876 ms | 2.873 ms | 2.878 ms | 1.28× → 1.28× / 1.28× | 0.999 ±0.030 (flat) | 1.001 ±0.030 (flat) |
| `get/val/v4k_touch` | 3.162 ms | 3.635 ms | 3.631 ms | 3.627 ms | 1.15× → 1.15× / 1.15× | 0.999 ±0.030 (flat) | 0.995 ±0.030 (flat) |
| `get/val/v8` | 2.211 ms | 1.963 ms | 1.957 ms | 1.959 ms | 0.89× → 0.88× / 0.89× | 0.999 ±0.030 (flat) | 0.997 ±0.030 (flat) |
| `get/val/v8_touch` | 2.213 ms | 1.969 ms | 1.968 ms | 1.966 ms | 0.89× → 0.89× / 0.89× | 0.999 ±0.030 (flat) | 0.999 ±0.030 (flat) |

¹ In this run LMDB drifted +7% on `ro_begin_abort` between halves. ZeroDB moved
+22% to +25% in every round, which is far beyond that drift.

bench-ab.py's run verdict is `regressed` for both variants. Lock on: 2 rungs
regressed, 21 flat, 2 drift. Lock off: 2 regressed, 22 flat, 1 drift.

### Flagged rungs, per round (ZeroDB after ÷ before; LMDB after ÷ before)

| rung | variant | rounds 1–5, ZeroDB | rounds 1–5, LMDB |
|---|---|---|---|
| `env/txn/ro_begin_abort` | lock on | 1.220 1.227 1.245 1.214 1.247 | 0.984 1.062 1.089 1.079 1.074 |
| `env/txn/ro_begin_abort` | lock off | 1.040 1.045 1.059 1.037 1.058 | 0.890 1.087 0.997 1.101 1.004 |
| `env/txn/rw_empty_commit` | lock on | 1.208 1.183 1.194 1.263 1.179 | 0.942 1.073 1.002 0.994 1.001 |
| `env/txn/rw_empty_commit` | lock off | 1.109 1.074 1.094 1.104 1.070 | 1.091 0.990 0.991 0.991 0.993 |
| `commit/batch/n1` | lock on | 1.032 1.053 1.060 1.059 1.054 | 0.992 0.906 0.997 0.989 0.968 |
| `commit/batch/n1` | lock off | 1.035 1.018 0.992 1.021 1.024 | 0.977 0.914 0.980 0.994 0.977 |

### Per operation

Each `ro_begin_abort` iteration runs 10,000 read txns, each `rw_empty_commit`
iteration 1,000 empty write txns, and each `commit/batch/n1` iteration 2,000
single-put commits.

| rung | LMDB | before | lock off | lock on |
|---|---:|---:|---:|---:|
| read txn begin + abort | 135 ns | 83 ns | 86 ns (+4) | 101 ns (+19) |
| empty write txn | 75 ns | 208 ns | 227 ns (+19) | 248 ns (+40) |
| single-put commit | 6.7 µs | 9.9 µs | 10.1 µs (+0.2) | 10.4 µs (+0.5) |

### Where the cost likely comes from (not yet verified)

These are leads for whoever takes up the regression. They were not tested in
this run.

- **Default path.** In the "after" binary, `ReaderTable::claim` is now a
  separate, non-inlined function; on `main` it is inlined into the
  read-txn path (checked with `nm`). The reason: the reader table now chooses
  between local and shared slots with a match on every slot access (in `claim`,
  `oldest` and `release`), where `main` iterated a boxed slice directly. This is
  the "enum per reader slot" cost the task-5 review flagged as a deferred minor,
  and it is now measured.
- **Lock file on.** Each slot access goes through a `dyn SharedRegion` virtual
  call plus a bounds `assert!`. The writer's `oldest()` scan pays that once per
  slot (126 by default) on every write txn. `claim` and `release` add a PID store
  each.
- **Unexplained.** `commit/batch/n1` with the lock on (+0.5 µs per commit) costs
  more than the empty-commit delta (+40 ns), so a single-put commit has more
  lock-file cost than slot scanning alone.

A `perf record` of the two `env/txn` rungs was dominated by criterion's own
analysis (sorting samples), so it did not attribute the delta further.

## Linux and macOS test runs

Bench server, Linux 6.1 x86-64, run natively. This is the first time the futex
writer-lock path was compiled:

| command | result |
|---|---|
| `cargo fmt --all -- --check` | ok |
| `cargo clippy --workspace --all-targets -- -D warnings` | ok (checks `zerodb-io` with the Linux-only modules) |
| `cargo test -p zerodb-io` | 6 passed, including the Linux-only `stale_writer_word_with_own_pid_is_taken_over` |
| `cargo test -p zerodb --test multi_process -- --test-threads=1`, 5 runs | 7/7 passed in each run (35/35) |
| `cargo test -p zerodb --test lock_file_single_process` | 4 passed |
| `cargo test --workspace` | 694 passed, 0 failed, 106 suites |

Local Mac (macOS aarch64): fmt, clippy (host target), `cargo test --workspace`
(693 passed, including the 7 two-process tests on the `fcntl` writer lock), miri
on `zerodb-core` (226 passed), `just loom` (9 passed), `just fuzz-quick`
(`diff_ops` 600 s and `fuzz_image_open` 120 s, no crash) and
`just crash-test-quick` (229 cycles verified). All green.

**Not covered:**

- Linux aarch64: no Graviton host was up (the AWS session had expired), so the
  futex path is untested on a weak memory model.
- The brief's `clippy --target x86_64-unknown-linux-gnu` on the Mac: that target
  is not installed there. Native Linux clippy above replaces it.

## Reproduce

```bash
# on the bench server, tree synced at ca27d00, tune.sh on
export CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
F='^(env/txn/|get/|commit/batch/n1/|commit/batch/n100/|commit/sync/)'
# "before" = main (7002d6e) with its own harness, in its own target dir;
# "after" = this branch; per round run before / after+ZERODB_BENCH_LOCK_FILE=1 /
# after without it, rotating the order; then lay out <run>/round-N/{before,after}
# for each after-variant and run scripts/bench-ab.py <run> --target "$F".
```
