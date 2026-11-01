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

> **Update, same day: after the fix (section "After the fix" below).** The
> default-path regression is gone: every rung is flat against `main` with the
> lock file off. With the lock file on, the residual cost is +15 ns per read txn
> (`ro_begin_abort` +19%), about +20 ns per empty write txn (+10%, within this
> run's noise) and nothing measurable on `commit/batch/*`. The numbers directly
> below are the original run, kept as they were measured.

## Verdict (original run, before the fix)

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
| ZeroDB after | `ca27d00` (a feature branch, tasks 1–7) |
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

## After the fix (`2b53e88`)

### What changed

Two commits on top of `ca27d00`; there is no protocol change.

- **`5f9b238` — reader slots resolved once per operation.**
  - `zerodb_core::shared::ReaderSlot` is a `repr(C, align(64))` slot: txnid
    `u64` at +0, owner PID `u32` at +8, padded to 128 bytes. That is the lock
    file's slot stride, and compile-time asserts pin the size, alignment and
    field offsets.
  - `SharedRegion` hands out the whole table as `&[ReaderSlot]` instead of one
    virtual call per slot. `zerodb-io` builds the slice over the mapping with
    a single `slice::from_raw_parts`, which has a `// SAFETY:` comment.
  - The local table is `Box<[ReaderSlot]>`, which replaces `CachePadded`, so
    `crossbeam-utils` leaves `zerodb-core`.
  - `claim`, `release`, `store_pin`, `oldest` and the introspection scans
    resolve the slice once and loop directly. `ReaderTable::claim` is inlined
    again, as on `main` (checked with `nm`).
  - loom builds model the table over a local slot type with loom atomics.
- **`2b53e88` — lock-file arms of the write-txn path moved out of line.**
  `lock_writer_shared` and `current_snapshot` branch inline and call
  `#[inline(never)]` helpers for the lock-file case. Without this,
  `write_txn` grew enough that `DirtyStore::with_spare` stopped being inlined.

### Per-transaction syscalls with the lock file on: none

`strace -c -f` of `commit/batch/n1` (142,504 commits in the traced run) and of
`env/txn/rw_empty_commit` shows the same per-commit syscalls with the lock
file on and off: the `pwrite64` count is identical, and there are 37 `pread64`
calls in both modes, so meta copies are not read per transaction.

The lock file adds only per-env-open calls: `fcntl` (+315), `futex` (+171) and
`ftruncate`. The cached-snapshot fast path in `snapshot_at` is taken when `T`
is unchanged, because the committing process re-caches its own snapshot.

The remaining lock-on cost is therefore CPU, from:

- two locked read-modify-write operations on the futex writer word per write
  txn;
- the `SeqCst` store of `T`;
- the owner-PID stores;
- one virtual call per operation for the slot slice and for `T`.

### Instruction counts per operation

Measured with a micro loop outside the repo: heed API, `NO_SYNC`, x86-64,
`perf stat -e instructions:u`. Instruction counts are exact; the cycle counts
of the same loop moved ±5% between repeats, so they are not shown.

| loop | main | lock off, fixed | lock on, fixed | lock off, `ca27d00` | lock on, `ca27d00` |
|---|---:|---:|---:|---:|---:|
| read txn begin + abort | 376 | 397 (+21) | 481 (+105) | 387¹ | 471¹ |
| empty write txn | 1,307 | 1,336 (+29) | 1,478 (+171) | 1,362 (+54) | 1,458 |
| single-put commit | 14,847 | 14,926 (+79) | 15,092 (+245) | 14,952 (+105) | 15,072 |

¹ Measured in an earlier session of the same loop in which `main` read 367;
compare these with each other, not with the fixed columns.

### Bench after the fix

Same three-sided driver, filter, `CODEGEN_UNITS=1` and 5 rounds as the
original run, on `2b53e88`. The after binary contains `lock_writer_region` and
`current_snapshot_shared` (both new in `2b53e88`) and 22 `zerodb_io::lockfile`
symbols, and `ReaderTable::claim` is no longer a separate symbol. This run was
noisier than the original: LMDB moved 11–18% between halves in some rounds of
the `env/txn` rungs.

| rung | LMDB | ZeroDB before | after, lock on | after, lock off | ÷ LMDB: before → on / off | on ÷ before | off ÷ before |
|---|---:|---:|---:|---:|---:|---:|---:|
| `commit/batch/n1` | 13.514 ms | 20.009 ms | 20.132 ms | 20.020 ms | 1.48× → 1.49× / 1.47× | 1.014 ±0.082 (flat) | 1.001 ±0.066 (flat) |
| `commit/batch/n100` | 12.314 ms | 12.471 ms | 12.506 ms | 12.474 ms | 1.01× → 1.02× / 1.01× | 1.007 ±0.037 (flat) | 1.002 ±0.030 (flat) |
| `commit/sync/n1` | 297.336 ms | 282.676 ms | 287.205 ms | 278.724 ms | 0.95× → 0.97× / 0.93× | 1.029 ±0.142 (flat) | 1.004 ±0.194 (flat) |
| `commit/sync/n100` | 584.311 ms | 567.780 ms | 554.958 ms | 612.817 ms | 0.97× → 0.95× / 1.05× | 0.970 ±0.156 (drift) | 1.025 ±0.183 (drift) |
| `env/txn/ro_begin_abort` | 1.257 ms | 815.2 µs | 966.3 µs | 831.2 µs | 0.65× → 0.77× / 0.63× | 1.186 ±0.164 (**slower**) | 1.007 ±0.182 (flat) |
| `env/txn/rw_empty_commit` | 75.4 µs | 220.0 µs | 240.1 µs | 217.2 µs | 2.92× → 3.18× / 2.86× | 1.102 ±0.108 (flat) | 1.000 ±0.040 (flat) |
| `get/access/hot` | 1.803 ms | 1.377 ms | 1.400 ms | 1.401 ms | 0.76× → 0.78× / 0.78× | 1.008 ±0.030 (flat) | 1.019 ±0.030 (flat) |
| `get/access/miss` | 1.267 ms | 1.228 ms | 1.251 ms | 1.247 ms | 0.97× → 0.99× / 0.98× | 1.021 ±0.030 (flat) | 1.012 ±0.030 (flat) |
| `get/access/rand` | 3.570 ms | 3.386 ms | 3.508 ms | 3.410 ms | 0.95× → 0.98× / 0.95× | 1.022 ±0.080 (flat) | 0.994 ±0.033 (flat) |
| `get/access/seq` | 1.865 ms | 1.669 ms | 1.670 ms | 1.666 ms | 0.90× → 0.90× / 0.89× | 1.001 ±0.030 (flat) | 1.001 ±0.030 (flat) |
| `get/db/named` | 3.569 ms | 3.358 ms | 3.375 ms | 3.383 ms | 0.94× → 0.95× / 0.95× | 1.004 ±0.093 (flat) | 1.002 ±0.038 (flat) |
| `get/db/named_x8` | 3.302 ms | 3.328 ms | 3.355 ms | 3.294 ms | 1.01× → 1.02× / 1.00× | 0.994 ±0.102 (flat) | 0.992 ±0.065 (flat) |
| `get/db/root` | 3.557 ms | 3.426 ms | 3.335 ms | 3.341 ms | 0.96× → 0.94× / 0.94× | 0.978 ±0.044 (flat) | 0.977 ±0.036 (flat) |
| `get/key/k128` | 4.834 ms | 5.472 ms | 5.494 ms | 5.499 ms | 1.13× → 1.14× / 1.14× | 1.003 ±0.030 (flat) | 1.000 ±0.049 (flat) |
| `get/key/k32` | 3.869 ms | 4.397 ms | 4.410 ms | 4.447 ms | 1.14× → 1.14× / 1.15× | 1.003 ±0.030 (flat) | 1.009 ±0.067 (flat) |
| `get/key/k8` | 3.710 ms | 3.510 ms | 3.565 ms | 3.438 ms | 0.95× → 0.96× / 0.95× | 1.001 ±0.045 (flat) | 0.979 ±0.088 (drift) |
| `get/size/n1k` | 1.773 ms | 1.641 ms | 1.629 ms | 1.632 ms | 0.93× → 0.92× / 0.92× | 0.992 ±0.030 (flat) | 0.994 ±0.030 (flat) |
| `get/size/n50k` | 3.681 ms | 3.428 ms | 3.530 ms | 3.390 ms | 0.93× → 0.96× / 0.94× | 1.021 ±0.070 (flat) | 1.003 ±0.075 (drift) |
| `get/val/v256` | 2.542 ms | 2.383 ms | 2.357 ms | 2.371 ms | 0.94× → 0.93× / 0.93× | 0.989 ±0.030 (flat) | 0.995 ±0.030 (flat) |
| `get/val/v2page` | 2.297 ms | 2.912 ms | 2.877 ms | 2.878 ms | 1.27× → 1.25× / 1.25× | 0.995 ±0.030 (flat) | 0.991 ±0.030 (flat) |
| `get/val/v2page_touch` | 3.234 ms | 3.706 ms | 3.663 ms | 3.662 ms | 1.15× → 1.13× / 1.13× | 0.988 ±0.030 (flat) | 0.991 ±0.030 (flat) |
| `get/val/v4k` | 2.248 ms | 2.878 ms | 2.868 ms | 2.862 ms | 1.28× → 1.28× / 1.27× | 0.992 ±0.030 (flat) | 0.995 ±0.030 (flat) |
| `get/val/v4k_touch` | 3.173 ms | 3.652 ms | 3.633 ms | 3.618 ms | 1.15× → 1.14× / 1.14× | 0.996 ±0.030 (flat) | 0.991 ±0.030 (flat) |
| `get/val/v8` | 2.230 ms | 1.977 ms | 1.967 ms | 1.965 ms | 0.89× → 0.88× / 0.88× | 0.992 ±0.030 (flat) | 0.994 ±0.030 (flat) |
| `get/val/v8_touch` | 2.216 ms | 1.972 ms | 1.949 ms | 1.946 ms | 0.89× → 0.88× / 0.88× | 0.990 ±0.030 (flat) | 0.987 ±0.030 (flat) |

bench-ab.py's run verdicts:

- **Lock off: `flat`.** 22 rungs flat, 3 drift, none regressed.
- **Lock on: `regressed`, on `ro_begin_abort` only.** 23 rungs flat, 1 drift.

Per round (ZeroDB after ÷ before; LMDB after ÷ before):

| rung | variant | rounds 1–5, ZeroDB | rounds 1–5, LMDB |
|---|---|---|---|
| `env/txn/ro_begin_abort` | lock on | 1.245 1.186 1.193 1.027 1.156 | 0.889 0.987 0.985 0.999 0.876 |
| `env/txn/ro_begin_abort` | lock off | 1.147 1.007 1.009 0.892 0.998 | 0.978 0.894 1.082 1.001 0.889 |
| `env/txn/rw_empty_commit` | lock on | 1.102 1.055 1.153 1.188 1.079 | 0.829 0.852 0.999 0.983 0.986 |
| `env/txn/rw_empty_commit` | lock off | 0.975 1.000 1.022 1.019 0.988 | 0.824 0.848 0.998 0.996 1.004 |
| `commit/batch/n1` | lock on | 1.006 1.016 1.017 1.014 0.922 | 0.988 0.993 0.931 0.980 0.974 |
| `commit/batch/n1` | lock off | 0.992 1.002 1.005 1.001 0.927 | 0.977 0.990 0.947 0.979 0.997 |

Per operation (medians):

| rung | before | lock off | lock on |
|---|---:|---:|---:|
| read txn begin + abort | 81.5 ns | 83.1 ns (+1.6) | 96.6 ns (+15.1) |
| empty write txn | 220 ns | 217 ns (−3) | 240 ns (+20) |
| single-put commit | 10.00 µs | 10.01 µs | 10.07 µs (+0.06) |

**Intermediate run** (`5f9b238` alone, same method; numbers not repeated
here): lock off had `ro_begin_abort` at +0.6% but `rw_empty_commit` still at
+6–8% in four of five rounds. That residual led to `2b53e88`.

### Verdict after the fix

- **The default path no longer regresses.** Lock off is flat against `main`
  on every rung (`ro_begin_abort` 1.007, `rw_empty_commit` 1.000, `n1` 1.001,
  every `get/*` within ±2.3%).
- **With the lock file on, reads are within noise and commits are flat.**
  `get/*` and `commit/batch/*` are unchanged (n1 1.014, flat).
- **Residual lock-on cost is per transaction.**
  - +15 ns per read txn (`ro_begin_abort` +19%): a virtual call for the slot
    slice and for `T`, owner-PID stores and the `Result` plumbing of
    `pin_shared`.
  - About +20 ns per empty write txn (+10%, inside this run's noise band):
    the futex lock and unlock RMWs plus the `SeqCst` `T` store.
  - ZeroDB stays ahead of LMDB on read-txn begin/abort with the lock file on
    (0.77×).

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

After the fix (`2b53e88`), on the same server: fmt and clippy clean,
`zerodb-io` 6/6, `multi_process` 7/7 in each of 5 runs, `lock_file_single_process`
4/4 and workspace 694 passed. On the Mac: fmt, clippy, workspace (693), miri
(226) and `just loom` (9) all green.

## Reproduce

```bash
# on the bench server, tree synced at ca27d00, CPU tuned (turbo off, performance governor)
export CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
F='^(env/txn/|get/|commit/batch/n1/|commit/batch/n100/|commit/sync/)'
# "before" = main (7002d6e) with its own harness, in its own target dir;
# "after" = this branch; per round run before / after+ZERODB_BENCH_LOCK_FILE=1 /
# after without it, rotating the order; then lay out <run>/round-N/{before,after}
# for each after-variant and run scripts/bench-ab.py <run> --target "$F".
```

## Stage 2 (2026-10-12, `164e0e9`)

Stage 2 adds reaping of dead readers' slots and changes the lock-file release
order (ADR-0023 amendment 7 adds one store to the release). This run checks
that the default path (lock file off) is still flat against `main`, and measures
what the lock-file path costs now.

Same three-sided driver as stage 1 (`job3.sh`, retargeted to a new job
directory), same filter, `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`, 5 interleaved
rounds, base `7002d6e` (main), bench server (Xeon E3-1230 v2, turbo off). The
checkout was synced at `164e0e9` with no uncommitted changes.

### Lock file off (default path)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 13.722 ms | 20.092 ms | 20.244 ms | 1.46× → **1.48×** | 1.015 (±0.034) | 0.992 | flat |
| `commit/batch/n100` | 12.337 ms | 12.477 ms | 12.565 ms | 1.01× → **1.02×** | 1.007 (±0.030) | 1.001 | flat |
| `commit/sync/n1` 🎯 | 295.775 ms | 276.133 ms | 283.816 ms | 0.93× → **0.96×** | 1.014 (±0.080) | 1.027 | flat |
| `commit/sync/n100` 🎯 | 567.945 ms | 560.906 ms | 593.713 ms | 0.99× → **1.05×** | 0.999 (±0.143) | 0.982 | flat |
| `env/txn/ro_begin_abort` 🎯 | 1.359 ms | 815.574 µs | 825.127 µs | 0.60× → **0.61×** | 1.012 (±0.030) | 1.079 | ⚠ drift |
| `env/txn/rw_empty_commit` 🎯 | 75.695 µs | 211.063 µs | 216.501 µs | 2.79× → **2.86×** | 1.026 (±0.052) | 1.012 | flat |
| `get/access/hot` 🎯 | 1.801 ms | 1.375 ms | 1.378 ms | 0.76× → **0.77×** | 1.002 (±0.030) | 1.003 | flat |
| `get/access/miss` 🎯 | 1.274 ms | 1.226 ms | 1.229 ms | 0.96× → **0.97×** | 1.003 (±0.030) | 0.996 | flat |
| `get/access/rand` 🎯 | 3.555 ms | 3.342 ms | 3.372 ms | 0.94× → **0.95×** | 1.010 (±0.030) | 1.007 | flat |
| `get/access/seq` 🎯 | 1.864 ms | 1.664 ms | 1.657 ms | 0.89× → **0.89×** | 0.996 (±0.030) | 1.004 | flat |
| `get/db/named` 🎯 | 3.552 ms | 3.348 ms | 3.360 ms | 0.94× → **0.95×** | 1.005 (±0.030) | 1.005 | flat |
| `get/db/named_x8` 🎯 | 3.253 ms | 3.234 ms | 3.255 ms | 0.99× → **1.00×** | 1.003 (±0.042) | 0.998 | flat |
| `get/db/root` 🎯 | 3.544 ms | 3.324 ms | 3.340 ms | 0.94× → **0.94×** | 1.003 (±0.030) | 1.010 | flat |
| `get/key/k128` 🎯 | 4.832 ms | 5.456 ms | 5.476 ms | 1.13× → **1.13×** | 1.005 (±0.038) | 1.010 | flat |
| `get/key/k32` 🎯 | 3.860 ms | 4.403 ms | 4.383 ms | 1.14× → **1.14×** | 0.994 (±0.058) | 1.005 | flat |
| `get/key/k8` 🎯 | 3.608 ms | 3.407 ms | 3.430 ms | 0.94× → **0.95×** | 1.004 (±0.060) | 1.003 | flat |
| `get/size/n1k` 🎯 | 1.767 ms | 1.639 ms | 1.630 ms | 0.93× → **0.92×** | 0.995 (±0.030) | 1.009 | flat |
| `get/size/n50k` 🎯 | 3.551 ms | 3.355 ms | 3.371 ms | 0.94× → **0.95×** | 1.003 (±0.050) | 1.003 | flat |
| `get/val/v256` 🎯 | 2.539 ms | 2.377 ms | 2.379 ms | 0.94× → **0.94×** | 1.001 (±0.030) | 1.004 | flat |
| `get/val/v2page` 🎯 | 2.297 ms | 2.893 ms | 2.876 ms | 1.26× → **1.25×** | 0.995 (±0.030) | 1.008 | flat |
| `get/val/v2page_touch` 🎯 | 3.231 ms | 3.693 ms | 3.673 ms | 1.14× → **1.14×** | 0.998 (±0.030) | 0.990 | flat |
| `get/val/v4k` 🎯 | 2.251 ms | 2.880 ms | 2.865 ms | 1.28× → **1.27×** | 0.994 (±0.030) | 1.006 | flat |
| `get/val/v4k_touch` 🎯 | 3.185 ms | 3.654 ms | 3.631 ms | 1.15× → **1.14×** | 0.993 (±0.030) | 0.992 | flat |
| `get/val/v8` 🎯 | 2.225 ms | 1.972 ms | 1.956 ms | 0.89× → **0.88×** | 0.992 (±0.030) | 1.004 | flat |
| `get/val/v8_touch` 🎯 | 2.217 ms | 1.973 ms | 1.969 ms | 0.89× → **0.89×** | 1.001 (±0.030) | 0.990 | flat |

**Verdict: flat**. Improved: 0, regressed: 0, flat: 24, unreliable (LMDB drift > 3%): 1. 5 interleaved round(s); before = `7002d6eb7`, after = `164e0e9e lock_file=off`, host `Linux x86_64`.

### Lock file on

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 13.810 ms | 20.092 ms | 20.771 ms | 1.45× → **1.50×** | 1.034 (±0.041) | 1.001 | flat |
| `commit/batch/n100` | 12.351 ms | 12.477 ms | 12.633 ms | 1.01× → **1.02×** | 1.017 (±0.030) | 1.002 | flat |
| `commit/sync/n1` 🎯 | 291.777 ms | 276.133 ms | 277.043 ms | 0.95× → **0.95×** | 1.004 (±0.082) | 1.038 | ⚠ drift |
| `commit/sync/n100` 🎯 | 557.060 ms | 560.906 ms | 558.146 ms | 1.01× → **1.00×** | 1.003 (±0.098) | 0.956 | ⚠ drift |
| `env/txn/ro_begin_abort` 🎯 | 1.312 ms | 815.574 µs | 989.410 µs | 0.62× → **0.75×** | 1.213 (±0.030) | 1.034 | ⚠ drift |
| `env/txn/rw_empty_commit` 🎯 | 75.419 µs | 211.063 µs | 255.894 µs | 2.80× → **3.39×** | 1.166 (±0.101) | 1.009 | ❌ slower |
| `get/access/hot` 🎯 | 1.801 ms | 1.375 ms | 1.387 ms | 0.76× → **0.77×** | 1.009 (±0.030) | 1.001 | flat |
| `get/access/miss` 🎯 | 1.274 ms | 1.226 ms | 1.229 ms | 0.96× → **0.97×** | 1.003 (±0.030) | 0.996 | flat |
| `get/access/rand` 🎯 | 3.558 ms | 3.342 ms | 3.363 ms | 0.94× → **0.95×** | 1.007 (±0.030) | 1.007 | flat |
| `get/access/seq` 🎯 | 1.869 ms | 1.664 ms | 1.662 ms | 0.89× → **0.89×** | 0.999 (±0.030) | 1.008 | flat |
| `get/db/named` 🎯 | 3.551 ms | 3.348 ms | 3.369 ms | 0.94× → **0.95×** | 1.005 (±0.030) | 1.001 | flat |
| `get/db/named_x8` 🎯 | 3.257 ms | 3.234 ms | 3.217 ms | 0.99× → **0.99×** | 0.997 (±0.030) | 0.997 | flat |
| `get/db/root` 🎯 | 3.536 ms | 3.324 ms | 3.335 ms | 0.94× → **0.94×** | 1.003 (±0.030) | 1.005 | flat |
| `get/key/k128` 🎯 | 4.818 ms | 5.456 ms | 5.463 ms | 1.13× → **1.13×** | 1.004 (±0.030) | 1.008 | flat |
| `get/key/k32` 🎯 | 3.862 ms | 4.403 ms | 4.382 ms | 1.14× → **1.13×** | 0.995 (±0.030) | 1.006 | flat |
| `get/key/k8` 🎯 | 3.610 ms | 3.407 ms | 3.440 ms | 0.94× → **0.95×** | 1.008 (±0.038) | 1.007 | flat |
| `get/size/n1k` 🎯 | 1.764 ms | 1.639 ms | 1.630 ms | 0.93× → **0.92×** | 0.995 (±0.030) | 0.998 | flat |
| `get/size/n50k` 🎯 | 3.550 ms | 3.355 ms | 3.384 ms | 0.95× → **0.95×** | 1.008 (±0.047) | 1.006 | flat |
| `get/val/v256` 🎯 | 2.532 ms | 2.377 ms | 2.382 ms | 0.94× → **0.94×** | 1.002 (±0.030) | 1.003 | flat |
| `get/val/v2page` 🎯 | 2.295 ms | 2.893 ms | 2.880 ms | 1.26× → **1.26×** | 0.997 (±0.030) | 1.015 | flat |
| `get/val/v2page_touch` 🎯 | 3.234 ms | 3.693 ms | 3.677 ms | 1.14× → **1.14×** | 0.995 (±0.030) | 0.991 | flat |
| `get/val/v4k` 🎯 | 2.251 ms | 2.880 ms | 2.863 ms | 1.28× → **1.27×** | 0.993 (±0.030) | 1.009 | flat |
| `get/val/v4k_touch` 🎯 | 3.179 ms | 3.654 ms | 3.630 ms | 1.15× → **1.14×** | 0.994 (±0.030) | 0.990 | flat |
| `get/val/v8` 🎯 | 2.228 ms | 1.972 ms | 1.967 ms | 0.88× → **0.88×** | 0.997 (±0.030) | 1.005 | flat |
| `get/val/v8_touch` 🎯 | 2.220 ms | 1.973 ms | 1.966 ms | 0.89× → **0.89×** | 0.999 (±0.030) | 0.991 | flat |

**Verdict: regressed**. Improved: 0, regressed: 1, flat: 21, unreliable (LMDB drift > 3%): 3. 5 interleaved round(s); before = `7002d6eb7`, after = `164e0e9e lock_file=on`, host `Linux x86_64`.

### Verdict

- **Lock off: `flat`, not a regression.** 24 rungs flat, 1 drift, none
  regressed. The one `drift` rung is `env/txn/ro_begin_abort`, where LMDB moved
  +8% between halves in the median round; ZeroDB's after/before is 1.000-1.013 in
  all five rounds, so it is flat.
  - `env/txn/ro_begin_abort`: 1.012 (82.5 ns vs 81.6 ns per read txn).
  - `env/txn/rw_empty_commit`: 1.026 (216.5 ns vs 211.1 ns, +5 ns, inside the
    0.052 threshold; rounds 0.98-1.05).
  - `commit/batch/n1`: 1.015 (rounds 1.00-1.04).
  - Every `get/*` rung is within +-1%.
  - Stage 1 after the fix had 1.007, 1.000 and 1.001 on those three; the small
    rise on `rw_empty_commit` is inside noise but is the one number to watch.
- **Lock on: `regressed`, on `rw_empty_commit` only.** 21 flat, 3 drift.
  - `env/txn/rw_empty_commit`: 1.166 (255.9 ns vs 211.1 ns, +45 ns per empty
    write txn; rounds 1.11-1.24, so it holds in every round). Stage 1 after the
    fix was +20 ns (1.102, flagged flat only because that run's noise band was
    wider). Stage 2 adds about 25 ns here.
  - `env/txn/ro_begin_abort`: 1.213 (98.9 ns vs 81.6 ns, +17 ns per read txn;
    rounds 1.198-1.232). bench-ab marks it `drift` because LMDB moved, but the
    ZeroDB ratio is stable. Stage 1 after the fix was +15 ns.
  - `commit/batch/n1`: 1.034 (+0.34 us per commit; rounds 1.005-1.048; flat
    under its 0.041 threshold). Stage 1 was 1.014 (+0.06 us).
  - `commit/batch/n100`, `commit/sync/*` and every `get/*` rung are flat.
  - ZeroDB stays ahead of LMDB on read-txn begin/abort with the lock file on
    (0.75x).
- The lock file stays opt-in, so the lock-on increase is a cost on the opt-in
  path only. It is not a blocker, because the default path is the gate for this
  stage, but it is a measured step up from stage 1 and is reported as such.

### Platform runs (stage 2)

| platform | result |
|---|---|
| Linux x86-64 (bench server) | `cargo clippy --workspace --all-targets -- -D warnings` ok; `cargo test --workspace` ok (all suites 0 failed); `multi_process` 5 runs, 15/15 each; `crash-harness --mechanism mp --cycles 2000`: 2007 cycles verified, 0 failures, 173 s; `reader_stress` 180 s ok (3 variants); `multi_process_stress` 180 s ok: commits parent=23544 peer=19606, reaped-reader kills=356, reaped slots=712 |
| macOS aarch64 | fmt, clippy (host and `x86_64-unknown-linux-gnu`), `cargo test --workspace` (773 passed), miri on `zerodb-core`, `just loom` (15 passed), `crash-test-quick` (229 + 57 cycles verified), `just fuzz-quick` (`diff_ops` 600 s: 49,891 runs; `fuzz_image_open` 120 s: 7,487,334 runs; no crash) all green |
| Linux aarch64 (Graviton) | **not run.** The Graviton test instances no longer exist, so there was no host to start. aarch64 coverage is CI's only. |

## Stage 3 (Graviton4, 2026-10-13)

Stage 3 (tasks 3–9) added a begin-time map check to every write txn, a
`WRITE_MAP` file-growth step and the cross-process `info()`/`stat()` view. This
run asks whether the lock file's single-process cost is back within the ADR's
budget ("one shared load and one slot store per txn begin, plus one uncontended
CAS per write txn"). The stage-1 and stage-2 runs above were on the x86 bench
server, which no longer exists, so the stage-1 commit is re-measured here as
the target column.

**Target:** the lock-on overhead against `main` is at or below stage 1's on the
same host, within bench-ab noise, and the lock-off path is flat against `main`.

### Setup

| | |
|---|---|
| Host | AWS c8g.xlarge: Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB, Ubuntu 24.04 arm64, kernel 7.0, 4 KiB OS pages. No cpufreq governor is exposed. apt, sysstat, snapd and the other timers were stopped; the host ran nothing else. |
| Disk | EBS io2, 3000 IOPS (`/data`). Bench temp dirs were placed on it (`TMPDIR`). |
| Toolchain | rustc 1.99.0 |
| Build | criterion bench profile, `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`, one target dir per commit |
| LMDB | heed 0.22.1 (the Meilisearch LMDB fork), in every binary, as the drift control |
| Columns | `main` `7002d6e`; stage 1 `2b53e88`; before = `f2603c3` (stage-3 HEAD, tasks 1–9); after = `c77e822` (this task) |
| Rungs | the stage-2 filter: `^(env/txn/\|get/\|commit/batch/n1/\|commit/batch/n100/\|commit/sync/)` |
| Rounds | 5 interleaved rounds of 7 sides (main; stage 1, before, after, each with lock on and off), the starting side rotating each round |

Method: the three-sided driver of stage 1, extended to seven sides. Each side
runs its own binary; the lock-on sides set `ZERODB_BENCH_LOCK_FILE=1` (present
in the stage-1, before and after harnesses). Every variant is fed to
`scripts/bench-ab.py` against the same `main` measurements. The `main` binary
has no `zerodb_io::lockfile` symbols; the others have them.

### Attribution (profile of the before commit)

Criterion's own analysis drowns a `perf record` of the bench binary, so the
cost was attributed with a micro loop outside the repo (heed API, `NO_SYNC`,
pinned to one core, warm env): 20 M read txns or 5 M empty write txns, under
`perf stat -e instructions:u,cycles:u` minus an empty run, median of 5
interleaved runs; and `perf record -e cycles:u` per symbol. 1 ns ≈ 2.8 cycles.

| loop | main | stage 1 off | before off | after off | stage 1 on | before on | after on |
|---|---:|---:|---:|---:|---:|---:|---:|
| read txn, instructions | 466 | 490 | 492 | 492 | 575 | 585 | 585 |
| read txn, cycles | 226 | 221–225 | 231 | 232 | 249 | 251 | 255 |
| empty write txn, instructions | 1,028 | 1,053 | 1,095 | **1,028** | 1,205 | 1,262 | **1,195** |
| empty write txn, cycles | 406 | 420 | 428 | **398** | 487 | 513 | **469** |

Where the before commit's extra write-txn cycles went:

| cost | on / off | cycles | what it is | in the ADR budget? |
|---|---|---:|---|---|
| stage-3 begin additions | both | ≈ +22 | a second `dyn Backing::view` call (Acquire load of the view cell) for the base check, on top of the one that derives the txn's read view; the `WRITE_MAP` growth arm inline in `write_txn`, which made the begin larger | no: not a lock-file cost at all, and it made the lock-off path +4.8% |
| writer-word release (`swap`, Release) | on | ≈ +30 | the futex unlock; `__aarch64_swp4_rel` is the largest single delta in the profile | yes: LMDB's mutex unlock is also a release RMW |
| writer-word claim (`compare_exchange`, Acquire) | on | ≈ +5–8 | the "one uncontended CAS" | yes |
| `T` load and snapshot lookup | on | ≈ +10–15 | `published_txnid()` through the vtable, the SeqCst load, `current_snapshot_shared` and its retry loop; the snapshot clone itself is also paid without a lock file | yes ("one shared load") |
| lock call chain | on | ≈ +10 | `lock_writer_region` → `dyn SharedRegion::lock_writer` → `Result<bool>`; the takeover check is one untaken branch | plumbing around the budgeted CAS |
| `T` publish | — | 0 | an empty commit publishes nothing; on a real commit it is one SeqCst store | yes |

The read txn is unchanged by stage 3's tasks beyond stage 2's release: its
lock-on cost is the slot claim CAS and owner-word store, stage 2's three-store
release (ADR-0023 amendment 7, ≈ +5–8 cycles in `RoTxn::drop`), and the
`slots()` and `published_txnid()` vtable calls (≈ +6 cycles each).

So the only cost outside the budget was the stage-3 begin additions, which
the lock-on and lock-off paths share.

### What changed

- **`c77e822`: one map view per write-txn begin; `WRITE_MAP` growth out of
  line.** The begin loads the backing's map view once and uses it for both the
  base check (ADR-0023 amendment 15) and the txn's read view, which it used to
  load a second time through `backing_bytes()`. The `WRITE_MAP` file growth
  moved into a `#[cold]` out-of-line helper. Same checks, same order, same
  errors; no protocol change. This is not an LMDB technique: it removes
  overhead stage 3 added. Empty write txn: 1,095 → 1,028 instructions (main:
  1,028), 428 → 398 cycles.
- **Tried and reverted: capture the map's page limit at begin.** A review
  suggested storing `map_size / page_size` in the txn instead of an Acquire
  load and a vtable call per page-extension allocation. It is valid (this
  process's `resize` refuses while a write txn is live), but steady-state
  commits take their pages from GC and loose reuse, not from file extension,
  so the saving never shows. The begin pays a division and a wider struct
  instead: empty write txn +18 instructions, cycles within noise; single-put
  commit +26 instructions. It does not simplify anything either, so it was
  reverted (perf ledger).

### Results

Per operation (ns per txn, from the medians; `+x` is the change against
`main`, then after ÷ before as bench-ab reports it). `ro_begin_abort` runs
10,000 read txns per iteration, `rw_empty_commit` 1,000 empty write txns,
`commit/batch/n1` 2,000 single-put commits.

| rung | LMDB | main | stage 1 on | before on | **after on** | stage 1 off | before off | **after off** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `env/txn/ro_begin_abort` | 84.7 | 67.4 | 72.9 (+5.5) 1.083 | 73.9 (+6.5) 1.102 | 73.8 (+6.4) 1.097 | 67.1 (-0.3) 0.995 | 67.4 (+0.0) 1.003 | 67.6 (+0.2) 1.006 |
| `env/txn/rw_empty_commit` | 54.6 | 155.5 | 185.3 (+29.8) 1.189 | 191.4 (+35.9) 1.228 | 183.0 (+27.6) 1.177 | 158.4 (+3.0) 1.019 | 163.0 (+7.5) 1.048 | 154.4 (-1.0) 0.993 |
| `commit/batch/n1` | 3.63 µs | 5.08 µs | 5.17 µs (+0.10) 1.026 | 5.15 µs (+0.07) 1.017 | 5.15 µs (+0.08) 1.013 | 5.08 µs (+0.00) 1.006 | 5.16 µs (+0.09) 1.017 | 5.14 µs (+0.06) 1.010 |
| `commit/batch/n100` | 5.926 ms | 6.541 ms | 6.532 ms (-0.009) 1.004 | 6.609 ms (+0.068) 1.010 | 6.739 ms (+0.198) 1.006 | 6.550 ms (+0.009) 1.000 | 6.674 ms (+0.133) 1.003 | 6.734 ms (+0.193) 1.006 |

The `commit/batch/n100` changes are in ms. For this rung the change of the
medians (up to +3.0%) is larger than bench-ab's ratio, which is the value
bench-ab judges and stays inside its ±0.030 threshold.

The LMDB column's source runs are not recorded in this report. The bench-ab
tables below report LMDB at 85.8 and 85.6 ns per `ro_begin_abort` txn
(857.768 and 856.094 µs per 10,000 txns), not 84.7 ns, and flag that rung's
LMDB drift (1.043 and 1.038). Read the LMDB column as indicative.

Per round (ZeroDB variant ÷ `main`):

| rung | variant | rounds 1–5 | LMDB, rounds 1–5 |
|---|---|---|---|
| `env/txn/ro_begin_abort` | stage 1, lock on | 1.083 1.092 1.081 1.080 1.087 | 1.017 1.060 0.999 1.026 1.057 |
| `env/txn/ro_begin_abort` | before, lock on | 1.117 1.102 1.091 1.104 1.095 | 1.029 1.091 1.045 1.048 1.066 |
| `env/txn/ro_begin_abort` | after, lock on | 1.097 1.099 1.097 1.095 1.100 | 1.028 1.071 1.038 1.034 1.107 |
| `env/txn/ro_begin_abort` | stage 1, lock off | 1.000 1.002 0.994 0.995 0.992 | 1.007 1.047 1.002 1.010 1.123 |
| `env/txn/ro_begin_abort` | before, lock off | 1.003 1.004 1.003 1.000 1.003 | 1.041 1.080 1.031 1.038 1.052 |
| `env/txn/ro_begin_abort` | after, lock off | 1.006 1.009 1.050 1.002 1.001 | 1.043 1.087 1.035 1.040 1.072 |
| `env/txn/rw_empty_commit` | stage 1, lock on | 1.184 1.192 1.189 1.185 1.198 | 1.003 1.017 1.004 1.001 1.004 |
| `env/txn/rw_empty_commit` | before, lock on | 1.211 1.240 1.226 1.228 1.232 | 1.016 1.020 1.014 1.029 1.003 |
| `env/txn/rw_empty_commit` | after, lock on | 1.181 1.177 1.134 1.168 1.179 | 0.997 0.990 1.000 0.999 0.996 |
| `env/txn/rw_empty_commit` | stage 1, lock off | 1.026 1.020 1.019 1.010 1.010 | 1.004 0.998 0.997 1.009 1.006 |
| `env/txn/rw_empty_commit` | before, lock off | 1.058 1.051 1.046 1.043 1.048 | 1.020 1.019 1.002 1.009 1.001 |
| `env/txn/rw_empty_commit` | after, lock off | 0.994 0.992 0.988 0.993 0.994 | 0.994 0.993 0.997 0.992 1.002 |

#### After (`c77e822`), lock file off, against `main`

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.274 ms | 10.154 ms | 10.271 ms | 1.40× → **1.41×** | 1.010 (±0.034) | 1.007 | flat |
| `commit/batch/n100` | 5.997 ms | 6.541 ms | 6.734 ms | 1.09× → **1.12×** | 1.006 (±0.030) | 1.003 | flat |
| `commit/sync/n1` 🎯 | 282.829 ms | 245.165 ms | 245.226 ms | 0.87× → **0.87×** | 1.000 (±0.030) | 1.000 | flat |
| `commit/sync/n100` 🎯 | 432.683 ms | 437.673 ms | 437.884 ms | 1.01× → **1.01×** | 1.000 (±0.030) | 1.000 | flat |
| `env/txn/ro_begin_abort` 🎯 | 857.768 µs | 674.104 µs | 676.476 µs | 0.79× → **0.79×** | 1.006 (±0.041) | 1.043 | ⚠ drift |
| `env/txn/rw_empty_commit` 🎯 | 54.517 µs | 155.453 µs | 154.436 µs | 2.85× → **2.83×** | 0.993 (±0.030) | 0.994 | flat |
| `get/access/hot` 🎯 | 1.254 ms | 939.024 µs | 936.013 µs | 0.75× → **0.75×** | 0.996 (±0.030) | 0.990 | flat |
| `get/access/miss` 🎯 | 854.902 µs | 910.999 µs | 912.569 µs | 1.07× → **1.07×** | 1.002 (±0.030) | 1.001 | flat |
| `get/access/rand` 🎯 | 2.630 ms | 2.314 ms | 2.308 ms | 0.88× → **0.88×** | 0.994 (±0.047) | 1.013 | flat |
| `get/access/seq` 🎯 | 1.423 ms | 1.236 ms | 1.241 ms | 0.87× → **0.87×** | 1.004 (±0.030) | 1.004 | flat |
| `get/db/named` 🎯 | 2.611 ms | 2.320 ms | 2.323 ms | 0.89× → **0.89×** | 0.998 (±0.037) | 1.001 | flat |
| `get/db/named_x8` 🎯 | 2.273 ms | 2.166 ms | 2.184 ms | 0.95× → **0.96×** | 1.008 (±0.030) | 1.011 | flat |
| `get/db/root` 🎯 | 2.660 ms | 2.340 ms | 2.364 ms | 0.88× → **0.89×** | 1.006 (±0.030) | 0.974 | flat |
| `get/key/k128` 🎯 | 3.305 ms | 3.590 ms | 3.669 ms | 1.09× → **1.11×** | 0.994 (±0.042) | 0.982 | flat |
| `get/key/k32` 🎯 | 2.766 ms | 2.862 ms | 2.911 ms | 1.03× → **1.05×** | 1.010 (±0.035) | 1.041 | ⚠ drift |
| `get/key/k8` 🎯 | 2.623 ms | 2.316 ms | 2.330 ms | 0.88× → **0.89×** | 1.009 (±0.046) | 1.005 | flat |
| `get/size/n1k` 🎯 | 1.357 ms | 1.201 ms | 1.194 ms | 0.88× → **0.88×** | 0.994 (±0.030) | 1.004 | flat |
| `get/size/n50k` 🎯 | 2.632 ms | 2.302 ms | 2.310 ms | 0.87× → **0.88×** | 1.001 (±0.057) | 1.021 | flat |
| `get/val/v256` 🎯 | 2.196 ms | 1.784 ms | 1.794 ms | 0.81× → **0.82×** | 1.004 (±0.030) | 1.027 | flat |
| `get/val/v2page` 🎯 | 1.839 ms | 1.758 ms | 1.767 ms | 0.96× → **0.96×** | 0.996 (±0.066) | 1.001 | flat |
| `get/val/v2page_touch` 🎯 | 2.302 ms | 1.842 ms | 1.974 ms | 0.80× → **0.86×** | 1.028 (±0.110) | 0.981 | flat |
| `get/val/v4k` 🎯 | 1.826 ms | 1.765 ms | 1.786 ms | 0.97× → **0.98×** | 1.001 (±0.043) | 0.999 | flat |
| `get/val/v4k_touch` 🎯 | 2.227 ms | 1.839 ms | 1.918 ms | 0.83× → **0.86×** | 0.998 (±0.180) | 0.999 | flat |
| `get/val/v8` 🎯 | 1.796 ms | 1.463 ms | 1.469 ms | 0.81× → **0.82×** | 1.004 (±0.030) | 1.003 | flat |
| `get/val/v8_touch` 🎯 | 1.803 ms | 1.479 ms | 1.476 ms | 0.82× → **0.82×** | 0.998 (±0.030) | 0.999 | flat |

**Verdict: flat**. Improved: 0, regressed: 0, flat: 23, unreliable (LMDB drift > 3%): 2. 5 interleaved round(s); before = `main-off`, after = `e1-off`, host `Linux aarch64 Graviton4 c8g.xlarge`.

#### After (`c77e822`), lock file on, against `main`

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.272 ms | 10.154 ms | 10.307 ms | 1.40× → **1.42×** | 1.013 (±0.030) | 1.001 | flat |
| `commit/batch/n100` | 5.964 ms | 6.541 ms | 6.739 ms | 1.10× → **1.13×** | 1.006 (±0.030) | 1.000 | flat |
| `commit/sync/n1` 🎯 | 282.817 ms | 245.165 ms | 245.105 ms | 0.87× → **0.87×** | 1.000 (±0.030) | 1.000 | flat |
| `commit/sync/n100` 🎯 | 432.718 ms | 437.673 ms | 437.791 ms | 1.01× → **1.01×** | 1.000 (±0.030) | 1.000 | flat |
| `env/txn/ro_begin_abort` 🎯 | 856.094 µs | 674.104 µs | 738.436 µs | 0.79× → **0.86×** | 1.097 (±0.030) | 1.038 | ⚠ drift |
| `env/txn/rw_empty_commit` 🎯 | 54.508 µs | 155.453 µs | 183.043 µs | 2.85× → **3.36×** | 1.177 (±0.039) | 0.997 | ❌ slower |
| `get/access/hot` 🎯 | 1.257 ms | 939.024 µs | 934.129 µs | 0.75× → **0.74×** | 0.996 (±0.030) | 0.992 | flat |
| `get/access/miss` 🎯 | 854.778 µs | 910.999 µs | 913.220 µs | 1.07× → **1.07×** | 1.000 (±0.030) | 1.001 | flat |
| `get/access/rand` 🎯 | 2.651 ms | 2.314 ms | 2.321 ms | 0.87× → **0.88×** | 0.993 (±0.043) | 1.015 | flat |
| `get/access/seq` 🎯 | 1.423 ms | 1.236 ms | 1.243 ms | 0.87× → **0.87×** | 1.005 (±0.030) | 1.002 | flat |
| `get/db/named` 🎯 | 2.634 ms | 2.320 ms | 2.324 ms | 0.88× → **0.88×** | 0.987 (±0.030) | 1.016 | flat |
| `get/db/named_x8` 🎯 | 2.287 ms | 2.166 ms | 2.201 ms | 0.95× → **0.96×** | 1.000 (±0.051) | 1.004 | flat |
| `get/db/root` 🎯 | 2.641 ms | 2.340 ms | 2.346 ms | 0.89× → **0.89×** | 0.999 (±0.030) | 0.978 | flat |
| `get/key/k128` 🎯 | 3.255 ms | 3.590 ms | 3.744 ms | 1.10× → **1.15×** | 1.029 (±0.066) | 1.018 | flat |
| `get/key/k32` 🎯 | 2.784 ms | 2.862 ms | 2.943 ms | 1.03× → **1.06×** | 1.016 (±0.087) | 1.008 | flat |
| `get/key/k8` 🎯 | 2.633 ms | 2.316 ms | 2.356 ms | 0.88× → **0.90×** | 1.004 (±0.064) | 1.024 | flat |
| `get/size/n1k` 🎯 | 1.356 ms | 1.201 ms | 1.197 ms | 0.89× → **0.88×** | 0.995 (±0.030) | 1.001 | flat |
| `get/size/n50k` 🎯 | 2.651 ms | 2.302 ms | 2.319 ms | 0.87× → **0.87×** | 1.004 (±0.030) | 1.013 | flat |
| `get/val/v256` 🎯 | 2.204 ms | 1.784 ms | 1.775 ms | 0.81× → **0.81×** | 0.994 (±0.030) | 1.019 | flat |
| `get/val/v2page` 🎯 | 1.839 ms | 1.758 ms | 1.769 ms | 0.96× → **0.96×** | 1.006 (±0.100) | 1.004 | flat |
| `get/val/v2page_touch` 🎯 | 2.203 ms | 1.842 ms | 1.903 ms | 0.84× → **0.86×** | 1.005 (±0.046) | 0.964 | ⚠ drift |
| `get/val/v4k` 🎯 | 1.827 ms | 1.765 ms | 1.749 ms | 0.97× → **0.96×** | 0.989 (±0.030) | 1.001 | flat |
| `get/val/v4k_touch` 🎯 | 2.216 ms | 1.839 ms | 1.871 ms | 0.83× → **0.84×** | 0.994 (±0.134) | 0.998 | flat |
| `get/val/v8` 🎯 | 1.794 ms | 1.463 ms | 1.471 ms | 0.82× → **0.82×** | 1.003 (±0.030) | 0.999 | flat |
| `get/val/v8_touch` 🎯 | 1.803 ms | 1.479 ms | 1.479 ms | 0.82× → **0.82×** | 0.999 (±0.030) | 0.999 | flat |

**Verdict: regressed**. Improved: 0, regressed: 1, flat: 22, unreliable (LMDB drift > 3%): 2. 5 interleaved round(s); before = `main-off`, after = `e1-on`, host `Linux aarch64 Graviton4 c8g.xlarge`.

The other variants' bench-ab verdicts in the same run:

- stage 1, lock off: `flat` (23 flat, 2 drift). Lock on: `regressed` on
  `ro_begin_abort` (1.083) and `rw_empty_commit` (1.189).
- before, lock off: `regressed` on `rw_empty_commit` only (1.048, +7.5 ns,
  every round 1.043–1.058). Lock on: `regressed` on `rw_empty_commit` (1.228).

### Verdict (stage 3)

- **Target met.**
  - Write: the lock-on cost of an empty write txn goes from +35.9 ns to
    +27.6 ns against `main`. Stage 1 on this host is +29.8 ns.
  - Read: the lock-on cost of a read txn is +6.4 ns (stage 1: +5.5 ns,
    before: +6.5 ns). The 0.9 ns over stage 1 is systematic, not noise: the
    per-round ranges do not overlap (stage 1 1.080–1.092, after 1.095–1.100).
    It is attributed to stage 2's extra release store (amendment 7,
    approved), not to a stage-3 addition, and it is inside bench-ab's ±0.030
    threshold (1.097 vs 1.083).
  - Lock off, `ro_begin_abort`: round 3 of the after variant is an outlier
    (1.050 against 1.001–1.009 in the other rounds); the median absorbs
    it.
- **Lock off is flat against `main` again:** bench-ab `flat`, with
  `rw_empty_commit` at 0.993 (−1.0 ns), against 1.048 before.
- `commit/batch/n1` (1.013 on, 1.010 off), `commit/batch/n100`,
  `commit/sync/*` and every `get/*` rung are flat in both modes.
- **What remains with the lock file on** is the budget itself:
  - per write txn, the futex writer word's acquire CAS and release swap,
    the `T` load, and the dynamic-dispatch plumbing around them (about 63
    cycles, 22–28 ns);
  - per read txn, the shared slot's claim, the owner word and the release
    stores (about 6 ns).

  LMDB pays a lock RMW and an unlock RMW per write txn too. ZeroDB keeps its
  in-process writer lock under the cross-process one (ADR-0023 §2), so the
  remaining gap cannot be closed without changing the lock protocol.
- ZeroDB stays ahead of LMDB on read-txn begin/abort with the lock file on
  (0.86×). The empty write txn is 3.36× LMDB with the lock on and 2.83× with it
  off (`main`: 2.85×).

### Platform runs (stage 3, after `c77e822`)

| platform | result |
|---|---|
| Linux aarch64 (Graviton4, native) | `cargo clippy --workspace --all-targets -- -D warnings` ok; `cargo test --workspace`: 914 passed, 0 failed, 181 suites |
| macOS aarch64 | fmt, clippy (host and `x86_64-unknown-linux-gnu`), `cargo test --workspace` (908 passed, 0 failed), miri on `zerodb-core` (all suites ok), `just crash-test-quick` (215 + 57 cycles verified) |

### Reproduce (stage 3)

```bash
# Graviton host, one worktree and one CARGO_TARGET_DIR per commit:
# main 7002d6e, s1 2b53e88, head f2603c3, e1 c77e822.
export CARGO_PROFILE_BENCH_CODEGEN_UNITS=1
F='^(env/txn/|get/|commit/batch/n1/|commit/batch/n100/|commit/sync/)'
# Each round runs every side once, rotating the starting side:
#   main, s1:on, s1:off, head:on, head:off, e1:on, e1:off
# A side runs its own binary with CRITERION_HOME=<run>/round-N/<side> and
# TMPDIR on the io2 volume; ":on" adds ZERODB_BENCH_LOCK_FILE=1.
# (After the default flip the knob is inverted: the harness now opens with
# the lock file by default, and ZERODB_BENCH_NO_LOCK=1 turns it off.)
# For each variant, lay out <dir>/round-N/{before=main,after=variant}
# and run scripts/bench-ab.py <dir> --target "$F".
```
