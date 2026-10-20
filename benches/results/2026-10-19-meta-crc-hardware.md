# Meta CRC on the commit path: ARMv8 hardware CRC32C (ADR-0031)

> **Rebased 2026-10-20.** `main` took its own slicing-by-8 table in #117
> while this work was in review, so the slicing-by-16 commit measured here
> was dropped when the branch was rebased; `main`'s slicing-by-8 is the
> software path. The hardware CRC32C (ADR-0031, numbered 0029 when these
> runs were made) and the `del/random/n1` rung were kept. Commit hashes in
> this report are from the pre-rebase branch and are not in `main`'s
> history.
>
> Here "before" is the slicing-by-16 variant. The rebased build is compared
> with `main` in "Rebased on main" at the end.

- **Date:** 2026-10-19.
- **Change:** `b2f5b6c`, ADR-0031 Option B (approved 2026-10-19). On aarch64
  CPUs with the `crc` feature the meta CRC folds 8 bytes per `crc32cx`
  instruction, detected at run time. Elsewhere, and under miri, it keeps the
  slicing-by-16 table of `9f26931`
  (`2026-10-19-meta-crc-slicing.md`).
- **Before:** `0e98443` (slicing-by-16). **After:** `b2f5b6c`.
- **Host:** Apple M-series Mac, macOS, 16 KiB pages. Busy: other sessions
  were benchmarking and profiling at the same time (load 5–13.5 on 8 cores).

## Results

**CRC32C alone** (16,380 bytes, the CRC span of a full 16 KiB meta page):

| slicing-by-16 | hardware |
|---:|---:|
| 5.66 µs (2.9 GB/s) | 1.99 µs (8.2 GB/s) |

**Delete repro** (the custom case for this mechanism): 200,000 random 26-byte
keys with 1,500-byte values, 16 KiB pages, `WRITE_MAP | MAP_ASYNC | NO_SYNC`,
trusted-file mode, every key deleted in shuffled order, one delete per write
transaction. 7 interleaved runs, median µs per commit:

| LMDB | ZeroDB before | ZeroDB after | after ÷ before |
|---:|---:|---:|---:|
| 6.62 | 16.71 | 14.64 | 0.876 |

Paired per run, after ÷ before was 0.716, 0.862, 0.866, 0.887, 0.876, 0.865
and 0.867. The first is low because of one slow "before" run (20.6 µs).

**crud-bench** (500,000 `string26` keys, 128 clients, 48 threads,
`--optimised`; on the delete-rebalance branch, ADR-0028 `0bd9bd7`, with
slicing-by-16 before and with this change after; 3 rounds, medians, ops/s):

| metric | LMDB (before run) | LMDB (after run) | ZeroDB before | ZeroDB after | after ÷ before | ZeroDB/LMDB before → after |
|---|---:|---:|---:|---:|---:|---:|
| Create | 62,917 | 64,209 | 59,068 | 58,265 | 0.99× | 0.94× → 0.91× |
| Update | 65,804 | 67,051 | 61,576 | 60,932 | 0.99× | 0.94× → 0.91× |
| Delete | 69,412 | 66,025 | 56,840 | 61,482 | **1.08×** | 0.82× → **0.93×** |

Per round, ZeroDB deletes were 57,607 / 56,840 / 28,528 before (the last an
outlier) and 61,747 / 61,482 / 60,303 after. The read and scan rows moved in
both directions by more than any plausible effect, for LMDB as much as for
ZeroDB (`batch_read_100` ranged 2.3k–8.3k ops/s across rounds for both).
A single-process read never recomputes the meta CRC: it runs at commit, at
open, and when refreshing a snapshot another process committed.

**Criterion** (`scripts/bench-ab.sh`, base `0e98443`). The host was too busy
for a verdict; both runs are recorded as they came out:

| run | rung | LMDB | before | after | after ÷ before | LMDB drift | verdict |
|---|---|---:|---:|---:|---:|---:|---|
| 5 rounds, load 5.5 | `del/random/n1` | 1.173 s | 1.225 s | 1.285 s | 0.969 (±0.150) | 1.022 | flat |
| 7 rounds, load 13.5 | `del/random/n1` | 1.392 s | 1.449 s | 1.357 s | 0.937 (±0.207) | 1.036 | drift |
| 7 rounds, load 13.5 | `commit/batch/n1` | 32.860 ms | 34.447 ms | 32.502 ms | 0.907 (±0.650) | 0.909 | drift |
| 7 rounds, load 13.5 | `env/txn/rw_empty_commit` | 621.628 µs | 1.038 ms | 1.041 ms | 0.975 (±0.709) | 1.000 | flat |

A 3-round run of `^(commit/|env/txn/)` at load 5 had six of its eight rungs
over the LMDB drift gate (LMDB moved by up to 58 % between halves) and the
other two flat. No row shows a regression beyond its noise. The expected
effect on `del/random/n1` is about 7 % (≈ 1.7 µs of ≈ 24 µs per commit),
below what this host resolved today. The rerun on a quiet host is still to
do.

## Scorecard

| | |
|---|---|
| **Faster** | Meta CRC 2.8× faster on aarch64. One delete per commit on a large tree: 16.71 → 14.64 µs (LMDB 6.62). crud-bench deletes 0.82× → 0.93× LMDB. Criterion inconclusive on this busy host, with no regression seen. |
| **Behavior** | Identical. CRC32C is a function: a new test compares the hardware path with the table path directly on every length 0–300 from every block offset, with three starting states, and on 4 KiB–64 KiB inputs. The existing tests (RFC 3720 vectors, bit-at-a-time reference, split streams) now run through the hardware path on aarch64 and the table path elsewhere. Flipping the hardware path's byte order fails six tests. Files are byte-identical. `cargo test --workspace` passes; differential fuzz `diff_ops` 36,037 runs in 10 min and `fuzz_image_open` 6.8 M runs in 2 min, no finding, both on the hardware path. MSRV 1.98 builds and passes the CRC tests. |
| **Safe** | One `unsafe` call, sanctioned by ADR-0031 and listed in `AGENTS.md` and `lib.rs`. Its only precondition is the CPU feature, detected on the line above it. No pointer, aliasing, alignment or atomics reasoning. Miri passes on all `zerodb-core` tests, on the table path (miri cannot run the intrinsic). Clippy clean on macOS aarch64, x86-64 Linux and nightly. |
| **Durable** | Commit ordering and barriers unchanged; same checksum value and coverage. Crash smoke passes: 229 single-process cycles (15 image cuts, 47 SIGKILL) and 57 multi-process cycles. |
| **Secure** | Torn and hostile meta pages are detected exactly as before: same CRC, same coverage, annex count still bounds-checked before the CRC reads it. `fuzz_image_open` covers hostile meta pages. |

## Not measured

- Graviton. The instruction is the same ARMv8 one; its throughput there is
  not measured here.
- x86-64 keeps the table path (ADR-0031 Option C not taken).

## Rebased on main (2026-10-20)

`e6d69fa`: the same hardware path on `main`'s slicing-by-8 table (#117),
against `main` (`2013c13`). Mac, during heavy load from other work (load
15–22 on 8 cores), so absolute numbers are inflated; read the paired ratios.

**Delete repro** (as above, 7 interleaved runs, µs per commit):

| LMDB | `main` (slicing-by-8) | rebased (hardware) |
|---:|---:|---:|
| 10.49 | 21.96 | 17.19 |

Paired per run, rebased ÷ `main`: 0.677, 0.742, 0.803, 0.789, 0.768,
0.727, and one run where the rebased side stalled at 907 µs per commit (a
host hiccup, dropped). CRC32C of 16,380 bytes: 10.53 µs (`main`) → 2.44 µs.
Against slicing-by-8 the hardware path gains more than it did against
slicing-by-16 (0.876).

**Criterion** (`scripts/bench-ab.sh`, 3 rounds, base `2013c13`, load 14):
invalid, every rung over the drift gate. `del/random/n1` read 0.772
(±0.288, LMDB drift 0.920), `commit/batch/n1` 1.020 (±0.557, drift 0.842),
`env/txn/rw_empty_commit` 0.883 (±0.302, drift 1.039). `commit/batch/n1`
took 161 ms for LMDB against 7–25 ms on a normal day, a measure of how loaded
the host was.

**Checks on the rebased tree:** fmt; clippy on macOS, x86-64 Linux and
nightly; MSRV 1.98 CRC tests; `cargo test --workspace` (exit 0); crash smoke
(214 single-process and 57 multi-process cycles); `diff_ops` fuzz (20,910
runs in 10 min) and `fuzz_image_open` (2.7 M runs in 2 min), no finding;
miri on `zerodb-core` (12 test binaries, 0 failures).
