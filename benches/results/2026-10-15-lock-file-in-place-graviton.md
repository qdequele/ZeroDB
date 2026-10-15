# Lock file rebuilt in place: env open on Linux (ADR-0023 amendment 20)

- **Date:** 2026-10-15.
- **Change:** when no other process has an environment open, the opening process rebuilds the lock file in place instead of truncating it to zero (ADR-0023 amendment 20, issue #94).
- **Branch:** `qdequele/adr-0023-amendment-20`, commits `877cffe` (implementation) and `5e4e3bf` (review fixes, tests and docs only).
- **Before:** `main` at `f035790`, which has the lock file on by default.

## Host and method

- AWS Graviton4 `c8g.xlarge` (4 vCPU, 8 GiB), Ubuntu 24.04 arm64, eu-west-1. It was launched for this run and terminated after it.
- Filesystem: ext4 on a gp3 root volume. The 2026-10-13 stage-3 ladder ran on an io2 `/data` volume instead, so the "before" numbers here differ from that report's: the truncation costs more on this volume.
- Driver: `scripts/bench-ab.sh` with `BASE=main` and `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`. It runs LMDB, ZeroDB before and ZeroDB after with the harness held fixed, in interleaved rounds.
- The lock file is on in every ZeroDB column (the default).
- The `env/open/*` rungs open `N_OPEN = 20` environments per iteration. The per-open figures below divide by 20.

## The claim: `env/*`, 5 rounds

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `env/open/create` | 2.109 ms | 83.765 ms | 61.264 ms | 39.71× → **29.05×** | 0.731 | 1.007 | faster |
| `env/open/reopen` | 756.697 µs | 20.330 ms | 727.054 µs | 26.87× → **0.96×** | 0.036 | 0.999 | faster |
| `env/stat/non_free` | 234.0 ns | 111.9 ns | 112.8 ns | 0.48× → 0.48× | 1.009 | 0.999 | flat |
| `env/txn/ro_begin_abort` | 1.042 ms | 736.126 µs | 735.378 µs | 0.71× → 0.71× | 0.999 | 1.014 | flat |
| `env/txn/rw_empty_commit` | 54.408 µs | 180.662 µs | 181.517 µs | 3.32× → 3.34× | 1.001 | 1.003 | flat |

Per open:

| | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| reopen an existing env | 37.8 µs | 1016 µs | **36.4 µs** |
| create a new env | 105 µs | 4.19 ms | 3.06 ms |

- **Reopen is at parity with LMDB** (0.96×). This was the cost #94 tracked.
- **Create improved by 1.13 ms per env but is still 29× LMDB.** The lock file is not what is left: with `NO_LOCK`, create was already 9.0× LMDB on 2026-10-13 (`2026-10-13-multi-process-stage3-graviton.md`). The remaining gap is the data file's creation, which `docs/PERF-GAP-VS-LMDB.md` tracks separately.

## No other rung moves: a sample, 3 rounds

Run at `5e4e3bf`, whose engine code is identical to `877cffe`.

| rung | LMDB | ZeroDB before | ZeroDB after | after ÷ before | verdict |
|---|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.354 ms | 10.467 ms | 10.428 ms | 0.998 | flat |
| `commit/batch/n100` | 6.308 ms | 6.963 ms | 6.935 ms | 0.995 | flat |
| `commit/batch/n10k` | 14.008 ms | 13.356 ms | 13.321 ms | 0.997 | flat |
| `del/bulk/all` | 13.849 ms | 22.972 ms | 23.139 ms | 1.007 | flat |
| `get/access/hot` | 1.257 ms | 939.152 µs | 935.676 µs | 0.996 | flat |
| `get/val/v8` | 1.798 ms | 1.471 ms | 1.474 ms | 1.002 | flat |
| `get/val/v8_touch` | 1.802 ms | 1.482 ms | 1.478 ms | 0.997 | flat |
| `put/api/plain` | 14.712 ms | 13.946 ms | 13.968 ms | 1.002 | flat |

The change touches only the lock file's initialization at open, so no per-transaction or per-operation path runs it.

## Tests on the same host

- At `877cffe`, all exit 0:
  - clippy (`-D warnings`);
  - `cargo test --workspace`;
  - `multi_process`: 28/28 in each of 3 runs;
  - the lock-file process tests (`lockfile_processes`);
  - `just crash-test-mp 500`;
  - `just stress` (both 180 s runs).
- At `5e4e3bf`: `cargo test -p zerodb-io` passed, including the step-order check added in review.

## macOS (development machine, not a gate)

A throwaway spike, opening and closing one env in a loop on APFS, gave 220–238 µs per open before the change and 104–113 µs after; `NO_LOCK` gave 48–55 µs. The rest of the cost is the lock file's own system calls.
