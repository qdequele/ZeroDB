# ADR-0023 stage 3 on Graviton4: Linux aarch64 gate, consumer suites and full ladder

Date: 2026-10-13 (runs 19:18–23:46 UTC). Branch `qdequele/adr-0023-stage3` at
`1deebf3`, with the lock file on by default and `NO_LOCK` as the opt-out.
Before = `main` `7002d6e` (v0.2.0).

## Host

- AWS c8g.xlarge, Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB RAM, no swap.
- Ubuntu 24.04, kernel 7.0.0-1014-aws, aarch64, 4 KiB OS pages.
- Repo, target dirs, `TMPDIR` and bench temp dirs on `/data` (EBS io2, ext4).
- rustc 1.99.0 (stable). Every test and suite step ran under `ulimit -f 4194304`.
- Background timers (apt, sysstat, snapd, fwupd, man-db) were stopped earlier (Task 10) and stayed stopped.

## Linux gate (aarch64, native)

| step | result |
|---|---|
| `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| `cargo test --workspace` | 988 passed, 0 failed, 0 ignored (213 test binaries) |
| `cargo test -p zerodb --test multi_process`, 5 runs | 28/28 passed in each of the 5 runs |
| `lmdb_stage3_observation` | 39 passed (as `ubuntu`; the read-only-media observations print `SKIPPED` here, see the root run below) |
| `lmdb_reaping_observation` | 8 passed |
| `multi_process_differential` (the stage-1/2/3 differentials against LMDB) | 27 passed |
| `thousand_sequential_envs_leak_no_descriptors` | passed. On Linux it counts `/proc/self/fd`, and the child reported `1 passed`. |
| `just stress`: `reader_stress` (180 s) | 3 passed: 810 commits (810 spilled), 3219 commits (WRITE_MAP in place) and 587 commits (587 spilled), about 2,600 reader double-walks each |
| `just stress`: `multi_process_stress` (180 s) | `stress_two_processes` passed |
| `just crash-test-mp 2000` | 2003 mp cycles, all verified; 0 abandoned. Modes: default 799, mapasync 308, nometasync 283, nosync 293, writemap 320. 3046 spilling txns. 774.9 s. |

### Read-only bind mounts, run as root so they really run

The compiled test binaries were run with `sudo` (CAP_SYS_ADMIN), so the
`mount --bind` + `remount,bind,ro` path is taken instead of `SKIPPED`:

| test | result |
|---|---|
| engine: `multi_process::read_only_env_on_read_only_media_opens_without_a_lock_file` | ok, no `SKIPPED` line |
| heed adapter: `env_file_naming::read_only_media_env_has_no_lock_file_and_does_not_report_no_lock` | ok, no `SKIPPED` line |
| tools: `read_only_media::read_tools_work_on_read_only_media_without_a_lock_file` | ok, no `SKIPPED` line |
| oracle: the whole `lmdb_stage3_observation` as root | 39 passed, no `SKIPPED` line. It printed `MDB_RDONLY on a read-only filesystem, no lock.mdb: rc 0` and `MDB_RDONLY on a read-only filesystem: flags 0x220000, NOLOCK false`. The uid-drop observations also ran as root. |

## Consumer gate (lock file on, the default)

| suite | ZeroDB | stock LMDB | note |
|---|---|---|---|
| hannoy `v0.1.7-nested-rtxns` (`scripts/hannoy.sh suites`) | 34 passed, 10 failed, 1 ignored | 34 passed, 10 failed, 1 ignored, same 10 tests | Every failure is an insta snapshot diff of one line: `Version { … patch: 6 }` vs `patch: 7`. The tag bumped the crate version without refreshing its snapshots. This was already recorded in `2026-09-09-hannoy-v0.1.7-macos.md`, and it is not engine behavior. |
| index-scheduler v1.53.1 | 80 passed | (not rerun) | Not run on stock LMDB: see the note below the table. |
| milli v1.53.1 `--lib` | 270 passed, 3 ignored, 1 skipped (`filter_depth`, see below) | OOM-killed too (see below) | |
| milli v1.53.1 `tests/` | 92 passed | | |
| doc-tests (milli, index-scheduler) | 2 + 1 passed, 1 ignored | | |

`lmdb-master-sys` is absent from the dependency trees of milli and
index-scheduler with the shim patched in, as asserted by the script.

The stock-LMDB runs of the Meilisearch suites were made only to check
whether the milli lib OOM below depends on the engine; the task record gives
no other reason, and index-scheduler, milli `tests/` and the doc-tests were
not rerun on stock LMDB. That is acceptable for this gate: ZeroDB passed
every test it ran there, so there is no failure for a stock-LMDB baseline to
explain. The baseline matters only where a suite fails, as for hannoy above.

What had to change to run the suites on this host. No test or engine source
was changed.

1. **Toolchain.** `scripts/consumer.sh suites` fails at once under Meilisearch
   v1.53.1's `rust-toolchain.toml` (1.91.1). The error is `rustc 1.91.1 is not
   supported … heed@0.22.1 (the shim) requires rustc 1.98`, because ZeroDB's
   MSRV went to 1.98 in #90. The suites ran with `RUSTUP_TOOLCHAIN=stable`
   (1.99.0).
2. **Memory, linking.** With 4 build jobs, `ld` for the milli test binary was
   OOM-killed. dmesg: `Out of memory: Killed process (ld) anon-rss:2351996kB`.
   The run used `CARGO_BUILD_JOBS=1`.
3. **Memory, one test.** The milli lib test binary is OOM-killed (rc 137) on
   **both engines**:
   - ZeroDB: `anon-rss:7406648kB` with 4 test threads, `7507132kB` with 1.
   - stock LMDB: `anon-rss:7439864kB` with 4 test threads, `7475952kB` with 1.
   - Single-threaded, both die in `search::facet::filter::tests::filter_depth`.
     That test builds a 2 MiB filter string of 14,361 `OR` clauses and parses
     it, with no index or storage involved.

   The final run therefore passed `--skip search::facet::filter::tests::filter_depth`
   to `cargo test -p milli -p index-scheduler`, with the shim patched in
   exactly as `consumer.sh` does it. The test needs a host with more than
   8 GiB of RAM. Its outcome does not depend on the engine.

## Full three-column ladder

- **Builds:** one worktree and one `CARGO_TARGET_DIR` per checkout: `/data/wt-main` → `/data/tgt-main` and `/data/wt-h13` → `/data/tgt-h13`. The bench profile used `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`. The binaries were copied to `/data/bin/{main,h13}`.
- **Harness:** the ruler is held fixed, as `bench-ab.sh` does it. HEAD's `crates/zerodb-oracle/benches` and its `Cargo.toml` are overlaid on main; the only harness difference is the `ZERODB_BENCH_NO_LOCK` axis.
- **Driver:** `/data/rounds13.sh`, 3 interleaved rounds of the whole default-tier ladder (60 rungs, both engines). Each round runs main, HEAD (lock on, the default) and HEAD with `ZERODB_BENCH_NO_LOCK=1`, rotating which side starts. About 22 min per side.
- **Analysis:** `scripts/bench-ab.py`, once per variant against the same main measurements. Raw data is in `/data/results/ladder13b-20261013T203045Z/` on the host.
- **The axis took effect.** `strace` of the HEAD binary on one rung counted the `lock.mdb` opens:
  - default: 35 ZeroDB `lock.mdb` opens (mode 0666) and 35 LMDB ones (0600);
  - `ZERODB_BENCH_NO_LOCK=1`: 0 ZeroDB opens and 35 LMDB ones.
  - The main binary has 0 `lockfile` symbols; HEAD has 33.

### Verdicts

- **Lock on (default) vs main: `regressed`.** 5 regressed, 55 flat, 0 unreliable (no LMDB drift above 3 %).
- **NO_LOCK vs main: `flat`.** 60 flat, 0 regressed, 0 unreliable.

### Per-operation view of the regressed rungs (lock on)

| rung | unit | LMDB | main | HEAD on | delta vs main |
|---|---|---:|---:|---:|---:|
| `env/txn/rw_empty_commit` | per txn | 54.5 ns | 156.4 ns | 186.2 ns | +29.8 ns (1.185). Budgeted; Task 10 had +27.6 ns. |
| `env/txn/ro_begin_abort` | per txn | 101.7 ns | 67.5 ns | 74.3 ns | +6.8 ns (1.100). Budgeted; Task 10 had +6.4 ns. |
| `env/stat/non_free` | per call | 234 ns | 105 ns | 112 ns | +7 ns (1.063): `stat` reads the shared `T` (Task 9) |
| `env/open/create` | per create | 103 µs | 925 µs | 1081 µs | +155 µs (1.168) |
| `env/open/reopen` | per open | 36 µs | 17.1 µs | 347 µs | **+330 µs (20.3×; 9.6× LMDB)** |

**`env/open/*` with the lock file on is a finding, not part of the ADR's
per-transaction budget.**

- Each open of an env that no other process has open makes this process the first opener (initializer). Per ADR-0023 amendment 10, the initializer:
  - truncates the lock file to 0;
  - `pwrite`s the initializing magic;
  - extends the file with `set_len`;
  - writes every reader slot.
- On ext4 that frees the file's blocks and allocates them again on every open. A `perf` profile of the rung shows the cost:
  - `LockFile::open` is 12.8 % of samples. It goes through `ext4_buffered_write_iter` / `ext4_da_write_begin` and `do_truncate` / `ext4_setattr`.
  - The drop adds another 3.9 % (`munmap`, `ext4_evict_inode`).
- macOS shows the same shape: reopen takes 5.39 ms per 20 opens with the lock on and 1.49 ms with `NO_LOCK`. `ftruncate` is the top self-time symbol in `sample`.
- LMDB's first opener does not truncate. It reinitializes the header in place and grows the file only when it is too short.
- Matching LMDB here changes the initialization protocol of amendment 10 (its crash-safety argument rests on truncate-then-magic). Rule 5 says that needs an ADR amendment first, so it was not changed in this task.
- For Meilisearch the cost is about 0.3 ms per index open, against index opens that take milliseconds.

### Full table

`÷ main` is HEAD time ÷ main time (median of 3 per-round ratios). `vs LMDB`
is HEAD ÷ LMDB. LMDB and main columns are from the lock-on pairing. The NO_LOCK
pairing uses the same main measurements.

| rung | LMDB | main | HEAD lock on | on ÷ main | on vs LMDB | HEAD NO_LOCK | off ÷ main | off vs LMDB | verdict on / off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.254 ms | 10.118 ms | 10.314 ms | 1.020 | 1.42× | 10.201 ms | 1.009 | 1.41× | flat / flat |
| `commit/batch/n100` | 5.963 ms | 6.540 ms | 6.577 ms | 1.008 | 1.10× | 6.575 ms | 1.002 | 1.10× | flat / flat |
| `commit/batch/n10k` | 13.015 ms | 12.422 ms | 12.455 ms | 1.004 | 0.96× | 12.469 ms | 1.004 | 0.96× | flat / flat |
| `commit/sync/n1` | 282.834 ms | 245.209 ms | 245.200 ms | 1.000 | 0.87× | 245.090 ms | 1.000 | 0.87× | flat / flat |
| `commit/sync/n100` | 434.144 ms | 438.634 ms | 438.706 ms | 1.000 | 1.01× | 438.562 ms | 1.000 | 1.01× | flat / flat |
| `del/bulk/all` | 13.490 ms | 22.961 ms | 22.830 ms | 0.993 | 1.69× | 22.792 ms | 0.993 | 1.68× | flat / flat |
| `del/bulk/half` | 6.873 ms | 11.635 ms | 11.551 ms | 0.990 | 1.68× | 11.527 ms | 0.990 | 1.68× | flat / flat |
| `del/churn/reinsert` | 8.608 ms | 12.773 ms | 12.688 ms | 0.995 | 1.47× | 12.686 ms | 0.995 | 1.47× | flat / flat |
| `del/clear/all` | 106.015 µs | 192.024 µs | 195.887 µs | 1.009 | 1.85× | 195.574 µs | 1.011 | 1.87× | flat / flat |
| `del/cursor/drain` | 4.995 ms | 14.536 ms | 14.570 ms | 1.002 | 2.92× | 14.558 ms | 1.002 | 2.91× | flat / flat |
| `del/range/half` | 5.014 ms | 4.228 ms | 4.168 ms | 0.986 | 0.83× | 4.178 ms | 0.995 | 0.83× | flat / flat |
| `env/open/create` | 2.058 ms | 18.507 ms | 21.611 ms | 1.168 | 10.50× | 18.591 ms | 1.005 | 9.04× | ❌ slower / flat |
| `env/open/reopen` | 719.117 µs | 342.063 µs | 6.936 ms | 20.264 | 9.65× | 347.399 µs | 1.016 | 0.49× | ❌ slower / flat |
| `env/stat/non_free` | 234.1 ns | 105.4 ns | 112.5 ns | 1.063 | 0.48× | 103.6 ns | 0.983 | 0.44× | ❌ slower / flat |
| `env/txn/ro_begin_abort` | 1.017 ms | 674.774 µs | 742.811 µs | 1.100 | 0.73× | 676.693 µs | 1.001 | 0.66× | ❌ slower / flat |
| `env/txn/rw_empty_commit` | 54.462 µs | 156.366 µs | 186.187 µs | 1.185 | 3.42× | 152.887 µs | 0.973 | 2.80× | ❌ slower / flat |
| `get/access/hot` | 1.263 ms | 936.719 µs | 944.922 µs | 1.008 | 0.75× | 950.044 µs | 1.015 | 0.75× | flat / flat |
| `get/access/miss` | 857.540 µs | 914.037 µs | 909.325 µs | 0.994 | 1.06× | 907.651 µs | 0.995 | 1.06× | flat / flat |
| `get/access/rand` | 2.544 ms | 2.319 ms | 2.323 ms | 1.004 | 0.91× | 2.338 ms | 1.008 | 0.91× | flat / flat |
| `get/access/seq` | 1.422 ms | 1.236 ms | 1.235 ms | 1.000 | 0.87× | 1.235 ms | 0.998 | 0.87× | flat / flat |
| `get/db/named` | 2.549 ms | 2.320 ms | 2.339 ms | 1.010 | 0.92× | 2.341 ms | 1.013 | 0.92× | flat / flat |
| `get/db/named_x8` | 2.250 ms | 2.185 ms | 2.191 ms | 1.006 | 0.97× | 2.191 ms | 1.006 | 0.98× | flat / flat |
| `get/db/root` | 2.542 ms | 2.347 ms | 2.371 ms | 1.012 | 0.93× | 2.363 ms | 1.005 | 0.93× | flat / flat |
| `get/key/k128` | 3.076 ms | 3.614 ms | 3.666 ms | 1.018 | 1.19× | 3.637 ms | 1.010 | 1.20× | flat (layout offset, threshold widened) / flat |
| `get/key/k32` | 2.655 ms | 2.870 ms | 2.885 ms | 1.006 | 1.09× | 2.878 ms | 1.007 | 1.09× | flat / flat |
| `get/key/k8` | 2.554 ms | 2.333 ms | 2.363 ms | 1.013 | 0.93× | 2.342 ms | 1.006 | 0.92× | flat / flat |
| `get/size/n1k` | 1.357 ms | 1.207 ms | 1.221 ms | 1.011 | 0.90× | 1.220 ms | 1.010 | 0.90× | flat / flat |
| `get/size/n50k` | 2.548 ms | 2.316 ms | 2.331 ms | 1.006 | 0.91× | 2.328 ms | 1.005 | 0.92× | flat / flat |
| `get/val/v256` | 2.177 ms | 1.780 ms | 1.805 ms | 1.014 | 0.83× | 1.806 ms | 1.015 | 0.83× | flat / flat |
| `get/val/v2page` | 1.842 ms | 1.747 ms | 1.785 ms | 1.021 | 0.97× | 1.759 ms | 1.009 | 0.95× | flat / flat |
| `get/val/v2page_touch` | 2.131 ms | 1.850 ms | 1.833 ms | 0.991 | 0.86× | 1.837 ms | 0.990 | 0.87× | flat / flat |
| `get/val/v4k` | 1.828 ms | 1.745 ms | 1.776 ms | 1.020 | 0.97× | 1.762 ms | 1.009 | 0.96× | flat / flat |
| `get/val/v4k_touch` | 2.106 ms | 1.843 ms | 1.833 ms | 0.995 | 0.87× | 1.827 ms | 0.991 | 0.87× | flat / flat |
| `get/val/v8` | 1.797 ms | 1.473 ms | 1.480 ms | 1.005 | 0.82× | 1.480 ms | 1.006 | 0.82× | flat / flat |
| `get/val/v8_touch` | 1.803 ms | 1.487 ms | 1.486 ms | 0.999 | 0.82× | 1.483 ms | 0.997 | 0.82× | flat / flat |
| `maint/copy/compact` | 2.493 ms | 3.465 ms | 3.491 ms | 1.002 | 1.40× | 3.472 ms | 1.002 | 1.39× | flat / flat |
| `maint/copy/raw` | 2.863 ms | 2.161 ms | 2.137 ms | 0.996 | 0.75× | 2.125 ms | 0.992 | 0.74× | flat / flat |
| `mixed/rw/8dbs` | 39.841 ms | 36.087 ms | 35.753 ms | 1.002 | 0.90× | 35.890 ms | 1.007 | 0.91× | flat / flat |
| `put/api/plain` | 14.058 ms | 13.139 ms | 13.077 ms | 0.997 | 0.93× | 13.089 ms | 0.995 | 0.93× | flat / flat |
| `put/api/reserved` | 14.020 ms | 14.779 ms | 14.722 ms | 0.997 | 1.05× | 14.708 ms | 1.000 | 1.05× | flat / flat |
| `put/gc/drain_big` | 21.218 ms | 24.044 ms | 23.958 ms | 0.998 | 1.13× | 23.903 ms | 0.989 | 1.13× | flat / flat |
| `put/order/append` | 8.463 ms | 10.732 ms | 10.704 ms | 0.997 | 1.26× | 10.791 ms | 1.002 | 1.28× | flat / flat |
| `put/order/rand` | 30.859 ms | 28.706 ms | 28.915 ms | 1.014 | 0.94× | 28.600 ms | 0.990 | 0.93× | flat / flat |
| `put/order/seq` | 14.009 ms | 12.993 ms | 13.010 ms | 1.003 | 0.93× | 12.994 ms | 0.993 | 0.93× | flat / flat |
| `put/over/grow` | 7.578 ms | 9.383 ms | 9.356 ms | 0.996 | 1.23× | 9.420 ms | 1.005 | 1.24× | flat / flat |
| `put/over/same_size` | 2.750 ms | 2.938 ms | 2.943 ms | 1.002 | 1.07× | 2.948 ms | 1.006 | 1.08× | flat / flat |
| `put/val/v256` | 2.515 ms | 2.353 ms | 2.356 ms | 1.001 | 0.94× | 2.345 ms | 0.997 | 0.93× | flat / flat |
| `put/val/v2page` | 5.795 ms | 5.868 ms | 5.860 ms | 1.001 | 1.01× | 5.852 ms | 0.998 | 1.00× | flat / flat |
| `put/val/v4k` | 23.052 ms | 23.246 ms | 23.024 ms | 0.990 | 1.00× | 23.023 ms | 0.998 | 1.00× | flat / flat |
| `put/val/v8` | 1.412 ms | 1.398 ms | 1.405 ms | 1.005 | 1.00× | 1.403 ms | 1.003 | 0.99× | flat / flat |
| `scan/edge/first_last` | 984.387 µs | 1.649 ms | 1.661 ms | 1.007 | 1.69× | 1.657 ms | 1.011 | 1.69× | flat / flat |
| `scan/full/fwd` | 481.092 µs | 980.614 µs | 972.580 µs | 0.992 | 2.02× | 972.153 µs | 0.991 | 2.02× | flat / flat |
| `scan/full/rev` | 478.504 µs | 934.551 µs | 940.906 µs | 1.005 | 1.97× | 938.126 µs | 1.003 | 1.96× | flat / flat |
| `scan/meta/len` | 43.915 µs | 33.334 µs | 33.592 µs | 1.010 | 0.76× | 33.789 µs | 1.008 | 0.77× | flat / flat |
| `scan/prefix/bucket` | 3.400 µs | 5.859 µs | 5.929 µs | 1.006 | 1.74× | 5.887 µs | 1.005 | 1.73× | flat / flat |
| `scan/range/10pct` | 69.239 µs | 94.040 µs | 93.744 µs | 0.997 | 1.35× | 94.969 µs | 1.009 | 1.37× | flat / flat |
| `scan/range/1pct` | 7.711 µs | 10.168 µs | 10.161 µs | 0.999 | 1.32× | 10.155 µs | 0.997 | 1.32× | flat / flat |
| `seek/ge/gap` | 2.858 ms | 3.182 ms | 3.194 ms | 1.004 | 1.12× | 3.182 ms | 1.000 | 1.11× | flat / flat |
| `seek/ge/rand` | 2.718 ms | 2.523 ms | 2.538 ms | 1.001 | 0.93× | 2.539 ms | 1.006 | 0.94× | flat / flat |
| `seek/ge/seq` | 1.769 ms | 1.756 ms | 1.758 ms | 1.002 | 0.99× | 1.761 ms | 1.004 | 1.00× | flat / flat |

## Reproduce

On the host, from `/data`:

1. Build each checkout into its own target dir: `build.sh main` and `build.sh h13`. This uses `/data/wt-<name>` and `/data/tgt-<name>`, sets `CODEGEN_UNITS=1`, and copies the binary to `/data/bin/<name>`.
2. Run the rounds: `rounds13.sh <out> 3 "" main h13 h13:nolock`.
3. Lay out the bench-ab directories for each variant (`main-def` as before, `h13-def` or `h13-nolock` as after) and run `scripts/bench-ab.py`.

The gate driver is `/data/gate13b.sh`, with logs in `/data/results/gate13b/`.
The consumer logs are in `/data/results/consumer13b/`.
