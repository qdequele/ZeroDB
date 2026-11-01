# Meta CRC on the commit path: the Graviton rerun

> **Rebased 2026-10-20.** `main` took its own slicing-by-8 table in #117
> while this work was in review, so the slicing-by-16 commit measured here
> was dropped when the branch was rebased; `main`'s slicing-by-8 is the
> software path. The hardware CRC32C (ADR-0031, numbered 0029 when these
> runs were made) and the `del/random/n1` rung were kept. Commit hashes in
> this report are from the pre-rebase branch and are not in `main`'s
> history.
>
> Here the middle column ("slicing-by-16") is the dropped variant; `main`'s
> slicing-by-8 sits between it and the original. The rebased build was not
> rerun on Graviton.

- **Date:** 2026-10-19.
- **Question:** the two meta CRC changes, slicing-by-16 (`9f26931`) and the
  ARMv8 hardware CRC32C (`b2f5b6c`, ADR-0031), were measured on a busy Mac,
  and the hardware step's criterion runs there were invalid. Do they hold on
  the primary target, and is the hardware path really taken on Linux, where
  the CPU feature is detected at run time? This answers ADR-0031's open
  question 3.
- **ZeroDB:** `f98cd08` (original byte-at-a-time CRC), `0e98443`
  (slicing-by-16) and `ef7ee3a` (= `b2f5b6c` plus docs; hardware CRC).
- **Host:** AWS c8g.xlarge, Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB, Ubuntu
  24.04, kernel 7.0, 4 KiB OS pages, 60 GiB gp3 root. Launched for this run
  and terminated after it. Nothing else ran on it; load 1–1.5 during the
  criterion runs.

## The hardware path on Linux aarch64

`rustc` for `aarch64-unknown-linux-gnu` does not enable the `crc` feature at
compile time (`cfg!(target_feature = "crc")` is false), and run-time
detection finds it (`is_aarch64_feature_detected!("crc")` is true). So the
dispatch in `crc32c.rs` takes the hardware path, as ADR-0031 intended. A
compile-time-only path would never have run here. On this host
`hardware_path_matches_table_path` runs its comparisons, and all 11 CRC
tests pass. `cargo test --workspace` passes: 219 suites, 0 failures.

## CRC32C alone

16,380 bytes (the CRC span of a full 16 KiB meta page), median of 5:

| original (byte at a time) | slicing-by-16 | hardware |
|---:|---:|---:|
| 41.04 µs | 5.66 µs | 0.75 µs (22 GB/s) |

Graviton4 runs the instruction about 2.7× faster than the Mac's M-series
core (1.99 µs).

## Delete repro

200,000 random 26-byte keys with 1,500-byte values, `WRITE_MAP | MAP_ASYNC |
NO_SYNC`, trusted-file mode, every key deleted in shuffled order, one delete
per write transaction. 5 interleaved runs, build order alternating, median
µs per commit. LMDB uses its own 4 KiB pages. Run to run spread under 2 %.

| ZeroDB page size | LMDB | original | slicing-by-16 | hardware | slicing ÷ original | hardware ÷ slicing |
|---|---:|---:|---:|---:|---:|---:|
| 4 KiB (the Linux default) | 4.49 | 14.46 | 9.94 | 9.11 | 0.69 | 0.91 |
| 16 KiB | – | 34.07 | 15.18 | 12.53 | 0.45 | 0.83 |

At 4 KiB pages the annex holds at most 490 ids (about 4 KB), so the CRC is a
smaller share of the commit than at 16 KiB (2,026 ids). Both steps still
pay. Together they make a single-delete commit 1.59× faster at 4 KiB and
2.72× at 16 KiB.

## Criterion

`scripts/bench-ab.sh`, 5 interleaved rounds, base `f98cd08`, candidate
`ef7ee3a` (both steps), page size = the OS page, 4 KiB:

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.330 ms | 11.178 ms | 10.246 ms | 1.52× → **1.40×** | 0.915 (±0.030) | 0.996 | faster |
| `commit/batch/n100` | 6.076 ms | 7.071 ms | 6.958 ms | 1.16× → 1.15× | 0.986 (±0.061) | 1.000 | flat |
| `commit/batch/n10k` | 13.191 ms | 13.227 ms | 13.235 ms | 1.00× → 1.00× | 0.995 (±0.193) | 0.997 | flat |
| `commit/sync/n1` | 395.067 ms | 388.384 ms | 387.767 ms | 0.98× → 0.98× | 1.000 (±0.030) | 1.001 | flat |
| `commit/sync/n100` | 770.083 ms | 770.906 ms | 769.698 ms | 1.00× → 1.00× | 0.998 (±0.030) | 1.000 | flat |
| `commit/sync/n1_map16g` | 394.839 ms | 387.562 ms | 387.422 ms | 0.98× → 0.98× | 1.000 (±0.030) | 0.998 | flat |
| `del/random/n1` | 748.304 ms | 1.504 s | 1.229 s | 2.01× → **1.64×** | 0.814 (±0.039) | 0.995 | faster |
| `env/txn/ro_begin_abort` | 797.665 µs | 778.939 µs | 781.980 µs | 0.98× → 0.98× | 1.001 (±0.065) | 0.965 | flat |
| `env/txn/rw_empty_commit` | 54.481 µs | 193.365 µs | 192.892 µs | 3.55× → 3.54× | 0.993 (±0.030) | 0.972 | flat |

Verdict: improved, 2 faster, 7 flat, none regressed, none over the drift
gate.

A separate 5-round run of the hardware step alone (`0e98443` → `ef7ee3a`)
on `del/random/n1` was unreliable: 1.011 (±0.155), LMDB drift 0.968. It
started right after the workspace test suite, and two of its rounds repeat
the previous round's values to the millisecond, which points at the harness
rather than the engine. The repro above measures that step directly.

## crud-bench

500,000 `string26` keys, 128 clients, 48 threads, `--optimised`
(`WRITE_MAP | MAP_ASYNC | NO_SYNC`, trusted-file mode), OS page size (4 KiB).
On the delete-rebalance branch: ADR-0028 `0bd9bd7` before, the same plus
both CRC steps after. Two runs of 3 rounds each, medians, ops/s:

| metric | run | LMDB (before) | LMDB (after) | ZeroDB before | ZeroDB after | after ÷ before | ZeroDB/LMDB before → after |
|---|---|---:|---:|---:|---:|---:|---:|
| Create | 1 | 196,392 | 197,861 | 115,064 | 122,010 | 1.06× | 0.59× → 0.62× |
| Create | 2 | 197,916 | 199,005 | 112,153 | 126,087 | 1.12× | 0.57× → 0.63× |
| Update | 1 | 340,043 | 346,616 | 132,748 | 146,065 | 1.10× | 0.39× → 0.42× |
| Update | 2 | 340,398 | 340,968 | 135,449 | 146,474 | 1.08× | 0.40× → 0.43× |
| Delete | 1 | 155,649 | 144,749 | 62,924 | 90,875 | **1.44×** | 0.40× → **0.63×** |
| Delete | 2 | 144,801 | 136,935 | 62,728 | 93,159 | **1.49×** | 0.43× → **0.68×** |

ZeroDB deletes per round: before 63,132 / 62,924 / 62,735 and 60,836 /
62,728 / 62,913; after 90,875 / 89,137 / 93,490 and 93,159 / 94,133 /
91,627.

In run 1, `batch_update_1000` and `batch_delete_1000` came out at 0.87× and
0.88×: two of the three after-rounds dipped by about 14 % and the third
matched before. Run 2 did not reproduce it (0.99× and 1.00×, with a dip on
a before-round this time). The criterion multi-op commit rungs
(`commit/batch/n100`, `n10k`) are flat on the same host, and the hardware
CRC is faster at every input size, so this reads as noise. Read and scan
rows move by 0–5 % in both runs, with LMDB moving as much. Single-process
reads do not recompute the meta CRC.

## What the Graviton numbers show beyond this change

- **ZeroDB writes trail LMDB much more on Linux than on the Mac.** On the
  Mac, crud-bench creates and updates were at 0.91–0.98× LMDB. Here they are
  at 0.6× and 0.4×, and deletes at 0.65× even after this change. From the
  Mac to Graviton (both with both CRC steps), LMDB's creates, updates and
  deletes run 3.1×, 5.1× and 2.1× faster, ZeroDB's only 2.2×, 2.4× and
  1.5× (`2026-10-19-meta-crc-hardware.md` for the Mac figures).
- `env/txn/rw_empty_commit` is 3.55× LMDB here, 1.65× on the Mac: an empty
  write transaction costs about 190 ns against LMDB's 54 ns.
- `start(5000) limit` scans are at 0.48× LMDB, `count()` at 0.71×.

These are not caused by the CRC (both before and after show them). They are
listed for the next profiling pass.

## Scorecard

| | |
|---|---|
| **Faster** | On Graviton4, both steps together: `del/random/n1` 2.01× → 1.64× LMDB (0.814 ±0.039), `commit/batch/n1` 0.915 (±0.030); single-delete commit 14.46 → 9.11 µs at 4 KiB pages, 34.07 → 12.53 µs at 16 KiB; crud-bench deletes 1.44–1.49×, creates 1.06–1.12×, updates 1.08–1.10×. Hardware step alone: 0.91 (4 KiB) and 0.83 (16 KiB) of the slicing commit. |
| **Behavior** | Identical. The hardware path is taken on Linux aarch64 through run-time detection, and the test comparing it with the table path passes there. Workspace tests pass on Graviton (219 suites). No criterion rung regressed. |
| **Safe** | No new `unsafe` beyond ADR-0031's one call, whose precondition (the `crc` feature) is detected on this CPU. |
| **Durable** | `commit/sync/*` flat (1.000, 0.998, 1.000): barriers and ordering unchanged. |
| **Secure** | Same checksum, same coverage; nothing here changes how torn or hostile meta pages are detected. |
