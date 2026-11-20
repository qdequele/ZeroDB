# ZeroDB page-size matrix, 2026-11-20T09:27:05Z to 2026-11-20T10:28:35Z

OS page size: **16384 bytes**. Command: `scripts/page-size-matrix.sh` (CRASH_CYCLES=2000, CRASH_MP_CYCLES=200, STRESS_SECS=180, FUZZ_SECS=600).

- Host: `Darwin 27.2.0 Darwin Kernel Version 27.2.0: Thu Oct 15 00:17:32 PDT 2026; root:xnu-13432.41.4~26/RELEASE_ARM64_T8152 arm64`
- OS page size: 16384 bytes
- CPUs: 12
- CPU: Apple M6
- Filesystem of the run directory: apfs
- Stable toolchain: `rustc 1.100.0 (0ef1bc386 2026-11-09)`
- Nightly toolchain: `rustc 1.102.0-nightly (f5cedb83a 2026-11-07)`
- ZeroDB commit: `bf82af27ba1207786e5e316cb241d5fd88111ec6` (working tree modified)

| Step | Result | Time | Log |
|---|---|---|---|
| cargo test --workspace | pass | 962 s | `test.log` |
| crash harness, 4 KiB DB pages | pass | 37 s | `crash-4096.log` |
| crash harness, 16 KiB DB pages | pass | 30 s | `crash-16384.log` |
| crash harness, 64 KiB DB pages | pass | 32 s | `crash-65536.log` |
| reader/writer stress, 180 s each | pass | 362 s | `stress.log` |
| fuzz diff_ops-native, 600 s | pass | 601 s | `diff_ops-native.log` |
| fuzz diff_ops-heed, 600 s | pass | 602 s | `diff_ops-heed.log` |
| fuzz fuzz_page_decode, 600 s | pass | 601 s | `fuzz_page_decode.log` |
| fuzz fuzz_image_open, 600 s | FAIL (exit 71) | 461 s | `fuzz_image_open.log` |

Test totals: 1185 passed, 0 failed, 0 ignored

Crash cycles verified:
- 4 KiB DB pages: 2244
- 16 KiB DB pages: 2273
- 64 KiB DB pages: 2276

| Fuzz job | Inputs executed | Findings |
|---|---|---|
| diff_ops-native | 30619 | 0 |
| diff_ops-heed | 35633 | 0 |
| fuzz_page_decode | 33704730 | 0 |
| fuzz_image_open | 7120031 | 1 |

## Verdict: fail (1 steps)

## Notes

Added after the run; everything above is the runner's `summary.md`, with the
host name removed and the filesystem line (blank in this run's version of the
runner) filled in.

- **This is macOS, not Linux.** The kernel is Darwin with 16 KiB pages. It
  exercises DB pages smaller than the OS page (the 4 KiB case) through the
  same `mmap`, `msync` and `madvise` paths, but it is not the 64 KiB-page Linux
  kernel the README item asks for. That item stays open.
- **The working-tree modification** was in the runner scripts only (`scripts/`,
  later committed); no engine code differed from `bf82af2`.
- **The `fuzz_image_open` failure is the runner's, not ZeroDB's.** libFuzzer
  stopped at its 2,048 MB RSS limit (`oom-c881f2f3…`, 5 bytes). Replayed
  alone, that input runs in 0 ms. RSS grew steadily with the number of inputs
  (about 280 MB per million) while the live heap at the stop was 24 MB, 85%
  of it libFuzzer's own. The same target built without a sanitizer ran
  3.9 million inputs from the same corpus at a flat 28 MB. The growth is
  AddressSanitizer's bookkeeping in one long-lived process. The runners now
  run every fuzz job in fork mode, whose child processes live at most 300 s,
  and the matrix fails the fuzz step on any artifact.
- **Confirmation after the fix:** `fuzz_image_open` with AddressSanitizer, one
  fork-mode worker, 600 s from this run's corpus: 13,277,549 inputs, 0 crashes,
  0 timeouts, 0 out-of-memory stops, no artifacts. The other steps were not
  rerun.
