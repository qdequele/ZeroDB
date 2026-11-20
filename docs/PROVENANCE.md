# Provenance

This page says how ZeroDB was built, what has been verified about it, and what
has not. A storage engine asks for a lot of trust, so the limits are stated as
plainly as the strengths.

## How it was built

- **The code was written by AI coding agents** (Anthropic's Claude), directed
  and reviewed by the maintainer. The ADRs were drafted the same way, for
  the maintainer to decide. Commits are authored under the maintainer's name;
  only some carry a co-author trailer.
- **Agents and people follow the same rules.** They are in
  [CONTRIBUTING.md](../CONTRIBUTING.md), and
  [AGENTS.md](../AGENTS.md) points agents to them.
- **LMDB's behavior is observed, not guessed** (CONTRIBUTING rule 1). The
  reference is the LMDB build Meilisearch ships: Meilisearch's LMDB fork,
  vendored in `lmdb-master-sys 0.2.6` under heed 0.22.1. To learn what LMDB
  does in some case, a differential test is written and run. A difference
  between the engines is a bug unless it is listed in
  [LMDB-COMPATIBILITY.md](LMDB-COMPATIBILITY.md#behavior-differences). An
  expectation is never changed, and a test never deleted or weakened, to make
  ZeroDB pass (rules 1 and 2).
- **Design decisions are recorded as ADRs** ([DECISIONS.md](DECISIONS.md)).
  For format, durability, free-page, reader-table and other
  correctness-critical changes, the ADR is written and approved by the
  maintainer before the implementation (rule 5). This was not always
  recorded: four implemented ADRs (0009, 0013, 0032 and 0033) have no
  recorded approval, and the index marks them. Some behavior differences in
  LMDB-COMPATIBILITY.md are still marked "Proposed".
- **Clean room** (rule 4). LMDB and libmdbx C source may be read to understand
  an algorithm. C code is not transliterated; the engine is implemented from
  the [SPEC](SPEC/).

## What has been verified

- **Differential tests against LMDB.** `crates/zerodb-oracle` runs one
  sequence of operations on LMDB, on native ZeroDB and on ZeroDB through the
  heed adapter, and compares each operation's result for equality: return
  values, error codes and iteration order. Generated scenarios
  (`zerodb_oracle::scenario`) compare, through heed on both engines,
  `delete_range` (a cursor walk deleting each entry on LMDB) and custom key
  comparators (reverse, native-endian integer, length then bytes), several
  read transactions held at different snapshots while a writer mutates and
  commits, and close and reopen sequences with `READ_ONLY`, `PREV_SNAPSHOT`,
  `NO_LOCK` and `WRITE_MAP`, map sizes and `resize`. Generated
  multi-process sequences (`zerodb_oracle::mp`) run two or three worker
  processes on one environment, with reader slots, `ReadersFull`, growth
  past another process's map, `MapResized`, `resize`, workers killed with
  `SIGKILL` while holding readers, stale-reader cleanup and respawns.
  Hand-written differential tests cover what the generators do not, such as
  environment copies.
- **Differential fuzzing in CI.** Every pull request runs the fuzzer for
  10 minutes on native ZeroDB and 2 minutes through the heed adapter, on
  x86-64 and aarch64, plus 2 minutes each of two targets that feed arbitrary
  bytes to the page decoder and to `open`, 3 minutes of the generated
  scenarios and 200 generated multi-process sequences. A nightly job runs
  it for about four and a half hours on each architecture.
- **Crash testing.** A fault-injecting write backend builds the file images a
  crash could leave between two sync barriers: each pending write kept,
  dropped or torn, in any order. A second mechanism kills real processes with
  `SIGKILL`, including a writer killed in mid-commit while a reader in another
  process is live. After each crash the reopened environment must hold exactly
  one committed prefix of the workload and pass the invariant checker. Pull
  requests run 250 cycles; the nightly job runs 10,000.
- **Model checking.** The reader table, nested read transactions and two
  other concurrent structures of `zerodb-core` (its `handles` and `stamps`
  modules) are model-checked with loom. The loom
  build compiles the same source that ships, with the synchronization types
  swapped for loom's.
- **Miri.** The tests of `zerodb-core`, the crate with the page format, the
  trees and the transactions, run under miri on every pull request.
- **Stress.** Two reader and writer stress tests, one in a single process
  and one across processes, run nightly for three minutes each.
- **Consumers.** Meilisearch's milli and index-scheduler test suites and
  hannoy's test suite pass on ZeroDB, with no source changes, and run in CI
  nightly ([maintainers/CONSUMER-GATE.md](maintainers/CONSUMER-GATE.md)).
  Last recorded run: 2026-11-20, `main` at
  `c7e1abf6b30ee964052f75e47ca6b63386a7f192`, macOS aarch64, rustc 1.98.1
  for Meilisearch and 1.100.0 for hannoy. Meilisearch v1.55.1
  (`cargo test -p milli -p index-scheduler`): 448 passed, 0 failed, 4
  ignored. hannoy v0.2.0: 59 passed, 0 failed, 1 ignored. In both builds
  `lmdb-master-sys` was absent from the dependency tree.

## What has not been verified

- **No independent audit of the `unsafe` code.** No one outside the project
  has reviewed it. Every `unsafe` block has a `// SAFETY:` comment, and the
  places where `unsafe` is allowed are listed in
  [CONTRIBUTING.md](../CONTRIBUTING.md#unsafe-policy), but both were written
  by the same process as the code. Counts in each crate's `src/` directory,
  unit tests included, comments excluded:

  | Crate | `unsafe` blocks | `unsafe fn` | `unsafe impl` |
  |---|---:|---:|---:|
  | `zerodb-core` | 21 | 6 | 0 |
  | `zerodb-io` | 26 | 5 | 3 |
  | `zerodb` | 3 | 1 | 0 |
  | `heed-zerodb` | 9 | 9 | 1 |
  | `zerodb-tools` | 3 | 0 | 0 |
  | `zerodb-oracle` (test harness, FFI to LMDB) | 39 | 1 | 0 |

  Integration tests, benches and examples add about 400 `unsafe` blocks.
  Every call of `open` is one, because `open` is an `unsafe fn`.
- **Miri covers `zerodb-core` only.** `zerodb-io` (mmap, the lock file, the
  futex and `fcntl` code) and `heed-zerodb` (the lifetime-erased cursor and
  `Send` impls) do not run under miri, and they hold most of the `unsafe`
  code that touches the operating system.
- **I/O-error injection covers the data file's write, sync and growth calls
  and the copy's destination writes, not the lock file.** The
  fault-injecting backend can fail the `n`th write, sync or file-growth call
  with `EIO`, `ENOSPC` or a short write, and the tests fail every such call of
  a commit, of a spill, of the `WRITE_MAP` ranged `msync` and of a copy or
  compaction ([SPEC 06 REC-23](SPEC/06-recovery.md)). Not covered: the lock
  file (its creation, `fcntl` locks, `futex`), `mmap` itself, the file-copy
  and clone routes of a raw copy (which move bytes through `std::io::copy`
  and `std::fs::copy`), and `SIGBUS` from a `WRITE_MAP` store into a sparse
  file on a full device, which kills the process as it does LMDB's. Only a
  failed page write in a commit is compared with LMDB on a real failure (an
  `EFBIG` from a file-size limit); LMDB's handling of the other failures was
  read in its source. The tests found that a failed meta write left the
  environment writable, which could corrupt the file; a failed meta write or
  meta barrier now restores the meta slot and poisons the environment
  ([ADR-0047](adr/0047-failed-meta-write-policy.md)). The restore is tested
  in one process only.
- **No power-loss test on a real device.** Crash tests use a model of what a
  disk may keep, and process kills, which leave the page cache intact. No
  test has cut power to a machine.
- **The differential tests do not cover everything.**
  - The point at which a full map returns `MapFull` is not compared, because
    it depends on each engine's page allocation. A weaker property is
    compared instead: every failure is a clean `MapFull`, the failed
    transaction cannot commit, the committed state is intact and still
    readable, the environment grows with `resize` and then accepts the
    failed writes, and both engines end with the same contents. For the
    same reason a map set below the data is not compared by size: LMDB
    raises it to the pages in use, a count that depends on allocation. The
    reopen and multi-process generators stop comparing a case at its first
    `MapFull`.
  - Concurrency is compared through generated interleavings that run one
    step at a time, in one process or across processes, not through threads
    running in parallel. Parallel readers and writers are stress-tested on
    ZeroDB alone, not compared with LMDB.
  - The multi-process generator does not use `WRITE_MAP` (a joining LMDB
    process can truncate the data file under another process's mapping,
    [D-024](LMDB-COMPATIBILITY.md#d-024)), `PREV_SNAPSHOT` (the approved
    differences of [D-025](LMDB-COMPATIBILITY.md#d-025)) or `NO_LOCK`, and
    no worker holds a write transaction between steps, so no process ever
    waits for another's writer lock.
  - Custom comparators and `delete_range` are compared through the heed
    adapter only; the native API is not driven against LMDB for them.
  - `Env::info().last_txn_id` is not compared in a process whose map is
    smaller than the data another process committed
    ([D-029](LMDB-COMPATIBILITY.md#d-029)).
- **No 64 KiB-page kernel.** CI runs on GitHub's x86-64 and aarch64 Linux
  runners, which use 4 KiB OS pages. ZeroDB has not been run on a 64 KiB-page
  kernel. macOS on Apple silicon (16 KiB pages) runs the tests and the crash
  smoke in CI, but not the fuzz, miri, loom or nightly runs.
  The one fuller run on a larger OS page is a page-size matrix on a 16 KiB-page
  macOS host (Apple silicon) on 2026-11-20, recorded in
  [hardening-runs/](hardening-runs/2026-11-20-macos-arm64-16k-page-size-matrix.md):
  the test suite, 6,793 crash cycles across DB pages of 4, 16 and 64 KiB,
  the stress tests and three of the four fuzz jobs passed; the fourth stopped
  at libFuzzer's memory limit from sanitizer growth, not an engine fault (the
  record explains). DB pages of 4 KiB there are smaller
  than the OS page, the case a 64 KiB kernel reaches with 4 and 16 KiB DB
  pages, but on Darwin's virtual memory system, not Linux's.
  [maintainers/HARDENING-RUNS.md](maintainers/HARDENING-RUNS.md) has the
  runner (`just page-size-matrix`) and the machine to use.
- **No Windows support.** The crates do not build on Windows (a
  `compile_error!`); ADR-0046 is a draft. LMDB's behavior on Windows has not
  been observed by the oracle.
- **No 24-hour fuzz soak.** The longest scheduled fuzz run is the nightly one.
  The runner exists (`just soak`, same page); no 24-hour run is recorded.
- **Consumers newer than the pins.** Meilisearch v1.56 and later, and
  hannoy v0.3.0, depend on heed 0.23, which the adapter does not provide; on
  them the `[patch]` is ignored and the build keeps C LMDB. The consumer
  suites run on Linux x86-64 in CI only; the last recorded run with a result
  is the macOS one above. Meilisearch's other crates (`meilisearch`,
  `meilisearch-auth`) run only with `FULL=1`, by hand.
- **Performance figures are being re-measured.** See the status box at the top
  of [PERFORMANCE.md](PERFORMANCE.md).

Known limits of the security model are in [SECURITY.md](../SECURITY.md).
