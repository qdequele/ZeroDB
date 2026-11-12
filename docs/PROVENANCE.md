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
  values, error codes and iteration order. Hand-written differential tests
  cover cases the sequences cannot express, such as read-only opens,
  multi-process scenarios and environment copies.
- **Differential fuzzing in CI.** Every pull request runs the fuzzer for
  10 minutes on native ZeroDB and 2 minutes through the heed adapter, on
  x86-64 and aarch64, plus 2 minutes each of two targets that feed arbitrary
  bytes to the page decoder and to `open`. A nightly job runs it for about
  four hours on each architecture.
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
  hannoy's test suite have passed on ZeroDB, with no source changes. These
  runs are manual; see the last item of the next section.

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
- **No I/O-error injection.** The fault-injecting backend models crashes, not
  failing system calls. No test injects `EIO` or `ENOSPC` into a write or a
  sync.
- **No power-loss test on a real device.** Crash tests use a model of what a
  disk may keep, and process kills, which leave the page cache intact. No
  test has cut power to a machine.
- **The differential tests do not cover everything.** The generated operation
  sequences use one transaction at a time, plus one nested read transaction.
  Not compared with LMDB at all: `delete_range` and custom key comparators,
  which have ZeroDB-only tests. Not compared: the point at which a full map
  returns `MapFull`, which depends on each engine's page allocation.
  Read-only environments and multi-process behavior are covered by
  hand-written scenarios only, not by generated sequences. Concurrent readers
  and writers are stress-tested on ZeroDB alone, not compared with LMDB.
- **4 KiB-page kernels only.** CI runs on GitHub's x86-64 and aarch64 runners,
  which use 4 KiB OS pages. ZeroDB has not been run on a 64 KiB-page kernel.
- **No 24-hour fuzz soak.** The longest scheduled fuzz run is the nightly one.
- **The consumer suites are not in CI.** They are run by hand
  ([maintainers/CONSUMER-GATE.md](maintainers/CONSUMER-GATE.md)). The last
  recorded passes are Meilisearch v1.55.0's milli and index-scheduler suites
  on 2026-10-18, and Meilisearch v1.53.1's and hannoy's suites on 2026-09-09.
  No run is recorded on the current `main`.
- **Performance figures are being re-measured.** See the status box at the top
  of [PERFORMANCE.md](PERFORMANCE.md).

Known limits of the security model are in [SECURITY.md](../SECURITY.md).
