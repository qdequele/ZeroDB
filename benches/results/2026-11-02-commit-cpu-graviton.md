# Per-commit CPU fixes (issue #146), Graviton4

- Date: 2026-11-02
- Host: AWS `c8g.xlarge` (Graviton4, 4 vCPU, 8 GiB), Ubuntu 24.04, Linux
  7.0, 60 GiB gp3 root, 4 KiB OS pages. Load average about 1 during the runs.
- Rust 1.99.1, default release profile.
- Commits: `3c4f37f` (`main`), then the issue-146 branch: `8cff2c1` (meta
  buffer in the writer carry), `df2427d` (free-list id validation),
  `0817187` (three-stream meta CRC).

## Ladder, three columns

`scripts/bench-ab.sh '^(env/txn/rw_empty_commit|commit/batch/(n1|n100)|del/random/n1)/'`,
3 interleaved rounds per pair, each commit against its parent and the branch
against `main`. LMDB moved by at most 0.9 % between halves in every run.

| rung | LMDB | `main` | branch | branch ÷ `main` | verdict |
|---|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.371 ms | 8.865 ms | 8.952 ms | 1.008 | flat |
| `commit/batch/n100` | 6.321 ms | 6.818 ms | 6.783 ms | 0.993 | flat |
| `del/random/n1` | 937.4 ms | 1.077 s | 1.051 s | 0.987 | flat |
| `env/txn/rw_empty_commit` | 54.47 µs | 189.7 µs | 196.1 µs | 1.035 | slower |

Per commit, after ÷ before (all flat within ±3 %):

| rung | meta buffer | validation | CRC |
|---|---:|---:|---:|
| `commit/batch/n1` | 1.004 | 0.991 | 1.013 |
| `commit/batch/n100` | 0.994 | 0.992 | 0.994 |
| `del/random/n1` | 1.006 | 0.986 | 1.004 |
| `env/txn/rw_empty_commit` | 0.995 | 1.009 | 1.008 |

The branch's `rw_empty_commit` reading (1.035) is not confirmed by the
per-commit runs (product 1.012) or by the cycle counts below, where an empty
write transaction costs the same at every commit. None of the three changes
runs in an empty write transaction except the validation, whose instruction
count there is unchanged (1,557 → 1,558).

## Census, user-space cycles and instructions per commit

`commit_census` (one write transaction per operation, `NO_SYNC`) under
`perf stat -e cycles:u,instructions:u`, 3 rounds, median (min–max of
cycles). `empty`: 1,000,000 unchanged write transactions. `put`: 100,000
single-put commits. `del`: 40,000 single-delete commits of 1,536-byte values
in shuffled order; its counts include the one-transaction fill of the 40,000
keys, which is the same work at every commit.

| shape | LMDB | `main` | meta buffer | validation | CRC |
|---|---:|---:|---:|---:|---:|
| empty, cycles | 159 | 564 (563–570) | 568 | 563 | 561 (560–565) |
| empty, instructions | 660 | 1,557 | 1,557 | 1,558 | 1,558 |
| put, cycles | 2,618 | 5,722 (5,685–5,728) | 5,570 (5,542–5,607) | 5,610 | 5,673 (5,604–5,707) |
| put, instructions | 9,284 | 16,714 | 16,681 | 16,702 | 16,747 |
| del, cycles | 11,994 | 23,266 (23,248–23,329) | 23,221 | 22,589 (22,374–22,664) | 22,471 (22,322–22,623) |
| del, instructions | 25,859 | 65,778 | 65,748 | 61,943 | 61,599 |

Reading:

- The meta buffer leaving its mutex saves about 150 cycles (−2.7 %) per
  single-put commit, with no overlap between the before and after ranges.
- The validation saves about 630 cycles (−2.7 %) and 3,800 instructions per
  delete commit, where the base snapshot's annex is long.
- The three-stream CRC saves about 120 cycles per delete commit at 4 KiB
  pages, where the annex holds at most 490 ids. Its single-put reading
  (+63 cycles) overlaps the previous commit's range. The gain grows with the
  page size (standalone on an M1: 490 ids 464 → 175 ns, 2,026 ids
  1,982 → 741 ns).
- Branch against `main`: single-put −0.9 %, delete −3.4 % of user cycles;
  an empty write transaction is unchanged. All are below the ladder's 3 %
  resolution, which is why every criterion rung reads flat.

## What remains of the gap

An empty write transaction is still 564 against 159 cycles (3.5×) and a
single-put commit 5,722 against 2,618 (2.2×). The causes measured in the
profile behind ADR-0035, largest first: copying the 856-byte `RwTxn` by value
through the heed wrapper, the writer lock's 6 atomic read-modify-writes
(ADR-0035), and the scan of every reader slot at the first page draw of each
write transaction.
