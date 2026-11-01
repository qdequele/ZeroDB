# Write txns reuse their containers (issue #8) — Graviton4, 2026-10-31

Host: c8g.xlarge (Graviton4 Neoverse-V2, 4 vCPU, 8 GiB), Ubuntu 24.04, kernel 7.0, 60 GiB gp3 root, rustc 1.99.1.
Before = `main` at `0adf868`. After = `ea9bbaf` (the in-place form). LMDB = the Meilisearch fork through heed 0.22.1.

## The change

LMDB allocates its write transaction once per environment (`me_txn0`), with its dirty list and page arrays, and
reuses it. ZeroDB built every write txn's containers empty: the dirty-frame and spilled indexes, the freed, loose
and reclaimed sets, the commit's page lists, the descent buffers and the open-table slots. They now live in a
`WriterCarry` box that rides the env's writer slot. A write txn uses the containers in place and hands the box
back, emptied, when it ends. Hash indexes keep capacity for about 128 entries and lists 4,096. The txn also starts
from the snapshot the previous one published when nothing has committed since, and drains the base meta page's
free-list annex in place.

## Census: one single-put no-sync commit, per commit

`crates/zerodb-oracle/examples/commit_census.rs` runs 200,000 commits under `perf stat`. The table gives the mean of
3 runs per column, which agree to within 1%.

| | LMDB | ZeroDB before | ZeroDB after | after ÷ before |
|---|---:|---:|---:|---:|
| wall time per commit | 4.25 µs | 5.51 µs | 5.17 µs | 0.94 |
| user cycles | 2.64k | 6.68k | 5.74k | 0.86 |
| user instructions | 9.37k | 19.78k | 16.40k | 0.83 |
| kernel cycles | 9.41k | 8.90k | 8.87k | 1.00 |
| begin (phases run) | 68 ns | 169 ns | 121 ns | 0.72 |
| put (phases run) | 601 ns | 1,066 ns | 927 ns | 0.87 |
| commit (phases run) | 3,687 ns | 4,388 ns | 4,203 ns | 0.96 |

An empty write txn (begin and commit, 2,000,000 iterations; a scratch loop shaped like
`env/txn/rw_empty_commit`):

| | before | after |
|---|---:|---:|
| time per txn | 191 ns | 182 ns |
| user cycles | 536 | 512 |
| user instructions | 1,434 | 1,506 |

Heap allocations, counted by `crates/zerodb/tests/write_txn_allocations.rs`:

| | before | after |
|---|---:|---:|
| single-put commit, named DB | 19 | 5 |
| empty write txn | 1 | 0 |

## Ladder: `scripts/bench-ab.sh '^(commit/|env/txn/|put/|del/)'`, 3 interleaved rounds

The verdict is **improved**: 1 rung improved, 0 regressed and 32 were flat, with no LMDB drift beyond 3%.

| rung | LMDB | before | after | vs LMDB before → after | after ÷ before | verdict |
|---|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.402 ms | 9.331 ms | 8.697 ms | 1.26× → 1.17× | 0.932 | faster |
| `commit/batch/n100` | 6.365 ms | 7.062 ms | 6.910 ms | 1.11× → 1.09× | 0.976 | flat |
| `del/random/n1` | 946.7 ms | 1.461 s | 1.410 s | 1.54× → 1.49× | 0.970 | flat |
| `env/txn/rw_empty_commit` | 53.6 µs | 186.8 µs | 188.7 µs | 3.48× → 3.52× | 1.010 | flat |
| `env/txn/ro_begin_abort` | 809.8 µs | 780.7 µs | 764.2 µs | 0.96× → 0.94× | 0.978 | flat |

The other 28 rungs were within ±1.8%.

## A first form that regressed

The first form (`a0a5a08`, ladder run at `28b807e`) moved each container out of the carry at begin and back at the
end. On the same host it made `commit/batch/n1` 0.968× but `env/txn/rw_empty_commit` 1.455× slower: an empty txn
went from 191 to 279 ns, 536 to 781 user cycles. A line-level profile showed where the time went:

- building the txn struct from the moved fields, 72 cycles;
- `mem::take` / `mem::replace`, 68 cycles;
- drop glue of the emptied fields, 25 cycles;
- `hashbrown::shrink_to` on every txn, because a table shrunk to 128 entries reports a capacity of 224, which is
  above the 128 the check compared against.

Together these cost more than the roughly 90 cycles of atomics the cached snapshot saved. The kept form uses the
containers in place, shrinks an index only past twice its cap, and resets the handle-check memo only when a check
wrote it. Both runs are in the perf ledger.

## What remains of the per-commit gap

User cycles per single-put commit are still 5.74k against LMDB's 2.64k.

- **Writer lock atomics:** about 137 cycles of an empty txn. The in-process slot mutex costs lock and unlock at
  both acquire and release, on top of the lock file's word: about 8 read-modify-write atomics, against LMDB's 2.
  Reaching LMDB's count needs an owned lock cell or a dependency, so it is an ADR-level change.
- **Commit allocations:** the published snapshot `Arc` and its annex, and the `pwritev` iovec list.
- **After a large delete:** small txns decode and rewrite the free-list tree record. LMDB keeps its reclaimed list
  (`me_pghead`) across txns.
