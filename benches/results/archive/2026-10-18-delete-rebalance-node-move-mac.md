# Delete rebalance moves cells page to page (ADR-0028) — macOS, 2026-10-18

- Change: borrows and merges move encoded cells verbatim through a per-txn
  scratch buffer; parent separators are rewritten in place (SPEC 03 §10.1
  REB-2..REB-4). Tree shape unchanged (golden logical images match).
- Before: `655c376` (engine as on main `f98cd08`, plus the golden test).
  After: `41880bc`.
- Host: Mac, Apple M-series, 8 CPUs, macOS, 16 KiB OS pages. **Busy machine**
  (load average 11–44 from other work during every run), so every number is
  indicative; LMDB, measured in both halves, is the drift control.

## Where the time went before the change

`scripts/bench-profile.sh del/bulk/half zerodb` (macOS `sample`, 15 s),
inclusive share of the timed `delete_keys` loop:

| frame | share |
|---|---:|
| `delete_tree` | 97 % |
| `rebalance` | 68 % |
| `borrow_entry` | 58 % |
| `update_parent_key` (separator remove + re-insert) | 31 % |
| `remove_cell` (all heap compactions) | 47 % |
| `search_path_into` (descent) | 15 % |
| `commit` | 1.5 % |

The ADR's riskiest assumption held: the time was in the per-move mechanics.

## Criterion ladder, `^del/`, 3 interleaved rounds (`scripts/bench-ab.sh`)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before |
|---|---:|---:|---:|---:|---:|
| `del/bulk/all` | 20.49 ms | 46.29 ms | 28.06 ms | 2.26× → **1.37×** | 0.57 |
| `del/bulk/half` | 10.74 ms | 26.48 ms | 13.92 ms | 2.46× → **1.30×** | 0.51 |
| `del/cursor/drain` | 9.79 ms | 25.37 ms | 14.63 ms | 2.59× → **1.50×** | 0.56 |
| `del/churn/reinsert` | 12.10 ms | 20.17 ms | 14.60 ms | 1.67× → **1.21×** | 0.66 |
| `del/range/half` | 8.08 ms | 5.43 ms | 1.98 ms | 0.67× → **0.24×** | 0.39 |
| `del/clear/all` (not touched; control) | 59.5 µs | 98.2 µs | 108.1 µs | 1.65× → 1.82× | 0.97 |

bench-ab's verdict is "invalid" because LMDB drifted more than 3 % between
halves on five rungs (busy host). Per round, ZeroDB after ÷ before was:

| rung | round 1 | round 2 | round 3 |
|---|---:|---:|---:|
| `del/bulk/all` | 0.44 | 0.64 | 0.57 |
| `del/bulk/half` | 0.44 | 0.53 | 0.51 |
| `del/cursor/drain` | 0.38 | 0.58 | 0.56 |
| `del/churn/reinsert` | 0.66 | 0.83 | 0.64 |
| `del/range/half` | 0.39 | 0.36 | 0.41 |
| `del/clear/all` | 0.87 | 1.13 | 0.97 |

Every round of every rebalance rung improves by far more than LMDB moved;
the untouched `clear/all` stays around 1.0.

## crud-bench, 3 interleaved rounds

500k random 26-byte keys, ~1.5 KB values, 16 KiB pages,
`WRITE_MAP | MAP_ASYNC | NO_SYNC`, ZeroDB in trusted-file mode, `-c 128 -t 48`,
LMDB and ZeroDB built into two crud-bench binaries (before, after).

The host was too noisy for crud-bench's create/read/update phases (the same
binary's creates swung 30k–56k ops/s between rounds). Delete, per round:

| round | ZeroDB after ÷ before | LMDB after ÷ before | ZeroDB/LMDB before → after |
|---|---:|---:|---:|
| 1 | 1.07 | 1.27 | 0.52 → 0.44 |
| 2 | 1.13 | 0.82 | 0.38 → 0.52 |
| 3 | 1.39 | 0.95 | 0.32 → 0.48 |

Median delete throughput: ZeroDB 27.5k → 31.7k ops/s, LMDB ~67–71k ops/s.
A modest gain, because crud-bench deletes are not rebalance-bound. A profile
of the delete phase (after the change):

- 88 % of thread samples wait on the writer lock (48 threads, one writer);
- of the serialized work, `commit` is ~75 %, `delete_tree` ~18 %,
  `rebalance` ~4 %;
- ~78 % of `commit` is `MetaPage::encode_with_annex`: the meta page's
  free-list annex (ADR-0022) is re-encoded on every commit, and a
  delete-heavy run keeps it full.

So crud-bench's one-delete-per-transaction workload is bound by per-commit
meta encoding, a separate lever (not part of ADR-0028).

## Correctness gates on the change

- `crates/zerodb/tests/rebalance_logical_image.rs`: eight golden logical
  images recorded before the change, all identical after, in default and
  `WRITE_MAP` envs; in-place separator rewrites ran 31,840 times and the
  split fallback 4 times across them.
- `crates/zerodb-core/tests/cell_move.rs`: proptests of the primitives
  against the operations they replace.
- `cargo test --workspace` (LMDB differential suites included), clippy
  (macOS and `x86_64-unknown-linux-gnu`), miri on `zerodb-core`,
  `just fuzz-quick`, `just crash-test-quick`.
