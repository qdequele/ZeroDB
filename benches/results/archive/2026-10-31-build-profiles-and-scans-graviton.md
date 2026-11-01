# Build profiles and scans on Graviton4 (#139)

- **Date:** 2026-10-31.
- **Questions (issue #139):** (1) the benchmarks build with the workspace
  release profile (16 codegen units, no LTO), so a call from `heed-zerodb`
  into `zerodb-core` is only inlined for generic or `#[inline]` functions.
  Do one codegen unit, thin LTO or fat LTO make ZeroDB much faster, and if
  so, which functions need `#[inline]` at the crate boundaries? (2) Which
  `scan/*` and `seek/*` rungs still trail LMDB on Graviton, in the default
  (validating) and the opt-in trusted-file mode, and why?
- **ZeroDB:** `main` at `0adf868`, unchanged. No engine change was tried, so
  there is no perf-ledger entry.
- **LMDB:** the Meilisearch fork vendored by `lmdb-master-sys 0.2.6`, through
  heed 0.22.1, built by `cc` at `-O3` in every profile.
- **Host:** AWS c8g.xlarge, Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB, Ubuntu
  24.04, kernel 7.0, 4 KiB OS and database pages, 60 GiB gp3 root, rustc
  1.99.1. Launched for this run and shut down after it. Nothing else ran on
  it. Load average 1.0–1.6 during every criterion run.

## Summary

| question | answer |
|---|---|
| Does the build profile hide a large speedup? | **No.** Over the 71 default-tier rungs, ZeroDB gets 0.8 % (1 codegen unit), 2.0 % (thin LTO) and 2.6 % (fat LTO) faster than under the default profile. LMDB's side (heed's Rust wrapper and the harness; the C library is unaffected) gets 1.5 %, 1.9 % and 2.8 % faster. ZeroDB ÷ LMDB stays at 1.12–1.13 under all four. |
| Where does LTO help ZeroDB more than LMDB? | Point gets: `get/*` rungs are 3–10 % faster under thin or fat LTO with LMDB flat (`get/access/hot` 0.90, `get/access/miss` 0.90, `get/size/n1k` 0.92, `get/val/v8` 0.93). One codegen unit, which is how Meilisearch builds, gets almost none of it (0.96–1.01). |
| Anything worse? | Under 1 codegen unit, `put/cursor/rewrite` is 19 % slower (2.06× → 2.47× LMDB) and `del/cursor/drain` 9 % slower (2.39× → 2.58×), with LMDB flat. Thin and fat LTO make both rungs faster instead. Meilisearch builds with 1 codegen unit, so this one matters to the consumer. |
| Which scans trail LMDB, validating mode? | `scan/full/big` 1.60×, `scan/full/fwd` 1.47×, `scan/full/rev` 1.47×, `scan/edge/first_last` 1.41×, `scan/prefix/bucket` 1.27×, `scan/meta/len` 1.12×, `seek/ge/gap` 1.06×. Range scans (0.82–0.85×), paged listings (0.47×) and point seeks (0.92–0.93×) are faster than LMDB. |
| Trusted-file mode? | Full scans drop to 1.03–1.09× and the prefix bucket to 1.08×, so page validation was their gap. `scan/edge/first_last` (1.42×) and `scan/meta/len` (1.15×) do not move: their gap is not validation. |
| Why | Full scans and the prefix bucket pay per-leaf validation (default mode). A full scan also pays two out-of-line calls per entry, which is the residual in trusted mode. `first_last` pays root-to-leaf re-resolution on every call. Details and profiles below. |

## Method

**Build profiles.** One checkout, four `engine_comparison` binaries, each
built into its own `CARGO_TARGET_DIR` with only the bench profile changed:

| name | env | binary |
|---|---|---:|
| `cgu16` (today) | none: 16 codegen units, `lto = false` (thin-local) | 6.2 MB |
| `cgu1` | `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1` | 4.8 MB |
| `thin` | `CARGO_PROFILE_BENCH_LTO=thin` | 6.3 MB |
| `fat` | `CARGO_PROFILE_BENCH_LTO=fat` | 4.7 MB |

Fat LTO merges every crate into one module before code generation, so `fat`
stands for fat LTO with 1 codegen unit as well. Each build took 31–46 s.

Three rounds of the whole default-tier ladder (71 rungs, both engines). Every
round ran all four binaries back to back, in a different order each round
(`cgu16 cgu1 thin fat`, then `fat thin cgu1 cgu16`, then `cgu1 fat cgu16
thin`). Each pass took 24 minutes. A binary measures LMDB and ZeroDB in the
same process, so **ZeroDB ÷ LMDB** under one profile is self-normalised. The
second table divides each engine's time by the same engine's time under
`cgu16`, per round. LMDB is not a pure drift control here: the profile also
changes heed's Rust wrapper and the harness code around the C calls. A
profile only helps ZeroDB where ZeroDB moves and LMDB does not.

**Scans.** `scripts/bench-ab.sh '^(scan|seek)/'` with `ROUNDS=3` and
`CAND_ENV=ZERODB_BENCH_TRUST_FILE=1` on a clean tree. "before" is the
validating default, "after" the same binary with every ZeroDB environment
opened with `FileTrust::trust_contents`. Default profile (`cgu16`).

**Profiles.** `perf record -F 1999 --call-graph dwarf` of one rung of one
engine with criterion's `--profile-time 12`, on the same four binaries the
ladder measured. Most rungs build their fixture once per process, so the
profiles also show a few percent of fixture setup (`search_path_into`, `mdb_*`
from the LMDB fixture). `put/cursor/rewrite` rebuilds its fixture on every
iteration, and that setup dominates its profile.

## 1. Build profiles

### Geometric mean over the 71 rungs

| profile | ZeroDB ÷ LMDB | ZeroDB ÷ ZeroDB cgu16 | LMDB ÷ LMDB cgu16 |
|---|---:|---:|---:|
| cgu16 | 1.122 | 1.000 | 1.000 |
| cgu1 | 1.128 | 0.992 | 0.985 |
| thin | 1.119 | 0.980 | 0.981 |
| fat | 1.124 | 0.974 | 0.972 |

### ZeroDB ÷ LMDB per profile (median of 3 rounds)

| rung | cgu16 | cgu1 | thin | fat |
|---|---:|---:|---:|---:|
| `commit/batch/n1` | 1.25 | 1.23 | 1.24 | 1.23 |
| `commit/batch/n100` | 1.11 | 1.11 | 1.10 | 1.11 |
| `commit/batch/n10k` | 0.99 | 0.99 | 0.98 | 0.99 |
| `commit/sync/n1` | 0.98 | 0.98 | 0.98 | 0.98 |
| `commit/sync/n100` | 1.00 | 1.00 | 1.00 | 1.00 |
| `commit/sync/n1_map16g` | 0.98 | 0.98 | 0.98 | 0.98 |
| `del/batch/k1000` | 1.24 | 1.25 | 1.25 | 1.25 |
| `del/bulk/all` | 1.40 | 1.38 | 1.42 | 1.37 |
| `del/bulk/half` | 1.40 | 1.38 | 1.41 | 1.36 |
| `del/churn/reinsert` | 1.31 | 1.29 | 1.30 | 1.30 |
| `del/clear/all` | 1.74 | 1.74 | 1.67 | 1.75 |
| `del/cursor/drain` | 2.39 | 2.58 | 2.38 | 2.24 |
| `del/random/n1` | 1.52 | 1.52 | 1.53 | 1.49 |
| `del/range/half` | 0.59 | 0.58 | 0.60 | 0.60 |
| `env/open/create` | 29.77 | 30.05 | 29.80 | 29.81 |
| `env/open/reopen` | 0.93 | 0.93 | 0.92 | 0.92 |
| `env/stat/non_free` | 0.47 | 0.47 | 0.49 | 0.52 |
| `env/txn/ro_begin_abort` | 0.76 | 0.77 | 0.75 | 0.81 |
| `env/txn/ro_get_one` | 1.42 | 1.42 | 1.38 | 1.38 |
| `env/txn/rw_empty_commit` | 3.40 | 3.34 | 3.65 | 4.13 |
| `get/access/hot` | 0.80 | 0.76 | 0.74 | 0.71 |
| `get/access/miss` | 1.05 | 1.02 | 0.97 | 0.95 |
| `get/access/rand` | 0.96 | 0.95 | 0.92 | 0.93 |
| `get/access/seq` | 0.92 | 0.90 | 0.87 | 0.85 |
| `get/db/named` | 0.95 | 0.95 | 0.92 | 0.89 |
| `get/db/named_x8` | 1.01 | 0.97 | 0.94 | 0.96 |
| `get/db/root` | 0.96 | 0.94 | 0.93 | 0.90 |
| `get/key/k128` | 1.07 | 1.06 | 1.06 | 1.05 |
| `get/key/k32` | 1.06 | 1.06 | 1.05 | 1.03 |
| `get/key/k8` | 0.97 | 0.95 | 0.94 | 0.92 |
| `get/size/n1k` | 0.97 | 0.92 | 0.90 | 0.89 |
| `get/size/n50k` | 0.95 | 0.94 | 0.94 | 0.92 |
| `get/val/v256` | 0.81 | 0.79 | 0.81 | 0.79 |
| `get/val/v2page` | 1.36 | 1.32 | 1.31 | 1.27 |
| `get/val/v2page_touch` | 0.94 | 0.93 | 0.94 | 0.92 |
| `get/val/v4k` | 1.35 | 1.29 | 1.31 | 1.28 |
| `get/val/v4k_touch` | 0.96 | 0.93 | 0.93 | 0.94 |
| `get/val/v8` | 0.88 | 0.87 | 0.84 | 0.82 |
| `get/val/v8_touch` | 0.89 | 0.87 | 0.85 | 0.82 |
| `maint/copy/compact` | 1.19 | 1.24 | 1.21 | 1.25 |
| `maint/copy/raw` | 1.07 | 1.10 | 1.08 | 1.09 |
| `mixed/rw/8dbs` | 0.96 | 0.96 | 0.94 | 0.96 |
| `put/api/plain` | 0.97 | 0.97 | 0.95 | 0.95 |
| `put/api/reserved` | 1.09 | 1.09 | 1.06 | 1.06 |
| `put/cursor/rewrite` | 2.06 | 2.47 | 2.00 | 1.98 |
| `put/gc/drain_big` | 1.14 | 1.14 | 1.13 | 1.12 |
| `put/gc/ovf_across` | 1.37 | 1.37 | 1.33 | 1.37 |
| `put/gc/ovf_miss` | 1.06 | 1.07 | 1.07 | 1.05 |
| `put/order/append` | 1.27 | 1.25 | 1.25 | 1.35 |
| `put/order/rand` | 0.98 | 0.98 | 0.97 | 0.97 |
| `put/order/seq` | 0.96 | 0.97 | 0.96 | 0.95 |
| `put/over/grow` | 1.11 | 1.12 | 1.10 | 1.11 |
| `put/over/same_size` | 1.09 | 1.09 | 1.06 | 1.05 |
| `put/val/v256` | 0.99 | 0.99 | 0.97 | 0.97 |
| `put/val/v2page` | 0.97 | 0.99 | 0.99 | 0.98 |
| `put/val/v2page_abort` | 1.09 | 1.13 | 1.13 | 1.11 |
| `put/val/v4k` | 0.99 | 0.99 | 0.99 | 1.00 |
| `put/val/v8` | 1.01 | 1.01 | 0.99 | 0.96 |
| `scan/edge/first_last` | 1.41 | 1.40 | 1.51 | 1.49 |
| `scan/full/big` | 1.61 | 1.59 | 1.61 | 1.61 |
| `scan/full/fwd` | 1.47 | 1.48 | 1.52 | 1.54 |
| `scan/full/rev` | 1.47 | 1.48 | 1.51 | 1.51 |
| `scan/meta/len` | 1.15 | 1.39 | 2.01 | 1.49 |
| `scan/prefix/bucket` | 1.27 | 1.25 | 1.17 | 1.49 |
| `scan/range/10pct` | 0.82 | 0.95 | 0.86 | 1.13 |
| `scan/range/1pct` | 0.85 | 0.95 | 0.88 | 1.14 |
| `scan/skip_take/fwd` | 0.46 | 0.47 | 0.43 | 0.43 |
| `scan/skip_take/rev` | 0.45 | 0.48 | 0.44 | 0.44 |
| `seek/ge/gap` | 1.06 | 1.07 | 1.07 | 1.07 |
| `seek/ge/rand` | 0.95 | 0.94 | 0.93 | 0.93 |
| `seek/ge/seq` | 0.94 | 0.92 | 1.01 | 0.97 |

The rows that move by more than 0.06 between profiles, and why:

- **`get/*`** improves under LTO for ZeroDB only (next table).
- **`scan/range/*`** reads 0.82–0.85× under `cgu16` but 1.13–1.14× under
  `fat`. ZeroDB's time is flat (1.02–1.03); LMDB's falls to 0.75–0.76×.
  Under LTO, heed's Rust range iterator, with its per-step bound check, gets
  inlined into the loop. ZeroDB's per-entry step (`RoRange::next` →
  `Cursor::next`) stays out of line in every profile.
- **`scan/meta/len`** is a 7 ns call. Both engines move ±20–40 % between
  profiles with code layout, so its ratio is noise at this scale.
- **`env/txn/rw_empty_commit`** gets 8 % slower for ZeroDB under fat LTO and
  10 % faster for LMDB.
- **`put/order/append`** gets 5 % slower for ZeroDB under fat LTO.
- **`put/cursor/rewrite`** and **`del/cursor/drain`** get slower under 1
  codegen unit (next section).

### Each engine's time ÷ the same engine under `cgu16` (median [min–max] of 3 rounds)

| rung | ZeroDB cgu1 | ZeroDB thin | ZeroDB fat | LMDB cgu1 | LMDB thin | LMDB fat |
|---|---:|---:|---:|---:|---:|---:|
| `commit/batch/n1` | 0.99 [0.98–0.99] | 1.00 [1.00–1.00] | 0.99 [0.99–1.00] | 1.01 [1.00–1.01] | 1.01 [1.00–1.01] | 1.00 [0.99–1.02] |
| `commit/batch/n100` | 0.99 [0.99–1.00] | 0.99 [0.99–1.01] | 0.98 [0.97–0.98] | 0.99 [0.99–0.99] | 1.00 [1.00–1.00] | 0.98 [0.98–0.98] |
| `commit/batch/n10k` | 0.99 [0.99–0.99] | 0.99 [0.99–1.01] | 0.98 [0.97–0.98] | 0.99 [0.99–0.99] | 1.00 [1.00–1.01] | 0.97 [0.97–0.98] |
| `commit/sync/n1` | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] |
| `commit/sync/n100` | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] |
| `commit/sync/n1_map16g` | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] | 1.00 [1.00–1.00] |
| `del/batch/k1000` | 1.00 [0.98–1.01] | 0.99 [0.99–1.00] | 0.99 [0.98–0.99] | 0.99 [0.99–1.01] | 0.98 [0.98–0.99] | 0.98 [0.97–0.99] |
| `del/bulk/all` | 0.98 [0.98–0.99] | 1.00 [1.00–1.00] | 0.97 [0.97–0.97] | 0.99 [0.98–1.00] | 0.98 [0.97–0.99] | 0.99 [0.98–1.00] |
| `del/bulk/half` | 0.99 [0.98–0.99] | 1.00 [1.00–1.00] | 0.97 [0.97–0.98] | 1.00 [1.00–1.01] | 1.00 [0.99–1.00] | 1.00 [1.00–1.01] |
| `del/churn/reinsert` | 0.99 [0.97–0.99] | 0.99 [0.98–0.99] | 0.98 [0.98–0.98] | 1.00 [0.98–1.00] | 1.00 [0.98–1.01] | 0.99 [0.98–0.99] |
| `del/clear/all` | 0.99 [0.98–0.99] | 1.00 [1.00–1.01] | 0.98 [0.98–1.00] | 0.97 [0.94–0.99] | 1.04 [0.96–1.06] | 0.99 [0.93–1.03] |
| `del/cursor/drain` | 1.08 [1.08–1.11] | 0.98 [0.98–0.98] | 0.92 [0.92–0.92] | 1.00 [1.00–1.01] | 0.99 [0.98–1.00] | 0.98 [0.98–0.99] |
| `del/random/n1` | 1.00 [0.99–1.02] | 1.01 [1.01–1.02] | 0.98 [0.98–1.00] | 1.00 [1.00–1.01] | 1.00 [1.00–1.01] | 1.00 [0.99–1.00] |
| `del/range/half` | 0.99 [0.97–1.01] | 1.00 [0.99–1.00] | 1.00 [1.00–1.00] | 1.00 [0.99–1.00] | 0.98 [0.98–0.99] | 0.98 [0.98–0.98] |
| `env/open/create` | 1.00 [1.00–1.00] | 1.00 [0.99–1.00] | 1.00 [0.99–1.00] | 0.99 [0.99–1.00] | 1.00 [0.99–1.00] | 1.00 [0.99–1.00] |
| `env/open/reopen` | 1.00 [0.99–1.01] | 0.98 [0.94–0.98] | 0.98 [0.97–1.00] | 1.00 [0.99–1.03] | 0.97 [0.96–0.99] | 0.99 [0.98–1.03] |
| `env/stat/non_free` | 0.98 [0.98–1.00] | 0.99 [0.99–1.00] | 1.07 [1.07–1.07] | 0.99 [0.99–1.00] | 0.96 [0.96–0.96] | 0.98 [0.97–0.98] |
| `env/txn/ro_begin_abort` | 1.03 [1.02–1.04] | 1.00 [0.99–1.00] | 1.03 [1.03–1.04] | 1.01 [0.99–1.04] | 1.01 [0.97–1.04] | 0.97 [0.96–1.04] |
| `env/txn/ro_get_one` | 1.00 [0.99–1.00] | 0.96 [0.96–0.96] | 0.97 [0.96–0.98] | 1.00 [1.00–1.02] | 0.99 [0.98–1.01] | 1.00 [0.99–1.02] |
| `env/txn/rw_empty_commit` | 0.98 [0.97–0.99] | 1.00 [0.98–1.00] | 1.08 [1.07–1.12] | 0.99 [0.98–1.01] | 0.93 [0.92–0.94] | 0.91 [0.88–0.91] |
| `get/access/hot` | 0.96 [0.96–0.97] | 0.91 [0.91–0.92] | 0.90 [0.90–0.90] | 1.01 [1.00–1.01] | 0.98 [0.98–0.98] | 1.00 [1.00–1.00] |
| `get/access/miss` | 0.97 [0.97–0.97] | 0.89 [0.89–0.90] | 0.90 [0.89–0.90] | 1.00 [1.00–1.00] | 0.97 [0.97–0.97] | 1.00 [0.99–1.00] |
| `get/access/rand` | 1.01 [0.96–1.02] | 0.96 [0.94–0.99] | 0.96 [0.95–1.01] | 1.01 [0.96–1.03] | 0.99 [0.97–1.03] | 0.99 [0.97–1.01] |
| `get/access/seq` | 0.99 [0.98–0.99] | 0.93 [0.93–0.94] | 0.93 [0.92–0.93] | 1.00 [1.00–1.01] | 0.99 [0.98–0.99] | 1.00 [1.00–1.00] |
| `get/db/named` | 0.98 [0.97–0.98] | 0.96 [0.93–0.98] | 0.93 [0.93–0.94] | 0.98 [0.97–1.01] | 0.99 [0.98–1.01] | 1.01 [0.97–1.01] |
| `get/db/named_x8` | 1.00 [0.97–1.00] | 0.96 [0.93–0.97] | 0.97 [0.95–0.98] | 0.99 [0.98–1.04] | 1.00 [0.97–1.04] | 1.01 [0.96–1.01] |
| `get/db/root` | 1.00 [0.97–1.02] | 0.95 [0.94–0.96] | 0.95 [0.94–0.97] | 1.00 [1.00–1.03] | 0.98 [0.97–0.99] | 1.01 [0.99–1.02] |
| `get/key/k128` | 0.99 [0.99–1.00] | 0.98 [0.98–1.00] | 0.98 [0.96–1.00] | 1.00 [0.99–1.01] | 0.99 [0.99–1.05] | 0.99 [0.98–1.00] |
| `get/key/k32` | 1.00 [0.98–1.02] | 0.96 [0.96–1.00] | 0.96 [0.95–0.98] | 1.00 [0.97–1.03] | 0.96 [0.95–1.01] | 0.98 [0.96–1.02] |
| `get/key/k8` | 0.97 [0.96–0.98] | 0.95 [0.95–0.97] | 0.94 [0.93–0.96] | 0.99 [0.99–1.00] | 0.98 [0.97–0.99] | 1.00 [1.00–1.01] |
| `get/size/n1k` | 0.96 [0.96–0.96] | 0.92 [0.91–0.92] | 0.92 [0.92–0.92] | 1.00 [1.00–1.00] | 0.98 [0.98–0.99] | 1.00 [1.00–1.00] |
| `get/size/n50k` | 0.99 [0.97–0.99] | 0.97 [0.96–0.97] | 0.94 [0.93–0.96] | 0.98 [0.97–1.03] | 0.99 [0.97–1.00] | 0.98 [0.97–1.03] |
| `get/val/v256` | 0.98 [0.97–1.00] | 0.95 [0.93–0.99] | 0.95 [0.92–0.99] | 0.99 [0.91–1.00] | 0.94 [0.93–1.04] | 0.99 [0.94–1.02] |
| `get/val/v2page` | 0.97 [0.92–0.98] | 0.95 [0.93–0.96] | 0.93 [0.89–1.00] | 1.00 [1.00–1.00] | 0.98 [0.97–0.99] | 1.00 [0.99–1.00] |
| `get/val/v2page_touch` | 0.98 [0.97–0.99] | 0.97 [0.97–0.97] | 0.97 [0.96–0.98] | 1.00 [0.99–1.00] | 0.98 [0.97–0.98] | 0.99 [0.99–1.00] |
| `get/val/v4k` | 0.97 [0.96–0.97] | 0.95 [0.95–0.97] | 0.96 [0.95–1.00] | 1.00 [1.00–1.00] | 0.98 [0.98–0.98] | 1.00 [1.00–1.00] |
| `get/val/v4k_touch` | 0.97 [0.97–0.99] | 0.96 [0.95–0.96] | 0.98 [0.97–0.99] | 1.00 [0.99–1.01] | 0.99 [0.98–0.99] | 0.99 [0.99–1.01] |
| `get/val/v8` | 0.99 [0.98–0.99] | 0.93 [0.93–0.93] | 0.93 [0.93–0.94] | 1.00 [1.00–1.00] | 0.98 [0.97–0.98] | 1.00 [1.00–1.00] |
| `get/val/v8_touch` | 0.98 [0.97–0.98] | 0.93 [0.93–0.93] | 0.93 [0.92–0.93] | 1.00 [1.00–1.00] | 0.98 [0.97–0.98] | 1.00 [1.00–1.00] |
| `maint/copy/compact` | 1.02 [1.01–1.05] | 1.00 [0.98–1.03] | 1.03 [1.01–1.04] | 0.99 [0.97–1.02] | 0.99 [0.99–1.02] | 0.98 [0.98–0.99] |
| `maint/copy/raw` | 1.01 [1.00–1.04] | 1.02 [0.99–1.02] | 1.01 [0.99–1.01] | 1.00 [0.98–1.00] | 0.99 [0.99–1.01] | 0.98 [0.98–0.99] |
| `mixed/rw/8dbs` | 0.99 [0.99–1.02] | 0.99 [0.98–0.99] | 0.99 [0.98–0.99] | 0.99 [0.99–1.00] | 1.00 [1.00–1.00] | 0.98 [0.98–0.99] |
| `put/api/plain` | 1.00 [1.00–1.01] | 0.99 [0.98–0.99] | 0.98 [0.98–0.99] | 0.99 [0.99–1.00] | 1.00 [1.00–1.01] | 0.99 [0.99–1.00] |
| `put/api/reserved` | 1.00 [1.00–1.00] | 0.98 [0.98–0.98] | 0.96 [0.96–0.96] | 0.99 [0.99–1.00] | 0.99 [0.99–1.01] | 0.98 [0.98–0.98] |
| `put/cursor/rewrite` | 1.18 [1.15–1.19] | 0.93 [0.93–0.97] | 0.93 [0.92–0.95] | 0.97 [0.97–1.03] | 0.98 [0.94–1.01] | 0.97 [0.96–0.99] |
| `put/gc/drain_big` | 0.99 [0.99–1.00] | 0.99 [0.99–0.99] | 0.98 [0.97–0.98] | 1.00 [0.98–1.00] | 0.99 [0.99–1.00] | 0.99 [0.98–1.00] |
| `put/gc/ovf_across` | 0.97 [0.97–0.98] | 0.98 [0.95–0.98] | 0.95 [0.94–0.98] | 0.97 [0.96–0.98] | 0.99 [0.97–1.00] | 0.96 [0.94–0.97] |
| `put/gc/ovf_miss` | 0.98 [0.97–0.99] | 0.98 [0.96–1.01] | 0.96 [0.96–0.97] | 0.99 [0.96–0.99] | 0.97 [0.96–1.01] | 0.97 [0.97–0.98] |
| `put/order/append` | 0.98 [0.98–0.98] | 0.99 [0.98–0.99] | 1.05 [1.05–1.06] | 0.99 [0.99–1.00] | 1.00 [0.99–1.01] | 0.98 [0.98–0.99] |
| `put/order/rand` | 0.99 [0.98–1.01] | 0.98 [0.98–1.00] | 0.97 [0.97–0.97] | 0.99 [0.99–1.00] | 1.00 [0.98–1.00] | 0.98 [0.98–0.99] |
| `put/order/seq` | 1.00 [1.00–1.01] | 0.99 [0.99–0.99] | 0.98 [0.98–0.98] | 0.99 [0.99–1.00] | 1.00 [1.00–1.01] | 0.99 [0.99–0.99] |
| `put/over/grow` | 1.01 [1.00–1.01] | 0.99 [0.99–1.00] | 1.00 [1.00–1.01] | 1.00 [1.00–1.01] | 1.00 [1.00–1.01] | 1.00 [0.99–1.01] |
| `put/over/same_size` | 1.01 [0.99–1.01] | 0.98 [0.97–1.00] | 0.96 [0.95–0.96] | 1.00 [0.98–1.01] | 1.00 [0.99–1.03] | 0.99 [0.98–1.00] |
| `put/val/v256` | 1.00 [0.99–1.00] | 0.98 [0.98–0.99] | 0.97 [0.96–0.98] | 0.99 [0.99–1.00] | 1.00 [1.00–1.00] | 0.99 [0.98–1.00] |
| `put/val/v2page` | 1.01 [0.98–1.04] | 1.01 [0.99–1.02] | 1.00 [0.99–1.00] | 0.99 [0.98–1.01] | 0.99 [0.99–1.01] | 0.99 [0.99–0.99] |
| `put/val/v2page_abort` | 1.03 [0.98–1.05] | 1.02 [0.99–1.03] | 1.01 [0.96–1.03] | 1.00 [1.00–1.00] | 1.00 [0.99–1.00] | 0.99 [0.98–1.00] |
| `put/val/v4k` | 1.01 [1.00–1.01] | 1.01 [1.00–1.01] | 1.00 [0.99–1.01] | 1.00 [0.99–1.00] | 1.00 [1.00–1.01] | 0.99 [0.99–1.00] |
| `put/val/v8` | 1.00 [0.99–1.00] | 0.98 [0.97–0.99] | 0.94 [0.94–0.94] | 0.99 [0.99–0.99] | 1.00 [1.00–1.00] | 0.98 [0.98–0.99] |
| `scan/edge/first_last` | 0.95 [0.94–0.95] | 0.99 [0.99–1.00] | 0.95 [0.95–0.95] | 0.95 [0.95–0.95] | 0.92 [0.92–0.93] | 0.89 [0.89–0.90] |
| `scan/full/big` | 0.99 [0.98–1.00] | 1.01 [0.98–1.01] | 1.00 [1.00–1.01] | 1.01 [0.99–1.02] | 1.00 [0.98–1.01] | 1.00 [1.00–1.01] |
| `scan/full/fwd` | 1.01 [1.01–1.01] | 1.01 [0.99–1.03] | 1.02 [1.02–1.02] | 1.00 [0.99–1.08] | 0.98 [0.96–0.98] | 0.98 [0.97–0.98] |
| `scan/full/rev` | 1.00 [1.00–1.01] | 1.01 [1.00–1.01] | 1.03 [1.03–1.03] | 0.99 [0.99–1.01] | 0.98 [0.97–0.99] | 1.01 [0.99–1.02] |
| `scan/meta/len` | 0.80 [0.79–0.80] | 1.00 [1.00–1.02] | 0.78 [0.78–0.78] | 0.66 [0.65–0.67] | 0.58 [0.57–0.61] | 0.60 [0.60–0.62] |
| `scan/prefix/bucket` | 0.99 [0.98–1.00] | 1.00 [0.99–1.00] | 1.01 [1.00–1.02] | 1.00 [1.00–1.00] | 1.08 [1.08–1.09] | 0.86 [0.86–0.86] |
| `scan/range/10pct` | 1.00 [0.99–1.00] | 1.01 [0.99–1.02] | 1.04 [1.03–1.04] | 0.86 [0.86–0.86] | 0.96 [0.96–0.96] | 0.75 [0.75–0.75] |
| `scan/range/1pct` | 0.99 [0.98–1.00] | 1.00 [0.99–1.01] | 1.02 [1.00–1.02] | 0.87 [0.87–0.88] | 0.96 [0.96–0.97] | 0.76 [0.75–0.76] |
| `scan/skip_take/fwd` | 1.00 [1.00–1.01] | 0.98 [0.98–0.98] | 0.98 [0.98–0.99] | 0.97 [0.97–1.00] | 1.03 [1.02–1.05] | 1.04 [1.02–1.06] |
| `scan/skip_take/rev` | 1.04 [1.03–1.04] | 1.00 [1.00–1.01] | 1.03 [1.03–1.04] | 0.97 [0.97–1.03] | 1.03 [1.02–1.07] | 1.05 [1.03–1.09] |
| `seek/ge/gap` | 0.98 [0.97–0.98] | 0.99 [0.97–1.03] | 0.98 [0.94–0.98] | 0.97 [0.96–0.99] | 0.98 [0.97–0.98] | 0.97 [0.96–0.98] |
| `seek/ge/rand` | 0.98 [0.98–0.98] | 0.97 [0.97–0.97] | 0.95 [0.94–0.96] | 0.99 [0.97–1.00] | 0.99 [0.99–1.00] | 0.97 [0.96–0.99] |
| `seek/ge/seq` | 0.96 [0.95–1.00] | 0.99 [0.96–1.00] | 0.96 [0.93–0.97] | 1.01 [0.93–1.02] | 0.92 [0.89–0.95] | 0.93 [0.88–0.99] |

### Where LTO helps: the point-get path

Even under fat LTO, the scan step and the point-get descent are not inlined
into the caller. The binaries' disassembly shows heed-zerodb's (generic,
caller-instantiated) `Database::get` calling `zerodb_core::btree::Tree::get`
out of line, and the iterator's `next` calling `RoRange::next` out of line,
in all four profiles. The LTO gain on gets is in the small calls around the
descent.

`perf` of `get/access/hot` (one key read 10,000 times in one read txn),
self time:

| frame | `cgu1` | `fat` |
|---|---:|---:|
| `Tree::find_exact` (the descent) | 45.5 % | 48.3 % |
| leaf binary search (`leaf_lookup` / `LeafRef::lookup_with`) | 9.1 % | 9.7 % |
| heed-zerodb `Database::get` | 7.8 % | 6.2 % |
| `<RoTxn as TxnRead>::record_for` | 1.3 % | 1.5 % |
| `<RoTxn as TxnRead>::check_db` | 1.3 % | 1.5 % |
| `<RoTxn as TxnRead>::source` | 0.9 % | inlined |
| `<RoTxn as TxnRead>::comparator_for` | 0.5 % | inlined |
| `<MmapBacking as Backing>::bytes` (a `dyn Backing` call per op) | 1.7 % | 0.9 % |
| `DirtyStore::bytes` | 1.5 % | 1.3 % |

The functions that exist out of line under `cgu1` but are inlined under
`fat` include `<RoTxn as TxnRead>::source` and `::comparator_for`,
`RoTxn::map_bytes`, `Env::map_size`, `EnvInner::pin_reader`, `BranchRef::key`,
`LeafRef::key` and `CommonHeader::read`. These are all small, non-generic
`zerodb-core` functions called from code that heed-zerodb instantiates. Under
`cgu16` and `cgu1`, nothing outside `zerodb-core` can inline them.

**Candidate `#[inline]` sites** (not applied; see "Next steps"):

1. `impl TxnRead for RoTxn` (`crates/zerodb-core/src/rotxn.rs`): `source`,
   `comparator_for`, `page_size`, `main_record`, and the trivial
   `EnvInner::comparator_for` / `backing_bytes` they call. They are called once
   or more per `get`, `first`, `last` and iterator constructor from the
   generic `Database` methods heed-zerodb instantiates. `check_db` already
   carries `#[inline]`, but LLVM does not inline it. The same
   applies to the `NestedRoTxn` and `RwTxn` impls.
2. `BranchRef::key`, `LeafRef::key`, `CommonHeader::read`
   (`zerodb-core::page`): tiny accessors that stay out of line in some
   `zerodb-core` callers under `cgu1`.
3. Not an `#[inline]` fix: `Source::Map` re-reads the map through a `dyn
   Backing` call on every operation (`MmapBacking::bytes`, 0.9–1.7 % of a hot
   get). LTO cannot devirtualise it. Caching the map slice in the read txn
   would remove it.

The most this can recover is what thin LTO shows: 3–10 % on `get/*`, none on
scans or writes.

### Where 1 codegen unit hurts: the write cursor

| rung | ZeroDB `cgu1` ÷ `cgu16` | LMDB `cgu1` ÷ `cgu16` | ZeroDB `thin` ÷ `cgu16` | ZeroDB `fat` ÷ `cgu16` |
|---|---:|---:|---:|---:|
| `put/cursor/rewrite` | 1.19 [1.18–1.19] | 1.00 | 0.95 | 0.94 |
| `del/cursor/drain` | 1.09 [1.08–1.11] | 1.00 | 0.98 | 0.92 |

All three rounds agree, so this is not noise. The `put/cursor/rewrite`
profile is dominated by its per-iteration fixture build, which looks the same
in both binaries. The one visible difference is
`Database::checked_tree::<RwTxn>` (`#[inline]`), which is out of line under
`cgu1` (1.6 %) and inlined under `cgu16`. That alone does not account for
19 %. The cause needs a profile of the timed cursor loop without the fixture,
and is not established here.

## 2. Scans on Graviton

`bench-ab.sh '^(scan|seek)/'`, 3 rounds, default profile. "before" =
validating default, "after" = trusted-file mode, same binary. LMDB moved at
most 1.5 % between halves, and no rung was flagged unreliable.

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `scan/edge/first_last` 🎯 | 1.040 ms | 1.463 ms | 1.482 ms | 1.41× → **1.42×** | 1.013 (±0.030) | 0.999 | flat |
| `scan/full/big` 🎯 | 17.510 ms | 28.097 ms | 18.043 ms | 1.60× → **1.03×** | 0.639 (±0.030) | 1.000 | ✅ faster |
| `scan/full/fwd` 🎯 | 492.264 µs | 724.225 µs | 535.032 µs | 1.47× → **1.09×** | 0.740 (±0.046) | 0.992 | ✅ faster |
| `scan/full/rev` 🎯 | 490.144 µs | 721.155 µs | 512.960 µs | 1.47× → **1.05×** | 0.714 (±0.030) | 0.992 | ✅ faster |
| `scan/meta/len` 🎯 | 67.373 µs | 75.280 µs | 77.174 µs | 1.12× → **1.15×** | 1.023 (±0.030) | 1.003 | flat |
| `scan/prefix/bucket` 🎯 | 3.379 µs | 4.286 µs | 3.635 µs | 1.27× → **1.08×** | 0.852 (±0.030) | 1.003 | ✅ faster |
| `scan/range/10pct` 🎯 | 80.404 µs | 66.290 µs | 55.868 µs | 0.82× → **0.69×** | 0.842 (±0.030) | 0.999 | ✅ faster |
| `scan/range/1pct` 🎯 | 8.748 µs | 7.396 µs | 6.081 µs | 0.85× → **0.70×** | 0.825 (±0.030) | 1.001 | ✅ faster |
| `scan/skip_take/fwd` 🎯 | 49.203 µs | 22.994 µs | 11.651 µs | 0.47× → **0.24×** | 0.506 (±0.048) | 0.993 | ✅ faster |
| `scan/skip_take/rev` 🎯 | 49.580 µs | 23.261 µs | 11.803 µs | 0.47× → **0.24×** | 0.507 (±0.030) | 0.989 | ✅ faster |
| `seek/ge/gap` 🎯 | 4.125 ms | 4.388 ms | 4.182 ms | 1.06× → **1.01×** | 0.953 (±0.030) | 0.991 | ✅ faster |
| `seek/ge/rand` 🎯 | 3.911 ms | 3.593 ms | 3.466 ms | 0.92× → **0.89×** | 0.953 (±0.030) | 1.008 | ✅ faster |
| `seek/ge/seq` 🎯 | 2.042 ms | 1.902 ms | 1.735 ms | 0.93× → **0.85×** | 0.913 (±0.060) | 1.015 | ✅ faster |

**Verdict: improved**. Improved: 11, regressed: 0, flat: 2, unreliable (LMDB drift > 3%): 0. 3 interleaved round(s); before = `0adf868c3`, after = `0adf868`, host `Linux aarch64`.

The ladder's three `cgu16` passes reproduce the "before" column within 0.01–0.03.

### Why the remaining gaps

**`scan/full/fwd`, `scan/full/rev` (1.47× → 1.05–1.09×), `scan/full/big`
(1.60× → 1.03×), `scan/prefix/bucket` (1.27× → 1.08×): page validation,
then per-step cost.** Validating `scan/full/fwd` profile, self time:

| frame | share |
|---|---:|
| `Cursor::next` | 22.6 % |
| `RoRange::next` | 14.4 % |
| harness loop | 7.3 % |
| `leaf_view_over` (leaf cell validation) | 6.1 % |
| `StampCache::contains` (validation cache probe) | 4.6 % |
| `__aarch64_cas8_acq_rel` + `__aarch64_ldadd8_relax` | 5.5 % |
| `Cursor::descend_min` | 4.2 % |
| `branch_view_over` | 2.8 % |

Both atomic helpers are called only from `ValidatedPages::insert`, which runs
from `validate_leaf_miss` once per leaf a scan enters. They are out of line
because Rust's `aarch64-unknown-linux-gnu` target uses outline atomics: every
atomic read-modify-write calls a helper that checks at run time whether the
CPU has the LSE instructions. Together, validation is about 16–19 % of the
validating scan. With `scan/full/big` (300 MB, cache-missing), it reaches
`descend_min` 42.9 % self (the leaf cell walk, inlined there) plus
`StampCache::contains` 7.8 %.

In trusted mode those frames are gone. What remains is the step:
`Cursor::next` 28.6 % and `RoRange::next` 18.3 %, two out-of-line calls per
entry from the iterator. LMDB makes one (`mdb_cursor_get`). That is the
1.05–1.09× residual.

**`scan/edge/first_last` (1.41×, 1.42× trusted): root-to-leaf re-resolution
on every call.** Each `first` or `last` checks the handle, builds the
`DBRecord` and `Tree`, opens a cursor, and descends from the root, resolving
every level with header and bounds checks (`pgno × psize` overflow test,
page-type byte, memo probe). Profile:

| ZeroDB frame | share | LMDB frame | share |
|---|---:|---|---:|
| `Cursor::descend_max` | 20.7 % | `mdb_page_search` | 33.1 % |
| `Cursor::descend_min` | 14.0 % | `mdb_cursor_last` | 7.2 % |
| heed-zerodb `Database::first` + `::last` | 17.5 % | `mdb_page_get` | 6.8 % |
| `branch_view_over` | 5.2 % | `malloc` + `free` (heed's cursor) | 6.1 % |
| `Cursor::current` | 4.8 % | `mdb_cursor_first` | 4.0 % |
| `record_for` + `check_db` | 3.6 % | `mdb_cursor_open` | 3.9 % |

An annotation of `descend_max` spreads its time over the page-header load,
the page-type compare and the `pgno × psize` multiply-with-overflow-check of
each level, with no single hot instruction. This is per-level page-resolution
cost, about 73 ns per call against LMDB's 52 ns. It is not validation, which
is why trusted mode does not move it. In a read txn, LMDB resolves each level
with one pointer computation (`mdb_page_get`) and no per-page checks.

**`scan/meta/len` (1.12–1.15×)** is 7.5 ns against 6.7 ns per call:
`check_db` plus `record_for`. Its ratio changes by ±40 % with code layout
across build profiles (table above), so it is not a real gap.

**`seek/ge/gap` (1.06× → 1.01×)** is mostly validation.

## Caveats

- One host type, one run per profile per round. Rounds agreed within 1–2 % on
  most rungs (see the min–max columns). The ±3 % threshold `bench-ab.py`
  uses is the right bar for single-rung claims.
- The LMDB column moves with the profile because heed's Rust wrapper and the
  harness are rebuilt. It is a drift control for the C library only.
- Profiles use `--profile-time` loops that include fixture setup. Shares are
  of the whole process, not of the timed loop alone.
- Default tier only: the 1M-key rung and the concurrent suite (`long` tier)
  were not run.

## Next steps (proposed, not done)

1. Try the `#[inline]` candidates above as one change. Three-column A/B under
   `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1` (how Meilisearch builds) on
   `^(get|scan|seek|env/txn)/`. Expected: up to the 3–10 % thin LTO shows on
   `get/*`.
2. Profile `put/cursor/rewrite` under `cgu1` vs `cgu16` with a loop that
   excludes the fixture, to find the 19 % codegen-unit regression.
3. Measure `ValidatedPages::insert` with LSE atomics (`-C
   target-feature=+lse`, which every Graviton has) or with a non-RMW insert
   path, for the validating scans.
4. For `first_last`: keep the resolved root page view in the read txn, so a
   descent does not re-check the root on every call.

## Reproduce

On a fresh c8g.xlarge with a checkout of `0adf868` in `~/zerodb`:

```
cd ~/zerodb
for p in cgu16:"" cgu1:CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 \
         thin:CARGO_PROFILE_BENCH_LTO=thin fat:CARGO_PROFILE_BENCH_LTO=fat; do
  n=${p%%:*}; e=${p#*:}
  env $e CARGO_TARGET_DIR=~/t-$n cargo bench -p zerodb-oracle \
    --bench engine_comparison --no-run --message-format=json |
    jq -r 'select(.executable != null and .target.name == "engine_comparison") | .executable' |
    tail -1 | xargs -I{} cp {} ~/bin/$n
done
ROUNDS=3 CAND_ENV=ZERODB_BENCH_TRUST_FILE=1 scripts/bench-ab.sh '^(scan|seek)/'
for r in 1 2 3; do for p in <rotated order>; do
  CRITERION_HOME=~/out/ladder/round-$r/$p ~/bin/$p --bench --noplot
done; done
```

Per round and profile, ZeroDB ÷ LMDB and engine ÷ engine-under-`cgu16` are
computed from `scripts/bench-report.py`'s `load_estimates` (criterion's slope
estimate, or the mean for flat sampling), then the median over rounds is taken.
