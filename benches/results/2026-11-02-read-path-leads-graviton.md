# Read-path leads on Graviton4 (#147)

- **Date:** 2026-11-02.
- **Question (issue #147):** the build-profile run of #139 found four read-path
  leads on `main` at `0adf868`. Re-measure each on current `main`, try the
  proposed change, and keep it only with a three-column win.
- **Host:** AWS c8g.xlarge, Graviton4 (Neoverse-V2), 4 vCPU, 8 GiB, Ubuntu
  24.04, kernel 7.0.0-1014-aws, 4 KiB OS and database pages, 60 GiB gp3 root,
  rustc 1.99.1. Launched for this run and shut down after it. Load average 1.0
  during every run.
- **LMDB:** the Meilisearch fork vendored by `lmdb-master-sys 0.2.6`, through
  heed 0.22.1.
- **Method:** `scripts/bench-ab.sh`, 3 interleaved rounds, LMDB measured in both
  halves as the drift control, at `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1` (how
  Meilisearch builds) unless stated. Ratios are ZeroDB ÷ LMDB; "after ÷ before"
  is ZeroDB's own speedup, below 1 is faster.

## Summary

| lead | change | result | kept |
|---|---|---|---|
| 3 | `#[inline]` on the small `zerodb-core` accessors the heed adapter calls per operation | 8 rungs faster, 0 slower: `get/access/miss` 0.90, `get/access/hot` 0.93, `scan/edge/first_last` 0.93 | yes, `d37076c` |
| 1 | resolve the root once per read txn (a per-database copy of the root's memo entry) | used by every descent: `first_last` 1.03 slower, gets 0.93–0.96. Used by keyed descents only: 6 faster, 0 slower (`get/db/root` 0.93, `get/access/miss` 0.93), `first_last` 1.03 (flat) | keyed-only variant, `aaf6c33` |
| 4 | none: find why one codegen unit slows the write cursor | the regression is gone on current `main`: CGU1 ÷ CGU16 is 0.94 (`put/cursor/rewrite`) and 0.99 (`del/cursor/drain`) | — |
| 2 | none: size the validated-pages insert in scans | insert path 6.9 % of a validating `scan/full/fwd`; inline LSE atomics change nothing (0.99–1.00) | — |

`scan/edge/first_last` is still 1.50× LMDB after both kept changes. Its gap is
not the root resolution; see lead 1.

## Current `main` (the "before" column of lead 3)

| rung | ZeroDB ÷ LMDB at `3c4f37f`, CGU1 |
|---|---:|
| `scan/edge/first_last` | 1.61 |
| `scan/full/big` | 1.42 |
| `scan/full/fwd` | 1.39 |
| `scan/full/rev` | 1.44 |
| `scan/meta/len` | 1.41 |
| `scan/prefix/bucket` | 1.20 |
| `get/access/hot` | 0.75 |
| `get/access/miss` | 1.03 |
| `get/val/v4k` | 1.23 |
| `get/val/v2page` | 1.25 |

## Lead 3: cross-crate `#[inline]`

`heed-zerodb` instantiates the generic read methods in the consumer's crate.
Without `#[inline]`, the small non-generic accessors they call stay out of
line there under one codegen unit: `RoTxn`'s `source`, `page_size`,
`main_record`, `comparator_for`, `validated_pages`, `map_bytes`,
`env_ident`, the `Env` / `EnvInner` accessors behind them, `Tree`'s
constructors, `CommonHeader::read` and the unchecked `LeafRef::key` /
`BranchRef::key`.

`bench-ab.sh '^(get|scan|seek)/'`, before = `3c4f37f`, after = `d37076c`:

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `get/access/hot` | 1.274 ms | 960.0 µs | 889.9 µs | 0.75× → 0.70× | 0.927 | 0.994 | faster |
| `get/access/miss` | 855.7 µs | 882.3 µs | 792.3 µs | 1.03× → 0.93× | 0.899 | 0.998 | faster |
| `get/access/rand` | 3.443 ms | 3.029 ms | 3.014 ms | 0.88× → 0.88× | 0.947 (±0.113) | 1.005 | flat |
| `get/access/seq` | 1.421 ms | 1.297 ms | 1.202 ms | 0.91× → 0.85× | 0.926 | 1.000 | faster |
| `get/size/n1k` | 1.353 ms | 1.264 ms | 1.193 ms | 0.93× → 0.88× | 0.944 | 1.004 | faster |
| `get/val/v2page` | 1.830 ms | 2.292 ms | 2.187 ms | 1.25× → 1.19× | 0.954 | 0.991 | faster |
| `get/val/v8` | 1.784 ms | 1.559 ms | 1.460 ms | 0.87× → 0.82× | 0.937 | 0.991 | faster |
| `get/val/v8_touch` | 1.790 ms | 1.572 ms | 1.465 ms | 0.88× → 0.82× | 0.934 | 0.993 | faster |
| `scan/edge/first_last` | 984.8 µs | 1.581 ms | 1.475 ms | 1.61× → 1.50× | 0.933 | 0.996 | faster |
| `scan/full/fwd` | 483.7 µs | 670.4 µs | 674.7 µs | 1.39× → 1.39× | 1.006 | 0.994 | flat |
| `scan/full/big` | 16.70 ms | 23.73 ms | 23.35 ms | 1.42× → 1.40× | 0.989 | 0.986 | flat |
| `seek/ge/seq` | 1.858 ms | 1.952 ms | 1.869 ms | 1.05× → 1.01× | 0.953 (±0.054) | 1.002 | flat |

Improved 8, regressed 0, flat 21, unreliable 3 (LMDB drift over 3 %:
`get/db/named_x8`, `get/val/v256`, `scan/prefix/bucket`). The flat rungs
moved between 0.95 and 1.02.

## Lead 1: the root, resolved once per read transaction

A read txn keeps, per database, a copy of the memo entry of its root page
(page number and kind), next to the database's record, so a descent from the
root skips the page-type check and the memo probe there. The copy is made only
for a map page the memo already holds, so it vouches for nothing the memo does
not (SPEC 04 TXN-38).

Before = `d37076c` (lead 3) in both runs.

| rung | slot in every descent (`65c4aab`) | slot in keyed descents only (`aaf6c33`) |
|---|---:|---:|
| `scan/edge/first_last` | **1.030, slower** | 1.028 (flat) |
| `get/access/hot` | 0.952 | 0.942 |
| `get/access/miss` | 0.935 | 0.929 |
| `get/access/rand` | 0.972 (±0.102) | 0.948 |
| `get/access/seq` | 0.957 | 0.964 |
| `get/db/named` | 0.982 | 0.967 |
| `get/db/root` | 0.932 | 0.927 |
| `seek/ge/rand` | 0.934 (±0.120) | 0.970 (±0.039) |
| `seek/ge/seq` | 0.928 | 0.923 (LMDB drift 0.963) |
| verdict | regressed (target slower) | improved 6, regressed 0 |

The lead's own target did not move. A memo hit on the root is already cheap,
and in `descend_min` / `descend_max` the extra level-0 branch cost as much as
the slot saved. Point lookups and seeks, which compare the key at every level,
gain 3–7 %. The keyed-only variant is kept for those. `first_last` stays about
3 % slower with either variant, under the 3 % threshold. The likely cause is
the wider `Tree` and `Cursor` it builds on every call.

What `first_last` still pays, from the #139 profile and a profile of this
change on aarch64 macOS: two out-of-line handle and record lookups per call
(`check_db`, `record_for`), one per-level page resolution, a 512-byte path
stack initialised per cursor, and the heed adapter's `Database::first` /
`last` wrapper. LMDB's equivalent is one `mdb_cursor_open` plus
`mdb_page_search`.

## Lead 4: one codegen unit and the write cursor

Same source (branch head, engine as `d37076c`), two bench binaries, 3 rounds of
the two rungs, then `cursor_put_census` (no fixture, 5 runs, CPU ns per op).

| | ZeroDB CGU1 | ZeroDB CGU16 | CGU1 ÷ CGU16 | LMDB CGU1 ÷ CGU16 |
|---|---:|---:|---:|---:|
| `put/cursor/rewrite` | 2.133 ms | 2.266 ms | 0.942 | 1.000 |
| `del/cursor/drain` | 9.368 ms | 9.482 ms | 0.988 | 1.000 |
| census rewrite (abort) | 123.3 ns | 134.3 ns | 0.918 | 1.000 |
| census rewrite (commit) | 170.7 ns | 178.8 ns | 0.955 | 1.021 |
| census drain (abort) | 360.9 ns | 371.6 ns | 0.971 | 0.999 |

At `0adf868`, #139 measured CGU1 ÷ CGU16 at 1.19 and 1.09. On current `main`,
one codegen unit is faster for both, so there is no cause left to fix.
`put/cursor/rewrite` is 1.39× LMDB and `del/cursor/drain` 1.79× (CGU1); the
per-entry cost belongs to the write-cursor work (#148).

## Lead 2: validation in full scans

`perf record` of `scan/full/fwd` (ZeroDB, default profile), self time:

| frame | share |
|---|---:|
| `Cursor::next` | 21.7 % |
| `RoRange::next` | 14.5 % |
| harness loop | 7.9 % |
| `leaf_view_over` | 6.3 % |
| `StampCache::contains` | 4.8 % |
| `Cursor::descend_min` | 4.7 % |
| `__aarch64_cas8_acq_rel` | 3.3 % |
| `__aarch64_ldadd8_relax` | 2.1 % |
| `validate_leaf_miss` | 1.7 % |
| `ValidatedPages::insert` | 1.5 % |

The memo insert path is 6.9 %. The same source built with `-C
target-feature=+lse` (atomics inline, no run-time dispatch), 3 rounds:
`scan/full/fwd` 0.992, `scan/full/rev` 1.000, `scan/full/big` 0.994,
`scan/prefix/bucket` 1.002. So the cost is the two atomic
read-modify-writes per leaf, not the outline call.

Removing them is not a code-generation fix:

- A plain load-and-store insert can lose an entry when two threads insert at
  once. The memo's tests require every inserted page to be found again, and
  its drop relies on an exact count to clear the slots it hands to the next
  transaction.
- Not inserting leaves a scan enters into the txn memo (a later visit would
  hit the env-wide stamp cache instead) changes which pages count as validated
  for the transaction.

Both need a maintainer decision. ADR-0034, if approved, removes validation
from the heed path by default, which makes this lead apply only to ZeroDB's
native API.

## Reproduce

```
git checkout <after>
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 BASE=<before> ROUNDS=3 scripts/bench-ab.sh '^(get|scan|seek)/'
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 BASE=d37076c ROUNDS=3 scripts/bench-ab.sh '^(scan/edge|get/access|get/db|seek/ge)'
# lead 4: build engine_comparison and the cursor_put_census example with
# CARGO_PROFILE_{BENCH,RELEASE}_CODEGEN_UNITS=1 and =16 into separate target
# dirs; run '^(put/cursor/rewrite|del/cursor/drain)' and
# `cursor_put_census <engine> <rewrite|drain> 30` (CENSUS_COMMIT=1 for commits).
# lead 2: perf record -F 1999 -g on `--bench --profile-time 12 '^scan/full/fwd/zerodb$'`;
# RUSTFLAGS="-C target-feature=+lse" for the inline-atomics binary.
```
