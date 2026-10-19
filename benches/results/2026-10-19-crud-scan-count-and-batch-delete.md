# crud-bench's worst rows: full `count()`, `start(5000) limit(100)`, `batch_delete_1000`

- **Date:** 2026-10-19.
- **Question:** after ADR-0028 and the meta CRC work, the rows furthest behind LMDB
  in crud-bench (500,000 random `string26` keys, 128 clients, 48 threads,
  `--optimised`: `WRITE_MAP | MAP_ASYNC | NO_SYNC`, trusted-file mode) were the full
  `count()` scan (~0.55× LMDB), `start(5000) limit(100)` (0.7–0.8×) and
  `batch_delete_1000` (~0.72×). Where does ZeroDB spend the time, what does LMDB do
  on the same path, and what can be closed?
- **ZeroDB:** `main` at `e96ea4a` (before; it includes ADR-0028 and #118) and that
  commit plus this change (after). The ADR-0029 hardware-CRC branch is not included;
  it only changes the per-commit meta checksum.
- **Host:** Apple M-series Mac, 8 cores, 16 GiB, 16 KiB OS and database pages. Not a
  quiet host: other sessions ran stress tests, a container VM and a video call (load
  average 4–5 for the repro runs, 9–41 for the criterion and crud-bench runs). Every
  comparison interleaves its sides and runs LMDB in the same window as the drift
  control.

## Summary

| row | cause | result |
|---|---|---|
| `start(5000) limit(100)` | `skip` stepped every skipped entry | already fixed by #118 (`Iterator::nth` crosses whole leaves): ZeroDB is now 2.2–4.6× LMDB's throughput in every round |
| full `count()` | per-txn validated-pages memo bookkeeping for every leaf, and a long per-entry step | **fixed here**: repro 20 → 34 ops/s against LMDB's 33 (single thread); Graviton4 `scan/full/*` 2.05× → 1.02× LMDB's time, no rung regressed |
| `batch_delete_1000` | at 16 KiB the copy-on-write copy moving the whole page; LMDB copies only the used bytes | **ADR-0030 accepted and implemented**: ~11 % per delete at 16 KiB (Mac spike), flat at 4 KiB on Graviton4, no rung regressed; the 4 KiB gap (0.78×) is elsewhere |

## Full `count()`

### Repro

A standalone binary links heed (LMDB, `lmdb-master-sys 0.2.6`) and heed-zerodb.
It loads 500,000 random 26-letter keys with 900–1,500-byte values in 1,000-key
transactions, with the same flags as crud-bench's `--optimised` run. That gives
~0.9 GB per engine and ~10 entries per 16 KiB leaf. It then times
`db.iter(&txn)?.skip(0).take(usize::MAX).count()`, exactly crud-bench's
expression, in a fresh read transaction each time, after one untimed warm-up pass.

### Profile (before, single thread, `sample`, 4 s)

| frame (self time) | samples | share |
|---|---:|---:|
| `Cursor::current` (reading the entry: the cell cache misses) | 1,778 | 53 % |
| `leaf_view_over` (memo probe on each new leaf) | 476 | 14 % |
| `branch_view_over` (memo probe on each leaf hop's parent) | 360 | 11 % |
| `RoRange::next` | 272 | 8 % |
| `Cursor::next` | 117 | 4 % |
| `ValidatedPages::insert` | 115 | 3 % |
| `resolve_value` | 102 | 3 % |

LMDB's profile of the same walk is `mdb_cursor_next` (2,639), `mdb_cursor_get`
(380) and `mdb_node_read` (77): bump the slot index, read the node, return.

**Why the memo cost so much.** Every read transaction has a memo of the pages it
has already checked (ADR-0014, ADR-0018). Under the trusting policy a miss runs
the O(1) header checks and then *records* the page so that later views skip them.
A scan sees each of its ~50,000 leaves once, so every leaf is a miss. Each
insert grows the memo, four times larger per level, to ~2 MB of tables
allocated and zeroed per transaction. Each probe then misses through every
level, each probe a likely cache miss. LMDB keeps no such table.

**Why the step mattered on a cache-missing walk.** With the memo gone (variant 1
below), the time moved into `resolve_value`, `RoRange::next` and the caller
instead of disappearing. The walk is bound by cache-miss latency, and how many
of the next entries' misses the core can overlap depends on how long each step's
code is. ZeroDB's step went `RoRange::next` → `Cursor::next` → `Cursor::current`,
which re-derived the position (flags, stack top, leaf cache, bound check). Then it
called an out-of-line `resolve_value`, which copied the source handle and
returned through memory.

### Change

1. **Trusted leaves are not recorded in the memo** (`leaf_view_over`; ADR-0014
   amendment 2026-10-19). A trusted leaf's memo miss still runs the header checks
   (page type, reserved fields, free-space bounds) and returns the view. It just
   does not record the page. Branches are still recorded: there are few of them,
   so the memo stays one small level, and every descent revisits them. The
   validating policy is unchanged, code path included: the trust flag is still
   tested only on a memo miss.
2. **The same-leaf step reads the entry from the leaf view in hand**
   (`Cursor::next`, `Cursor::prev`), as LMDB's `mdb_cursor_next` reads the node
   right after `mc_ki++`, instead of going through `current()`. `resolve_value`'s
   inline-value arm is forced inline. Its overflow arm moved to an out-of-line
   `resolve_overflow`, so the inline arm stays small at its call sites.

No format, API or behavior change. The entries returned and the errors raised
are the same.

### Results

**Repro, single thread** (ops/s, 3 interleaved rounds, load 3.9–4.6):

| round | LMDB | ZeroDB before | step change only | both changes |
|---|---:|---:|---:|---:|
| r1 | 33 | 20 | 29 | 33 |
| r2 | 35 | 17 | 30 | 35 |
| r3 | 31 | 21 | 32 | 34 |

So 0.57× → **~1.0× LMDB**. The step change gives the larger share, and the memo
change the rest.

**Repro, 48 threads** (ops/s, 2 rounds, crud-bench's thread count):

| round | LMDB | ZeroDB before | step only | both |
|---|---:|---:|---:|---:|
| r1 | 249 | 162 | 167 | 229 |
| r2 | 180 | 149 | 180 | 208 |

Under concurrency the memo change matters more: every transaction allocated and
zeroed its own growing memo.

**Hot point gets under the trusting policy** (the 2026-09-28 note in ADR-0014
found that recording *nothing* cost hot reads 4–5 %). Same key, 1M gets in one
read transaction, best of 10, 5 interleaved rounds at load 20–30: before
349–377 ns, step-only 334–389 ns, both 345–392 ns. No difference the host can
resolve. An earlier 4-round run read +4–12 %, but the next run did not
reproduce it. The Graviton4 gate below settles it: `get/access/hot` is faster
(0.89–0.90× of before) in both policies.

**Criterion** (`scripts/bench-ab.sh '^(scan|get|seek)/'`, 3 rounds each):

- Validating policy: verdict **invalid**, load 8.9. 25 of 31 rungs failed the
  LMDB-drift gate. `scan/range/1pct` passed as faster (0.620 ±0.072). The rest
  point the same way but cannot be read: `scan/full/fwd` 0.531 ±0.508,
  `scan/full/rev` 0.533 ±0.712, `scan/range/10pct` 0.579 ±0.107. No rung passed
  the gate as regressed.
- Trusting policy: verdict **invalid**, load 20.6. 30 of 32 rungs drifted. The
  new `scan/full/big` rung (below) read 1.47× → 0.91× LMDB's time
  (0.671 ±0.423).

New rung **`scan/full/big`**: a forward walk over 200,000 random-order inserts of
1,200-byte values (~300 MB). It is the crud-bench `count()` shape, which
`scan/full/fwd` (50,000 ascending keys, 256-byte values, cache-resident) does not
cover. Added to BENCH-MAP.

**crud-bench** (3 rounds, load 30–41, medians in ops/s). The write rows read
0.26–0.56× LMDB on *both* sides against 0.91–0.93× this morning, so this run
measures the host as much as the engines. Read it per round:

| `count()` per round | LMDB | ZeroDB | ZeroDB/LMDB |
|---|---:|---:|---:|
| before r1 | 40 | 51 | 1.28 (LMDB outlier) |
| before r2 | 117 | 62 | 0.53 |
| before r3 | 114 | 33 | 0.29 |
| after r1 | 80 | 81 | 1.01 |
| after r2 | 127 | 125 | 0.98 |
| after r3 | 57 | 53 | 0.93 |

Medians: before 0.45×, after 1.01× LMDB.

## `start(5000) limit(100)`

#118 (merged as `e96ea4a`, the base here) made `Iterator::nth` cross whole leaves.
The 0.7–0.8× in the question came from runs before it. In this run, both builds
contain it, and ZeroDB's `select(id)` and `select(*)` rows were 76,941–138,826
ops/s against LMDB's 25,280–50,246 in every round: 2.2–4.6× LMDB's throughput.
Nothing left to do.

## `batch_delete_1000`

### Repro

Same binary, crud-bench's order on a fresh environment: load 500k, 250 × 1,000
batch creates, 250 × 1,000 batch updates of those keys, then 250 timed
transactions each deleting 1,000 of them. LMDB ~2.2 µs per delete, ZeroDB
~2.65–2.9 µs (0.79–0.83×). That is close to crud-bench's 0.65–0.72×. An earlier
version that re-created the keys in one 250k-put transaction measured file-growth
page faults (90 µs per delete on both engines) and was discarded.

### Profile (before, delete phase only)

| frame | share |
|---|---:|
| `touch` → `DirtyStore::insert_copy` → `memmove` (the copy-on-write copy) | 37 % |
| `search_path_into` (descent; the hot spot is the branch binary-search key compare, a cache miss) | 40 % |
| `LeafMut::remove` → `memmove` (cell heap compaction) | 9 % |
| commit | 3 % |

The descent is the same work LMDB's `mdb_node_search` does. Batched point reads
in a read transaction are at parity (`batch_read_1000` 0.97–1.15× in earlier
runs), so the descent is not the gap. The count-scan change does not move this
row: repro 2.63–2.90 µs before and after.

### What LMDB does differently

`mdb_page_copy` copies the header and pointer array (`[0, lower)`) and the cell
heap (`[upper, end)`), and skips the free gap between them. ZeroDB's
`insert_copy` copies all 16 KiB. With ~10 entries of ~1.2 KB per leaf, the gap is
often 2–4 KiB.

### Spike

A scratch build copied only the used bytes. In 5 interleaved rounds (load 10–13,
one outlier round dropped), it ran ~2.43 µs per delete against ~2.73 µs, about
11 % faster and close to LMDB's ~2.2–2.4 µs. It changes which bytes a committed
page carries in its gap: the earlier contents of its new page number instead of
its source's gap. Nothing reads gap bytes, and ZeroDB already leaves stale bytes
there (`remove_cell` does not zero what it vacates). It is still a change to the
bytes written to disk, so it went through **ADR-0030**, accepted (Option A)
and implemented; the Graviton4 measurement is further below.

## Graviton4 gate (quiet host)

`c8g.xlarge` (Graviton4, 4 vCPU, 8 GiB, Ubuntu 24.04, **4 KiB pages**, CGU1),
load ~1. `scripts/bench-ab.sh '^(scan|get|seek)/'`, 3 interleaved rounds each,
before = `e96ea4a`, after = this change. Both verdicts: **improved, 0 regressed**.
Excerpt: every scan rung, the `get/access` rungs and a few others. No rung
outside the excerpt regressed either; all are faster or flat.

### Validating policy (the default)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `get/access/hot` | 1.262 ms | 1.115 ms | 1.007 ms | 0.88× → **0.80×** | 0.903 (±0.030) | 0.956 | ⚠ drift |
| `get/access/miss` | 861.675 µs | 945.599 µs | 909.761 µs | 1.10× → **1.06×** | 0.960 (±0.030) | 0.978 | ✅ faster |
| `get/access/rand` | 3.326 ms | 3.060 ms | 2.958 ms | 0.92× → **0.89×** | 0.942 (±0.037) | 1.001 | ✅ faster |
| `get/access/seq` | 1.437 ms | 1.394 ms | 1.317 ms | 0.97× → **0.92×** | 0.945 (±0.030) | 0.975 | ✅ faster |
| `get/db/root` | 3.283 ms | 3.113 ms | 2.966 ms | 0.95× → **0.90×** | 0.959 (±0.053) | 0.980 | flat |
| `get/val/v8` | 1.808 ms | 1.634 ms | 1.560 ms | 0.90× → **0.86×** | 0.955 (±0.030) | 0.977 | ✅ faster |
| `scan/edge/first_last` | 1.028 ms | 1.832 ms | 1.474 ms | 1.78× → **1.43×** | 0.805 (±0.030) | 1.000 | ✅ faster |
| `scan/full/big` 🎯 | 17.224 ms | 30.376 ms | 28.285 ms | 1.76× → **1.64×** | 0.933 (±0.030) | 0.998 | ✅ faster |
| `scan/full/fwd` 🎯 | 478.738 µs | 1.076 ms | 692.790 µs | 2.25× → **1.45×** | 0.641 (±0.030) | 1.000 | ✅ faster |
| `scan/full/rev` 🎯 | 480.016 µs | 1.053 ms | 699.496 µs | 2.19× → **1.46×** | 0.664 (±0.030) | 1.000 | ✅ faster |
| `scan/meta/len` | 67.428 µs | 76.186 µs | 75.738 µs | 1.13× → **1.12×** | 0.994 (±0.030) | 0.984 | flat |
| `scan/prefix/bucket` | 3.391 µs | 6.674 µs | 4.515 µs | 1.97× → **1.33×** | 0.676 (±0.030) | 1.000 | ✅ faster |
| `scan/range/10pct` | 80.130 µs | 105.085 µs | 66.289 µs | 1.31× → **0.83×** | 0.631 (±0.045) | 0.996 | ✅ faster |
| `scan/range/1pct` | 8.457 µs | 11.644 µs | 7.468 µs | 1.38× → **0.88×** | 0.645 (±0.031) | 0.990 | ✅ faster |
| `scan/skip_take/fwd` | 49.213 µs | 24.059 µs | 23.036 µs | 0.49× → **0.47×** | 0.957 (±0.030) | 0.997 | ✅ faster |
| `scan/skip_take/rev` | 49.804 µs | 24.393 µs | 23.385 µs | 0.49× → **0.47×** | 0.959 (±0.030) | 1.000 | ✅ faster |
| `seek/ge/rand` | 3.516 ms | 3.275 ms | 3.253 ms | 0.93× → **0.93×** | 0.984 (±0.089) | 1.024 | flat |

**Verdict: improved**. Improved: 17, regressed: 0, flat: 10, unreliable (LMDB drift > 3%): 5.

### Trusting policy (`ZERODB_BENCH_TRUST_FILE=1`, both sides)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `get/access/hot` | 1.258 ms | 1.118 ms | 1.000 ms | 0.89× → **0.79×** | 0.893 (±0.044) | 0.999 | ✅ faster |
| `get/access/miss` | 856.085 µs | 947.186 µs | 903.422 µs | 1.11× → **1.06×** | 0.953 (±0.030) | 1.000 | ✅ faster |
| `get/access/rand` | 3.238 ms | 3.028 ms | 2.812 ms | 0.94× → **0.87×** | 0.934 (±0.030) | 1.000 | ✅ faster |
| `get/access/seq` | 1.427 ms | 1.367 ms | 1.299 ms | 0.96× → **0.91×** | 0.948 (±0.030) | 0.999 | ✅ faster |
| `get/db/root` | 3.234 ms | 3.083 ms | 2.833 ms | 0.95× → **0.88×** | 0.919 (±0.030) | 1.001 | ✅ faster |
| `get/val/v8` | 1.799 ms | 1.630 ms | 1.563 ms | 0.91× → **0.87×** | 0.960 (±0.030) | 0.998 | ✅ faster |
| `scan/edge/first_last` | 1.029 ms | 1.853 ms | 1.493 ms | 1.80× → **1.45×** | 0.806 (±0.030) | 1.001 | ✅ faster |
| `scan/full/big` 🎯 | 16.755 ms | 23.395 ms | 17.187 ms | 1.40× → **1.03×** | 0.725 (±0.038) | 0.943 | ⚠ drift |
| `scan/full/fwd` 🎯 | 499.079 µs | 1.021 ms | 510.048 µs | 2.05× → **1.02×** | 0.500 (±0.030) | 1.080 | ⚠ drift |
| `scan/full/rev` 🎯 | 481.028 µs | 977.998 µs | 510.349 µs | 2.03× → **1.06×** | 0.522 (±0.030) | 1.002 | ✅ faster |
| `scan/meta/len` | 67.290 µs | 74.741 µs | 75.240 µs | 1.11× → **1.12×** | 0.997 (±0.030) | 1.004 | flat |
| `scan/prefix/bucket` | 3.392 µs | 6.194 µs | 3.833 µs | 1.83× → **1.13×** | 0.619 (±0.053) | 1.018 | ✅ faster |
| `scan/range/10pct` | 80.196 µs | 100.485 µs | 56.157 µs | 1.25× → **0.70×** | 0.557 (±0.030) | 1.000 | ✅ faster |
| `scan/range/1pct` | 8.468 µs | 10.860 µs | 6.245 µs | 1.28× → **0.74×** | 0.575 (±0.030) | 1.000 | ✅ faster |
| `scan/skip_take/fwd` | 49.310 µs | 19.214 µs | 11.886 µs | 0.39× → **0.24×** | 0.617 (±0.030) | 1.000 | ✅ faster |
| `scan/skip_take/rev` | 49.880 µs | 19.552 µs | 12.010 µs | 0.39× → **0.24×** | 0.614 (±0.030) | 1.001 | ✅ faster |
| `seek/ge/rand` | 3.575 ms | 3.215 ms | 3.031 ms | 0.90× → **0.85×** | 0.943 (±0.053) | 1.017 | ✅ faster |

**Verdict: improved**. Improved: 22, regressed: 0, flat: 8, unreliable (LMDB drift > 3%): 2.

Reading it:

- Full scans: 2.0–2.25× → **1.02–1.06× LMDB's time** under trust, 1.45× when
  validating. The step change helps both policies; the memo change only the
  trusting one.
- `scan/full/big` (the crud-bench `count()` shape) 1.40× → 1.03× under trust.
  When validating it moves less (1.76× → 1.64×): there, each leaf's first view
  pays the cell walk, which this change does not touch.
- Range scans move ahead of LMDB (0.70–0.74× trusted, 0.83–0.88× validating).
- **Hot point gets are faster, not slower**: `get/access/hot` 0.893 (±0.044)
  trusted, 0.903 validating. The 2026-09-28 "record nothing" variant lost 4–5 %
  there; keeping branches in the memo avoids that, and the inlined inline-value
  arm of `resolve_value` speeds every get by 3–10 %.
- Every other get and seek rung: faster or flat in both policies.

## ADR-0030 on Graviton4 (implemented, commit `2142004`)

`c8g.xlarge`, Ubuntu 24.04, **4 KiB pages**, load ~1. Before = `3dfb457`.

**Repro** (`batch_delete_1000` shape, 5 interleaved rounds, ns per delete):

| round | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| r1 | 2,315 | 2,767 | 2,685 |
| r2 | 4,802 | 3,245 | 2,806 |
| r3 | 2,154 | 2,815 | 2,806 |
| r4 | 2,167 | 2,854 | 2,805 |
| r5 | 2,211 | 2,792 | 2,807 |

Round 2 is disturbed (LMDB doubled; the root gp3 volume was writing back). On
the other four rounds before and after are within 3 % of each other, around
2.80 µs against LMDB's ~2.2 µs (0.78×).

**Criterion** (`scripts/bench-ab.sh '^(del|put|commit/batch)/'`, 3 rounds):
verdict **flat**, 0 regressed, 21 flat, 1 faster (`commit/batch/n1` 0.960).

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.410 ms | 11.175 ms | 10.732 ms | 1.51× → **1.45×** | 0.960 (±0.030) | 1.000 | ✅ faster |
| `commit/batch/n100` | 6.298 ms | 7.190 ms | 7.140 ms | 1.14× → **1.13×** | 0.991 (±0.030) | 1.004 | flat |
| `commit/batch/n10k` | 14.039 ms | 14.006 ms | 14.091 ms | 1.00× → **1.00×** | 1.007 (±0.030) | 1.007 | flat |
| `del/batch/k1000` 🎯 | 32.043 ms | 41.208 ms | 40.994 ms | 1.29× → **1.28×** | 0.990 (±0.030) | 1.010 | flat |
| `del/bulk/all` 🎯 | 13.923 ms | 19.621 ms | 19.648 ms | 1.41× → **1.41×** | 1.002 (±0.030) | 1.002 | flat |
| `del/bulk/half` 🎯 | 7.095 ms | 9.885 ms | 9.914 ms | 1.39× → **1.40×** | 1.004 (±0.030) | 1.003 | flat |
| `del/churn/reinsert` 🎯 | 9.423 ms | 12.302 ms | 12.405 ms | 1.31× → **1.32×** | 1.010 (±0.030) | 1.000 | flat |
| `del/clear/all` 🎯 | 117.230 µs | 212.231 µs | 209.430 µs | 1.81× → **1.79×** | 0.999 (±0.044) | 1.005 | flat |
| `del/cursor/drain` 🎯 | 5.247 ms | 12.654 ms | 12.577 ms | 2.41× → **2.40×** | 0.994 (±0.030) | 1.002 | flat |
| `del/range/half` 🎯 | 5.230 ms | 3.072 ms | 3.072 ms | 0.59× → **0.59×** | 1.002 (±0.030) | 1.007 | flat |
| `put/api/plain` | 14.807 ms | 14.385 ms | 14.365 ms | 0.97× → **0.97×** | 0.998 (±0.030) | 1.002 | flat |
| `put/api/reserved` | 14.888 ms | 16.286 ms | 16.179 ms | 1.09× → **1.09×** | 0.993 (±0.030) | 0.998 | flat |
| `put/gc/drain_big` | 23.242 ms | 26.803 ms | 26.730 ms | 1.15× → **1.15×** | 1.000 (±0.030) | 1.018 | flat |
| `put/order/append` | 9.344 ms | 11.805 ms | 11.802 ms | 1.26× → **1.26×** | 0.999 (±0.030) | 0.998 | flat |
| `put/order/rand` | 36.999 ms | 35.708 ms | 35.798 ms | 0.97× → **0.97×** | 0.999 (±0.030) | 0.989 | flat |
| `put/order/seq` | 14.397 ms | 14.368 ms | 14.345 ms | 1.00× → **1.00×** | 0.998 (±0.030) | 1.003 | flat |
| `put/over/grow` | 8.852 ms | 9.850 ms | 9.910 ms | 1.11× → **1.12×** | 1.004 (±0.030) | 0.992 | flat |
| `put/over/same_size` | 2.983 ms | 3.259 ms | 3.236 ms | 1.09× → **1.08×** | 0.991 (±0.045) | 1.009 | flat |
| `put/val/v256` | 2.688 ms | 2.666 ms | 2.686 ms | 0.99× → **1.00×** | 1.011 (±0.030) | 1.012 | flat |
| `put/val/v2page` | 6.632 ms | 6.578 ms | 6.701 ms | 0.99× → **1.01×** | 1.015 (±0.030) | 1.003 | flat |
| `put/val/v4k` | 23.778 ms | 23.298 ms | 23.741 ms | 0.98× → **1.00×** | 1.011 (±0.030) | 1.002 | flat |
| `put/val/v8` | 1.446 ms | 1.457 ms | 1.450 ms | 1.01× → **1.00×** | 0.995 (±0.030) | 1.006 | flat |

**Reading it.** The copy-on-write copy shrinks by the page's free gap. On the
Mac's 16 KiB pages with ~1.2 KB values that gap is several KiB per leaf, and the
spike measured ~11 % per delete. On 4 KiB pages a leaf holds about three such
values and the gap is a few hundred bytes, so the copy is not where a 4 KiB
delete spends its time. The change keeps ZeroDB doing LMDB's work on this path
at every page size and costs nothing at 4 KiB. The rest of the batch-delete gap
on Linux (`del/batch/k1000` 1.28× LMDB's time, the repro at 0.78×) lies
elsewhere and has not been profiled on Linux yet.

## Scorecard (the kept change: count scan)

| | |
|---|---|
| **Faster** | Full `count()` over a 500k-entry, 0.9 GB tree: 0.57× → ~1.0× LMDB in the repro (single thread), 0.45× → 1.01× in crud-bench medians on a loaded host. Graviton4 (quiet): both policies **improved, 0 regressed**; full scans 2.05× → 1.02× LMDB under trust (1.45× validating), `scan/full/big` 1.40× → 1.03×, range scans 0.70–0.88× LMDB, `get/access/hot` 0.89–0.90× of before. |
| **Behavior** | Unchanged. `next`/`prev` return the same entries and errors. The memo is a cache: a trusted leaf not recorded is re-checked the same way on its next view. `cargo test --workspace` passes, including the cursor model tests and the oracle differentials. New unit test `trusting_policy_memoizes_branches_but_not_leaves`. |
| **Safe** | No new `unsafe`. `resolve_value` / `resolve_overflow` are the same code split in two. Clippy `-D warnings` is clean on the host and on `x86_64-unknown-linux-gnu`, and fmt is clean. `cargo +nightly miri test -p zerodb-core` passes. Differential fuzz `just fuzz-quick`: `diff_ops` 44,530 runs in 10 min, image-open 3.18 M runs, no finding. |
| **Durable** | No write path touched. `just crash-test-quick` passes: 229 single-process cycles (15 image cuts, 45 SIGKILL) and 57 multi-process cycles verified. |
| **Secure** | The trusting policy's checks are unchanged per view: page type, reserved fields and free-space bounds still run on every first view of a leaf. The validating policy is untouched: the memo still skips only cell walks that already passed. |

## What is left

- The batch-delete gap at 4 KiB pages (0.78× LMDB in the repro,
  `del/batch/k1000` 1.28× LMDB's time): a Linux `perf` profile of the delete
  phase next to LMDB's, to find the next lever.
- The page size Meilisearch's production kernels use (`getconf PAGESIZE`):
  4 KiB makes the Graviton numbers the ones to optimise.
- ADR number 0029 is used twice on unmerged branches (`hardware-crc32c` and
  `free-page-run-search`); one needs renumbering before both merge.

## Scorecard (ADR-0030: copy-on-write copies only the used bytes)

| | |
|---|---|
| **Faster** | ~11 % per batch delete at 16 KiB pages (Mac spike, busy host). Flat at 4 KiB on Graviton4: repro within 3 %, criterion `^(del|put|commit/batch)/` flat with 0 regressed and `commit/batch/n1` 0.960. |
| **Behavior** | Unchanged through every API: a copy's header, pointer array and cells are byte-identical to its source's, so every view reads the same entries. `cargo test --workspace` passes, including `writemap_in_place.rs` (heap vs in-place `WRITE_MAP` results) and `rightmost_finger.rs` (byte-identical files across two runs of one mode). New tests `copy_tests::copies_the_used_bytes_and_leaves_the_gap`, `copy_tests::bad_bounds_copy_the_whole_page`, `insert_copy_into_a_spare_copies_the_used_bytes`. |
| **Safe** | No new `unsafe`. The two copied ranges come from bounds that pass the same check every view runs (`lower` even, `lower <= upper <= body`); any other source is copied whole. Miri on `zerodb-core` passes; clippy clean on host and `x86_64-unknown-linux-gnu`. |
| **Durable** | Commit ordering, barriers and the bytes recovery reads are unchanged. Crash smoke: 213 single-process cycles (14 image cuts, 43 SIGKILL) and 57 multi-process cycles verified. |
| **Secure** | A page's free gap now keeps bytes from an earlier page at its page number, or from a reused frame of the same transaction, instead of its source's gap. These are bytes the file or transaction already held, and gaps already carried stale cell bytes (`remove_cell` does not zero). A freshly allocated heap frame is still filled whole, so no uninitialised memory is written. Accepted in ADR-0030 (Option A). Differential fuzz: `diff_ops` 63,600 runs in 10 min, image-open 5.9 M runs, no finding. |
