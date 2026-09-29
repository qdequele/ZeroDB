# ADR-0016: Lazy (read-time) cell validation on first sight

- Status: Accepted (2026-09-29); kept only if the measurement gate below passes
- Milestone: Phase 3 (performance), PERF-GAP A2(b) / issue #21 (ADR-0014 Option D)
- Date: 2026-09-29

## Context

ZeroDB's default promise: a corrupt or hostile data file yields a typed error,
never undefined behaviour. Today that is kept by validating **every cell** of a
map page the first time a txn views it (`LeafRef::new` / `BranchRef::new`,
O(`num_keys`)), and by the txn-scoped validated-pages memo that makes every
later view of the page free (PERF-GAP A2/A8). The unchecked accessors of the
page module (`key`, `value`, `leaf_lookup`, `child_pgno`, …) rest on that "A3
view contract": the cell was proven in bounds when the view was built.

The cost falls on pages a txn sees **once**:

- a scan views each leaf once, so every leaf pays the whole walk:
  `LeafRef::new` is 22 % of `scan/full/fwd` (2026-09-28 profile);
- a point lookup on a cold page walks all ~50–100 cells to read the ~7 its
  binary search touches.

The trusted-file policy (ADR-0014) shows the ceiling of removing the walk:
`get/size/n1m` 1.47× → 1.02×, `get/access/rand` 1.14× → 1.01×,
`scan/range/1pct` 1.58× → 1.21×, and Meilisearch's hackernews search +6 % →
+3 % end to end. Two code-shape levers on the walk itself were measured and
reverted (a branch-free walk +5 %, 2026-09-28 ledger). The remaining default-
mode option is to check less: only the cells a read actually touches.

The maintainer approved this direction on 2026-09-29 ("go with both", after
the recommendation to do lazy validation for the default mode).

## Options

### Option A — Validate every cell on first sight (today)
No change. Keeps the ceiling above out of reach for users who do not opt into
trusted mode.

### Option B — Lazy everywhere
Every view checks each cell as it is read; no full walk ever. Hot pages would
re-check the cells of every lookup on every view (today: zero checks after the
first view), a likely regression on `get/access/hot` and descents through the
top levels.

### Option C — Lazy on first sight, full on second sight (proposed)
- **First view of a map page in a txn:** the O(1) header checks only (type,
  reserved fields, free-space bounds — `new_prevalidated`), and a *lazy view*
  whose accessors check each cell before reading it (the exact
  `check_ptr_in_heap` + `leaf_cell_len` conditions for that cell, returning
  the same typed errors). The page is recorded in the memo as *seen*.
- **Second view in the same txn:** the full cell walk, then the existing
  *validated* memo entry — the zero-check `new_trusted` path from then on.
- Pages seen once (scans, cold point lookups) never pay the full walk; pages
  seen twice or more pay it once, as today.

### Option D — Lazy with a per-cell validated bitmap
Remembers which cells passed, per page. More state and atomics in the memo for
little gain over C, since C already stops re-checking hot pages.

## Decision

**Option C**, introduced in steps, each behind the gate:

1. **Step 1 — read txns, leaf pages.** `RoTxn` and nested read txns use lazy
   leaf views on first sight for point lookups (`Tree::find_exact`,
   `get`) and cursor reads (scans, seeks). Branch pages keep the full walk
   (descents re-visit the top levels, which are hot).
2. **Step 2 (only if step 1 is kept)** — branch pages on first sight.
3. **Write txns keep the full walk** on every map page they view: a writer
   copies map pages into dirty frames, which are then treated as
   engine-authored (`new_prevalidated`), so a map page must be fully proven
   before it can become a frame.

**Mechanism.**
- A separate view type, `LazyLeaf<'a>`, whose accessors (`key`, `value`,
  `lookup`, `node_flags`) are **checked** and return `Result<_, PageError>`.
  Fully validated `LeafRef` keeps its zero-check accessors, so the hot path's
  code does not change.
- The memo gains a *seen* kind next to *leaf*/*branch* (the key already
  carries a kind tag, PERF-GAP A8). A *seen* hit is not a *validated* hit: it
  triggers the full walk and promotes the entry.
- `zerodb check`, `check_image`, tools and the trusted-file policy are
  unchanged. Under trusted mode no lazy view is built (the policy already
  skips the walk).

**Observable change (corrupt files only).** A corrupt cell yields its typed
error when something reads that cell (or when the page is viewed a second
time in the txn), no longer as soon as the page is first viewed. A corrupt
cell nothing reads never errors — harmless, since nothing reads it. On valid
files results are identical.

**Safety contract.** The A3 contract becomes: an unchecked cell read happens
only through a fully validated view (`LeafRef` built by `new`, `new_trusted`
behind a *validated* memo hit, `new_prevalidated` on an engine-authored frame
or under the trusting policy), or after the lazy view's check of **that**
cell. `LazyLeaf` contains no unchecked read that is not preceded, in the same
function, by the check of the cell it reads.

## Consequences

- **Tests:**
  - differential of lazy vs full on valid pages (same results for every
    accessor, point gets and full/partial scans, both directions);
  - corrupt-page property tests: for random corruption, a lazy read either
    returns the same bytes as a full view would or a typed error — never a
    panic, never an out-of-bounds read (Miri on the lazy accessors);
  - the hostile-image suites keep passing (errors may now surface at the read
    rather than the first view; any test asserting "error at first view" is
    reviewed, not weakened);
  - a test that a page viewed twice in a txn is fully validated and memoized.
- **Bench gate:** (a) `get/access/hot` and the other hot-page rungs flat in
  both codegen settings; (b) `scan/*`, `seek/*`, `get/size/n1m`,
  `get/access/rand` improve; (c) hackernews search replay improves.
- **Docs:** SPEC 02 (page validation) and SPEC 04 TXN-38 describe first-sight
  lazy validation; PERF-GAP A2(b) closes; DIVERGENCES needs no entry (LMDB
  validates nothing; ZeroDB still returns typed errors).

## Open questions for human review

1. Step 2 (branch pages lazy) — worth doing if step 1 is kept, or keep
   branches fully validated since the top levels are hot anyway?
2. Should write txns get lazy leaf views for pure reads (`RwTxn::get`,
   write-cursor reads) as long as a page is fully validated before it is
   copied? More gain for indexing's read-your-writes lookups, more audit.
