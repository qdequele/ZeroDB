# ADR-0014: Opt-in trusted-file mode (LMDB parity for page reads)

- Status: Draft
- Milestone: Phase 3 (performance), PERF-GAP A2(b) / issue #21
- Date: 2026-09-28

## Context

ZeroDB's default promise is that a corrupt or hostile data file produces a
typed error, never undefined behaviour. PERF-GAP section D records that
"removing validation outright is not on the table". LMDB makes no such
promise: it treats the mapped file as trusted and reads pages through raw
pointers. The maintainer asked (2026-09-26) that ZeroDB be able to do the
same, **as an option**: "for the 'trusting the file' it would be great if it
could be an option." This ADR designs that option. It does not change the
default.

### What ZeroDB checks today

On the read path (`crates/zerodb-core/src/btree.rs`, `page/tree.rs`):

- `Source::bytes_from_classified` (btree.rs:104-147): `pgno <= last_pg`
  (`PageOutOfBounds`, SPEC 04 TXN-38, SPEC 06 REC-14), checked address
  arithmetic, and a full page inside the map.
- First sight of a page in a txn: `LeafRef::new` / `BranchRef::new`
  (tree.rs:166-175, 658-666) validate the type flag, reserved fields, free-space
  bounds, and **every cell** (pointer in the heap, header readable, key and value
  spans in-bounds, overflow reference sane). That is O(num_keys) per page.
- Later sights: the txn-scoped `ValidatedPages` memo (btree.rs:190-205, a
  lock-free open-addressed `AtomicU64` set, PERF-GAP A2/A8) turns the view into
  `new_trusted` (two header reads, no checks). The memo probe itself costs a
  hash, one Acquire load, and a short probe per page view.
- Engine-authored dirty frames (a writer's own pages) skip the memo and use
  `new_prevalidated` (O(1) type/bounds checks).
- Overflow runs: `OverflowRef::new` (overflow.rs:44-75) checks type, run length
  and payload bounds.
- The free list is never trusted: `validate_pil_ids` range-checks every page id
  before a GC entry is drawn (SPEC 05 GC-18), so a corrupt free list can never
  feed a meta page to the allocator.

Every `unsafe` block in `zerodb-core::page` (the `read_*_unchecked` helpers,
`get_unchecked` in `LeafRef::key/value`, `BranchRef::child_pgno`,
`leaf_lookup`, …) already reads without bounds checks. Their SAFETY comments
rest on one contract: the cell was proven in-bounds when the view was
constructed (the "A3 view contract"), or the frame is engine-authored.

Measured cost of this validation (bench server, x86-64, 4 KiB pages):
first-touch validation is ~1.5 µs of the ~8.4 µs gap per single-put commit
(PERF-GAP B12); on reads the cost is concentrated on the first view of each
page per txn plus the memo probe on every later view.

### What LMDB and libmdbx do

- **LMDB** (vendored fork, `lmdb-master-sys 0.2.6`, `mdb.c`): `mdb_page_get`
  rejects only `pgno >= mt_next_pgno` (`MDB_PAGE_NOTFOUND`, mdb.c:6576-6578).
  The descent, `mdb_cursor_prev`/`next` and `mdb_cursor_del` check only that
  the bottom page is a leaf (`MDB_CORRUPTED`, mdb.c:6676, 7165, 8269). Cell
  pointers, key sizes, value spans and overflow references are used as-is. A
  corrupt page is undefined behaviour.
- **libmdbx** (not vendored here; from its public documentation, to be
  confirmed): checks page type and bounds on load and returns
  `MDBX_CORRUPTED` / `MDBX_PAGE_NOTFOUND`, and offers an opt-in
  `MDBX_VALIDATION` mode for deeper per-page content checks. Its default sits
  between LMDB's (almost nothing) and ZeroDB's (everything).

ZeroDB's default is stricter than both. The trusted mode would make an opted-in
env behave like LMDB.

## Options

### Option A — Runtime flag that short-circuits the memo

Add an env-level flag, fixed at open. When set, `ValidatedPages::contains`
returns `true` for every map page (or `leaf_view`/`branch_view` take the
existing `new_trusted` arm directly), so every page view goes straight to the
two-header-read path: no per-cell walk, no memo probe, no memo insert.

- **What stays checked, even trusted:** `pgno <= last_pg` and the full-page map
  bound (it keeps a bad pointer from faulting past the file, D-017; LMDB checks
  the equivalent `mt_next_pgno`), the page-type dispatch in `node_view` (LMDB's
  `IS_LEAF`), meta-page validation at open, and the free-list id checks
  (`validate_pil_ids`: a corrupt free list is durable corruption, not just a bad
  read, and the check is off the hot path).
- **Cost on the default path:** one well-predicted branch on a flag read once
  per page view. The flag can live next to the memo in the same cache line.
  The risk is code layout: PERF-GAP B13 shows the hot path sits at LLVM's
  inlining threshold, so even one branch can move timings by a few percent.
  That must be measured, not assumed.
- **Pros:** small change (one field, one branch, one open option); no new type
  parameters; heed surface unchanged apart from the option; both modes share one
  binary, so an app can open a trusted index env and a validating env for
  user-supplied files side by side.
- **Cons:** the default path carries the branch; UB is chosen per env at
  runtime, which is harder to audit than a type.

### Option B — Type-level mode, monomorphized

Make the validation policy a type parameter (e.g. `Source<'a, V: Validation>`
threaded through `Tree`, `Cursor`, `RoTxn`/`RwTxn`), with `Checked` and
`Trusted` implementations. Each mode is its own monomorphized code path, so the
default carries no branch at all.

- **Pros:** zero cost on the default path by construction; the mode is visible
  in the types.
- **Cons:** large API churn — the heed-shaped `Env`/`RoTxn`/`RwTxn`/`Database`
  surface would need the parameter or type erasure, and heed consumers
  (Meilisearch, hannoy) name those types directly; roughly doubles the hot code
  in the binary, which at the inlining threshold (B13) can cost more than the
  branch it removes; much larger review surface.

### Option C — Cargo feature, whole binary

A `trusted-pages` feature that compiles validation out.

- **Pros:** zero runtime cost; tiny change.
- **Cons:** Cargo features are additive and unified across the dependency
  graph: any crate that enables it silently turns every ZeroDB env in the
  process into UB-on-corruption, including envs opened on untrusted files.
  That is the wrong failure mode for a safety opt-out. Cannot mix modes in one
  process.

### Option D (complementary, not a replacement) — Lazy validation by default

Issue #21 / PERF-GAP A2(b): validate only the cells a lookup actually touches
(O(log K) per descent) instead of all K cells on first sight. It keeps the
no-UB promise and removes most of the first-touch cost. It does not remove the
memo probe or reach LMDB's floor, but it helps every user, not only those who
opt in. It can be done before or after A.

## Decision

**Proposed: Option A**, gated on measurement, with D considered separately.

1. **API shape.** An `unsafe` builder method on heed-zerodb's `EnvOpenOptions`,
   for example `unsafe fn trust_file_contents(&mut self, yes: bool)`, plus the
   matching option in `zerodb-core`'s open options. Being `unsafe`, its
   `# Safety` section carries the contract: the caller guarantees the data file
   was written by ZeroDB and has not been modified by anything else; with this
   option on, a corrupt or hostile file is **undefined behaviour, exactly as in
   LMDB**. It is off by default. (heed's `EnvOpenOptions::open` is already
   `unsafe`; a separate method keeps the new obligation explicit instead of
   hiding it in an existing contract.)
2. **Mechanism.** A flag on `EnvInner`, fixed at open, that makes every map-page
   view take the existing `new_trusted` arm (skipping the per-cell walk and the
   memo). No new `unsafe` blocks: the existing unchecked reads get a second
   justification in their SAFETY comments ("or the env was opened with
   `trust_file_contents`, whose contract makes the page valid").
3. **Scope.**
   - Readers and nested read txns (ADR-0007) inherit the env's mode; they
     already share the parent's memo.
   - Write txns trust committed pages they read too, as LMDB does. The
     consequence must be documented: a corrupt committed page read by a writer
     can be propagated into newly written pages. Dirty frames are unchanged
     (already engine-authored).
   - `zerodb check` / `check::check_image` always validates fully and ignores
     the flag. It is the tool a trusted-mode user runs on any file of uncertain
     origin (for example an imported snapshot).
   - Kept even when trusted: page-number bounds, page-type dispatch, meta
     validation at open, free-list id validation.
4. **Measurement gate.** Keep the lever only if (a) the default (validating)
   path shows no regression beyond noise in both codegen settings, and (b)
   trusted mode improves the read rungs enough to matter. If (a) fails because
   of layout, try placing the flag test inside the already force-inlined
   `ValidatedPages::contains` before considering Option B.

## Consequences

- **Easier:** consumers that own their files (Meilisearch's index envs, hannoy)
  can opt into LMDB's read cost; the gap in PERF-GAP section A that remains after
  the memo (first-touch cell walk, memo probe) closes for them.
- **Harder:** two safety regimes to document and review; every future change to
  the page module must keep the trusted-mode contract in mind.
- **Tests and invariants to add:**
  - the whole existing suite, oracle differentials and fuzz run in both modes
    (a test-matrix axis), since behaviour on valid files must be identical;
  - a test that `check_image` still reports corruption when the env was opened
    trusted;
  - hostile-image tests stay default-mode only (in trusted mode they are UB by
    contract and must not be run);
  - Miri runs in default mode only for hostile inputs; valid-file Miri runs in
    both modes.
- **SPEC / docs to update:** SPEC 02 (page validation) and SPEC 04 TXN-38 gain
  a trusted-mode paragraph; SPEC 01 or the open-options spec lists the option;
  PERF-GAP section D is amended to "removing validation is not on the table
  **by default**; ADR-0014 adds an opt-in"; DIVERGENCES gains an entry
  describing the option as a ZeroDB extension (LMDB has no validating mode to
  diverge from; default behaviour is unchanged); CLAUDE.md's unsafe policy
  needs no new module but the SAFETY-contract wording changes.
- **Bench plan (both codegen settings, bench server, then Graviton):**
  three columns, LMDB | ZeroDB validating | ZeroDB trusted, on the read ladder
  (`^(get|seek|scan)/`), `commit/batch/n1` (first-touch validation is ~1.5 µs of
  its gap, B12), a cold-memo rung (a fresh read txn per lookup, so every page is
  a first sight), plus the hannoy `search_hnsw` and Meilisearch search consumer
  benches. The default-mode column against today's numbers is the regression
  check for gate (a).

## Open questions for human review

1. **Should write txns trust too?** LMDB does. The alternative, trusting only
   readers, keeps writers from propagating corruption but halves the benefit
   for indexing (Meilisearch's hot path).
2. **Keep the free-list id checks in trusted mode?** Recommended yes (a corrupt
   free list is durable corruption and the check is not on the read hot path),
   but it is a deviation from "trust like LMDB".
3. **API surface:** an `unsafe` method on heed-zerodb's `EnvOpenOptions`
   (heed-shaped, ZeroDB-only addition), or a ZeroDB extension trait in the style
   of ADR-0012 so the heed-mirrored surface stays untouched?
4. **Should Option D (lazy validation, no UB) come first?** It benefits every
   user and might close enough of the gap that fewer users need the unsafe
   option.
5. **Meilisearch's threat model:** Meilisearch imports raw snapshots, which may
   come from users. If it enables trusted mode for its index envs, it should run
   `zerodb check` on imported snapshots, or open them validating. Who owns that
   guidance?
