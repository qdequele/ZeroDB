# ADR-0028: Delete rebalance moves cells page to page, as LMDB

- Status: **Accepted (Option A, maintainer, 2026-10-18)**. Correctness-critical
  B+tree change (AGENTS.md rule 5); staged implementation, spike measurements
  first (rule 6). Open questions answered under "Resolution" below.
- Related issue: none yet. Background: the delete-rebalance entry in
  [`docs/PERF-GAP-VS-LMDB.md`](../PERF-GAP-VS-LMDB.md) ("What remains"), and
  the 2026-09-10 `blocked-adr` entry in `benches/results/perf-ledger.jsonl`.
- Date: 2026-10-18

## Context

### The gap

Deletes are ZeroDB's largest remaining write gap against LMDB:

| measurement | ZeroDB vs LMDB |
|---|---|
| criterion ladder `del/bulk/half`, `del/bulk/all` (50k ascending keys, 256 B values, 4 KiB pages, Linux x86-64; `benches/results/2026-09-28-evening-bench-server-linux-x86.md`) | 1.75× the time |
| criterion ladder `del/cursor/drain` (same run) | 2.72× the time |
| criterion ladder `del/churn/reinsert` (same run) | 1.40× the time |
| crud-bench deletes: 500k random 26-byte keys, ~1.5 KB values, 16 KiB pages, macOS M1, `WRITE_MAP \| MAP_ASYNC \| NO_SYNC` (ZeroDB also in trusted-file mode), median of 3 rounds | **0.55×** the throughput |
| the same crud-bench at 4 KiB pages | ZeroDB alone: ~31k → ~50k deletes/s |

The same crud-bench run puts ZeroDB ahead on point reads (4.31×) and limit-100
scans (1.66×) and close on creates/updates (0.93×/0.92×), so deletes stand
out. Shrinking the page from 16 KiB to 4 KiB raised ZeroDB's delete
throughput by ~60 %, which points at a per-rebalance cost that scales with
page bytes rather than at the descent or the commit.

A 2026-09-10 diagnosis counted 63,542 cell removals and 44,041 cell
insertions for 25,000 deletes on `del/bulk/half`: about 2.5 removals per
delete, consistent with roughly three of every four deletes taking the borrow
arm of the rebalance.

That frequency is **not** itself the divergence. LMDB has the same trigger and
the same one-entry borrow (below), so it rebalances on the same deletes. The
difference is what each rebalance costs.

### The current algorithm (SPEC 03 §10, §9, §5.4a)

After a delete removes a cell, `RwTxn::rebalance`
(`crates/zerodb-core/src/rwtxn.rs`) runs on the leaf:

1. **Trigger** (SPEC 03 §10, ADR-0002 §D6): a non-root leaf rebalances when
   it has fewer than `MIN_KEYS_LEAF = 1` entries or its used body bytes fall
   below `FILL_THRESHOLD_PERMILLE = 250` (25.0 %); a non-root branch when it
   has fewer than `MIN_KEYS_BRANCH = 2` children. The threshold is a
   delete-time trigger, not a committed invariant (INV-8).
2. **Root** (§9): an empty root leaf frees the tree; a single-child root
   branch collapses one level.
3. **Sibling choice** (§10): child 0 pairs with its right neighbor; any other
   child with its left neighbor (`fromleft`). The sibling is COW-touched
   (§5.1–§5.3) and the parent's child pointer rewritten if its pgno changed.
4. **Borrow** when the sibling is at or above the threshold and has more than
   `min_keys` entries. `borrow_entry` moves **one** boundary entry:
   - it decodes the sibling's boundary cell into an `OwnedLeafCell` — a fresh
     `Vec` for the key and another for an inline value (a whole value, up to a
     page);
   - `LeafMut::remove` takes it out of the sibling (heap compaction: a
     `memmove` of the cells below it plus a pass over every pointer);
   - for a right-sibling borrow, the sibling's new first key is copied into a
     third `Vec`;
   - `insert_owned_leaf_cell` writes the cell into the underfull page;
   - `update_parent_key` rewrites the parent separator by **removing** the
     separator cell (a branch-heap compaction plus a pass over every branch
     pointer) and **re-inserting** it through the generic
     `insert_into_branch`, which splits the parent when the new key no longer
     fits.

   Branch borrows do the same with branch cells: the moved child carries the
   parent separator down (ZeroDB uses the old parent separator as the moved
   node's key) and the parent separator is rewritten as above.
5. **Merge** otherwise, always right into left (§10): `extract_leaf_cells`
   decodes **every** cell of the right page into owned `Vec`s, each is
   re-inserted into the left page, the parent's node for the right page is
   removed, the right page freed (§8; loose when allocated in this txn), and
   the parent is rebalanced recursively, possibly collapsing the root (§9).
6. **Cursor path** (§5.4a): `rebalance` reports a `PathFate`. Borrow and merge
   in the leftmost pairing keep the retained `del_current` path as-is (the
   underfull page keeps its pgno; moved cells land at or after the vacated
   slot); the left pairing and any ancestor split invalidate it, and the next
   cursor step re-descends. Debug builds re-derive every kept path (the shadow
   check).

Invariants this path must preserve: INV-5 (in-page order), INV-6 (separator
bounds — a separator need only be a valid lower bound), INV-8 (min keys),
INV-9 (bounds), INV-10/16 (each page reachable once or free), INV-11 (an
overflow run is referenced by exactly one leaf cell), INV-18 (stat accuracy).
Under `WRITE_MAP` the dirty frames live in the map (ADR-0021, SPEC 04 TXN-62);
a spilled page touched again is unspilled at the same pgno (TXN-72). All of it
runs under the single writer (TXN-6).

### What LMDB does

Observed in the Meilisearch fork (`mdb.master.nested-rtxns`, vendored in
`lmdb-master-sys 0.2.6`), reading `mdb_rebalance`, `mdb_node_move`,
`mdb_page_merge`, `mdb_update_key` and `mdb_node_del` for the algorithm only
(rule 4). Described in prose; nothing here is to be transliterated.

- **Same trigger, same sibling choice, same one-entry borrow, same merge
  direction.** Leaf threshold 25 % fill and 1 key; branch at least 2 keys (and
  a fill floor of one permille, which any non-empty branch meets). Child 0
  pairs right, others pair left. The neighbor gives up one entry when it is at
  or above the threshold and has more than the minimum keys; otherwise the
  right page of the pair is merged into the left one and the parent is
  rebalanced. So the number of borrows and merges per delete is the same in
  both engines on the same tree.
- **A borrow copies the node straight from the source page into the
  destination page.** The destination insert reads the key and value bytes
  where they sit in the source page (for an overflow value, the head page
  number with its flag); nothing is staged in an intermediate buffer. The node
  is then deleted from the source (heap compaction — the one cost both engines
  share).
- **Separators are rewritten in place.** When the source's first entry moved,
  the parent separator is overwritten with the source's new first key without
  removing the separator node: if the padded key length is unchanged
  (always, for fixed-width keys) it is a key-sized copy; if it changed, the
  heap below the node shifts by the difference. Only when the page lacks room
  for a longer key does LMDB delete the node and split the parent.
- **A merge copies the source page's nodes one by one into the destination**,
  again straight from the source page, then removes the source's parent node
  and puts the source page on the transaction's loose list.
- **Branch moves and merges take the moved subtree's lowest key** as the
  branch node's key, found by descending to the leftmost leaf of that
  subtree. ZeroDB instead reuses the parent separator, which INV-6 permits to
  be strictly below that lowest key. Routing is correct either way; only the
  stored separator bytes (and so branch fill, with variable-length keys)
  differ. This ADR does not change ZeroDB's choice (open question 2).

Per borrow, then, ZeroDB pays three heap allocations and a value-sized copy,
plus a branch-page removal (compaction plus a pass over every separator) and
a branch insert, where LMDB pays a key-sized copy into the parent. With 26-byte
keys a 16 KiB branch holds about 400 separators, so the branch work grows with
the page, which is consistent with the 4 KiB result above.

## Options

### Option A — direct cell move and in-place separator rewrite (LMDB's technique)

Keep the algorithm of SPEC 03 §10 exactly — same trigger, sibling, one-entry
borrow, merge direction, separator **values** — and change only how the bytes
move:

1. **Cell move.** A page-level primitive moves one encoded cell from a source
   page to a destination page at a given index without decoding it: the cell's
   bytes (node header, key, inline value or overflow head pgno, flags) are
   copied verbatim, then removed from the source with the existing compaction.
   No `OwnedLeafCell`, no `Vec`.
2. **Two frames at once.** The source and destination are two dirty frames.
   The cell travels through a per-transaction scratch buffer of at most one
   page, allocated once and reused: one extra cell-sized `memcpy` over LMDB,
   no allocation, no new `unsafe`, no `DirtyStore` API change, and identical
   in heap and `WRITE_MAP` modes. (A disjoint two-frame `&mut` borrow from the
   dirty store would skip that copy but needs new `unsafe` under `WRITE_MAP`
   and an amendment to the AGENTS.md exception, which allows one live `&mut`
   per map region; not proposed. Revisit only if a profile shows the scratch
   copy.)
3. **In-place separator rewrite.** A branch-page primitive replaces the key of
   separator `i` in place: same padded size → overwrite key bytes and `ksize`;
   different size and the growth fits the free space → shift the heap below
   the cell by the difference and fix the affected pointers; growth does not
   fit → report it, and the caller falls back to today's path (remove the
   separator, `insert_into_branch`, which splits).

   The fallback fires in exactly the cases today's path splits. Today, after
   the removal the page has `free + old_clen + 2` bytes and the re-insert needs
   `new_clen + 2`, so it splits iff `new_clen − old_clen > free`. The in-place
   rewrite needs `new_clen − old_clen ≤ free`. Same condition, so the same
   split decisions, so the same pgno allocations.
4. **Merge** appends the right page's cells through the same move primitive,
   in order, instead of `extract_leaf_cells` / `extract_branch_cells`.
5. **Branch borrows and merges** keep today's separator values (the parent
   separator moves down; node 0 stays empty-keyed per SPEC 02 §4.1). Their
   parent separator rewrite is in place (item 3); a branch merge appends the
   right page's nodes from a scratch copy of that page instead of owned
   per-node copies; the one- or two-node rebuild inside the pages of a branch
   borrow stays remove + insert (branch borrows are rare, and the node-0
   empty-key rule makes an in-place rewrite of that node awkward).

Pros: copies LMDB's technique for the path (rule 7); no behavior, shape or
format change; the per-borrow cost becomes LMDB's plus one small copy.
Cons: two new page primitives on a correctness-critical path; the heap layout
of a page whose separator was rewritten in place differs from today's (the
cell stays where it was instead of moving to the top of the heap) — logical
content, free space and every later decision are unchanged, but raw page
bytes are not (see "What stays identical"). Crash safety: unchanged — pages
are still edited only in dirty frames, commit ordering and fsyncs untouched.
ARM: no atomics or shared memory involved.

### Option B — move several entries per borrow

When borrowing, move entries until the underfull page reaches some target
(the threshold plus a margin, or an even split of the pair's bytes), so the
next deletes do not rebalance again.

Pros: fewer rebalances on drains and near-threshold churn; would compound
with Option A. Cons: not what LMDB does, so it needs a stated reason as a
novel technique (rule 7) and, under the parity-by-default rule, would ship
only as an opt-in; it changes the committed tree shape — page fill after
deletes, leaf and branch counts, file size under churn (the
`gc_churn_parity` bands), where later splits fall — against both LMDB and
ZeroDB-before; it moves the borrow target away from ADR-0002 §D6; and it
changes which §5.4a repair rows apply (several cells now land after the
vacated slot). Larger blast radius for a gain Option A may already deliver.

### Option C — status quo

Keep the remove + insert round trip. Deletes stay 1.75–2.7× LMDB on the
ladder and 0.55× on crud-bench, the gap Meilisearch's delete-heavy settings
changes and document deletions pay.

## Decision

Proposed: **Option A**, staged, after the spike below confirms its premise.
Option B is not adopted; it can come back as a separate opt-in ADR if, after
Option A, a delete gap remains that the profile attributes to rebalance
frequency rather than per-move cost.

Out of scope: re-pairing the retained cursor path for the left pairing
(§5.4a "from left" row, where `del_current` still re-descends) — it is cursor
work, not page work, and can follow independently; and adopting LMDB's
lowest-key separator choice for branch moves (open question 2).

### What stays identical

| property | bar |
|---|---|
| **Logical tree image** | For any operation sequence, every committed tree page has the same pgno, type, ordered entries (key, flags, inline value bytes or overflow head, child pgno) and free-space count as ZeroDB-before. Therefore identical: `stat` (`entries`, `depth`, `leaf_pages`, `branch_pages`, `overflow_pages`, INV-18), pgno allocation order, the freed and loose lists, GC contents, file size. |
| **Raw page bytes** | Not required identical. A separator rewritten in place keeps its heap position, so the order of cells inside a page's heap can differ from ZeroDB-before. Not observable through the API, by `check`, by dump or by compaction. Heap and `WRITE_MAP` modes run the same primitives, so they keep giving identical results (`writemap_in_place`). |
| **LMDB-parity behavior** | Everything observable through the heed surface: gets, iteration, `entries`, errors, `delete_range` counts. No oracle expectation changes; `docs/DIVERGENCES.md` gains no entry. |
| **Cursor position after delete** | SPEC 03 §7's pinned table (successor after a mid-page or end-of-leaf delete, `None` after the last entry, full and partial drains visit each key once). The §5.4a repair rows still hold: in a borrow from the right the underfull page keeps its pgno and the moved cell is appended at its end; in a merge the right page's cells are appended after the left page's. Every `PathFate` is the same as ZeroDB-before; the debug shadow check stays. |
| **dump / load** | `zerodb-tools dump` output byte-identical (it is per entry); load is untouched (it does not delete). |
| **Compaction** | A compacting copy (`copy_to_file`, `zerodb-tools`) rewrites from a tree walk, so its output for the same logical content is byte-identical to ZeroDB-before. |
| **Overflow values** | A moved `F_BIGDATA` cell carries its head pgno unchanged; the run is neither copied nor freed; INV-11 holds. |
| **Hostile input** | The move primitive bounds-checks the source cell exactly as `LeafMut::remove` does and returns a typed error on an undecodable cell, never a panic (`hostile_input.rs`). |
| **Durability and crash consistency** | No change to commit ordering, fsync, meta writes, spilling or GC. |

### SPEC changes (in the implementing change, rule 3)

SPEC 03 §10 gains numbered rules (new prefix `REB-`; §10 has none today):
REB-1 the trigger and sibling choice as written; REB-2 a borrow moves exactly
one cell, copied verbatim from the sibling's page; REB-3 a separator is
rewritten in place when its padded growth fits the page's free space, and
otherwise removed and re-inserted through the splitting branch insert; REB-4 a
merge appends the right page's cells, verbatim and in order, to the left page;
REB-5 the cell bytes of a page are not required to sit in any particular heap
order (logical content, not layout, is the contract). §5.4a's repair table
cites REB-2/REB-4 instead of describing the mechanism.

## Consequences

### Tests

New, landing **before** the engine change, green on main:

- **Golden logical-image digests** (`crates/zerodb/tests`, e.g.
  `rebalance_logical_image.rs`): fixed seeded workloads — ascending drain,
  descending drain, interior-band deletes, random deletes with 26-byte keys
  and ~1.5 KB values at 4 KiB and 16 KiB pages, overflow values, variable-length
  keys that grow separators and split parents, the ~500-byte-key depth-3
  geometry of `deep_tree_rebalance.rs`, both `del_current` drains and key
  deletes, heap and `WRITE_MAP` modes. After each commit, a digest of the
  logical page walk (per page: pgno, type, ordered entries, free space) plus
  `stat`. Digests recorded on main; the implementation must reproduce them
  unchanged. Includes a compacting-copy hash and a dump hash per workload.

New, with the implementation:

- **Page-primitive proptests** (`zerodb-core::page::tree`, run under miri):
  for random leaf and branch pages and indices, the cell move equals
  decode + `remove` + insert in logical content and free space; the in-place
  separator rewrite equals remove + insert whenever it fits, and reports
  "does not fit" exactly when remove + insert would return `PageFull`.
- **Cell-operation counters** in the debug build behind the existing test
  hooks: the 2026-09-10 diagnostic (removals, insertions per delete) re-run,
  expected unchanged in borrow/merge counts and lower in branch removals and
  insertions.

Must stay green unmodified: `write_rebalance_differential`,
`write_rebalance_symmetric`, `deep_tree_rebalance`,
`deep_tree_rebalance_symmetric`, `cursor_delete_position` (7 cases),
`delete_range_leafwise`, `gc_churn_parity`, `stat_differential`, `gc_reclaim`,
`writemap_in_place`, `dump_load_roundtrip`, `copy_to_file_differential`,
`hostile_input`, `heed_adapter_differential`, `write_proptest`; and the
AGENTS.md checks: fmt, clippy (also for `x86_64-unknown-linux-gnu`), test,
miri on `zerodb-core`, `just fuzz-quick`, `just crash-test-quick`, and
`just stress` before the integration gate. `just loom` is not affected (no
reader-table or nested-txn change).

### Benchmarks (acceptance, rule 7)

Three columns — LMDB, ZeroDB-before, ZeroDB-after — with the result report in
`benches/results/` and a `perf-ledger.jsonl` entry:

- criterion ladder rungs `del/bulk/half`, `del/bulk/all`, `del/cursor/drain`,
  `del/churn/reinsert`, plus `del/range/half` and the `put/*` rungs as
  no-regression guards, on a quiet Linux host (4 KiB pages) and on macOS
  (16 KiB);
- **crud-bench delete A/B**: 500k random 26-byte keys, ~1.5 KB values, the same
  flags as the reference run (`WRITE_MAP | MAP_ASYNC | NO_SYNC`, ZeroDB in
  trusted-file mode), median of 3 rounds, at 16 KiB and 4 KiB pages, with the
  other crud-bench phases as no-regression guards.

Proposed success bar (for the maintainer to set): every delete rung improves
beyond noise with no other rung regressing, and the crud-bench delete ratio
closes most of the way from 0.55× towards parity.

### Riskiest assumption and the spike (rule 6)

**Riskiest assumption:** the delete gap lives in the per-move mechanics — the
intermediate `Vec` copies and the remove + insert separator rewrite — so that
making each move cost what LMDB's costs closes most of it. If the time is
instead in the rest of the delete (descent, COW touch of the sibling and the
parent pointer rewrite, the repeated page decodes in `page_stats`, commit),
Option A is correct but buys little.

A second assumption — that the in-place rewrite reproduces today's split
decisions — is arithmetic (shown above) and is pinned by the proptest.

**Spike**, smallest first, on a throwaway branch, not merged:

1. *Profile, no code.* Sample `del/bulk/half` and crud-bench's delete phase at
   16 KiB pages (macOS) and 4 KiB (Linux). Attribute time to `borrow_entry`,
   `update_parent_key`, `merge_pages`, `extract_leaf_cells` and allocation
   versus the rest of the delete. If those account for well under half of the
   delete time, stop and re-diagnose before more work.
2. *Leaf borrow only.* Implement the scratch-buffer cell move and the in-place
   separator rewrite (with fallback) for **leaf** borrows in both directions;
   leave merges and branch borrows on today's path. A few hundred lines. Run
   the four delete rungs and the crud-bench delete A/B (three columns), the
   rebalance and cursor-position suites, and the oracle differentials. This
   shows both the gain and the blast radius before the staged implementation.

Staged implementation after approval: (1) golden digests test on main;
(2) leaf borrow with the SPEC 03 §10 amendment; (3) merge and branch borrow.
Each stage carries its own three-column report.

## Resolution (maintainer, 2026-10-18)

Option A approved ("go for option A"). ZeroDB has no users yet, so choices
that only protect ZeroDB-before's internal layout are free. The open questions
below are settled as follows:

1. **Bar:** logically identical image (pgnos, entries, free space, `stat`);
   heap order inside a page may differ.
2. **Branch separators:** keep ZeroDB's choice (the parent separator moves
   down; no extra descent). Not observable through the API; not a divergence.
3. **Two-frame access:** the per-txn scratch buffer; no new `unsafe`.
4. **Option B:** not adopted. A separate opt-in ADR only if a gap remains
   after Option A and the profile blames rebalance frequency.
5. **Bar for keeping each stage:** every targeted delete rung improves beyond
   noise (bench-ab's 3 % effect gate, LMDB drift ≤ 3 %) and no other rung
   regresses; every stage reports a scorecard (faster, behavior, safety,
   durability, security).

## Open questions (as proposed)

1. **Bar for "identical".** Is a logically identical image (same pgnos,
   entries, free space, stat; heap order may differ) the right bar, given
   that an in-place separator rewrite cannot keep today's heap order? The
   alternative — byte-identical pages — would forbid LMDB's in-place rewrite.
2. **Branch separator values.** LMDB stores the moved subtree's lowest key
   (one extra descent per branch move or merge); ZeroDB reuses the parent
   separator. Not observable through the API and not compared cross-engine
   (`stat_differential` compares no page counts). Keep ZeroDB's choice, or
   record it in `docs/DIVERGENCES.md` as a known internal difference, or
   align with LMDB in a later change?
3. **Two-frame access.** Is the per-txn scratch buffer acceptable, or should
   a disjoint two-frame borrow (new `unsafe` under `WRITE_MAP`) be considered
   if a profile shows the extra copy?
4. **Multi-entry borrow (Option B).** Close it, or keep it as a possible
   opt-in for a later ADR?
5. **Success thresholds** for the ladder rungs and the crud-bench A/B.
