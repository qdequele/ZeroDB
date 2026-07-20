# ADR-0011: DUPSORT / DUPFIXED — encoding, second ordering, cursor model, staging

- Status: Approved — Quentin, 2026-07-20 (standing directive, session-lead review). All five open questions resolved below.
- Milestone: 2.8 (descoped from 1.7 on 2026-07-15, D-004)
- Date: 2026-07-20

## Context

Milestone 2.8 cashes in the format hooks deliberately reserved in Phase 1:

- Page flags `P_LEAF2` (0x0020) and `P_SUBP` (0x0040) — SPEC 02 §1, "never set
  in Phase 1".
- Leaf-node flags `F_DUPDATA` (0x0004) and the `F_SUBDATA|F_DUPDATA`
  combination — SPEC 02 §4.2/§10.
- `leaf2_ksize` at the branch/leaf variant-tail offset 28 (u16) and at
  DBRecord offset 44 (u32); DBRecord `flags` at offset 42 (u16) — SPEC 02
  §2.1/§3.1.
- SPEC 03 §12 (reserved DUPSORT trees) and INV-21 (reserved fields zero).
- SPEC 01 Table 2 (`MDB_DUPSORT`/`DUPFIXED`/`INTEGERDUP`/`REVERSEDUP`),
  Table 3 (`MDB_NODUPDATA`/`APPENDDUP`/`MULTIPLE`), Table 5 (the eleven dup
  cursor ops) — all WON'T-with-2.8-revival rows.

No pinned consumer uses any of this (D-004, verified across milli, meilisearch,
arroy, hannoy, cellulite). PLAN.md calls DUPSORT "the highest-defect-density
area of LMDB" and budgets a dedicated ≥ 2 h differential fuzz target. The
consequence for this ADR: there is **no consumer pressure to deviate**, so
every observable behavior is LMDB-fork parity, pinned by oracle observation
(CLAUDE.md rule 1 — never guess LMDB semantics), and every internal choice is
free so long as the observables match.

Prior art consulted (clean-room: behavior and structure, no transliteration):

- **LMDB** (`mdb.master.nested-rtxns` fork = the oracle): small dup sets live
  in an *embedded sub-page* inside the leaf node's value area (flag `P_SUBP`,
  a miniature leaf page whose "keys" are the dup values, data size 0); when
  the set outgrows the node budget it is *promoted* to a sub-database — the
  node value becomes an `MDB_db` record (`F_DUPDATA|F_SUBDATA`) rooting a
  full B+tree of dup values. `MDB_DUPFIXED` packs same-sized items with no
  per-item headers (`P_LEAF2` pages / packed sub-pages, item size in the
  page's pad field). Persistent DB flags (`md_flags`, including
  DUPSORT/DUPFIXED/INTEGERDUP/REVERSEDUP) **are stored on disk** and a
  reopen with a mismatched flag set fails `MDB_INCOMPATIBLE` (SPEC 01 §S8).
  The *custom* dup comparator (`mdb_set_dupsort`) is a per-open function
  pointer and is **not** persisted — the same hazard as `mdb_set_compare`
  (D-014). Dup cursor state is a nested sub-cursor (`MDB_xcursor`) embedded
  in the main cursor. Because dup values become sub-tree *keys*, LMDB bounds
  DUPSORT data items by its key-size limit — dup values never hit the
  overflow-page path.
- **libmdbx**: same two-level design (sub-page → nested tree, `P_LEAF2`
  dupfixed pages), kept through years of divergence — evidence the shape is
  load-bearing, not incidental. mdbx's changelog is also the defect record
  this ADR's risk register draws from: the recurring DUPSORT bug classes are
  sub-page↔sub-tree conversion, dup-cursor tracking across sibling mutation,
  and DUPFIXED size-edge handling.

Current zerodb shape this extends: `crates/zerodb-core/src/page/tree.rs`
(LeafRef/LeafMut/BranchRef/BranchMut, `F_BIGDATA`/`F_SUBDATA` cells, the
`max_node_size` inline rule), `crates/zerodb-core/src/btree.rs` (Cursor =
pgno/ki stack + `initialized`/`eof`, SPEC 03 §4 state machine),
`crates/zerodb-core/src/rwtxn.rs` (insert/split/delete/rebalance, per-txn
DBRecord write-back at C1a), `crates/zerodb-core/src/cmp.rs` (M2.4
`ComparatorRegistry`, one `KeyCmp` per dbi), `crates/heed-zerodb/src/
database.rs` (mirrored heed surface: `get_duplicates`,
`delete_one_duplicate`, `DatabaseFlags`, `PutFlags::{NO_DUP_DATA,
APPEND_DUP}`).

---

## Decision 1 — Duplicate-set encoding: sub-page then sub-tree (mirror LMDB)

### Option 1A — Two-level: embedded sub-page, promoted to sub-tree (LMDB/libmdbx)
A leaf node with `F_DUPDATA` holds either (i) an embedded **sub-page** in its
value area — a mini leaf page of dup values — or (ii) with
`F_DUPDATA|F_SUBDATA`, a 48-byte **DBRecord** rooting a sub-B+tree whose keys
are the dup values (data size 0).
*Pros:* matches LMDB's observable surface exactly — `Database::stat` page/entry
counts, split behavior, `mdb_dump` output shape — which is what the oracle
differentials compare; small dup sets (the common case) cost zero extra pages;
sub-tree reuses the existing §2–§10 algorithms and the M1.6 `F_SUBDATA`
record machinery unchanged. *Cons:* the conversion boundary is the
highest-defect-density code in LMDB's own history; two encodings to check.

### Option 1B — Always a sub-tree
Every dup set, even of 2, gets its own root page.
*Pros:* one encoding, no promotion logic. *Cons:* a page per key with dups
(catastrophic for the 2–3-dup common case); `stat` page counts and file sizes
diverge observably from the oracle, so differential tests would need a
sanctioned divergence for a Phase-2 *parity* feature — backwards; loses cache
locality LMDB users expect.

### Option 1C — Flatten dups into composite keys internally
Store `key‖value` as the tree key (what milli does at the application level).
*Cons (disqualifying):* blows the 511-byte key budget (key+value ≤ 511 instead
of each ≤ bound), makes `entries`/`stat`/`GET_MULTIPLE` semantics synthetic,
and APPENDDUP/NODUPDATA edge behavior would be emulated rather than structural.

**Decision: Option 1A.** Specifics (exact byte layouts to be written into SPEC
02 §10 and SPEC 03 §12 as the first act of stage 2.8a):

- **Sub-page layout.** Compact **8-byte header**, not the full 32-byte common
  header (a sub-page is not a file page: it has no pgno, no txnid stamp, no
  checksum slot; reusing the common header would waste 24 bytes per dup key
  and force INV-4 exemptions): `flags: u16` (`P_SUBP|P_LEAF`, or
  `P_SUBP|P_LEAF2` when packed), `lower: u16`, `upper: u16` (both relative to
  the sub-page body, same grow-up/grow-down discipline as §2.2),
  `leaf2_ksize: u16` (0 unless packed). Dup cells are **value-only**: header
  `dvsize: u16` + bytes, even-padded, pointer array sorted by the dup
  ordering (Decision 3). The sub-page codec is pure in-memory slice logic —
  miri-coverable, no mmap involvement.
- **Dup value bound.** Dup values are bounded by the sub-tree key limit
  (`MAX_KEY_SIZE = 511` expected; **pinned by oracle observation before the
  format freezes**, SPEC 01 §S4 gains the dup row). Consequence: no
  `F_BIGDATA` and no overflow runs anywhere inside dup structures — sub-pages
  and sub-trees hold inline values only. The decoder rejects `F_BIGDATA`
  inside a dup structure (INV-24).
- **Sub-tree root storage.** The leaf value area holds exactly the 48-byte
  DBRecord (`F_DUPDATA|F_SUBDATA`), byte-identical in layout to a catalog
  entry's value (SPEC 02 §3.1/§6). Its `entries` is the dup count for that
  key; its `leaf2_ksize` carries the DUPFIXED item size (Decision 2). Like
  named-DB records, the working copy is held per-txn and written back into
  the parent leaf on mutation (it COWs the parent leaf — same discipline as
  the C1a catalog write-back, but per-op since the record lives in a user
  leaf, not the catalog).
- **Promotion threshold and owner.** A dup set stays a sub-page while the
  containing leaf cell (`8 + ksize +` sub-page bytes) fits `max_node_size`
  (the SPEC 02 §4.2 inline rule — one threshold, one owner). The first insert
  that would exceed it allocates a sub-tree, moves all items, and rewrites
  the node as `F_DUPDATA|F_SUBDATA`. The threshold logic lives in
  `rwtxn.rs`'s put path, next to the inline-vs-overflow decision it mirrors.
  **Demotion** (sub-tree back to sub-page on deletes, and sub-page back to a
  plain single-value node when one dup remains) is *believed* to follow LMDB's
  asymmetric behavior — the exact rules are an oracle-pinning item in 2.8a's
  test-first list, not guessed here.
- **GC participation.** Sub-tree pages are ordinary pages: allocated through
  the txn allocator, COWed through the normal dirty set, and freed into the
  txn free set exactly like main-tree pages — the freelist (SPEC 05) never
  learns they are "sub" pages. `delete`-all-dups, `clear`, `drop`, and
  demotion free the whole sub-tree by walking it (same walk `drop` already
  does for named DBs). INV-10 (reachability XOR freeness) extends to reach
  through `F_DUPDATA|F_SUBDATA` records; a leaked or double-freed sub-tree
  page is caught by the existing check, once the walker learns to descend
  (Decision 6). Crash safety inherits ADR-0004/0005 wholesale: sub-tree pages
  are dirty pages in the same commit pipeline, so no new fsync ordering is
  introduced — the invariant between any two commit steps is unchanged.
- **Stat accounting.** Whether the parent DB's DBRecord page counters fold in
  sub-tree pages, and what `depth` reports for a dup DB, is **observable**
  via `Database::stat` — pinned by oracle before implementation, replicated
  exactly, recorded in SPEC 02 §3.1.

## Decision 2 — DUPFIXED packed layout

- `P_LEAF2` **file pages** appear only as sub-tree leaves of a
  DUPSORT|DUPFIXED database: common 32-byte header, `leaf2_ksize` (variant
  tail offset 28) = item size, items packed contiguously from body offset 0
  (`item(i) = body + i*leaf2_ksize`), count derivable from `lower`
  (`num_keys = lower / leaf2_ksize` — exact rule fixed in the SPEC
  amendment). No pointer array, no per-item headers. Branch pages of a
  dupfixed sub-tree stay ordinary `P_BRANCH`.
- **Packed sub-pages** (`P_SUBP|P_LEAF2`): same idea inside the compact
  header, item size in the header's fourth u16.
- The **main tree never uses `P_LEAF2`** in 2.8. LMDB's `MDB_INTEGERKEY`
  leaf2 main-tree pages stay out of scope (Table 2 routes INTEGERKEY to the
  2.4 comparator machinery; activating a persisted flag bit for it is an open
  question below).
- `GET_MULTIPLE`/`NEXT_MULTIPLE`/`PREV_MULTIPLE` return a borrowed slice
  spanning the packed run of the **current page only** (chunk boundary = page,
  LMDB parity — callers observe chunk sizes, so this is pinned, not chosen).
  Zero-copy: the slice borrows from the mmap or dirty page under the SPEC 04
  value-borrow contract, no assembly buffer.
- Size discipline: DUPFIXED requires equal-size items. What LMDB actually
  does on a mismatched-size put (error vs silent un-fixing of the sub-page)
  is an **oracle-pinning item** — the answer decides whether
  `DBRecord.leaf2_ksize` may ever reset. Until pinned, no code.

## Decision 3 — The dup comparator: second ordering, and what is persisted

The dup ordering is to the sub-tree exactly what the key ordering is to the
main tree: total, deterministic, fixed for the tree's lifetime (BT-1 extended).
A wrong dup order corrupts sub-pages/sub-trees precisely as a wrong key order
corrupts the main tree.

- **Registry.** `ComparatorRegistry` (cmp.rs) grows a second per-dbi slot:
  `dup_cmp`. `Tree`/`Cursor` already carry a `KeyCmp`; dup structures carry a
  second one. Same M2.4 rules: **named DBs only** (the main DB is the catalog
  and can't be DUPSORT anyway — pin the oracle's answer to *that* too), same
  in-process double-registration guard by `Comparator::name`.
- **Built-ins are persisted and safe.** `MDB_INTEGERDUP` and `MDB_REVERSEDUP`
  are DBRecord.flags bits (offset 42), written at create, checked at every
  open: mismatch → `MdbError::Incompatible` (SPEC 01 §S8). This is LMDB
  parity — LMDB persists `md_flags` — so the *built-in* dup orderings do
  **not** inherit D-014. The bit values mirror LMDB's (`DUPSORT` 0x04,
  `DUPFIXED` 0x10, `INTEGERDUP` 0x20, `REVERSEDUP` 0x40) so
  `migrate-from-lmdb` copies flags verbatim. INTEGERDUP's comparator is the
  native-word-size unsigned compare LMDB uses (`mdb_cmp_cint` family) —
  behavior pinned by oracle on both 4- and 8-byte items.
- **Custom dup comparators** (`mdb_set_dupsort` analogue — a zerodb trait
  extension; heed 0.22.1 exposes no set_dupsort, so this is third-table
  surface): by default this inherits D-014's cross-open hazard verbatim.
- **Persistence recommendation (the D-014 question, resurfaced at the right
  moment).** This milestone formats DBRecord bytes 42–47 anyway — it is the
  last cheap moment to claim guard bits. Observation: `leaf2_ksize` needs at
  most 16 bits (a dupfixed item must fit a ≤ 64 KiB page; real bound
  `≤ max_node_size < 32768`). Proposal: split the reserved u32 at offset 44
  into `leaf2_ksize: u16` + `cmp_fingerprint: u16` — a 16-bit hash of the
  registered comparator names (key ⊕ dup; 0 = both defaults). At open,
  fingerprint mismatch → `Incompatible`. This closes the cross-open hole for
  *both* the 2.4 key comparator and the 2.8 dup comparator, at zero bytes of
  growth, before any 2.8 file exists (no migration ever needed).
  **This is better-than-LMDB behavior and therefore needs explicit maintainer
  sign-off (it is exactly the open D-014 decision)** — see open questions. If
  declined, zerodb reproduces LMDB's hazard bit-for-bit and D-014's text
  extends to the dup comparator.

## Decision 4 — Cursor model: nested sub-cursor, not a merged stack

### Option 4A — Separate sub-cursor (LMDB `MDB_xcursor` shape)
`Cursor` gains `sub: Option<Box<SubCursor>>` where
`SubCursor = InPage { cell offset state } | Tree(Cursor)` — a second, private
positioning object derived from the main cursor's current leaf cell.
*Pros:* the SPEC 03 §4 state machine and its `initialized`/`eof` semantics are
untouched for non-dup DBs (zero regression surface on the Phase-1-parity
paths); the sub-tree case literally reuses `Cursor` (it is a real tree);
op-mapping mirrors the oracle's structure so EOF edge cases line up naturally.
*Cons:* a validity protocol is needed between the two levels.

### Option 4B — One unified stack with sub-levels appended
*Pros:* one traversal loop. *Cons:* every existing §4 op must learn to
distinguish "main levels" from "sub levels" (a `depth`-boundary index carried
everywhere); sub-page frames are not (pgno, ki) frames — they'd need a variant
frame type polluting the hot non-dup path; the blast radius includes every op
already pinned green against the oracle.

**Decision: Option 4A.** The sub-cursor is **derived state**: it is valid only
while the main cursor's position is; any mutation through the txn that COWs,
splits, or rebalances the parent leaf re-resolves it by re-seek
(key, then dup value) — cheap, since zerodb cursors already re-descend a pgno
stack rather than holding pointers (unlike LMDB's in-struct page pointers,
the source of a long line of its cursor-fixup CVEs). This revalidation rule is
written into SPEC 03 §12 as **SUBC-1** with the same normative force as §4.

Op mapping (each gets a §4-style entry in SPEC 03 §12 — position, result, EOF
behavior, error — **pinned by a differential test before implementation**):

| Op | Mapping (expected; oracle pins the edges) |
|---|---|
| `FIRST` / `LAST` | main first/last key, sub-cursor at first/last dup |
| `NEXT` / `PREV` | next/prev dup; at dup boundary, next/prev key (first/last dup respectively) |
| `FIRST_DUP` / `LAST_DUP` | positioned cursor required (else `EINVAL`); sub first/last |
| `NEXT_DUP` / `PREV_DUP` | sub step; `NotFound` at the dup boundary (never crosses keys) |
| `NEXT_NODUP` / `PREV_NODUP` | main step; sub at **first** dup (NEXT) / **last** dup (PREV) |
| `SET` / `SET_KEY` / `SET_RANGE` | main seek, sub at first dup |
| `GET_BOTH` | exact key + exact dup (dup_cmp), else `NotFound` |
| `GET_BOTH_RANGE` | exact key, dup ≥ given (dup_cmp); the returned key/data quirks are pinned |
| `GET_CURRENT` | current (key, dup) pair without moving |
| `GET_MULTIPLE` / `NEXT_MULTIPLE` / `PREV_MULTIPLE` | DUPFIXED-only page-run slices (Decision 2); non-DUPFIXED → `Incompatible` |

`NEXT_NODUP`/`PREV_NODUP` on a non-dup DB degenerate to `NEXT`/`PREV`
(Table 5); dup-only ops on a non-dup DB return what the oracle returns
(expected `EINVAL`/`Incompatible` — pinned, not guessed).

## Decision 5 — Write flags and delete semantics

All differential-pinned before implementation; expected shapes:

- **`NODUPDATA` (put):** exact (key, value) pair already present →
  `KeyExist`; whether the §S2 returned-existing-value contract applies to the
  dup case is pinned. heed surface: `PutFlags::NO_DUP_DATA`.
- **`NODUPDATA` (cursor del) / `Database::delete(key)`:** delete **all** dups
  of the key, freeing the sub-page in place or the whole sub-tree into the
  txn free set. `Database::delete_one_duplicate(key, val)` /
  `del_current` delete exactly one pair. `entries` decrements per pair.
- **`APPENDDUP`:** the dup analogue of §S1 — compares only against the
  **current last dup** of the key under `dup_cmp`; `new ≤ last` (equal
  included) → `KeyExist`; first dup of a key always succeeds. The
  `APPEND|APPENDDUP` combination and APPENDDUP-after-clear interactions are
  fuzz-reachable and pinned (mindful of the FORK-1 crash class — see
  Decision 7).
- **`MULTIPLE`:** DUPFIXED-only bulk put of N same-size items; on a
  non-DUPFIXED DB → `Incompatible`. heed exposes no surface for it, so it
  lands in `zerodb-core` + the native `zerodb` API (third-table extension);
  the adapter mirrors nothing. Its differential runs through direct
  `lmdb-master-sys` FFI inside `zerodb-oracle` (the one crate allowed to),
  since heed cannot drive it. Partial-success semantics (how many items were
  stored when it errors mid-batch) are pinned.

## Decision 6 — Invariants and the check tool

New invariants continue from INV-21 (SPEC 03 §11):

- **INV-22 — Dup-flag coherence:** `F_DUPDATA` appears only in DBs whose
  DBRecord.flags has `DUPSORT`; in a DUPSORT DB **every** multi-value key uses
  it (no mixed encodings); `P_SUBP`/`P_LEAF2` never appear outside dup
  structures; `DUPFIXED` implies `DUPSORT`.
- **INV-23 — Sub-page well-formedness:** compact header bounds coherent
  (INV-9 analogue); dup values strictly ascending under the dup ordering
  (memcmp for `check`, same §2.0 file-level caveat as INV-5); the cell fits
  `max_node_size`; ≥ 2 dups (a 1-dup sub-page, if the pinned demotion rule
  says it can't exist, is a defect — final wording follows the pinned rule).
- **INV-24 — Sub-tree well-formedness:** INV-5/6/7/8/9/16/19 recurse one
  level down; sub-tree cells have `dsize == 0`; **no** `F_BIGDATA`,
  `F_DUPDATA`, or `F_SUBDATA` inside a sub-tree (depth-one nesting only).
- **INV-25 — Dup count accuracy:** parent DBRecord.`entries` = Σ dup pairs;
  each embedded sub-tree DBRecord's own counters match its walk (INV-18
  analogue); stat folding matches the pinned Decision-1 accounting.
- **INV-26 — LEAF2 packing:** `leaf2_ksize` > 0, uniform across the DB and
  equal to DBRecord.`leaf2_ksize`; page byte-bounds are exact multiples;
  items ascending under the dup ordering.
- **INV-27 — Dup reachability:** sub-tree pages participate in INV-10
  (exactly-once reachable-XOR-free), INV-16 (single parent — reachable
  through exactly one `F_DUPDATA|F_SUBDATA` record), and INV-20 (txnid
  stamps).

`zerodb-tools check` learns to descend `F_DUPDATA` values (both encodings) and
enforce INV-22..27. `dump`/`load` already speak the `mdb_dump` VERSION=3
format (ADR-0009), which represents duplicates as repeated key lines and
carries dup flags in the header — the byte-identical-dump differential
extends to DUPSORT DBs, and `migrate-from-lmdb` gains real DUPSORT migration
(flag bits copy verbatim per Decision 3). INV-21's Phase-1 wording is
rewritten to "zero unless licensed by DBRecord.flags per §10/§12" — the
defensive rejection stays for non-dup DBs.

## Decision 7 — Test strategy

1. **Pin first, implement second.** Every "pinned by oracle" item above
   becomes a differential test in `zerodb-oracle` written and run against the
   fork **before** the corresponding zerodb code exists (the M1.10/S1
   protocol). The pin list is the first deliverable of each stage.
2. **Fuzz driver extension.** `zerodb-oracle/src/op.rs` `Op` gains dup
   variants: `CreateDb` grows a `DatabaseFlags` dimension (dup-flag subset),
   plus `PutDup`, `PutFlaggedDup` (NODUPDATA/APPENDDUP), `DeleteOneDup`,
   `DeleteAllDups`, `GetBoth`, `GetBothRange`, `DupCursorWalk` (scripted
   FIRST_DUP/NEXT_DUP/... sequences compared step-by-step), and DUPFIXED
   `PutMultiple`/`GetMultiple` (FFI-driven on the oracle side). The driver's
   validity classification becomes dup-aware: a dup op against a non-dup DB
   is a **compared** error outcome (both engines must refuse identically),
   not a skipped input. Value generation is biased toward the promotion
   boundary (sub-page-capacity ± a few items) and toward equal/adjacent dup
   values under the active dup ordering.
3. **Fork-crash avoidance (the FORK-1 lesson).** Known fork crashes are
   pre-classified in the driver by pattern and skipped *on both engines*
   (never fuzzed into, never silently divergent); any new fork SIGSEGV found
   during 2.8 is minimized, filed in `docs/UPSTREAM-BUGS.md`, guarded the
   same way, and noted in DIVERGENCES — the APPEND-family × same-txn-clear
   region that produced FORK-1 overlaps APPENDDUP and gets the guard
   proactively extended before the long fuzz runs.
4. **Gates.** The PLAN-mandated dedicated dup fuzz target runs **≥ 2 h clean**
   as a 2.8d exit gate (plus the 10-min `just fuzz-quick` slice from 2.8a on).
   `fuzz_page_decode` learns arbitrary `P_SUBP`/`P_LEAF2` bytes must never
   panic the decoder. The sub-page codec (pure slice logic) is under
   `cargo miri test -p zerodb-core`. The crash harness gains a dup-heavy
   workload (no new fsync ordering exists, but sub-tree alloc/free churn is
   exactly what stresses INV-10 across recovery). loom: none required —
   Decision 4 adds no new lock-free interaction (the sub-cursor is
   txn-confined state); if implementation contradicts this, that is a stop-
   and-re-ADR event.

## Decision 8 — Staging (each stage lands green through the full gate)

- **2.8a — Format, flags, sub-page, point ops.** SPEC 02 §10 / SPEC 03 §12
  amendments with exact layouts (the format freeze — *all* on-disk decisions,
  including sub-tree and LEAF2 layouts and the Decision-3 persistence
  outcome, are written here even though later stages implement them);
  DBRecord.flags persistence + open-time `Incompatible` checks + `EINVAL` on
  unknown flag bits (Table 2 note); the dup-comparator registry slot;
  sub-page codec; put/get/delete-one/delete-all + NODUPDATA on sub-page-sized
  sets; `Database::len` counting pairs. Dup sets that would outgrow the
  sub-page return a clean temporary `Unsupported` error, and the stage-a fuzz
  driver bounds dup cardinality below the cap. **Fence:** every on-disk byte
  written in 2.8a is final — later stages add structures, never reinterpret.
- **2.8b — Sub-tree promotion + full dup cursors.** Promotion/demotion,
  sub-tree GC/free integration, the eleven cursor ops + `get_duplicates`
  iterator on both encodings, APPENDDUP; the cardinality cap and the
  `Unsupported` error are deleted. INV-27 becomes checkable and the crash-
  harness dup workload lands here.
- **2.8c — DUPFIXED.** `P_LEAF2` pages + packed sub-pages, leaf2_ksize
  plumbing, size-discipline semantics as pinned, GET_MULTIPLE /
  NEXT_MULTIPLE / PREV_MULTIPLE, MULTIPLE bulk put + oracle FFI driver.
- **2.8d — Adversarial closure.** check-tool INV-22..27 + dump/load/migrate
  dup support; the dedicated fuzz target's op-distribution tuning; the ≥ 2 h
  clean run (plus a 24 h nightly soak before the milestone is called done);
  DIVERGENCES/SPEC final sweep; spec-reviewer pass.

A stage may **not** leave: on-disk state a later stage migrates (fence above);
a partially-implemented cursor op (an op is absent-with-clean-error or fully
pinned-and-green, never approximate); an unpinned observable filled with a
guess; a weakened INV-21 without its replacement invariant active in `check`.

## Decision 9 — Risk register (likeliest defects, with guards)

1. **Sub-page ↔ sub-tree conversion** (LMDB/mdbx's richest bug seam:
   promotion mid-split, conversion losing the pending item, demotion edge).
   *Guards:* fuzz value-size distribution biased to straddle the boundary
   (Decision 7.2); a dedicated proptest driving every dup set across
   promotion and back under randomized interleaved deletes; INV-25 count
   check after every fuzz case, not just at the end.
2. **Cursor tracking across mutation** (sub-cursor stale after parent leaf
   COW/split/rebalance; LMDB fixed variants of this for years). *Guards:*
   SUBC-1 re-resolution is the *only* legal path (no incremental fixup code
   to get wrong); multi-cursor fuzz ops interleaving writes with positioned
   dup cursors; the §S3 no-borrow-across-mutation rule extended to dup reads
   and miri-covered.
3. **GC leak / double-free of dup structures** (delete-all vs sub-tree walk,
   demotion freeing, abort discarding a mid-promotion state). *Guards:*
   INV-10/INV-27 run by the harness after **every** fuzz iteration commit;
   crash-harness dup workload (kill mid-promotion, reopen, full check);
   abort-path differential ops in the fuzz driver.
4. **DUPFIXED size-edges and MULTIPLE partial success** (mismatched item
   size, item size vs page size interactions at 64 KiB pages, batch failing
   mid-way). *Guards:* the semantics are oracle-pinned before code
   (Decision 2); the FFI MULTIPLE differential compares stored-count and
   post-state, not just the error code; page-size axis (4K–64K) already in
   the fuzz driver applies to all dup targets.

## Consequences

- SPEC 02 §1/§2.1/§3.1/§4.2/§10 and SPEC 03 §2.0/§12 get normative
  amendments in 2.8a; SPEC 01 Tables 2/3/5 rows flip WON'T→landed per stage;
  SPEC 00 second-table dup rows move to the main table as they land.
- INV-21 is rewritten (Decision 6); `check`, `dump`, `load`,
  `migrate-from-lmdb` all grow dup support; D-014 either gets its resolution
  (fingerprint approved) or its scope extended to dup comparators.
- The M2.4 comparator work generalizes cleanly (a second `KeyCmp` slot);
  no public heed-mirrored signature changes — heed's existing dup surface
  activates, MULTIPLE stays native-only.
- Cost honestly stated: this is the largest remaining parity surface, four
  gated stages, each ending with the full CLAUDE.md command gate.

## Open questions for human review

1. **Comparator fingerprint (resolves D-014):** approve splitting DBRecord
   offset 44 into `leaf2_ksize: u16` + `cmp_fingerprint: u16` with
   refuse-on-mismatch (better than LMDB, needs sign-off), or decline and
   extend D-014's documented hazard to dup comparators (exact LMDB parity)?
2. **MULTIPLE exposure:** core + native `zerodb` API only, with the oracle
   driving LMDB via direct FFI (heed has no surface) — acceptable, or should
   MULTIPLE stay engine-internal/untested-by-differential until a consumer
   appears?
3. **Scope of persisted-flag activation:** 2.8a persists and enforces the
   four dup bits. Should `REVERSEKEY`/`INTEGERKEY` bits (pure key-comparator
   selections mapping onto M2.4 machinery) ride along in 2.8a since the flag
   plumbing is being built, or stay parked?
4. **Staging fence acceptance:** is 2.8a's temporary `Unsupported` error for
   over-capacity dup sets acceptable as a stage boundary (it is not LMDB
   behavior; it exists only until 2.8b and is unreachable once promotion
   lands)?
5. **Pre-implementation pin list:** the oracle-observation items marked
   "pinned" above (dup value size bound, stat folding, demotion rules,
   GET_BOTH_RANGE return quirks, DUPFIXED size-mismatch, MULTIPLE partial
   success, dup-op-on-non-dup-DB errors, main-DB-DUPSORT interaction with
   §S8) will be executed and written into SPEC before any engine code —
   confirm this ordering is the required reading of CLAUDE.md rule 1 for
   this milestone.


---

## Open-question resolutions (2026-07-20, session lead under standing directive)

**Q1 — comparator fingerprint split: APPROVED.** Split `DBRecord` offset 44 into
`leaf2_ksize: u16` + `cmp_fingerprint: u16`; refuse-on-mismatch at open. This
**supersedes D-014's "Phase 3 candidate" note**: the marginal cost now is 16
provably-spare bits and one check at open, whereas the same fix after any 2.8
file exists costs a format-version bump and a migration path. It converts silent
corruption into a clean refusal, changes nothing observable for consumers (none
use custom comparators), and this is the last moment it is free. Yes, it is
better-than-LMDB — that is acceptable when the alternative is preserving a
known silent-corruption mode for symmetry's sake.

**Q2 — `MDB_MULTIPLE` as native-only API with an FFI-driven differential:
ACCEPTED.** heed exposes no surface for it, so nothing consumer-facing depends
on it; FFI inside `zerodb-oracle` is the sanctioned location (ADR-0001).

**Q3 — `REVERSEKEY`/`INTEGERKEY` persisted bits ride along in 2.8a: YES.** Same
reasoning as Q1 — one format touch, not two.

**Q4 — 2.8a's over-capacity `Unsupported` error as a stage boundary: ACCEPTED**,
conditional on it being a clean typed error (never a silent truncation or a
partial write) and on 2.8a writing no on-disk state that 2.8b must migrate,
which the staging fence already requires.

**Q5 — the pre-implementation oracle pin list as the required first act:
CONFIRMED, emphatically.** In the area PLAN.md calls LMDB's
highest-defect-density, behavior is observed before it is implemented. No engine
code lands until the pin list is green against the fork.
