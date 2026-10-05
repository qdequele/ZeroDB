# ADR-0022: Meta free-list annex — the per-commit freed PIL rides in the meta page

- Status: **Accepted (2026-10-05, Quentin)** — format change (`FORMAT_VERSION`
  1→2) directly ratified per CLAUDE.md rule 6, together with the crash-harness
  seed re-pin (198). Implemented first as a spike on approval relayed
  2026-10-02/03; re-gated on main after ADR-0021 (in-place WRITE_MAP) merged,
  with WRITE_MAP in-place twins of the annex battery.
- Result (2026-10-03): **measured win, gate green, spec-review clean.** Bench
  server 3-column A/B (x86-64, turbo off, 5 interleaved rounds, CODEGEN_UNITS=1,
  BASE = main 84582e8): `commit/batch/n1` 2.04× → **1.60×** LMDB (−22%,
  after÷before 0.780), `commit/batch/n100` 1.09× → 1.02×; `n10k` and both sync
  rungs flat (overhead amortized / fsync-bound — as predicted). Full gate: test
  630/0, miri 0-fail, crash-test-quick all durability modes, loom 9/0, stress
  180s 2/0, fuzz-quick 2.1M clean. Adversarial spec-review found no technical
  blockers; its should-fix/nit items are addressed in the branch.
- Real-case check (2026-10-05, three columns LMDB / before `8b066a6` / after,
  `benches/results/2026-10-05-meta-annex-real-case.md`): YCSB on Graviton4 NVMe
  durable B **1.078×** before (258k vs 239k ops/s, now 1.08× LMDB), no-sync
  0.98–1.04×; on the x86 bench server no-sync **1.03–1.07×**, durable flat
  (SATA-flush-bound). Write p50 −14% to −25% in every configuration on both
  machines. Meilisearch: no regression — movies indexing 1.00×, hackernews
  incremental additions 0.98× (commit span 0.94×), search 0.97×. Scope of the
  claim: frequent or durable commits; not a Meilisearch bulk-indexing speed-up.
- Milestone: perf track "non-copy per-commit CPU" lever #1 (cheaper free-list
  save), PERF-GAP-VS-LMDB §B12; forward-looking toward PLAN 3.1.
- Date: 2026-10-03
- Numbering note: ADR-0021 (WRITE_MAP in-place) lives on its own open branch;
  this ADR takes 0022 to avoid colliding with it.

## Context

B12 (bench server, x86-64, 4 KiB, 20k single-put NO_SYNC commits) attributes
the largest single slice of ZeroDB's +3.1 µs commit-CPU gap vs the LMDB fork
to the free-list save: ~1.6 µs/commit. Local census re-measurement in this
worktree (macOS aarch64, 4 KiB DB pages, temporary sub-phase instrumentation)
confirms both the magnitude and the shape:

- `freelist_save` total ≈ 1.2–1.5 µs/commit;
- per commit it executes ~2 `put_pil` + ~1 `delete_tree` on the GC tree
  (steady state: this txn's own entry, one partial-drain GC-20 rewrite, one
  fully-drained entry delete);
- of that, ~0.3 µs is one real COW touch of the GC leaf (frame + full-page
  copy), ~0.8–0.9 µs is the generic tree-op machinery of the three ops,
  ~0.1–0.2 µs is encode/sort/descent (descent is *not* the problem: the GC
  tree is one leaf; `search_path` is ~90 ns total), and the GC-11 loop runs
  exactly 1.00 iterations/commit;
- the save also dirties the GC leaf, so commit C2 writes one extra page per
  commit (ZeroDB 5.93 pages vs LMDB 4.96 in B12).

The format-stable ceiling is low. LMDB's own save does the same three
B-tree ops on the same flat txnid→PIL format and costs ~0.5–0.7 µs; matching
it means shaving generic per-op overhead (separate levers: touch cost B12-1,
lazy validation A2(b)) and buys at most ~0.5 µs here. The three tree ops per
commit are *inherent to the format*: the freed list of txn N must be durable
and crash-consistent with meta N, and in the flat-freelist format the only
place for it is the GC tree, whose mutation is itself a COW tree write.

With the format constraint lifted, the structural observation is: **the meta
page already is the one page every commit must write and CRC**. At 4 KiB the
meta's fixed content ends at byte 172; the remaining ~3.9 KiB are reserved
zeros. A typical commit frees a handful of pages (~5 in the census; a PIL of
c ids is 8c bytes). The freed list of txn N can therefore ride *inside* meta
N — written by the same `write_at_page`, covered by the same CRC, atomic with
the same meta — and the GC tree drops out of the per-commit path entirely.

Prior art: LMDB has no equivalent (its meta is ~112 bytes of a page and it
keeps the freelist in FREE_DBI unconditionally — and pays the same tree-op
cost we do). libmdbx likewise keeps a GC table but batches differently. This
is a deliberate, documented **non-LMDB lever** (observable heed-level
semantics unchanged; allocation-order determinism preserved), in the same
spirit as the ratified rightmost-leaf finger (ADR-0015 note in rwtxn.rs):
LMDB's own technique for this path cannot go below LMDB's own cost, and the
B12 target is to *beat* that bucket, with PLAN 3.1 already pointing at a
freelist redesign.

## Options

### Option A — format-stable micro-optimisation (rejected)

One search+touch for the whole save (all steady-state keys land in the same
GC leaf), direct `LeafMut` edits, kill the two per-save `Vec` clones
(`freed.clone()`, `remaining.to_vec()`).

- Pros: no format change, no recovery surface.
- Cons: measured ceiling ~0.3–0.5 µs of the 1.6; duplicates leaf-edit logic in
  the riskiest file; still COWs + rewrites a GC leaf and still writes the
  extra page at C2; becomes dead code when 3.1 lands.

### Option B — freed-PIL annex in the meta page (chosen)

Meta format (FORMAT_VERSION 1 → 2; offsets `[0, 168)` unchanged):

| Off | Size | Name | Meaning |
|----:|-----:|------|---------|
| 168 | 4 | `fl_count` (u32) | Number of annex ids. `0 ≤ fl_count ≤ (psize − 176) / 8`. |
| 172 | 4 | `meta_crc` (u32) | CRC32C over `[0, 172) ∪ [176, 176 + 8·fl_count)`. |
| 176 | 8·c | `fl_ids` | LE u64 page ids, strictly ascending, each in `[FIRST_DATA_PGNO, last_pg]`. |
| rest | — | reserved | MUST be 0; not CRC-covered. |

Semantics: the annex is **exactly the PIL that version 1 stored in the GC
tree under key `BE(meta.txnid)`** — the pages freed by that commit — hoisted
into the meta. Freeing-txnid = the meta's own txnid; no separate field.

- **Write side (commit C1).** `freelist_save` keeps the *unconsumed
  remainder* of the base meta's annex as a live, gated **in-save pool** (a
  fourth GC-12 source, exactly the drain-pool rule — an upfront fold into
  `freed` was the spike's first cut and ratcheted the file unboundedly,
  because folded ids are un-allocatable in-save and the save's own GC-tree
  ops then extend on every commit; caught by `churn_parity_general`). The
  GC-11 loop for drain rewrites (GC-20) runs unchanged. Step (c) persists
  the **merge** `freed ∪ annex.live()`: if it fits the annex cap it is
  **stashed for the meta encode** and no tree put happens (the stash cannot
  allocate or free pages, so the GC-11/13 fixed point is reached with no
  step-(c) feedback); otherwise the whole merged list goes to the tree under
  `BE(writer_txnid)` exactly as today and the annex is empty — re-looping if
  the put itself drew from the pool, so the persisted set never lists a
  handed-out page. All-or-nothing — one freeing-txn's pages are never split
  between annex and tree. Once the tree arm has run in a save, it stays the
  arm for that save (no flip-flop if a drain rewrite later shrinks `freed`).
- **Read side (allocation).** The writer parses the base meta's annex at txn
  begin (one u32 read when empty; `validate_pil_ids` on the ids otherwise —
  the freelist is still never trusted, GC-18). `allocate()` draw order
  becomes: loose → GC tree (`gc_reclaim`, unchanged) → **annex pool** →
  extend. The annex's freeing-txn (`base.txnid`) is the *newest* possible F,
  so trying the tree first preserves the GC-18/19 oldest-first determinism.
  The annex draw is gated exactly like any GC draw: `F ≤ oldest_reader()`
  (here: no live reader pinned below base). Drawn ids enter `reclaimed`.
  Inside `freelist_save` (GcSave mode) the annex pool **is** consulted, as
  the carried pool (GC-30): after the drain pool (`save_pool_draw`) is
  exhausted, `allocate()`'s GcSave arm falls through to the same
  `annex_draw`, gated the same way. This is safe only because the Write-side
  step (c) placement re-reads `annex.live()` after every tree put before
  deciding what to persist, so a draw here can never leave a handed-out id in
  the persisted set — the GC-12-amended anti-leak argument applies to this
  draw exactly as it does to `save_pool_draw` (`crates/zerodb-core/src/
  rwtxn.rs`, the `AllocMode::GcSave` arm of `allocate`, ~line 1277).
- **Crash safety.** The annex is CRC-covered by the meta it belongs to: a
  torn meta write that corrupts the annex tears the whole slot, which the
  §3.2 selection discards (fallback to N−1, whose own annex+tree state is
  per-snapshot consistent). The TXN-62 argument for reusing annex ids is the
  in-save pool-draw argument verbatim: an id in meta B's annex was freed *by*
  txn B, is absent from B's trees, and B is the crash-fallback meta for the
  writer W = B+1 — so overwriting it before W's meta lands is safe, and any
  reader that pins after the draw pins `≥ B`.

- Pros: steady-state save does **zero tree ops** (no GC-leaf COW, no descent,
  no node edits, no extra C2 page); next txn's reclaim is an O(1) pool draw
  instead of a cursor scan + PIL decode; CRC/encode cost grows by 8 bytes/id
  (~40 B/commit typical).
- Cons: format break (version bump; pre-release, no consumers — approved);
  recovery/check/tools must learn the annex; a new carry invariant to hold;
  under a parked reader the carried list is re-encoded into every meta until
  it exceeds the cap and spills (bounded by the cap, ≤ ~3.9 KiB at 4 KiB psize).

### Option C — O_DSYNC-style / batching tricks

Not applicable: the cost is CPU in tree ops, not flush count (B12 is NO_SYNC).

## Decision

Option B, as a **spike** (CLAUDE.md rule 7: format change ⇒ smallest change
that tests the riskiest assumption — here, that the annex removes the
measured CPU while recovery, INV-22 and bounded growth stay provable).
Always-on with the FORMAT_VERSION bump (an opt-in annex would double the
format test matrix for no consumer benefit; "parity by default" governs
API-visible behavior, which is unchanged).

## Invariants (normative; SPEC 05 §2a / SPEC 02 §3 own the final wording)

- **GC-29 (annex = hoisted own-entry).** The annex of meta N holds exactly
  the ids version 1 would have written under `BE(N)`, strictly ascending,
  unique, in `[FIRST_DATA_PGNO, last_pg]`; `fl_count = 0` means no entry.
  A meta with `fl_count > 0` has **no** GC-tree entry keyed `BE(N)`.
- **GC-30 (carry + in-save pool).** Every committing txn W accounts for
  every live id of its base's annex: drawn ids are in `reclaimed` (reused or
  loose), and the remainder — drawable in-save as a gated pool — is merged
  into the step-(c) persisted set (W's annex or W's tree entry).
  Consequence: the per-image partition (INV-22, with the annex counted as
  free — INV-28) holds for every committed meta.
- **GC-31 (gate).** Annex draws require `base.txnid ≤ oldest_reader()`; the
  TXN-20/TXN-62 proofs apply unchanged with F = base.txnid.
- **GC-32 (all-or-nothing).** One freeing-txn's pages are never split between
  its annex and a tree entry.
- **GC-33 (never trusted).** Annex ids pass `validate_pil_ids` before any id
  is handed out, exactly like a tree PIL (GC-18 validation rule); `fl_count`
  is bounds-checked and the ids CRC-checked at slot validation.
- **INV-28 (check).** The checker counts annex ids of the selected meta into
  the free set: reachable XOR (tree-free ∪ annex-free), no double listing
  (extends INV-22/24/25/26 to the annex).

## Consequences

- `FORMAT_VERSION` 2; version-1 files are rejected at open (`BadVersion`) —
  pre-release, sanctioned. `migrate-from-lmdb` output moves to v2 implicitly
  (it writes through the engine).
- SPEC 02 §3 (meta table, §3.3 CRC coverage, §3.5 example note), SPEC 05
  (new §2a + GC-16/18/23 amendments, GC-29..33, INV-28), SPEC 06 (REC-8 note:
  the annex is inside the torn-meta guarantee) updated in this change.
- `Snapshot` carries the annex count so `free_page_count()` stays exact per
  snapshot (GC-23 note).
- New tests: meta annex codec roundtrip + torn-annex rejection + cap bounds;
  annex reclaim/carry/spill behavior (incl. reader-gate block); checker
  annex accounting; crash harness runs unchanged on the new format.
- The census' `allocate` cost also drops in steady state (the GC cursor scan
  disappears when the tree is empty). That is a *consequence* of this format
  change, not the separate allocate lever being folded in.

## Open questions for human review

1. ~~Ratify the format change itself (CLAUDE.md rule 6).~~ **Resolved
   2026-10-05:** ratified directly by Quentin.
2. ~~Cap policy: full `(psize − 176)/8` (chosen) vs a smaller policy cap.~~
   **Resolved 2026-10-05:** the full cap is accepted with the ADR.
3. Whether PLAN 3.1 should absorb this as its first stage (the annex is the
   hot tier of any future freelist redesign) or whether 3.1 supersedes it.
