# ADR-0021: True in-place WRITE_MAP — dirty pages live in the map, no heap staging, no commit write-back

- Status: **Accepted (2026-10-02, maintainer) — spike first**. Opt-in behind `WRITE_MAP`; default (heap-staged) path unchanged. Implementation via `critical-implementer`, full gate (loom for commit/meta ordering, crash, stress) + the adapter borrow audit; never merged without a green gate + spec-review. (Was: Draft, gated on the spike numbers below and human approval — rule 6: write-path + concurrency + unsafe.)
- Implementation note (2026-10-05): spike plus the production hardening pass below merged as PR #88 (8b066a6, 2026-10-05; the PR title still read "DRAFT, do not merge"). In-place is now what `WRITE_MAP` does. Still open: ranged msync (issue #45), the open questions at the end.
- Milestone: Phase 3 (performance), PERF-GAP B5 / issue #13; supersedes ADR-0017 (spilling) **for WRITE_MAP envs only**
- Date: 2026-10-01

## Context

The analysis of "why LMDB is still faster" localised the remaining gap, on the
workloads where ZeroDB still trails, to **one architectural cause**: during a
write transaction ZeroDB keeps every touched page as a heap `Box<[u8]>` in the
dirty store, and only lands it in the file at commit. This is true in **both**
backings today, including `WRITE_MAP`:

> *"during a write txn the dirty bytes still live in the engine-core heap
> dirty-page store … they are copied into the writable map at commit C2"*
> — `zerodb-io/src/lib.rs` `WriteMapBacking` (SPEC 04 §6.4, TXN-45a: "the map
> never changes [during the txn]").

So `WRITE_MAP` today changes only the *commit* mechanism (memcpy into the map +
`msync`, instead of `pwrite` + `fdatasync`). It does **not** reduce in-txn
memory and does **not** remove the per-page COW `Box` allocation + copy.

The measured consequences (fresh Graviton4 NVMe, 2026-10-01; and
`benches/results/2026-09-30-*.md`):

- **Memory / page-cache competition.** A write txn's heap dirty set competes
  with the OS page cache under a cap. ADR-0017 spilling bounds it (YCSB B
  no-sync 0.31× → 0.69×, A 0.58× → 0.76×, YCSB C no longer OOMs), but the dirty
  set is still *anonymous* heap the kernel cannot treat as clean file cache, and
  the residual 0.69–0.76× remains.
- **Per-commit CPU (B12).** `touch` allocates a fresh page `Box` and copies the
  whole page; commit then writes it back. Single-put commit ≈ 2× LMDB.

LMDB's `MDB_WRITEMAP` avoids both. It is still a copy-on-write B-tree — a
touched page gets a **fresh page number** (from the free list of pages older
than the oldest reader, or by growing the file), and the new version is written
**directly into the mapped file** at that new page's offset. At commit
`mdb_page_flush` early-outs (the bytes are already in the map); only `msync` +
the meta write remain. Because the dirty pages are *file-backed* map pages, the
kernel flushes and evicts them under memory pressure on its own — which is why
LMDB-WRITEMAP stays memory-bounded with no explicit spill.

This ADR makes ZeroDB's `WRITE_MAP` do the same. Per the maintainer's directive
(2026-10-01, "for everything unsafe but that improves perf, allow it by
activating a flag"), it is **opt-in behind the existing `WRITE_MAP` flag**; the
default (heap-staged) path is unchanged, keeping typed-error safety, abort-by-
drop, and no `PROT_WRITE` mapping.

## The three "contracts" — which actually block, re-examined

ADR-0004 / the B5 note cite three reasons the heap store exists. Grounding each
against the code shows the blast radius is smaller than "redesign everything":

1. **Reader safety / MVCC — not affected.** Writing into the map happens at a
   **freshly-allocated page number**, and the GC/reader-table protocol
   (ADR-0005/0006, TXN-62) guarantees a reclaimed pgno is older than the oldest
   live reader. So a write into the map at a fresh pgno can never clobber a page
   any live snapshot observes. The MVCC invariant is page-number COW, unchanged.

2. **The value-borrow contract — upheld by `&`/`&mut`, not by the heap.** LMDB's
   C API lets a returned `MDB_val` dangle after the next `put` on the same txn;
   the caller must not hold it. heed/Rust is *stricter*: `get` borrows the txn
   immutably and `put` needs `&mut`, so the borrow checker already forbids
   holding a value borrow across a mutation on the same txn. A dirty value
   borrowed into the map is therefore released before any in-place rewrite can
   occur. **Audit item:** the M1.13 lifetime-erased adapter write cursor
   (`heed-zerodb`) must be confirmed to preserve this exclusion (it is the one
   place lifetimes are erased).

3. **Abort-by-drop — becomes "don't advance the meta".** Pages written into the
   map at fresh pgnos that a commit never links are simply unreferenced; the
   next txn reclaims them (LMDB aborts under WRITEMAP exactly this way). A grown
   file is tolerated. Abort no longer unwinds map bytes — it releases the
   dirty-page tracking and leaves the committed meta untouched.

**Nested read txns (the fork's feature, ADR-0007)** read the map directly and
would see the writer's uncommitted pages at their new pgnos — correct, and
simpler than routing a nested reader through the heap store. The nesting
discipline already pauses the parent writer while a nested reader is live, so no
in-place rewrite races a nested borrow.

## Options

### Option A — true in-place WRITE_MAP (this ADR)
`touch`/new-page allocate the dirty frame **in the map** at the COW'd pgno via a
`zerodb-io`-brokered `&mut [u8]` slice (map `unsafe` stays in `zerodb-io`, per
the unsafe policy). Commit writes nothing back (data already mapped); it `msync`s
and writes the meta (ADR-0019 O_DSYNC path). Spilling (ADR-0017) is unnecessary
under WRITE_MAP — the kernel pages dirty map pages out. Default path unchanged.
- **Pro:** removes the heap dirty set (memory-bounded without spill, fixes the
  H2 page-cache competition and the YCSB C OOM at the root), removes the
  per-commit write-back and the per-touch heap alloc (B12).
- **Con:** the heaviest `unsafe` in the engine; miri cannot check mmap, so the
  burden falls on reasoning + loom + the crash/stress harness. ARM: the map
  writes must happen-before the meta flush (already the commit barrier ordering).

### Option B — keep heap staging, only pool frames harder
Reuse the dirty-frame pool (B18) more aggressively and `malloc_trim`/arena-return
after commit. Keeps all three contracts trivially and no new `unsafe`.
- **Pro:** safe, small. **Con:** does not make the dirty set file-backed, so the
  page-cache competition (H2) and the commit write-back remain; it chips at the
  constant, not the mechanism.

## Decision

Propose **Option A, opt-in behind `WRITE_MAP`**, default unchanged — *pending a
spike + human approval* (rule 6: write-path + concurrency + unsafe).

### Measured motivation (Graviton4 NVMe, 2026-10-01 — `benches-server/research/validation-results.md`)

A fair WRITE_MAP comparison (rust-storage-bench YCSB, 10M×128 B, 2 GB cap,
no-sync, WRITE_MAP set on **both** engines) settles whether this lever is worth
the unsafe:

| YCSB A | ops/s | ÷LMDB-default | write p50 |
|---|---:|---:|---:|
| LMDB default | 273k | 1.00 | 6.1 µs |
| **LMDB WRITE_MAP** | **801k** | **2.93×** | **1.2 µs** |
| ZeroDB default | 212k | 0.78 | 13.2 µs |
| ZeroDB WRITE_MAP (today, heap-staged) | 335k | 1.23× | 4.9 µs |

(YCSB B: LMDB-writemap 2.49× its default; ZeroDB-writemap 1.32×.)

- LMDB's WRITE_MAP is **true in-place** and is 2.5–2.9× its own default — commit
  write cost nearly vanishes (write p50 → 1.2–1.4 µs).
- ZeroDB's WRITE_MAP today only removes the per-commit `pwrite` **syscalls**
  (13.2 → 4.9 µs) but still heap-stages and memcpies heap→map at commit, so it
  reaches only 1.2–1.3× and sits at **0.42× (A) / 0.53× (B) of LMDB-writemap**.
- That 0.42–0.53× residual is precisely the heap-stage + heap→map copy + commit
  machinery this ADR removes. **The prize is large and is NOT captured by
  ADR-0017 spilling** (spilling bounds memory; it does nothing for this CPU/copy
  cost, and anon memory was identical 532 MiB in every current-WRITE_MAP run).

Earlier draft text said the spilling work had captured the prize and the spike
could be deferred — that was measured against LMDB-*default* and was wrong. The
fair comparison shows a 2–2.4× headroom that true in-place targets.

**Recommendation:** approve the spike. It should land behind `WRITE_MAP` on a
branch (never merged), run the full gate (fmt/clippy/test/miri where applicable/
loom for the commit-meta ordering/crash/stress + the adapter borrow audit), and
A/B on this host against LMDB-writemap. Target: close the 0.42–0.53× gap. If a
clean, sound implementation cannot approach LMDB-writemap, fall back to Option B
for the default path and keep WRITE_MAP heap-staged.

(Default-path consumers — Meilisearch does not set WRITE_MAP today — are served
separately by the *safe* B12 levers: used-portion COW copy + frame reuse +
publish-validated-pages-at-commit. Those need no ADR and no unsafe.)

## Spike result & adversarial review (2026-10-02)

A spike was implemented behind `WRITE_MAP` (commit d7f7d52, branch
`worktree-agent-a053adc0a6f95dd7a`, **not merged**) and reviewed by
`spec-reviewer`.

**Design verdict: sound.** The reviewer traced abort ("don't advance the meta";
aborted in-place pages are unreferenced, reclaimed later), nested read txns
(child reads in-map dirty frames through the paused writer's source; no torn
read observable), the general-leaf-split scratch copy (`DirtyStore::remove`
copies a map frame out to heap — LMDB does the same), the spill-degenerate path
(TXN-68..72 preserved, I/O zeroed), and the C2(noop)→C3 msync→C4 meta→C5 ordering
— all correct. Full gate green (test 623/0, miri 199/0, fuzz-quick, crash-quick).

**Measured win (commit_census, macOS arm64, NO_SYNC, 20k single-put commits):**
ZeroDB-writemap heap-staged → in-place: total **29.8–43.5 µs → 8.6–11.1 µs
(~3.5×)**; put 5.6–8.3 → 2.2–2.6 µs (COW Box alloc+copy gone); commit 24–35 →
6.3–8.5 µs (heap→map write-back gone). Default (non-WRITE_MAP) path unchanged.
Addresses the 0.42–0.53× architecture residual; the Graviton/NVMe A/B is still
the headline referee (owed).

**NOT mergeable as-is — production punch-list (review findings):**
- **B1 (confirmed soundness, BLOCKER):** `Backing::map_dirty_page` /
  `MmapWritable::slice_mut` are *safe* fns that mint `&mut [u8]` from `&self`
  (`clippy::mut_from_ref` suppressed). Safe code could call twice and alias →
  UB mintable from safe code. Fix: make them `unsafe fn` — which moves the
  `unsafe` call into `zerodb-core::dirty`, a home the unsafe policy does NOT
  currently sanction, so **CLAUDE.md must be amended (human decision)** — OR
  redesign the broker to hand out an exclusivity token tied to `&mut`.
- **B2 (SAFETY-argument gap, BLOCKER):** the writer's own later read of a
  *spilled* in-place page goes through the stale whole-map `&[u8]` taken at txn
  begin; under Stacked/Tree Borrows the root-derived `&mut` writes invalidate
  that tag. Pre-exists in kind (heap WRITE_MAP + ADR-0017 spill) but becomes the
  normal case here and the SAFETY comment overlooks it. Fix: re-derive
  `self.bytes` from `backing.bytes()` at each spill (where `read_high` changes).
- **B3 (vacuous crash gate, BLOCKER):** the fault-injection backend inherits
  `dirty_in_map()=false`, so every image-cut/torn-write crash scenario ran the
  heap path; SIGKILL can't drop page cache. Needs a map-aware fault backend
  before the crash gate means anything for this feature.
- **B4 (ADR gate unmet):** loom (commit/meta ordering), 180 s stress, and the
  M1.13 adapter borrow audit were not run. (Reviewer spot-checked the erased
  cursor; argument carries, but note: an in-place contract violation is a
  *silent* wrong-bytes read, not an ASAN-catchable UAF.)
- **M1:** add an `UnsafeCell`-backed test `Backing` with `dirty_in_map()=true`
  so the whole brokered discipline (B1/B2) runs under miri/Tree-Borrows.
- **M2:** TXN-62 is only debug_assert-guarded; in-place turns a future allocator
  bug into a silent committed-data clobber — add a release-mode
  `pgno>committed_last_pg || reclaimed` check on the brokered path.
- **M3:** re-run abort-after-spill, nested-fanout, put_reserved, and the crash
  battery parameterized over WRITE_MAP.
- **m1:** amend TXN-71's "map bytes change only on a later spill" (false in-place)
  and the §6.1/C5a "every frame C2 wrote" wording.

Next step per the ADR: the production pass under `critical-implementer`, gated
on the above, **after** the human calls B1 (unsafe-policy amendment vs typed
token). The spike branch stays unmerged as the reference.

## Production hardening pass (2026-10-02, critical-implementer; B1 called as unsafe-policy amendment)

- **B1** — `MmapWritable::slice_mut` and `Backing::map_dirty_page` are
  `unsafe fn`; the one sanctioned `unsafe` call lives in
  `zerodb-core::dirty::map_mut` with the policy's four invariants in its
  `SAFETY` block. On the lint: clippy's `mut_from_ref` fires on `unsafe fn`
  too (verified, clippy 1.97), so the `#[allow]` could not be dropped
  outright — but it now suppresses a documented false positive on an
  `unsafe fn` whose `# Safety` contract is exactly the exclusivity the lint
  fears, not a safe fn minting `&mut` from `&self` (B1's actual hazard,
  which is gone). The trait keeps a declaration-only `#[allow(unsafe_code)]`
  in `env.rs` (an `unsafe fn` signature with a trivially safe default body).
  A witness token was considered and rejected: per-region exclusivity through
  a `dyn Backing` cannot be typed by a token tied to one `&mut` without
  freezing the store's disjoint-field borrows; the `unsafe fn` + single call
  site is the honest shape.
- **B2 (+ a stronger M1 finding)** — the heap-staged paths re-derive the
  writer's whole-map `&[u8]` at every spill. Running the discipline under
  miri (M1) then showed spill-time re-derivation is **insufficient under
  Stacked Borrows for the in-place mode**: copying the stale view (every
  `Source` construction), and even moving the `RwTxn` (whose reference
  *field* is retagged on `commit(self)`), is UB at in-place-written
  locations. In-place txns therefore hold **no** cached whole-map reference
  at all (`bytes` is empty there) and borrow the view lazily per access,
  like readers (`RwTxn::whole_map`). TXN-71 amended accordingly.
- **B3** — the fault backend forwards `dirty_in_map`/`map_dirty_page` and
  journals brokered regions (deduplicated; bytes resolved against the live
  map at every `sync` seal point and at capture — `MS_ASYNC` seals preserve
  per-commit versions for the ordered sub-model). The image mechanism opens
  the real writable map for the WRITE_MAP modes, a vacuousness tripwire
  fails any WRITE_MAP cycle that issues an fd data write, and
  `crash_harness_smoke::writemap_image_cuts_run_in_place` pins non-vacuity
  in `cargo test`. The harness summary reports the journaled region count.
- **B4** — *loom:* no new model: in-place moves the writer's plain map
  stores earlier in program order (allocation/edit time instead of C2), but
  they stay single-threaded-writer work strictly before the same C3→C4→C6
  publish edges the existing reader-table/stamp-cache models check; readers
  still dereference only after the SeqCst pin/verify, nested children only
  behind the ChildCounter pause + scoped-join edges (identical to heap
  frames; TXN-35). No new atomic, lock, or ordering was introduced — `just
  loom` runs unchanged. *Stress:* `stress_readers_vs_writemap_in_place_writer`
  (8 readers × in-place churn writer with spills, 180 s under `just
  stress`). *Adapter audit:* see "M1.13 adapter borrow audit" below; plus
  `heed-zerodb::iterator::erased_cursor_in_place_tests` runs the
  lifetime-erased write cursor under miri over the in-place realization.
- **M1** — `zerodb_io::testmap::TestWriteMap` (`test-backing` feature;
  `UnsafeCell<Box<[u8]>>`, brokered `&mut` from the cell root, fd writes as
  reference-free raw copies) + `zerodb-core/tests/writemap_in_place_miri.rs`
  (mixed ops, splits, runs, `put_reserved`, spill/unspill, nested reads,
  abort, readers across commits). It caught the B2 insufficiency above on
  its first run — the discipline is now machine-checked on every miri gate.
- **M2** — `RwTxn::allocate` wraps every allocation in in-map mode with the
  release-mode typed guard (`pgno > committed_last_pg ∨ reclaimed`, each
  page of a run); failure errors the txn (`Io(other)`, only abort remains).
- **M3** — WRITE_MAP twins added for abort-after-spill (`dirty_spill`), all
  three nested fan-outs (`nested_fanout`, real threads over in-map dirty
  state), and the `put_reserved` adversarial battery; each asserts
  `dirty_in_map_mode()` so the twin cannot go vacuous. The crash battery
  runs in-place via B3.
- **m1** — TXN-71 and the §6.1/C5a "every frame C2 wrote" wording amended;
  TXN-45b gained the B1/M1/M2/B3 rules.

### M1.13 adapter borrow audit (contract 2, ADR-0021 B4)

The adapter erases lifetimes in exactly one structure, `RwGuts`
(`heed-zerodb/src/iterator.rs`): `RwGuts::new` reborrows `&'txn mut
RwTxn<'_>` through a `NonNull` cast so the native `RwCursor<'txn, 'txn>` can
carry the erased env lifetime, and `step` stretches the yielded `(k, v)` to
`&'txn [u8]`. Findings:

1. **No aliasing from the erasure itself.** The cast consumes an exclusive
   `&'txn mut` and the original is unusable for `'txn` (the iterator holds
   the borrow); the erased pointer only shortens the env lifetime parameter,
   it never duplicates access.
2. **The stretched borrows are governed by the same `unsafe fn` surface as
   heed/LMDB.** Every safe method on the `Rw*` iterators either yields
   (`next`, a `&mut` call that invalidates the previous pair before
   producing the next) or is `unsafe` (`del_current`, `put_current*`), whose
   documented contract — "no `&` borrow of the current entry may be live
   across this call" — is precisely the exclusion in-place needs. Safe code
   cannot hold `(k, v)` across a mutation of the same iterator (`next`
   takes `&mut self`), and cannot reach the underlying txn while the
   iterator lives (its `&mut` is captured).
3. **What in-place changes is the failure mode, not the contract**: a
   violator of the `unsafe` contract now reads rewritten map bytes (silent
   wrong data — LMDB `MDB_WRITEMAP`'s own behavior) instead of stale heap
   bytes. No adapter change is needed; the contract text already demands
   the exclusion. The miri battery above pins the compliant discipline on
   plain heap memory, where any internal slip (e.g. the bound test's
   comparator borrow overlapping the stretched pair) would be reported.
4. **`put_current_reserved_with_flags`** stages through a caller-side
   `vec![0; size]` and a plain `put_with_flags`, so no `ReservedSpace`
   points into the map from the erased cursor path at all.

Residual (pre-existing, unchanged by this pass): the `unsafe fn` mutation
surface relies on callers honoring the heed contract — under in-place the
blast radius of a violation is wrong bytes rather than a crash; this is the
fork's own WRITEMAP trade, accepted with the opt-in flag (Open question 3).

## Consequences

- New `zerodb-io` map-slice API (`&mut [u8]` for a page offset); SAFETY: exclusive
  `&mut` into a region no reader `&` covers (fresh pgno / own uncommitted page),
  single-writer, no concurrent writer.
- SPEC 04 §6.4 rewritten: under WRITE_MAP the dirty page is realised in the map,
  not the heap; TXN-45a ("the map never changes during the txn") amended for the
  WRITE_MAP case; abort semantics (TXN-xx) restated as "unreferenced fresh pages".
- SPEC 04 §6.3a (spilling) marked not-applicable under WRITE_MAP.
- Tests: the full dirty_spill / abort / nested-rtxn / crash battery re-run under
  WRITE_MAP in-place; a loom model for the commit/meta ordering; the adapter
  lifetime-erased-cursor borrow audit (contract 2).

## Open questions for human review

1. Make WRITE_MAP in-place the *only* WRITE_MAP behaviour, or a further sub-flag
   (`WRITE_MAP` + `MAP_IN_PLACE`)? LMDB has only `MDB_WRITEMAP`; matching it
   argues for no sub-flag.
2. Should Meilisearch be advised to set `WRITE_MAP`? It does not today; the win
   only reaches a consumer that opts in. (If Meilisearch will never set it, the
   effort buys bench numbers, not consumer speed — weigh against Option B, which
   helps the default path every consumer runs.)
3. `WRITE_MAP` makes a stray write corrupt the DB through the map (no typed
   error). That is LMDB's own trade; acceptable as an explicit opt-in?
