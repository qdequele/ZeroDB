# ADR-0025: Nested write transactions

- Status: Draft
- Related issue: #71 (divergence D-003)
- Date: 2026-10-17

## Context

### What heed exposes

heed 0.22.1 and Meilisearch's heed fork both expose a nested write
transaction publicly:

```rust
// heed-0.22.1/src/envs/env.rs
pub fn nested_write_txn<'p>(&'p self, parent: &'p mut RwTxn) -> Result<RwTxn<'p>>
```

It calls the crate-private `RwTxn::nested` (`heed-0.22.1/src/txn.rs`), which
calls `mdb_txn_begin(env, parent, 0, ..)`. The child is an ordinary `RwTxn`:
it has `commit`, `abort`, `nested_read_txn`, can itself be the parent of
another `nested_write_txn`, and aborts on drop. The parent is borrowed
mutably for the child's whole life. heed's `examples/nested.rs` shows the
intended use: a child that aborts leaves the parent untouched, and a child of
a child that commits up both levels lands in the parent.

ZeroDB's adapter has no such method (`crates/heed-zerodb/src/env.rs` has
`nested_read_txn` only), so a caller does not compile against ZeroDB. SPEC 04
TXN-40, D-003 and ADR-0007 (Q2/Q3) all rest on the premise that heed keeps
nested writes private. That premise holds for `RwTxn::nested` but not for
`Env::nested_write_txn`, which is public. The issue was corrected on
2026-10-17; TXN-40, D-003 and `docs/COMPATIBILITY.md` (rows for
`nested_write_txn`) still carry the old wording.

### Consumers

No consumer calls it. Checked on 2026-10-17:

- Meilisearch, `origin/main` at `541dd6e94` (2026-10-15):
  `git grep -e nested_write_txn -e "RwTxn::nested"` finds nothing.
- hannoy 0.2.0 in the cargo registry, and the arroy and cellulite git
  checkouts: no match.
- The only matches in the registry and git checkouts are heed's own source
  (`env.rs`, `encrypted_env.rs`) and its `examples/nested.rs`.

### What LMDB does

Read from the vendored fork source
(`lmdb-master-sys-0.2.6/lmdb/libraries/liblmdb/mdb.c`, the
`mdb.master.nested-rtxns` branch heed 0.22.1 builds). This is a reading, not an
observation. Rule 1 requires the observable parts to be pinned by an oracle
test before any implementation (see the spike, step 0).

**Begin (`mdb_txn_begin` with a parent, no `MDB_RDONLY`).**

- Only a write txn can be a parent. A write txn has at most one write child.
- The begin fails with `MDB_BAD_TXN` when the parent is blocked: finished,
  errored, or already has a child (`MDB_TXN_BLOCKED` includes
  `MDB_TXN_HAS_CHILD`). So a write child cannot open while nested read
  children of the same parent are live, and vice versa.
- **Under `MDB_WRITEMAP` a nested write txn is refused** with `MDB_BAD_TXN`
  (the parent is a write txn). The comment states the rule: "if the nested txn
  is a write txn there may only be 1, no writemap". Nested *read* children are
  allowed under `MDB_WRITEMAP` (SPEC 04 TXN-35 already records this).
- The child gets its own, empty dirty list and its own, empty free-page list.
  It copies the parent's database table (`mt_dbs`), its next page number
  and its remaining dirty room. It shares the parent's txnid, so `id()` is
  the same on both.
- The env-level list of pages reclaimed from the free-list DB
  (`me_pghead`, `me_pglast`) is saved and a private copy is given to the child,
  so pages the child takes from it can be given back on abort.
- Cursors open on the parent are shadowed: the child gets copies
  (`mdb_cursor_shadow`), merged back on commit and restored on abort.

**Writing in the child.**

- Touching a committed page works as at the top level: a new page number is
  allocated, the old one goes on the child's free list.
- Touching a page that is dirty *in an ancestor* copies it into a new buffer
  **at the same page number** in the child's dirty list (`mdb_page_touch`,
  the `txn->mt_parent` branch). The ancestor's copy is not modified. This is
  page shadowing.
- Freeing a page: it becomes a reusable "loose" page only if it is dirty *in
  the child's own* dirty list (`mdb_page_loose`). A page dirty only in an
  ancestor goes on the child's free list instead, so the child never reuses
  the ancestor's buffer.
- Spilling (`mdb_page_spill`) only writes pages from the child's own dirty
  list, and never a page that an ancestor has already spilled ("can't spill
  twice"): overwriting it would destroy the ancestor's only copy.
  Un-spilling a page an ancestor spilled copies it into the child; the
  ancestor's spill mark stays until the child commits.

**Commit of a child (`_mdb_txn_commit`, the `txn->mt_parent` branch).** No
I/O. The child's free list is appended to the parent's. The parent takes the
child's next page number, database table and flags. Pages dirty in the child
are removed from the parent's spill list; pages the child spilled are removed
from the parent's dirty list. The two dirty lists are merged by page number;
where both hold a page, the child's copy wins and the parent's buffer is
freed. The child's spill list and loose pages are appended to the parent's.
Children commit recursively: committing a txn first commits its live child.

**Abort of a child (`mdb_txn_end`).** The child's dirty buffers, spill list
and free list are freed. The saved reclaimed-page state is restored into the
env. The parent's flags lose `MDB_TXN_HAS_CHILD`; nothing else in the parent
changed. Pages the child spilled stay in the file as unreferenced bytes.

**Errors.** Committing a child that has errored returns `MDB_BAD_TXN` and
**marks the parent errored too**. Aborting an errored child leaves the parent
usable.

**Nested read txns (the fork feature).** A nested write child is itself a
write txn, so it can be the parent of nested read children. A parent with a
live write child cannot open nested read children (blocked, as above).

**Database handles.** A child shares the parent's handle sequence numbers
(`mt_dbiseqs`). Handles opened in a child are closed when the child aborts
and become the parent's when it commits (`mdb_dbis_update`, called from
`mdb_txn_end`).

### How ZeroDB's write path is built today

One `RwTxn` (`crates/zerodb-core/src/rwtxn.rs`, `struct RwTxn`) holds all of a
write transaction's state, flat:

- `dirty: DirtyStore` (`crates/zerodb-core/src/dirty.rs`): one map from page
  number to a stable frame (`Slot::Heap` box, or `Slot::Map` under in-place
  WRITE_MAP), the spilled set, the spare-frame pool and the page count the
  dirty limit bounds (SPEC 04 TXN-41..44, TXN-68).
- The allocator: `freed` (committed pages this txn obsoleted), `loose` (pages
  this txn allocated and freed again), `drains` and `reclaimed` (pages taken
  from the GC tree), `annex` and `annex_out` (the meta free-list annex,
  ADR-0022), `alloc_mode`, `save_touched`, `next_pgno`, `committed_last_pg`.
- The working trees: `main_db`, `free_db` and `open` (named-DB working
  records, with a `dirty` flag for the commit write-back).
- Caches: `fingers` (rightmost-leaf fingers, SPEC 03 §6.6), `validated` (the
  validated-pages memo), `oldest_cache` (the GC gate), `path_buf`.
- Spill state: `dirty_limit`, `spills`, `read_high` (SPEC 04 §6.3a).
- `errored` (LMDB's `MDB_TXN_ERROR`), and `children`, the nested-read counter
  (ADR-0007).

Points that matter here:

- **Reads take one lookup.** `TxnRead::source` builds `Source::Writer`, and
  `Source::bytes_from_classified` (`crates/zerodb-core/src/btree.rs`,
  `#[inline(always)]`) probes the dirty store, then the map. There is no
  parent chain. A previous attempt to add one more lookup inline on this path
  (the spilled set, PERF-GAP B13) cost 3–13 % on read benchmarks.
- **An already-dirty page is edited in place.** `RwTxn::touch` returns early
  when `self.dirty.contains(pgno)`; the caller then edits through
  `DirtyStore::bytes_mut`. A child cannot do that to a page its parent owns.
- **Loose classification is by page number, not by owner.** `free_page` and
  `free_run` call `self.dirty.discard(pgno)` and classify a page as loose when
  `pgno > committed_last_pg` or it was reclaimed this txn. `allocate_raw` pops
  `loose` first. In a child, that would drop a parent's frame and let the
  child reuse its page number.
- **Spilling** (`maybe_spill`, `spill`, `unspill`, `write_frames`) writes
  frames at their page numbers, drops them, marks them spilled, re-derives
  the whole-map view and resets the memo.
- **In-place WRITE_MAP** (ADR-0021, TXN-45b): a dirty frame *is* the map at
  its page number (`dirty::map_mut`). There is only one place a page's bytes
  can live, so a child cannot hold a second version of a parent's page at the
  same page number.
- **Nested read txns** (`crates/zerodb-core/src/nested.rs`, ADR-0007) borrow
  `&RwTxn` and delegate reads live to the parent. The writer is frozen while
  they live (TXN-29/30). The `RwTxn: Sync` contract (no interior mutability on
  `&self` paths) is asserted at compile time there.
- **Cursors.** `RwCursor<'t, 'env>` holds `&'t mut RwTxn`. Read cursors and
  iterators borrow `&RwTxn`. A child that borrows `&mut parent` therefore
  makes every parent cursor and every parent nested reader unusable for its
  life: LMDB's cursor shadowing has no equivalent to build.
- **Database handles.** With ADR-0024 (branch `qdequele/stale-dbi-handles`,
  approved), handles are `(slot, generation)` and each txn carries a
  `TxnHandles` record of what it opened (`crates/zerodb-core/src/handles.rs`:
  `HandleTable::begin`, `open`, `end(committed)`, `check`). `check` already
  takes an optional parent so a nested read child accepts its parent's
  handles.
- **Adapter types.** `heed_zerodb::RwTxn<'p>` (`crates/heed-zerodb/src/txn.rs`)
  wraps an owned `zerodb::RwTxn<'p>` where `'p` is the env borrow. heed's
  nested child is `RwTxn<'p>` with `'p` the *parent* borrow. A safe Rust
  child holding `&'p mut zerodb::RwTxn<'env>` cannot be stored in a type with
  only `'p`, because `&mut T` is invariant in `T`. The adapter's write
  iterator (`crates/heed-zerodb/src/iterator.rs`, `RwGuts`) is today "the
  single place the adapter needs raw pointers / lifetime erasure".

### What does not change in any option

- **Crash safety.** A child writes nothing durable. Its commit is a merge in
  memory; only the outermost commit runs the pipeline (SPEC 04 §9, C0–C6).
  A child's spills and in-place stores only target pages this write txn
  allocated (TXN-62/70), so a crash or an abort leaves them as unreferenced
  bytes, the state SPEC 06 REC-6 H0/H1 already allows.
- **Multi-process (ADR-0023).** The writer locks, the reader table and the GC
  gate belong to the outermost txn. A child takes no lock and claims no slot.
- **Memory ordering.** A child is reached only through `&mut parent`, so it
  runs on one thread at a time; moving it between threads goes through the
  move's own happens-before edge. No new atomic is needed. Nested read
  children of a child use the child's own `ChildCounter` with the existing
  `Release`/`Acquire` pairing (TXN-31). The `RwTxn: Sync` contract must hold
  for any new field: nesting state lives behind `&mut`, never in a `Cell`.

## Options

### Option A — Overlay dirty store per child (LMDB's page shadowing)

The `RwTxn` keeps a stack of dirty-store layers. The outermost layer is
today's store. A child pushes an empty layer.

- **Reads** probe the layers from the innermost out, then the map.
- **Touch.** A page dirty in the current layer is edited in place. A page
  dirty in an outer layer is copied into the current layer at the same page
  number (as `unspill_copy` does today): no new page number, no parent
  pointer rewrite, no free. A committed page is copied to a new page number
  as today.
- **Free.** A page in the current layer is dropped and becomes loose. A page
  dirty only in an outer layer is not dropped: it is recorded as freed in
  the current layer and dropped from the outer layer at merge. The child
  starts with an empty loose list, so it never reuses an outer layer's page.
- **Allocator and trees.** At begin, the child saves the scalar and list
  state it can change: `next_pgno`, `drains`, `reclaimed`, `annex`, `loose`,
  `freed` (a length mark, if it stays append-only), `main_db`, `free_db`,
  `open`, `fingers`, `errored`. Abort restores it. Commit keeps the child's.
- **Spilling** writes only frames of the current layer, and skips any page an
  outer layer has spilled. Un-spilling an outer layer's page copies it into
  the current layer and leaves the outer mark until merge. The dirty limit
  counts all layers.
- **Commit** merges the current layer into the next one out, by page number:
  the child's frame replaces the parent's (the parent frame goes to the spare
  pool), the child's spilled pages leave the parent's frames, the child's
  dirty pages leave the parent's spilled set, and freed outer pages are
  dropped and classified. Frames are boxes, so the merge moves pointers, not
  bytes: cost is proportional to the child's frames.
- **Abort** drops the current layer and restores the saved state. The parent
  was never modified.
- **WRITE_MAP.** Refused with `MdbError::BadTxn`, as LMDB does. Under in-place
  WRITE_MAP an outer page's only copy is the map at its page number, so a
  shadow at the same page number cannot exist. The heap-staged realization
  (TXN-45a) could support it, but parity says refuse.

Pros:

- It is LMDB's own technique, so behavior (including which pages are reused
  and when the dirty limit trips) maps one to one onto the fork's.
- Abort is trivially safe: the parent's frames are never written by the
  child.

Cons:

- **The read hot path changes for every write txn.** A dirty-store miss must
  check whether outer layers exist before going to the map. Even a branch on
  an empty stack sits inside `bytes_from_classified`, which is force-inlined
  and was measured sensitive (B13). Avoiding that cost needs a separate
  `Source` variant used only while a child is live, which grows the code the
  inliner sees.
- More of the write path changes: `touch`, `free_page`, `free_run`, `spill`,
  `unspill`, `write_frames`, `is_unchanged`, `Source`, and the commit
  pipeline's frame iteration (it must see one merged layer).
- Merge has to get the spill and frame bookkeeping exactly right in both
  directions. A mistake there is silent corruption on the common path
  (commit), caught only by the differential tests.

### Option B — Undo journal on the single dirty store (savepoints)

The `RwTxn` keeps its single dirty store. A child is a savepoint: a nesting
depth plus a journal of pre-images.

- **Frames carry a depth tag** (one byte in each `Slot`). Every operation
  that changes or removes an existing frame (`bytes_mut`, `discard`,
  `remove`, `spill`, and `insert_*` over an existing page number) first
  checks the tag. If the frame is older than the current depth, its bytes
  (or "spilled" state) are copied to the journal and the tag is raised. A new
  frame gets the current depth. All of this lives in `DirtyStore`; `rwtxn.rs`
  does not change how it edits pages.
- **Allocator and trees** are saved and restored exactly as in Option A.
- **Spilling** needs no new rule: a frame from an outer depth is journaled
  before it is spilled, so its pre-image survives in memory.
- **Commit** of a child pops the depth. If the parent is itself a child, the
  child's journal entries for pages the parent has not journaled yet move to
  the parent's journal (the oldest pre-image wins). Cost is proportional to
  the child's journal.
- **Abort** removes every frame tagged with the current depth, reinstalls the
  pre-images (as heap frames, or written back into the map under in-place
  WRITE_MAP), restores the spilled marks and the saved state, and resets the
  validated-pages memo.
- **WRITE_MAP.** Refused by default for LMDB parity. Unlike Option A, the
  design does not need the refusal: under in-place WRITE_MAP the pre-image is
  copied out of the map, and an abort writes it back at the same page number,
  which is a page this write txn allocated (TXN-62). That write back would be
  a new use of the `dirty::map_mut` broker and would need the AGENTS.md
  `WRITE_MAP` exception widened. Not proposed now.

Pros:

- **Reads do not change.** One store, one lookup, no chain. The only cost on
  a non-nested txn is one comparison of a byte tag inside `bytes_mut` and
  `discard`, on the write side.
- The change is concentrated in `dirty.rs` plus one savepoint type in
  `rwtxn.rs`. `touch`, `free_page`, the split and merge code, and the commit
  pipeline keep their current logic.
- Child commit is cheap; abort cost is proportional to what the child did.

Cons:

- It is not LMDB's technique. Observable results must still match, but page
  reuse and file growth inside nested txns will differ in detail. That is
  allowed (D-002 covers the file layout), but the oracle must compare
  results, not page numbers.
- **The abort path carries the risk.** If any frame change bypasses the
  journaling accessors, the parent is corrupted, and only when a child
  aborts, which is the rarer path. Tests must force aborts often.
- The savepoint must cover every field a child can change. A field added
  later and forgotten would leak a child's change into the parent. Building
  the savepoint by destructuring `RwTxn` exhaustively turns that into a
  compile error.

### Option C — Keep it unsupported, but add the method

Add `Env::nested_write_txn(&self, parent: &mut RwTxn) -> Result<RwTxn>` to
the adapter. It always returns a documented error (`MdbError::BadTxn`, or a
dedicated "unsupported" error mapped through the heed taxonomy, SPEC 04
§8.1). TXN-40 already anticipates this ("should any future surface be forced
to exist ... it must return a clean documented unsupported error").

Pros:

- A few dozen lines. Code that names the method compiles.

Cons:

- It turns a compile error into a runtime error. Today a consumer that adopts
  the method finds out at build time; with Option C it finds out when the
  call runs, possibly in production. For a call with no fallback, the build
  error is the more honest signal. ADR-0007 reached the same conclusion
  ("unrepresentable is a stronger form of clean error").
- It closes no capability gap: partial rollback is still impossible.
- It can only help code that has a fallback, and no such code exists.

### Shared: the public API shape

Engine (`zerodb`):

```rust
impl<'env> RwTxn<'env> {
    pub fn nested_write_txn(&mut self) -> Result<NestedRwTxn<'_, 'env>>;
}
// NestedRwTxn: commit(self), abort(self), Drop = abort,
// nested_write_txn(&mut self), nested_read_txn(&self),
// and access to the write API of the RwTxn it wraps.
```

With Option A or B the child is a depth on the same `RwTxn`, so the engine
child only has to borrow `&mut RwTxn`. The child must check, on commit and
abort, that the depth it pushed is still the innermost one: safe code can
`mem::swap` the `RwTxn` behind a `&mut`.

Adapter (`heed-zerodb`): heed's child is `RwTxn<'p>`, the same type as a top
level write txn. Holding `&'p mut zerodb::RwTxn<'env>` in a type that names
only `'p` is not expressible in safe Rust (invariance). Two ways out:

1. **Erase the env lifetime once at construction**, as `RwGuts` does for the
   write iterator: the child holds a raw pointer to the parent's engine txn,
   valid for `'p` because the child borrows the parent mutably for `'p`. This
   adds a second lifetime-erasure site to `heed-zerodb` and needs an AGENTS.md
   unsafe-policy amendment and a miri test like `erased_cursor_in_place_tests`.
2. **Dispatch through a trait object** implemented by the parent adapter
   type, so the child calls the engine through `&'p mut dyn ...`. This is safe
   but needs the trait to cover every mutating entry point and the write
   cursor, which itself is already lifetime-erased.

### GC and free-list summary (Options A and B)

- Pages the child **allocated** past the parent's `next_pgno`: on abort
  `next_pgno` is restored, so the parent allocates the same numbers again. If
  the child spilled them, the file holds stale bytes there, which TXN-70
  already allows.
- Pages the child **drew from the GC tree or the meta annex**: on abort
  `drains`, `reclaimed` and `annex` are restored, so the pages are free again
  for the parent. The GC tree itself is only written at commit C1
  (`freelist_save`), never by a child.
- Committed pages the child **freed** (COW or delete): on abort the child's
  additions to `freed` are dropped; the parent's trees still reference them.
- Pages the parent **allocated and the child freed**: Option A keeps the
  parent frame and classifies the page at merge; Option B journals the frame
  before `discard` and reinstalls it on abort.
- `oldest_cache` survives an abort: the gate only becomes more conservative.
  `read_high` keeps its maximum (the file is backed up to it either way). The
  validated-pages memo is reset on abort.

## Decision

Draft, awaiting the maintainer. Recommendation:

1. **Keep D-003 in force and expose nothing yet.** Reject Option C as an end
   state: it trades a build error for a runtime error and adds no capability.
2. **Spike Option B** at the engine level only, to test the riskiest
   assumption below. Option A is the fallback if the spike shows journaling
   cannot be made complete inside `DirtyStore`.
3. Decide on a staged implementation only after the spike, with its line
   count and a rule 7 benchmark showing no regression on non-nested write
   txns. The adapter shape (erased pointer or trait object) is decided then.

Why B over A: it leaves the read path untouched for every write txn, which is
the path Meilisearch spends its time in, and it confines the change to the
dirty store plus one savepoint. The cost is that it is not LMDB's technique;
the stated reason (AGENTS.md rule 7) is the measured sensitivity of
`bytes_from_classified` to any extra probe (B13).

Correct the wording of TXN-40, D-003 and `docs/COMPATIBILITY.md` (they say
heed has no public nested-write entry point) in a separate small change,
whatever is decided here.

## Riskiest assumption and the spike

**Riskiest assumption.** Everything a child can change outside the frame
bytes (the allocator lists, the drain and annex pools, the named-DB working
records, the fingers, the spilled set, the error flag and the handle record)
can be saved at child begin and restored on abort, completely and at a cost
proportional to the parent's small state, without touching the commit
pipeline. This is shared by Options A and B; B adds the assumption that every
frame change already passes through a `DirtyStore` method.

**Spike, smallest form.**

0. Rule 1 first: an oracle observation test,
   `crates/zerodb-oracle/tests/lmdb_nested_write_observation.rs`, that pins
   through heed 0.22.1: abort and commit of a child, a child of a child, a
   child that creates, drops and clears databases, a handle opened in an
   aborted child, an errored child committed (parent errored) and aborted
   (parent fine), a child under `WRITE_MAP` (`BadTxn`), a nested read of a
   child, and a child that exceeds a small dirty limit and spills.
1. Engine only, no adapter, no public API: a `#[doc(hidden)]`
   `RwTxn::nested_write_txn` returning a child type, Option B journaling in
   `crates/zerodb-core/src/dirty.rs`, the savepoint in
   `crates/zerodb-core/src/rwtxn.rs`, `WRITE_MAP` refused.
2. A randomized differential test against LMDB: random put, delete,
   `delete_range`, clear, create and drop, with nested commit and abort up to
   depth 3, under a tiny `max_dirty_bytes` so spills cross child boundaries.
   Results compared after each step and after the outermost commit.
3. The ADR-0024 handle record in the child, if ADR-0024 has merged by then;
   otherwise named-DB create and drop inside children are left out of the
   spike and listed as unknown.

Files touched, estimated: `dirty.rs`, `rwtxn.rs`, possibly `nested.rs` (a
nested read child of a nested write child) and `handles.rs`, `crates/zerodb`
(re-export of the hidden entry point), and the two oracle tests. That is two
or three engine files, under the rule 6 threshold, and no format change.
Expected size: a few hundred lines of engine code plus the tests. If the
spike grows well past that, or needs changes in `btree.rs` or the commit
pipeline, that is the signal to stop and reconsider.

## Consequences

If the spike succeeds and the option is approved:

- SPEC 04 gains a section on nested write txns (new rule IDs after TXN-73);
  TXN-40 is retired explicitly, not renumbered; D-003 is closed; TXN-34 and
  TXN-36 lose their "nested write is unrepresentable" clauses.
- The `WRITE_MAP` refusal becomes a recorded parity rule, citing the fork's
  begin-time check.
- New tests: the oracle observation and the differential test above, a
  `heed-zerodb` adapter-pair test, `fuzz/` gains nested commit and abort
  operations, and the crash harness gains a case with a child that spilled and
  aborted before the outermost commit (it must recover to `N-1`).
- If the adapter erases a lifetime, AGENTS.md's unsafe list gains that site
  and a miri test covers it.
- Rule 7 numbers (LMDB, ZeroDB before, ZeroDB after) for the standard write
  benchmarks, to show non-nested txns did not slow down.

If the spike fails, D-003 stays, its rationale is corrected, and this ADR
records why.

## Open questions for human review

1. Is closing D-003 worth the work while no consumer calls
   `nested_write_txn`? The alternative is to correct D-003's rationale and
   leave the gap documented until a consumer asks.
2. Option B (journal, reads unchanged) or Option A (LMDB's overlay, reads gain
   a parent probe)? Is the B13 measurement enough of a reason to depart from
   LMDB's technique here?
3. Should `WRITE_MAP` refuse nested write txns (LMDB parity, `BadTxn`) for
   good, or should a later opt-in allow them, which Option B makes possible?
4. Adapter shape: accept a second lifetime-erasure site in `heed-zerodb`
   (with a policy amendment and a miri test), or require the safe trait-object
   dispatch even if it is more code?
5. Is Option C acceptable as an interim step (compiles, always errors), or
   should the method stay absent until it works?
6. Should the spike wait for ADR-0024 to merge, so handles opened in an
   aborted child can be tested from the start?
7. When an errored child is committed, LMDB marks the parent errored. Mirror
   that exactly, or treat it as a divergence candidate?
