# ADR-0007: Nested read transactions over a write txn — implementation strategy

- Status: Approved — Quentin, 2026-07-16, standing directive, session-lead review
- Milestone: 1.9
- Date: 2026-07-16

## Resolution record (2026-07-16)

- **Q1 ACCEPTED** — `RwTxn: Sync` is a standing asserted contract, guarded by
  R2's permanent compile-time assertions (`RwTxn: Sync` and
  `NestedRoTxn: Send` asserted in `zerodb-core/src/nested.rs`, so a future
  interior-mutability field breaks the build, never soundness). Option B
  stays the documented fallback.
- **Q2 + Q3 RESOLVED: drop the `nested_write_txn` stub entirely** — heed's
  nested write is `pub(crate)`, so no public surface exists to error from;
  "unrepresentable in the API" is a stronger form of D-003's "clean error".
  TXN-40 amended accordingly; D-003's zerodb-behavior wording updated. No new
  error variant.
- **Q4 CONFIRMED** — `MdbError::BadTxn` for the TXN-29 runtime guard
  (`MDB_BAD_TXN` parity flavor).
- **Q5** — L6 models BOTH the counter-only world (L6a) and the join edge
  (L6b).

### Post-implementation notes (same change)

- **Drop-time assert dropped (D3 refinement):** the ADR's "drop path:
  `debug_assert` + proceed" is not implemented — a nonzero counter at
  `RwTxn`-drop is reachable *only* via the sanctioned-sound
  `mem::forget(child)` degradation (the borrow checker makes a live child at
  parent-drop unrepresentable), so the assert could only ever fire on sound
  code. TXN-33 is carried by the commit-C0 check and the compile-time
  by-value rule.
- **L6 mutation-check record (R1-style honesty):** weakening both counter
  orderings to `Relaxed` is NOT caught by L6 — the violation is
  load-buffering-shaped and outside loom 0.7's exploration (same class as the
  M1.8 L2b record). It is real on AArch64 (load→store reordering), so the
  `Release`/`Acquire` pair is normative per TXN-31 (as amended), guarded by
  the per-site comments and the record in `nested.rs`. A protocol-shape
  mutation (`live()` lying about quiescence) IS caught by L6a — verified,
  reverted.

## Context

The **semantics are already specced and ratified**: SPEC 04 §5 (TXN-25..36)
defines the fork's read-only-child-of-a-write-txn — snapshot = the writer's
in-progress view (working roots + dirty set, TXN-26/27), arbitrarily many
concurrent `Send` children (TXN-28), **no reader-table slot** (TXN-25/32),
writer quiescence while any child lives (TXN-29/30, **D-005 APPROVED**), child
must not outlive the parent (TXN-31/33). Nested *write* txns are unsupported
(TXN-40, **D-003 APPROVED**). This ADR decides only the **implementation
strategy**: the child type's shape, how it reads, the `Send` story, the
`child_count` mechanics, and the oracle/test plan.

Forces:

- **The 6 call sites** (SPEC 00 row 16 / Findings §A; verified in the
  scratchpad clones): milli ×5 (`words_prefix_docids.rs` ×2,
  `facet_bulk.rs`, `indexer/mod.rs`, `upgrade/v1_32.rs`), hannoy ×1
  (`parallel.rs::FrozenReader`). All share one pattern: **open
  N = rayon_threads(+1) children once per batch, fan them out to workers
  (into_par_iter / a crossbeam channel + thread_local pool), read while the
  writer is paused, join, resume the writer**. hannoy even takes
  `&'t mut RwTxn` and holds the pool for `'t`. Children are *moved* to worker
  threads and may be *dropped* on worker threads.
- **heed's shape** (heed 0.22.1, `txn.rs`):
  `RwTxn::nested_read_txn<'a>(&'a self) -> Result<RoTxn<'a, WithoutTls>>` and
  `Env::nested_read_txn<'p>(&'p self, parent: &'p RwTxn) -> Result<RoTxn<'p, WithoutTls>>`
  (the Env form delegates to the RwTxn form). The returned child is the
  ordinary `RoTxn` type, covariant in its lifetime, and
  `unsafe impl Send for RoTxn<'_, WithoutTls>`. Under the hood the child holds
  **no Rust pointer into the parent** — only a `NonNull<MDB_txn>` (the parent
  link lives inside C) plus the env handle; the `'a` lifetime is a pure
  compile-time constraint. So heed gets `Send` via `unsafe impl` over an
  FFI pointer.
- **The fork** (`mdb.master.nested-rtxns`, ITS#10395): `mdb_txn_begin(env,
  parent_wtxn, MDB_RDONLY)`; the child reads the parent's dirty pages
  **zero-copy** (it walks the parent's `mt_dbs` roots and dirty list). It
  does *not* enforce writer quiescence; Meilisearch pauses the writer by
  convention. ZeroDB enforces it (D-005).
- **libmdbx prior art**: `mdbx_txn_begin(parent, …)` supports nested **write**
  txns (shadow dirty-lists); a parented **read** txn that sees the parent's
  uncommitted state does not exist in libmdbx. So the fork is the only prior
  art for this exact feature; libmdbx is prior art only for the
  "ops-on-parent-with-live-child ⇒ `MDB_BAD_TXN`" error convention (stock
  LMDB likewise: `MDB_BAD_TXN` = "transaction must abort, has a child, or is
  invalid").
- **zerodb-core is `#![forbid(unsafe_code)]`** (CLAUDE.md unsafe policy —
  the reader table is the sanctioned core area; nothing is sanctioned for a
  nested module). heed's raw-pointer trick is therefore not available in-core
  without a policy amendment.
- **Current code seams** (M1.4/M1.8): all reads go through
  `TxnRead` (`source() -> Source<'_>`, `main_record`, `free_record`,
  `record_for`, `page_size`) — the entire `Database` read API + `RoRange`
  iterators are generic over `T: TxnRead` (ADR-0004 D2, built for this
  milestone). `RwTxn`'s dirty store is `HashMap<pgno, Box<[u8]>>` with
  **stable frames** (TXN-41/44). `commit_pipeline` has an explicit C0 comment
  slot for the TXN-33 assert. The oracle already models
  `BeginNestedRo`/`EndNestedRo` (driver `TxnState::RwNested`,
  `Skip::WriteBlockedByNested`, `Skip::NoWriteTxnForNested`/`NoNestedToEnd`),
  drives the real fork through `LmdbEngine::Active::RwNested`, and gates the
  ops off in `ZerodbEngine::implements`.
- **Empirical fact (probed 2026-07-16, transient compile test)**:
  `zerodb_core::RwTxn<'static>` is **auto-`Sync` today**. Every field is
  `Sync` (`&Env`, `MutexGuard<'env, ()>` — `Sync` though `!Send` —, `&[u8]`,
  `Arc<Snapshot>`, `DirtyStore = HashMap`, `Vec`/`BTreeMap`/`HashSet`,
  `DBRecord`, plain `bool`/`u64`/`Option<u64>`); there is **no interior
  mutability on any `&self` path** (`oldest_cache` is a plain field mutated
  only under `&mut`). This is what makes a fully safe `Send` child possible.

## Decision points and options

### D1 — The child type shape and its `Send` story

The prompt-level question: heed gets `Send` from an `unsafe impl` over a raw
pointer; zerodb-core cannot. Walk the auto-traits honestly:
`&'p RwTxn<'env>: Send` ⇔ `RwTxn<'env>: Sync`. Probed: **it is**, and it is
*semantically* sound to keep it so — concurrent `&RwTxn` use is concurrent
read-only access to stable dirty frames, `Copy` records, and the mapped bytes;
the only shared-mutable state this milestone adds is the `child_count`
counter, which is an `AtomicUsize` (itself `Sync`). So the borrow-based design
is `Send` **by compiler derivation, with zero `unsafe` and zero copies**.

#### Option A — safe borrow-based child: `NestedRoTxn<'p>` holding `&'p RwTxn` (recommended)

```rust
pub struct NestedRoTxn<'p> {
    parent: &'p RwTxn<'p>,   // covariant squash of &'p RwTxn<'env>
}
impl TxnRead for NestedRoTxn<'_> { /* delegate all five methods to parent */ }
impl Drop for NestedRoTxn<'_> { /* parent.child_count.fetch_sub(1, Release) */ }
// Send: DERIVED by the compiler from RwTxn: Sync. No unsafe impl anywhere.
```

- `RwTxn::nested_read_txn(&self) -> Result<NestedRoTxn<'_>>` bumps
  `child_count` and returns the child. `Env::nested_read_txn<'p>(&'p self,
  parent: &'p RwTxn) -> Result<NestedRoTxn<'p>>` delegates (heed shape).
- Reads: the child's `TxnRead::source()` returns the parent's
  `Source::Writer { dirty, bytes }` — dirty frames first, then the map,
  byte-identical to the writer's own view (TXN-27/38). The whole M1.3–M1.6
  read API (get, cursors, ranges, prefix iters, named DBs via `record_for`,
  `stat`, overflow runs) works on the child **unchanged**, because it is all
  generic over `TxnRead`.
- **Zero-copy** (matches the fork), **zero unsafe**, ~40 lines of code.
- Compile-time quiescence (TXN-30) is the borrow checker verbatim: the child
  holds a real shared borrow.
- Pros: simplest possible; named-DB records, the open-table working records,
  and the free DB come along for free through delegation; covariance in `'p`
  falls out of `&'p RwTxn<'p>` (heed documents the same covariance for its
  `RoTxn`).
- Cons: **couples child `Send` to `RwTxn: Sync`** — a future field with
  interior mutability (`Cell`, `RefCell`) silently breaks `NestedRoTxn: Send`
  and consumers' builds. Guard: permanent compile-time assertions
  (`assert_sync::<RwTxn>()`, `assert_send::<NestedRoTxn>()`) in zerodb-core
  tests, mirroring the M1.8 `RoTxn: Send` assertion, plus a comment on
  `RwTxn` stating the contract ("no interior mutability on `&self` paths;
  `NestedRoTxn: Send` depends on it").
- ARM implications: none beyond the counter (D3) — reads are data-race-free
  by the borrow rules; the cross-thread hand-off of the child is synchronized
  by whatever moves it (rayon/scoped-thread spawn/join edges).

#### Option B — freeze the dirty store into `Arc`: self-contained child, no `&RwTxn`

Analyzed seriously per the fan-out pattern (open N once per batch, fan out,
join, resume):

- Make `RwTxn.dirty` an `Arc<DirtyStore>` permanently. Mutating ops access it
  via `Arc::get_mut()` — O(1), succeeds exactly when no child holds a clone,
  which **is** the TXN-29 runtime guard for free (`None` ⇒ children live ⇒
  return the misuse error). `nested_read_txn(&self)` clones the `Arc` (O(1),
  no data copy — the *store* is shared, not cloned) and snapshots the `Copy`
  roots (`main_db`, `free_db`, per-named-DB records resolved lazily through
  the frozen store) plus `bytes: &'env [u8]` and `psize`. A
  `PhantomData<&'p ()>` keeps the compile-time quiescence lifetime without
  dragging in `RwTxn: Sync`.
- Thaw is implicit: after the last child drops, the next `&mut` op's
  `Arc::get_mut()` succeeds again. No freeze/thaw ceremony, O(1) both ways,
  zero copies. (The prompt's "freeze at first-child-open, thaw on resume" —
  making the store `Arc` *only* while children live — is strictly worse: the
  freeze must happen through `&self`, which forces a `Mutex`/`OnceCell`
  around the store and taxes every page lookup on the writer's own hot path.)
- Pros: child `Send`/`Sync` is trivially self-evident (owns `Arc` + `&[u8]` +
  `Copy` records); immune to future `RwTxn` field changes.
- Cons: every dirty-store access in the mutation path (~all of `rwtxn.rs`)
  goes through a `get_mut` seam; the child needs its own `record_for`
  resolution path (working named records must be snapshotted or re-resolved —
  a second code path to keep in parity with the writer's `open` table);
  `put_reserved`'s returned `&mut [u8]` already requires `&mut self`, fine,
  but the `Arc` indirection touches TXN-41's stability argument (still holds:
  `Arc` never moves the `HashMap`'s frames; document it). More moving parts
  for the same observable behavior.

#### Option C — heed-style raw pointer child (sanctioned `unsafe` in a new `zerodb-core::nested` module)

Child holds erased pointers to the dirty store + records; `unsafe impl Send`.
Requires a one-line CLAUDE.md unsafe-policy amendment (the policy currently
sanctions only mmap/page-casting, the reader table, and oracle FFI).
Rejected: it buys nothing over Option A (A is already zero-copy and `Send`),
and it converts a compiler-checked guarantee into a human-audited one.

**Decision: Option A**, with Option B recorded as the sanctioned fallback if
`RwTxn: Sync` ever has to be given up (the static assertions make that moment
loud, and B is a mechanical refactor away). No CLAUDE.md amendment needed;
`unsafe` count in zerodb-core stays 0.

### D2 — What the child sees: delegate live vs snapshot roots at open

TXN-26 says the child's snapshot is the writer's working roots *at child-open
time*. Reason it through: mutation is impossible from the moment the **first**
child exists until the **last** child drops — compile-time (every live child
holds a shared `&wtxn` borrow, so no `&mut` method can be called; opening
*more* children needs only `&`, which coexists) and runtime (D3 counter). So
between any child's open and its drop, `main_db`/`free_db`/`open`/`dirty` are
**constant**, and every child opened in one paused window sees the identical
state. Therefore *snapshot-at-open* and *delegate-live-through-the-parent* are
observably equivalent — there is no window in which they could differ.

**Decision: delegate live** (the child's `TxnRead` forwards to the parent's).
It is less code, uses one resolution path for named DBs (no drift risk), and
the equivalence argument above goes into the module docs. `snapshot_txnid` =
`parent.txnid()` (TXN-26). No reader-table slot is claimed (TXN-25/32) — the
child never touches `readers.rs`.

### D3 — `child_count` mechanics and the resume point

- **Counter**: `child_count: AtomicUsize` on `RwTxn` (TXN-31 offers
  `Cell` vs `AtomicUsize`; `Cell` would destroy `RwTxn: Sync` — D1 forces the
  atomic, and the call sites really do drop children on worker threads:
  milli's `into_par_iter` consumes each child on a worker; hannoy's
  `ThreadLocal` pool drops wherever the `FrozenReader` drops).
- **Orderings** (explicit per CLAUDE.md rule; assume ARM/weak memory):
  - open: `fetch_add(1, Relaxed)` — creation happens on a thread holding
    `&RwTxn`; the child is published to its worker by the spawning
    mechanism's own happens-before edge (thread spawn / channel send), so the
    increment needs no ordering of its own.
  - child drop: `fetch_sub(1, Release)` — makes every read the child
    performed happen-before whoever observes the count reach 0.
  - every `&mut self` op, `commit`, `abort`-path debug check:
    `load(Acquire)` — pairs with the `Release` decrements, so all child
    activity happens-before the writer mutates or commits. (In the intended
    safe usage the join edge already provides this; the atomic pair makes the
    *runtime backstop itself* sound even for a hypothetical `unsafe`/FFI
    bypass, which is exactly the case it exists for.)
- **Runtime guard (TXN-29, D-005)**: every mutating `RwTxn` entry point
  (`put`, `put_with_flags`, `put_reserved`, `delete`, `delete_range`,
  `clear`, `drop_db`, `create_database`'s `create_named`, `put_current`,
  `del_current`, `rw_cursor`) checks `child_count == 0` on entry in **both
  debug and release** and returns **`MdbError::BadTxn`** otherwise — LMDB's
  own `MDB_BAD_TXN` is literally "transaction … has a child", so the error
  choice is parity-flavored, and the check slots next to the existing
  `guard_ok()` errored-txn check.
- **Resume/thaw point**: **RAII, implicit** — matching heed (children are
  `RoTxn` drops; the parent resumes when the borrow region ends). No explicit
  `resume()`/`thaw()` API. The last child's drop decrements to 0; the next
  `&mut` op simply passes the guard.
- **Commit/abort**: `commit(self)`/`abort(self)` are by-value, so live
  children make them unreachable at compile time; C0 gains the runtime
  `child_count == 0` check (TXN-33/58) returning `BadTxn` before the pipeline
  runs. `Drop` (abort path) cannot return an error: a nonzero count at drop
  is only reachable via `mem::forget` of a child (see risk R1 — nothing is
  aliased at that point), so it is `debug_assert!` + proceed.
- **Loom** (CLAUDE.md rule 4 — new lock-free interaction): add **L6** to the
  M1.8 suite: two loom threads each read a model "dirty" value through a
  child and `fetch_sub(1, Release)`; the writer thread `load(Acquire)`s until
  0 and then mutates the model value; assert no read observes the mutation.
  This is the TXN-29 backstop modeled without the borrow checker's help —
  precisely the world where only the atomics carry the safety.

### D4 — Interaction with the commit pipeline, abort, and the value-borrow contract

- **Commit**: C0 = `child_count == 0` assert (the comment slot at the top of
  `commit_pipeline` becomes real code). Everything downstream (C1a catalog
  flush, C1 freelist_save, C2 writes) is unreachable while children exist.
- **Abort**: same guard shape; by-value + borrow rules make it compile-time,
  `debug_assert` at drop for the forget case.
- **Value-borrow contract**: a child read returns `&'child [u8]` into a dirty
  frame (or the map). Frames are stable (TXN-41: `Box<[u8]>` never
  reallocates; TXN-44: index growth moves only handles) and no frame can be
  removed/invalidated while the child lives, because every invalidation point
  is a `&mut self` op — blocked compile-time by the child's borrow and
  runtime by D3. Borrows derived *from* the child are bounded by the child's
  own lifetime (`'child ⊆ 'p`), so child reads **do not extend the TXN-39
  invalidation problem** — they narrow it: the frozen window has strictly
  fewer invalidation points (none) than the writer's own single-threaded
  window. The SPEC 04 §6.6 miri scenario 3 (nested-reader-reads-dirty)
  becomes implementable exactly as written.
- **GC**: no interaction — the child claims no slot (TXN-25/32); the gate
  expression `min(oldest live reader, writer_txnid − 1)` is unchanged, and no
  GC draw can run while children live (draws happen inside mutating ops).
- **WRITE_MAP (TXN-35, M1.10 forward-compat)**: nothing to do now —
  `Source::Writer` abstracts where dirty bytes live; when M1.10 adds a
  writemap variant the child inherits it through delegation.

### D5 — Oracle and test plan

1. **Flip the ops on**: `ZerodbEngine::implements` returns `true` for
   `BeginNestedRo`/`EndNestedRo`; mirror `LmdbEngine`'s
   `Active::RwNested { nested, wtxn }`. **Address-stability note**: unlike
   heed's child (raw C pointer, no Rust reference into `wtxn`), zerodb's
   child holds a real `&RwTxn`, so the parent must be **`Box`ed** before the
   `'static` lifetime-erasure transmute (`Active::Rw(Box<zerodb RwTxn>)` …)
   — the Box gives the referent a stable heap address across moves of the
   `Active` enum, same pattern as the engine's `Box<Env>`. The transmute is
   oracle-sanctioned unsafe with a `// SAFETY:` stating: Boxed parent (stable
   address), child dropped before parent (field/drop order), no `&mut` parent
   use while nested (enforced by `Active` state), both dropped before env.
2. **Differential sequences** (fuzz + deterministic `nested_read_differential.rs`):
   write-uncommitted → `BeginNestedRo` → `Get`/`Iter`/`PrefixIter`/`Len`
   through the child sees the uncommitted state (incl. values only reachable
   through dirty overflow runs, and named DBs created this txn) →
   `EndNestedRo` → more writes → nested again → `Commit` → fresh `BeginRo`
   verifies. **Kept as-is**: the driver's `WriteBlockedByNested` symmetric
   skip — the single-threaded op model classifies any write op in
   `TxnState::RwNested` as a skip *before either engine sees it*. The fork
   would technically allow the put (D-005: it does not enforce quiescence),
   but zerodb never will; classifying symmetrically keeps the divergence
   unobservable to the harness, exactly as D-005 (APPROVED) prescribes. Also
   verified: `LmdbEngine::write_txn` already returns the same skip in
   `RwNested`, so the two engines cannot drift on this.
3. **Op-model limitation, stated**: `TxnState::RwNested` models exactly
   **one** live child at a time (like M1.8's one-txn limitation). Multiple
   concurrent children + real parallelism are covered outside the op model by
   the fan-out replay (next item), the same pattern as
   `readers_full_differential.rs`.
4. **milli/hannoy fan-out replay** (`crates/zerodb/tests/nested_fanout.rs`,
   real threads via `std::thread::scope` — rayon is not a dependency and the
   scoped-join edge is the same): open wtxn → write a mixed workload (inline
   values + multi-page overflow values + a named DB) → open N=9 children →
   fan out to scoped threads, each does gets/prefix-iters/range walks of
   uncommitted state incl. dirty overflow values and asserts exact bytes →
   join → write more (children gone: must succeed) → second batch of children
   sees both generations → join → commit → fresh RoTxn sees the final state;
   also replay hannoy's pool shape (channel + per-thread child). A
   miri-friendly single-threaded variant of open-child-read-dirty lives in
   zerodb-core (TXN-49 scenario 3).
5. **D-003 test**: `Env::nested_write_txn(&self, &mut RwTxn)` is provided as
   an explicit, documented, always-erroring stub in the public `zerodb` crate
   (TXN-40 promises "a clean, documented unsupported error", and the
   acceptance criterion needs a callable surface); the test asserts the error
   and that the parent txn remains usable (no partial state, no poison).
   Error variant: see open question Q2.
6. **Guard tests**: runtime TXN-29 check (via the oracle transmute path or a
   `Box`-erased test double), commit-with-forgotten-child returns `BadTxn`,
   `mem::forget(child)` → writer permanently erroring but sound (R1).
7. **Static assertions**: `RwTxn: Sync`, `NestedRoTxn: Send`,
   `NestedRoTxn: !Send` never required — plus loom L6 (D3).

### D6 — heed-shape preview for M1.13

heed's actual signatures (checked in the scratchpad clone, heed 0.22.1):

```rust
impl<'p> RwTxn<'p> { pub fn nested_read_txn<'a>(&'a self) -> Result<RoTxn<'a, WithoutTls>> }
impl<T>  Env<T>    { pub fn nested_read_txn<'p>(&'p self, parent: &'p RwTxn) -> Result<RoTxn<'p, WithoutTls>> }
unsafe impl Send for RoTxn<'_, WithoutTls> {}
// RoTxn is covariant in its lifetime (heed pins this with a compile test).
// RwTxn::nested (write child) is pub(crate) — heed exposes NO public nested-write API.
```

Constraint set the adapter must mirror: child returned **as the plain
`RoTxn<'_, WithoutTls>` type** (not a distinct type — milli passes children to
code generic over `RoTxn`), `Send`, covariant, `&self` on the parent, both
entry points. Therefore the M1.13 `heed-zerodb` `RoTxn<'e, WithoutTls>` wraps
an internal enum `{ Plain(zerodb::RoTxn<'e>), Nested(zerodb::NestedRoTxn<'e>) }`
(both variants `Send`; covariance must be preserved — `NestedRoTxn<'p>`'s
covariance in `'p` is a design requirement pinned by a compile test, satisfied
by `&'p RwTxn<'p>` since every `RwTxn` field is covariant in `'env`). Core
deliberately does **not** contort its own type system to make
`NestedRoTxn == RoTxn`; the unification happens at the adapter boundary per
ADR-0003. `Database::get<T: TxnRead>` already accepts both natively.

## Decision

Summary of the six decisions:

1. **D1** — `NestedRoTxn<'p>` holds `&'p RwTxn<'p>`; `Send` is
   compiler-derived from `RwTxn: Sync` (probed true; pinned by static
   assertions). Zero unsafe, zero copies. Freeze-into-`Arc` (Option B) is the
   documented fallback; raw pointers (Option C) rejected.
2. **D2** — the child delegates `TxnRead` live to the parent; equivalence
   with snapshot-at-open proven by quiescence (no mutation window exists
   between first-open and last-drop). No reader slot.
3. **D3** — `child_count: AtomicUsize` on `RwTxn`; open `Relaxed` add, drop
   `Release` sub, every `&mut` op / commit `Acquire` load → `BadTxn` on
   nonzero (both build profiles). Resume is implicit RAII (heed parity). Loom
   L6 covers the counter as the sole safety carrier.
4. **D4** — C0 becomes the real TXN-33 check; child reads narrow (not extend)
   the TXN-39 borrow problem — frames stable per TXN-41, invalidation points
   unreachable while children live.
5. **D5** — oracle flips `BeginNestedRo`/`EndNestedRo` on (Boxed parent for
   the sanctioned transmute); `WriteBlockedByNested` stays a symmetric
   pre-engine skip per D-005; fan-out replay with real scoped threads covers
   what the single-child op model cannot; D-003 gets an explicit erroring
   `nested_write_txn` stub to test.
6. **D6** — core keeps a distinct `NestedRoTxn` type; the M1.13 adapter
   unifies it into heed's single covariant `Send` `RoTxn<'_, WithoutTls>`
   via an internal enum.

## Consequences

- Easier: the entire read API works on children with no new read code
  (ADR-0004 D2 pays off); GC, reader table, commit pipeline are untouched
  except one C0 check; Phase 3.8 `snapshot()` later reuses the same child
  shape with a different record source.
- Harder / new obligations: `RwTxn` acquires a **standing `Sync` contract**
  (no interior mutability on `&self` paths) enforced by compile-time asserts —
  any future cache field must be atomic or move behind `&mut`. The oracle's
  `Active` must Box the parent txn.
- Tests to add: loom L6; `nested_read_differential.rs`; fuzz flip-on;
  `nested_fanout.rs` (milli + hannoy patterns); miri TXN-49 scenario 3;
  D-003 stub test; TXN-29/33 guard tests; `Sync`/`Send`/covariance static
  assertions.
- SPEC updates in the implementation change (rule 3): TXN-31 — ratify the
  `AtomicUsize` choice (drop the `Cell` alternative; children drop on worker
  threads at real call sites); §5.1 — note delegate-live and its equivalence
  argument; TXN-40 — name the concrete erroring API surface once Q2 is
  decided.
- DIVERGENCES: none new expected. D-005 (APPROVED) is exercised for the first
  time; the symmetric-skip modeling keeps it unobservable, as ratified.

## Open questions for human review

1. **Q1 — `RwTxn: Sync` as a standing contract**: Option A makes this a
   load-bearing, permanently-asserted property of the write txn. Comfortable,
   or prefer Option B's self-contained child (slightly more machinery, no
   such coupling) despite the second named-DB resolution path?
2. **Q2 — D-003 error variant**: the erroring `nested_write_txn` stub needs a
   concrete error. Proposal: a new top-level `zerodb::Error::NestedWriteTxnsUnsupported`
   (documented, mapped by the M1.13 adapter to whatever heed taxonomy slot the
   1.14 gate needs — heed itself has **no** public nested-write API, so no
   heed-side parity constraint exists). Alternative: reuse `MdbError::BadTxn`.
   ADR recommends the dedicated variant (a misuse of an unsupported feature,
   not a txn-state error).
3. **Q3 — stub surface**: is providing `Env::nested_write_txn` *at all* (an
   API that exists only to error) acceptable, or should the D-003 acceptance
   test instead live purely at the heed-adapter boundary in M1.13 (where
   there is likewise nothing public to call)? The stub is the letter of
   TXN-40; dropping it would need a TXN-40 amendment.
4. **Q4 — `BadTxn` for the TXN-29 runtime guard**: LMDB uses `MDB_BAD_TXN`
   for parent-with-child ops (nested-write context), which makes it the
   parity-adjacent choice for our reads-frozen guard too. Confirm, or prefer
   a dedicated variant here as well (note: this path is unreachable from safe
   Rust, so its error identity is nearly unobservable).
5. **Q5 — loom L6 scope**: modeled with the counter as the *only*
   synchronization (the unsafe-bypass world). Sufficient, or should a second
   model include the scoped-join edge to document the intended-usage
   happens-before as well?
