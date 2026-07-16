# ADR-0006: MVCC reader table — implementation strategy (slot table, publish cell, oldest-reader, loom/stress plan)

- Status: Approved — 2026-07-16, via **session-lead review under Quentin's
  standing directive of 2026-07-16** (not a direct per-document sign-off; the
  standing directive delegates ratification of M1.8 decisions to session-lead
  review, and the spec-review verdict of the same day — "committable
  conditionally, protocol passes site-by-site on ARM" — is incorporated with
  its conditions applied below)
- Milestone: 1.8 (MVCC reader table and concurrency)
- Date: 2026-07-16

> **Ratification record (2026-07-16, applied to the open questions below):**
> (1) Option B approved; the TXN-18 clarification amendment is **ratified**
> (recorded in SPEC 04 with the ratification line; the "never blocks on the
> writer's work" spirit statement is kept — a bounded pointer-swap critical
> section satisfies TXN-9, with LMDB-NOTLS read-open parity as the bar).
> Fallback options D/E stay recorded. (2) loom allowlist addition approved as
> `[target.'cfg(loom)'.dependencies]` + a `just loom` recipe. (3) Stress
> cadence: the ~5 s default runs in the normal suite; the minutes-long
> `ZERODB_STRESS_SECS` variant is a `just stress` recipe, documented for
> nightly CI and **mandatory before the 1.14 gate** (CI wiring out of scope
> here). (4) Per-txn oldest caching accepted as specced (TXN-22); no
> refresh-on-miss now. (5) Bench bar recorded as informational:
> read-txn-open cost vs heed/LMDB NOTLS is measured at the **1.14 bench
> gate**; a material regression there reopens the publish-cell decision. No
> bench is built in M1.8.

## Context

SPEC 04 §3/§4 (TXN-10..25) already *designs* the reader table in full: a
fixed array of `max_readers` cache-padded slots, each a single `AtomicU64`
encoding occupancy + pinned txnid via top-of-`u64` sentinels
(`RDR_FREE = u64::MAX`, `RDR_CLAIMED = u64::MAX − 1`); a lock-free
`compare_exchange(FREE → CLAIMED)` claim (TXN-15, `Acquire`/`Relaxed`);
`MDB_READERS_FULL` on exhaustion (TXN-16); the **SeqCst publish-and-verify
pin** against the env's `commit_point` atomic (TXN-17, the StoreLoad pairing
that only `SeqCst` closes on ARM); the immutable **published-snapshot object**
with the TXN-19 object-before-counter publish order at commit step C6; the
SeqCst **oldest-reader scan** with the two-case correctness proof as reviewed
and fixed in M0.4 (TXN-20); the GC gate and its caching rule (TXN-21/22); and
the slot lifecycle for the three read-txn shapes (TXN-23..25). **This ADR does
not redesign any of that.** It decides the implementation strategy: concrete
types, the one genuinely open structural question (what replaces the M1.4
`Mutex<Arc<Snapshot>>` publish cell, given `zerodb-core` is
`#![forbid(unsafe_code)]` and no ArcSwap-like crate is allowlisted), the
loom/stress test plan PLAN §1.8's acceptance requires, and the migration off
the M1.5 interim registry.

Current state (what gets replaced):

- `EnvInner.readers: Mutex<BTreeMap<u64, usize>>` — the M1.5 **interim reader
  registry** (ADR-0005 OQ1). `pin_reader()` clones the snapshot and bumps a
  refcount under the registry mutex; `deregister_reader()` in `RoTxn::drop`;
  `oldest_live_reader()` reads the map's min key. Deleted wholesale (SPEC 04
  TXN-21 says exactly this).
- `EnvInner.snapshot: Mutex<Arc<Snapshot>>` — the M1.4 **publish-cell
  placeholder** (ADR-0004 OQ1), already publishing in TXN-19 order via
  `publish_snapshot()` (swap object, then `commit_point.store(SeqCst)`).
- `EnvInner.commit_point: AtomicU64` — already SeqCst on all sites; stays.
- `RwTxn::oldest_reader()` (`rwtxn.rs:674`) — the single GC-gate consumer:
  `min(oldest_live_reader(), writer_txnid − 1)`. Its expression is unchanged;
  only the callee's implementation swaps.
- `max_readers` exists on `EnvOpenOptions` (default 126 per TXN-14) but is
  **not yet threaded** into `zerodb_core::env::open_with_backing`; `MdbError`
  has **no `ReadersFull` variant yet**. Both land here.

Prior art (clean-room, algorithm-level):

- **LMDB** (the fork): reader table in a shared-memory lock file; slots carry
  `mr_pid`/`mr_tid`/`mr_txnid`. Slot *binding* is not lock-free — under
  `MDB_NOTLS` (heed's `read_txn_without_tls`, the only mode Meilisearch uses)
  **every** `mdb_txn_begin(RDONLY)` takes the reader mutex (`me_rmutex`) to
  find/claim a slot, then publishes `mr_txnid` and drops the mutex; the
  steady-state read path and the writer's `mdb_find_oldest` scan are
  lock-free. So *"LMDB's read-txn open cost" — the parity bar — already
  includes one mutex acquisition per open.* The writer never blocks on
  readers; oldest is a plain scan whose safety leans on the writer re-checking
  under its own locks.
- **libmdbx**: same table shape, hardened — explicit atomic ops/barriers on
  slot fields, a cached-and-refreshed "oldest" the writer invalidates on
  demand, and thread-local slot caching (`rthc`) to skip the bind cost.
  Cross-process concerns (pid liveness, robust mutexes) dominate both designs;
  **D-001 (single-process) deletes that entire axis for ZeroDB** — no lock
  file, no pids, no reaping — which is why TXN-14's slot is a single
  `AtomicU64`.

Constraints from standing law: `zerodb-core` is `#![forbid(unsafe_code)]`
(M1.1 posture; zero unsafe blocks to date — CLAUDE.md *permits* unsafe in the
reader table but M1.1 chose stricter, and every milestone since has preserved
it). Dependency allowlist: memmap2, libc, thiserror, **crossbeam-utils**,
rand, proptest, arbitrary, criterion — `loom` and `arc-swap` are **not** on
it; PLAN §1.8 and PLAN §Testing nonetheless *require* loom for the reader
table, so this ADR must sanction it (precedent: ADR-0001's `libfuzzer-sys`
treatment — test-infrastructure-only scope, human-approved). All atomics need
explicit `Ordering` + per-site justification, ARM weak-memory assumed.

## Decisions with a single sensible option (restated, not relitigated)

These follow directly from SPEC 04; recorded here so the implementation diff
can cite one place.

### D1 — Slot structure: `Box<[CachePadded<AtomicU64>]>`, one word per slot

```rust
// zerodb-core/src/readers.rs (new module; no unsafe — forbid stays)
pub(crate) const RDR_FREE: u64 = u64::MAX;          // TXN-14
pub(crate) const RDR_CLAIMED: u64 = u64::MAX - 1;   // TXN-14

pub(crate) struct ReaderTable {
    slots: Box<[crossbeam_utils::CachePadded<AtomicU64>]>, // len = max_readers, fixed at open
}
```

- **Single `AtomicU64`, not separate state+txnid fields.** TXN-14 pins this,
  and the ABA story seals it: every observation of a slot is atomic — a
  two-field design (`AtomicU8` state + `AtomicU64` txnid) admits torn
  observations (state = pinned, txnid = stale from the previous occupant),
  which would need its own inter-field ordering protocol and would re-open the
  exact class of bug the M0.4 review fixed. The claim CAS
  (`FREE → CLAIMED`) has **no ABA hazard**: unlike a lock-free stack pop, no
  thread holds a stale expectation derived from an earlier read — the compare
  value `RDR_FREE` means "free *now*", and ownership is conferred by the
  successful exchange itself, not by prior history. The only history-sensitive
  consumer is the writer's scan, and its safety proof (TXN-20) is a case
  analysis on the *current* value (real txnid / `CLAIMED` / `FREE`), never on
  how the slot got there. Slot reuse (A releases, B claims the same slot) is
  covered by the same proof: B can only publish a txnid `≥` the current commit
  point, never resurrect A's older pin.
- **`CachePadded`** (crossbeam-utils, allowlisted): 128-byte alignment on
  aarch64, satisfying TXN-14's ≥ 64-byte padding — a pinning reader writes
  only its own line; the writer's scan doesn't false-share with it.
- Allocated once at env open from `max_readers` (`EnvOpenOptions`, default
  126 = LMDB `DEFAULT_READERS` parity), never resized (TXN-14). 1024 slots
  (Meilisearch's setting) = 128 KiB per env. Every slot initialized
  `RDR_FREE`.

### D2 — Claim: scan-from-0 with a Relaxed pre-filter; no early-exit bound

Per TXN-15: for `i in 0..max_readers`, `Relaxed`-load the slot and skip if
`!= RDR_FREE` (pure optimization — the CAS is the authority), else
`compare_exchange(RDR_FREE, RDR_CLAIMED, Acquire, Relaxed)`; first success
owns the slot. Full scan with no success → `MdbError::ReadersFull` (TXN-16,
new error variant, heed-taxonomy Debug rendering).

- Rejected: a claim *hint* (rotating start index) — the contention it would
  relieve (many simultaneous claims CAS-colliding on slot 0) self-disperses,
  because each loser advances to the next slot; not worth the state.
- Rejected: a high-water-mark atomic to bound the **writer's** scan. A stale
  (Relaxed) HWM read could hide an *already-pinned* reader in a
  just-claimed high slot — that is exactly the missed-pin bug, and making the
  HWM SeqCst-correct buys nothing over scanning all slots (1024 `ldar` loads
  on aarch64, once per allocation attempt at most, per TXN-22). The writer
  scans the whole table, always. Dumb and provable.

### D3 — Pin, release, oldest-scan: transcribe TXN-17 / TXN-18a / TXN-20 verbatim

All orderings exactly as specced, each site carrying a `// TXN-nn:` comment
with the justification (`SeqCst` on the pin store + both `commit_point` loads
and on every scan load — the StoreLoad pairings; `Release` on the
`RDR_FREE` release store, paired with the next claimer's `Acquire` CAS;
the stale-read-is-conservative argument for release-vs-scan is TXN-20's
closing paragraph and gets quoted at the scan site). No deviation, no
"equivalent" fence reformulation — the spec pins SeqCst *accesses* and the
loom suite (D7) locks them in.

`oldest_live_reader()` keeps its signature (`Option<u64>`: `None` when no
slot holds a real txnid) so `RwTxn::oldest_reader()` — the only consumer —
is textually unchanged; the `min(reader, writer_txnid − 1)` fold stays in
`rwtxn.rs` where TXN-20's `oldest = writer_txnid − 1` seed lives today.

## Options — the publish cell (the real decision)

The M1.4 placeholder is `Mutex<Arc<Snapshot>>`. TXN-18 as written says the
cell is "an atomically-swappable `Arc<Snapshot>` (an ArcSwap-style cell …
living in the reader-table module where `unsafe` is sanctioned)" and that
readers clone it "without taking any lock (never blocking, TXN-9)". The
constraint to weigh everything against is **TXN-9's actual guarantee**:
*readers never block on the write transaction; the writer never blocks on
readers* — with LMDB's own read-open cost (one `me_rmutex` acquisition under
NOTLS, see Context) as the parity bar.

### Option A — Hand-rolled `AtomicPtr` Arc cell (unsafe in zerodb-core)

A lock-free swap cell over the `Arc`'s raw pointer, in a module that relaxes
`forbid(unsafe_code)` to `deny` + local `allow`.

- Pros: literally what TXN-18 describes; zero reader blocking of any kind.
- Cons: **this is the hardest unsafe in the whole project.** A naive
  `AtomicPtr` load-then-`Arc::clone` races with the writer swapping and
  dropping the last reference between the reader's pointer load and its
  refcount increment — use-after-free. Doing it correctly requires hazard
  pointers, epoch reclamation, or arc-swap's split-refcount debt machinery;
  none of it is loom-trivial and all of it is exactly the "prefer safe code
  even at minor cost" territory. It also breaks the M1.1
  `forbid(unsafe_code)` posture that five milestones have preserved, for a
  cell whose contention window is a pointer swap once per commit.
- Variant (leak the history — swap to a fresh `&'static` leaked snapshot,
  never free): removes the reclamation race but leaks ~112 bytes + allocator
  overhead per commit, unbounded over a long-lived Meilisearch process.
  Rejected.
- ARM/crash-safety: no on-disk impact; the memory-ordering surface would grow
  (pointer publish ordering *in addition to* TXN-17/19/20).

### Option B — Keep `Mutex<Arc<Snapshot>>`; the pin protocol carries all safety (recommended)

The insight: **TXN-17/19/20's correctness never rested on the cell being
lock-free.** The load-bearing StoreLoad pairing is entirely between the slot
stores and the `commit_point` atomic — plain safe atomics. The cell only
manages the `Arc`'s lifetime; the mutex critical sections are all O(1)
pointer operations (writer: one swap per commit at C6; reader: one clone per
`read_txn` open), never held across I/O, allocation of the snapshot (built
before locking), or any tree work.

Reader pin path (TXN-10 order):

1. Claim slot (D2, lock-free).
2. TXN-17 publish-and-verify loop against `commit_point` — **lock-free**, no
   mutex inside the loop.
3. Lock the cell, clone the `Arc`, unlock. If `snap.txnid > t` (a commit
   landed between verify and clone), adopt the newer object and re-store its
   txnid into the slot — exactly TXN-17's tail; monotone, still validly
   pinned. Invariant enforced (debug_assert + loom): **`slot value ≤ cloned
   snapshot txnid`** — pinning *older* than what you read is conservative and
   safe; the reverse would let GC reclaim pages the cloned roots still
   reference.

Why the clone can never see roots *older* than the verified `t`: the writer's
swap (inside its critical section) is sequenced-before its
`commit_point.store(t, SeqCst)`; a reader whose load reads `t` therefore
happens-after the swap, and its subsequent lock of the same mutex is ordered
after the writer's unlock — it observes the swapped (or newer) `Arc`. The
TXN-19 object-before-counter order is what makes this a one-way check.

- Pros: **zero unsafe** — `forbid(unsafe_code)` survives M1.8 intact; the
  entire novel-correctness surface is loom-able safe atomics plus
  `loom::sync::Mutex`; `publish_snapshot()` (commit C6) is **byte-for-byte
  unchanged** from the M1.4-audited pipeline; smallest possible diff.
- Cons / honest accounting on TXN-9: a reader *can* block — on another
  reader's clone, or on the writer's C6 swap — for the duration of a pointer
  op. Is a block a block? Strictly yes: under pathological contention on ARM
  a futex sleep is possible. But (a) the writer holds the cell exactly once
  per commit for ~2 pointer writes — a reader can never wait on anything
  proportional to the write *transaction*, which is what TXN-9 protects
  consumers from; (b) the parity bar — LMDB under NOTLS — takes a full
  pthread mutex on **every** read-txn open, so Option B's open cost
  (one CAS + two SeqCst loads + one short mutex + one `Arc` bump) is at or
  below what Meilisearch pays today; (c) the writer **never** blocks on
  readers (it takes the same cell only at C6, readers hold it for a clone —
  bounded — and `oldest_reader()` touches no lock at all). **This requires a
  TXN-18 clarification amendment** (text below) because TXN-18's letter says
  "without taking any lock"; the amendment scopes the guarantee to its intent
  (never block on the write transaction; cell critical sections must be O(1)
  pointer ops) rather than wait-freedom. Flagged for ratification with this
  ADR.
- Quantification obligation: no performance claim without a criterion diff
  (CLAUDE.md). The milestone adds a `read_txn_open` bench (zerodb vs heed/
  the fork through the oracle's dependency) so the "at or below the bar"
  argument is measured, not asserted. Non-blocking for acceptance; if the
  bench shows the cell dominating open cost, the escape hatch is Option D
  behind the same `pin_reader()` signature — the protocol doesn't change.

### Option C — `crossbeam::AtomicCell<Snapshot>`

`Snapshot` is 112 bytes and `Copy`, so `AtomicCell` falls back to its global
spinlock-array path (not lock-free above word size). Strictly worse than B:
still a lock (a spinlock, worse under contention than a parking mutex), plus
a hidden *global* lock shared across unrelated cells, minus the `Arc`
(readers would copy 112 bytes — fine — but the cell's lock is invisible to
loom and to reviewers). Rejected.

### Option D — Seqlock over `(txnid, last_pg, main_db, free_db)` in plain atomics

Writer: bump a sequence atomic to odd, `Relaxed`-store the 14 `AtomicU64`
fields, bump to even (Release). Reader: read seq (Acquire), read fields,
re-read seq; retry on odd/changed. Fully safe Rust, truly lock-free reads
(retries only overlap the writer's O(1) publish window).

- Pros: no mutex anywhere on the read path; no unsafe; loom-able.
- Cons: explodes `Snapshot` into per-field atomics — every future field
  (Phase 2 catalog caching, M1.9 interactions) must be hand-threaded through
  the seqlock discipline, a standing bug invitation; readers get the snapshot
  by value instead of `Arc`, changing `RoTxn`/`TxnRead` plumbing; and it
  diverges from TXN-18's "immutable object + Arc clone" model *more* than
  Option B does (B keeps the object model exactly and relaxes only "no
  lock"). A reasonable fallback if the human rejects the TXN-18 amendment but
  still wants zero deps and zero unsafe.

### Option E — Sanction the `arc-swap` crate

Precedent exists (memmap2 = outsourced, widely-audited unsafe). But: a new
runtime dependency in the engine core for a window one mutex already covers;
its lock-free guarantees exceed what TXN-9 needs; its internals are the
Option A machinery we'd be trusting rather than avoiding; and loom cannot see
through it (arc-swap has no loom integration), so the *pin protocol* tests
would model the cell as an abstract atomic anyway. Rejected for Phase 1;
revisit only if the D8 bench falsifies Option B's cost argument.

## Decision

**Option B** for the publish cell, plus D1–D3 as restated. Concretely:

1. New `zerodb-core/src/readers.rs`: `ReaderTable` (D1), claim (D2), pin
   loop (TXN-17), release (TXN-18a), `oldest()` scan (TXN-20) — all safe
   code, all orderings SeqCst/Acquire/Release exactly as specced with
   per-site `// TXN-nn` justification comments, ARM-first reasoning.
2. `EnvInner`: `readers: Mutex<BTreeMap<u64, usize>>` **deleted**; add
   `reader_table: ReaderTable`. `snapshot: Mutex<Arc<Snapshot>>` and
   `commit_point: AtomicU64` stay as-is; `publish_snapshot()` unchanged.
3. `pin_reader()` becomes `pin_reader() -> Result<(Arc<Snapshot>, u32), Error>`
   (slot index out; `ReadersFull` in); `deregister_reader(txnid)` becomes
   `release_reader(slot: u32)`; `oldest_live_reader()` keeps its
   signature/semantics (scan-backed). `RoTxn` gains `slot: u32`;
   `Env::read_txn()` maps the new error (its `Result` shape anticipated this
   since M1.3). `rwtxn.rs` diff ≈ zero lines beyond comments.
4. `MdbError::ReadersFull` added (heed-parity Debug rendering, §8.1 table);
   `max_readers: u32` threaded through `open_with_backing` (callers:
   `zerodb/src/lib.rs`, `mem_env`, `value_borrow_contract.rs`).
5. `Env::static_read_txn()` (TXN-24): `RoTxn` generalizes its env access to
   an owned-or-borrowed handle (`enum: Borrowed(&'e Env) | Owned(Env)`), so
   `static_read_txn()` returns `RoTxn<'static>` holding an `Env` clone —
   heed's exact shape. Drop order inside `RoTxn::drop`: release the slot
   *first*, then drop the handle — the owned clone keeps `EnvInner` (map,
   table) alive until after the release store, and blocks close/
   `EnvClosingEvent` until dropped (TXN-52/53, refcount does this for free).
6. Oldest-reader consumption (TXN-22): cached **per write txn** — computed on
   the first `gc_reclaim` call, stored in the `RwTxn`, reused for the txn's
   remaining draws. Sanctioned by TXN-22's own argument (a cache is only ever
   *more* conservative — a reader releasing mid-txn is simply not
   reclaimed-against this txn) and by the "must recompute in a fresh write
   txn" rule (the cache dies with the `RwTxn`). An opportunistic
   refresh-when-a-draw-is-gated (libmdbx's cached-oldest pattern) is noted as
   a Phase-3-adjacent refinement, **not** built now — churn parity already
   passes without it and every branch here is GC-gate risk.
7. Leaked readers (`mem::forget(ro_txn)` — safe Rust, cannot be prevented):
   the slot pins its txnid forever, GC stalls behind it, the file grows.
   Same failure mode as LMDB's stale reader, minus the cross-process reap
   (D-001: nothing to reap — the "owner" provably still is this process).
   **Documented stall**, rustdoc'd on `read_txn`/`static_read_txn`; reader
   introspection is Phase 2.2 and no reaping path is added.
8. WithoutTls/Send: slots bind to the txn object (the `u32` index is a plain
   field), never a thread — `RoTxn: Send` holds structurally (TXN-13);
   release from any thread is the TXN-18a Release store. WithTls is a shim
   adding no semantics (SPEC 04 §0); nothing here is thread-identity-aware.

### SPEC 04 amendment required (ratify with this ADR)

**TXN-18, bullet 2** currently: readers "load-and-clone the `Arc` **without
taking any lock** (never blocking, TXN-9)". Amend to:

> Readers load-and-clone the `Arc` **without ever blocking on the write
> transaction** (TXN-9). The cell may be lock-free or a mutex whose critical
> sections are all bounded O(1) pointer operations (the writer's single swap
> at C6; a reader's clone), never held across I/O, allocation, tree work, or
> any other writer step — so the worst reader wait is another thread's
> pointer op, independent of write-txn duration. (Implementation: ADR-0006,
> Option B; the M1.4 placeholder cell is thereby ratified as the M1.8 cell.)

And strike TXN-18's "(an ArcSwap-style cell: a lock-free atomic pointer …
living in the reader-table module where `unsafe` is sanctioned)"
parenthetical — no unsafe is used; the module note becomes false. TXN-9's
own text ("readers never take the write mutex and never block on it") is
already accurate — the cell mutex is not the write mutex — but gets a
cross-reference to the amended TXN-18. Per CLAUDE.md rule 3 the amendment
lands in the same change as the implementation, after human approval here.

### Dependency sanction: `loom` (test infrastructure only)

`loom = "0.7"` is added to `zerodb-core` as
`[target.'cfg(loom)'.dependencies]` — compiled **only** under
`RUSTFLAGS="--cfg loom"`, never in a normal, miri, or release build (the
tokio pattern; a plain dev-dependency would compile it on every `cargo
test`). Scope mirrors ADR-0001's `libfuzzer-sys` sanction: test
infrastructure only, `zerodb-core` only, Phases 1–3; it must never appear in
a non-`cfg(loom)` dependency edge. `Cargo.toml` gains the
`unexpected_cfgs`/`check-cfg = ['cfg(loom)']` lint allowance; a `just loom`
recipe runs the suite (`--release`, loom exploration is slow).

Code structure for loom-ability: `readers.rs` (and the cell's mutex use in
`pin_reader`) import atomics/`Mutex` through a crate-internal shim —
`crate::sync` re-exporting `loom::sync::*` under `cfg(loom)` and
`std::sync::*` otherwise — so the **same source** runs natively, under miri,
and under loom. The shim is confined to `readers.rs` + the `EnvInner` fields
it owns; the rest of the crate keeps plain `std::sync` (loom tests drive the
table + cell + commit-point protocol in isolation with a model harness, not
a whole env — `EnvInner` itself is not loom-instrumented).

## Test plan

### Loom suite (acceptance gate 1) — `zerodb-core`, `cfg(loom)`

Model = `ReaderTable` (2–3 slots) + `commit_point` + a cell (loom `Mutex`
around an `Arc<(u64, marker)>`), 2–3 threads (loom's tractable bound):

| # | Interleaving | Asserted invariant |
|---|--------------|--------------------|
| L1 | 2 readers race claims on 1- and 2-slot tables | exactly one CAS wins per slot; loser advances or gets `ReadersFull`; no double-ownership; released slot reclaimable |
| L2 | 1 reader pins (full TXN-17 loop + clone + adopt-newer tail) vs 1 writer publishing (swap → `commit_point.store`) then scanning | **reader's final pinned `t` ≥ writer's computed `oldest`** — the machine-checked form of TXN-20's two-case proof (visible pin is folded in; mid-pin reader lands `≥ N−1`); and `slot ≤ cloned.txnid` |
| L3 | reader releases (`Release` store `FREE`) racing the writer's SeqCst scan | `oldest ≤ N−1` always; a stale non-FREE read only *lowers* `oldest` (conservative, TXN-20 closing argument); never raises it |
| L4 | writer publishes mid-pin-loop (the retry path) | loop terminates; reader adopts the newer object; `slot ≤ cloned.txnid` never violated (catches a publish-order inversion: counter-before-object makes L4 fail) |
| L5 | slot reuse: A pins, releases; B claims the same slot and pins; writer scans throughout | B's pin ≥ current commit point (no resurrection of A's older txnid); `oldest` correct at every scan |

Mutation-check the suite once during development (flip one SeqCst to
Release/Acquire per site; loom must fail) so the tests demonstrably encode
the orderings, then revert — never committed weakened. Honest limitation,
stated: loom's SeqCst modeling has known-incomplete corners; the guard
against a residual false-negative is gate 2 running on real weak-memory
hardware (dev machines are aarch64; CI target is Graviton).

*Outcome (M1.8 review; full record in `readers.rs`): the publish-order
inversion is caught (L2/L4/L5 fail under it). The SeqCst-access weakenings
are **not loom-detectable**: the with-mutex models make them genuinely safe
(HB chains via the Option-B cell mutex), and the mutex-free L2b model built
to detect them was withdrawn because loom 0.7 fails the correct all-SeqCst
code — a minimal all-SeqCst store-buffer litmus under loom explores the
both-miss outcome C++20/AArch64 forbid. See R1 for the resulting guards and
the Option-D/E migration precondition.*

### Stress test (acceptance gate 2) — `crates/zerodb/tests/reader_stress.rs`

Real-file env. N reader threads (8 default) in a loop: open `read_txn` (mix
of plain and `static_read_txn`), hold across several writer commits (forces
the gate to matter), walk/sample the pinned snapshot and verify **internal
consistency** (every key readable, values intact, `check_image`-style
invariant walk on the pinned root — a reclaim-under-reader manifests as a
corrupt walk); 1 writer thread committing GC-heavy churn
(insert/delete/overwrite, overflow values mixed in) with periodic
`ReadersFull`-boundary probes. Duration: ~5 s in a default `cargo test`
run; `ZERODB_STRESS_SECS` env var scales it to the minutes-long acceptance
run (`just stress-readers` recipe documents it) — env-var over feature-gate
so the same binary serves both and CI can dial it.

**Debug shadow tracking** (acceptance: "GC never reclaims a reachable
page"): under `cfg(debug_assertions)`, at the moment `gc_reclaim` drains a
page freed by txn `F`, re-scan the reader table and `assert!(F ≤ min(live
pins))` — catching a gate violation at the reclaim site, not via downstream
corruption; plus a shadow map `pgno → F` asserting no page is reclaimed
twice per txn (extends the existing `reclaimed: HashSet` / INV-24 assert).
Reader-side corruption walks (above) remain the end-to-end oracle for
anything the point-assert misses. The M1.5 `gc_reclaim.rs` reader-gate
corruption probe and release-reuse tests are kept and now exercise the real
table; the oracle differential + `diff_ops` fuzz (with the size tripwire)
re-run unchanged as regression.

Miri: `readers.rs` logic (claim/release/scan over the table, pin loop
against a stubbed commit-point) runs under plain `cargo miri test -p
zerodb-core` (not `cfg(loom)`), keeping the miri gate green end-to-end.

## Consequences

- Easier: M1.9 (nested readers explicitly take **no** slot, TXN-25 — the
  table needs no changes); Phase 2.2 reader introspection (iterate slots,
  render txnids); the GC gate gains its final form — ADR-0005's interim-
  registry caveats in `env.rs`/`rotxn.rs`/SPEC 04 TXN-21 all get their
  "replaced by M1.8" notes resolved in the same change.
- Harder / debt accepted: the cell mutex is a measured bet (D8 bench guards
  it; Option D is the pre-designed fallback with no protocol change);
  per-txn oldest caching holds reclamation back within very long write txns
  (bounded by the txn, same as LMDB's per-txn oldest; the known-weakness
  note in PLAN §1.5 already covers huge txns → Phase 3.1).
- SPEC updates in the implementation change (after approval): TXN-18
  amendment (text above); TXN-21 interim-registry paragraph retired; TXN-22
  annotated with the per-txn cache choice; SPEC 04 §11 loom row pointed at
  the suite.
- Risk register:

| # | Trap | Guard |
|---|------|-------|
| R1 | **Pin/scan StoreLoad race on ARM** — any of the four SeqCst sites (pin store, two pin loads, scan loads) quietly weakened during a refactor reintroduces the M0.4-reviewed missed-pin bug; invisible on x86 | **Loom cannot serve as the weakening detector** (established during M1.8 review): (i) in the faithful with-mutex models the weakenings are *genuinely safe* — the Option-B cell mutex forms happens-before chains (pin-completing clone-unlock → later publish/begin lock → scan) that keep old pins visible, and mid-pin readers land `≥ N − 1` (TXN-20 case 2); (ii) a mutex-free "L2b" fallback-world model was built (the TXN-17 loop factored as `ReaderTable::publish_and_verify` so it drives shipped code) and **rejected: loom 0.7 reports the Dekker violation for the correct all-SeqCst code**, and a minimal all-SeqCst store-buffer litmus under loom confirms it explores the both-miss outcome that C++20 (P0668) and AArch64 RCsc forbid — loom's SC support is fence-oriented (tokio-rs/loom#180 class) and does not implement the SC-access total-order read rule. Actual guards: SPEC 04 TXN-17/20 pin the orderings normatively; per-site `// TXN-nn SeqCst:` comments; the mutation-check record in `readers.rs`; the stress gate on real aarch64. **Standing precondition: any Option-D/E migration (lock-free cell) MUST bring its own StoreLoad verification** — an SC-fence reformulation loom can check, a different model checker, or hardware litmus runs — because the mutex HB chains that make the weakenings survivable today do not exist there. |
| R2 | **Slot-reuse / torn-observation confusion** — multi-field slot designs or a scan that reasons about slot *history* rather than current value | D1's single-word slot makes torn observation unrepresentable; TXN-20's proof is current-value-only and quoted at the scan site; loom L1/L5 |
| R3 | **Publish-order inversion at C6** — `commit_point` stored before the object swap lets a reader pin `t_new` over `t_old` roots (`slot > snap`, the unsafe direction) | single publish site (`publish_snapshot`, unchanged and already ordered); `slot ≤ cloned.txnid` debug_assert in `pin_reader`; loom L4 |
| R4 | **Release-path over-strengthening or over-weakening** — `Relaxed` release could reorder before the reader's last page read (dangling deref window); SeqCst release would mask the stale-read argument tests must exercise | TXN-18a `Release` + comment; L3 exercises the stale-read conservatism explicitly |
| R5 | **Leaked `RoTxn` stalls GC forever** (safe Rust, unpreventable) | documented stall (rustdoc + DIVERGENCES-adjacent note); stress test asserts the *bounded* case (held-across-commits readers) behaves; Phase 2.2 introspection is the observability answer |

## Open questions for human review

1. **The TXN-18 amendment (Option B).** Approve the Mutex-backed cell +
   amendment text? If you want the letter of "no lock on the reader path"
   kept instead, say which fallback: **Option D** (seqlock, zero deps, zero
   unsafe, more plumbing + a per-field discipline) or **Option E**
   (`arc-swap` dependency sanction). Option A (hand-rolled unsafe) is not
   recommended under any answer.
2. **loom sanction** as a `cfg(loom)`-gated dependency of `zerodb-core`
   (never compiled outside `just loom`) — approve the allowlist addition on
   the ADR-0001 precedent?
3. **Stress-duration mechanism**: env-var (`ZERODB_STRESS_SECS`) as decided,
   and should CI run the minutes-long variant on every push or nightly-only?
4. **Per-txn oldest cache** (Decision 6): accept, or require the
   refresh-on-gated-miss refinement now (more reclamation inside huge txns,
   at the cost of an extra code path through the GC gate)?
5. **Bench bar** (D8): is "read_txn open within parity of heed/fork on the
   same machine" the right non-blocking target, and should a regression on
   it re-open the cell decision automatically?
