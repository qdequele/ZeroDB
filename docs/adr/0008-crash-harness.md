# ADR-0008: Crash harness — fault-injection write backend, two-mechanism crash cycles, crash-harness binary

- Status: **Approved** — Quentin, 2026-07-16, standing directive, session-lead review
- Milestone: 1.11 (Recovery, torn writes, and crash consistency)
- Date: 2026-07-16

### Ratified answers to the open questions (Quentin, 2026-07-16)

1. **Cycle accounting:** each recovered-and-verified crash state counts toward
   the ≥10k (one image variant / one SIGKILL recovery = one cycle) — the
   verification work is per-image and that is the acceptance's spirit.
2. **Async SIGKILL stays in CI** — verification tolerates any legal recovery
   point, so flake risk is structurally zero; deterministic cycles remain the
   majority.
3. **Adversarial NO_SYNC floor approved as proposed** (no panic + designed
   taxonomy, distribution logged) — asserting more would invent guarantees
   REC-11 does not make.
4. **In-house ~20-line splitmix64-family PRNG approved** (bit-stable forever;
   CRC32C in-house precedent; no allowlist change).
5. **CI venue:** nightly `crash-test-full` on the Graviton runner alongside
   fuzz-long; `crash-test-quick` (200) joins the milestone gate. This
   milestone's local acceptance: quick gate + one large local run capped at
   ≤30 min wall (`--jobs`), reporting the achieved cycle count; if <10k
   locally, the 10k run becomes a recorded CI obligation in PROGRESS.
6. **80/20 image/SIGKILL split approved**, with a floor written into the
   full-run acceptance: **≥1k of the counted cycles must be mechanism-B
   SIGKILL recoveries.**

## Context

**PLAN §1.11 acceptance, restated.** A crash-injection harness with **two
mechanisms** — SIGKILL alone cannot tear a write (the OS page cache survives
process death; only power loss tears/reorders un-fsynced sectors):
(1) process kill — workload in a child process, SIGKILL at random points and
between the fsync barriers via the M1.4 hooks, reopen, verify; (2) a
fault-injection write backend in `zerodb-io` that simulates power loss by
tearing / reordering / dropping writes not yet covered by an fsync
(CrashMonkey/ALICE-style), producing disk images to reopen-and-verify. Verify
after every cycle: last committed txn fully visible, no partial txn, torn meta
rejected via CRC, check tool clean. Add an optional O_DIRECT-friendly write
sizing note. **Accept: ≥ 10k crash-recovery cycles clean in CI (parallelized),
across both mechanisms.**

**SPEC 06 rules this ADR implements, restated.** REC-17 (mechanism 1: kill at
random points and at each hook H0..H4; validates control-flow ordering only),
REC-18 (per-cycle obligations: 1 open-succeeds-or-designed-Invalid, 2 recovered
snapshot ∈ {N−1, N} for a crash during commit `N`, 3 check tool clean
[INV-1..27], 4 monotonic durability in default mode, weakened per-mode under
§3), REC-19 (mechanism 2: drop/reorder/tear un-fsynced writes; **the CRC
rejects only sub-sector meta tears; sector-aligned tears yield a CRC-valid
old-or-new meta resolved by txnid selection — the harness MUST NOT assert a
CRC failure for sector-aligned tears**), REC-20 (barrier model: durable writes
always present and intact; each pending write independently present-intact /
present-torn / absent, in any order; C3/C5 syncs are the barriers), REC-21
(≥10k cycles, randomized workloads, values 0 B–16 MB, across the §3
durability modes, each asserting its own obligation strength), REC-9..12 (the
durability-flag crash windows the harness must assert exactly — no more).

**Existing assets.** The commit pipeline is one function with always-compiled
`H0..H4` hooks (`CommitHook`/`HookPoint` on `EnvInner`, ADR-0004 D3/OQ5;
`Env::set_commit_hook`). M1.4's `crates/zerodb/tests/crash_smoke.rs` runs the
child-abort H0–H4 matrix; M1.5's `gc_reclaim.rs::gc_crash_matrix` extends it
over GC churn. `zerodb-core::env::Backing` is the injection seam: the engine
performs *all* commit I/O through `write_at_page(pgno, psize, data)` and
`sync(async_flush)`, reads through `bytes()`, and
`zerodb_core::env::open_with_backing` accepts any `Box<dyn Backing>` with a
synthetic path (it never touches the filesystem). `zerodb-io` provides
`MmapBacking` (pwrite mode) and `WriteMapBacking` (M1.10, msync mode) — both
behind the same trait, so one fault wrapper covers both write strategies. The
justfile already expects `cargo run -p zerodb-oracle --bin crash-harness --
--cycles {200|10000}`. The oracle crate owns the `Op` model, `driver::classify`
(the single op-validity authority), and `Arbitrary` decoding — the workload
generator reuses them.

**Prior art.** LMDB ships **no** crash-injection harness: its crash safety is
argued by design (double-buffered meta, ordered fsync) plus field history;
`mtest`/`mdb_chk`-style verification is offline only. libmdbx goes further:
its long stochastic suite (`mdbx_test` in fork/kill mode) kills a child at
random points and re-verifies with `mdbx_chk` — mechanism 1, but no
torn/reordered-write simulation (it relies on sector-atomicity arguments for
its meta). The academic tooling (ALICE [OSDI'14], CrashMonkey [OSDI'18])
established the model this ADR adopts for mechanism 2: record the write/sync
trace, then materialize *crash states* = durable prefix + any legal
subset/permutation/tear of the un-synced suffix, and check each state.
ZeroDB's mandatory meta CRC (SPEC 02 §3.3) makes our obligations *stronger*
than either C engine's, so we need the stronger (ALICE-style) mechanism they
lack.

**Constraints.** No new dependencies without an ADR (rand/proptest/arbitrary
already allowlisted). `unsafe` in `zerodb-io` is permitted (mmap/page
casting); `zerodb-core` stays `#![forbid(unsafe_code)]`. No new lock-free
interactions are introduced by this design (the fault backend is used by the
single writer thread; per-cycle state is thread-confined), so no new loom
models are required — this is stated as a checked property, not an omission.

---

## Decision 1 — Fault-injection backend design (`zerodb-io::fault`)

A `FaultBacking` implementing `Backing`, plus a shared handle
(`Arc<Mutex<FaultState>>`) the harness keeps to trigger simulated power cuts.

### State model (both options share this)

- `durable: Vec<u8>` — the disk image as of the last completed sync barrier,
  plus `durable_len: u64` (the durable file length; extension is itself
  fs-metadata that a crash can lose, REC-14).
- `pending: Vec<WriteRecord { offset: u64, data: Box<[u8]> }>` — every
  `write_at_page` since the last barrier, in issue order. Overflow runs stay
  one record (they are issued as one positioned write).
- `sync(async_flush = false)` → fold `pending` into `durable` in order,
  advance `durable_len`, clear `pending`. This is REC-20 verbatim: C3/C5 are
  the only pending→durable transitions.
- `sync(async_flush = true)` (`MAP_ASYNC`) → **not a barrier**: pending stays
  pending. This is the most adversarial sound model of `msync(MS_ASYNC)`
  (kernel writeback with no completion guarantee) and is what lets the harness
  *characterize* the MAP_ASYNC window (REC-11 “characterize — not
  guarantee-away”).

### Crash materialization (`crash(seed) -> Vec<DiskImage>` + `FaultPlan` per image)

Deterministic: `seed → FaultPlan` via a small in-house splitmix64/xoshiro256**
(≈20 lines, test-only) so plans are bit-stable across platforms, rand-crate
versions, and time — a saved `(seed, cycle)` pair reproduces the exact image
forever. (rand's `StdRng` is only stable per lockfile; `SmallRng` differs by
pointer width. Open question 4 offers the alternative.)

Each image = `durable` + a plan-selected subset of `pending`, where:

- **Decomposition.** Each pending write is decomposed into 512 B sector units
  (`SECTOR = 512`, independent of psize — a psize page is psize/512 sectors).
- **Per-sector choice.** Every pending sector independently takes: the durable
  value (absent), any of the values written to it in the barrier window
  (reorder), or the last value (present-intact). Per-sector independent choice
  **subsumes cross-page reordering**: for sectors written once it is
  drop/apply; for sectors written more than once, choosing an earlier value is
  exactly the “later write persisted before the crash, earlier after” — the
  ALICE reordering model without simulating queues.
- **Tears.** A plan additionally picks victim writes to tear:
  - **sector-aligned tear** — keep a prefix of the write's sectors, or a
    suffix (both directions, per the brief); the rest revert to durable
    content. On a meta write this yields a fully CRC-valid old-or-new slot —
    the **txnid-selection path** (REC-8/REC-19).
  - **sub-sector tear** — split at a byte offset *inside* a sector (both
    prefix-kept and suffix-kept), specifically including offsets inside the
    meta's sector 0 (bytes 0..512, which contain the whole CRC-covered region
    `[0,168)` + the CRC field). This is the **CRC-rejection path**.
- **Plan-distribution quotas** (hard-coded in the generator, asserted per
  batch): every batch of images from a cut that has a pending meta write must
  include ≥1 sub-sector meta tear, ≥1 sector-aligned meta tear keeping old,
  ≥1 keeping new; every batch from a cut with pending data pages must include
  ≥1 all-data-dropped image (the H1 “meta untouched, garbage data” row) and
  ≥1 reorder image. Without quotas, uniform sampling can starve the CRC path
  (REC-19's explicit warning).
- **File length.** Image length ∈ {`durable_len`, extended-to-highest-applied
  write}; one variant per batch truncates the extension entirely (REC-14: the
  recovered meta must then be `N−1`).
- The `FaultPlan` is serializable (Debug-dump) and saved with the image on a
  violation.

### Option A — pure in-memory live view (`Box<[u8]>` + raw-pointer writes)

`bytes()` serves a heap buffer that `write_at_page(&self)` mutates through a
raw pointer. Pros: zero filesystem traffic, fastest cycles. Cons: **one new
`unsafe` block** — `&self` mutation aliasing an outstanding `bytes()` borrow
is exactly the mmap aliasing problem, and would need the same TXN-62 SAFETY
argument as `MmapWritable` (writes only touch pages no committed snapshot
references), duplicated in a second place; miri cannot check it (it is real
aliasing UB by Rust's rules, tolerated only under the mmap-style argument).

### Option B — wrap a real mapped backing; journal on top (recommended)

`FaultBacking { inner: Box<dyn Backing>, state: Arc<Mutex<FaultState>> }`
where `inner` is a real `MmapBacking`/`WriteMapBacking` on a per-cycle temp
file. `bytes()`/`real_disk_size`/`try_clone_file` delegate to `inner` (the
live process view — identical semantics to production, where un-fsynced
writes are visible through the page cache). `write_at_page` journals into
`pending`, then delegates. `sync` folds the journal and delegates. Crash
images are materialized as owned `Vec<u8>`s from `durable` + plan — the inner
file is never used as the crash state, so host-FS behavior never leaks into
the fault model. Pros: **zero new `unsafe`** (reuses the one audited mmap
block); live-view semantics identical to production; the same wrapper covers
pwrite mode and WRITE_MAP mode (M1.10 made both go through
`write_at_page`+`sync`). Cons: a temp file per cycle and doubled write cost
(journal copy + real write) — irrelevant at harness scale; `durable` is a
map_size-sized Vec (bounded by choosing harness map_size 64–256 MiB, reused
across cycles per worker).

**ARM note:** the `Mutex<FaultState>` is plain lock-based; the backend adds no
atomics. The writer thread is the only writer; the harness reads state only
after the writer is quiescent (cut point) — no weak-memory reasoning beyond
the mutex.

**Crash-safety reasoning made explicit (rule 3):** the fault model *is* the
between-steps death model. For every pipeline write sequence
C1a/C1→C2→C3→C4→C5, the invariant per REC-20 is: no materialized image can
contain a durable meta `N` without durable `N`-data, because C3 folds the
data writes into `durable` before C4's meta write ever enters `pending`. If
the implementation ever reordered C3 after C4, the harness would materialize
a meta-without-data image and REC-18.3 would fail — that is the formal
tripwire this backend exists to arm.

## Decision 2 — Harness architecture: two mechanisms, one verifier

### Workload generator (shared)

Seeded generator producing `Op` sequences via the oracle's existing
`Arbitrary`-based decoding (`decode_ops` over PRNG bytes), gated through
`driver::classify` — the same single validity authority the differential uses
(guards risk 3). Coverage knobs per REC-21: GC churn (delete/clear-heavy
phases), overflow values 0 B–16 MB (map_size sized accordingly), named DBs
(catalog C1a coverage), aborts, and durability mode per cycle
(default / NO_META_SYNC / NO_SYNC / MAP_ASYNC / WRITE_MAP / WRITE_MAP+MAP_ASYNC,
REC-12). LMDB is **not** in the loop — crash cycles are zerodb-only (the fork
crashes differently by design; there is no oracle for torn writes), so the
harness's oracle is the model + check tool, not the C engine.

### Data-model oracle (shared verification, REC-18)

A pure shadow model (`BTreeMap<DbName, BTreeMap<Vec<u8>, Vec<u8>>>` per
committed txnid) that replays the deterministic op stream. Because the stream
is seed-deterministic, the expected state **for any txnid** is reconstructed
by replaying ops `1..=recovered_txnid` — no per-txnid snapshot storage, and
recovery to any `N−k` (relaxed modes) is checkable. Per cycle, after
crash+reopen:

1. Open succeeds, or fails `MdbError::Invalid` only where the plan corrupted
   2+ slots (only reachable under NO_SYNC multi-commit pending windows —
   see Decision 4); a single-torn-commit cycle MUST open (REC-3/REC-18.1).
2. Recovered txnid ∈ the mode's legal set (default: `{N−1, N}` per REC-6 row
   of the cut point; exact-per-hook when the cut is at a known hook).
3. `check::check_image` on the recovered image is clean (INV-1..27).
4. Full data comparison: iterate every DB of the recovered env and compare
   **bidirectionally** with the model at the recovered txnid (missing +
   extra + value bytes).
5. Monotonic durability (default mode): recovered txnid ≥ last
   acknowledged-committed txnid (a commit that returned `Ok` after C5 never
   disappears).

**Anti-drift guard (risk 3):** before every injected crash, the harness
asserts model == live env state at the last clean commit. Drift fails loudly
pre-crash instead of surfacing as a phantom post-crash violation.

### Mechanism A — in-process image cycles (fast path, REC-19/20)

One cycle: build a fresh env on a `FaultBacking` (unique synthetic registry
path — `open_with_backing` never touches the FS); run K seeded ops with
commits; pick a cut point — either a random op/commit boundary or a specific
`HookPoint` via a `CommitHook` that signals “capture here” (the hook does not
kill anything; it freezes the `(durable, pending)` pair mid-pipeline —
cheaper and more precise than dying); materialize V images (default V ≈ 16)
per the plan quotas; reopen **each** image via a read-only Vec backing under a
unique synthetic path; run the verifier. Cut-at-hook cycles assert the exact
REC-6 row; cut-at-random-point cycles assert the general obligations.

### Mechanism B — child-process SIGKILL cycles (real OS path, REC-17)

Generalizes `crash_smoke.rs`: the harness spawns a child (its own binary in
child mode) that runs a seeded workload against a **real** env
(mmap/pwrite or WRITE_MAP) and dies. Two kill styles:

- **deterministic self-kill** — the child computes from its seed a
  `(commit_index, HookPoint)` and `std::process::abort()`s there via a
  `CommitHook` (fully reproducible), or at a seeded op boundary;
- **asynchronous SIGKILL** — the parent kills after a seeded-but-wall-clock
  delay, which can land *mid*-C2 (a partial pwrite loop — states the hooks
  cannot produce). Best-effort repro (elapsed ns logged); the saved disk
  image is the authoritative repro artifact on violation.

Parent reopens the real file and runs the same verifier (obligations 1–5;
obligation 2 uses the child's last-acked txnid, written by the child to a
side-channel file that is itself fsynced before each commit returns —
otherwise the “acked” record could outrun the ack).

Both mechanisms run all durability modes; WRITE_MAP is exercised in both
(mechanism A via `FaultBacking` over `WriteMapBacking` semantics — msync is
just `sync()`; mechanism B on the real writable map).

## Decision 3 — The `crash-harness` binary (`zerodb-oracle/src/bin/crash-harness.rs`)

Matches the justfile contract (`--cycles 200` quick / `--cycles 10000` full).

- CLI (hand-rolled `std::env::args` parsing — no new deps):
  `--cycles N` (total verification cycles), `--seed S` (default: entropy,
  always printed), `--mechanism image|sigkill|both` (default `both`, split
  ≈80/20 image/sigkill — images are ~100× cheaper and carry the tear
  coverage; sigkill carries the real-OS coverage), `--jobs J` (default
  `available_parallelism`), `--modes default|all|<list>` (default `all`,
  weighted toward `default`), `--repro <seed:cycle>` (re-run one cycle).
- **Cycle accounting:** one cycle = one crash-state reopened and verified
  (each materialized image variant and each SIGKILL recovery counts as one).
  This is the natural reading of PLAN's “crash-recovery cycles” — flagged as
  open question 1 for sign-off, since the alternative (one cycle = one cut
  point) multiplies the bar by V.
- Parallelism: worker threads for mechanism A (per-worker reusable buffers,
  unique synthetic paths namespaced by worker id); a bounded pool of child
  processes for mechanism B. Cycle seed = `splitmix64(base_seed ^ cycle_idx)`
  — cycles are independent and a run is reproducible regardless of `--jobs`.
- On violation: exit nonzero; print `seed`, cycle index, mechanism, mode, cut
  point, and the failed obligation; save the disk image + `FaultPlan` dump +
  op stream under `target/crash-repro/<seed>-<cycle>/`. First failure stops
  the run (CI semantics); `--keep-going` for triage.
- justfile: recipes already exist and are honored as written; `crash-test-full`
  is the nightly-CI target (PLAN Testing strategy), `crash-test-quick` joins
  the per-milestone gate from M1.11 on.

## Decision 4 — Durability-flag windows: assert exactly REC-9..12, no more

| Mode | Legal recovered set | Asserted | **Not** asserted |
|------|--------------------|----------|------------------|
| default | `{N−1, N}` per REC-6 row | obligations 1–5 in full | — |
| NO_META_SYNC | any `M ≤ N` with `M` ≥ last barrier-covered meta | open succeeds; walk clean; data == model at `M`; never corruption (REC-10) | zero loss; any numeric bound on `k` beyond “≥ last explicit force_sync / open” |
| NO_SYNC / MAP_ASYNC, **ordered sub-model** (pending persists only as an issue-order prefix — models an ordered-writeback FS) | some prefix txnid `M ≤ N` | walk clean; data == model at `M` (REC-11's conditional guarantee under its stated condition) | zero loss |
| NO_SYNC / MAP_ASYNC, **adversarial sub-model** (full REC-20 reorder/tear) | anything, incl. meta-without-data and both-slots-torn | open never panics/UB; errors confined to the designed taxonomy (`Invalid`/`Corrupted`-class); *if* the walk passes and a txnid is recovered, no assertion on data values | structural consistency, bounded loss — REC-11 explicitly does not promise them under reordering; asserting them would be a false guarantee and a permanently red harness |
| WRITE_MAP (+ sync modes) | as default (REC-12: REC-6 holds verbatim with msync) | as default | — |
| WRITE_MAP + MAP_ASYNC | as MAP_ASYNC | as MAP_ASYNC rows | — |

Notes: (a) both-slots-invalid is reachable **only** in the NO_SYNC adversarial
sub-model (two commits' meta writes pending simultaneously, both torn
sub-sector) — there it is a legal outcome, asserted as designed-`Invalid`, not
a violation; in default mode the harness asserts it is **unreachable**
(REC-3's single-torn guarantee). (b) The adversarial sub-model's thin
assertion set is deliberate: its purpose is REC-11 *characterization* — the
harness logs (does not gate on) the observed loss/corruption distribution.

## Decision 5 — O_DIRECT posture: defer to Phase 3.5; pin the sizing invariant now

Phase 1 stays on buffered I/O (pwrite + fdatasync / mmap + msync). Actual
`O_DIRECT` belongs to the Phase 3.5 io_uring backend (its alignment,
allocator, and queue-depth story). What M1.11 does now — PLAN's “O_DIRECT-
friendly write sizing” note — is pin and enforce the invariant that makes
that future backend a drop-in: **every `write_at_page` the engine issues is
psize-aligned at a psize-multiple offset with a psize-multiple length** (pages
and whole overflow runs; the meta write is one full page). The `FaultBacking`
debug-asserts this on every write, so 10k cycles double as a continuous audit
that no sub-page or misaligned write ever creeps into the commit path. No
`O_DIRECT` flag, no allocation-alignment work, in Phase 1. (EBS sanity per
PLAN: alignment is the property EBS/O_DIRECT needs; recorded here so Phase 3.5
starts from an already-clean write pattern.)

## Decision 6 — Risk register (3 traps + a guard each)

1. **Fault model too kind** — uniform sampling starves the nasty corners
   (sub-sector meta tears, lost file extension, meta-persisted-data-dropped),
   and the harness “passes” without testing REC-8/REC-14.
   *Guard:* hard plan-distribution quotas (Decision 1) asserted per batch,
   **plus a mutation self-test**: a test-only misordered pipeline (fsync-data
   *after* meta write — built inside the harness by wrapping `FaultBacking`
   to swap barrier accounting, never by touching the shipped pipeline) MUST
   make the harness fail within a bounded cycle budget. A crash harness that
   cannot catch the bug class it exists for is untrustworthy; this is run as
   a unit test of the harness itself.
2. **In-process cycles sharing state** — registry residue, leaked snapshots/
   reader slots, a poisoned env contaminating later cycles; 10k cycles then
   test one polluted world instead of 10k fresh ones.
   *Guard:* unique synthetic registry path per env per cycle; post-cycle
   assertions that the registry entry is gone (id-guarded Drop) and the
   reader table is fully released; per-worker state is scope-confined per
   cycle; `--isolate` flag runs image batches inside child processes for
   contamination triage. Mechanism B is process-isolated by construction and
   cross-checks A.
3. **Verification-oracle drift** — the shadow model's semantics diverge from
   the engine's (APPEND/NO_OVERWRITE edges, clear-vs-drop, catalog behavior),
   producing phantom violations or masking real ones.
   *Guard:* the model reuses `driver::classify` + the oracle op semantics
   (single authority, already differential-tested against the fork for
   M1.3–M1.10); and the pre-crash model==env assertion (Decision 2) validates
   the model against the live engine on **every** cycle before any fault is
   injected — drift can never be misattributed to recovery.

## Decision

Adopt: **D1 Option B** (journaling `FaultBacking` wrapping a real backing —
zero new unsafe, REC-20 barrier model, 512 B sector decomposition with
sector-aligned *and* sub-sector tears under quota-enforced deterministic
plans from an in-house PRNG); **D2** two mechanisms over one shared
seeded-Op workload generator and one shared verifier (replayable shadow model
+ `check_image` + REC-18 obligations; in-process image cycles as the volume
mechanism, child-process SIGKILL — deterministic self-kill + async kill — as
the real-OS mechanism); **D3** hand-rolled-CLI `crash-harness` bin in
`zerodb-oracle` honoring the existing justfile recipes, parallel,
seed-reproducible, saving image+plan+ops on violation; **D4** per-mode
assertion table scoped exactly to REC-9..12 (adversarial NO_SYNC/MAP_ASYNC
characterizes, never gates on, what REC-11 does not promise); **D5** O_DIRECT
deferred to Phase 3.5, psize-aligned-write invariant debug-asserted now;
**D6** the three-trap risk register with quotas+mutation-self-test,
isolation+residue-asserts, and single-authority-model+pre-crash-cross-check.

## Implementation findings (2026-07-16, same session)

The harness found one **real crash-consistency window** and two of its own
model bugs during bring-up (D6's traps firing as designed):

1. **`NO_META_SYNC` reclaim-clobber window** (repro seed
   15797139550980166469, found by the first `--modes nometasync` batch):
   txn `N+1`'s legally reclaimed pages (freed by txn `N`, i.e. snapshot
   `N−1`'s pages, GC-18) can persist while the un-fsynced meta `N` tears →
   recovery falls to a structurally corrupted `N−1`. REC-10's blanket "never
   corruption" overclaims; LMDB shares the window (libmdbx's steady/weak
   metas close it). SPEC 06 REC-10 amended (scoped claim, **ratification
   pending**, conflict-block item 4); verifier scoped to exactly the
   corrected claim ("stale fallbacks" counted, never gated); steady-meta
   gating filed as a Phase 3 candidate. Default/`WRITE_MAP` remain fully
   asserted — they are immune (C5 orders meta durability before the next
   txn exists).
2. Verifier bookkeeping: acked-at-cut vs acked-at-workload-end under
   capture-and-continue (regression-pinned, seed 8467876453780440666).
3. Model effectiveness rule: `clear` on an already-empty db still bumps the
   txnid (dirties the working record; matches LMDB's always-dirty
   `mdb_drop(dbi,0)`) — probed empirically, model corrected.

## Consequences

- Easier: REC-8/REC-14/REC-20 get executable form; M1.12's `check` tool gains
  a battle-tested library core (`check_image` is already the walker); the
  Phase 3.5 io_uring/O_DIRECT backend inherits a verified aligned write
  pattern and a harness to re-certify against; every future pipeline change
  is regression-gated by `crash-test-quick`.
- Harder: the harness itself is now trusted code — hence the mutation
  self-test (D6.1); CI gains a nightly `crash-test-full` job whose wall-time
  budget needs sizing (open question 5).
- Tests to add (implementation phase, by me per rule 6): fault-backend unit
  tests (barrier folding, tear decomposition, quota assertions, determinism:
  same seed ⇒ byte-identical images), the mutation self-test, harness-model
  parity tests, plus the 200-cycle quick gate. No loom additions (no new
  lock-free interactions — the backend is mutex-based and thread-confined).
- SPEC updates in the same change (rule 3): SPEC 06 §5 gains the cycle
  accounting definition, the NO_SYNC ordered/adversarial sub-model split, and
  the D5 alignment invariant cross-ref; SPEC 02 §8 unchanged (no format
  change — this ADR is test infrastructure plus one debug-assert).
- New `unsafe`: none. New dependencies: none (in-house PRNG is ~20 lines in
  the harness; subject to open question 4).

## Open questions for human review

1. **Cycle accounting:** does each materialized image variant / each SIGKILL
   recovery count as one of the ≥10k cycles (recommended, stated in D3), or
   must 10k be *cut points* (≈160k verifications at V=16 — full run cost ×16)?
2. **Async SIGKILL in CI:** keep the non-deterministic-timing kill style in
   the CI run (extra coverage, best-effort repro via saved image), or restrict
   CI to deterministic self-kill and leave async kill as a manual/nightly-only
   flag?
3. **Adversarial NO_SYNC assertion floor** (D4): is “never panics, designed
   error taxonomy only, distribution logged” acceptable as the full gate for
   the reordering sub-model, per REC-11's non-guarantee? Any stronger gate
   would assert what the spec explicitly does not promise.
4. **PRNG:** in-house splitmix64/xoshiro256** in the harness (bit-stable
   forever, ~20 lines of test-only code) vs allowlisted `rand::StdRng`
   (lockfile-stable only). Recommended: in-house; needs your sign-off since
   CLAUDE.md prefers the allowlist.
5. **CI budget:** `crash-test-full` (10k cycles) target wall time and venue —
   nightly on the Graviton runner alongside fuzz-long (recommended), with
   `crash-test-quick` (200) in the per-milestone gate?
6. **Mechanism split default** (80% image / 20% sigkill of the 10k): acceptable,
   or do you want a fixed floor (e.g. ≥2k) of SIGKILL cycles written into the
   acceptance?
