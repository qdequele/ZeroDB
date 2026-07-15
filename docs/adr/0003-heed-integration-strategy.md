# ADR-0003: heed integration strategy — how ZeroDB gets behind milli & hannoy

- Status: Proposed
- Milestone: 0.5 (this ADR); implemented in 1.13, gated in 1.14
- Date: 2026-07-15

> **ADR-only milestone.** Per CLAUDE.md rule 6, milestone 0.5 is ADR-only:
> **nothing is implemented until a human approves this document.** No code,
> no crate scaffolding, no `[patch]` entries. This file + its `docs/DECISIONS.md`
> index row are the entire deliverable for M0.5.

## Context

ZeroDB must become a drop-in replacement for LMDB **at the heed API level** for
five consumers (milli, other Meilisearch crates, arroy, hannoy, cellulite). The
frozen contract is `heed 0.22.1` (`docs/SPEC/00-api-surface.md`, 61 MUST rows).
This ADR chooses *how* ZeroDB is wired behind that surface; the choice constrains
lifetimes, error types, and `Env` semantics for all of Phase 1 and is
implemented by milestone 1.13.

### What the decision must enable (PLAN §1.13 / §1.14)

- **1.13** builds the adapter over the *full SPEC 00 surface*, maps the error
  taxonomy 1:1, replicates the env registry + `EnvClosingEvent`, and patches
  milli + hannoy to build against ZeroDB behind a cargo feature at the pinned
  SHAs. Accept: milli + hannoy compile on the ZeroDB backend; heed's test suite
  (scoped per this ADR + the 1.9 decision) passes; the oracle re-run **through
  the adapter** shows zero divergences.
- **1.14** is the Phase-1 exit gate: the Meilisearch test suite and hannoy suite
  run **green on ZeroDB behind the cargo feature**, plus 24 h differential fuzz.
  The gate implicitly requires running the *same* consumer test build on **both**
  backends (LMDB baseline, ZeroDB candidate) — so the integration strategy must
  make an A/B swap cheap.

### Hard constraints (SPEC 00 Findings §C — load-bearing, non-negotiable)

Walked one-by-one in the option analysis below:

- **C1 — concrete types, not generics.** Consumers name exact paths:
  `Env<WithoutTls>`, `RoTxn<'a, WithoutTls>`, `RoTxn<'static, WithoutTls>`,
  `RwTxn<'p>`, `Database<KC, DC>`, `WithoutTls`/`WithTls`, `EnvOpenOptions<T>`,
  `EnvFlags`, `PutFlags`, `DatabaseFlags`, `CompactionOption`, `EnvClosingEvent`,
  `MdbError`, `heed::Error`, `ReservedSpace`, `LazyDecode`, `DecodeIgnore`,
  `BytesEncode`/`BytesDecode`/`BoxedError`. heed is **not** generic over a
  backend today, and PLAN rule 2 forbids signature changes to existing heed APIs
  in Phases 0–2.
- **C2 — `Env: Clone`, shared ownership** (`Arc`-style; SPEC 04 TXN-50).
- **C3 — same-process registry + `EnvAlreadyOpened`** (SPEC 04 TXN-51).
- **C4 — `EnvClosingEvent` / `prepare_for_closing()`** deferred close with
  `wait`/`wait_timeout` (SPEC 04 TXN-52/53).
- **C5 — error taxonomy 1:1**, with `Encoding`/`Decoding` **publicly
  constructible** from a `BoxedError` (consumers construct them; SPEC 04 §8.1).
- **C6 — `WithoutTls` universal**, makes `RoTxn: Send` (SPEC 04 §4, TXN-13).
- **C7 — `nested_read_txn` on both `RwTxn` and `Env`** with the fork's
  read-child-of-write-txn semantics (SPEC 00 §A, SPEC 04 §5, D-003/D-005).
- **C8 — `put_reserved` + `WRITE_MAP` interaction** (value-byte location; SPEC 04 §6).

### Prior-art facts established from the pinned repos (not guessed)

- heed's public API is **already** the exact concrete shape C1 requires. The C
  binding is `pub(crate)`: 251 `ffi::` uses confined to ~9 engine-coupled
  modules (`envs/`, `txn.rs`, `databases/`, `cursor.rs`, `iterator/`,
  `reserved_space.rs`, `mdb/{error,flags,ffi}.rs`). ffi leaks into a *public*
  signature only via `mdb_filehandle_t` (`copy_to_fd`) and `lmdb_version()` /
  `LmdbVersion` — both classified WON'T in SPEC 00.
- **`heed-traits` and `heed-types` are backend-independent.** `BytesEncode`,
  `BytesDecode`, `Comparator`, `LexicographicComparator`, `BoxedError` live in
  `heed-traits`; all codecs (`Str`, `Bytes`, `Unit`, `SerdeJson`,
  `SerdeBincode`, integer BE codecs, `LazyDecode`, `DecodeIgnore`) live in
  `heed-types`. Both are pure `&[u8]` Rust with **zero** LMDB dependency.
- milli depends on `heed = "0.22.1"` from **crates.io by name** and re-exports
  it: `pub use { …, heed, … }` (milli/src/lib.rs:66). Consumers reference the
  crate both as `heed::…` (direct dep) and as `milli::heed::…` /
  `meilisearch_types::heed::…` (re-export). There is **no `[patch]`** in the
  Meilisearch workspace today (SPEC 00). The swap point at the milli level is
  therefore a crate that *presents as `heed`*.
- heed has **no `tests/` directory**. Its suite is: inline `#[cfg(test)] mod
  tests` (28 `#[test]` fns across lib.rs, txn.rs, mdb/lmdb_error.rs,
  databases/{database,encrypted_database}.rs, iterator/mod.rs, envs/env.rs),
  10 `examples/*.rs` (compiled+run smoke tests), and `cookbook.rs` doctests.

## Options

### Option A — Fork heed; add a compile-time backend feature inside it

Keep the `meilisearch/heed` repo, add an internal seam (a `zerodb` cargo feature
that swaps the `mdb::ffi` module for a ZeroDB-native module). Consumers keep
`heed::` paths unchanged.

Constraint walk: **C1 ✔** (public types untouched — they *are* heed's). **C2–C8
✔ by construction** — every one of these types/semantics already exists in heed
exactly as consumers expect; only the *bodies* are reimplemented. The strategy
inherently satisfies §C because it reuses heed's surface verbatim.

- Pros: zero consumer churn; §C free by construction; the reference LMDB backend
  and ZeroDB live in one tree, easing side-by-side reading.
- Cons: the seam is **deep and invasive**. heed's internals thread raw
  `NonNull<ffi::MDB_env>`, `MDB_val`, `MDB_dbi`, and cursor-op integers through
  ~9 modules; a compile-time split means either `#[cfg]`-forking those modules
  or introducing an internal backend trait the two impls satisfy — a large,
  intrusive refactor of a crate Meilisearch currently tracks unmodified from
  crates.io. **Permanent fork-rebase treadmill**: every upstream heed release
  must be rebased onto the seam. A compile-time feature also means the two
  backends **cannot coexist in one build**, so the 1.14 A/B story is "build the
  whole consumer twice," not an in-process diff.
- Crash-safety / ARM: neutral (backend bodies own that regardless of seam).

### Option B — Upstream a backend trait into heed proper

No fork: heed grows a real backend abstraction (trait) that stock LMDB and
ZeroDB both implement; ZeroDB ships as an external crate implementing it.

Constraint walk: **C1 ✘ (at risk)** — a genuine backend trait pushes a type
parameter onto the public types (`Env<B, …>`). Unless it is a *defaulted*
generic that still resolves `heed::Env<WithoutTls>`, this breaks the
concrete-type contract and PLAN rule 2 (no signature changes in Phases 0–2). A
defaulted generic is possible but perturbs inference and every turbofish across
five consumer repos — a real risk against a frozen contract. **C2–C8** are
achievable *if* C1 is finessed, but they inherit the same "only if the generic
is invisible" caveat.

- Pros: cleanest long-term; no fork; one canonical heed.
- Cons: **gated on upstream review & timeline ZeroDB does not control** — heed
  is explicitly not backend-generic today; this is a large architectural PR to
  negotiate. If C1 is preserved via a defaulted generic or cfg, the result is
  effectively Option A wearing a trait, with the same invasiveness but now
  upstream-blocked. Highest schedule risk; worst fit for a Phase-1 deadline.

### Option C — Standalone `heed-zerodb` crate re-implementing heed's surface 1:1

A new crate whose public surface is a 1:1 clone of heed 0.22.1's type
names/signatures/paths, ZeroDB underneath. **heed itself is untouched (no
fork).** Consumers switch via `[patch.crates-io] heed = { path = ".../heed-zerodb" }`
where the crate's `[package] name = "heed"` — a drop-in named `heed`, activated
at the workspace level (this *is* PLAN 1.13's "behind a cargo feature").

Key structural lever: **re-export `heed-traits` and `heed-types` verbatim**
(`heed_zerodb::types = heed_types`, `heed_zerodb::{BytesEncode, BytesDecode,
BoxedError, Comparator} = heed_traits::…`, version-locked to the 0.22.1 line).
Because those crates are backend-independent, the codec and trait types are
**literally the same types**, not reimplementations — so `BytesEncode`/
`BytesDecode` identity and trait coherence are preserved. Only heed's ~9
engine-coupled modules are reimplemented over ZeroDB's native API.

Constraint walk:
- **C1 ✔** — the crate defines the exact concrete paths; no generics introduced.
- **C2 ✔** — `Env` = `Arc<EnvInner>` per SPEC 04 TXN-50 (already specced).
- **C3 ✔** — registry + `EnvAlreadyOpened` per SPEC 04 TXN-51.
- **C4 ✔** — `EnvClosingEvent`/`prepare_for_closing` per SPEC 04 TXN-52/53.
- **C5 ✔** — error enum reimplemented with `Encoding`/`Decoding` publicly
  constructible from `BoxedError` (SPEC 04 §8.1); `MdbError` variants
  `MapFull`/`Invalid`/`BadValSize`/`KeyExist`/`ReadersFull` mapped from ZeroDB's
  native errors.
- **C6 ✔** — `WithoutTls`/`WithTls` markers reimplemented; `RoTxn: Send` under
  `WithoutTls` (SPEC 04 §4).
- **C7 ✔** — `nested_read_txn` on both `RwTxn` and `Env` per SPEC 04 §5 /
  D-003/D-005 (native, no C-fork dependency).
- **C8 ✔** — `put_reserved`/`ReservedSpace` + `WRITE_MAP` value-byte placement
  per SPEC 04 §6.

- Pros: **full source control** — no fork of heed, no upstream dependency, no
  rebase treadmill. The surface is **frozen** by PLAN rule 2 and SPEC 00, so
  drift is bounded — new upstream heed methods are added only if a pinned
  consumer uses one (none, by definition of the freeze). **Best 1.14
  differential story**: heed (LMDB) and heed-zerodb (ZeroDB) are two *sibling
  crates with an identical surface*, so (a) the same consumer build runs on
  either backend by toggling the `[patch]`, and (b) the oracle can link **both**
  in one test binary (distinct crate identities) for in-process A/B diffing —
  and the "oracle re-run through the adapter" accept-criterion is just
  instantiating the oracle `Engine` over `heed_zerodb`'s public API.
- Cons: real re-implementation work across the whole envs/txn/databases/cursor/
  iterator surface; must keep the `heed-traits`/`heed-types` re-export
  version-locked to 0.22.1 so trait identity holds; the `[patch]` presents as
  crate `heed`, so the workspace can build *only one* backend at a time for the
  consumer (the A/B in-process trick is available to the **oracle**, which names
  both crates directly, not to milli, which names only `heed`).
- Crash-safety / ARM: owned entirely by ZeroDB's native engine; the adapter is a
  thin type/lifetime/error shim, keeping unsafe out of the seam.

### Option D — Hybrid: C now, keep B as a future convergence option

Ship Option C for Phase 1–2 (control, speed, clean A/B). Hold Option B (upstream
a backend trait, defaulted so C1 holds) as a **Phase 3+ aspiration**, pursued
only if (a) maintaining a parallel surface proves painful and (b) heed
maintainers are willing. C→B is a non-breaking internal move for consumers if B
preserves the concrete paths. This de-risks the schedule (never blocked on
upstream) while leaving the door open to a single canonical heed later.

## Decision

**Adopt Option C — a standalone `heed-zerodb` crate re-implementing heed
0.22.1's public surface 1:1 over ZeroDB's native API — framed as Option D:
C is the Phase-1/2 commitment, upstreaming (B) is a recorded, optional Phase-3+
convergence, not a Phase-1 obligation.**

Rationale (one paragraph): Option C is the only strategy that satisfies **every**
§C constraint *natively* — with concrete types, no defaulted-generic gamble
against the frozen contract, and no upstream-review dependency — while turning
heed's own architecture to our advantage: because `heed-traits` and `heed-types`
are backend-independent, ZeroDB re-exports them **verbatim** (preserving
`BytesEncode`/`BytesDecode`/codec identity and trait coherence) and only
reimplements the ~9 LMDB-coupled modules over the engine. It gives ZeroDB full
source control with **no fork-rebase treadmill** (Option A) and **no dependency
on heed maintainer timelines** (Option B), and it produces the best 1.14
differential story: two sibling crates with an identical surface let the same
Meilisearch/hannoy build run on either backend via `[patch.crates-io]`, and let
the oracle link the real `heed` (LMDB) and `heed-zerodb` (ZeroDB) in one binary
for in-process A/B diffing. The surface is frozen by PLAN rule 2 and SPEC 00, so
the perennial worry — behavioral drift between backends — is contained by the
oracle and the 1.14 gate, not by the wiring, and Option C makes that A/B
verification *cheaper* than either alternative.

## Consequences

### What 1.13 builds (and its diff surface)

- **New crate `crates/heed-zerodb`** (`[package] name = "heed"`), depending on
  `zerodb` (native API) + verbatim re-exports of `heed-traits` / `heed-types`
  pinned to the 0.22.1 line. Reimplements: `EnvOpenOptions<T>`, `Env<T>`,
  `RoTxn`/`RwTxn`, `Database<KC,DC>` + `DatabaseOpenOptions`, cursors +
  iterators (fwd/rev/range/prefix + `_mut` variants used by consumers),
  `ReservedSpace`, `EnvFlags`/`PutFlags`/`DatabaseFlags`, `CompactionOption`,
  `heed::Error`/`MdbError`, `EnvClosingEvent`, the process registry, and
  `WithoutTls`/`WithTls`.
- **Consumer diff is minimal and mechanical**: a workspace `[patch.crates-io]
  heed = { path = "…/heed-zerodb" }` (or git), behind a cargo feature, at the
  pinned milli/hannoy SHAs. milli/hannoy source is **unchanged** (they keep
  `heed::`/`milli::heed::` paths). Estimated consumer diff: a handful of
  `Cargo.toml` lines per repo; **zero** `.rs` edits in the happy path.
- `DatabaseFlags` exists as a type (C1) but any non-empty value errors per D-004
  (SPEC 00); nested *write* txns error per D-003.

### How 1.14 runs BOTH backends (the differential story)

1. **Consumer suites, two builds**: run the Meilisearch (milli + integration)
   and hannoy suites first without the patch (LMDB baseline via crates.io
   `heed`), then with the `[patch]` active (ZeroDB). Green on both = gate pass.
2. **Oracle, one binary**: the oracle names `heed` (LMDB, via
   `lmdb-master-sys 0.2.6` fork) and `heed_zerodb` as **distinct crate
   dependencies simultaneously**, driving both `Engine` impls over the same op
   stream for in-process A/B divergence detection — this is the "oracle re-run
   through the adapter" accept-criterion (ADR-0001), now expressible because the
   two surfaces are identical.

### Tests / invariants to add

- Adapter-level parity: re-run the full oracle op-model through
  `heed_zerodb`'s public API (not just the native API) — zero divergences.
- A registry/close conformance suite (EnvAlreadyOpened, prepare_for_closing,
  wait/wait_timeout, static_read_txn keeps env alive) mirroring SPEC 04 §7.
- Error-taxonomy tests asserting each SPEC 04 §8.1 condition maps to the exact
  `heed::Error`/`MdbError` variant, and that `Encoding`/`Decoding` are
  publicly constructible.

### heed's own test suite — scoped IN / OUT under this ADR

heed's suite = inline `#[cfg(test)] mod tests` + `examples/` + `cookbook.rs`
doctests. Scope follows SPEC 00, D-003 (nested write), D-004 (DUPSORT/flags),
and the WON'T rows (encryption, custom comparators).

**Inline `#[cfg(test)] mod tests` — IN:** `lib.rs` (1), `txn.rs` (2),
`databases/database.rs` (3), `iterator/mod.rs` (9 — cursor/iteration parity,
M1.3), `envs/env.rs` (10 incl. `resize_database`, `info`/`stat`, registry).
**Partially IN:** `mdb/lmdb_error.rs` (1) — keep the variant-mapping assertions,
drop LMDB-specific raw-rc→string assertions (ZeroDB emits its own codes).
**OUT:** `databases/encrypted_database.rs` (2) — encryption is Phase 3.9 / WON'T
Phase 1 (`master3` feature).

**Examples — IN:** `all-types.rs` (codecs/ops), `clear-database.rs` (M1.6),
`cursor-append.rs` (APPEND, M1.10), `multi-env.rs` (registry/EnvAlreadyOpened,
M1.13), `nested-rtxns.rs` (nested READ txns + WRITE_MAP — the fork MUST, M1.9;
**the single most important example**), `prev-snapshot.rs` (PREV_SNAPSHOT, M1.2).
**OUT:** `custom-comparator.rs` (custom `mdb_set_compare`; no consumer, Phase
2.4), `custom-dupsort-comparator.rs` (DUPSORT + custom dupsort; D-004, Phase
2.8), `nested.rs` (nested **write** txns; D-003 — replaced by a scoped variant
asserting the clean "unsupported" error), `rmp-serde.rs` (needs `serde-rmp`,
which milli does not enable; codec-only, engine-agnostic — optional include).

**`cookbook.rs` doctests — IN** except snippets exercising encryption, DUPSORT,
or custom comparators, which are OUT on the same grounds.

### SPEC sections to update (in the 1.13 change, not now)

- SPEC 00: mark rows 23–26, 51–54, 61 as satisfied by `heed-zerodb`.
- SPEC 04 §7/§8.1: cross-reference the adapter as the realization of the
  registry/close model and error taxonomy.
- If human approves, flip this ADR to Approved and add a `docs/DIVERGENCES.md`
  cross-ref for the D-003 `nested.rs` scoping.

### Acceptance criteria 1.13 inherits from this ADR

1. `heed-zerodb` presents the full SPEC 00 MUST surface with the exact concrete
   type paths (C1–C8), re-exporting `heed-traits`/`heed-types` verbatim.
2. Error taxonomy maps 1:1 (SPEC 04 §8.1); `Encoding`/`Decoding` publicly
   constructible.
3. Registry + `EnvClosingEvent` semantics per SPEC 04 §7.
4. milli + hannoy compile against `heed-zerodb` behind a cargo feature at the
   pinned SHAs, with zero `.rs` edits in the happy path.
5. heed's test suite passes at the IN scope above.
6. The oracle re-run **through** `heed-zerodb` shows zero divergences.

## Open questions for human review

1. **crates.io publishing.** Is `heed-zerodb` (crate name `heed`) published, or
   consumed only via path/git `[patch]`? Publishing a crate literally named
   `heed` to crates.io is not possible (name taken); a differently-named
   published crate would break milli's by-name `heed::` references unless
   patched. Recommend: **git/path `[patch.crates-io]` only**, never published
   under `heed`.
2. **`[patch]` placement.** Should the milli/Meilisearch-side switch be a
   workspace-level `[patch.crates-io]` gated by a cargo feature, or a separate
   build profile / CI matrix axis? (Affects how 1.14 runs both backends.)
3. **Trait/codec version lock.** Pin `heed-traits`/`heed-types` re-exports to
   exactly `=0.22.1`? If upstream bumps them, do we track or freeze at 0.22.1
   for the life of Phase 1?
4. **arroy / hannoy / cellulite pinning during the transition.** All are
   crates.io deps of milli at fixed versions (SPEC 00). Do we patch them too, or
   rely on the single `heed` patch flowing through (they name `heed` by the same
   mechanism)? Confirm the single-patch assumption holds for all four.
5. **`WithTls` shim depth.** SPEC 00 marks WithTls SHOULD (Phase 2). For 1.13,
   is a compile-only `WithTls` shim (type exists, universal `WithoutTls`
   behavior) acceptable, deferring real TLS-slot semantics to Phase 2?
6. **Convergence trigger (Option D→B).** Under what condition, if any, do we
   revisit upstreaming a backend trait into heed proper? (Recorded as optional,
   not scheduled.)
7. **D-005 status.** D-005 (writer-quiescence enforcement) is still PROPOSED and
   is observable through this adapter's `nested_read_txn`; does it need to be
   APPROVED before 1.13 implements the borrow/assert model?
