# ADR-0001: The oracle crate links heed =0.22.1 (the Meilisearch LMDB fork)

- Status: Approved — Quentin, 2026-07-16 (chat: 'continue' on the presented Phase 0 ratification queue, per recommendations)
- Milestone: 0.3 (Oracle harness)
- Date: 2026-07-15

## Context

Ground rule 1 (CLAUDE.md, PLAN.md §0.3) requires every LMDB behavior question to
be answered by a *differential test against the oracle*, never by guessing. The
oracle is the **Meilisearch LMDB fork** — branch `mdb.master.nested-rtxns`,
commit `cd767228d31285a73af44f7fb15267e95d1df86f` — as vendored by
`lmdb-master-sys 0.2.6` inside **heed v0.22.1**. This is exactly what Meilisearch
runs (SPEC 00 pinned-consumer table); stock LMDB 0.9 is *not* the oracle, because
it lacks the read-txn-nested-in-a-write-txn feature milli uses on the hot path
(SPEC 00 Finding §A, SPEC 01 §S9).

To run the oracle, `zerodb-oracle` must call into that exact C library. The
cleanest, least-error-prone way to reach the fork with the same build flags
Meilisearch uses (notably `MDB_MAXKEYSIZE == 511`, no `longer-keys` feature) is
to depend on `heed` itself at the pinned version and drive LMDB through heed's
safe API, rather than re-vendoring or hand-binding `lmdb-master-sys`. heed is the
API surface Phase 1 reproduces, so exercising LMDB through it also matches the
observable behavior consumers actually see.

CLAUDE.md unsafe/dependency policy: FFI and C linkage are permitted **only inside
`zerodb-oracle`**. No new dependency may be added elsewhere without an ADR;
`heed`/`lmdb-master-sys` are C-linking crates and therefore need this decision
recorded.

## Options

### Option A — Depend on `heed = "=0.22.1"` in `zerodb-oracle` only
Drive the reference engine through heed's safe API (`Env`, `RwTxn`, `RoTxn`,
`Database`, `nested_read_txn`, `PutFlags`, `MdbError`). Exact `=` pin so the
oracle and Meilisearch resolve byte-for-byte identical C.

- Pros: byte-for-byte agreement with what Meilisearch runs; reuses heed's
  `nested_read_txn` (fork-only) directly; no hand-written FFI to audit; heed's
  error taxonomy is the one Phase 1 must mirror, so mapping is 1:1; single
  crate carries all C linkage, satisfying the unsafe policy boundary.
- Cons: pulls heed (and transitively `lmdb-master-sys`, `libc`, `bitflags`,
  `byteorder`, `synchronoise`, `page_size`) into the *dev/test* dependency
  graph. Contained to the oracle crate; never in the shipping engine.
- Crash-safety / ARM: none directly (test-only crate). The C build is the same
  one Meilisearch validates on Graviton.

### Option B — Depend on `lmdb-master-sys = "=0.2.6"` directly, hand-write bindings
Bind `mdb_*` calls in `zerodb-oracle` without heed.

- Pros: fewer transitive crates; full control over txn-begin flags (e.g. calling
  `mdb_txn_begin(env, wtxn, MDB_RDONLY)` for nested reads explicitly).
- Cons: re-implements, and must keep in sync with, the exact semantics heed
  already encodes (lifetime model, error mapping, nested-read entry point,
  reserved-space handling); large `unsafe` surface to audit for a *test* harness;
  higher risk the oracle itself is wrong, which would be catastrophic (a wrong
  oracle silently blesses zerodb bugs). Diverges from "what Meilisearch runs" at
  the API layer even if the C is identical.

### Option C — Re-vendor the fork C source into the repo
Copy the fork's `mdb.c`/`lmdb.h` and build with `cc`.

- Pros: no registry dependency; pin is a git submodule/commit.
- Cons: transliteration/clean-room hazard (CLAUDE.md rule 4 forbids copying C
  into the engine; vendoring it into a *test* crate is allowed but muddies the
  boundary); we would still hand-bind it (Option B's cons); build-system burden.
  No upside over Option A for a test-only oracle.

## Decision

**Option A.** `zerodb-oracle` depends on `heed = "=0.22.1"` (exact pin) plus
`thiserror` and `arbitrary` (already on the allowlist) and, as dev-dependencies,
`proptest` (allowlisted). The reference `LmdbEngine` opens the env the way
Meilisearch does: `EnvOpenOptions::new().read_txn_without_tls()` (i.e. `MDB_NOTLS`,
`RoTxn: Send`), an explicit `map_size`, and `max_dbs`.

**Scope of this dependency:** `heed` and any LMDB `*-sys` crate are permitted in
**`zerodb-oracle` only**, and only for Phases 0–1. No other crate
(`zerodb-core`, `zerodb-io`, `zerodb`, `heed-zerodb`, `zerodb-tools`) may depend
on `heed` or an LMDB sys crate during Phases 0–1. `heed-zerodb` (milestone 1.13)
will depend on/replace heed per the milestone-0.5 integration ADR, not this one;
the `fuzz/` crate (outside the workspace) additionally depends on `libfuzzer-sys 0.4`,
the standard cargo-fuzz runtime the pre-existing justfile presupposes — covered by
this ADR with the same oracle-only, test-infrastructure-only scope;
that is a separate, human-approved decision. The oracle routes zerodb through its
**native** API, never through `heed-zerodb` (PLAN.md §0.3); adapter-level parity
is re-verified at the 1.14 gate.

## Consequences

- The oracle's reference side is the fork's observable behavior, satisfying
  ground rule 1. The `zerodb` side lands in M1.2+ as a second `Engine` impl; the
  M0.3 acceptance mode is `run_self_test` (LmdbEngine vs a second independent
  LmdbEngine), which also guards harness determinism.
- The exact `=0.22.1` pin means a heed bump is a deliberate, reviewed change
  (it moves the oracle's definition of truth). Encoded in `Cargo.toml`.
- Error taxonomy mapping (`MdbError` → the oracle's normalized `OracleError`)
  lives in the oracle and is the reference for the M1.13 heed-adapter taxonomy.
- Tests/invariants to add: covered by M0.3 deliverable 3 (trivial-sequence
  self-test, proptest self-test, and the §S4 key-bound self-test).
- SPEC to update: SPEC 01 §S4 open question is answered empirically by the
  key-bound self-test in the same change.

## Open questions for human review

- None blocking. Note for the maintainer: this ADR documents a dependency that
  PLAN.md §0.3 already mandates ("LMDB implements it via heed 0.22.1 /
  lmdb-master-sys 0.2.6"); it is recorded as **Approved** on that basis. If you
  would rather gate it as Draft pending explicit sign-off, say so and I will flip
  the status.
