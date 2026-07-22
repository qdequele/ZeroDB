# ADR-0013: dbi-handle lifetime parity — generation-checked `Database` handles (D-013)

- Status: Approved — Quentin, 2026-07-22 (chat: 'ADR-0013 -> option A'); open questions resolved per the in-ADR recommendations (recorded in Decision)
- Milestone: 2.9 (new; parity-closure track, 2026-07-22 directive)
- Date: 2026-07-22
- Closes: DIVERGENCES.md D-013; public tracking issue [#73](https://github.com/qdequele/ZeroDB/issues/73)

## Context

LMDB's contract (`lmdb.h`, `mdb_dbi_open`): a dbi handle is *private to the
transaction that opened it* until that transaction commits; **if the
transaction aborts, the handle is closed automatically**. The fork implements
this in `mdb_dbis_update(txn, keep)` from `mdb_txn_end`: on the non-commit path
every dbi flagged `DB_NEW` has its validity flag cleared and its sequence
number (`me_dbiseqs[i]`) bumped. Any later use of that handle fails the
`TXN_DBI_EXIST(txn, dbi, DB_VALID)` gate (`mdb_cursor_open`, and `DB_USRVALID`
in `mdb_put`) with **`EINVAL`**, which heed surfaces as `Io(InvalidInput)`.
Separately, `mdb_drop(txn, dbi, del=1)` calls `mdb_dbi_close` directly — an
**env-wide, immediate** close that a later abort does *not* undo (the data
comes back; the handle stays dead).

libmdbx keeps the same env-level dbi table with per-slot sequence counters but
gives the failure a dedicated error (`MDBX_BAD_DBI`) instead of overloading
`EINVAL`, and validates the sequence on every handle use. It also treats
pid/slot reuse hazards more defensively than LMDB (cf. our D-009, an LMDB
dbi-slot-reuse bug zerodb structurally lacks).

ZeroDB today (M1.6, SPEC 04 TXN-10): `Database` is a **plain `Copy` value**
(name + `DbSel`), resolved per-txn from the *committed catalog*; the env-level
`NamedRegistry` maps dbi→name only and is **append-only** within a process.
Nothing can go stale, so usage LMDB defines as invalid **silently works**
(D-013, filed 2026-07-20 from the adapter-pair fuzzer; harness regressions in
`crates/zerodb-oracle/tests/dbi_handle_lifetime.rs`). No pinned consumer can
observe the difference — milli/hannoy commit the txns that create their
databases — but the leniency masks real bugs in future consumer code, and the
maintainer directed on 2026-07-22 that the four LMDB-ahead divergences be
closed rather than sanctioned.

Constraints:

- heed's `Database<KC, DC>` is `Copy + 'static`; the adapter mirrors it. The
  fix must not change the handle's type shape (SPEC 00 surface, ADR-0003).
- Validity is **per-process, in-memory** state in LMDB too (the dbi table
  lives in `MDB_env`, not in the file) — no on-disk format impact.
- Hot paths (get/put/cursor) must not gain a lock: the M1.8 reader protocol
  and the write path are lock-free/single-writer by design.

## Options

### Option A — Engine-level generation registry (recommended)

Extend `NamedRegistry` slots with `{generation: AtomicU64, alive: bool}`;
`Database` carries `(dbi, generation)` captured at `create_database`/
`open_database`. Every per-txn handle bind (the existing `ensure_open`/
`record_for` resolution point) validates `handle.generation ==
registry[dbi].generation`; a same-txn `drop_db(delete=true)` and the
abort-path of a creating txn bump the generation (abort bumps only dbis the
txn created, matching `DB_NEW`). Mismatch → typed error, adapter maps to
heed's observable `Io(InvalidInput)`.

- Pros: parity everywhere (native API and adapter see the same strictness);
  one source of truth; the registry read is a single `Acquire` load per txn
  bind + per post-drop use — no hot-path cost on gets/puts after bind;
  append-only registry is preserved (no slot reuse → the whole D-009 hazard
  class stays unrepresentable).
- Cons: touches `zerodb-core` env state and the abort path (critical-path
  review per CLAUDE.md agents rule); per-txn bind granularity is coarser than
  LMDB's per-op gate (see Open Q2).
- Crash safety: none — purely in-process validity, disk format untouched.
- ARM: one `Acquire` load against a `Release` bump; no mixed-size atomics.

### Option B — Adapter-boundary tracking only

`heed-zerodb` keeps its own dbi table (handle → generation) and re-imposes the
`EINVAL` at the heed boundary, like the M1.13 re-impositions (D-006/D-010).
Core stays a plain value.

- Pros: `zerodb-core` untouched; smallest diff; matches the established
  boundary-re-imposition pattern.
- Cons: the boundary pattern exists for *leniencies no consumer can trigger*;
  D-013 is a *bug-catching strictness* — hiding it in the adapter leaves the
  native API (and future non-heed consumers, e.g. hannoy-native experiments)
  unprotected. The adapter would also need an abort hook into the engine
  anyway to learn which dbis died, duplicating registry state.

### Option C — Sanction the leniency (status quo)

Flip D-013 to APPROVED and keep plain-value handles. Rejected by the
2026-07-22 directive; recorded for completeness.

## Decision

**Option A — approved** (Quentin, 2026-07-22). Generation-checked handles at
the engine level, with the adapter mapping the typed error to heed's exact
observable (`Io(InvalidInput)`), the same split as M1.13.

Open questions resolved per recommendation: Q1 `MdbError::BadDbi`; Q2
per-txn-bind + drop-site granularity (escalate to per-op only if the oracle
ever exhibits a visible difference); Q3 bind-time linearization under the
registry atomics. Flag at implementation if any resolution proves wrong.

## Consequences

- SPEC 04: amend TXN-10 (handle resolution gains the generation gate) and add
  a TXN rule for abort-path invalidation + `drop_db` immediate close; state
  explicitly that generation state is in-memory per-process (parity with
  LMDB's dbi table).
- New engine error variant (see Open Q1); `heed-zerodb` maps it to
  `Io(InvalidInput)`.
- Oracle: `dbi_handle_lifetime.rs` graduates from harness-regression to a
  **differential** (use-after-abort, drop-then-abort-then-use, drop-then-use
  same txn — all must error identically to the fork); remove the adapter-pair
  fuzz guard for this state.
- DIVERGENCES.md: retire D-013; close issue #73. PROGRESS.md milestone line.
- Tests to add: native-API strictness unit tests; proptest that a handle
  captured before any sequence of abort/drop behaves exactly as a re-opened
  one afterward.

## Open questions for human review

1. **Error taxonomy**: new `MdbError::BadDbi` (libmdbx-style, clearer native
   diagnostics; adapter still maps to `Io(InvalidInput)` for fork parity) — or
   reuse `MdbError::Invalid`? Recommendation: `BadDbi`.
2. **Validation granularity**: per-txn bind + drop sites (recommended;
   observably equivalent for every legal op sequence we can construct, since
   invalidation only happens at txn boundaries or same-txn `drop_db`, both of
   which we gate) — or a strict per-op check to mirror `TXN_DBI_EXIST`
   mechanically? Per-op adds an atomic load to every operation for no known
   observable difference; if the oracle later finds a sequence where
   granularity is visible, per-op becomes mandatory.
3. **Cross-txn concurrent invalidation**: in LMDB, txn B aborting can kill a
   handle txn A is *about to* bind (env-global table). Proposed semantics:
   the generation check happens at bind time under the registry's atomics —
   A's bind either sees the bump (error, like LMDB) or completed before it
   (legal use of a then-valid handle). Confirm this linearization is
   acceptable as the spec'd behavior.
