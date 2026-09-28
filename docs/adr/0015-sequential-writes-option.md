# ADR-0015: Opt-in sequential-writes fast path (env option, per-database override)

- Status: Accepted (2026-09-28); kept only if the measurement gate below passes
- Milestone: Phase 3 (performance), PERF-GAP roadmap #6
- Date: 2026-09-28

## Context

Roadmap #6, the rightmost-leaf finger, remembers each tree's root-to-rightmost-
leaf path inside a write txn. A put whose key sorts after the last key of that
leaf goes straight there instead of descending from the root. SPEC 03 §6.6
(landed with the patch) defines the invariants: the finger is re-verified
against the live tree on every use, dropped by every structural or ownership
change of its pages, and cross-checked against a fresh descent on every hit
in debug builds.

Measured always-on in the perf loop (bench server, x86-64, 4 KiB, 3 rounds,
ZeroDB ÷ LMDB): `put/val/v8` 0.82× → 0.62×, `put/api/reserved` 0.94× → 0.81×,
`commit/batch/n10k` 0.90× → 0.77×, `put/order/append` 1.11× → 1.07×; but
`put/order/rand` +3–5 % and `mixed/rw/8dbs` +3–4 %. The lever was reverted
pending a call. The maintainer asked (2026-09-28) for it as "an option of the
env that optimize it but access to tradeoff", and approved an env option with
a per-database override ("perfect").

LMDB has no counterpart: `mdb_put` sets up a fresh cursor per call and even
`MDB_APPEND` re-descends through `mdb_cursor_last`. This is a ZeroDB
extension; results are identical with the option on or off.

## Options

- **A — Always on.** Rejected: random-key writes pay 3–5 %.
- **B — Env option only.** One switch; every database of the env pays the
  random-write cost when it is on.
- **C — Env option plus a per-database override.** The env option sets the
  default; a database can be switched on or off individually. Meilisearch can
  enable it for tables whose keys only grow (documents by internal id, hannoy
  item ids) and keep random-key tables (word → docids) off, at no cost.
- **D — Per-put flag.** Rejected: `PutFlags::APPEND` already exists for callers
  that know their order per put; the point here is callers that do not
  annotate each put.

## Decision

**Option C.**

1. **API.** `zerodb::EnvOpenOptions::sequential_writes(bool)` (default
   `false`) and `heed_zerodb::EnvOpenOptions::sequential_writes(bool)`, both
   safe. Per database: `zerodb::Env::set_sequential_writes(&Database,
   Option<bool>)` and the same on `heed_zerodb::Env`; `None` follows the env
   default. The override is runtime state, not persisted (like a comparator,
   D-014); it applies to write txns that start after the call.
2. **Mechanism.** A write txn resolves each tree's setting once: the main DB
   at txn begin, a named DB when the txn first touches it. With the setting
   off, `put` skips the finger hit test and the finger bookkeeping entirely;
   the remaining invalidation hooks are no-ops on an empty finger table. The
   GC tree never uses it.
3. **No `unsafe`, no format change.** The option changes speed only.

## Consequences

- **Tests:** the finger's model and proptest suites run with the setting on;
  a differential runs the same workloads with it on and off and requires
  identical contents; a test pins that the default is off and that a
  per-database override wins over the env default in both directions.
- **Bench gate:** (a) with the option off, the write ladder (`put/*`,
  `commit/*`, `mixed/*`, `del/*`) must be flat against the commit before this
  change in both codegen settings; (b) with it on (bench knob
  `ZERODB_BENCH_SEQUENTIAL_WRITES=1`), the sequential rungs must keep the
  measured gains.
- **Docs:** SPEC 00 gains the option rows, SPEC 03 §6.6 states that the finger
  runs only for trees with the setting on, DIVERGENCES gains the extension
  entry.
