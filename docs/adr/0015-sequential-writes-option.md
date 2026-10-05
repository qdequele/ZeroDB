# ADR-0015: Opt-in sequential-writes fast path (env option, per-database override)

- Status: Accepted (2026-09-28); kept only if the measurement gate below passes
- Implementation note (2026-10-05): gate passed, kept — 1fece73 + 7f6a36b (`benches/results/perf-ledger.jsonl`).
- Milestone: Phase 3 (performance)
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

## Measured (bench server, x86-64, 4 KiB pages, 2026-09-28)

**Gate (a), option off, against the commit before the option.** A first
layout carried the setting through the put path and inlined the finger
hooks into `free_page`; it cost `put/order/rand` +3.3 % (CGU1, 3 rounds) and
`commit/batch/n1` +5.6 %, `del/clear/all` +6.3 % (CGU16, 1 round). The kept
layout branches once at the top of `put` into an out-of-line finger-aware
copy of the put path and keeps only an empty-table test inline in the hooks:
flat on all 22 `put/*`, `commit/*`, `mixed/*`, `del/*` rungs at CGU1 (3
rounds) and on the 8 previously flagged rungs at CGU16 (3 rounds).

**Gate (b), option on vs off, same build (CGU1, 1 round), ZeroDB ÷ LMDB.**
Faster: `put/val/v8` 0.87× → 0.73×, `put/api/plain` 0.87× → 0.79×,
`put/api/reserved` 0.93× → 0.83×, `put/order/seq` 0.87× → 0.79×,
`put/val/v256` 0.89× → 0.83×, `commit/batch/n10k` 0.91× → 0.82×,
`commit/batch/n100` 1.14× → 1.05×. Slower: `put/order/rand` +5.8 %,
`mixed/rw/8dbs` +6.9 %, `del/churn/reinsert` +6.9 %, `commit/batch/n1` +7.1 %
(a finger is established and dropped in every one-put txn). That is the
trade the option exists for: turn it on for databases whose keys only grow,
through the per-database override, and leave random-key databases off.
