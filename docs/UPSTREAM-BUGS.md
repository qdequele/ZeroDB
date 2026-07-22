# Upstream bugs found by the oracle

Bugs in the **vendored Meilisearch LMDB fork** (`mdb.master.nested-rtxns`,
commit `cd767228`, via `lmdb-master-sys 0.2.6` / heed `v0.22.1`) discovered by
zerodb's differential fuzzing. These are NOT zerodb divergences — they are
crashes/UB in the reference engine itself. Each entry has a live repro kept
in-repo and a harness guard so fuzzing can continue; guards are removed when
the fork fix lands.

## FORK-1 — SEGV in `_mdb_cursor_put` (APPEND after same-txn clear of another db)

- **Found:** 2026-07-16, by `fuzz_diff_ops` (M1.2 gate run). Status: **open,
  not yet reported upstream** (repro ready).
- **Recipe** (all four ingredients required; confirmed by variant isolation):
  1. Write txn A: create named db `X`, commit.
  2. Write txn B: create a second db `Y`, `clear(Y)` (a fresh same-txn db).
  3. `put_with_flags(APPEND, k1, v)` into `X` — succeeds.
  4. `put_with_flags(APPEND, k2 < k1, v')` into `X` — should return
     `KeyExist`; instead **SIGSEGV** (near-null deref, `_mdb_cursor_put+0x8b8`
     via `mdb_put`). Value sizes are irrelevant (4 KiB and 1 MB both crash);
     skipping the clear, the prior successful APPEND, or using ordered keys
     avoids it.
- **Repro:** `cargo build -p zerodb-oracle --example fork_segv_repro`, then
  `VARIANT=with_clear ./target/debug/examples/fork_segv_repro` → exit 139.
  Plain build — no sanitizer needed. Minimized fuzz artifact:
  `fuzz/artifacts/diff_ops/crash-min-append-oversized-key`.
- **Real-world exposure:** milli uses both `clear` and APPEND-mode puts during
  indexing; the combination inside one write txn is plausible.
- **Harness guard:** `driver::classify` skips `PutFlagged(Append)` in any
  write txn that has executed a `ClearDb` (`Skip::KnownForkBug`), symmetric on
  both engines. Pinned by `tests/fork_bug_guard.rs`. Remove the guard when the
  fork is fixed and the repro exits 0.
- **zerodb behavior (M1.4+):** returns `KeyExist` per SPEC 01 §S1 — a crash is
  UB, not observable behavior to replicate (see DIVERGENCES D-007).

---

## Appendix: ready-to-file issue draft for FORK-1

(Absorbed from `docs/upstream/` during the public-repo cleanup, 2026-07-22.)

# Ready-to-file issue draft — NOT YET FILED

Target: https://github.com/meilisearch/lmdb (branch `mdb.master.nested-rtxns`)
Prepared 2026-07-17 by the zerodb project. A human must review and file this.
Full local context: `docs/UPSTREAM-BUGS.md` FORK-1.

---

**Title:** SIGSEGV in `_mdb_cursor_put` — `MDB_APPEND` returning `MDB_KEYEXIST`
after an `mdb_drop(..., 0)` of another DBI in the same write txn

**Environment**

- Fork: `meilisearch/lmdb`, branch `mdb.master.nested-rtxns`, commit
  `cd767228d31285a73af44f7fb15267e95d1df86f` (the submodule pinned by
  `lmdb-master-sys 0.2.6` / heed `v0.22.1`)
- Reproduced on macOS aarch64, plain (non-sanitizer) debug build; also under
  ASan, which reports `SEGV on unknown address 0x00000000000a` at
  `_mdb_cursor_put+0x8b8`, called from `mdb_put`.

**Summary**

A write transaction that (1) clears a second, freshly-created database and
(2) performs a successful `MDB_APPEND` put into a *committed* database, then
(3) performs a second `MDB_APPEND` put whose key is **not** greater than the
last key, crashes instead of returning `MDB_KEYEXIST`.

Expected: `MDB_KEYEXIST` (the documented `MDB_APPEND` misuse result).
Actual: segmentation fault inside `_mdb_cursor_put`.

**Minimal recipe** (all four ingredients required; verified by isolation)

1. Write txn A: create named DB `X`; commit.
2. Write txn B: create a second DB `Y`; `clear(Y)`.
3. Still in txn B: `MDB_APPEND` put `(k1, v)` into `X` — succeeds.
4. Still in txn B: `MDB_APPEND` put `(k2, v')` into `X` with `k2 <= k1` —
   **SIGSEGV**.

Value sizes are irrelevant (4 KiB and 1 MB both crash). Removing the `clear`,
removing the first successful append, or using an ordered key all avoid it.

**Reproducer**

A standalone Rust reproducer (via heed 0.22.1) lives at
`crates/zerodb-oracle/examples/fork_segv_repro.rs` in the zerodb repo:
`VARIANT=with_clear cargo run --example fork_segv_repro` → exit 139.
Happy to port it to a plain C test case against `mtest`-style harness if that
is more useful for triage.

**How it was found**

Differential fuzzing of a reimplementation against this fork as the reference
oracle. The combination is plausible in real Meilisearch indexing workloads
(milli uses both `clear` and append-mode puts inside single write txns), which
is why we are reporting rather than only guarding it locally.
