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
