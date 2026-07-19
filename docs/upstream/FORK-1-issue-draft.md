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
