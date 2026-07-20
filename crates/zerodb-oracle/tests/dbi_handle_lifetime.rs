//! Regression corpus for the **dbi-handle lifetime** rule (found 2026-07-20 by
//! the `ZERODB_FUZZ_PAIR=heed` differential fuzzer, two artifacts).
//!
//! ## The rule
//!
//! LMDB, `lmdb.h` on `mdb_dbi_open`:
//!
//! > "The database handle will be private to the current transaction until the
//! > transaction is successfully committed. If the transaction is aborted the
//! > handle will be closed automatically."
//!
//! `mdb.c` implements it in `mdb_dbis_update(txn, keep)`, called from
//! `mdb_txn_end`: on the non-commit path (`keep == 0`) every dbi flagged
//! `DB_NEW` — i.e. opened in this txn — has `me_dbflags[i]` cleared and
//! `me_dbiseqs[i]` bumped. Any later use of that handle then fails the
//! `TXN_DBI_EXIST(txn, dbi, DB_VALID)` gate at the top of `mdb_cursor_open`
//! (and `TXN_DBI_EXIST(.., DB_USRVALID)` in `mdb_put`) and returns **`EINVAL`**.
//!
//! Separately, `mdb_drop(txn, dbi, del=1)` calls `mdb_dbi_close(env, dbi)`
//! directly — an **env-level** close that a subsequent abort does *not* undo.
//!
//! ## What the fuzzer actually found
//!
//! Both artifacts were **harness** defects, not engine bugs. The oracle tracked
//! "which handles survive a rollback" with a positional watermark
//! (`committed_dbs`) into the `dbs` vec, which is only sound while that vec is
//! append-only. `drop_db` removes from the middle, and after that
//! `dbs.truncate(committed_dbs)` retained the wrong set: it kept a handle whose
//! creating txn was aborted (dead under the rule above) while discarding a
//! committed one. The harness then issued a use-after-close; LMDB correctly
//! answered `EINVAL`, ZeroDB — whose `Database` is a plain value, not an
//! env-level dbi slot carrying a validity flag and sequence number — served the
//! request. The watermark is now a per-entry `committed` flag.
//!
//! These tests pin the exact op shapes so the harness cannot regress into
//! generating that use-after-close again.

use zerodb_oracle::{run, DbName, HeedZerodbEngine, Key, LmdbEngine, Op, Value, ZerodbEngine};

/// Assert both engine pairs agree on `ops` — the native pair and the adapter
/// pair, since the harness bookkeeping is shared by all three engines.
fn diff_both_pairs(ops: &[Op]) {
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(ops) {
        panic!("native divergence:\n{d}");
    }
    if let Err(d) = run::<LmdbEngine, HeedZerodbEngine>(ops) {
        panic!("adapter divergence:\n{d}");
    }
}

fn k(s: &str) -> Key {
    Key(s.as_bytes().to_vec())
}
fn v(s: &str) -> Value {
    Value(s.as_bytes().to_vec())
}

/// Artifact `crash-c48149179fae…`, minimized: commit a named DB, open a second
/// DB in a txn that is never committed, drop the **committed** one, then abort.
/// The abort closes the uncommitted handle (`mdb_dbis_update` keep=0) *and* the
/// dropped handle stays closed (`mdb_drop` → `mdb_dbi_close`), so no usable
/// handle survives. The cursor path (`iter_mut` → `mdb_cursor_open`) is what
/// returned `EINVAL`.
#[test]
fn cursor_after_abort_of_txn_that_opened_the_handle() {
    let ops = vec![
        // Committed named DB.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(40),
        },
        Op::Commit,
        // A second DB opened in a txn that never commits.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        // Drop the committed one from the middle of the harness's list.
        Op::DropDb { db: 0 },
        // Rollback: closes the uncommitted handle; the dropped dbi stays closed.
        Op::Abort,
        // Fresh txn — the write-cursor path that produced EINVAL.
        Op::BeginRw,
        Op::IterMutPutCurrent {
            db: 45,
            nth: 1,
            val: v("after-abort"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// Artifact `crash-580edc9bd18e…`, minimized: the same stale-handle situation
/// reached through `mdb_put` instead of a cursor. The reported key was an
/// ordinary 14-byte key — the size was a red herring; `mdb_put`'s very first
/// check is the same `TXN_DBI_EXIST` gate, so a dead dbi yields `EINVAL`
/// regardless of key or value.
#[test]
fn put_after_abort_of_txn_that_opened_the_handle() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(255),
        },
        Op::Commit,
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::DropDb { db: 0 },
        Op::Abort,
        Op::BeginRw,
        Op::Put {
            db: 226,
            key: k("an-ordinary-key"),
            val: v("an-ordinary-value"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// The positive control the watermark used to get wrong in the other direction:
/// a handle whose creating txn **committed** must stay usable across a later
/// unrelated abort.
#[test]
fn committed_handle_survives_a_later_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k("k"),
            val: v("v"),
        },
        Op::Commit,
        // An unrelated txn that rolls back.
        Op::BeginRw,
        Op::Put {
            db: 0,
            key: k("rolled-back"),
            val: v("x"),
        },
        Op::Abort,
        // The committed handle is still good.
        Op::BeginRw,
        Op::Get { db: 0, key: k("k") },
        Op::Put {
            db: 0,
            key: k("k2"),
            val: v("v2"),
        },
        Op::Len { db: 0 },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// `mdb_drop(.., del=1)` closes the dbi at env level, and an abort does **not**
/// resurrect it — even though the abort *does* roll back the deletion of the
/// database itself. Pins that asymmetry.
#[test]
fn dropped_handle_is_not_resurrected_by_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(7),
        },
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Abort,
        // Both engines must agree about what is reachable now.
        Op::BeginRw,
        Op::Len { db: 0 },
        Op::Len { db: 1 },
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// Drop + abort + re-create under the same name: the re-created handle is a
/// fresh dbi and must behave identically on both engines.
#[test]
fn recreate_after_drop_and_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(3),
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Abort,
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(3),
        },
        Op::Put {
            db: 0,
            key: k("fresh"),
            val: v("entry"),
        },
        Op::Commit,
        Op::BeginRo,
        Op::Get {
            db: 0,
            key: k("fresh"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}
