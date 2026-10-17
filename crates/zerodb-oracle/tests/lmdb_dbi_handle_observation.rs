//! What the Meilisearch LMDB fork does with a database handle (`dbi`) once the
//! transaction that opened it has ended, observed through heed 0.22.1, the
//! API consumers call. Input for the design of issue #73 (divergence D-013, ADR-0024).
//! AGENTS.md rule 1: observed, not guessed.
//!
//! Each case is a short script of heed calls; every step's outcome is recorded
//! as a string (`ok`, a value, `none`, or the error: `EINVAL` for errno 22, `BAD_DBI`
//! for `MDB_BAD_DBI`).
//! The `OBS_*` constants pin LMDB's outcomes; a diff means the fork changed or
//! the pin was wrong, never edit a pin to make ZeroDB pass. Pins observed on
//! macOS (Darwin 27.0.0, aarch64), 2026-10-17. ZeroDB runs the
//! same scripts and its outcomes are printed beside LMDB's (`--nocapture`),
//! not asserted: they differ today, and the design decides which must match.

use zerodb_oracle::tempdir::TempDir;

/// A newly created database `a`, its creating write txn aborted: put and get
/// through the handle in a new write txn, commit, then whether `a` exists.
const OBS_CREATE_ABORTED: &[&str] = &["EINVAL", "EINVAL", "ok", "none"];
/// Created then aborted, used from a read txn.
const OBS_CREATE_ABORTED_READ: &[&str] = &["EINVAL"];
/// Created as `h1` in an aborted txn, re-created as `h2` and committed with
/// `k = 2`: get through `h1` from a read txn, then put `k = 1` through `h1` in
/// a write txn, commit, and read `k` through `h2`.
const OBS_CREATE_ABORTED_THEN_RECREATE: &[&str] = &["2", "ok", "1"];
/// An existing database opened for the first time since the env opened, in a
/// write txn that aborts: get through the handle from a new read txn.
const OBS_FIRST_OPEN_IN_ABORTED_WRITE: &[&str] = &["EINVAL"];
/// The same first open in a read txn that is dropped (aborted): get through
/// the handle from a new read txn. A read txn's abort closes the handles it
/// opened, as a write txn's does.
const OBS_FIRST_OPEN_IN_DROPPED_READ: &[&str] = &["EINVAL"];
/// The same first open in a read txn that is committed (`RoTxn::commit`).
const OBS_FIRST_OPEN_IN_COMMITTED_READ: &[&str] = &["v"];
/// A database already open in the env, opened again in a write txn that
/// aborts: get through the second handle.
const OBS_REOPEN_IN_ABORTED_WRITE: &[&str] = &["v"];
/// A committed database removed (`Database::remove`) and committed: put
/// through the old handle, then whether `a` exists.
const OBS_REMOVE_COMMITTED: &[&str] = &["ok", "EINVAL", "none"];
/// A committed database removed in a txn that aborts: get through the old
/// handle, whether `a` exists, and get through a freshly opened handle.
const OBS_REMOVE_ABORTED: &[&str] = &["ok", "EINVAL", "some", "v"];
/// A database created in a still-open write txn, used from a read txn begun
/// before the write txn commits, then (after the commit) from that read txn
/// again and from a new one.
const OBS_UNCOMMITTED_HANDLE_IN_READ: &[&str] = &["EINVAL", "EINVAL", "v"];
/// `a` created in an aborted txn, then `b` created and committed with
/// `k = b`: get through the stale `a` handle (does it reach `b`?), and
/// whether `a` exists. `b` takes the slot the abort freed, so the stale
/// handle reads `b`.
const OBS_CREATE_ABORTED_THEN_OTHER_NAME: &[&str] = &["b", "none"];
/// A read txn holding a committed handle while a write txn removes the
/// database and commits: get through the handle from that read txn, then
/// from a new one. The open read txn gets `MDB_BAD_DBI`, not its snapshot's
/// value.
const OBS_REMOVE_UNDER_OPEN_READ: &[&str] = &["BAD_DBI", "EINVAL"];
/// An existing database first opened inside a nested read txn of a write txn,
/// the child then dropped: get through the handle from the parent, then the
/// parent commits and a new read txn gets through it.
/// Dropping the child frees the handle, as any txn end without a commit does.
const OBS_FIRST_OPEN_IN_NESTED_READ_PARENT_COMMITS: &[&str] = &["EINVAL", "EINVAL"];
/// The same, but the parent aborts: get from a new read txn.
const OBS_FIRST_OPEN_IN_NESTED_READ_PARENT_ABORTS: &[&str] = &["EINVAL"];
/// The child committed (`RoTxn::commit`) instead of dropped: get from the
/// parent, then from a new read txn after the parent commits.
/// The child's commit publishes the handle for txns begun after it; the
/// parent, begun before, still gets `EINVAL`.
const OBS_NESTED_READ_COMMITTED_PARENT_COMMITS: &[&str] = &["EINVAL", "v"];
/// The child committed, then the parent aborts: get from a new read txn. The
/// parent's abort does not undo the child's publication.
const OBS_NESTED_READ_COMMITTED_PARENT_ABORTS: &[&str] = &["v"];
/// Two read txns, both begun, each first-open `a` (`h1`, `h2`); the first is
/// dropped: get through `h2` from its own txn, then that txn commits and new
/// read txns get through `h2` and through `h1`. Both handles are one slot:
/// the first txn's drop frees it under the second, whose commit then leaves
/// it valid in txns but with a changed sequence, so later txns get
/// `MDB_BAD_DBI` even through the committed handle.
const OBS_TWO_READERS_FIRST_OPEN: &[&str] = &["v", "BAD_DBI", "BAD_DBI"];
/// A committed database removed, then the handle used in the same write txn:
/// put through it.
const OBS_REMOVE_THEN_USE_SAME_TXN: &[&str] = &["EINVAL"];
/// Handles the parent write txn opened and has not committed, used from a
/// nested read child: `a` created in the parent (get through it in the
/// child), and an existing `b` first opened in the parent (get in the child).
/// The child sees every handle its parent can use.
const OBS_PARENT_HANDLES_IN_NESTED_READ: &[&str] = &["a", "b"];
/// A committed database cleared in a txn that aborts: get through the handle.
const OBS_CLEAR_ABORTED: &[&str] = &["ok", "v"];

fn outcome<T: std::fmt::Debug, E: std::fmt::Display>(r: Result<T, E>) -> String {
    match r {
        Ok(_) => "ok".into(),
        Err(e) => {
            let s = e.to_string();
            if s.contains("os error 22") {
                "EINVAL".into()
            } else if s.starts_with("MDB_BAD_DBI") {
                "BAD_DBI".into()
            } else {
                s
            }
        }
    }
}

fn value<E: std::fmt::Display>(r: Result<Option<&[u8]>, E>) -> String {
    match r {
        Ok(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        Ok(None) => "none".into(),
        Err(e) => outcome::<(), E>(Err(e)),
    }
}

macro_rules! cases {
    ($m:ident, $heed:ident) => {
        mod $m {
            use super::{outcome, value};
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvOpenOptions, WithoutTls};

            type Db = Database<Bytes, Bytes>;

            pub fn open(dir: &Path) -> Env<WithoutTls> {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.max_dbs(4);
                // SAFETY: a private temp dir, one process.
                unsafe { o.open(dir) }.expect("open env")
            }

            fn close(env: Env<WithoutTls>) {
                env.prepare_for_closing().wait();
            }

            fn exists(env: &Env<WithoutTls>) -> String {
                let r = env.read_txn().unwrap();
                match env.open_database::<Bytes, Bytes>(&r, Some("a")) {
                    Ok(Some(_)) => "some".into(),
                    Ok(None) => "none".into(),
                    Err(e) => outcome::<(), _>(Err(e)),
                }
            }

            fn committed(env: &Env<WithoutTls>) -> Db {
                let mut w = env.write_txn().unwrap();
                let db: Db = env.create_database(&mut w, Some("a")).unwrap();
                db.put(&mut w, b"k", b"v").unwrap();
                w.commit().unwrap();
                db
            }

            fn get(env: &Env<WithoutTls>, db: Db) -> String {
                let r = env.read_txn().unwrap();
                value(db.get(&r, b"k"))
            }

            /// A committed `a`, the env closed and reopened so no handle is open.
            fn reopened(dir: &Path) -> Env<WithoutTls> {
                let env = open(dir);
                committed(&env);
                close(env);
                open(dir)
            }

            pub fn create_aborted(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let w = {
                    let mut w = env.write_txn().unwrap();
                    let db: Db = env.create_database(&mut w, Some("a")).unwrap();
                    w.abort();
                    db
                };
                let mut t = env.write_txn().unwrap();
                let put = outcome(w.put(&mut t, b"k", b"v"));
                let got = value(w.get(&t, b"k"));
                let commit = outcome(t.commit());
                vec![put, got, commit, exists(&env)]
            }

            pub fn create_aborted_read(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let db: Db = env.create_database(&mut w, Some("a")).unwrap();
                w.abort();
                vec![get(&env, db)]
            }

            pub fn create_aborted_then_recreate(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let h1: Db = env.create_database(&mut w, Some("a")).unwrap();
                w.abort();
                let mut w = env.write_txn().unwrap();
                let h2: Db = env.create_database(&mut w, Some("a")).unwrap();
                h2.put(&mut w, b"k", b"2").unwrap();
                w.commit().unwrap();
                let read = get(&env, h1);
                let mut w = env.write_txn().unwrap();
                let put = outcome(h1.put(&mut w, b"k", b"1"));
                w.commit().unwrap();
                vec![read, put, get(&env, h2)]
            }

            pub fn first_open_in_aborted_write(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let w = env.write_txn().unwrap();
                let db: Db = env.open_database(&w, Some("a")).unwrap().unwrap();
                w.abort();
                vec![get(&env, db)]
            }

            pub fn first_open_in_dropped_read(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let r = env.read_txn().unwrap();
                let db: Db = env.open_database(&r, Some("a")).unwrap().unwrap();
                drop(r);
                vec![get(&env, db)]
            }

            pub fn first_open_in_committed_read(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let r = env.read_txn().unwrap();
                let db: Db = env.open_database(&r, Some("a")).unwrap().unwrap();
                r.commit().unwrap();
                vec![get(&env, db)]
            }

            pub fn reopen_in_aborted_write(dir: &Path) -> Vec<String> {
                let env = open(dir);
                committed(&env);
                let w = env.write_txn().unwrap();
                let db: Db = env.open_database(&w, Some("a")).unwrap().unwrap();
                w.abort();
                vec![get(&env, db)]
            }

            pub fn remove_committed(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let db = committed(&env);
                let mut w = env.write_txn().unwrap();
                // SAFETY: the handle is used again only to observe the result.
                let removed = outcome(unsafe { db.remove(&mut w) });
                w.commit().unwrap();
                let mut w = env.write_txn().unwrap();
                let put = outcome(db.put(&mut w, b"k", b"v"));
                w.abort();
                vec![removed, put, exists(&env)]
            }

            pub fn remove_aborted(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let db = committed(&env);
                let mut w = env.write_txn().unwrap();
                // SAFETY: as above.
                let removed = outcome(unsafe { db.remove(&mut w) });
                w.abort();
                let old = get(&env, db);
                let ex = exists(&env);
                let r = env.read_txn().unwrap();
                let fresh = match env.open_database::<Bytes, Bytes>(&r, Some("a")) {
                    Ok(Some(d)) => value(d.get(&r, b"k")),
                    Ok(None) => "none".into(),
                    Err(e) => outcome::<(), _>(Err(e)),
                };
                vec![removed, old, ex, fresh]
            }

            pub fn uncommitted_handle_in_read(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let db: Db = env.create_database(&mut w, Some("a")).unwrap();
                db.put(&mut w, b"k", b"v").unwrap();
                let r = env.read_txn().unwrap();
                let before = value(db.get(&r, b"k"));
                w.commit().unwrap();
                let after_same = value(db.get(&r, b"k"));
                drop(r);
                vec![before, after_same, get(&env, db)]
            }

            pub fn create_aborted_then_other_name(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let a: Db = env.create_database(&mut w, Some("a")).unwrap();
                w.abort();
                let mut w = env.write_txn().unwrap();
                let b: Db = env.create_database(&mut w, Some("b")).unwrap();
                b.put(&mut w, b"k", b"b").unwrap();
                w.commit().unwrap();
                vec![get(&env, a), exists(&env)]
            }

            pub fn remove_under_open_read(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let db = committed(&env);
                let r = env.read_txn().unwrap();
                let mut w = env.write_txn().unwrap();
                // SAFETY: as above.
                unsafe { db.remove(&mut w) }.unwrap();
                w.commit().unwrap();
                let old = value(db.get(&r, b"k"));
                drop(r);
                vec![old, get(&env, db)]
            }

            pub fn first_open_in_nested_read_parent_commits(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let w = env.write_txn().unwrap();
                let child = env.nested_read_txn(&w).unwrap();
                let db: Db = env.open_database(&child, Some("a")).unwrap().unwrap();
                drop(child);
                let in_parent = value(db.get(&w, b"k"));
                w.commit().unwrap();
                vec![in_parent, get(&env, db)]
            }

            pub fn first_open_in_nested_read_parent_aborts(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let w = env.write_txn().unwrap();
                let child = env.nested_read_txn(&w).unwrap();
                let db: Db = env.open_database(&child, Some("a")).unwrap().unwrap();
                drop(child);
                w.abort();
                vec![get(&env, db)]
            }

            pub fn nested_read_committed_parent_commits(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let w = env.write_txn().unwrap();
                let child = env.nested_read_txn(&w).unwrap();
                let db: Db = env.open_database(&child, Some("a")).unwrap().unwrap();
                child.commit().unwrap();
                let in_parent = value(db.get(&w, b"k"));
                w.commit().unwrap();
                vec![in_parent, get(&env, db)]
            }

            pub fn nested_read_committed_parent_aborts(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let w = env.write_txn().unwrap();
                let child = env.nested_read_txn(&w).unwrap();
                let db: Db = env.open_database(&child, Some("a")).unwrap().unwrap();
                child.commit().unwrap();
                w.abort();
                vec![get(&env, db)]
            }

            pub fn two_readers_first_open(dir: &Path) -> Vec<String> {
                let env = reopened(dir);
                let r1 = env.read_txn().unwrap();
                let r2 = env.read_txn().unwrap();
                let h1: Db = env.open_database(&r1, Some("a")).unwrap().unwrap();
                let h2: Db = env.open_database(&r2, Some("a")).unwrap().unwrap();
                drop(r1);
                let own = value(h2.get(&r2, b"k"));
                r2.commit().unwrap();
                vec![own, get(&env, h2), get(&env, h1)]
            }

            pub fn remove_then_use_same_txn(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let db = committed(&env);
                let mut w = env.write_txn().unwrap();
                // SAFETY: as above.
                unsafe { db.remove(&mut w) }.unwrap();
                let put = outcome(db.put(&mut w, b"k", b"v"));
                w.abort();
                vec![put]
            }

            pub fn parent_handles_in_nested_read(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let b: Db = env.create_database(&mut w, Some("b")).unwrap();
                b.put(&mut w, b"k", b"b").unwrap();
                w.commit().unwrap();
                close(env);
                let env = open(dir);
                let mut w = env.write_txn().unwrap();
                let a: Db = env.create_database(&mut w, Some("a")).unwrap();
                a.put(&mut w, b"k", b"a").unwrap();
                let b: Db = env.open_database(&w, Some("b")).unwrap().unwrap();
                let child = env.nested_read_txn(&w).unwrap();
                let out = vec![value(a.get(&child, b"k")), value(b.get(&child, b"k"))];
                drop(child);
                w.abort();
                out
            }

            pub fn clear_aborted(dir: &Path) -> Vec<String> {
                let env = open(dir);
                let db = committed(&env);
                let mut w = env.write_txn().unwrap();
                let cleared = outcome(db.clear(&mut w));
                w.abort();
                vec![cleared, get(&env, db)]
            }
        }
    };
}

cases!(lmdb, heed);
cases!(zerodb, heed_zerodb);

macro_rules! observe {
    ($test:ident, $pin:ident) => {
        #[test]
        fn $test() {
            let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
            let l = lmdb::$test(a.path());
            let z = zerodb::$test(b.path());
            println!(
                "{:32} lmdb {:?}  zerodb {:?}{}",
                stringify!($test),
                l,
                z,
                if l == z { "" } else { "  DIFFERS" }
            );
            assert_eq!(l, $pin, "LMDB outcome changed for {}", stringify!($test));
        }
    };
}

observe!(create_aborted, OBS_CREATE_ABORTED);
observe!(create_aborted_read, OBS_CREATE_ABORTED_READ);
observe!(
    create_aborted_then_recreate,
    OBS_CREATE_ABORTED_THEN_RECREATE
);
observe!(first_open_in_aborted_write, OBS_FIRST_OPEN_IN_ABORTED_WRITE);
observe!(first_open_in_dropped_read, OBS_FIRST_OPEN_IN_DROPPED_READ);
observe!(
    first_open_in_committed_read,
    OBS_FIRST_OPEN_IN_COMMITTED_READ
);
observe!(reopen_in_aborted_write, OBS_REOPEN_IN_ABORTED_WRITE);
observe!(remove_committed, OBS_REMOVE_COMMITTED);
observe!(remove_aborted, OBS_REMOVE_ABORTED);
observe!(uncommitted_handle_in_read, OBS_UNCOMMITTED_HANDLE_IN_READ);
observe!(clear_aborted, OBS_CLEAR_ABORTED);
observe!(
    create_aborted_then_other_name,
    OBS_CREATE_ABORTED_THEN_OTHER_NAME
);
observe!(remove_under_open_read, OBS_REMOVE_UNDER_OPEN_READ);
observe!(
    first_open_in_nested_read_parent_commits,
    OBS_FIRST_OPEN_IN_NESTED_READ_PARENT_COMMITS
);
observe!(
    first_open_in_nested_read_parent_aborts,
    OBS_FIRST_OPEN_IN_NESTED_READ_PARENT_ABORTS
);
observe!(
    nested_read_committed_parent_commits,
    OBS_NESTED_READ_COMMITTED_PARENT_COMMITS
);
observe!(
    nested_read_committed_parent_aborts,
    OBS_NESTED_READ_COMMITTED_PARENT_ABORTS
);
observe!(two_readers_first_open, OBS_TWO_READERS_FIRST_OPEN);
observe!(remove_then_use_same_txn, OBS_REMOVE_THEN_USE_SAME_TXN);
observe!(
    parent_handles_in_nested_read,
    OBS_PARENT_HANDLES_IN_NESTED_READ
);
