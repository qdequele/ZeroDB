//! Milestone 2.2 — reader introspection (`Env::reader_list`) and
//! `Env::clear_stale_readers`.
//!
//! Phase 2 acceptance: "differential semantics tests where LMDB has the
//! feature, and doc + unit tests where it's zerodb-defined". There is **no
//! differential half** here, and that is not an omission: heed exposes no
//! reader introspection at all, so there is no through-heed path to drive C
//! LMDB's `mdb_reader_list`, and LMDB's row shape (pid/thread columns over a
//! shared `lock.mdb`) is meaningless under D-001 single-process anyway. The
//! quantities that *are* cross-engine comparable — `max_readers` and the
//! `me_numreaders` high-water mark — are already covered differentially by
//! `zerodb-oracle/tests/env_info_differential.rs` (M2.1).
//!
//! What is pinned here is ZeroDB's own contract, cross-checked against the
//! reader-table facts M2.1 already exposes (`live_readers`, `num_readers`)
//! so the two views of the same table cannot disagree.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use zerodb::{Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-rdrlist-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn open(dir: &Path, max_readers: u32) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(8 << 20);
    opts.max_dbs(4);
    opts.max_readers(max_readers);
    opts.open(dir).expect("open env")
}

/// Commit one no-op-ish write so the env's txnid advances by exactly 1.
fn bump(env: &Env, key: &[u8]) {
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, key, b"v").unwrap();
    w.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Shape and emptiness
// ---------------------------------------------------------------------------

#[test]
fn no_readers_means_an_empty_list() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    assert!(
        env.reader_list().is_empty(),
        "a fresh env with no open read txn lists no readers"
    );
    // ... and stays empty across commits, which touch no reader slot.
    bump(&env, b"a");
    assert!(env.reader_list().is_empty());
}

#[test]
fn one_read_txn_appears_and_disappears() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    bump(&env, b"a");
    let at_open = env.txnid();

    {
        let _r = env.read_txn().unwrap();
        let list = env.reader_list();
        assert_eq!(list.len(), 1, "exactly one occupied slot");
        let e = list[0];
        assert!(e.slot < 16, "slot index is within the table");
        assert_eq!(
            e.txnid,
            Some(at_open),
            "a live reader pins the current commit point"
        );
        assert_eq!(e.age, Some(0), "freshly opened reader has age 0");
    }

    assert!(
        env.reader_list().is_empty(),
        "dropping the RoTxn releases its slot, so it leaves the list"
    );
}

// ---------------------------------------------------------------------------
// N live readers across threads
// ---------------------------------------------------------------------------

#[test]
fn n_live_read_txns_across_threads_are_all_listed() {
    const N: usize = 8;
    let dir = TempDir::new();
    let env = open(dir.path(), 32);
    bump(&env, b"a");

    // Two barriers: all readers open, then main observes, then all release.
    let opened = Arc::new(Barrier::new(N + 1));
    let release = Arc::new(Barrier::new(N + 1));

    std::thread::scope(|s| {
        for _ in 0..N {
            let env = &env;
            let opened = Arc::clone(&opened);
            let release = Arc::clone(&release);
            s.spawn(move || {
                let _r = env.read_txn().expect("read txn");
                opened.wait();
                // Hold the slot until main has observed the table.
                release.wait();
            });
        }

        opened.wait();
        let list = env.reader_list();
        assert_eq!(list.len(), N, "every live reader occupies a listed slot");
        assert_eq!(
            list.len() as u32,
            env.info().live_readers,
            "reader_list and EnvInfo::live_readers are two views of one table \
             and must agree while it is quiescent"
        );

        let mut slots: Vec<u32> = list.iter().map(|e| e.slot).collect();
        let sorted = slots.clone();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots.len(), N, "slot indices are distinct");
        assert_eq!(sorted, slots, "entries are returned in slot order");

        for e in &list {
            assert!(e.txnid.is_some(), "a settled reader has published its pin");
            assert_eq!(e.age, Some(0));
        }

        release.wait();
    });

    assert!(
        env.reader_list().is_empty(),
        "all slots are released once every reader thread has finished"
    );
    assert_eq!(env.info().live_readers, 0);
    assert_eq!(
        env.info().num_readers,
        N as u32,
        "the high-water mark does NOT decrease (D-011) — reader_list does"
    );
}

// ---------------------------------------------------------------------------
// Ages advance as commits land
// ---------------------------------------------------------------------------

#[test]
fn age_advances_by_one_per_commit_while_a_reader_is_pinned() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    bump(&env, b"seed");

    let pinned = env.txnid();
    let r = env.read_txn().unwrap();

    for expected_age in 0..5u64 {
        let list = env.reader_list();
        assert_eq!(list.len(), 1);
        assert_eq!(
            list[0].txnid,
            Some(pinned),
            "the pin never moves: a read txn is a fixed snapshot"
        );
        assert_eq!(
            list[0].age,
            Some(expected_age),
            "age = published txnid - pinned txnid, and each commit adds one"
        );
        bump(&env, format!("k{expected_age}").as_bytes());
    }

    // Age is exactly the arithmetic it claims to be.
    let list = env.reader_list();
    assert_eq!(list[0].age, Some(env.txnid() - pinned));

    drop(r);
    assert!(env.reader_list().is_empty());
}

#[test]
fn two_readers_at_different_snapshots_report_different_ages() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    bump(&env, b"seed");

    let old = env.read_txn().unwrap();
    let old_txnid = env.txnid();
    bump(&env, b"a");
    bump(&env, b"b");
    let young = env.read_txn().unwrap();
    let young_txnid = env.txnid();
    assert_eq!(young_txnid, old_txnid + 2);

    let list = env.reader_list();
    assert_eq!(list.len(), 2);
    let mut ages: Vec<u64> = list.iter().map(|e| e.age.expect("pinned")).collect();
    ages.sort_unstable();
    assert_eq!(
        ages,
        vec![0, 2],
        "the older reader is exactly 2 commits behind; the newer is current"
    );

    // The oldest listed pin is the one holding GC back.
    let oldest = list.iter().filter_map(|e| e.txnid).min().unwrap();
    assert_eq!(oldest, old_txnid);

    drop(old);
    drop(young);
}

// ---------------------------------------------------------------------------
// static_read_txn and nested read txns
// ---------------------------------------------------------------------------

#[test]
fn a_static_read_txn_occupies_a_listed_slot() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    bump(&env, b"seed");

    let r = env.clone().static_read_txn().unwrap();
    let list = env.reader_list();
    assert_eq!(
        list.len(),
        1,
        "an env-owning 'static read txn is an ordinary reader-table occupant"
    );
    assert_eq!(list[0].txnid, Some(env.txnid()));
    drop(r);
    assert!(env.reader_list().is_empty());
}

// ---------------------------------------------------------------------------
// clear_stale_readers
// ---------------------------------------------------------------------------

#[test]
fn clear_stale_readers_is_zero_and_never_disturbs_live_readers() {
    let dir = TempDir::new();
    let env = open(dir.path(), 16);
    bump(&env, b"seed");

    assert_eq!(env.clear_stale_readers().unwrap(), 0, "no readers at all");

    let r = env.read_txn().unwrap();
    let pinned = env.txnid();
    assert_eq!(
        env.clear_stale_readers().unwrap(),
        0,
        "a LIVE reader is not stale — under D-001 there is no such thing as a \
         stale reader, so 0 is the correct answer, not a stub"
    );
    // The critical property: the call must not have freed the live slot.
    let list = env.reader_list();
    assert_eq!(list.len(), 1, "the live reader still holds its slot");
    assert_eq!(list[0].txnid, Some(pinned));
    // ... and the reader still reads.
    assert!(env.main_database().get(&r, b"seed").unwrap().is_some());
    drop(r);

    assert_eq!(env.clear_stale_readers().unwrap(), 0);
}

// ---------------------------------------------------------------------------
// Table capacity relationship
// ---------------------------------------------------------------------------

#[test]
fn free_slot_count_is_max_readers_minus_list_len() {
    let dir = TempDir::new();
    let env = open(dir.path(), 4);
    bump(&env, b"seed");

    let a = env.read_txn().unwrap();
    let b = env.read_txn().unwrap();
    let info = env.info();
    assert_eq!(info.max_readers, 4);
    assert_eq!(
        info.max_readers as usize - env.reader_list().len(),
        2,
        "free slots = capacity - occupied; the docs promise this identity \
         because free slots are omitted from the list"
    );

    // Exhausting the table is still ReadersFull, and the list shows why.
    let c = env.read_txn().unwrap();
    let d = env.read_txn().unwrap();
    assert_eq!(env.reader_list().len(), 4);
    assert!(
        env.read_txn().is_err(),
        "table full (SPEC 04 TXN-16) — reader_list showing 4 of 4 is the \
         diagnostic that explains it"
    );
    drop((a, b, c, d));
    assert!(env.reader_list().is_empty());
}
