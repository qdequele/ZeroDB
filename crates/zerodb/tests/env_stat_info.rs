//! Milestone 2.1 — `Env::stat()` and the completed `Env::info()`.
//!
//! Phase 2 acceptance: "differential semantics tests where LMDB has the
//! feature, and doc + unit tests where it's zerodb-defined". This file is the
//! **zerodb-defined** half. The cross-engine half (`last_txnid`, `entries`,
//! `num_readers`, `max_readers` — the fields whose values are format-
//! independent) lives in `crates/zerodb-oracle/tests/env_info_differential.rs`.
//!
//! The page counts (`branch_pages` / `leaf_pages` / `overflow_pages`) are
//! format-specific by construction (D-002), so they are **not** compared to
//! LMDB. They are instead pinned to ZeroDB's own truth two ways:
//!
//!   1. `check::check_image` walks the committed image and validates the stored
//!      counters against the real tree (INV-18) — a stat that disagreed with
//!      the walk would fail the walk.
//!   2. `Env::stat()` must equal `main_database().stat(&rtxn)` exactly — the
//!      env-level and per-DB views of the same tree cannot disagree.
//!
//! Do not weaken these (CLAUDE.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvOpenOptions};

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
        let path = std::env::temp_dir().join(format!("zerodb-envstat-{pid}-{nanos}-{seq}"));
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

const MAP: usize = 16 << 20;

fn open_with(dir: &Path, psize: u32, max_dbs: u32, max_readers: u32) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(psize);
    opts.max_dbs(max_dbs);
    opts.max_readers(max_readers);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path, psize: u32) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, psize);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

// ---------------------------------------------------------------------------
// Env::stat()
// ---------------------------------------------------------------------------

#[test]
fn env_stat_on_a_fresh_env_is_an_empty_main_tree() {
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 32);
    let s = env.stat();
    assert_eq!(s.page_size, 4096, "page_size mirrors the created geometry");
    assert_eq!(s.depth, 0, "empty main tree has depth 0 (MDB_stat parity)");
    assert_eq!(s.branch_pages, 0);
    assert_eq!(s.leaf_pages, 0);
    assert_eq!(s.overflow_pages, 0);
    assert_eq!(s.entries, 0);
}

#[test]
fn env_stat_equals_main_database_stat() {
    // The env-level and per-DB views of the *same* main tree must agree field
    // for field, at every shape: empty, single leaf, and a split catalog.
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 400, 32);

    for batch in [0usize, 1, 3, 40, 300] {
        {
            let mut w = env.write_txn().unwrap();
            for i in 0..batch {
                let name = format!("database-number-{i:04}");
                let db = env.create_database(&mut w, Some(name.as_bytes())).unwrap();
                db.put(&mut w, b"k", b"v").unwrap();
            }
            w.commit().unwrap();
        }
        let r = env.read_txn().unwrap();
        let per_db = env.main_database().stat(&r).unwrap();
        drop(r);
        let env_level = env.stat();
        assert_eq!(env_level.depth, per_db.depth, "depth at batch={batch}");
        assert_eq!(
            env_level.branch_pages, per_db.branch_pages,
            "branch_pages at batch={batch}"
        );
        assert_eq!(
            env_level.leaf_pages, per_db.leaf_pages,
            "leaf_pages at batch={batch}"
        );
        assert_eq!(
            env_level.overflow_pages, per_db.overflow_pages,
            "overflow_pages at batch={batch}"
        );
        assert_eq!(
            env_level.entries, per_db.entries,
            "entries at batch={batch}"
        );
    }
    assert_clean(dir.path(), 4096);
}

#[test]
fn env_stat_entries_counts_named_dbs_and_depth_grows() {
    // `MDB_stat` over the main DB of a named-DB env counts catalog records.
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 300, 32);
    {
        let mut w = env.write_txn().unwrap();
        for i in 0..200u32 {
            let name = format!("database-number-{i:04}");
            let db = env.create_database(&mut w, Some(name.as_bytes())).unwrap();
            db.put(&mut w, b"k", b"v").unwrap();
        }
        w.commit().unwrap();
    }
    let s = env.stat();
    assert_eq!(s.entries, 200, "one catalog entry per named DB");
    assert!(
        s.depth >= 2,
        "catalog should have split (depth {})",
        s.depth
    );
    assert!(s.leaf_pages >= 1, "a non-empty tree has leaves");
    assert!(
        s.branch_pages >= 1,
        "a depth-{} tree has branch pages",
        s.depth
    );
    assert_clean(dir.path(), 4096);
}

#[test]
fn env_stat_does_not_consume_a_reader_slot() {
    // `Env::stat()` reads the published snapshot, not a read txn. With every
    // reader slot occupied it must still return real numbers — the Phase-1
    // adapter implementation silently degraded to zeros here.
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 2);
    {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"a")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    let _r1 = env.read_txn().unwrap();
    let _r2 = env.read_txn().unwrap();
    // The table is now full; a third read txn would be ReadersFull.
    assert!(
        env.read_txn().is_err(),
        "precondition: reader table is full"
    );
    let s = env.stat();
    assert_eq!(s.entries, 1, "stat works with the reader table exhausted");
    assert_eq!(s.page_size, 4096);
}

#[test]
fn env_stat_page_size_reflects_the_created_geometry() {
    for psize in [4096u32, 8192, 65536] {
        let dir = TempDir::new();
        let env = open_with(dir.path(), psize, 8, 32);
        assert_eq!(env.stat().page_size, psize);
        assert_eq!(env.page_size(), psize);
    }
}

// ---------------------------------------------------------------------------
// Env::info()
// ---------------------------------------------------------------------------

#[test]
fn env_info_map_size_is_unchanged_from_phase_1() {
    // Regression guard: the one field consumers actually read (SPEC 00 row 20).
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 32);
    assert_eq!(env.info().map_size, MAP as u64);
    assert_eq!(env.info().map_size, env.map_size());
}

#[test]
fn env_info_last_txnid_advances_once_per_commit() {
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 32);
    let start = env.info().last_txnid;
    assert_eq!(start, env.txnid(), "info().last_txnid == Env::txnid()");
    for n in 1..=5u64 {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"a")).unwrap();
        db.put(&mut w, format!("k{n}").as_bytes(), b"v").unwrap();
        w.commit().unwrap();
        assert_eq!(
            env.info().last_txnid,
            start + n,
            "last_txnid must advance by exactly one per commit"
        );
    }
    // An aborted write txn must not advance it.
    let before = env.info().last_txnid;
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"a")).unwrap();
    db.put(&mut w, b"zzz", b"v").unwrap();
    w.abort();
    assert_eq!(env.info().last_txnid, before, "abort does not advance");
}

#[test]
fn env_info_last_pgno_is_the_snapshot_high_water() {
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 32);
    let start = env.info().last_pgno;
    {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"a")).unwrap();
        for i in 0..2000u32 {
            db.put(&mut w, format!("k{i:06}").as_bytes(), &[b'v'; 64])
                .unwrap();
        }
        w.commit().unwrap();
    }
    let after = env.info().last_pgno;
    assert!(
        after > start,
        "last_pgno must grow as pages are allocated ({start} -> {after})"
    );
    // It is a page id, so it must stay inside the map (SPEC 05 GC-15).
    assert!(
        after < MAP as u64 / 4096,
        "last_pgno {after} outside the map"
    );
    // And it must be consistent with the file actually on disk: the high-water
    // page has been written, so the file covers it.
    let disk = env.real_disk_size().unwrap();
    assert!(
        disk >= (after + 1) * 4096,
        "file ({disk} B) must cover last_pgno {after}"
    );
}

#[test]
fn env_info_max_readers_is_the_configured_table_size() {
    for configured in [1u32, 8, 126, 1024] {
        let dir = TempDir::new();
        let env = open_with(dir.path(), 4096, 8, configured);
        assert_eq!(env.info().max_readers, configured);
    }
}

#[test]
fn env_info_live_readers_tracks_live_read_txns() {
    // `live_readers` is the ZeroDB extension: the count that actually follows
    // the reader population. (`num_readers` is the LMDB-parity high-water mark
    // — see `env_info_num_readers_is_a_high_water_mark` and D-011.)
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 16);
    assert_eq!(env.info().live_readers, 0, "no readers on a fresh env");
    let r1 = env.read_txn().unwrap();
    assert_eq!(env.info().live_readers, 1);
    let r2 = env.read_txn().unwrap();
    let r3 = env.read_txn().unwrap();
    assert_eq!(env.info().live_readers, 3);
    drop(r2);
    assert_eq!(env.info().live_readers, 2, "a dropped txn frees its slot");
    drop(r1);
    drop(r3);
    assert_eq!(env.info().live_readers, 0);
    // Neither counter ever exceeds max_readers, even at exhaustion.
    let held: Vec<_> = (0..16).map(|_| env.read_txn().unwrap()).collect();
    let i = env.info();
    assert_eq!(i.live_readers, 16);
    assert!(i.live_readers <= i.max_readers);
    assert!(i.num_readers <= i.max_readers);
    assert!(env.read_txn().is_err(), "table exhausted");
    drop(held);
    assert_eq!(env.info().live_readers, 0);
}

#[test]
fn env_info_num_readers_is_a_high_water_mark() {
    // LMDB parity (D-011): `MDB_envinfo::me_numreaders` is monotone — it is the
    // maximum number of simultaneously live readers ever observed, and ending a
    // read txn does not lower it. The cross-engine proof is in
    // `zerodb-oracle/tests/env_info_differential.rs`; this pins the native side.
    let dir = TempDir::new();
    let env = open_with(dir.path(), 4096, 8, 32);
    assert_eq!(env.info().num_readers, 0);
    {
        let _held: Vec<_> = (0..5).map(|_| env.read_txn().unwrap()).collect();
        assert_eq!(env.info().num_readers, 5);
    }
    // All released — the high-water stays put, the live count drops to zero.
    let i = env.info();
    assert_eq!(i.num_readers, 5, "high-water must not decrease");
    assert_eq!(i.live_readers, 0);
    // A smaller reader population does not lower it either.
    {
        let _one = env.read_txn().unwrap();
        let i = env.info();
        assert_eq!(i.num_readers, 5, "still the high-water");
        assert_eq!(i.live_readers, 1);
    }
    // A larger one raises it.
    {
        let _held: Vec<_> = (0..9).map(|_| env.read_txn().unwrap()).collect();
        assert_eq!(env.info().num_readers, 9, "high-water rises to a new peak");
    }
    assert_eq!(env.info().num_readers, 9);
}

#[test]
fn env_info_is_stable_across_a_reopen() {
    // Everything except the reader counters is snapshot state, so it must
    // survive a close/reopen of the same store.
    let dir = TempDir::new();
    let (txnid, pgno) = {
        let env = open_with(dir.path(), 8192, 8, 32);
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"a")).unwrap();
        for i in 0..500u32 {
            db.put(&mut w, format!("k{i:06}").as_bytes(), b"v").unwrap();
        }
        w.commit().unwrap();
        let i = env.info();
        (i.last_txnid, i.last_pgno)
    };
    let env = open_with(dir.path(), 8192, 8, 32);
    let i = env.info();
    assert_eq!(i.last_txnid, txnid);
    assert_eq!(i.last_pgno, pgno);
    assert_eq!(
        i.num_readers, 0,
        "reader state is per-process, not persisted"
    );
    assert_eq!(i.live_readers, 0);
    assert_eq!(env.stat().page_size, 8192);
}
