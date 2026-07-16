//! Milestone 1.4 coverage pass, area 7: real-OS-thread concurrency smoke
//! (NOT the M1.8 reader table — that milestone tests lock-free reader-slot
//! contention specifically; this is the much simpler TXN-6/7/9/11 surface
//! that M1.4 already claims). No existing test spins a second OS thread.
//!
//! 1. Two threads racing `Env::write_txn()` must serialize on the
//!    in-process write mutex (TXN-6/7): no lost update, no torn/interleaved
//!    write, exactly `threads * puts_per_thread` entries land.
//! 2. A `RoTxn` opened before a commit on another thread must keep reading
//!    the pre-commit snapshot after that commit completes (TXN-4/11: a
//!    reader's pin does not move once taken).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use zerodb::{check, Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-concurrency-{pid}-{seq}"));
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

const PS: u32 = 4096;
const MAP: usize = 16 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

#[test]
fn racing_write_txns_serialize_no_lost_updates() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let n_threads = 6usize;
    let puts_per_thread = 80u32;
    // A barrier so every thread actually contends for `write_txn()` at
    // roughly the same instant, rather than trivially queueing up one at a
    // time by launch order.
    let barrier = Arc::new(Barrier::new(n_threads));

    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let env = env.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let db = env.main_database();
                let mut wtxn = env.write_txn().expect("write_txn under contention");
                for i in 0..puts_per_thread {
                    let k = format!("t{t}-k{i:04}").into_bytes();
                    db.put(&mut wtxn, &k, format!("v{t}-{i}").as_bytes())
                        .unwrap();
                }
                wtxn.commit().expect("commit under contention");
            })
        })
        .collect();
    for h in handles {
        h.join().expect("writer thread panicked");
    }

    assert_clean(dir.path());
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.len(&rtxn).unwrap(),
        (n_threads as u64) * (puts_per_thread as u64),
        "every writer's puts must have landed exactly once (TXN-6/7 serialization)"
    );
    for t in 0..n_threads {
        for i in 0..puts_per_thread {
            let k = format!("t{t}-k{i:04}").into_bytes();
            assert_eq!(
                db.get(&rtxn, &k).unwrap(),
                Some(format!("v{t}-{i}").as_bytes()),
                "missing entry from thread {t}"
            );
        }
    }
    // Sequential commit ids: N racing single-page-ish txns produce N
    // sequential txnids (TXN-2), never fewer (no commit silently dropped)
    // nor more (no phantom commit).
    assert_eq!(env.txnid(), n_threads as u64);
}

#[test]
fn ro_txn_opened_before_concurrent_commit_keeps_pre_commit_snapshot() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"before", b"v0").unwrap();
        wtxn.commit().unwrap();
    }

    // Open the reader *before* spawning the writer thread, and synchronize
    // so the writer's commit only proceeds once the reader is confirmed
    // open and has already taken its first read (pinning the pre-commit
    // snapshot, TXN-3/4).
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"before").unwrap(), Some(b"v0".as_slice()));
    assert_eq!(rtxn.txnid(), 1);

    let reader_ready = Arc::new(Barrier::new(2));
    let writer_done = Arc::new(Barrier::new(2));
    let env2 = env.clone();
    let reader_ready2 = Arc::clone(&reader_ready);
    let writer_done2 = Arc::clone(&writer_done);
    let handle = thread::spawn(move || {
        let db = env2.main_database();
        reader_ready2.wait(); // wait for the main thread's reader to be pinned
        let mut wtxn = env2.write_txn().unwrap();
        db.put(&mut wtxn, b"before", b"v1-overwritten").unwrap();
        db.put(&mut wtxn, b"only-after-commit", b"new").unwrap();
        wtxn.commit().unwrap();
        writer_done2.wait(); // signal the commit is fully durable + published
    });

    reader_ready.wait();
    writer_done.wait();
    handle.join().expect("writer thread panicked");

    // The already-open RoTxn must still see the pre-commit state (TXN-4/11:
    // readers never block, and never see a commit that started after they
    // pinned) — even though the writer's commit is now fully durable.
    assert_eq!(
        db.get(&rtxn, b"before").unwrap(),
        Some(b"v0".as_slice()),
        "an already-open RoTxn must not observe a later commit's overwrite"
    );
    assert_eq!(db.get(&rtxn, b"only-after-commit").unwrap(), None);
    assert_eq!(rtxn.txnid(), 1);

    // A *fresh* reader opened after the commit sees the new state.
    let rtxn2 = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn2, b"before").unwrap(),
        Some(b"v1-overwritten".as_slice())
    );
    assert_eq!(
        db.get(&rtxn2, b"only-after-commit").unwrap(),
        Some(b"new".as_slice())
    );
    assert_eq!(rtxn2.txnid(), 2);
    drop(rtxn);
    assert_clean(dir.path());
}
