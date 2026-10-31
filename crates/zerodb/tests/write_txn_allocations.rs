//! Heap allocations per write transaction (issue #8).
//!
//! A write txn reuses the previous one's containers, carried on the env's
//! writer slot (`zerodb_core::rwtxn::WriterCarry`), as LMDB reuses its one
//! write txn (`me_txn0`). Before that, a single-put commit on a named
//! database made 19 heap allocations and an empty write txn one. This test
//! counts them with a global allocator that counts the current thread's
//! allocations only, so the test harness's other threads do not disturb it.
//!
//! The bounds leave room for what a commit still allocates on purpose: the
//! published snapshot and its free-list annex (two allocations), and the
//! vectored-write iovec lists of the file backend.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{EnvFlags, EnvOpenOptions};

struct CountingAlloc;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

fn note_alloc() {
    // `try_with`: the thread-local may already be gone during thread exit.
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            let _ = COUNT.try_with(|n| n.set(n.get() + 1));
        }
    });
}

// SAFETY: every method forwards to `System` unchanged; the counting touches
// only `const`-initialized thread-locals, which never allocate.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: the caller's contract is passed through to `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: as above.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as above.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_alloc();
        // SAFETY: as above.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Heap allocations `f` makes on this thread.
fn allocations(f: impl FnOnce()) -> u64 {
    COUNT.with(|n| n.set(0));
    COUNTING.with(|on| on.set(true));
    f();
    COUNTING.with(|on| on.set(false));
    COUNT.with(Cell::get)
}

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-allocs-{pid}-{seq}"));
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

#[test]
fn steady_state_write_txns_reuse_their_containers() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(1 << 26).max_dbs(16).flags(EnvFlags::NO_SYNC);
    let env = opts.open(dir.path()).expect("open env");
    let mut w = env.write_txn().expect("write_txn");
    let db = env.create_database(&mut w, Some(b"t")).expect("create");
    w.commit().expect("commit");
    let val = [0xABu8; 256];
    // Warm up: grow the carried containers and the frame pool, and reach
    // the steady state where every commit frees the pages it replaces.
    for i in 0..200u64 {
        let mut w = env.write_txn().expect("write_txn");
        db.put(&mut w, &i.to_be_bytes(), &val).expect("put");
        w.commit().expect("commit");
    }

    let empty_commit = allocations(|| {
        env.write_txn()
            .expect("write_txn")
            .commit()
            .expect("commit");
    });
    let empty_abort = allocations(|| drop(env.write_txn().expect("write_txn")));
    assert_eq!(empty_commit, 0, "an empty write txn allocates nothing");
    assert_eq!(
        empty_abort, 0,
        "an aborted empty write txn allocates nothing"
    );

    for i in 200..220u64 {
        let n = allocations(|| {
            let mut w = env.write_txn().expect("write_txn");
            db.put(&mut w, &i.to_be_bytes(), &val).expect("put");
            w.commit().expect("commit");
        });
        // 19 before the carry; the snapshot, its annex and the iovec lists
        // remain.
        assert!(n <= 8, "single-put commit {i} made {n} allocations");
    }
    for i in 0..20u64 {
        let n = allocations(|| {
            let mut w = env.write_txn().expect("write_txn");
            db.delete(&mut w, &(i * 7).to_be_bytes()).expect("delete");
            w.commit().expect("commit");
        });
        assert!(n <= 8, "single-delete commit {i} made {n} allocations");
    }
}

#[test]
fn a_large_txn_does_not_disturb_the_next_small_ones() {
    // A txn far past every carried capacity cap, then small ones: the caps
    // shrink what it grew, and the next txns still reuse what is left.
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(1 << 28).max_dbs(4).flags(EnvFlags::NO_SYNC);
    let env = opts.open(dir.path()).expect("open env");
    let mut w = env.write_txn().expect("write_txn");
    let db = env.create_database(&mut w, Some(b"t")).expect("create");
    for i in 0..50_000u64 {
        db.put(&mut w, &i.to_be_bytes(), &[1u8; 64]).expect("put");
    }
    w.commit().expect("commit");
    for i in 0..50u64 {
        let mut w = env.write_txn().expect("write_txn");
        db.put(&mut w, &(i * 2).to_be_bytes(), &[2u8; 64])
            .expect("put");
        w.commit().expect("commit");
    }
    let n = allocations(|| {
        let mut w = env.write_txn().expect("write_txn");
        db.put(&mut w, &1_000_000u64.to_be_bytes(), &[3u8; 64])
            .expect("put");
        w.commit().expect("commit");
    });
    assert!(
        n <= 8,
        "single-put commit after a large txn made {n} allocations"
    );

    // A large delete leaves a large freed-page record in the GC tree. The
    // small txns after it draw from that record, which decodes and rewrites
    // it (allocations this test does not bound); they must stay correct.
    let mut w = env.write_txn().expect("write_txn");
    for i in 0..25_000u64 {
        db.delete(&mut w, &(i * 2 + 1).to_be_bytes())
            .expect("delete");
    }
    w.commit().expect("commit");
    for i in 0..50u64 {
        let mut w = env.write_txn().expect("write_txn");
        db.put(&mut w, &(2_000_000 + i).to_be_bytes(), &[4u8; 64])
            .expect("put");
        w.commit().expect("commit");
    }
    let r = env.read_txn().expect("read_txn");
    assert_eq!(db.len(&r).expect("len"), 25_051);
}
