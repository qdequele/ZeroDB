//! Single-process coverage for the opt-in lock-file environment (ADR-0023).
//!
//! These tests run entirely within one process: they check that turning
//! `lock_file(true)` on changes nothing observable about reads/writes, that
//! the file only appears when asked for, and that the existing
//! `EnvAlreadyOpened` / reopen-after-close behavior is unaffected. True
//! multi-process coordination is covered elsewhere (Task 7+); this file is
//! the "did wiring it up break the default path" smoke test.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{EnvOpenOptions, Error, LOCK_FILE_NAME};

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

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
        let path = std::env::temp_dir().join(format!("zerodb-lockfile-{pid}-{nanos}-{seq}"));
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

fn opts() -> EnvOpenOptions {
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).page_size(4096).lock_file(true);
    o
}

#[test]
fn lock_file_env_reads_and_writes_like_the_default() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(dir.path().join(LOCK_FILE_NAME).exists());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
}

#[test]
fn default_env_creates_no_lock_file() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20);
    let _env = o.open(dir.path()).expect("open");
    assert!(!dir.path().join(LOCK_FILE_NAME).exists());
}

#[test]
fn double_open_in_process_keeps_first_env_working() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(matches!(
        opts().open(dir.path()),
        Err(Error::EnvAlreadyOpened)
    ));
    let db = env.main_database();
    let mut w = env.write_txn().expect("first env still writes");
    db.put(&mut w, b"a", b"1").unwrap();
    w.commit().unwrap();
}

#[test]
fn reopen_after_close_reinitializes() {
    let dir = TempDir::new();
    {
        let env = opts().open(dir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"x", b"1").unwrap();
        w.commit().unwrap();
    }
    let env = opts().open(dir.path()).unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"x").unwrap(), Some(&b"1"[..]));
}

/// `EnvClosingEvent::wait` promises the environment is fully closed (SPEC 04
/// TXN-53), so an immediate reopen must succeed. With a lock file this
/// includes releasing the lock file: the last handle may drop on another
/// thread, and the close event must not fire while that handle's lock file is
/// still open in this process.
#[test]
fn reopen_after_closing_event_wait_succeeds() {
    let dir = TempDir::new();
    for i in 0..200 {
        let env = opts()
            .open(dir.path())
            .unwrap_or_else(|e| panic!("open {i} failed: {e:?}"));
        let clone = env.clone();
        let (go, wait_go) = std::sync::mpsc::channel::<()>();
        let t = std::thread::spawn(move || {
            let r = clone.read_txn().expect("read txn");
            drop(r);
            wait_go.recv().expect("go");
            // The last handle: the environment closes on this thread.
            drop(clone);
        });
        let closing = env.prepare_for_closing();
        go.send(()).expect("send go");
        closing.wait();
        // Reopen before joining, so nothing but the close event orders the
        // reopen after the other thread's teardown.
        let reopened = opts().open(dir.path());
        t.join().expect("thread");
        reopened.unwrap_or_else(|e| panic!("reopen {i} after wait failed: {e:?}"));
    }
    let env = opts().open(dir.path()).expect("final reopen");
    drop(env);
}

/// An open that fails after the lock file was opened releases it, so a later
/// open of the same directory succeeds. Here the data file name is taken by a
/// directory, so opening the data file fails.
#[test]
fn failed_open_after_lock_file_releases_it() {
    let dir = TempDir::new();
    std::fs::create_dir(dir.path().join("taken")).expect("mkdir");
    let mut bad = opts();
    bad.data_file_name("taken");
    assert!(bad.open(dir.path()).is_err(), "a directory is no data file");
    let env = opts().open(dir.path()).expect("open after a failed open");
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
}

/// An open that fails in the engine (the data file is not a ZeroDB file)
/// releases the lock file too.
#[test]
fn failed_engine_open_releases_lock_file() {
    let dir = TempDir::new();
    let data = dir.path().join("garbage.mdb");
    std::fs::write(&data, vec![0xA5u8; 4096 * 4]).expect("write garbage");
    let mut bad = opts();
    bad.data_file_name("garbage.mdb");
    assert!(bad.open(dir.path()).is_err(), "garbage is no env");
    std::fs::remove_file(&data).expect("rm garbage");
    let env = bad.open(dir.path()).expect("open after a failed open");
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"k").unwrap(), None);
}

/// A lock-file name equal to the data-file name would make the lock-file
/// initializer truncate the data file. The open must be rejected before any
/// file is touched, and the data must survive.
#[test]
fn lock_file_name_equal_to_data_file_name_is_rejected() {
    let dir = TempDir::new();
    {
        let mut o = EnvOpenOptions::new();
        o.map_size(16 << 20).page_size(4096);
        let env = o.open(dir.path()).expect("open");
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"keep", b"me").unwrap();
        w.commit().unwrap();
    }
    let mut bad = opts();
    bad.lock_file_name(zerodb::DATA_FILE_NAME);
    match bad.open(dir.path()) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).page_size(4096);
    let env = o.open(dir.path()).expect("plain reopen");
    let r = env.read_txn().unwrap();
    assert_eq!(
        env.main_database().get(&r, b"keep").unwrap(),
        Some(&b"me"[..])
    );
}

/// An out-of-range `max_readers` or `max_dbs` is rejected before the lock
/// file is created. The lock file is sized from `max_readers` and its
/// initializer writes every slot, so creating it first would allocate up to
/// 512 GiB on disk for `max_readers(u32::MAX)` before the open failed.
#[test]
fn out_of_range_limits_are_rejected_before_the_lock_file_exists() {
    // Both limits are 1 << 20 (SPEC 00 rows 4 and 5).
    let limit: u32 = 1 << 20;
    for (readers, dbs) in [(limit + 1, 8), (126, limit + 1)] {
        let dir = TempDir::new();
        let e = opts()
            .max_readers(readers)
            .max_dbs(dbs)
            .open(dir.path())
            .unwrap_err();
        match e {
            Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
        assert!(
            !dir.path().join(LOCK_FILE_NAME).exists(),
            "the lock file was created before the limits were checked \
             (max_readers {readers}, max_dbs {dbs})"
        );
    }
}
