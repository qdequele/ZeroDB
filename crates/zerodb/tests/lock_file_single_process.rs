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
