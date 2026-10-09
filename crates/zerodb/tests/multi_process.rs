//! Two-process tests for lock-file environments (ADR-0023 stage 1). The test
//! binary re-runs itself as the second process (see `child_entry`).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb::{Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A self-cleaning temporary directory (no `tempfile` dependency).
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
        let path = std::env::temp_dir().join(format!("zerodb-mp-{pid}-{nanos}-{seq}"));
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

const ROLE: &str = "ZERODB_MP_ROLE";
const DIR: &str = "ZERODB_MP_DIR";

fn open(dir: &Path) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(64 << 20)
        .page_size(4096)
        .max_readers(16)
        .lock_file(true);
    o.open(dir).expect("open lock-file env")
}

fn child(role: &str, dir: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .spawn()
        .expect("spawn child")
}

fn wait_for(path: &Path, deadline: Duration) {
    let end = Instant::now() + deadline;
    while !path.exists() {
        assert!(
            Instant::now() < end,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn mark(dir: &Path, name: &str) {
    std::fs::write(dir.join(name), b"").unwrap();
}

/// Child entry point: a no-op unless run by `child()`.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let env = open(&dir);
    let db = env.main_database();
    match role.as_str() {
        "write_one" => {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, b"from-child", b"1").unwrap();
            w.commit().unwrap();
        }
        "churn" => {
            // Rewrite and delete every key many times, so freed pages would be
            // reused if the parent's reader slot were invisible here.
            for round in 0..50u32 {
                let mut w = env.write_txn().unwrap();
                for i in 0..200u32 {
                    let k = format!("k{i:04}");
                    if round % 2 == 0 {
                        db.delete(&mut w, k.as_bytes()).unwrap();
                    } else {
                        db.put(
                            &mut w,
                            k.as_bytes(),
                            format!("r{round}").repeat(40).as_bytes(),
                        )
                        .unwrap();
                    }
                }
                w.commit().unwrap();
            }
        }
        "write_when_free" => {
            mark(&dir, "child-waiting");
            let mut w = env.write_txn().unwrap(); // blocks while the parent writes
            mark(&dir, "child-got-lock");
            db.put(&mut w, b"child", b"2").unwrap();
            w.commit().unwrap();
        }
        "hold_and_die" => {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, b"doomed", b"x").unwrap();
            mark(&dir, "child-holds-lock");
            std::thread::sleep(Duration::from_secs(600)); // killed by the parent
        }
        other => panic!("unknown role {other}"),
    }
}

#[test]
fn commit_in_one_process_is_visible_in_another() {
    let dir = TempDir::new();
    let env = open(dir.path());
    assert!(child("write_one", dir.path()).wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(
        env.main_database().get(&r, b"from-child").unwrap(),
        Some(&b"1"[..])
    );
}

#[test]
fn foreign_reader_holds_back_page_reuse() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    for i in 0..200u32 {
        db.put(
            &mut w,
            format!("k{i:04}").as_bytes(),
            format!("orig{i}").repeat(40).as_bytes(),
        )
        .unwrap();
    }
    w.commit().unwrap();
    let r = env.read_txn().unwrap(); // pinned in a shared slot
    assert!(child("churn", dir.path()).wait().unwrap().success());
    for i in 0..200u32 {
        let want = format!("orig{i}").repeat(40);
        assert_eq!(
            db.get(&r, format!("k{i:04}").as_bytes()).unwrap(),
            Some(want.as_bytes()),
            "key {i} changed under a pinned snapshot"
        );
    }
    drop(r);
    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    assert!(zerodb::check::check_image(&bytes, 4096).is_empty());
}

#[test]
fn writers_in_two_processes_exclude_each_other() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    let mut c = child("write_when_free", dir.path());
    wait_for(&dir.path().join("child-waiting"), Duration::from_secs(30));
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !dir.path().join("child-got-lock").exists(),
        "child took the writer lock while the parent held it"
    );
    db.put(&mut w, b"parent", b"1").unwrap();
    w.commit().unwrap();
    assert!(c.wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"parent").unwrap(), Some(&b"1"[..]));
    assert_eq!(db.get(&r, b"child").unwrap(), Some(&b"2"[..]));
}

#[test]
fn killed_writer_lock_is_taken_over() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let mut c = child("hold_and_die", dir.path());
    wait_for(
        &dir.path().join("child-holds-lock"),
        Duration::from_secs(30),
    );
    c.kill().unwrap(); // SIGKILL
    c.wait().unwrap();
    let start = Instant::now();
    let w = env.write_txn().expect("take over the dead writer's lock");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "takeover took {:?}",
        start.elapsed()
    );
    drop(w);
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"doomed").unwrap(), None);
}

#[test]
fn second_process_with_other_max_readers_is_refused() {
    let dir = TempDir::new();
    let _env = open(dir.path()); // max_readers 16
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry_other_readers", "--nocapture"])
        .env(DIR, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
}

/// Child entry point for the `max_readers` mismatch case.
#[test]
fn child_entry_other_readers() {
    let Ok(dir) = std::env::var(DIR) else { return };
    let mut o = EnvOpenOptions::new();
    o.map_size(64 << 20)
        .page_size(4096)
        .max_readers(32)
        .lock_file(true);
    match o.open(Path::new(&dir)) {
        Err(zerodb::Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected a max_readers mismatch error, got {other:?}"),
    }
}
