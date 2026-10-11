//! Two-process tests for lock-file environments (ADR-0023 stage 1). The test
//! binary re-runs itself as the second process (see `child_entry`).

use std::ops::{Deref, DerefMut};
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

/// A spawned child process that is killed and reaped when dropped, so a
/// test that panics mid-way (for example on a `wait_for` deadline) never
/// leaves its child running. It dereferences to the `Child`, so the happy
/// path calls `wait`/`kill` on it directly; killing an already reaped child
/// on drop is harmless and its error is ignored.
struct ChildGuard(Child);

impl Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn child(role: &str, dir: &Path) -> ChildGuard {
    let c = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .spawn()
        .expect("spawn child");
    ChildGuard(c)
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
        "write_many" => {
            // Rewrite a run of keys the parent seeded, so this commit moves
            // pages a stale writer in the parent would overwrite.
            let mut w = env.write_txn().unwrap();
            for i in 0..200u32 {
                db.put(&mut w, format!("k{i:04}").as_bytes(), b"child")
                    .unwrap();
            }
            db.put(&mut w, b"from-child", b"1").unwrap();
            w.commit().unwrap();
        }
        "write_after_parent" => {
            // Open first, then let the parent commit, then write on top.
            mark(&dir, "child-open");
            wait_for(&dir.join("parent-committed"), Duration::from_secs(30));
            let mut w = env.write_txn().unwrap();
            assert_eq!(
                db.get(&w, b"from-parent").unwrap(),
                Some(&b"1"[..]),
                "the child's write txn misses the parent's commit"
            );
            for i in 0..200u32 {
                db.put(&mut w, format!("k{i:04}").as_bytes(), b"child")
                    .unwrap();
            }
            db.put(&mut w, b"from-child", b"1").unwrap();
            w.commit().unwrap();
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
        "hold_reader" => {
            // Hold a read txn until told to stop, then exit cleanly.
            let r = env.read_txn().unwrap();
            mark(&dir, "child-reading");
            wait_for(&dir.join("child-stop"), Duration::from_secs(60));
            drop(r);
        }
        "hold_readers_and_die" => {
            let a = env.read_txn().unwrap();
            let b = env.read_txn().unwrap();
            mark(&dir, "child-reading");
            std::thread::sleep(Duration::from_secs(600)); // killed by the parent
            drop((a, b));
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

/// Seed 200 keys so the tree spans several pages.
fn seed(env: &Env) {
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    for i in 0..200u32 {
        db.put(
            &mut w,
            format!("k{i:04}").as_bytes(),
            format!("seed{i}").repeat(10).as_bytes(),
        )
        .unwrap();
    }
    w.commit().unwrap();
}

/// Both processes' keys are present and the file is consistent.
fn assert_both_writes(env: &Env, dir: &Path) {
    let db = env.main_database();
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"from-child").unwrap(), Some(&b"1"[..]));
    assert_eq!(db.get(&r, b"from-parent").unwrap(), Some(&b"1"[..]));
    for i in 0..200u32 {
        assert!(
            db.get(&r, format!("k{i:04}").as_bytes()).unwrap().is_some(),
            "key {i} lost"
        );
    }
    drop(r);
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).unwrap();
    let problems = zerodb::check::check_image(&bytes, 4096);
    assert!(problems.is_empty(), "check_image: {problems:?}");
}

#[test]
fn writer_builds_on_another_processes_commit() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    // The parent's env was open (and had committed) before the child's commit.
    assert!(child("write_many", dir.path()).wait().unwrap().success());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    assert_eq!(
        db.get(&w, b"from-child").unwrap(),
        Some(&b"1"[..]),
        "the parent's write txn misses the child's commit"
    );
    db.put(&mut w, b"from-parent", b"1").unwrap();
    w.commit().unwrap();
    assert_both_writes(&env, dir.path());
}

#[test]
fn other_process_writer_builds_on_this_processes_commit() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("write_after_parent", dir.path());
    wait_for(&dir.path().join("child-open"), Duration::from_secs(30));
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"from-parent", b"1").unwrap();
    w.commit().unwrap();
    mark(dir.path(), "parent-committed");
    assert!(c.wait().unwrap().success());
    assert_both_writes(&env, dir.path());
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

/// Rewrite every seeded key `rounds` times. Returns the growth of
/// `last_pgno` across the rewrites.
fn churn_growth(env: &Env, rounds: u32) -> u64 {
    let db = env.main_database();
    let before = env.info().last_pgno;
    for round in 0..rounds {
        let mut w = env.write_txn().unwrap();
        for i in 0..200u32 {
            db.put(
                &mut w,
                format!("k{i:04}").as_bytes(),
                format!("r{round}").repeat(40).as_bytes(),
            )
            .unwrap();
        }
        w.commit().unwrap();
    }
    env.info().last_pgno - before
}

#[test]
fn killed_readers_slots_are_reaped_by_clear_stale_readers() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    let pid = c.id();
    c.kill().unwrap();
    c.wait().unwrap();
    let listed = env.reader_list();
    assert_eq!(
        listed.iter().filter(|e| e.pid == Some(pid)).count(),
        2,
        "{listed:?}"
    );
    // LMDB counts slots, not processes (OBS_CHECK_COUNTS_SLOTS in
    // lmdb_reaping_observation.rs), so the two-reader child gives 2.
    assert_eq!(env.clear_stale_readers().unwrap(), 2);
    assert!(env.reader_list().is_empty());
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
}

#[test]
fn reaping_never_touches_a_live_process() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_reader", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
    assert_eq!(
        env.reader_list()
            .iter()
            .filter(|e| e.pid == Some(c.id()))
            .count(),
        1
    );
    mark(dir.path(), "child-stop");
    assert!(c.wait().unwrap().success());
}

#[test]
fn reaped_slot_lets_the_writer_reuse_pages() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    c.kill().unwrap();
    c.wait().unwrap();
    // The dead reader pins the seed snapshot: every rewrite allocates fresh
    // pages.
    let pinned = churn_growth(&env, 20);
    assert!(pinned > 0, "the pinned churn did not grow the file");
    assert_eq!(env.clear_stale_readers().unwrap(), 2);
    churn_growth(&env, 2); // let the freelist catch up
    let freed = churn_growth(&env, 20);
    assert!(
        freed * 4 < pinned,
        "pages are not reused after reaping: growth {freed} vs {pinned} while pinned"
    );
}

/// With no dead writer to take over from, a plain write txn and commit do
/// not reap: only `clear_stale_readers` and a writer takeover (Linux) do.
#[test]
fn plain_write_txn_does_not_reap_a_dead_reader() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    let pid = c.id();
    c.kill().unwrap();
    c.wait().unwrap();
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"plain", b"1").unwrap();
    w.commit().unwrap();
    let listed = env.reader_list();
    assert_eq!(
        listed.iter().filter(|e| e.pid == Some(pid)).count(),
        2,
        "a plain write reaped the dead reader: {listed:?}"
    );
}

/// The two-process form of the zerodb-io unit test
/// `writer_lock_releases_from_another_thread`. On macOS that unit test
/// cannot tell a released POSIX lock from a held one (a process never
/// conflicts with itself); another process can.
#[test]
fn writer_lock_released_on_another_thread_admits_another_process() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"parent", b"1").unwrap();
    let mut c = child("write_when_free", dir.path());
    wait_for(&dir.path().join("child-waiting"), Duration::from_secs(30));
    std::thread::sleep(Duration::from_millis(300));
    assert!(!dir.path().join("child-got-lock").exists(), "lock not held");
    std::thread::scope(|s| {
        s.spawn(move || w.commit().unwrap());
    });
    assert!(c.wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"child").unwrap(), Some(&b"2"[..]));
}

/// Linux only: the futex writer lock reports a dead owner's takeover, and
/// the new writer then reaps dead readers, as the LMDB fork does. On macOS
/// the `fcntl` writer lock cannot tell a takeover from a normal acquire.
#[cfg(target_os = "linux")]
#[test]
fn writer_takeover_reaps_a_dead_readers_slot() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut r = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    let reader_pid = r.id();
    r.kill().unwrap();
    r.wait().unwrap();
    let mut w = child("hold_and_die", dir.path());
    wait_for(
        &dir.path().join("child-holds-lock"),
        Duration::from_secs(30),
    );
    w.kill().unwrap();
    w.wait().unwrap();
    assert!(env.reader_list().iter().any(|e| e.pid == Some(reader_pid)));
    drop(env.write_txn().expect("take over the dead writer's lock"));
    assert!(
        !env.reader_list().iter().any(|e| e.pid == Some(reader_pid)),
        "the takeover did not reap the dead reader"
    );
}
