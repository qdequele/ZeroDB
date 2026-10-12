//! Multi-process behavior of the lock file's first-opener election
//! (ADR-0023 §1, amendment 10). The test binary re-runs itself as the other
//! processes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb_core::shared::SharedRegion;
use zerodb_io::lockfile::LockFile;

const ROLE: &str = "ZERODB_LOCKFILE_ROLE";
const DIR: &str = "ZERODB_LOCKFILE_DIR";

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> TempDir {
        static N: AtomicU64 = AtomicU64::new(0);
        // Relaxed: only uniqueness matters.
        let p = std::env::temp_dir().join(format!(
            "zerodb-lockproc-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
            .env(ROLE, role)
            .env(DIR, dir)
            .spawn()
            .unwrap(),
    )
}

fn wait_for(p: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !p.exists() {
        assert!(Instant::now() < end, "timed out on {}", p.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let path = dir.join("zerodb.lock");
    match role.as_str() {
        // Win the election, never finish: stand in for an initializer that
        // dies (SIGKILL) before seeding T.
        "init_and_hang" => {
            let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).unwrap();
            assert!(l.is_initializer());
            std::fs::write(dir.join("initializing"), b"").unwrap();
            std::thread::sleep(Duration::from_secs(600));
        }
        // Join (or initialize), finish, report, exit.
        "join_and_report" => {
            let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).unwrap();
            l.bind_page_size(4096).unwrap();
            l.finish_open().unwrap();
            let name = if l.is_initializer() {
                "child-init"
            } else {
                "child-join"
            };
            std::fs::write(dir.join(name), b"").unwrap();
            // Hold the environment until the parent has also opened.
            wait_for(&dir.join("parent-open"));
        }
        other => panic!("unknown role {other}"),
    }
}

/// A joiner waits on the election lock while the initializer dies before
/// finishing. With every other process gone, the joiner must initialize
/// rather than adopt a half-built file.
#[test]
fn joiner_whose_initializer_died_reinitializes() {
    let dir = TempDir::new();
    let mut init = spawn("init_and_hang", &dir.0);
    wait_for(&dir.0.join("initializing"));
    let path = dir.0.join("zerodb.lock");
    let opener = std::thread::spawn(move || {
        LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).map(|l| l.is_initializer())
    });
    std::thread::sleep(Duration::from_millis(200)); // the joiner is now blocked
    init.0.kill().unwrap();
    init.0.wait().unwrap();
    let became_initializer = opener
        .join()
        .unwrap()
        .expect("open after the initializer died");
    assert!(
        became_initializer,
        "the joiner adopted a half-built lock file"
    );
}

/// Two waiting processes, initializer dies: both opens succeed, exactly one
/// initializes. Repeated, because which one wins the upgrade varies.
#[test]
fn three_way_race_after_initializer_death() {
    for _ in 0..20 {
        let dir = TempDir::new();
        let mut init = spawn("init_and_hang", &dir.0);
        wait_for(&dir.0.join("initializing"));
        let child = spawn("join_and_report", &dir.0);
        let path = dir.0.join("zerodb.lock");
        let parent = std::thread::spawn(move || {
            let l =
                LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).expect("parent open");
            l.bind_page_size(4096).unwrap();
            l.finish_open().unwrap();
            l
        });
        std::thread::sleep(Duration::from_millis(100));
        init.0.kill().unwrap();
        init.0.wait().unwrap();
        let l = parent.join().unwrap();
        std::fs::write(dir.0.join("parent-open"), b"").unwrap();
        let mut child = child;
        assert!(child.0.wait().unwrap().success(), "child open failed");
        let child_init = dir.0.join("child-init").exists();
        assert!(
            l.is_initializer() != child_init,
            "exactly one of the two must initialize (parent {}, child {child_init})",
            l.is_initializer()
        );
    }
}

/// A live process is alive; a SIGKILLed one is not (the kernel drops its
/// liveness lock).
#[test]
fn liveness_probe_sees_death() {
    let dir = TempDir::new();
    let path = dir.0.join("zerodb.lock");
    let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).unwrap();
    l.bind_page_size(4096).unwrap();
    l.finish_open().unwrap();
    let mut c = spawn("join_and_report", &dir.0);
    wait_for(&dir.0.join("child-join"));
    let pid = c.0.id();
    assert!(l.is_alive(pid).unwrap());
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    assert!(!l.is_alive(pid).unwrap());
    assert!(l.is_alive(std::process::id()).unwrap());
}
