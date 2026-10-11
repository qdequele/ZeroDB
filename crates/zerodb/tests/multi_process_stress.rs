//! Two-process reader/writer stress for lock-file environments (ADR-0023).
//!
//! Two peer processes (this test process and one re-exec of the test binary)
//! open one environment; each runs 3 reader threads and 1 writer thread for
//! `ZERODB_STRESS_SECS` (default 5). A third "reaped reader" process is
//! spawned and SIGKILLed every 500 ms while it holds two read transactions,
//! and the test process reaps its dead slots with `clear_stale_readers`.
//!
//! The data set is a 2,000-key space plus a `__sum` key holding the wrapping
//! sum of `hash(key, value)` over every other entry. Writers keep the sum
//! current incrementally inside each transaction; every read transaction
//! recomputes it from a full walk and must see the stored value in the same
//! snapshot. That catches torn snapshots, lost updates between the two
//! processes' writers, and pages reclaimed while a reader could still reach
//! them. `just stress` runs the 180 s variant.

use std::hash::{DefaultHasher, Hasher};
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
        let path = std::env::temp_dir().join(format!("zerodb-mps-{pid}-{nanos}-{seq}"));
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
const SEQ: &str = "ZERODB_MP_SEQ";

fn open(dir: &Path) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(128 << 20)
        .page_size(4096)
        .max_readers(64)
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

fn child(role: &str, dir: &Path, seq: u64) -> ChildGuard {
    let c = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .env(SEQ, seq.to_string())
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

const KEYS: u64 = 2000;
const SUM_KEY: &[u8] = b"__sum";

fn stress_duration() -> Duration {
    let secs = std::env::var("ZERODB_STRESS_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);
    Duration::from_secs(secs)
}

fn key_of(i: u64) -> String {
    format!("k{i:05}")
}

/// Hash of one entry. `DefaultHasher::new()` uses fixed keys, and both peers
/// run the same binary, so they agree on every value.
fn entry_hash(key: &[u8], value: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write(key);
    h.write(value);
    h.finish()
}

fn value_of(key: &str, generation: u64, pad: usize) -> Vec<u8> {
    format!("{key}:{generation}:{}", "x".repeat(pad)).into_bytes()
}

/// Small deterministic generator (no `rand` dependency).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

/// Writes every key at generation 0 and the matching `__sum`.
fn seed(env: &Env) {
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    let mut sum = 0u64;
    for i in 0..KEYS {
        let k = key_of(i);
        let v = value_of(&k, 0, 16);
        sum = sum.wrapping_add(entry_hash(k.as_bytes(), &v));
        db.put(&mut w, k.as_bytes(), &v).unwrap();
    }
    db.put(&mut w, SUM_KEY, &sum.to_le_bytes()).unwrap();
    w.commit().unwrap();
}

/// One writer transaction: rewrites 1..=64 random keys and keeps `__sum`
/// current by subtracting each replaced value's hash and adding the new one.
fn write_once(env: &Env, rng: &mut Lcg, generation: u64) {
    let db = env.main_database();
    let mut w = env.write_txn().expect("begin write txn");
    let raw = db.get(&w, SUM_KEY).expect("read sum").expect("sum present");
    let mut sum = u64::from_le_bytes(raw.try_into().expect("sum is 8 bytes"));
    let n = 1 + rng.next() % 64;
    for _ in 0..n {
        let k = key_of(rng.next() % KEYS);
        let pad = (rng.next() % 200) as usize;
        let new = value_of(&k, generation, pad);
        if let Some(old) = db.get(&w, k.as_bytes()).expect("read old") {
            sum = sum.wrapping_sub(entry_hash(k.as_bytes(), old));
        }
        sum = sum.wrapping_add(entry_hash(k.as_bytes(), &new));
        db.put(&mut w, k.as_bytes(), &new).expect("put");
    }
    db.put(&mut w, SUM_KEY, &sum.to_le_bytes())
        .expect("put sum");
    w.commit().expect("commit");
}

/// One read transaction: walks the whole database and checks it.
fn read_once(env: &Env) {
    let db = env.main_database();
    let r = env.read_txn().expect("begin read txn");
    let mut sum = 0u64;
    let mut count = 0u64;
    for entry in db.iter(&r) {
        let (k, v) = entry.expect("walk read");
        if k == SUM_KEY {
            continue;
        }
        assert!(
            v.starts_with(k) && v.get(k.len()) == Some(&b':'),
            "value does not carry its key: {:?} => {:?}",
            String::from_utf8_lossy(k),
            String::from_utf8_lossy(v)
        );
        sum = sum.wrapping_add(entry_hash(k, v));
        count += 1;
    }
    let raw = db.get(&r, SUM_KEY).expect("read sum").expect("sum present");
    let stored = u64::from_le_bytes(raw.try_into().expect("sum is 8 bytes"));
    assert_eq!(count, KEYS, "key count changed within a snapshot");
    assert_eq!(
        sum, stored,
        "recomputed sum differs from __sum in one snapshot"
    );
}

/// Runs 3 reader threads and 1 writer thread until `deadline`. Returns
/// `(commits, read_txns)`.
fn run_peer(env: &Env, deadline: Instant) -> (u64, u64) {
    let commits = AtomicU64::new(0);
    let reads = AtomicU64::new(0);
    let seed_rng = u64::from(std::process::id()) ^ 0x9e37_79b9_7f4a_7c15;
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut rng = Lcg(seed_rng);
            let mut generation = 1u64;
            while Instant::now() < deadline {
                write_once(env, &mut rng, generation);
                generation += 1;
                commits.fetch_add(1, Ordering::Relaxed);
            }
        });
        for _ in 0..3 {
            s.spawn(|| {
                // At least one read, even if the deadline is already past.
                loop {
                    read_once(env);
                    reads.fetch_add(1, Ordering::Relaxed);
                    if Instant::now() >= deadline {
                        break;
                    }
                }
            });
        }
    });
    (
        commits.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
    )
}

/// Child entry point: a no-op unless run by `child()`.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let env = open(&dir);
    match role.as_str() {
        "peer" => {
            let (commits, reads) = run_peer(&env, Instant::now() + stress_duration());
            std::fs::write(dir.join("peer-commits"), commits.to_string()).unwrap();
            std::fs::write(dir.join("peer-reads"), reads.to_string()).unwrap();
        }
        "reaped_reader" => {
            let seq = std::env::var(SEQ).unwrap();
            let a = env.read_txn().unwrap();
            let b = env.read_txn().unwrap();
            mark(&dir, &format!("reader-ready-{seq}"));
            std::thread::sleep(Duration::from_secs(600)); // killed by the parent
            drop((a, b));
        }
        other => panic!("unknown role {other}"),
    }
}

#[test]
fn stress_two_processes() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let secs = stress_duration();
    let deadline = Instant::now() + secs;
    let mut peer = child("peer", dir.path(), 0);
    let me = std::process::id();
    let peer_pid = peer.id();

    let reaped_total = AtomicU64::new(0);
    let kills = AtomicU64::new(0);
    let (my_commits, my_reads) = std::thread::scope(|s| {
        // Kills a fresh two-reader process every 500 ms.
        s.spawn(|| {
            let mut seq = 1u64;
            while Instant::now() < deadline {
                let mut c = child("reaped_reader", dir.path(), seq);
                wait_for(
                    &dir.path().join(format!("reader-ready-{seq}")),
                    Duration::from_secs(60),
                );
                std::thread::sleep(Duration::from_millis(500));
                c.kill().unwrap();
                c.wait().unwrap();
                kills.fetch_add(1, Ordering::Relaxed);
                seq += 1;
            }
        });
        // Frees the dead process's slots every 100 ms.
        s.spawn(|| {
            while Instant::now() < deadline {
                let n = env.clear_stale_readers().expect("clear_stale_readers");
                reaped_total.fetch_add(n as u64, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        run_peer(&env, deadline)
    });

    // The respawn loop is stopped and reaped; free any slots of the last kill.
    let n = env
        .clear_stale_readers()
        .expect("final clear_stale_readers");
    reaped_total.fetch_add(n as u64, Ordering::Relaxed);
    let listed = env.reader_list();
    assert!(
        listed
            .iter()
            .all(|e| e.pid.is_none_or(|p| p == me || p == peer_pid)),
        "a PID other than the two peers is still listed: {listed:?}"
    );

    assert!(peer.wait().unwrap().success(), "peer process failed");
    let read_count = |name: &str| -> u64 {
        std::fs::read_to_string(dir.path().join(name))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let peer_commits = read_count("peer-commits");
    let peer_reads = read_count("peer-reads");
    drop(env);

    let total = my_commits + peer_commits;
    println!(
        "multi_process_stress: {secs:?}; commits parent={my_commits} peer={peer_commits}; \
         read txns parent={my_reads} peer={peer_reads}; reaped-reader kills={}; \
         reaped slots={}",
        kills.load(Ordering::Relaxed),
        reaped_total.load(Ordering::Relaxed)
    );
    assert!(total >= 10, "writers barely ran: {total} commits");

    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    let problems = zerodb::check::check_image(&bytes, 4096);
    assert!(problems.is_empty(), "check_image: {problems:?}");
}
