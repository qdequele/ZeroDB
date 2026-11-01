//! Where the Meilisearch LMDB fork reclaims the reader slots of dead
//! processes (ADR-0023 §3 "When", amendment 12). AGENTS.md rule 1: observed,
//! not guessed. Drives the vendored fork directly through `lmdb-master-sys`
//! (heed exposes no `mdb_reader_list`); the test binary re-runs itself as the
//! reader/writer process that gets SIGKILLed.
//!
//! Each scenario: a child opens the env, begins read txns (or a write txn),
//! signals, and is SIGKILLed; the parent then performs one action and checks
//! whether the dead child's PID is still listed by `mdb_reader_list`. The
//! `OBS_*` constants pin what the fork does; a diff means the fork changed or
//! the pin was wrong — never edit a pin to make ZeroDB pass.

use std::path::{Path, PathBuf};
use std::time::Duration;

use lmdb_master_sys as ffi;
use zerodb_oracle::child::{self, ChildGuard};
use zerodb_oracle::ffi_lmdb::Lmdb;
use zerodb_oracle::tempdir::TempDir;

/// The map size every env here opens with.
const MAP: u64 = 1 << 20;
const ROLE: &str = "ZERODB_REAP_OBS_ROLE";
const DIR: &str = "ZERODB_REAP_OBS_DIR";

// Pinned observations (filled from the first run on macOS and Linux; see
// the ADR-0023 amendment 12 table).
const OBS_READ_BEGIN_REAPS: bool = false;
const OBS_WRITE_COMMIT_REAPS: bool = false;
const OBS_JOINER_OPEN_REAPS: bool = false;
const OBS_FULL_TABLE_REAPS: bool = false;
const OBS_CHECK_COUNTS_SLOTS: bool = true;
#[cfg(target_os = "linux")]
const OBS_WRITER_TAKEOVER_REAPS: bool = true;

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    child::spawn_self(
        module_path!(),
        &[(ROLE, role.as_ref()), (DIR, dir.as_os_str())],
    )
}

/// Spawn a child that holds `n` read txns, wait until it holds them, SIGKILL
/// it. Returns its PID.
fn kill_reader_child(dir: &Path, n: u32) -> u32 {
    child::kill_when_held(dir, || spawn(&format!("hold_readers:{n}"), dir))
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let env = Lmdb::open(&dir, MAP, 4);
    if let Some(n) = role.strip_prefix("hold_readers:") {
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.try_read().unwrap())
            .collect();
        std::fs::write(dir.join("held"), b"").unwrap();
        std::thread::sleep(Duration::from_secs(600));
        drop(txns);
    } else if role == "hold_writer" {
        // The txn is never ended: the process is killed.
        let _txn = env.begin(0).unwrap();
        std::fs::write(dir.join("held"), b"").unwrap();
        std::thread::sleep(Duration::from_secs(600));
    } else if role == "open_close" {
        let t = env.try_read().unwrap();
        Lmdb::abort(t);
    } else {
        panic!("unknown role {role}");
    }
}

/// The parent keeps the env open throughout (no first-opener wipe), seeds
/// one commit, then lets `f` act after the child's death.
fn scenario(n_readers: u32, max_readers: u32, f: impl FnOnce(&Lmdb, u32) -> bool) -> bool {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), MAP, max_readers);
    env.commit_one();
    let pid = kill_reader_child(dir.path(), n_readers);
    assert!(
        env.reader_pids().contains(&pid),
        "baseline: a dead reader stays listed"
    );
    f(&env, pid)
}

#[test]
fn observe_read_begin() {
    let reaped = scenario(1, 4, |env, pid| {
        Lmdb::abort(env.try_read().unwrap());
        !env.reader_pids().contains(&pid)
    });
    println!("read txn begin reaps: {reaped}");
    assert_eq!(reaped, OBS_READ_BEGIN_REAPS);
}

#[test]
fn observe_write_commit() {
    let reaped = scenario(1, 4, |env, pid| {
        env.commit_one();
        !env.reader_pids().contains(&pid)
    });
    println!("write txn commit reaps: {reaped}");
    assert_eq!(reaped, OBS_WRITE_COMMIT_REAPS);
}

#[test]
fn observe_joiner_open() {
    let reaped = scenario(1, 4, |env, pid| {
        let path = PathBuf::from(env.path());
        let mut c = spawn("open_close", &path);
        assert!(c.0.wait().unwrap().success());
        !env.reader_pids().contains(&pid)
    });
    println!("joiner open reaps: {reaped}");
    assert_eq!(reaped, OBS_JOINER_OPEN_REAPS);
}

#[test]
fn observe_full_table() {
    // Two slots: the dead child holds one, the parent holds the other, then
    // asks for a third reader.
    let reaped = scenario(1, 2, |env, _pid| {
        let live = env.try_read().unwrap();
        let next = env.try_read();
        let ok = next.is_ok();
        if let Ok(t) = next {
            Lmdb::abort(t);
        } else {
            assert_eq!(next.unwrap_err(), ffi::MDB_READERS_FULL);
        }
        Lmdb::abort(live);
        ok
    });
    println!("full table reaps and retries: {reaped}");
    assert_eq!(reaped, OBS_FULL_TABLE_REAPS);
}

#[test]
fn observe_reader_check_count() {
    let counted = scenario(2, 4, |env, pid| {
        let n = env.reader_check();
        assert!(
            !env.reader_pids().contains(&pid),
            "reader_check frees the dead slots"
        );
        assert_eq!(env.reader_check(), 0, "a second check finds nothing");
        n == 2
    });
    println!("reader_check counts slots (2) rather than processes (1): {counted}");
    assert_eq!(counted, OBS_CHECK_COUNTS_SLOTS);
}

/// Linux only: the writer takeover is not observed on macOS; the takeover
/// scenario runs only on Linux.
#[cfg(target_os = "linux")]
#[test]
fn observe_writer_takeover() {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), MAP, 4);
    env.commit_one();
    let reader_pid = kill_reader_child(dir.path(), 1);
    let mut w = spawn("hold_writer", dir.path());
    child::wait_for(&dir.path().join("held"));
    w.0.kill().unwrap();
    w.0.wait().unwrap();
    assert!(
        env.reader_pids().contains(&reader_pid),
        "baseline: the dead reader is still listed before the takeover"
    );
    env.commit_one(); // takes over the dead writer's mutex
    let reaped = !env.reader_pids().contains(&reader_pid);
    println!("writer takeover reaps: {reaped}");
    assert_eq!(reaped, OBS_WRITER_TAKEOVER_REAPS);
}

#[test]
fn first_opener_starts_with_an_empty_table() {
    let dir = TempDir::new().unwrap();
    {
        let env = Lmdb::open(dir.path(), MAP, 4);
        env.commit_one();
    }
    // The child is the only process; it dies holding a reader.
    let pid = kill_reader_child(dir.path(), 1);
    let env = Lmdb::open(dir.path(), MAP, 4);
    assert!(
        !env.reader_pids().contains(&pid),
        "the first opener wipes the table"
    );
}
