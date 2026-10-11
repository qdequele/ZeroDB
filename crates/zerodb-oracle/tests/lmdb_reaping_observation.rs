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

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::ptr;
use std::time::{Duration, Instant};

use lmdb_master_sys as ffi;
use zerodb_oracle::tempdir::TempDir;

const MDB_NOTLS: c_uint = 0x0020_0000;
const MDB_RDONLY: c_uint = 0x0002_0000;
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

struct Lmdb {
    env: *mut ffi::MDB_env,
}

impl Lmdb {
    fn open(dir: &Path, max_readers: u32) -> Lmdb {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle owned by this
        // struct; `path` outlives `mdb_env_open`.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, max_readers), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, 1 << 20), 0);
            assert_eq!(ffi::mdb_env_open(env, path.as_ptr(), MDB_NOTLS, 0o644), 0);
        }
        Lmdb { env }
    }

    /// Begin a read txn; the raw return code on failure.
    fn try_read(&self) -> Result<*mut ffi::MDB_txn, c_int> {
        let mut txn = ptr::null_mut();
        // SAFETY: `self.env` is open; `txn` receives a handle we own.
        let rc = unsafe { ffi::mdb_txn_begin(self.env, ptr::null_mut(), MDB_RDONLY, &mut txn) };
        if rc == 0 {
            Ok(txn)
        } else {
            Err(rc)
        }
    }

    fn abort(txn: *mut ffi::MDB_txn) {
        // SAFETY: `txn` came from `mdb_txn_begin` and is ended exactly once.
        unsafe { ffi::mdb_txn_abort(txn) }
    }

    /// Begin a write txn, put one key, commit.
    fn commit_one(&self) {
        // SAFETY: standard begin/dbi_open/put/commit sequence on an open
        // env; every pointer is live for the duration of its call.
        unsafe {
            let mut txn = ptr::null_mut();
            assert_eq!(
                ffi::mdb_txn_begin(self.env, ptr::null_mut(), 0, &mut txn),
                0
            );
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            let mut k = ffi::MDB_val {
                mv_size: 1,
                mv_data: b"k".as_ptr() as *mut c_void,
            };
            let mut v = ffi::MDB_val {
                mv_size: 1,
                mv_data: b"v".as_ptr() as *mut c_void,
            };
            assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
            assert_eq!(ffi::mdb_txn_commit(txn), 0);
        }
    }

    /// PIDs listed by `mdb_reader_list` (one per occupied slot).
    fn reader_pids(&self) -> Vec<u32> {
        unsafe extern "C" fn collect(msg: *const c_char, ctx: *mut c_void) -> c_int {
            // SAFETY: `ctx` is the `&mut Vec<String>` passed below, live for
            // the call; `msg` is a NUL-terminated string from LMDB.
            let out = unsafe { &mut *(ctx as *mut Vec<String>) };
            out.push(
                unsafe { CStr::from_ptr(msg) }
                    .to_string_lossy()
                    .into_owned(),
            );
            0
        }
        let mut lines: Vec<String> = Vec::new();
        // SAFETY: the callback only touches `lines`, which outlives the call.
        unsafe {
            ffi::mdb_reader_list(self.env, Some(collect), &mut lines as *mut _ as *mut c_void);
        }
        lines
            .iter()
            .flat_map(|l| l.lines())
            .filter_map(|l| l.split_whitespace().next()?.parse::<u32>().ok())
            .collect()
    }

    /// `mdb_reader_check`'s dead count.
    fn reader_check(&self) -> i32 {
        let mut dead: c_int = 0;
        // SAFETY: `dead` outlives the call.
        assert_eq!(unsafe { ffi::mdb_reader_check(self.env, &mut dead) }, 0);
        dead
    }
}

impl Drop for Lmdb {
    fn drop(&mut self) {
        // SAFETY: every txn of this env has ended; closed exactly once.
        unsafe { ffi::mdb_env_close(self.env) }
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

fn wait_for(path: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            Instant::now() < end,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Spawn a child that holds `n` read txns, wait until it holds them, SIGKILL
/// it. Returns its PID.
fn kill_reader_child(dir: &Path, n: u32) -> u32 {
    let mut c = spawn(&format!("hold_readers:{n}"), dir);
    wait_for(&dir.join("held"));
    let pid = c.0.id();
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    std::fs::remove_file(dir.join("held")).unwrap();
    pid
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let env = Lmdb::open(&dir, 4);
    if let Some(n) = role.strip_prefix("hold_readers:") {
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.try_read().unwrap())
            .collect();
        std::fs::write(dir.join("held"), b"").unwrap();
        std::thread::sleep(Duration::from_secs(600));
        drop(txns);
    } else if role == "hold_writer" {
        let mut txn = ptr::null_mut();
        // SAFETY: as `commit_one`; the txn is never ended (the process is killed).
        assert_eq!(
            unsafe { ffi::mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn) },
            0
        );
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
    let env = Lmdb::open(dir.path(), max_readers);
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
        let path = PathBuf::from(env_path(env));
        let mut c = spawn("open_close", &path);
        assert!(c.0.wait().unwrap().success());
        !env.reader_pids().contains(&pid)
    });
    println!("joiner open reaps: {reaped}");
    assert_eq!(reaped, OBS_JOINER_OPEN_REAPS);
}

/// The directory an env was opened at (`mdb_env_get_path`).
fn env_path(env: &Lmdb) -> String {
    let mut p: *const c_char = ptr::null();
    // SAFETY: LMDB returns a pointer into the env's own storage, valid while
    // the env is open; copied out immediately.
    unsafe {
        assert_eq!(ffi::mdb_env_get_path(env.env, &mut p), 0);
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
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

/// Linux only: on macOS the fork's writer mutex is not robust, so a dead
/// writer blocks the next one forever.
#[cfg(target_os = "linux")]
#[test]
fn observe_writer_takeover() {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), 4);
    env.commit_one();
    let reader_pid = kill_reader_child(dir.path(), 1);
    let mut w = spawn("hold_writer", dir.path());
    wait_for(&dir.path().join("held"));
    w.0.kill().unwrap();
    w.0.wait().unwrap();
    env.commit_one(); // takes over the dead writer's mutex
    let reaped = !env.reader_pids().contains(&reader_pid);
    println!("writer takeover reaps: {reaped}");
    assert_eq!(reaped, OBS_WRITER_TAKEOVER_REAPS);
}

#[test]
fn first_opener_starts_with_an_empty_table() {
    let dir = TempDir::new().unwrap();
    {
        let env = Lmdb::open(dir.path(), 4);
        env.commit_one();
    }
    // The child is the only process; it dies holding a reader.
    let pid = kill_reader_child(dir.path(), 1);
    let env = Lmdb::open(dir.path(), 4);
    assert!(
        !env.reader_pids().contains(&pid),
        "the first opener wipes the table"
    );
}
