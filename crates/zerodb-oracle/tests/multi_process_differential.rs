//! Differential test of dead-process handling (ADR-0023 stage 2): the same
//! scenario run against the Meilisearch LMDB fork (through `lmdb-master-sys`)
//! and against ZeroDB with a lock file, comparing what each engine reports.
//!
//! The test binary re-runs itself as the child process that gets SIGKILLed.
//! A mismatch between the engines is a ZeroDB bug unless recorded in
//! `docs/DIVERGENCES.md`; never edit an expectation to make ZeroDB pass.

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
const ROLE: &str = "ZERODB_MPDIFF_ROLE";
const DIR: &str = "ZERODB_MPDIFF_DIR";
const MAX_READERS: u32 = 8;

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

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
    /// Whether the main database holds `key`, read in a fresh read txn.
    fn has_key(&self, key: &[u8]) -> bool {
        // SAFETY: begin/dbi_open/get/abort on an open env; `key` outlives the
        // call and the returned data pointer is not used.
        unsafe {
            let txn = self.try_read().unwrap();
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            let mut k = ffi::MDB_val {
                mv_size: key.len(),
                mv_data: key.as_ptr() as *mut c_void,
            };
            let mut v = ffi::MDB_val {
                mv_size: 0,
                mv_data: ptr::null_mut(),
            };
            let rc = ffi::mdb_get(txn, dbi, &mut k, &mut v);
            Lmdb::abort(txn);
            match rc {
                0 => true,
                ffi::MDB_NOTFOUND => false,
                other => panic!("mdb_get failed: {other}"),
            }
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

fn zdb_open(dir: &Path) -> zerodb::Env {
    let mut o = zerodb::EnvOpenOptions::new();
    o.map_size(1 << 20).max_readers(MAX_READERS).lock_file(true);
    o.open(dir).expect("open lock-file env")
}

fn zdb_commit_one(env: &zerodb::Env) {
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
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

/// Spawn a child in `role`, wait for its `held` marker, SIGKILL it, remove
/// the marker. Returns its PID.
fn kill_child(role: &str, dir: &Path) -> u32 {
    let mut c = spawn(role, dir);
    wait_for(&dir.join("held"));
    let pid = c.0.id();
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    std::fs::remove_file(dir.join("held")).unwrap();
    pid
}

/// Child entry point: a no-op unless run by `spawn`. Roles are
/// `{zdb,lmdb}_hold_readers:<n>` and `{zdb,lmdb}_hold_writer`; the writer
/// roles put `doomed` in an uncommitted write txn before signaling.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let signal = || std::fs::write(dir.join("held"), b"").unwrap();
    let park = || std::thread::sleep(Duration::from_secs(600));
    if let Some(n) = role.strip_prefix("lmdb_hold_readers:") {
        let env = Lmdb::open(&dir, MAX_READERS);
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.try_read().unwrap())
            .collect();
        signal();
        park();
        drop(txns);
    } else if role == "lmdb_hold_writer" {
        let env = Lmdb::open(&dir, MAX_READERS);
        // SAFETY: as `commit_one`; the txn is never ended (the process is
        // killed while holding it).
        unsafe {
            let mut txn = ptr::null_mut();
            assert_eq!(ffi::mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn), 0);
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            let mut k = ffi::MDB_val {
                mv_size: 6,
                mv_data: b"doomed".as_ptr() as *mut c_void,
            };
            let mut v = ffi::MDB_val {
                mv_size: 1,
                mv_data: b"x".as_ptr() as *mut c_void,
            };
            assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
        }
        signal();
        park();
    } else if let Some(n) = role.strip_prefix("zdb_hold_readers:") {
        let env = zdb_open(&dir);
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.read_txn().unwrap())
            .collect();
        signal();
        park();
        drop(txns);
    } else if role == "zdb_hold_writer" {
        let env = zdb_open(&dir);
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"doomed", b"x").unwrap();
        signal();
        park();
    } else {
        panic!("unknown role {role}");
    }
}

/// One dead reader process holding 2 read txns: after the kill, both
/// engines list it, `clear_stale_readers` returns the same count, and a
/// second call returns 0.
#[test]
fn killed_reader_then_clear_stale_readers_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        let pid = kill_child("lmdb_hold_readers:2", dir.path());
        let listed = env.reader_pids().iter().filter(|&&p| p == pid).count();
        assert_eq!(listed, 2, "setup: the dead child's two slots are listed");
        let first = env.reader_check();
        assert!(first > 0, "setup: the first check finds the dead slots");
        (listed, first, env.reader_check())
    };
    eprintln!("lmdb (listed, first, second) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let pid = kill_child("zdb_hold_readers:2", dir.path());
        let listed = env
            .reader_list()
            .iter()
            .filter(|e| e.pid == Some(pid))
            .count();
        assert_eq!(listed, 2, "setup: the dead child's two slots are listed");
        let first = env.clear_stale_readers().unwrap() as i32;
        assert!(first > 0, "setup: the first check finds the dead slots");
        (listed, first, env.clear_stale_readers().unwrap() as i32)
    };
    eprintln!("zdb (listed, first, second) = {zdb:?}");
    assert_eq!(zdb, lmdb, "(listed, first check, second check)");
}

/// Linux only (the fork's macOS writer mutex is not robust): a SIGKILLed
/// writer's lock is taken over, its uncommitted write is absent, and a dead
/// reader is reaped or not exactly as LMDB does at takeover.
#[cfg(target_os = "linux")]
#[test]
fn killed_writer_takeover_matches_lmdb() {
    let run_lmdb = || {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        let rpid = kill_child("lmdb_hold_readers:1", dir.path());
        assert!(
            env.reader_pids().contains(&rpid),
            "setup: the dead reader is listed before the takeover"
        );
        kill_child("lmdb_hold_writer", dir.path());
        env.commit_one();
        let t = (env.reader_pids().contains(&rpid), env.has_key(b"doomed"));
        eprintln!("lmdb (reader listed, doomed present) = {t:?}");
        t
    };
    let run_zdb = || {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let rpid = kill_child("zdb_hold_readers:1", dir.path());
        assert!(
            env.reader_list().iter().any(|e| e.pid == Some(rpid)),
            "setup: the dead reader is listed before the takeover"
        );
        kill_child("zdb_hold_writer", dir.path());
        zdb_commit_one(&env);
        // List first, as the LMDB side does, so the read txn's own slot is
        // not yet registered when sampling.
        let listed = env.reader_list().iter().any(|e| e.pid == Some(rpid));
        let r = env.read_txn().unwrap();
        let t = (
            listed,
            env.main_database().get(&r, b"doomed").unwrap().is_some(),
        );
        eprintln!("zdb (reader listed, doomed present) = {t:?}");
        t
    };
    assert_eq!(
        run_zdb(),
        run_lmdb(),
        "(dead reader still listed, doomed key present)"
    );
}
