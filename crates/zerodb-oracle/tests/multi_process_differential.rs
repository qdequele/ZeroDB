//! Differential tests of multi-process behavior (ADR-0023): the same
//! scenario run against the Meilisearch LMDB fork (through `lmdb-master-sys`)
//! and against ZeroDB with a lock file, comparing what each engine reports.
//! Stage 2 covers dead-process handling; stage 3 covers map growth across
//! processes (amendment 15).
//!
//! The test binary re-runs itself as the child process (SIGKILLed for the
//! dead-process cases).
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
const MIB: u64 = 1 << 20;
/// ZeroDB's page size in these tests. LMDB uses the OS page size
/// ([`Lmdb::psize`]); each engine's sizes are computed with its own.
const PSIZE: u64 = 4096;
/// Past 1 MiB (so a 1 MiB map cannot hold it) and under 2 MiB.
const GROW_TARGET: u64 = MIB + MIB / 4;

struct Lmdb {
    env: *mut ffi::MDB_env,
}

impl Lmdb {
    fn open(dir: &Path, max_readers: u32) -> Lmdb {
        Lmdb::open_sized(dir, max_readers, MIB)
    }

    fn open_sized(dir: &Path, max_readers: u32, mapsize: u64) -> Lmdb {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle owned by this
        // struct; `path` outlives `mdb_env_open`.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, max_readers), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, mapsize as _), 0);
            assert_eq!(ffi::mdb_env_open(env, path.as_ptr(), MDB_NOTLS, 0o644), 0);
        }
        Lmdb { env }
    }

    /// Open with `mapsize` and extra `flags` (`MDB_NOTLS` is added); the raw
    /// return code on failure.
    fn try_open_flags(
        dir: &Path,
        max_readers: u32,
        mapsize: u64,
        flags: c_uint,
    ) -> Result<Lmdb, c_int> {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle; on failure the
        // handle is closed as the C API requires, and `path` outlives the
        // `mdb_env_open` call.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, max_readers), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, mapsize as _), 0);
            let rc = ffi::mdb_env_open(env, path.as_ptr(), MDB_NOTLS | flags, 0o644);
            if rc != 0 {
                ffi::mdb_env_close(env);
                return Err(rc);
            }
        }
        Ok(Lmdb { env })
    }

    /// `mdb_env_get_maxreaders`.
    fn maxreaders(&self) -> u32 {
        let mut n: c_uint = 0;
        // SAFETY: plain call on an open env; `n` outlives it.
        assert_eq!(unsafe { ffi::mdb_env_get_maxreaders(self.env, &mut n) }, 0);
        n
    }

    /// Begin a read txn; the raw return code on failure.
    fn try_read(&self) -> Result<*mut ffi::MDB_txn, c_int> {
        self.try_begin(MDB_RDONLY)
    }

    /// Begin a txn with `flags`; the raw return code on failure.
    fn try_begin(&self, flags: c_uint) -> Result<*mut ffi::MDB_txn, c_int> {
        let mut txn = ptr::null_mut();
        // SAFETY: `self.env` is open; `txn` receives a handle we own.
        let rc = unsafe { ffi::mdb_txn_begin(self.env, ptr::null_mut(), flags, &mut txn) };
        if rc == 0 {
            Ok(txn)
        } else {
            Err(rc)
        }
    }

    /// Begin and abort a txn with `flags`, normalized for comparison.
    fn begin_outcome(&self, flags: c_uint) -> String {
        match self.try_begin(flags) {
            Ok(txn) => {
                Lmdb::abort(txn);
                "ok".to_string()
            }
            Err(ffi::MDB_MAP_RESIZED) => "MapResized".to_string(),
            Err(rc) => format!("rc={rc}"),
        }
    }

    /// The page size (LMDB uses the OS page size: 16 KiB on macOS aarch64).
    fn psize(&self) -> u64 {
        // SAFETY: MDB_stat is plain data; LMDB fills it.
        let st = unsafe {
            let mut st: ffi::MDB_stat = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_stat(self.env, &mut st), 0);
            st
        };
        u64::from(st.ms_psize)
    }

    /// `(me_mapsize, me_last_pgno)`.
    fn map_info(&self) -> (u64, u64) {
        // SAFETY: MDB_envinfo is plain data; LMDB fills it.
        let i = unsafe {
            let mut i: ffi::MDB_envinfo = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_info(self.env, &mut i), 0);
            i
        };
        (i.me_mapsize as u64, i.me_last_pgno as u64)
    }

    /// Commit batches of 1000-byte values until the file's last page lies
    /// past [`GROW_TARGET`].
    fn grow(&self) {
        let value = [7u8; 1000];
        let psize = self.psize();
        let mut i = 0u32;
        while (self.map_info().1 + 1) * psize <= GROW_TARGET {
            // SAFETY: standard begin/dbi_open/put/commit sequence on an open
            // env; every pointer is live for the duration of its call.
            unsafe {
                let txn = self.try_begin(0).unwrap();
                let mut dbi = 0;
                assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
                for j in 0..8 {
                    let key = format!("g{:08}", i * 8 + j);
                    let mut k = ffi::MDB_val {
                        mv_size: key.len(),
                        mv_data: key.as_ptr() as *mut c_void,
                    };
                    let mut v = ffi::MDB_val {
                        mv_size: value.len(),
                        mv_data: value.as_ptr() as *mut c_void,
                    };
                    assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
                }
                assert_eq!(ffi::mdb_txn_commit(txn), 0);
            }
            i += 1;
            assert!(i < 100_000, "never grew");
        }
    }

    /// `mdb_env_set_mapsize`'s raw return code.
    fn set_mapsize(&self, size: u64) -> c_int {
        // SAFETY: `self.env` is open; the call takes a plain size.
        unsafe { ffi::mdb_env_set_mapsize(self.env, size as _) }
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

    /// Put `n` 1000-byte values in **one** txn and commit, so the meta's
    /// `last_pg` moves well past a small env in a single generation (unlike
    /// [`Lmdb::grow`], which commits many small txns and so would make the
    /// on-disk "previous" meta the second-to-last of those, not whatever the
    /// caller committed before calling this).
    fn commit_many(&self, n: u32) {
        let value = [7u8; 1000];
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
            for i in 0..n {
                let key = format!("g{i:08}");
                let mut k = ffi::MDB_val {
                    mv_size: key.len(),
                    mv_data: key.as_ptr() as *mut c_void,
                };
                let mut v = ffi::MDB_val {
                    mv_size: value.len(),
                    mv_data: value.as_ptr() as *mut c_void,
                };
                assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
            }
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
    zdb_open_sized(dir, Some(MIB))
}

/// A lock-file env at `map_size` bytes, or at the persisted size for `None`.
fn zdb_open_sized(dir: &Path, map_size: Option<u64>) -> zerodb::Env {
    let mut o = zerodb::EnvOpenOptions::new();
    o.page_size(PSIZE as u32)
        .max_readers(MAX_READERS)
        .lock_file(true);
    if let Some(m) = map_size {
        o.map_size(usize::try_from(m).unwrap());
    }
    o.open(dir).expect("open lock-file env")
}

/// Normalize a ZeroDB begin result for comparison with [`Lmdb::begin_outcome`].
fn zdb_outcome<T>(r: zerodb::Result<T>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(zerodb::Error::Mdb(zerodb::MdbError::MapResized)) => "MapResized".to_string(),
        Err(e) => format!("{e:?}"),
    }
}

/// [`Lmdb::grow`] for ZeroDB: the same batches, until the same bound.
fn zdb_grow(env: &zerodb::Env) {
    let db = env.main_database();
    let value = [7u8; 1000];
    let mut i = 0u32;
    while (env.info().last_pgno + 1) * PSIZE <= GROW_TARGET {
        let mut w = env.write_txn().unwrap();
        for j in 0..8 {
            db.put(&mut w, format!("g{:08}", i * 8 + j).as_bytes(), &value)
                .unwrap();
        }
        w.commit().unwrap();
        i += 1;
        assert!(i < 100_000, "never grew");
    }
}

/// `mm_mapsize` of the newest meta page in LMDB's `data.mdb` (page size
/// `psize`), read from the file. Offsets inside a meta page: 16-byte page
/// header, then magic u32, version u32, address u64, mapsize u64 (@32), two
/// 48-byte MDB_db records, last_pg u64 (@136), txnid u64 (@144).
fn lmdb_meta_mapsize(dir: &Path, psize: u64) -> u64 {
    let bytes = std::fs::read(dir.join("data.mdb")).unwrap();
    let u64_at = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    let magic = |base: usize| u32::from_le_bytes(bytes[base + 16..base + 20].try_into().unwrap());
    (0..2)
        .map(|i| (i * psize) as usize)
        .inspect(|&base| assert_eq!(magic(base), 0xBEEF_C0DE, "meta magic"))
        .map(|base| (u64_at(base + 144), u64_at(base + 32)))
        .max()
        .unwrap()
        .1
}

/// Both meta slots' `(mm_mapsize, mm_last_pg, mm_txnid)`, ascending by
/// `mm_txnid`: `[0]` is the older ("prev") generation, `[1]` the newest. Same
/// offsets as [`lmdb_meta_mapsize`]; mirrors
/// `lmdb_stage3_observation.rs::read_metas`.
fn lmdb_meta_generations(dir: &Path, psize: u64) -> Vec<(u64, u64, u64)> {
    let bytes = std::fs::read(dir.join("data.mdb")).unwrap();
    let u64_at = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    let mut metas: Vec<(u64, u64, u64)> = (0..2)
        .map(|i| {
            let base = (i * psize) as usize;
            let magic = u32::from_le_bytes(bytes[base + 16..base + 20].try_into().unwrap());
            assert_eq!(magic, 0xBEEF_C0DE, "meta magic");
            (u64_at(base + 32), u64_at(base + 136), u64_at(base + 144))
        })
        .collect();
    metas.sort_by_key(|m| m.2);
    metas
}

fn zdb_commit_one(env: &zerodb::Env) {
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
}

/// [`zdb_commit_one`], but `n` 1000-byte values in **one** txn, so the meta's
/// `last_pg` moves well past a small env in a single generation (see
/// [`Lmdb::commit_many`] for why that matters here).
fn zdb_commit_many(env: &zerodb::Env, n: u32) {
    let db = env.main_database();
    let value = vec![7u8; 1000];
    let mut w = env.write_txn().unwrap();
    for i in 0..n {
        db.put(&mut w, format!("g{i:08}").as_bytes(), &value)
            .unwrap();
    }
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

/// Run a child role to completion; panics on a failure exit or a 60 s
/// timeout.
fn run_child(role: &str, dir: &Path) {
    let mut c = spawn(role, dir);
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(st) = c.0.try_wait().unwrap() {
            assert!(st.success(), "child {role} failed: {st}");
            return;
        }
        assert!(Instant::now() < end, "timed out waiting for child {role}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Wait for an already-spawned child to exit successfully (30 s deadline),
/// for a role that exits on its own once told to (unlike [`run_child`],
/// which spawns too, and unlike [`kill_child`], which SIGKILLs).
fn wait_exit(c: &mut ChildGuard, role: &str) {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(st) = c.0.try_wait().unwrap() {
            assert!(st.success(), "child {role} failed: {st}");
            return;
        }
        assert!(
            Instant::now() < end,
            "timed out waiting for child {role} to exit"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Child entry point: a no-op unless run by `spawn`. Roles are
/// `{zdb,lmdb}_hold_readers:<n>` and `{zdb,lmdb}_hold_writer`; the writer
/// roles put `doomed` in an uncommitted write txn before signaling. The map
/// roles open with an 8 MiB map and exit: `{zdb,lmdb}_grow` commit past
/// [`GROW_TARGET`], `{zdb,lmdb}_commit_one` commit one small key.
/// `{zdb,lmdb}_hold_open` just hold the env open (no txn) until `stop` is
/// written, then exit cleanly.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let signal = || std::fs::write(dir.join("held"), b"").unwrap();
    let park = || std::thread::sleep(Duration::from_secs(600));
    match role.as_str() {
        "lmdb_grow" => return Lmdb::open_sized(&dir, MAX_READERS, 8 * MIB).grow(),
        "lmdb_commit_one" => return Lmdb::open_sized(&dir, MAX_READERS, 8 * MIB).commit_one(),
        "zdb_grow" => return zdb_grow(&zdb_open_sized(&dir, Some(8 * MIB))),
        "zdb_commit_one" => return zdb_commit_one(&zdb_open_sized(&dir, Some(8 * MIB))),
        "lmdb_hold_open" => {
            let _env = Lmdb::open(&dir, MAX_READERS);
            signal();
            wait_for(&dir.join("stop"));
            return;
        }
        "zdb_hold_open" => {
            let _env = zdb_open(&dir);
            signal();
            wait_for(&dir.join("stop"));
            return;
        }
        _ => {}
    }
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

/// Linux only (the writer takeover is not observed on macOS; the takeover
/// scenario runs only on Linux): a SIGKILLed
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

/// A process with a 1 MiB map, after an 8 MiB process committed past it:
/// `(read begin, write begin)` (ADR-0023 amendment 15;
/// `OBS_READ_BEGIN_SMALL_MAP`, `OBS_WRITE_BEGIN_SMALL_MAP`).
#[test]
fn begins_on_a_small_map_after_another_process_grew_match_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        run_child("lmdb_grow", dir.path());
        (env.begin_outcome(MDB_RDONLY), env.begin_outcome(0))
    };
    eprintln!("lmdb (read, write) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        run_child("zdb_grow", dir.path());
        (zdb_outcome(env.read_txn()), zdb_outcome(env.write_txn()))
    };
    eprintln!("zdb (read, write) = {zdb:?}");
    assert_eq!(zdb, lmdb, "(read begin, write begin) on a small map");
}

/// A larger map size in the meta alone, while the data still fits this
/// process's map: `(read begin, write begin)`
/// (`OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES` = false).
#[test]
fn begins_with_only_a_bigger_meta_size_match_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        run_child("lmdb_commit_one", dir.path());
        assert_eq!(lmdb_meta_mapsize(dir.path(), env.psize()), 8 * MIB, "setup");
        (env.begin_outcome(MDB_RDONLY), env.begin_outcome(0))
    };
    eprintln!("lmdb (read, write) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        run_child("zdb_commit_one", dir.path());
        (zdb_outcome(env.read_txn()), zdb_outcome(env.write_txn()))
    };
    eprintln!("zdb (read, write) = {zdb:?}");
    assert_eq!(
        zdb, lmdb,
        "(read begin, write begin) with a bigger meta size"
    );
}

/// A 1 MiB process commits after an 8 MiB process recorded 8 MiB in the
/// meta: the persisted map size afterwards
/// (`OBS_COMMIT_PERSISTS_OWN_MAPSIZE` = false).
#[test]
fn smaller_map_commit_keeps_the_meta_size_like_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        run_child("lmdb_commit_one", dir.path());
        env.commit_one();
        let psize = env.psize();
        drop(env);
        lmdb_meta_mapsize(dir.path(), psize)
    };
    eprintln!("lmdb persisted map size = {lmdb}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        run_child("zdb_commit_one", dir.path());
        zdb_commit_one(&env);
        drop(env);
        // Reopened without a map size, the env adopts the persisted one.
        zdb_open_sized(dir.path(), None).info().map_size
    };
    eprintln!("zdb persisted map size = {zdb}");
    assert_eq!(zdb, lmdb, "persisted map size after the smaller-map commit");
}

/// Open with a 1 MiB map below the committed data: the resulting map size,
/// classified against the request and `(last_pg + 1) * page_size`
/// (`OBS_OPEN_BELOW_LAST_PG = "minsize"`).
#[test]
fn open_below_last_pg_matches_lmdb() {
    let classify = |map_size: u64, last_pg: u64, psize: u64| {
        let minsize = (last_pg + 1) * psize;
        assert!(minsize > MIB, "setup: the data is past 1 MiB");
        if map_size == MIB {
            "requested".to_string()
        } else if map_size == minsize {
            "minsize".to_string()
        } else {
            format!("other={map_size}")
        }
    };
    let lmdb = {
        let dir = TempDir::new().unwrap();
        run_child("lmdb_grow", dir.path());
        let env = Lmdb::open(dir.path(), MAX_READERS);
        let (m, last) = env.map_info();
        Lmdb::abort(env.try_read().unwrap());
        classify(m, last, env.psize())
    };
    eprintln!("lmdb open below last_pg = {lmdb}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        run_child("zdb_grow", dir.path());
        let env = zdb_open(dir.path());
        let info = env.info();
        drop(env.read_txn().unwrap());
        classify(info.map_size, info.last_pgno, PSIZE)
    };
    eprintln!("zdb open below last_pg = {zdb}");
    assert_eq!(zdb, lmdb, "map size after an open below last_pg");
}

/// Two meta generations with different `last_pg` (a small first commit, then
/// a resize and a much larger second commit) and different persisted map
/// sizes: `MDB_PREVSNAPSHOT`'s minimum (a requested map below both
/// generations' `(last_pg + 1) * psize`) and its map-size-0 default both
/// come from the **older** generation, not the newest
/// (`OBS_PREV_SNAPSHOT_MINSIZE_FROM`, `OBS_PREV_SNAPSHOT_ZERO_MAPSIZE_FROM` =
/// "prev"). This test checks ZeroDB mirrors that (ADR-0023 stage 3, extra
/// item on Task 7).
#[test]
fn prev_snapshot_rounding_matches_lmdb() {
    let classify = |got: u64, prev: u64, newest: u64| {
        if got == prev {
            "prev".to_string()
        } else if got == newest {
            "newest".to_string()
        } else {
            format!("other={got}")
        }
    };

    let lmdb = {
        let dir = TempDir::new().unwrap();
        let psize = {
            let env = Lmdb::open_sized(dir.path(), MAX_READERS, 2 * MIB);
            env.commit_one();
            env.psize()
            // Dropped: exactly two meta generations now sit on disk (the
            // creation meta is long gone), so the raw read below sees
            // exactly the "prev" (commit_one) and this big commit's
            // "newest" generation, not an intermediate one (a `grow` loop
            // of many small commits would make the on-disk "prev" meta the
            // *second-to-last* loop commit, not `commit_one`'s).
        };
        {
            let env = Lmdb::open_sized(dir.path(), MAX_READERS, 16 * MIB);
            env.commit_many(300);
        }
        let metas = lmdb_meta_generations(dir.path(), psize);
        assert_eq!(metas.len(), 2);
        let (prev_mapsize, prev_last_pg) = (metas[0].0, metas[0].1);
        let (newest_mapsize, newest_last_pg) = (metas[1].0, metas[1].1);
        assert!(
            newest_last_pg > prev_last_pg,
            "setup: the second commit moved last_pg past the first's"
        );
        assert_ne!(prev_mapsize, newest_mapsize, "setup: lmdb mapsize changed");
        let prev_minsize = (prev_last_pg + 1) * psize;
        let newest_minsize = (newest_last_pg + 1) * psize;
        let requested = psize;
        assert!(
            requested < prev_minsize,
            "setup: the requested map is below both minimums"
        );
        let minsize_from =
            match Lmdb::try_open_flags(dir.path(), MAX_READERS, requested, ffi::MDB_PREVSNAPSHOT) {
                Ok(e) => classify(e.map_info().0, prev_minsize, newest_minsize),
                Err(rc) => format!("rc={rc}"),
            };
        let zero_from =
            match Lmdb::try_open_flags(dir.path(), MAX_READERS, 0, ffi::MDB_PREVSNAPSHOT) {
                Ok(e) => classify(e.map_info().0, prev_mapsize, newest_mapsize),
                Err(rc) => format!("rc={rc}"),
            };
        (minsize_from, zero_from)
    };
    eprintln!("lmdb (minsize_from, zero_from) = {lmdb:?}");

    let zdb = {
        let dir = TempDir::new().unwrap();
        let prev_info = {
            let mut o = zerodb::EnvOpenOptions::new();
            o.page_size(PSIZE as u32)
                .max_readers(MAX_READERS)
                .lock_file(true)
                .map_size(usize::try_from(2 * MIB).unwrap());
            let env = o.open(dir.path()).expect("open lock-file env");
            zdb_commit_one(&env);
            let info = env.info();
            // Dropped before the second generation, for the same reason as
            // the LMDB side above: exactly two generations end up on disk.
            info
        };
        let newest_info = {
            let mut o = zerodb::EnvOpenOptions::new();
            o.page_size(PSIZE as u32)
                .max_readers(MAX_READERS)
                .lock_file(true)
                .map_size(usize::try_from(16 * MIB).unwrap());
            let env = o.open(dir.path()).expect("open lock-file env");
            zdb_commit_many(&env, 300);
            env.info()
        };
        assert!(
            newest_info.last_pgno > prev_info.last_pgno,
            "setup: the second commit moved last_pg past the first's"
        );
        assert_ne!(
            prev_info.map_size, newest_info.map_size,
            "setup: zdb mapsize changed"
        );
        let prev_minsize = (prev_info.last_pgno + 1) * PSIZE;
        let newest_minsize = (newest_info.last_pgno + 1) * PSIZE;
        let requested = PSIZE;
        assert!(
            requested < prev_minsize,
            "setup: the requested map is below both minimums"
        );

        let mut po = zerodb::EnvOpenOptions::new();
        po.page_size(PSIZE as u32)
            .max_readers(MAX_READERS)
            .lock_file(true)
            .flags(zerodb::EnvFlags::PREV_SNAPSHOT)
            .map_size(usize::try_from(requested).unwrap());
        let minsize_from = match po.open(dir.path()) {
            Ok(e) => classify(e.info().map_size, prev_minsize, newest_minsize),
            Err(e) => format!("{e:?}"),
        };

        let mut zo = zerodb::EnvOpenOptions::new();
        zo.page_size(PSIZE as u32)
            .max_readers(MAX_READERS)
            .lock_file(true)
            .flags(zerodb::EnvFlags::PREV_SNAPSHOT);
        let zero_from = match zo.open(dir.path()) {
            Ok(e) => classify(e.info().map_size, prev_info.map_size, newest_info.map_size),
            Err(e) => format!("{e:?}"),
        };
        (minsize_from, zero_from)
    };
    eprintln!("zdb (minsize_from, zero_from) = {zdb:?}");
    assert_eq!(zdb, lmdb, "(minsize_from, zero_from) under PREV_SNAPSHOT");
}

/// `PREV_SNAPSHOT` requires winning the first-opener election (ADR-0023 §5):
/// opening while another process already has the environment open fails
/// (`OBS_PREV_SNAPSHOT_JOINER = EAGAIN`); once that process lets go, the same
/// open succeeds. A differential tuple for
/// `multi_process.rs::prev_snapshot_open_fails_while_another_process_has_the_env`.
#[test]
fn prev_snapshot_joiner_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let mut c = spawn("lmdb_hold_open", dir.path());
        wait_for(&dir.path().join("held"));
        let while_held = lmdb_rc(
            Lmdb::try_open_flags(dir.path(), MAX_READERS, MIB, ffi::MDB_PREVSNAPSHOT)
                .map(|_| 0)
                .unwrap_or_else(|rc| rc),
        );
        std::fs::write(dir.path().join("stop"), b"").unwrap();
        wait_exit(&mut c, "lmdb_hold_open");
        let after = lmdb_rc(
            Lmdb::try_open_flags(dir.path(), MAX_READERS, MIB, ffi::MDB_PREVSNAPSHOT)
                .map(|_| 0)
                .unwrap_or_else(|rc| rc),
        );
        (while_held, after)
    };
    eprintln!("lmdb (while_held, after) = {lmdb:?}");

    let zdb = {
        let dir = TempDir::new().unwrap();
        let mut c = spawn("zdb_hold_open", dir.path());
        wait_for(&dir.path().join("held"));
        let while_held = zdb_open_rc(zdb_prev_snapshot_open(dir.path()));
        std::fs::write(dir.path().join("stop"), b"").unwrap();
        wait_exit(&mut c, "zdb_hold_open");
        let after = zdb_open_rc(zdb_prev_snapshot_open(dir.path()));
        (while_held, after)
    };
    eprintln!("zdb (while_held, after) = {zdb:?}");
    assert_eq!(zdb, lmdb, "(while_held, after) under PREV_SNAPSHOT");
}

/// Normalize a `resize` / `mdb_env_set_mapsize` result: `ok`, or the raw OS
/// error code.
fn zdb_rc(r: zerodb::Result<()>) -> String {
    match r {
        Ok(()) => "ok".to_string(),
        Err(zerodb::Error::Io(e)) if e.raw_os_error().is_some() => {
            format!("rc={}", e.raw_os_error().unwrap())
        }
        Err(e) => format!("{e:?}"),
    }
}

fn lmdb_rc(rc: c_int) -> String {
    if rc == 0 {
        "ok".to_string()
    } else {
        format!("rc={rc}")
    }
}

/// [`zdb_rc`], for an open result instead of a unit result: `ok` (the env is
/// dropped immediately, releasing its lock file), or the raw OS error code.
fn zdb_open_rc(r: zerodb::Result<zerodb::Env>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(zerodb::Error::Io(e)) if e.raw_os_error().is_some() => {
            format!("rc={}", e.raw_os_error().unwrap())
        }
        Err(e) => format!("{e:?}"),
    }
}

/// A lock-file env open with `EnvFlags::PREV_SNAPSHOT`.
fn zdb_prev_snapshot_open(dir: &Path) -> zerodb::Result<zerodb::Env> {
    let mut o = zerodb::EnvOpenOptions::new();
    o.page_size(PSIZE as u32)
        .max_readers(MAX_READERS)
        .lock_file(true)
        .flags(zerodb::EnvFlags::PREV_SNAPSHOT)
        .map_size(usize::try_from(MIB).unwrap());
    o.open(dir)
}

/// After an 8 MiB process committed past this process's 1 MiB map,
/// `resize(0)` / `mdb_env_set_mapsize(0)`: `(result, adopted map size, read
/// begin afterwards)` (ADR-0023 amendment 15; `OBS_SET_MAPSIZE_ZERO_ADOPTS`).
#[test]
fn set_mapsize_zero_after_another_process_grew_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        run_child("lmdb_grow", dir.path());
        let rc = lmdb_rc(env.set_mapsize(0));
        (rc, env.map_info().0, env.begin_outcome(MDB_RDONLY))
    };
    eprintln!("lmdb (set_mapsize(0), map size, read begin) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        run_child("zdb_grow", dir.path());
        let rc = zdb_rc(env.resize(0));
        (rc, env.info().map_size, zdb_outcome(env.read_txn()))
    };
    eprintln!("zdb (resize(0), map size, read begin) = {zdb:?}");
    assert_eq!(zdb, lmdb, "(resize(0), adopted map size, read begin)");
}

/// `resize` / `mdb_env_set_mapsize` while this env has a live write txn
/// (`OBS_SET_MAPSIZE_WITH_WRITE_TXN`).
#[test]
fn set_mapsize_with_a_write_txn_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MAX_READERS);
        env.commit_one();
        let txn = env.try_begin(0).unwrap();
        let rc = lmdb_rc(env.set_mapsize(2 * MIB));
        Lmdb::abort(txn);
        rc
    };
    eprintln!("lmdb set_mapsize with a write txn = {lmdb}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let w = env.write_txn().unwrap();
        let rc = zdb_rc(env.resize(2 * MIB));
        drop(w);
        rc
    };
    eprintln!("zdb resize with a write txn = {zdb}");
    assert_eq!(zdb, lmdb, "resize with a live write txn");
}

/// While another process holds the environment with [`MAX_READERS`] reader
/// slots, a joiner that asks for fewer and one that asks for more: each
/// open's outcome and the reader count it ends up with (ADR-0023 §1 as
/// amended; `OBS_JOINER_FEWER_MAXREADERS`, `OBS_JOINER_MORE_MAXREADERS`). A
/// differential tuple for
/// `multi_process.rs::second_process_with_other_max_readers_adopts_the_lock_files`.
#[test]
fn joiner_with_other_max_readers_matches_lmdb() {
    let asks = [MAX_READERS / 2, MAX_READERS * 4];
    let lmdb: Vec<(String, u32)> = {
        let dir = TempDir::new().unwrap();
        let mut c = spawn("lmdb_hold_open", dir.path());
        wait_for(&dir.path().join("held"));
        let got = asks
            .iter()
            .map(|&n| match Lmdb::try_open_flags(dir.path(), n, MIB, 0) {
                Ok(env) => ("ok".to_string(), env.maxreaders()),
                Err(rc) => (lmdb_rc(rc), 0),
            })
            .collect();
        std::fs::write(dir.path().join("stop"), b"").unwrap();
        wait_exit(&mut c, "lmdb_hold_open");
        got
    };
    eprintln!("lmdb (outcome, maxreaders) per ask {asks:?} = {lmdb:?}");

    let zdb: Vec<(String, u32)> = {
        let dir = TempDir::new().unwrap();
        let mut c = spawn("zdb_hold_open", dir.path());
        wait_for(&dir.path().join("held"));
        let got = asks
            .iter()
            .map(|&n| {
                let mut o = zerodb::EnvOpenOptions::new();
                o.page_size(PSIZE as u32)
                    .max_readers(n)
                    .lock_file(true)
                    .map_size(usize::try_from(MIB).unwrap());
                match o.open(dir.path()) {
                    Ok(env) => ("ok".to_string(), env.info().max_readers),
                    Err(e) => (zdb_open_rc(Err(e)), 0),
                }
            })
            .collect();
        std::fs::write(dir.path().join("stop"), b"").unwrap();
        wait_exit(&mut c, "zdb_hold_open");
        got
    };
    eprintln!("zdb (outcome, maxreaders) per ask {asks:?} = {zdb:?}");
    assert_eq!(
        zdb, lmdb,
        "(outcome, maxreaders) of joiners with other counts"
    );
}
