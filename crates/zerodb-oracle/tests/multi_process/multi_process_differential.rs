//! Differential tests of multi-process behavior (ADR-0023): the same
//! scenario run against the Meilisearch LMDB fork (through `lmdb-master-sys`)
//! and against ZeroDB with a lock file, comparing what each engine reports.
//! Stage 2 covers dead-process handling; stage 3 covers map growth across
//! processes (amendment 15), the reader count with readers in another
//! process, the data-file length a `WRITE_MAP` joiner leaves, and joining a
//! `PREV_SNAPSHOT` environment before its first commit.
//!
//! The test binary re-runs itself as the child process (SIGKILLed for the
//! dead-process cases).
//! A mismatch between the engines is a ZeroDB bug unless recorded in
//! `docs/LMDB-COMPATIBILITY.md`; never edit an expectation to make ZeroDB pass.

use std::os::raw::{c_int, c_void};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

use lmdb_master_sys as ffi;
use zerodb_oracle::child::{self, ChildGuard};
use zerodb_oracle::ffi_lmdb::{Lmdb, Txn, MDB_RDONLY};
use zerodb_oracle::tempdir::TempDir;

const ROLE: &str = "ZERODB_MPDIFF_ROLE";
const DIR: &str = "ZERODB_MPDIFF_DIR";
const MAX_READERS: u32 = 8;
const MIB: u64 = 1 << 20;
/// ZeroDB's page size in these tests. LMDB uses the OS page size
/// ([`Lmdb::psize`]); each engine's sizes are computed with its own.
const PSIZE: u64 = 4096;
/// Past 1 MiB (so a 1 MiB map cannot hold it) and under 2 MiB.
const GROW_TARGET: u64 = MIB + MIB / 4;

/// Commit batches of 1000-byte values until the file's last page lies past
/// [`GROW_TARGET`].
fn lmdb_grow(env: &Lmdb) {
    let value = [7u8; 1000];
    let psize = env.psize();
    let mut i = 0u32;
    while (env.map_info().1 + 1) * psize <= GROW_TARGET {
        let pairs: Vec<(String, &[u8])> = (0..8)
            .map(|j| (format!("g{:08}", i * 8 + j), &value[..]))
            .collect();
        env.commit(&pairs);
        i += 1;
        assert!(i < 100_000, "never grew");
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

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    child::spawn_self(
        module_path!(),
        &[(ROLE, role.as_ref()), (DIR, dir.as_os_str())],
    )
}

/// Spawn a child in `role`, wait for its `held` marker, SIGKILL it, remove
/// the marker. Returns its PID.
fn kill_child(role: &str, dir: &Path) -> u32 {
    child::kill_when_held(dir, || spawn(role, dir))
}

/// Run a child role to completion; panics on a failure exit or a 60 s
/// timeout.
fn run_child(role: &str, dir: &Path) {
    child::wait_success(&mut spawn(role, dir), role, Duration::from_secs(60));
}

/// Wait for an already-spawned child to exit successfully (30 s deadline),
/// for a role that exits on its own once told to (unlike [`run_child`],
/// which spawns too, and unlike [`kill_child`], which SIGKILLs).
fn wait_exit(c: &mut ChildGuard, role: &str) {
    child::wait_success(c, role, Duration::from_secs(30));
}

/// Child entry point: a no-op unless run by `spawn`. Roles are
/// `{zdb,lmdb}_hold_readers:<n>` and `{zdb,lmdb}_hold_writer`; the writer
/// roles put `doomed` in an uncommitted write txn before signaling. The map
/// roles open with an 8 MiB map and exit: `{zdb,lmdb}_grow` commit past
/// [`GROW_TARGET`], `{zdb,lmdb}_commit_one` commit one small key.
/// `{zdb,lmdb}_hold_open` just hold the env open (no txn) until `stop` is
/// written, then exit cleanly. `{zdb,lmdb}_reads_until:<n>` begin n read
/// txns, end them when `release` is written, and exit when `stop` is.
/// `{zdb,lmdb}_join_writemap:<map>` open with `WRITE_MAP` at `<map>` bytes
/// and commit once, writing the data-file lengths to `result` (see
/// [`write_map_joiner_lengths`]). `{zdb,lmdb}_join_report` open (16 MiB map)
/// and write the snapshot a read txn sees to `result` (see
/// [`join_before_prev_snapshot_commit`]); `{zdb,lmdb}_join_info` also write
/// the read txn's id and the env's last txnid. `{zdb,lmdb}_prev_join_report`
/// do the same as `join_report` with `PREV_SNAPSHOT` ("EAGAIN" when refused),
/// and `{zdb,lmdb}_prev_hold` open with `PREV_SNAPSHOT`, signal and park.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let signal = || std::fs::write(dir.join("held"), b"").unwrap();
    let park = || std::thread::sleep(Duration::from_secs(600));
    match role.as_str() {
        "lmdb_grow" => return lmdb_grow(&Lmdb::open(&dir, 8 * MIB, MAX_READERS)),
        "lmdb_commit_one" => return Lmdb::open(&dir, 8 * MIB, MAX_READERS).commit_one(),
        "zdb_grow" => return zdb_grow(&zdb_open_sized(&dir, Some(8 * MIB))),
        "zdb_commit_one" => return zdb_commit_one(&zdb_open_sized(&dir, Some(8 * MIB))),
        "lmdb_hold_open" => {
            let _env = Lmdb::open(&dir, MIB, MAX_READERS);
            signal();
            child::wait_for(&dir.join("stop"));
            return;
        }
        "zdb_hold_open" => {
            let _env = zdb_open(&dir);
            signal();
            child::wait_for(&dir.join("stop"));
            return;
        }
        "lmdb_join_report" => {
            let out = match Lmdb::try_open(&dir, 16 * MIB, MAX_READERS, 0) {
                Ok(env) => {
                    let txn = env.try_read().unwrap();
                    let snap = lmdb_snapshot_of(&txn);
                    Lmdb::abort(txn);
                    format!("ok:{snap}")
                }
                Err(rc) => lmdb_rc(rc),
            };
            return write_result(&dir, &out);
        }
        "zdb_join_report" => {
            let out = match zdb_try_open(&dir, Some(16 * MIB), zerodb::EnvFlags::EMPTY) {
                Ok(env) => format!("ok:{}", zdb_snapshot_of(&env)),
                Err(e) => zdb_open_rc(Err(e)),
            };
            return write_result(&dir, &out);
        }
        "lmdb_join_info" => {
            let out = match Lmdb::try_open(&dir, 16 * MIB, MAX_READERS, 0) {
                Ok(env) => {
                    let (id, snap) = lmdb_read_view(&env);
                    format!("ok:{id} {} {snap}", env.last_txnid())
                }
                Err(rc) => lmdb_rc(rc),
            };
            return write_result(&dir, &out);
        }
        "zdb_join_info" => {
            let out = match zdb_try_open(&dir, Some(16 * MIB), zerodb::EnvFlags::EMPTY) {
                Ok(env) => {
                    let (id, snap) = zdb_read_view(&env);
                    format!("ok:{id} {} {snap}", env.info().last_txnid)
                }
                Err(e) => zdb_open_rc(Err(e)),
            };
            return write_result(&dir, &out);
        }
        "lmdb_prev_join_report" => {
            let out = match Lmdb::try_open(&dir, 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT) {
                Ok(env) => format!("ok:{}", lmdb_read_view(&env).1),
                Err(libc::EAGAIN) => "EAGAIN".to_string(),
                Err(rc) => lmdb_rc(rc),
            };
            return write_result(&dir, &out);
        }
        "zdb_prev_join_report" => {
            let out = match zdb_try_open(&dir, Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT) {
                Ok(env) => format!("ok:{}", zdb_snapshot_of(&env)),
                Err(zerodb::Error::Io(e)) if e.raw_os_error() == Some(libc::EAGAIN) => {
                    "EAGAIN".to_string()
                }
                Err(e) => zdb_open_rc(Err(e)),
            };
            return write_result(&dir, &out);
        }
        "lmdb_rdonly_prev_join" => {
            let out = match Lmdb::try_open(
                &dir,
                16 * MIB,
                MAX_READERS,
                MDB_RDONLY | ffi::MDB_PREVSNAPSHOT,
            ) {
                Ok(env) => {
                    let (id, snap) = lmdb_read_view(&env);
                    format!("ok:{id} {snap}")
                }
                Err(libc::EAGAIN) => "EAGAIN".to_string(),
                Err(rc) => lmdb_rc(rc),
            };
            return write_result(&dir, &out);
        }
        "zdb_rdonly_prev_join" => {
            let out = match zdb_try_open(
                &dir,
                Some(16 * MIB),
                zerodb::EnvFlags::READ_ONLY | zerodb::EnvFlags::PREV_SNAPSHOT,
            ) {
                Ok(env) => {
                    let (id, snap) = zdb_read_view(&env);
                    format!("ok:{id} {snap}")
                }
                Err(zerodb::Error::Io(e)) if e.raw_os_error() == Some(libc::EAGAIN) => {
                    "EAGAIN".to_string()
                }
                Err(e) => zdb_open_rc(Err(e)),
            };
            return write_result(&dir, &out);
        }
        "lmdb_prev_hold" => {
            let _env = Lmdb::try_open(&dir, 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap();
            signal();
            park();
            return;
        }
        "zdb_prev_hold" => {
            let _env = zdb_try_open(&dir, Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap();
            signal();
            park();
            return;
        }
        _ => {}
    }
    if let Some(map) = role.strip_prefix("lmdb_join_writemap:") {
        let len = || std::fs::metadata(dir.join("data.mdb")).unwrap().len();
        let before = len();
        let env =
            Lmdb::try_open(&dir, map.parse().unwrap(), MAX_READERS, ffi::MDB_WRITEMAP).unwrap();
        let after_open = len();
        env.commit_one();
        let after_write = len();
        drop(env);
        return write_result(&dir, &format!("{before} {after_open} {after_write}"));
    }
    if let Some(map) = role.strip_prefix("zdb_join_writemap:") {
        let len = || {
            std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME))
                .unwrap()
                .len()
        };
        let before = len();
        let env = zdb_try_open(
            &dir,
            Some(map.parse().unwrap()),
            zerodb::EnvFlags::WRITE_MAP,
        )
        .unwrap();
        assert!(!env.opened_exclusively(), "the joiner joins a running env");
        let after_open = len();
        zdb_commit_one(&env);
        let after_write = len();
        drop(env);
        return write_result(&dir, &format!("{before} {after_open} {after_write}"));
    }
    // `{lmdb,zdb}_reads_until:<n>`: begin n read txns, signal `held`; end
    // them when `release` appears and signal `released`; exit cleanly when
    // `stop` appears.
    let released = || {
        std::fs::write(dir.join("released"), b"").unwrap();
        child::wait_for(&dir.join("stop"));
    };
    if let Some(n) = role.strip_prefix("lmdb_reads_until:") {
        let env = Lmdb::open(&dir, MIB, MAX_READERS);
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.try_read().unwrap())
            .collect();
        signal();
        child::wait_for(&dir.join("release"));
        txns.into_iter().for_each(Lmdb::abort);
        return released();
    }
    if let Some(n) = role.strip_prefix("zdb_reads_until:") {
        let env = zdb_open(&dir);
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.read_txn().unwrap())
            .collect();
        signal();
        child::wait_for(&dir.join("release"));
        drop(txns);
        return released();
    }
    if let Some(n) = role.strip_prefix("lmdb_hold_readers:") {
        let env = Lmdb::open(&dir, MIB, MAX_READERS);
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap())
            .map(|_| env.try_read().unwrap())
            .collect();
        signal();
        park();
        drop(txns);
    } else if role == "lmdb_hold_writer" {
        let env = Lmdb::open(&dir, MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
            let env = Lmdb::open(dir.path(), 2 * MIB, MAX_READERS);
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
            let env = Lmdb::open(dir.path(), 16 * MIB, MAX_READERS);
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
            match Lmdb::try_open(dir.path(), requested, MAX_READERS, ffi::MDB_PREVSNAPSHOT) {
                Ok(e) => classify(e.map_info().0, prev_minsize, newest_minsize),
                Err(rc) => format!("rc={rc}"),
            };
        let zero_from = match Lmdb::try_open(dir.path(), 0, MAX_READERS, ffi::MDB_PREVSNAPSHOT) {
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
        child::wait_for(&dir.path().join("held"));
        let while_held = lmdb_rc(
            Lmdb::try_open(dir.path(), MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT)
                .map(|_| 0)
                .unwrap_or_else(|rc| rc),
        );
        std::fs::write(dir.path().join("stop"), b"").unwrap();
        wait_exit(&mut c, "lmdb_hold_open");
        let after = lmdb_rc(
            Lmdb::try_open(dir.path(), MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT)
                .map(|_| 0)
                .unwrap_or_else(|rc| rc),
        );
        (while_held, after)
    };
    eprintln!("lmdb (while_held, after) = {lmdb:?}");

    let zdb = {
        let dir = TempDir::new().unwrap();
        let mut c = spawn("zdb_hold_open", dir.path());
        child::wait_for(&dir.path().join("held"));
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
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
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
        env.commit_one();
        let txn = env.begin(0).unwrap();
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
        child::wait_for(&dir.path().join("held"));
        let got = asks
            .iter()
            .map(|&n| match Lmdb::try_open(dir.path(), MIB, n, 0) {
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
        child::wait_for(&dir.path().join("held"));
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

/// A sole opener after an earlier, closed run: the reader count it ends up
/// with when it asks for fewer slots than that run used (LMDB keeps the
/// larger table, `OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS`), then for
/// more (the table grows). ADR-0023 §1 as amended.
#[test]
fn sole_opener_after_a_larger_run_matches_lmdb() {
    let asks = [MAX_READERS * 4, MAX_READERS, MAX_READERS * 8];
    let lmdb: Vec<u32> = {
        let dir = TempDir::new().unwrap();
        asks.iter()
            .map(|&n| Lmdb::open(dir.path(), MIB, n).maxreaders())
            .collect()
    };
    eprintln!("lmdb maxreaders per sequential sole open {asks:?} = {lmdb:?}");

    let zdb: Vec<u32> = {
        let dir = TempDir::new().unwrap();
        asks.iter()
            .map(|&n| {
                let mut o = zerodb::EnvOpenOptions::new();
                o.page_size(PSIZE as u32)
                    .max_readers(n)
                    .lock_file(true)
                    .map_size(usize::try_from(MIB).unwrap());
                let env = o.open(dir.path()).expect("sole open");
                assert!(env.opened_exclusively());
                env.info().max_readers
            })
            .collect()
    };
    eprintln!("zdb maxreaders per sequential sole open {asks:?} = {zdb:?}");
    assert_eq!(zdb, lmdb, "maxreaders of sequential sole opens");
}

/// `info()`/`stat()` (`mdb_env_info`/`mdb_env_stat`) see another process's
/// commit without this process opening a read txn of its own:
/// `(last_txnid advanced, entries advanced)`
/// (`OBS_INFO_SEES_OTHER_COMMIT`/`OBS_STAT_SEES_OTHER_COMMIT`, both `true` —
/// `crates/zerodb-oracle/tests/multi_process/lmdb_stage3_observation.rs`). A differential
/// tuple for
/// `multi_process.rs::info_and_stat_see_another_processes_commit`.
#[test]
fn info_and_stat_see_other_commit_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
        // One distinct key so a later, different key from the child is a
        // genuine entry-count increase, not an overwrite.
        env.commit_many(1);
        let txnid_before = env.last_txnid();
        let entries_before = env.entries();
        run_child("lmdb_commit_one", dir.path());
        let t = (
            env.last_txnid() > txnid_before,
            env.entries() > entries_before,
        );
        eprintln!("lmdb (last_txnid advanced, entries advanced) = {t:?}");
        t
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_many(&env, 1);
        let txnid_before = env.info().last_txnid;
        let entries_before = env.stat().entries;
        run_child("zdb_commit_one", dir.path());
        let t = (
            env.info().last_txnid > txnid_before,
            env.stat().entries > entries_before,
        );
        eprintln!("zdb (last_txnid advanced, entries advanced) = {t:?}");
        t
    };
    assert_eq!(zdb, lmdb, "(last_txnid advanced, entries advanced)");
}

/// Run `role` (a `*_reads_until:3` child) while this process, holding no
/// reader, samples `(reader count, reader-list entries)` with `sample`: while
/// the child holds its 3 read txns, after it ends them, after it exits, and
/// with `own`, which samples while this process holds one read txn of its
/// own.
fn reader_counts<E>(
    dir: &Path,
    env: &E,
    role: &str,
    sample: impl Fn(&E) -> (u32, usize),
    own: impl Fn(&E) -> (u32, usize),
) -> [(u32, usize); 4] {
    let mut c = spawn(role, dir);
    child::wait_for(&dir.join("held"));
    let held = sample(env);
    std::fs::write(dir.join("release"), b"").unwrap();
    child::wait_for(&dir.join("released"));
    let ended = sample(env);
    std::fs::write(dir.join("stop"), b"").unwrap();
    wait_exit(&mut c, role);
    [held, ended, sample(env), own(env)]
}

/// `EnvInfo::num_readers` (`me_numreaders`) and the reader list with readers
/// in another process (`OBS_NUMREADERS_OTHER_PROCESS`): LMDB's count is the
/// shared high-water mark of reader slots, so this process sees the other
/// process's readers, and the count never drops.
#[test]
fn reader_count_with_readers_in_another_process_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), MIB, MAX_READERS);
        env.commit_one();
        let sample = |e: &Lmdb| {
            // SAFETY: MDB_envinfo is plain data; LMDB fills it.
            let i = unsafe {
                let mut i: ffi::MDB_envinfo = std::mem::zeroed();
                assert_eq!(ffi::mdb_env_info(e.env, &mut i), 0);
                i
            };
            (i.me_numreaders, e.reader_pids().len())
        };
        reader_counts(dir.path(), &env, "lmdb_reads_until:3", sample, |e| {
            let txn = e.try_read().unwrap();
            let got = sample(e);
            Lmdb::abort(txn);
            got
        })
    };
    eprintln!("lmdb (numreaders, listed) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let sample = |e: &zerodb::Env| (e.info().num_readers, e.reader_list().len());
        reader_counts(dir.path(), &env, "zdb_reads_until:3", sample, |e| {
            let _txn = e.read_txn().unwrap();
            sample(e)
        })
    };
    eprintln!("zdb (numreaders, listed) = {zdb:?}");
    assert_eq!(
        zdb, lmdb,
        "(num_readers, reader-list entries): held elsewhere, ended, exited, own reader"
    );
}

/// A lock-file env with `flags` at `map_size` bytes (the persisted size for
/// `None`).
fn zdb_try_open(
    dir: &Path,
    map_size: Option<u64>,
    flags: zerodb::EnvFlags,
) -> zerodb::Result<zerodb::Env> {
    let mut o = zerodb::EnvOpenOptions::new();
    o.page_size(PSIZE as u32)
        .max_readers(MAX_READERS)
        .lock_file(true)
        .flags(flags);
    if let Some(m) = map_size {
        o.map_size(usize::try_from(m).unwrap());
    }
    o.open(dir)
}

/// Write `out` to `dir/result` atomically (a reader never sees it partial).
fn write_result(dir: &Path, out: &str) {
    std::fs::write(dir.join("result.tmp"), out).unwrap();
    std::fs::rename(dir.join("result.tmp"), dir.join("result")).unwrap();
}

/// The contents of `path` once it exists, or `None` after `timeout`.
fn poll_file(path: &Path, timeout: Duration) -> Option<String> {
    let end = Instant::now() + timeout;
    loop {
        if let Ok(s) = std::fs::read_to_string(path) {
            return Some(s);
        }
        if Instant::now() >= end {
            return None;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Classify a data-file length against the one before the joiner's open and
/// the joiner's map size.
fn classify_len(len: u64, before: u64, joiner_map: u64) -> String {
    if len == before {
        "unchanged".to_string()
    } else if len == joiner_map {
        "joiner_map".to_string()
    } else {
        format!("other={len}")
    }
}

/// This process holds the env open (one commit); a child joins it with
/// `WRITE_MAP` at `joiner_map` bytes and commits once. The data-file length
/// right after the joiner's open and right after its commit, each classified
/// by [`classify_len`]. `holder_write_map` opens the holder with `WRITE_MAP`
/// at 8 MiB (its file is then 8 MiB long), otherwise without it at 1 MiB.
fn write_map_joiner_lengths(holder_write_map: bool, joiner_map: u64) -> [(String, String); 2] {
    let parse = |dir: &Path| -> (u64, u64, u64) {
        let out = std::fs::read_to_string(dir.join("result")).unwrap();
        let v: Vec<u64> = out.split(' ').map(|n| n.parse().unwrap()).collect();
        (v[0], v[1], v[2])
    };
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let (flags, map) = if holder_write_map {
            (ffi::MDB_WRITEMAP, 8 * MIB)
        } else {
            (0, MIB)
        };
        let env = Lmdb::try_open(dir.path(), map, MAX_READERS, flags).unwrap();
        env.commit_one();
        run_child(&format!("lmdb_join_writemap:{joiner_map}"), dir.path());
        let (before, open, write) = parse(dir.path());
        eprintln!("lmdb lengths: before {before}, after open {open}, after commit {write}");
        drop(env);
        (
            classify_len(open, before, joiner_map),
            classify_len(write, before, joiner_map),
        )
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = if holder_write_map {
            zdb_try_open(dir.path(), Some(8 * MIB), zerodb::EnvFlags::WRITE_MAP).unwrap()
        } else {
            zdb_open(dir.path())
        };
        zdb_commit_one(&env);
        run_child(&format!("zdb_join_writemap:{joiner_map}"), dir.path());
        let (before, open, write) = parse(dir.path());
        eprintln!("zdb lengths: before {before}, after open {open}, after commit {write}");
        drop(env);
        (
            classify_len(open, before, joiner_map),
            classify_len(write, before, joiner_map),
        )
    };
    [lmdb, zdb]
}

/// A process joins a running env with `WRITE_MAP` and a larger map (8 MiB)
/// than the 1 MiB holder's file: LMDB's open truncates the file up to its
/// map size (`OBS_WRITE_MAP_JOINER_LARGER_MAP = "joiner_map"`); ZeroDB's
/// open leaves the file as it is and its first write txn grows it to the
/// same length (docs/LMDB-COMPATIBILITY.md D-024; ADR-0023 amendment 15, decision
/// 4). After the joiner's first commit both files have the joiner's map
/// size.
#[test]
fn write_map_joiner_with_a_larger_map_file_length_vs_lmdb() {
    let [lmdb, zdb] = write_map_joiner_lengths(false, 8 * MIB);
    eprintln!("(after open, after commit): lmdb {lmdb:?}, zdb {zdb:?}");
    assert_eq!(
        lmdb,
        ("joiner_map".to_string(), "joiner_map".to_string()),
        "LMDB as observed"
    );
    assert_eq!(
        zdb,
        ("unchanged".to_string(), "joiner_map".to_string()),
        "ZeroDB as D-024 records"
    );
}

/// [`write_map_joiner_with_a_larger_map_file_length_vs_lmdb`] with the holder
/// opened `WRITE_MAP` at 8 MiB and the joiner at 2 MiB: LMDB's open shrinks
/// the file to 2 MiB under the holder's 8 MiB mapping
/// (`OBS_WRITE_MAP_JOINER_SMALLER_MAP = "joiner_map"`); ZeroDB never shrinks
/// it (D-024).
#[test]
fn write_map_joiner_with_a_smaller_map_file_length_vs_lmdb() {
    let [lmdb, zdb] = write_map_joiner_lengths(true, 2 * MIB);
    eprintln!("(after open, after commit): lmdb {lmdb:?}, zdb {zdb:?}");
    assert_eq!(
        lmdb,
        ("joiner_map".to_string(), "joiner_map".to_string()),
        "LMDB as observed"
    );
    assert_eq!(
        zdb,
        ("unchanged".to_string(), "unchanged".to_string()),
        "ZeroDB as D-024 records"
    );
}

/// Classify a snapshot of [`join_before_prev_snapshot_commit`]'s env by the
/// keys `has` finds: "a_commit2" (`after2`), "a_commit" (`after`), "newest"
/// (`g00000000`, the generation `PREV_SNAPSHOT` rolled back) or "prev".
fn snapshot_by(has: impl Fn(&[u8]) -> bool) -> &'static str {
    if has(b"after2") {
        "a_commit2"
    } else if has(b"after") {
        "a_commit"
    } else if has(b"g00000000") {
        "newest"
    } else {
        "prev"
    }
}

fn lmdb_snapshot_of(txn: &Txn) -> &'static str {
    snapshot_by(|k| Lmdb::txn_has_key(txn, k))
}

/// The snapshot a fresh read txn of `env` sees.
fn zdb_snapshot_of(env: &zerodb::Env) -> &'static str {
    let r = env.read_txn().unwrap();
    let db = env.main_database();
    snapshot_by(|k| db.get(&r, k).unwrap().is_some())
}

/// B's outcome at three points, as in `OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`
/// (`lmdb_stage3_observation.rs`): within 3 s of starting B, within 10 s after
/// A's first commit, within 10 s after A closes; then A's own read view
/// after its first and second commits.
type JoinTimeline = ([String; 3], [&'static str; 2]);

/// Process A (this one) opens an env with two meta generations with
/// `PREV_SNAPSHOT`; process B (`role`) opens it before A's first commit.
/// `open_prev` opens A, `commit` commits one key through it, `view` reads
/// A's snapshot.
fn join_before_prev_snapshot_commit<E>(
    dir: &Path,
    role: &str,
    open_prev: impl FnOnce() -> E,
    commit: impl Fn(&E, &[u8]),
    view: impl Fn(&E) -> &'static str,
) -> JoinTimeline {
    let result = dir.join("result");
    let a = open_prev();
    let _b = spawn(role, dir);
    let mut pending = true;
    let mut step = |timeout: Duration| {
        if !pending {
            return "-".to_string();
        }
        match poll_file(&result, timeout) {
            Some(s) => {
                pending = false;
                s
            }
            None => "blocked".to_string(),
        }
    };
    let s0 = step(Duration::from_secs(3));
    commit(&a, b"after");
    let first = view(&a);
    let s1 = step(Duration::from_secs(10));
    commit(&a, b"after2");
    let second = view(&a);
    drop(a);
    let s2 = step(Duration::from_secs(10));
    ([s0, s1, s2], [first, second])
}

/// A process joins an environment that another process opened with
/// `PREV_SNAPSHOT`, before that process's first commit. LMDB holds the
/// joiner's open until the first commit, after which the joiner and the
/// rolling-back process both read the generation rolled back to until a
/// second commit (`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`,
/// `OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW`). ZeroDB mirrors it (ADR-0023
/// amendment 19, SPEC 04 §12.1).
#[test]
fn join_before_prev_snapshot_first_commit_vs_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        {
            let env = Lmdb::open(dir.path(), 2 * MIB, MAX_READERS);
            env.commit_one();
        }
        {
            let env = Lmdb::open(dir.path(), 16 * MIB, MAX_READERS);
            env.commit_many(300);
        }
        join_before_prev_snapshot_commit(
            dir.path(),
            "lmdb_join_report",
            || Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap(),
            |e, key| {
                e.commit_key(key);
            },
            |e| {
                let txn = e.try_read().unwrap();
                let snap = lmdb_snapshot_of(&txn);
                Lmdb::abort(txn);
                snap
            },
        )
    };
    eprintln!("lmdb (B's timeline, A's view) = {lmdb:?}");
    let zdb = {
        let dir = TempDir::new().unwrap();
        {
            let env = zdb_open_sized(dir.path(), Some(2 * MIB));
            zdb_commit_one(&env);
        }
        {
            let env = zdb_open_sized(dir.path(), Some(16 * MIB));
            zdb_commit_many(&env, 300);
        }
        join_before_prev_snapshot_commit(
            dir.path(),
            "zdb_join_report",
            || zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap(),
            |e, key| {
                let mut w = e.write_txn().unwrap();
                e.main_database().put(&mut w, key, b"v").unwrap();
                w.commit().unwrap();
            },
            zdb_snapshot_of,
        )
    };
    eprintln!("zdb (B's timeline, A's view) = {zdb:?}");
    let s = |v: [&str; 3]| v.map(str::to_string);
    assert_eq!(
        lmdb,
        (s(["blocked", "ok:prev", "-"]), ["prev", "a_commit2"]),
        "LMDB as observed"
    );
    assert_eq!(zdb, lmdb, "ZeroDB mirrors LMDB (ADR-0023 amendment 19)");
}

/// Two meta generations in an LMDB env, as in
/// [`join_before_prev_snapshot_first_commit_vs_lmdb`]: txn 1 ("prev", key
/// `k`, 2 MiB map), then txn 2 (300 values, 16 MiB map).
fn lmdb_two_generations(dir: &Path) {
    {
        let env = Lmdb::open(dir, 2 * MIB, MAX_READERS);
        env.commit_one();
    }
    let env = Lmdb::open(dir, 16 * MIB, MAX_READERS);
    env.commit_many(300);
}

/// [`lmdb_two_generations`] for ZeroDB.
fn zdb_two_generations(dir: &Path) {
    {
        let env = zdb_open_sized(dir, Some(2 * MIB));
        zdb_commit_one(&env);
    }
    let env = zdb_open_sized(dir, Some(16 * MIB));
    zdb_commit_many(&env, 300);
}

/// [`|e: &Lmdb, k: &[u8]| e.commit_key(k)`] for ZeroDB.
fn zdb_commit_key(e: &zerodb::Env, key: &[u8]) -> u64 {
    let mut w = e.write_txn().unwrap();
    let id = w.txnid();
    e.main_database().put(&mut w, key, b"v").unwrap();
    w.commit().unwrap();
    id
}

/// A fresh read txn's id and snapshot (see [`snapshot_by`]).
fn lmdb_read_view(e: &Lmdb) -> (u64, &'static str) {
    let txn = e.try_read().unwrap();
    let id = txn.id();
    let snap = lmdb_snapshot_of(&txn);
    Lmdb::abort(txn);
    (id, snap)
}

/// [`lmdb_read_view`] for ZeroDB.
fn zdb_read_view(e: &zerodb::Env) -> (u64, &'static str) {
    let r = e.read_txn().unwrap();
    let db = e.main_database();
    (r.txnid(), snapshot_by(|k| db.get(&r, k).unwrap().is_some()))
}

/// `([(write txn id, read txn id after, last txnid after); 2], second
/// commit keeps the first's key, snapshot of a fresh open after one commit
/// and a close)`, as in `OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`,
/// `OBS_PREV_SNAPSHOT_SECOND_COMMIT_KEEPS_FIRST` and
/// `OBS_PREV_SNAPSHOT_ONE_COMMIT_THEN_REOPEN`.
type CommitTxnids = ([(u64, u64, u64); 2], bool, &'static str);

/// A `PREV_SNAPSHOT` env's first two commits, in one process: the first
/// commit is not published, so readers keep the older generation and the
/// second commit (the same txnid) rebuilds from it, dropping the first
/// commit's data; a fresh open after one commit and a close serves that
/// commit (ADR-0023 amendment 19).
#[test]
fn prev_snapshot_commit_txnids_vs_lmdb() {
    let lmdb: CommitTxnids = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        let a = Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap();
        let step = |key: &[u8]| {
            let w = a.commit_key(key);
            let (r, _) = lmdb_read_view(&a);
            (w, r, a.last_txnid())
        };
        let first = step(b"after");
        let second = step(b"after2");
        let txn = a.try_read().unwrap();
        let keeps_first = Lmdb::txn_has_key(&txn, b"after");
        Lmdb::abort(txn);
        drop(a);
        let dir2 = TempDir::new().unwrap();
        lmdb_two_generations(dir2.path());
        {
            let a =
                Lmdb::try_open(dir2.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap();
            a.commit_key(b"after");
        }
        let env = Lmdb::try_open(dir2.path(), 16 * MIB, MAX_READERS, 0).unwrap();
        ([first, second], keeps_first, lmdb_read_view(&env).1)
    };
    eprintln!("lmdb = {lmdb:?}");
    let zdb: CommitTxnids = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        let a = zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap();
        let step = |key: &[u8]| {
            let w = zdb_commit_key(&a, key);
            let (r, _) = zdb_read_view(&a);
            (w, r, a.info().last_txnid)
        };
        let first = step(b"after");
        let second = step(b"after2");
        let keeps_first = {
            let r = a.read_txn().unwrap();
            a.main_database().get(&r, b"after").unwrap().is_some()
        };
        drop(a);
        let dir2 = TempDir::new().unwrap();
        zdb_two_generations(dir2.path());
        {
            let a =
                zdb_try_open(dir2.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap();
            zdb_commit_key(&a, b"after");
        }
        let env = zdb_try_open(dir2.path(), Some(16 * MIB), zerodb::EnvFlags::EMPTY).unwrap();
        ([first, second], keeps_first, zdb_read_view(&env).1)
    };
    eprintln!("zdb = {zdb:?}");
    assert_eq!(
        lmdb,
        ([(2, 1, 2), (2, 2, 2)], false, "a_commit"),
        "LMDB as observed"
    );
    assert_eq!(zdb, lmdb, "ZeroDB mirrors LMDB (ADR-0023 amendment 19)");
}

/// A process joins a `PREV_SNAPSHOT` env after the opener's first commit,
/// the opener still open: `"ok:<read txn id> <last txnid> <snapshot>"`
/// (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`). The joiner reads
/// the older generation in both engines, and both engines' `info` name the
/// newest meta on disk (txn 2), which the joiner selected at open (ADR-0023
/// amendment 19).
#[test]
fn prev_snapshot_joiner_after_first_commit_vs_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        let a = Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap();
        a.commit_key(b"after");
        run_child("lmdb_join_info", dir.path());
        std::fs::read_to_string(dir.path().join("result")).unwrap()
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        let a = zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap();
        zdb_commit_key(&a, b"after");
        run_child("zdb_join_info", dir.path());
        std::fs::read_to_string(dir.path().join("result")).unwrap()
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    assert_eq!(lmdb, "ok:1 2 prev", "LMDB as observed");
    assert_eq!(zdb, lmdb, "ZeroDB mirrors LMDB (ADR-0023 amendment 19)");
}

/// B waits to join a `PREV_SNAPSHOT` env; the opener A ends before any
/// commit, by closing (`kill` false: A is this process) or by being killed
/// (`kill` true: A is a child). B's outcome within 3 s of starting, then
/// within 10 s after A ends.
fn prev_snapshot_opener_ends<E>(
    dir: &Path,
    kill: bool,
    hold_role: &str,
    join_role: &str,
    open_prev: impl FnOnce() -> E,
) -> [String; 2] {
    let result = dir.join("result");
    let (mut a_child, a_env) = if kill {
        let c = spawn(hold_role, dir);
        child::wait_for(&dir.join("held"));
        (Some(c), None)
    } else {
        (None, Some(open_prev()))
    };
    let _b = spawn(join_role, dir);
    let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
    if let Some(c) = a_child.as_mut() {
        c.0.kill().unwrap();
        c.0.wait().unwrap();
    }
    drop(a_env);
    let s1 = if s0 == "blocked" {
        poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
    } else {
        "-".to_string()
    };
    [s0, s1]
}

/// `(LMDB, ZeroDB)` for [`prev_snapshot_opener_ends`].
fn prev_snapshot_opener_ends_both(kill: bool) -> ([String; 2], [String; 2]) {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        prev_snapshot_opener_ends(
            dir.path(),
            kill,
            "lmdb_prev_hold",
            "lmdb_join_report",
            || Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap(),
        )
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        prev_snapshot_opener_ends(dir.path(), kill, "zdb_prev_hold", "zdb_join_report", || {
            zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap()
        })
    };
    eprintln!("kill {kill}: lmdb = {lmdb:?}, zdb = {zdb:?}");
    (lmdb, zdb)
}

/// The `PREV_SNAPSHOT` opener closes before its first commit while another
/// process waits to join (`OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT`).
/// Both engines hold the waiter while the opener is open. After the close,
/// LMDB's waiter fails with `EINVAL` on macOS and joins the rolled-back
/// generation on Linux; ZeroDB's waiter becomes the first opener and serves
/// the newest generation. The second field is docs/LMDB-COMPATIBILITY.md D-025
/// (approved), pinned here.
#[test]
fn prev_snapshot_opener_closes_before_commit_vs_lmdb() {
    let (lmdb, zdb) = prev_snapshot_opener_ends_both(false);
    let s = |v: [&str; 2]| v.map(str::to_string);
    let lmdb_expected = if cfg!(target_os = "macos") {
        ["blocked", "rc=22"]
    } else {
        ["blocked", "ok:prev"]
    };
    assert_eq!(lmdb, s(lmdb_expected), "LMDB as observed");
    assert_eq!(zdb, s(["blocked", "ok:newest"]), "ZeroDB: D-025");
}

/// As [`prev_snapshot_opener_closes_before_commit_vs_lmdb`], but the opener
/// is killed (`OBS_PREV_SNAPSHOT_OPENER_DIES_BEFORE_COMMIT`): LMDB's waiter
/// joins the rolled-back generation; ZeroDB's waiter becomes the first
/// opener and serves the newest one (D-025, approved).
#[test]
fn prev_snapshot_opener_dies_before_commit_vs_lmdb() {
    let (lmdb, zdb) = prev_snapshot_opener_ends_both(true);
    let s = |v: [&str; 2]| v.map(str::to_string);
    assert_eq!(lmdb, s(["blocked", "ok:prev"]), "LMDB as observed");
    assert_eq!(zdb, s(["blocked", "ok:newest"]), "ZeroDB: D-025");
}

/// A second process opens with `PREV_SNAPSHOT` while the first
/// `PREV_SNAPSHOT` opener has not committed: it waits for the opener's first
/// commit, then fails with `EAGAIN` as any `PREV_SNAPSHOT` joiner
/// (`OBS_PREV_SNAPSHOT_SECOND_PREV_OPENER`; ADR-0023 amendment 19).
#[test]
fn second_prev_snapshot_opener_vs_lmdb() {
    fn run<E>(
        dir: &Path,
        role: &str,
        open_prev: impl FnOnce() -> E,
        commit: impl Fn(&E),
    ) -> [String; 2] {
        let result = dir.join("result");
        let a = open_prev();
        let _c = spawn(role, dir);
        let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
        commit(&a);
        let s1 = if s0 == "blocked" {
            poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
        } else {
            "-".to_string()
        };
        [s0, s1]
    }
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        run(
            dir.path(),
            "lmdb_prev_join_report",
            || Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap(),
            |a| {
                a.commit_key(b"after");
            },
        )
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        run(
            dir.path(),
            "zdb_prev_join_report",
            || zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap(),
            |a| {
                zdb_commit_key(a, b"after");
            },
        )
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    let s = |v: [&str; 2]| v.map(str::to_string);
    assert_eq!(lmdb, s(["blocked", "EAGAIN"]), "LMDB as observed");
    assert_eq!(zdb, lmdb, "ZeroDB mirrors LMDB (ADR-0023 amendment 19)");
}

/// A sole `READ_ONLY | PREV_SNAPSHOT` opener, which can never commit, holds
/// the election for its whole life: a joiner waits until it closes
/// (`OBS_PREV_SNAPSHOT_READ_ONLY_OPENER`). After the close the engines
/// differ as in [`prev_snapshot_opener_closes_before_commit_vs_lmdb`]
/// (D-025, approved).
#[test]
fn prev_snapshot_read_only_opener_vs_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        prev_snapshot_opener_ends(dir.path(), false, "-", "lmdb_join_report", || {
            Lmdb::try_open(
                dir.path(),
                16 * MIB,
                MAX_READERS,
                MDB_RDONLY | ffi::MDB_PREVSNAPSHOT,
            )
            .unwrap()
        })
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        prev_snapshot_opener_ends(dir.path(), false, "-", "zdb_join_report", || {
            zdb_try_open(
                dir.path(),
                Some(16 * MIB),
                zerodb::EnvFlags::READ_ONLY | zerodb::EnvFlags::PREV_SNAPSHOT,
            )
            .unwrap()
        })
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    let s = |v: [&str; 2]| v.map(str::to_string);
    let lmdb_expected = if cfg!(target_os = "macos") {
        ["blocked", "rc=22"]
    } else {
        ["blocked", "ok:prev"]
    };
    assert_eq!(lmdb, s(lmdb_expected), "LMDB as observed");
    assert_eq!(zdb, s(["blocked", "ok:newest"]), "ZeroDB: D-025");
}

/// The `PREV_SNAPSHOT` opener commits a write txn with no changes while
/// another process waits to join: the waiter stays blocked until a commit
/// that writes (`OBS_PREV_SNAPSHOT_EMPTY_COMMIT_KEEPS_LOCK`; ADR-0023
/// amendment 19).
#[test]
fn prev_snapshot_empty_commit_keeps_the_election_vs_lmdb() {
    fn run<E>(
        dir: &Path,
        role: &str,
        open_prev: impl FnOnce() -> E,
        empty_commit: impl Fn(&E),
        commit: impl Fn(&E),
    ) -> [String; 2] {
        let result = dir.join("result");
        let a = open_prev();
        let _b = spawn(role, dir);
        empty_commit(&a);
        let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
        commit(&a);
        let s1 = if s0 == "blocked" {
            poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
        } else {
            "-".to_string()
        };
        [s0, s1]
    }
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        run(
            dir.path(),
            "lmdb_join_report",
            || Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, ffi::MDB_PREVSNAPSHOT).unwrap(),
            |a| {
                let txn = a.begin(0).unwrap();
                assert_eq!(Lmdb::commit_txn(txn), 0);
            },
            |a| {
                a.commit_key(b"after");
            },
        )
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        run(
            dir.path(),
            "zdb_join_report",
            || zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::PREV_SNAPSHOT).unwrap(),
            |a| a.write_txn().unwrap().commit().unwrap(),
            |a| {
                zdb_commit_key(a, b"after");
            },
        )
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    let s = |v: [&str; 2]| v.map(str::to_string);
    assert_eq!(lmdb, s(["blocked", "ok:prev"]), "LMDB as observed");
    assert_eq!(zdb, lmdb, "ZeroDB mirrors LMDB (ADR-0023 amendment 19)");
}

/// Without a lock file a `PREV_SNAPSHOT` env's first commit is current at
/// once and the second builds on it (`OBS_PREV_SNAPSHOT_NOLOCK_COMMITS`):
/// `(snapshot after the first commit, after the second, the second keeps
/// the first's key)`.
#[test]
fn prev_snapshot_without_a_lock_file_vs_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        let a = Lmdb::try_open(
            dir.path(),
            16 * MIB,
            MAX_READERS,
            ffi::MDB_NOLOCK | ffi::MDB_PREVSNAPSHOT,
        )
        .unwrap();
        a.commit_key(b"after");
        let first = lmdb_read_view(&a).1;
        a.commit_key(b"after2");
        let second = lmdb_read_view(&a).1;
        let txn = a.try_read().unwrap();
        let keeps_first = Lmdb::txn_has_key(&txn, b"after");
        Lmdb::abort(txn);
        (first, second, keeps_first)
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        let mut o = zerodb::EnvOpenOptions::new();
        o.page_size(PSIZE as u32)
            .max_readers(MAX_READERS)
            .lock_file(false)
            .flags(zerodb::EnvFlags::PREV_SNAPSHOT)
            .map_size(usize::try_from(16 * MIB).unwrap());
        let a = o.open(dir.path()).unwrap();
        zdb_commit_key(&a, b"after");
        let first = zdb_read_view(&a).1;
        zdb_commit_key(&a, b"after2");
        let second = zdb_read_view(&a).1;
        let r = a.read_txn().unwrap();
        let keeps_first = a.main_database().get(&r, b"after").unwrap().is_some();
        (first, second, keeps_first)
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    assert_eq!(lmdb, ("a_commit", "a_commit2", true), "LMDB as observed");
    assert_eq!(zdb, lmdb, "ZeroDB without a lock file (unchanged)");
}

/// A process holds an env open; another opens it `READ_ONLY | PREV_SNAPSHOT`,
/// reads and closes; then the holder begins a read txn and a write txn
/// (`OBS_PREV_SNAPSHOT_READ_ONLY_JOINER`): `[joiner outcome, holder's read
/// "<id> <snapshot>", holder's write txn id]`. LMDB admits the read-only
/// joiner and its open rolls every process back to the older meta; ZeroDB
/// refuses it with `EAGAIN` and the holder is untouched. Not mirrored:
/// docs/LMDB-COMPATIBILITY.md D-025 (approved), pinned here.
#[test]
fn prev_snapshot_read_only_joiner_vs_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        lmdb_two_generations(dir.path());
        let a = Lmdb::try_open(dir.path(), 16 * MIB, MAX_READERS, 0).unwrap();
        run_child("lmdb_rdonly_prev_join", dir.path());
        let joiner = std::fs::read_to_string(dir.path().join("result")).unwrap();
        let (id, snap) = lmdb_read_view(&a);
        let w = a.commit_key(b"after");
        [joiner, format!("{id} {snap}"), w.to_string()]
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        zdb_two_generations(dir.path());
        let a = zdb_try_open(dir.path(), Some(16 * MIB), zerodb::EnvFlags::EMPTY).unwrap();
        run_child("zdb_rdonly_prev_join", dir.path());
        let joiner = std::fs::read_to_string(dir.path().join("result")).unwrap();
        let (id, snap) = zdb_read_view(&a);
        let w = zdb_commit_key(&a, b"after");
        [joiner, format!("{id} {snap}"), w.to_string()]
    };
    eprintln!("lmdb = {lmdb:?}, zdb = {zdb:?}");
    let s = |v: [&str; 3]| v.map(str::to_string);
    assert_eq!(lmdb, s(["ok:1 prev", "1 prev", "2"]), "LMDB as observed");
    assert_eq!(zdb, s(["EAGAIN", "2 newest", "3"]), "ZeroDB: D-025");
}
