//! What the Meilisearch LMDB fork does at the multi-process parity points
//! ADR-0023 stage 3 still has to match: map growth by another process,
//! `mdb_env_set_mapsize`, `MDB_PREVSNAPSHOT`, read-only opens, `MDB_NOLOCK`, and
//! `mdb_env_info` / `mdb_env_stat` seeing another process's commit.
//! AGENTS.md rule 1: observed, not guessed. Drives the vendored fork through
//! `lmdb-master-sys`; the test binary re-runs itself as the second process.
//!
//! The `OBS_*` constants pin what the fork does; a diff means the fork changed
//! or the pin was wrong — never edit a pin to make ZeroDB pass.

use std::ffi::CString;
use std::os::raw::{c_int, c_uint, c_void};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::ptr;
use std::time::{Duration, Instant};

use lmdb_master_sys as ffi;
use zerodb_oracle::tempdir::TempDir;

const MDB_NOTLS: c_uint = 0x0020_0000;
const MDB_RDONLY: c_uint = 0x0002_0000;
const MDB_NOSYNC: c_uint = 0x0001_0000;
const ROLE: &str = "ZERODB_S3_OBS_ROLE";
const DIR: &str = "ZERODB_S3_OBS_DIR";
/// Where a `ro_nobody` child writes its result when the env directory itself
/// is not writable.
const OUT: &str = "ZERODB_S3_OBS_OUT";

const MIB: u64 = 1 << 20;

// Pinned observations (see the report of the run that filled them in).
const OBS_READ_BEGIN_SMALL_MAP: i32 = ffi::MDB_MAP_RESIZED;
const OBS_WRITE_BEGIN_SMALL_MAP: i32 = ffi::MDB_MAP_RESIZED;
const OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES: bool = false;
const OBS_SET_MAPSIZE_ZERO_ADOPTS: u64 = 8 * MIB;
const OBS_SET_MAPSIZE_WITH_WRITE_TXN: i32 = libc::EINVAL;
const OBS_COMMIT_PERSISTS_OWN_MAPSIZE: bool = false;
const OBS_OPEN_SMALLER_THAN_META: u64 = MIB;
const OBS_PREV_SNAPSHOT_JOINER: i32 = libc::EAGAIN;
const OBS_RDONLY_EXISTING_LOCK: i32 = 0;
const OBS_RDONLY_LOCK_UNWRITABLE: i32 = libc::EACCES;
const OBS_NOLOCK_CREATES_LOCK_FILE: bool = false;
const OBS_INFO_SEES_OTHER_COMMIT: bool = true;
const OBS_STAT_SEES_OTHER_COMMIT: bool = true;
/// Open with a map smaller than `(last_pg + 1) * psize`: "requested" (keeps
/// the requested map), "minsize" (rounds up to `(last_pg + 1) * psize`),
/// "meta" (adopts the meta's map size), or "rc=N" (the open fails).
const OBS_OPEN_BELOW_LAST_PG: &str = "minsize";
/// `MDB_RDONLY` open of an env with no `lock.mdb` in a directory the process
/// cannot write.
const OBS_RDONLY_LOCK_DIR_UNWRITABLE: i32 = libc::EACCES;
/// `MDB_RDONLY` open of an env with no `lock.mdb` on a read-only filesystem.
/// -1 when no read-only filesystem could be made here.
const OBS_RDONLY_LOCK_READONLY_FS: i32 = 0;

struct Lmdb {
    env: *mut ffi::MDB_env,
}

impl Lmdb {
    /// Open `dir` with the given map size and flags (`MDB_NOTLS` is added);
    /// the raw return code on failure.
    fn try_open(dir: &Path, mapsize: u64, flags: c_uint) -> Result<Lmdb, c_int> {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle; `path` outlives
        // `mdb_env_open`. On failure the handle is closed as the C API requires.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, 8), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, mapsize as _), 0);
            let rc = ffi::mdb_env_open(env, path.as_ptr(), MDB_NOTLS | flags, 0o644);
            if rc != 0 {
                ffi::mdb_env_close(env);
                return Err(rc);
            }
        }
        Ok(Lmdb { env })
    }

    fn open(dir: &Path, mapsize: u64) -> Lmdb {
        Lmdb::try_open(dir, mapsize, 0).unwrap()
    }

    fn begin(&self, flags: c_uint) -> Result<*mut ffi::MDB_txn, c_int> {
        let mut txn = ptr::null_mut();
        // SAFETY: `self.env` is open; `txn` receives a handle we own.
        let rc = unsafe { ffi::mdb_txn_begin(self.env, ptr::null_mut(), flags, &mut txn) };
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

    /// Write `pairs` in one write txn and commit.
    fn commit(&self, pairs: &[(&[u8], &[u8])]) {
        let txn = self.begin(0).unwrap();
        // SAFETY: standard dbi_open/put/commit sequence on a live write txn;
        // every pointer is live for the duration of its call.
        unsafe {
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            for (k, v) in pairs {
                let mut k = ffi::MDB_val {
                    mv_size: k.len(),
                    mv_data: k.as_ptr() as *mut c_void,
                };
                let mut v = ffi::MDB_val {
                    mv_size: v.len(),
                    mv_data: v.as_ptr() as *mut c_void,
                };
                assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
            }
            assert_eq!(ffi::mdb_txn_commit(txn), 0);
        }
    }

    fn commit_key(&self, key: &[u8]) {
        self.commit(&[(key, b"v")]);
    }

    fn info(&self) -> ffi::MDB_envinfo {
        // SAFETY: MDB_envinfo is plain data; LMDB fills it.
        unsafe {
            let mut i: ffi::MDB_envinfo = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_info(self.env, &mut i), 0);
            i
        }
    }

    fn stat(&self) -> ffi::MDB_stat {
        // SAFETY: MDB_stat is plain data; LMDB fills it.
        unsafe {
            let mut s: ffi::MDB_stat = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_stat(self.env, &mut s), 0);
            s
        }
    }

    fn psize(&self) -> u64 {
        self.stat().ms_psize as u64
    }

    fn set_mapsize(&self, size: u64) -> c_int {
        // SAFETY: plain call on an open env.
        unsafe { ffi::mdb_env_set_mapsize(self.env, size as _) }
    }
}

impl Drop for Lmdb {
    fn drop(&mut self) {
        // SAFETY: every txn of this env has ended; closed exactly once.
        unsafe { ffi::mdb_env_close(self.env) }
    }
}

/// `(mm_mapsize, mm_last_pg, mm_txnid)` of the newest meta page in `data.mdb`,
/// read straight from the file. Offsets inside a meta page: 16-byte page
/// header, then magic u32, version u32, address u64, mapsize u64 (@32), two
/// 48-byte MDB_db records, last_pg u64 (@136), txnid u64 (@144).
fn read_meta(dir: &Path, psize: u64) -> (u64, u64, u64) {
    let bytes = std::fs::read(dir.join("data.mdb")).unwrap();
    let u64_at = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    let metas: Vec<(u64, u64, u64)> = (0..2)
        .map(|i| {
            let base = (i * psize) as usize;
            let magic = u32::from_le_bytes(bytes[base + 16..base + 20].try_into().unwrap());
            assert_eq!(magic, 0xBEEF_C0DE, "meta magic");
            (u64_at(base + 32), u64_at(base + 136), u64_at(base + 144))
        })
        .collect();
    *metas.iter().max_by_key(|m| m.2).unwrap()
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    spawn_out(role, dir, None)
}

fn spawn_out(role: &str, dir: &Path, out: Option<&Path>) -> ChildGuard {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir);
    if let Some(out) = out {
        cmd.env(OUT, out);
    }
    ChildGuard(cmd.spawn().unwrap())
}

/// Run a child role to completion; panics on a failure exit or a 60 s timeout.
fn run_child(role: &str, dir: &Path) {
    run_child_out(role, dir, None)
}

/// [`run_child`], with the child's result file at `out`.
fn run_child_out(role: &str, dir: &Path, out: Option<&Path>) {
    let mut c = spawn_out(role, dir, out);
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

/// Past 1 MiB (so a 1 MiB map cannot hold it) and under 2 MiB (so a 2 MiB map
/// can).
const GROW_TARGET: u64 = MIB + MIB / 4;

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let mut parts = role.split(':');
    let name = parts.next().unwrap();
    let mapsize: u64 = parts.next().map(|s| s.parse().unwrap()).unwrap_or(MIB);
    match name {
        // Commit batches of small values until the file's last page is past
        // GROW_TARGET.
        "grow" => {
            let env = Lmdb::try_open(&dir, mapsize, MDB_NOSYNC).unwrap();
            let ps = env.psize();
            let value = [7u8; 1000];
            let mut i = 0u32;
            while (env.info().me_last_pgno as u64 + 1) * ps <= GROW_TARGET {
                let keys: Vec<String> = (0..8).map(|j| format!("g{:08}", i * 8 + j)).collect();
                let pairs: Vec<(&[u8], &[u8])> =
                    keys.iter().map(|k| (k.as_bytes(), &value[..])).collect();
                env.commit(&pairs);
                i += 1;
                assert!(i < 100_000, "never grew");
            }
            assert!((env.info().me_last_pgno as u64 + 1) * ps <= 2 * MIB);
        }
        // One small commit under key "b", then exit.
        "seed_small" => {
            let env = Lmdb::try_open(&dir, mapsize, 0).unwrap();
            env.commit_key(b"b");
        }
        // One small commit under key "c", then exit.
        "commit_c" => {
            let env = Lmdb::try_open(&dir, mapsize, 0).unwrap();
            env.commit_key(b"c");
        }
        // Hold the env open (and a write txn, if asked) until killed.
        "hold_open" | "hold_writer" => {
            let env = Lmdb::try_open(&dir, mapsize, 0).unwrap();
            let _txn = (name == "hold_writer").then(|| env.begin(0).unwrap());
            std::fs::write(dir.join("held"), b"").unwrap();
            std::thread::sleep(Duration::from_secs(600));
        }
        // Drop root privileges, then open read-only; the rc goes to `result`.
        "ro_nobody" => {
            // SAFETY: plain libc calls; this child is single-threaded here.
            let dropped = unsafe { libc::setgid(65534) == 0 && libc::setuid(65534) == 0 };
            let out = if !dropped {
                "skip".to_string()
            } else {
                match Lmdb::try_open(&dir, MIB, MDB_RDONLY) {
                    Ok(_) => "0".to_string(),
                    Err(rc) => rc.to_string(),
                }
            };
            let path = std::env::var(OUT)
                .map(PathBuf::from)
                .unwrap_or_else(|_| dir.join("result"));
            std::fs::write(path, out).unwrap();
        }
        _ => panic!("unknown role {role}"),
    }
}

/// Process A (1 MiB map) is open with one commit; process B (8 MiB map) then
/// commits past A's map and exits. Returns A and the directory.
fn a_small_b_grown() -> (Lmdb, TempDir) {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), MIB);
    a.commit_key(b"a");
    run_child(&format!("grow:{}", 8 * MIB), dir.path());
    let (mapsize, last_pg, _) = read_meta(dir.path(), a.psize());
    assert_eq!(mapsize, 8 * MIB, "precondition: B persisted its map size");
    assert!(
        (last_pg + 1) * a.psize() > MIB,
        "precondition: B's data is past A's map"
    );
    assert_eq!(a.info().me_mapsize as u64, MIB, "A still has its 1 MiB map");
    (a, dir)
}

#[test]
fn observe_read_begin_small_map() {
    let (a, _dir) = a_small_b_grown();
    let rc = a
        .begin(MDB_RDONLY)
        .map(Lmdb::abort)
        .map_or_else(|e| e, |_| 0);
    println!("read begin on a small map: {rc}");
    assert_eq!(rc, OBS_READ_BEGIN_SMALL_MAP);
}

#[test]
fn observe_write_begin_small_map() {
    let (a, _dir) = a_small_b_grown();
    let rc = a.begin(0).map(Lmdb::abort).map_or_else(|e| e, |_| 0);
    println!("write begin on a small map: {rc}");
    assert_eq!(rc, OBS_WRITE_BEGIN_SMALL_MAP);
}

#[test]
fn observe_bigger_meta_mapsize_alone() {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), MIB);
    a.commit_key(b"a");
    run_child(&format!("seed_small:{}", 8 * MIB), dir.path());
    let (mapsize, last_pg, _) = read_meta(dir.path(), a.psize());
    assert_eq!(mapsize, 8 * MIB, "precondition: the meta says 8 MiB");
    assert!(
        (last_pg + 1) * a.psize() <= MIB,
        "precondition: the data still fits A's map"
    );
    let resizes = match a.begin(MDB_RDONLY) {
        Ok(t) => {
            Lmdb::abort(t);
            false
        }
        Err(rc) => {
            assert_eq!(rc, ffi::MDB_MAP_RESIZED);
            true
        }
    };
    println!("bigger meta mapsize alone resizes: {resizes}");
    assert_eq!(resizes, OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES);
}

#[test]
fn observe_set_mapsize_zero_adopts() {
    let (a, _dir) = a_small_b_grown();
    assert_eq!(a.set_mapsize(0), 0);
    let adopted = a.info().me_mapsize as u64;
    println!("set_mapsize(0) adopts: {adopted}");
    assert_eq!(adopted, OBS_SET_MAPSIZE_ZERO_ADOPTS);
    // The adopted map is usable again.
    Lmdb::abort(a.begin(MDB_RDONLY).unwrap());
}

#[test]
fn observe_set_mapsize_with_write_txn() {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), MIB);
    a.commit_key(b"a");
    let txn = a.begin(0).unwrap();
    let rc = a.set_mapsize(2 * MIB);
    Lmdb::abort(txn);
    println!("set_mapsize with a write txn: {rc}");
    assert_eq!(rc, OBS_SET_MAPSIZE_WITH_WRITE_TXN);
}

#[test]
fn observe_commit_persists_own_mapsize() {
    let (a, dir) = a_small_b_grown();
    // A adopts B's size and commits: the meta keeps 8 MiB.
    assert_eq!(a.set_mapsize(0), 0);
    a.commit_key(b"a2");
    assert_eq!(read_meta(dir.path(), a.psize()).0, 8 * MIB);
    // A process opened with 2 MiB (the data fits it) commits a small txn.
    run_child(&format!("commit_c:{}", 2 * MIB), dir.path());
    let persisted = read_meta(dir.path(), a.psize()).0;
    println!("meta mapsize after a 2 MiB process commits: {persisted}");
    assert!(persisted == 2 * MIB || persisted == 8 * MIB, "{persisted}");
    assert_eq!(persisted == 2 * MIB, OBS_COMMIT_PERSISTS_OWN_MAPSIZE);
}

#[test]
fn observe_open_smaller_than_meta() {
    let dir = TempDir::new().unwrap();
    run_child(&format!("seed_small:{}", 8 * MIB), dir.path());
    let env = Lmdb::open(dir.path(), MIB);
    assert_eq!(read_meta(dir.path(), env.psize()).0, 8 * MIB);
    let got = env.info().me_mapsize as u64;
    println!("open with 1 MiB, meta 8 MiB: me_mapsize {got}");
    assert_eq!(got, OBS_OPEN_SMALLER_THAN_META);
}

#[test]
fn observe_prev_snapshot_joiner() {
    let dir = TempDir::new().unwrap();
    let _c = {
        let c = spawn("hold_open", dir.path());
        wait_for(&dir.path().join("held"));
        c
    };
    let rc = Lmdb::try_open(dir.path(), MIB, ffi::MDB_PREVSNAPSHOT).map_or_else(|e| e, |_| 0);
    println!("MDB_PREVSNAPSHOT with another process open: {rc}");
    assert_eq!(rc, OBS_PREV_SNAPSHOT_JOINER);
}

#[test]
fn observe_rdonly_existing_lock() {
    let dir = TempDir::new().unwrap();
    let _c = {
        let c = spawn("hold_writer", dir.path());
        wait_for(&dir.path().join("held"));
        c
    };
    let rc = match Lmdb::try_open(dir.path(), MIB, MDB_RDONLY) {
        Ok(env) => {
            Lmdb::abort(env.begin(MDB_RDONLY).unwrap());
            0
        }
        Err(rc) => rc,
    };
    println!("MDB_RDONLY open while a writer holds the env: {rc}");
    assert_eq!(rc, OBS_RDONLY_EXISTING_LOCK);
}

#[test]
fn observe_rdonly_lock_unwritable() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    {
        let env = Lmdb::open(dir.path(), MIB);
        env.commit_key(b"a");
    }
    std::fs::set_permissions(
        dir.path().join("lock.mdb"),
        PermissionsExt::from_mode(0o444),
    )
    .unwrap();
    // SAFETY: plain libc call.
    let root = unsafe { libc::geteuid() } == 0;
    let rc: i32 = if root {
        // Root ignores file modes, so open as `nobody` in a child process. If
        // that is impossible, record -1 and skip the assertion. The child
        // writes its answer into the directory, so let `nobody` write there.
        std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o777)).unwrap();
        run_child("ro_nobody", dir.path());
        match std::fs::read_to_string(dir.path().join("result"))
            .unwrap()
            .as_str()
        {
            "skip" => {
                println!("SKIPPED: running as root and cannot drop to uid 65534");
                -1
            }
            n => n.parse().unwrap(),
        }
    } else {
        Lmdb::try_open(dir.path(), MIB, MDB_RDONLY).map_or_else(|e| e, |_| 0)
    };
    println!("MDB_RDONLY open with a mode-0444 lock.mdb: {rc}");
    if rc != -1 {
        assert_eq!(rc, OBS_RDONLY_LOCK_UNWRITABLE);
    }
}

#[test]
fn observe_nolock_creates_lock_file() {
    let dir = TempDir::new().unwrap();
    {
        let _env = Lmdb::try_open(dir.path(), MIB, ffi::MDB_NOLOCK).unwrap();
    }
    let exists = dir.path().join("lock.mdb").exists();
    println!("MDB_NOLOCK creates lock.mdb: {exists}");
    assert_eq!(exists, OBS_NOLOCK_CREATES_LOCK_FILE);
}

#[test]
fn observe_info_sees_other_commit() {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), MIB);
    a.commit_key(b"a");
    let before = a.info().me_last_txnid;
    run_child(&format!("seed_small:{MIB}"), dir.path());
    let after = a.info().me_last_txnid;
    println!("A's last_txnid before {before}, after B's commit {after}");
    assert_eq!(after > before, OBS_INFO_SEES_OTHER_COMMIT);
}

#[test]
fn observe_stat_sees_other_commit() {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), MIB);
    a.commit_key(b"a");
    let before = a.stat().ms_entries;
    run_child(&format!("seed_small:{MIB}"), dir.path());
    let after = a.stat().ms_entries;
    println!("A's ms_entries before {before}, after B's commit {after}");
    assert_eq!(after > before, OBS_STAT_SEES_OTHER_COMMIT);
}

/// Commit past 1 MiB with an 8 MiB map (in a child, which then exits), then
/// open the env again with a 1 MiB map.
#[test]
fn observe_open_below_last_pg() {
    let dir = TempDir::new().unwrap();
    run_child(&format!("grow:{}", 8 * MIB), dir.path());
    let psize = {
        let env = Lmdb::open(dir.path(), 8 * MIB);
        env.psize()
    };
    let (meta_mapsize, last_pg, _) = read_meta(dir.path(), psize);
    let minsize = (last_pg + 1) * psize;
    assert!(minsize > MIB, "precondition: the data is past 1 MiB");
    let got = match Lmdb::try_open(dir.path(), MIB, 0) {
        Ok(env) => {
            let m = env.info().me_mapsize as u64;
            println!(
                "open 1 MiB below last_pg: me_mapsize {m}, minsize {minsize}, meta {meta_mapsize}"
            );
            if m == MIB {
                "requested".to_string()
            } else if m == minsize {
                "minsize".to_string()
            } else if m == meta_mapsize {
                "meta".to_string()
            } else {
                format!("other={m}")
            }
        }
        Err(rc) => format!("rc={rc}"),
    };
    println!("open below last_pg: {got}");
    assert_eq!(got, OBS_OPEN_BELOW_LAST_PG);
}

/// An env whose `lock.mdb` was removed, closed, in `dir`.
fn env_without_lock(dir: &Path) {
    {
        let env = Lmdb::open(dir, MIB);
        env.commit_key(b"a");
    }
    std::fs::remove_file(dir.join("lock.mdb")).unwrap();
}

/// `MDB_RDONLY` open, as `nobody` in a child when this process is root
/// (root ignores file modes); -1 if that is impossible.
fn rdonly_open_rc(dir: &Path) -> i32 {
    // SAFETY: plain libc call.
    let root = unsafe { libc::geteuid() } == 0;
    if !root {
        return Lmdb::try_open(dir, MIB, MDB_RDONLY).map_or_else(|e| e, |_| 0);
    }
    use std::os::unix::fs::PermissionsExt;
    let out_dir = TempDir::new().unwrap();
    std::fs::set_permissions(out_dir.path(), PermissionsExt::from_mode(0o777)).unwrap();
    let out = out_dir.path().join("result");
    run_child_out("ro_nobody", dir, Some(&out));
    match std::fs::read_to_string(&out).unwrap().as_str() {
        "skip" => {
            println!("SKIPPED: running as root and cannot drop to uid 65534");
            -1
        }
        n => n.parse().unwrap(),
    }
}

#[test]
fn observe_rdonly_lock_dir_unwritable() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    env_without_lock(dir.path());
    std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o555)).unwrap();
    let rc = rdonly_open_rc(dir.path());
    let created = dir.path().join("lock.mdb").exists();
    std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o755)).unwrap();
    println!("MDB_RDONLY, no lock.mdb, directory mode 0555: rc {rc}, lock created {created}");
    if rc != -1 {
        assert_eq!(rc, OBS_RDONLY_LOCK_DIR_UNWRITABLE);
        assert!(
            !created,
            "a RDONLY open of an unwritable dir created lock.mdb"
        );
    }
}

/// Run `cmd` to completion within 60 s; `false` on a failure exit, a spawn
/// error or the timeout (the child is then killed).
fn run_bounded(cmd: &mut Command) -> bool {
    let Ok(mut child) = cmd.spawn() else {
        return false;
    };
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        match child.try_wait() {
            Ok(Some(st)) => return st.success(),
            Ok(None) if Instant::now() < end => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

/// A read-only filesystem holding a copy of `src`, unmounted on drop.
struct ReadOnlyFs {
    mount: PathBuf,
    _keep: TempDir,
}

impl Drop for ReadOnlyFs {
    fn drop(&mut self) {
        let m = self.mount.to_str().unwrap();
        if cfg!(target_os = "macos") {
            run_bounded(Command::new("hdiutil").args(["detach", "-force", m]));
        } else {
            run_bounded(Command::new("umount").arg(m));
        }
    }
}

/// Make a read-only filesystem with `src`'s files, or say why not: a
/// read-only disk image on macOS (`hdiutil`), a read-only bind mount on
/// Linux (needs `CAP_SYS_ADMIN`, e.g. a privileged container).
fn read_only_fs(src: &Path) -> Result<ReadOnlyFs, String> {
    let keep = TempDir::new().unwrap();
    let mount = keep.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let ok = run_bounded;
    if cfg!(target_os = "macos") {
        let img = keep.path().join("ro.dmg");
        if !ok(Command::new("hdiutil")
            .args(["create", "-quiet", "-format", "UDRO", "-srcfolder"])
            .arg(src)
            .arg(&img))
        {
            return Err("hdiutil create failed".into());
        }
        if !ok(Command::new("hdiutil")
            .args(["attach", "-quiet", "-readonly", "-nobrowse", "-mountpoint"])
            .arg(&mount)
            .arg(&img))
        {
            return Err("hdiutil attach failed".into());
        }
    } else {
        if !ok(Command::new("mount").arg("--bind").arg(src).arg(&mount)) {
            return Err("bind mount failed (needs CAP_SYS_ADMIN)".into());
        }
        if !ok(Command::new("mount")
            .args(["-o", "remount,bind,ro"])
            .arg(&mount))
        {
            run_bounded(Command::new("umount").arg(&mount));
            return Err("read-only remount failed".into());
        }
    }
    Ok(ReadOnlyFs { mount, _keep: keep })
}

#[test]
fn observe_rdonly_lock_readonly_fs() {
    let src = TempDir::new().unwrap();
    env_without_lock(src.path());
    let rc = match read_only_fs(src.path()) {
        Ok(fs) => {
            let rc = match Lmdb::try_open(&fs.mount, MIB, MDB_RDONLY) {
                Ok(env) => {
                    // The env must also serve a read txn without a lock file.
                    Lmdb::abort(env.begin(MDB_RDONLY).unwrap());
                    0
                }
                Err(rc) => rc,
            };
            println!("MDB_RDONLY on a read-only filesystem, no lock.mdb: rc {rc}");
            rc
        }
        Err(why) => {
            println!("SKIPPED: MDB_RDONLY on a read-only filesystem not observed here ({why})");
            -1
        }
    };
    if rc != -1 {
        assert_eq!(rc, OBS_RDONLY_LOCK_READONLY_FS);
    }
}
