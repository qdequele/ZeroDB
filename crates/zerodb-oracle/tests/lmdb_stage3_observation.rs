//! What the Meilisearch LMDB fork does at the multi-process parity points
//! ADR-0023 stage 3 still has to match: map growth by another process,
//! `mdb_env_set_mapsize`, `MDB_PREVSNAPSHOT`, read-only opens, `MDB_NOLOCK`, and
//! `mdb_env_info` / `mdb_env_stat` seeing another process's commit, a
//! joiner's reader-table size, `mdb_env_set_mapsize` while another process
//! holds a write txn, `mdb_env_info`'s reader count with readers in
//! another process, the data-file length a `MDB_WRITEMAP` joiner leaves, and
//! whether a process can join an `MDB_PREVSNAPSHOT` env before its first
//! commit, with the txn ids and snapshots around that env's first two
//! commits and what a waiting process gets when the opener ends first.
//! AGENTS.md rule 1: observed, not guessed. Drives the vendored fork through
//! `lmdb-master-sys`; the test binary re-runs itself as the second process.
//!
//! The `OBS_*` constants pin what the fork does; a diff means the fork changed
//! or the pin was wrong — never edit a pin to make ZeroDB pass.

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
const MDB_NOSYNC: c_uint = 0x0001_0000;
const MDB_WRITEMAP: c_uint = ffi::MDB_WRITEMAP;
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
/// With two meta generations whose `last_pg` differ, `MDB_PREVSNAPSHOT` opened
/// with a map size below both metas' `(last_pg + 1) * psize`: "prev" (the
/// minimum comes from the older meta, the one `MDB_PREVSNAPSHOT` reads) or
/// "newest" (the usual, non-`PREVSNAPSHOT` rule). macOS (Darwin 25.0.0),
/// 2026-10-12.
const OBS_PREV_SNAPSHOT_MINSIZE_FROM: &str = "prev";
/// `MDB_PREVSNAPSHOT` opened with map size 0, where the two generations
/// persisted different `mm_mapsize` values: the resulting `me_mapsize` is
/// "prev" (the older meta's) or "newest". macOS (Darwin 25.0.0), 2026-10-12.
const OBS_PREV_SNAPSHOT_ZERO_MAPSIZE_FROM: &str = "prev";
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
/// A joiner asks for 126 reader slots while another process holds the env
/// with 200: the open's rc, and `mdb_env_get_maxreaders` afterwards. The
/// joiner adopts the lock file's count. macOS (Darwin 27.0.0), 2026-10-13.
const OBS_JOINER_FEWER_MAXREADERS: (i32, u32) = (0, 200);
/// A joiner asks for 200 reader slots while another process holds the env
/// with 126: the open's rc, and `mdb_env_get_maxreaders` afterwards. The
/// joiner adopts the lock file's count. macOS (Darwin 27.0.0), 2026-10-13.
const OBS_JOINER_MORE_MAXREADERS: (i32, u32) = (0, 126);
/// A sole opener asks for 126 reader slots after a previous run (now
/// closed) left a lock file sized for 200: `mdb_env_get_maxreaders`. LMDB
/// never shrinks the lock file, and sizes the table from it. macOS (Darwin
/// 27.0.0), 2026-10-13.
const OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS: u32 = 200;
/// `MDB_RDONLY` open on a read-only filesystem with no `lock.mdb` (it runs
/// without a lock file): whether `mdb_env_get_flags` then reports
/// `MDB_NOLOCK` (it reports `MDB_RDONLY | MDB_NOTLS` only). macOS (Darwin
/// 27.0.0, `hdiutil` read-only image), 2026-10-13.
const OBS_RDONLY_READONLY_FS_REPORTS_NOLOCK: bool = false;
/// `mdb_env_set_mapsize` (2 MiB) in one process while another process holds
/// a write txn on the env: "returned:<rc>" when the call returns while the
/// other process still holds the txn, "blocked" when it waits for the txn
/// to end. It returns: LMDB takes no lock to resize. macOS (Darwin 27.0.0),
/// 2026-10-13.
const OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER: &str = "returned:0";
/// [`OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`] with the resizing process opened
/// `MDB_RDONLY`. It returns too. macOS (Darwin 27.0.0), 2026-10-13.
const OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER_RDONLY: &str = "returned:0";
/// Another process holds 3 read txns; this process holds none. The
/// `(me_numreaders, mdb_reader_list entries)` this process sees: `[0]` while
/// the 3 txns are held, `[1]` after the other process ends them (its env
/// still open), `[2]` after it closes the env and exits, `[3]` while this
/// process then holds one read txn of its own. `me_numreaders` is the
/// shared high-water mark of reader slots used, counting every process's
/// readers and never decreasing; `mdb_reader_list` lists the live ones.
/// macOS (Darwin 27.0.0), 2026-10-13.
const OBS_NUMREADERS_OTHER_PROCESS: [(u32, usize); 4] = [(3, 3), (3, 0), (3, 0), (3, 1)];
/// A process joins a running env (held open, one commit, 1 MiB map, no
/// `MDB_WRITEMAP`) with `MDB_WRITEMAP` and an 8 MiB map: the `data.mdb`
/// length right after its open, "unchanged" (as before the open) or
/// "joiner_map" (the joiner's map size). The open truncates the file to its
/// map size (49,152 bytes before, 8 MiB after). macOS (Darwin 27.0.0),
/// 2026-10-13.
const OBS_WRITE_MAP_JOINER_LARGER_MAP: &str = "joiner_map";
/// [`OBS_WRITE_MAP_JOINER_LARGER_MAP`] with the holder opened `MDB_WRITEMAP`
/// at 8 MiB (so `data.mdb` is 8 MiB, longer than the joiner's map) and the
/// joiner at 2 MiB: the joiner's open shrinks the file to 2 MiB under the
/// holder's 8 MiB mapping. macOS (Darwin 27.0.0), 2026-10-13.
const OBS_WRITE_MAP_JOINER_SMALLER_MAP: &str = "joiner_map";
/// Process A opens an env with two meta generations with `MDB_PREVSNAPSHOT`;
/// process B then opens it (no flags) before A's first commit. B's outcome
/// at three points: within 3 s of starting B ("blocked" while B's open has
/// not returned), within 10 s after A's first commit, and within 10 s after
/// A closes the env ("-" once B has already returned). A returned open is
/// "ok:<snapshot>", the snapshot B's first read txn sees (see
/// [`snapshot_of`]), or "rc=<rc>". B's open waits until A's first commit,
/// then sees "prev": the generation A rolled back to, not A's commit. macOS
/// (Darwin 27.0.0), 2026-10-13.
const OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT: [&str; 3] = ["blocked", "ok:prev", "-"];
/// In [`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`]'s run, the snapshot a
/// fresh read txn of A sees right after A's first commit, and right after
/// its second. After the first commit A's own readers still see "prev" (the
/// commit is on disk as the newest meta, but the shared snapshot txnid
/// names the older one); after the second they see it ("a_commit2").
/// macOS (Darwin 27.0.0), 2026-10-13.
const OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW: [&str; 2] = ["prev", "a_commit2"];
/// A second process opens with `MDB_PREVSNAPSHOT` while the first
/// `MDB_PREVSNAPSHOT` opener has not committed: its outcome within 3 s, then
/// within 10 s after the opener's first commit ("ok:<snapshot>", "EAGAIN" or
/// "rc=<rc>"). It waits for the opener's exclusive lock, then joins and
/// fails with `EAGAIN`, as any `MDB_PREVSNAPSHOT` joiner does
/// ([`OBS_PREV_SNAPSHOT_JOINER`]). macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_SECOND_PREV_OPENER: [&str; 2] = ["blocked", "EAGAIN"];
/// A sole process opens with `MDB_RDONLY | MDB_PREVSNAPSHOT`, which can never
/// commit; a second process opens without flags. Its outcome within 3 s,
/// then within 10 s after the first process closes. The read-only opener
/// keeps the exclusive lock for its whole life. On macOS its close then
/// removes the SysV semaphores, so the waiter fails with `EINVAL`, as in
/// [`OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT`]. macOS (Darwin 27.0.0),
/// 2026-10-13.
#[cfg(target_os = "macos")]
const OBS_PREV_SNAPSHOT_READ_ONLY_OPENER: [&str; 2] = ["blocked", "rc=22"];
/// As the macOS pin, on Linux: after the close the waiter joins and reads
/// the older generation, as in the Linux
/// [`OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT`]. Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
#[cfg(not(target_os = "macos"))]
const OBS_PREV_SNAPSHOT_READ_ONLY_OPENER: [&str; 2] = ["blocked", "ok:prev"];
/// The `MDB_PREVSNAPSHOT` opener commits a write txn with no changes while a
/// process waits to join; the waiter's outcome within 3 s, then within 10 s
/// after a commit that writes a key. The empty commit writes no meta and
/// keeps the exclusive lock. macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_EMPTY_COMMIT_KEEPS_LOCK: [&str; 2] = ["blocked", "ok:prev"];
/// `MDB_NOLOCK | MDB_PREVSNAPSHOT`, one process, two commits: `(snapshot
/// after the first, snapshot after the second, the second keeps the first
/// commit's key)`. Without a lock file the first commit is current at once
/// and the second builds on it. macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_NOLOCK_COMMITS: (&str, &str, bool) = ("a_commit", "a_commit2", true);
/// One process opens [`two_meta_generations`]'s env with `MDB_PREVSNAPSHOT`
/// and commits twice: whether the second commit writes its main DB root to
/// the page number that the newest meta on disk names after the first
/// commit. It does (page 3 both times): between the two commits the newest
/// meta on disk is the first commit, and the second commit overwrites a page
/// that meta references before it replaces the meta, so a crash in between
/// leaves that meta naming a rewritten page. macOS (Darwin 27.0.0) and
/// Linux aarch64 (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_SECOND_COMMIT_REWRITES_FIRST_ROOT: bool = true;
/// A process holds an env open (no flags, two meta generations); a second
/// process opens it with `MDB_RDONLY | MDB_PREVSNAPSHOT`. `[the joiner's
/// "ok:<read txn id> <snapshot>", the holder's next read txn "<id>
/// <snapshot>" after the joiner closed, the holder's next write txn id]`.
/// The read-only joiner is not refused: it opens and reads the older
/// generation, and after its open and close the shared txnid is the older
/// meta's for every process, so the holder's readers roll back to txn 1 and
/// its next write txn is txn 2, built on the older meta. The holder is
/// observed only after the joiner closed, so whether the open or the close
/// moves the shared txnid is not observed. macOS (Darwin 27.0.0) and
/// Linux aarch64 (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_READ_ONLY_JOINER: [&str; 3] = ["ok:1 prev", "1 prev", "2"];
/// One process opens [`two_meta_generations`]'s env with `MDB_PREVSNAPSHOT`
/// (16 MiB map) and commits twice. For each commit: `(write txn id, id of a
/// fresh read txn right after it, me_last_txnid right after it)`. Both
/// commits are txn 2, both written to the meta slot of the generation the
/// open rolled back: the first commit leaves the shared txnid on the older
/// meta (read txn 1), so the second write txn is again `1 + 1` and rebuilds
/// from the older meta. `me_last_txnid` names the newest meta on disk.
/// macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_COMMIT_TXNIDS: [(u64, u64, u64); 2] = [(2, 1, 2), (2, 2, 2)];
/// In [`OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`]'s run, whether the snapshot after
/// the second commit still holds the first commit's key: it does not; the
/// second commit's base is the older meta, so the first commit is lost.
/// macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_SECOND_COMMIT_KEEPS_FIRST: bool = false;
/// One `MDB_PREVSNAPSHOT` commit, then the env is closed and opened again
/// without flags: the snapshot the fresh open serves (see [`snapshot_of`]).
/// macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_ONE_COMMIT_THEN_REOPEN: &str = "a_commit";
/// A process joins a `MDB_PREVSNAPSHOT` env after the opener's first commit
/// (the opener still open): "ok:<read txn id> <me_last_txnid> <snapshot>".
/// The joiner reads the older generation (txn 1) while its `mdb_env_info`
/// names the newest meta on disk (txn 2, the opener's first commit).
/// macOS (Darwin 27.0.0) and Linux aarch64
/// (OrbStack container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT: &str = "ok:1 2 prev";
/// A process waits to join a `MDB_PREVSNAPSHOT` env; the opener closes the
/// env without committing. The waiter's outcome within 3 s of starting, and
/// within 10 s after the close (as in
/// [`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`]). On macOS the fork uses
/// SysV semaphores, which the closing opener removes because it still holds
/// the exclusive lock; the waiter then gets the shared lock as a joiner and
/// fails with `EINVAL` (22). macOS (Darwin 27.0.0), 2026-10-13.
#[cfg(target_os = "macos")]
const OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT: [&str; 2] = ["blocked", "rc=22"];
/// As the macOS pin, on Linux, where the fork's mutexes live in the lock
/// file: the waiter joins the lock file as the closed opener left it and
/// reads the older generation, the one the opener rolled back to. Linux
/// aarch64 (OrbStack container, kernel 7.2.9), 2026-10-13.
#[cfg(not(target_os = "macos"))]
const OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT: [&str; 2] = ["blocked", "ok:prev"];
/// As [`OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT`], but the opener is
/// killed (`SIGKILL`) instead of closing. The waiter joins the lock file as
/// the dead opener left it and reads the older generation, the one the
/// opener rolled back to. macOS (Darwin 27.0.0) and Linux aarch64 (OrbStack
/// container, kernel 7.2.9), 2026-10-13.
const OBS_PREV_SNAPSHOT_OPENER_DIES_BEFORE_COMMIT: [&str; 2] = ["blocked", "ok:prev"];

struct Lmdb {
    env: *mut ffi::MDB_env,
}

impl Lmdb {
    /// Open `dir` with the given map size and flags (`MDB_NOTLS` is added);
    /// the raw return code on failure.
    fn try_open(dir: &Path, mapsize: u64, flags: c_uint) -> Result<Lmdb, c_int> {
        Lmdb::try_open_readers(dir, mapsize, flags, 8)
    }

    /// [`Lmdb::try_open`] with `maxreaders` reader slots requested.
    fn try_open_readers(
        dir: &Path,
        mapsize: u64,
        flags: c_uint,
        maxreaders: c_uint,
    ) -> Result<Lmdb, c_int> {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle; `path` outlives
        // `mdb_env_open`. On failure the handle is closed as the C API requires.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, maxreaders), 0);
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

    fn maxreaders(&self) -> u32 {
        let mut n: c_uint = 0;
        // SAFETY: plain call on an open env; `n` outlives it.
        assert_eq!(unsafe { ffi::mdb_env_get_maxreaders(self.env, &mut n) }, 0);
        n
    }

    fn flags(&self) -> c_uint {
        let mut f: c_uint = 0;
        // SAFETY: plain call on an open env; `f` outlives it.
        assert_eq!(unsafe { ffi::mdb_env_get_flags(self.env, &mut f) }, 0);
        f
    }

    fn set_mapsize(&self, size: u64) -> c_int {
        // SAFETY: plain call on an open env.
        unsafe { ffi::mdb_env_set_mapsize(self.env, size as _) }
    }

    /// Whether the main database holds `key` in the read txn `txn`.
    fn txn_has_key(txn: *mut ffi::MDB_txn, key: &[u8]) -> bool {
        // SAFETY: dbi_open/get on a live read txn; `key` outlives the call
        // and the returned data pointer is not used.
        unsafe {
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
            match ffi::mdb_get(txn, dbi, &mut k, &mut v) {
                0 => true,
                ffi::MDB_NOTFOUND => false,
                rc => panic!("mdb_get failed: {rc}"),
            }
        }
    }

    /// The number of entries `mdb_reader_list` prints (one per reader slot
    /// with an owner PID, in any process).
    fn reader_list_len(&self) -> usize {
        unsafe extern "C" fn collect(msg: *const c_char, ctx: *mut c_void) -> c_int {
            // SAFETY: `ctx` is the `&mut Vec<String>` passed below, live for
            // the call, and no other reference to it exists meanwhile.
            let out = unsafe { &mut *(ctx as *mut Vec<String>) };
            out.push(
                // SAFETY: LMDB passes a valid NUL-terminated string that
                // lives for the duration of this callback; it is copied out
                // before the callback returns.
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
            .filter(|l| {
                l.split_whitespace()
                    .next()
                    .is_some_and(|w| w.parse::<u32>().is_ok())
            })
            .count()
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

/// Both meta slots' `(mm_mapsize, mm_last_pg, mm_txnid)`, ascending by
/// `mm_txnid`: `[0]` is the older ("prev") generation, `[1]` the newest.
fn read_metas(dir: &Path, psize: u64) -> Vec<(u64, u64, u64)> {
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
        // Begin N read txns (the role's number), signal `held`; end them
        // when `release` appears and signal `released`; exit when `stop`
        // appears.
        "hold_reads" => {
            let env = Lmdb::try_open(&dir, MIB, 0).unwrap();
            let txns: Vec<_> = (0..mapsize)
                .map(|_| env.begin(MDB_RDONLY).unwrap())
                .collect();
            std::fs::write(dir.join("held"), b"").unwrap();
            wait_for(&dir.join("release"));
            txns.into_iter().for_each(Lmdb::abort);
            std::fs::write(dir.join("released"), b"").unwrap();
            wait_for(&dir.join("stop"));
        }
        // Hold the env open with `maxreaders` reader slots (the role's
        // number) until killed.
        "hold_readers" => {
            let _env = Lmdb::try_open_readers(&dir, MIB, 0, mapsize as c_uint).unwrap();
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
        // Record `data.mdb`'s length, open with `MDB_WRITEMAP` at the role's
        // map size, record it again, close; "<before> <after>" goes to
        // `result`.
        "join_writemap" => {
            let len = || std::fs::metadata(dir.join("data.mdb")).unwrap().len();
            let before = len();
            let env = Lmdb::try_open(&dir, mapsize, MDB_WRITEMAP).unwrap();
            let after = len();
            drop(env);
            write_result(&dir, &format!("{before} {after}"));
        }
        // Open (no flags) at the role's map size and begin a read txn:
        // "ok:<snapshot>" (see `snapshot_of`) or "rc=<rc>" goes to `result`.
        "join_report" => {
            let out = match Lmdb::try_open(&dir, mapsize, 0) {
                Ok(env) => {
                    let txn = env.begin(MDB_RDONLY).unwrap();
                    let snap = snapshot_of(txn);
                    Lmdb::abort(txn);
                    format!("ok:{snap}")
                }
                Err(rc) => format!("rc={rc}"),
            };
            write_result(&dir, &out);
        }
        // As `join_report`, but "ok:<read txn id> <me_last_txnid> <snapshot>"
        // goes to `result`.
        "join_info" => {
            let out = match Lmdb::try_open(&dir, mapsize, 0) {
                Ok(env) => {
                    let txn = env.begin(MDB_RDONLY).unwrap();
                    // SAFETY: `txn` is a live read txn.
                    let id = unsafe { ffi::mdb_txn_id(txn) };
                    let snap = snapshot_of(txn);
                    Lmdb::abort(txn);
                    format!("ok:{id} {} {snap}", env.info().me_last_txnid)
                }
                Err(rc) => format!("rc={rc}"),
            };
            write_result(&dir, &out);
        }
        // Open with `MDB_PREVSNAPSHOT` at the role's map size and begin a
        // read txn: "ok:<snapshot>", "EAGAIN" or "rc=<rc>" goes to `result`.
        "prev_join_report" => {
            let out = match Lmdb::try_open(&dir, mapsize, ffi::MDB_PREVSNAPSHOT) {
                Ok(env) => {
                    let txn = env.begin(MDB_RDONLY).unwrap();
                    let snap = snapshot_of(txn);
                    Lmdb::abort(txn);
                    format!("ok:{snap}")
                }
                Err(libc::EAGAIN) => "EAGAIN".to_string(),
                Err(rc) => format!("rc={rc}"),
            };
            write_result(&dir, &out);
        }
        // Open with `MDB_RDONLY | MDB_PREVSNAPSHOT` at the role's map size
        // and begin a read txn: "ok:<read txn id> <snapshot>", "EAGAIN" or
        // "rc=<rc>" goes to `result`; then close.
        "rdonly_prev_join" => {
            let out = match Lmdb::try_open(&dir, mapsize, MDB_RDONLY | ffi::MDB_PREVSNAPSHOT) {
                Ok(env) => {
                    let txn = env.begin(MDB_RDONLY).unwrap();
                    // SAFETY: `txn` is a live read txn.
                    let id = unsafe { ffi::mdb_txn_id(txn) };
                    let snap = snapshot_of(txn);
                    Lmdb::abort(txn);
                    format!("ok:{id} {snap}")
                }
                Err(libc::EAGAIN) => "EAGAIN".to_string(),
                Err(rc) => format!("rc={rc}"),
            };
            write_result(&dir, &out);
        }
        // Open with `MDB_PREVSNAPSHOT` at the role's map size, signal
        // `held`, and park until killed (never commits).
        "prev_hold" => {
            let _env = Lmdb::try_open(&dir, mapsize, ffi::MDB_PREVSNAPSHOT).unwrap();
            std::fs::write(dir.join("held"), b"").unwrap();
            std::thread::sleep(Duration::from_secs(600));
        }
        _ => panic!("unknown role {role}"),
    }
}

/// Which generation of [`two_meta_generations`]'s env, as rolled back and
/// written by [`observe_prev_snapshot_join_before_first_commit`], the read
/// txn `txn` sees: "a_commit2" (holds key `after2`), "a_commit" (`after`),
/// "newest" (`big0000`, the generation `MDB_PREVSNAPSHOT` rolled back), or
/// "prev" (none of them).
fn snapshot_of(txn: *mut ffi::MDB_txn) -> &'static str {
    if Lmdb::txn_has_key(txn, b"after2") {
        "a_commit2"
    } else if Lmdb::txn_has_key(txn, b"after") {
        "a_commit"
    } else if Lmdb::txn_has_key(txn, b"big0000") {
        "newest"
    } else {
        "prev"
    }
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

/// An env with two commits whose meta generations differ both in `last_pg`
/// (a small first commit, then a much larger second one) and in `mm_mapsize`
/// (a resize between the two commits), so the two tests below can tell which
/// generation `MDB_PREVSNAPSHOT` reads from. Returns the directory, the page
/// size, and the two generations ascending by txnid (`[0]` = prev, `[1]` =
/// newest).
fn two_meta_generations() -> (TempDir, u64, Vec<(u64, u64, u64)>) {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::open(dir.path(), 2 * MIB);
    a.commit_key(b"a");
    let psize = a.psize();
    assert_eq!(a.set_mapsize(16 * MIB), 0);
    let value = [7u8; 4000];
    let keys: Vec<String> = (0..300).map(|i| format!("big{i:04}")).collect();
    let pairs: Vec<(&[u8], &[u8])> = keys.iter().map(|k| (k.as_bytes(), &value[..])).collect();
    a.commit(&pairs);
    drop(a);
    let metas = read_metas(dir.path(), psize);
    assert_eq!(metas.len(), 2);
    assert_eq!(metas[0].0, 2 * MIB, "precondition: prev meta's mm_mapsize");
    assert_eq!(
        metas[1].0,
        16 * MIB,
        "precondition: newest meta's mm_mapsize"
    );
    assert!(
        metas[1].1 > metas[0].1,
        "precondition: the second commit moved last_pg past the first's"
    );
    (dir, psize, metas)
}

/// With two meta generations whose `last_pg` differ, `MDB_PREVSNAPSHOT`
/// opened with a map size below both generations' `(last_pg + 1) * psize`:
/// which generation's minimum wins.
#[test]
fn observe_prev_snapshot_minsize() {
    let (dir, psize, metas) = two_meta_generations();
    let prev_minsize = (metas[0].1 + 1) * psize;
    let newest_minsize = (metas[1].1 + 1) * psize;
    assert!(
        prev_minsize < newest_minsize,
        "precondition: prev meta's minimum is smaller than newest's"
    );
    let requested = psize;
    assert!(
        requested < prev_minsize,
        "precondition: the requested map is below both minimums"
    );
    let got = match Lmdb::try_open(dir.path(), requested, ffi::MDB_PREVSNAPSHOT) {
        Ok(env) => {
            let m = env.info().me_mapsize as u64;
            if m == prev_minsize {
                "prev".to_string()
            } else if m == newest_minsize {
                "newest".to_string()
            } else {
                format!("other={m}")
            }
        }
        Err(rc) => format!("rc={rc}"),
    };
    println!(
        "MDB_PREVSNAPSHOT open below both minsizes: {got} (prev_minsize {prev_minsize}, newest_minsize {newest_minsize})"
    );
    assert_eq!(got, OBS_PREV_SNAPSHOT_MINSIZE_FROM);
}

/// `MDB_PREVSNAPSHOT` opened with map size 0 (adopt): which generation's
/// persisted `mm_mapsize` the resulting `me_mapsize` matches.
#[test]
fn observe_prev_snapshot_zero_mapsize() {
    let (dir, _psize, metas) = two_meta_generations();
    let prev_mapsize = metas[0].0;
    let newest_mapsize = metas[1].0;
    let got = match Lmdb::try_open(dir.path(), 0, ffi::MDB_PREVSNAPSHOT) {
        Ok(env) => {
            let m = env.info().me_mapsize as u64;
            if m == prev_mapsize {
                "prev".to_string()
            } else if m == newest_mapsize {
                "newest".to_string()
            } else {
                format!("other={m}")
            }
        }
        Err(rc) => format!("rc={rc}"),
    };
    println!(
        "MDB_PREVSNAPSHOT open with map size 0: {got} (prev_mapsize {prev_mapsize}, newest_mapsize {newest_mapsize})"
    );
    assert_eq!(got, OBS_PREV_SNAPSHOT_ZERO_MAPSIZE_FROM);
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

/// Open with `mine` reader slots while a child holds the env with `held`:
/// `(rc, mdb_env_get_maxreaders)` (`0` readers when the open fails).
fn joiner_maxreaders(held: u32, mine: u32) -> (i32, u32) {
    let dir = TempDir::new().unwrap();
    let _c = {
        let c = spawn(&format!("hold_readers:{held}"), dir.path());
        wait_for(&dir.path().join("held"));
        c
    };
    match Lmdb::try_open_readers(dir.path(), MIB, 0, mine) {
        Ok(env) => (0, env.maxreaders()),
        Err(rc) => (rc, 0),
    }
}

#[test]
fn observe_joiner_fewer_maxreaders() {
    let got = joiner_maxreaders(200, 126);
    println!("joiner maxreaders 126, lock file sized for 200: (rc, maxreaders) {got:?}");
    assert_eq!(got, OBS_JOINER_FEWER_MAXREADERS);
}

#[test]
fn observe_joiner_more_maxreaders() {
    let got = joiner_maxreaders(126, 200);
    println!("joiner maxreaders 200, lock file sized for 126: (rc, maxreaders) {got:?}");
    assert_eq!(got, OBS_JOINER_MORE_MAXREADERS);
}

#[test]
fn observe_sole_opener_after_larger_run_maxreaders() {
    let dir = TempDir::new().unwrap();
    drop(Lmdb::try_open_readers(dir.path(), MIB, 0, 200).unwrap());
    let got = Lmdb::try_open_readers(dir.path(), MIB, 0, 126)
        .unwrap()
        .maxreaders();
    println!("sole opener maxreaders 126 after a run with 200: maxreaders {got}");
    assert_eq!(got, OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS);
}

#[test]
fn observe_rdonly_readonly_fs_reports_nolock() {
    let src = TempDir::new().unwrap();
    env_without_lock(src.path());
    let fs = match read_only_fs(src.path()) {
        Ok(fs) => fs,
        Err(why) => {
            println!("SKIPPED: no read-only filesystem here ({why})");
            return;
        }
    };
    let env = Lmdb::try_open(&fs.mount, MIB, MDB_RDONLY).unwrap();
    let nolock = env.flags() & ffi::MDB_NOLOCK != 0;
    println!(
        "MDB_RDONLY on a read-only filesystem: flags {:#x}, NOLOCK {nolock}",
        env.flags()
    );
    drop(env);
    assert_eq!(nolock, OBS_RDONLY_READONLY_FS_REPORTS_NOLOCK);
}

/// This process opens the env with `flags` while a child process holds a
/// write txn on it, then calls `mdb_env_set_mapsize(2 MiB)` on a helper
/// thread and waits up to 3 s: "returned:<rc>" if the call returned while
/// the child still held its txn, "blocked" if not (the child is then killed,
/// which releases the writer lock, and the call is joined).
fn set_mapsize_while_other_process_writes(flags: c_uint) -> String {
    let dir = TempDir::new().unwrap();
    let mut c = spawn("hold_writer", dir.path());
    wait_for(&dir.path().join("held"));
    let env = Lmdb::try_open(dir.path(), MIB, flags).unwrap();
    let raw = env.env as usize;
    let (tx, rx) = std::sync::mpsc::channel();
    let t = std::thread::spawn(move || {
        // SAFETY: the env stays open until this thread is joined below, and
        // no other thread uses it meanwhile; the call takes a plain size.
        let rc = unsafe { ffi::mdb_env_set_mapsize(raw as *mut ffi::MDB_env, (2 * MIB) as _) };
        let _ = tx.send(rc);
    });
    let got = match rx.recv_timeout(Duration::from_secs(3)) {
        Ok(rc) => {
            assert!(
                c.0.try_wait().unwrap().is_none(),
                "the writer child must still hold its txn when the call returns"
            );
            format!("returned:{rc}")
        }
        Err(_) => "blocked".to_string(),
    };
    drop(c);
    t.join().unwrap();
    println!(
        "set_mapsize(2 MiB), flags {flags:#x}, another process writing: {got}, me_mapsize {}",
        env.info().me_mapsize
    );
    got
}

#[test]
fn observe_set_mapsize_other_process_writer() {
    let got = set_mapsize_while_other_process_writes(0);
    assert_eq!(got, OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER);
}

#[test]
fn observe_set_mapsize_other_process_writer_rdonly() {
    let got = set_mapsize_while_other_process_writes(MDB_RDONLY);
    assert_eq!(got, OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER_RDONLY);
}

#[test]
fn observe_numreaders_other_process() {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), MIB);
    env.commit_key(b"a");
    let sample = |env: &Lmdb| (env.info().me_numreaders, env.reader_list_len());
    let mut c = spawn("hold_reads:3", dir.path());
    wait_for(&dir.path().join("held"));
    let held = sample(&env);
    std::fs::write(dir.path().join("release"), b"").unwrap();
    wait_for(&dir.path().join("released"));
    let ended = sample(&env);
    std::fs::write(dir.path().join("stop"), b"").unwrap();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(st) = c.0.try_wait().unwrap() {
            assert!(st.success(), "child hold_reads failed: {st}");
            break;
        }
        assert!(
            Instant::now() < end,
            "timed out waiting for child hold_reads"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let exited = sample(&env);
    let txn = env.begin(MDB_RDONLY).unwrap();
    let own = sample(&env);
    Lmdb::abort(txn);
    let got = [held, ended, exited, own];
    println!(
        "(me_numreaders, reader_list entries): 3 held elsewhere {held:?}, ended {ended:?}, other process exited {exited:?}, one own reader {own:?}"
    );
    assert_eq!(got, OBS_NUMREADERS_OTHER_PROCESS);
}

/// This process holds the env open with `holder_flags` at `holder_map` and
/// one commit; a child joins with `MDB_WRITEMAP` at `joiner_map` and reports
/// the `data.mdb` length before and after its open: "unchanged" or
/// "joiner_map".
fn write_map_joiner_file_len(holder_flags: c_uint, holder_map: u64, joiner_map: u64) -> String {
    let dir = TempDir::new().unwrap();
    let a = Lmdb::try_open(dir.path(), holder_map, holder_flags).unwrap();
    a.commit_key(b"a");
    let held_len = std::fs::metadata(dir.path().join("data.mdb"))
        .unwrap()
        .len();
    run_child(&format!("join_writemap:{joiner_map}"), dir.path());
    let out = std::fs::read_to_string(dir.path().join("result")).unwrap();
    let (before, after) = out.split_once(' ').unwrap();
    let (before, after): (u64, u64) = (before.parse().unwrap(), after.parse().unwrap());
    let closed_len = std::fs::metadata(dir.path().join("data.mdb"))
        .unwrap()
        .len();
    println!(
        "WRITEMAP joiner (holder flags {holder_flags:#x}, holder map {holder_map}, joiner map {joiner_map}): \
         length held {held_len}, before open {before}, after open {after}, after joiner closed {closed_len}"
    );
    assert_eq!(before, held_len, "nothing else touched the file");
    drop(a);
    if after == before {
        "unchanged".to_string()
    } else if after == joiner_map {
        "joiner_map".to_string()
    } else {
        format!("other={after}")
    }
}

#[test]
fn observe_write_map_joiner_larger_map() {
    let got = write_map_joiner_file_len(0, MIB, 8 * MIB);
    println!("WRITEMAP joiner with a larger map: {got}");
    assert_eq!(got, OBS_WRITE_MAP_JOINER_LARGER_MAP);
}

#[test]
fn observe_write_map_joiner_smaller_map() {
    let got = write_map_joiner_file_len(MDB_WRITEMAP, 8 * MIB, 2 * MIB);
    println!("WRITEMAP joiner with a smaller map: {got}");
    assert_eq!(got, OBS_WRITE_MAP_JOINER_SMALLER_MAP);
}

/// Process A rolls back with `MDB_PREVSNAPSHOT`; process B opens before A's
/// first commit. Pins B's outcome ([`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`])
/// and what A's own read txns see after its first and second commits
/// ([`OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW`]).
#[test]
fn observe_prev_snapshot_join_before_first_commit() {
    let (dir, psize, metas) = two_meta_generations();
    let result = dir.path().join("result");
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    let _b = spawn(&format!("join_report:{}", 16 * MIB), dir.path());
    let a_view = |a: &Lmdb| {
        let t = a.begin(MDB_RDONLY).unwrap();
        // SAFETY: `t` is a live read txn.
        let id = unsafe { ffi::mdb_txn_id(t) };
        let snap = snapshot_of(t);
        Lmdb::abort(t);
        println!(
            "A's read txn: id {id}, sees {snap}; me_last_txnid {}; metas on disk {:?}",
            a.info().me_last_txnid,
            read_metas(dir.path(), psize)
        );
        snap
    };
    println!("metas before A's open: {metas:?}");
    let mut b = Vec::new();
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
    b.push(step(Duration::from_secs(3)));
    a.commit_key(b"after");
    let first = a_view(&a);
    b.push(step(Duration::from_secs(10)));
    a.commit_key(b"after2");
    let second = a_view(&a);
    drop(a);
    b.push(step(Duration::from_secs(10)));
    println!("B joining before a PREVSNAPSHOT env's first commit: {b:?}");
    println!("A's read view after its first and second commits: [{first}, {second}]");
    assert_eq!(b, OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT);
    assert_eq!([first, second], OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW);
}

/// Commit `key` in one write txn, returning the write txn's id
/// (`mdb_txn_id`).
fn commit_key_id(env: &Lmdb, key: &[u8]) -> u64 {
    let txn = env.begin(0).unwrap();
    // SAFETY: standard txn_id/dbi_open/put/commit sequence on a live write
    // txn; every pointer is live for the duration of its call.
    unsafe {
        let id = ffi::mdb_txn_id(txn) as u64;
        let mut dbi = 0;
        assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
        let mut k = ffi::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut c_void,
        };
        let mut v = ffi::MDB_val {
            mv_size: 1,
            mv_data: b"v".as_ptr() as *mut c_void,
        };
        assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
        assert_eq!(ffi::mdb_txn_commit(txn), 0);
        id
    }
}

/// The finer-grained form of [`OBS_PREV_SNAPSHOT_FIRST_COMMIT_READ_VIEW`],
/// in one process: txn ids around a `MDB_PREVSNAPSHOT` env's first two
/// commits ([`OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`]), whether the second commit
/// keeps the first one's key ([`OBS_PREV_SNAPSHOT_SECOND_COMMIT_KEEPS_FIRST`]),
/// and what a fresh open sees after one commit and a close
/// ([`OBS_PREV_SNAPSHOT_ONE_COMMIT_THEN_REOPEN`]).
#[test]
fn observe_prev_snapshot_commit_txnids() {
    let (dir, psize, _) = two_meta_generations();
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    let after_commit = |a: &Lmdb, write_id: u64| {
        let t = a.begin(MDB_RDONLY).unwrap();
        // SAFETY: `t` is a live read txn.
        let read_id = unsafe { ffi::mdb_txn_id(t) } as u64;
        let has_after = Lmdb::txn_has_key(t, b"after");
        Lmdb::abort(t);
        let last = a.info().me_last_txnid as u64;
        println!(
            "write txn {write_id}; then read txn {read_id}, me_last_txnid {last}, \
             holds `after` {has_after}; metas on disk {:?}",
            read_metas(dir.path(), psize)
        );
        ((write_id, read_id, last), has_after)
    };
    let id1 = commit_key_id(&a, b"after");
    let (first, _) = after_commit(&a, id1);
    let id2 = commit_key_id(&a, b"after2");
    let (second, keeps_first) = after_commit(&a, id2);
    drop(a);
    println!("(write id, read id, me_last_txnid): [{first:?}, {second:?}]; second keeps first: {keeps_first}");
    assert_eq!([first, second], OBS_PREV_SNAPSHOT_COMMIT_TXNIDS);
    assert_eq!(keeps_first, OBS_PREV_SNAPSHOT_SECOND_COMMIT_KEEPS_FIRST);

    let (dir, _, _) = two_meta_generations();
    {
        let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
        a.commit_key(b"after");
    }
    let env = Lmdb::try_open(dir.path(), 16 * MIB, 0).unwrap();
    let t = env.begin(MDB_RDONLY).unwrap();
    let reopened = snapshot_of(t);
    Lmdb::abort(t);
    println!("one PREVSNAPSHOT commit, close, fresh open: {reopened}");
    assert_eq!(reopened, OBS_PREV_SNAPSHOT_ONE_COMMIT_THEN_REOPEN);
}

/// Process B joins a `MDB_PREVSNAPSHOT` env after the opener's first commit,
/// while the opener still has it open: B's read txn id, `me_last_txnid` and
/// snapshot ([`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`]).
#[test]
fn observe_prev_snapshot_joiner_info_after_first_commit() {
    let (dir, _, _) = two_meta_generations();
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    a.commit_key(b"after");
    run_child(&format!("join_info:{}", 16 * MIB), dir.path());
    let got = std::fs::read_to_string(dir.path().join("result")).unwrap();
    drop(a);
    println!("B joining after a PREVSNAPSHOT env's first commit: {got}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT);
}

/// B waits to join a `MDB_PREVSNAPSHOT` env; the opener A ends before any
/// commit, by closing the env (`kill` false: A is this process) or by being
/// killed (`kill` true: A is a child). B's outcome within 3 s of starting,
/// then within 10 s after A ends, as in
/// [`OBS_PREV_SNAPSHOT_JOIN_BEFORE_FIRST_COMMIT`].
fn prev_snapshot_opener_ends_before_commit(kill: bool) -> [String; 2] {
    let (dir, _, _) = two_meta_generations();
    let result = dir.path().join("result");
    let (mut a_child, a_env) = if kill {
        let c = spawn(&format!("prev_hold:{}", 16 * MIB), dir.path());
        wait_for(&dir.path().join("held"));
        (Some(c), None)
    } else {
        let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
        (None, Some(a))
    };
    let _b = spawn(&format!("join_report:{}", 16 * MIB), dir.path());
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

#[test]
fn observe_prev_snapshot_opener_closes_before_commit() {
    let got = prev_snapshot_opener_ends_before_commit(false);
    println!("B waiting while the PREVSNAPSHOT opener closes uncommitted: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_OPENER_CLOSES_BEFORE_COMMIT);
}

#[test]
fn observe_prev_snapshot_opener_dies_before_commit() {
    let got = prev_snapshot_opener_ends_before_commit(true);
    println!("B waiting while the PREVSNAPSHOT opener is killed uncommitted: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_OPENER_DIES_BEFORE_COMMIT);
}

/// A second process opens with `MDB_PREVSNAPSHOT` while the first
/// `MDB_PREVSNAPSHOT` opener has not committed yet: its outcome within 3 s,
/// then within 10 s after the opener's first commit
/// ([`OBS_PREV_SNAPSHOT_SECOND_PREV_OPENER`]).
#[test]
fn observe_prev_snapshot_second_prev_opener() {
    let (dir, _, _) = two_meta_generations();
    let result = dir.path().join("result");
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    let _c = spawn(&format!("prev_join_report:{}", 16 * MIB), dir.path());
    let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
    a.commit_key(b"after");
    let s1 = if s0 == "blocked" {
        poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
    } else {
        "-".to_string()
    };
    drop(a);
    let got = [s0, s1];
    println!("a second PREVSNAPSHOT opener before the first one's commit: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_SECOND_PREV_OPENER);
}

/// A sole process opens with `MDB_RDONLY | MDB_PREVSNAPSHOT` (it can never
/// commit); a second process then opens without flags. The second
/// process's outcome within 3 s, then within 10 s after the first closes
/// ([`OBS_PREV_SNAPSHOT_READ_ONLY_OPENER`]).
#[test]
fn observe_prev_snapshot_read_only_opener() {
    let (dir, _, _) = two_meta_generations();
    let result = dir.path().join("result");
    let a = Lmdb::try_open(dir.path(), 16 * MIB, MDB_RDONLY | ffi::MDB_PREVSNAPSHOT).unwrap();
    let _b = spawn(&format!("join_report:{}", 16 * MIB), dir.path());
    let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
    drop(a);
    let s1 = if s0 == "blocked" {
        poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
    } else {
        "-".to_string()
    };
    let got = [s0, s1];
    println!("joining a sole MDB_RDONLY|MDB_PREVSNAPSHOT opener: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_READ_ONLY_OPENER);
}

/// The `MDB_PREVSNAPSHOT` opener commits a write txn that changes nothing
/// while another process waits to join: the waiter's outcome within 3 s of
/// that empty commit, then within 10 s after a commit that writes a key
/// ([`OBS_PREV_SNAPSHOT_EMPTY_COMMIT_KEEPS_LOCK`]).
#[test]
fn observe_prev_snapshot_empty_commit_keeps_lock() {
    let (dir, _, _) = two_meta_generations();
    let result = dir.path().join("result");
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    let _b = spawn(&format!("join_report:{}", 16 * MIB), dir.path());
    let txn = a.begin(0).unwrap();
    // SAFETY: `txn` is a live write txn with no changes; committed once.
    assert_eq!(unsafe { ffi::mdb_txn_commit(txn) }, 0);
    let s0 = poll_file(&result, Duration::from_secs(3)).unwrap_or_else(|| "blocked".into());
    a.commit_key(b"after");
    let s1 = if s0 == "blocked" {
        poll_file(&result, Duration::from_secs(10)).unwrap_or_else(|| "blocked".into())
    } else {
        "-".to_string()
    };
    drop(a);
    let got = [s0, s1];
    println!("joining after an empty PREVSNAPSHOT commit, then a real one: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_EMPTY_COMMIT_KEEPS_LOCK);
}

/// `MDB_NOLOCK | MDB_PREVSNAPSHOT`, one process, two commits: the snapshot
/// a fresh read txn sees after each, and whether the second commit keeps the
/// first commit's key ([`OBS_PREV_SNAPSHOT_NOLOCK_COMMITS`]).
#[test]
fn observe_prev_snapshot_nolock_commits() {
    let (dir, _, _) = two_meta_generations();
    let a = Lmdb::try_open(
        dir.path(),
        16 * MIB,
        ffi::MDB_NOLOCK | ffi::MDB_PREVSNAPSHOT,
    )
    .unwrap();
    let view = |a: &Lmdb| {
        let t = a.begin(MDB_RDONLY).unwrap();
        let snap = snapshot_of(t);
        let has_after = Lmdb::txn_has_key(t, b"after");
        Lmdb::abort(t);
        (snap, has_after)
    };
    a.commit_key(b"after");
    let (first, _) = view(&a);
    a.commit_key(b"after2");
    let (second, keeps_first) = view(&a);
    drop(a);
    let got = (first, second, keeps_first);
    println!(
        "NOLOCK|PREVSNAPSHOT: after 1st {first}, after 2nd {second}, keeps first {keeps_first}"
    );
    assert_eq!(got, OBS_PREV_SNAPSHOT_NOLOCK_COMMITS);
}

/// The main DB root page number of the newest meta in `data.mdb` (the
/// `md_root` of the main `MDB_db`, at offset 128 of a meta page: 88 for the
/// record, plus 40).
fn newest_main_root(dir: &Path, psize: u64) -> u64 {
    let bytes = std::fs::read(dir.join("data.mdb")).unwrap();
    let u64_at = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
    (0..2)
        .map(|i| {
            let base = (i * psize) as usize;
            (u64_at(base + 144), u64_at(base + 128))
        })
        .max()
        .unwrap()
        .1
}

/// The crash window between a `MDB_PREVSNAPSHOT` env's first and second
/// commits: the main DB root page of the newest meta after the first
/// commit, and whether the second commit wrote its own root to that same
/// page number ([`OBS_PREV_SNAPSHOT_SECOND_COMMIT_REWRITES_FIRST_ROOT`]).
#[test]
fn observe_prev_snapshot_second_commit_rewrites_first_root() {
    let (dir, psize, _) = two_meta_generations();
    let a = Lmdb::try_open(dir.path(), 16 * MIB, ffi::MDB_PREVSNAPSHOT).unwrap();
    a.commit_key(b"after");
    let first_root = newest_main_root(dir.path(), psize);
    let first_metas = read_metas(dir.path(), psize);
    a.commit_key(b"after2");
    let second_root = newest_main_root(dir.path(), psize);
    let second_metas = read_metas(dir.path(), psize);
    drop(a);
    println!(
        "main root after the 1st commit {first_root} (metas {first_metas:?}), after the 2nd {second_root} (metas {second_metas:?})"
    );
    assert_eq!(
        first_root == second_root,
        OBS_PREV_SNAPSHOT_SECOND_COMMIT_REWRITES_FIRST_ROOT
    );
}

/// A process holds an env open (no flags, [`two_meta_generations`]); a
/// second process opens it with `MDB_RDONLY | MDB_PREVSNAPSHOT`, reads and
/// closes. Then the holder begins a read txn and a write txn
/// ([`OBS_PREV_SNAPSHOT_READ_ONLY_JOINER`]).
#[test]
fn observe_prev_snapshot_read_only_joiner() {
    let (dir, _, _) = two_meta_generations();
    let a = Lmdb::try_open(dir.path(), 16 * MIB, 0).unwrap();
    run_child(&format!("rdonly_prev_join:{}", 16 * MIB), dir.path());
    let joiner = std::fs::read_to_string(dir.path().join("result")).unwrap();
    let t = a.begin(MDB_RDONLY).unwrap();
    // SAFETY: `t` is a live read txn.
    let read_id = unsafe { ffi::mdb_txn_id(t) };
    let holder_view = format!("{read_id} {}", snapshot_of(t));
    Lmdb::abort(t);
    let write_id = commit_key_id(&a, b"after").to_string();
    drop(a);
    let got = [joiner, holder_view, write_id];
    println!("RDONLY|PREVSNAPSHOT joiner on a running env: {got:?}");
    assert_eq!(got, OBS_PREV_SNAPSHOT_READ_ONLY_JOINER);
}
