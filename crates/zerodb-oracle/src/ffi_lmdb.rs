//! A thin raw-FFI handle on the Meilisearch LMDB fork, for observation tests
//! that need what heed does not expose (`mdb_reader_list`, `mdb_reader_check`,
//! `mdb_env_get_path`, raw open flags and return codes).
//!
//! Every call goes straight to `lmdb-master-sys`, the same vendored fork heed
//! links (ADR-0001). Failures that a test does not observe panic; the ones it
//! does observe come back as the raw LMDB return code.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::Path;
use std::ptr;

use lmdb_master_sys as ffi;

/// `MDB_NOTLS`: reader slots are tied to the txn, not the thread. Always set
/// by [`Lmdb::try_open`], as heed sets it.
pub const MDB_NOTLS: c_uint = 0x0020_0000;
/// `MDB_RDONLY`, for [`Lmdb::begin`].
pub const MDB_RDONLY: c_uint = 0x0002_0000;

/// An open LMDB environment, closed on drop.
pub struct Lmdb {
    /// The raw handle, for the calls this wrapper does not cover.
    pub env: *mut ffi::MDB_env,
}

/// A transaction from [`Lmdb::begin`]. It is not ended on drop: a test ends
/// it with [`Lmdb::abort`] (or a commit), or leaves it open on purpose, as a
/// child process does that is killed while holding it.
#[derive(Debug)]
pub struct Txn(*mut ffi::MDB_txn);

impl Txn {
    /// The raw handle, for the calls this wrapper does not cover. It stays
    /// valid until the txn is ended.
    pub fn as_ptr(&self) -> *mut ffi::MDB_txn {
        self.0
    }

    /// `mdb_txn_id`.
    pub fn id(&self) -> u64 {
        // SAFETY: `self.0` came from `mdb_txn_begin` and has not been ended
        // (ending one consumes the `Txn`).
        unsafe { ffi::mdb_txn_id(self.0) as u64 }
    }
}

/// Wrap `bytes` as an `MDB_val` for an input argument. LMDB does not write
/// through input values, so the `*mut` is never used to mutate.
fn val(bytes: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_size: bytes.len(),
        mv_data: bytes.as_ptr() as *mut c_void,
    }
}

impl Lmdb {
    /// Open `dir` with `mapsize` bytes, `max_readers` reader slots and extra
    /// `flags` ([`MDB_NOTLS`] is added); the raw return code on failure.
    pub fn try_open(
        dir: &Path,
        mapsize: u64,
        max_readers: u32,
        flags: c_uint,
    ) -> Result<Lmdb, c_int> {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle; `path` outlives
        // `mdb_env_open`. On failure the handle is closed as the C API requires.
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

    /// [`Lmdb::try_open`] with no extra flags, panicking on failure.
    pub fn open(dir: &Path, mapsize: u64, max_readers: u32) -> Lmdb {
        Lmdb::try_open(dir, mapsize, max_readers, 0).unwrap()
    }

    /// Begin a top-level txn with `flags`; the raw return code on failure.
    pub fn begin(&self, flags: c_uint) -> Result<Txn, c_int> {
        let mut txn = ptr::null_mut();
        // SAFETY: `self.env` is open; `txn` receives a handle the caller owns.
        let rc = unsafe { ffi::mdb_txn_begin(self.env, ptr::null_mut(), flags, &mut txn) };
        if rc == 0 {
            Ok(Txn(txn))
        } else {
            Err(rc)
        }
    }

    /// Begin a read txn; the raw return code on failure.
    pub fn try_read(&self) -> Result<Txn, c_int> {
        self.begin(MDB_RDONLY)
    }

    /// Begin and abort a txn with `flags`, normalized for comparison with
    /// ZeroDB: `"ok"`, `"MapResized"` or `"rc=<code>"`.
    pub fn begin_outcome(&self, flags: c_uint) -> String {
        match self.begin(flags) {
            Ok(txn) => {
                Lmdb::abort(txn);
                "ok".to_string()
            }
            Err(ffi::MDB_MAP_RESIZED) => "MapResized".to_string(),
            Err(rc) => format!("rc={rc}"),
        }
    }

    /// End `txn` without committing.
    pub fn abort(txn: Txn) {
        // SAFETY: `txn` came from `mdb_txn_begin`; consuming it ends it once.
        unsafe { ffi::mdb_txn_abort(txn.0) }
    }

    /// Commit `txn`; the raw return code.
    pub fn commit_txn(txn: Txn) -> c_int {
        // SAFETY: `txn` came from `mdb_txn_begin`; consuming it ends it once.
        unsafe { ffi::mdb_txn_commit(txn.0) }
    }

    /// Write `pairs` to the main database in one write txn and commit;
    /// the write txn's id.
    pub fn commit<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, pairs: &[(K, V)]) -> u64 {
        let txn = self.begin(0).unwrap();
        let id = txn.id();
        let txn = txn.0;
        // SAFETY: standard dbi_open/put/commit sequence on a live write txn;
        // every pointer is live for the duration of its call.
        unsafe {
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            for (k, v) in pairs {
                let mut k = val(k.as_ref());
                let mut v = val(v.as_ref());
                assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
            }
            assert_eq!(ffi::mdb_txn_commit(txn), 0);
        }
        id
    }

    /// Commit `key` → `"v"`; the write txn's id.
    pub fn commit_key(&self, key: &[u8]) -> u64 {
        self.commit(&[(key, b"v")])
    }

    /// Commit the single entry `k` → `v`.
    pub fn commit_one(&self) {
        self.commit_key(b"k");
    }

    /// Put `n` 1000-byte values (keys `g00000000`, …) in **one** txn and
    /// commit, so the meta's last page moves well past a small env in a
    /// single generation.
    pub fn commit_many(&self, n: u32) {
        let value = [7u8; 1000];
        let pairs: Vec<(String, &[u8])> =
            (0..n).map(|i| (format!("g{i:08}"), &value[..])).collect();
        self.commit(&pairs);
    }

    /// Whether the main database holds `key` in the live txn `txn`.
    pub fn txn_has_key(txn: &Txn, key: &[u8]) -> bool {
        let txn = txn.0;
        // SAFETY: dbi_open/get on a live txn (a `Txn` is live until consumed);
        // `key` outlives the call and the returned data pointer is not used.
        unsafe {
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            let mut k = val(key);
            let mut v = val(&[]);
            match ffi::mdb_get(txn, dbi, &mut k, &mut v) {
                0 => true,
                ffi::MDB_NOTFOUND => false,
                rc => panic!("mdb_get failed: {rc}"),
            }
        }
    }

    /// Whether the main database holds `key`, read in a fresh read txn.
    pub fn has_key(&self, key: &[u8]) -> bool {
        let txn = self.try_read().unwrap();
        let found = Lmdb::txn_has_key(&txn, key);
        Lmdb::abort(txn);
        found
    }

    /// `mdb_env_info`.
    pub fn info(&self) -> ffi::MDB_envinfo {
        // SAFETY: MDB_envinfo is plain data; LMDB fills it.
        unsafe {
            let mut i: ffi::MDB_envinfo = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_info(self.env, &mut i), 0);
            i
        }
    }

    /// `mdb_env_stat` (the main database).
    pub fn stat(&self) -> ffi::MDB_stat {
        // SAFETY: MDB_stat is plain data; LMDB fills it.
        unsafe {
            let mut s: ffi::MDB_stat = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_stat(self.env, &mut s), 0);
            s
        }
    }

    /// The page size: LMDB uses the OS page size (16 KiB on macOS aarch64).
    pub fn psize(&self) -> u64 {
        u64::from(self.stat().ms_psize)
    }

    /// `(me_mapsize, me_last_pgno)`.
    pub fn map_info(&self) -> (u64, u64) {
        let i = self.info();
        (i.me_mapsize as u64, i.me_last_pgno as u64)
    }

    /// `mdb_env_info`'s `me_last_txnid`.
    pub fn last_txnid(&self) -> u64 {
        self.info().me_last_txnid as u64
    }

    /// The main database's entry count.
    pub fn entries(&self) -> u64 {
        self.stat().ms_entries as u64
    }

    /// `mdb_env_get_maxreaders`.
    pub fn maxreaders(&self) -> u32 {
        let mut n: c_uint = 0;
        // SAFETY: plain call on an open env; `n` outlives it.
        assert_eq!(unsafe { ffi::mdb_env_get_maxreaders(self.env, &mut n) }, 0);
        n
    }

    /// `mdb_env_get_flags`.
    pub fn flags(&self) -> c_uint {
        let mut f: c_uint = 0;
        // SAFETY: plain call on an open env; `f` outlives it.
        assert_eq!(unsafe { ffi::mdb_env_get_flags(self.env, &mut f) }, 0);
        f
    }

    /// `mdb_env_get_path`.
    pub fn path(&self) -> String {
        let mut p: *const c_char = ptr::null();
        // SAFETY: LMDB returns a pointer into the env's own storage, valid
        // while the env is open; copied out immediately.
        unsafe {
            assert_eq!(ffi::mdb_env_get_path(self.env, &mut p), 0);
            CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    }

    /// `mdb_env_set_mapsize`'s raw return code.
    pub fn set_mapsize(&self, size: u64) -> c_int {
        // SAFETY: `self.env` is open; the call takes a plain size.
        unsafe { ffi::mdb_env_set_mapsize(self.env, size as _) }
    }

    /// The lines `mdb_reader_list` prints.
    fn reader_lines(&self) -> Vec<String> {
        unsafe extern "C" fn collect(msg: *const c_char, ctx: *mut c_void) -> c_int {
            // SAFETY: `ctx` is the `&mut Vec<String>` passed below, live for
            // the call, and no other reference to it exists meanwhile.
            let out = unsafe { &mut *(ctx as *mut Vec<String>) };
            // SAFETY: LMDB passes a valid NUL-terminated string that lives for
            // the duration of this callback; it is copied out before returning.
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
            .map(str::to_owned)
            .collect()
    }

    /// PIDs listed by `mdb_reader_list`, one per occupied reader slot, in any
    /// process.
    pub fn reader_pids(&self) -> Vec<u32> {
        self.reader_lines()
            .iter()
            .filter_map(|l| l.split_whitespace().next()?.parse::<u32>().ok())
            .collect()
    }

    /// The number of occupied reader slots `mdb_reader_list` prints.
    pub fn reader_list_len(&self) -> usize {
        self.reader_pids().len()
    }

    /// `mdb_reader_check`'s dead count.
    pub fn reader_check(&self) -> i32 {
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
