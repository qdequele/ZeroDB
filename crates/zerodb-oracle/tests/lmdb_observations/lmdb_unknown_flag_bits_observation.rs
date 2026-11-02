//! What the Meilisearch LMDB fork does with persisted flag bits it does not
//! define, observed by patching them into a closed environment's data file.
//! Input for docs/adr/0040-refuse-unknown-flag-bits.md.
//! CONTRIBUTING.md rule 1: observed, not guessed.
//!
//! LMDB keeps a 16-bit `md_flags` word in every database record: the two
//! records in each meta page (the free-page database, whose `md_flags` is
//! also LMDB's persistent env-flags word `mm_flags`, and the main database)
//! and each named database's record, stored as its catalog value. The fork
//! defines six of those bits (`REVERSEKEY`, `DUPSORT`, `INTEGERKEY`,
//! `DUPFIXED`, `INTEGERDUP`, `REVERSEDUP`, `0x02..=0x40`). Each script sets a
//! bit outside that set in one record, reopens the environment and records:
//!
//! 1. `open=`: the result of `mdb_env_open`;
//! 2. `dbi=`: the result of `mdb_dbi_open` without flags, and the flags
//!    `mdb_dbi_flags` then reports;
//! 3. `get=` / `commit=`: whether a read and a write commit still succeed;
//! 4. `kept=`: whether the bit is still in the record after that commit and a
//!    second reopen (`yes`/`no`).
//!
//! LMDB's meta pages and catalog values carry no checksum, so the patched
//! file is as valid to the fork as one it wrote itself. Meta field offsets
//! are those of the 64-bit build (16-byte page header, `mm_dbs` at byte 40 of
//! the page, 48-byte records); `meta_offsets_hold` checks them against values
//! the fork always writes before any patch relies on them.
//!
//! A last script sets the bit in the named record while the environment is
//! open and a handle to the database is already registered, then reads
//! through that handle.
//!
//! What the pins show about LMDB:
//! - It never validates persisted flag bits at open: an unknown bit in the
//!   env-flags word, the main record or a named record opens, reads and
//!   writes normally, and `mdb_dbi_flags` reports it.
//! - It keeps them: every commit copies the records it read, unknown bits
//!   included, so the bit is still there after a write.
//! - Its one flag check is against itself: when a registered handle's
//!   record no longer carries the flags the handle was opened with, the
//!   next transaction to use the handle fails with `MDB_INCOMPATIBLE`.
//!
//! The `OBS_*` constants pin the fork's outcomes; a diff means the fork
//! changed or the pin was wrong, never edit a pin to make ZeroDB pass. Pins
//! observed on macOS (aarch64, 16 KiB pages).

use std::ffi::CString;
use std::os::raw::{c_int, c_uint, c_void};
use std::path::Path;
use std::ptr;

use lmdb_master_sys as ffi;
use zerodb_oracle::tempdir::TempDir;

const MDB_CREATE: c_uint = 0x40000;
const MDB_INTEGERKEY: u16 = 0x08;

/// Two bits outside the fork's persistent database flags: the lowest one
/// above `REVERSEDUP`, and one near the top of the word.
const UNKNOWN_BITS: [u16; 2] = [0x0100, 0x4000];

/// Byte offset of `mm_dbs` in a meta page: page header (16) + `mm_magic` (4)
/// + `mm_version` (4) + `mm_address` (8) + `mm_mapsize` (8).
const META_DBS: usize = 40;
/// Size of one `MDB_db` record.
const DB_LEN: usize = 48;
/// `md_flags` inside a record, after the 4-byte `md_pad`.
const DB_FLAGS: usize = 4;
/// `mm_txnid` in a meta page: after both records and `mm_last_pg`.
const META_TXNID: usize = META_DBS + 2 * DB_LEN + 8;

const NAME: &str = "unknown-flag-bits-probe";

fn rc_name(rc: c_int) -> String {
    match rc {
        0 => "OK".into(),
        ffi::MDB_NOTFOUND => "NOTFOUND".into(),
        ffi::MDB_INCOMPATIBLE => "INCOMPATIBLE".into(),
        ffi::MDB_INVALID => "INVALID".into(),
        ffi::MDB_VERSION_MISMATCH => "VERSION_MISMATCH".into(),
        ffi::MDB_CORRUPTED => "CORRUPTED".into(),
        22 => "EINVAL".into(),
        other => format!("rc({other})"),
    }
}

fn val(bytes: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_size: bytes.len(),
        mv_data: bytes.as_ptr() as *mut c_void,
    }
}

/// An open fork environment on `dir`. Closed on drop.
struct Fork {
    env: *mut ffi::MDB_env,
}

impl Fork {
    /// Open `dir`, returning the `mdb_env_open` code on failure.
    fn open(dir: &Path) -> Result<Fork, c_int> {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        // SAFETY: standard env-creation sequence; on failure the env is closed
        // here, on success in Drop, after every txn below has ended.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxdbs(env, 4), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, 32 << 20), 0);
            let rc = ffi::mdb_env_open(env, path.as_ptr(), 0, 0o664);
            if rc != 0 {
                ffi::mdb_env_close(env);
                return Err(rc);
            }
        }
        Ok(Fork { env })
    }

    fn page_size(&self) -> usize {
        // SAFETY: env is live; the stat struct is plain data.
        unsafe {
            let mut st: ffi::MDB_stat = std::mem::zeroed();
            assert_eq!(ffi::mdb_env_stat(self.env, &mut st), 0);
            st.ms_psize as usize
        }
    }

    fn txn(&self, flags: c_uint) -> *mut ffi::MDB_txn {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        // SAFETY: env is live; one txn at a time on this thread.
        unsafe {
            assert_eq!(
                ffi::mdb_txn_begin(self.env, ptr::null_mut(), flags, &mut txn),
                0
            );
        }
        txn
    }
}

impl Drop for Fork {
    fn drop(&mut self) {
        // SAFETY: every txn has been committed or aborted before drop.
        unsafe { ffi::mdb_env_close(self.env) }
    }
}

/// Open `name` (`None` = the main database) without flags: the result code
/// and, on success, the dbi and the flags the fork reports for it.
fn dbi_open(
    txn: *mut ffi::MDB_txn,
    name: Option<&str>,
    flags: c_uint,
) -> (c_int, ffi::MDB_dbi, c_uint) {
    let name = name.map(|n| CString::new(n).unwrap());
    let name_ptr = name.as_ref().map_or(ptr::null(), |n| n.as_ptr());
    let mut dbi: ffi::MDB_dbi = 0;
    let mut got: c_uint = 0;
    // SAFETY: txn is live; `name` outlives the call.
    unsafe {
        let rc = ffi::mdb_dbi_open(txn, name_ptr, flags, &mut dbi);
        if rc == 0 {
            assert_eq!(ffi::mdb_dbi_flags(txn, dbi, &mut got), 0);
        }
        (rc, dbi, got)
    }
}

fn put(txn: *mut ffi::MDB_txn, dbi: ffi::MDB_dbi, k: &[u8], v: &[u8]) -> c_int {
    let mut key = val(k);
    let mut data = val(v);
    // SAFETY: key/data point at live slices for the duration of the call.
    unsafe { ffi::mdb_put(txn, dbi, &mut key, &mut data, 0) }
}

fn get(txn: *mut ffi::MDB_txn, dbi: ffi::MDB_dbi, k: &[u8]) -> c_int {
    let mut key = val(k);
    let mut data = ffi::MDB_val {
        mv_size: 0,
        mv_data: ptr::null_mut(),
    };
    // SAFETY: txn is live; the out-val is only inspected through its code.
    unsafe { ffi::mdb_get(txn, dbi, &mut key, &mut data) }
}

/// Create an environment holding one entry in the main database and one in
/// the named database `NAME`, then close it. Returns the page size.
fn seed(dir: &Path) -> usize {
    let fork = Fork::open(dir).unwrap();
    let txn = fork.txn(0);
    let (rc, named, _) = dbi_open(txn, Some(NAME), MDB_CREATE);
    assert_eq!(rc, 0);
    assert_eq!(put(txn, named, b"k", b"v"), 0);
    // SAFETY: txn is live and committed exactly once.
    unsafe { assert_eq!(ffi::mdb_txn_commit(txn), 0) };
    fork.page_size()
}

fn data_file(dir: &Path) -> std::path::PathBuf {
    dir.join("data.mdb")
}

fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([b[off], b[off + 1]])
}

fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

fn read_u64(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}

/// Where a database record lives in the file.
#[derive(Clone, Copy)]
enum Record {
    /// `mm_dbs[0]` of both meta pages: the free-page database, whose
    /// `md_flags` is LMDB's `mm_flags`.
    MetaFree,
    /// `mm_dbs[1]` of both meta pages: the main database.
    MetaMain,
    /// The named database's catalog value.
    Named,
}

/// Byte offsets of `md_flags` for `rec` in `file`: one per meta page for the
/// meta records; for the named record, every copy of the catalog value in
/// the file (the name followed by its record).
fn flag_offsets(file: &[u8], psize: usize, rec: Record) -> Vec<usize> {
    let slot = match rec {
        Record::MetaFree => 0,
        Record::MetaMain => 1,
        Record::Named => {
            let name = NAME.as_bytes();
            return file
                .windows(name.len())
                .enumerate()
                .filter(|(_, w)| *w == name)
                .map(|(i, _)| i + name.len() + DB_FLAGS)
                .collect();
        }
    };
    (0..2)
        .map(|m| m * psize + META_DBS + slot * DB_LEN + DB_FLAGS)
        .collect()
}

/// The `md_flags` the fork would read now: the newest meta's for a meta
/// record, the live catalog value's for the named one (the copy the fork
/// reports through `mdb_dbi_flags`, read by opening the database).
fn live_flags(dir: &Path, psize: usize, rec: Record) -> u16 {
    let file = std::fs::read(data_file(dir)).unwrap();
    match rec {
        Record::MetaFree | Record::MetaMain => {
            let newest = if read_u64(&file, META_TXNID) >= read_u64(&file, psize + META_TXNID) {
                0
            } else {
                1
            };
            let offs = flag_offsets(&file, psize, rec);
            read_u16(&file, offs[newest])
        }
        Record::Named => {
            let fork = Fork::open(dir).unwrap();
            let txn = fork.txn(ffi::MDB_RDONLY);
            let (rc, _, flags) = dbi_open(txn, Some(NAME), 0);
            // SAFETY: read txn, aborted exactly once.
            unsafe { ffi::mdb_txn_abort(txn) };
            assert_eq!(rc, 0);
            flags as u16
        }
    }
}

/// OR `bit` into every copy of `rec`'s `md_flags`.
fn patch(dir: &Path, psize: usize, rec: Record, bit: u16) {
    let path = data_file(dir);
    let mut file = std::fs::read(&path).unwrap();
    let offs = flag_offsets(&file, psize, rec);
    assert!(!offs.is_empty(), "no copy of the record found");
    for off in offs {
        let v = read_u16(&file, off) | bit;
        file[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }
    std::fs::write(&path, file).unwrap();
}

/// One script: seed, set `bit` in `rec`, reopen, read, write, reopen.
fn probe(rec: Record, bit: u16) -> String {
    let dir = TempDir::new().unwrap();
    let psize = seed(dir.path());
    patch(dir.path(), psize, rec, bit);

    let fork = match Fork::open(dir.path()) {
        Ok(f) => f,
        Err(rc) => return format!("open={}", rc_name(rc)),
    };
    let target = match rec {
        Record::Named => Some(NAME),
        Record::MetaFree | Record::MetaMain => None,
    };
    let txn = fork.txn(0);
    let (rc, dbi, flags) = dbi_open(txn, target, 0);
    if rc != 0 {
        // SAFETY: write txn, aborted exactly once.
        unsafe { ffi::mdb_txn_abort(txn) };
        return format!("open=OK dbi={}", rc_name(rc));
    }
    let got = get(txn, dbi, b"k");
    let wrote = put(txn, dbi, b"k2", b"v2");
    // SAFETY: write txn, committed exactly once.
    let commit = unsafe { ffi::mdb_txn_commit(txn) };
    drop(fork);
    let kept = if live_flags(dir.path(), psize, rec) & bit != 0 {
        "yes"
    } else {
        "no"
    };
    format!(
        "open=OK dbi=OK flags={flags:#x} get={} put={} commit={} kept={kept}",
        // The main database holds the catalog entry, not `k`.
        if target.is_some() {
            rc_name(got)
        } else {
            format!("{}(main)", rc_name(got))
        },
        rc_name(wrote),
        rc_name(commit),
    )
}

/// Set `bit` in the named record while the environment is open and a handle
/// to the database is already registered, then read through that handle in
/// a new read and a new write transaction. The fork re-reads a named
/// record's flags when a transaction first uses a handle opened earlier and
/// compares them with the flags it holds for the handle.
fn probe_open_handle(bit: u16) -> String {
    let dir = TempDir::new().unwrap();
    let psize = seed(dir.path());
    let fork = Fork::open(dir.path()).unwrap();
    let txn = fork.txn(0);
    let (rc, dbi, _) = dbi_open(txn, Some(NAME), 0);
    assert_eq!(rc, 0);
    // SAFETY: write txn, committed exactly once; the dbi stays registered.
    unsafe { assert_eq!(ffi::mdb_txn_commit(txn), 0) };

    // A plain file write: the fork's shared read-only map sees it.
    patch(dir.path(), psize, Record::Named, bit);

    let ro = fork.txn(ffi::MDB_RDONLY);
    let get_ro = get(ro, dbi, b"k");
    // SAFETY: read txn, aborted exactly once.
    unsafe { ffi::mdb_txn_abort(ro) };
    let rw = fork.txn(0);
    let get_rw = get(rw, dbi, b"k");
    // SAFETY: write txn, aborted exactly once.
    unsafe { ffi::mdb_txn_abort(rw) };
    format!("get_ro={} get_rw={}", rc_name(get_ro), rc_name(get_rw))
}

#[test]
fn meta_offsets_hold() {
    let dir = TempDir::new().unwrap();
    let psize = seed(dir.path());
    let file = std::fs::read(data_file(dir.path())).unwrap();
    for m in 0..2 {
        let base = m * psize;
        // `mm_psize` is the free database's `md_pad`; its `md_flags` (the
        // env-flags word) always carries `INTEGERKEY`.
        assert_eq!(read_u32(&file, base + META_DBS) as usize, psize);
        assert_eq!(read_u16(&file, base + META_DBS + DB_FLAGS), MDB_INTEGERKEY);
        assert_eq!(read_u16(&file, base + META_DBS + DB_LEN + DB_FLAGS), 0);
    }
    let offs = flag_offsets(&file, psize, Record::Named);
    assert!(!offs.is_empty());
    for off in offs {
        assert_eq!(read_u16(&file, off), 0);
    }
}

const OBS_UNKNOWN_FLAG_BITS: &[&str] = &[
    "env_flags 0x0100: open=OK dbi=OK flags=0x0 get=NOTFOUND(main) put=OK commit=OK kept=yes",
    "env_flags 0x4000: open=OK dbi=OK flags=0x0 get=NOTFOUND(main) put=OK commit=OK kept=yes",
    "main_record 0x0100: open=OK dbi=OK flags=0x100 get=NOTFOUND(main) put=OK commit=OK kept=yes",
    "main_record 0x4000: open=OK dbi=OK flags=0x4000 get=NOTFOUND(main) put=OK commit=OK kept=yes",
    "named_record 0x0100: open=OK dbi=OK flags=0x100 get=OK put=OK commit=OK kept=yes",
    "named_record 0x4000: open=OK dbi=OK flags=0x4000 get=OK put=OK commit=OK kept=yes",
    "named_record_open_handle 0x0100: get_ro=INCOMPATIBLE get_rw=INCOMPATIBLE",
    "named_record_open_handle 0x4000: get_ro=INCOMPATIBLE get_rw=INCOMPATIBLE",
];

#[test]
fn unknown_flag_bits() {
    let mut rows = Vec::new();
    for (label, rec) in [
        ("env_flags", Record::MetaFree),
        ("main_record", Record::MetaMain),
        ("named_record", Record::Named),
    ] {
        for bit in UNKNOWN_BITS {
            rows.push(format!("{label} {bit:#06x}: {}", probe(rec, bit)));
        }
    }
    for bit in UNKNOWN_BITS {
        rows.push(format!(
            "named_record_open_handle {bit:#06x}: {}",
            probe_open_handle(bit)
        ));
    }
    for r in &rows {
        println!("{r}");
    }
    assert_eq!(rows, OBS_UNKNOWN_FLAG_BITS);
}
