//! File-level regression tests for the first-release security review
//! (2026-09): a corrupt or hostile **data file** yields a typed error —
//! never a SIGBUS, panic, or silent write through an attacker-planted path —
//! and env files are created owner-only. Do not weaken (CLAUDE.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{CompactionOption, CopyToFile, Env, EnvOpenOptions, Error, MdbError, DATA_FILE_NAME};
use zerodb_core::page::crc32c;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "zerodb-hostile-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ));
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

const PS: u32 = 4096;
const MAP: u64 = 1 << 20;

fn open_env(dir: &Path) -> zerodb::Result<Env> {
    EnvOpenOptions::new().map_size(MAP as usize).open(dir)
}

/// Create a small env with one committed entry, then close it.
fn seed_env(dir: &Path) {
    let env = open_env(dir).unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    db.put(&mut txn, b"key", b"value").unwrap();
    txn.commit().unwrap();
}

/// Meta layout constants (SPEC 02 §3): field offsets inside a meta slot.
const OFF_LAST_PG: usize = 56;
const OFF_MAIN_DB: usize = 120;
const DB_OFF_ROOT: usize = 0;
const OFF_META_CRC: usize = 168;

/// Patch both meta slots of the data file with `edit(slot_bytes)` and
/// recompute the CRC (over `[0, 168)`) with the crate's own function, so the
/// slots stay CRC-valid — the geometry itself is the hostile part.
fn patch_metas(dir: &Path, edit: impl Fn(&mut [u8])) {
    let data = dir.join(DATA_FILE_NAME);
    let mut bytes = std::fs::read(&data).unwrap();
    let ps = PS as usize;
    for slot in 0..2usize {
        let s = &mut bytes[slot * ps..(slot + 1) * ps];
        edit(s);
        let crc = crc32c(&s[..OFF_META_CRC]);
        s[OFF_META_CRC..OFF_META_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    }
    std::fs::write(&data, bytes).unwrap();
}

// ---------------------------------------------------------------------------
// H1 — a valid-CRC meta naming geometry past EOF must fail open, not SIGBUS
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_valid_crc_meta_pointing_past_eof() {
    let dir = TempDir::new("h1-root");
    seed_env(dir.path());
    // Point the main root (and the high-water) at page 200: inside the 1 MiB
    // map, far past the real few-KiB file. Pre-fix this opened fine and the
    // first `get` dereferenced unbacked map bytes -> SIGBUS.
    patch_metas(dir.path(), |s| {
        s[OFF_LAST_PG..OFF_LAST_PG + 8].copy_from_slice(&200u64.to_le_bytes());
        s[OFF_MAIN_DB + DB_OFF_ROOT..OFF_MAIN_DB + DB_OFF_ROOT + 8]
            .copy_from_slice(&200u64.to_le_bytes());
    });
    let e = open_env(dir.path()).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn open_rejects_root_beyond_high_water_on_disk() {
    let dir = TempDir::new("h1-root2");
    seed_env(dir.path());
    // Keep last_pg honest, point the root past it.
    patch_metas(dir.path(), |s| {
        s[OFF_MAIN_DB + DB_OFF_ROOT..OFF_MAIN_DB + DB_OFF_ROOT + 8]
            .copy_from_slice(&150u64.to_le_bytes());
    });
    let e = open_env(dir.path()).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

// ---------------------------------------------------------------------------
// M2 — env files are created owner-only (0600)
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn data_file_is_created_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("mode");
    seed_env(dir.path());
    let md = std::fs::metadata(dir.path().join(DATA_FILE_NAME)).unwrap();
    assert_eq!(
        md.permissions().mode() & 0o777,
        0o600,
        "data file must be owner-only like LMDB's"
    );
}

// ---------------------------------------------------------------------------
// M3 — creation does not write through a planted symlink
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn env_creation_refuses_planted_symlink() {
    let dir = TempDir::new("symlink");
    let victim = dir.path().join("victim");
    std::fs::write(&victim, b"precious").unwrap();
    std::os::unix::fs::symlink(&victim, dir.path().join(DATA_FILE_NAME)).unwrap();
    // Pre-fix: create(true).truncate(true) followed the symlink and clobbered
    // the victim with a fresh env image.
    let r = open_env(dir.path());
    assert!(r.is_err(), "open through a planted symlink must fail");
    assert_eq!(
        std::fs::read(&victim).unwrap(),
        b"precious",
        "the symlink target must be untouched"
    );
}

#[cfg(unix)]
#[test]
fn copy_staging_does_not_write_through_planted_symlink() {
    let dir = TempDir::new("copy-src");
    seed_env(dir.path());
    let out = TempDir::new("copy-out");
    let dest = out.path().join("copy.zdb");
    // Plant a symlink at the previously predictable staging name
    // `<dest>.copy-tmp-<pid>`. The staging path now carries a random nonce
    // and is opened with create_new, so the plant must be a no-op.
    let victim = out.path().join("victim");
    std::fs::write(&victim, b"precious").unwrap();
    let planted = out
        .path()
        .join(format!("copy.zdb.copy-tmp-{}", std::process::id()));
    std::os::unix::fs::symlink(&victim, &planted).unwrap();

    let env = open_env(dir.path()).unwrap();
    env.copy_to_file(&dest, CompactionOption::Enabled).unwrap();

    assert_eq!(
        std::fs::read(&victim).unwrap(),
        b"precious",
        "the planted symlink target must be untouched"
    );
    assert!(dest.exists(), "the copy itself must have landed");
    // The staged copy is owner-only too (M2).
    use std::os::unix::fs::PermissionsExt;
    let md = std::fs::metadata(&dest).unwrap();
    assert_eq!(md.permissions().mode() & 0o777, 0o600);
}

// ---------------------------------------------------------------------------
// M6 — unbounded max_readers / max_dbs fail fast at open
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_unbounded_reader_and_db_counts() {
    let dir = TempDir::new("limits");
    let e = EnvOpenOptions::new()
        .map_size(MAP as usize)
        .max_readers(u32::MAX)
        .open(dir.path())
        .unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    let e = EnvOpenOptions::new()
        .map_size(MAP as usize)
        .max_dbs(u32::MAX)
        .open(dir.path())
        .unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M1 — RwTxn is genuinely Send: the writer lock releases from any thread
// ---------------------------------------------------------------------------

#[test]
fn rwtxn_is_send_and_droppable_on_another_thread() {
    // Compile-time: the writer guard must not be a std MutexGuard (!Send).
    fn assert_send<T: Send>() {}
    assert_send::<zerodb::RwTxn<'static>>();

    let dir = TempDir::new("send");
    let env = open_env(dir.path()).unwrap();

    // Abort-by-drop on a foreign thread (pre-fix: unlocking a std Mutex from
    // a thread that did not lock it — UB per std, an abort on macOS).
    let txn = env.write_txn().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || drop(txn));
    });

    // Commit on a foreign thread; the writer slot must be released so a
    // subsequent writer on this thread can proceed.
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    db.put(&mut txn, b"a", b"1").unwrap();
    std::thread::scope(|s| {
        s.spawn(move || txn.commit().unwrap());
    });
    let mut txn = env.write_txn().unwrap();
    db.put(&mut txn, b"b", b"2").unwrap();
    txn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"a").unwrap(), Some(&b"1"[..]));
    assert_eq!(db.get(&rtxn, b"b").unwrap(), Some(&b"2"[..]));
}
