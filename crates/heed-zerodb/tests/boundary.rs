//! Boundary re-imposition tests: the adapter re-imposes three cheap fork
//! behaviors ZeroDB is lenient about, so the consumer test suites see exact
//! parity — `map_size` must be an OS-page multiple, `max_readers(0)` is
//! refused, and DB names are C strings. Each divergence note in
//! docs/DIVERGENCES.md authorizes the adapter to re-impose the check at the
//! heed boundary.

use heed_zerodb::types::Bytes;
use heed_zerodb::{Database, EnvOpenOptions, Error, WithoutTls};

fn opts() -> EnvOpenOptions<WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

fn os_page() -> usize {
    // SAFETY: pure query.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v > 0 {
        v as usize
    } else {
        4096
    }
}

/// `max_readers(0)` → `Io(InvalidInput)` at open (the fork's EINVAL).
#[test]
fn max_readers_zero_is_invalid_input() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16).max_readers(0);
    match unsafe { o.open(dir.path()) } {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
}

/// `max_readers` control: a nonzero `max_readers` opens fine.
#[test]
fn max_readers_nonzero_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16).max_readers(64);
    assert!(unsafe { o.open(dir.path()) }.is_ok());
}

/// A `map_size` that is not a multiple of the OS page size →
/// `Io(InvalidInput)`.
#[test]
fn map_size_non_page_multiple_is_invalid_input() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    // One byte over an exact multiple: guaranteed non-multiple on every page
    // size (which is always > 1).
    o.map_size(os_page() * 16 + 1);
    match unsafe { o.open(dir.path()) } {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
}

/// `map_size` control: an exact multiple opens fine.
#[test]
fn map_size_page_multiple_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16);
    assert!(unsafe { o.open(dir.path()) }.is_ok());
}

/// A DB name with an embedded NUL reproduces heed's
/// `CString::new(name).unwrap()` panic (the fork's observable behavior).
#[test]
#[should_panic(expected = "NulError")]
fn db_name_embedded_nul_panics_like_the_fork() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16).max_dbs(4);
    let env = unsafe { o.open(dir.path()).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let _db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some("bad\0name")).unwrap();
}

/// DB-name control: a NUL-free name creates fine.
#[test]
fn db_name_without_nul_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16).max_dbs(4);
    let env = unsafe { o.open(dir.path()).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let _db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some("good-name")).unwrap();
    wtxn.commit().unwrap();
}

/// `NO_SUB_DIR` is refused at open (`Io(Unsupported)`) instead of being
/// silently ignored and producing a directory env.
#[test]
fn no_sub_dir_is_refused_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16);
    unsafe { o.flags(heed_zerodb::EnvFlags::NO_SUB_DIR) };
    match unsafe { o.open(dir.path().join("env")) } {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::Unsupported),
        other => panic!("expected Io(Unsupported), got {other:?}"),
    }
    assert!(!dir.path().join("env").exists(), "nothing must be created");
}
