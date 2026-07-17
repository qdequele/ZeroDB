//! Boundary re-imposition tests (milestone 1.13): the adapter re-imposes three
//! cheap fork behaviors ZeroDB is lenient about, so the 1.14 gate sees exact
//! parity — D-006 (`map_size` OS-page multiple), D-010 (`max_readers(0)`), and
//! D-008 secondary (C-string DB names). Each divergence note authorizes the
//! adapter to re-impose the check at the heed boundary.

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

/// D-010: `max_readers(0)` → `Io(InvalidInput)` at open (the fork's EINVAL).
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

/// D-010 control: a nonzero `max_readers` opens fine.
#[test]
fn max_readers_nonzero_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16).max_readers(64);
    assert!(unsafe { o.open(dir.path()) }.is_ok());
}

/// D-006: a `map_size` that is not a multiple of the OS page size →
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

/// D-006 control: an exact multiple opens fine.
#[test]
fn map_size_page_multiple_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.map_size(os_page() * 16);
    assert!(unsafe { o.open(dir.path()) }.is_ok());
}

/// D-008 secondary: a DB name with an embedded NUL reproduces heed's
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

/// D-008 control: a NUL-free name creates fine.
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
