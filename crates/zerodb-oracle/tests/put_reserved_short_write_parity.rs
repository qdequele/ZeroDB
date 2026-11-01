//! `put_reserved` when the closure succeeds but leaves part of the reserved
//! space unwritten, run side by side on the LMDB fork (through heed) and on
//! ZeroDB (through `heed-zerodb`).
//!
//! heed 0.22.1 checks `ReservedSpace::remaining()` after the closure and
//! returns `Io(UnexpectedEof)` when bytes are left; the entry itself was
//! already reserved inside the page, so it stays. Pinned per case: the error
//! kind the call returns (if any), and the key's presence and length once the
//! transaction commits. Value bytes are not compared: the unwritten tail is
//! unspecified on LMDB.

use std::io::{ErrorKind, Write as _};

use zerodb_oracle::tempdir::TempDir;

const LEN: usize = 64;

/// What one backend observed: the `Io` error kind of the call, if it failed,
/// and the stored value length after commit.
type Outcome = (Option<ErrorKind>, Option<usize>);

fn fork(written: usize) -> Outcome {
    let dir = TempDir::new().unwrap();
    let mut opts = heed::EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(1 << 20);
    opts.max_dbs(4);
    // SAFETY: no cross-process flags; private temp dir, single-threaded.
    let env = unsafe { opts.open(dir.path()).unwrap() };
    let mut w = env.write_txn().unwrap();
    let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
        env.create_database(&mut w, None).unwrap();
    let r = db.put_reserved(&mut w, b"key", LEN, |sp| sp.write_all(&vec![7u8; written]));
    let kind = match r {
        Ok(()) => None,
        Err(heed::Error::Io(e)) => Some(e.kind()),
        Err(e) => panic!("fork: unexpected error {e:?}"),
    };
    w.commit().unwrap();
    let rt = env.read_txn().unwrap();
    let len = db.get(&rt, b"key").unwrap().map(<[u8]>::len);
    (kind, len)
}

fn adapter(written: usize) -> Outcome {
    let dir = TempDir::new().unwrap();
    let mut opts = heed_zerodb::EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(1 << 20);
    opts.max_dbs(4);
    // SAFETY: as above.
    let env = unsafe { opts.open(dir.path()).unwrap() };
    let mut w = env.write_txn().unwrap();
    let db: heed_zerodb::Database<heed_zerodb::types::Bytes, heed_zerodb::types::Bytes> =
        env.create_database(&mut w, None).unwrap();
    let r = db.put_reserved(&mut w, b"key", LEN, |sp| sp.write_all(&vec![7u8; written]));
    let kind = match r {
        Ok(()) => None,
        Err(heed_zerodb::Error::Io(e)) => Some(e.kind()),
        Err(e) => panic!("adapter: unexpected error {e:?}"),
    };
    w.commit().unwrap();
    let rt = env.read_txn().unwrap();
    let len = db.get(&rt, b"key").unwrap().map(<[u8]>::len);
    (kind, len)
}

fn assert_parity(written: usize) {
    let f = fork(written);
    let a = adapter(written);
    assert_eq!(
        f, a,
        "put_reserved writing {written} of {LEN} bytes: fork {f:?}, adapter {a:?}"
    );
}

#[test]
fn put_reserved_short_write_parity() {
    assert_parity(7);
}

#[test]
fn put_reserved_empty_write_parity() {
    assert_parity(0);
}

#[test]
fn put_reserved_exact_write_parity() {
    assert_parity(LEN);
}
