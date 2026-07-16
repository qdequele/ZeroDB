//! Milestone 1.6 named-DB **name edge cases**: observed against the LMDB fork
//! and matched by zerodb (PLAN §1.6 / CLAUDE.md rule 1 — observe, never guess).
//!
//! Driven directly against both real APIs (the fuzzed `DbName` only reaches a
//! bounded name set). Observed LMDB behavior (empirically, this fork):
//!
//! | name              | LMDB `create_database`      | zerodb            |
//! |-------------------|-----------------------------|-------------------|
//! | empty (`""`)      | `BadValSize`                | `BadValSize`      |
//! | 511 bytes         | `Ok`                        | `Ok`              |
//! | 512 bytes         | `BadValSize`                | `BadValSize`      |
//! | embedded `0x00`   | rejected at heed's `CString` boundary (panics `NulError`) | `Ok` (byte-string name) |
//!
//! The embedded-NUL row is a **heed adapter-boundary** difference, not a core
//! one: heed names are C strings, so heed cannot even express a NUL-containing
//! name; zerodb-core names are byte strings. The `heed-zerodb` adapter (M1.13)
//! will impose heed's C-string restriction. No consumer uses NUL in a DB name.

use heed::types::Bytes;
use heed::EnvOpenOptions as LmdbOpts;
use zerodb::{EnvOpenOptions as ZOpts, MdbError};

const MAP: usize = 16 << 20;

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    BadValSize,
    OtherErr(String),
}

fn lmdb_create(name: &str) -> Outcome {
    let dir = zerodb_oracle::tempdir::TempDir::new().unwrap();
    let mut o = LmdbOpts::new().read_txn_without_tls();
    o.map_size(MAP);
    o.max_dbs(8);
    // SAFETY: no cross-process flags; private temp dir, single-threaded.
    let env = unsafe { o.open(dir.path()) }.unwrap();
    let mut w = env.write_txn().unwrap();
    let r = env.create_database::<Bytes, Bytes>(&mut w, Some(name));
    match r {
        Ok(_) => Outcome::Ok,
        Err(heed::Error::Mdb(heed::MdbError::BadValSize)) => Outcome::BadValSize,
        Err(e) => Outcome::OtherErr(format!("{e:?}")),
    }
}

fn zerodb_create(name: &[u8]) -> Outcome {
    let dir = zerodb_oracle::tempdir::TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_dbs(8);
    o.page_size(4096);
    let env = o.open(dir.path()).unwrap();
    let mut w = env.write_txn().unwrap();
    let r = env.create_database(&mut w, Some(name));
    match r {
        Ok(_) => Outcome::Ok,
        Err(zerodb::Error::Mdb(MdbError::BadValSize)) => Outcome::BadValSize,
        Err(e) => Outcome::OtherErr(format!("{e:?}")),
    }
}

#[test]
fn empty_name_is_bad_val_size_on_both() {
    assert_eq!(lmdb_create(""), Outcome::BadValSize);
    assert_eq!(zerodb_create(b""), Outcome::BadValSize);
}

#[test]
fn max_length_name_ok_on_both() {
    let name511 = "a".repeat(511);
    assert_eq!(lmdb_create(&name511), Outcome::Ok);
    assert_eq!(zerodb_create(name511.as_bytes()), Outcome::Ok);
}

#[test]
fn over_length_name_is_bad_val_size_on_both() {
    let name512 = "a".repeat(512);
    assert_eq!(lmdb_create(&name512), Outcome::BadValSize);
    assert_eq!(zerodb_create(name512.as_bytes()), Outcome::BadValSize);
}

#[test]
fn embedded_nul_name_is_a_valid_byte_string_in_zerodb() {
    // heed cannot express this (C-string names); zerodb-core treats names as
    // byte strings, so a NUL-containing name is a distinct, usable DB. Verify
    // it round-trips and is independent of a similar NUL-free name.
    let dir = zerodb_oracle::tempdir::TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_dbs(8);
    o.page_size(4096);
    let env = o.open(dir.path()).unwrap();

    let mut w = env.write_txn().unwrap();
    let a = env.create_database(&mut w, Some(b"ab\0cd")).unwrap();
    let b = env.create_database(&mut w, Some(b"abcd")).unwrap();
    a.put(&mut w, b"k", b"in-a").unwrap();
    b.put(&mut w, b"k", b"in-b").unwrap();
    w.commit().unwrap();

    let r = env.read_txn().unwrap();
    let a = env.open_database(&r, Some(b"ab\0cd")).unwrap().unwrap();
    let b = env.open_database(&r, Some(b"abcd")).unwrap().unwrap();
    assert_eq!(a.get(&r, b"k").unwrap(), Some(b"in-a".as_slice()));
    assert_eq!(b.get(&r, b"k").unwrap(), Some(b"in-b".as_slice()));
}
