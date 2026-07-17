//! heed's own test suite, ported to the adapter at the IN scope defined by
//! ADR-0003 ("heed's own test suite — scoped IN / OUT"). Each test is labeled
//! with its heed origin; only imports were adapted (`heed::` → `heed_zerodb::`).
//!
//! ## IN (ported below)
//! - examples: `all-types`, `clear-database`, `cursor-append`, `multi-env`,
//!   `nested-rtxns` (dependency-free adaptation — see note), `prev-snapshot`.
//! - inline `envs/env.rs`: `close_env` (registry+closing event),
//!   `reopen_env_with_different_options_is_err` (`EnvAlreadyOpened`),
//!   `create_database_without_commit`, `open_already_existing_database`.
//! - inline `txn.rs`: `ro_txns_are_send`.
//! - `mdb/lmdb_error.rs` (partial): the variant-mapping / public-constructibility
//!   assertions (the raw-rc→string assertions are dropped — ZeroDB emits its own
//!   codes, per ADR-0003).
//!
//! ## OUT (excluded, with reason)
//! - `databases/encrypted_database.rs`, examples `custom-comparator`,
//!   `custom-dupsort-comparator`: encryption / custom comparators are WON'T /
//!   Phase 2.4 (SPEC 00 second table).
//! - example `nested.rs` (nested **write** txns): D-003 — nested write txns are
//!   **unrepresentable** in the adapter (there is deliberately no
//!   `Env::nested_write_txn` / `RwTxn::nested`; TXN-40). Replaced by the
//!   `nested_write_txn_is_unrepresentable` compile-time note below.
//! - example `rmp-serde`: needs the `serde-rmp` feature milli does not enable.
//! - inline `env.rs` `resize_database` / `open_database_with_nosubdir` /
//!   `max_key_size` / `open_read_only_without_no_env_opened_before`: exercise
//!   LMDB-specific `resize`/`NO_SUB_DIR`/platform max-key/read-only-open-create
//!   behaviors outside the Phase-1 adapter surface (SPEC 01 / D-001).
//! - inline `txn.rs` `rw_txns_are_send`: heed's `RwTxn` is `Send`; the adapter's
//!   is deliberately `!Send` (its write-mutex guard must not cross threads — no
//!   consumer moves a live write txn). Documented divergence, `txn.rs`.

use std::error::Error;

use heed_zerodb::byteorder::BE;
use heed_zerodb::types::*;
use heed_zerodb::{BoxedError, Database, EnvFlags, EnvOpenOptions, MdbError, PutFlags, WithoutTls};

fn opts() -> EnvOpenOptions<WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

/// heed example: `all-types.rs` (codecs/ops). Serde types use std-native types
/// so no `serde` derive dependency is added (the codecs handle it internally).
#[test]
fn all_types() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3000);
    let env = unsafe { o.open(path)? };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("kiki"))?;
    db.put(&mut wtxn, "hello", &[2, 3][..])?;
    let ret: Option<&[u8]> = db.get(&wtxn, "hello")?;
    assert_eq!(ret, Some(&[2, 3][..]));
    wtxn.commit()?;

    // serde-bincode / serde-json over std-native types.
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, SerdeBincode<(u32, String)>> =
        env.create_database(&mut wtxn, Some("serde-bincode"))?;
    db.put(&mut wtxn, "hello", &(7, "hi".to_string()))?;
    assert_eq!(db.get(&wtxn, "hello")?, Some((7, "hi".to_string())));
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, SerdeJson<Vec<u32>>> =
        env.create_database(&mut wtxn, Some("serde-json"))?;
    db.put(&mut wtxn, "hello", &vec![1u32, 2, 3])?;
    assert_eq!(db.get(&wtxn, "hello")?, Some(vec![1u32, 2, 3]));
    wtxn.commit()?;

    // Unit data + big-endian iteration + range + delete_range.
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Unit> = env.create_database(&mut wtxn, Some("ignored-data"))?;
    db.put(&mut wtxn, "hello", &())?;
    assert_eq!(db.get(&wtxn, "hello")?, Some(()));
    assert_eq!(db.get(&wtxn, "non-existant")?, None);
    // Reopen with the same types is fine.
    let _db: Database<Str, Unit> = env.create_database(&mut wtxn, Some("ignored-data"))?;

    type BEI64 = I64<BE>;
    let db: Database<BEI64, Unit> = env.create_database(&mut wtxn, Some("big-endian-iter"))?;
    for k in [0i64, 68, 35, 42] {
        db.put(&mut wtxn, &k, &())?;
    }
    let rets: Result<Vec<(i64, _)>, _> = db.iter(&wtxn)?.collect();
    assert_eq!(
        rets?.iter().map(|(k, ())| *k).collect::<Vec<_>>(),
        vec![0, 35, 42, 68]
    );

    let range = 35..=42;
    let rets: Vec<(i64, ())> = db.range(&wtxn, &range)?.collect::<Result<_, _>>()?;
    assert_eq!(
        rets.iter().map(|(k, ())| *k).collect::<Vec<_>>(),
        vec![35, 42]
    );
    let deleted: usize = db.delete_range(&mut wtxn, &range)?;
    assert_eq!(deleted, 2);
    let rets: Vec<(i64, ())> = db.iter(&wtxn)?.collect::<Result<_, _>>()?;
    assert_eq!(
        rets.iter().map(|(k, ())| *k).collect::<Vec<_>>(),
        vec![0, 68]
    );
    wtxn.commit()?;
    Ok(())
}

/// heed example: `clear-database.rs` (clear then write in the same txn, M1.6).
#[test]
fn clear_database() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3);
    let env = unsafe { o.open(path)? };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    db.put(&mut wtxn, "I am here", "to test things")?;
    db.put(&mut wtxn, "I am here too", "for the same purpose")?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    db.put(&mut wtxn, "And I come back", "to test things")?;
    let mut iter = db.iter(&wtxn)?;
    assert_eq!(
        iter.next().transpose()?,
        Some(("And I come back", "to test things"))
    );
    assert_eq!(iter.next().transpose()?, None);
    drop(iter);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut iter = db.iter(&rtxn)?;
    assert_eq!(
        iter.next().transpose()?,
        Some(("And I come back", "to test things"))
    );
    assert_eq!(iter.next().transpose()?, None);
    Ok(())
}

/// heed example: `cursor-append.rs` (APPEND via `put_current_with_options`,
/// M1.10) across multiple databases.
#[test]
fn cursor_append() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3);
    let env = unsafe { o.open(path)? };

    let mut wtxn = env.write_txn()?;
    let first: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    let second: Database<Str, Str> = env.create_database(&mut wtxn, Some("second"))?;
    first.put(&mut wtxn, "I am here", "to test things")?;
    first.put(&mut wtxn, "I am here too", "for the same purpose")?;

    let mut iter = second.iter_mut(&mut wtxn)?;
    unsafe { iter.put_current_with_options::<Str>(PutFlags::APPEND, "aaaa", "lol")? };
    unsafe { iter.put_current_with_options::<Str>(PutFlags::APPEND, "abcd", "lol")? };
    unsafe { iter.put_current_with_options::<Str>(PutFlags::APPEND, "bcde", "lol")? };
    drop(iter);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let got: Vec<String> = second
        .iter(&rtxn)?
        .map(|r| r.unwrap().0.to_string())
        .collect();
    assert_eq!(got, vec!["aaaa", "abcd", "bcde"]);
    Ok(())
}

/// heed example: `multi-env.rs` (registry/EnvAlreadyOpened across two envs,
/// M1.13).
#[test]
fn multi_env() -> Result<(), Box<dyn Error>> {
    type BEU32 = U32<BE>;
    let p1 = tempfile::tempdir()?;
    let p2 = tempfile::tempdir()?;
    let (mut o1, mut o2) = (opts(), opts());
    o1.map_size(10 * 1024 * 1024).max_dbs(3000);
    o2.map_size(10 * 1024 * 1024).max_dbs(3000);
    let env1 = unsafe { o1.open(p1)? };
    let env2 = unsafe { o2.open(p2)? };

    let mut wtxn1 = env1.write_txn()?;
    let mut wtxn2 = env2.write_txn()?;
    let db1: Database<Str, Bytes> = env1.create_database(&mut wtxn1, Some("hello"))?;
    let db2: Database<BEU32, BEU32> = env2.create_database(&mut wtxn2, Some("hello"))?;
    db1.clear(&mut wtxn1)?;
    wtxn1.commit()?;
    db2.clear(&mut wtxn2)?;
    wtxn2.commit()?;

    let mut wtxn1 = env1.write_txn()?;
    db1.put(&mut wtxn1, "what", &[4, 5][..])?;
    db1.get(&wtxn1, "what")?;
    wtxn1.commit()?;

    let rtxn2 = env2.read_txn()?;
    assert_eq!(db2.last(&rtxn2)?, None);
    Ok(())
}

/// heed example: `prev-snapshot.rs` (PREV_SNAPSHOT / the meta double-buffer,
/// M1.2). `create_database(None)` = the main DB.
#[test]
fn prev_snapshot() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;

    let open = |flags: EnvFlags| -> Result<_, Box<dyn Error>> {
        let mut o = opts();
        o.map_size(10 * 1024 * 1024).max_dbs(3);
        unsafe { o.flags(flags) };
        Ok(unsafe { o.open(path.path())? })
    };

    let env = open(EnvFlags::empty())?;
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
    db.put(&mut wtxn, "I am here", "to test things")?;
    db.put(&mut wtxn, "I am here too", "for the same purpose")?;
    wtxn.commit()?;
    env.prepare_for_closing().wait();

    // The previous snapshot is empty.
    let env = open(EnvFlags::PREV_SNAPSHOT)?;
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
    assert!(db.is_empty(&wtxn)?);
    wtxn.abort();
    env.prepare_for_closing().wait();

    // Not committing keeps the latest version.
    let env = open(EnvFlags::empty())?;
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
    assert_eq!(db.get(&wtxn, "I am here")?, Some("to test things"));
    db.put(&mut wtxn, "I will fade away", "I am so sad")?;
    wtxn.commit()?;
    env.prepare_for_closing().wait();

    // Previous snapshot again: the last put disappears.
    let env = open(EnvFlags::PREV_SNAPSHOT)?;
    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env.open_database(&rtxn, None)?.unwrap();
    assert_eq!(db.get(&rtxn, "I am here")?, Some("to test things"));
    assert_eq!(db.get(&rtxn, "I will fade away")?, None);
    Ok(())
}

/// heed example: `nested-rtxns.rs` (nested READ txns fanned out to threads —
/// the single most important example, M1.9). Dependency-free adaptation: std
/// threads instead of rayon, plain byte values instead of roaring/rand (no new
/// dev-dependencies), but the same shape — N nested readers over an in-progress
/// write txn, each read on its own thread, seeing uncommitted state.
#[test]
fn nested_rtxns_fanout() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut o = opts();
    // WRITE_MAP (as the example does) + an OS-page-multiple map size.
    unsafe { o.flags(EnvFlags::WRITE_MAP) };
    o.map_size(64 * 1024 * 1024);
    let env = unsafe { o.open(dir.path())? };

    let mut wtxn = env.write_txn()?;
    let db: Database<U32<byteorder::BigEndian>, Bytes> = env.create_database(&mut wtxn, None)?;

    let value_for = |i: u32| -> Vec<u8> { vec![(i % 251) as u8; 300 + (i as usize % 500)] };

    for i in 0..64u32 {
        db.put(&mut wtxn, &i, &value_for(i))?;
    }

    let rtxns = (0..64)
        .map(|_| env.nested_read_txn(&wtxn))
        .collect::<heed_zerodb::Result<Vec<_>>>()?;

    std::thread::scope(|s| {
        for (i, rtxn) in rtxns.into_iter().enumerate() {
            s.spawn(move || {
                let i = i as u32;
                let ret = db.get(&rtxn, &i).unwrap();
                assert_eq!(ret, Some(&value_for(i)[..]));
            });
        }
    });

    wtxn.commit()?;
    Ok(())
}

/// heed inline `envs/env.rs`: `close_env` — a committed env reopens and reads,
/// and `env_closing_event` fires on close.
#[test]
fn close_env() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3);
    let env = unsafe { o.open(dir.path())? };
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("some"))?;
    db.put(&mut wtxn, "hello", "hello")?;
    db.put(&mut wtxn, "world", "world")?;
    wtxn.commit()?;

    let signal = env.prepare_for_closing();
    signal.wait();

    // Reopen after full close and read back.
    let env = unsafe { o.open(dir.path())? };
    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env.open_database(&rtxn, Some("some"))?.unwrap();
    let mut iter = db.iter(&rtxn)?;
    assert_eq!(iter.next().transpose()?, Some(("hello", "hello")));
    assert_eq!(iter.next().transpose()?, Some(("world", "world")));
    assert_eq!(iter.next().transpose()?, None);
    Ok(())
}

/// heed inline `envs/env.rs`: `reopen_env_with_different_options_is_err` — a
/// second open of a still-open path is `EnvAlreadyOpened` (SPEC 04 TXN-51).
#[test]
fn reopen_env_is_env_already_opened() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024);
    let _env = unsafe { o.open(dir.path())? };
    let result = unsafe { o.open(dir.path()) };
    assert!(matches!(result, Err(heed_zerodb::Error::EnvAlreadyOpened)));
    Ok(())
}

/// heed inline `envs/env.rs`: `create_database_without_commit` — a DB created
/// then rolled back (abort) is absent afterwards.
#[test]
fn create_database_without_commit() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3);
    let env = unsafe { o.open(dir.path())? };

    let mut wtxn = env.write_txn()?;
    let _db: Database<Str, Str> = env.create_database(&mut wtxn, Some("some"))?;
    wtxn.abort();

    let rtxn = env.read_txn()?;
    let option: Option<Database<Str, Str>> = env.open_database(&rtxn, Some("some"))?;
    assert!(option.is_none());
    Ok(())
}

/// heed inline `envs/env.rs`: `open_already_existing_database` — a committed DB
/// is openable in a later txn.
#[test]
fn open_already_existing_database() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut o = opts();
    o.map_size(10 * 1024 * 1024).max_dbs(3);
    let env = unsafe { o.open(dir.path())? };
    let mut wtxn = env.write_txn()?;
    let _db: Database<Str, Str> = env.create_database(&mut wtxn, Some("some"))?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let option: Option<Database<Str, Str>> = env.open_database(&rtxn, Some("some"))?;
    assert!(option.is_some());
    Ok(())
}

/// heed inline `txn.rs`: `ro_txns_are_send` (WithoutTls read txns are `Send`).
#[test]
fn ro_txns_are_send() {
    fn is_send<T: Send>() {}
    is_send::<heed_zerodb::RoTxn<WithoutTls>>();
}

/// heed inline `mdb/lmdb_error.rs` (partial IN): the variant identities and
/// `Error::{Encoding, Decoding}` public constructibility from a `BoxedError`
/// (SPEC 04 §8.1 / C5). The raw-rc→string assertions are dropped (ZeroDB emits
/// its own codes, per ADR-0003).
#[test]
fn error_taxonomy_variants_and_constructibility() {
    assert!(MdbError::NotFound.not_found());
    assert!(!MdbError::KeyExist.not_found());
    // Every SPEC-00 MdbError the consumers match must exist and be nameable.
    let _ = [
        MdbError::KeyExist,
        MdbError::NotFound,
        MdbError::MapFull,
        MdbError::Invalid,
        MdbError::BadValSize,
        MdbError::DbsFull,
        MdbError::Incompatible,
        MdbError::ReadersFull,
        MdbError::BadTxn,
    ];
    // Encoding / Decoding are publicly constructible from a BoxedError.
    let be: BoxedError = Box::<dyn std::error::Error + Send + Sync>::from("boom");
    assert!(matches!(
        heed_zerodb::Error::Encoding(be),
        heed_zerodb::Error::Encoding(_)
    ));
    let bd: BoxedError = Box::from("boom");
    assert!(matches!(
        heed_zerodb::Error::Decoding(bd),
        heed_zerodb::Error::Decoding(_)
    ));
}

/// D-003 (replaces heed's `nested.rs` example): nested **write** txns are
/// unrepresentable in the adapter. This is enforced *by absence* — there is no
/// `Env::nested_write_txn` and no `RwTxn::nested` — so a call to them does not
/// compile (TXN-40, stronger than a runtime error). This test documents that
/// the only nesting the adapter offers is the read variant.
#[test]
fn nested_write_txn_is_unrepresentable() {
    // If a `nested_write_txn` API were ever added, this comment (and the ADR)
    // would need revisiting. The adapter exposes only `nested_read_txn`, which
    // is exercised by `nested_rtxns_fanout` above.
}
