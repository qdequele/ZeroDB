//! Abort/poison semantics (SPEC 04 TXN-59, REC-13). The `MapFull`-poisons
//! case lives in `zerodb-core/src/rwtxn.rs`'s unit tests.
//!
//! - An up-front validation error (`BadValSize` on an oversized or empty
//!   key) does NOT poison the txn: LMDB only poisons on internal errors, and
//!   the size checks return before any mutation starts. Every later op on
//!   the same txn, including commit, must still work.
//! - Abort after an overflow-run-heavy mutation leaves the data file
//!   byte-identical.

mod common;
use common::{assert_clean, TempDir};

use std::path::Path;

use zerodb::{Env, EnvOpenOptions, Error, MdbError};

const PS: u32 = 4096;
const MAP: usize = 8 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

#[test]
fn oversized_key_error_does_not_poison_txn() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();

    // A baseline entry, so we can prove the txn is still fully usable after
    // the error (not merely "doesn't panic").
    db.put(&mut wtxn, b"before", b"ok").unwrap();

    // 512 bytes: one past MAX_KEY_SIZE (511, SPEC 01 §S4).
    let oversized = vec![7u8; 512];
    let e = db.put(&mut wtxn, &oversized, b"x").unwrap_err();
    assert!(
        matches!(e, Error::Mdb(MdbError::BadValSize)),
        "expected BadValSize, got {e:?}"
    );

    // The empty key too — same up-front-validated, non-poisoning class.
    let e2 = db.put(&mut wtxn, &[], b"x").unwrap_err();
    assert!(matches!(e2, Error::Mdb(MdbError::BadValSize)));

    // The txn must still be fully usable: not `BadTxn`, every op type works.
    db.put(&mut wtxn, b"after", b"still-works").unwrap();
    assert_eq!(db.get(&wtxn, b"before").unwrap(), Some(b"ok".as_slice()));
    assert!(db.delete(&mut wtxn, b"before").unwrap());
    assert_eq!(db.len(&wtxn).unwrap(), 1);
    // And commit succeeds — the ultimate proof the txn was never poisoned.
    wtxn.commit().unwrap();
    assert_clean(dir.path(), PS);

    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"after").unwrap(),
        Some(b"still-works".as_slice())
    );
    assert_eq!(db.get(&rtxn, b"before").unwrap(), None);
}

#[test]
fn abort_after_overflow_heavy_mutation_leaves_file_byte_identical() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"committed", b"1").unwrap();
        wtxn.commit().unwrap();
    }
    let before = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();

    {
        let mut wtxn = env.write_txn().unwrap();
        // Several multi-page overflow runs: allocate, then delete some of
        // them (freeing the run), replace another with a different-size
        // overflow value (COW + free-old-run), and delete the pre-existing
        // committed entry too — all inside the doomed txn.
        for i in 0..5u32 {
            let k = format!("ovf-{i}").into_bytes();
            db.put(&mut wtxn, &k, &vec![i as u8; 3 * PS as usize + 777])
                .unwrap();
        }
        assert!(db.delete(&mut wtxn, b"ovf-1").unwrap());
        assert!(db.delete(&mut wtxn, b"ovf-3").unwrap());
        db.put(&mut wtxn, b"ovf-0", &vec![0xEEu8; 5 * PS as usize])
            .unwrap();
        db.delete(&mut wtxn, b"committed").unwrap();
        assert!(zerodb::TxnRead::main_record(&wtxn).overflow_pages > 0);
        wtxn.abort();
    }
    let after = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    assert_eq!(
        before, after,
        "abort after overflow-heavy mutation must not change the file (TXN-59)"
    );
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"committed").unwrap(), Some(b"1".as_slice()));
    assert_eq!(db.len(&rtxn).unwrap(), 1);
    for i in 0..5u32 {
        let k = format!("ovf-{i}").into_bytes();
        assert_eq!(db.get(&rtxn, &k).unwrap(), None);
    }
}
