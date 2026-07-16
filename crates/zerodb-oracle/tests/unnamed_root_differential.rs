//! Milestone 1.6: the **true unnamed/root database** (`create_database(None)`),
//! LMDB fork vs zerodb, driven directly against both real APIs.
//!
//! The fuzzed `DbName::Unnamed` maps to milli's *named* `"main"` DB (so the root
//! is a pure catalog, never read as data — see `op.rs` and DIVERGENCES D-008).
//! The arroy/hannoy pattern is the opposite: they use ONLY the true unnamed/root
//! DB and never create named DBs, so the root holds user data and no catalog
//! entries — no format-specific record bytes are ever surfaced. This test covers
//! that path (the `DbSel::Main` / `meta.main_db` record) at parity.

use heed::types::Bytes;
use heed::EnvOpenOptions as LmdbOpts;
use zerodb::EnvOpenOptions as ZOpts;

const MAP: usize = 64 << 20;

/// An ascending key→value dump of one database.
type Dump = Vec<(Vec<u8>, Vec<u8>)>;

/// Full ascending dump of the unnamed DB on each engine, plus its `len`.
fn dumps(pairs: &[(Vec<u8>, Vec<u8>)], dels: &[Vec<u8>]) -> (Dump, u64) {
    // returns (lmdb_dump, lmdb_len) and asserts zerodb equals it inline.
    let ldir = zerodb_oracle::tempdir::TempDir::new().unwrap();
    let zdir = zerodb_oracle::tempdir::TempDir::new().unwrap();

    // ---- LMDB ----
    let (l_dump, l_len) = {
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP);
        // no max_dbs: only the unnamed DB (arroy/hannoy)
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { o.open(ldir.path()) }.unwrap();
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
        for (k, v) in pairs {
            db.put(&mut w, k, v).unwrap();
        }
        for k in dels {
            db.delete(&mut w, k).unwrap();
        }
        let dump: Vec<_> = db
            .iter(&w)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        let len = db.len(&w).unwrap();
        w.commit().unwrap();
        (dump, len)
    };

    // ---- zerodb ----
    let (z_dump, z_len) = {
        let mut o = ZOpts::new();
        o.map_size(MAP);
        o.page_size(4096);
        let env = o.open(zdir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.main_database();
        for (k, v) in pairs {
            db.put(&mut w, k, v).unwrap();
        }
        for k in dels {
            db.delete(&mut w, k).unwrap();
        }
        let dump: Vec<_> = db
            .iter(&w)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        let len = db.len(&w).unwrap();
        w.commit().unwrap();
        (dump, len)
    };

    assert_eq!(l_dump, z_dump, "unnamed-DB dump mismatch");
    assert_eq!(l_len, z_len, "unnamed-DB len mismatch");
    (l_dump, l_len)
}

#[test]
fn unnamed_db_put_del_iter_parity() {
    let mut pairs = Vec::new();
    for i in 0..300u32 {
        pairs.push((
            format!("key{i:05}").into_bytes(),
            format!("value-{i}").into_bytes(),
        ));
    }
    // A couple of overflow values.
    pairs.push((b"big1".to_vec(), vec![0xAAu8; 9000]));
    pairs.push((b"big2".to_vec(), vec![0xBBu8; 20000]));
    let dels: Vec<Vec<u8>> = (0..300u32)
        .filter(|i| i % 7 == 0)
        .map(|i| format!("key{i:05}").into_bytes())
        .collect();
    let (_dump, len) = dumps(&pairs, &dels);
    // 300 keys - floor(300/7 -> 0,7,..294 => 43) + 2 big = 300 - 43 + 2 = 259
    assert_eq!(len, 259);
}

#[test]
fn unnamed_db_empty_parity() {
    let (dump, len) = dumps(&[], &[]);
    assert!(dump.is_empty());
    assert_eq!(len, 0);
}
