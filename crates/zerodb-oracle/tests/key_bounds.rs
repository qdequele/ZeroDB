//! SPEC 01 §S4 — the fork's key-size bounds, observed directly through heed.
//!
//! Observed with heed 0.22.1 / lmdb-master-sys 0.2.6 (fork
//! `mdb.master.nested-rtxns`):
//!   * `Env::max_key_size()` == 511, identical for map sizes 1 MiB and 64 MiB.
//!   * key len 1..=511  -> Ok
//!   * key len 0        -> Err(Mdb(BadValSize))  (empty keys rejected)
//!   * key len 512+     -> Err(Mdb(BadValSize))
//!
//! heed 0.22 does not expose page-size selection, and in this fork build
//! `MDB_MAXKEYSIZE` is the compile-time constant 511, so the bound cannot vary
//! with page size; the reachable invariant is that it is constant across map
//! sizes.

use heed::types::Bytes;
use heed::{Database, EnvOpenOptions, MdbError};
use zerodb_oracle::tempdir::TempDir;

const MAP_SIZES: [usize; 2] = [1 << 20, 64 << 20];

fn open(map_size: usize, dir: &std::path::Path) -> heed::Env<heed::WithoutTls> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(map_size);
    opts.max_dbs(4);
    unsafe { opts.open(dir) }.expect("open env")
}

#[test]
fn max_key_size_is_511_and_map_size_independent() {
    for map_size in MAP_SIZES {
        let dir = TempDir::new().unwrap();
        let env = open(map_size, dir.path());
        assert_eq!(
            env.max_key_size(),
            511,
            "fork reports a constant 511-byte max key (map_size={map_size})"
        );
    }
}

#[test]
fn key_size_511_boundary_and_empty_key_rejection() {
    for map_size in MAP_SIZES {
        let dir = TempDir::new().unwrap();
        let env = open(map_size, dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();

        // Empty key -> BadValSize (mv_size - 1 underflows in `_mdb_cursor_put`).
        assert_bad_val_size(
            db.put(&mut wtxn, b"".as_slice(), b"v".as_slice()),
            map_size,
            0,
        );

        // 1..=511 accepted.
        for len in [1usize, 255, 510, 511] {
            let key = vec![b'k'; len];
            db.put(&mut wtxn, key.as_slice(), b"v".as_slice())
                .unwrap_or_else(|e| {
                    panic!("len {len} should be Ok, got {e:?} (map_size={map_size})")
                });
        }

        // 512+ rejected with BadValSize.
        for len in [512usize, 513, 1024, 65535] {
            let key = vec![b'k'; len];
            assert_bad_val_size(
                db.put(&mut wtxn, key.as_slice(), b"v".as_slice()),
                map_size,
                len,
            );
        }

        wtxn.abort();
    }
}

fn assert_bad_val_size(res: heed::Result<()>, map_size: usize, len: usize) {
    match res {
        Err(heed::Error::Mdb(MdbError::BadValSize)) => {}
        other => panic!(
            "key len {len} (map_size={map_size}): expected Err(Mdb(BadValSize)), got {other:?}"
        ),
    }
}
