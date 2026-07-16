//! M1.8 differential: reader-slot exhaustion (`MDB_READERS_FULL`, SPEC 04
//! TXN-16) — the LMDB fork vs zerodb, **directly through both engines' env
//! APIs**, not through the op-model harness.
//!
//! Why direct: the oracle's `Op` state machine models **one active txn at a
//! time** (M0.3 harness note), so "hold `max_readers` concurrent read txns,
//! then open one more" is inexpressible as an op sequence. This test opens
//! both engines side by side with the same `max_readers` and compares the
//! observable behavior at and past the slot limit. (Noted per the M1.8
//! handback requirement.)
//!
//! LMDB side: `MDB_NOTLS` (heed `read_txn_without_tls`, the only mode
//! Meilisearch uses) lets one thread hold many read txns, each consuming a
//! reader-table slot — same as zerodb's WithoutTls-only model.

use heed::types::Bytes;
use zerodb_oracle::tempdir::TempDir;

const MAP: usize = 1 << 20;
const MAX_READERS: u32 = 3;

/// Both engines: exactly `max_readers` concurrent read txns succeed; the next
/// one fails with the engines' respective `ReadersFull`; releasing one slot
/// makes the next open succeed again.
#[test]
fn readers_full_boundary_matches_lmdb() {
    // ---- LMDB (the fork, through heed) ----
    let lmdb_dir = TempDir::new().expect("tempdir");
    let lenv = {
        let mut opts = heed::EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(MAP);
        opts.max_readers(MAX_READERS);
        // SAFETY: `open` is `unsafe` only because LMDB env flags can enable
        // cross-process behaviors; we pass none, and the path is a private
        // temp dir used only by this test.
        unsafe { opts.open(lmdb_dir.path()) }.expect("open lmdb env")
    };
    // Seed one key so post-recovery reads have something to look at.
    {
        let mut wtxn = lenv.write_txn().expect("lmdb wtxn");
        let db: heed::Database<Bytes, Bytes> = lenv
            .create_database(&mut wtxn, None)
            .expect("lmdb create db");
        db.put(&mut wtxn, &b"k"[..], &b"v"[..]).expect("lmdb put");
        wtxn.commit().expect("lmdb commit");
    }
    let mut lmdb_held = Vec::new();
    for i in 0..MAX_READERS {
        lmdb_held.push(
            lenv.read_txn()
                .unwrap_or_else(|e| panic!("lmdb reader {i} within max_readers failed: {e}")),
        );
    }
    let lmdb_is_readers_full = match lenv.read_txn() {
        Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => true,
        Err(other) => panic!("oracle: LMDB max_readers+1 errored, but not ReadersFull: {other:?}"),
        Ok(_) => false,
    };
    assert!(
        lmdb_is_readers_full,
        "oracle: LMDB max_readers+1 unexpectedly succeeded"
    );
    drop(lmdb_held.pop());
    let lmdb_recovered = lenv.read_txn().is_ok();
    assert!(lmdb_recovered, "oracle: LMDB recovers after one release");

    // ---- zerodb ----
    let z_dir = TempDir::new().expect("tempdir");
    let zenv = {
        let mut opts = zerodb::EnvOpenOptions::new();
        opts.map_size(MAP);
        opts.max_readers(MAX_READERS);
        opts.open(z_dir.path()).expect("open zerodb env")
    };
    {
        let mut wtxn = zenv.write_txn().expect("zerodb wtxn");
        let db = zenv.main_database();
        db.put(&mut wtxn, b"k", b"v").expect("zerodb put");
        wtxn.commit().expect("zerodb commit");
    }
    let mut z_held = Vec::new();
    for i in 0..MAX_READERS {
        z_held.push(
            zenv.read_txn()
                .unwrap_or_else(|e| panic!("zerodb reader {i} within max_readers failed: {e}")),
        );
    }
    let z_is_readers_full = match zenv.read_txn() {
        Err(zerodb::Error::Mdb(zerodb::MdbError::ReadersFull)) => true,
        Err(other) => panic!("zerodb max_readers+1 errored, but not ReadersFull: {other:?}"),
        Ok(_) => false,
    };
    assert!(
        z_is_readers_full,
        "zerodb max_readers+1 unexpectedly succeeded"
    );
    drop(z_held.pop());
    let z_recovered = zenv.read_txn().is_ok();

    // ---- differential verdict ----
    assert_eq!(
        (lmdb_is_readers_full, lmdb_recovered),
        (z_is_readers_full, z_recovered),
        "ReadersFull boundary behavior diverges from the fork"
    );
}

/// Held readers keep serving their snapshot at the boundary in both engines
/// (exhaustion affects new opens only).
#[test]
fn held_readers_unaffected_by_exhaustion() {
    // LMDB.
    let lmdb_dir = TempDir::new().expect("tempdir");
    let lenv = {
        let mut opts = heed::EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(MAP);
        opts.max_readers(2);
        // SAFETY: no cross-process flags; private temp dir (see above).
        unsafe { opts.open(lmdb_dir.path()) }.expect("open lmdb env")
    };
    let ldb: heed::Database<Bytes, Bytes> = {
        let mut wtxn = lenv.write_txn().expect("wtxn");
        let db = lenv.create_database(&mut wtxn, None).expect("create");
        db.put(&mut wtxn, &b"k"[..], &b"v"[..]).expect("put");
        wtxn.commit().expect("commit");
        db
    };
    let l1 = lenv.read_txn().expect("r1");
    let l2 = lenv.read_txn().expect("r2");
    assert!(lenv.read_txn().is_err(), "table of 2 exhausted");
    let lv1 = ldb.get(&l1, &b"k"[..]).expect("get").map(<[u8]>::to_vec);
    let lv2 = ldb.get(&l2, &b"k"[..]).expect("get").map(<[u8]>::to_vec);

    // zerodb.
    let z_dir = TempDir::new().expect("tempdir");
    let zenv = {
        let mut opts = zerodb::EnvOpenOptions::new();
        opts.map_size(MAP);
        opts.max_readers(2);
        opts.open(z_dir.path()).expect("open zerodb env")
    };
    let zdb = zenv.main_database();
    {
        let mut wtxn = zenv.write_txn().expect("wtxn");
        zdb.put(&mut wtxn, b"k", b"v").expect("put");
        wtxn.commit().expect("commit");
    }
    let z1 = zenv.read_txn().expect("r1");
    let z2 = zenv.read_txn().expect("r2");
    assert!(zenv.read_txn().is_err(), "table of 2 exhausted");
    let zv1 = zdb.get(&z1, b"k").expect("get").map(<[u8]>::to_vec);
    let zv2 = zdb.get(&z2, b"k").expect("get").map(<[u8]>::to_vec);

    assert_eq!((lv1, lv2), (zv1, zv2), "held-reader reads diverge");
}
