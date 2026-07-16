//! Milestone 1.6 `Database::stat` parity: LMDB fork vs zerodb, driven directly
//! against both real APIs (not the fuzzed `Op` enum).
//!
//! **Stat-parity scope (documented).** `stat` reports `depth`, `branch_pages`,
//! `leaf_pages`, `overflow_pages`, `entries`. Only fields with *format-
//! independent* values are compared cross-engine:
//!
//! * `entries` — the key/value count — is compared **exactly** at every size.
//! * `depth` — the B-tree height — has identical *semantics*, but its *value*
//!   depends on per-page fan-out, which differs by on-disk format (ZeroDB's
//!   node/branch layout vs LMDB's `MDB_db`/packed pgno). It is compared exactly
//!   for the format-independent cases (empty → 0, single leaf → 1) and bounded
//!   to `±1` elsewhere.
//! * Page counts (`branch_pages`/`leaf_pages`/`overflow_pages`) are **not**
//!   compared cross-engine — they are format-specific by construction (D-002).
//!   ZeroDB's page counts are verified against a full tree walk by
//!   `crates/zerodb/tests/named_db.rs` (the `check` invariant walker) instead.

use heed::types::Bytes;
use heed::EnvOpenOptions as LmdbOpts;
use zerodb::EnvOpenOptions as ZOpts;
use zerodb_oracle::tempdir::TempDir;

const MAP: usize = 64 << 20;

/// Insert the same `n` ascending entries into a named DB on both engines and
/// return `(lmdb, zerodb)` `(entries, depth)` for the DB.
fn stats_for(n: u32) -> ((u64, u32), (u64, u32)) {
    let ldir = TempDir::new().unwrap();
    let zdir = TempDir::new().unwrap();

    // ---- LMDB fork ----
    let lenv = {
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP);
        o.max_dbs(8);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        unsafe { o.open(ldir.path()) }.unwrap()
    };
    let l_stat = {
        let mut w = lenv.write_txn().unwrap();
        let db: heed::Database<Bytes, Bytes> = lenv.create_database(&mut w, Some("d")).unwrap();
        for i in 0..n {
            db.put(&mut w, format!("k{i:06}").as_bytes(), b"v").unwrap();
        }
        let s = db.stat(&w).unwrap();
        w.commit().unwrap();
        (s.entries as u64, s.depth)
    };
    let _ = lenv;

    // ---- zerodb ----
    let zenv = {
        let mut o = ZOpts::new();
        o.map_size(MAP);
        o.max_dbs(8);
        o.page_size(4096);
        o.open(zdir.path()).unwrap()
    };
    let z_stat = {
        let mut w = zenv.write_txn().unwrap();
        let db = zenv.create_database(&mut w, Some(b"d")).unwrap();
        for i in 0..n {
            db.put(&mut w, format!("k{i:06}").as_bytes(), b"v").unwrap();
        }
        let s = db.stat(&w).unwrap();
        w.commit().unwrap();
        (s.entries, u32::from(s.depth))
    };

    (l_stat, z_stat)
}

#[test]
fn stat_entries_parity_across_sizes() {
    for n in [0u32, 1, 5, 50, 200, 1000, 4000] {
        let ((le, ld), (ze, zd)) = stats_for(n);
        assert_eq!(le, ze, "entries mismatch at n={n}: lmdb={le} zerodb={ze}");
        assert_eq!(le, u64::from(n), "lmdb entries wrong at n={n}");
        // depth: exact for empty / single leaf; bounded ±1 otherwise.
        if n == 0 {
            assert_eq!(ld, 0, "lmdb empty depth");
            assert_eq!(zd, 0, "zerodb empty depth");
        } else if n <= 5 {
            assert_eq!(ld, 1, "lmdb single-leaf depth at n={n}");
            assert_eq!(zd, 1, "zerodb single-leaf depth at n={n}");
        } else {
            let diff = ld.abs_diff(zd);
            assert!(
                diff <= 1,
                "depth diverges by >1 at n={n}: lmdb={ld} zerodb={zd} (format fan-out)"
            );
        }
    }
}

#[test]
fn stat_entries_match_len() {
    // `stat().entries` must equal `len()` for the same DB (zerodb self-check).
    let zdir = TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_dbs(8);
    o.page_size(4096);
    let env = o.open(zdir.path()).unwrap();
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"d")).unwrap();
    for i in 0..123u32 {
        db.put(&mut w, format!("k{i}").as_bytes(), b"v").unwrap();
    }
    assert_eq!(db.stat(&w).unwrap().entries, db.len(&w).unwrap());
    assert_eq!(db.stat(&w).unwrap().entries, 123);
    w.commit().unwrap();
}
