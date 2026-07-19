//! Milestone 2.1 differential: `Env::info()` / `Env::stat()` vs the LMDB fork.
//!
//! **Comparison scope (documented, deliberate).** `MDB_envinfo` and `MDB_stat`
//! mix format-independent facts with format-specific counts. Only the former
//! are compared cross-engine; the latter are pinned to ZeroDB's own truth by
//! `crates/zerodb/tests/env_stat_info.rs` (env-stat == per-DB stat, plus the
//! `check_image` invariant walk). Per field:
//!
//! | field | compared? | why |
//! |---|---|---|
//! | `me_mapaddr` | no | `MDB_FIXEDMAP` only; ZeroDB has no fixed map (always null). |
//! | `me_mapsize` | no | Both echo the configured value; LMDB rounds to the OS page size, ZeroDB does not (D-006 is re-imposed at the adapter, not here). |
//! | `me_last_pgno` | **no** | Same meaning, format-specific value (D-002). Compared as *monotonic growth* only. |
//! | `me_last_txnid` | **yes, exactly** | Commit counter; same domain on both engines. |
//! | `me_maxreaders` | **yes, exactly** | Echoes the configured reader-table size. |
//! | `me_numreaders` | **yes, exactly** | High-water reader-slot count — see D-011: LMDB never decrements it. |
//! | `ms_psize` | no | ZeroDB selects it (M2.6); LMDB derives it from the OS. |
//! | `ms_depth` | bounded | Same meaning; value depends on fan-out (D-002). |
//! | `ms_*_pages` | no | Format-specific by construction (D-002). |
//! | `ms_entries` | **yes, exactly** | Main-DB record count = number of named DBs. |
//!
//! Never change an expectation here to make ZeroDB pass (CLAUDE.md rule 1).

use heed::types::Bytes;
use heed::EnvOpenOptions as LmdbOpts;
use zerodb::EnvOpenOptions as ZOpts;
use zerodb_oracle::tempdir::TempDir;

const MAP: usize = 64 << 20;

// ---------------------------------------------------------------------------
// last_txn_id
// ---------------------------------------------------------------------------

/// Commit `n` single-put transactions and return `last_txn_id` after each,
/// plus the value observed on the fresh (pre-commit) env.
fn txnid_series_lmdb(n: usize) -> (u64, Vec<u64>) {
    let dir = TempDir::new().unwrap();
    let mut o = LmdbOpts::new().read_txn_without_tls();
    o.map_size(MAP);
    o.max_dbs(8);
    // SAFETY: no cross-process flags; private temp dir, single-threaded.
    let env = unsafe { o.open(dir.path()) }.unwrap();
    let fresh = env.info().last_txn_id as u64;
    let mut series = Vec::new();
    for i in 0..n {
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<Bytes, Bytes> = env.create_database(&mut w, Some("d")).unwrap();
        db.put(&mut w, format!("k{i}").as_bytes(), b"v").unwrap();
        w.commit().unwrap();
        series.push(env.info().last_txn_id as u64);
    }
    (fresh, series)
}

fn txnid_series_zerodb(n: usize) -> (u64, Vec<u64>) {
    let dir = TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_dbs(8);
    let env = o.open(dir.path()).unwrap();
    let fresh = env.info().last_txnid;
    let mut series = Vec::new();
    for i in 0..n {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        db.put(&mut w, format!("k{i}").as_bytes(), b"v").unwrap();
        w.commit().unwrap();
        series.push(env.info().last_txnid);
    }
    (fresh, series)
}

#[test]
fn info_last_txnid_matches_exactly() {
    const N: usize = 12;
    let (lfresh, lseries) = txnid_series_lmdb(N);
    let (zfresh, zseries) = txnid_series_zerodb(N);
    assert_eq!(
        lfresh, zfresh,
        "fresh-env last_txn_id: lmdb={lfresh} zerodb={zfresh}"
    );
    assert_eq!(
        lseries, zseries,
        "committed last_txn_id series diverges:\n  lmdb   = {lseries:?}\n  zerodb = {zseries:?}"
    );
}

#[test]
fn info_last_txnid_does_not_advance_on_abort() {
    // Both engines must leave the counter alone when a write txn aborts.
    let ldir = TempDir::new().unwrap();
    let l_before_after = {
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP);
        o.max_dbs(8);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { o.open(ldir.path()) }.unwrap();
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<Bytes, Bytes> = env.create_database(&mut w, Some("d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
        let before = env.info().last_txn_id as u64;
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, b"k2", b"v").unwrap();
        w.abort();
        (before, env.info().last_txn_id as u64)
    };

    let zdir = TempDir::new().unwrap();
    let z_before_after = {
        let mut o = ZOpts::new();
        o.map_size(MAP);
        o.max_dbs(8);
        let env = o.open(zdir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
        let before = env.info().last_txnid;
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        db.put(&mut w, b"k2", b"v").unwrap();
        w.abort();
        (before, env.info().last_txnid)
    };

    assert_eq!(l_before_after.0, l_before_after.1, "lmdb advanced on abort");
    assert_eq!(
        z_before_after.0, z_before_after.1,
        "zerodb advanced on abort"
    );
    assert_eq!(
        l_before_after, z_before_after,
        "abort-txnid behavior diverges: lmdb={l_before_after:?} zerodb={z_before_after:?}"
    );
}

// ---------------------------------------------------------------------------
// max_readers / num_readers
// ---------------------------------------------------------------------------

#[test]
fn info_max_readers_matches_the_configured_value() {
    for configured in [1u32, 8, 126, 1024] {
        let ldir = TempDir::new().unwrap();
        let l = {
            let mut o = LmdbOpts::new().read_txn_without_tls();
            o.map_size(MAP);
            o.max_readers(configured);
            // SAFETY: no cross-process flags; private temp dir, single-threaded.
            let env = unsafe { o.open(ldir.path()) }.unwrap();
            (env.info().maximum_number_of_readers, env.max_readers())
        };
        let zdir = TempDir::new().unwrap();
        let z = {
            let mut o = ZOpts::new();
            o.map_size(MAP);
            o.max_readers(configured);
            let env = o.open(zdir.path()).unwrap();
            (env.info().max_readers, env.info().max_readers)
        };
        assert_eq!(
            l.0, z.0,
            "info().max_readers at configured={configured}: lmdb={} zerodb={}",
            l.0, z.0
        );
        assert_eq!(l.0, configured, "lmdb should echo the configured value");
        assert_eq!(l.1, z.1, "max_readers() getter at configured={configured}");
    }
}

#[test]
fn info_num_readers_matches_for_equivalent_reader_populations() {
    const CAP: u32 = 16;
    // Hold k concurrent read txns on each engine and compare the count.
    let ldir = TempDir::new().unwrap();
    let mut o = LmdbOpts::new().read_txn_without_tls();
    o.map_size(MAP);
    o.max_dbs(8);
    o.max_readers(CAP);
    // SAFETY: no cross-process flags; private temp dir, single-threaded.
    let lenv = unsafe { o.open(ldir.path()) }.unwrap();
    {
        let mut w = lenv.write_txn().unwrap();
        let db: heed::Database<Bytes, Bytes> = lenv.create_database(&mut w, Some("d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }

    let zdir = TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_dbs(8);
    o.max_readers(CAP);
    let zenv = o.open(zdir.path()).unwrap();
    {
        let mut w = zenv.write_txn().unwrap();
        let db = zenv.create_database(&mut w, Some(b"d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }

    assert_eq!(
        lenv.info().number_of_readers,
        zenv.info().num_readers,
        "num_readers with no live readers"
    );

    let mut lheld = Vec::new();
    let mut zheld = Vec::new();
    for k in 1..=CAP {
        lheld.push(lenv.read_txn().unwrap());
        zheld.push(zenv.read_txn().unwrap());
        let l = lenv.info().number_of_readers;
        let z = zenv.info().num_readers;
        assert_eq!(
            l, z,
            "num_readers with {k} live readers: lmdb={l} zerodb={z}"
        );
        assert_eq!(l, k, "lmdb num_readers should be {k}");
    }

    // Releasing readers. **LMDB's `me_numreaders` does not go back down** —
    // it is a high-water mark (`mti_numreaders` is only ever incremented, by
    // `if (i == nr) ti->mti_numreaders = ++nr;`; ending a txn just clears
    // `mr_pid`). This was observed here, not assumed, and ZeroDB reproduces it
    // rather than "fixing" it (CLAUDE.md rule 1). Logged as D-011.
    for remaining in (0..CAP).rev() {
        lheld.pop();
        zheld.pop();
        let l = lenv.info().number_of_readers;
        let z = zenv.info().num_readers;
        assert_eq!(
            l, z,
            "num_readers after releasing down to {remaining} live: lmdb={l} zerodb={z}"
        );
        assert_eq!(
            l, CAP,
            "LMDB's me_numreaders is a high-water mark: expected it to stay at \
             {CAP} with {remaining} readers live, got {l}. If this ever fails, \
             LMDB changed — do NOT relax the assertion, re-derive D-011."
        );
        // The ZeroDB extension is the count that *does* track the live set.
        assert_eq!(
            zenv.info().live_readers,
            remaining,
            "live_readers must follow the actual reader population"
        );
    }
    assert!(lheld.is_empty() && zheld.is_empty());
}

#[test]
fn num_readers_high_water_survives_a_full_drain_and_reuse() {
    // Second, sharper probe of D-011: take 4 slots, release them all, then take
    // 1. LMDB's counter must still read 4 (slots are reused from index 0, and
    // the high-water is never lowered). ZeroDB must agree.
    let ldir = TempDir::new().unwrap();
    let mut o = LmdbOpts::new().read_txn_without_tls();
    o.map_size(MAP);
    o.max_readers(32);
    // SAFETY: no cross-process flags; private temp dir, single-threaded.
    let lenv = unsafe { o.open(ldir.path()) }.unwrap();

    let zdir = TempDir::new().unwrap();
    let mut o = ZOpts::new();
    o.map_size(MAP);
    o.max_readers(32);
    let zenv = o.open(zdir.path()).unwrap();

    {
        let _l: Vec<_> = (0..4).map(|_| lenv.read_txn().unwrap()).collect();
        let _z: Vec<_> = (0..4).map(|_| zenv.read_txn().unwrap()).collect();
    } // all released

    let l1 = lenv.read_txn().unwrap();
    let z1 = zenv.read_txn().unwrap();
    let l = lenv.info().number_of_readers;
    let z = zenv.info().num_readers;
    assert_eq!(l, z, "high-water after drain+reuse: lmdb={l} zerodb={z}");
    assert_eq!(l, 4, "LMDB high-water should still be 4, got {l}");
    assert_eq!(zenv.info().live_readers, 1, "but only one reader is live");
    drop(l1);
    drop(z1);
}

// ---------------------------------------------------------------------------
// env-level stat
// ---------------------------------------------------------------------------

/// `(entries, depth)` of the env-level (main DB) stat after creating `n` named
/// DBs, on each engine.
fn env_stat_for(n: u32) -> ((u64, u32), (u64, u32)) {
    let ldir = TempDir::new().unwrap();
    let l = {
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP);
        o.max_dbs(512);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { o.open(ldir.path()) }.unwrap();
        {
            let mut w = env.write_txn().unwrap();
            for i in 0..n {
                let name = format!("database-number-{i:04}");
                let db: heed::Database<Bytes, Bytes> =
                    env.create_database(&mut w, Some(&name)).unwrap();
                db.put(&mut w, b"k", b"v").unwrap();
            }
            w.commit().unwrap();
        }
        let s = env.stat();
        (s.entries as u64, s.depth)
    };

    let zdir = TempDir::new().unwrap();
    let z = {
        let mut o = ZOpts::new();
        o.map_size(MAP);
        o.max_dbs(512);
        let env = o.open(zdir.path()).unwrap();
        {
            let mut w = env.write_txn().unwrap();
            for i in 0..n {
                let name = format!("database-number-{i:04}");
                let db = env.create_database(&mut w, Some(name.as_bytes())).unwrap();
                db.put(&mut w, b"k", b"v").unwrap();
            }
            w.commit().unwrap();
        }
        let s = env.stat();
        (s.entries, u32::from(s.depth))
    };

    (l, z)
}

#[test]
fn env_stat_entries_and_depth_parity() {
    for n in [0u32, 1, 5, 60, 300] {
        let ((le, ld), (ze, zd)) = env_stat_for(n);
        assert_eq!(
            le, ze,
            "env stat entries mismatch at n={n}: lmdb={le} zerodb={ze}"
        );
        assert_eq!(le, u64::from(n), "lmdb env entries wrong at n={n}");
        if n == 0 {
            assert_eq!(ld, 0, "lmdb empty env-stat depth");
            assert_eq!(zd, 0, "zerodb empty env-stat depth");
        } else {
            // Same meaning, fan-out-dependent value (D-002) — bounded, as in
            // the M1.6 per-DB stat differential.
            assert!(
                ld.abs_diff(zd) <= 1,
                "env-stat depth diverges by >1 at n={n}: lmdb={ld} zerodb={zd}"
            );
        }
    }
}
