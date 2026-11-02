//! Duplicate-key (DUPSORT) spike: the consumer paths a duplicate-aware entry
//! walk can break silently, compared against the LMDB fork.
//!
//! Each test drives a DUPSORT database through a path that never looks at
//! duplicates itself — the compacting copy, and heed's `open` with a custom
//! duplicate comparator after an environment reopen — and compares the
//! stored `(key, value)` pairs with what LMDB stores for the same operations.
//! The `dump`/`load` round trip is in `zerodb-tools/tests/dup_dump_load.rs`.
//!
//! Only built with the `dupsort-spike` feature: duplicate keys are not
//! supported by default (docs/LMDB-COMPATIBILITY.md), and the spike stores
//! only sets that fit inside one leaf cell.

#![cfg(feature = "dupsort-spike")]

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use heed::types::Bytes;
use heed::{DatabaseFlags, EnvOpenOptions as LEnvOpenOptions};
use zerodb::{
    collect_pairs, CompactionOption as ZCompaction, CopyToFile, EnvFlags as ZEnvFlags,
    EnvOpenOptions as ZEnvOpenOptions, DATA_FILE_NAME, DB_DUPSORT, HEED_DATA_FILE_NAME,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-dupspike-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

const MAP_SIZE: usize = 64 << 20;

/// 120 keys holding 1 to 5 duplicates each, inserted in descending value
/// order so both engines must sort them. Every set fits one leaf cell.
fn dup_dataset() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    for i in 0u32..120 {
        let key = format!("key{i:04}").into_bytes();
        for j in (0..=(i % 5)).rev() {
            out.push((key.clone(), format!("dup{j:02}-{i}").into_bytes()));
        }
    }
    // A repeated pair is a no-op in a DUPSORT database (SPEC 03 §12.1 O5).
    out.push((b"key0003".to_vec(), b"dup01-3".to_vec()));
    out
}

fn plain_dataset() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0u32..50)
        .map(|i| (i.to_be_bytes().to_vec(), vec![b'p'; 10 + i as usize]))
        .collect()
}

fn heed_pairs(db: heed::Database<Bytes, Bytes>, rtxn: &heed::RoTxn) -> Pairs {
    db.iter(rtxn)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Compacting copy
// ---------------------------------------------------------------------------

/// LMDB: build, copy with compaction, reopen the copy, read both DBs.
fn lmdb_compacted(tag: &str) -> (Pairs, Pairs) {
    let src = tmp(&format!("{tag}-lsrc"));
    let copy = tmp(&format!("{tag}-lcopy")).join("copy.mdb");
    {
        let mut opts = LEnvOpenOptions::new();
        opts.max_dbs(8).map_size(MAP_SIZE);
        // SAFETY: fresh private temp env, single-threaded.
        let env = unsafe { opts.open(&src).unwrap() };
        let mut wtxn = env.write_txn().unwrap();
        let dups: heed::Database<Bytes, Bytes> = env
            .database_options()
            .types()
            .flags(DatabaseFlags::DUP_SORT)
            .name("dups")
            .create(&mut wtxn)
            .unwrap();
        let plain: heed::Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some("plain")).unwrap();
        for (k, v) in dup_dataset() {
            dups.put(&mut wtxn, &k, &v).unwrap();
        }
        for (k, v) in plain_dataset() {
            plain.put(&mut wtxn, &k, &v).unwrap();
        }
        wtxn.commit().unwrap();
        let mut f = std::fs::File::create(&copy).unwrap();
        env.copy_to_file(&mut f, heed::CompactionOption::Enabled)
            .unwrap();
        f.sync_all().unwrap();
    }
    let mut opts = LEnvOpenOptions::new();
    opts.max_dbs(8).map_size(MAP_SIZE);
    // SAFETY: NO_SUB_DIR opens the single copied file; private, single-threaded.
    let env = unsafe {
        opts.flags(heed::EnvFlags::NO_SUB_DIR);
        opts.open(&copy).unwrap()
    };
    let rtxn = env.read_txn().unwrap();
    let dups = env.open_database(&rtxn, Some("dups")).unwrap().unwrap();
    let plain = env.open_database(&rtxn, Some("plain")).unwrap().unwrap();
    (heed_pairs(dups, &rtxn), heed_pairs(plain, &rtxn))
}

#[test]
fn compacting_copy_keeps_duplicates() {
    let (lmdb_dups, lmdb_plain) = lmdb_compacted("copy");
    assert_eq!(
        lmdb_dups.len(),
        dup_dataset().len() - 1,
        "fork stores pairs"
    );

    let src = tmp("copy-zsrc");
    let copy = tmp("copy-zcopy").join("copy.dat");
    let psize;
    {
        let env = ZEnvOpenOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(8)
            .open(&src)
            .unwrap();
        psize = env.page_size();
        let mut wtxn = env.write_txn().unwrap();
        let dups = env
            .create_database_with_flags(&mut wtxn, Some(b"dups"), DB_DUPSORT)
            .unwrap();
        let plain = env.create_database(&mut wtxn, Some(b"plain")).unwrap();
        for (k, v) in dup_dataset() {
            dups.put(&mut wtxn, &k, &v).unwrap();
        }
        for (k, v) in plain_dataset() {
            plain.put(&mut wtxn, &k, &v).unwrap();
        }
        wtxn.commit().unwrap();
        env.copy_to_file(&copy, ZCompaction::Enabled).unwrap();
    }

    // The copy must be structurally valid, not just return Ok.
    let image = std::fs::read(&copy).unwrap();
    let violations = zerodb::check::check_image(&image, psize);
    assert!(
        violations.is_empty(),
        "compacted copy fails check: {violations:?}"
    );

    let dir = tmp("copy-zread");
    std::fs::copy(&copy, dir.join(DATA_FILE_NAME)).unwrap();
    let env = ZEnvOpenOptions::new()
        .map_size(MAP_SIZE)
        .max_dbs(8)
        .open(&dir)
        .unwrap();
    {
        let rtxn = env.read_txn().unwrap();
        let dups = env.open_database(&rtxn, Some(b"dups")).unwrap().unwrap();
        let plain = env.open_database(&rtxn, Some(b"plain")).unwrap().unwrap();
        assert_eq!(collect_pairs(&dups, &rtxn).unwrap(), lmdb_dups);
        assert_eq!(collect_pairs(&plain, &rtxn).unwrap(), lmdb_plain);
        assert_eq!(dups.len(&rtxn).unwrap(), lmdb_dups.len() as u64);
    }

    // The copy kept the database's DUPSORT flag: a new value for an existing
    // key adds a pair instead of replacing the value.
    let mut wtxn = env.write_txn().unwrap();
    let dups = env.open_database(&wtxn, Some(b"dups")).unwrap().unwrap();
    dups.put(&mut wtxn, b"key0000", b"dup99-0").unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(dups.len(&rtxn).unwrap(), lmdb_dups.len() as u64 + 1);
}

// ---------------------------------------------------------------------------
// heed `open` with a custom duplicate comparator, across an env reopen
// ---------------------------------------------------------------------------

/// Duplicates in descending byte order.
enum DescendingDup {}

impl heed::Comparator for DescendingDup {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

/// The same scenario through both heed crates: create with the comparator,
/// close the env, reopen it, `open` the database with the same options, add
/// more duplicates, read everything back.
macro_rules! custom_dup_scenario {
    ($heed:ident, $dir:expr) => {{
        use $heed::types::Bytes;
        type Db = $heed::Database<Bytes, Bytes, $heed::DefaultComparator, DescendingDup>;
        let dir: &Path = $dir;
        let open_env = || {
            let mut opts = $heed::EnvOpenOptions::new();
            opts.max_dbs(8).map_size(MAP_SIZE);
            // SAFETY: private temp env, single-threaded, opened once at a time.
            unsafe { opts.open(dir) }.unwrap()
        };
        {
            let env = open_env();
            let mut wtxn = env.write_txn().unwrap();
            let db: Db = env
                .database_options()
                .types::<Bytes, Bytes>()
                .dup_sort_comparator::<DescendingDup>()
                .flags($heed::DatabaseFlags::DUP_SORT)
                .name("d")
                .create(&mut wtxn)
                .unwrap();
            for v in [&b"b"[..], b"a", b"c"] {
                db.put(&mut wtxn, b"k1", v).unwrap();
            }
            wtxn.commit().unwrap();
        }
        let env = open_env();
        // Opened inside the write txn, so the handle outlives it (a handle
        // opened in a read txn that is not committed is not kept by LMDB).
        let mut wtxn = env.write_txn().unwrap();
        let db: Option<Db> = env
            .database_options()
            .types::<Bytes, Bytes>()
            .dup_sort_comparator::<DescendingDup>()
            .flags($heed::DatabaseFlags::DUP_SORT)
            .name("d")
            .open(&wtxn)
            .unwrap();
        let db = db.expect("reopened database");
        for (k, v) in [(&b"k1"[..], &b"d"[..]), (b"k2", b"x"), (b"k2", b"y")] {
            db.put(&mut wtxn, k, v).unwrap();
        }
        wtxn.commit().unwrap();
        let rtxn = env.read_txn().unwrap();
        let first = db.get(&rtxn, b"k1").unwrap().map(<[u8]>::to_vec);
        let len = db.len(&rtxn).unwrap();
        (first, len)
    }};
}

#[test]
fn custom_dup_comparator_reopens() {
    let ldir = tmp("cmp-lmdb");
    let (lfirst, llen) = custom_dup_scenario!(heed, &ldir);
    let lpairs = {
        let mut opts = LEnvOpenOptions::new();
        opts.max_dbs(8).map_size(MAP_SIZE);
        // SAFETY: private temp env, single-threaded.
        let env = unsafe { opts.open(&ldir) }.unwrap();
        let rtxn = env.read_txn().unwrap();
        let db = env.open_database(&rtxn, Some("d")).unwrap().unwrap();
        heed_pairs(db, &rtxn)
    };
    assert_eq!(
        lpairs,
        [
            (b"k1".to_vec(), b"d".to_vec()),
            (b"k1".to_vec(), b"c".to_vec()),
            (b"k1".to_vec(), b"b".to_vec()),
            (b"k1".to_vec(), b"a".to_vec()),
            (b"k2".to_vec(), b"y".to_vec()),
            (b"k2".to_vec(), b"x".to_vec()),
        ],
        "fork: duplicates in comparator order"
    );

    let zdir = tmp("cmp-zerodb");
    let (zfirst, zlen) = custom_dup_scenario!(heed_zerodb, &zdir);
    assert_eq!((zfirst, zlen), (lfirst, llen));
    // The physical order on disk is the comparator's: read it back natively.
    let env = ZEnvOpenOptions::new()
        .map_size(MAP_SIZE)
        .max_dbs(8)
        .data_file_name(HEED_DATA_FILE_NAME)
        .flags(ZEnvFlags::READ_ONLY)
        .open(&zdir)
        .unwrap();
    let rtxn = env.read_txn().unwrap();
    let db = env.open_database(&rtxn, Some(b"d")).unwrap().unwrap();
    assert_eq!(collect_pairs(&db, &rtxn).unwrap(), lpairs);
}
