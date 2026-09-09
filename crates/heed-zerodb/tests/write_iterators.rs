//! The `Rw*` write iterators must behave like their `Ro*` siblings — same bound
//! semantics (under the database's comparator, SPEC 03 §2.0) and the same
//! boundary re-impositions (empty prefix → `BadValSize`, SPEC 03 §2.1). The
//! write path was built as a parallel cursor rather than over the checked read
//! path, and these are the seams where it drifted (codebase review, 2026-09-09).

use std::ops::Bound;

use heed_zerodb::types::{Bytes, Str};
use heed_zerodb::{Database, EnvOpenOptions, Error, MdbError};

fn env_opts() -> EnvOpenOptions<heed_zerodb::WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

/// Reverse byte order — the simplest comparator under which memcmp bound
/// tests give the wrong answer.
enum ReverseComparator {}

impl heed_zerodb::Comparator for ReverseComparator {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
}

fn keys<'txn, I>(it: I) -> Vec<String>
where
    I: Iterator<Item = heed_zerodb::Result<(&'txn str, &'txn [u8])>>,
{
    it.map(|e| e.unwrap().0.to_string()).collect()
}

/// `range_mut` / `rev_range_mut` test their terminating bound under the
/// database's comparator, exactly like `range` / `rev_range` (and heed, whose
/// `RwRange` does `C::compare(key, end)`).
#[test]
fn range_mut_bounds_follow_the_registered_comparator() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(8 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let rev: Database<Str, Bytes, ReverseComparator> = env
        .database_options()
        .types::<Str, Bytes>()
        .key_comparator::<ReverseComparator>()
        .name("rev")
        .create(&mut wtxn)
        .unwrap();
    for k in ["a", "b", "c", "d"] {
        rev.put(&mut wtxn, k, b"v").unwrap();
    }
    wtxn.commit().unwrap();

    // Under the reverse ordering the tree reads d, c, b, a. The range
    // [d, b) in *that* order is {d, c}.
    let range = (Bound::Included("d"), Bound::Excluded("b"));

    let rtxn = env.read_txn().unwrap();
    let fwd_ro = keys(rev.range(&rtxn, &range).unwrap());
    let rev_ro = keys(rev.rev_range(&rtxn, &range).unwrap());
    drop(rtxn);
    assert_eq!(fwd_ro, vec!["d", "c"], "read-side control");
    assert_eq!(rev_ro, vec!["c", "d"], "read-side control (reverse)");

    let mut wtxn = env.write_txn().unwrap();
    let fwd_rw = keys(rev.range_mut(&mut wtxn, &range).unwrap());
    assert_eq!(
        fwd_rw, fwd_ro,
        "range_mut must terminate on the comparator bound, not a memcmp one \
         (memcmp says \"d\" < \"b\" is false and yields nothing)"
    );
    let rev_rw = keys(rev.rev_range_mut(&mut wtxn, &range).unwrap());
    assert_eq!(rev_rw, rev_ro, "rev_range_mut must agree with rev_range");
    wtxn.abort();
}

/// Control: with the default comparator the write iterators are unchanged.
#[test]
fn range_mut_default_comparator_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for i in 0u8..6 {
        db.put(&mut wtxn, &[i], b"v").unwrap();
    }
    let range = (Bound::Included(&[1u8][..]), Bound::Included(&[3u8][..]));
    let fwd: Vec<u8> = db
        .range_mut(&mut wtxn, &range)
        .unwrap()
        .map(|e| e.unwrap().0[0])
        .collect();
    assert_eq!(fwd, vec![1, 2, 3]);
    let rev: Vec<u8> = db
        .rev_range_mut(&mut wtxn, &range)
        .unwrap()
        .map(|e| e.unwrap().0[0])
        .collect();
    assert_eq!(rev, vec![3, 2, 1]);
    wtxn.abort();
}

/// SPEC 03 §2.1: a forward prefix scan of the **empty** prefix is an explicit
/// `MDB_SET_RANGE` with a zero-size key → `BadValSize`. The adapter re-imposes
/// this on `prefix_iter`; `prefix_iter_mut` is the same LMDB call and must
/// answer the same way. The reverse variants seek via `last` and succeed.
#[test]
fn prefix_iter_mut_empty_prefix_is_bad_val_size_like_prefix_iter() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for k in [b"aa".as_slice(), b"ab", b"b"] {
        db.put(&mut wtxn, k, b"v").unwrap();
    }
    wtxn.commit().unwrap();

    // Read side (already pinned at M1.13): the reference behavior.
    let rtxn = env.read_txn().unwrap();
    assert!(
        matches!(
            db.prefix_iter(&rtxn, b""),
            Err(Error::Mdb(MdbError::BadValSize))
        ),
        "read-side control"
    );
    assert_eq!(db.rev_prefix_iter(&rtxn, b"").unwrap().count(), 3);
    drop(rtxn);

    let mut wtxn = env.write_txn().unwrap();
    match db.prefix_iter_mut(&mut wtxn, b"") {
        Err(Error::Mdb(MdbError::BadValSize)) => {}
        Ok(_) => panic!("prefix_iter_mut(\"\") must be BadValSize, not a full scan"),
        Err(other) => panic!("expected BadValSize, got {other:?}"),
    }
    // Reverse variant mirrors the read side: works, full reverse iteration.
    let n = db.rev_prefix_iter_mut(&mut wtxn, b"").unwrap().count();
    assert_eq!(n, 3);
    // A real prefix still works through the write cursor.
    let n = db.prefix_iter_mut(&mut wtxn, b"a").unwrap().count();
    assert_eq!(n, 2);
    wtxn.abort();
}

/// `copy_to_file` streams through a private staging directory and leaves
/// nothing behind in the system temp dir, on success.
#[test]
fn copy_to_file_leaves_no_staging_files_behind() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for i in 0u32..500 {
        db.put(&mut wtxn, &i.to_be_bytes(), &[0xAB; 200]).unwrap();
    }
    wtxn.commit().unwrap();

    let dst = tempfile::tempdir().unwrap();
    let mut out = std::fs::File::create(dst.path().join("data.mdb")).unwrap();
    env.copy_to_file(&mut out, heed_zerodb::CompactionOption::Enabled)
        .unwrap();
    drop(out);

    let prefix = format!("heed-zerodb-copy-{}-", std::process::id());
    let leftovers: Vec<_> = std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
        .collect();
    assert!(leftovers.is_empty(), "staging left behind: {leftovers:?}");

    // The copy is a complete, reopenable env.
    let copy = unsafe {
        env_opts()
            .map_size(4 * 1024 * 1024)
            .max_dbs(4)
            .open(dst.path())
            .unwrap()
    };
    let rtxn = copy.read_txn().unwrap();
    let db2: Database<Bytes, Bytes> = copy.open_database(&rtxn, None).unwrap().unwrap();
    assert_eq!(db2.len(&rtxn).unwrap(), 500);
}
