//! Leaf-granular `Database::delete_range` (SPEC 00 row 37, SPEC 03
//! delete_range note): the range is deleted one leaf span at a time (one COW +
//! one splice + one rebalance per covered leaf) rather than one descent +
//! rebalance per key. These tests pin the **committed image** after such
//! deletes: exact survivors, exact `entries`/`overflow_pages` stats, covered
//! `F_BIGDATA` runs freed (and uncovered ones kept), and `check_image`'s full
//! invariant sweep — INV-22's reachable-XOR-free partition would expose a
//! leaked or double-freed page.

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, free_page_count, Comparator, Env, EnvOpenOptions, FnComparator};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-delrange-{pid}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

const PS: u32 = 4096;
const MAP: usize = 64 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.open(dir).expect("open env")
}

/// Full-image invariant sweep (INV-22 included): after a range delete
/// commits, every freed page must be in the GC, none may be both reachable
/// and free, and none may be neither.
fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

/// ~430-byte cells keep leaf fanout low: 1000 entries span >100 leaves, so
/// an interior range crosses whole leaves, partial leaves, and leaf seams.
fn inline_val(i: u32) -> Vec<u8> {
    let mut v = format!("val-{i:06}-").into_bytes();
    v.resize(400, b'x');
    v
}

/// Values above the inline threshold: a 3-page overflow run each at 4 K.
fn big_val(i: u32) -> Vec<u8> {
    let mut v = format!("big-{i:06}-").into_bytes();
    v.resize(9000, b'B');
    v
}

/// (a) Interior range across many leaves, overflow values inside and outside
/// the range: exact count, exact survivors at the edges, exact stats, clean
/// committed image, and the freed pages actually land in the GC.
#[test]
fn interior_range_multi_leaf_with_overflow() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    const N: u32 = 3000;
    let is_big = |i: u32| i.is_multiple_of(300); // 10 overflow runs, spread out
    let mut txn = env.write_txn().unwrap();
    for i in 0..N {
        let v = if is_big(i) { big_val(i) } else { inline_val(i) };
        db.put(&mut txn, &key(i), &v).unwrap();
    }
    txn.commit().unwrap();
    assert_clean(dir.path());

    let (ovf_before, free_before) = {
        let rtxn = env.read_txn().unwrap();
        let stat = db.stat(&rtxn).unwrap();
        assert!(stat.depth >= 3, "want a multi-level tree, got {stat:?}");
        assert_eq!(stat.entries, u64::from(N));
        assert_eq!(stat.overflow_pages, 30, "10 runs x 3 pages");
        (stat.overflow_pages, free_page_count(&rtxn).unwrap())
    };

    // [key-000450, key-002550): covers 2100 keys and 7 of the 10 runs
    // (i = 600..=2400 step 300); the runs at 0, 300, 2700 survive.
    let mut txn = env.write_txn().unwrap();
    let n = db
        .delete_range(
            &mut txn,
            Included(key(450).as_slice()),
            Excluded(key(2550).as_slice()),
        )
        .unwrap();
    assert_eq!(n, 2100);
    txn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    let stat = db.stat(&rtxn).unwrap();
    assert_eq!(stat.entries, u64::from(N) - 2100);
    assert_eq!(stat.overflow_pages, ovf_before - 7 * 3);
    // Edges: last survivor below, first deleted, last deleted, first
    // survivor above.
    assert!(db.get(&rtxn, &key(449)).unwrap().is_some());
    assert_eq!(db.get(&rtxn, &key(450)).unwrap(), None);
    assert_eq!(db.get(&rtxn, &key(2549)).unwrap(), None);
    assert!(db.get(&rtxn, &key(2550)).unwrap().is_some());
    // Surviving overflow values are intact end to end.
    assert_eq!(db.get(&rtxn, &key(0)).unwrap(), Some(big_val(0).as_slice()));
    assert_eq!(
        db.get(&rtxn, &key(2700)).unwrap(),
        Some(big_val(2700).as_slice())
    );
    // Exact survivor set, in order.
    let want: Vec<u32> = (0..450).chain(2550..N).collect();
    let got: Vec<Vec<u8>> = db.iter(&rtxn).map(|r| r.unwrap().0.to_vec()).collect();
    assert_eq!(got.len(), want.len());
    for (g, i) in got.iter().zip(&want) {
        assert_eq!(g, &key(*i));
    }
    // The covered leaves and runs are free for reuse now.
    let free_after = free_page_count(&rtxn).unwrap();
    assert!(
        free_after > free_before,
        "no pages were freed (before {free_before}, after {free_after})"
    );
}

/// (b) Edge drains that end exactly at leaf seams, then a full unbounded
/// wipe: the tree empties through the ordinary root-shrink path and the
/// committed image stays clean at every step.
#[test]
fn edge_drains_then_full_wipe() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    const N: u32 = 600;
    let mut txn = env.write_txn().unwrap();
    for i in 0..N {
        db.put(&mut txn, &key(i), &inline_val(i)).unwrap();
    }
    // Prefix drain (unbounded lower edge).
    let n = db
        .delete_range(&mut txn, Unbounded, Excluded(key(100).as_slice()))
        .unwrap();
    assert_eq!(n, 100);
    // Suffix drain (unbounded upper edge, inclusive lower).
    let n = db
        .delete_range(&mut txn, Included(key(500).as_slice()), Unbounded)
        .unwrap();
    assert_eq!(n, 100);
    // Empty and inverted ranges are no-ops.
    assert_eq!(
        db.delete_range(
            &mut txn,
            Excluded(key(200).as_slice()),
            Excluded(key(200).as_slice()),
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.delete_range(
            &mut txn,
            Included(key(400).as_slice()),
            Included(key(300).as_slice()),
        )
        .unwrap(),
        0
    );
    assert_eq!(db.len(&txn).unwrap(), 400);
    txn.commit().unwrap();
    assert_clean(dir.path());

    // Full wipe through delete_range (not clear): root shrink to empty.
    let mut txn = env.write_txn().unwrap();
    let n = db.delete_range(&mut txn, Unbounded, Unbounded).unwrap();
    assert_eq!(n, 400);
    assert_eq!(db.len(&txn).unwrap(), 0);
    txn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    let stat = db.stat(&rtxn).unwrap();
    assert_eq!(stat.entries, 0);
    assert_eq!(stat.depth, 0);
    assert_eq!(stat.branch_pages + stat.leaf_pages + stat.overflow_pages, 0);
    drop(rtxn);

    // The emptied tree accepts inserts again and stays clean.
    let mut txn = env.write_txn().unwrap();
    db.put(&mut txn, &key(7), &inline_val(7)).unwrap();
    txn.commit().unwrap();
    assert_clean(dir.path());
}

/// As [`assert_clean`], for an image holding a custom-comparator tree:
/// `check_image` is memcmp-defined (SPEC 03 §2.0), so a comparator-ordered
/// tree legitimately trips INV-5/INV-6; everything else must still hold
/// (the same filter `custom_comparator.rs` pins).
fn assert_clean_ignoring_key_order(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let unrelated: Vec<String> = check::check_image(&bytes, PS)
        .into_iter()
        .filter(|s| !s.starts_with("INV-5") && !s.starts_with("INV-6"))
        .collect();
    assert!(
        unrelated.is_empty(),
        "invariant violations unrelated to key order: {unrelated:#?}"
    );
}

fn reverse() -> Box<dyn Comparator> {
    Box::new(FnComparator::new(
        "test.reverse.v1",
        |a: &[u8], b: &[u8]| b.cmp(a),
    ))
}

/// (c) A reverse-comparator DB spanning many leaves: the range is the
/// comparator-ordered interval (memcmp-inverted), the splice walk crosses
/// leaf seams in comparator order, and the committed image is clean. A
/// memcmp control DB in the same env is unaffected.
#[test]
fn reverse_comparator_range_spans_leaves() {
    let dir = TempDir::new();
    let env = open(dir.path());

    const N: u32 = 600;
    let (rev, plain) = {
        let mut txn = env.write_txn().unwrap();
        let rev = env
            .create_database_with_comparator(&mut txn, Some(b"rev"), reverse())
            .unwrap();
        let plain = env.create_database(&mut txn, Some(b"plain")).unwrap();
        for i in 0..N {
            rev.put(&mut txn, &key(i), &inline_val(i)).unwrap();
            plain.put(&mut txn, &key(i), b"p").unwrap();
        }
        txn.commit().unwrap();
        (rev, plain)
    };
    assert_clean_ignoring_key_order(dir.path());

    // In comparator order the DB runs key-000599 down to key-000000, so
    // [key-000450, key-000050) is the comparator interval covering
    // i = 450 down to 51 — 400 keys across many leaves.
    let mut txn = env.write_txn().unwrap();
    let n = rev
        .delete_range(
            &mut txn,
            Included(key(450).as_slice()),
            Excluded(key(50).as_slice()),
        )
        .unwrap();
    assert_eq!(n, 400, "comparator interval [450 .. 51]");
    txn.commit().unwrap();
    assert_clean_ignoring_key_order(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert_eq!(rev.len(&rtxn).unwrap(), u64::from(N) - 400);
    for i in 0..N {
        let want = !(51..=450).contains(&i);
        assert_eq!(
            rev.get(&rtxn, &key(i)).unwrap().is_some(),
            want,
            "key {i} survival"
        );
    }
    // Survivors iterate in comparator (descending) order.
    let got: Vec<Vec<u8>> = rev.iter(&rtxn).map(|r| r.unwrap().0.to_vec()).collect();
    let want: Vec<Vec<u8>> = (0..N)
        .rev()
        .filter(|i| !(51..=450).contains(i))
        .map(key)
        .collect();
    assert_eq!(got, want);
    // The memcmp control DB in the same env is untouched.
    assert_eq!(plain.len(&rtxn).unwrap(), u64::from(N));
}
