//! M1.4 fix follow-up: regression coverage for the **symmetric** branch-level
//! rebalance paths that the ascending-delete storm tests
//! (`deep_tree_rebalance.rs`, `write_rebalance_differential.rs`) do not reach.
//!
//! The fixed bug (repeated `remove(0)` in branch borrow-from-**right**: after
//! the sentinel is removed, the real-keyed old node 1 shifts into index 0 and
//! the node-0 empty-separator rule panics) has two symmetric siblings audited
//! in the same pass: branch borrow-from-**left** and branch **merge** (both
//! directions). Ascending deletes underfill the *leftmost* subtree first
//! (`pki == 0` → sibling on the right); **descending** deletes underfill the
//! *rightmost* subtree (`pki >= 1` → sibling on the left), driving
//! borrow-from-left while the left sibling is still full and merge-from-left
//! once it drains to `MIN_KEYS_BRANCH`. The interior-band test drives merges
//! where the survivor is on either side. Every commit is validated with the
//! SPEC 03 §11 walker.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvOpenOptions, TxnRead};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-deep-sym-{pid}-{seq}"));
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
const MAP: usize = 16 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

/// Same wide-key geometry as `deep_tree_rebalance.rs` (~7 entries/page at
/// 4 KiB → depth 3 within a few hundred entries).
fn wide_key(i: u32) -> Vec<u8> {
    let mut k = format!("wk{i:06}").into_bytes();
    k.resize(500, b'.');
    k
}

/// Symmetric regression 1: **descending** cascade delete — the underful
/// branch is never the leftmost child, so branch rebalance takes the
/// borrow-from-LEFT path (left sibling above threshold), then merge-from-LEFT
/// (left sibling at `MIN_KEYS_BRANCH`), all the way to root collapse.
#[test]
fn descending_delete_drives_borrow_and_merge_from_left() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let n = 400u32;

    let mut wtxn = env.write_txn().unwrap();
    for i in 0..n {
        db.put(&mut wtxn, &wide_key(i), format!("v{i}").as_bytes())
            .unwrap();
    }
    let rec = *TxnRead::main_record(&wtxn);
    assert!(rec.depth >= 3, "geometry must reach depth >= 3");
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    // Strictly descending: every rebalance sees its sibling on the LEFT.
    let mut wtxn = env.write_txn().unwrap();
    for i in (0..n).rev() {
        assert!(db.delete(&mut wtxn, &wide_key(i)).unwrap(), "delete {i}");
    }
    let rec = *TxnRead::main_record(&wtxn);
    assert_eq!(rec.entries, 0);
    assert_eq!(rec.depth, 0, "descending cascade must collapse to depth 0");
    assert_eq!(rec.leaf_pages, 0);
    assert_eq!(rec.branch_pages, 0);
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert!(db.is_empty(&rtxn).unwrap());
}

/// Symmetric regression 2: delete an **interior band** so underful pages sit
/// between two populated neighbors — merges run with the survivor on either
/// side (left-absorbs-right with `P` as left AND as right), and the remaining
/// two populated flanks stay correct.
#[test]
fn interior_band_delete_merges_in_both_directions() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let n = 400u32;

    let mut wtxn = env.write_txn().unwrap();
    for i in 0..n {
        db.put(&mut wtxn, &wide_key(i), format!("v{i}").as_bytes())
            .unwrap();
    }
    assert!(TxnRead::main_record(&wtxn).depth >= 3);
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    // Hollow out the middle half [100, 300), alternating ends of the band so
    // the underful frontier advances from BOTH sides toward the middle.
    let mut wtxn = env.write_txn().unwrap();
    let (mut lo, mut hi) = (100u32, 299u32);
    while lo <= hi {
        assert!(db.delete(&mut wtxn, &wide_key(lo)).unwrap());
        if hi != lo {
            assert!(db.delete(&mut wtxn, &wide_key(hi)).unwrap());
        }
        lo += 1;
        hi -= 1;
    }
    assert_eq!(TxnRead::main_record(&wtxn).entries, 200);
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    // Both flanks intact and correctly ordered.
    let rtxn = env.read_txn().unwrap();
    let all: Vec<_> = db.iter(&rtxn).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all.len(), 200);
    let expect: Vec<Vec<u8>> = (0..100u32).chain(300..400).map(wide_key).collect();
    assert!(all.iter().map(|(k, _)| k.to_vec()).eq(expect));
}
