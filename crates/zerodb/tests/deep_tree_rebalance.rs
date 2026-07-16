//! Milestone 1.4 coverage pass, area 1: independent confirmation that the
//! wide-key geometry used by
//! `crates/zerodb-oracle/tests/write_rebalance_differential.rs` actually
//! reaches depth >= 3 (branch-of-branches), and that the delete cascade walks
//! all the way back down to an empty, depth-0 tree. The `Op` model the
//! oracle differential drives has no depth-introspection op, so this is
//! asserted here directly against `TxnRead::main_record` (same technique as
//! `write_api.rs`'s `split_exact_fit_boundary_*`), on a real file-backed env
//! so `check::check_image` runs at every step (CLAUDE.md rule 1/2: never
//! weaken, so every commit is validated).

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
        let path = std::env::temp_dir().join(format!("zerodb-deep-tree-{pid}-{seq}"));
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

/// Mirrors `write_rebalance_differential.rs::wide_key`: a ~500-byte key
/// keeps branch fanout low (~7 entries/page) so depth 3 is reached in a few
/// hundred entries.
fn wide_key(i: u32) -> Vec<u8> {
    let mut k = format!("wk{i:06}").into_bytes();
    k.resize(500, b'.');
    k
}

#[test]
fn wide_key_geometry_reaches_depth_three_then_collapses() {
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
    assert!(
        rec.depth >= 3,
        "wide-key geometry must reach depth >= 3 for a multi-level split \
         cascade test to be meaningful, got depth {}",
        rec.depth
    );
    assert!(rec.branch_pages >= 2, "expected multiple branch pages");
    assert_eq!(rec.entries, n as u64);
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    // Cascade delete: evens, then odds — must walk back to an empty,
    // depth-0 tree (§9 root collapse) with every merge/borrow along the way
    // leaving a structurally clean image.
    let mut wtxn = env.write_txn().unwrap();
    for i in (0..n).step_by(2) {
        assert!(db.delete(&mut wtxn, &wide_key(i)).unwrap());
    }
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    for i in (1..n).step_by(2) {
        assert!(db.delete(&mut wtxn, &wide_key(i)).unwrap());
    }
    let rec = *TxnRead::main_record(&wtxn);
    assert_eq!(rec.entries, 0);
    assert_eq!(rec.depth, 0, "fully-deleted tree must collapse to depth 0");
    assert_eq!(rec.root, zerodb_core::page::PGNO_INVALID);
    assert_eq!(rec.leaf_pages, 0);
    assert_eq!(rec.branch_pages, 0);
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert!(db.is_empty(&rtxn).unwrap());
    assert_eq!(db.first(&rtxn).unwrap(), None);
}
