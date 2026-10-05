//! Roadmap #5a — leaf-skipping `clear`/`drop` (LMDB `mdb_drop0` parity,
//! SPEC 02 §6.1): a tree whose record says `overflow_pages == 0` is cleared by
//! reading only its branch pages; the leaf pgnos are freed straight from the
//! lowest branch level, unread. These tests pin that the freed set is exactly
//! the set the reading walk freed — via `check_image`'s INV-22
//! reachable-XOR-free partition (a leaked leaf is "neither", a double-freed
//! one "both"), the stat counters, and the free-page count — that an
//! overflow-bearing tree still takes the reading walk and frees its runs, and
//! that the freed pages are actually reusable afterwards. Do not weaken
//! (AGENTS.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, free_page_count, Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-clearskip-{pid}-{seq}"));
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

/// Full-image invariant sweep, INV-22 included: after a clear commits, every
/// page the tree occupied must be in the GC ("free"), no page may be both
/// reachable and free, and none may be neither — i.e. the freed set is exactly
/// what the reading walk produced.
fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

/// A value large enough to keep leaf fanout low (multi-level tree from a
/// moderate entry count) but far below the overflow threshold.
fn inline_val(i: u32) -> Vec<u8> {
    let mut v = format!("val-{i:06}-").into_bytes();
    v.resize(400, b'x');
    v
}

/// (a) A multi-level, overflow-free tree: clear takes the leaf-skipping path
/// (`overflow_pages == 0`) and the committed image must account for every
/// page the tree held — leaves freed unread included.
#[test]
fn clear_multi_level_no_overflow_frees_every_page() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    const N: u32 = 2000;
    let mut txn = env.write_txn().unwrap();
    for i in 0..N {
        db.put(&mut txn, &key(i), &inline_val(i)).unwrap();
    }
    txn.commit().unwrap();
    assert_clean(dir.path());

    let (tree_pages, free_before) = {
        let rtxn = env.read_txn().unwrap();
        let stat = db.stat(&rtxn).unwrap();
        // The shape under test: at least one branch level BELOW the root, so
        // the lowest-branch-level walk is exercised, and no overflow.
        assert!(stat.depth >= 3, "want a multi-level tree, got {stat:?}");
        assert_eq!(stat.overflow_pages, 0, "fixture must be overflow-free");
        assert_eq!(stat.entries, u64::from(N));
        (
            stat.branch_pages + stat.leaf_pages,
            free_page_count(&rtxn).unwrap(),
        )
    };

    let mut txn = env.write_txn().unwrap();
    db.clear(&mut txn).unwrap();
    assert_eq!(db.len(&txn).unwrap(), 0);
    assert_eq!(db.get(&txn, &key(0)).unwrap(), None);
    assert_eq!(db.get(&txn, &key(N - 1)).unwrap(), None);
    txn.commit().unwrap();

    // INV-22 is the exact freed-set equivalence: a leaf skipped but leaked
    // would be neither reachable nor free; a double-freed pgno would be
    // listed twice / both.
    assert_clean(dir.path());
    let rtxn = env.read_txn().unwrap();
    let stat = db.stat(&rtxn).unwrap();
    assert_eq!(stat.entries, 0);
    assert_eq!(stat.depth, 0);
    assert_eq!(stat.branch_pages + stat.leaf_pages + stat.overflow_pages, 0);
    // Every page of the old tree is in the GC now (the GC's own save may
    // additionally consume or free bookkeeping pages, so >=, with the exact
    // partition already pinned by INV-22 above).
    let free_after = free_page_count(&rtxn).unwrap();
    assert!(
        free_after >= tree_pages,
        "tree held {tree_pages} pages (free before: {free_before}), \
         only {free_after} are free after clear"
    );
}

/// (b) A tree WITH overflow values takes the reading walk and frees the
/// overflow runs too.
#[test]
fn clear_with_overflow_frees_the_runs() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    const N: u32 = 200;
    let big = vec![0xabu8; 3 * PS as usize]; // > page size: an overflow run
    let mut txn = env.write_txn().unwrap();
    for i in 0..N {
        db.put(&mut txn, &key(i), &big).unwrap();
    }
    txn.commit().unwrap();
    assert_clean(dir.path());

    let (tree_pages, overflow_pages) = {
        let rtxn = env.read_txn().unwrap();
        let stat = db.stat(&rtxn).unwrap();
        assert!(stat.overflow_pages > 0, "fixture must have overflow runs");
        (
            stat.branch_pages + stat.leaf_pages + stat.overflow_pages,
            stat.overflow_pages,
        )
    };

    let mut txn = env.write_txn().unwrap();
    db.clear(&mut txn).unwrap();
    txn.commit().unwrap();

    assert_clean(dir.path());
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.stat(&rtxn).unwrap().entries, 0);
    let free_after = free_page_count(&rtxn).unwrap();
    assert!(
        free_after >= tree_pages,
        "tree held {tree_pages} pages ({overflow_pages} overflow), \
         only {free_after} are free after clear"
    );
    drop(rtxn);

    // The runs are genuinely free: a fresh overflow value draws from them.
    let mut txn = env.write_txn().unwrap();
    db.put(&mut txn, b"again", &big).unwrap();
    txn.commit().unwrap();
    assert_clean(dir.path());
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"again").unwrap(), Some(&big[..]));
}

/// (d) clear → commit → reinsert cycles: the leaf pgnos freed unread must be
/// genuinely allocatable — the data round-trips and the file stays flat
/// (reuse actually fires), mirroring the churn bound in `gc_reclaim.rs`.
#[test]
fn clear_then_reuse_round_trips_and_stays_flat() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    const N: u32 = 2500;
    const CYCLES: usize = 4;
    let mut first_size = 0u64;
    for cycle in 0..CYCLES {
        let mut txn = env.write_txn().unwrap();
        for i in 0..N {
            db.put(&mut txn, &key(i), &inline_val(i)).unwrap();
        }
        txn.commit().unwrap();
        assert_clean(dir.path());
        {
            let rtxn = env.read_txn().unwrap();
            assert!(db.stat(&rtxn).unwrap().depth >= 3);
            for i in (0..N).step_by(97) {
                assert_eq!(
                    db.get(&rtxn, &key(i)).unwrap(),
                    Some(&inline_val(i)[..]),
                    "cycle {cycle}: key {i} did not round-trip"
                );
            }
        }
        let mut txn = env.write_txn().unwrap();
        db.clear(&mut txn).unwrap();
        txn.commit().unwrap();
        assert_clean(dir.path());

        let size = std::fs::metadata(dir.path().join(zerodb::DATA_FILE_NAME))
            .unwrap()
            .len();
        if cycle == 0 {
            first_size = size;
        } else {
            assert!(
                size <= first_size + first_size / 20,
                "cycle {cycle}: file grew from {first_size} to {size} — \
                 pages freed by the leaf-skipping clear were not reused"
            );
        }
    }
}

/// `drop` of a named DB goes through the same collection walk; a multi-level
/// overflow-free named tree must drop clean and its pages must be reusable.
#[test]
fn drop_named_db_leaf_skips_and_frees() {
    let dir = TempDir::new();
    let env = open(dir.path());

    let mut txn = env.write_txn().unwrap();
    let db = env.create_database(&mut txn, Some(b"sub")).unwrap();
    for i in 0..2500u32 {
        db.put(&mut txn, &key(i), &inline_val(i)).unwrap();
    }
    txn.commit().unwrap();
    assert_clean(dir.path());
    {
        let rtxn = env.read_txn().unwrap();
        let stat = db.stat(&rtxn).unwrap();
        assert!(
            stat.depth >= 3,
            "want a multi-level named tree, got {stat:?}"
        );
        assert_eq!(stat.overflow_pages, 0);
    }

    let mut txn = env.write_txn().unwrap();
    db.drop_db(&mut txn).unwrap();
    txn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert!(
        env.open_database(&rtxn, Some(b"sub")).unwrap().is_none(),
        "dropped DB must be gone from the catalog"
    );
}
