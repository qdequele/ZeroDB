//! Milestone 1.4 coverage pass, area 5: the loose-page fast path (GC-7/8) and
//! trailing-shrink (GC-10), ADR-0004 D4. Neither is exercised by an existing
//! test through observable file-size + `check_image` assertions.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-loose-{pid}-{seq}"));
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
const MAP: usize = 8 << 20;

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

/// Same-txn alloc-then-free: an overflow run allocated and deleted **within
/// one txn** must be reused (GC-8 loose fast path / GC-10 trailing shrink),
/// not leaked as an unreferenced hole. The only growth this txn can
/// legitimately leave behind is the **one** new pgno for the COW of the
/// pre-existing committed leaf that "big"/"small" write into (SPEC 03 §5.1:
/// first touch of a committed page always copies to a fresh pgno) — the
/// 5-page overflow run itself must be fully reclaimed by GC-10, not just
/// "not grow further". (Confirmed by hand-tracing `allocate`/`free_run`/
/// `release_trailing_loose` in `rwtxn.rs`: this is exact, not an
/// approximation — see the arithmetic in this test's git history/PR
/// discussion if the exact-equality assertion below ever needs revisiting.)
#[test]
fn same_txn_alloc_then_free_overflow_reused_leaves_only_the_leaf_cow() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    // Baseline: commit something small first, so the loose-reuse path (which
    // only applies to pages allocated *after* the committed high-water) has
    // a real committed floor to compare against.
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"seed", b"x").unwrap();
        wtxn.commit().unwrap();
    }
    let size_before = std::fs::metadata(dir.path().join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len();

    {
        let mut wtxn = env.write_txn().unwrap();
        // Allocate a multi-page overflow run (this also COWs the pre-existing
        // "seed" leaf — the one page of legitimate growth)...
        db.put(&mut wtxn, b"big", &vec![0xABu8; 5 * PS as usize])
            .unwrap();
        let overflow_pages_in_txn = zerodb::TxnRead::main_record(&wtxn).overflow_pages;
        assert!(overflow_pages_in_txn >= 5);
        // ...then free it in the SAME txn.
        assert!(db.delete(&mut wtxn, b"big").unwrap());
        // A single-page put should now reuse a loose page (GC-8) rather than
        // extending the file further beyond the leaf COW.
        db.put(&mut wtxn, b"small", b"y").unwrap();
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path());
    let size_after = std::fs::metadata(dir.path().join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len();
    assert_eq!(
        size_after,
        size_before + PS as u64,
        "the whole 5-page overflow run must be reclaimed (GC-7/8/10); the only \
         durable growth allowed is the one-page leaf COW"
    );

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"big").unwrap(), None);
    assert_eq!(db.get(&rtxn, b"small").unwrap(), Some(b"y".as_slice()));
    assert_eq!(db.get(&rtxn, b"seed").unwrap(), Some(b"x".as_slice()));
}

/// GC-10 trailing shrink: pages allocated by this txn (bumping `next_pgno`)
/// but never written, whose pgnos are the highest-numbered ones, roll
/// `next_pgno` back at commit rather than leaving a hole — so freeing the
/// tail of a same-txn allocation run shrinks the persisted `last_pg`
/// (visible as the committed file not growing to cover the run at all, since
/// `write_at_page` only extends the file for pgnos that are actually
/// written, SPEC 04 §9 C2). As in the loose-reuse test above, inserting
/// through `Database::put` unavoidably COWs the pre-existing "seed" leaf
/// first (one legitimate extra page); the GC-10 assertion here is that nothing
/// **beyond** that single COW page survives, regardless of how large the
/// trailing run was.
#[test]
fn trailing_loose_pages_shrink_next_pgno() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"seed", b"x").unwrap();
        wtxn.commit().unwrap();
    }
    let size_seed = std::fs::metadata(dir.path().join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len();

    {
        let mut wtxn = env.write_txn().unwrap();
        // Allocate several pages beyond the committed high-water (bumps
        // next_pgno), then free exactly the highest-numbered ones — the
        // GC-10 trailing-shrink set — before committing. A large overflow
        // run followed by deleting it is the simplest way to allocate many
        // trailing pgnos in one shot and then free all of them (a pure
        // trailing run, no interior survivors).
        db.put(&mut wtxn, b"trailing", &vec![0x22u8; 10 * PS as usize])
            .unwrap();
        assert!(db.delete(&mut wtxn, b"trailing").unwrap());
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path());
    let size_after = std::fs::metadata(dir.path().join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len();
    assert_eq!(
        size_after,
        size_seed + PS as u64,
        "a 10-page trailing alloc-then-free must shrink back to just the one \
         unavoidable leaf-COW page (GC-10), not leak any of the 10 overflow pages"
    );

    // The env is still fully usable afterward (the rolled-back pgnos are
    // available for a fresh allocation, not stranded).
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, b"post-shrink", &vec![0x33u8; 3 * PS as usize])
        .unwrap();
    wtxn.commit().unwrap();
    assert_clean(dir.path());
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"post-shrink").unwrap(),
        Some(vec![0x33u8; 3 * PS as usize].as_slice())
    );
}
