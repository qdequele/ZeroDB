//! `free_page_count` over a real, fragmented GC tree.
//!
//! Complements the entry-level equivalence proof in
//! `crates/zerodb-core/tests/free_count_prefix.rs`: here the count is taken from
//! an actual B+tree free DB holding many small PIL entries — the shape milli's
//! `non_free_pages_size()` probe pays for, and the one the count-prefix-only
//! walk (SPEC 05 GC-23) optimises. A filled DB is torn down
//! across several committed txns under a pinned reader so each commit's freed
//! pages accumulate as their own GC entry instead of recycling the previous
//! one, then the count is checked for internal consistency (INV-27) against an
//! independent decode-based walk of the same image (`check::check_image`,
//! INV-22/24/26).

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
        let path = std::env::temp_dir().join(format!("zerodb-nff-{pid}-{seq}"));
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
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

#[test]
fn free_page_count_over_fragmented_gc_tree() {
    const KEYS: u32 = 4_000;
    const COMMITS: u32 = 8;
    const PER: u32 = KEYS / COMMITS;

    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    // Fill a tree of several hundred pages.
    {
        let mut w = env.write_txn().unwrap();
        for i in 0..KEYS {
            db.put(&mut w, format!("k{i:05}").as_bytes(), &[0x5Au8; 200])
                .unwrap();
        }
        w.commit().unwrap();
    }

    // Pin the filled snapshot: while it lives, the oldest-reader gate stops any
    // delete commit from reclaiming a prior commit's freed pages, so each of the
    // COMMITS txns leaves its own GC entry — a deliberately fragmented free list.
    let pin = env.read_txn().unwrap();
    for c in 0..COMMITS {
        let mut w = env.write_txn().unwrap();
        for i in c * PER..(c + 1) * PER {
            assert!(db.delete(&mut w, format!("k{i:05}").as_bytes()).unwrap());
        }
        w.commit().unwrap();
    }

    // Independent decode-based walk of the committed image: check_image decodes
    // every GC entry's ids and asserts reachable-XOR-free (INV-22/24/26). If the
    // count-prefix walk mis-shaped an entry, this image would not be clean.
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    let free = free_page_count(&rtxn).unwrap();
    // Every delete commit freed at least its copy-on-write leaves, so the count
    // spans many entries and far exceeds the commit count.
    assert!(
        free > u64::from(COMMITS),
        "expected a large fragmented free set across ~{COMMITS} entries, got {free}"
    );
    // The count is a pure function of the snapshot.
    assert_eq!(
        free_page_count(&rtxn).unwrap(),
        free,
        "count not deterministic"
    );
    let g = rtxn.snapshot().free_db;
    let gc_tree = g.branch_pages + g.leaf_pages + g.overflow_pages;
    drop(rtxn);

    // INV-27 (SPEC 05 §9) over the same several-entry
    // tree: user-tree pages (`non_free_pages_size`), GC-tree pages, free pages
    // and the two meta slots partition the file exactly.
    let disk = env.real_disk_size().unwrap();
    let non_free = env.non_free_pages_size().unwrap();
    assert_eq!(
        non_free + (free + gc_tree + 2) * u64::from(PS),
        disk,
        "INV-27 violated: disk={disk} non_free={non_free} free={free} gc_tree={gc_tree}"
    );
    // Every key was deleted: the databases hold no pages (heed reports the
    // same for empty trees).
    assert_eq!(non_free, 0, "an emptied env must report no used pages");

    drop(pin);
}
