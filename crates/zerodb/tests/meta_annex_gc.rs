//! Meta free-list annex behavior (SPEC 05 §2a GC-29..33, SPEC 02 §3 format
//! v2, ADR-0022): steady-state placement, the GC-31 reader gate, the GC-30
//! carry, the GC-32 spill-to-tree, copy, and reopen — each asserted through
//! observable state (file size, `check_image`, `free_page_count`, the meta
//! bytes) rather than internals.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, free_page_count, CompactionOption, CopyToFile, Env, EnvOpenOptions};
use zerodb_core::page::{MetaPage, MetaValidity, PGNO_INVALID};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-annex-{pid}-{seq}"));
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
const MAP: usize = 32 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn data_file(dir: &Path) -> PathBuf {
    dir.join(zerodb::DATA_FILE_NAME)
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(data_file(dir)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

fn file_size(dir: &Path) -> u64 {
    std::fs::metadata(data_file(dir)).unwrap().len()
}

/// Read the LIVE meta's `(txnid, fl_count, free_db_root)` straight from the
/// data file (both slots validated, higher txnid wins — SPEC 02 §3.2).
fn live_meta(dir: &Path) -> (u64, u32, u64) {
    let bytes = std::fs::read(data_file(dir)).unwrap();
    let ps = PS as usize;
    let pick = |b: &[u8]| match MetaPage::validate(b, PS).unwrap() {
        MetaValidity::Valid(m) => Some(m),
        _ => None,
    };
    let m0 = pick(&bytes[..ps]);
    let m1 = pick(&bytes[ps..2 * ps]);
    let m = match (m0, m1) {
        (Some(a), Some(b)) => {
            if a.txnid >= b.txnid {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => panic!("no valid meta slot"),
    };
    (m.txnid, m.fl_count, m.free_db.root)
}

/// GC-29/GC-32 steady state: small-churn commits keep the whole free list in
/// the meta annex — the GC tree never materializes — and page reuse keeps the
/// file size flat.
#[test]
fn steady_state_annex_only_gc_tree_stays_empty() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    let val = vec![0xABu8; 256];
    for i in 0u64..60 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let (_, fl_mid, root_mid) = live_meta(dir.path());
    assert!(fl_mid > 0, "steady-state commits must carry a meta annex");
    assert_eq!(
        root_mid, PGNO_INVALID,
        "small churn must never materialize the GC tree (GC-29/GC-32)"
    );

    // Overwrite churn: every commit frees the COW'd path and reuses the
    // previous commit's annex pages. After a few settling commits the file
    // must not grow at all.
    for i in 0u64..10 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_settled = file_size(dir.path());
    for i in 0u64..50 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    assert_eq!(
        file_size(dir.path()),
        size_settled,
        "annex reuse must keep overwrite churn at zero file growth"
    );
    assert_clean(dir.path());

    // free_page_count sees the annex ids (GC-23 as amended).
    let rtxn = env.read_txn().unwrap();
    let (_, fl_now, root_now) = live_meta(dir.path());
    assert_eq!(root_now, PGNO_INVALID);
    assert_eq!(free_page_count(&rtxn).unwrap(), u64::from(fl_now));
}

/// GC-31 gate + GC-30 carry: a reader pinned below the base blocks annex
/// draws (the file grows while it lives, nothing leaks), and after it
/// releases, the carried ids are reclaimed and growth stops.
#[test]
fn reader_gate_blocks_annex_draw_then_carry_reclaims() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let val = vec![0x44u8; 256];

    for i in 0u64..8 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    // Pin NOW; the next commit's base will be newer than this pin, so every
    // later annex (and tree entry) is gated off while the reader lives.
    let pin = env.read_txn().unwrap();
    {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &0u64.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_pinned_base = file_size(dir.path());
    for i in 0u64..6 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_during = file_size(dir.path());
    assert!(
        size_during > size_pinned_base,
        "with a reader pinned below every base, commits must extend the file \
         (the GC-31 gate refuses the annex) — got no growth, so the gate leaked"
    );
    assert_clean(dir.path());
    drop(pin);
    // Carried ids (GC-30: each commit re-listed the still-free remainder)
    // become reclaimable; churn must stop growing the file.
    for _ in 0..3 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &1u64.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_settled = file_size(dir.path());
    for i in 0u64..8 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    assert_eq!(
        file_size(dir.path()),
        size_settled,
        "after the reader releases, carried annex ids must satisfy churn"
    );
    assert_clean(dir.path());
}

/// GC-32 all-or-nothing spill: a commit freeing more ids than the annex cap
/// writes the whole PIL to the GC tree (annex empty), and those pages are
/// reclaimable afterwards.
#[test]
fn over_cap_freed_set_spills_whole_to_gc_tree() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let cap = zerodb_core::page::meta_annex_cap(PS) as u64; // 490 at 4 KiB

    // One huge overflow value, committed...
    {
        let mut w = env.write_txn().unwrap();
        db.put(
            &mut w,
            b"huge",
            &vec![0x7Au8; (cap as usize + 30) * PS as usize],
        )
        .unwrap();
        w.commit().unwrap();
    }
    // ...then freed in the next txn: freed > cap ⇒ the tree arm.
    {
        let mut w = env.write_txn().unwrap();
        assert!(db.delete(&mut w, b"huge").unwrap());
        w.commit().unwrap();
    }
    let (_, fl, root) = live_meta(dir.path());
    assert_eq!(
        fl, 0,
        "an over-cap freed set must leave the annex empty (GC-32)"
    );
    assert_ne!(root, PGNO_INVALID, "the spill must land in the GC tree");
    assert_clean(dir.path());
    {
        let rtxn = env.read_txn().unwrap();
        assert!(free_page_count(&rtxn).unwrap() > cap);
    }

    // The spilled pages are drawn back out (tree first, GC-18): re-inserting
    // a similar value must not grow the file beyond its high-water.
    let size_spilled = file_size(dir.path());
    {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, b"huge2", &vec![0x7Bu8; cap as usize * PS as usize])
            .unwrap();
        w.commit().unwrap();
    }
    assert!(
        file_size(dir.path()) <= size_spilled,
        "re-insert must reuse the spilled run, not extend"
    );
    assert_clean(dir.path());
}

/// Non-compact copy carries the pinned snapshot's annex (the live slot may
/// already hold a newer meta — the ids are pinned in the Snapshot), so the
/// copy is leak-free and reports the same free count. The compact copy drops
/// free pages by construction and must also come out clean.
#[test]
fn copy_preserves_annex_compact_drops_it() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let val = vec![0x55u8; 256];
    for i in 0u64..20 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let (_, fl, _) = live_meta(dir.path());
    assert!(fl > 0, "fixture must have a non-empty annex");
    let src_free = {
        let rtxn = env.read_txn().unwrap();
        free_page_count(&rtxn).unwrap()
    };

    let raw = TempDir::new();
    let raw_path = raw.path().join("copy-raw.zdb");
    env.copy_to_file(&raw_path, CompactionOption::Disabled)
        .unwrap();
    let bytes = std::fs::read(&raw_path).unwrap();
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "raw copy violations: {v:#?}");
    let copy_env = {
        let dirp = raw.path().join("raw-env");
        std::fs::create_dir_all(&dirp).unwrap();
        std::fs::copy(&raw_path, dirp.join(zerodb::DATA_FILE_NAME)).unwrap();
        open(&dirp)
    };
    {
        let rtxn = copy_env.read_txn().unwrap();
        assert_eq!(
            free_page_count(&rtxn).unwrap(),
            src_free,
            "raw copy must preserve the freelist, annex included"
        );
        assert_eq!(
            copy_env
                .main_database()
                .get(&rtxn, &3u64.to_be_bytes())
                .unwrap(),
            Some(val.as_slice())
        );
    }

    let compact_path = raw.path().join("copy-compact.zdb");
    env.copy_to_file(&compact_path, CompactionOption::Enabled)
        .unwrap();
    let bytes = std::fs::read(&compact_path).unwrap();
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "compact copy violations: {v:#?}");
}

/// Reopen re-parses the annex from the selected slot (GC-33 validation at the
/// writer's begin): reuse keeps working across a close/open cycle.
#[test]
fn reopen_reparses_annex_and_reuses() {
    let dir = TempDir::new();
    {
        let env = open(dir.path());
        let db = env.main_database();
        let val = vec![0x66u8; 256];
        for i in 0u64..20 {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
            w.commit().unwrap();
        }
    }
    let (_, fl, _) = live_meta(dir.path());
    assert!(fl > 0);
    let size_before = file_size(dir.path());

    let env = open(dir.path());
    let db = env.main_database();
    let val = vec![0x66u8; 256];
    for i in 0u64..10 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    assert_eq!(
        file_size(dir.path()),
        size_before,
        "the reopened env must draw from the persisted annex, not extend"
    );
    assert_clean(dir.path());
}
