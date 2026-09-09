//! Hostile / corrupt on-disk input must yield a **typed error** — never a
//! panic, hang, unbounded allocation, out-of-bounds read, or (through the
//! real mmap backing) a SIGBUS. These tests craft images by hand (valid CRCs,
//! hostile geometry) and drive them through the same `open_with_backing` /
//! txn entry points the real I/O layer uses, over a heap backing so the whole
//! suite also runs under miri.
//!
//! Regression tests for the first-release security review (2026-09):
//! truncated-file geometry (open validation + the read-side `last_pg` bound),
//! zero-child branches, hostile GC freelists, hostile tree depths, and the
//! checker's checked arithmetic. Do not weaken these (CLAUDE.md rule 2).

use zerodb_core::check::check_image;
use zerodb_core::env::testutil::VecBacking;
use zerodb_core::env::{open_with_backing, DurabilityFlags, Env};
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::{BranchMut, BranchRef, LeafMut, MetaPage, PageError, PGNO_INVALID};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const PS: u32 = 4096;
const MAP: u64 = 1 << 20; // 256 pages

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_path(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/virtual/hostile-{tag}-{}-{n}", std::process::id()))
}

/// Build an image of `file_pages` pages: both meta slots encoded from
/// `edit(meta)`, then each `(pgno, frame)` copied in. The vector length is
/// exactly the "file" size (`VecBacking::real_disk_size` returns it), so a
/// meta naming geometry past `file_pages` simulates a truncated/hostile file.
fn craft_image(
    file_pages: usize,
    edit: impl Fn(&mut MetaPage),
    pages: &[(u64, Vec<u8>)],
) -> Vec<u8> {
    let ps = PS as usize;
    let mut buf = vec![0u8; file_pages * ps];
    for slot in [0u64, 1] {
        let mut m = MetaPage::create(slot, PS, MAP);
        edit(&mut m);
        let base = slot as usize * ps;
        m.encode(&mut buf[base..base + ps]).expect("valid meta");
    }
    for (pgno, frame) in pages {
        let base = *pgno as usize * ps;
        buf[base..base + frame.len()].copy_from_slice(frame);
    }
    buf
}

fn open_image(buf: Vec<u8>, tag: &str) -> Result<Env, Error> {
    open_with_backing(
        unique_path(tag),
        Box::new(VecBacking(buf)),
        PS,
        MAP,
        false,
        8,
        8,
        DurabilityFlags::default(),
    )
}

/// A one-entry leaf page frame stamped `(pgno, txnid)`.
fn leaf_frame(pgno: u64, txnid: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, pgno, txnid).unwrap();
    leaf.insert_inline(0, key, 0, val).unwrap();
    buf
}

// ---------------------------------------------------------------------------
// H1 — open-time geometry validation (SPEC 06 REC-1a / SPEC 02 §3.2 step 6)
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_last_pg_past_file_end() {
    // Valid CRC, but last_pg names pages the 2-page "file" cannot back. On a
    // real env the mapping covers the whole map_size, so pre-fix the first
    // read of such a page was a SIGBUS on unbacked bytes.
    let img = craft_image(2, |m| m.last_pg = 100, &[]);
    let e = open_image(img, "lastpg").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn open_rejects_last_pg_overflow() {
    // (last_pg + 1) * page_size must be computed checked: u64::MAX wraps.
    let img = craft_image(2, |m| m.last_pg = u64::MAX, &[]);
    let e = open_image(img, "lastpg-max").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn open_rejects_txnid_in_sentinel_band() {
    // Found by fuzz_image_open: the reader table encodes slot occupancy in
    // the top-of-u64 sentinel band (RDR_FREE/RDR_CLAIMED), so a hostile meta
    // txnid up there panicked the pin protocol's TXN-14 assumption (debug)
    // or aliased the sentinels (release); the writer's `txnid + 1` can also
    // wrap. Must be rejected at open.
    // The bound leaves a 2^32 margin below the band; everything from the
    // margin up is refused, not just the three sentinel-adjacent values.
    let limit = u64::MAX - 1 - (1u64 << 32); // readers::MAX_COMMITTED_TXNID
    for bad in [
        u64::MAX,
        u64::MAX - 1,
        u64::MAX - 2,
        u64::MAX - 3,
        limit + 1,
    ] {
        let img = craft_image(2, |m| m.txnid = bad, &[]);
        let e = open_image(img, "txnid-band").unwrap_err();
        assert!(
            matches!(e, Error::Mdb(MdbError::Invalid)),
            "txnid {bad:#x}: got {e:?}"
        );
    }
}

#[test]
fn writer_refuses_to_commit_into_the_sentinel_band() {
    // Spec review 2026-09-09: an open-time check alone is one commit deep — a
    // meta exactly at the bound would be accepted and reach RDR_CLAIMED two
    // commits later. The writer re-enforces the bound: at `base.txnid ==
    // MAX_COMMITTED_TXNID` the store still opens (reads work) but no write
    // transaction can start, so the band is unreachable however many commits
    // follow.
    let limit = u64::MAX - 1 - (1u64 << 32);
    let img = craft_image(2, |m| m.txnid = limit, &[]);
    let env = open_image(img, "txnid-limit").expect("exactly at the bound still opens");
    let r = env.read_txn().expect("reads work");
    drop(r);
    let e = env
        .write_txn()
        .err()
        .expect("no writer may start at the bound");
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
    // One below the bound a writer may still start (it would commit `limit`,
    // after which the check above applies). The in-memory backing here is
    // read-only, so the commit itself is not exercised in this harness.
    let img = craft_image(2, |m| m.txnid = limit - 1, &[]);
    let env = open_image(img, "txnid-limit-1").unwrap();
    assert!(
        env.write_txn().is_ok(),
        "one below the bound: a writer may start"
    );
}

#[test]
fn open_rejects_root_beyond_last_pg() {
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 200; // within the map, past the high-water
            m.main_db.depth = 1;
            m.main_db.leaf_pages = 1;
            m.main_db.entries = 1;
        },
        &[],
    );
    let e = open_image(img, "root-oob").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");

    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.free_db.root = 0; // a meta slot as GC root
            m.free_db.depth = 1;
            m.free_db.leaf_pages = 1;
        },
        &[],
    );
    let e = open_image(img, "gcroot-oob").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn open_accepts_valid_geometry() {
    let leaf = leaf_frame(2, 1, b"k", b"v");
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = 1;
            m.main_db.leaf_pages = 1;
            m.main_db.entries = 1;
        },
        &[(2, leaf)],
    );
    let env = open_image(img, "ok").unwrap();
    let txn = env.read_txn().unwrap();
    let db = env.main_database();
    assert_eq!(db.get(&txn, b"k").unwrap(), Some(&b"v"[..]));
}

// ---------------------------------------------------------------------------
// H1b — the read-side page resolver refuses pgnos beyond the snapshot
// high-water (typed error, not an out-of-bounds map read)
// ---------------------------------------------------------------------------

#[test]
fn read_refuses_reference_past_high_water() {
    // Open-time validation only sees the roots; a *deeper* hostile reference
    // (here: an overflow head past last_pg) must be refused by the resolver.
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
    leaf.insert_bigdata(0, b"big", 64, 100).unwrap(); // head pgno 100 > last_pg 2
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = 1;
            m.main_db.leaf_pages = 1;
            m.main_db.overflow_pages = 1;
            m.main_db.entries = 1;
        },
        &[(2, buf)],
    );
    let env = open_image(img, "ovf-oob").unwrap();
    let txn = env.read_txn().unwrap();
    let db = env.main_database();
    let e = db.get(&txn, b"big").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

// ---------------------------------------------------------------------------
// H2 — a branch page with zero children is rejected at decode
// ---------------------------------------------------------------------------

#[test]
fn zero_child_branch_fails_decode() {
    let mut buf = vec![0u8; PS as usize];
    BranchMut::init(&mut buf, PS, 2, 1).unwrap(); // 0 children
    let e = BranchRef::new(&buf, PS).unwrap_err();
    assert!(matches!(e, PageError::EmptyBranch), "got {e:?}");
}

#[test]
fn zero_child_branch_as_root_reads_typed_error() {
    let mut buf = vec![0u8; PS as usize];
    BranchMut::init(&mut buf, PS, 2, 1).unwrap();
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = 2;
            m.main_db.branch_pages = 1;
        },
        &[(2, buf)],
    );
    let env = open_image(img, "empty-branch").unwrap();
    let txn = env.read_txn().unwrap();
    let db = env.main_database();
    // Pre-fix: BranchRef::new accepted the page and descend_min's
    // `child_pgno(0)` panicked on the index assert.
    assert!(db.get(&txn, b"k").is_err());
    assert!(db.first(&txn).is_err());
    assert!(db.last(&txn).is_err());
}

// ---------------------------------------------------------------------------
// H4 — a hostile GC freelist must not hand out pages
// ---------------------------------------------------------------------------

/// An image whose GC DB holds one entry `{txnid 1 -> pil}`.
fn gc_image(pil_ids: &[u64]) -> Vec<u8> {
    let mut pil = Vec::with_capacity(8 + 8 * pil_ids.len());
    pil.extend_from_slice(&(pil_ids.len() as u64).to_le_bytes());
    for id in pil_ids {
        pil.extend_from_slice(&id.to_le_bytes());
    }
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
    leaf.insert_inline(0, &1u64.to_be_bytes(), 0, &pil).unwrap();
    craft_image(
        3,
        |m| {
            m.txnid = 2;
            m.last_pg = 2;
            m.free_db.root = 2;
            m.free_db.depth = 1;
            m.free_db.leaf_pages = 1;
            m.free_db.entries = 1;
        },
        &[(2, buf)],
    )
}

#[test]
fn gc_freelist_naming_meta_page_is_invalid() {
    // A hostile PIL listing page 0 (a meta slot). Pre-fix `gc_reclaim` handed
    // it out and commit C2 clobbered the meta. The allocation-triggering put
    // must fail typed instead.
    let env = open_image(gc_image(&[0]), "gc-meta").unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    let e = db.put(&mut txn, b"k", b"v").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn gc_freelist_past_high_water_is_invalid() {
    // A PIL naming a page beyond the committed file: under WRITE_MAP that is
    // a write into unbacked map bytes; under pwrite a silent file extension
    // over unallocated space.
    let env = open_image(gc_image(&[200]), "gc-oob").unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    let e = db.put(&mut txn, b"k", b"v").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn gc_freelist_unsorted_is_invalid() {
    // find_run/binary_search assume ascending ids; unsorted input previously
    // reached an `expect` (panic) instead of a typed error. Both ids are in
    // range (last_pg = 5 below), so only the ordering check can reject them.
    let ps = PS as usize;
    let mut pil = Vec::new();
    pil.extend_from_slice(&2u64.to_le_bytes());
    pil.extend_from_slice(&4u64.to_le_bytes());
    pil.extend_from_slice(&3u64.to_le_bytes());
    let mut leaf_buf = vec![0u8; ps];
    let mut leaf = LeafMut::init(&mut leaf_buf, PS, 2, 1).unwrap();
    leaf.insert_inline(0, &1u64.to_be_bytes(), 0, &pil).unwrap();
    let img = craft_image(
        6,
        |m| {
            m.txnid = 2;
            m.last_pg = 5;
            m.free_db.root = 2;
            m.free_db.depth = 1;
            m.free_db.leaf_pages = 1;
            m.free_db.entries = 1;
        },
        &[(2, leaf_buf)],
    );
    let env = open_image(img, "gc-unsorted").unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    let e = db.put(&mut txn, b"k", b"v").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

// ---------------------------------------------------------------------------
// M7/M8 — hostile depth: bounded everywhere, typed errors
// ---------------------------------------------------------------------------

/// A self-cycle branch (child 0 -> itself): with a hostile huge `depth` this
/// is the unbounded-descent shape.
fn cycle_branch_frame(pgno: u64, txnid: u64) -> Vec<u8> {
    let mut buf = vec![0u8; PS as usize];
    let mut br = BranchMut::init(&mut buf, PS, pgno, txnid).unwrap();
    br.insert(0, &[], pgno).unwrap();
    br.insert(1, b"m", pgno).unwrap();
    buf
}

#[test]
fn hostile_depth_read_and_write_paths_fail_typed() {
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = u16::MAX; // hostile on-disk depth
            m.main_db.branch_pages = 1;
        },
        &[(2, cycle_branch_frame(2, 1))],
    );
    let env = open_image(img, "depth").unwrap();
    // Read path: the cursor's fixed CURSOR_STACK bound fails typed.
    {
        let txn = env.read_txn().unwrap();
        let db = env.main_database();
        assert!(db.get(&txn, b"k").is_err());
        assert!(db.first(&txn).is_err());
    }
    // Write path: search_path / rightmost_path gate the on-disk depth before
    // building a path (pre-fix: a 65k-frame heap path whose delete-side
    // rebalance recursion overflows the stack).
    {
        let mut txn = env.write_txn().unwrap();
        let db = env.main_database();
        assert!(db.put(&mut txn, b"k", b"v").is_err());
        assert!(db.delete(&mut txn, b"k").is_err());
    }
    // clear() walks the tree recursively from the on-disk depth too
    // (collect_tree); it must fail typed, not overflow the stack.
    {
        let mut txn = env.write_txn().unwrap();
        let db = env.main_database();
        assert!(db.clear(&mut txn).is_err());
    }
}

#[test]
fn hostile_stat_counters_do_not_panic_the_write_path() {
    // Found by fuzz_image_open: DBRecord stats are on-disk data; a hostile
    // `entries = u64::MAX` (or 0) overflow/underflow-panicked the write
    // path's `+= 1` / `-= 1` bookkeeping in debug builds. The mutation must
    // proceed (stats saturate; INV-18 reports the drift), never panic.
    let make = |entries: u64| {
        craft_image(
            3,
            |m| {
                m.txnid = 1;
                m.last_pg = 2;
                m.main_db.root = 2;
                m.main_db.depth = 1;
                m.main_db.leaf_pages = 1;
                m.main_db.entries = entries;
            },
            &[(2, leaf_frame(2, 1, b"k", b"v"))],
        )
    };
    // Overflowing increment on insert.
    let env = open_image(make(u64::MAX), "stat-max").unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    db.put(&mut txn, b"z", b"v").unwrap();
    drop(txn);
    drop(env);
    // Underflowing decrement on delete.
    let env = open_image(make(0), "stat-zero").unwrap();
    let mut txn = env.write_txn().unwrap();
    let db = env.main_database();
    assert!(db.delete(&mut txn, b"k").unwrap());
}

// ---------------------------------------------------------------------------
// H3 — the checker on hostile geometry: fast, checked, early-returning
// ---------------------------------------------------------------------------

#[test]
fn checker_survives_last_pg_u64_max() {
    // Pre-fix: `(last_pg + 1) * ps` wrapped, INV-17 did not return, and the
    // reachable-XOR-free sweep looped ~2^64 times pushing a String per page.
    let img = craft_image(2, |m| m.last_pg = u64::MAX, &[]);
    let v = check_image(&img, PS);
    assert!(
        v.iter().any(|s| s.starts_with("INV-17")),
        "expected INV-17, got {v:?}"
    );
}

#[test]
fn checker_bounds_hostile_depth() {
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = u16::MAX;
            m.main_db.branch_pages = 1;
        },
        &[(2, cycle_branch_frame(2, 1))],
    );
    // Pre-fix: `walk` recursed one frame per level -> stack overflow.
    let v = check_image(&img, PS);
    assert!(
        v.iter()
            .any(|s| s.contains("exceeds the maximum legal tree depth")),
        "expected the depth violation, got {v:?}"
    );
}

#[test]
fn checker_survives_hostile_overflow_run() {
    // An overflow head whose `head + expect - 1` wraps u64 must be a
    // violation, not an arithmetic overflow panic.
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
    leaf.insert_bigdata(0, b"big", u32::MAX, u64::MAX).unwrap();
    let img = craft_image(
        3,
        |m| {
            m.txnid = 1;
            m.last_pg = 2;
            m.main_db.root = 2;
            m.main_db.depth = 1;
            m.main_db.leaf_pages = 1;
            m.main_db.entries = 1;
        },
        &[(2, buf)],
    );
    let v = check_image(&img, PS);
    assert!(
        v.iter().any(|s| s.starts_with("INV-11")),
        "expected INV-11, got {v:?}"
    );
}

// ---------------------------------------------------------------------------
// Low-1 — `LeafMut::remove` over an unvalidated cell returns a typed error
// ---------------------------------------------------------------------------

#[test]
fn leaf_remove_on_corrupt_cell_is_typed() {
    let mut buf = vec![0u8; PS as usize];
    {
        let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
        leaf.insert_inline(0, b"key", 0, b"value").unwrap();
    }
    // Corrupt the cell's dsize so it runs past the page body. `from_valid`
    // is O(1) structural checks only (PERF-GAP A8) and is `pub`, so the cell
    // walk cannot be assumed; pre-fix `remove` hit an `expect` (panic).
    let lower = u16::from_le_bytes([buf[24], buf[25]]) as usize;
    assert_eq!(lower, 2, "one pointer");
    let cpos = u16::from_le_bytes([buf[32], buf[33]]) as usize;
    let abs = 32 + cpos; // HEADER_SIZE + body-relative cell offset
    buf[abs + 4..abs + 8].copy_from_slice(&u32::MAX.to_le_bytes()); // dsize
    let mut leaf = LeafMut::from_valid(&mut buf, PS).unwrap();
    let e = leaf.remove(0).unwrap_err();
    assert!(matches!(e, PageError::CellOutOfBounds { .. }), "got {e:?}");
}

// ---------------------------------------------------------------------------
// M6 — max_readers / max_dbs upper bounds
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_unbounded_max_readers_and_max_dbs() {
    // Pre-fix: `max_readers(u32::MAX)` eagerly allocated u32::MAX cache-padded
    // reader slots -> allocation-failure abort. Must be a fast typed error.
    let img = craft_image(2, |_| {}, &[]);
    let e = open_with_backing(
        unique_path("readers-max"),
        Box::new(VecBacking(img.clone())),
        PS,
        MAP,
        false,
        8,
        u32::MAX,
        DurabilityFlags::default(),
    )
    .unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    let e = open_with_backing(
        unique_path("dbs-max"),
        Box::new(VecBacking(img)),
        PS,
        MAP,
        false,
        u32::MAX,
        8,
        DurabilityFlags::default(),
    )
    .unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    // The documented bounds themselves stay accepted (boundary check) — use
    // small values here so the test does not allocate 128 MiB of slots.
    let img2 = craft_image(2, |_| {}, &[]);
    assert!(open_with_backing(
        unique_path("readers-ok"),
        Box::new(VecBacking(img2)),
        PS,
        MAP,
        false,
        8,
        1024,
        DurabilityFlags::default(),
    )
    .is_ok());
}

#[test]
fn sanity_pgno_invalid_is_not_a_root() {
    // Guard the craft helper's assumption: a fresh meta has no roots.
    let m = MetaPage::create(0, PS, MAP);
    assert_eq!(m.main_db.root, PGNO_INVALID);
    assert_eq!(m.free_db.root, PGNO_INVALID);
}
