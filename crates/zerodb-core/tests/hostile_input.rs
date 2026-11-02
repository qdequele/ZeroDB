//! Hostile / corrupt on-disk input must yield a **typed error** — never a
//! panic, hang, unbounded allocation, out-of-bounds read, or (through the
//! real mmap backing) a SIGBUS. These tests craft images by hand (valid CRCs,
//! hostile geometry) and drive them through the same `open_with_backing` /
//! txn entry points the real I/O layer uses, over a heap backing so the whole
//! suite also runs under miri.
//!
//! Covers truncated-file geometry (open validation + the read-side `last_pg`
//! bound), zero-child branches, hostile GC freelists, hostile tree depths,
//! hostile record stats, and the checker's checked arithmetic.

use zerodb_core::check::check_image;
use zerodb_core::env::testutil::VecBacking;
use zerodb_core::env::{open_with_backing, DurabilityFlags};
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::{
    geometry, BranchMut, BranchRef, LeafMut, MetaPage, PageError, PGNO_INVALID,
};

mod common;
use common::{craft_image, leaf_frame, open_image, unique_path, MAP, PS};

// ---------------------------------------------------------------------------
// Open-time geometry validation (SPEC 06 REC-1a / SPEC 02 §3.2 step 6)
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_last_pg_past_file_end() {
    // Valid CRC, but last_pg names pages the 2-page "file" cannot back. On a
    // real env the mapping covers the whole map_size, so without this check
    // the first read of such a page would SIGBUS on unbacked bytes.
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
    // The reader table encodes slot occupancy in the top-of-u64 sentinel band
    // (RDR_FREE/RDR_CLAIMED), so a hostile meta txnid up there would break
    // the pin protocol's TXN-14 assumption (debug panic) or alias the
    // sentinels (release); the writer's `txnid + 1` can also wrap. Must be
    // rejected at open.
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
    // An open-time check alone is one commit deep — a
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
// The read-side page resolver refuses pgnos beyond the snapshot
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
// A branch page with zero children is rejected at decode
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
    // Regression: descend_min's `child_pgno(0)` must not reach its index
    // assert on a zero-child branch.
    assert!(db.get(&txn, b"k").is_err());
    assert!(db.first(&txn).is_err());
    assert!(db.last(&txn).is_err());
}

// ---------------------------------------------------------------------------
// A hostile GC freelist must not hand out pages
// ---------------------------------------------------------------------------

/// The 16-byte GC key `(txnid, seq)` (SPEC 02 §7).
fn gc_key(txnid: u64, seq: u64) -> Vec<u8> {
    geometry::gc_key_encode(geometry::GcKey { txnid, seq }).to_vec()
}

/// An image of `file_pages` pages whose GC DB holds one record `key -> pil`
/// on page 2, committed by txn 2.
fn gc_record_image(file_pages: usize, key: &[u8], pil_ids: &[u64]) -> Vec<u8> {
    let mut pil = Vec::with_capacity(8 + 8 * pil_ids.len());
    pil.extend_from_slice(&(pil_ids.len() as u64).to_le_bytes());
    for id in pil_ids {
        pil.extend_from_slice(&id.to_le_bytes());
    }
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
    leaf.insert_inline(0, key, 0, &pil).unwrap();
    craft_image(
        file_pages,
        |m| {
            m.txnid = 2;
            m.last_pg = file_pages as u64 - 1;
            m.free_db.root = 2;
            m.free_db.depth = 1;
            m.free_db.leaf_pages = 1;
            m.free_db.entries = 1;
        },
        &[(2, buf)],
    )
}

/// An image whose GC DB holds one record `{(txnid 1, seq 0) -> pil}`.
fn gc_image(pil_ids: &[u64]) -> Vec<u8> {
    gc_record_image(3, &gc_key(1, 0), pil_ids)
}

/// A put in a write txn of `img`: it allocates, so it reads the GC record.
fn put_after_reading_the_gc_record(img: Vec<u8>, tag: &str) -> Result<(), Error> {
    let env = open_image(img, tag).unwrap();
    let mut txn = env.write_txn().unwrap();
    env.main_database().put(&mut txn, b"k", b"v")
}

#[test]
fn gc_freelist_naming_meta_page_is_invalid() {
    // A hostile PIL listing page 0 (a meta slot). Handing it out would let
    // commit C2 clobber the meta; the allocation-triggering put must fail
    // typed instead.
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
    // find_run/binary_search assume ascending ids; unsorted input must yield
    // a typed error, not reach an `expect` (panic). Both ids are in
    // range (last_pg = 5 below), so only the ordering check can reject them.
    let ps = PS as usize;
    let mut pil = Vec::new();
    pil.extend_from_slice(&2u64.to_le_bytes());
    pil.extend_from_slice(&4u64.to_le_bytes());
    pil.extend_from_slice(&3u64.to_le_bytes());
    let mut leaf_buf = vec![0u8; ps];
    let mut leaf = LeafMut::init(&mut leaf_buf, PS, 2, 1).unwrap();
    leaf.insert_inline(0, &gc_key(1, 0), 0, &pil).unwrap();
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

#[test]
fn gc_record_with_a_format_2_key_is_invalid() {
    // An 8-byte key (format 2) in a format-3 file: the record's freeing txn
    // cannot be read, so nothing in it can pass the reuse gate.
    let img = gc_record_image(6, &1u64.to_be_bytes(), &[3]);
    let e = put_after_reading_the_gc_record(img, "gc-v2-key").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn gc_record_listing_an_id_below_its_key_is_invalid() {
    // INV-29: a record keyed (F, 4) lists only ids >= 4. Its GC-20 rewrite
    // would write that id under the same key, so the read refuses it.
    let ok = gc_record_image(6, &gc_key(1, 3), &[3]);
    put_after_reading_the_gc_record(ok, "gc-seq-ok").expect("id at its key's seq");
    let img = gc_record_image(6, &gc_key(1, 4), &[3]);
    let e = put_after_reading_the_gc_record(img, "gc-seq-below").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

#[test]
fn gc_record_longer_than_the_cap_is_invalid() {
    // GC-5: a record holds at most `gc_chunk_cap` ids (249 at 4 KiB), so a
    // hostile record cannot make the writer decode an unbounded list.
    let cap = geometry::gc_chunk_cap(PS) as u64;
    let ids: Vec<u64> = (3..3 + cap).collect();
    let ok = gc_record_image(3 + cap as usize, &gc_key(1, 0), &ids);
    put_after_reading_the_gc_record(ok, "gc-cap-ok").expect("a record of the cap");
    let ids: Vec<u64> = (3..4 + cap).collect();
    let img = gc_record_image(4 + cap as usize, &gc_key(1, 0), &ids);
    let e = put_after_reading_the_gc_record(img, "gc-cap-over").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}

// ---------------------------------------------------------------------------
// Hostile depth: bounded everywhere, typed errors
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
    // building a path (otherwise: a 65k-frame heap path whose delete-side
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
    // DBRecord stats are on-disk data; a hostile `entries = u64::MAX` (or 0)
    // must not overflow/underflow-panic the write path's `+= 1` / `-= 1`
    // bookkeeping in debug builds. The mutation must proceed (stats
    // saturate; INV-18 reports the drift), never panic.
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
// The checker on hostile geometry: fast, checked, early-returning
// ---------------------------------------------------------------------------

#[test]
fn checker_survives_last_pg_u64_max() {
    // Guards against `(last_pg + 1) * ps` wrapping and INV-17 not returning,
    // which would loop the reachable-XOR-free sweep ~2^64 times pushing a
    // String per page.
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
    // `walk` recurses one frame per level: an unbounded depth would overflow
    // the stack.
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
// `LeafMut::remove` over an unvalidated cell returns a typed error
// ---------------------------------------------------------------------------

#[test]
fn leaf_remove_on_corrupt_cell_is_typed() {
    let mut buf = vec![0u8; PS as usize];
    {
        let mut leaf = LeafMut::init(&mut buf, PS, 2, 1).unwrap();
        leaf.insert_inline(0, b"key", 0, b"value").unwrap();
    }
    // Corrupt the cell's dsize so it runs past the page body. `from_valid`
    // is O(1) structural checks only and is `pub`, so the cell
    // walk cannot be assumed; `remove` must not hit an `expect` (panic).
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
// max_readers / max_dbs upper bounds
// ---------------------------------------------------------------------------

#[test]
fn open_rejects_unbounded_max_readers_and_max_dbs() {
    // `max_readers(u32::MAX)` would eagerly allocate u32::MAX cache-padded
    // reader slots -> allocation-failure abort. Must be a fast typed error.
    let img = craft_image(2, |_| {}, &[]);
    let e = open_with_backing(
        unique_path("readers-max"),
        Box::new(VecBacking::new(img.clone(), MAP)),
        PS,
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
        Box::new(VecBacking::new(img, MAP)),
        PS,
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
        Box::new(VecBacking::new(img2, MAP)),
        PS,
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
