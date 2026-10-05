//! The leaf-skipping `clear` (SPEC 02 §6.1, LMDB `mdb_drop0`)
//! frees the lowest branch level's children WITHOUT reading them, so its
//! bound check is the only thing between a crafted branch pointer and the
//! free list. A child naming a meta slot (page 0/1), a pgno past the
//! committed high-water, or the same leaf twice MUST yield the typed
//! `MdbError::Invalid` (poisoning the txn, freeing nothing) — never feed the
//! allocator. Crafted images over a heap backing, as `hostile_input.rs` (the
//! suite also runs under miri). Do not weaken (AGENTS.md rule 2).

use zerodb_core::env::testutil::VecBacking;
use zerodb_core::env::{open_with_backing, DurabilityFlags, Env};
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::{BranchMut, LeafMut, MetaPage};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const PS: u32 = 4096;
const MAP: u64 = 1 << 20; // 256 pages

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_path(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!(
        "/virtual/hostile-clear-{tag}-{}-{n}",
        std::process::id()
    ))
}

/// As `hostile_input.rs::craft_image`: both meta slots from `edit(meta)`,
/// then each `(pgno, frame)` copied in.
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

/// A two-child branch frame `{[] -> left, "m" -> right}` stamped `(pgno, 1)`.
fn branch_frame(pgno: u64, left: u64, right: u64) -> Vec<u8> {
    let mut buf = vec![0u8; PS as usize];
    let mut br = BranchMut::init(&mut buf, PS, pgno, 1).unwrap();
    br.insert(0, &[], left).unwrap();
    br.insert(1, b"m", right).unwrap();
    buf
}

/// A depth-2 image (branch at 2 over children `left`/`right`), main record
/// claiming no overflow — the leaf-skipping path. Real leaves exist at 3/4;
/// hostile variants point the branch elsewhere.
fn two_level_image(left: u64, right: u64) -> Vec<u8> {
    craft_image(
        5,
        |m| {
            m.txnid = 1;
            m.last_pg = 4;
            m.main_db.root = 2;
            m.main_db.depth = 2;
            m.main_db.branch_pages = 1;
            m.main_db.leaf_pages = 2;
            m.main_db.entries = 2;
        },
        &[
            (2, branch_frame(2, left, right)),
            (3, leaf_frame(3, 1, b"a", b"v1")),
            (4, leaf_frame(4, 1, b"m", b"v2")),
        ],
    )
}

/// The honest baseline: a well-formed two-level committed tree clears through
/// the leaf-skipping path (the leaves at 3/4 are never loaded) and the txn
/// stays fully usable. The heap backing is read-only, so the commit-side
/// freed-set accounting is pinned over real files in
/// `zerodb/tests/clear_leaf_skip.rs` (INV-22); this covers the in-txn half.
#[test]
fn clear_two_level_tree_skips_leaves_in_txn() {
    let env = open_image(two_level_image(3, 4), "ok").unwrap();
    let db = env.main_database();
    {
        let txn = env.read_txn().unwrap();
        assert_eq!(db.get(&txn, b"a").unwrap(), Some(&b"v1"[..]));
    }
    let mut txn = env.write_txn().unwrap();
    db.clear(&mut txn).unwrap();
    assert_eq!(db.len(&txn).unwrap(), 0);
    assert_eq!(db.get(&txn, b"a").unwrap(), None);
    assert_eq!(db.get(&txn, b"m").unwrap(), None);
    // Not poisoned: the tree rebuilds in the same txn.
    db.put(&mut txn, b"fresh", b"v3").unwrap();
    assert_eq!(db.get(&txn, b"fresh").unwrap(), Some(&b"v3"[..]));
}

/// The corruption guard: a lowest-level branch child naming a meta slot or a
/// pgno past the committed high-water must fail typed BEFORE anything is
/// freed — a meta page on the free list would be handed to the allocator and
/// overwritten (durable corruption). The txn is poisoned like every other
/// write-path corruption error.
#[test]
fn clear_refuses_unread_child_out_of_bounds() {
    // (pgno the branch points at, tag)
    for (bad, tag) in [(0u64, "meta0"), (1, "meta1"), (100, "past-hw")] {
        let env = open_image(two_level_image(bad, 4), tag).unwrap();
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let e = db.clear(&mut txn).unwrap_err();
        assert!(
            matches!(e, Error::Mdb(MdbError::Invalid)),
            "child {bad}: got {e:?}"
        );
        // Poisoned: no further op, and no commit, can push the collected
        // pages (or anything else) out of this txn.
        let e = db.put(&mut txn, b"k", b"v").unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadTxn)), "got {e:?}");
        let e = txn.commit().unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadTxn)), "got {e:?}");
    }
}

/// Aliasing guard: two branch pointers naming the SAME leaf would double-free
/// it unread. The collected set is refused as a whole (typed, nothing freed).
#[test]
fn clear_refuses_aliased_unread_child() {
    let env = open_image(two_level_image(3, 3), "alias").unwrap();
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let e = db.clear(&mut txn).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
    let e = txn.commit().unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::BadTxn)), "got {e:?}");
}

/// The hostile-depth guard carries over to the leaf-skipping walk: an on-disk
/// `depth` past the cursor bound fails typed, no unbounded recursion (the
/// no-overflow record routes `clear` through the new walk).
#[test]
fn clear_leaf_skip_bounds_hostile_depth() {
    let img = craft_image(
        5,
        |m| {
            m.txnid = 1;
            m.last_pg = 4;
            m.main_db.root = 2;
            m.main_db.depth = u16::MAX;
            m.main_db.branch_pages = 1;
        },
        &[(2, branch_frame(2, 3, 4))],
    );
    let env = open_image(img, "depth").unwrap();
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let e = db.clear(&mut txn).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)), "got {e:?}");
}
