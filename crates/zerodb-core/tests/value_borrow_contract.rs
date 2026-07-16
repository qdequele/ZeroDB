//! Milestone 1.4 coverage pass, area 2: SPEC 04 §6 (TXN-37..49) value-borrow
//! contract, exercised as a black-box consumer of `zerodb-core`'s public API
//! (no crate-internal access — same posture as `page_edges.rs`) so this file
//! runs under `cargo miri test -p zerodb-core` (deterministic, no mmap/file
//! I/O — every env here is the `env::testutil` heap-backed `VecBacking`).
//!
//! `crates/zerodb-core/src/rwtxn.rs`'s own `#[cfg(test)] mod tests` already
//! covers TXN-49 items 1/2/4 (`txn49_get_then_put`, `txn49_reserve_then_split`,
//! `txn49_frame_stability_across_index_growth`) — not duplicated here. This
//! file adds the two adversarial angles the coverage-pass brief calls out
//! that were NOT already pinned:
//!
//! 1. A `RoTxn`'s borrow into **already-committed** page bytes must stay
//!    valid and unmoved while a **separate, later** `RwTxn` COWs that same
//!    logical page (TXN-37/38: the reader's bytes come from the read-only
//!    backing, which the writer's private dirty store never touches).
//! 2. A borrow into a **dirty overflow run** frame must stay stable (address
//!    and bytes) across further allocations that grow the dirty-page index
//!    and dirty other, unrelated tree pages (TXN-41/44, the overflow-run
//!    variant of `txn49_frame_stability_across_index_growth`, which only
//!    exercised an inline/tree-page frame).
//!
//! ## Why a live borrow can't span a mutation (item 3's "document as a
//! comment" ask)
//!
//! `Database::get`/cursor reads take `&RwTxn`; every mutating op
//! (`put`/`put_reserved`/`delete`/`clear`/`RwCursor::put_current`/
//! `del_current`) takes `&mut RwTxn`. A `&'txn [u8]` returned by a read
//! therefore borrows the `RwTxn` immutably for its `'txn` lifetime; the
//! borrow checker will not allow a `&mut RwTxn` mutation to be called while
//! that immutable borrow is still live (E0502). This is SPEC 04 TXN-39
//! enforced entirely at compile time — no runtime check is possible or
//! needed, and no test can observe the "held across a mutation" case because
//! it is a compile error, not a runtime one. What tests here verify instead
//! is the *positive* half of the contract: a borrow that legitimately ends
//! before a mutation (or a `.to_vec()`-copied value that outlives it) sees
//! stable, correct bytes and is never invalidated by unrelated activity.

use zerodb_core::env::testutil::{mem_env, VecBacking};
use zerodb_core::env::{open_with_backing, Env};
use zerodb_core::page::{DBRecord, LeafMut, MetaPage, PGNO_INVALID};
use zerodb_core::rotxn::TxnRead;

const PS: u32 = 4096;

/// Build a raw two-meta-slot + one-leaf-page image with a single committed
/// entry `(key, val)` at txnid 0, and open it — `env::testutil` only ships an
/// *empty* `mem_env`; this is the same raw-image technique
/// `zerodb-core/src/env.rs`'s own meta-selection tests and `check.rs`'s use,
/// applied through the public API so it works from an external test crate.
fn committed_env_with_one_entry(map_size: u64, key: &[u8], val: &[u8]) -> Env {
    let ps = PS as usize;
    let leaf_pgno = 2u64; // just past the two meta slots
    let mut buf = vec![0u8; map_size as usize];

    // The leaf page, stamped as committed at txnid 0.
    {
        let leaf_buf = &mut buf[leaf_pgno as usize * ps..(leaf_pgno as usize + 1) * ps];
        let mut leaf = LeafMut::init(leaf_buf, PS, leaf_pgno, 0).expect("init leaf");
        leaf.insert_inline(0, key, 0, val).expect("insert entry");
    }

    // Both meta slots point at the same committed state (SPEC 02 §3.4 shape,
    // txnid 0 for both — mirrors `MetaPage::create`'s fresh-env baseline but
    // with a populated main_db instead of `DBRecord::empty()`).
    let main_db = DBRecord {
        root: leaf_pgno,
        branch_pages: 0,
        leaf_pages: 1,
        overflow_pages: 0,
        entries: 1,
        depth: 1,
        flags: 0,
        leaf2_ksize: 0,
    };
    for slot in [0u64, 1] {
        let mut meta = MetaPage::create(slot, PS, map_size);
        meta.last_pg = leaf_pgno;
        meta.main_db = main_db;
        let base = slot as usize * ps;
        meta.encode(&mut buf[base..base + ps]).expect("encode meta");
    }

    let path = std::path::PathBuf::from(format!(
        "/virtual/value-borrow-contract-{:p}",
        &buf as *const _
    ));
    open_with_backing(
        path,
        Box::new(VecBacking(buf)),
        PS,
        map_size,
        false,
        128,
        126,
    )
    .expect("open pre-baked committed image")
}

/// Item 1: a `RoTxn` opened against a committed page must keep serving the
/// exact same bytes at the exact same address after a *separate* `RwTxn`
/// COWs that page — the writer's COW allocates a brand-new frame in its own
/// private `DirtyStore`; it must never touch the read-only backing bytes the
/// reader is borrowing from (TXN-37, TXN-38's "a caller cannot tell which,
/// but the distinction is invisible and *safe*" — safe specifically because
/// the reader's storage is untouched).
#[test]
fn ro_txn_borrow_stable_across_later_rw_txn_cow() {
    let key = b"committed-key";
    let val = b"committed-value-untouched-by-writer";
    let env = committed_env_with_one_entry(1 << 20, key, val);
    let db = env.main_database();

    let rtxn = env.read_txn().unwrap();
    let borrowed: &[u8] = db.get(&rtxn, key).unwrap().unwrap();
    let addr_before = borrowed.as_ptr() as usize;
    assert_eq!(borrowed, val.as_slice());

    // A separate write txn COWs the same leaf (any mutation touching it —
    // here, replacing the same key with a different-length value forces a
    // remove+reinsert, guaranteeing the leaf is dirtied).
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, key, b"a-completely-different-longer-value")
            .unwrap();
        db.put(&mut wtxn, b"another-key", b"more").unwrap();
        // The writer's own view reflects its mutation (TXN-38) — proving the
        // COW actually ran, not that the put was a no-op.
        assert_eq!(
            db.get(&wtxn, key).unwrap(),
            Some(b"a-completely-different-longer-value".as_slice())
        );
        wtxn.abort(); // never committed: the reader's snapshot must be untouched either way.
    }

    // The reader's original borrow: same address, same bytes, unaffected by
    // the writer's (aborted) COW of the same logical page.
    let addr_after = borrowed.as_ptr() as usize;
    assert_eq!(addr_before, addr_after, "reader borrow address moved");
    assert_eq!(borrowed, val.as_slice(), "reader borrow bytes changed");
    // And an independent fresh read through the same (still-live) RoTxn
    // confirms the committed value is exactly what it was.
    assert_eq!(db.get(&rtxn, key).unwrap(), Some(val.as_slice()));
}

/// Item 2: a borrow into a dirty **overflow run** frame (TXN-41's contiguous
/// `N*psize` frame) must stay address-stable while further allocations grow
/// the dirty-page index (TXN-44) and dirty unrelated tree pages — the
/// overflow-run counterpart of `txn49_frame_stability_across_index_growth`
/// (which only covered a one-page tree frame).
#[test]
fn overflow_run_borrow_stable_across_further_allocation() {
    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();

    // A multi-page overflow value (well past the ~2 KiB inline threshold at
    // 4 KiB pages).
    let big = vec![0xABu8; 3 * PS as usize + 500];
    db.put(&mut txn, b"overflow-key", &big).unwrap();
    assert!(txn.main_record().overflow_pages >= 4);

    let borrowed = db.get(&txn, b"overflow-key").unwrap().unwrap();
    let addr_before = borrowed.as_ptr() as usize;
    let bytes_before = borrowed.to_vec();

    // Dirty many unrelated tree pages (forces leaf splits and grows the
    // pgno -> frame `HashMap` index well past its initial capacity).
    for i in 0..500u32 {
        let k = format!("filler-{i:05}").into_bytes();
        db.put(&mut txn, &k, &[0x11u8; 64]).unwrap();
    }
    // And allocate a second, unrelated overflow run too.
    db.put(
        &mut txn,
        b"another-overflow",
        &vec![0xCDu8; 2 * PS as usize],
    )
    .unwrap();

    let borrowed_after = db.get(&txn, b"overflow-key").unwrap().unwrap();
    assert_eq!(
        borrowed_after.as_ptr() as usize,
        addr_before,
        "overflow-run frame moved under further allocation"
    );
    assert_eq!(borrowed_after, bytes_before.as_slice());
}

/// Sanity check on the raw-image builder itself: the pre-baked one-entry
/// image must actually decode as a valid one-entry committed tree (not an
/// empty one) — guards against `committed_env_with_one_entry` silently
/// building a no-op image that would make the two tests above vacuous.
#[test]
fn raw_image_builder_baseline_is_empty_tree() {
    let env = committed_env_with_one_entry(1 << 20, b"only-key", b"only-value");
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.len(&rtxn).unwrap(), 1);
    assert_eq!(
        db.get(&rtxn, b"only-key").unwrap(),
        Some(b"only-value".as_slice())
    );
    assert_eq!(db.get(&rtxn, b"missing").unwrap(), None);
    assert_ne!(rtxn.main_record().root, PGNO_INVALID);
}
