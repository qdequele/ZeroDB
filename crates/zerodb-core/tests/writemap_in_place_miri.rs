//! ADR-0021 M1 — the in-place `WRITE_MAP` brokered discipline under
//! **miri** (SPEC 04 §6.4 TXN-45b; `cargo +nightly miri test -p zerodb-core`).
//!
//! The real writable map is OS memory miri cannot see, so this battery runs
//! the full write-txn surface over `zerodb_io::testmap::TestWriteMap` — an
//! `UnsafeCell<Box<[u8]>>`-backed `Backing` with `dirty_in_map() = true`
//! whose brokered `&mut` regions and whole-map `&[u8]` views are plain heap
//! memory Stacked/Tree Borrows fully check. What this proves on every run:
//!
//! - the brokered `&mut` (ADR-0021 B1) is exclusive in practice: no two live
//!   mutable views of one region, every view transient and re-tied to
//!   `&mut` on the dirty store (TXN-39/41/42);
//! - the writer's whole-map `&[u8]` is re-derived at every spill (ADR-0021
//!   B2), so no stale shared borrow is ever read at locations an in-place
//!   write touched (TXN-71) — the exact Tree-Borrows hazard the ADR review
//!   flagged;
//! - readers holding snapshots across in-place commits, nested read children
//!   of the writer, `put_reserved` closures, the general split's scratch
//!   copy and abort-by-drop all stay inside the discipline.
//!
//! Sized for miri (hundreds of ops, 4 KiB pages, small map). The native
//! `cargo test` run executes it too, as a fast smoke. Do not weaken
//! (CLAUDE.md rule 2).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb_core::env::{open_with_backing_policy, DurabilityFlags, Env};
use zerodb_core::page::FileTrust;
use zerodb_core::rotxn::TxnRead;
use zerodb_io::testmap::TestWriteMap;

const PS: u32 = 4096;
const MAP: u64 = 4 << 20; // 1024 pages — plenty for the miri-sized workload.

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fresh in-memory in-place WRITE_MAP env (unique virtual registry path).
fn wm_env(dirty_limit: Option<u64>) -> Env {
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = PathBuf::from(format!("/virtual/wm-miri-{}-{seq}", std::process::id()));
    let backing = TestWriteMap::fresh_env(PS, MAP as usize);
    open_with_backing_policy(
        path,
        Box::new(backing),
        PS,
        MAP,
        false,
        8,
        16,
        DurabilityFlags {
            write_map: true,
            ..DurabilityFlags::default()
        },
        FileTrust::VALIDATE,
        false,
        dirty_limit,
    )
    .expect("open in-place writemap env")
}

/// xorshift64 — deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn contents(txn: &impl TxnRead, db: zerodb_core::rotxn::Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(txn)
        .map(|e| {
            let (k, v) = e.expect("iter entry");
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// The mixed-op discipline run: COW touches, fresh pages (splits), overflow
/// runs, `put_reserved`, deletes, nested reads, writer reads, readers pinned
/// across commits, one aborted txn.
#[test]
fn in_place_discipline_under_miri() {
    let env = wm_env(None);
    let db = env.main_database();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(0xADB0_0021_0000_0001);

    // A reader pinned before anything commits: its (empty) snapshot must
    // survive every in-place write below (TXN-62 applied at store time).
    let r0 = env.read_txn().expect("reader 0");

    for round in 0..3u64 {
        let mut w = env.write_txn().expect("write txn");
        assert!(w.dirty_in_map_mode(), "in-place WRITE_MAP must be active");
        for step in 0..60u64 {
            let k = format!("key-{:04}", rng.below(160)).into_bytes();
            match rng.below(10) {
                0..=5 => {
                    // Inline put; every ~8th an overflow run (2–3 pages).
                    let len = if rng.below(8) == 0 {
                        5_000 + rng.below(6_000) as usize
                    } else {
                        40 + rng.below(300) as usize
                    };
                    let v: Vec<u8> = format!("v{round}.{step}-")
                        .bytes()
                        .cycle()
                        .take(len)
                        .collect();
                    db.put(&mut w, &k, &v).expect("put");
                    model.insert(k, v);
                }
                6 => {
                    let len = 24 + rng.below(2_000) as usize;
                    let v: Vec<u8> = format!("r{round}.{step}-")
                        .bytes()
                        .cycle()
                        .take(len)
                        .collect();
                    db.put_reserved(&mut w, &k, len, |buf| buf.copy_from_slice(&v))
                        .expect("put_reserved");
                    model.insert(k, v);
                }
                7 => {
                    let was = db.delete(&mut w, &k).expect("delete");
                    assert_eq!(was, model.remove(&k).is_some(), "delete step {step}");
                }
                8 => {
                    // Nested read child sees the writer's in-map dirty state.
                    let child = w.nested_read_txn().expect("nested child");
                    assert_eq!(
                        db.get(&child, &k).expect("nested get"),
                        model.get(&k).map(Vec::as_slice),
                        "nested read, step {step}"
                    );
                }
                _ => {
                    assert_eq!(
                        db.get(&w, &k).expect("writer get"),
                        model.get(&k).map(Vec::as_slice),
                        "writer read, step {step}"
                    );
                }
            }
        }
        let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(contents(&w, db), want, "writer view before commit");
        w.commit().expect("commit");
    }

    // The pre-everything reader still sees the empty tree.
    assert_eq!(contents(&r0, db), Vec::new(), "r0 snapshot intact");
    drop(r0);

    // An aborted in-place txn leaves the last commit intact (TXN-45b abort =
    // don't advance the meta; the scribbled pages are unreferenced).
    let before: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let r1 = env.read_txn().expect("reader 1");
    let mut w = env.write_txn().expect("abort txn");
    for i in 0..80u64 {
        let v = if i % 20 == 0 {
            vec![b'T'; 6_000]
        } else {
            vec![b't'; 200]
        };
        db.put(&mut w, format!("temp{i:04}").as_bytes(), &v)
            .expect("put in doomed txn");
    }
    w.abort();
    assert_eq!(contents(&r1, db), before, "reader across the abort");
    drop(r1);
    assert_eq!(
        contents(&env.read_txn().expect("reader 2"), db),
        before,
        "post-abort state"
    );
}

/// Spill/unspill as pure bookkeeping (TXN-68..72 degenerate under TXN-45b)
/// plus the ADR-0021 B2 re-derivation: with a tiny dirty limit nearly every
/// mutating call spills, spilled pages are resolved through the (re-derived)
/// whole-map view, and touches of spilled pages unspill in place.
#[test]
fn spill_unspill_in_place_under_miri() {
    let env = wm_env(Some(8)); // far below SPILL_NEED: spill on nearly every call
    let db = env.main_database();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(0xADB0_0021_0000_0002);

    let mut w = env.write_txn().expect("write txn");
    assert!(w.dirty_in_map_mode(), "in-place WRITE_MAP must be active");
    for i in 0..90u64 {
        let k = format!("spill-{:03}", rng.below(70)).into_bytes();
        let v = if i % 25 == 24 {
            vec![b'O'; 5_500] // overflow run among the spills
        } else {
            format!("val-{i}-")
                .bytes()
                .cycle()
                .take(120 + rng.below(500) as usize)
                .collect()
        };
        db.put(&mut w, &k, &v).expect("put");
        model.insert(k.clone(), v);
        if i % 9 == 8 {
            // Read back through the source: spilled pages resolve from the
            // re-derived map view (TXN-71 / ADR-0021 B2).
            assert_eq!(
                db.get(&w, &k).expect("get"),
                model.get(&k).map(Vec::as_slice)
            );
        }
    }
    assert!(w.spill_count() > 0, "the tiny limit must have spilled");
    // Rewrite keys whose leaves are (likely) spilled: unspill-in-place.
    for i in 0..18u64 {
        let k = format!("spill-{:03}", i * 3).into_bytes();
        db.put(&mut w, &k, b"rewritten").expect("rewrite");
        model.insert(k, b"rewritten".to_vec());
    }
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(contents(&w, db), want, "writer view with spills");
    w.commit().expect("commit");
    assert_eq!(
        contents(&env.read_txn().expect("reader"), db),
        want,
        "committed state"
    );
}
