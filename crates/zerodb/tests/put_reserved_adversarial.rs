//! `put_reserved` adversarial cases (SPEC 00 row 35, SPEC 01 §S3, SPEC 04
//! TXN-47) beyond `write_api.rs::put_reserved_through_commit` (exact
//! full-fill, no split) and `zerodb-core::rwtxn`'s `txn49_reserve_then_split`
//! (in-txn only, no commit):
//!
//! 1. Reserve exactly at the inline/overflow boundary (both sides), through
//!    commit + reopen.
//! 2. Reserve that forces a leaf split (the ADR-0004 §6.2 "zero-filled
//!    placeholder the caller then overwrites" path), through commit +
//!    reopen, so the placeholder is verified through the C0–C6 pipeline.
//! 3. Adversarial partial fill: the closure violates its "must fully write"
//!    contract (TXN-47) and only writes part of the reserved region. Only the
//!    *split* path is documented as zero-filled; TXN-47 says the engine "MUST
//!    NOT zero the reserved region" (parity with writemap peek), a
//!    *non-mandate* on the non-split path rather than a promise of any
//!    content. The test pins the **observed** behavior (not a spec guarantee)
//!    so a dirty-frame allocation change that starts leaking stale committed
//!    bytes into unfilled RESERVE regions shows up as a behavior change.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvFlags, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-reserve-adv-{pid}-{seq}"));
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

/// The battery runs in both dirty-page realizations (ADR-0021 M3): default
/// heap frames, and in-place `WRITE_MAP` (TXN-45b), where the reserved
/// slice the closure fills points straight into the writable map.
fn open_flags(dir: &Path, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.flags(flags);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

/// SPEC 02 §4.2 worked boundary at psize 4096: `max_node_size = 2030`; a
/// 1-byte key inlines up to `dsize = 2021` (`8+1+2021 = 2030`) and overflows
/// at `2022`.
#[test]
fn reserve_at_inline_overflow_boundary_both_sides() {
    reserve_at_boundary(EnvFlags::EMPTY);
}

/// ADR-0021 M3: the boundary reserves with the slice in the writable map.
#[test]
fn reserve_at_inline_overflow_boundary_both_sides_writemap_in_place() {
    reserve_at_boundary(EnvFlags::WRITE_MAP);
}

fn reserve_at_boundary(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open_flags(dir.path(), flags);
    let db = env.main_database();
    let inline_val: Vec<u8> = (0..2021u32).map(|i| (i % 250) as u8).collect();
    let overflow_val: Vec<u8> = (0..2022u32).map(|i| (i % 250) as u8).collect();

    let mut wtxn = env.write_txn().unwrap();
    assert_eq!(
        wtxn.dirty_in_map_mode(),
        flags.contains(EnvFlags::WRITE_MAP)
    );
    db.put_reserved(&mut wtxn, b"k", inline_val.len(), |buf| {
        buf.copy_from_slice(&inline_val);
    })
    .unwrap();
    db.put_reserved(&mut wtxn, b"o", overflow_val.len(), |buf| {
        buf.copy_from_slice(&overflow_val);
    })
    .unwrap();
    // In-txn: the inline one landed in the leaf, the overflow one on a run.
    assert_eq!(
        zerodb::TxnRead::main_record(&wtxn).overflow_pages,
        zerodb_core::page::geometry::overflow_page_count(overflow_val.len() as u64, PS)
    );
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), Some(inline_val.as_slice()));
    assert_eq!(db.get(&rtxn, b"o").unwrap(), Some(overflow_val.as_slice()));
}

#[test]
fn reserve_forcing_split_lands_correctly_through_commit_and_reopen() {
    reserve_forcing_split(EnvFlags::EMPTY);
}

/// ADR-0021 M3: the split-materializing reserve under in-place `WRITE_MAP`
/// (the general split's scratch copy of a map frame, then the placeholder
/// overwrite — all in the map), through commit + reopen.
#[test]
fn reserve_forcing_split_through_commit_and_reopen_writemap_in_place() {
    reserve_forcing_split(EnvFlags::WRITE_MAP);
}

fn reserve_forcing_split(flags: EnvFlags) {
    let dir = TempDir::new();
    {
        let env = open_flags(dir.path(), flags);
        let db = env.main_database();
        let mut wtxn = env.write_txn().unwrap();
        // Pack a leaf near-full with regular puts first (same technique as
        // `write_api.rs::split_exact_fit_boundary_case`, less exact).
        for i in 0..30u32 {
            let k = format!("neighbor-{i:03}").into_bytes();
            db.put(&mut wtxn, &k, &[0x11u8; 100]).unwrap();
        }
        let leaves_before = zerodb::TxnRead::main_record(&wtxn).leaf_pages;
        // A reserve landing in the middle of the key range, sized so this
        // insert cannot fit in the current leaf — forces §6.2's split, which
        // per the rwtxn.rs doc comment materializes the reserved slot as a
        // zero-filled placeholder the caller then overwrites via `f`.
        let payload = vec![0x99u8; 1500];
        db.put_reserved(&mut wtxn, b"neighbor-015-reserved", payload.len(), |buf| {
            buf.copy_from_slice(&payload);
        })
        .unwrap();
        let leaves_after = zerodb::TxnRead::main_record(&wtxn).leaf_pages;
        assert!(
            leaves_after > leaves_before,
            "reserve must have forced a split: {leaves_before} -> {leaves_after}"
        );
        assert_eq!(
            db.get(&wtxn, b"neighbor-015-reserved").unwrap(),
            Some(payload.as_slice()),
            "in-txn readback after the split-triggering reserve"
        );
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path());
    // Reopen: the split-materialized reserve must have survived the commit
    // pipeline (C0-C6) intact, not just the in-txn dirty-frame view.
    let env = open_flags(dir.path(), flags);
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"neighbor-015-reserved").unwrap(),
        Some(vec![0x99u8; 1500].as_slice())
    );
    // Every neighbor entry also survived the split + commit.
    for i in 0..30u32 {
        let k = format!("neighbor-{i:03}").into_bytes();
        assert_eq!(db.get(&rtxn, &k).unwrap(), Some([0x11u8; 100].as_slice()));
    }
}

/// Adversarial: the `put_reserved` closure only fills **half** the reserved
/// region, violating TXN-47's "the closure MUST fully write the reserved
/// bytes" contract. This is misuse — no engine code path enforces it — but
/// the behavior must still be *safe* (no crash/UB, checked further under
/// miri by `crates/zerodb-core/tests/value_borrow_contract.rs`) and
/// deterministic. A `put_reserved` that does **not** force a split lands in
/// a page frame that, for these setup conditions (fresh dirty leaf,
/// never-before-written region — `dirty.rs::DirtyStore::insert_tree_frame`
/// zero-initializes new frames), is zero-filled — **not** because the engine
/// zeros the RESERVE region (TXN-47 forbids that), but because the fresh
/// frame started as zero. This is an implementation detail, not a spec
/// promise (TXN-47 reserves the right to leave stale bytes); the test pins
/// the observed value so a change is visible, not a contract for callers.
#[test]
fn reserve_partial_fill_unfilled_tail_is_observed_deterministic() {
    reserve_partial_fill(EnvFlags::EMPTY);
}

/// ADR-0021 M3: same misuse under in-place `WRITE_MAP` — the fresh frame is
/// the zero-filled map page (`insert_tree_frame` zero-fills the map region
/// exactly as it zero-fills a heap frame), so the observed tail matches.
#[test]
fn reserve_partial_fill_unfilled_tail_writemap_in_place() {
    reserve_partial_fill(EnvFlags::WRITE_MAP);
}

fn reserve_partial_fill(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open_flags(dir.path(), flags);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    assert_eq!(
        wtxn.dirty_in_map_mode(),
        flags.contains(EnvFlags::WRITE_MAP)
    );
    let len = 200usize;
    db.put_reserved(&mut wtxn, b"half-filled", len, |buf| {
        // Contract violation: only write the first half.
        buf[..100].fill(0x7A);
    })
    .unwrap();
    let got = db.get(&wtxn, b"half-filled").unwrap().unwrap().to_vec();
    assert_eq!(got.len(), len);
    assert_eq!(&got[..100], &[0x7Au8; 100][..], "filled half must be exact");
    // Read it again (same txn, no intervening mutation): deterministic, not
    // re-randomized garbage on every read.
    let got2 = db.get(&wtxn, b"half-filled").unwrap().unwrap().to_vec();
    assert_eq!(got, got2, "unfilled tail must be stable across reads");
    // The observed content of the unfilled tail (see doc comment above):
    // zero, because this reserve landed in a fresh
    // zero-initialized leaf frame, not because RESERVE zeroes on purpose.
    assert_eq!(
        &got[100..],
        &[0u8; 100][..],
        "unfilled RESERVE tail observed as zero on a fresh frame (implementation \
         detail, not a TXN-47 guarantee — see this test's doc comment)"
    );
    wtxn.abort(); // never persist a contract-violating write.
}
