//! Ranged `WRITE_MAP` commit barriers (SPEC 06 REC-12): every page a commit
//! writes is covered by the `msync` ranges its barriers flush.
//!
//! Under `WRITE_MAP` the commit pipeline no longer flushes the whole map: C3
//! flushes the pages the txn wrote (its dirty frames, in the map, and its
//! spilled pages) plus both meta slots, and C5 the meta slot C4 wrote. A page
//! missing from those ranges would be a silent durability bug: the commit
//! returns `Ok`, but a power cut can lose the page while the durable meta
//! references it.
//!
//! The observer is the crash harness's [`FaultBacking`] over the real
//! writable map. It journals every in-map dirty page and every meta write,
//! and a ranged barrier folds into the durable image only the journaled
//! bytes inside the ranges the engine passed (not widened to OS pages, and
//! without the `fdatasync` the real barrier adds). After every commit the
//! durable-only image — the crash image in which every unflushed write is
//! lost — must pass `check_image` and hold exactly the committed state.
//! Pages a txn wrote and then freed may legitimately stay unflushed (no meta
//! references them); every other journaled page must have been folded.

use std::collections::BTreeMap;
use std::path::Path;

use zerodb_core::check::check_image;
use zerodb_core::env::{open_with_backing, open_with_backing_policy, testutil::VecBacking};
use zerodb_core::env::{DurabilityFlags, Env};
use zerodb_core::page::FileTrust;
use zerodb_io::fault::{CapturedDisk, FaultBacking, FaultHandle};
use zerodb_oracle::tempdir::TempDir;

const PS: u32 = 4096;
const MAP: u64 = 32 << 20;

fn writemap(no_meta_sync: bool) -> DurabilityFlags {
    DurabilityFlags {
        write_map: true,
        no_meta_sync,
        ..DurabilityFlags::default()
    }
}

/// A real writable-map env under the fault wrapper, with a small dirty limit
/// (in pages) so large txns spill.
fn open_faulted(dir: &Path, durability: DurabilityFlags) -> (Env, FaultHandle) {
    let opened =
        zerodb_io::open_or_create(&dir.join("zerodb.dat"), PS, Some(MAP), MAP, false, true)
            .expect("open_or_create");
    let (fault, handle) = FaultBacking::wrap(opened.backing, PS).expect("fault wrap");
    let env = open_with_backing_policy(
        dir.to_path_buf(),
        Box::new(fault),
        PS,
        false,
        16,
        126,
        durability,
        FileTrust::VALIDATE,
        false,
        Some(24),
    )
    .expect("env open");
    (env, handle)
}

/// Deterministic xorshift for the workload.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// One commit's worth of mixed ops into named DB `d`: small and overflow
/// values (overflow runs), overwrites and deletes (freed pages the next
/// commits reclaim). Mirrors the ops into `model`. Returns the txn's spill
/// count.
fn commit_round(
    env: &Env,
    rng: &mut Rng,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    ops: usize,
) -> u64 {
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"d")).unwrap();
    for _ in 0..ops {
        let r = rng.next();
        let key = format!("key{:05}", r % 3000).into_bytes();
        if r.is_multiple_of(5) {
            db.delete(&mut w, &key).unwrap();
            model.remove(&key);
        } else {
            // One value in eight spans several pages (an overflow run).
            let len = if r % 8 == 1 {
                PS as usize * (1 + (r >> 40) as usize % 3) + 17
            } else {
                16 + (r >> 20) as usize % 300
            };
            let val = vec![(r >> 8) as u8; len];
            db.put(&mut w, &key, &val).unwrap();
            model.insert(key, val);
        }
    }
    let spills = w.spill_count();
    w.commit().unwrap();
    spills
}

/// An aborted txn: its in-place pages are journaled but never referenced.
fn aborted_round(env: &Env, rng: &mut Rng) {
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"d")).unwrap();
    for _ in 0..50 {
        let r = rng.next();
        db.put(
            &mut w,
            format!("key{:05}", r % 3000).as_bytes(),
            &[0xEE; 200],
        )
        .unwrap();
    }
    w.abort();
}

/// Open the durable-only image (`CapturedDisk::floor_image`) and assert it
/// holds exactly `model`.
fn assert_floor_is(cut: &CapturedDisk, verify: &Path, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut img = cut.floor_image();
    img.resize(MAP as usize, 0);
    let violations = check_image(&img, PS);
    assert!(
        violations.is_empty(),
        "durable image fails the check: {violations:?}"
    );
    let env = open_with_backing(
        verify.to_path_buf(),
        Box::new(VecBacking::new(img, MAP)),
        PS,
        false,
        16,
        126,
        DurabilityFlags::default(),
    )
    .expect("the durable image must open");
    let r = env.read_txn().unwrap();
    let db = env
        .open_database(&r, Some(b"d"))
        .unwrap()
        .expect("named DB durable");
    assert_eq!(
        db.len(&r).unwrap(),
        model.len() as u64,
        "durable entry count"
    );
    for (k, v) in model {
        assert_eq!(
            db.get(&r, k).unwrap(),
            Some(v.as_slice()),
            "key {k:?} missing or stale in the durable image"
        );
    }
}

#[test]
fn every_writemap_commit_page_is_covered_by_its_barriers() {
    let dir = TempDir::new().unwrap();
    let mut rng = Rng(0x5EED_0045_0045_5EED);
    let mut model = BTreeMap::new();
    let (env, handle) = open_faulted(dir.path(), writemap(false));
    let mut spills = 0;
    for round in 0..40usize {
        if round % 7 == 3 {
            aborted_round(&env, &mut rng);
        }
        let ops = if round % 5 == 4 { 400 } else { 40 };
        spills += commit_round(&env, &mut rng, &mut model, ops);
        let cut = handle.capture();
        assert!(
            cut.pending.iter().all(|w| w.offset >= 2 * u64::from(PS)),
            "round {round}: the meta C4 wrote is not covered by C5"
        );
        let vdir = TempDir::new().unwrap();
        assert_floor_is(&cut, vdir.path(), &model);
    }
    let s = handle.stats();
    assert!(
        spills > 0,
        "the workload must spill (spilled pages are a range source)"
    );
    assert!(
        s.map_regions > 0,
        "the in-place WRITE_MAP path must be engaged"
    );
    assert_eq!(
        s.data_writes, 0,
        "in-place WRITE_MAP writes no data page through the fd"
    );
    assert_eq!(
        s.ranged_barriers, s.barriers,
        "every WRITE_MAP commit barrier is ranged"
    );
    assert_eq!(s.barriers, 2 * 40, "C3 + C5 per commit");
}

#[test]
fn writemap_no_meta_sync_makes_the_previous_meta_durable_at_c3() {
    // NO_META_SYNC skips C5, so the meta written at C4 stays pending. The
    // next commit's C3 covers both meta slots, so the previous commit's meta
    // becomes durable there (the REC-10 window bound, as LMDB's `msync` of
    // the used prefix gives): the durable image is the previous commit.
    let dir = TempDir::new().unwrap();
    let mut rng = Rng(0x0045_0000_0000_0045);
    let mut model = BTreeMap::new();
    let (env, handle) = open_faulted(dir.path(), writemap(true));
    let mut floor_model = BTreeMap::new();
    for round in 0..20usize {
        let ops = if round % 4 == 3 { 400 } else { 40 };
        commit_round(&env, &mut rng, &mut model, ops);
        let cut = handle.capture();
        assert!(
            cut.pending
                .iter()
                .any(|w| w.offset == (round as u64 + 1) % 2 * u64::from(PS)),
            "round {round}: the meta C4 wrote must stay pending (C5 skipped)"
        );
        if round > 0 {
            // The previous commit's meta was folded by this commit's C3, so
            // the durable image is at least the previous commit.
            let vdir = TempDir::new().unwrap();
            assert_floor_is(&cut, vdir.path(), &floor_model);
        }
        floor_model.clone_from(&model);
    }
    assert_eq!(handle.stats().barriers, 20, "C3 only per commit");
}
