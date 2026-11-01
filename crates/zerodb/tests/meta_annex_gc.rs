//! Meta free-list annex behavior (SPEC 05 §2a GC-29..33, SPEC 02 §3 format
//! v2, ADR-0022): steady-state placement, the GC-31 reader gate, the GC-30
//! carry, the GC-32 spill-to-tree, copy, and reopen — each asserted through
//! observable state (file size, `check_image`, `free_page_count`, the meta
//! bytes) rather than internals.

mod common;
use common::TempDir;

use std::path::{Path, PathBuf};

use zerodb::{check, free_page_count, CompactionOption, CopyToFile, Env, EnvFlags, EnvOpenOptions};
use zerodb_core::page::{MetaPage, MetaValidity, PGNO_INVALID};

const PS: u32 = 4096;
const MAP: usize = 32 << 20;

fn open(dir: &Path, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.flags(flags);
    opts.open(dir).expect("open env")
}

/// Non-vacuousness tripwire for the WRITE_MAP twins (ADR-0021/ADR-0022
/// convention, see `nested_fanout.rs::assert_mode`): the txn's dirty-page
/// realization must match the env flags it was opened with, so a twin can
/// never silently run the default heap-staged path instead of in-place
/// WRITE_MAP.
fn assert_mode(wtxn: &zerodb::RwTxn<'_>, flags: EnvFlags) {
    assert_eq!(
        wtxn.dirty_in_map_mode(),
        flags.contains(EnvFlags::WRITE_MAP),
        "dirty-page realization does not match the env flags"
    );
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

/// Growth metric that stays meaningful under both dirty-page realizations.
/// The default backing grows the file by exactly what each commit needs, so
/// `file_size` tracks page reuse vs. extension precisely. Under `WRITE_MAP`
/// the file is `set_len`d to the full `map_size` at map time (see
/// `zerodb-core::env`'s read-annex comment on the geometry-validation length
/// check), so `file_size` is pinned from the first commit on and can never
/// show growth — the meta's logical page high-water mark (`last_pg`) is the
/// mode-independent stand-in: it only advances when a commit extends past
/// the previous high water, and stays put when a commit's allocations are all
/// satisfied by annex/pool reuse.
fn high_water(dir: &Path, flags: EnvFlags) -> u64 {
    if !flags.contains(EnvFlags::WRITE_MAP) {
        return file_size(dir);
    }
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
    m.last_pg
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
    steady_state_annex_only_gc_tree_stays_empty_inner(EnvFlags::EMPTY);
}

/// WRITE_MAP in-place twin (ADR-0021 convention): the same steady-state
/// churn, but with dirty pages realized in the writable map. The annex draw
/// (ids reused from the meta's loose pool, not fresh-extended pages — see the
/// flat `file_size` assertions below) is orthogonal to WRITE_MAP's dirty-page
/// realization, so it must behave identically; `assert_mode` is the tripwire
/// that the twin actually engaged in-place writes instead of silently running
/// the default heap-staged path.
#[test]
fn steady_state_annex_only_gc_tree_stays_empty_writemap_in_place() {
    steady_state_annex_only_gc_tree_stays_empty_inner(EnvFlags::WRITE_MAP);
}

fn steady_state_annex_only_gc_tree_stays_empty_inner(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open(dir.path(), flags);
    let db = env.main_database();

    let val = vec![0xABu8; 256];
    for i in 0u64..60 {
        let mut w = env.write_txn().unwrap();
        assert_mode(&w, flags);
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
    let size_settled = high_water(dir.path(), flags);
    for i in 0u64..50 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    assert_eq!(
        high_water(dir.path(), flags),
        size_settled,
        "annex reuse must keep overwrite churn at zero growth (file size \
         under the default backing, last_pg under WRITE_MAP)"
    );
    assert_clean(dir.path());

    // free_page_count sees the annex ids (GC-23).
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
    reader_gate_blocks_annex_draw_then_carry_reclaims_inner(EnvFlags::EMPTY);
}

/// WRITE_MAP in-place twin (ADR-0021 convention) of the parked-reader
/// case: the GC-31 gate and GC-30 carry are meta free-list bookkeeping,
/// independent of whether dirty pages are realized in the writable map, so
/// the same growth-then-settle shape must hold. `assert_mode` tripwires that
/// the twin really ran in-place.
#[test]
fn reader_gate_blocks_annex_draw_then_carry_reclaims_writemap_in_place() {
    reader_gate_blocks_annex_draw_then_carry_reclaims_inner(EnvFlags::WRITE_MAP);
}

fn reader_gate_blocks_annex_draw_then_carry_reclaims_inner(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open(dir.path(), flags);
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
        assert_mode(&w, flags);
        db.put(&mut w, &0u64.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_pinned_base = high_water(dir.path(), flags);
    for i in 0u64..6 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    let size_during = high_water(dir.path(), flags);
    assert!(
        size_during > size_pinned_base,
        "with a reader pinned below every base, commits must extend (file \
         size under the default backing, last_pg under WRITE_MAP) — the \
         GC-31 gate refuses the annex — got no growth, so the gate leaked"
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
    let size_settled = high_water(dir.path(), flags);
    for i in 0u64..8 {
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
        w.commit().unwrap();
    }
    assert_eq!(
        high_water(dir.path(), flags),
        size_settled,
        "after the reader releases, carried annex ids must satisfy churn \
         with no further growth"
    );
    assert_clean(dir.path());
}

/// GC-32 all-or-nothing spill: a commit freeing more ids than the annex cap
/// writes the whole PIL to the GC tree (annex empty), and those pages are
/// reclaimable afterwards.
#[test]
fn over_cap_freed_set_spills_whole_to_gc_tree() {
    let dir = TempDir::new();
    let env = open(dir.path(), EnvFlags::EMPTY);
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
    let env = open(dir.path(), EnvFlags::EMPTY);
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
        open(&dirp, EnvFlags::EMPTY)
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
        let env = open(dir.path(), EnvFlags::EMPTY);
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

    let env = open(dir.path(), EnvFlags::EMPTY);
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

/// Every meta slot on disk is byte-for-byte the canonical full-page encode of
/// the meta it holds (fields, annex ids, CRC and an all-zero tail) while the
/// annex grows to near its capacity and shrinks back. The commit encodes into
/// one buffer it reuses across commits and clears only the prefix it wrote,
/// so a shorter annex after a longer one must not leave stale ids behind.
#[test]
fn meta_slots_stay_canonical_as_the_annex_grows_and_shrinks() {
    meta_slots_stay_canonical_inner(EnvFlags::EMPTY);
}

#[test]
fn meta_slots_stay_canonical_as_the_annex_grows_and_shrinks_writemap_in_place() {
    meta_slots_stay_canonical_inner(EnvFlags::WRITE_MAP);
}

/// Assert both meta slots of the file at `dir` are canonical; returns the
/// larger annex length of the two.
fn assert_meta_slots_canonical(dir: &Path) -> usize {
    let bytes = std::fs::read(data_file(dir)).unwrap();
    let ps = PS as usize;
    let mut max_annex = 0;
    for slot in 0..2 {
        let page = &bytes[slot * ps..(slot + 1) * ps];
        let MetaValidity::Valid(m) = MetaPage::validate(page, PS).unwrap() else {
            panic!("meta slot {slot} is not valid");
        };
        let annex = MetaPage::read_annex(page, PS).expect("annex in bounds");
        // Pre-filled with non-zero bytes: the full encode must overwrite all.
        let mut canonical = vec![0xEEu8; ps];
        m.encode_with_annex(&mut canonical, &annex).unwrap();
        assert!(
            page == canonical.as_slice(),
            "meta slot {slot} (txn {}, {} annex ids) differs from its canonical encode",
            m.txnid,
            annex.len()
        );
        max_annex = max_annex.max(annex.len());
    }
    max_annex
}

fn meta_slots_stay_canonical_inner(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open(dir.path(), flags);
    let db = env.main_database();
    let cap = zerodb_core::page::meta_annex_cap(PS);
    let commit = |f: &dyn Fn(&mut zerodb::RwTxn<'_>)| {
        let mut w = env.write_txn().unwrap();
        assert_mode(&w, flags);
        f(&mut w);
        w.commit().unwrap();
        assert_meta_slots_canonical(dir.path())
    };
    // A large overflow value, freed in the next txn: the annex fills up to
    // near its capacity. Re-inserting a similar value draws it back down.
    let big = vec![0x5Au8; (cap - 10) * PS as usize];
    let mut max_annex = commit(&|w| db.put(w, b"big", &big).unwrap());
    max_annex = max_annex.max(commit(&|w| assert!(db.delete(w, b"big").unwrap())));
    max_annex = max_annex.max(commit(&|w| db.put(w, b"big2", &big).unwrap()));
    for i in 0u32..6 {
        commit(&|w| db.put(w, &i.to_be_bytes(), b"small").unwrap());
    }
    assert!(
        max_annex > cap / 2,
        "the annex never filled up ({max_annex} ids): the shrink case went untested"
    );
    let (_, fl, _) = live_meta(dir.path());
    assert!(
        (fl as usize) < cap / 2,
        "the annex never shrank back ({fl} ids): the shrink case went untested"
    );
    assert_clean(dir.path());
}
