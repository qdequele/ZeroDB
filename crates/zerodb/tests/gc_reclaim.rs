//! Free-page GC guard tests (ADR-0005) over **real files**: freed-page
//! reclamation (SPEC 05 GC-14..20), the freelist-writes-to-itself commit step
//! (GC-11..13), the partial-drain remainder rewrite (GC-20), the interim
//! oldest-reader gate (ADR-0005; SPEC 04 TXN-21), the huge-txn PIL spill
//! (GC-5/GC-26),
//! `non_free_pages_size` (GC-22..24, INV-27), and GC atomicity across crashes
//! (GC-14, REC-6).
//!
//! Every committed image is validated with `zerodb::check::check_image`,
//! **including** the INV-22 reachable-XOR-free partition — a leaked page
//! ("neither") or a double-hand-out ("both") fails loudly.

mod common;
use common::{assert_clean, TempDir};

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use zerodb::{free_page_count, CommitHook, Env, EnvOpenOptions, HookPoint};

const PS: u32 = 4096;
const MAP: usize = 16 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn file_size(dir: &Path) -> u64 {
    std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len()
}

/// Bounded file growth on insert/delete churn (GC acceptance criterion;
/// ADR-0005 flatness bound): with a constant live set, reclamation must
/// hold the file at a steady state — size at the last cycle within 1.05× of
/// the size at the halfway cycle. The INV-22 walk after every commit is the
/// structural half of the guarantee (no leak, no double-hand-out); this is
/// the quantitative half (reuse actually fires).
#[test]
fn churn_file_growth_reaches_steady_state() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    const CYCLES: usize = 60;
    const KEYS: u32 = 200;
    let mut sizes = Vec::with_capacity(CYCLES);
    for cycle in 0..CYCLES {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..KEYS {
            let k = format!("key{i:05}").into_bytes();
            // Size varies per cycle so replaces are not same-size in-place.
            let v = vec![0xA5u8; 100 + ((cycle * 37 + i as usize) % 300)];
            db.put(&mut wtxn, &k, &v).unwrap();
        }
        // Delete a deterministic half.
        for i in (0..KEYS).step_by(2) {
            let k = format!("key{i:05}").into_bytes();
            db.delete(&mut wtxn, &k).unwrap();
        }
        wtxn.commit().unwrap();
        assert_clean(dir.path(), PS);
        sizes.push(file_size(dir.path()));
    }
    let half = sizes[CYCLES / 2] as f64;
    let last = sizes[CYCLES - 1] as f64;
    assert!(
        last <= half * 1.05,
        "file growth not bounded: half-way size {half}, final size {last} \
         (> 1.05x — reclamation is not firing)"
    );
}

/// GC-20 partial-drain remainder rewrite (ADR-0005 risk 3): draining part
/// of a GC entry across a commit must keep the remainder listed (INV-22
/// "never neither"), remove the drained ids (INV-22 "never both" — they are
/// reachable again), and leave the remainder genuinely reusable (a follow-up
/// allocation consumes it without growing the file).
#[test]
fn partial_drain_remainder_survives_commits() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    // Txn 1: a spread of entries across several leaves.
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..600u32 {
            db.put(&mut wtxn, format!("k{i:04}").as_bytes(), &[0x11u8; 200])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    // Txn 2: delete everything -> a large freed set under one GC entry.
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..600u32 {
            assert!(db.delete(&mut wtxn, format!("k{i:04}").as_bytes()).unwrap());
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    let free_after_del = {
        let rtxn = env.read_txn().unwrap();
        free_page_count(&rtxn).unwrap()
    };
    assert!(
        free_after_del > 10,
        "expected a substantial freed set, got {free_after_del}"
    );
    let size_after_del = file_size(dir.path());

    // Txn 3: a small insert drains only a few pages from the entry (partial
    // drain -> remainder rewrite at C1).
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..5u32 {
            db.put(&mut wtxn, format!("p{i}").as_bytes(), &[0x22u8; 100])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS); // INV-22/24: remainder listed once, drained ids not
    let free_after_partial = {
        let rtxn = env.read_txn().unwrap();
        free_page_count(&rtxn).unwrap()
    };
    assert!(
        free_after_partial < free_after_del,
        "drain did not shrink the free set ({free_after_del} -> {free_after_partial})"
    );
    assert!(
        free_after_partial > 0,
        "partial drain deleted the whole entry (GC-20 violation)"
    );
    assert_eq!(
        file_size(dir.path()),
        size_after_del,
        "a partial drain must reuse listed pages, not extend the file"
    );

    // Reopen (fresh mmap, tree read back from disk) and drain more: the
    // rewritten remainder must be genuinely reusable.
    drop(env);
    let env = open(dir.path());
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..40u32 {
            db.put(&mut wtxn, format!("q{i:02}").as_bytes(), &[0x33u8; 400])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    assert_eq!(
        file_size(dir.path()),
        size_after_del,
        "post-reopen inserts must be served from the rewritten remainder"
    );
}

/// GC-11..13 freelist-writes-to-itself (ADR-0005 risk 1) + the GC-26
/// huge-txn spill: one txn frees far more than the `c >= 251` (psize 4096)
/// spill threshold, so the PIL goes to an overflow run, and `freelist_save`'s
/// own tree writes free/allocate GC pages mid-save. INV-22 catches any page
/// the loop leaks or double-lists. Also records the baseline numbers (GC-26)
/// for a huge-txn GC redesign in the test output.
#[test]
fn freelist_self_write_split_and_spill() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    // Build a tree of several hundred pages.
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..3000u32 {
            db.put(&mut wtxn, format!("k{i:05}").as_bytes(), &[0x44u8; 300])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    // One txn freeing the whole tree: clear().
    let t0 = std::time::Instant::now();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.clear(&mut wtxn).unwrap();
        wtxn.commit().unwrap();
    }
    let commit_time = t0.elapsed();
    assert_clean(dir.path(), PS);
    let rtxn = env.read_txn().unwrap();
    let free = free_page_count(&rtxn).unwrap();
    drop(rtxn);
    assert!(
        free > 251,
        "expected a spilled PIL (> 251 freed pages at psize 4096, GC-26), got {free}"
    );
    // GC-26 baseline for a huge-txn GC redesign (printed, not asserted).
    println!(
        "GC-26 baseline: freed_pages={free} file_size={} clear_commit={:?}",
        file_size(dir.path()),
        commit_time
    );
    // The spilled free set is reusable: re-insert without growing the file.
    let size_cleared = file_size(dir.path());
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..3000u32 {
            db.put(&mut wtxn, format!("r{i:05}").as_bytes(), &[0x55u8; 300])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    // Slack note: a commit's own GC pages are committed-freed and age one txn
    // before reuse (TXN-62), so a single post-clear commit may exceed the pool
    // by the GC-tree size (~2 pages) — bounded, not a leak (INV-22 above).
    assert!(
        file_size(dir.path()) <= size_cleared + 2 * u64::from(PS),
        "re-inserting an equal working set must be served from the freed pages \
         ({} -> {})",
        size_cleared,
        file_size(dir.path())
    );
}

/// The interim oldest-reader gate (ADR-0005 —
/// reuse-while-referenced): a live `RoTxn` pins its snapshot; writers that
/// churn concurrently must NOT reclaim pages that snapshot still reaches.
/// The reader's borrows come zero-copy from the mmap, so a gate bug is
/// observable as corrupted reads. After the reader drops, the pages become
/// reclaimable and a follow-up commit reuses them.
#[test]
fn reader_gate_blocks_reuse_until_release() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    // Snapshot S1: distinctive data.
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..400u32 {
            let v = format!("orig-{i:04}-{}", "s".repeat(120)).into_bytes();
            db.put(&mut wtxn, format!("k{i:04}").as_bytes(), &v)
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    let rtxn = env.read_txn().unwrap(); // pins S1
    let pinned = rtxn.txnid();

    // Writers churn: S1's pages are freed by txn 2 (F = 2 > oldest = 1), so
    // they must NOT be handed out while the reader lives.
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..400u32 {
            let v = format!("new1-{i:04}-{}", "x".repeat(120)).into_bytes();
            db.put(&mut wtxn, format!("k{i:04}").as_bytes(), &v)
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..400u32 {
            let v = format!("new2-{i:04}-{}", "y".repeat(120)).into_bytes();
            db.put(&mut wtxn, format!("k{i:04}").as_bytes(), &v)
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    // The pinned snapshot must read back byte-correct originals.
    assert_eq!(rtxn.txnid(), pinned);
    for i in (0..400u32).step_by(7) {
        let expect = format!("orig-{i:04}-{}", "s".repeat(120)).into_bytes();
        let got = db.get(&rtxn, format!("k{i:04}").as_bytes()).unwrap();
        assert_eq!(
            got,
            Some(expect.as_slice()),
            "reader at snapshot {pinned} observed a reclaimed page (gate bug) at k{i:04}"
        );
    }
    let size_pinned = file_size(dir.path());
    drop(rtxn); // release the pin

    // With the pin gone, the S1/S2-era freed pages are reclaimable: another
    // full rewrite must be served from them (no growth).
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..400u32 {
            let v = format!("new3-{i:04}-{}", "z".repeat(120)).into_bytes();
            db.put(&mut wtxn, format!("k{i:04}").as_bytes(), &v)
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    // Same 2-page slack as above: the commit's own GC storage ages one txn.
    assert!(
        file_size(dir.path()) <= size_pinned + 2 * u64::from(PS),
        "post-release rewrite must reuse the previously gated pages \
         ({size_pinned} -> {})",
        file_size(dir.path())
    );
}

/// INV-27 (SPEC 05 §9): every page of a committed
/// image is exactly one of the two meta slots, a page of a user tree (main +
/// named DBs, which `non_free_pages_size` counts), a page of the GC tree, or
/// a free page (`free_page_count`), so
/// `non_free_pages_size + (free + gc_tree_pages + 2) * psize == real_disk_size`
/// (GC-22/23), checked against an independent free-list walk.
#[test]
fn non_free_pages_size_identity() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    for cycle in 0..8u32 {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..300u32 {
            db.put(
                &mut wtxn,
                format!("k{i:04}").as_bytes(),
                &vec![cycle as u8; 150 + (i as usize % 200)],
            )
            .unwrap();
        }
        for i in (0..300u32).step_by(3) {
            db.delete(&mut wtxn, format!("k{i:04}").as_bytes()).unwrap();
        }
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        let free = free_page_count(&rtxn).unwrap();
        let g = rtxn.snapshot().free_db;
        let gc_tree = g.branch_pages + g.leaf_pages + g.overflow_pages;
        let m = rtxn.snapshot().main_db;
        let main_tree = m.branch_pages + m.leaf_pages + m.overflow_pages;
        drop(rtxn);
        let disk = env.real_disk_size().unwrap();
        let non_free = env.non_free_pages_size().unwrap();
        // Only the main DB here: the value is exactly its pages.
        assert_eq!(non_free, main_tree * u64::from(PS), "cycle {cycle}");
        assert_eq!(
            non_free + (free + gc_tree + 2) * u64::from(PS),
            disk,
            "INV-27 violated at cycle {cycle}: disk={disk} non_free={non_free} free={free} gc_tree={gc_tree}"
        );
        assert!(non_free > 0 && non_free < disk);
    }
}

// ---------------------------------------------------------------------------
// GC atomicity across crashes (GC-14; ADR-0005, crash-churn variant of the
// commit-pipeline crash smoke — the full crash harness lives in zerodb-oracle).
// ---------------------------------------------------------------------------

const CHILD_ENV_VAR: &str = "ZDB_GC_CRASH_CHILD_HOOK";
const DIR_ENV_VAR: &str = "ZDB_GC_CRASH_CHILD_DIR";

struct AbortAt(HookPoint);

impl CommitHook for AbortAt {
    fn at(&self, point: HookPoint) {
        if point == self.0 {
            std::process::abort();
        }
    }
}

fn hook_from_name(name: &str) -> HookPoint {
    match name {
        "H0" => HookPoint::H0,
        "H1" => HookPoint::H1,
        "H2" => HookPoint::H2,
        "H3" => HookPoint::H3,
        "H4" => HookPoint::H4,
        other => panic!("unknown hook {other}"),
    }
}

/// Number of *completed* churn commits the child runs before the aborted one.
const CHURN_TXNS: u64 = 6;

/// Child body: churn commits that build GC entries **and** reclaim from them,
/// then one more churn commit aborted at the target hook. No-op unless
/// spawned by the parent.
#[test]
fn gc_crash_child_runner() {
    let Ok(hook_name) = std::env::var(CHILD_ENV_VAR) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(DIR_ENV_VAR).expect("child dir env var"));
    let env = open(&dir);
    let db = env.main_database();
    let churn = |wtxn: &mut zerodb::RwTxn<'_>, round: u64| {
        for i in 0..250u32 {
            let v = vec![round as u8; 100 + (i as usize % 250)];
            db.put(wtxn, format!("k{i:04}").as_bytes(), &v).unwrap();
        }
        for i in (0..250u32).step_by(2) {
            db.delete(wtxn, format!("k{i:04}").as_bytes()).unwrap();
        }
        // A per-round marker so the parent can identify the recovered txn.
        db.put(wtxn, format!("round{round:02}").as_bytes(), b"done")
            .unwrap();
    };
    for round in 1..=CHURN_TXNS {
        let mut wtxn = env.write_txn().unwrap();
        churn(&mut wtxn, round);
        wtxn.commit().unwrap();
        assert_eq!(env.txnid(), round);
    }
    // The aborted commit: full churn again (GC entries drained + rewritten +
    // a fresh entry written mid-freelist_save when the abort fires).
    env.set_commit_hook(Some(Arc::new(AbortAt(hook_from_name(&hook_name)))));
    let mut wtxn = env.write_txn().unwrap();
    churn(&mut wtxn, CHURN_TXNS + 1);
    let _ = wtxn.commit(); // aborts at the hook for H0..H4
    unreachable!("the abort hook must fire");
}

#[test]
fn gc_crash_matrix_recovers_consistently() {
    let exe = std::env::current_exe().expect("test binary path");
    let cases: [(&str, &[u64]); 5] = [
        ("H0", &[CHURN_TXNS]),
        ("H1", &[CHURN_TXNS]),
        ("H2", &[CHURN_TXNS]),
        ("H3", &[CHURN_TXNS, CHURN_TXNS + 1]),
        ("H4", &[CHURN_TXNS + 1]),
    ];
    for (hook, expect) in cases {
        let dir = TempDir::new();
        let status = Command::new(&exe)
            .arg("gc_crash_child_runner")
            .arg("--exact")
            .arg("--nocapture")
            .env(CHILD_ENV_VAR, hook)
            .env(DIR_ENV_VAR, dir.path())
            .status()
            .expect("spawn child");
        assert!(!status.success(), "{hook}: child must die at the hook");

        // Reopen: the recovered snapshot obeys the REC-6 row, the GC DB is
        // exactly the recovered txn's (never half-applied, GC-14), and the
        // full walk incl. INV-22 is clean.
        let env = open(dir.path());
        let recovered = env.txnid();
        assert!(
            expect.contains(&recovered),
            "{hook}: recovered txn {recovered}, expected one of {expect:?}"
        );
        let db = env.main_database();
        let rtxn = env.read_txn().unwrap();
        for round in 1..=recovered {
            assert_eq!(
                db.get(&rtxn, format!("round{round:02}").as_bytes())
                    .unwrap(),
                Some(b"done".as_slice()),
                "{hook}: round {round} marker missing after recovery to {recovered}"
            );
        }
        assert_eq!(
            db.get(&rtxn, format!("round{:02}", recovered + 1).as_bytes())
                .unwrap(),
            None,
            "{hook}: data from the unrecovered txn is visible"
        );
        drop(rtxn);
        drop(env);
        assert_clean(dir.path(), PS);
    }
}

// ---------------------------------------------------------------------------
// Runs spanning freed-page records (SPEC 05 GC-21): a multi-page value whose
// only fitting run lies across two records of the free-page database.
// ---------------------------------------------------------------------------

/// Pages of `a` and of `b`: more than the meta page's freed list holds at
/// 4 KiB pages (`(4096 - 176) / 8 = 490` ids), so each deleting transaction
/// writes its own record to the free-page database.
const BIG: usize = 600;

/// A value of `pages` overflow pages: `pages - 1` full pages plus half a page
/// leaves room for the overflow-page header.
fn pages_value(fill: u8, pages: usize) -> Vec<u8> {
    vec![fill; (pages - 1) * PS as usize + PS as usize / 2]
}

fn commit_put(env: &Env, key: &[u8], val: &[u8]) {
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, key, val).unwrap();
    wtxn.commit().unwrap();
}

fn commit_delete(env: &Env, key: &[u8]) {
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    assert!(db.delete(&mut wtxn, key).unwrap());
    wtxn.commit().unwrap();
}

/// Txns 1..=5 of the cross-record scripts: lay out a cushion `x` (64 pages;
/// the bookkeeping pages of the next commits come from it, so `a`'s run
/// stays whole), a kept separator `y`, then `a` and `b` side by side, and
/// free `x`, `a` and `b` in three transactions. `a`'s and `b`'s pages end up
/// in two records of the free-page database. `before_b` runs between the
/// deletes of `a` and `b`.
fn lay_out_and_free_a_then_b(env: &Env, dir: &Path, before_b: impl FnOnce()) {
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"x", &pages_value(b'x', 64)).unwrap();
        db.put(&mut wtxn, b"y", &pages_value(b'y', 2)).unwrap();
        wtxn.commit().unwrap();
    }
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"a", &pages_value(b'a', BIG)).unwrap();
        db.put(&mut wtxn, b"b", &pages_value(b'b', BIG)).unwrap();
        wtxn.commit().unwrap();
    }
    commit_delete(env, b"x");
    assert_clean(dir, PS);
    commit_delete(env, b"a");
    assert_clean(dir, PS);
    before_b();
    commit_delete(env, b"b");
    assert_clean(dir, PS);
}

/// SPEC 05 GC-21: once both records pass the reuse gate, a value as long as
/// `a` and `b` together is placed in their pages, though neither record alone
/// holds a long enough run. The partition walk after every commit (no page
/// both reachable and free, none neither) shows each page left exactly one
/// record when drawn. LMDB places it there too
/// (`crates/zerodb-oracle/tests/lmdb_free_run_observation.rs`,
/// `big_run_across_two_records_cushioned`).
#[test]
fn run_across_two_gc_records_is_reused() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    lay_out_and_free_a_then_b(&env, dir.path(), || {});
    commit_put(&env, b"s", b"spacer");
    let before = file_size(dir.path());
    let c = pages_value(b'c', 2 * BIG);
    commit_put(&env, b"c", &c);
    assert_clean(dir.path(), PS);
    let grew = (file_size(dir.path()) - before) / u64::from(PS);
    assert!(
        grew < 8,
        "c ({} pages) must reuse the run across the two records; the file grew by {grew} pages",
        2 * BIG
    );
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"c").unwrap(), Some(c.as_slice()));
    assert_eq!(
        db.get(&rtxn, b"y").unwrap(),
        Some(pages_value(b'y', 2).as_slice())
    );
    drop(rtxn);
    // Reopen: the remainders the draw left in each record are read back
    // from disk and stay consistent.
    drop(env);
    let env = open(dir.path());
    commit_put(&env, b"d", &pages_value(b'd', 3));
    assert_clean(dir.path(), PS);
}

/// The reuse gate holds for every record a run is drawn from: a reader that
/// still sees `b` keeps `b`'s record out of the search, so `c` cannot take
/// the run across `a` and `b` and goes to the end of the file; the reader
/// reads `b` intact afterwards. Once it is gone, the run is reused.
#[test]
fn reader_pin_keeps_a_record_out_of_the_cross_record_search() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut pinned = None;
    lay_out_and_free_a_then_b(&env, dir.path(), || {
        pinned = Some(env.read_txn().unwrap()); // still sees `b`
    });
    let rtxn = pinned.expect("reader opened before b's delete");
    commit_put(&env, b"s", b"spacer");
    let before = file_size(dir.path());
    let c = pages_value(b'c', 2 * BIG);
    commit_put(&env, b"c", &c);
    assert_clean(dir.path(), PS);
    let grew = (file_size(dir.path()) - before) / u64::from(PS);
    assert!(
        grew >= 2 * BIG as u64,
        "c drew from b's record while a reader still sees b (grew {grew} pages)"
    );
    assert_eq!(
        db.get(&rtxn, b"b").unwrap(),
        Some(pages_value(b'b', BIG).as_slice()),
        "the pinned reader observed a reused page of b"
    );
    drop(rtxn);
    commit_delete(&env, b"c");
    commit_put(&env, b"s2", b"spacer");
    let before = file_size(dir.path());
    commit_put(&env, b"d", &pages_value(b'd', 2 * BIG));
    assert_clean(dir.path(), PS);
    let grew = (file_size(dir.path()) - before) / u64::from(PS);
    assert!(
        grew < 8,
        "with the reader gone the run is reusable; the file grew by {grew} pages"
    );
}

const CROSS_CHILD_ENV_VAR: &str = "ZDB_GC_CROSS_CRASH_CHILD_HOOK";

/// Commits the cross-record script runs before the aborted one: five of
/// `lay_out_and_free_a_then_b` and the spacer.
const CROSS_TXNS: u64 = 6;

/// Child body for the crash matrix below: the cross-record script, then the
/// commit that draws `c`'s run from two records, aborted at the target hook.
/// No-op unless spawned by the parent.
#[test]
fn gc_cross_record_crash_child_runner() {
    let Ok(hook_name) = std::env::var(CROSS_CHILD_ENV_VAR) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(DIR_ENV_VAR).expect("child dir env var"));
    let env = open(&dir);
    lay_out_and_free_a_then_b(&env, &dir, || {});
    commit_put(&env, b"s", b"spacer");
    assert_eq!(env.txnid(), CROSS_TXNS);
    env.set_commit_hook(Some(Arc::new(AbortAt(hook_from_name(&hook_name)))));
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, b"c", &pages_value(b'c', 2 * BIG))
        .unwrap();
    let _ = wtxn.commit(); // aborts at the hook for H0..H4
    unreachable!("the abort hook must fire");
}

/// GC-14 for a run drawn from two records: a crash at every commit step —
/// after the free-page database is updated in memory (H0), after its pages
/// and the data pages are written but before the meta page (H1, H2), after
/// the meta write (H3, H4) — recovers either the state before `c`, with
/// `a`'s and `b`'s pages still listed free, or the state with `c` and both
/// records shrunk, never a mix: the partition walk is clean, and a `c`
/// written after recovery to the earlier state still finds the run (nothing
/// leaked).
#[test]
fn gc_cross_record_crash_matrix_recovers_consistently() {
    let exe = std::env::current_exe().expect("test binary path");
    let cases: [(&str, &[u64]); 5] = [
        ("H0", &[CROSS_TXNS]),
        ("H1", &[CROSS_TXNS]),
        ("H2", &[CROSS_TXNS]),
        ("H3", &[CROSS_TXNS, CROSS_TXNS + 1]),
        ("H4", &[CROSS_TXNS + 1]),
    ];
    let c = pages_value(b'c', 2 * BIG);
    for (hook, expect) in cases {
        let dir = TempDir::new();
        let status = Command::new(&exe)
            .arg("gc_cross_record_crash_child_runner")
            .arg("--exact")
            .arg("--nocapture")
            .env(CROSS_CHILD_ENV_VAR, hook)
            .env(DIR_ENV_VAR, dir.path())
            .status()
            .expect("spawn child");
        assert!(!status.success(), "{hook}: child must die at the hook");

        let env = open(dir.path());
        let recovered = env.txnid();
        assert!(
            expect.contains(&recovered),
            "{hook}: recovered txn {recovered}, expected one of {expect:?}"
        );
        let db = env.main_database();
        let rtxn = env.read_txn().unwrap();
        let got = db.get(&rtxn, b"c").unwrap().map(<[u8]>::to_vec);
        assert_eq!(db.get(&rtxn, b"a").unwrap(), None, "{hook}");
        assert_eq!(db.get(&rtxn, b"b").unwrap(), None, "{hook}");
        drop(rtxn);
        assert_clean(dir.path(), PS);
        if recovered == CROSS_TXNS {
            assert_eq!(
                got, None,
                "{hook}: data from the unrecovered txn is visible"
            );
            // Nothing leaked: the run across `a`'s and `b`'s records is
            // still listed free and serves `c` now.
            let before = file_size(dir.path());
            commit_put(&env, b"c", &c);
            let grew = (file_size(dir.path()) - before) / u64::from(PS);
            assert!(
                grew < 8,
                "{hook}: c after recovery grew the file by {grew} pages"
            );
        } else {
            assert_eq!(got.as_deref(), Some(c.as_slice()), "{hook}");
            commit_put(&env, b"d", &pages_value(b'd', 3));
        }
        drop(env);
        assert_clean(dir.path(), PS);
    }
}
