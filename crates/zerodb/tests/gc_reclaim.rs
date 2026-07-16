//! Milestone 1.5 GC guard tests (ADR-0005 D6) over **real files**: freed-page
//! reclamation (SPEC 05 GC-14..20), the freelist-writes-to-itself commit step
//! (GC-11..13), the partial-drain remainder rewrite (GC-20), the interim
//! oldest-reader gate (ADR-0005 OQ1; SPEC 04 TXN-21 as amended), the huge-txn
//! PIL spill (GC-5/GC-26 — the Phase 3.1 baseline), `non_free_pages_size`
//! (GC-22..24, INV-27), and GC atomicity across crashes (GC-14, REC-6).
//!
//! Every committed image is validated with `zerodb::check::check_image`,
//! **including** the INV-22 reachable-XOR-free partition — a leaked page
//! ("neither") or a double-hand-out ("both") fails loudly. Do not weaken
//! (CLAUDE.md rule 2).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zerodb::{check, free_page_count, CommitHook, Env, EnvOpenOptions, HookPoint};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-gc-{pid}-{seq}"));
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
const MAP: usize = 16 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

fn file_size(dir: &Path) -> u64 {
    std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME))
        .unwrap()
        .len()
}

/// Bounded file growth on insert/delete churn (PLAN §1.5 acceptance;
/// ADR-0005 D5 flatness bound): with a constant live set, reclamation must
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
        assert_clean(dir.path());
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

/// GC-20 partial-drain remainder rewrite (ADR-0005 D6 risk 3): draining part
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
    assert_clean(dir.path());
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
    assert_clean(dir.path()); // INV-22/24: remainder listed once, drained ids not
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
    assert_clean(dir.path());
    assert_eq!(
        file_size(dir.path()),
        size_after_del,
        "post-reopen inserts must be served from the rewritten remainder"
    );
}

/// GC-11..13 freelist-writes-to-itself (ADR-0005 D6 risk 1) + the GC-26
/// huge-txn spill: one txn frees far more than the `c >= 251` (psize 4096)
/// spill threshold, so the PIL goes to an overflow run, and `freelist_save`'s
/// own tree writes free/allocate GC pages mid-save. INV-22 catches any page
/// the loop leaks or double-lists. Also records the Phase-3.1 baseline
/// numbers (GC-26) in the test output.
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
    assert_clean(dir.path());
    // One txn freeing the whole tree: clear().
    let t0 = std::time::Instant::now();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.clear(&mut wtxn).unwrap();
        wtxn.commit().unwrap();
    }
    let commit_time = t0.elapsed();
    assert_clean(dir.path());
    let rtxn = env.read_txn().unwrap();
    let free = free_page_count(&rtxn).unwrap();
    drop(rtxn);
    assert!(
        free > 251,
        "expected a spilled PIL (> 251 freed pages at psize 4096, GC-26), got {free}"
    );
    // GC-26 baseline for Phase 3.1 (printed, not asserted).
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
    assert_clean(dir.path());
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

/// The interim oldest-reader gate (ADR-0005 OQ1, D6 risk 2 —
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
    assert_clean(dir.path());
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
    assert_clean(dir.path());
    // Same 2-page slack as above: the commit's own GC storage ages one txn.
    assert!(
        file_size(dir.path()) <= size_pinned + 2 * u64::from(PS),
        "post-release rewrite must reuse the previously gated pages \
         ({size_pinned} -> {})",
        file_size(dir.path())
    );
}

/// INV-27 (SPEC 05 §9): `real_disk_size − non_free_pages_size ==
/// free_page_count * psize`, asserted at the API level against an
/// independent walk under a read snapshot (GC-22/23).
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
        drop(rtxn);
        let disk = env.real_disk_size().unwrap();
        let non_free = env.non_free_pages_size().unwrap();
        assert_eq!(
            disk - non_free,
            free * u64::from(PS),
            "INV-27 violated at cycle {cycle}: disk={disk} non_free={non_free} free={free}"
        );
        assert!(non_free > 0 && non_free <= disk);
    }
}

// ---------------------------------------------------------------------------
// GC atomicity across crashes (GC-14; ADR-0005 D6, crash-churn variant of the
// M1.4 smoke — the full harness is M1.11).
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
        assert_clean(dir.path());
    }
}
