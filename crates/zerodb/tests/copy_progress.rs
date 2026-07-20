//! Milestone 2.3 — `copy_to_file_with_progress`.
//!
//! Phase 2 acceptance: "doc + unit tests where it's zerodb-defined". This is
//! entirely zerodb-defined — `mdb_env_copy2` reports no progress and heed
//! exposes no callback, so there is nothing to diff against. What the M1.12
//! oracle already covers (that the *copy itself* is byte-correct in both
//! modes, `zerodb-oracle/tests/copy_to_file_differential.rs`) is unchanged and
//! not re-litigated here; these tests are about the callback contract and,
//! critically, that adding it did not change the bytes.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{CompactionOption, CopyProgress, CopyToFile, Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-copyprog-{pid}-{nanos}-{seq}"));
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

/// An env with enough data to span many pages and several named DBs, so the
/// progress sequence has real intermediate steps rather than just 0 → total.
fn populated(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(64 << 20);
    opts.max_dbs(8);
    opts.max_readers(16);
    let env = opts.open(dir).expect("open env");

    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"main-key", b"v").unwrap();
    for d in 0..4 {
        let name = format!("db{d}");
        let db = env.create_database(&mut w, Some(name.as_bytes())).unwrap();
        for i in 0..2000 {
            let k = format!("k{i:06}");
            db.put(&mut w, k.as_bytes(), &[b'x'; 200]).unwrap();
        }
    }
    w.commit().unwrap();
    env
}

/// Record every callback and assert the universal contract on the sequence.
fn run(env: &Env, dest: &Path, option: CompactionOption) -> Vec<CopyProgress> {
    let mut seen: Vec<CopyProgress> = Vec::new();
    env.copy_to_file_with_progress(dest, option, &mut |p| seen.push(p))
        .expect("copy succeeds");

    assert!(
        seen.len() >= 2,
        "at least an opening and a closing call: {seen:?}"
    );
    let total = seen[0].total;
    assert!(total > 0, "a populated env has pages to copy");
    assert!(
        seen.iter().all(|p| p.total == total),
        "`total` is fixed for the whole run: {seen:?}"
    );
    assert_eq!(seen[0].done, 0, "the first call reports no work done");
    assert_eq!(
        seen.last().unwrap().done,
        total,
        "the last call reports done == total"
    );
    assert!(
        seen.windows(2).all(|w| w[0].done <= w[1].done),
        "progress is monotonically non-decreasing: {seen:?}"
    );
    assert!(
        seen.iter().all(|p| p.done <= p.total),
        "`done` never exceeds `total` (the compacting estimate is clamped): {seen:?}"
    );
    seen
}

#[test]
fn raw_copy_progress_reaches_the_total_monotonically() {
    let dir = TempDir::new();
    let env = populated(dir.path());
    let out = dir.path().join("raw.dat");

    let seen = run(&env, &out, CompactionOption::Disabled);

    // The raw copy's total is EXACT: every page of the snapshot is copied.
    let expected_total = env.info().last_pgno + 1;
    assert_eq!(
        seen[0].total, expected_total,
        "for a raw copy `total` is the snapshot's exact page count"
    );
    let file_pages = std::fs::metadata(&out).unwrap().len() / u64::from(env.page_size());
    assert_eq!(
        file_pages, expected_total,
        "and the file that lands has exactly that many pages"
    );
    assert!(
        seen.len() > 2,
        "an env this size reports intermediate progress, not just 0 and total: {}",
        seen.len()
    );
}

#[test]
fn compacting_copy_progress_reaches_the_total_monotonically() {
    let dir = TempDir::new();
    let env = populated(dir.path());
    let out = dir.path().join("compact.dat");

    let seen = run(&env, &out, CompactionOption::Enabled);
    assert!(
        seen.len() > 2,
        "one step per named DB section plus the ends: {}",
        seen.len()
    );
    assert!(out.exists());
}

#[test]
fn progress_callback_does_not_change_the_bytes_written() {
    // The no-callback signature is heed parity and must be untouched (PLAN
    // ground rule for this tranche). Prove it by producing both ways and
    // comparing byte for byte, in both modes.
    for option in [CompactionOption::Disabled, CompactionOption::Enabled] {
        let dir = TempDir::new();
        let env = populated(dir.path());
        let plain = dir.path().join("plain.dat");
        let instrumented = dir.path().join("instrumented.dat");

        env.copy_to_file(&plain, option).unwrap();
        let mut n = 0u32;
        env.copy_to_file_with_progress(&instrumented, option, &mut |_| n += 1)
            .unwrap();

        assert!(n >= 2, "the instrumented run really did call back");
        assert_eq!(
            std::fs::read(&plain).unwrap(),
            std::fs::read(&instrumented).unwrap(),
            "instrumenting a copy must not alter one byte of the output ({option:?})"
        );
    }
}

#[test]
fn an_empty_env_still_gets_a_well_formed_progress_sequence() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(1 << 20);
    let env = opts.open(dir.path()).unwrap();

    for (option, name) in [
        (CompactionOption::Disabled, "raw.dat"),
        (CompactionOption::Enabled, "compact.dat"),
    ] {
        let mut seen: Vec<CopyProgress> = Vec::new();
        env.copy_to_file_with_progress(dir.path().join(name), option, &mut |p| seen.push(p))
            .unwrap();
        assert!(!seen.is_empty(), "even an empty env reports something");
        assert_eq!(seen[0].done, 0);
        assert_eq!(seen.last().unwrap().done, seen.last().unwrap().total);
        assert!(seen.windows(2).all(|w| w[0].done <= w[1].done));
    }
}

// ---------------------------------------------------------------------------
// A panicking callback
// ---------------------------------------------------------------------------

/// The documented contract: a panicking callback unwinds out of the copy, and
/// because every callback fires before any destination write, the destination
/// is left exactly as it was — there is no truncated or half-written image to
/// be mistaken for a good copy.
fn panics_without_corrupting(option: CompactionOption, panic_on_call: u32) {
    let dir = TempDir::new();
    let env = populated(dir.path());
    let out = dir.path().join("out.dat");

    // Pre-seed the destination with recognizable content so we can prove the
    // failed copy did not touch it (the harder case than "file absent").
    let sentinel = b"PRE-EXISTING CONTENT, MUST SURVIVE A PANICKING CALLBACK".to_vec();
    std::fs::write(&out, &sentinel).unwrap();

    let mut calls = 0u32;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        env.copy_to_file_with_progress(&out, option, &mut |_| {
            calls += 1;
            if calls == panic_on_call {
                panic!("callback panics on call {panic_on_call}");
            }
        })
    }));

    assert!(
        result.is_err(),
        "the panic propagates out of copy_to_file_with_progress ({option:?})"
    );
    assert_eq!(
        std::fs::read(&out).unwrap(),
        sentinel,
        "the destination is byte-for-byte untouched ({option:?}, panic on call \
         {panic_on_call}) — no partial copy is ever left behind"
    );

    // The source env is unharmed and still fully usable: no leaked reader slot,
    // no poisoned state, and a subsequent copy succeeds.
    assert_eq!(
        env.reader_list().len(),
        0,
        "the internal read txn released its slot while unwinding ({option:?})"
    );
    let good = dir.path().join("good.dat");
    env.copy_to_file(&good, option)
        .expect("the env still copies fine after a panicking callback");
    assert!(good.exists());

    let r = env.read_txn().unwrap();
    assert!(env.main_database().get(&r, b"main-key").unwrap().is_some());
}

#[test]
fn panicking_callback_leaves_the_raw_copy_destination_untouched() {
    // First call (before any work) and a middle call (mid-assembly).
    panics_without_corrupting(CompactionOption::Disabled, 1);
    panics_without_corrupting(CompactionOption::Disabled, 3);
}

#[test]
fn panicking_callback_leaves_the_compacting_copy_destination_untouched() {
    panics_without_corrupting(CompactionOption::Enabled, 1);
    panics_without_corrupting(CompactionOption::Enabled, 3);
}

// ---------------------------------------------------------------------------
// The copy is still a usable env
// ---------------------------------------------------------------------------

#[test]
fn a_progress_tracked_copy_reopens_with_the_same_contents() {
    for (option, name) in [
        (CompactionOption::Disabled, "raw"),
        (CompactionOption::Enabled, "compact"),
    ] {
        let src = TempDir::new();
        let env = populated(src.path());
        let dst = TempDir::new();

        env.copy_to_file_with_progress(
            dst.path().join(zerodb::DATA_FILE_NAME),
            option,
            &mut |_| {},
        )
        .unwrap();

        let mut opts = EnvOpenOptions::new();
        opts.map_size(64 << 20);
        opts.max_dbs(8);
        let copy = opts.open(dst.path()).expect("the copy opens as an env");

        let r = copy.read_txn().unwrap();
        assert_eq!(
            copy.main_database().get(&r, b"main-key").unwrap(),
            Some(b"v".as_slice()),
            "{name}: main DB data survived"
        );
        for d in 0..4 {
            let db = copy
                .open_database(&r, Some(format!("db{d}").as_bytes()))
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: db{d} missing from the copy"));
            assert_eq!(db.len(&r).unwrap(), 2000, "{name}: db{d} entry count");
            assert_eq!(
                db.get(&r, b"k001234").unwrap().map(<[u8]>::len),
                Some(200),
                "{name}: db{d} values intact"
            );
        }
        drop(r);

        let bytes = std::fs::read(dst.path().join(zerodb::DATA_FILE_NAME)).unwrap();
        let v = zerodb::check::check_image(&bytes, copy.page_size());
        assert!(v.is_empty(), "{name}: copy fails invariants: {v:#?}");
    }
}
