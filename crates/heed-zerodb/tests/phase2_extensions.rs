//! Phase 2 tranche A at the adapter boundary (milestones 2.1, 2.5, 2.6).
//!
//! Two things are checked here that the native `zerodb` tests cannot:
//!
//!   1. The **completed** `Env::info()` / `Env::stat()` / `max_readers()`
//!      reach the adapter with real values (Phase 1 hardcoded zeros and the
//!      126 constant), *without* changing the mirrored heed struct shapes.
//!   2. The two **new** methods — `EnvOpenOptions::page_size` and
//!      `Env::sync(force)` — exist, work, and are purely additive: heed has
//!      neither, and code that never calls them behaves exactly as before.

use heed_zerodb::types::{Bytes, Str};
use heed_zerodb::{Database, EnvOpenOptions};

fn env_opts() -> EnvOpenOptions<heed_zerodb::WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

// ---------------------------------------------------------------------------
// 2.1 — stat / info
// ---------------------------------------------------------------------------

#[test]
fn env_info_is_fully_populated_through_the_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(8).max_readers(64);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    // Fresh env.
    let i = env.info();
    assert!(i.map_addr.is_null(), "no fixed map (MDB_FIXEDMAP is WON'T)");
    assert_eq!(i.map_size, 4 * 1024 * 1024);
    assert_eq!(
        i.maximum_number_of_readers, 64,
        "Phase 1 returned 0 here; 2.1 must report the configured value"
    );
    assert_eq!(env.max_readers(), 64, "Phase 1 hardcoded 126 here");
    assert_eq!(i.number_of_readers, 0);
    let txn0 = i.last_txn_id;

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    for k in 0..2000u32 {
        db.put(&mut wtxn, &format!("k{k:06}"), b"value").unwrap();
    }
    wtxn.commit().unwrap();

    let i = env.info();
    assert_eq!(i.last_txn_id, txn0 + 1, "one commit advances the txn id");
    assert!(
        i.last_page_number > 0,
        "Phase 1 returned 0 here; 2.1 must report the real high-water"
    );

    // Reader counters through the adapter.
    let r1 = env.read_txn().unwrap();
    let r2 = env.read_txn().unwrap();
    assert_eq!(env.info().number_of_readers, 2);
    assert_eq!(env.live_readers(), 2);
    drop(r1);
    drop(r2);
    // D-011: `number_of_readers` mirrors LMDB's high-water `me_numreaders`, so
    // it stays at 2; the ZeroDB extension `live_readers` drops to 0.
    assert_eq!(
        env.info().number_of_readers,
        2,
        "high-water, not live count"
    );
    assert_eq!(env.live_readers(), 0);
}

#[test]
fn env_stat_reports_the_main_tree_through_the_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(64);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    assert_eq!(env.stat().entries, 0, "fresh env: empty main tree");
    assert_eq!(env.stat().depth, 0);

    let mut wtxn = env.write_txn().unwrap();
    for n in 0..20u32 {
        let db: Database<Str, Bytes> = env
            .create_database(&mut wtxn, Some(&format!("db{n:03}")))
            .unwrap();
        db.put(&mut wtxn, "k", b"v").unwrap();
    }
    wtxn.commit().unwrap();

    let s = env.stat();
    assert_eq!(s.entries, 20, "one main-DB record per named DB");
    assert!(s.depth >= 1);
    assert!(s.leaf_pages >= 1);
    assert!(s.page_size >= 4096);
}

#[test]
fn env_stat_works_with_the_reader_table_exhausted() {
    // Phase 1's adapter `stat()` opened its own read txn and silently returned
    // all-zeros if that failed. 2.1 reads the published snapshot instead.
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4).max_readers(1);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    db.put(&mut wtxn, "k", b"v").unwrap();
    wtxn.commit().unwrap();

    let _hold = env.read_txn().unwrap();
    assert!(env.read_txn().is_err(), "precondition: table exhausted");
    assert_eq!(
        env.stat().entries,
        1,
        "stat must not depend on a reader slot"
    );
}

// ---------------------------------------------------------------------------
// 2.5 — sync(force)
// ---------------------------------------------------------------------------

#[test]
fn sync_and_force_sync_are_both_reachable_and_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    db.put(&mut wtxn, "k", b"v").unwrap();
    wtxn.commit().unwrap();

    // heed's form.
    env.force_sync().expect("force_sync");
    // The 2.5 extension, both values. On a default (non-NO_SYNC) env both
    // flush, so both simply succeed; the behavioral split is only visible
    // under NO_SYNC and is asserted in
    // `zerodb-oracle/tests/force_sync_durability.rs` against the fault backing.
    env.sync(true).expect("sync(true)");
    env.sync(false).expect("sync(false)");

    // Data still readable afterwards.
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, "k").unwrap(), Some(b"v".as_slice()));
}

#[test]
fn sync_on_a_readonly_env_is_eacces() {
    // `EACCES` is 13 on every Unix ZeroDB targets; spelled out to avoid adding
    // a `libc` dependency to this crate.
    const EACCES: i32 = 13;
    let dir = tempfile::tempdir().unwrap();
    {
        let mut opts = env_opts();
        opts.map_size(1024 * 1024).max_dbs(4);
        let env = unsafe { opts.open(dir.path()).unwrap() };
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
        db.put(&mut wtxn, "k", b"v").unwrap();
        wtxn.commit().unwrap();
    }
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    unsafe { opts.flags(heed_zerodb::EnvFlags::READ_ONLY) };
    let env = unsafe { opts.open(dir.path()).unwrap() };

    for err in [
        env.force_sync().unwrap_err(),
        env.sync(true).unwrap_err(),
        env.sync(false).unwrap_err(),
    ] {
        match err {
            heed_zerodb::Error::Io(e) => assert_eq!(
                e.raw_os_error(),
                Some(EACCES),
                "read-only sync must be EACCES, got {e:?}"
            ),
            other => panic!("expected Io(EACCES), got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// 2.6 — page_size
// ---------------------------------------------------------------------------

#[test]
fn page_size_is_selectable_through_the_adapter() {
    for psize in [4096u32, 8192, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = env_opts();
        opts.map_size(8 * 1024 * 1024).max_dbs(4).page_size(psize);
        let env = unsafe { opts.open(dir.path()).unwrap() };
        assert_eq!(
            env.stat().page_size,
            psize,
            "adapter must forward the page-size selection"
        );

        // And the env is fully functional at that geometry.
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
        for k in 0..500u32 {
            db.put(&mut wtxn, &format!("k{k:06}"), &[b'v'; 300])
                .unwrap();
        }
        wtxn.commit().unwrap();
        let rtxn = env.read_txn().unwrap();
        assert_eq!(db.len(&rtxn).unwrap(), 500);
        assert_eq!(
            db.get(&rtxn, "k000250").unwrap(),
            Some([b'v'; 300].as_slice())
        );
    }
}

#[test]
fn omitting_page_size_defaults_to_the_os_page_size() {
    // LMDB parity (contract CHANGED 2026-07-21, perf-parity spike): the fork
    // derives `me_psize` from `sysconf(_SC_PAGE_SIZE)` (capped 64 K) at store
    // creation, so an adapter-created store must adopt the same geometry —
    // 16 K on Apple Silicon, 4 K on x86_64 Linux — not the native engine's
    // fixed 4 K default. Before this change the adapter silently created 4 K
    // stores on 16 K-page hosts, a heed-observable divergence
    // (`Env::stat().page_size`) and an unfair handicap vs LMDB. SPEC 00
    // row 164 records the new default.
    let os_page = u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) })
        .unwrap()
        .clamp(zerodb::MIN_PAGE_SIZE, zerodb::MAX_PAGE_SIZE);
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };
    assert_eq!(env.stat().page_size, os_page);
}

#[test]
fn an_invalid_page_size_is_rejected_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).page_size(3000);
    match unsafe { opts.open(dir.path()) } {
        Err(heed_zerodb::Error::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        }
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
}

#[test]
fn page_size_survives_the_tls_retag() {
    // `read_txn_without_tls()` rebuilds the options struct; the new field must
    // be carried across (a field added to a builder is easy to drop in retag).
    let dir = tempfile::tempdir().unwrap();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(4 * 1024 * 1024).page_size(16384);
    let mut opts = opts.read_txn_without_tls();
    opts.max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };
    assert_eq!(env.stat().page_size, 16384, "page_size lost across retag");
}

// ---------------------------------------------------------------------------
// Phase 2 tranche B (milestones 2.2, 2.3, 2.4, 2.7) at the adapter boundary
// ---------------------------------------------------------------------------

// 2.2 — reader introspection -------------------------------------------------

#[test]
fn reader_list_through_the_adapter_tracks_live_read_txns() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(4).max_readers(16);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    db.put(&mut wtxn, "k", b"v").unwrap();
    wtxn.commit().unwrap();

    assert!(env.reader_list().is_empty());

    let r1 = env.read_txn().unwrap();
    let r2 = env.read_txn().unwrap();
    let list = env.reader_list();
    assert_eq!(list.len(), 2, "both read txns occupy listed slots");
    assert_eq!(list.len() as u32, env.live_readers());
    let pinned = env.info().last_txn_id as u64;
    for e in &list {
        assert_eq!(e.txnid, Some(pinned));
        assert_eq!(e.age, Some(0));
    }

    // Ages advance with commits while the readers stay pinned.
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, "k2", b"v").unwrap();
    wtxn.commit().unwrap();
    assert!(env.reader_list().iter().all(|e| e.age == Some(1)));

    drop(r1);
    assert_eq!(env.reader_list().len(), 1);
    drop(r2);
    assert!(env.reader_list().is_empty());
}

#[test]
fn clear_stale_readers_through_the_adapter_is_zero_and_harmless() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(2);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    assert_eq!(env.clear_stale_readers().unwrap(), 0);
    let r = env.read_txn().unwrap();
    assert_eq!(
        env.clear_stale_readers().unwrap(),
        0,
        "a live reader is not stale (D-001: no cross-process readers exist)"
    );
    assert_eq!(env.reader_list().len(), 1, "and it was not evicted");
    drop(r);
}

// 2.3 — copy with progress ---------------------------------------------------

#[test]
fn copy_to_path_with_progress_through_the_adapter() {
    use heed_zerodb::CompactionOption;

    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(16 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    for k in 0..3000u32 {
        db.put(&mut wtxn, &format!("k{k:06}"), &[b'x'; 100])
            .unwrap();
    }
    wtxn.commit().unwrap();

    for (option, name) in [
        (CompactionOption::Enabled, "compact.mdb"),
        (CompactionOption::Disabled, "raw.mdb"),
    ] {
        let out = dir.path().join(name);
        let mut seen = Vec::new();
        env.copy_to_path_with_progress(&out, option, &mut |p| seen.push(p))
            .unwrap();
        assert!(
            seen.len() >= 2,
            "{name}: opening and closing calls at least"
        );
        assert_eq!(seen[0].done, 0);
        assert_eq!(seen.last().unwrap().done, seen.last().unwrap().total);
        assert!(
            seen.windows(2).all(|w| w[0].done <= w[1].done),
            "{name}: monotone"
        );
        assert!(out.exists());
    }
}

// 2.4 — heed's type-level comparator is finally honored -----------------------

/// Descending byte order, in heed's type-level `Comparator` shape.
enum ReverseComparator {}

impl heed_zerodb::Comparator for ReverseComparator {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
}

#[test]
fn database_open_options_key_comparator_is_actually_applied() {
    // Before 2.4 the `C` type parameter on `DatabaseOpenOptions::key_comparator`
    // was accepted and then silently ignored — every database was memcmp. A
    // caller asking for a different ordering got no error and no effect. This
    // asserts the ordering now reaches the engine.
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(8 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let rev: Database<Str, Bytes, ReverseComparator> = env
        .database_options()
        .types::<Str, Bytes>()
        .key_comparator::<ReverseComparator>()
        .name("rev")
        .create(&mut wtxn)
        .unwrap();
    let plain: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("plain")).unwrap();
    for k in ["a", "b", "c", "d"] {
        rev.put(&mut wtxn, k, b"v").unwrap();
        plain.put(&mut wtxn, k, b"v").unwrap();
    }
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let got: Vec<String> = rev
        .iter(&rtxn)
        .unwrap()
        .map(|e| e.unwrap().0.to_string())
        .collect();
    assert_eq!(
        got,
        vec!["d", "c", "b", "a"],
        "the registered comparator governs iteration order"
    );
    // Every key still resolves through the comparator-aware descent.
    for k in ["a", "b", "c", "d"] {
        assert!(rev.get(&rtxn, k).unwrap().is_some(), "missing {k}");
    }
    // The DefaultComparator database in the same env is untouched.
    let got: Vec<String> = plain
        .iter(&rtxn)
        .unwrap()
        .map(|e| e.unwrap().0.to_string())
        .collect();
    assert_eq!(got, vec!["a", "b", "c", "d"]);
}

#[test]
fn default_comparator_databases_are_unaffected_by_the_2_4_plumbing() {
    // The whole consumer tree is DefaultComparator (SPEC 00 row 53). 2.4 must
    // be a strict no-op for them — no registration, no behavior change.
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(4 * 1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    for k in 0..500u32 {
        db.put(&mut wtxn, &format!("k{k:05}"), b"v").unwrap();
    }
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let got: Vec<String> = db
        .iter(&rtxn)
        .unwrap()
        .map(|e| e.unwrap().0.to_string())
        .collect();
    let mut expected: Vec<String> = (0..500u32).map(|k| format!("k{k:05}")).collect();
    expected.sort();
    assert_eq!(got, expected, "plain byte order, exactly as before 2.4");
    drop(rtxn);

    // And compaction — which refuses a custom-comparator env — still works.
    let out = dir.path().join("c.mdb");
    env.copy_to_path(&out, heed_zerodb::CompactionOption::Enabled)
        .expect("no custom comparator registered, so compaction is available");
    assert!(out.exists());
}

// 2.7 — SHOULD leftovers -----------------------------------------------------

#[test]
fn max_key_size_reflects_the_engine_constant() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024);
    let env = unsafe { opts.open(dir.path()).unwrap() };
    assert_eq!(
        env.max_key_size(),
        zerodb::MAX_KEY_SIZE,
        "must read the engine's constant, not a copy of its current value \
         (the M2.1 max_readers defect class)"
    );
    assert_eq!(env.max_key_size(), 511, "and that constant is still 511");
}

#[test]
fn txn_id_is_exposed_on_both_txn_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(2 * 1024 * 1024).max_dbs(2);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let before = env.info().last_txn_id;
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("d")).unwrap();
    db.put(&mut wtxn, "k", b"v").unwrap();
    assert_eq!(
        wtxn.id(),
        before + 1,
        "a write txn's id is the commit it will publish"
    );
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        rtxn.id(),
        env.info().last_txn_id,
        "a read txn's id is its pinned snapshot"
    );
    // ... and it agrees with what reader_list reports for that reader's slot.
    let list = env.reader_list();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].txnid, Some(rtxn.id() as u64));
}
