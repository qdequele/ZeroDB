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
fn omitting_page_size_keeps_the_pre_2_6_default() {
    // The additive guarantee: an options builder that never mentions page_size
    // must behave exactly as it did before 2.6.
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };
    assert_eq!(env.stat().page_size, zerodb::DEFAULT_PAGE_SIZE);
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
