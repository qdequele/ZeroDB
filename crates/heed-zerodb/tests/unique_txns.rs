//! The API of Meilisearch's heed fork (`meilisearch/heed@mutexify-open-dbi`),
//! built with the `unique-txns` feature (ADR-0027): unique transactions hold
//! the environment's database-open mutex, only they open or create
//! databases, and `UniqueRoTxn::commit` replaces `RoTxn::commit`.
//!
//! Run with `cargo test -p heed-zerodb --features unique-txns`. The other test
//! files cover heed 0.22.1's API and are compiled without the feature.

#![cfg(feature = "unique-txns")]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use heed_zerodb::types::{Bytes, Str};
use heed_zerodb::{AsUniqueTxnRef, Database, Env, EnvOpenOptions, Error, WithoutTls};

fn open_env(dir: &tempfile::TempDir) -> Env<WithoutTls> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(16 * 1024 * 1024).max_dbs(8);
    unsafe { opts.open(dir.path()).unwrap() }
}

/// Create a named database holding one entry, through a unique write txn.
fn create_filled(env: &Env<WithoutTls>, name: &str) {
    let mut wtxn = env.unique_write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some(name)).unwrap();
    db.put(&mut wtxn, "k", "v").unwrap();
    wtxn.commit().unwrap();
}

/// Time a blocked thread is given to (wrongly) get through.
const BLOCKED: Duration = Duration::from_millis(200);

#[test]
fn create_then_open_through_unique_txns() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);

    let mut wtxn = env.unique_write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("a")).unwrap();
    // A unique write txn derefs to `RwTxn`: ordinary writes and reads work.
    db.put(&mut wtxn, "k", "v").unwrap();
    assert_eq!(db.get(&wtxn, "k").unwrap(), Some("v"));
    // Opening through the write txn itself, as the fork allows.
    let again = env
        .open_database::<Str, Str, _>(&wtxn, Some("a"))
        .unwrap()
        .unwrap();
    assert_eq!(again.get(&wtxn, "k").unwrap(), Some("v"));
    wtxn.commit().unwrap();

    let rtxn = env.unique_read_txn().unwrap();
    let db: Database<Str, Str> = env
        .open_database::<Str, Str, _>(&rtxn, Some("a"))
        .unwrap()
        .unwrap();
    assert_eq!(db.get(&rtxn, "k").unwrap(), Some("v"));
    assert!(env
        .open_database::<Str, Str, _>(&rtxn, Some("absent"))
        .unwrap()
        .is_none());
    rtxn.commit().unwrap();

    // A handle kept by a committed unique txn works in ordinary txns.
    let plain = env.read_txn().unwrap();
    assert_eq!(db.get(&plain, "k").unwrap(), Some("v"));
}

#[test]
fn database_options_take_unique_txns() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);

    let mut wtxn = env.unique_write_txn().unwrap();
    let db = env
        .database_options()
        .types::<Str, Str>()
        .name("opts")
        .create(&mut wtxn)
        .unwrap();
    db.put(&mut wtxn, "k", "v").unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.unique_read_txn().unwrap();
    // `open` takes any `AsUniqueTxnRef`, references included.
    let by_ref = &rtxn;
    let db = env
        .database_options()
        .types::<Str, Str>()
        .name("opts")
        .open(&by_ref)
        .unwrap()
        .unwrap();
    assert_eq!(db.get(&rtxn, "k").unwrap(), Some("v"));
}

/// `UniqueRoTxn::commit` keeps the databases the txn opened; dropping the txn
/// closes them, as `RoTxn::commit` and a drop do on heed 0.22.1 (ADR-0024).
#[test]
fn unique_read_commit_keeps_opened_databases() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);
    create_filled(&env, "kept");
    create_filled(&env, "dropped");
    // Reopen so neither database is open any more.
    env.prepare_for_closing().wait();
    let env = open_env(&dir);

    let rtxn = env.unique_read_txn().unwrap();
    let kept: Database<Str, Str> = env.open_database(&rtxn, Some("kept")).unwrap().unwrap();
    rtxn.commit().unwrap();

    let rtxn = env.unique_read_txn().unwrap();
    let dropped: Database<Str, Str> = env.open_database(&rtxn, Some("dropped")).unwrap().unwrap();
    drop(rtxn);

    let rtxn = env.read_txn().unwrap();
    assert_eq!(kept.get(&rtxn, "k").unwrap(), Some("v"));
    match dropped.get(&rtxn, "k") {
        Err(Error::Io(e)) => assert_eq!(e.raw_os_error(), Some(libc::EINVAL)),
        other => panic!("expected EINVAL for a handle of a dropped read txn, got {other:?}"),
    }
}

#[test]
fn try_constructors_return_none_while_a_unique_txn_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);

    let held = env.unique_read_txn().unwrap();
    assert!(env.try_unique_read_txn().unwrap().is_none());
    assert!(env.try_unique_write_txn().unwrap().is_none());
    // Ordinary transactions do not take the mutex.
    drop(env.read_txn().unwrap());
    env.write_txn().unwrap().abort();
    drop(held);

    let held = env.unique_write_txn().unwrap();
    assert!(env.try_unique_read_txn().unwrap().is_none());
    held.abort();

    let rtxn = env.try_unique_read_txn().unwrap().expect("mutex is free");
    drop(rtxn);
    let wtxn = env.try_unique_write_txn().unwrap().expect("mutex is free");
    wtxn.commit().unwrap();
}

/// Clones of an env share one mutex: a unique txn on one clone blocks a
/// unique txn on another until it ends.
#[test]
fn unique_txns_wait_for_each_other_across_clones() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);
    create_filled(&env, "a");

    let held = env.unique_read_txn().unwrap();
    let (tx, rx) = mpsc::channel();
    let clone = env.clone();
    let waiter = thread::spawn(move || {
        let mut wtxn = clone.unique_write_txn().unwrap();
        let db: Database<Bytes, Bytes> = clone.create_database(&mut wtxn, Some("b")).unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        wtxn.commit().unwrap();
        tx.send(()).unwrap();
    });

    assert!(
        rx.recv_timeout(BLOCKED).is_err(),
        "a unique write txn began while a unique read txn held the mutex"
    );
    held.commit().unwrap();
    rx.recv_timeout(Duration::from_secs(10))
        .expect("the waiting unique write txn never began");
    waiter.join().unwrap();

    let rtxn = env.unique_read_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.open_database(&rtxn, Some("b")).unwrap().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
}

/// A panic while a unique txn is held poisons the mutex; the next unique txn
/// clears the poison and proceeds, as in the fork.
#[test]
fn a_panic_holding_the_mutex_does_not_lock_it_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);

    let clone = env.clone();
    let panicked = thread::spawn(move || {
        let _wtxn = clone.unique_write_txn().unwrap();
        panic!("panic while holding a unique write txn");
    })
    .join();
    assert!(panicked.is_err());

    let wtxn = env.try_unique_write_txn().unwrap().expect("poison cleared");
    wtxn.abort();
    let wtxn = env.unique_write_txn().unwrap();
    wtxn.abort();
}

#[test]
fn non_free_page_size_counts_tree_pages() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);

    let mut wtxn = env.unique_write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some("big")).unwrap();
    // Enough small entries for branch pages, and values larger than any
    // page size (up to 64 KiB) for overflow pages.
    let small = [7u8; 100];
    for i in 0u32..2000 {
        db.put(&mut wtxn, &i.to_be_bytes(), &small).unwrap();
    }
    let large = vec![9u8; 70_000];
    for i in 2000u32..2010 {
        db.put(&mut wtxn, &i.to_be_bytes(), &large).unwrap();
    }
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let stat = db.stat(&rtxn).unwrap();
    let pages = stat.leaf_pages + stat.branch_pages + stat.overflow_pages;
    assert!(stat.branch_pages > 0 && stat.overflow_pages > 0);
    assert_eq!(stat.non_free_page_size(), pages * stat.page_size as usize);

    let env_stat = env.stat();
    assert_eq!(
        env_stat.non_free_page_size(),
        (env_stat.leaf_pages + env_stat.branch_pages + env_stat.overflow_pages)
            * env_stat.page_size as usize
    );
    // The main database holds the catalog entry for "big".
    assert!(env_stat.non_free_page_size() > 0);
}

/// The trait is implemented for every unique txn type and for references to
/// them, so generic helpers like the fork's `list_dbs` example compile.
#[test]
fn as_unique_txn_ref_is_usable_generically() {
    fn open_a<'e, U: AsUniqueTxnRef<'e>>(env: &'e Env<WithoutTls>, txn: &U) -> bool {
        env.open_database::<Str, Str, _>(txn, Some("a"))
            .unwrap()
            .is_some()
    }

    let dir = tempfile::tempdir().unwrap();
    let env = open_env(&dir);
    create_filled(&env, "a");

    let wtxn = env.unique_write_txn().unwrap();
    assert!(open_a(&env, &wtxn));
    assert!(open_a(&env, &&wtxn));
    wtxn.abort();
    let rtxn = env.unique_read_txn().unwrap();
    assert!(open_a(&env, &rtxn));
}
