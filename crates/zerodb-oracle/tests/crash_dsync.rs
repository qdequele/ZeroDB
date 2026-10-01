//! ADR-0019 integration coverage: the durable meta write (fused C4+C5),
//! its per-mode routing as seen by the fault backend, and the REC-13
//! failed-write scrub end-to-end — including the guarantee the scrub exists
//! for: a clean reopen *before* power loss must not read back a meta whose
//! commit was never acknowledged.

use std::path::PathBuf;

use zerodb_core::env::{open_with_backing_policy, DurabilityFlags, Env};
use zerodb_io::fault::{FaultBacking, FaultHandle};
use zerodb_oracle::crash::verify::image_txnid;
use zerodb_oracle::tempdir::TempDir;

const PS: u32 = 4096;
const MAP: u64 = 16 << 20;

/// A real temp-file env under the fault wrapper (the image mechanism's
/// setup), with the given durability flags.
fn fault_env(tag: &str, durability: DurabilityFlags) -> (TempDir, Env, FaultHandle) {
    let tmp = TempDir::new().expect("tempdir");
    let data_path = tmp.path().join(zerodb::DATA_FILE_NAME);
    let opened = zerodb_io::open_or_create(&data_path, PS, Some(MAP), MAP, false, false)
        .expect("open_or_create");
    let (fault, handle) = FaultBacking::wrap(opened.backing, PS).expect("fault wrap");
    let env = open_with_backing_policy(
        PathBuf::from(format!(
            "/crash-dsync/{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        )),
        Box::new(fault),
        PS,
        MAP,
        false,
        16,
        126,
        durability,
        zerodb::FileTrust::VALIDATE,
        false,
        None,
    )
    .expect("env open");
    (tmp, env, handle)
}

fn commit_n(env: &Env, n: u64) {
    for i in 0..n {
        let mut w = env.write_txn().expect("write txn");
        let db = env.create_database(&mut w, None).expect("main db");
        db.put(&mut w, format!("k{i}").as_bytes(), b"v")
            .expect("put");
        w.commit().expect("commit");
    }
}

/// Default mode: every effective commit issues exactly one durable meta write
/// and one data barrier (C3) — the ADR-0019 ≈1-barrier-per-commit shape — and
/// a post-commit capture has nothing pending (the fused C4+C5 folded itself).
#[test]
fn default_mode_routes_meta_through_durable_write() {
    let (_tmp, env, handle) = fault_env("default", DurabilityFlags::default());
    commit_n(&env, 3);
    let s = handle.stats();
    assert_eq!(s.durable_writes, 3, "one durable meta write per commit");
    assert_eq!(s.barriers, 3, "one C3 barrier per commit — C5 is gone");
    let cap = handle.capture();
    assert!(
        cap.pending.is_empty(),
        "after an acked default-mode commit nothing may be losable"
    );
    assert_eq!(image_txnid(&cap.floor_image(), PS), Some(3));
}

/// NO_META_SYNC and NO_SYNC route the meta through the plain fd: zero durable
/// writes (SPEC 01 §S6 routing; the dsync fd stays unused, exactly LMDB's
/// `mfd = me_fd` arm).
#[test]
fn relaxed_modes_never_use_the_durable_write() {
    for (tag, d) in [
        (
            "nometasync",
            DurabilityFlags {
                no_meta_sync: true,
                ..Default::default()
            },
        ),
        (
            "nosync",
            DurabilityFlags {
                no_sync: true,
                ..Default::default()
            },
        ),
    ] {
        let (_tmp, env, handle) = fault_env(tag, d);
        commit_n(&env, 2);
        let s = handle.stats();
        assert_eq!(
            s.durable_writes, 0,
            "{tag}: meta must go through the plain fd"
        );
        let cap = handle.capture();
        assert!(
            cap.last_meta_write().is_some(),
            "{tag}: the un-synced meta write must be pending (its crash window)"
        );
    }
}

/// The REC-13 scrub end-to-end: a failed durable meta write fails the commit,
/// poisons the env, and — the guarantee the scrub exists for — a clean reopen
/// of the store (the OS-page-cache view, which the failed write may have
/// reached) recovers the last ACKED txnid, never the unacknowledged one.
#[test]
fn failed_durable_meta_write_scrubs_poisons_and_reopen_sees_acked_state() {
    let (tmp, env, handle) = fault_env("scrub", DurabilityFlags::default());
    commit_n(&env, 2); // acked: txnid 2
    handle.set_fail_durable_writes(true);

    let mut w = env.write_txn().expect("write txn");
    let db = env.create_database(&mut w, None).expect("main db");
    db.put(&mut w, b"doomed", b"v").expect("put");
    match w.commit() {
        Err(zerodb::Error::Io(_)) => {}
        other => panic!("commit over a failed durable write: expected Io error, got {other:?}"),
    }
    assert!(env.inner().is_poisoned(), "REC-13: env must be poisoned");
    assert_eq!(env.txnid(), 2, "failed commit must not publish txnid 3");

    // The fault journal holds the failed durable write AND the scrub, in
    // issue order: materializations cover {absent, torn, intact, scrubbed}.
    let cap = handle.capture();
    assert_eq!(
        handle.stats().durable_writes,
        3,
        "two fused commits + the failed one"
    );
    assert!(
        cap.pending.len() >= 2,
        "failed write + scrub must be pending"
    );
    // All-applied in issue order == the page-cache view: the scrub wins.
    assert_eq!(
        image_txnid(&cap.ceil_image(), PS),
        Some(2),
        "the scrub must erase the unacknowledged meta from the live view"
    );
    // Power cut right here loses both pending writes: still txnid 2.
    assert_eq!(image_txnid(&cap.floor_image(), PS), Some(2));

    // And the real store on disk (written through by the live view): a clean
    // reopen — the "reopen before power loss" — sees exactly the acked state.
    drop(env);
    let mut ropts = zerodb::EnvOpenOptions::new();
    ropts.max_dbs(16);
    let reopened = ropts.open(tmp.path()).expect("reopen after poisoned env");
    assert_eq!(
        reopened.txnid(),
        2,
        "reopen must recover the acked txnid, not the unacknowledged commit"
    );
    let r = reopened.read_txn().expect("read txn");
    let db = reopened
        .open_database(&r, None)
        .expect("open main")
        .expect("main exists");
    assert_eq!(
        db.get(&r, b"doomed").expect("get"),
        None,
        "the failed commit's data must be invisible after reopen"
    );
    assert!(db.get(&r, b"k1").expect("get").is_some());
}
