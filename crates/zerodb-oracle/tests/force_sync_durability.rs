//! Milestone 2.5 — explicit `sync(force)` / `force_sync()`, `mdb_env_sync`
//! parity, proved against the M1.11 fault-injection backing.
//!
//! The claim "`force_sync` makes everything durable" is only meaningful if
//! something can observe non-durability. [`FaultBacking`] is exactly that
//! observer: it journals every un-barriered write and folds the journal into
//! the durable image only on a real barrier. So the milestone reduces to two
//! checkable facts about a `NO_SYNC` env, which commits without any barrier:
//!
//!   1. **Before** `force_sync`, the journal is non-empty and the crash-floor
//!      image (durable writes only) does *not* contain the data.
//!   2. **After** `force_sync`, the journal is empty and the crash-floor image
//!      opens and reads back every committed key — i.e. a power cut at that
//!      instant loses nothing.
//!
//! The `force` parameter's semantics come from the fork's `mdb_env_sync0`,
//! read at `lmdb-master-sys-0.2.6/lmdb/libraries/liblmdb/mdb.c`:
//!
//! ```text
//! if (env->me_flags & MDB_RDONLY) return EACCES;
//! if (force || !(env->me_flags & MDB_NOSYNC)) { ...flush... }
//! ```
//!
//! so `sync(false)` on a `NO_SYNC` env is a **no-op returning success** — the
//! one behavioral difference between the two `force` values, asserted below.
//! heed exposes only `force_sync()` (= `force = true`), so there is no heed
//! API to run a cross-engine differential against for `force = false`; the
//! LMDB side is pinned by reading the fork and by the `EACCES` differential,
//! which heed *does* reach.

use std::collections::BTreeMap;
use std::path::Path;

use heed::EnvOpenOptions as LmdbOpts;
use zerodb_core::env::{open_with_backing, testutil::VecBacking, DurabilityFlags, Env};
use zerodb_io::fault::{CapturedDisk, FaultBacking, FaultHandle};
use zerodb_oracle::tempdir::TempDir;

/// `EACCES`. Spelled out rather than pulled from `libc` so this test crate
/// needs no new dependency; the value is 13 on every Unix ZeroDB targets.
const EACCES: i32 = 13;

const PS: u32 = 4096;
const MAP: u64 = 16 << 20;

/// Build a real-file env wrapped in a [`FaultBacking`], under `durability`.
fn open_faulted(dir: &Path, durability: DurabilityFlags) -> (Env, FaultHandle) {
    let data_path = dir.join("zerodb.dat");
    let opened =
        zerodb_io::open_or_create(&data_path, PS, Some(MAP), MAP, false, durability.write_map)
            .expect("open_or_create");
    let (fault, handle) = FaultBacking::wrap(opened.backing, PS).expect("fault wrap");
    let env = open_with_backing(
        dir.to_path_buf(),
        Box::new(fault),
        PS,
        MAP,
        false,
        16,
        126,
        durability,
    )
    .expect("env open");
    (env, handle)
}

fn nosync() -> DurabilityFlags {
    DurabilityFlags {
        no_sync: true,
        ..DurabilityFlags::default()
    }
}

/// Write `keys` into a named DB and commit.
fn commit_keys(env: &Env, keys: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"d")).unwrap();
    for (k, v) in keys {
        db.put(&mut w, k, v).unwrap();
    }
    w.commit().unwrap();
}

/// Open the crash-floor image (durable bytes only) and assert it contains
/// exactly `keys` at `txnid`.
fn assert_floor_has(cut: &CapturedDisk, verify_path: &Path, keys: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut img = cut.floor_image();
    // Pad to a whole page so the backing can be mapped.
    let rem = img.len() % PS as usize;
    if rem != 0 {
        img.resize(img.len() + (PS as usize - rem), 0);
    }
    let env = open_with_backing(
        verify_path.to_path_buf(),
        Box::new(VecBacking(img)),
        PS,
        MAP,
        false,
        16,
        126,
        DurabilityFlags::default(),
    )
    .expect("crash-floor image must open after force_sync");
    let r = env.read_txn().unwrap();
    let db = env
        .open_database(&r, Some(b"d"))
        .expect("open_database on the recovered image")
        .expect("the named DB must be durable after force_sync");
    assert_eq!(
        db.len(&r).unwrap(),
        keys.len() as u64,
        "recovered entry count after force_sync"
    );
    for (k, v) in keys {
        assert_eq!(
            db.get(&r, k).unwrap(),
            Some(v.as_slice()),
            "key {k:?} missing from the crash-floor image after force_sync"
        );
    }
}

fn sample_keys(n: usize, tag: u8) -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..n)
        .map(|i| {
            (
                format!("key{i:05}").into_bytes(),
                vec![tag; 32 + (i % 97) * 7],
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The core milestone claim
// ---------------------------------------------------------------------------

#[test]
fn force_sync_on_a_nosync_env_empties_the_journal_and_makes_data_durable() {
    let dir = TempDir::new().unwrap();
    let vdir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), nosync());

    let keys = sample_keys(300, b'A');
    commit_keys(&env, &keys);

    // (1) Under NO_SYNC the commit issued no barrier: writes are still pending
    // and the crash floor is the pre-commit state.
    let before = handle.capture();
    assert!(
        !before.pending.is_empty(),
        "precondition: NO_SYNC must leave the commit un-barriered \
         (nothing pending means this test proves nothing)"
    );
    assert_eq!(
        handle.stats().barriers,
        0,
        "NO_SYNC commit must not have issued a barrier"
    );

    // (2) force_sync — the whole point of the milestone.
    env.force_sync().expect("force_sync on a NO_SYNC env");

    let after = handle.capture();
    assert!(
        after.pending.is_empty(),
        "force_sync must fold every pending write into the durable image; \
         {} still pending",
        after.pending.len()
    );
    assert_eq!(
        handle.stats().barriers,
        1,
        "force_sync must issue exactly one barrier"
    );

    // (3) A power cut now loses nothing: the floor image *is* the full state.
    assert_floor_has(&after, vdir.path(), &keys);
}

#[test]
fn force_sync_makes_multiple_accumulated_nosync_commits_durable_at_once() {
    // NO_SYNC lets pending writes span several commits. One force_sync must
    // cover all of them, not just the last.
    let dir = TempDir::new().unwrap();
    let vdir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), nosync());

    let mut all = BTreeMap::new();
    for round in 0..5u8 {
        let batch = sample_keys(60, b'A' + round);
        let batch: BTreeMap<Vec<u8>, Vec<u8>> = batch
            .into_iter()
            .map(|(k, v)| ([format!("r{round}-").into_bytes(), k].concat(), v))
            .collect();
        commit_keys(&env, &batch);
        all.extend(batch);
    }
    assert_eq!(handle.stats().barriers, 0, "no barrier across 5 commits");
    assert!(!handle.capture().pending.is_empty());

    env.force_sync().unwrap();

    let cut = handle.capture();
    assert!(cut.pending.is_empty(), "journal drained");
    assert_floor_has(&cut, vdir.path(), &all);
}

#[test]
fn force_sync_is_idempotent_and_always_issues_a_barrier() {
    // `mdb_env_sync(force=1)` calls through unconditionally, even with nothing
    // outstanding. Reproduced: three calls, three barriers, no error.
    let dir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), nosync());
    commit_keys(&env, &sample_keys(10, b'A'));
    env.force_sync().unwrap();
    let after_first = handle.stats().barriers;
    assert_eq!(after_first, 1);
    env.force_sync().unwrap();
    env.force_sync().unwrap();
    assert_eq!(
        handle.stats().barriers,
        3,
        "each force_sync issues a barrier even when nothing is pending"
    );
    assert!(handle.capture().pending.is_empty());
}

// ---------------------------------------------------------------------------
// The `force` parameter (the 2.5 API addition)
// ---------------------------------------------------------------------------

#[test]
fn sync_false_is_a_noop_on_a_nosync_env() {
    // mdb_env_sync0: `if (force || !(flags & MDB_NOSYNC))`. With NO_SYNC set
    // and force=0 the flush is skipped entirely — success, but nothing durable.
    let dir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), nosync());
    commit_keys(&env, &sample_keys(50, b'A'));

    let pending_before = handle.capture().pending.len();
    assert!(pending_before > 0, "precondition");

    env.sync(false)
        .expect("sync(false) must succeed, not error");

    assert_eq!(
        handle.stats().barriers,
        0,
        "sync(false) on a NO_SYNC env must issue NO barrier (mdb_env_sync0 gate)"
    );
    assert_eq!(
        handle.capture().pending.len(),
        pending_before,
        "sync(false) must leave the journal untouched on a NO_SYNC env"
    );

    // ...and force=true then does the job, proving the difference is the flag.
    env.sync(true).unwrap();
    assert_eq!(handle.stats().barriers, 1);
    assert!(handle.capture().pending.is_empty());
}

#[test]
fn sync_false_does_flush_on_a_default_env() {
    // Same gate, other branch: without NO_SYNC, `!(flags & MDB_NOSYNC)` is
    // true, so force=0 still flushes.
    let dir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), DurabilityFlags::default());
    commit_keys(&env, &sample_keys(20, b'A'));
    let before = handle.stats().barriers;
    env.sync(false).unwrap();
    assert_eq!(
        handle.stats().barriers,
        before + 1,
        "sync(false) on a default env must flush"
    );
}

#[test]
fn force_sync_equals_sync_true() {
    let dir = TempDir::new().unwrap();
    let (env, handle) = open_faulted(dir.path(), nosync());
    commit_keys(&env, &sample_keys(10, b'A'));
    env.force_sync().unwrap();
    let a = handle.stats().barriers;
    commit_keys(&env, &sample_keys(10, b'B'));
    env.sync(true).unwrap();
    let b = handle.stats().barriers;
    assert_eq!(b - a, 1, "sync(true) behaves exactly as force_sync");
}

#[test]
fn map_async_is_downgraded_to_a_real_barrier_by_force() {
    // mdb_env_sync0: `flags = ((MAPASYNC) && !force) ? MS_ASYNC : MS_SYNC`.
    // The fault backing models MS_ASYNC as explicitly *not* a barrier (REC-9),
    // so this is directly observable.
    let dir = TempDir::new().unwrap();
    let durability = DurabilityFlags {
        no_sync: true,
        map_async: true,
        write_map: true,
        ..DurabilityFlags::default()
    };
    let (env, handle) = open_faulted(dir.path(), durability);
    commit_keys(&env, &sample_keys(40, b'A'));
    assert!(!handle.capture().pending.is_empty(), "precondition");

    env.force_sync().unwrap();
    assert_eq!(
        handle.stats().barriers,
        1,
        "force must downgrade MAP_ASYNC to a synchronous flush"
    );
    assert!(
        handle.capture().pending.is_empty(),
        "a forced sync under MAP_ASYNC must still drain the journal"
    );
}

// ---------------------------------------------------------------------------
// Differential: the one mdb_env_sync behavior heed reaches
// ---------------------------------------------------------------------------

#[test]
fn force_sync_on_a_readonly_env_is_eacces_on_both_engines() {
    // `mdb_env_sync0` rejects MDB_RDONLY with EACCES before anything else.
    // heed exposes `force_sync()`, so this half *is* differentiable.
    let ldir = TempDir::new().unwrap();
    {
        // Create the store first — RDONLY cannot create.
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP as usize);
        o.max_dbs(4);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { o.open(ldir.path()) }.unwrap();
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut w, Some("d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    let lerr = {
        let mut o = LmdbOpts::new().read_txn_without_tls();
        o.map_size(MAP as usize);
        o.max_dbs(4);
        // SAFETY: read-only open of a store this test just created.
        unsafe { o.flags(heed::EnvFlags::READ_ONLY) };
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { o.open(ldir.path()) }.unwrap();
        env.force_sync().unwrap_err()
    };

    let zdir = TempDir::new().unwrap();
    {
        let mut o = zerodb::EnvOpenOptions::new();
        o.map_size(MAP as usize);
        o.max_dbs(4);
        o.page_size(PS);
        let env = o.open(zdir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    let zerr = {
        let mut o = zerodb::EnvOpenOptions::new();
        o.map_size(MAP as usize);
        o.max_dbs(4);
        o.page_size(PS);
        o.flags(zerodb::EnvFlags::READ_ONLY);
        let env = o.open(zdir.path()).unwrap();
        env.force_sync().unwrap_err()
    };

    let lcode = match &lerr {
        heed::Error::Io(e) => e.raw_os_error(),
        other => panic!("lmdb force_sync on RDONLY gave a non-Io error: {other:?}"),
    };
    let zcode = match &zerr {
        zerodb::Error::Io(e) => e.raw_os_error(),
        other => panic!("zerodb force_sync on RDONLY gave a non-Io error: {other:?}"),
    };
    assert_eq!(
        lcode,
        Some(EACCES),
        "lmdb should report EACCES, got {lerr:?}"
    );
    assert_eq!(
        lcode, zcode,
        "RDONLY force_sync errno diverges: lmdb={lerr:?} zerodb={zerr:?}"
    );

    // `sync(false)` takes the same early return — RDONLY is checked before the
    // NO_SYNC gate, so it is EACCES for either value of `force`.
    let zdir2 = TempDir::new().unwrap();
    {
        let mut o = zerodb::EnvOpenOptions::new();
        o.map_size(MAP as usize);
        o.max_dbs(4);
        o.page_size(PS);
        o.open(zdir2.path()).unwrap();
    }
    let mut o = zerodb::EnvOpenOptions::new();
    o.map_size(MAP as usize);
    o.max_dbs(4);
    o.page_size(PS);
    o.flags(zerodb::EnvFlags::READ_ONLY);
    let env = o.open(zdir2.path()).unwrap();
    match env.sync(false).unwrap_err() {
        zerodb::Error::Io(e) => assert_eq!(e.raw_os_error(), Some(EACCES)),
        other => panic!("sync(false) on RDONLY: {other:?}"),
    }
}
