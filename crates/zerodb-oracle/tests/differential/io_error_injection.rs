//! Failing system calls in the commit pipeline, in dirty-page spilling, in
//! the `WRITE_MAP` ranged `msync` and in file growth (SPEC 06 REC-13, REC-23).
//!
//! Each case runs a real-file environment under the fault-injection backing
//! ([`FaultBacking`], ADR-0008), arms one [`IoFault`] on the `n`th write, sync
//! or growth call of one operation, and checks four things:
//!
//! 1. the operation returns a typed error carrying the injected `errno`
//!    (`EIO` or `ENOSPC`; a short write reports `ENOSPC`);
//! 2. the previous commit is still what every read transaction sees;
//! 3. the environment then behaves as SPEC 06 REC-23 states: after a failed
//!    write it keeps working and the same transaction can be redone; after a
//!    failed sync it refuses every write transaction (REC-13);
//! 4. the file as a reopen would see it right after the failure, and every
//!    crash image the journal allows at that moment, pass the
//!    `zerodb-tools check` walk ([`check_image`]) and open at either the
//!    previous commit or, where REC-23 says so, the failed one.
//!
//! Every step of each operation is covered: the number of write and sync
//! calls is measured on an identical run without a fault first, then each
//! call is failed in turn. The tests use the native engine API; the heed
//! adapter maps the same `Error::Io` through heed's taxonomy unchanged.
//!
//! LMDB's side of the same events is observed in
//! `io_error_lmdb_differential.rs` where a real failure can be produced on
//! both engines.

use std::collections::BTreeMap;
use std::path::PathBuf;

use zerodb::check::check_image;
use zerodb_core::env::{
    open_with_backing, open_with_backing_policy, testutil::VecBacking, DurabilityFlags, Env,
};
use zerodb_core::{Error, MdbError};
use zerodb_io::fault::{FaultBacking, FaultHandle, IoFailure, IoFault, IoOp};
use zerodb_oracle::tempdir::TempDir;

const PS: u32 = 4096;
const MAP: u64 = 8 << 20;
const EIO: i32 = 5;
const ENOSPC: i32 = 28;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

/// A real-file env under a fault-injecting backing.
struct Rig {
    _dir: TempDir,
    env_path: PathBuf,
    data_path: PathBuf,
    env: Option<Env>,
    handle: FaultHandle,
}

impl Rig {
    fn open(durability: DurabilityFlags, dirty_limit: Option<u64>) -> Rig {
        let dir = TempDir::new().unwrap();
        let env_path = dir.path().to_path_buf();
        let data_path = dir.path().join("zerodb.dat");
        let opened =
            zerodb_io::open_or_create(&data_path, PS, Some(MAP), MAP, false, durability.write_map)
                .expect("open_or_create");
        let (fault, handle) = FaultBacking::wrap(opened.backing, PS).expect("fault wrap");
        let env = open_with_backing_policy(
            env_path.clone(),
            Box::new(fault),
            PS,
            false,
            16,
            126,
            durability,
            zerodb::FileTrust::VALIDATE,
            false,
            dirty_limit,
        )
        .expect("env open");
        Rig {
            _dir: dir,
            env_path,
            data_path,
            env: Some(env),
            handle,
        }
    }

    fn env(&self) -> &Env {
        self.env.as_ref().expect("env is open")
    }

    /// Close the env and reopen its file under a new fault-injecting
    /// backing, as a new process would.
    fn reopen_faulty(&mut self, durability: DurabilityFlags) {
        drop(self.env.take());
        let opened = zerodb_io::open_or_create(
            &self.data_path,
            PS,
            Some(MAP),
            MAP,
            false,
            durability.write_map,
        )
        .expect("reopen: open_or_create");
        let (fault, handle) = FaultBacking::wrap(opened.backing, PS).expect("fault wrap");
        let env = open_with_backing_policy(
            self.env_path.clone(),
            Box::new(fault),
            PS,
            false,
            16,
            126,
            durability,
            zerodb::FileTrust::VALIDATE,
            false,
            None,
        )
        .expect("reopen: env open");
        self.env = Some(env);
        self.handle = handle;
    }

    /// The data file as a reopen right now would read it: the page cache,
    /// failed and unsynced writes included.
    fn file_bytes(&self) -> Vec<u8> {
        std::fs::read(&self.data_path).unwrap()
    }

    /// Close the env and reopen its file without fault injection, as a
    /// process restart would (REC-1/REC-2 select the meta again).
    fn reopen_clean(&mut self, durability: DurabilityFlags) -> Env {
        drop(self.env.take());
        let opened = zerodb_io::open_or_create(
            &self.data_path,
            PS,
            Some(MAP),
            MAP,
            false,
            durability.write_map,
        )
        .expect("reopen: open_or_create");
        open_with_backing(
            self.env_path.clone(),
            opened.backing,
            PS,
            false,
            16,
            126,
            durability,
        )
        .expect("reopen: env open")
    }
}

fn default_mode() -> DurabilityFlags {
    DurabilityFlags::default()
}

fn write_map_mode() -> DurabilityFlags {
    DurabilityFlags {
        write_map: true,
        ..DurabilityFlags::default()
    }
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

fn key(i: u32) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn value(i: u32, round: u8) -> Vec<u8> {
    // Every 37th value is an overflow run (more than one page), so commits
    // and spills write multi-page frames too.
    let len = if i.is_multiple_of(37) {
        2 * PS as usize + 100
    } else {
        40 + (i as usize % 61)
    };
    vec![round.wrapping_add(i as u8); len]
}

/// Three seeding commits with inserts, updates and deletes, so the target
/// transaction reuses freed pages as well as extending the file.
fn seed(env: &Env) -> Model {
    let mut model = Model::new();
    for round in 0..3u8 {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        for i in 0..300u32 {
            if (i + u32::from(round)) % 5 == 0 && round > 0 {
                db.delete(&mut w, &key(i)).unwrap();
                model.remove(&key(i));
            } else {
                db.put(&mut w, &key(i), &value(i, round)).unwrap();
                model.insert(key(i), value(i, round));
            }
        }
        w.commit().unwrap();
    }
    model
}

/// The target transaction's changes, applied to `w` (or to the model).
/// Returns the first error a call reported.
fn target_ops(env: &Env, w: &mut zerodb_core::RwTxn<'_>, n: u32) -> Result<(), Error> {
    let db = env.create_database(w, Some(b"d"))?;
    for i in 0..n {
        if i % 7 == 3 {
            db.delete(w, &key(i))?;
        } else {
            db.put(w, &key(i), &value(i, 9))?;
        }
    }
    Ok(())
}

fn target_model(before: &Model, n: u32) -> Model {
    let mut m = before.clone();
    for i in 0..n {
        if i % 7 == 3 {
            m.remove(&key(i));
        } else {
            m.insert(key(i), value(i, 9));
        }
    }
    m
}

fn contents(env: &Env) -> Model {
    let r = env.read_txn().unwrap();
    let Some(db) = env.open_database(&r, Some(b"d")).unwrap() else {
        return Model::new();
    };
    db.iter(&r)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

fn assert_errno(e: &Error, want: i32, what: &str) {
    match e {
        Error::Io(io) => assert_eq!(io.raw_os_error(), Some(want), "{what}: {e}"),
        other => panic!("{what}: expected Error::Io(errno {want}), got {other:?}"),
    }
}

fn want_errno(f: IoFailure) -> i32 {
    match f {
        IoFailure::Eio => EIO,
        IoFailure::Enospc | IoFailure::ShortWrite { .. } => ENOSPC,
    }
}

fn assert_poisoned(env: &Env, what: &str) {
    match env.write_txn() {
        Err(Error::Io(e)) => assert!(
            e.to_string().contains("poisoned"),
            "{what}: unexpected write_txn error {e}"
        ),
        Err(e) => panic!("{what}: expected the poisoned-env error, got {e:?}"),
        Ok(_) => panic!("{what}: a write txn began on a poisoned env (REC-13)"),
    }
    assert!(env.force_sync().is_err(), "{what}: sync on a poisoned env");
}

/// Open `img` (a file image) through the engine's meta selection and return
/// its contents. The image is first walked by the `zerodb-tools check` code,
/// which must find nothing.
fn image_contents(img: &[u8], what: &str) -> Model {
    let report = check_image(img, PS);
    assert!(report.is_empty(), "{what}: check reports {report:#?}");
    let mut img = img.to_vec();
    let rem = img.len() % PS as usize;
    if rem != 0 {
        img.resize(img.len() + PS as usize - rem, 0);
    }
    let dir = TempDir::new().unwrap();
    let env = open_with_backing(
        dir.path().to_path_buf(),
        Box::new(VecBacking::new(img, MAP)),
        PS,
        false,
        16,
        126,
        default_mode(),
    )
    .unwrap_or_else(|e| panic!("{what}: image does not open: {e:?}"));
    contents(&env)
}

/// The crash images the journal allows right now — the all-lost and
/// all-kept images and a seeded adversarial set (torn, reordered,
/// lost-extension) — each checked clean and recovered to one of `allowed`.
fn assert_crash_images(handle: &FaultHandle, allowed: &[&Model], what: &str) {
    let cap = handle.capture();
    let mut plans = cap.plans_adversarial(0x176, 12);
    plans.extend(cap.plans_ordered(0x176, 4));
    for plan in plans {
        let img = cap.materialize(&plan);
        let got = image_contents(&img, &format!("{what}, crash image `{}`", plan.label));
        assert!(
            allowed.iter().any(|m| **m == got),
            "{what}: crash image `{}` recovered {} entries, none of the allowed states",
            plan.label,
            got.len()
        );
    }
}

/// Calls of each class one closure makes on `rig`'s backing.
fn count_calls(rig: &Rig, f: impl FnOnce()) -> (u64, u64, u64) {
    let before = rig.handle.stats();
    f();
    let after = rig.handle.stats();
    (
        after.writes - before.writes,
        after.syncs - before.syncs,
        after.grows - before.grows,
    )
}

const WRITE_FAILURES: [IoFailure; 4] = [
    IoFailure::Eio,
    IoFailure::Enospc,
    // A prefix inside the first sector: a meta cut this short fails its CRC.
    IoFailure::ShortWrite { keep: 16 },
    // Half a page: a meta cut here keeps its whole encoded prefix.
    IoFailure::ShortWrite {
        keep: PS as usize / 2,
    },
];

const SYNC_FAILURES: [IoFailure; 2] = [IoFailure::Eio, IoFailure::Enospc];

// ---------------------------------------------------------------------------
// The commit pipeline (SPEC 04 §9): C2 data writes, C3 data sync, C4 meta
// write, C5 meta sync
// ---------------------------------------------------------------------------

const TARGET_N: u32 = 260;

/// The write and sync calls of the target commit, from a run without faults.
fn commit_calls(durability: DurabilityFlags) -> (u64, u64) {
    let rig = Rig::open(durability, None);
    seed(rig.env());
    let mut w = rig.env().write_txn().unwrap();
    target_ops(rig.env(), &mut w, TARGET_N).unwrap();
    let (writes, syncs, _) = count_calls(&rig, || w.commit().unwrap());
    (writes, syncs)
}

/// Where the injected call falls in the commit pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// C2: a data page or overflow run.
    DataWrite,
    /// C3.
    DataSync,
    /// C4.
    MetaWrite,
    /// C5.
    MetaSync,
}

fn run_commit_case(durability: DurabilityFlags, fault: IoFault, step: Step) {
    let what = format!("{step:?} {fault:?}");
    let mut rig = Rig::open(durability, None);
    let before = seed(rig.env());
    let after = target_model(&before, TARGET_N);
    let pinned = rig.env().read_txn().unwrap();

    let mut w = rig.env().write_txn().unwrap();
    target_ops(rig.env(), &mut w, TARGET_N).unwrap();
    rig.handle.inject(fault);
    let err = w
        .commit()
        .expect_err("the injected failure must fail the commit");
    rig.handle.clear_injection();
    assert_errno(&err, want_errno(fault.failure), &what);
    assert_eq!(rig.handle.stats().injected, 1, "{what}: fault fired once");

    // (2) The previous commit is what every reader sees.
    drop(pinned);
    assert_eq!(contents(rig.env()), before, "{what}: reads after failure");

    // (4) The file as a reopen sees it now, and every crash image.
    let on_reopen = image_contents(&rig.file_bytes(), &format!("{what}, file"));
    // REC-13: a failed meta write or meta barrier puts the slot's previous
    // bytes back, so a reopen without a power loss reads the previous
    // commit, even when a short write let the whole encoded meta through
    // (the encoded meta is well under half a page here) or C5 failed after
    // the meta reached the page cache.
    assert_eq!(
        on_reopen, before,
        "{what}: what a reopen reads right after the failure"
    );
    let allowed: Vec<&Model> = match step {
        Step::DataWrite | Step::DataSync => vec![&before],
        Step::MetaWrite | Step::MetaSync => vec![&before, &after],
    };
    assert_crash_images(&rig.handle, &allowed, &what);
    if step == Step::DataSync || step == Step::DataWrite {
        // The failed commit's data never reached a durable meta.
        let floor = image_contents(&rig.handle.capture().floor_image(), &what);
        assert_eq!(floor, before, "{what}: crash floor");
    }

    // (3) What the environment does next.
    match step {
        Step::DataWrite => {
            // Not poisoned: the same transaction can be redone and commits.
            let mut w = rig
                .env()
                .write_txn()
                .expect("write txn after a failed write");
            target_ops(rig.env(), &mut w, TARGET_N).unwrap();
            w.commit().expect("the redone commit");
            assert_eq!(contents(rig.env()), after, "{what}: redone commit");
            assert_crash_images(&rig.handle, &[&after], &format!("{what}, redone"));
        }
        Step::DataSync | Step::MetaWrite | Step::MetaSync => {
            assert_poisoned(rig.env(), &what);
            assert_eq!(contents(rig.env()), before, "{what}: poisoned env reads");
        }
    }

    // A reopen without faults reads a clean file and accepts writes again.
    let expect_after_reopen = match step {
        Step::DataWrite => after.clone(),
        Step::DataSync | Step::MetaWrite | Step::MetaSync => before.clone(),
    };
    let env = rig.reopen_clean(durability);
    assert_eq!(contents(&env), expect_after_reopen, "{what}: after reopen");
    let mut w = env.write_txn().expect("write txn after reopen");
    target_ops(&env, &mut w, 20).unwrap();
    w.commit().expect("commit after reopen");
    drop(env);
    let report = check_image(&std::fs::read(&rig.data_path).unwrap(), PS);
    assert!(report.is_empty(), "{what}: check after reopen {report:#?}");
}

fn commit_pipeline_cases(durability: DurabilityFlags) {
    let (writes, syncs) = commit_calls(durability);
    assert_eq!(syncs, 2, "C3 and C5");
    assert!(writes >= 1, "at least the meta write");
    for n in 1..=writes {
        // The meta is the commit's last write (C4 follows C2 and C3).
        let step = if n == writes {
            Step::MetaWrite
        } else {
            Step::DataWrite
        };
        for failure in WRITE_FAILURES {
            run_commit_case(durability, IoFault::once(IoOp::Write, n, failure), step);
        }
    }
    for (n, step) in [(1, Step::DataSync), (2, Step::MetaSync)] {
        for failure in SYNC_FAILURES {
            run_commit_case(durability, IoFault::once(IoOp::Sync, n, failure), step);
        }
    }
}

#[test]
fn commit_pipeline_failures_default_mode() {
    let (writes, _) = commit_calls(default_mode());
    assert!(writes >= 8, "C2 writes several frames before the meta");
    commit_pipeline_cases(default_mode());
}

/// `WRITE_MAP`: the data pages are stored in the map in place (ADR-0021), so
/// the commit's only write is the meta (C4), and its barriers are the ranged
/// `msync`s of SPEC 06 REC-12.
#[test]
fn commit_pipeline_failures_write_map_ranged_msync() {
    let (writes, syncs) = commit_calls(write_map_mode());
    assert_eq!(writes, 1, "in-place WRITE_MAP writes only the meta");
    assert_eq!(syncs, 2, "two ranged msyncs");
    commit_pipeline_cases(write_map_mode());
}

/// A short meta write that lets the whole encoded meta through, then, in a
/// new process, a different transaction that stops after its page writes
/// (its C3 fails). The restore of SPEC 06 REC-13 keeps the failed commit's
/// meta out of the slot, so the second writer, which reuses its txnid on the
/// previous snapshot, cannot leave the file pointing at pages it overwrote.
/// The writing process itself is poisoned and takes no second writer.
#[test]
fn failed_meta_write_then_interrupted_writer_keeps_the_file_consistent() {
    for mode in [default_mode(), write_map_mode()] {
        let what = format!("{mode:?}");
        let mut rig = Rig::open(mode, None);
        let before = seed(rig.env());
        let (meta_write, _) = commit_calls(mode);
        let mut w = rig.env().write_txn().unwrap();
        target_ops(rig.env(), &mut w, TARGET_N).unwrap();
        rig.handle.inject(IoFault::once(
            IoOp::Write,
            meta_write,
            IoFailure::ShortWrite {
                keep: PS as usize / 2,
            },
        ));
        assert_errno(&w.commit().unwrap_err(), ENOSPC, &what);
        rig.handle.clear_injection();
        assert_poisoned(rig.env(), &what);

        rig.reopen_faulty(mode);
        assert_eq!(contents(rig.env()), before, "{what}: reopen");
        let mut w = rig.env().write_txn().unwrap();
        let db = rig.env().create_database(&mut w, Some(b"d")).unwrap();
        for i in 0..2000u32 {
            db.put(&mut w, format!("other{i:06}").as_bytes(), &[0x5A; 120])
                .unwrap();
        }
        rig.handle
            .inject(IoFault::once(IoOp::Sync, 1, IoFailure::Eio));
        assert_errno(&w.commit().unwrap_err(), EIO, &what);
        rig.handle.clear_injection();

        let on_reopen = image_contents(&rig.file_bytes(), &format!("{what}, file"));
        assert_eq!(on_reopen, before, "{what}: the file after both failures");
        assert_crash_images(&rig.handle, &[&before], &what);
        let env = rig.reopen_clean(mode);
        assert_eq!(contents(&env), before, "{what}: clean reopen");
    }
}

/// A sticky failure (a device that stays full) fails every retry the same
/// way and never poisons the env by itself; once space returns, the redo
/// commits.
#[test]
fn full_device_fails_every_retry_until_space_returns() {
    let rig = Rig::open(default_mode(), None);
    let before = seed(rig.env());
    rig.handle.inject(IoFault {
        op: IoOp::Write,
        nth: 1,
        failure: IoFailure::Enospc,
        sticky: true,
    });
    for _ in 0..3 {
        let mut w = rig.env().write_txn().unwrap();
        target_ops(rig.env(), &mut w, TARGET_N).unwrap();
        assert_errno(&w.commit().unwrap_err(), ENOSPC, "full device");
        assert_eq!(contents(rig.env()), before);
    }
    rig.handle.clear_injection();
    let mut w = rig.env().write_txn().unwrap();
    target_ops(rig.env(), &mut w, TARGET_N).unwrap();
    w.commit().unwrap();
    assert_eq!(contents(rig.env()), target_model(&before, TARGET_N));
}

// ---------------------------------------------------------------------------
// Spilling dirty pages (SPEC 04 TXN-68..72)
// ---------------------------------------------------------------------------

/// A dirty limit small enough that the target transaction spills often.
const SPILL_LIMIT: Option<u64> = Some(140);
const SPILL_N: u32 = 3000;

#[test]
fn spill_write_failures() {
    // The writes the target transaction's spills make, from a run without
    // faults.
    let spill_writes = {
        let rig = Rig::open(default_mode(), SPILL_LIMIT);
        seed(rig.env());
        let mut w = rig.env().write_txn().unwrap();
        let (writes, _, _) = count_calls(&rig, || target_ops(rig.env(), &mut w, SPILL_N).unwrap());
        assert!(w.spill_count() >= 2, "the workload must spill");
        drop(w);
        writes
    };
    assert!(spill_writes > 0);
    // The first spill's writes one by one, then a spread over the rest.
    let mut ns: Vec<u64> = (1..=spill_writes.min(24)).collect();
    ns.extend((24..=spill_writes).step_by(53));
    ns.push(spill_writes);
    ns.dedup();
    for n in ns {
        for failure in WRITE_FAILURES {
            let fault = IoFault::once(IoOp::Write, n, failure);
            let what = format!("spill {fault:?}");
            let mut rig = Rig::open(default_mode(), SPILL_LIMIT);
            let before = seed(rig.env());
            let mut w = rig.env().write_txn().unwrap();
            rig.handle.inject(fault);
            let err = target_ops(rig.env(), &mut w, SPILL_N)
                .expect_err("the failed spill must fail the call that spilled");
            rig.handle.clear_injection();
            assert_errno(&err, want_errno(failure), &what);
            // TXN-69: the transaction is errored; only abort remains.
            assert!(
                matches!(w.commit(), Err(Error::Mdb(MdbError::BadTxn))),
                "{what}: commit of the errored txn"
            );
            assert_eq!(contents(rig.env()), before, "{what}: reads");
            assert_crash_images(&rig.handle, &[&before], &what);
            assert_eq!(
                image_contents(&rig.file_bytes(), &what),
                before,
                "{what}: file"
            );
            // The env keeps working: the same transaction commits.
            let mut w = rig.env().write_txn().unwrap();
            target_ops(rig.env(), &mut w, SPILL_N).unwrap();
            w.commit().unwrap();
            let after = target_model(&before, SPILL_N);
            assert_eq!(contents(rig.env()), after, "{what}: redone");
            let env = rig.reopen_clean(default_mode());
            assert_eq!(contents(&env), after, "{what}: after reopen");
        }
    }
}

// ---------------------------------------------------------------------------
// File growth (ADR-0023) and explicit syncs
// ---------------------------------------------------------------------------

/// Under `WRITE_MAP` a write transaction grows the file to the map size at
/// begin; a failed growth fails `write_txn`, releases the writer lock and
/// changes nothing.
#[test]
fn write_map_growth_failure_at_write_txn_begin() {
    for failure in SYNC_FAILURES {
        let rig = Rig::open(write_map_mode(), None);
        let before = seed(rig.env());
        rig.handle.inject(IoFault::once(IoOp::Grow, 1, failure));
        let err = rig.env().write_txn().map(|_| ()).unwrap_err();
        assert_errno(&err, want_errno(failure), "growth at write_txn");
        assert_eq!(contents(rig.env()), before);
        let mut w = rig.env().write_txn().expect("the writer lock was released");
        target_ops(rig.env(), &mut w, TARGET_N).unwrap();
        w.commit().unwrap();
        assert_eq!(contents(rig.env()), target_model(&before, TARGET_N));
    }
}

/// `Env::resize` grows the map (and, under `WRITE_MAP`, the file); a failed
/// growth leaves the old size in force and the env usable.
#[test]
fn resize_growth_failure() {
    for durability in [default_mode(), write_map_mode()] {
        for failure in SYNC_FAILURES {
            let mut rig = Rig::open(durability, None);
            let before = seed(rig.env());
            let size = rig.env().map_size();
            rig.handle.inject(IoFault::once(IoOp::Grow, 1, failure));
            let err = rig.env().resize(2 * MAP).unwrap_err();
            assert_errno(&err, want_errno(failure), "resize");
            assert_eq!(rig.env().map_size(), size, "the old size stays in force");
            let mut w = rig.env().write_txn().unwrap();
            target_ops(rig.env(), &mut w, TARGET_N).unwrap();
            w.commit().unwrap();
            let after = target_model(&before, TARGET_N);
            let env = rig.reopen_clean(durability);
            assert_eq!(contents(&env), after);
        }
    }
}

/// An explicit `force_sync` that fails poisons the env exactly as a failed
/// commit barrier does (REC-13), including under `NO_SYNC`, where it is the
/// only barrier the commits get.
#[test]
fn failed_explicit_sync_poisons() {
    let no_sync = DurabilityFlags {
        no_sync: true,
        ..DurabilityFlags::default()
    };
    for durability in [default_mode(), no_sync, write_map_mode()] {
        for failure in SYNC_FAILURES {
            let mut rig = Rig::open(durability, None);
            let before = seed(rig.env());
            rig.handle.inject(IoFault::once(IoOp::Sync, 1, failure));
            let err = rig.env().force_sync().unwrap_err();
            assert_errno(&err, want_errno(failure), "force_sync");
            assert_poisoned(rig.env(), "force_sync");
            assert_eq!(contents(rig.env()), before);
            if !durability.no_sync {
                // Under NO_SYNC nothing was ever durable: REC-11 promises no
                // crash state, so only the reopen below is checked.
                assert_crash_images(&rig.handle, &[&before], "force_sync");
            }
            let env = rig.reopen_clean(durability);
            assert_eq!(contents(&env), before);
        }
    }
}
