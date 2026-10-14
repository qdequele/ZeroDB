//! An open with a cross-process region (ADR-0023) that fails after building
//! its environment returns the error and releases the region with it.
//!
//! `EnvInner::drop` checks, in every build but zerodb-core's own unit tests,
//! that it holds the last reference to the region, so the lock file closes
//! before the close event fires. These tests run against the non-test build
//! of the crate, so a debug build exercises that check on the two error
//! returns that follow the environment's construction: a live handle for the
//! same path (`EnvAlreadyOpened`, SPEC 04 TXN-51) and a failed
//! `SharedRegion::finish_open`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use zerodb_core::env::{open_with_backing_shared, DurabilityFlags, Env};
use zerodb_core::error::Error;
use zerodb_core::page::FileTrust;
use zerodb_core::shared::{ReaderSlot, SharedRegion};
use zerodb_io::testmap::TestWriteMap;

const PS: u32 = 4096;
const MAP: usize = 1 << 20;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A single-process stand-in for the lock-file region: heap atomics, the
/// writer lock left to the in-process lock, and a `finish_open` that can be
/// made to fail.
struct TestRegion {
    t: AtomicU64,
    slots: Box<[ReaderSlot]>,
    fail_finish_open: AtomicBool,
}

impl TestRegion {
    fn new(fail_finish_open: bool) -> Arc<TestRegion> {
        Arc::new(TestRegion {
            t: AtomicU64::new(0),
            slots: (0..8).map(|_| ReaderSlot::new_free()).collect(),
            fail_finish_open: AtomicBool::new(fail_finish_open),
        })
    }
}

impl SharedRegion for TestRegion {
    fn published_txnid(&self) -> &AtomicU64 {
        &self.t
    }
    fn slots(&self) -> &[ReaderSlot] {
        &self.slots
    }
    fn pid(&self) -> u32 {
        std::process::id()
    }
    fn is_initializer(&self) -> bool {
        true
    }
    fn is_alive(&self, _pid: u32) -> std::io::Result<bool> {
        Ok(true)
    }
    fn lock_writer(&self) -> std::io::Result<bool> {
        // The tests run one environment per region in one thread; the
        // in-process writer lock already serializes its writers.
        Ok(false)
    }
    fn unlock_writer(&self) {}
    fn finish_open(&self) -> std::io::Result<()> {
        // Relaxed: a test knob set before the region is shared.
        if self.fail_finish_open.load(Ordering::Relaxed) {
            Err(std::io::Error::other("test finish_open failure"))
        } else {
            Ok(())
        }
    }
}

fn virtual_path() -> PathBuf {
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!(
        "/virtual/shared-open-errors-{}-{seq}",
        std::process::id()
    ))
}

/// Open `path` over a fresh in-memory data file and `region`, which is moved
/// in: the caller keeps only a `Weak`, as the engine's own caller does.
fn open(path: PathBuf, region: Arc<TestRegion>) -> Result<Env, Error> {
    open_with_backing_shared(
        path,
        Box::new(TestWriteMap::fresh_env(PS, MAP)),
        PS,
        false,
        8,
        8,
        DurabilityFlags {
            write_map: true,
            ..DurabilityFlags::default()
        },
        FileTrust::VALIDATE,
        false,
        None,
        Some(region as Arc<dyn SharedRegion>),
    )
}

#[test]
fn already_opened_path_returns_the_error_and_releases_the_region() {
    let path = virtual_path();
    let first = open(path.clone(), TestRegion::new(false)).expect("first open");
    let region = TestRegion::new(false);
    let weak: Weak<TestRegion> = Arc::downgrade(&region);
    let e = open(path, region).unwrap_err();
    assert!(
        matches!(e, Error::EnvAlreadyOpened),
        "expected EnvAlreadyOpened, got {e:?}"
    );
    assert!(
        weak.upgrade().is_none(),
        "the refused open kept its region alive"
    );
    drop(first);
}

#[test]
fn failed_finish_open_returns_the_error_and_releases_the_region() {
    let path = virtual_path();
    let region = TestRegion::new(true);
    let weak: Weak<TestRegion> = Arc::downgrade(&region);
    let e = open(path.clone(), region).unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.to_string(), "test finish_open failure"),
        other => panic!("expected Io, got {other:?}"),
    }
    assert!(
        weak.upgrade().is_none(),
        "the failed open kept its region alive"
    );
    // The failed open left no live handle behind: the path opens again.
    drop(open(path, TestRegion::new(false)).expect("reopen after the failure"));
}
