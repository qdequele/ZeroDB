//! M1.10 — durability-flag control-flow parity (SPEC 01 §S6, SPEC 06 REC-9),
//! amended by ADR-0019 (durable meta write through the meta-sync fd).
//!
//! The oracle differential tests confirm that a commit under each durability
//! flag produces identical *data*. This test confirms the other half: that the
//! commit pipeline runs exactly the right fsync/msync **barriers** and routes
//! the meta write through exactly the right **primitive** per flag — the
//! control flow the flags are *for* (their crash-window semantics are then
//! validated by the M1.11 harness). It drives a real commit through a counting
//! [`Backing`] and asserts the number and kind of `sync` calls and durable
//! meta writes:
//!
//! | Flags | C3 (data) | meta | `sync` calls | durable meta writes | async? |
//! |-------|-----------|------|--------------|---------------------|--------|
//! | default | yes | durable write (C4+C5 fused) | 1 | 1 | no |
//! | `WRITE_MAP` | yes | plain write + C5 msync | 2 | 0 | no |
//! | `NO_META_SYNC` | yes | plain write, no barrier | 1 | 0 | no |
//! | `NO_SYNC` | no | plain write, no barrier | 0 | 0 | — |
//! | `MAP_ASYNC`+`WRITE_MAP` | yes | plain write + C5 msync | 2 | 0 | yes |
//!
//! Also here: the ADR-0019 / REC-13 failed-durable-meta-write handling — the
//! slot's previous bytes are scrubbed back through the plain write path and
//! the env is poisoned.

use std::sync::{Arc, Mutex};

use zerodb_core::env::{open_with_backing, Backing, DurabilityFlags};
use zerodb_core::page::MetaPage;

const PS: u32 = 4096;
const MAP: u64 = 1 << 20;

/// A valid, fresh two-meta env image at txnid 0 (SPEC 02 §3.4).
fn fresh_image() -> Box<[u8]> {
    let ps = PS as usize;
    let mut buf = vec![0u8; MAP as usize];
    for slot in [0u64, 1] {
        let meta = MetaPage::create(slot, PS, MAP);
        let base = slot as usize * ps;
        meta.encode(&mut buf[base..base + ps]).expect("valid meta");
    }
    buf.into_boxed_slice()
}

/// What the pipeline asked the backing to do, in call order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    /// `sync(async_flush)`.
    Sync(bool),
    /// `write_page_durable(pgno)` — the ADR-0019 durable meta write.
    DurableWrite(u64),
    /// `write_at_page(pgno)` of a **meta** slot (pgno 0/1). Data-page writes
    /// are not recorded (their count is workload-dependent).
    MetaWrite(u64),
}

/// A [`Backing`] that accepts writes (no-op — the call sequence is all we
/// assert) and records barriers + meta-page writes into a shared `Arc` the
/// caller can read after the backing is moved into the env. `bytes()` returns
/// a fixed valid two-meta image so `open` and the in-txn reads succeed.
/// `fail_durable` makes the durable meta write fail (the REC-13 scrub path);
/// the scrubbed bytes are captured for comparison.
struct CountingBacking {
    data: Box<[u8]>,
    calls: Arc<Mutex<Vec<Call>>>,
    fail_durable: bool,
    /// Bytes of every meta-slot `write_at_page`, call-order aligned with the
    /// `MetaWrite` entries in `calls` (for the scrub-content assertion).
    meta_bytes: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Backing for CountingBacking {
    fn bytes(&self) -> &[u8] {
        &self.data
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        Ok(self.data.len() as u64)
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no fd",
        ))
    }
    fn write_at_page(&self, pgno: u64, _psize: u32, data: &[u8]) -> std::io::Result<()> {
        if pgno < 2 {
            self.calls.lock().unwrap().push(Call::MetaWrite(pgno));
            self.meta_bytes.lock().unwrap().push(data.to_vec());
        }
        Ok(())
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.calls.lock().unwrap().push(Call::Sync(false));
        Ok(())
    }
    fn sync(&self, async_flush: bool) -> std::io::Result<()> {
        self.calls.lock().unwrap().push(Call::Sync(async_flush));
        Ok(())
    }
    fn write_page_durable(&self, pgno: u64, _psize: u32, _data: &[u8]) -> std::io::Result<()> {
        self.calls.lock().unwrap().push(Call::DurableWrite(pgno));
        if self.fail_durable {
            return Err(std::io::Error::other("injected durable-write failure"));
        }
        Ok(())
    }
}

type Recorder = (Arc<Mutex<Vec<Call>>>, Arc<Mutex<Vec<Vec<u8>>>>);

fn env_with(
    durability: DurabilityFlags,
    fail_durable: bool,
    tag: &str,
) -> (zerodb_core::env::Env, Recorder) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let meta_bytes = Arc::new(Mutex::new(Vec::new()));
    let backing = CountingBacking {
        data: fresh_image(),
        calls: Arc::clone(&calls),
        fail_durable,
        meta_bytes: Arc::clone(&meta_bytes),
    };
    let path = std::path::PathBuf::from(format!(
        "/virtual/dur-{}-{tag}-{:?}",
        std::process::id(),
        durability
    ));
    let env = open_with_backing(path, Box::new(backing), PS, MAP, false, 8, 16, durability)
        .expect("open");
    (env, (calls, meta_bytes))
}

/// Drive one create-db + put + commit under `durability` and return the
/// recorded call sequence.
fn commit_and_record(durability: DurabilityFlags) -> Vec<Call> {
    let (env, (calls, _)) = env_with(durability, false, "ok");
    let mut w = env.write_txn().expect("write txn");
    let db = env.create_database(&mut w, None).expect("main db");
    db.put(&mut w, b"k", b"v").expect("put");
    w.commit().expect("commit");
    let out = calls.lock().unwrap().clone();
    out
}

#[test]
fn default_mode_one_barrier_plus_durable_meta_write() {
    // ADR-0019: C3 fdatasync, then ONE durable meta write (C4+C5 fused) to
    // slot `1 & 1 = 1` — no separate meta barrier, strictly in that order
    // (REC-7: the data barrier completes before the meta write begins).
    let calls = commit_and_record(DurabilityFlags::default());
    assert_eq!(
        calls,
        vec![Call::Sync(false), Call::DurableWrite(1)],
        "default: C3, then the fused durable meta write — nothing else"
    );
}

#[test]
fn write_map_keeps_explicit_meta_barrier() {
    // SPEC 06 REC-12 untouched by ADR-0019: a WRITE_MAP env has no meta-sync
    // fd; C4 is a plain (map) write and C5 an explicit msync.
    let d = DurabilityFlags {
        write_map: true,
        ..Default::default()
    };
    let calls = commit_and_record(d);
    assert_eq!(
        calls,
        vec![Call::Sync(false), Call::MetaWrite(1), Call::Sync(false)],
        "WRITE_MAP: C3 msync, plain C4, C5 msync"
    );
}

#[test]
fn no_meta_sync_skips_meta_barrier() {
    let d = DurabilityFlags {
        no_meta_sync: true,
        ..Default::default()
    };
    let calls = commit_and_record(d);
    assert_eq!(
        calls,
        vec![Call::Sync(false), Call::MetaWrite(1)],
        "NO_META_SYNC: C3 only; the meta goes through the plain fd, unsynced"
    );
}

#[test]
fn no_sync_skips_both_barriers() {
    let d = DurabilityFlags {
        no_sync: true,
        ..Default::default()
    };
    let calls = commit_and_record(d);
    assert_eq!(
        calls,
        vec![Call::MetaWrite(1)],
        "NO_SYNC: no barrier at all; plain meta write only"
    );
}

#[test]
fn no_sync_dominates_no_meta_sync() {
    // NO_SYNC subsumes NO_META_SYNC (SPEC 06 REC-9): still zero barriers.
    let d = DurabilityFlags {
        no_sync: true,
        no_meta_sync: true,
        ..Default::default()
    };
    let calls = commit_and_record(d);
    assert_eq!(calls, vec![Call::MetaWrite(1)]);
}

#[test]
fn map_async_makes_barriers_async() {
    let d = DurabilityFlags {
        map_async: true,
        write_map: true,
        ..Default::default()
    };
    let calls = commit_and_record(d);
    assert_eq!(
        calls,
        vec![Call::Sync(true), Call::MetaWrite(1), Call::Sync(true)],
        "MAP_ASYNC: C3 + C5 both async msync, plain C4 — no durable write"
    );
}

#[test]
fn failed_durable_meta_write_scrubs_old_bytes_and_poisons() {
    // ADR-0019 / SPEC 06 REC-13 as amended: on a failed durable meta write
    // the pipeline (1) rewrites the slot's PREVIOUS bytes through the plain
    // write path (LMDB's scrub — a failed O_DSYNC write may have left the new
    // meta in the OS page cache, and a clean reopen before power loss must
    // not read back an unacknowledged commit), then (2) poisons the env.
    let (env, (calls, meta_bytes)) = env_with(DurabilityFlags::default(), true, "fail");
    let old_slot1 = fresh_image()[PS as usize..2 * PS as usize].to_vec();

    let mut w = env.write_txn().expect("write txn");
    let db = env.create_database(&mut w, None).expect("main db");
    db.put(&mut w, b"k", b"v").expect("put");
    let err = w.commit().expect_err("commit must fail");
    assert!(matches!(err, zerodb_core::error::Error::Io(_)));

    let seq = calls.lock().unwrap().clone();
    assert_eq!(
        seq,
        vec![
            Call::Sync(false),     // C3
            Call::DurableWrite(1), // fused C4+C5, fails
            Call::MetaWrite(1),    // the scrub, through the plain path
        ],
        "failure order: C3, failed durable write, then the scrub"
    );
    // The scrub rewrote the slot's previous content byte-for-byte (here the
    // creation meta of slot 1 — the slot the first commit targets, TXN-63).
    let scrubbed = meta_bytes.lock().unwrap().clone();
    assert_eq!(scrubbed.len(), 1);
    assert_eq!(
        scrubbed[0], old_slot1,
        "scrub must restore the slot's previous bytes exactly"
    );

    // REC-13: the env is poisoned — every subsequent write txn is refused.
    assert!(env.inner().is_poisoned(), "env must be poisoned");
    match env.write_txn() {
        Err(zerodb_core::error::Error::Io(_)) => {}
        Err(other) => panic!("poisoned env: expected Io error, got {other}"),
        Ok(_) => panic!("poisoned env must refuse writers"),
    }
    // The snapshot was never published: readers still see txnid 0.
    assert_eq!(env.txnid(), 0, "failed commit must not publish");
}

#[test]
fn failed_meta_barrier_under_write_map_still_poisons() {
    // The pre-ADR-0019 C5 failure path (WRITE_MAP keeps it): a failed meta
    // msync poisons without the durable-write scrub.
    struct FailingSecondSync {
        data: Box<[u8]>,
        syncs: Arc<Mutex<u32>>,
    }
    impl Backing for FailingSecondSync {
        fn bytes(&self) -> &[u8] {
            &self.data
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(self.data.len() as u64)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "no fd",
            ))
        }
        fn write_at_page(&self, _: u64, _: u32, _: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.sync(false)
        }
        fn sync(&self, _async_flush: bool) -> std::io::Result<()> {
            let mut n = self.syncs.lock().unwrap();
            *n += 1;
            if *n >= 2 {
                return Err(std::io::Error::other("injected C5 failure"));
            }
            Ok(())
        }
    }
    let syncs = Arc::new(Mutex::new(0));
    let backing = FailingSecondSync {
        data: fresh_image(),
        syncs: Arc::clone(&syncs),
    };
    let path = std::path::PathBuf::from(format!("/virtual/dur-{}-c5fail", std::process::id()));
    let d = DurabilityFlags {
        write_map: true,
        ..Default::default()
    };
    let env = open_with_backing(path, Box::new(backing), PS, MAP, false, 8, 16, d).expect("open");
    let mut w = env.write_txn().expect("write txn");
    let db = env.create_database(&mut w, None).expect("main db");
    db.put(&mut w, b"k", b"v").expect("put");
    w.commit().expect_err("C5 failure must fail the commit");
    assert!(env.inner().is_poisoned());
    assert_eq!(env.txnid(), 0);
}
