//! M1.10 — durability-flag control-flow parity (SPEC 01 §S6, SPEC 06 REC-9).
//!
//! The oracle differential tests confirm that a commit under each durability
//! flag produces identical *data*. This test confirms the other half: that the
//! commit pipeline runs exactly the right fsync/msync **barriers** per flag —
//! the control flow the flags are *for* (their crash-window semantics are then
//! validated by the M1.11 harness). It drives a real commit through a counting
//! [`Backing`] and asserts the number and kind of `sync` calls:
//!
//! | Flags | C3 (data) | C5 (meta) | `sync` calls | async? |
//! |-------|-----------|-----------|--------------|--------|
//! | default | yes | yes | 2 | no |
//! | `NO_META_SYNC` | yes | no | 1 | no |
//! | `NO_SYNC` | no | no | 0 | — |
//! | `MAP_ASYNC` | yes | yes | 2 | yes |

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

/// A [`Backing`] that accepts writes (no-op — the barrier count is all we
/// assert) and records every `sync` call and whether it was async, into a
/// shared `Arc` the caller can read after the backing is moved into the env.
/// `bytes()` returns a fixed valid two-meta image so `open` and the in-txn
/// reads succeed.
struct CountingBacking {
    data: Box<[u8]>,
    /// One entry per `sync` call: `true` = async flush (`MAP_ASYNC`).
    syncs: Arc<Mutex<Vec<bool>>>,
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
    fn write_at_page(&self, _pgno: u64, _psize: u32, _data: &[u8]) -> std::io::Result<()> {
        Ok(())
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.syncs.lock().unwrap().push(false);
        Ok(())
    }
    fn sync(&self, async_flush: bool) -> std::io::Result<()> {
        self.syncs.lock().unwrap().push(async_flush);
        Ok(())
    }
}

/// Drive one create-db + put + commit under `durability` and return the recorded
/// `sync` calls (each `true` if async).
fn commit_and_record(durability: DurabilityFlags) -> Vec<bool> {
    let syncs = Arc::new(Mutex::new(Vec::new()));
    let backing = CountingBacking {
        data: fresh_image(),
        syncs: Arc::clone(&syncs),
    };
    let path = std::path::PathBuf::from(format!(
        "/virtual/dur-{}-{:?}",
        std::process::id(),
        durability
    ));
    let env = open_with_backing(path, Box::new(backing), PS, MAP, false, 8, 16, durability)
        .expect("open");
    let mut w = env.write_txn().expect("write txn");
    let db = env.create_database(&mut w, None).expect("main db");
    db.put(&mut w, b"k", b"v").expect("put");
    w.commit().expect("commit");
    let out = syncs.lock().unwrap().clone();
    out
}

#[test]
fn default_mode_syncs_data_and_meta() {
    let syncs = commit_and_record(DurabilityFlags::default());
    assert_eq!(
        syncs,
        vec![false, false],
        "default: C3 + C5, both synchronous"
    );
}

#[test]
fn no_meta_sync_skips_meta_barrier() {
    let d = DurabilityFlags {
        no_meta_sync: true,
        ..Default::default()
    };
    let syncs = commit_and_record(d);
    assert_eq!(syncs, vec![false], "NO_META_SYNC: only C3 runs");
}

#[test]
fn no_sync_skips_both_barriers() {
    let d = DurabilityFlags {
        no_sync: true,
        ..Default::default()
    };
    let syncs = commit_and_record(d);
    assert!(
        syncs.is_empty(),
        "NO_SYNC: neither C3 nor C5 runs, got {syncs:?}"
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
    let syncs = commit_and_record(d);
    assert!(
        syncs.is_empty(),
        "NO_SYNC|NO_META_SYNC: zero barriers, got {syncs:?}"
    );
}

#[test]
fn map_async_makes_barriers_async() {
    let d = DurabilityFlags {
        map_async: true,
        write_map: true,
        ..Default::default()
    };
    let syncs = commit_and_record(d);
    assert_eq!(syncs, vec![true, true], "MAP_ASYNC: C3 + C5, both async");
}
