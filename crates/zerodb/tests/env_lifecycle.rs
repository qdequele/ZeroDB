//! Milestone 1.2 zerodb-only env-lifecycle tests (SPEC 02 §3.2, SPEC 06 §1,
//! SPEC 04 §7). These drive the public `zerodb::Env` API directly rather than
//! through the oracle `Op` model, because the corrupted-meta / PREV_SNAPSHOT
//! cases depend on **our** on-disk format, which LMDB does not share (the
//! differential env-lifecycle parity tests live in `zerodb-oracle`).
//!
//! Each test names the REC rule / SPEC section it pins. Do not weaken these
//! (CLAUDE.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use zerodb::{EnvFlags, EnvOpenOptions, Error, MdbError};
use zerodb_core::page::MetaPage;

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

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
        let path = std::env::temp_dir().join(format!("zerodb-env-{pid}-{nanos}-{seq}"));
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

const PS: u32 = 4096;
const MAP: u64 = 1 << 20;

/// Encode one slot's meta with a given txnid into a `psize` buffer.
fn slot_bytes(slot: u64, txnid: u64) -> Vec<u8> {
    let mut m = MetaPage::create(slot, PS, MAP);
    m.txnid = txnid;
    let mut buf = vec![0u8; PS as usize];
    m.encode(&mut buf).expect("encode meta");
    buf
}

/// Hand-craft a two-slot `zerodb.dat` in `dir` with the given per-slot txnids.
fn craft_env(dir: &Path, txnid0: u64, txnid1: u64) {
    let mut file = slot_bytes(0, txnid0);
    file.extend_from_slice(&slot_bytes(1, txnid1));
    std::fs::write(dir.join(zerodb::DATA_FILE_NAME), file).expect("write crafted env");
}

// ---------------------------------------------------------------------------

/// create → info/path/geometry, then reopen the existing env (SPEC 02 §3.4/§8).
#[test]
fn create_then_reopen_roundtrip() {
    let dir = TempDir::new();
    {
        let mut o = EnvOpenOptions::new();
        o.map_size(MAP as usize);
        let env = o.open(dir.path()).expect("create env");
        assert_eq!(env.page_size(), PS);
        assert_eq!(env.info().map_size, MAP);
        assert_eq!(env.txnid(), 0, "fresh env is at txnid 0");
        // The env path is the canonical directory.
        assert_eq!(env.path(), dir.path().canonicalize().unwrap());
        // A fresh file is exactly the two meta slots (SPEC 02 §3.4).
        assert_eq!(env.real_disk_size().unwrap(), 2 * PS as u64);
        env.prepare_for_closing().wait();
    }
    // Reopen the now-existing env: page size / map size persist.
    let env = EnvOpenOptions::new().open(dir.path()).expect("reopen env");
    assert_eq!(env.page_size(), PS);
    assert_eq!(env.info().map_size, MAP);
}

/// Reopening with a larger map_size raises the runtime ceiling (SPEC 02 §8;
/// growth = reopen-larger). `info().map_size` reflects the new value.
#[test]
fn reopen_with_larger_map_size() {
    let dir = TempDir::new();
    {
        let mut o = EnvOpenOptions::new();
        o.map_size(MAP as usize);
        let env = o.open(dir.path()).unwrap();
        assert_eq!(env.info().map_size, MAP);
        env.prepare_for_closing().wait();
    }
    let bigger = (MAP * 4) as usize;
    let mut o = EnvOpenOptions::new();
    o.map_size(bigger);
    let env = o.open(dir.path()).unwrap();
    assert_eq!(env.info().map_size, bigger as u64);
}

/// SPEC 04 TXN-51: a second open of a still-live canonical path is
/// `EnvAlreadyOpened`.
#[test]
fn second_open_is_env_already_opened() {
    let dir = TempDir::new();
    let env = EnvOpenOptions::new().open(dir.path()).unwrap();
    let e = EnvOpenOptions::new().open(dir.path()).unwrap_err();
    assert!(matches!(e, Error::EnvAlreadyOpened));
    // Dropping the first frees the path.
    drop(env);
    let _again = EnvOpenOptions::new().open(dir.path()).unwrap();
}

/// SPEC 02 §3.2 / SPEC 06 REC-2: normal open picks the higher txnid; a
/// PREV_SNAPSHOT open picks the lower.
#[test]
fn prev_snapshot_selects_older_meta() {
    let dir = TempDir::new();
    craft_env(dir.path(), 5, 9);
    {
        let env = EnvOpenOptions::new().open(dir.path()).unwrap();
        assert_eq!(env.txnid(), 9, "normal open selects the higher txnid");
        env.prepare_for_closing().wait();
    }
    let mut o = EnvOpenOptions::new();
    o.flags(EnvFlags::PREV_SNAPSHOT);
    let env = o.open(dir.path()).unwrap();
    assert_eq!(env.txnid(), 5, "PREV_SNAPSHOT selects the older txnid");
    assert!(env.is_prev_snapshot());
}

/// SPEC 06 REC-2 (one-valid fallback): corrupting the higher-txnid slot makes
/// open fall back to the older intact slot — the torn-meta recovery guarantee.
#[test]
fn corrupted_higher_slot_falls_back_to_older() {
    let dir = TempDir::new();
    craft_env(dir.path(), 4, 9);
    let data = dir.path().join(zerodb::DATA_FILE_NAME);
    let mut bytes = std::fs::read(&data).unwrap();
    // Flip a byte inside slot 1's CRC-covered region [ps, ps+168) → torn.
    bytes[PS as usize + 100] ^= 0xFF;
    std::fs::write(&data, &bytes).unwrap();

    let env = EnvOpenOptions::new().open(dir.path()).unwrap();
    assert_eq!(env.txnid(), 4, "must recover the older intact snapshot");
}

/// SPEC 06 REC-3: both slots torn → `MdbError::Invalid` (unrecoverable).
#[test]
fn both_slots_corrupted_is_invalid() {
    let dir = TempDir::new();
    craft_env(dir.path(), 4, 9);
    let data = dir.path().join(zerodb::DATA_FILE_NAME);
    let mut bytes = std::fs::read(&data).unwrap();
    bytes[100] ^= 0xFF; // slot 0 CRC region
    bytes[PS as usize + 100] ^= 0xFF; // slot 1 CRC region
    std::fs::write(&data, &bytes).unwrap();

    let e = EnvOpenOptions::new().open(dir.path()).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
}

/// SPEC 06 REC-2†: exactly one valid slot + PREV_SNAPSHOT → hard `Invalid`.
#[test]
fn one_valid_slot_with_prev_snapshot_is_invalid() {
    let dir = TempDir::new();
    craft_env(dir.path(), 4, 9);
    let data = dir.path().join(zerodb::DATA_FILE_NAME);
    let mut bytes = std::fs::read(&data).unwrap();
    bytes[PS as usize + 100] ^= 0xFF; // torn slot 1; only slot 0 valid
    std::fs::write(&data, &bytes).unwrap();

    let mut o = EnvOpenOptions::new();
    o.flags(EnvFlags::PREV_SNAPSHOT);
    let e = o.open(dir.path()).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
}

/// A foreign / garbage file is rejected as `Invalid` (SPEC 02 §3.2 rule 1).
#[test]
fn garbage_file_is_invalid() {
    let dir = TempDir::new();
    std::fs::write(
        dir.path().join(zerodb::DATA_FILE_NAME),
        vec![0xFFu8; PS as usize],
    )
    .unwrap();
    let e = EnvOpenOptions::new().open(dir.path()).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
}

/// `try_clone_inner_file` dups the data-file fd (SPEC 00 row 22).
#[test]
fn try_clone_inner_file_dups_fd() {
    let dir = TempDir::new();
    let env = EnvOpenOptions::new().open(dir.path()).unwrap();
    let f = env.try_clone_inner_file().expect("dup fd");
    assert_eq!(f.metadata().unwrap().len(), 2 * PS as u64);
}

/// A non-existent env directory is an I/O error (LMDB parity), not a panic.
#[test]
fn missing_directory_is_io_error() {
    let missing = std::env::temp_dir().join("zerodb-does-not-exist-xyz-12345");
    let _ = std::fs::remove_dir_all(&missing);
    let e = EnvOpenOptions::new().open(&missing).unwrap_err();
    assert!(matches!(e, Error::Io(_)));
}

/// A page_size open option that reopens an existing env is ignored — the
/// persisted page size wins (SPEC 02 §3.2). The crafted env is 4096; asking for
/// 8192 must still open at 4096.
#[test]
fn persisted_page_size_wins_over_option() {
    let dir = TempDir::new();
    craft_env(dir.path(), 1, 1);
    let mut o = EnvOpenOptions::new();
    o.page_size(8192);
    let env = o.open(dir.path()).unwrap();
    assert_eq!(env.page_size(), PS, "persisted page size wins");
}

/// EnvClosingEvent fires only after the last handle drops (SPEC 04 TXN-52/53).
#[test]
fn closing_event_waits_for_last_handle() {
    let dir = TempDir::new();
    let env = EnvOpenOptions::new().open(dir.path()).unwrap();
    let clone = env.clone();
    let ev = env.prepare_for_closing();
    assert!(
        !ev.wait_timeout(Duration::from_millis(0)),
        "close must wait while a clone is alive"
    );
    drop(clone);
    ev.wait();
    assert!(ev.wait_timeout(Duration::from_millis(0)));
}
