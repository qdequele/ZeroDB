//! Milestone 1.4 coverage pass, area 4: abort/poison semantics not already
//! covered.
//!
//! `crates/zerodb-core/src/rwtxn.rs`'s own `#[cfg(test)] mod tests` already
//! has `map_full_poisons_txn` (area 4b: a mid-mutation `MapFull` poisons the
//! txn — subsequent mutation AND commit both return `BadTxn`, matching
//! SPEC 04 TXN-59/REC-13/§8.1) and `key_and_value_bounds` (empty/oversized
//! key -> `BadValSize`, followed by a *successful* put in the same txn,
//! which already implies non-poisoning but doesn't say so). This file adds:
//!
//! 4a. An explicit, dedicated test that an oversized-key `BadValSize` does
//!     NOT poison the txn (LMDB only poisons on internal errors, per
//!     `rwtxn.rs::put_main`'s early-return-before-`self.errored=true` shape,
//!     confirmed by reading the source: the key/value size checks return
//!     before `put_apply` is ever called, so `self.errored` is never
//!     touched) — every subsequent op on the same txn, including commit,
//!     must still work.
//! 4c. Abort after a heavy, overflow-run-heavy mutation: extends
//!     `write_api.rs::abort_leaves_disk_untouched`'s byte-identical-file
//!     assertion to a txn that allocates and frees several multi-page
//!     overflow runs before aborting.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvOpenOptions, Error, MdbError};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-poison-{pid}-{seq}"));
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
const MAP: usize = 8 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

#[test]
fn oversized_key_error_does_not_poison_txn() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();

    // A baseline entry, so we can prove the txn is still fully usable after
    // the error (not merely "doesn't panic").
    db.put(&mut wtxn, b"before", b"ok").unwrap();

    // 512 bytes: one past MAX_KEY_SIZE (511, SPEC 01 §S4).
    let oversized = vec![7u8; 512];
    let e = db.put(&mut wtxn, &oversized, b"x").unwrap_err();
    assert!(
        matches!(e, Error::Mdb(MdbError::BadValSize)),
        "expected BadValSize, got {e:?}"
    );

    // The empty key too — same up-front-validated, non-poisoning class.
    let e2 = db.put(&mut wtxn, &[], b"x").unwrap_err();
    assert!(matches!(e2, Error::Mdb(MdbError::BadValSize)));

    // The txn must still be fully usable: not `BadTxn`, every op type works.
    db.put(&mut wtxn, b"after", b"still-works").unwrap();
    assert_eq!(db.get(&wtxn, b"before").unwrap(), Some(b"ok".as_slice()));
    assert!(db.delete(&mut wtxn, b"before").unwrap());
    assert_eq!(db.len(&wtxn).unwrap(), 1);
    // And commit succeeds — the ultimate proof the txn was never poisoned.
    wtxn.commit().unwrap();
    assert_clean(dir.path());

    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"after").unwrap(),
        Some(b"still-works".as_slice())
    );
    assert_eq!(db.get(&rtxn, b"before").unwrap(), None);
}

#[test]
fn abort_after_overflow_heavy_mutation_leaves_file_byte_identical() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"committed", b"1").unwrap();
        wtxn.commit().unwrap();
    }
    let before = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();

    {
        let mut wtxn = env.write_txn().unwrap();
        // Several multi-page overflow runs: allocate, then delete some of
        // them (freeing the run), replace another with a different-size
        // overflow value (COW + free-old-run), and delete the pre-existing
        // committed entry too — all inside the doomed txn.
        for i in 0..5u32 {
            let k = format!("ovf-{i}").into_bytes();
            db.put(&mut wtxn, &k, &vec![i as u8; 3 * PS as usize + 777])
                .unwrap();
        }
        assert!(db.delete(&mut wtxn, b"ovf-1").unwrap());
        assert!(db.delete(&mut wtxn, b"ovf-3").unwrap());
        db.put(&mut wtxn, b"ovf-0", &vec![0xEEu8; 5 * PS as usize])
            .unwrap();
        db.delete(&mut wtxn, b"committed").unwrap();
        assert!(zerodb::TxnRead::main_record(&wtxn).overflow_pages > 0);
        wtxn.abort();
    }
    let after = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    assert_eq!(
        before, after,
        "abort after overflow-heavy mutation must not change the file (TXN-59)"
    );
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"committed").unwrap(), Some(b"1".as_slice()));
    assert_eq!(db.len(&rtxn).unwrap(), 1);
    for i in 0..5u32 {
        let k = format!("ovf-{i}").into_bytes();
        assert_eq!(db.get(&rtxn, &k).unwrap(), None);
    }
}
