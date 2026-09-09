//! Surface parity pins between real heed (LMDB fork) and `heed-zerodb`, for
//! behaviours a consumer observes without touching a database: error message
//! text, the environment/transaction pairing assertion, and how DUPSORT-only
//! put flags are treated on a plain database (observed, not guessed — rule 1).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A throwaway directory (no `tempfile` dependency in this crate).
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-surface-{pid}-{seq}"));
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

/// Every `MdbError` variant heed 0.22.1 exposes (non-`master3`), paired.
macro_rules! variants {
    ($($v:ident),* $(,)?) => {
        vec![$( (heed::MdbError::$v, heed_zerodb::MdbError::$v, stringify!($v)) ),*]
    };
}

/// `Display` of every `MdbError` variant is byte-identical to heed's, which
/// prints LMDB's `mdb_strerror` text. Consumers format these into logs and
/// HTTP error bodies, so the wording is part of the drop-in surface.
#[test]
fn mdb_error_display_matches_heed_for_every_variant() {
    let pairs = variants![
        KeyExist,
        NotFound,
        PageNotFound,
        Corrupted,
        Panic,
        VersionMismatch,
        Invalid,
        MapFull,
        DbsFull,
        ReadersFull,
        TlsFull,
        TxnFull,
        CursorFull,
        PageFull,
        MapResized,
        Incompatible,
        BadRslot,
        BadTxn,
        BadValSize,
        BadDbi,
        Problem,
    ];
    assert_eq!(
        pairs.len(),
        21,
        "heed 0.22.1 has 21 named MdbError variants"
    );
    for (lmdb, zdb, name) in pairs {
        assert_eq!(
            lmdb.to_string(),
            zdb.to_string(),
            "MdbError::{name} Display text differs from heed's"
        );
    }
    // `Other(code)` renders through the OS error text on both sides.
    assert_eq!(
        heed::MdbError::Other(2).to_string(),
        heed_zerodb::MdbError::Other(2).to_string()
    );
}

/// `Error::Mdb` wraps the same text (heed: `write!(f, "{e}")`), so the full
/// `heed::Error` display matches too.
#[test]
fn error_display_wrapping_matches_heed() {
    assert_eq!(
        heed::Error::Mdb(heed::MdbError::MapFull).to_string(),
        heed_zerodb::Error::Mdb(heed_zerodb::MdbError::MapFull).to_string()
    );
}

/// heed panics with a fixed message when a `Database` handle is used with a
/// transaction from another environment (`assert_eq_env_db_txn!`). The adapter
/// must panic the same way instead of walking the wrong file's pages.
#[test]
fn database_used_with_another_envs_txn_panics_like_heed() {
    // --- real heed / LMDB
    let (a, b) = (TempDir::new(), TempDir::new());
    let env_a = unsafe {
        heed::EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(1 << 24)
            .open(a.path())
            .unwrap()
    };
    let env_b = unsafe {
        heed::EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(1 << 24)
            .open(b.path())
            .unwrap()
    };
    let mut w = env_a.write_txn().unwrap();
    let db_a: heed::Database<heed::types::Bytes, heed::types::Bytes> =
        env_a.create_database(&mut w, None).unwrap();
    w.commit().unwrap();
    let r_b = env_b.read_txn().unwrap();
    let lmdb_panic = catch_unwind(AssertUnwindSafe(|| db_a.get(&r_b, b"k").unwrap()))
        .err()
        .and_then(|p| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("heed must panic on an env/txn mismatch");

    // --- adapter
    let (a, b) = (TempDir::new(), TempDir::new());
    let env_a = unsafe {
        heed_zerodb::EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(1 << 24)
            .open(a.path())
            .unwrap()
    };
    let env_b = unsafe {
        heed_zerodb::EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(1 << 24)
            .open(b.path())
            .unwrap()
    };
    let mut w = env_a.write_txn().unwrap();
    let db_a: heed_zerodb::Database<heed_zerodb::types::Bytes, heed_zerodb::types::Bytes> =
        env_a.create_database(&mut w, None).unwrap();
    w.commit().unwrap();
    let r_b = env_b.read_txn().unwrap();
    let zdb_panic = catch_unwind(AssertUnwindSafe(|| db_a.get(&r_b, b"k").unwrap()))
        .err()
        .and_then(|p| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("adapter must panic on an env/txn mismatch");

    assert_eq!(lmdb_panic, zdb_panic, "panic message must match heed's");
    // Same-env use still works, of course.
    let r_a = env_a.read_txn().unwrap();
    assert!(db_a.get(&r_a, b"k").unwrap().is_none());
}

/// What does the fork do with DUPSORT-only put flags on a plain (non-DUPSORT)
/// database? Observed here and mirrored by the adapter: the two engines must
/// return the same `Result` shape for `NO_DUP_DATA` and `APPEND_DUP`.
#[test]
fn dup_only_put_flags_on_plain_db_match_the_fork() {
    fn outcome_lmdb(flags: heed::PutFlags) -> String {
        let d = TempDir::new();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .map_size(1 << 24)
                .open(d.path())
                .unwrap()
        };
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut w, None).unwrap();
        let r = db.put_with_flags(&mut w, flags, b"k", b"v");
        let after = r
            .as_ref()
            .map(|_| db.get(&w, b"k").unwrap().map(<[u8]>::to_vec));
        format!("{:?}", after.map_err(|e| e.to_string()))
    }
    fn outcome_zdb(flags: heed_zerodb::PutFlags) -> String {
        let d = TempDir::new();
        let env = unsafe {
            heed_zerodb::EnvOpenOptions::new()
                .read_txn_without_tls()
                .map_size(1 << 24)
                .open(d.path())
                .unwrap()
        };
        let mut w = env.write_txn().unwrap();
        let db: heed_zerodb::Database<heed_zerodb::types::Bytes, heed_zerodb::types::Bytes> =
            env.create_database(&mut w, None).unwrap();
        let r = db.put_with_flags(&mut w, flags, b"k", b"v");
        let after = r
            .as_ref()
            .map(|_| db.get(&w, b"k").unwrap().map(<[u8]>::to_vec));
        format!("{:?}", after.map_err(|e| e.to_string()))
    }
    for (l, z, name) in [
        (
            heed::PutFlags::NO_DUP_DATA,
            heed_zerodb::PutFlags::NO_DUP_DATA,
            "NO_DUP_DATA",
        ),
        (
            heed::PutFlags::APPEND_DUP,
            heed_zerodb::PutFlags::APPEND_DUP,
            "APPEND_DUP",
        ),
        (
            heed::PutFlags::APPEND_DUP | heed::PutFlags::APPEND,
            heed_zerodb::PutFlags::APPEND_DUP | heed_zerodb::PutFlags::APPEND,
            "APPEND_DUP|APPEND",
        ),
    ] {
        let (a, b) = (outcome_lmdb(l), outcome_zdb(z));
        assert_eq!(
            a, b,
            "put_with_flags({name}) on a plain DB: fork={a} adapter={b}"
        );
    }
}
