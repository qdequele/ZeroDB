//! ADR-0010 / D-012 — `EnvOpenOptions::data_file_name` on the native engine.
//!
//! The engine keeps `zerodb.dat` as its default and resolves the name **once**,
//! at open, with **no fallback probing**: it opens exactly the name it was
//! configured with. That determinism is what lets the `heed-zerodb` adapter
//! always read the same file it writes.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Env, EnvOpenOptions, Error, DATA_FILE_NAME, HEED_DATA_FILE_NAME};

const MAP_SIZE: usize = 1024 * 1024;

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
        let path = std::env::temp_dir().join(format!("zerodb-name-{pid}-{nanos}-{seq}"));
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

fn dir_listing(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn put(env: &Env, k: &[u8], v: &[u8]) {
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, None).unwrap();
    db.put(&mut wtxn, k, v).unwrap();
    wtxn.commit().unwrap();
}

fn get(env: &Env, k: &[u8]) -> Option<Vec<u8>> {
    let rtxn = env.read_txn().unwrap();
    let db = env.open_database(&rtxn, None).unwrap().unwrap();
    db.get(&rtxn, k).unwrap().map(<[u8]>::to_vec)
}

/// Regression: the native default is unchanged — a plain `open` creates
/// `zerodb.dat` and nothing else.
#[test]
fn native_default_is_zerodb_dat() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP_SIZE);
    assert_eq!(o.get_data_file_name(), DATA_FILE_NAME);
    let env = o.open(dir.path()).unwrap();
    put(&env, b"k", b"v");

    assert_eq!(dir_listing(dir.path()), vec![DATA_FILE_NAME.to_string()]);
    assert_eq!(DATA_FILE_NAME, "zerodb.dat");
}

/// The knob works natively: an env opened with the adapter's name materializes
/// under that name and round-trips.
#[test]
fn configured_name_is_used_and_reopens() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP_SIZE).data_file_name(HEED_DATA_FILE_NAME);
    let env = o.open(dir.path()).unwrap();
    put(&env, b"k", b"v");
    assert_eq!(dir_listing(dir.path()), vec!["data.mdb".to_string()]);
    env.prepare_for_closing().wait();

    let reopened = o.open(dir.path()).unwrap();
    assert_eq!(get(&reopened, b"k").as_deref(), Some(&b"v"[..]));
}

/// **No fallback probing in the engine.** A directory holding only
/// `zerodb.dat` opened with `data_file_name("data.mdb")` yields a *fresh empty*
/// env at the configured name — it does not adopt the other file. Deterministic
/// by design (ADR-0010 §Options D3).
#[test]
fn name_mismatch_creates_a_fresh_env_and_does_not_adopt() {
    let dir = TempDir::new();

    let mut native = EnvOpenOptions::new();
    native.map_size(MAP_SIZE);
    let env = native.open(dir.path()).unwrap();
    put(&env, b"native", b"data");
    assert_eq!(get(&env, b"native").as_deref(), Some(&b"data"[..]));
    env.prepare_for_closing().wait();

    let mut heed_named = EnvOpenOptions::new();
    heed_named
        .map_size(MAP_SIZE)
        .data_file_name(HEED_DATA_FILE_NAME);
    let fresh = heed_named.open(dir.path()).unwrap();
    // Fresh and empty: the `zerodb.dat` next door was not adopted.
    assert_eq!(get(&fresh, b"native"), None);
    // Both files now exist side by side — two separate databases. (This is the
    // state `zerodb-tools` refuses as ambiguous.)
    assert_eq!(
        dir_listing(dir.path()),
        vec!["data.mdb".to_string(), "zerodb.dat".to_string()]
    );
}

/// Two names in one directory are two independent envs, each keeping its own
/// contents.
#[test]
fn two_names_are_two_independent_envs() {
    let dir = TempDir::new();

    let mut a = EnvOpenOptions::new();
    a.map_size(MAP_SIZE);
    let env_a = a.open(dir.path()).unwrap();
    put(&env_a, b"who", b"native");
    env_a.prepare_for_closing().wait();

    let mut b = EnvOpenOptions::new();
    b.map_size(MAP_SIZE).data_file_name(HEED_DATA_FILE_NAME);
    let env_b = b.open(dir.path()).unwrap();
    put(&env_b, b"who", b"adapter");
    assert_eq!(get(&env_b, b"who").as_deref(), Some(&b"adapter"[..]));
    env_b.prepare_for_closing().wait();

    let env_a = a.open(dir.path()).unwrap();
    assert_eq!(get(&env_a, b"who").as_deref(), Some(&b"native"[..]));
}

/// A name that is not a single path component is rejected at open with
/// `Io(InvalidInput)` — the D-006/D-010 open-time taxonomy. It must never be
/// able to name a file outside the env directory.
#[test]
fn invalid_names_are_invalid_input() {
    let dir = TempDir::new();
    for bad in ["", "sub/data.mdb", "/abs/data.mdb", ".", "..", "a/"] {
        let mut o = EnvOpenOptions::new();
        o.map_size(MAP_SIZE).data_file_name(bad);
        match o.open(dir.path()) {
            Err(Error::Io(e)) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::InvalidInput,
                "wrong error kind for {bad:?}"
            ),
            other => panic!("expected Io(InvalidInput) for {bad:?}, got {other:?}"),
        }
        // Nothing was created.
        assert!(dir_listing(dir.path()).is_empty(), "{bad:?} created a file");
    }
}

/// A valid-but-unusual name is accepted: the knob is a general integration
/// point, not a two-value enum.
#[test]
fn arbitrary_single_component_name_works() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP_SIZE).data_file_name("tasks.store");
    let env = o.open(dir.path()).unwrap();
    put(&env, b"k", b"v");
    assert_eq!(dir_listing(dir.path()), vec!["tasks.store".to_string()]);
}
