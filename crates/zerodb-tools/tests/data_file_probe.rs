//! ADR-0010 / D-012 — the tools' two-name data-file probe.
//!
//! Since ADR-0010 an env directory holds `zerodb.dat` (native) or `data.mdb`
//! (created through the `heed-zerodb` adapter, so Meilisearch's hardcoded
//! `data.mdb` paths work). The read tools must work on either without the
//! operator knowing which stack wrote the env — and must **refuse** a directory
//! holding both, because those are two different databases and no pick is
//! defensible.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Env, EnvOpenOptions, DATA_FILE_NAME, HEED_DATA_FILE_NAME};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let p = std::env::temp_dir().join(format!(
        "zerodb-probe-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Build an env under an explicit data-file name and close it (tools are
/// offline; the engine registry also keys per canonical dir).
fn build_env(dir: &Path, name: &str, marker: &[u8]) {
    let env: Env = EnvOpenOptions::new()
        .map_size(1 << 20)
        .data_file_name(name)
        .open(dir)
        .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    env.main_database().put(&mut wtxn, b"k", marker).unwrap();
    wtxn.commit().unwrap();
    env.prepare_for_closing().wait();
}

// ---------------------------------------------------------------------------
// Probe: either name alone works
// ---------------------------------------------------------------------------

/// `stat`/`dump`/`check` all work on a **native** env (`zerodb.dat`).
#[test]
fn tools_read_a_native_named_env() {
    let dir = tmp_dir("native");
    build_env(&dir, DATA_FILE_NAME, b"native");

    let stat = zerodb_tools::commands::cmd_stat(&dir).unwrap();
    assert!(stat.contains("zerodb.dat"), "stat output: {stat}");
    let dump = zerodb_tools::commands::cmd_dump(&dir).unwrap();
    assert!(dump.contains(&hex(b"native")), "dump output: {dump}");
    let (report, clean) = zerodb_tools::commands::cmd_check(&dir).unwrap();
    assert!(clean, "check report: {report}");
}

/// The regression this ADR exists for: `stat`/`dump`/`check` all work on an
/// **adapter-created** env (`data.mdb`). Before the two-name probe these
/// reported "no zerodb env here".
#[test]
fn tools_read_an_adapter_named_env() {
    let dir = tmp_dir("adapter");
    build_env(&dir, HEED_DATA_FILE_NAME, b"adapter");

    let stat = zerodb_tools::commands::cmd_stat(&dir).unwrap();
    assert!(stat.contains("data.mdb"), "stat output: {stat}");
    let dump = zerodb_tools::commands::cmd_dump(&dir).unwrap();
    assert!(dump.contains(&hex(b"adapter")), "dump output: {dump}");
    let (report, clean) = zerodb_tools::commands::cmd_check(&dir).unwrap();
    assert!(clean, "check report: {report}");
}

/// `stat` identifies the real engine from the file's own `ZDB1` magic — so an
/// operator looking at a `data.mdb` that is not an LMDB file gets a straight
/// answer instead of `mdb_stat`'s `MDB_INVALID`.
#[test]
fn stat_prints_the_engine_identification_line() {
    let dir = tmp_dir("engine-line");
    build_env(&dir, HEED_DATA_FILE_NAME, b"x");

    let stat = zerodb_tools::commands::cmd_stat(&dir).unwrap();
    let line = stat
        .lines()
        .find(|l| l.trim_start().starts_with("engine:"))
        .expect("stat must print an engine line");
    assert!(line.contains("zerodb"), "engine line: {line}");
    assert!(line.contains("ZDB1"), "engine line: {line}");
    assert!(line.contains("format_version 1"), "engine line: {line}");
    assert!(line.contains("data.mdb"), "engine line: {line}");
}

// ---------------------------------------------------------------------------
// Both present is a HARD ERROR
// ---------------------------------------------------------------------------

/// A directory holding both names is ambiguous: every read tool refuses, and
/// the message names both files. No silent pick.
#[test]
fn both_names_present_is_a_hard_error() {
    let dir = tmp_dir("both");
    build_env(&dir, DATA_FILE_NAME, b"native");
    build_env(&dir, HEED_DATA_FILE_NAME, b"adapter");
    assert!(dir.join(DATA_FILE_NAME).exists());
    assert!(dir.join(HEED_DATA_FILE_NAME).exists());

    for (tool, err) in [
        (
            "stat",
            zerodb_tools::commands::cmd_stat(&dir).err().is_none(),
        ),
        (
            "dump",
            zerodb_tools::commands::cmd_dump(&dir).err().is_none(),
        ),
        (
            "check",
            zerodb_tools::commands::cmd_check(&dir).err().is_none(),
        ),
    ] {
        assert!(!err, "{tool} must refuse an ambiguous env dir");
    }

    let msg = zerodb_tools::commands::cmd_stat(&dir)
        .unwrap_err()
        .to_string();
    assert!(msg.contains("data.mdb"), "message: {msg}");
    assert!(msg.contains("zerodb.dat"), "message: {msg}");
    assert!(
        msg.to_lowercase().contains("both") || msg.to_lowercase().contains("ambiguous"),
        "message must say it is ambiguous: {msg}"
    );
}

/// The flock guard itself resolves both names, and reports ambiguity.
#[test]
fn lock_guard_probes_both_names() {
    let native = tmp_dir("lock-native");
    build_env(&native, DATA_FILE_NAME, b"n");
    let (_g, resolved) = zerodb_tools::lock::acquire_existing(&native).unwrap();
    assert_eq!(resolved.name, DATA_FILE_NAME);

    let adapter = tmp_dir("lock-adapter");
    build_env(&adapter, HEED_DATA_FILE_NAME, b"a");
    let (_g, resolved) = zerodb_tools::lock::acquire_existing(&adapter).unwrap();
    assert_eq!(resolved.name, HEED_DATA_FILE_NAME);

    let both = tmp_dir("lock-both");
    build_env(&both, DATA_FILE_NAME, b"n");
    build_env(&both, HEED_DATA_FILE_NAME, b"a");
    assert!(matches!(
        zerodb_tools::lock::acquire_existing(&both),
        Err(zerodb_tools::lock::LockError::Ambiguous(_))
    ));

    let empty = tmp_dir("lock-empty");
    assert!(matches!(
        zerodb_tools::lock::acquire_existing(&empty),
        Err(zerodb_tools::lock::LockError::Missing(_))
    ));
}

// ---------------------------------------------------------------------------
// Write tools refuse to clobber an env under either name
// ---------------------------------------------------------------------------

/// `load` must not drop a `zerodb.dat` next to a populated adapter `data.mdb` —
/// that would manufacture the ambiguous state the read tools refuse.
#[test]
fn load_refuses_a_dir_holding_an_adapter_env() {
    let dir = tmp_dir("load-refuse");
    build_env(&dir, HEED_DATA_FILE_NAME, b"adapter");

    let dump = tmp_dir("load-refuse-src").join("dump.txt");
    std::fs::write(&dump, "VERSION=3\nformat=bytevalue\nHEADER=END\nDATA=END\n").unwrap();

    let err = zerodb_tools::commands::cmd_load(&dump, &dir, 4096, 1 << 20)
        .unwrap_err()
        .to_string();
    assert!(err.contains("data.mdb"), "message: {err}");
    assert!(err.contains("refusing"), "message: {err}");
    // And it did not create a second data file.
    assert!(!dir.join(DATA_FILE_NAME).exists());
}

/// Hex-encode as the dump format renders values, so the assertions above can
/// look for a marker in the dump text.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
