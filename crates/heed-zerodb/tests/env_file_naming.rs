//! ADR-0010 / D-012 — the env directory's data-file name at the heed boundary.
//!
//! Before ADR-0010 an adapter-created env dir held exactly `["zerodb.dat"]`,
//! while Meilisearch joins the literal `"data.mdb"` onto env paths in
//! **production** code: index compaction (`process_batch.rs`,
//! `routes/tasks/compact.rs`, `meilitool`) and snapshot creation
//! (`process_snapshot_creation.rs`, `enterprise_edition/s3.rs`). Compaction
//! failed loudly on the `fs::metadata` probe; snapshot restore failed
//! *silently*, producing a directory that reopened as a fresh empty env — data
//! loss behind a green suite, because index-scheduler ships no compaction or
//! snapshot round-trip test.
//!
//! The two round-trip tests below are exactly the shapes those suites miss.

use std::fs;

use heed_zerodb::types::Bytes;
use heed_zerodb::{CompactionOption, Database, EnvOpenOptions, WithoutTls};

const MAP_SIZE: usize = 4 * 1024 * 1024;

fn opts() -> EnvOpenOptions<WithoutTls> {
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(MAP_SIZE).max_dbs(8);
    o
}

/// Populate an env with `n` entries in the main DB and a named DB, so a
/// round-trip has both catalog records and user data to preserve.
fn populate(env: &heed_zerodb::Env<WithoutTls>, n: u32) {
    let mut wtxn = env.write_txn().unwrap();
    let main: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    let named: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some("docs")).unwrap();
    for i in 0..n {
        let k = format!("key-{i:06}");
        let v = vec![(i % 251) as u8; 64 + (i as usize % 97)];
        main.put(&mut wtxn, k.as_bytes(), &v).unwrap();
        named.put(&mut wtxn, k.as_bytes(), &v).unwrap();
    }
    wtxn.commit().unwrap();
}

/// Named sub-databases this fixture creates. Their catalog records live in the
/// main DB and are excluded from the comparison below.
const SUB_DBS: [&str; 1] = ["docs"];

/// One database's entries, keyed by its name (`None` = the main DB).
type DbContents = (Option<String>, Vec<(Vec<u8>, Vec<u8>)>);

/// The full **logical** content of an env: main DB user entries + every named
/// DB's entries.
///
/// The main DB's `F_SUBDATA` catalog records are deliberately excluded: their
/// value is a `DBRecord` holding the sub-tree's **root page number**, which a
/// compacting copy legitimately renumbers. Comparing them would assert physical
/// page layout, not the logical equality a snapshot must preserve.
fn contents(env: &heed_zerodb::Env<WithoutTls>) -> Vec<DbContents> {
    let rtxn = env.read_txn().unwrap();
    let mut out = Vec::new();
    let main: Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();
    out.push((
        None,
        main.iter(&rtxn)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .filter(|(k, _)| !SUB_DBS.iter().any(|n| n.as_bytes() == k.as_slice()))
            .collect(),
    ));
    for name in SUB_DBS {
        if let Some(db) = env
            .open_database::<Bytes, Bytes>(&rtxn, Some(name))
            .unwrap()
        {
            out.push((
                Some(name.to_string()),
                db.iter(&rtxn)
                    .unwrap()
                    .map(|r| {
                        let (k, v) = r.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .collect(),
            ));
        }
    }
    out
}

/// Every file name in a directory, sorted.
fn dir_listing(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// Criterion 1 — adapter naming
// ---------------------------------------------------------------------------

/// A fresh env dir created through the adapter contains **exactly**
/// `["data.mdb"]`: no `zerodb.dat`, and no `lock.mdb` (nothing in the consumer
/// tree reads one — D-001 stands).
#[test]
fn adapter_env_dir_contains_exactly_data_mdb() {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(dir.path()).unwrap() };
    populate(&env, 32);

    assert_eq!(dir_listing(dir.path()), vec!["data.mdb".to_string()]);
    assert_eq!(heed_zerodb::DATA_FILE_NAME, "data.mdb");
    // `Env::path()` still returns the *directory*, not the data file — heed
    // parity, unchanged by ADR-0010. (Compared canonicalized: the engine
    // canonicalizes for its registry key, and macOS temp dirs are symlinked.)
    assert_eq!(env.path(), dir.path().canonicalize().unwrap());
}

/// The compaction call sites' very first step: `fs::metadata` on the joined
/// path. This is the ENOENT that used to abort every compaction task.
#[test]
fn metadata_probe_on_live_adapter_env_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(dir.path()).unwrap() };
    populate(&env, 64);

    let src = dir.path().join("data.mdb");
    let meta = fs::metadata(&src).expect("data.mdb must exist on a live adapter env");
    assert!(meta.len() > 0);
    assert!(meta.is_file());
}

// ---------------------------------------------------------------------------
// Criterion 3 — snapshot round-trip (process_snapshot_creation.rs shape)
// ---------------------------------------------------------------------------

/// `copy_to_path(dst.join("data.mdb"), option)` → reopen `dst` through the
/// adapter → full logical equality. Run for **both** compaction options.
///
/// This is the test whose absence made snapshot restore silently yield an
/// empty env.
fn snapshot_round_trip(option: CompactionOption) {
    let src_dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(src_dir.path()).unwrap() };
    populate(&env, 500);
    let expected = contents(&env);
    assert!(!expected[0].1.is_empty(), "source must be non-empty");

    // Exactly the process_snapshot_creation.rs call.
    let dst_dir = tempfile::tempdir().unwrap();
    env.copy_to_path(dst_dir.path().join("data.mdb"), option)
        .unwrap();

    // The snapshot dir looks like an LMDB env dir.
    assert_eq!(dir_listing(dst_dir.path()), vec!["data.mdb".to_string()]);

    // Restore: extract, open the directory. Before ADR-0010 this produced a
    // fresh EMPTY env instead of the snapshot's contents.
    let restored = unsafe { opts().open(dst_dir.path()).unwrap() };
    assert_eq!(contents(&restored), expected);
}

#[test]
fn snapshot_round_trip_compaction_disabled() {
    snapshot_round_trip(CompactionOption::Disabled);
}

#[test]
fn snapshot_round_trip_compaction_enabled() {
    snapshot_round_trip(CompactionOption::Enabled);
}

// ---------------------------------------------------------------------------
// Criterion 4 — compaction-persist round-trip (process_batch.rs shape)
// ---------------------------------------------------------------------------

/// The full Meilisearch index-compaction sequence:
///
/// 1. `fs::metadata(dir/data.mdb)` for the pre-compaction size,
/// 2. `copy_to_file` into `data.mdb.cpy`,
/// 3. rename the copy over `data.mdb` (`NamedTempFile::persist`),
/// 4. close the env (`prepare_for_closing` + `wait`),
/// 5. reopen and verify.
///
/// With `CompactionOption::Enabled` after deletions, also asserts the file
/// actually shrank — otherwise "compaction" could be a silent no-op.
fn compaction_persist_round_trip(option: CompactionOption, delete_first: bool) {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(dir.path()).unwrap() };
    populate(&env, 1200);

    if delete_first {
        // Free a lot of pages so compaction has something to reclaim.
        let mut wtxn = env.write_txn().unwrap();
        let main: Database<Bytes, Bytes> = env.open_database(&wtxn, None).unwrap().unwrap();
        let named: Database<Bytes, Bytes> =
            env.open_database(&wtxn, Some("docs")).unwrap().unwrap();
        for i in 0..1000u32 {
            let k = format!("key-{i:06}");
            main.delete(&mut wtxn, k.as_bytes()).unwrap();
            named.delete(&mut wtxn, k.as_bytes()).unwrap();
        }
        wtxn.commit().unwrap();
    }
    let expected = contents(&env);

    // Step 1 — the probe that used to ENOENT.
    let src_path = dir.path().join("data.mdb");
    let size_before = fs::metadata(&src_path).unwrap().len();

    // Step 2 — copy into `data.mdb.cpy`.
    let cpy_path = dir.path().join("data.mdb.cpy");
    let mut cpy = fs::File::create(&cpy_path).unwrap();
    env.copy_to_file(&mut cpy, option).unwrap();
    drop(cpy);

    // Step 3 — rename over the live, mmapped original. POSIX keeps the old
    // inode alive for the still-open env; the new name is picked up at reopen.
    fs::rename(&cpy_path, &src_path).unwrap();

    // Step 4 — close.
    env.prepare_for_closing().wait();

    // Step 5 — reopen the same directory and verify.
    let reopened = unsafe { opts().open(dir.path()).unwrap() };
    assert_eq!(contents(&reopened), expected);
    assert_eq!(dir_listing(dir.path()), vec!["data.mdb".to_string()]);

    if delete_first && matches!(option, CompactionOption::Enabled) {
        let size_after = fs::metadata(&src_path).unwrap().len();
        assert!(
            size_after < size_before,
            "compaction must reclaim: {size_after} !< {size_before}"
        );
    }
}

#[test]
fn compaction_persist_round_trip_disabled() {
    compaction_persist_round_trip(CompactionOption::Disabled, false);
}

#[test]
fn compaction_persist_round_trip_enabled() {
    compaction_persist_round_trip(CompactionOption::Enabled, false);
}

#[test]
fn compaction_persist_round_trip_enabled_shrinks_after_deletes() {
    compaction_persist_round_trip(CompactionOption::Enabled, true);
}

// ---------------------------------------------------------------------------
// The S3 streaming shape (enterprise_edition/s3.rs) — raw fd bytes under the
// `data.mdb` tarball entry name.
// ---------------------------------------------------------------------------

/// `try_clone_inner_file` streams the real data file; writing those bytes out
/// under `data.mdb` yields a directory that reopens through the adapter.
#[test]
fn streamed_inner_file_bytes_reopen_as_env() {
    use std::io::Read as _;

    let src_dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(src_dir.path()).unwrap() };
    populate(&env, 200);
    let expected = contents(&env);

    // Hold a write txn as the exclusive lock, exactly as s3.rs does.
    let wtxn = env.write_txn().unwrap();
    let mut fd = env.try_clone_inner_file().unwrap();
    let mut bytes = Vec::new();
    fd.read_to_end(&mut bytes).unwrap();
    drop(wtxn);

    let dst_dir = tempfile::tempdir().unwrap();
    fs::write(dst_dir.path().join("data.mdb"), &bytes).unwrap();
    let restored = unsafe { opts().open(dst_dir.path()).unwrap() };
    assert_eq!(contents(&restored), expected);
}
