//! ADR-0010 — the env directory's data-file name at the heed boundary.
//!
//! Meilisearch joins the literal `"data.mdb"` onto env paths in **production**
//! code: index compaction (`process_batch.rs`, `routes/tasks/compact.rs`,
//! `meilitool`) and snapshot creation (`process_snapshot_creation.rs`,
//! `enterprise_edition/s3.rs`). With any other file name, compaction fails on
//! the `fs::metadata` probe and snapshot restore *silently* reopens as a fresh
//! empty env — data loss that index-scheduler's own suite would not catch, as
//! it has no compaction or snapshot round-trip test. The round-trip tests
//! below are exactly those shapes.

// heed 0.22.1's API; the `unique-txns` feature replaces it with the heed
// fork's (ADR-0027), covered by `unique_txns.rs`.
#![cfg(not(feature = "unique-txns"))]

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
// Adapter naming
// ---------------------------------------------------------------------------

/// A fresh env dir created through the adapter contains **exactly**
/// `["data.mdb", "lock.mdb"]`, as an LMDB env dir does: no `zerodb.dat`, and
/// the lock file is on by default (ADR-0023).
#[test]
fn adapter_env_dir_contains_exactly_data_mdb_and_lock_mdb() {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(dir.path()).unwrap() };
    populate(&env, 32);

    assert_eq!(
        dir_listing(dir.path()),
        vec!["data.mdb".to_string(), "lock.mdb".to_string()]
    );
    assert_eq!(heed_zerodb::DATA_FILE_NAME, "data.mdb");
    // `Env::path()` returns the *directory*, not the data file — heed
    // parity. (Compared canonicalized: the engine
    // canonicalizes for its registry key, and macOS temp dirs are symlinked.)
    assert_eq!(env.path(), dir.path().canonicalize().unwrap());
}

/// The compaction call sites' very first step: `fs::metadata` on the joined
/// path must not ENOENT.
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
// Snapshot round-trip (process_snapshot_creation.rs shape)
// ---------------------------------------------------------------------------

/// `copy_to_path(dst.join("data.mdb"), option)` → reopen `dst` through the
/// adapter → full logical equality. Run for **both** compaction options.
/// Regression: a mismatched file name makes the restore silently yield an
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

    // The snapshot dir looks like an LMDB env dir after `mdb_env_copy`: the
    // data file only; the lock file appears when the env is opened.
    assert_eq!(dir_listing(dst_dir.path()), vec!["data.mdb".to_string()]);

    // Restore: extract, open the directory — must yield the snapshot's
    // contents, not a fresh EMPTY env.
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
// Compaction-persist round-trip (process_batch.rs shape)
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

    // Step 1 — the `fs::metadata` probe.
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
    assert_eq!(
        dir_listing(dir.path()),
        vec!["data.mdb".to_string(), "lock.mdb".to_string()]
    );

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

// ---------------------------------------------------------------------------
// NO_LOCK (ADR-0023 amendment 17)
// ---------------------------------------------------------------------------

/// heed's `NO_LOCK` maps to the engine's `lock_file(false)` and turns off the
/// lock file the adapter has by default: `NO_LOCK` leaves no `lock.mdb` in
/// the env dir (`OBS_NOLOCK_CREATES_LOCK_FILE = false`), while the same
/// options without `NO_LOCK` create one.
#[test]
fn no_lock_creates_no_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    // SAFETY: heed signature parity only; setting flags performs no unsafe
    // operation in the adapter.
    unsafe { o.flags(heed_zerodb::EnvFlags::NO_LOCK) };
    let env = unsafe { o.open(dir.path()).unwrap() };
    populate(&env, 8);
    assert_eq!(dir_listing(dir.path()), vec!["data.mdb".to_string()]);
    drop(env);

    let locked = tempfile::tempdir().unwrap();
    let o = opts();
    let env = unsafe { o.open(locked.path()).unwrap() };
    populate(&env, 8);
    assert_eq!(
        dir_listing(locked.path()),
        vec!["data.mdb".to_string(), "lock.mdb".to_string()]
    );
}

/// `Env::flags()` reports `NO_LOCK` for an env opened with it, and not for
/// one opened without it (which has the default lock file), as
/// `mdb_env_get_flags` does.
#[test]
fn flags_report_no_lock() {
    use heed_zerodb::EnvFlags;
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    // SAFETY: as in `no_lock_creates_no_lock_file`.
    unsafe { o.flags(EnvFlags::NO_LOCK | EnvFlags::NO_SYNC) };
    let env = unsafe { o.open(dir.path()).unwrap() };
    let flags = env.flags().unwrap().unwrap();
    assert!(flags.contains(EnvFlags::NO_LOCK), "{flags:?}");
    assert!(flags.contains(EnvFlags::NO_SYNC), "{flags:?}");
    assert_ne!(env.get_flags().unwrap() & EnvFlags::NO_LOCK.bits(), 0);
    drop(env);

    let other = tempfile::tempdir().unwrap();
    let env = unsafe { opts().open(other.path()).unwrap() };
    assert!(other.path().join("lock.mdb").exists());
    let flags = env.flags().unwrap().unwrap();
    assert!(!flags.contains(EnvFlags::NO_LOCK), "{flags:?}");
    assert_eq!(env.get_flags().unwrap() & EnvFlags::NO_LOCK.bits(), 0);
}

// ---------------------------------------------------------------------------
// Read-only media (ADR-0023 amendment 17)
// ---------------------------------------------------------------------------

/// Run `cmd` to completion within 60 s; `false` on a failure exit, a spawn
/// error or the timeout (the child is then killed).
fn run_bounded(cmd: &mut std::process::Command) -> bool {
    use std::time::{Duration, Instant};
    let Ok(mut child) = cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    else {
        return false;
    };
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        match child.try_wait() {
            Ok(Some(st)) => return st.success(),
            Ok(None) if Instant::now() < end => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

/// A read-only filesystem holding a copy of a directory, unmounted on drop.
struct ReadOnlyFs {
    mount: std::path::PathBuf,
    _keep: tempfile::TempDir,
}

impl Drop for ReadOnlyFs {
    fn drop(&mut self) {
        use std::process::Command;
        if cfg!(target_os = "macos") {
            run_bounded(
                Command::new("hdiutil")
                    .arg("detach")
                    .arg("-force")
                    .arg(&self.mount),
            );
        } else {
            run_bounded(Command::new("umount").arg(&self.mount));
        }
    }
}

/// Make a read-only filesystem with `src`'s files, or say why not: a
/// read-only disk image on macOS (`hdiutil`), a read-only bind mount on
/// Linux (needs `CAP_SYS_ADMIN`, for example a privileged container).
fn read_only_fs(src: &std::path::Path) -> Result<ReadOnlyFs, String> {
    use std::process::Command;
    let keep = tempfile::tempdir().unwrap();
    let mount = keep.path().join("mnt");
    fs::create_dir(&mount).unwrap();
    if cfg!(target_os = "macos") {
        let img = keep.path().join("ro.dmg");
        if !run_bounded(
            Command::new("hdiutil")
                .args(["create", "-quiet", "-format", "UDRO", "-srcfolder"])
                .arg(src)
                .arg(&img),
        ) {
            return Err("hdiutil create failed".into());
        }
        if !run_bounded(
            Command::new("hdiutil")
                .args(["attach", "-quiet", "-readonly", "-nobrowse", "-mountpoint"])
                .arg(&mount)
                .arg(&img),
        ) {
            return Err("hdiutil attach failed".into());
        }
    } else {
        if !run_bounded(Command::new("mount").arg("--bind").arg(src).arg(&mount)) {
            return Err("bind mount failed (needs CAP_SYS_ADMIN)".into());
        }
        if !run_bounded(
            Command::new("mount")
                .args(["-o", "remount,bind,ro"])
                .arg(&mount),
        ) {
            run_bounded(Command::new("umount").arg(&mount));
            return Err("read-only remount failed".into());
        }
    }
    Ok(ReadOnlyFs { mount, _keep: keep })
}

/// The one way an adapter env runs without a lock file and without
/// `NO_LOCK`: a `READ_ONLY` open on read-only media, where no `lock.mdb`
/// can be created (`OBS_RDONLY_LOCK_READONLY_FS = 0`). It opens and reads,
/// creates no lock file, and `flags()` does **not** report `NO_LOCK`
/// (`OBS_RDONLY_READONLY_FS_REPORTS_NOLOCK = false`). The adapter-level
/// counterpart of the engine's
/// `multi_process::read_only_env_on_read_only_media_opens_without_a_lock_file`.
#[test]
fn read_only_media_env_has_no_lock_file_and_does_not_report_no_lock() {
    use heed_zerodb::EnvFlags;
    let src = tempfile::tempdir().unwrap();
    {
        let mut o = opts();
        // SAFETY: heed signature parity only; private temp dir, one process.
        // `NO_LOCK` here only keeps `lock.mdb` out of the read-only copy.
        unsafe { o.flags(EnvFlags::NO_LOCK) };
        let env = unsafe { o.open(src.path()).unwrap() };
        populate(&env, 8);
        env.prepare_for_closing().wait();
    }
    assert_eq!(dir_listing(src.path()), vec!["data.mdb".to_string()]);
    let ro = match read_only_fs(src.path()) {
        Ok(fs) => fs,
        Err(why) => {
            println!("SKIPPED: no read-only filesystem here ({why})");
            return;
        }
    };
    {
        let mut o = opts();
        // SAFETY: as above.
        unsafe { o.flags(EnvFlags::READ_ONLY) };
        let env = unsafe { o.open(&ro.mount) }.expect("READ_ONLY on read-only media");
        let flags = env.flags().unwrap().unwrap();
        assert!(!flags.contains(EnvFlags::NO_LOCK), "{flags:?}");
        assert_eq!(env.get_flags().unwrap() & EnvFlags::NO_LOCK.bits(), 0);
        let rtxn = env.read_txn().unwrap();
        let main: Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();
        assert!(main.get(&rtxn, b"key-000000").unwrap().is_some());
    }
    assert!(!ro.mount.join("lock.mdb").exists());
}
