//! The read tools on read-only media (ADR-0023 amendments 17 and 18): no lock
//! file can be created there, so `stat`, `dump` and `check` open the
//! environment read-only without one, as LMDB's tools do
//! (`OBS_RDONLY_LOCK_READONLY_FS = 0`).
//!
//! The read-only filesystem is a read-only disk image (`hdiutil`) on macOS
//! and a read-only bind mount on Linux, which needs `CAP_SYS_ADMIN`. Where
//! neither can be made, each test prints `SKIPPED: <reason>` and passes.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb::{Env, EnvOpenOptions, DATA_FILE_NAME};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A self-cleaning temporary directory.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!(
            "zerodb-rofs-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Run `cmd` to completion within 60 s; `false` on a failure exit, a spawn
/// error or the timeout (the child is then killed).
fn run_bounded(cmd: &mut Command) -> bool {
    let Ok(mut child) = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn() else {
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
    mount: PathBuf,
    _keep: TempDir,
}

impl Drop for ReadOnlyFs {
    fn drop(&mut self) {
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

/// Make a read-only filesystem with `src`'s files, or say why not.
fn read_only_fs(src: &Path) -> Result<ReadOnlyFs, String> {
    let keep = TempDir::new("mnt");
    let mount = keep.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
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

/// A closed native environment holding `live`, with no lock file
/// (`lock_file(false)`; the default would create one).
fn closed_env(dir: &Path) {
    let env: Env = EnvOpenOptions::new()
        .map_size(1 << 20)
        .lock_file(false)
        .open(dir)
        .unwrap();
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"live", b"1").unwrap();
    w.commit().unwrap();
}

/// Hex-encode as the dump format renders keys.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `stat`, `dump` and `check` work on an environment on read-only media: the
/// tools' read-only open proceeds without a lock file, and creates none.
#[test]
fn read_tools_work_on_read_only_media_without_a_lock_file() {
    let src = TempDir::new("src");
    closed_env(src.path());
    let fs = match read_only_fs(src.path()) {
        Ok(fs) => fs,
        Err(why) => {
            println!("SKIPPED: no read-only filesystem here ({why})");
            return;
        }
    };
    let dir = &fs.mount;

    let stat = zerodb_tools::commands::cmd_stat(dir).expect("stat on read-only media");
    assert!(stat.contains(DATA_FILE_NAME), "stat output: {stat}");
    let dump = zerodb_tools::commands::cmd_dump(dir).expect("dump on read-only media");
    assert!(dump.contains(&hex(b"live")), "dump output: {dump}");
    let (report, clean) = zerodb_tools::commands::cmd_check(dir).expect("check on read-only media");
    assert!(clean, "check report: {report}");
    // The attached walk's header, not the raw-file fallback's.
    assert!(report.contains("txnid="), "check report: {report}");

    let env = zerodb_tools::common::open_ro(dir, DATA_FILE_NAME).expect("open_ro");
    assert!(!env.has_lock_file());
    drop(env);
    assert!(!dir.join("zerodb.lock").exists());
}
