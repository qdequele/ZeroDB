//! A tool pointed at a locked (simulated-live) env refuses cleanly (M1.12
//! acceptance §7c, PLAN §1.12): "a tool invoked on a locked live env fails
//! cleanly".
#![cfg(unix)]

use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Env, EnvOpenOptions, DATA_FILE_NAME};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-lock-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn build_empty_env(dir: &std::path::Path) {
    let env: Env = EnvOpenOptions::new().map_size(1 << 20).open(dir).unwrap();
    let mut wtxn = env.write_txn().unwrap();
    env.main_database().put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();
}

/// Hold an exclusive flock on the data file, simulating a live holder, and
/// assert every read tool refuses with a clear "locked" error.
#[test]
fn tools_refuse_a_locked_env() {
    let dir = tmp_dir("busy");
    build_empty_env(&dir);

    // Take a competing exclusive lock (as another process/tool would).
    let data = dir.join(DATA_FILE_NAME);
    let holder = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&data)
        .unwrap();
    let fd = holder.as_raw_fd();
    // SAFETY: flock is a pure syscall on a valid borrowed fd; `holder` outlives
    // the call. Test-only.
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(rc, 0, "test failed to take the competing lock");

    // stat / dump / check must all refuse while the lock is held.
    let stat_err = zerodb_tools::commands::cmd_stat(&dir).unwrap_err();
    assert!(
        stat_err.to_string().contains("locked"),
        "stat error was: {stat_err}"
    );
    let dump_err = zerodb_tools::commands::cmd_dump(&dir).unwrap_err();
    assert!(dump_err.to_string().contains("locked"));
    let check_err = zerodb_tools::commands::cmd_check(&dir).unwrap_err();
    assert!(check_err.to_string().contains("locked"));

    // Releasing the lock lets the tools run again.
    drop(holder);
    assert!(zerodb_tools::commands::cmd_stat(&dir).is_ok());
}

/// A tool on a missing env fails cleanly (not a panic).
#[test]
fn tools_refuse_a_missing_env() {
    let dir = tmp_dir("missing");
    let err = zerodb_tools::commands::cmd_stat(&dir).unwrap_err();
    assert!(err.to_string().contains("not found"), "err: {err}");
}
