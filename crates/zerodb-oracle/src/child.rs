//! Multi-process tests: a test binary re-runs itself as a child process.
//!
//! The child is the same test binary, started with `--exact <path>::child_entry`
//! and a few environment variables that tell `child_entry` which role to play.
//! When those variables are absent, `child_entry` returns at once, so it passes
//! as an ordinary test in the parent run.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// A child process, killed and reaped on drop so a failing test never leaks
/// one.
pub struct ChildGuard(pub Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Re-run the current test binary as a child that runs only the
/// `child_entry` test of the module `module_path` (pass `module_path!()`),
/// with `envs` set.
pub fn spawn_self(module_path: &str, envs: &[(&str, &OsStr)]) -> ChildGuard {
    // libtest names a test by its module path without the crate name.
    let test = match module_path.split_once("::") {
        Some((_, module)) => format!("{module}::child_entry"),
        None => "child_entry".to_string(),
    };
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", &test, "--nocapture", "--test-threads=1"]);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    ChildGuard(cmd.spawn().unwrap())
}

/// Wait (30 s at most) until `path` exists: the marker file a child writes
/// once it reached the state the parent observes.
pub fn wait_for(path: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            Instant::now() < end,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Wait (`timeout` at most) for `c` to exit, and require a success status.
pub fn wait_success(c: &mut ChildGuard, role: &str, timeout: Duration) {
    let end = Instant::now() + timeout;
    loop {
        if let Some(st) = c.0.try_wait().unwrap() {
            assert!(st.success(), "child {role} failed: {st}");
            return;
        }
        assert!(
            Instant::now() < end,
            "timed out waiting for child {role} to exit"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Spawn a child with `spawn`, wait for its `held` marker in `dir`, SIGKILL
/// it and remove the marker. Returns its PID.
pub fn kill_when_held(dir: &Path, spawn: impl FnOnce() -> ChildGuard) -> u32 {
    let mut c = spawn();
    wait_for(&dir.join("held"));
    let pid = c.0.id();
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    std::fs::remove_file(dir.join("held")).unwrap();
    pid
}
