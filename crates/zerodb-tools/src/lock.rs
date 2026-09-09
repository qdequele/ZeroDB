//! Best-effort exclusive lock on an env's data file (PLAN §1.12, D-001).
//!
//! ZeroDB has **no cross-process reader protocol** (D-001: single-process by
//! design, no lock file). So a tool pointed at a *live* env would read a
//! concurrently-mutated file. The tools therefore **require the env to be
//! closed** and detect liveness only **best-effort**: they take an advisory
//! `flock(LOCK_EX | LOCK_NB)` on the env's data file and refuse if it is already
//! held. This detects *another tool invocation* (or any process that flocks the
//! file); it does **not** detect the engine itself, which takes no flock — the
//! documented contract is "run tools offline".
//!
//! ## Unsafe policy note
//!
//! `std` has no stable file-locking API and the `fs2`/`fs4` crates are not on
//! the allowlist, so the lock is one `libc::flock` FFI call. `libc` **is** on
//! the CLAUDE.md allowlist, but that unsafe-policy currently sanctions `unsafe`
//! only in `zerodb-core::{page,readers}`, `zerodb-io`, and `zerodb-oracle`.
//! This single, SAFETY-commented block **expands** that policy to
//! `zerodb-tools` — recorded in CLAUDE.md on 2026-09-09 as "in use, not yet
//! ratified"; a human still has to sanction or remove it.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use zerodb::DATA_FILE_NAME;

use crate::naming::{self, NameError};

/// A held advisory lock. Dropping it closes the fd, which releases the
/// `flock` (POSIX semantics: the lock is released when the last descriptor for
/// the open file description is closed).
#[derive(Debug)]
pub struct EnvLock {
    _file: File,
}

/// Why acquiring the env lock failed.
#[derive(Debug)]
pub enum LockError {
    /// The data file does not exist under either candidate name (no env here).
    Missing(std::path::PathBuf),
    /// The lock is already held — the env may be live or another tool is running.
    Busy(std::path::PathBuf),
    /// The directory holds **both** candidate data-file names (ADR-0010).
    Ambiguous(std::path::PathBuf),
    /// An I/O error opening or locking the file.
    Io(io::Error),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Missing(p) => {
                write!(f, "{}", NameError::Missing(p.clone()))
            }
            LockError::Ambiguous(p) => {
                write!(f, "{}", NameError::Ambiguous(p.clone()))
            }
            LockError::Busy(p) => write!(
                f,
                "env at {} is locked (in use by a live process or another tool); \
                 tools require the env to be closed",
                p.display()
            ),
            LockError::Io(e) => write!(f, "lock I/O error: {e}"),
        }
    }
}

impl std::error::Error for LockError {}

/// Take an exclusive advisory lock on an existing env's data file (read tools:
/// `stat`/`dump`/`check`), probing **both** candidate names (ADR-0010). Returns
/// the lock together with the resolved data file so the caller opens the same
/// one that is locked.
///
/// # Errors
///
/// [`LockError::Missing`] if the env has no data file under either name,
/// [`LockError::Ambiguous`] if it has both, [`LockError::Busy`] if the lock is
/// held, [`LockError::Io`] on other I/O errors.
pub fn acquire_existing(env_dir: &Path) -> Result<(EnvLock, naming::EnvDataFile), LockError> {
    let resolved = naming::resolve(env_dir).map_err(|e| match e {
        NameError::Missing(p) => LockError::Missing(p),
        NameError::Ambiguous(p) => LockError::Ambiguous(p),
    })?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&resolved.path)
        .map_err(LockError::Io)?;
    let guard = lock_or_busy(file, env_dir)?;
    Ok((guard, resolved))
}

/// Take an exclusive advisory lock on a (possibly not-yet-existing) data file,
/// creating it if absent (write tools: `load`/`migrate` targeting a fresh dir).
///
/// Write tools create **native** envs, so this always uses
/// [`zerodb::DATA_FILE_NAME`] — no probing. Callers must already have refused a
/// directory holding an existing env under either name
/// (`crate::naming::existing_data_files`), so this never creates a second data
/// file next to a live one (ADR-0010).
///
/// # Errors
///
/// [`LockError::Busy`] if the lock is held, [`LockError::Io`] on other I/O errors.
pub fn acquire_or_create(env_dir: &Path) -> Result<EnvLock, LockError> {
    let data = env_dir.join(DATA_FILE_NAME);
    // `mode(0o600)`: when this call *creates* the data file (fresh load /
    // migrate target) it must be owner-only, matching the engine's own
    // creation mode — a default-mode store leaks data via umask. An existing
    // file keeps its mode (the mode applies at creation only).
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&data)
            .map_err(LockError::Io)?
    };
    lock_or_busy(file, env_dir)
}

fn lock_or_busy(file: File, env_dir: &Path) -> Result<EnvLock, LockError> {
    match try_flock_exclusive(&file) {
        Ok(true) => Ok(EnvLock { _file: file }),
        Ok(false) => Err(LockError::Busy(env_dir.to_path_buf())),
        Err(e) => Err(LockError::Io(e)),
    }
}

/// `flock(fd, LOCK_EX | LOCK_NB)`. Returns `Ok(true)` if the lock was acquired,
/// `Ok(false)` if it is already held (`EWOULDBLOCK`), or an I/O error otherwise.
#[cfg(unix)]
fn try_flock_exclusive(file: &File) -> io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `flock` is a pure syscall taking a valid open file descriptor and
    // an int flag; it has no memory-safety preconditions. `fd` is borrowed from
    // `file`, which outlives this call, so the descriptor is valid for the
    // duration. We read the result and (on -1) the thread-local `errno` via the
    // safe `io::Error::last_os_error()` wrapper; no pointers are dereferenced.
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // POSIX: a non-blocking flock that would block returns EWOULDBLOCK
        // (== EAGAIN on Linux/macOS).
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => Ok(false),
        _ => Err(err),
    }
}

#[cfg(not(unix))]
fn try_flock_exclusive(_file: &File) -> io::Result<bool> {
    // Non-unix is not a target platform (linux-aarch64 primary; linux-x86_64 /
    // macOS aarch64 for dev). Treat as "acquired" so tools still run, without a
    // liveness guard.
    Ok(true)
}
