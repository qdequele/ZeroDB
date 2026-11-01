//! Shared helpers for the read tools: attach to an env read-only.

use std::path::Path;

use zerodb::{Env, EnvFlags, EnvOpenOptions, MdbError, RoTxn};

/// A generous catalog capacity for tools: an env may hold many named DBs and
/// the tool does not know the count in advance (milli uses ~20). Opening a named
/// DB assigns a dbi slot bounded by this.
pub const MAX_TOOL_DBS: u32 = 1 << 16;

/// A boxed error for the tool layer (no `anyhow` dependency).
pub type BoxErr = Box<dyn std::error::Error>;

/// The options a tool opens an existing environment with: `READ_ONLY`, the
/// data file `data_file_name`, and the lock file on, under the name derived
/// from the data file ([`zerodb::derived_lock_file_name`]), so the tool joins
/// the environment as any other process does (ADR-0023).
///
/// `max_readers` is the value the environment's lock file records, if any;
/// otherwise the engine default. A tool that joins a running environment
/// adopts that environment's count anyway (ADR-0023 §1); reading
/// it first keeps the count when every other process has closed and the
/// tool's open reinitializes the lock file.
///
/// # Errors
///
/// [`zerodb_io::lockfile::LockOpenError`] if the lock file cannot be read,
/// or this process already has it open.
pub fn tool_open_options(env_dir: &Path, data_file_name: &str) -> Result<EnvOpenOptions, BoxErr> {
    let mut opts = EnvOpenOptions::new();
    opts.max_dbs(MAX_TOOL_DBS)
        .flags(EnvFlags::READ_ONLY)
        .data_file_name(data_file_name)
        .lock_file(true);
    if let Some(n) = recorded_max_readers(env_dir, &opts)? {
        opts.max_readers(n);
    }
    Ok(opts)
}

/// The `max_readers` recorded in the lock file `opts` would open in
/// `env_dir`, if that file is a finished lock file.
fn recorded_max_readers(env_dir: &Path, opts: &EnvOpenOptions) -> Result<Option<u32>, BoxErr> {
    let path = env_dir.join(opts.get_lock_file_name());
    match zerodb_io::lockfile::published_max_readers(&path) {
        Ok(n) => Ok(n),
        Err(zerodb_io::lockfile::LockOpenError::AlreadyOpen) => {
            Err(Box::new(zerodb::Error::EnvAlreadyOpened))
        }
        Err(e) => Err(Box::new(e)),
    }
}

/// Open an existing env **read-only** for a tool (SPEC 01 `MDB_RDONLY`),
/// attached through its lock file (see [`tool_open_options`]). The persisted
/// page size wins, so the builder default is irrelevant.
///
/// `data_file_name` is the name resolved by the ADR-0010 two-name probe
/// (`crate::naming::resolve`) — the engine itself never guesses, so the tool
/// must tell it which file the directory actually holds.
///
/// On read-only media no lock file can be created, and the open proceeds
/// without one, as LMDB's tools do (ADR-0023).
///
/// # Errors
///
/// Propagates [`zerodb::Error`] (missing dir, corrupt file, already-open).
pub fn open_ro(env_dir: &Path, data_file_name: &str) -> Result<Env, zerodb::Error> {
    let to_zerodb = |e: BoxErr| match e.downcast::<zerodb::Error>() {
        Ok(e) => *e,
        Err(e) => zerodb::Error::Io(std::io::Error::other(e.to_string())),
    };
    tool_open_options(env_dir, data_file_name)
        .map_err(to_zerodb)?
        .open(env_dir)
}

/// Begin a read txn on a tool's env. A [`MdbError::MapResized`] (another
/// process grew the environment past this process's map) is answered as
/// LMDB's tools do: adopt the newest map size and retry once (ADR-0023).
///
/// # Errors
///
/// The read txn's error, or the resize's.
pub fn read_txn(env: &Env) -> Result<RoTxn<'_>, zerodb::Error> {
    match env.read_txn() {
        Err(zerodb::Error::Mdb(MdbError::MapResized)) => {
            env.resize(0)?;
            env.read_txn()
        }
        r => r,
    }
}
