//! Shared helpers for the read tools: attach to an env read-only and collect
//! its logical content.

use std::path::Path;

use zerodb::{
    collect_entries_flagged, named_databases, Database, Env, EnvFlags, EnvOpenOptions, MdbError,
    RoTxn,
};

use crate::dump_format::DumpDb;

/// A generous catalog capacity for tools: an env may hold many named DBs and
/// the tool does not know the count in advance (milli uses ~20). Opening a named
/// DB assigns a dbi slot bounded by this.
pub const MAX_TOOL_DBS: u32 = 1 << 16;

/// A boxed error for the tool layer (no `anyhow` dependency).
pub type BoxErr = Box<dyn std::error::Error>;

/// An owned key/value pair.
pub type Kv = (Vec<u8>, Vec<u8>);

/// A named database's name paired with its entries (tools collect DBs this way).
pub type NamedDb = (Vec<u8>, Vec<Kv>);

/// The options a tool opens an existing environment with: `READ_ONLY`, the
/// data file `data_file_name`, and the lock file on, under the name derived
/// from the data file ([`zerodb::derived_lock_file_name`]), so the tool joins
/// the environment as any other process does (ADR-0023 amendment 18).
///
/// `max_readers` is the value the environment's lock file records, if any;
/// otherwise the engine default. A tool that joins a running environment
/// adopts that environment's count anyway (ADR-0023 §1 as amended); reading
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
/// without one, as LMDB's tools do (ADR-0023 amendment 17).
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
/// LMDB's tools do: adopt the newest map size and retry once (ADR-0023
/// amendment 18).
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

/// Collect every DB's logical content under a single read snapshot: the main DB
/// (user entries only — `F_SUBDATA` catalog records are followed into their own
/// sections) followed by every named DB in name order.
///
/// # Errors
///
/// Propagates read errors (a corrupt tree surfaces as [`zerodb::Error`]).
pub fn collect_dump(env: &Env, txn: &RoTxn<'_>) -> Result<Vec<DumpDb>, BoxErr> {
    let mut dbs = Vec::new();

    // Main DB user data (exclude catalog records).
    let main = env.main_database();
    let main_entries: Vec<(Vec<u8>, Vec<u8>)> = collect_entries_flagged(&main, txn)?
        .into_iter()
        .filter(|(_, flags, _)| flags & zerodb_core_f_subdata() == 0)
        .map(|(k, _, v)| (k, v))
        .collect();
    dbs.push(DumpDb {
        name: None,
        entries: main_entries,
    });

    // Named DBs, in name order.
    for name in named_databases(txn)? {
        let dbh: Database = env
            .open_database(txn, Some(&name))?
            .ok_or("named DB vanished between enumeration and open")?;
        let entries: Vec<(Vec<u8>, Vec<u8>)> = collect_entries_flagged(&dbh, txn)?
            .into_iter()
            .map(|(k, _, v)| (k, v))
            .collect();
        dbs.push(DumpDb {
            name: Some(name),
            entries,
        });
    }
    Ok(dbs)
}

/// The `F_SUBDATA` leaf-node flag bit (SPEC 02 §6). Re-exposed locally to avoid
/// a direct `zerodb-core::page` path in every caller.
#[inline]
fn zerodb_core_f_subdata() -> u16 {
    zerodb_core::page::F_SUBDATA
}
