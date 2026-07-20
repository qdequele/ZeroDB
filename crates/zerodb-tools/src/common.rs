//! Shared helpers for the read tools: open an env read-only and collect its
//! logical content (M1.12).

use zerodb::{
    collect_entries_flagged, named_databases, Database, Env, EnvFlags, EnvOpenOptions, RoTxn,
};

use crate::dump_format::DumpDb;

/// A generous catalog capacity for tools: an env may hold many named DBs and
/// the tool does not know the count in advance (milli uses ~20). Opening a named
/// DB assigns a dbi slot bounded by this.
pub const MAX_TOOL_DBS: u32 = 1 << 16;

/// A boxed error for the tool layer (no `anyhow` dependency — not on the
/// allowlist).
pub type BoxErr = Box<dyn std::error::Error>;

/// An owned key/value pair.
pub type Kv = (Vec<u8>, Vec<u8>);

/// A named database's name paired with its entries (tools collect DBs this way).
pub type NamedDb = (Vec<u8>, Vec<Kv>);

/// Open an existing env **read-only** for a tool (SPEC 01 `MDB_RDONLY`). The
/// persisted page size wins, so the builder default here is irrelevant.
///
/// `data_file_name` is the name resolved by the ADR-0010 two-name probe
/// (`crate::naming::resolve`) — the engine itself never guesses, so the tool
/// must tell it which file the directory actually holds.
///
/// # Errors
///
/// Propagates [`zerodb::Error`] (missing dir, corrupt file, already-open).
pub fn open_ro(env_dir: &std::path::Path, data_file_name: &str) -> Result<Env, BoxErr> {
    let env = EnvOpenOptions::new()
        .max_dbs(MAX_TOOL_DBS)
        .flags(EnvFlags::READ_ONLY)
        .data_file_name(data_file_name)
        .open(env_dir)?;
    Ok(env)
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
