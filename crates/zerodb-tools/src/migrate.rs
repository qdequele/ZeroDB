//! `migrate-from-lmdb` — stream a real LMDB env into a fresh zerodb env (the
//! one place outside the oracle where linking C is accepted). Behind the
//! off-by-default `migrate-lmdb` feature (ADR-0001).
//!
//! Opens the source LMDB env **read-only** via heed =0.22.1 (the Meilisearch
//! fork), enumerates the main DB and every named sub-DB, and writes their
//! records into a fresh zerodb env as they are read, in write txns of about
//! 64 MiB each that grow the map when it fills (as `load` does). Verifies the
//! per-DB entry counts after the copy.
//!
//! ### Enumerating LMDB sub-databases (generic, no a-priori name list)
//!
//! LMDB stores named sub-DBs as entries in the unnamed (main) DB, flagged
//! `F_SUBDATA` — but heed's typed iteration does not expose that flag. So we
//! iterate the main DB and, for each key, *try to open it as a sub-database*:
//! `mdb_dbi_open` succeeds (`Ok(Some)`) for a real sub-DB and returns
//! `MDB_INCOMPATIBLE` for a plain user key. That cleanly partitions the main DB
//! into user data and sub-DB pointers without needing the caller to name the
//! sub-DBs.

use std::fmt::Write as _;
use std::path::Path;

use heed::types::Bytes;
use heed::{EnvFlags, EnvOpenOptions};

use zerodb::collect_entries_flagged;

use crate::commands::{DbWriter, BATCH_BYTES};
use crate::common::{BoxErr, MAX_TOOL_DBS};

/// Open the source LMDB env read-only and stream it into a fresh zerodb env at
/// `dst_dir`. Returns a human-readable summary.
///
/// # Errors
///
/// Propagates heed/zerodb errors, a lock error, or a per-DB count mismatch.
pub fn cmd_migrate(
    src_dir: &Path,
    dst_dir: &Path,
    map_size: usize,
    page_size: u32,
) -> Result<String, BoxErr> {
    // The source must actually be an LMDB env. A ZeroDB env can *also* be
    // named `data.mdb` (adapter-created, ADR-0010), so the file name does not
    // discriminate — the magic does (SPEC 02 §3). Check it up front and say so
    // explicitly, rather than letting heed return an opaque MDB_INVALID.
    let src_data = src_dir.join(zerodb::HEED_DATA_FILE_NAME);
    if src_data.exists() && crate::naming::is_zerodb_image(&src_data)? {
        return Err(format!(
            "{} is a ZeroDB env, not an LMDB one: it carries the ZDB1 magic (SPEC 02 §3) even \
             though it is named {}, because envs created through the heed-zerodb adapter use \
             LMDB's file name (ADR-0010). There is nothing to migrate — open it directly.",
            src_data.display(),
            zerodb::HEED_DATA_FILE_NAME
        )
        .into());
    }
    // The destination must be fresh under either name, and opened by this
    // process alone; the `Env` is held for the whole migration (ADR-0023).
    std::fs::create_dir_all(dst_dir)?;
    crate::commands::refuse_existing_env(dst_dir, "migrate into")?;
    let dst =
        crate::commands::open_exclusive_destination(dst_dir, page_size, map_size, "migrate into")?;

    // --- Source: open LMDB read-only via heed ---
    let mut opts = EnvOpenOptions::new();
    opts.max_dbs(MAX_TOOL_DBS);
    opts.map_size(map_size);
    // SAFETY: heed's `open`/`flags` are `unsafe` only because LMDB env flags can
    // enable cross-process behaviors. We set only `READ_ONLY` (single-process
    // safe) and point at an existing on-disk env we open read-only; no pointers
    // are handled here. This is the migrate feature's sole `unsafe`.
    let src = unsafe {
        opts.flags(EnvFlags::READ_ONLY);
        opts.open(src_dir)?
    };
    let rtxn = src.read_txn()?;
    let main: heed::Database<Bytes, Bytes> = src
        .open_database(&rtxn, None)?
        .ok_or("source has no main database")?;

    let mut report = String::new();
    writeln!(
        report,
        "migrate {} -> {}",
        src_dir.display(),
        dst_dir.display()
    )?;

    // The main DB's user records, written as they are read. Its other keys
    // name the sub-DBs, copied next.
    let mut db = DbWriter::new(&dst, None, BATCH_BYTES);
    let mut subdb_names: Vec<String> = Vec::new();
    for item in main.iter(&rtxn)? {
        let (k, v) = item?;
        match subdb_name(&src, &rtxn, k) {
            Some(name) => subdb_names.push(name),
            None => db.push(k, v)?,
        }
    }
    let main_written = db.finish()?;
    writeln!(report, "  <main>: {main_written} entries")?;

    // Each named sub-DB.
    let mut subdb_counts: Vec<(String, u64)> = Vec::new();
    for name in &subdb_names {
        let sub: heed::Database<Bytes, Bytes> = src
            .open_database(&rtxn, Some(name))?
            .ok_or("sub-DB vanished")?;
        let mut db = DbWriter::new(&dst, Some(name.as_bytes()), BATCH_BYTES);
        for item in sub.iter(&rtxn)? {
            let (k, v) = item?;
            db.push(k, v)?;
        }
        let n = db.finish()?;
        writeln!(report, "  {name}: {n} entries")?;
        subdb_counts.push((name.clone(), n));
    }

    // --- Verify per-DB counts ---
    verify_counts(&dst, main_written, &subdb_counts)?;
    writeln!(report, "  verified: all per-DB entry counts match")?;
    Ok(report)
}

/// `Some(name)` if the main-DB key `key` names a sub-database of `src`: it
/// is UTF-8 and opens as a database (a plain user key fails with
/// `MDB_INCOMPATIBLE`).
fn subdb_name(src: &heed::Env, rtxn: &heed::RoTxn, key: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(key).ok()?;
    match src.open_database::<Bytes, Bytes>(rtxn, Some(name)) {
        Ok(Some(_)) => Some(name.to_string()),
        _ => None,
    }
}

/// Verify the migrated env's per-DB entry counts against the source counts.
fn verify_counts(
    dst: &zerodb::Env,
    main_expected: u64,
    subdbs: &[(String, u64)],
) -> Result<(), BoxErr> {
    let rtxn = dst.read_txn()?;
    // Main: count user (non-catalog) entries.
    let main_got = collect_entries_flagged(&dst.main_database(), &rtxn)?
        .into_iter()
        .filter(|(_, flags, _)| flags & zerodb_core::page::F_SUBDATA == 0)
        .count() as u64;
    if main_got != main_expected {
        return Err(
            format!("main DB count mismatch: source {main_expected}, migrated {main_got}").into(),
        );
    }
    for (name, expected) in subdbs {
        let db = dst
            .open_database(&rtxn, Some(name.as_bytes()))?
            .ok_or_else(|| format!("migrated DB {name} missing"))?;
        let got = db.len(&rtxn)?;
        if got != *expected {
            return Err(
                format!("DB {name} count mismatch: source {expected}, migrated {got}").into(),
            );
        }
    }
    Ok(())
}
