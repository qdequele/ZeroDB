//! `migrate-from-lmdb` — stream a real LMDB env into a fresh zerodb env
//! (M1.12, PLAN §1.12: "the one place linking C is fine"). Behind the
//! off-by-default `migrate-lmdb` feature (ADR-0001 M1.12 amendment).
//!
//! Opens the source LMDB env **read-only** via heed =0.22.1 (the Meilisearch
//! fork), enumerates the main DB and every named sub-DB, and re-writes them into
//! a fresh zerodb env through the normal write path in **batched** write txns.
//! Verifies the per-DB entry counts after the copy.
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

use zerodb::{collect_entries_flagged, EnvOpenOptions as ZEnvOpenOptions};

use crate::common::{BoxErr, MAX_TOOL_DBS};
use crate::lock;

/// Commit every `BATCH` puts so a single write txn never holds the whole DB.
const BATCH: usize = 10_000;

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
    // The destination must be fresh; hold its lock for the whole migration.
    let data = dst_dir.join(zerodb::DATA_FILE_NAME);
    if data.exists() && std::fs::metadata(&data)?.len() > 0 {
        return Err(format!(
            "refusing to migrate into a non-empty env: {} exists",
            data.display()
        )
        .into());
    }
    let _dst_guard = lock::acquire_or_create(dst_dir)?;

    // --- Source: open LMDB read-only via heed ---
    let mut opts = EnvOpenOptions::new();
    opts.max_dbs(MAX_TOOL_DBS);
    opts.map_size(map_size);
    // SAFETY: heed's `open`/`flags` are `unsafe` only because LMDB env flags can
    // enable cross-process behaviors. We set only `READ_ONLY` (single-process
    // safe) and point at an existing on-disk env we open read-only; no pointers
    // are handled here. This is the migrate feature's sole `unsafe` (plus the
    // flock in `lock.rs`); flagged for the CLAUDE.md unsafe-policy note.
    let src = unsafe {
        opts.flags(EnvFlags::READ_ONLY);
        opts.open(src_dir)?
    };
    let rtxn = src.read_txn()?;
    let main: heed::Database<Bytes, Bytes> = src
        .open_database(&rtxn, None)?
        .ok_or("source has no main database")?;

    // Partition the main DB into user data and sub-DB names.
    let mut main_user: Vec<crate::common::Kv> = Vec::new();
    let mut subdb_names: Vec<String> = Vec::new();
    for item in main.iter(&rtxn)? {
        let (k, v) = item?;
        let as_subdb = std::str::from_utf8(k).ok().and_then(|name| {
            match src.open_database::<Bytes, Bytes>(&rtxn, Some(name)) {
                Ok(Some(_)) => Some(name.to_string()),
                _ => None,
            }
        });
        match as_subdb {
            Some(name) => subdb_names.push(name),
            None => main_user.push((k.to_vec(), v.to_vec())),
        }
    }

    // --- Destination: fresh zerodb env ---
    let dst = ZEnvOpenOptions::new()
        .map_size(map_size)
        .max_dbs(MAX_TOOL_DBS)
        .page_size(page_size)
        .open(dst_dir)?;

    let mut report = String::new();
    writeln!(
        report,
        "migrate {} -> {}",
        src_dir.display(),
        dst_dir.display()
    )?;

    // Main DB user data.
    let main_written = write_db_batched(&dst, None, &main_user)?;
    writeln!(report, "  <main>: {main_written} entries")?;

    // Each named sub-DB.
    let mut subdb_counts: Vec<(String, u64)> = Vec::new();
    for name in &subdb_names {
        let sub: heed::Database<Bytes, Bytes> = src
            .open_database(&rtxn, Some(name))?
            .ok_or("sub-DB vanished")?;
        let entries: Vec<crate::common::Kv> = sub
            .iter(&rtxn)?
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()?;
        let n = write_db_batched(&dst, Some(name.as_bytes()), &entries)?;
        writeln!(report, "  {name}: {n} entries")?;
        subdb_counts.push((name.clone(), n));
    }

    // --- Verify per-DB counts ---
    verify_counts(&dst, main_user.len() as u64, &subdb_counts)?;
    writeln!(report, "  verified: all per-DB entry counts match")?;
    Ok(report)
}

/// Write `entries` into `name` (or the main DB) via the zerodb write path,
/// committing every [`BATCH`]. Creates a named DB even when empty so its catalog
/// entry exists.
fn write_db_batched(
    dst: &zerodb::Env,
    name: Option<&[u8]>,
    entries: &[crate::common::Kv],
) -> Result<u64, BoxErr> {
    // Ensure a named DB exists (empty or not).
    if let Some(n) = name {
        let mut wtxn = dst.write_txn()?;
        dst.create_database(&mut wtxn, Some(n))?;
        wtxn.commit()?;
    }
    let mut written = 0u64;
    for chunk in entries.chunks(BATCH) {
        let mut wtxn = dst.write_txn()?;
        let db = match name {
            None => dst.main_database(),
            Some(n) => dst
                .open_database(&wtxn, Some(n))?
                .ok_or("just-created DB missing")?,
        };
        for (k, v) in chunk {
            db.put(&mut wtxn, k, v)?;
            written += 1;
        }
        wtxn.commit()?;
    }
    Ok(written)
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
