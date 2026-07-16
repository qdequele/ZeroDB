//! `stat`, `dump`, `load`, `check` subcommand implementations (M1.12).

use std::fmt::Write as _;
use std::fs::File;
use std::path::Path;

use zerodb::{
    build_multi_db_image, check::check_image, NamedDbData, DATA_FILE_NAME, DEFAULT_FILL_PERMILLE,
};

use crate::common::{collect_dump, open_ro, BoxErr};
use crate::dump_format;
use crate::lock;

/// `stat <env-dir>`: env-level and per-DB statistics. Offline (exclusive lock).
///
/// # Errors
///
/// [`lock::LockError`] if the env is busy/missing, or any read error.
pub fn cmd_stat(env_dir: &Path) -> Result<String, BoxErr> {
    let _guard = lock::acquire_existing(env_dir)?;
    let env = open_ro(env_dir)?;
    let txn = env.read_txn()?;
    let snap = txn.snapshot();
    let psize = env.page_size();

    let mut out = String::new();
    writeln!(out, "env: {}", env_dir.display())?;
    writeln!(out, "  page_size:            {psize}")?;
    writeln!(out, "  map_size:             {}", env.map_size())?;
    writeln!(out, "  real_disk_size:       {}", env.real_disk_size()?)?;
    writeln!(
        out,
        "  non_free_pages_size:  {}",
        env.non_free_pages_size()?
    )?;
    writeln!(
        out,
        "  free_pages:           {}",
        zerodb::free_page_count(&txn)?
    )?;
    writeln!(out, "  last_pgno:            {}", snap.last_pg)?;
    writeln!(out, "  txnid:                {}", snap.txnid)?;

    // Per-DB stats. main first, then named DBs.
    let main = env.main_database();
    let mut total_branch = 0u64;
    let mut total_leaf = 0u64;
    let mut total_overflow = 0u64;
    let s = main.stat(&txn)?;
    total_branch += s.branch_pages;
    total_leaf += s.leaf_pages;
    total_overflow += s.overflow_pages;
    writeln!(out, "database <main>:")?;
    write_db_stat(&mut out, &s)?;

    for name in zerodb::named_databases(&txn)? {
        let dbh = env
            .open_database(&txn, Some(&name))?
            .ok_or("named DB vanished between enumeration and open")?;
        let s = dbh.stat(&txn)?;
        total_branch += s.branch_pages;
        total_leaf += s.leaf_pages;
        total_overflow += s.overflow_pages;
        writeln!(out, "database {}:", printable_name(&name))?;
        write_db_stat(&mut out, &s)?;
    }

    writeln!(out, "totals (all DBs):")?;
    writeln!(out, "  branch_pages:  {total_branch}")?;
    writeln!(out, "  leaf_pages:    {total_leaf}")?;
    writeln!(out, "  overflow_pages:{total_overflow}")?;
    Ok(out)
}

fn write_db_stat(out: &mut String, s: &zerodb::DatabaseStat) -> Result<(), BoxErr> {
    writeln!(out, "  depth:         {}", s.depth)?;
    writeln!(out, "  branch_pages:  {}", s.branch_pages)?;
    writeln!(out, "  leaf_pages:    {}", s.leaf_pages)?;
    writeln!(out, "  overflow_pages:{}", s.overflow_pages)?;
    writeln!(out, "  entries:       {}", s.entries)?;
    Ok(())
}

/// Render a DB name for the stat report: printable ASCII verbatim, else hex.
fn printable_name(name: &[u8]) -> String {
    if name.iter().all(|&b| b.is_ascii_graphic() || b == b' ') {
        String::from_utf8_lossy(name).into_owned()
    } else {
        let mut s = String::from("0x");
        for b in name {
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}

/// `dump <env-dir>`: logical dump text (mdb_dump-shaped) of every DB. Offline.
///
/// # Errors
///
/// [`lock::LockError`] if the env is busy/missing, or any read error.
pub fn cmd_dump(env_dir: &Path) -> Result<String, BoxErr> {
    let _guard = lock::acquire_existing(env_dir)?;
    let env = open_ro(env_dir)?;
    let txn = env.read_txn()?;
    let dbs = collect_dump(&env, &txn)?;
    Ok(dump_format::render(&dbs))
}

/// `load <dump-file> <env-dir>`: build a fresh env from a logical dump. Uses the
/// bulk builder (every DB's records are sorted+unique in a valid dump); falls
/// back to sorting if a hand-edited dump is out of order. The target dir must be
/// empty of a `zerodb.dat` (a fresh env).
///
/// # Errors
///
/// [`lock::LockError`], parse errors, or a page-build error.
pub fn cmd_load(dump_path: &Path, env_dir: &Path, psize: u32, map_size: u64) -> Result<(), BoxErr> {
    std::fs::create_dir_all(env_dir)?;
    let data = env_dir.join(DATA_FILE_NAME);
    if data.exists() && std::fs::metadata(&data)?.len() > 0 {
        return Err(format!(
            "refusing to load into a non-empty env: {} already exists",
            data.display()
        )
        .into());
    }
    // Lock the (to-be-created) data file for the duration.
    let _guard = lock::acquire_or_create(env_dir)?;

    let text = std::fs::read_to_string(dump_path)?;
    let dbs = dump_format::parse(&text)?;

    // Split into main user data + named DBs; sort+dedup each defensively.
    let mut main_user: Vec<crate::common::Kv> = Vec::new();
    let mut named_owned: Vec<crate::common::NamedDb> = Vec::new();
    for db in dbs {
        let mut entries = db.entries;
        normalize(&mut entries);
        match db.name {
            None => main_user = entries,
            Some(name) => named_owned.push((name, entries)),
        }
    }
    named_owned.sort_by(|a, b| a.0.cmp(&b.0));
    let named: Vec<NamedDbData<'_>> = named_owned
        .iter()
        .map(|(n, e)| NamedDbData {
            name: n,
            entries: e,
        })
        .collect();

    // Build once at the requested map_size (txnid 1 = a fresh env's first
    // state). If the packed image is larger than that map_size, rebuild with a
    // map_size that comfortably covers the file so the loaded env re-opens.
    let build =
        |ms: u64| build_multi_db_image(psize, ms, 1, &main_user, &named, DEFAULT_FILL_PERMILLE);
    let mut img = build(map_size)?;
    let need = img.len() as u64;
    if need > map_size {
        let ms = round_up(need + need / 4, u64::from(psize));
        img = build(ms)?;
    }
    std::fs::write(&data, &img)?;

    // Verify the produced image is structurally clean before returning.
    let violations = check_image(&img, psize);
    if !violations.is_empty() {
        return Err(format!(
            "loaded image failed the invariant check ({} violations): {:?}",
            violations.len(),
            violations
        )
        .into());
    }
    Ok(())
}

/// Round `n` up to the next multiple of `m` (`m > 0`).
fn round_up(n: u64, m: u64) -> u64 {
    n.div_ceil(m) * m
}

/// Sort by key and drop duplicate keys so the bulk builder sees
/// strictly-ascending, unique input. A valid dump is already sorted and unique
/// (this is purely defensive against a hand-edited dump); on a duplicate key the
/// first occurrence is kept.
fn normalize(entries: &mut Vec<crate::common::Kv>) {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries.dedup_by(|a, b| a.0 == b.0);
}

/// `check <env-dir>`: run the SPEC 03 §11 / SPEC 05 §9 invariant walker over the
/// data file and report. Returns `(report, clean)`; the caller exits nonzero
/// when `!clean`. Offline (exclusive lock); tolerant of a corrupt file (it is
/// exactly when `check` matters).
///
/// # Errors
///
/// [`lock::LockError`] if the env is busy/missing, or file I/O errors.
pub fn cmd_check(env_dir: &Path) -> Result<(String, bool), BoxErr> {
    let _guard = lock::acquire_existing(env_dir)?;
    let data = env_dir.join(DATA_FILE_NAME);
    let file = File::open(&data)?;
    let psize = match zerodb_io::probe_page_size(&file)? {
        Some(p) => p,
        None => {
            return Ok((
                format!(
                    "CORRUPT: {} has no readable page-size field (not a zerodb env)",
                    data.display()
                ),
                false,
            ));
        }
    };
    drop(file);
    let bytes = std::fs::read(&data)?;
    let violations = check_image(&bytes, psize);
    let mut out = String::new();
    writeln!(out, "check {} (page_size={psize})", env_dir.display())?;
    if violations.is_empty() {
        writeln!(out, "  OK: no invariant violations")?;
        Ok((out, true))
    } else {
        writeln!(out, "  {} violation(s):", violations.len())?;
        for v in &violations {
            writeln!(out, "    {v}")?;
        }
        Ok((out, false))
    }
}
