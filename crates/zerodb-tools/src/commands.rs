//! `stat`, `dump`, `load`, `check` subcommand implementations.

use std::fmt::Write as _;
use std::fs::File;
use std::path::Path;

use zerodb::{check::check_image, check::check_snapshot, Env, EnvOpenOptions, MdbError};

use crate::common::{collect_dump, open_ro, read_txn, BoxErr};
use crate::dump_format;
use crate::naming;

/// `stat <env-dir>`: env-level and per-DB statistics. Attaches to the env
/// read-only through its lock file, so it also runs while other processes have
/// the env open (ADR-0023 amendment 18).
///
/// Probes both data-file names (ADR-0010) and prints an engine-identification
/// line naming the file, the magic and the format version — so an operator
/// staring at a `data.mdb` that is *not* an LMDB file gets the answer from the
/// tool rather than from `mdb_stat`'s `MDB_INVALID`.
///
/// # Errors
///
/// [`naming::NameError`] if the env is missing or ambiguous (both names
/// present), the open's error, or any read error.
pub fn cmd_stat(env_dir: &Path) -> Result<String, BoxErr> {
    let data = naming::resolve(env_dir)?;
    let env = open_ro(env_dir, data.name)?;
    let txn = read_txn(&env)?;
    let snap = txn.snapshot();
    let psize = env.page_size();

    let mut out = String::new();
    writeln!(out, "env: {}", env_dir.display())?;
    writeln!(out, "  engine:               {}", engine_line(&data)?)?;
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

/// The engine-identification line for `stat` (ADR-0010 §Options D2 —
/// "in spirit": the magic in the file is the authoritative discriminator, so
/// the tool reports it rather than shipping a marker file). Shows the data-file
/// name actually found, the four magic bytes, and the format version.
fn engine_line(data: &naming::EnvDataFile) -> Result<String, BoxErr> {
    let probed = naming::probe_magic(&data.path)?;
    Ok(match probed {
        Some((magic, version)) if magic == zerodb_core::page::MAGIC => format!(
            "zerodb (magic {}, format_version {version}); data file {}",
            String::from_utf8_lossy(&magic),
            data.name
        ),
        Some((magic, _)) => format!(
            "UNKNOWN (magic {magic:02x?} is not zerodb's {:?}); data file {}",
            zerodb_core::page::MAGIC,
            data.name
        ),
        None => format!(
            "UNKNOWN (file too short for a meta page); data file {}",
            data.name
        ),
    })
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

/// `dump <env-dir>`: logical dump text (mdb_dump-shaped) of every DB, from one
/// read snapshot. Attaches to the env read-only through its lock file, like
/// `stat`. Probes both data-file names (ADR-0010).
///
/// # Errors
///
/// [`naming::NameError`] if the env is missing or ambiguous, the open's error,
/// or any read error.
pub fn cmd_dump(env_dir: &Path) -> Result<String, BoxErr> {
    let data = naming::resolve(env_dir)?;
    let env = open_ro(env_dir, data.name)?;
    let txn = read_txn(&env)?;
    let dbs = collect_dump(&env, &txn)?;
    Ok(dump_format::render(&dbs))
}

/// `load <dump-file> <env-dir>`: build a fresh env from a logical dump.
///
/// The target dir must hold no existing env under **either** data-file name
/// (ADR-0010); the env it creates is a native one, named
/// [`zerodb::DATA_FILE_NAME`], with its lock file. It is opened through the
/// engine and must be opened exclusively ([`Env::opened_exclusively`]): a
/// destination another process has open is refused, and the `Env` is held
/// for the whole load (ADR-0023 amendment 18). The records go in through
/// ordinary write txns with sequential writes (ADR-0015), as LMDB's
/// `mdb_load` uses txns; every DB's records are sorted and unique in a valid
/// dump, and a hand-edited dump is sorted first. The map grows when it fills.
///
/// # Errors
///
/// A refusal, parse errors, or an engine error.
pub fn cmd_load(dump_path: &Path, env_dir: &Path, psize: u32, map_size: u64) -> Result<(), BoxErr> {
    std::fs::create_dir_all(env_dir)?;
    refuse_existing_env(env_dir, "load into")?;

    let text = std::fs::read_to_string(dump_path)?;
    let dbs = dump_format::parse(&text)?;
    drop(text);

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

    let map_size = usize::try_from(map_size).map_err(|_| "map size does not fit in usize")?;
    let env = open_exclusive_destination(env_dir, psize, map_size, "load into")?;

    // Named DBs first, so each catalog record exists before any write lands
    // next to it; the main DB's user records last.
    for (name, entries) in &named_owned {
        write_db_batched(&env, Some(name), entries)?;
    }
    write_db_batched(&env, None, &main_user)?;
    drop(named_owned);
    drop(main_user);

    // Verify the produced env is structurally clean before returning. Unlike
    // the old `check_image` over the file, this does not re-verify the meta
    // slots or the file length: the engine wrote and validated those itself
    // (each commit writes a checksummed meta; this read txn's open accepted
    // it), and the walk covers every page the committed snapshot references.
    let txn = read_txn(&env)?;
    let violations = check_snapshot(txn.map_bytes(), env.page_size(), txn.snapshot());
    if !violations.is_empty() {
        return Err(format!(
            "loaded env failed the invariant check ({} violations): {:?}",
            violations.len(),
            violations
        )
        .into());
    }
    Ok(())
}

/// Open a fresh destination env (native name, lock file on) for `load` or
/// `migrate-from-lmdb`, and refuse it unless this process opened it alone
/// and it is still empty (ADR-0023 amendment 18).
///
/// The empty-destination check before the open ([`refuse_existing_env`]) ran
/// without holding anything, so it is repeated here, now that the `Env` is
/// held: another process may have created and filled the environment in
/// between and closed it again. A process that opens it after this check
/// joins as with `mdb_load` (a reader sees each commit; a writer interleaves
/// its txns).
///
/// # Errors
///
/// A refusal naming `verb`, or the open's error.
pub(crate) fn open_exclusive_destination(
    env_dir: &Path,
    psize: u32,
    map_size: usize,
    verb: &str,
) -> Result<Env, BoxErr> {
    let env = EnvOpenOptions::new()
        .map_size(map_size)
        .max_dbs(crate::common::MAX_TOOL_DBS)
        .page_size(psize)
        .sequential_writes(true)
        .lock_file(true)
        .open(env_dir)?;
    if !env.opened_exclusively() {
        return Err(format!(
            "refusing to {verb} {}: another process has the env open",
            env_dir.display()
        )
        .into());
    }
    let txn = read_txn(&env)?;
    if txn.snapshot().main_db.entries != 0 {
        return Err(format!(
            "refusing to {verb} a non-empty env: {} already holds data",
            env_dir.join(zerodb::DATA_FILE_NAME).display()
        )
        .into());
    }
    drop(txn);
    for candidate in naming::existing_data_files(env_dir) {
        if candidate.name != zerodb::DATA_FILE_NAME && std::fs::metadata(&candidate.path)?.len() > 0
        {
            return Err(format!(
                "refusing to {verb} a non-empty env: {} already exists",
                candidate.path.display()
            )
            .into());
        }
    }
    Ok(env)
}

/// Commit a write txn after this many bytes of keys and values, so one txn
/// never holds a large DB's dirty pages in memory.
const BATCH_BYTES: usize = 64 << 20;

/// Write `entries` (sorted, unique) into `name`, or the main DB, through
/// write txns of at most [`BATCH_BYTES`] of records each. Creates a named DB
/// even when it is empty, so its catalog record exists. When the map fills,
/// the txn is aborted, the map doubled ([`Env::resize`]) and the batch
/// retried. Returns the number of records written.
///
/// # Errors
///
/// An engine error other than a full map.
fn write_db_batched(
    env: &Env,
    name: Option<&[u8]>,
    entries: &[crate::common::Kv],
) -> Result<u64, BoxErr> {
    let mut start = 0;
    loop {
        let mut end = start;
        let mut bytes = 0;
        while end < entries.len() && (end == start || bytes < BATCH_BYTES) {
            bytes += entries[end].0.len() + entries[end].1.len();
            end += 1;
        }
        match write_batch(env, name, &entries[start..end]) {
            Ok(()) => {}
            Err(zerodb::Error::Mdb(MdbError::MapFull)) => {
                let grown = env
                    .map_size()
                    .checked_mul(2)
                    .ok_or("map size overflow while growing the map")?;
                env.resize(grown)?;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        start = end;
        if start == entries.len() {
            return Ok(entries.len() as u64);
        }
    }
}

/// One write txn: create or open the DB and put `batch` into it.
fn write_batch(
    env: &Env,
    name: Option<&[u8]>,
    batch: &[crate::common::Kv],
) -> Result<(), zerodb::Error> {
    let mut wtxn = env.write_txn()?;
    let db = match name {
        None => env.main_database(),
        Some(n) => env.create_database(&mut wtxn, Some(n))?,
    };
    for (k, v) in batch {
        db.put(&mut wtxn, k, v)?;
    }
    wtxn.commit()
}

/// Refuse to create an env in `env_dir` if it already holds a non-empty data
/// file under **either** candidate name (ADR-0010). Shared by `load` and
/// `migrate-from-lmdb`, which both create native envs and must not clobber (or
/// sit next to) an adapter-created `data.mdb`.
///
/// # Errors
///
/// A descriptive error naming the file that is in the way, or an I/O error.
pub fn refuse_existing_env(env_dir: &Path, verb: &str) -> Result<(), BoxErr> {
    for candidate in naming::existing_data_files(env_dir) {
        if std::fs::metadata(&candidate.path)?.len() > 0 {
            return Err(format!(
                "refusing to {verb} a non-empty env: {} already exists",
                candidate.path.display()
            )
            .into());
        }
    }
    Ok(())
}

/// Sort by key and drop duplicate keys so the write txns see
/// strictly-ascending, unique input (the case sequential writes serve
/// best). A valid dump is already sorted and unique
/// (this is purely defensive against a hand-edited dump); on a duplicate key the
/// first occurrence is kept.
fn normalize(entries: &mut Vec<crate::common::Kv>) {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries.dedup_by(|a, b| a.0 == b.0);
}

/// `check <env-dir>`: run the SPEC 03 §11 / SPEC 05 §9 invariant walker and
/// report. Returns `(report, clean)`; the caller exits nonzero when `!clean`.
///
/// It attaches to the env read-only through its lock file, like `stat`, and
/// walks the snapshot its read txn pins: that snapshot's roots, free tree and
/// `last_pg`, through the txn's map (ADR-0023 amendment 18). So it runs while
/// other processes have the env open, and never reads a page the snapshot
/// lists as free.
///
/// If the env does not open as a valid one ([`MdbError::Invalid`], which is
/// when `check` matters most), it walks the raw data file instead, but only
/// while holding the env's lock file exclusively ([`zerodb::ExclusiveLock`]),
/// so no process opens the env during the walk. If another process has the
/// env open, it refuses. On read-only media, where no lock file can be
/// created, the raw walk runs and its report says it had no liveness
/// guarantee.
///
/// # Errors
///
/// [`naming::NameError`] if the env is missing or ambiguous, a refusal while
/// the env is in use, or other open and file I/O errors.
pub fn cmd_check(env_dir: &Path) -> Result<(String, bool), BoxErr> {
    let resolved = naming::resolve(env_dir)?;
    let env = match open_ro(env_dir, resolved.name) {
        Ok(env) => env,
        Err(zerodb::Error::Mdb(MdbError::Invalid)) => {
            return check_raw_exclusively(env_dir, &resolved);
        }
        Err(e) => return Err(e.into()),
    };
    let txn = read_txn(&env)?;
    let psize = env.page_size();
    let violations = check_snapshot(txn.map_bytes(), psize, txn.snapshot());
    let mut out = String::new();
    writeln!(
        out,
        "check {} (page_size={psize}, txnid={})",
        env_dir.display(),
        txn.txnid()
    )?;
    Ok(report(out, &violations))
}

/// `check`'s raw-file fallback for an env that does not open: take the env's
/// lock file exclusively, then walk the data file's bytes.
fn check_raw_exclusively(
    env_dir: &Path,
    data: &naming::EnvDataFile,
) -> Result<(String, bool), BoxErr> {
    let opts = crate::common::tool_open_options(env_dir, data.name)?;
    let (_guard, note) = match zerodb::ExclusiveLock::acquire(env_dir, &opts) {
        Ok(guard) => (Some(guard), None),
        Err(zerodb::Error::Io(e)) if e.kind() == std::io::ErrorKind::ReadOnlyFilesystem => (
            None,
            Some(
                "  note: read-only media, so no lock file: walked the raw file offline, \
                 without a guarantee that no other process has the env open",
            ),
        ),
        Err(zerodb::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(format!(
                "the env at {} does not open as a valid env, and another process has it \
                 open ({e}); the raw-file check needs every other process to close it",
                env_dir.display()
            )
            .into());
        }
        Err(e) => return Err(e.into()),
    };
    let (mut out, clean) = check_raw_file(env_dir, &data.path)?;
    if let Some(note) = note {
        writeln!(out, "{note}")?;
    }
    Ok((out, clean))
}

/// Walk the raw data file at `data` ([`check_image`]). Tolerant of a corrupt
/// file.
fn check_raw_file(env_dir: &Path, data: &Path) -> Result<(String, bool), BoxErr> {
    let file = File::open(data)?;
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
    let bytes = std::fs::read(data)?;
    let violations = check_image(&bytes, psize);
    let mut out = String::new();
    writeln!(
        out,
        "check {} (page_size={psize}, raw file: the env did not open)",
        env_dir.display()
    )?;
    Ok(report(out, &violations))
}

/// Append the verdict to a `check` report.
fn report(mut out: String, violations: &[String]) -> (String, bool) {
    if violations.is_empty() {
        out.push_str("  OK: no invariant violations\n");
        (out, true)
    } else {
        let _ = writeln!(out, "  {} violation(s):", violations.len());
        for v in violations {
            let _ = writeln!(out, "    {v}");
        }
        (out, false)
    }
}
