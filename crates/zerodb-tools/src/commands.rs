//! `stat`, `dump`, `load`, `check` subcommand implementations.

use std::fmt::Write as _;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use zerodb::{check::check_image, check::check_snapshot, Env, EnvOpenOptions, MdbError, PutFlags};

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
/// `mdb_load` uses txns. The map grows when it fills.
///
/// The dump is read twice, a line at a time ([`dump_format::DumpReader`]),
/// so memory stays bounded by one write txn's batch whatever the dump's size:
/// a first pass checks the whole dump's syntax before the destination is
/// touched, the second writes it. Every DB's records are sorted and unique in
/// a valid dump; in a hand-edited one, out-of-order records are still
/// placed in key order by the B+tree, and of several records with the same
/// key in a database the first one in the dump is kept.
///
/// # Errors
///
/// A refusal, parse errors, or an engine error.
pub fn cmd_load(dump_path: &Path, env_dir: &Path, psize: u32, map_size: u64) -> Result<(), BoxErr> {
    load_batched(dump_path, env_dir, psize, map_size, BATCH_BYTES)
}

/// [`cmd_load`] committing a write txn after every `batch_bytes` of keys and
/// values (tests use a small value to cross many batches).
fn load_batched(
    dump_path: &Path,
    env_dir: &Path,
    psize: u32,
    map_size: u64,
    batch_bytes: usize,
) -> Result<(), BoxErr> {
    std::fs::create_dir_all(env_dir)?;
    refuse_existing_env(env_dir, "load into")?;

    // Pass 1: reject a malformed dump before creating anything.
    let mut reader = open_dump(dump_path)?;
    while reader.next_db()?.is_some() {}

    let map_size = usize::try_from(map_size).map_err(|_| "map size does not fit in usize")?;
    let env = open_exclusive_destination(env_dir, psize, map_size, "load into")?;

    // Pass 2: write each block's records in batches, in dump order (the main
    // DB first, then the named DBs).
    let mut reader = open_dump(dump_path)?;
    let mut batch = Batch::default();
    let (mut key, mut val) = (Vec::new(), Vec::new());
    while let Some(name) = reader.next_db()? {
        batch.clear();
        let mut written = false;
        while reader.next_record(&mut key, &mut val)? {
            batch.push(&key, &val);
            if batch.data.len() >= batch_bytes {
                write_batch_growing(&env, name.as_deref(), &batch)?;
                batch.clear();
                written = true;
            }
        }
        // The last partial batch; an empty one still creates an empty named
        // DB's catalog record.
        if !batch.is_empty() || !written {
            write_batch_growing(&env, name.as_deref(), &batch)?;
        }
    }
    drop(batch);

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

/// Open `path` for streaming through the dump parser.
fn open_dump(path: &Path) -> Result<dump_format::DumpReader<BufReader<File>>, BoxErr> {
    let file = File::open(path)?;
    Ok(dump_format::DumpReader::new(BufReader::with_capacity(
        1 << 20,
        file,
    ))?)
}

/// One write txn's records, packed into one buffer: about [`BATCH_BYTES`]
/// of keys and values, kept until the txn commits so a full map can be grown
/// and the batch retried.
#[derive(Default)]
struct Batch {
    /// Every record's key then value, back to back.
    data: Vec<u8>,
    /// Each record's `(key length, value length)`, in `data` order.
    lens: Vec<(usize, usize)>,
}

impl Batch {
    fn push(&mut self, key: &[u8], val: &[u8]) {
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(val);
        self.lens.push((key.len(), val.len()));
    }

    fn clear(&mut self) {
        self.data.clear();
        self.lens.clear();
    }

    fn is_empty(&self) -> bool {
        self.lens.is_empty()
    }

    /// The records, in the order they were pushed.
    fn records(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut at = 0;
        self.lens.iter().map(move |&(klen, vlen)| {
            let key = &self.data[at..at + klen];
            let val = &self.data[at + klen..at + klen + vlen];
            at += klen + vlen;
            (key, val)
        })
    }
}

/// Write `batch` into `name`, or the main DB, in one write txn. Creates a
/// named DB even when the batch is empty, so its catalog record exists. When
/// the map fills, the txn is aborted, the map doubled ([`Env::resize`]) and
/// the batch retried.
///
/// # Errors
///
/// An engine error other than a full map.
fn write_batch_growing(env: &Env, name: Option<&[u8]>, batch: &Batch) -> Result<(), BoxErr> {
    loop {
        match write_batch(env, name, batch) {
            Ok(()) => return Ok(()),
            Err(zerodb::Error::Mdb(MdbError::MapFull)) => {
                let grown = env
                    .map_size()
                    .checked_mul(2)
                    .ok_or("map size overflow while growing the map")?;
                env.resize(grown)?;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// One write txn: create or open the DB and put `batch` into it. A record
/// whose key the DB already holds is skipped (`NO_OVERWRITE`), so the first
/// record of a duplicated key wins. A valid dump has no duplicates, and its
/// ascending keys take the sequential-writes path whatever the flag.
fn write_batch(env: &Env, name: Option<&[u8]>, batch: &Batch) -> Result<(), zerodb::Error> {
    let mut wtxn = env.write_txn()?;
    let db = match name {
        None => env.main_database(),
        Some(n) => env.create_database(&mut wtxn, Some(n))?,
    };
    for (k, v) in batch.records() {
        match db.put_with_flags(&mut wtxn, PutFlags::NO_OVERWRITE, k, v) {
            Ok(()) | Err(zerodb::Error::Mdb(MdbError::KeyExist)) => {}
            Err(e) => return Err(e),
        }
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
/// guarantee. The walk leaves the lock file carrying only the initializing
/// magic, which records no reader count, so the next process to open the
/// env alone sizes the reader table from its own `max_readers`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dump_format::{render, DumpDb};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_dir(tag: &str) -> PathBuf {
        // Relaxed: the counter only makes names unique within this process.
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "zerodb-tools-unit-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), v.to_vec())
    }

    /// A hand-edited dump, written across many tiny batches into a map
    /// that must grow: out-of-order records land in key order and, of
    /// duplicated keys, the first in the dump is kept, also when the
    /// duplicate falls in a later batch.
    #[test]
    fn load_across_batches_sorts_and_keeps_first_duplicate() {
        let mut main = Vec::new();
        for i in (0u32..3000).rev() {
            main.push(kv(format!("k{i:05}").as_bytes(), &[b'a'; 40]));
        }
        main.push(kv(b"k00007", b"second"));
        main.insert(0, kv(b"k02999", b"first"));
        let named = vec![
            kv(b"z", b"1"),
            kv(b"a", b"2"),
            kv(b"z", b"3"),
            kv(b"m", &[7u8; 50_000]),
        ];
        let input = render(&[
            DumpDb {
                name: None,
                entries: main,
            },
            DumpDb {
                name: Some(b"n".to_vec()),
                entries: named,
            },
            DumpDb {
                name: Some(b"empty".to_vec()),
                entries: vec![],
            },
        ]);
        let dir = tmp_dir("batches");
        let dump = dir.join("in.dump");
        std::fs::write(&dump, input).unwrap();
        let env_dir = dir.join("env");
        load_batched(&dump, &env_dir, 4096, 64 << 10, 1000).unwrap();

        let mut want_main: Vec<_> = (0u32..3000)
            .map(|i| kv(format!("k{i:05}").as_bytes(), &[b'a'; 40]))
            .collect();
        want_main[2999].1 = b"first".to_vec();
        let want = render(&[
            DumpDb {
                name: None,
                entries: want_main,
            },
            DumpDb {
                name: Some(b"empty".to_vec()),
                entries: vec![],
            },
            DumpDb {
                name: Some(b"n".to_vec()),
                entries: vec![kv(b"a", b"2"), kv(b"m", &[7u8; 50_000]), kv(b"z", b"1")],
            },
        ]);
        assert_eq!(cmd_dump(&env_dir).unwrap(), want);
        let (report, clean) = cmd_check(&env_dir).unwrap();
        assert!(clean, "{report}");
    }

    /// A dump that is malformed anywhere, even after many valid records, is
    /// rejected before the destination env is created.
    #[test]
    fn malformed_dump_leaves_destination_untouched() {
        let mut text = render(&[DumpDb {
            name: None,
            entries: (0u32..500).map(|i| kv(&i.to_be_bytes(), b"v")).collect(),
        }]);
        text.push_str("format=bytevalue\nHEADER=END\n 6b\n zz\nDATA=END\n");
        let dir = tmp_dir("malformed");
        let dump = dir.join("in.dump");
        std::fs::write(&dump, text).unwrap();
        let env_dir = dir.join("env");
        let err = load_batched(&dump, &env_dir, 4096, 1 << 20, 100).unwrap_err();
        assert!(err.to_string().contains("bad hex"), "{err}");
        assert!(naming::existing_data_files(&env_dir).is_empty());
    }
}
