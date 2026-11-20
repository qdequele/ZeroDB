//! Reading an environment written in an older on-disk format, so that `dump`
//! and `upgrade` can carry its data into the current format with one tool.
//!
//! The engine opens only files of [`FORMAT_VERSION`] and refuses every other
//! version at open (SPEC 02 §3.2 rule 2). This module is the tools' own
//! read-only reader for [`LEGACY_FORMAT_VERSION`], the format of the 0.2.0
//! release. It is not a second engine: it never writes, never reads the free
//! (GC) database, and reads each page with `pread`, validating it as the
//! engine does under `FileTrust::VALIDATE`.
//!
//! Format 2 differs from format 3 only in the free database's records
//! (ADR-0037: format 3 keys them by `(txnid, seq)` and bounds their length;
//! format 2 keys them by txnid alone and may store a long list on an overflow
//! run). The meta page (SPEC 02 §3, ADR-0022), the common page header, the
//! leaf, branch and overflow pages and the named-database catalog are the
//! same in both. A logical dump reads only the main database and the named
//! databases, so it reads nothing whose layout changed.
//!
//! A format 2 environment comes from a release without a lock file, so no
//! reader slot can protect the pages being read. The reader takes the newest
//! valid meta, walks its trees, and then reads the metas again: the walk is a
//! consistent snapshot if no transaction committed meanwhile, since a writer
//! only reuses pages freed by a commit after that snapshot once that commit
//! is itself on disk. If one did, the read fails and names the cause: the
//! environment must be closed by every process before it is upgraded.

use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use zerodb_core::page::geometry::validate_page_size;
use zerodb_core::page::{
    BranchRef, DBRecord, LeafRef, LeafValue, MetaPage, MetaValidity, OverflowRef, PageRef,
    PageType, FORMAT_VERSION, F_SUBDATA, HEADER_SIZE, PGNO_INVALID,
};

use crate::commands::{open_exclusive_destination, refuse_existing_env, verify_written_env};
use crate::commands::{DbWriter, BATCH_BYTES};
use crate::common::BoxErr;
use crate::dump_format::DumpWriter;
use crate::naming;

/// The older on-disk format version this module reads: the format of the
/// 0.2.0 release (ADR-0022).
pub const LEGACY_FORMAT_VERSION: u32 = 2;

/// A read-only format 2 data file, pinned to one committed meta.
pub struct LegacyEnv {
    file: File,
    path: PathBuf,
    psize: u32,
    meta: MetaPage,
}

/// A named database of a [`LegacyEnv`]: its name and its catalog record.
pub struct LegacyDb {
    /// The database name (the main-database key of its catalog entry).
    pub name: Vec<u8>,
    /// The database's root and statistics.
    pub record: DBRecord,
}

impl LegacyEnv {
    /// Open the data file at `data` and select its newest valid meta slot.
    ///
    /// # Errors
    ///
    /// The file is not a ZeroDB file, is of another format version than
    /// [`LEGACY_FORMAT_VERSION`], has no valid meta slot, or a read fails.
    pub fn open(data: &Path) -> Result<LegacyEnv, BoxErr> {
        let file = File::open(data)?;
        let psize = zerodb_io::probe_page_size(&file)?
            .ok_or_else(|| format!("{} has no readable page-size field", data.display()))?;
        validate_page_size(psize)?;
        let meta = newest_meta(&file, data, psize)?;
        // SPEC 02 §3.2 rule 6: the selected meta's pages lie inside the file.
        let len = file.metadata()?.len();
        let fits = meta
            .last_pg
            .checked_add(1)
            .and_then(|pages| pages.checked_mul(u64::from(psize)))
            .is_some_and(|end| end <= len);
        if !fits {
            return Err(format!(
                "{}: the meta names page {} as the last, beyond the file's {len} bytes",
                data.display(),
                meta.last_pg
            )
            .into());
        }
        Ok(LegacyEnv {
            file,
            path: data.to_path_buf(),
            psize,
            meta,
        })
    }

    /// The environment's page size.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.psize
    }

    /// The selected meta.
    #[must_use]
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// The named databases, in catalog (key) order.
    ///
    /// # Errors
    ///
    /// A read error or a corrupt page.
    pub fn named_databases(&self) -> Result<Vec<LegacyDb>, BoxErr> {
        let mut dbs = Vec::new();
        self.for_each_entry(&self.meta.main_db, |key, flags, val| {
            if flags & F_SUBDATA != 0 {
                let record = DBRecord::from_bytes(val).ok_or_else(|| {
                    format!(
                        "catalog entry {key:02x?} holds {} bytes, not a database record",
                        val.len()
                    )
                })?;
                dbs.push(LegacyDb {
                    name: key.to_vec(),
                    record,
                });
            }
            Ok(())
        })?;
        Ok(dbs)
    }

    /// Call `f(key, node_flags, value)` for every entry of the tree `db`, in
    /// key order. Overflow values are read in full.
    ///
    /// # Errors
    ///
    /// A read error, a corrupt page, or `f`'s error.
    pub fn for_each_entry(
        &self,
        db: &DBRecord,
        mut f: impl FnMut(&[u8], u16, &[u8]) -> Result<(), BoxErr>,
    ) -> Result<(), BoxErr> {
        if db.root == PGNO_INVALID {
            return Ok(());
        }
        self.walk(db.root, db.depth, &mut f)
    }

    /// Fail unless the file's newest valid meta is still the selected one:
    /// no transaction committed while this reader walked its pages.
    ///
    /// # Errors
    ///
    /// The environment changed, or the metas no longer read as valid.
    pub fn ensure_unchanged(&self) -> Result<(), BoxErr> {
        let now = newest_meta(&self.file, &self.path, self.psize)?;
        if now.txnid != self.meta.txnid {
            return Err(format!(
                "{} changed while it was read (transaction {} committed after {}): \
                 close every process that has the environment open, then retry",
                self.path.display(),
                now.txnid,
                self.meta.txnid
            )
            .into());
        }
        Ok(())
    }

    /// Read `count` pages starting at `pgno`, all at or below the meta's
    /// `last_pg`.
    fn read_pages(&self, pgno: u64, count: u64) -> Result<Vec<u8>, BoxErr> {
        pgno.checked_add(count - 1)
            .filter(|&last| pgno >= 2 && last <= self.meta.last_pg)
            .ok_or_else(|| {
                format!(
                    "pages {pgno}..{pgno}+{count} are outside the data pages 2..={}",
                    self.meta.last_pg
                )
            })?;
        // `last_pg` was checked against the file's length at open, so these
        // products are bounded by it and the buffer by the file's size.
        let len = usize::try_from(count * u64::from(self.psize))?;
        let mut buf = vec![0u8; len];
        self.file
            .read_exact_at(&mut buf, pgno * u64::from(self.psize))?;
        let own = PageRef::new(&buf, self.psize)?.pgno();
        if own != pgno {
            return Err(format!("page {pgno} records page number {own}").into());
        }
        Ok(buf)
    }

    /// Depth-first, in-order walk of the subtree at `pgno`, which sits
    /// `levels` levels above the leaves plus one (a leaf has `levels == 1`).
    fn walk(
        &self,
        pgno: u64,
        levels: u16,
        f: &mut impl FnMut(&[u8], u16, &[u8]) -> Result<(), BoxErr>,
    ) -> Result<(), BoxErr> {
        let buf = self.read_pages(pgno, 1)?;
        let page = PageRef::new(&buf, self.psize)?;
        match (page.page_type(), levels) {
            (PageType::Branch, 2..) => {
                let branch = BranchRef::new(&buf, self.psize)?;
                for i in 0..branch.num_keys() {
                    self.walk(branch.child_pgno(i), levels - 1, f)?;
                }
                Ok(())
            }
            (PageType::Leaf, 1) => {
                let leaf = LeafRef::new(&buf, self.psize)?;
                for i in 0..leaf.num_keys() {
                    let key = leaf.key(i);
                    let flags = leaf.node_flags(i);
                    match leaf.value(i) {
                        LeafValue::Inline(v) => f(key, flags, v)?,
                        LeafValue::Overflow { head_pgno, dsize } => {
                            let run = self.read_overflow(head_pgno, dsize)?;
                            f(key, flags, &run[HEADER_SIZE..])?;
                        }
                    }
                }
                Ok(())
            }
            (found, _) => Err(format!(
                "page {pgno} is a {found:?} page where the tree's depth puts a {}",
                if levels == 1 { "leaf" } else { "branch" }
            )
            .into()),
        }
    }

    /// The overflow run at `head` holding a `dsize`-byte value, truncated to
    /// the header plus the value.
    fn read_overflow(&self, head: u64, dsize: u32) -> Result<Vec<u8>, BoxErr> {
        let psize = u64::from(self.psize);
        let needed = (HEADER_SIZE as u64 + u64::from(dsize)).div_ceil(psize);
        let first = self.read_pages(head, 1)?;
        let declared = u64::from(OverflowRef::new(&first, self.psize)?.ovf_pages());
        if declared < needed {
            return Err(format!(
                "overflow run at page {head} has {declared} pages, a {dsize}-byte value needs {needed}"
            )
            .into());
        }
        let mut run = if needed == 1 {
            first
        } else {
            self.read_pages(head, needed)?
        };
        OverflowRef::new(&run, self.psize)?.payload(dsize)?;
        run.truncate(HEADER_SIZE + dsize as usize);
        Ok(run)
    }
}

/// The newest valid meta slot of the format 2 file `file` (SPEC 02 §3.2):
/// a slot with another magic, version or page size makes the file invalid; a
/// torn slot is discarded.
fn newest_meta(file: &File, path: &Path, psize: u32) -> Result<MetaPage, BoxErr> {
    let mut newest: Option<MetaPage> = None;
    for slot in 0..2u64 {
        let mut buf = vec![0u8; psize as usize];
        file.read_exact_at(&mut buf, slot * u64::from(psize))?;
        match MetaPage::validate_version(&buf, psize, LEGACY_FORMAT_VERSION)? {
            MetaValidity::Valid(m) => {
                if newest.is_none_or(|n| m.txnid > n.txnid) {
                    newest = Some(m);
                }
            }
            MetaValidity::BadMagic => {
                return Err(format!("{} is not a ZeroDB data file", path.display()).into())
            }
            MetaValidity::BadVersion(v) => return Err(version_error(path, v).into()),
            MetaValidity::BadPageSize(p) => {
                return Err(
                    format!("{}: meta slot {slot} names page size {p}", path.display()).into(),
                )
            }
            _ => {}
        }
    }
    newest.ok_or_else(|| format!("{}: neither meta slot is valid", path.display()).into())
}

/// The error for a data file of a format version this tool cannot read.
fn version_error(path: &Path, version: u32) -> String {
    if version == FORMAT_VERSION {
        format!(
            "{} is already in the current on-disk format ({FORMAT_VERSION}); \
             use `dump` and `load`, or copy it, instead",
            path.display()
        )
    } else {
        format!(
            "{} is in on-disk format {version}; this zerodb-tools reads formats \
             {LEGACY_FORMAT_VERSION} and {FORMAT_VERSION} only",
            path.display()
        )
    }
}

/// The data file of `env_dir` if it is in [`LEGACY_FORMAT_VERSION`], so
/// `dump` reads it with this module rather than the engine.
///
/// # Errors
///
/// The directory holds no data file or both names (ADR-0010), or a read
/// error.
pub fn legacy_data_file(env_dir: &Path) -> Result<Option<naming::EnvDataFile>, BoxErr> {
    let data = naming::resolve(env_dir)?;
    match naming::probe_magic(&data.path)? {
        Some((magic, LEGACY_FORMAT_VERSION)) if magic == zerodb_core::page::MAGIC => Ok(Some(data)),
        _ => Ok(None),
    }
}

/// `dump` of a format 2 environment: the same dump, byte for byte, as the
/// 0.2.0 `zerodb-tools dump` of that environment.
///
/// # Errors
///
/// As [`LegacyEnv::open`], a corrupt page, a write error on `out`, or the
/// environment changing during the read ([`LegacyEnv::ensure_unchanged`]),
/// in which case `out` holds a dump that must be discarded.
pub fn write_dump(data: &Path, out: impl Write) -> Result<(), BoxErr> {
    let env = LegacyEnv::open(data)?;
    let mut w = DumpWriter::new(out)?;
    w.begin_db(None)?;
    env.for_each_entry(&env.meta().main_db, |k, flags, v| {
        if flags & F_SUBDATA == 0 {
            w.record(k, v)?;
        }
        Ok(())
    })?;
    w.end_db()?;
    for db in env.named_databases()? {
        w.begin_db(Some(&db.name))?;
        env.for_each_entry(&db.record, |k, _, v| Ok(w.record(k, v)?))?;
        w.end_db()?;
    }
    env.ensure_unchanged()?;
    w.finish()?;
    Ok(())
}

/// `upgrade <old-env-dir> <new-env-dir>`: copy every database of a format 2
/// environment into a fresh environment in the current format, in one
/// command.
///
/// The destination is created as `load` creates one (it must be empty and
/// not open elsewhere), with the source's data-file name, so an environment
/// that heed opens as `data.mdb` is upgraded to a `data.mdb`, and with the
/// source's page size unless `page_size` names another. Each database's entry
/// count is checked against the count its source record holds, the source is
/// checked to be unchanged since its meta was read
/// ([`LegacyEnv::ensure_unchanged`]), and the result passes the invariant
/// check before the command returns. The source is never written.
///
/// # Errors
///
/// The source is not a format 2 environment, a refusal of the destination,
/// a corrupt source page, a count mismatch, the source changing during the
/// copy, or an engine error. Every error after the destination is created
/// leaves a partial environment there, to be deleted, as with `load`.
pub fn cmd_upgrade(
    src_dir: &Path,
    dst_dir: &Path,
    page_size: Option<u32>,
    map_size: u64,
) -> Result<String, BoxErr> {
    let data = naming::resolve(src_dir)?;
    let src = LegacyEnv::open(&data.path)?;
    let psize = page_size.unwrap_or(src.page_size());
    let map_size = usize::try_from(map_size).map_err(|_| "map size does not fit in usize")?;

    std::fs::create_dir_all(dst_dir)?;
    refuse_existing_env(dst_dir, "upgrade into")?;
    let dst = open_exclusive_destination(dst_dir, data.name, psize, map_size, "upgrade into")?;

    let mut report = String::new();
    writeln!(
        report,
        "upgrade {} (format {LEGACY_FORMAT_VERSION}) -> {} (format {FORMAT_VERSION})",
        src_dir.display(),
        dst_dir.display()
    )?;

    // The main database's own records; its catalog entries are written by
    // creating the named databases below.
    let named = src.named_databases()?;
    let mut db = DbWriter::new(&dst, None, BATCH_BYTES);
    src.for_each_entry(&src.meta().main_db, |k, flags, v| {
        if flags & F_SUBDATA == 0 {
            db.push(k, v)?;
        }
        Ok(())
    })?;
    let written = db.finish()?;
    let expected = src.meta().main_db.entries - named.len() as u64;
    check_count("<main>", expected, written)?;
    writeln!(report, "  <main>: {written} entries")?;

    for named_db in &named {
        let mut db = DbWriter::new(&dst, Some(&named_db.name), BATCH_BYTES);
        src.for_each_entry(&named_db.record, |k, _, v| db.push(k, v))?;
        let written = db.finish()?;
        let label = crate::commands::printable_name(&named_db.name);
        check_count(&label, named_db.record.entries, written)?;
        writeln!(report, "  {label}: {written} entries")?;
    }

    src.ensure_unchanged()?;
    verify_written_env(&dst, "upgraded")?;
    writeln!(
        report,
        "  verified: entry counts match the source and the new env passes check"
    )?;
    Ok(report)
}

/// Fail unless a database's written entry count is its source count.
fn check_count(db: &str, expected: u64, written: u64) -> Result<(), BoxErr> {
    if written == expected {
        Ok(())
    } else {
        Err(format!("{db}: the source records {expected} entries, {written} were written").into())
    }
}
