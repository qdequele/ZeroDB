//! Compacting / raw environment copy — `Env::copy_to_file` parity (SPEC 00
//! row 17, `mdb_env_copy2`; ADR-0009). Milestone 1.12.
//!
//! Meilisearch snapshots the whole env with `copy_to_file`, in both modes:
//! `CompactionOption::Enabled` (`MDB_CP_COMPACT`, the normal snapshot/compaction
//! path) and `Disabled` (a raw env copy used for the experimental
//! no-compaction / S3 flows). Both open their **own** internal read txn — the
//! caller cannot supply one — so the copy is a consistent point-in-time image.
//!
//! The API is an extension trait ([`CopyToFile`]) rather than an inherent
//! method: [`Env`] lives in `zerodb-core`, which is deliberately I/O-free and
//! `miri`-clean, so the file-writing copy logic lives here in the public
//! `zerodb` crate. The `heed-zerodb` adapter (M1.13) maps heed's inherent
//! `Env::copy_to_file` onto this trait.
//!
//! ## Correctness under a concurrent writer (ADR-0009)
//!
//! `copy_to_file` may run on a **live** env (heed's does). It holds a
//! [`RoTxn`] for the whole copy, pinning snapshot `T`. Every page reachable
//! from `T` was live at `T` and can only be freed by a commit `> T`; the
//! reader's GC gate (SPEC 04 TXN-20/21) forbids reclaiming such a page while
//! this reader is the oldest, so **no reachable page is overwritten during the
//! copy** — the raw copy is torn-free for live data. Pages that were already
//! free at `T` may be concurrently reused by the writer, but their bytes are
//! never read as live data (the copy's freelist lists them free and they are
//! overwritten on the next reuse), so a torn free page is harmless. Compaction
//! sidesteps the question entirely: it reads only reachable entries.

use std::path::Path;

use zerodb_core::builder::{build_multi_db_image, NamedDbData, DEFAULT_FILL_PERMILLE};
use zerodb_core::env::Env;
use zerodb_core::error::{Error, MdbError, Result};
use zerodb_core::page::{MetaPage, FORMAT_VERSION, F_SUBDATA, MAGIC};
use zerodb_core::{collect_entries_flagged, named_databases, RoTxn};

/// Whether [`CopyToFile::copy_to_file`] compacts (`heed::CompactionOption`,
/// SPEC 00 row 59). `Enabled` rewrites into a fresh, densely-packed env with no
/// free pages (`MDB_CP_COMPACT`); `Disabled` copies the snapshot's pages
/// verbatim, keeping the freelist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionOption {
    /// Rewrite into a fresh compact env (bottom-up packed, no free pages).
    Enabled,
    /// Raw copy of the snapshot's pages, freelist preserved.
    Disabled,
}

/// `Env::copy_to_file` (SPEC 00 row 17). Produces a **single data file** (a
/// `zerodb.dat`-format image); to open the copy as an env, place it as
/// `<dir>/zerodb.dat` and open `<dir>`.
pub trait CopyToFile {
    /// Copy this environment to `path` under a fresh internal read snapshot.
    ///
    /// # Errors
    ///
    /// - [`MdbError::ReadersFull`] if no reader slot is free for the internal txn.
    /// - [`Error::Io`] on any write error.
    /// - [`MdbError::Invalid`] if the source is structurally corrupt.
    fn copy_to_file(&self, path: impl AsRef<Path>, option: CompactionOption) -> Result<()>;
}

impl CopyToFile for Env {
    fn copy_to_file(&self, path: impl AsRef<Path>, option: CompactionOption) -> Result<()> {
        // The internal read txn pins the snapshot for the whole copy (SPEC 00
        // row 17: copy opens its own read txn).
        let txn = self.read_txn()?;
        match option {
            CompactionOption::Disabled => copy_raw(self, &txn, path.as_ref()),
            CompactionOption::Enabled => copy_compact(self, &txn, path.as_ref()),
        }
    }
}

/// A key/value pair collected from a database.
type KvPair = (Vec<u8>, Vec<u8>);

/// Map a page-encode failure to the public taxonomy (never fires for a valid
/// snapshot).
fn corrupt(_e: zerodb_core::page::PageError) -> Error {
    Error::Mdb(MdbError::Invalid)
}

/// Non-compact copy: the snapshot's data pages `[FIRST_DATA_PGNO, last_pg]`
/// copied verbatim from the map, with two freshly-encoded meta slots pinned to
/// this snapshot (so the copy opens at exactly snapshot `T`, even if the live
/// env has committed newer metas since the txn began — SPEC 00 row 17).
fn copy_raw(env: &Env, txn: &RoTxn<'_>, dest: &Path) -> Result<()> {
    let psize = env.page_size();
    let ps = psize as usize;
    let snap = txn.snapshot();
    // Pages 0..=last_pg. Slots 0/1 are rewritten below; the rest are data.
    let data_end = (snap.last_pg as usize + 1) * ps;
    let map = txn.map_bytes();
    let src = map.get(..data_end).ok_or(Error::Mdb(MdbError::Invalid))?;

    let mut out = vec![0u8; data_end];
    if data_end > 2 * ps {
        // Copy the data pages verbatim (reachable pages are immutable under the
        // reader pin; free pages are harmless — see the module docs).
        out[2 * ps..data_end].copy_from_slice(&src[2 * ps..data_end]);
    }

    // Synthesize both meta slots from the pinned snapshot — the copy is a
    // self-contained env at snapshot T, freelist preserved (SPEC 02 §3).
    let mut meta = MetaPage {
        pgno: 0,
        txnid: snap.txnid,
        magic: MAGIC,
        format_version: FORMAT_VERSION,
        page_size: psize,
        env_flags: 0,
        map_size: env.map_size(),
        last_pg: snap.last_pg,
        free_db: snap.free_db,
        main_db: snap.main_db,
    };
    meta.encode(&mut out[0..ps]).map_err(corrupt)?;
    meta.pgno = 1;
    meta.encode(&mut out[ps..2 * ps]).map_err(corrupt)?;

    std::fs::write(dest, &out)?;
    Ok(())
}

/// Compacting copy: read every live entry of every DB under the snapshot and
/// rebuild a fresh, densely-packed image (`build_multi_db_image`) with no free
/// pages — the `MDB_CP_COMPACT` shape.
fn copy_compact(env: &Env, txn: &RoTxn<'_>, dest: &Path) -> Result<()> {
    let psize = env.page_size();
    let snap = txn.snapshot();

    // Main DB user data = main tree entries minus the F_SUBDATA catalog records
    // (which are followed into their own sections below).
    let main = env.main_database();
    let main_user: Vec<(Vec<u8>, Vec<u8>)> = collect_entries_flagged(&main, txn)?
        .into_iter()
        .filter(|(_, flags, _)| flags & F_SUBDATA == 0)
        .map(|(k, _, v)| (k, v))
        .collect();

    // Each named DB, in name order.
    let names = named_databases(txn)?;
    let mut named_entries: Vec<(Vec<u8>, Vec<KvPair>)> = Vec::with_capacity(names.len());
    for name in names {
        let dbh = env
            .open_database(txn, Some(&name))?
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let entries: Vec<(Vec<u8>, Vec<u8>)> = collect_entries_flagged(&dbh, txn)?
            .into_iter()
            .map(|(k, _, v)| (k, v))
            .collect();
        named_entries.push((name, entries));
    }
    let named: Vec<NamedDbData<'_>> = named_entries
        .iter()
        .map(|(n, e)| NamedDbData {
            name: n,
            entries: e,
        })
        .collect();

    let img = build_multi_db_image(
        psize,
        env.map_size(),
        snap.txnid,
        &main_user,
        &named,
        DEFAULT_FILL_PERMILLE,
    )
    .map_err(corrupt)?;
    std::fs::write(dest, &img)?;
    Ok(())
}
