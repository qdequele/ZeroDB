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

use zerodb_core::builder::{EnvStream, PageSink, StreamBuildError, DEFAULT_FILL_PERMILLE};
use zerodb_core::env::Env;
use zerodb_core::error::{Error, MdbError, Result};
use zerodb_core::page::FIRST_DATA_PGNO;
use zerodb_core::page::{MetaPage, FORMAT_VERSION, F_SUBDATA, MAGIC};
use zerodb_core::{for_each_entry_flagged, named_databases, RoTxn};

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

/// How far along a [`CopyToFile::copy_to_file_with_progress`] run is
/// (**milestone 2.3**).
///
/// A **ZeroDB extension**: `mdb_env_copy2` reports nothing, and heed's
/// `copy_to_file` is a blocking call with no observation point, which makes a
/// multi-gigabyte Meilisearch snapshot an opaque wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyProgress {
    /// Source pages processed so far. Monotonically non-decreasing within one
    /// copy, and equal to [`CopyProgress::total`] in the final call.
    pub done: u64,
    /// Total pages the copy expects to process.
    ///
    /// For [`CompactionOption::Disabled`] this is **exact**: the snapshot's
    /// page count, every one of which is copied. For
    /// [`CompactionOption::Enabled`] it is an **estimate** — the number of
    /// live pages reachable in the source, which is what the copy must *read*;
    /// the number it *writes* is smaller and unknowable until packing
    /// finishes. `total` never changes during a run, so `done as f64 / total
    /// as f64` is a usable fraction in both modes.
    pub total: u64,
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

    /// As [`CopyToFile::copy_to_file`], reporting progress to `on_progress`
    /// (**milestone 2.3**).
    ///
    /// `on_progress` is called at least twice — once with `done == 0` before
    /// any work, once with `done == total` when the image is complete — and
    /// with monotonically non-decreasing `done` in between. `total` is
    /// identical in every call of a run. See [`CopyProgress`] for what the
    /// numbers mean in each mode.
    ///
    /// # Where the callback runs, and what a panic does
    ///
    /// Every callback fires **before a single byte reaches `path`**. That is
    /// a deliberate contract, not an accident of the implementation, and both
    /// modes honor it by different means:
    ///
    /// - `Disabled` (raw): the image is assembled in memory and written to
    ///   `path` by one `std::fs::write` after the last callback returns.
    /// - `Enabled` (compacting, **streamed since PERF-GAP C1**): pages land
    ///   incrementally in a sibling temp file (`<name>.copy-tmp-<pid>` in
    ///   `path`'s directory, bounded memory — O(tree depth × page size));
    ///   `path` itself is touched only by the final atomic `rename`, after
    ///   the last callback. On any error — or a panicking callback — the
    ///   temp file is removed by a drop guard.
    ///
    /// In both modes therefore:
    ///
    /// - A panicking callback unwinds out of this function normally (panics
    ///   are not caught) and **`path` is left untouched** — absent if it did
    ///   not exist, and byte-for-byte its old contents if it did. There is no
    ///   half-written copy to mistake for a good one. (The compacting mode's
    ///   rename makes this hold even for a process kill mid-copy, which the
    ///   raw mode's single `write` cannot promise.)
    /// - The **source** environment is likewise untouched under any callback
    ///   behavior: a copy only ever reads it, under an internal read txn that
    ///   is released when this function returns (including while unwinding).
    ///   A panic leaks no reader slot.
    ///
    /// The cost of that guarantee is that progress tracks *source pages
    /// processed*, not bytes landed at `path`, and that the final stretch
    /// (the raw mode's single `write`; the compacting mode's rename) is not
    /// covered by any callback. Callers wanting a progress bar that ends
    /// exactly when the file is durable should treat `done == total` as
    /// "reading finished", not "file written".
    ///
    /// **Durability is the caller's concern** (LMDB `mdb_env_copy` parity —
    /// deliberate, revisited for issue #46): no mode fsyncs the copy, and the
    /// compacting mode does not fsync `path`'s directory after its rename. A
    /// caller that needs the snapshot crash-durable must fsync the produced
    /// file *and* its parent directory. (Contrast: *env creation* does both
    /// itself — SPEC 02 §3.4 step 4.)
    ///
    /// # Errors
    ///
    /// As [`CopyToFile::copy_to_file`].
    fn copy_to_file_with_progress(
        &self,
        path: impl AsRef<Path>,
        option: CompactionOption,
        on_progress: &mut dyn FnMut(CopyProgress),
    ) -> Result<()>;
}

impl CopyToFile for Env {
    fn copy_to_file(&self, path: impl AsRef<Path>, option: CompactionOption) -> Result<()> {
        // Same code path as the progress form, with a callback that does
        // nothing — so the no-callback signature (heed parity, SPEC 00 row 17)
        // cannot drift from the instrumented one.
        self.copy_to_file_with_progress(path, option, &mut |_| {})
    }

    fn copy_to_file_with_progress(
        &self,
        path: impl AsRef<Path>,
        option: CompactionOption,
        on_progress: &mut dyn FnMut(CopyProgress),
    ) -> Result<()> {
        // The internal read txn pins the snapshot for the whole copy (SPEC 00
        // row 17: copy opens its own read txn).
        let txn = self.read_txn()?;
        match option {
            CompactionOption::Disabled => copy_raw(self, &txn, path.as_ref(), on_progress),
            CompactionOption::Enabled => copy_compact(self, &txn, path.as_ref(), on_progress),
        }
    }
}

/// Drives a [`CopyProgress`] sequence: fixed `total`, monotone `done`, a
/// guaranteed `done == 0` opening call and `done == total` closing call.
struct Progress<'f> {
    total: u64,
    done: u64,
    f: &'f mut dyn FnMut(CopyProgress),
}

impl<'f> Progress<'f> {
    /// Start a run and emit the opening `done == 0` call.
    fn start(total: u64, f: &'f mut dyn FnMut(CopyProgress)) -> Progress<'f> {
        let mut p = Progress { total, done: 0, f };
        p.emit();
        p
    }

    /// Advance to `done` and report. Clamped to `total` and never allowed to
    /// go backwards, so the monotonicity the callback contract promises holds
    /// even when a caller's page estimate is off (the compacting mode's
    /// `total` is an estimate — see [`CopyProgress::total`]).
    fn advance_to(&mut self, done: u64) {
        let done = done.min(self.total);
        if done > self.done {
            self.done = done;
            self.emit();
        }
    }

    /// Emit the closing `done == total` call. Idempotent-safe: if `done`
    /// already equals `total`, `advance_to` suppresses the duplicate.
    fn finish(&mut self) {
        let total = self.total;
        self.advance_to(total);
    }

    fn emit(&mut self) {
        (self.f)(CopyProgress {
            done: self.done,
            total: self.total,
        });
    }
}

/// Map a page-encode failure to the public taxonomy (never fires for a valid
/// snapshot).
fn corrupt(_e: zerodb_core::page::PageError) -> Error {
    Error::Mdb(MdbError::Invalid)
}

/// Non-compact copy: the snapshot's data pages `[FIRST_DATA_PGNO, last_pg]`
/// copied verbatim from the map, with two freshly-encoded meta slots pinned to
/// this snapshot (so the copy opens at exactly snapshot `T`, even if the live
/// env has committed newer metas since the txn began — SPEC 00 row 17).
fn copy_raw(
    env: &Env,
    txn: &RoTxn<'_>,
    dest: &Path,
    on_progress: &mut dyn FnMut(CopyProgress),
) -> Result<()> {
    let psize = env.page_size();
    let ps = psize as usize;
    let snap = txn.snapshot();
    // Pages 0..=last_pg. Slots 0/1 are rewritten below; the rest are data.
    let n_pages = snap.last_pg + 1;
    let data_end = (snap.last_pg as usize + 1) * ps;
    let map = txn.map_bytes();
    let src = map.get(..data_end).ok_or(Error::Mdb(MdbError::Invalid))?;

    // M2.3: the raw copy's page count is exact — every page in the snapshot is
    // copied verbatim.
    let mut progress = Progress::start(n_pages, on_progress);

    let mut out = vec![0u8; data_end];
    if data_end > 2 * ps {
        // Copy the data pages verbatim (reachable pages are immutable under the
        // reader pin; free pages are harmless — see the module docs). Chunked
        // so progress is observable; the chunk size is a *reporting*
        // granularity only and does not affect one byte of the output.
        //
        // Sized relative to the env rather than fixed: a fixed chunk either
        // fires once on a small env (useless — the caller learns nothing
        // between 0 and total) or hundreds of thousands of times on a large
        // one. Aiming at ~`PROGRESS_STEPS` reports keeps the callback rate
        // bounded and the resolution usable at every scale.
        const PROGRESS_STEPS: u64 = 32;
        let pages_per_chunk = (n_pages / PROGRESS_STEPS).max(1);
        let mut pg = 2u64;
        while pg < n_pages {
            let end = (pg + pages_per_chunk).min(n_pages);
            let (from, to) = (pg as usize * ps, end as usize * ps);
            out[from..to].copy_from_slice(&src[from..to]);
            pg = end;
            progress.advance_to(pg);
        }
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

    // Last callback before any destination I/O — see the panic contract on
    // `copy_to_file_with_progress`.
    progress.finish();
    std::fs::write(dest, &out)?;
    Ok(())
}

/// Compacting copy: read every live entry of every DB under the snapshot and
/// rebuild a fresh, densely-packed image (`build_multi_db_image`) with no free
/// pages — the `MDB_CP_COMPACT` shape.
fn copy_compact(
    env: &Env,
    txn: &RoTxn<'_>,
    dest: &Path,
    on_progress: &mut dyn FnMut(CopyProgress),
) -> Result<()> {
    // M2.4 scope boundary (SPEC 03 §2.0). The compacting rebuild goes through
    // the bulk builder, whose ordering contract is memcmp end to end: it packs
    // the main catalog by `sort_by(memcmp)` and debug-asserts strictly
    // ascending memcmp order for every DB it packs. Feeding it entries ordered
    // by a caller's comparator would produce a tree whose physical order is
    // the comparator's but whose builder-side reasoning assumed memcmp — and
    // the resulting file records no comparator identity, so nothing downstream
    // (`zerodb-tools check`, `dump`/`load`) could tell. Refusing loudly is the
    // only honest option until the builder, the dump format and the tools are
    // made comparator-aware, which is its own milestone.
    //
    // `CompactionOption::Disabled` (the raw page copy) is unaffected: it is a
    // byte-level copy that preserves whatever order is on disk.
    if env.has_custom_comparator() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "compacting copy is not supported on an environment with a custom key comparator              (milestone 2.4, SPEC 03 §2.0); use CompactionOption::Disabled",
        )));
    }
    let psize = env.page_size();
    let snap = txn.snapshot();

    // M2.3: the compacting copy's unit of work is *reading* the source's live
    // pages; how many pages it writes is only known once packing finishes, so
    // `total` is the source's reachable page count (see `CopyProgress::total`).
    // The main tree's record covers the catalog; each named DB's record is
    // added as its section is read.
    let main_pages =
        snap.main_db.branch_pages + snap.main_db.leaf_pages + snap.main_db.overflow_pages;

    // Main DB user data = main tree entries minus the F_SUBDATA catalog records
    // (which are followed into their own sections below).
    let main = env.main_database();
    let mut total = main_pages;
    let names_probe = named_databases(txn)?;
    for name in &names_probe {
        if let Some(dbh) = env.open_database(txn, Some(name))? {
            let st = dbh.stat(txn)?;
            total += st.branch_pages + st.leaf_pages + st.overflow_pages;
        }
    }
    let mut progress = Progress::start(total, on_progress);

    // PERF-GAP C1: stream the rebuild. Pages land incrementally in a sibling
    // TEMP file; `dest` is touched only by the final atomic rename, after the
    // last progress callback — so the documented panic contract ("`path` is
    // left untouched") holds verbatim, now under a *stronger* mechanism: even
    // a process kill mid-copy cannot leave a half-written `dest` (the old
    // single `std::fs::write` could). Peak memory is O(tree depth × psize)
    // instead of the whole entry set + whole image (~2× env size).
    let file_name = dest
        .file_name()
        .ok_or_else(|| Error::Io(std::io::Error::other("copy destination has no file name")))?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(format!(".copy-tmp-{}", std::process::id()));
    let tmp = dest.with_file_name(tmp_name);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    // Remove the temp on every non-rename exit (error or unwinding callback).
    let mut guard = TmpGuard {
        path: &tmp,
        armed: true,
    };

    let sink = FileSink {
        file,
        psize,
        next: FIRST_DATA_PGNO,
    };
    let mut es = EnvStream::new(sink, psize, snap.txnid, DEFAULT_FILL_PERMILLE).map_err(corrupt)?;

    // Named DBs first (their roots feed the catalog), in name order — the
    // stream's contract and `named_databases`'s order. Progress advances per
    // source section read, monotone under the same fixed total as before
    // (the section *order* moved: named DBs now precede the main tree, which
    // the callback contract deliberately does not pin).
    let mut read = 0u64;
    for name in names_probe {
        let dbh = env
            .open_database(txn, Some(&name))?
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let st = dbh.stat(txn)?;
        es.named_db(&name, |ts| {
            for_each_entry_flagged(&dbh, txn, |k, _flags, v| ts.push(k, 0, v))
        })
        .map_err(stream_err)?;
        read += st.branch_pages + st.leaf_pages + st.overflow_pages;
        progress.advance_to(read);
    }

    // Main tree: user entries only (catalog records are regenerated by the
    // stream from the named-DB roots just built).
    let sink = es
        .finish_main(env.map_size(), |ms| {
            for_each_entry_flagged(&main, txn, |k, flags, v| {
                if flags & F_SUBDATA == 0 {
                    ms.push(k, v)
                } else {
                    Ok(())
                }
            })
        })
        .map_err(stream_err)?;
    read += main_pages;
    progress.advance_to(read);
    drop(sink); // close the temp file before renaming it

    // Last callback before `dest` is touched — see the panic contract on
    // `copy_to_file_with_progress`.
    progress.finish();
    std::fs::rename(&tmp, dest)?;
    guard.armed = false;
    Ok(())
}

/// Map a streaming-build failure to the public taxonomy.
fn stream_err(e: StreamBuildError) -> Error {
    match e {
        StreamBuildError::Page(_) => Error::Mdb(MdbError::Invalid),
        StreamBuildError::Io(io) => Error::Io(io),
    }
}

/// A [`PageSink`] over the destination file: one positioned write per page
/// (`pwrite`; writing past EOF extends). No userspace buffering, so there is
/// nothing to flush before the rename; durability matches the previous
/// implementation (no fsync — heed/LMDB `mdb_env_copy` parity).
struct FileSink {
    file: std::fs::File,
    psize: u32,
    next: u64,
}

impl PageSink for FileSink {
    fn alloc(&mut self, n: u64) -> u64 {
        let p = self.next;
        self.next += n;
        p
    }
    fn next_pgno(&self) -> u64 {
        self.next
    }
    fn emit(&mut self, pgno: u64, frame: &[u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(frame, pgno * self.psize as u64)
    }
}

/// Removes the temp file on drop unless disarmed (the rename succeeded).
struct TmpGuard<'p> {
    path: &'p Path,
    armed: bool,
}

impl Drop for TmpGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(self.path);
        }
    }
}
