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

use std::fs::File;
use std::path::{Path, PathBuf};

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
    /// Both modes stream the image into a sibling temp file
    /// (`<name>.copy-tmp-<pid>-<nonce>` in `path`'s directory); `path` itself
    /// is touched only by the final atomic `rename`, after the last callback.
    /// On any error — or a panicking callback — the temp file is removed by a
    /// drop guard.
    ///
    /// - `Disabled` (raw, **streamed since 2026-09-28**): the snapshot's data
    ///   pages are written straight from the map in large chunks, as LMDB's
    ///   `mdb_env_copyfd` writes from its map; no in-memory image.
    /// - `Enabled` (compacting, **streamed since PERF-GAP C1**): pages land
    ///   incrementally, bounded memory — O(tree depth × page size).
    ///
    /// In both modes therefore:
    ///
    /// - A panicking callback unwinds out of this function normally (panics
    ///   are not caught) and **`path` is left untouched** — absent if it did
    ///   not exist, and byte-for-byte its old contents if it did. There is no
    ///   half-written copy to mistake for a good one. (The rename makes this
    ///   hold even for a process kill mid-copy.)
    /// - The **source** environment is likewise untouched under any callback
    ///   behavior: a copy only ever reads it, under an internal read txn that
    ///   is released when this function returns (including while unwinding).
    ///   A panic leaks no reader slot.
    ///
    /// The cost of that guarantee is that progress tracks *source pages
    /// processed*, not bytes landed at `path`, and that the final stretch
    /// (the rename) is not covered by any callback. Callers wanting a progress bar that ends
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

    /// Copy this environment into an already-open `file`, the engine half of
    /// heed's `Env::copy_to_file(&mut File)` (`mdb_env_copyfd2`).
    ///
    /// The image is written starting at the file's current position, and the
    /// position is left at the end of the image, as LMDB's sequential writes
    /// leave it. Pages go straight into `file`: no temp file, no rename, no
    /// progress reporting, so a failure can leave a partial image in `file`
    /// (as with LMDB). Durability is the caller's concern, as for
    /// [`CopyToFile::copy_to_file`].
    ///
    /// # Errors
    ///
    /// As [`CopyToFile::copy_to_file`].
    fn copy_to_open_file(&self, file: &mut File, option: CompactionOption) -> Result<()>;
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
        let dest = path.as_ref();
        // The internal read txn pins the snapshot for the whole copy (SPEC 00
        // row 17: copy opens its own read txn).
        let txn = self.read_txn()?;
        let plan = Plan::new(self, &txn, option)?;
        let (tmp, file) = create_staging_file(dest)?;
        // Remove the temp on every non-rename exit (error or unwinding callback).
        let mut guard = TmpGuard {
            path: &tmp,
            armed: true,
        };
        let mut progress = Progress::start(plan.total, on_progress);
        plan.write(self, &txn, &file, 0, &mut progress)?;
        drop(file); // close the temp file before renaming it
                    // Last callback before `dest` is touched — see the panic contract on
                    // `copy_to_file_with_progress`.
        progress.finish();
        std::fs::rename(&tmp, dest)?;
        guard.armed = false;
        Ok(())
    }

    fn copy_to_open_file(&self, file: &mut File, option: CompactionOption) -> Result<()> {
        use std::io::{Seek, SeekFrom};
        let txn = self.read_txn()?;
        let plan = Plan::new(self, &txn, option)?;
        let base = file.stream_position()?;
        let mut silent = |_: CopyProgress| {};
        let mut progress = Progress::start(plan.total, &mut silent);
        let len = plan.write(self, &txn, file, base, &mut progress)?;
        file.seek(SeekFrom::Start(base + len))?;
        Ok(())
    }
}

/// What one copy will do, decided before any byte is written: the mode, the
/// progress total, and for the compacting mode the named DBs to rebuild.
struct Plan {
    option: CompactionOption,
    total: u64,
    names: Vec<Vec<u8>>,
}

impl Plan {
    fn new(env: &Env, txn: &RoTxn<'_>, option: CompactionOption) -> Result<Plan> {
        let snap = txn.snapshot();
        match option {
            // M2.3: the raw copy's page count is exact — every page in the
            // snapshot is copied verbatim.
            CompactionOption::Disabled => Ok(Plan {
                option,
                total: snap.last_pg + 1,
                names: Vec::new(),
            }),
            CompactionOption::Enabled => {
                refuse_custom_comparator(env)?;
                // M2.3: the compacting copy's unit of work is *reading* the
                // source's live pages; how many pages it writes is only known
                // once packing finishes, so `total` is the source's reachable
                // page count (see `CopyProgress::total`). The main tree's
                // record covers the catalog; each named DB's record is added
                // as its section is read.
                let mut total = snap.main_db.branch_pages
                    + snap.main_db.leaf_pages
                    + snap.main_db.overflow_pages;
                let names = named_databases(txn)?;
                for name in &names {
                    if let Some(dbh) = env.open_database(txn, Some(name))? {
                        let st = dbh.stat(txn)?;
                        total += st.branch_pages + st.leaf_pages + st.overflow_pages;
                    }
                }
                Ok(Plan {
                    option,
                    total,
                    names,
                })
            }
        }
    }

    /// Write the image into `file` at offset `base`; returns its length.
    fn write(
        self,
        env: &Env,
        txn: &RoTxn<'_>,
        file: &File,
        base: u64,
        progress: &mut Progress<'_>,
    ) -> Result<u64> {
        match self.option {
            CompactionOption::Disabled => write_raw(env, txn, file, base, progress),
            CompactionOption::Enabled => write_compact(env, txn, self.names, file, base, progress),
        }
    }
}

/// Create the sibling staging file for a copy to `dest`.
///
/// Staging file hygiene: `create_new` (O_CREAT|O_EXCL) never follows a
/// symlink planted at the staging path, `mode(0o600)` keeps the copy
/// owner-only like the engine's own env files, and the randomized suffix
/// (std `RandomState` is seeded from OS entropy) makes the name unpredictable
/// — retrying on `AlreadyExists` handles the astronomically unlikely collision
/// (and any pre-placed file at a guessed name). The destination path itself
/// is the caller's; the final rename replaces it.
fn create_staging_file(dest: &Path) -> Result<(PathBuf, File)> {
    use std::hash::{BuildHasher, Hasher};
    use std::os::unix::fs::OpenOptionsExt;
    let file_name = dest
        .file_name()
        .ok_or_else(|| Error::Io(std::io::Error::other("copy destination has no file name")))?;
    let mut attempt = 0u32;
    loop {
        let nonce = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        let mut tmp_name = file_name.to_os_string();
        tmp_name.push(format!(".copy-tmp-{}-{nonce:016x}", std::process::id()));
        let tmp = dest.with_file_name(tmp_name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(f) => return Ok((tmp, f)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 16 => {
                attempt += 1;
            }
            Err(e) => return Err(Error::Io(e)),
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
///
/// The data pages are written straight from the map in large positioned
/// writes, as LMDB's `mdb_env_copyfd` writes from its map: no in-memory image
/// (reachable pages are immutable under the reader pin; free pages are
/// harmless — see the module docs). Returns the image length.
fn write_raw(
    env: &Env,
    txn: &RoTxn<'_>,
    file: &File,
    base: u64,
    progress: &mut Progress<'_>,
) -> Result<u64> {
    use std::os::unix::fs::FileExt;
    let psize = env.page_size();
    let ps = psize as usize;
    let snap = txn.snapshot();
    // Pages 0..=last_pg. Slots 0/1 are synthesized below; the rest are data.
    let n_pages = snap.last_pg + 1;
    let data_end = (snap.last_pg as usize + 1) * ps;
    let map = txn.map_bytes();
    let src = map.get(..data_end).ok_or(Error::Mdb(MdbError::Invalid))?;

    // Synthesize both meta slots from the pinned snapshot — the copy is a
    // self-contained env at snapshot T, freelist preserved (SPEC 02 §3).
    let mut metas = vec![0u8; 2 * ps];
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
    meta.encode(&mut metas[0..ps]).map_err(corrupt)?;
    meta.pgno = 1;
    meta.encode(&mut metas[ps..2 * ps]).map_err(corrupt)?;
    file.write_all_at(&metas, base)?;

    // Data pages in chunks, so progress is observable. The chunk is sized
    // relative to the env (~`PROGRESS_STEPS` reports keep the callback rate
    // bounded and the resolution usable at every scale) and capped so one
    // write stays a bounded syscall. The chunk size is a reporting
    // granularity only and does not affect one byte of the output.
    const PROGRESS_STEPS: u64 = 32;
    const MAX_CHUNK_BYTES: u64 = 64 << 20;
    let pages_per_chunk = (n_pages / PROGRESS_STEPS).clamp(1, MAX_CHUNK_BYTES / u64::from(psize));
    let mut pg = FIRST_DATA_PGNO.min(n_pages);
    while pg < n_pages {
        let end = (pg + pages_per_chunk).min(n_pages);
        let (from, to) = (pg as usize * ps, end as usize * ps);
        file.write_all_at(&src[from..to], base + from as u64)?;
        pg = end;
        progress.advance_to(pg);
    }
    Ok(data_end as u64)
}

/// The M2.4 scope boundary (SPEC 03 §2.0): refuse a compacting copy of an env
/// with a custom key comparator.
///
/// The compacting rebuild goes through the bulk builder, whose ordering
/// contract is memcmp end to end: it packs the main catalog by
/// `sort_by(memcmp)` and debug-asserts strictly ascending memcmp order for
/// every DB it packs. Feeding it entries ordered by a caller's comparator
/// would produce a tree whose physical order is the comparator's but whose
/// builder-side reasoning assumed memcmp — and the resulting file records no
/// comparator identity, so nothing downstream (`zerodb-tools check`,
/// `dump`/`load`) could tell. Refusing loudly is the only honest option until
/// the builder, the dump format and the tools are made comparator-aware,
/// which is its own milestone.
///
/// `CompactionOption::Disabled` (the raw page copy) is unaffected: it is a
/// byte-level copy that preserves whatever order is on disk.
fn refuse_custom_comparator(env: &Env) -> Result<()> {
    if env.has_custom_comparator() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "compacting copy is not supported on an environment with a custom key comparator              (milestone 2.4, SPEC 03 §2.0); use CompactionOption::Disabled",
        )));
    }
    Ok(())
}

/// Compacting copy: read every live entry of every DB under the snapshot and
/// rebuild a fresh, densely-packed image (the `MDB_CP_COMPACT` shape),
/// streamed into `file` at `base` (PERF-GAP C1: peak memory O(tree depth ×
/// psize) plus the write buffer). Returns the image length.
fn write_compact(
    env: &Env,
    txn: &RoTxn<'_>,
    names: Vec<Vec<u8>>,
    file: &File,
    base: u64,
    progress: &mut Progress<'_>,
) -> Result<u64> {
    let psize = env.page_size();
    let snap = txn.snapshot();
    let main_pages =
        snap.main_db.branch_pages + snap.main_db.leaf_pages + snap.main_db.overflow_pages;
    let sink = FileSink::new(file, base, psize);
    let mut es = EnvStream::new(sink, psize, snap.txnid, DEFAULT_FILL_PERMILLE).map_err(corrupt)?;

    // Named DBs first (their roots feed the catalog), in name order — the
    // stream's contract and `named_databases`'s order. Progress advances per
    // source section read, monotone under the fixed total.
    let mut read = 0u64;
    for name in names {
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
    let main = env.main_database();
    let mut sink = es
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
    sink.flush()?;
    read += main_pages;
    progress.advance_to(read);
    Ok(sink.next * u64::from(psize))
}

/// Map a streaming-build failure to the public taxonomy.
fn stream_err(e: StreamBuildError) -> Error {
    match e {
        StreamBuildError::Page(_) => Error::Mdb(MdbError::Invalid),
        StreamBuildError::Io(io) => Error::Io(io),
    }
}

/// A [`PageSink`] over the destination file, writing at `base + pgno ×
/// psize`. Runs of consecutive pages are gathered into one buffer and landed
/// with a single positioned write (LMDB's compacting copy likewise writes
/// through a 1 MiB buffer, `MDB_WBUF`) instead of one `pwrite` per page. A
/// page that does not extend the current run flushes it first, so any
/// emission order lands correctly. [`FileSink::flush`] must run before the
/// file is used; durability matches LMDB's `mdb_env_copy` (no fsync).
struct FileSink<'f> {
    file: &'f File,
    base: u64,
    psize: u32,
    next: u64,
    /// First pgno of the buffered run (meaningful while `buf` is non-empty).
    run_start: u64,
    buf: Vec<u8>,
}

/// Bytes gathered before a flush.
const WRITE_BUF_BYTES: usize = 1 << 20;

impl<'f> FileSink<'f> {
    fn new(file: &'f File, base: u64, psize: u32) -> FileSink<'f> {
        FileSink {
            file,
            base,
            psize,
            next: FIRST_DATA_PGNO,
            run_start: 0,
            buf: Vec::with_capacity(WRITE_BUF_BYTES),
        }
    }

    /// Land the buffered run.
    fn flush(&mut self) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        if !self.buf.is_empty() {
            let off = self.base + self.run_start * u64::from(self.psize);
            self.file.write_all_at(&self.buf, off)?;
            self.buf.clear();
        }
        Ok(())
    }
}

impl PageSink for FileSink<'_> {
    fn alloc(&mut self, n: u64) -> u64 {
        let p = self.next;
        self.next += n;
        p
    }
    fn next_pgno(&self) -> u64 {
        self.next
    }
    fn emit(&mut self, pgno: u64, frame: &[u8]) -> std::io::Result<()> {
        let run_pages = (self.buf.len() / self.psize as usize) as u64;
        let extends = !self.buf.is_empty() && pgno == self.run_start + run_pages;
        if !extends || self.buf.len() + frame.len() > WRITE_BUF_BYTES {
            self.flush()?;
            self.run_start = pgno;
        }
        self.buf.extend_from_slice(frame);
        Ok(())
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
