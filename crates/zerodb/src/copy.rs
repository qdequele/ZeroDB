//! Compacting / raw environment copy — `Env::copy_to_file` parity (SPEC 00
//! row 17, `mdb_env_copy2`; ADR-0009).
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
//! `zerodb` crate. The `heed-zerodb` adapter maps heed's inherent
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
//!
//! ## Raw copies that read the data file instead of the map (ADR-0031)
//!
//! The same pin makes it safe for a raw copy to take the snapshot's pages
//! from the **data file** rather than from the map, with any method that
//! reads the file while the writer keeps committing:
//!
//! - **File copy** (Linux, both forms, when the destination is on the data
//!   file's own XFS, btrfs or bcachefs filesystem): `std::io::copy` of the
//!   file's first `(last_pg + 1) × page_size` bytes, which std performs with
//!   `copy_file_range`, which these filesystems turn into a block clone
//!   (reflink). Elsewhere `copy_file_range` copies bytes through a pipe,
//!   measured slower than streaming from the map, so the copy streams.
//! - **Clone** (macOS, a copy to a path without a progress callback):
//!   `std::fs::copy` of the whole file, which std performs with
//!   `fclonefileat` (an APFS clone that shares the file's blocks), and with
//!   a byte copy where cloning is unsupported. The clone is then trimmed to
//!   the snapshot's last page.
//!
//! Either way the copy's two meta pages are then overwritten with the ones
//! built for `T`, so whatever the file held at those offsets (a newer meta, a
//! meta being rewritten) is discarded, as is any page past `last_pg`.
//!
//! Why this reads `T`'s pages exactly: every page reachable from `T` was
//! written before `T` was published (with `pwrite`, or through the shared
//! map under `WRITE_MAP`), and the reader pin forbids rewriting it, in place
//! or otherwise (SPEC 04 TXN-62, which the `WRITE_MAP` in-place writes obey
//! too), until this copy's read transaction ends. On Linux and macOS a file's
//! page cache is the very memory a shared mapping maps, so `read`,
//! `copy_file_range` and the clone see those bytes whether or not they have
//! reached the disk; the clone and reflink paths write dirty cached pages
//! back before sharing blocks. A page that was free at `T` may change during
//! the copy and arrive torn — harmless, as above. Nothing here needs the copy
//! to be atomic, to pause the writer or to sync the source.
//!
//! The file is opened afresh by the path the env opened it at, and used only
//! when it is still the same file (device and inode) as the env's own
//! descriptor; otherwise, and for backings that are not a file, the copy
//! streams from the map.

use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
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

/// How far along a [`CopyToFile::copy_to_file_with_progress`] run is.
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

    /// As [`CopyToFile::copy_to_file`], reporting progress to `on_progress`.
    ///
    /// `on_progress` is called at least twice — once with `done == 0` before
    /// any work, once with `done == total` when the image is complete — and
    /// with monotonically non-decreasing `done` in between. `total` is
    /// identical in every call of a run. See [`CopyProgress`] for what the
    /// numbers mean in each mode.
    ///
    /// # Where the callback runs, and what a panic does
    ///
    /// Every callback fires **before a single byte reaches `path`** — a
    /// deliberate contract. Both modes stream the image into a sibling temp
    /// file (`<name>.copy-tmp-<pid>-<nonce>` in `path`'s directory); `path`
    /// itself is touched only by the final atomic `rename`, after the last
    /// callback. On any error — or a panicking callback — the temp file is
    /// removed by a drop guard.
    ///
    /// - `Disabled` (raw): the snapshot's data pages are written straight from
    ///   the map in large chunks, as LMDB's `mdb_env_copyfd` writes from its
    ///   map; no in-memory image. On Linux, onto the data file's own XFS,
    ///   btrfs or bcachefs filesystem, they are cloned from the data file in
    ///   the same chunks with `copy_file_range` instead (see the module
    ///   docs). This form never clones the whole file, which
    ///   [`CopyToFile::copy_to_file`] may do on macOS: a clone is one
    ///   filesystem call with nothing to report between its start and end.
    /// - `Enabled` (compacting): pages land incrementally, bounded memory —
    ///   O(tree depth × page size).
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
    /// (the rename) is not covered by any callback. Callers wanting a progress
    /// bar that ends exactly when the file is durable should treat
    /// `done == total` as "reading finished", not "file written".
    ///
    /// **Durability is the caller's concern** (LMDB `mdb_env_copy` parity,
    /// deliberate): no mode fsyncs the copy, and the
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
    /// leave it. A raw copy on Linux into a file on the data file's own XFS,
    /// btrfs or bcachefs filesystem clones the pages from the data file with
    /// `copy_file_range` (see the module docs); otherwise, and for a
    /// compacting copy, they are written from the map. Pages go straight into
    /// `file`: no temp file, no rename, no
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
        // The progress form's code path with a callback that does nothing, so
        // the no-callback signature (heed parity, SPEC 00 row 17) cannot
        // drift from the instrumented one. The one difference: with no
        // progress to report, a raw copy may clone the whole file.
        copy_to_path(
            self,
            path.as_ref(),
            option,
            &mut |_| {},
            |src, dest, data_end| path_route(src, dest, data_end, true),
        )
    }

    fn copy_to_file_with_progress(
        &self,
        path: impl AsRef<Path>,
        option: CompactionOption,
        on_progress: &mut dyn FnMut(CopyProgress),
    ) -> Result<()> {
        copy_to_path(
            self,
            path.as_ref(),
            option,
            on_progress,
            |src, dest, data_end| path_route(src, dest, data_end, false),
        )
    }

    fn copy_to_open_file(&self, file: &mut File, option: CompactionOption) -> Result<()> {
        copy_into_open_file(self, file, option, open_file_route)
    }
}

/// The body of [`CopyToFile::copy_to_file_with_progress`]; `pick` chooses how
/// a raw copy moves its pages (tests force each route through it).
fn copy_to_path(
    env: &Env,
    dest: &Path,
    option: CompactionOption,
    on_progress: &mut dyn FnMut(CopyProgress),
    pick: impl FnOnce(Option<&DataFile>, &Path, u64) -> RawRoute,
) -> Result<()> {
    // The internal read txn pins the snapshot for the whole copy (SPEC 00
    // row 17: copy opens its own read txn).
    let txn = env.read_txn()?;
    let plan = Plan::new(env, &txn, option)?;
    let src = plan.data_file(env);
    let route = pick(src.as_ref(), dest, raw_image_len(env, &txn));
    let mut progress = Progress::start(plan.total, on_progress);
    if let (RawRoute::Clone, Some(src)) = (route, &src) {
        if clone_raw(env, &txn, src, dest, &mut progress)? {
            return Ok(());
        }
        // The clone backed off (the path no longer names the env's data
        // file, or the clone came out short): stream from the map instead.
        return stream_to_path(env, &txn, plan, None, dest, &mut progress);
    }
    let src = src.filter(|_| route == RawRoute::FileCopy);
    stream_to_path(env, &txn, plan, src.as_ref(), dest, &mut progress)
}

/// Write the image into a sibling staging file and rename it over `dest`.
fn stream_to_path(
    env: &Env,
    txn: &RoTxn<'_>,
    plan: Plan,
    src: Option<&DataFile>,
    dest: &Path,
    progress: &mut Progress<'_>,
) -> Result<()> {
    let (tmp, file) = create_staging_file(dest)?;
    // Remove the temp on every non-rename exit (error or unwinding callback).
    let mut guard = TmpGuard {
        path: &tmp,
        armed: true,
    };
    plan.write(env, txn, &file, 0, src, progress)?;
    drop(file); // close the temp file before renaming it
                // Last callback before `dest` is touched — see the panic contract on
                // `copy_to_file_with_progress`.
    progress.finish();
    std::fs::rename(&tmp, dest)?;
    guard.armed = false;
    Ok(())
}

/// The body of [`CopyToFile::copy_to_open_file`]; `pick` as for
/// [`copy_to_path`].
fn copy_into_open_file(
    env: &Env,
    file: &mut File,
    option: CompactionOption,
    pick: impl FnOnce(Option<&DataFile>, &File) -> RawRoute,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    let txn = env.read_txn()?;
    let plan = Plan::new(env, &txn, option)?;
    let src = plan.data_file(env);
    let src = src.filter(|s| pick(Some(s), file) == RawRoute::FileCopy);
    let base = file.stream_position()?;
    let mut silent = |_: CopyProgress| {};
    let mut progress = Progress::start(plan.total, &mut silent);
    let len = plan.write(env, &txn, file, base, src.as_ref(), &mut progress)?;
    file.seek(SeekFrom::Start(base + len))?;
    Ok(())
}

/// How a raw copy moves the snapshot's pages (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawRoute {
    /// Positioned writes straight from the map, as LMDB's `mdb_env_copyfd`.
    Stream,
    /// `std::io::copy` of the image's bytes from the data file:
    /// `copy_file_range` on Linux (taken only where it clones blocks), a
    /// read/write loop elsewhere (tests only).
    FileCopy,
    /// `std::fs::copy` of the whole data file into a staging directory, then
    /// trimmed: `fclonefileat` on macOS.
    Clone,
}

/// The route of a raw copy to `dest`. `may_clone` is false when a progress
/// callback is watching (a clone has no intermediate progress).
fn path_route(src: Option<&DataFile>, dest: &Path, data_end: u64, may_clone: bool) -> RawRoute {
    let Some(src) = src else {
        return RawRoute::Stream;
    };
    if cfg!(target_os = "linux") {
        // The staging file is created in `dest`'s directory.
        let dir = match dest.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        let dev = std::fs::metadata(dir).ok().map(|m| m.dev());
        if src.reflinks_to(dev) {
            RawRoute::FileCopy
        } else {
            RawRoute::Stream
        }
    } else if cfg!(target_os = "macos") && may_clone && clone_is_bounded(src.len, data_end) {
        RawRoute::Clone
    } else {
        RawRoute::Stream
    }
}

/// The route of a raw copy into the open `file`: only Linux has an in-kernel
/// copy into an arbitrary descriptor through std (`copy_file_range`).
fn open_file_route(src: Option<&DataFile>, file: &File) -> RawRoute {
    let dev = file.metadata().ok().map(|m| m.dev());
    match src {
        Some(src) if cfg!(target_os = "linux") && src.reflinks_to(dev) => RawRoute::FileCopy,
        _ => RawRoute::Stream,
    }
}

/// Filesystems on which Linux `copy_file_range` within one filesystem shares
/// blocks (a reflink) instead of copying bytes. Elsewhere (ext4, tmpfs,
/// overlayfs, NFS, ...) the kernel copies the bytes through a pipe, which was
/// measured slower than writing from the map (ext4: about twice the time).
const REFLINK_FILESYSTEMS: [&str; 3] = ["xfs", "btrfs", "bcachefs"];

/// The type of the filesystem mounted at the deepest mount point containing
/// `path` (an absolute, canonical path), from `/proc/self/mountinfo`, or
/// `None` if it cannot be read. The last matching line wins: a later mount
/// over the same point hides the earlier one.
fn filesystem_type(path: &Path) -> Option<String> {
    let info = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<(usize, String)> = None;
    for line in info.lines() {
        // `id parent major:minor root mount-point options... - type source ...`
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let (Some(mount_point), Some(fs_type)) = (left.split(' ').nth(4), right.split(' ').next())
        else {
            continue;
        };
        let mount_point = unescape_mountinfo(mount_point);
        let depth = Path::new(&mount_point).components().count();
        if path.starts_with(&mount_point) && best.as_ref().is_none_or(|(d, _)| depth >= *d) {
            best = Some((depth, fs_type.to_owned()));
        }
    }
    best.map(|(_, fs_type)| fs_type)
}

/// Undo mountinfo's octal escapes (`\040` for a space, and so on).
fn unescape_mountinfo(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let octal = bytes.get(i + 1..i + 4).and_then(|d| {
            let d = std::str::from_utf8(d).ok()?;
            u8::from_str_radix(d, 8).ok()
        });
        match (bytes[i], octal) {
            (b'\\', Some(b)) => {
                out.push(b);
                i += 4;
            }
            (b, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether cloning a data file of `file_len` bytes for an image of
/// `data_end` bytes is bounded by the streaming copy's cost.
///
/// `std::fs::copy` does not say whether it cloned. Where it cannot (the
/// destination is on another volume, or the filesystem has no clones) it
/// byte-copies the **whole** file, which may be far longer than the
/// snapshot: under `WRITE_MAP` the file is as long as the map. The clone is
/// therefore attempted only when such a fallback would copy at most a
/// quarter more bytes than the streaming copy writes.
fn clone_is_bounded(file_len: u64, data_end: u64) -> bool {
    file_len <= data_end.saturating_add(data_end / 4)
}

/// The env's data file, opened afresh for a raw copy that reads the file
/// instead of the map, at its own file offset (a `dup` of the env's
/// descriptor would share the env's offset with any concurrent copy).
struct DataFile {
    path: PathBuf,
    file: File,
    /// The file's length when it was opened.
    len: u64,
    /// Device and inode, which identify the file the env maps.
    id: (u64, u64),
    /// Whether the file is on a filesystem where `copy_file_range` within
    /// the filesystem clones blocks (Linux only; see [`REFLINK_FILESYSTEMS`]).
    reflinks: bool,
}

impl DataFile {
    /// Open the file at the env's data path, or `None` if there is none or it
    /// is no longer the file the env maps (renamed or replaced since the
    /// open). Any failure means "stream from the map", never an error.
    ///
    /// Opening and closing a second descriptor of the data file releases no
    /// lock: the env holds its POSIX record locks on the lock file only.
    fn open(env: &Env) -> Option<DataFile> {
        let path = env.data_path()?.to_path_buf();
        let file = File::open(&path).ok()?;
        let ours = env.try_clone_inner_file().ok()?.metadata().ok()?;
        let theirs = file.metadata().ok()?;
        let id = (ours.dev(), ours.ino());
        if !theirs.is_file() || (theirs.dev(), theirs.ino()) != id {
            return None;
        }
        let reflinks = cfg!(target_os = "linux")
            && filesystem_type(&path).is_some_and(|t| REFLINK_FILESYSTEMS.contains(&t.as_str()));
        Some(DataFile {
            path,
            file,
            len: theirs.len(),
            id,
            reflinks,
        })
    }

    /// Whether `copy_file_range` from this file to a file on device `dev`
    /// clones blocks: both on the same filesystem, and one that reflinks.
    fn reflinks_to(&self, dev: Option<u64>) -> bool {
        self.reflinks && dev == Some(self.id.0)
    }

    /// Whether `path` still names this file.
    fn path_still_names_it(&self) -> bool {
        std::fs::metadata(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.id)
    }
}

/// The byte length of a raw image of `txn`'s snapshot.
fn raw_image_len(env: &Env, txn: &RoTxn<'_>) -> u64 {
    (txn.snapshot().last_pg + 1) * u64::from(env.page_size())
}

/// Clone the env's data file to `dest` (macOS: `fclonefileat`), then make the
/// clone the raw image of `txn`'s snapshot: its two meta pages overwritten
/// with the snapshot's, and its length trimmed to the snapshot's last page.
///
/// The clone lands in a fresh owner-only staging directory beside `dest`
/// (`fclonefileat` refuses an existing destination, and the directory keeps
/// anything else from being placed at the clone's path), and is renamed over
/// `dest` once complete, so `dest` is never seen half-written.
///
/// Returns `Ok(false)` without touching `dest` when the result could not be
/// the snapshot's image: the data path named another file by the time it
/// was cloned, or the clone is shorter than the image. The caller then
/// streams from the map.
fn clone_raw(
    env: &Env,
    txn: &RoTxn<'_>,
    src: &DataFile,
    dest: &Path,
    progress: &mut Progress<'_>,
) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let data_end = raw_image_len(env, txn);
    let dir = create_staging_dir(dest)?;
    let staged = dir.join("data");
    let _guard = StagingDirGuard {
        dir: &dir,
        file: &staged,
    };
    std::fs::copy(&src.path, &staged)?;
    if !src.path_still_names_it() {
        return Ok(false);
    }
    let file = std::fs::OpenOptions::new().write(true).open(&staged)?;
    // The clone carries the source's mode; make it owner-only, as the
    // streaming copy's staging file is.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    if file.metadata()?.len() < data_end {
        return Ok(false);
    }
    file.set_len(data_end)?;
    file.write_all_at(&snapshot_metas(env, txn)?, 0)?;
    drop(file);
    // Last callback before `dest` is touched, as on the streaming path.
    progress.finish();
    std::fs::rename(&staged, dest)?;
    Ok(true)
}

/// Create a fresh owner-only (0700) staging directory beside `dest`, named
/// as [`create_staging_file`] names its file.
fn create_staging_dir(dest: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    let mut attempt = 0u32;
    loop {
        let tmp = staging_name(dest)?;
        match std::fs::DirBuilder::new().mode(0o700).create(&tmp) {
            Ok(()) => return Ok(tmp),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 16 => {
                attempt += 1;
            }
            Err(e) => return Err(Error::Io(e)),
        }
    }
}

/// Removes a clone's staging file (if still there) and directory on drop.
struct StagingDirGuard<'p> {
    dir: &'p Path,
    file: &'p Path,
}

impl Drop for StagingDirGuard<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.file);
        let _ = std::fs::remove_dir(self.dir);
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
            // Progress: the raw copy's page count is exact — every page in the
            // snapshot is copied verbatim.
            CompactionOption::Disabled => Ok(Plan {
                option,
                total: snap.last_pg + 1,
                names: Vec::new(),
            }),
            CompactionOption::Enabled => {
                refuse_custom_comparator(env)?;
                // Progress: the compacting copy's unit of work is *reading* the
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

    /// The env's data file, for a raw copy that may read it (see
    /// [`DataFile::open`]); `None` for a compacting copy.
    fn data_file(&self, env: &Env) -> Option<DataFile> {
        match self.option {
            CompactionOption::Disabled => DataFile::open(env),
            CompactionOption::Enabled => None,
        }
    }

    /// Write the image into `file` at offset `base`, which must be `file`'s
    /// position when `src` is given; returns its length. A raw copy copies
    /// its pages from `src` when given, from the map otherwise.
    fn write(
        self,
        env: &Env,
        txn: &RoTxn<'_>,
        file: &File,
        base: u64,
        src: Option<&DataFile>,
        progress: &mut Progress<'_>,
    ) -> Result<u64> {
        match (self.option, src) {
            (CompactionOption::Disabled, Some(src)) => {
                copy_raw_file(env, txn, src, file, base, progress)
            }
            (CompactionOption::Disabled, None) => write_raw(env, txn, file, base, progress),
            (CompactionOption::Enabled, _) => {
                write_compact(env, txn, self.names, file, base, progress)
            }
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
    use std::os::unix::fs::OpenOptionsExt;
    let mut attempt = 0u32;
    loop {
        let tmp = staging_name(dest)?;
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

/// A fresh staging name beside `dest`: `<name>.copy-tmp-<pid>-<nonce>`.
fn staging_name(dest: &Path) -> Result<PathBuf> {
    use std::hash::{BuildHasher, Hasher};
    let file_name = dest
        .file_name()
        .ok_or_else(|| Error::Io(std::io::Error::other("copy destination has no file name")))?;
    let nonce = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(format!(".copy-tmp-{}-{nonce:016x}", std::process::id()));
    Ok(dest.with_file_name(tmp_name))
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
    let psize = env.page_size();
    let ps = psize as usize;
    let snap = txn.snapshot();
    // Pages 0..=last_pg. Slots 0/1 are synthesized; the rest are data.
    let n_pages = snap.last_pg + 1;
    let data_end = (snap.last_pg as usize + 1) * ps;
    let map = txn.map_bytes();
    let src = map.get(..data_end).ok_or(Error::Mdb(MdbError::Invalid))?;

    file.write_all_at(&snapshot_metas(env, txn)?, base)?;

    // Data pages in chunks, so progress is observable.
    let pages_per_chunk = pages_per_chunk(n_pages, psize);
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

/// Raw copy from the data file (see the module docs): the image's bytes
/// `[0, (last_pg + 1) × page_size)` copied from `src` with `std::io::copy`
/// into `file` at its current position `base`, in the streaming copy's
/// chunks, then the two meta pages overwritten with the snapshot's. Returns
/// the image length.
fn copy_raw_file(
    env: &Env,
    txn: &RoTxn<'_>,
    src: &DataFile,
    file: &File,
    base: u64,
    progress: &mut Progress<'_>,
) -> Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let psize = env.page_size();
    let n_pages = txn.snapshot().last_pg + 1;
    let chunk_pages = pages_per_chunk(n_pages, psize);
    let mut reader = &src.file;
    reader.seek(SeekFrom::Start(0))?;
    let mut writer = file;
    let mut pg = 0;
    while pg < n_pages {
        let end = (pg + chunk_pages).min(n_pages);
        let want = (end - pg) * u64::from(psize);
        // `File` to `File`: std copies in the kernel where it can.
        let got = std::io::copy(&mut reader.take(want), &mut writer)?;
        if got != want {
            // The data file is shorter than the snapshot it holds: corrupt.
            return Err(Error::Mdb(MdbError::Invalid));
        }
        pg = end;
        progress.advance_to(pg);
    }
    // The file's own meta pages were copied too; replace them.
    file.write_all_at(&snapshot_metas(env, txn)?, base)?;
    Ok(n_pages * u64::from(psize))
}

/// Pages per chunk of a raw copy. The chunk is sized relative to the env
/// (~`PROGRESS_STEPS` reports keep the callback rate bounded and the
/// resolution usable at every scale) and capped so one write stays a bounded
/// syscall. It is a reporting granularity only and does not affect one byte
/// of the output.
fn pages_per_chunk(n_pages: u64, psize: u32) -> u64 {
    const PROGRESS_STEPS: u64 = 32;
    const MAX_CHUNK_BYTES: u64 = 64 << 20;
    (n_pages / PROGRESS_STEPS).clamp(1, MAX_CHUNK_BYTES / u64::from(psize))
}

/// The raw image's two meta slots, both built for `txn`'s snapshot, so the
/// copy opens at exactly that snapshot (SPEC 00 row 17).
fn snapshot_metas(env: &Env, txn: &RoTxn<'_>) -> Result<Vec<u8>> {
    let psize = env.page_size();
    let ps = psize as usize;
    let snap = txn.snapshot();
    // Synthesize both meta slots from the pinned snapshot — the copy is a
    // self-contained env at snapshot T, freelist preserved (SPEC 02 §3).
    // Format v2 (ADR-0022): the snapshot's free-list annex rides along —
    // the ids are pinned in `Snapshot` (the live env's slot may already
    // hold a newer meta, TXN-63), and dropping them would leak those pages
    // in the copy (INV-22/INV-28).
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
        fl_count: snap.free_annex.len() as u32,
    };
    meta.encode_with_annex(&mut metas[0..ps], &snap.free_annex)
        .map_err(corrupt)?;
    meta.pgno = 1;
    meta.encode_with_annex(&mut metas[ps..2 * ps], &snap.free_annex)
        .map_err(corrupt)?;
    Ok(metas)
}

/// The custom-comparator scope boundary (SPEC 03 §2.0): refuse a compacting
/// copy of an env with a custom key comparator.
///
/// The compacting rebuild goes through the bulk builder, whose ordering
/// contract is memcmp end to end: it packs the main catalog by
/// `sort_by(memcmp)` and debug-asserts strictly ascending memcmp order for
/// every DB it packs. Feeding it entries ordered by a caller's comparator
/// would produce a tree whose physical order is the comparator's but whose
/// builder-side reasoning assumed memcmp — and the resulting file records no
/// comparator identity, so nothing downstream (`zerodb-tools check`,
/// `dump`/`load`) could tell. Refusing loudly is the only honest option while
/// the builder, the dump format and the tools are memcmp-only.
///
/// `CompactionOption::Disabled` (the raw page copy) is unaffected: it is a
/// byte-level copy that preserves whatever order is on disk.
fn refuse_custom_comparator(env: &Env) -> Result<()> {
    if env.has_custom_comparator() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "compacting copy is not supported on an environment with a custom key comparator (SPEC 03 §2.0); use CompactionOption::Disabled",
        )));
    }
    Ok(())
}

/// Compacting copy: read every live entry of every DB under the snapshot and
/// rebuild a fresh, densely-packed image (the `MDB_CP_COMPACT` shape),
/// streamed into `file` at `base` (peak memory O(tree depth × psize) plus the
/// write buffer; see docs/PERF-GAP-VS-LMDB.md, compaction RAM). Returns the
/// image length.
///
/// The walk and the writes overlap, as in LMDB's compacting copy
/// (`mdb_env_copyfd1` hands full buffers to a writer thread): this thread
/// walks and packs, a scoped writer thread lands each full buffer, and
/// buffers go back and forth over two channels (at most [`WRITE_BUFS`] exist).
/// The first write error stops the writer and is the error returned; a
/// panicking progress callback unwinds normally (dropping the sender ends the
/// writer, and the scope joins it).
fn write_compact(
    env: &Env,
    txn: &RoTxn<'_>,
    names: Vec<Vec<u8>>,
    file: &File,
    base: u64,
    progress: &mut Progress<'_>,
) -> Result<u64> {
    use std::sync::mpsc;
    std::thread::scope(|scope| {
        // Bounded: one buffer queued, one being written, one being filled.
        let (full_tx, full_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(WRITE_BUFS - 2);
        let (free_tx, free_rx) = mpsc::channel::<Vec<u8>>();
        let writer = scope.spawn(move || -> std::io::Result<()> {
            for (off, mut buf) in full_rx {
                file.write_all_at(&buf, off)?;
                buf.clear();
                // The sink may already be gone (last buffer): nothing to do.
                let _ = free_tx.send(buf);
            }
            Ok(())
        });
        let built = build_compact(
            env,
            txn,
            names,
            FileSink::new(full_tx, free_rx, base, env.page_size()),
            progress,
        );
        // `build_compact` consumed the sink, so the sender is dropped and the
        // writer finishes its queue. A write error outranks the build's own
        // (which is then only "the writer stopped").
        match writer.join() {
            Ok(Ok(())) => built,
            Ok(Err(e)) => Err(Error::Io(e)),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}

/// The walk-and-pack half of [`write_compact`], feeding `sink`.
fn build_compact(
    env: &Env,
    txn: &RoTxn<'_>,
    names: Vec<Vec<u8>>,
    sink: FileSink,
    progress: &mut Progress<'_>,
) -> Result<u64> {
    let psize = env.page_size();
    let snap = txn.snapshot();
    let main_pages =
        snap.main_db.branch_pages + snap.main_db.leaf_pages + snap.main_db.overflow_pages;
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

/// A [`PageSink`] that hands the destination's pages to the copy's writer
/// thread (see [`write_compact`]), to land at `base + pgno × psize`. Runs of
/// consecutive pages are gathered into one buffer and sent as a single
/// positioned write (LMDB's compacting copy likewise writes through 1 MiB
/// buffers, `MDB_WBUF`) instead of one `pwrite` per page. A page that does
/// not extend the current run sends it first, so any emission order lands
/// correctly (the writer applies buffers in order). [`FileSink::flush`] sends
/// the last run; durability matches LMDB's `mdb_env_copy` (no fsync).
struct FileSink {
    full: std::sync::mpsc::SyncSender<(u64, Vec<u8>)>,
    free: std::sync::mpsc::Receiver<Vec<u8>>,
    base: u64,
    psize: u32,
    next: u64,
    /// First pgno of the buffered run (meaningful while `buf` is non-empty).
    run_start: u64,
    buf: Vec<u8>,
}

/// Bytes gathered before a buffer goes to the writer.
const WRITE_BUF_BYTES: usize = 1 << 20;
/// Buffers in flight at most: one filling, one queued, one being written.
const WRITE_BUFS: usize = 3;

impl FileSink {
    fn new(
        full: std::sync::mpsc::SyncSender<(u64, Vec<u8>)>,
        free: std::sync::mpsc::Receiver<Vec<u8>>,
        base: u64,
        psize: u32,
    ) -> FileSink {
        FileSink {
            full,
            free,
            base,
            psize,
            next: FIRST_DATA_PGNO,
            run_start: 0,
            buf: Vec::with_capacity(WRITE_BUF_BYTES),
        }
    }

    /// Send the buffered run to the writer and take a recycled buffer (or a
    /// new one while fewer than [`WRITE_BUFS`] exist).
    fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let off = self.base + self.run_start * u64::from(self.psize);
        let next = self
            .free
            .try_recv()
            .unwrap_or_else(|_| Vec::with_capacity(WRITE_BUF_BYTES));
        let full = std::mem::replace(&mut self.buf, next);
        // A send fails only once the writer has stopped on an I/O error;
        // `write_compact` reports that error instead of this one.
        self.full
            .send((off, full))
            .map_err(|_| std::io::Error::other("copy writer stopped"))
    }
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

#[cfg(test)]
#[path = "copy_tests.rs"]
mod tests;
