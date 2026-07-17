//! Environment surface — `EnvOpenOptions<T>`, `Env<T>`, `EnvInfo`, `EnvStat`,
//! `EnvClosingEvent`, `CompactionOption`, `FlagSetMode`, the comparators, and
//! `env_closing_event` (SPEC 00 rows 1–26; SPEC 04 §7; ADR-0003 §C2/§C3/§C4).
//!
//! Everything wraps ZeroDB's native [`zerodb::Env`] (already `Arc`-cloneable,
//! registry-deduped with `EnvAlreadyOpened`, and deferred-close, TXN-50..53).
//! The adapter re-imposes three cheap fork boundary behaviors ZeroDB is lenient
//! about (D-006/D-008/D-010) so the 1.14 gate sees exact parity — each is
//! checked here, at the heed boundary, and covered by a test.

use std::cmp::Ordering;
use std::ffi::c_void;
use std::fs::File;
use std::path::Path;
use std::ptr;
use std::time::Duration;

use heed_traits::{Comparator, LexicographicComparator};

use crate::flags::EnvFlags;
use crate::txn::{RoTxn, RwTxn, TlsUsage, WithTls, WithoutTls};
use crate::{Database, DatabaseOpenOptions, Error, Result, Unspecified};

/// The OS page size (`sysconf(_SC_PAGESIZE)`), for the D-006 boundary check.
fn os_page_size() -> usize {
    // SAFETY (adapter boundary, D-006): `sysconf` with a valid name is a pure
    // query with no memory effects; the return is the page size (>0) or -1 on
    // failure, which we clamp to a safe default. This is the sole FFI call in
    // the adapter and mirrors heed's `page_size` crate dependency, used only to
    // reproduce the fork's `map_size` OS-page-multiple rejection.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v > 0 {
        v as usize
    } else {
        4096
    }
}

// ---------------------------------------------------------------------------
// EnvOpenOptions
// ---------------------------------------------------------------------------

/// Builder for opening an [`Env`] — the exact `heed::EnvOpenOptions<T>` shape
/// (default `WithTls`, immediately converted via `read_txn_without_tls` in
/// every production open; SPEC 00 rows 1/2).
#[derive(Clone)]
pub struct EnvOpenOptions<T: TlsUsage = WithTls> {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: u32,
    flags: EnvFlags,
    _tls: std::marker::PhantomData<T>,
}

impl Default for EnvOpenOptions<WithTls> {
    fn default() -> EnvOpenOptions<WithTls> {
        EnvOpenOptions::new()
    }
}

impl EnvOpenOptions<WithTls> {
    /// A fresh builder (SPEC 00 row 1). Default is `WithTls`.
    #[must_use]
    pub fn new() -> EnvOpenOptions<WithTls> {
        EnvOpenOptions {
            map_size: None,
            max_readers: None,
            max_dbs: 0,
            flags: EnvFlags::empty(),
            _tls: std::marker::PhantomData,
        }
    }
}

impl<T: TlsUsage> EnvOpenOptions<T> {
    fn retag<U: TlsUsage>(self) -> EnvOpenOptions<U> {
        EnvOpenOptions {
            map_size: self.map_size,
            max_readers: self.max_readers,
            max_dbs: self.max_dbs,
            flags: self.flags,
            _tls: std::marker::PhantomData,
        }
    }

    /// Select TLS-backed read txns (`WithTls`). Compile-only shim in Phase 1.
    #[must_use]
    pub fn read_txn_with_tls(self) -> EnvOpenOptions<WithTls> {
        self.retag()
    }

    /// Select NOTLS read txns (`WithoutTls`) — `RoTxn: Send` (SPEC 00 row 2).
    #[must_use]
    pub fn read_txn_without_tls(self) -> EnvOpenOptions<WithoutTls> {
        self.retag()
    }

    /// Set the map size (SPEC 00 row 3).
    pub fn map_size(&mut self, size: usize) -> &mut Self {
        self.map_size = Some(size);
        self
    }

    /// Set the reader-table size (SPEC 00 row 5).
    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers);
        self
    }

    /// Set the named-DB catalog capacity (SPEC 00 row 4).
    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = dbs;
        self
    }

    /// Set the env flags (SPEC 00 row 6). `unsafe` for heed signature parity —
    /// the unsafety is vestigial for ZeroDB (no reachable flag enables the
    /// cross-process behaviors that make this unsafe in LMDB, D-001); the body
    /// contains no unsafe operation.
    ///
    /// # Safety
    ///
    /// Mirrors `heed::EnvOpenOptions::flags`; safe in practice for ZeroDB.
    pub unsafe fn flags(&mut self, flags: EnvFlags) -> &mut Self {
        self.flags = flags;
        self
    }

    /// Open (or create) the environment at `path` (a directory; SPEC 00 row 7).
    /// `unsafe` for heed signature parity (see [`EnvOpenOptions::flags`]).
    ///
    /// Re-imposes the fork's open-time boundaries the native engine is lenient
    /// about: `max_readers(0)` → `Io(InvalidInput)` (D-010), and `map_size` not
    /// an OS-page multiple → `Io(InvalidInput)` (D-006).
    ///
    /// # Safety
    ///
    /// Mirrors `heed::EnvOpenOptions::open`; safe in practice for ZeroDB.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] (boundary rejections, missing dir, I/O), `Error::Mdb`
    /// (`Invalid`) on a bad store, [`Error::EnvAlreadyOpened`] (TXN-51).
    pub unsafe fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env<T>> {
        // D-010: the fork rejects `max_readers(0)` with EINVAL at open.
        if self.max_readers == Some(0) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_readers must be greater than zero",
            )));
        }
        // D-006: the fork rejects a `map_size` that is not a multiple of the OS
        // page size.
        if let Some(ms) = self.map_size {
            let page = os_page_size();
            if ms % page != 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("map size ({ms}) must be a multiple of the system page size ({page})"),
                )));
            }
        }

        let mut opts = zerodb::EnvOpenOptions::new();
        if let Some(ms) = self.map_size {
            opts.map_size(ms);
        }
        opts.max_dbs(self.max_dbs);
        if let Some(r) = self.max_readers {
            opts.max_readers(r);
        }
        opts.flags(zerodb_env_flags(self.flags));
        let env = opts.open(path).map_err(Error::from)?;
        Ok(Env {
            inner: env,
            _tls: std::marker::PhantomData,
        })
    }
}

/// Translate heed `EnvFlags` → ZeroDB `EnvFlags` (SPEC 01 Table 1). Only the
/// Phase-1 flags carry semantics; the rest (cross-process, no-op under D-001)
/// are dropped.
fn zerodb_env_flags(flags: EnvFlags) -> zerodb::EnvFlags {
    let mut z = zerodb::EnvFlags::EMPTY;
    if flags.contains(EnvFlags::WRITE_MAP) {
        z |= zerodb::EnvFlags::WRITE_MAP;
    }
    if flags.contains(EnvFlags::PREV_SNAPSHOT) {
        z |= zerodb::EnvFlags::PREV_SNAPSHOT;
    }
    if flags.contains(EnvFlags::READ_ONLY) {
        z |= zerodb::EnvFlags::READ_ONLY;
    }
    if flags.contains(EnvFlags::NO_SYNC) {
        z |= zerodb::EnvFlags::NO_SYNC;
    }
    if flags.contains(EnvFlags::NO_META_SYNC) {
        z |= zerodb::EnvFlags::NO_META_SYNC;
    }
    if flags.contains(EnvFlags::MAP_ASYNC) {
        z |= zerodb::EnvFlags::MAP_ASYNC;
    }
    z
}

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------

/// A cheaply-cloneable environment handle — the exact `heed::Env<T>` shape
/// (SPEC 00 row 25; `Arc`-backed shared ownership, TXN-50). `T` is a phantom
/// TLS marker; ZeroDB read txns are universally NOTLS.
pub struct Env<T = WithTls> {
    inner: zerodb::Env,
    _tls: std::marker::PhantomData<T>,
}

impl<T> Clone for Env<T> {
    fn clone(&self) -> Env<T> {
        Env {
            inner: self.inner.clone(),
            _tls: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for Env<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env")
            .field("path", &self.inner.path())
            .finish_non_exhaustive()
    }
}

// No `T: TlsUsage` bound — heed's `Env<T>` methods are available for any `T`
// (consumers like cellulite call them through an unbounded generic parameter).
impl<T> Env<T> {
    /// The native ZeroDB env behind this handle.
    pub(crate) fn zdb(&self) -> &zerodb::Env {
        &self.inner
    }

    /// Actual on-disk data-file size (SPEC 00 row 18).
    ///
    /// # Errors
    ///
    /// Propagates the `fstat` I/O error.
    pub fn real_disk_size(&self) -> Result<u64> {
        self.inner.real_disk_size().map_err(Into::into)
    }

    /// `dup()` the data-file fd (SPEC 00 row 22).
    ///
    /// # Errors
    ///
    /// Propagates the `dup` I/O error.
    pub fn try_clone_inner_file(&self) -> Result<File> {
        self.inner.try_clone_inner_file().map_err(Into::into)
    }

    /// The env flags as set at open (SPEC 00 second table — SHOULD). Returns the
    /// durability/write-mode bits ZeroDB records.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for heed shape.
    pub fn flags(&self) -> Result<Option<EnvFlags>> {
        Ok(Some(self.get_flags_bits()))
    }

    fn get_flags_bits(&self) -> EnvFlags {
        let d = self.inner.durability();
        let mut f = EnvFlags::empty();
        if d.write_map {
            f |= EnvFlags::WRITE_MAP;
        }
        if d.read_only {
            f |= EnvFlags::READ_ONLY;
        }
        if d.no_sync {
            f |= EnvFlags::NO_SYNC;
        }
        if d.no_meta_sync {
            f |= EnvFlags::NO_META_SYNC;
        }
        if d.map_async {
            f |= EnvFlags::MAP_ASYNC;
        }
        f
    }

    /// The raw env flag bits (SPEC 00 second table — SHOULD).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for heed shape.
    pub fn get_flags(&self) -> Result<u32> {
        Ok(self.get_flags_bits().bits())
    }

    /// Environment info (SPEC 00 rows 20/60). Only `map_size` is load-bearing.
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        EnvInfo {
            map_addr: ptr::null_mut(),
            map_size: self.inner.map_size() as usize,
            last_page_number: 0,
            last_txn_id: self.inner.txnid() as usize,
            maximum_number_of_readers: 0,
            number_of_readers: 0,
        }
    }

    /// Env-level statistics (SPEC 00 second table — SHOULD): the main DB's stat.
    #[must_use]
    pub fn stat(&self) -> EnvStat {
        let page_size = self.inner.page_size();
        match self.inner.read_txn() {
            Ok(rtxn) => match self.inner.main_database().stat(&rtxn) {
                Ok(s) => EnvStat {
                    page_size,
                    depth: u32::from(s.depth),
                    branch_pages: s.branch_pages as usize,
                    leaf_pages: s.leaf_pages as usize,
                    overflow_pages: s.overflow_pages as usize,
                    entries: s.entries as usize,
                },
                Err(_) => EnvStat::empty(page_size),
            },
            Err(_) => EnvStat::empty(page_size),
        }
    }

    /// `non_free_pages_size()` (SPEC 00 row 19).
    ///
    /// # Errors
    ///
    /// [`Error::Io`] from `fstat`; `Mdb(Invalid)` on a corrupt GC DB.
    pub fn non_free_pages_size(&self) -> Result<u64> {
        self.inner.non_free_pages_size().map_err(Into::into)
    }

    /// A typed database open-options builder (SPEC 00 row 12).
    #[must_use]
    pub fn database_options(
        &self,
    ) -> DatabaseOpenOptions<'_, 'static, T, Unspecified, Unspecified> {
        DatabaseOpenOptions::new(self)
    }

    /// Open a typed database that already exists (SPEC 00 rows 10/12).
    ///
    /// # Errors
    ///
    /// [`Error::Mdb`] (`BadValSize`/`Incompatible`) on a bad/colliding name;
    /// `Error::Io` on an embedded NUL in `name` (D-008 boundary).
    pub fn open_database<KC, DC>(
        &self,
        rtxn: &RoTxn,
        name: Option<&str>,
    ) -> Result<Option<Database<KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut options = self.database_options().types::<KC, DC>();
        if let Some(name) = name {
            options.name(name);
        }
        options.open(rtxn)
    }

    /// Create a typed database, if absent (SPEC 00 rows 11/12).
    ///
    /// # Errors
    ///
    /// As [`Env::open_database`], plus `Mdb(DbsFull)` when the catalog is full.
    pub fn create_database<KC, DC>(
        &self,
        wtxn: &mut RwTxn,
        name: Option<&str>,
    ) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut options = self.database_options().types::<KC, DC>();
        if let Some(name) = name {
            options.name(name);
        }
        options.create(wtxn)
    }

    /// Begin a write transaction (SPEC 00 row 13).
    ///
    /// # Errors
    ///
    /// `Io`(`EACCES`) on a read-only env; `Io` if the env is poisoned.
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        let w = self.inner.write_txn().map_err(Error::from)?;
        Ok(RwTxn::from_zdb(w))
    }

    /// Begin a read transaction over the live snapshot (SPEC 00 row 14).
    ///
    /// # Errors
    ///
    /// `Mdb(ReadersFull)` when the reader table is exhausted.
    pub fn read_txn(&self) -> Result<RoTxn<'_, T>> {
        let r = self.inner.read_txn().map_err(Error::from)?;
        Ok(RoTxn::from_inner(crate::txn::InnerTxn::Ro(r)))
    }

    /// Begin a `'static`, env-owning read transaction (SPEC 00 row 15).
    ///
    /// # Errors
    ///
    /// `Mdb(ReadersFull)` when the reader table is exhausted.
    pub fn static_read_txn(self) -> Result<RoTxn<'static, T>> {
        let r = self.inner.static_read_txn().map_err(Error::from)?;
        Ok(RoTxn::from_inner(crate::txn::InnerTxn::Ro(r)))
    }

    /// Open a nested read transaction parented to `parent` (SPEC 00 row 16).
    ///
    /// # Errors
    ///
    /// `Mdb(BadTxn)` if the parent has errored.
    pub fn nested_read_txn<'p>(&'p self, parent: &'p RwTxn) -> Result<RoTxn<'p, WithoutTls>> {
        parent.nested_read_txn()
    }

    /// Copy this environment to an open file (SPEC 00 row 17, `mdb_env_copy2`).
    ///
    /// # Errors
    ///
    /// `Mdb(ReadersFull)`, `Error::Io`, or `Mdb(Invalid)` from the copy.
    pub fn copy_to_file(&self, file: &mut File, option: CompactionOption) -> Result<()> {
        use std::io::{Read, Seek, Write};
        // ZeroDB's `CopyToFile` writes to a *path*; heed hands us an open File.
        // Stage into a unique temp path, then stream it into `file` (no
        // tempfile dependency: a pid+counter name in the system temp dir).
        let tmp = unique_temp_path();
        let res = self.copy_to_path_internal(&tmp, option);
        let out = res.and_then(|()| {
            let mut src = File::open(&tmp)?;
            let mut buf = Vec::new();
            src.read_to_end(&mut buf)?;
            file.write_all(&buf)?;
            file.flush()?;
            file.rewind()?;
            Ok(())
        });
        let _ = std::fs::remove_file(&tmp);
        out
    }

    /// Copy this environment to `path`, returning the created file (SPEC 00
    /// row 17).
    ///
    /// # Errors
    ///
    /// As [`Env::copy_to_file`].
    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        self.copy_to_path_internal(path.as_ref(), option)?;
        File::open(path.as_ref()).map_err(Into::into)
    }

    fn copy_to_path_internal(&self, path: &Path, option: CompactionOption) -> Result<()> {
        use zerodb::CopyToFile;
        let opt = match option {
            CompactionOption::Enabled => zerodb::CompactionOption::Enabled,
            CompactionOption::Disabled => zerodb::CompactionOption::Disabled,
        };
        self.inner.copy_to_file(path, opt).map_err(Into::into)
    }

    /// Force durability of all prior commits (SPEC 00 second table — SHOULD).
    ///
    /// # Errors
    ///
    /// `Io`(`EACCES`) on a read-only env; `Io` on a sync failure.
    pub fn force_sync(&self) -> Result<()> {
        self.inner.force_sync().map_err(Into::into)
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The configured reader-table size (SPEC 00 second table — SHOULD).
    /// ZeroDB does not surface the raw value post-open; returns the LMDB default.
    #[must_use]
    pub fn max_readers(&self) -> u32 {
        126
    }

    /// The maximum key size (SPEC 00 second table — SHOULD; SPEC 03 §2.1).
    #[must_use]
    pub fn max_key_size(&self) -> usize {
        511
    }

    /// Consume this handle and return the closing event (SPEC 00 row 23).
    #[must_use]
    pub fn prepare_for_closing(self) -> EnvClosingEvent {
        EnvClosingEvent(self.inner.prepare_for_closing())
    }

    /// Clear stale readers (SPEC 00 second table — SHOULD). Single-process
    /// model (D-001): there is never a stale cross-process reader, so 0.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for heed shape.
    pub fn clear_stale_readers(&self) -> Result<usize> {
        Ok(0)
    }
}

/// A unique temp path for the `copy_to_file` staging (no tempfile dependency).
fn unique_temp_path() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("heed-zerodb-copy-{pid}-{n}.dat"))
}

/// Contains information about the environment (SPEC 00 row 60).
#[derive(Debug, Clone, Copy)]
pub struct EnvInfo {
    /// Address of the map, if fixed (always null in ZeroDB).
    pub map_addr: *mut c_void,
    /// Size of the data memory map.
    pub map_size: usize,
    /// ID of the last used page.
    pub last_page_number: usize,
    /// ID of the last committed transaction.
    pub last_txn_id: usize,
    /// Maximum number of reader slots in the environment.
    pub maximum_number_of_readers: u32,
    /// Number of reader slots used in the environment.
    pub number_of_readers: u32,
}

/// Statistics for an environment (SPEC 00 second table — SHOULD).
#[derive(Debug, Clone, Copy)]
pub struct EnvStat {
    /// Size of a database page.
    pub page_size: u32,
    /// Depth (height) of the B-tree.
    pub depth: u32,
    /// Number of internal (non-leaf) pages.
    pub branch_pages: usize,
    /// Number of leaf pages.
    pub leaf_pages: usize,
    /// Number of overflow pages.
    pub overflow_pages: usize,
    /// Number of data items.
    pub entries: usize,
}

impl EnvStat {
    fn empty(page_size: u32) -> EnvStat {
        EnvStat {
            page_size,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
        }
    }
}

/// A signal fired once the environment is fully closed (SPEC 00 rows 23/24).
#[derive(Clone)]
pub struct EnvClosingEvent(zerodb::EnvClosingEvent);

impl EnvClosingEvent {
    /// Block until the environment is effectively closed.
    pub fn wait(&self) {
        self.0.wait();
    }

    /// Block until closed or `timeout` elapses; `true` if closed.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.0.wait_timeout(timeout)
    }
}

impl std::fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("EnvClosingEvent").finish()
    }
}

/// Path-based closing-event lookup (SPEC 00 second table — SHOULD; unused by
/// consumers, who use [`Env::prepare_for_closing`]). ZeroDB exposes no
/// path→event registry lookup, so this always returns `None`.
#[must_use]
pub fn env_closing_event<P: AsRef<Path>>(_path: P) -> Option<EnvClosingEvent> {
    None
}

/// Whether to compact while copying (SPEC 00 row 59).
#[derive(Debug, Copy, Clone)]
pub enum CompactionOption {
    /// Omit free pages and sequentially renumber all pages in output.
    Enabled,
    /// Copy everything without special free-page handling.
    Disabled,
}

/// Whether to enable or disable flags in `Env::set_flags` (SPEC 00 second
/// table; unused).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlagSetMode {
    /// Enable the flags.
    Enable,
    /// Disable the flags.
    Disable,
}

/// LMDB's built-in lexicographic (memcmp) key comparator (SPEC 00 row 53).
#[derive(Debug)]
pub enum DefaultComparator {}

impl LexicographicComparator for DefaultComparator {
    #[inline]
    fn compare_elem(a: u8, b: u8) -> Ordering {
        a.cmp(&b)
    }
    #[inline]
    fn successor(elem: u8) -> Option<u8> {
        match elem {
            u8::MAX => None,
            e => Some(e + 1),
        }
    }
    #[inline]
    fn predecessor(elem: u8) -> Option<u8> {
        match elem {
            u8::MIN => None,
            e => Some(e - 1),
        }
    }
    #[inline]
    fn max_elem() -> u8 {
        u8::MAX
    }
    #[inline]
    fn min_elem() -> u8 {
        u8::MIN
    }
}

/// Native-byte-order integer key comparator (SPEC 00 second table — SHOULD;
/// no consumer sets it — all keys are big-endian byte codecs).
#[derive(Debug)]
pub enum IntegerComparator {}

impl Comparator for IntegerComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        #[cfg(target_endian = "big")]
        {
            a.cmp(b)
        }
        #[cfg(target_endian = "little")]
        {
            let len = a.len().min(b.len());
            for i in (0..len).rev() {
                match a[i].cmp(&b[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }
    }
}
