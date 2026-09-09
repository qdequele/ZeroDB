//! Environment surface — `EnvOpenOptions<T>`, `Env<T>`, `EnvInfo`, `EnvStat`,
//! `EnvClosingEvent`, `CompactionOption`, `FlagSetMode`, the comparators, and
//! `env_closing_event` (SPEC 00 rows 1–26; SPEC 04 §7; ADR-0003 §C2/§C3/§C4).
//!
//! Everything wraps ZeroDB's native [`zerodb::Env`] (already `Arc`-cloneable,
//! registry-deduped with `EnvAlreadyOpened`, and deferred-close, TXN-50..53).
//! The adapter re-imposes three cheap fork boundary behaviors ZeroDB is lenient
//! about (D-006/D-008/D-010) so the 1.14 gate sees exact parity — each is
//! checked here, at the heed boundary, and covered by a test. A fourth
//! re-imposition landed with ADR-0010: the **data-file name** ([`DATA_FILE_NAME`]).

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

/// The name of the data file an adapter-opened env directory contains
/// (**ADR-0010**, D-012).
///
/// Matches LMDB's directory-env contract, because that name is not private to
/// LMDB: Meilisearch joins `"data.mdb"` onto an env path in production
/// compaction (`process_batch.rs`, `routes/tasks/compact.rs`, `meilitool`) and
/// snapshot code (`process_snapshot_creation.rs`, `enterprise_edition/s3.rs`).
/// An adapter env dir therefore contains **exactly** this one file — no
/// `zerodb.dat`, and no `lock.mdb` (nothing reads one; D-001).
///
/// The bytes inside are still ZeroDB's own `ZDB1` format (D-002): pointing
/// `mdb_stat`/`mdb_dump` at one fails loudly with `MDB_INVALID` rather than
/// misreading it. Use `zerodb-tools stat`, which reports the real engine.
pub const DATA_FILE_NAME: &str = zerodb::HEED_DATA_FILE_NAME;

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
    /// M2.6 ZeroDB extension — no heed counterpart. `None` = engine default.
    page_size: Option<u32>,
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
            page_size: None,
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
            page_size: self.page_size,
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

    /// Select the DB page size (**milestone 2.6**).
    ///
    /// **ZeroDB extension — heed has no such method**, because LMDB 0.9 derives
    /// its page size from the OS and offers no selector. Code that never calls
    /// it gets exactly what the fork would give it: new stores default to the
    /// **OS page size**, clamped to the engine window (LMDB parity —
    /// `me_psize = me_os_psize`, capped at 64 K; SPEC 00 row 164). So the
    /// frozen heed contract (PLAN ground rule 2) is untouched, and this method
    /// only ever *overrides* that parity default.
    ///
    /// `size` must be a power of two in
    /// `[`[`zerodb::MIN_PAGE_SIZE`]`, `[`zerodb::MAX_PAGE_SIZE`]`]`; an invalid
    /// value is reported by [`EnvOpenOptions::open`] as `Io(InvalidInput)`.
    /// The value applies only when **creating** a store — an existing env keeps
    /// its persisted page size (SPEC 02 §3.2). Read the effective value back
    /// from `Env::stat().page_size`. Note this is the **database** page size,
    /// independent of the OS page size (which gates `map_size` under D-006).
    pub fn page_size(&mut self, size: u32) -> &mut Self {
        self.page_size = Some(size);
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
        // M2.6 extension. Absent = the **OS page size**, clamped to the engine
        // window — LMDB parity: the fork derives `me_psize` from
        // `sysconf(_SC_PAGE_SIZE)` at creation (capped at 64 K), so a store
        // created through the heed surface must get the same geometry the fork
        // would give it (16 K on Apple Silicon / 64 K-page ARM distros, 4 K on
        // x86_64). The native `zerodb::EnvOpenOptions` keeps its own fixed
        // 4 K default; this parity default lives at the heed boundary only
        // (SPEC 00 row 164). Creation-only, as for every geometry option: an
        // existing store keeps its persisted page size.
        let ps = self.page_size.unwrap_or_else(|| {
            u32::try_from(os_page_size())
                .unwrap_or(zerodb::MAX_PAGE_SIZE)
                .clamp(zerodb::MIN_PAGE_SIZE, zerodb::MAX_PAGE_SIZE)
        });
        opts.page_size(ps);
        // ADR-0010 / D-012: re-impose heed's on-disk contract at the heed
        // boundary — an env opened through this adapter materializes as
        // `<dir>/data.mdb`, the name Meilisearch hardcodes in its production
        // compaction and snapshot paths. No `lock.mdb` is created: ZeroDB is
        // single-process (D-001) and nothing in the consumer tree reads one.
        opts.data_file_name(DATA_FILE_NAME);
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

    /// Environment info (SPEC 00 rows 20/60; `mdb_env_info`). **Completed in
    /// milestone 2.1** — every field is now populated from the live snapshot
    /// and the reader table, where Phase 1 filled only `map_size`.
    ///
    /// `map_addr` is always null: it is `MDB_envinfo::me_mapaddr`, meaningful
    /// only under `MDB_FIXEDMAP`, which ZeroDB does not implement (SPEC 01).
    /// The field is kept for heed signature parity. `last_page_number` carries
    /// LMDB's *meaning* but a ZeroDB-format *value* (D-002) — do not compare it
    /// against LMDB. See [`zerodb::EnvInfo`] for the full mapping table.
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        let i = self.inner.info();
        EnvInfo {
            map_addr: ptr::null_mut(),
            map_size: i.map_size as usize,
            last_page_number: i.last_pgno as usize,
            last_txn_id: i.last_txnid as usize,
            maximum_number_of_readers: i.max_readers,
            number_of_readers: i.num_readers,
        }
    }

    /// Env-level statistics (SPEC 00 second table — **landed in milestone
    /// 2.1**): the main DB's `MDB_stat`. Delegates to [`zerodb::Env::stat`],
    /// which reads the published snapshot directly — no read txn is opened, so
    /// this no longer consumes a reader slot or fails silently to zeros when
    /// the reader table is full.
    ///
    /// Page counts are ZeroDB-format values (D-002); see [`zerodb::EnvStat`].
    #[must_use]
    pub fn stat(&self) -> EnvStat {
        let s = self.inner.stat();
        EnvStat {
            page_size: s.page_size,
            depth: u32::from(s.depth),
            branch_pages: s.branch_pages as usize,
            leaf_pages: s.leaf_pages as usize,
            overflow_pages: s.overflow_pages as usize,
            entries: s.entries as usize,
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
        use std::io::{Seek, Write};
        // ZeroDB's `CopyToFile` writes to a *path*; heed hands us an open File.
        // Stage into a private, freshly-created directory (so no other process
        // can pre-place a file or symlink at the name we are about to write),
        // then **stream** the image into `file` — never the whole copy in RAM
        // (PERF-GAP C1: the compaction path is O(depth × page size); buffering
        // the result here would have restored a 1× env-size peak). The
        // directory and its contents are removed on every exit path.
        let stage = StagingDir::create()?;
        let tmp = stage.path().join("copy.dat");
        self.copy_to_path_internal(&tmp, option)?;
        let mut src = File::open(&tmp)?;
        std::io::copy(&mut src, file)?;
        file.flush()?;
        file.rewind()?;
        Ok(())
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

    /// Copy this environment to `path`, reporting progress (**milestone 2.3**).
    ///
    /// **ZeroDB extension — heed's copy is an opaque blocking call.** See
    /// [`zerodb::CopyToFile::copy_to_file_with_progress`] for the callback
    /// contract, in particular that every callback fires before any byte
    /// reaches `path`, so a panicking callback leaves no partial copy.
    ///
    /// The heed-mirrored [`Env::copy_to_file`] / [`Env::copy_to_path`]
    /// signatures are unchanged and behave identically to before.
    ///
    /// # Errors
    ///
    /// As [`Env::copy_to_path`].
    pub fn copy_to_path_with_progress<P: AsRef<Path>>(
        &self,
        path: P,
        option: CompactionOption,
        on_progress: &mut dyn FnMut(zerodb::CopyProgress),
    ) -> Result<File> {
        use zerodb::CopyToFile;
        let opt = zdb_compaction(option);
        self.inner
            .copy_to_file_with_progress(path.as_ref(), opt, on_progress)?;
        File::open(path.as_ref()).map_err(Into::into)
    }

    fn copy_to_path_internal(&self, path: &Path, option: CompactionOption) -> Result<()> {
        use zerodb::CopyToFile;
        self.inner
            .copy_to_file(path, zdb_compaction(option))
            .map_err(Into::into)
    }

    /// Force durability of all prior commits (SPEC 00 second table — **landed
    /// in milestone 2.5**). Exactly `mdb_env_sync(env, 1)`; the only form heed
    /// exposes. Equivalent to [`Env::sync`]`(true)`.
    ///
    /// # Errors
    ///
    /// `Io`(`EACCES`) on a read-only env; `Io` on a sync failure.
    pub fn force_sync(&self) -> Result<()> {
        self.inner.force_sync().map_err(Into::into)
    }

    /// Explicit environment sync — full `mdb_env_sync(env, force)` parity
    /// (**milestone 2.5**). **ZeroDB extension:** heed exposes only
    /// [`Env::force_sync`] (the `force = true` form), so there is no heed
    /// signature to mirror here.
    ///
    /// `force = false` reproduces LMDB's `mdb_env_sync0` gate: it is a
    /// **no-op** on a `NO_SYNC` env (prior commits stay non-durable), and it
    /// leaves `MAP_ASYNC` as an asynchronous `msync`. `force = true` always
    /// issues a real barrier. See [`zerodb::Env::sync`].
    ///
    /// # Errors
    ///
    /// `Io`(`EACCES`) on a read-only env; `Io` on a sync failure.
    pub fn sync(&self, force: bool) -> Result<()> {
        self.inner.sync(force).map_err(Into::into)
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The configured reader-table size (SPEC 00 second table — **landed in
    /// milestone 2.1**; `mdb_env_get_maxreaders`). Reads the real table
    /// capacity, where Phase 1 returned the LMDB default constant.
    #[must_use]
    pub fn max_readers(&self) -> u32 {
        self.inner.info().max_readers
    }

    /// Reader slots **currently** occupied (**milestone 2.1**).
    ///
    /// **ZeroDB extension — no heed/LMDB counterpart.** `EnvInfo`'s
    /// `number_of_readers` mirrors `MDB_envinfo::me_numreaders`, which is a
    /// *high-water mark* that never decreases (D-011); this is the live count
    /// it is usually mistaken for. Exposed as a method rather than an `EnvInfo`
    /// field so the mirrored struct keeps heed's exact shape.
    #[must_use]
    pub fn live_readers(&self) -> u32 {
        self.inner.info().live_readers
    }

    /// The maximum key size (SPEC 00 second table — SHOULD, **landed in
    /// milestone 2.7**; `mdb_env_get_maxkeysize`, SPEC 03 §2.1).
    ///
    /// Reports the engine's real [`zerodb::MAX_KEY_SIZE`]. Until 2.7 this
    /// returned a hardcoded `511` — the same defect class 2.1 fixed in
    /// [`Env::max_readers`]: the value happened to be right, but it was a
    /// literal that would silently stop matching the engine the moment the
    /// constant moved.
    #[must_use]
    pub fn max_key_size(&self) -> usize {
        zerodb::MAX_KEY_SIZE
    }

    /// List the environment's occupied reader slots (**milestone 2.2**).
    ///
    /// **ZeroDB extension — heed exposes no reader introspection.** This is
    /// the single-process analogue of `mdb_reader_list`. See
    /// [`zerodb::Env::reader_list`] for the full contract, in particular that
    /// the result is a *sample* of a lock-free table and entries may be stale
    /// by the time it returns.
    #[must_use]
    pub fn reader_list(&self) -> Vec<zerodb::ReaderEntry> {
        self.inner.reader_list()
    }

    /// Consume this handle and return the closing event (SPEC 00 row 23).
    #[must_use]
    pub fn prepare_for_closing(self) -> EnvClosingEvent {
        EnvClosingEvent(self.inner.prepare_for_closing())
    }

    /// Clear stale readers (SPEC 00 second table — SHOULD, **landed in
    /// milestone 2.2**; `mdb_reader_check`).
    ///
    /// **Always 0, and that is the correct answer rather than a stub.** ZeroDB
    /// is single-process (D-001): the reader table is process memory, every
    /// slot is owned by a `RoTxn` that releases it in `Drop`, and a dead
    /// process takes the whole table with it. There is no cross-process
    /// abandoned slot for `mdb_reader_check` to reap. Kept so heed code that
    /// calls it periodically keeps compiling and no-ops.
    ///
    /// See [`zerodb::Env::clear_stale_readers`] for the full argument, and
    /// [`Env::reader_list`] for the introspection that *is* meaningful here.
    ///
    /// # Errors
    ///
    /// Never. The [`Result`] exists for heed shape.
    pub fn clear_stale_readers(&self) -> Result<usize> {
        Ok(self.inner.clear_stale_readers()?)
    }
}

/// heed's `CompactionOption` → ZeroDB's (SPEC 00 row 59).
fn zdb_compaction(option: CompactionOption) -> zerodb::CompactionOption {
    match option {
        CompactionOption::Enabled => zerodb::CompactionOption::Enabled,
        CompactionOption::Disabled => zerodb::CompactionOption::Disabled,
    }
}

/// A private staging directory for `copy_to_file` (no tempfile dependency).
///
/// `create_dir` fails if the name already exists — including as a symlink —
/// so a directory we successfully created is ours alone; on Unix it is also
/// mode `0700`. The name mixes pid, a process-wide counter and a clock sample
/// so collisions are retried, not followed. Dropping the guard removes the
/// directory and everything staged in it.
struct StagingDir(std::path::PathBuf);

impl StagingDir {
    fn create() -> std::io::Result<StagingDir> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id();
        let base = std::env::temp_dir();
        for _ in 0..16 {
            let n = N.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let path = base.join(format!("heed-zerodb-copy-{pid}-{n}-{nanos:08x}"));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(StagingDir(path)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "heed-zerodb: could not create a private staging directory for copy_to_file",
        ))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        // Best-effort: the directory is ours (created above), so removing it
        // recursively cannot touch anything we did not stage.
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
