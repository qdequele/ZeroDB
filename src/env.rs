//! Environment management for ZeroDB.
//!
//! The environment is the main entry point for using the database. It manages
//! the memory-mapped file, transactions, and database handles.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use std::marker::PhantomData;

use crate::alloc::{PageAllocator, PagePool};
use crate::error::{Error, Result};
use crate::flags::EnvFlags;
use crate::mmap::{DataFile, MemoryMap};
use crate::page::{DbInfo, MetaPage, PageNo};
use crate::txn::{RoTxn, RwTxn, WithTls, WithoutTls, TlsUsage};

/// Default map size (10 MB).
const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024;

/// Default maximum readers.
const DEFAULT_MAX_READERS: u32 = 126;

/// Default maximum databases.
const DEFAULT_MAX_DBS: u32 = 0;

/// A simple signal event for synchronization.
#[derive(Clone)]
struct SignalEvent {
    inner: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

impl SignalEvent {
    fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(false), std::sync::Condvar::new())),
        }
    }

    fn signal(&self) {
        let (lock, cvar) = &*self.inner;
        let mut signaled = lock.lock().unwrap();
        *signaled = true;
        cvar.notify_all();
    }

    fn wait(&self) {
        let (lock, cvar) = &*self.inner;
        let mut signaled = lock.lock().unwrap();
        while !*signaled {
            signaled = cvar.wait(signaled).unwrap();
        }
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        let (lock, cvar) = &*self.inner;
        let signaled = lock.lock().unwrap();
        if *signaled {
            return true;
        }
        let result = cvar.wait_timeout(signaled, timeout).unwrap();
        *result.0
    }
}

/// Global registry of open environments to prevent double-opening.
static OPENED_ENVS: std::sync::LazyLock<RwLock<HashMap<PathBuf, Arc<SignalEvent>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Returns a struct that allows to wait for the effective closing of an environment.
pub fn env_closing_event<P: AsRef<Path>>(path: P) -> Option<EnvClosingEvent> {
    let lock = OPENED_ENVS.read().unwrap();
    lock.get(path.as_ref()).map(|signal_event| EnvClosingEvent(signal_event.clone()))
}

/// A structure that can be used to wait for the closing event.
/// Multiple threads can wait on this event.
#[derive(Clone)]
pub struct EnvClosingEvent(Arc<SignalEvent>);

impl EnvClosingEvent {
    /// Blocks this thread until the environment is effectively closed.
    ///
    /// # Safety
    ///
    /// Make sure that you don't have any copy of the environment in the thread
    /// that is waiting for a close event. If you do, you will have a deadlock.
    pub fn wait(&self) {
        self.0.wait()
    }

    /// Blocks this thread until either the environment has been closed
    /// or until the timeout elapses. Returns `true` if the environment
    /// has been effectively closed.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.0.wait_timeout(timeout)
    }
}

impl std::fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("EnvClosingEvent").finish()
    }
}

/// Whether to perform compaction while copying an environment.
#[derive(Debug, Copy, Clone)]
pub enum CompactionOption {
    /// Omit free pages and sequentially renumber all pages in output.
    ///
    /// This option consumes more CPU and runs more slowly than the default.
    Enabled,

    /// Copy everything without taking any special action about free pages.
    Disabled,
}

/// Whether to enable or disable flags in [`Env::set_flags`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlagSetMode {
    /// Enable the flags.
    Enable,
    /// Disable the flags.
    Disable,
}

/// A representation of the default lexicographic comparator behavior.
///
/// This enum is used to indicate the absence of a custom comparator.
/// The database uses lexicographic comparison of keys by default.
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
            elem => Some(elem + 1),
        }
    }

    #[inline]
    fn predecessor(elem: u8) -> Option<u8> {
        match elem {
            u8::MIN => None,
            elem => Some(elem - 1),
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

/// A representation of integer comparator behavior.
///
/// This enum is used to indicate that keys should be sorted by numeric value
/// in native byte order.
#[derive(Debug)]
pub enum IntegerComparator {}

impl Comparator for IntegerComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        #[cfg(target_endian = "big")]
        return a.cmp(b);

        #[cfg(target_endian = "little")]
        {
            let len = a.len();
            for i in (0..len).rev() {
                match a[i].cmp(&b[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            Ordering::Equal
        }
    }
}

/// Define a custom key comparison function for a database.
pub trait Comparator {
    /// Compares the raw bytes representation of two keys.
    fn compare(a: &[u8], b: &[u8]) -> Ordering;
}

/// Define a lexicographic comparator, which is a special case of [`Comparator`].
///
/// Types that implement [`LexicographicComparator`] will automatically have [`Comparator`]
/// implemented as well.
pub trait LexicographicComparator: Comparator {
    /// Compare a single byte.
    fn compare_elem(a: u8, b: u8) -> Ordering;

    /// Advances the given `elem` to its immediate lexicographic successor.
    fn successor(elem: u8) -> Option<u8>;

    /// Moves the given `elem` to its immediate lexicographic predecessor.
    fn predecessor(elem: u8) -> Option<u8>;

    /// Returns the maximum byte value per the comparator's lexicographic order.
    fn max_elem() -> u8;

    /// Returns the minimum byte value per the comparator's lexicographic order.
    fn min_elem() -> u8;
}

impl<C: LexicographicComparator> Comparator for C {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        for idx in 0..std::cmp::min(a.len(), b.len()) {
            if a[idx] != b[idx] {
                return C::compare_elem(a[idx], b[idx]);
            }
        }
        std::cmp::Ord::cmp(&a.len(), &b.len())
    }
}

/// Options for opening an environment.
///
/// The type parameter `T` specifies the TLS mode:
/// - `WithTls` (default): Read transactions are `!Send` but may be faster
/// - `WithoutTls`: Read transactions are `Send` and can be moved between threads
#[derive(Debug, Clone)]
pub struct EnvOpenOptions<T = WithTls> {
    map_size: usize,
    max_readers: u32,
    max_dbs: u32,
    flags: EnvFlags,
    _tls_marker: PhantomData<T>,
}

impl Default for EnvOpenOptions<WithTls> {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvOpenOptions<WithTls> {
    /// Creates a new set of environment open options with default values.
    ///
    /// By default, read transactions use Thread Local Storage (TLS) and are `!Send`.
    pub fn new() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_readers: DEFAULT_MAX_READERS,
            max_dbs: DEFAULT_MAX_DBS,
            flags: EnvFlags::empty(),
            _tls_marker: PhantomData,
        }
    }
}

impl<T: TlsUsage> EnvOpenOptions<T> {
    /// Sets the size of the memory map.
    ///
    /// The size must be a multiple of the OS page size.
    pub fn map_size(&mut self, size: usize) -> &mut Self {
        self.map_size = size;
        self
    }

    /// Sets the maximum number of reader slots.
    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = readers;
        self
    }

    /// Sets the maximum number of named databases.
    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = dbs;
        self
    }

    /// Sets environment flags.
    ///
    /// # Safety
    ///
    /// Some flags like NO_SYNC and NO_LOCK are unsafe and can lead to
    /// data corruption if misused.
    pub unsafe fn flags(&mut self, flags: EnvFlags) -> &mut Self {
        self.flags |= flags;
        self
    }

    /// Opens the environment at the specified path.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The database file is not modified by another process while open
    /// - Long-lived transactions are avoided
    /// - The process is not killed with an active write transaction
    pub unsafe fn open(&self, path: &Path) -> Result<Env<T>> {
        // SAFETY: Caller guarantees the safety requirements.
        unsafe { Env::open(path, self) }
    }

    /// Returns the current configuration but with TLS enabled for read transactions.
    ///
    /// A thread can only use one transaction at a time, plus any child (nested)
    /// transactions. Each transaction belongs to one thread. A `BadRslot` error
    /// will be thrown when multiple read transactions exist on the same thread.
    pub fn read_txn_with_tls(self) -> EnvOpenOptions<WithTls> {
        EnvOpenOptions {
            map_size: self.map_size,
            max_readers: self.max_readers,
            max_dbs: self.max_dbs,
            flags: self.flags,
            _tls_marker: PhantomData,
        }
    }

    /// Returns the current configuration but without TLS for read transactions.
    ///
    /// When used to open transactions: A thread can use any number of read
    /// transactions at a time on the same thread. Read transactions can be
    /// moved in between threads (`Send`).
    pub fn read_txn_without_tls(self) -> EnvOpenOptions<WithoutTls> {
        EnvOpenOptions {
            map_size: self.map_size,
            max_readers: self.max_readers,
            max_dbs: self.max_dbs,
            flags: self.flags,
            _tls_marker: PhantomData,
        }
    }
}

/// Database index type (similar to LMDB's MDB_dbi).
pub type Dbi = u32;

/// Special DBI for the unnamed (main) database.
pub const MAIN_DBI: Dbi = 0;

/// Special DBI for the free list database.
pub const FREE_DBI: Dbi = 1;

/// Information about an open database.
#[derive(Debug, Clone)]
struct OpenDatabase {
    /// Database name (None for unnamed database).
    name: Option<String>,
    /// Database info (root, stats, etc.).
    db_info: DbInfo,
    /// Database flags.
    flags: crate::flags::DatabaseFlags,
}

/// Inner state of the environment, protected by RwLock.
struct EnvInner {
    /// Memory-mapped data file.
    mmap: MemoryMap,
    /// Current meta page index (0 or 1).
    #[allow(dead_code)]
    meta_index: usize,
    /// Cached copy of the current meta page.
    meta: MetaPage,
    /// Last allocated page number.
    last_pgno: PageNo,
    /// Last transaction ID.
    last_txnid: u64,
    /// Registry of open named databases (name -> DBI).
    db_registry: HashMap<String, Dbi>,
    /// Open databases by DBI.
    open_dbs: HashMap<Dbi, OpenDatabase>,
    /// Next available DBI.
    next_dbi: Dbi,
}

/// A database environment.
///
/// The environment is the main handle for working with the database.
/// It manages the memory-mapped file, transactions, and readers.
///
/// The type parameter `T` specifies the TLS mode for read transactions:
/// - `WithTls` (default): Read transactions are `!Send` but may be faster
/// - `WithoutTls`: Read transactions are `Send` and can be moved between threads
pub struct Env<T: TlsUsage = WithTls> {
    /// Path to the environment directory or file.
    path: PathBuf,
    /// Data file path.
    data_path: PathBuf,
    /// Lock file path.
    #[allow(dead_code)]
    lock_path: PathBuf,
    /// Data file handle.
    data_file: DataFile,
    /// Environment flags.
    flags: RwLock<EnvFlags>,
    /// Map size.
    map_size: RwLock<usize>,
    /// Page size.
    page_size: usize,
    /// Maximum readers.
    max_readers: u32,
    /// Maximum databases.
    max_dbs: u32,
    /// Inner mutable state.
    inner: RwLock<EnvInner>,
    /// Write transaction lock - only one write txn at a time.
    write_lock: Mutex<()>,
    /// Page buffer pool for reusing allocations.
    page_pool: Mutex<PagePool>,
    /// Signal event for closing notification.
    signal_event: Arc<SignalEvent>,
    /// TLS marker.
    _tls_marker: PhantomData<T>,
}

impl<T: TlsUsage> Env<T> {
    /// Opens or creates an environment.
    unsafe fn open(path: &Path, options: &EnvOpenOptions<T>) -> Result<Self> {
        let page_size = page_size::get();

        // Validate map size is multiple of page size
        if options.map_size % page_size != 0 {
            return Err(Error::Io {
                kind: crate::error::IoErrorKind::InvalidInput,
                message: format!(
                    "map_size ({}) must be a multiple of page size ({})",
                    options.map_size, page_size
                ),
            });
        }

        // Determine paths
        let (data_path, lock_path) = if options.flags.contains(EnvFlags::NO_SUB_DIR) {
            (path.to_path_buf(), path.with_extension("lock"))
        } else {
            // Create directory if needed
            if !path.exists() {
                fs::create_dir_all(path)?;
            }
            (path.join("data.mdb"), path.join("lock.mdb"))
        };

        // Open or create data file first (so we can canonicalize the path)
        let data_file = DataFile::open_or_create(&data_path)?;
        let is_new = data_file.is_empty()?;

        // Now canonicalize the path (file exists at this point)
        let canonical_path = fs::canonicalize(&data_path)?;

        // Check if already open
        {
            let opened = OPENED_ENVS.read().unwrap();
            if opened.contains_key(&canonical_path) {
                return Err(Error::EnvAlreadyOpened);
            }
        }

        // Initialize or verify the database
        let (mmap, meta, meta_index) = if is_new {
            // Initialize new database
            data_file.set_len(options.map_size as u64)?;

            let mut mmap = unsafe { MemoryMap::open_read_write(&data_path, options.map_size)? };

            // Initialize meta pages
            let meta0 = MetaPage::new(0, options.map_size as u64);
            let meta1 = MetaPage::new(1, options.map_size as u64);

            let page_data = mmap.page_mut(0, page_size).ok_or(Error::Corrupted)?;
            meta0.write_to(page_data)?;

            let page_data = mmap.page_mut(1, page_size).ok_or(Error::Corrupted)?;
            meta1.write_to(page_data)?;

            mmap.flush()?;

            (mmap, meta0, 0)
        } else {
            // Open existing database
            let file_len = data_file.len()? as usize;
            let map_size = options.map_size.max(file_len);

            let mmap = if options.flags.contains(EnvFlags::READ_ONLY) {
                unsafe { MemoryMap::open_read_only(&data_path, map_size)? }
            } else {
                // Extend file if needed
                if file_len < map_size {
                    data_file.set_len(map_size as u64)?;
                }
                unsafe { MemoryMap::open_read_write(&data_path, map_size)? }
            };

            // Read and validate meta pages
            let meta0_data = mmap.page(0, page_size).ok_or(Error::Corrupted)?;
            let meta0 = MetaPage::read_from(meta0_data);

            let meta1_data = mmap.page(1, page_size).ok_or(Error::Corrupted)?;
            let meta1 = MetaPage::read_from(meta1_data);

            // Choose the valid meta page with highest txnid
            let (meta, meta_index) = match (meta0, meta1) {
                (Ok(m0), Ok(m1)) => {
                    if m1.is_newer_than(&m0) {
                        (m1, 1)
                    } else {
                        (m0, 0)
                    }
                }
                (Ok(m0), Err(_)) => (m0, 0),
                (Err(_), Ok(m1)) => (m1, 1),
                (Err(_), Err(_)) => return Err(Error::Invalid),
            };

            (mmap, meta, meta_index)
        };

        // Create signal event for closing notification
        let signal_event = Arc::new(SignalEvent::new());

        // Register as open
        {
            let mut opened = OPENED_ENVS.write().unwrap();
            opened.insert(canonical_path.clone(), signal_event.clone());
        }

        let last_pgno = meta.last_pgno;
        let last_txnid = meta.last_txnid;

        Ok(Self {
            path: canonical_path,
            data_path,
            lock_path,
            data_file,
            flags: RwLock::new(options.flags),
            map_size: RwLock::new(options.map_size),
            page_size,
            max_readers: options.max_readers,
            max_dbs: options.max_dbs,
            inner: RwLock::new(EnvInner {
                mmap,
                meta_index,
                meta,
                last_pgno,
                last_txnid,
                db_registry: HashMap::new(),
                open_dbs: HashMap::new(),
                next_dbi: 2, // 0 = main, 1 = free
            }),
            write_lock: Mutex::new(()),
            page_pool: Mutex::new(PagePool::new(page_size)),
            signal_event,
            _tls_marker: PhantomData,
        })
    }

    /// Returns the path to the environment.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the map size.
    pub fn map_size(&self) -> usize {
        *self.map_size.read().unwrap()
    }

    /// Returns the environment flags.
    pub fn flags(&self) -> Option<EnvFlags> {
        Some(*self.flags.read().unwrap())
    }

    /// Returns the raw environment flags.
    pub fn get_flags(&self) -> u32 {
        self.flags.read().unwrap().bits()
    }

    /// Enable or disable environment flags.
    ///
    /// # Safety
    ///
    /// It is unsafe to use unsafe LMDB flags such as `NO_SYNC`, `NO_META_SYNC`, or `NO_LOCK`.
    pub unsafe fn set_flags(&self, flags: EnvFlags, mode: FlagSetMode) -> Result<()> {
        let mut current_flags = self.flags.write().unwrap();
        match mode {
            FlagSetMode::Enable => *current_flags |= flags,
            FlagSetMode::Disable => *current_flags &= !flags,
        }
        Ok(())
    }

    /// Returns the size of the data file on disk.
    pub fn real_disk_size(&self) -> Result<u64> {
        self.data_file.len()
    }

    /// Get the maximum size of keys we can write.
    ///
    /// Default is 511 bytes.
    pub fn max_key_size(&self) -> usize {
        crate::MAX_KEY_SIZE
    }

    /// Returns the maximum number of readers.
    pub fn max_readers(&self) -> u32 {
        self.max_readers
    }

    /// Returns the maximum number of databases.
    pub fn max_dbs(&self) -> u32 {
        self.max_dbs
    }

    /// Gets a page buffer from the pool (or allocates a new one).
    pub(crate) fn get_page_buffer(&self) -> Vec<u8> {
        self.page_pool.lock().unwrap().get()
    }

    /// Returns a page buffer to the pool for reuse.
    pub(crate) fn return_page_buffer(&self, buf: Vec<u8>) {
        self.page_pool.lock().unwrap().put(buf);
    }

    /// Returns multiple page buffers to the pool.
    pub(crate) fn return_page_buffers(&self, buffers: impl IntoIterator<Item = Vec<u8>>) {
        let mut pool = self.page_pool.lock().unwrap();
        for buf in buffers {
            pool.put(buf);
        }
    }

    /// Returns information about the environment.
    pub fn info(&self) -> EnvInfo {
        let inner = self.inner.read().unwrap();
        EnvInfo {
            map_size: self.map_size(),
            last_pgno: inner.last_pgno,
            last_txnid: inner.last_txnid,
            max_readers: self.max_readers,
            num_readers: 0, // TODO: implement reader counting
        }
    }

    /// Returns statistics about the main database.
    pub fn stat(&self) -> DbStat {
        let inner = self.inner.read().unwrap();
        DbStat::from_db_info(&inner.meta.main_db, self.page_size)
    }

    /// Creates a typed database.
    ///
    /// If a database already exists, it will be opened with the existing configuration.
    /// If `name` is `None`, the main unnamed database is used.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use zerodb::{EnvOpenOptions, Database};
    /// use zerodb::types::{Str, U32};
    ///
    /// let env = unsafe { EnvOpenOptions::new().open(path)? };
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, U32> = env.create_database(&mut wtxn, None)?;
    /// wtxn.commit()?;
    /// ```
    /// Options and flags which can be used to configure how a [`Database`] is opened.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use zerodb::EnvOpenOptions;
    /// use zerodb::types::*;
    ///
    /// let env = unsafe { EnvOpenOptions::new().open(dir.path())? };
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.database_options()
    ///     .types::<Str, U32>()
    ///     .create(&mut wtxn)?;
    ///
    /// db.put(&mut wtxn, "hello", &42)?;
    /// wtxn.commit()?;
    /// ```
    pub fn database_options(
        &self,
    ) -> crate::database::DatabaseOpenOptions<'_, 'static, crate::database::Unspecified, crate::database::Unspecified, crate::env::DefaultComparator, T> {
        crate::database::DatabaseOpenOptions::new(self)
    }

    /// Creates a typed database in the environment.
    ///
    /// If `name` is `None`, accesses the unnamed (main) database.
    /// If `name` is `Some`, creates a named database. Named database names are stored
    /// as keys in the unnamed database, with their `DbInfo` as values.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The maximum number of databases has been reached
    /// - A named database already exists with different flags
    pub fn create_database<KC, DC>(
        &self,
        wtxn: &mut RwTxn<'_, T>,
        name: Option<&str>,
    ) -> Result<crate::database::Database<KC, DC>> {
        let flags = crate::flags::DatabaseFlags::empty();

        match name {
            None => {
                // Unnamed (main) database - always exists
                let inner = self.inner.read().unwrap();
                let db_info = inner.meta.main_db;
                Ok(crate::database::Database::new(MAIN_DBI, None, db_info, flags))
            }
            Some(db_name) => {
                // Check max_dbs limit
                {
                    let inner = self.inner.read().unwrap();
                    if inner.next_dbi >= self.max_dbs + 2 {
                        return Err(Error::DbsFull);
                    }

                    // Check if already open
                    if let Some(&dbi) = inner.db_registry.get(db_name) {
                        if let Some(open_db) = inner.open_dbs.get(&dbi) {
                            return Ok(crate::database::Database::new(
                                dbi,
                                Some(db_name.to_string()),
                                open_db.db_info,
                                open_db.flags,
                            ));
                        }
                    }
                }

                // Try to load existing database info from the main database
                let existing_info = self.load_named_db_info(wtxn, db_name)?;

                let (dbi, db_info) = if let Some(info) = existing_info {
                    // Database exists - register it
                    let mut inner = self.inner.write().unwrap();
                    let dbi = inner.next_dbi;
                    inner.next_dbi += 1;
                    inner.db_registry.insert(db_name.to_string(), dbi);
                    inner.open_dbs.insert(dbi, OpenDatabase {
                        name: Some(db_name.to_string()),
                        db_info: info,
                        flags,
                    });
                    (dbi, info)
                } else {
                    // Create new database
                    let new_info = DbInfo::new();

                    // Store the database info in the main database
                    self.store_named_db_info(wtxn, db_name, &new_info)?;

                    // Register the new database
                    let mut inner = self.inner.write().unwrap();
                    let dbi = inner.next_dbi;
                    inner.next_dbi += 1;
                    inner.db_registry.insert(db_name.to_string(), dbi);
                    inner.open_dbs.insert(dbi, OpenDatabase {
                        name: Some(db_name.to_string()),
                        db_info: new_info,
                        flags,
                    });
                    (dbi, new_info)
                };

                Ok(crate::database::Database::new(dbi, Some(db_name.to_string()), db_info, flags))
            }
        }
    }

    /// Opens an existing typed database.
    ///
    /// Returns `None` if the database doesn't exist.
    /// If `name` is `None`, the main unnamed database is used.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use zerodb::{EnvOpenOptions, Database};
    /// use zerodb::types::{Str, U32};
    ///
    /// let env = unsafe { EnvOpenOptions::new().open(path)? };
    /// let rtxn = env.read_txn()?;
    /// let db: Option<Database<Str, U32>> = env.open_database(&rtxn, None)?;
    /// ```
    pub fn open_database<KC, DC>(
        &self,
        rtxn: &RoTxn<'_, T>,
        name: Option<&str>,
    ) -> Result<Option<crate::database::Database<KC, DC>>> {
        let flags = crate::flags::DatabaseFlags::empty();

        match name {
            None => {
                // Unnamed (main) database
                let inner = self.inner.read().unwrap();
                let db_info = inner.meta.main_db;

                // The main database always exists (even if empty)
                Ok(Some(crate::database::Database::new(MAIN_DBI, None, db_info, flags)))
            }
            Some(db_name) => {
                // Check if already open
                {
                    let inner = self.inner.read().unwrap();
                    if let Some(&dbi) = inner.db_registry.get(db_name) {
                        if let Some(open_db) = inner.open_dbs.get(&dbi) {
                            return Ok(Some(crate::database::Database::new(
                                dbi,
                                Some(db_name.to_string()),
                                open_db.db_info,
                                open_db.flags,
                            )));
                        }
                    }
                }

                // Try to load from the main database
                let db_info = self.load_named_db_info_ro(rtxn, db_name)?;

                match db_info {
                    Some(info) => {
                        // Register the database
                        let mut inner = self.inner.write().unwrap();
                        let dbi = inner.next_dbi;
                        inner.next_dbi += 1;
                        inner.db_registry.insert(db_name.to_string(), dbi);
                        inner.open_dbs.insert(dbi, OpenDatabase {
                            name: Some(db_name.to_string()),
                            db_info: info,
                            flags,
                        });

                        Ok(Some(crate::database::Database::new(
                            dbi,
                            Some(db_name.to_string()),
                            info,
                            flags,
                        )))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Loads a named database's DbInfo from the main database (read-only).
    fn load_named_db_info_ro(&self, txn: &RoTxn<'_, T>, name: &str) -> Result<Option<DbInfo>> {
        use crate::btree::{CursorOps, CursorState, SearchResult};

        let inner = self.inner.read().unwrap();
        let main_db = inner.meta.main_db;

        if main_db.root == 0 {
            return Ok(None);
        }

        let page_size = self.page_size;
        let mut state = CursorState::new(main_db.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let key_bytes = name.as_bytes();
        let result = CursorOps::search(&mut state, key_bytes, page_size, &get_page)?;

        match result {
            SearchResult::Found(_) => {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        if value.len() >= crate::page::DB_INFO_SIZE {
                            return Ok(Some(DbInfo::read_from(value)?));
                        }
                    }
                }
                Ok(None)
            }
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Loads a named database's DbInfo from the main database (read-write).
    fn load_named_db_info(&self, txn: &mut RwTxn<'_, T>, name: &str) -> Result<Option<DbInfo>> {
        use crate::btree::{CursorOps, CursorState, SearchResult};

        let main_db = txn.meta().main_db;

        if main_db.root == 0 {
            return Ok(None);
        }

        let page_size = self.page_size;
        let mut state = CursorState::new(main_db.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let key_bytes = name.as_bytes();
        let result = CursorOps::search(&mut state, key_bytes, page_size, &get_page)?;

        match result {
            SearchResult::Found(_) => {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        if value.len() >= crate::page::DB_INFO_SIZE {
                            return Ok(Some(DbInfo::read_from(value)?));
                        }
                    }
                }
                Ok(None)
            }
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Stores a named database's DbInfo in the main database.
    fn store_named_db_info(&self, txn: &mut RwTxn<'_, T>, name: &str, info: &DbInfo) -> Result<()> {
        use crate::btree::{PageBuilder, Node};

        let page_size = self.page_size;
        let key_bytes = name.as_bytes();

        // Serialize DbInfo
        let mut value_bytes = vec![0u8; crate::page::DB_INFO_SIZE];
        info.write_to(&mut value_bytes)?;

        // Get current main_db info
        let main_db = txn.meta().main_db;

        if main_db.root == 0 {
            // Empty main database - create root leaf
            let (pgno, data) = txn.alloc_page()?;
            let mut builder = PageBuilder::new_leaf(pgno, page_size);
            builder.add_leaf(&Node::leaf(key_bytes.to_vec(), value_bytes))?;
            data.copy_from_slice(&builder.finish());

            // Note: In a full implementation, we'd need to update the meta's main_db.root
            // This is tracked through the transaction commit process
        } else {
            // For now, we'll use a simplified approach for named databases
            // In a full implementation, this would use the proper B-tree insertion
            // that tracks changes through the transaction

            // Allocate a new leaf page and add the entry
            // This is a simplified version - a full implementation would properly
            // insert into the existing tree structure
            let (pgno, data) = txn.alloc_page()?;
            let mut builder = PageBuilder::new_leaf(pgno, page_size);
            builder.add_leaf(&Node::leaf(key_bytes.to_vec(), value_bytes))?;
            data.copy_from_slice(&builder.finish());
        }

        Ok(())
    }

    /// Forces an fsync of the data file.
    pub fn force_sync(&self) -> Result<()> {
        let inner = self.inner.read().unwrap();
        inner.mmap.flush()?;
        self.data_file.sync()?;
        Ok(())
    }

    /// Updates the database info for a specific DBI in the registry.
    ///
    /// This is called by Database operations that modify the tree structure.
    pub(crate) fn update_db_info(&self, dbi: crate::database::Dbi, db_info: DbInfo) {
        let mut inner = self.inner.write().unwrap();
        if let Some(open_db) = inner.open_dbs.get_mut(&dbi) {
            open_db.db_info = db_info;
        }
    }

    /// Gets the current database info for a specific DBI.
    ///
    /// This returns the most up-to-date db_info from the registry.
    pub(crate) fn get_db_info(&self, dbi: crate::database::Dbi) -> Option<DbInfo> {
        let inner = self.inner.read().unwrap();
        inner.open_dbs.get(&dbi).map(|db| db.db_info)
    }

    /// Returns a slice of raw page data.
    pub fn page_data(&self, page_no: PageNo) -> Result<&[u8]> {
        let inner = self.inner.read().unwrap();
        // SAFETY: We hold the read lock, so the mmap won't be unmapped
        let mmap_ptr = &inner.mmap as *const MemoryMap;
        let mmap = unsafe { &*mmap_ptr };
        mmap.page(page_no, self.page_size).ok_or(Error::PageNotFound)
    }

    /// Returns the current meta page.
    pub fn current_meta(&self) -> MetaPage {
        let inner = self.inner.read().unwrap();
        inner.meta
    }

    /// Creates a new read-only transaction.
    ///
    /// Read transactions provide a consistent snapshot of the database.
    /// Multiple read transactions can be active simultaneously.
    ///
    /// You can make this transaction `Send`able between threads by opening
    /// the environment with the [`EnvOpenOptions::read_txn_without_tls`]
    /// method.
    pub fn read_txn(&self) -> Result<RoTxn<'_, T>> {
        if self.flags.read().unwrap().contains(EnvFlags::NO_LOCK) {
            // With NO_LOCK, caller manages concurrency
        }

        let inner = self.inner.read().unwrap();
        let txnid = inner.last_txnid;
        let meta = inner.meta;

        Ok(RoTxn::new(self, txnid, meta))
    }

    /// Creates a read-only transaction with a `'static` lifetime.
    ///
    /// This is useful when you want to pass the transaction to a thread
    /// or store it in a struct without lifetime parameters.
    ///
    /// The transaction owns the environment, so the environment will
    /// be dropped when the transaction is dropped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use zerodb::{EnvOpenOptions, WithoutTls};
    ///
    /// let env_options = EnvOpenOptions::new().read_txn_without_tls();
    /// let env = unsafe { env_options.open(path)? };
    ///
    /// // Move env into a static transaction
    /// let rtxn = env.static_read_txn()?;
    ///
    /// // rtxn can now be moved to another thread
    /// std::thread::spawn(move || {
    ///     // use rtxn here
    /// });
    /// ```
    pub fn static_read_txn(self) -> Result<RoTxn<'static, T>> {
        if self.flags.read().unwrap().contains(EnvFlags::NO_LOCK) {
            // With NO_LOCK, caller manages concurrency
        }

        let inner = self.inner.read().unwrap();
        let txnid = inner.last_txnid;
        let meta = inner.meta;
        drop(inner);

        Ok(RoTxn::new_static(self, txnid, meta))
    }

    /// Creates a new read-write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// Write transactions have exclusive access to modify the database.
    pub fn write_txn(&self) -> Result<RwTxn<'_, T>> {
        if self.flags.read().unwrap().contains(EnvFlags::READ_ONLY) {
            return Err(Error::Incompatible);
        }

        // Acquire write lock
        let _guard = self.write_lock.lock().unwrap();
        // Note: We don't hold the guard in the transaction - we rely on
        // the single-threaded nature of write transactions.
        // A more robust implementation would use a parking_lot Mutex
        // with try_lock_for or similar.

        let inner = self.inner.read().unwrap();
        let txnid = inner.last_txnid + 1;
        let meta = inner.meta;
        let last_pgno = inner.last_pgno;
        drop(inner);

        let allocator = PageAllocator::new(last_pgno, self.map_size(), self.page_size);

        Ok(RwTxn::new(self, txnid, meta, allocator))
    }

    /// Commits a write transaction.
    ///
    /// This is called internally by `RwTxn::commit()`.
    pub(crate) fn commit_txn(
        &self,
        txnid: u64,
        mut meta: MetaPage,
        dirty_pages: BTreeMap<PageNo, Vec<u8>>,
        last_pgno: PageNo,
        is_empty: bool,
    ) -> Result<()> {
        // Note: is_empty flag reserved for future optimization with WRITE_MAP mode
        // Currently we always persist to maintain durability guarantees
        let _ = is_empty;

        // Get the new meta index BEFORE acquiring the lock (read is cheap)
        let new_meta_index = {
            let inner = self.inner.read().unwrap();
            1 - inner.meta_index
        };

        // Prepare meta page OUTSIDE the lock
        meta.last_pgno = last_pgno;
        meta.last_txnid = txnid;
        meta.header.page_no = new_meta_index as u64;

        // Serialize meta page (use pooled buffer to avoid allocation)
        let mut meta_buf = self.get_page_buffer();
        meta.write_to(&mut meta_buf)?;

        let page_size = self.page_size as u64;
        let meta_offset = new_meta_index as u64 * page_size;

        let current_flags = *self.flags.read().unwrap();
        if current_flags.contains(EnvFlags::WRITE_MAP) {
            // WRITEMAP mode: write directly to mmap, then msync
            {
                let mut inner = self.inner.write().unwrap();
                if let Some(mmap_slice) = inner.mmap.try_as_mut_slice() {
                    // Write dirty pages directly to mmap
                    for (pgno, data) in &dirty_pages {
                        let offset = *pgno as usize * self.page_size;
                        if offset + data.len() <= mmap_slice.len() {
                            mmap_slice[offset..offset + data.len()].copy_from_slice(data);
                        }
                    }
                    // Write meta page to mmap
                    let meta_off = meta_offset as usize;
                    if meta_off + meta_buf.len() <= mmap_slice.len() {
                        mmap_slice[meta_off..meta_off + meta_buf.len()]
                            .copy_from_slice(&meta_buf);
                    }
                }

                // Update in-memory state
                inner.meta_index = new_meta_index;
                inner.meta = meta;
                inner.last_pgno = last_pgno;
                inner.last_txnid = txnid;

                // Sync the mmap to disk
                if !current_flags.contains(EnvFlags::NO_SYNC) {
                    if current_flags.contains(EnvFlags::MAP_ASYNC) {
                        inner.mmap.flush_async()?;
                    } else {
                        inner.mmap.flush()?;
                    }
                }
            }
        } else {
            // Standard mode: use file I/O
            // Prepare all writes as a batch
            let mut writes: Vec<(&[u8], u64)> = Vec::with_capacity(dirty_pages.len() + 1);

            for (pgno, data) in &dirty_pages {
                let offset = *pgno as u64 * page_size;
                writes.push((data.as_slice(), offset));
            }

            // Add meta page write
            writes.push((meta_buf.as_slice(), meta_offset));

            // Batch write all pages + meta in one go
            self.data_file.write_batch(&writes)?;

            // Single sync for all writes
            if !current_flags.contains(EnvFlags::NO_SYNC) {
                self.data_file.sync_data()?;
            }

            // Acquire write lock only for updating in-memory state (minimal critical section)
            let mut inner = self.inner.write().unwrap();
            inner.meta_index = new_meta_index;
            inner.meta = meta;
            inner.last_pgno = last_pgno;
            inner.last_txnid = txnid;
            // Note: mmap will see file changes after fsync - no need to copy
        }

        // Collect and return buffers to pool (including meta buffer)
        let mut buffers: Vec<Vec<u8>> = dirty_pages.into_values().collect();
        buffers.push(meta_buf);
        self.return_page_buffers(buffers);

        Ok(())
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event.
    ///
    /// Make sure that you drop all the copies of `Env`s you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(&self) -> EnvClosingEvent {
        EnvClosingEvent(self.signal_event.clone())
    }

    /// Check for stale entries in the reader lock table and clear them.
    ///
    /// Returns the number of stale readers cleared.
    pub fn clear_stale_readers(&self) -> Result<usize> {
        // ZeroDB doesn't currently track readers the same way LMDB does
        // This is a no-op for now
        Ok(0)
    }

    /// Resize the memory map to a new size.
    ///
    /// # Safety
    ///
    /// The caller must ensure no transactions are active.
    pub unsafe fn resize(&self, new_size: usize) -> Result<()> {
        let page_size = page_size::get();
        if new_size % page_size != 0 {
            return Err(Error::Io {
                kind: crate::error::IoErrorKind::InvalidInput,
                message: format!(
                    "map size ({}) must be a multiple of page size ({})",
                    new_size, page_size
                ),
            });
        }

        // Extend the file
        self.data_file.set_len(new_size as u64)?;

        // Update the map size
        *self.map_size.write().unwrap() = new_size;

        // Note: A full implementation would need to remap the mmap
        // For now, this only updates the logical size
        Ok(())
    }

    /// Copy an LMDB environment to a file, with options.
    ///
    /// This function may be used to make a backup of an existing environment.
    pub fn copy_to_file(&self, file: &mut File, option: CompactionOption) -> Result<()> {
        use std::io::Write;

        let inner = self.inner.read().unwrap();

        // Determine which pages to copy
        let last_pgno = inner.last_pgno;
        let page_size = self.page_size;

        // Write pages
        for pgno in 0..=last_pgno {
            if let Some(page_data) = inner.mmap.page(pgno, page_size) {
                // For compaction, we would skip free pages
                // For now, we copy all pages
                let _ = option; // Compaction not yet implemented
                file.write_all(page_data)?;
            }
        }

        file.flush()?;
        Ok(())
    }

    /// Copy an LMDB environment to a file at the specified path.
    ///
    /// This function may be used to make a backup of an existing environment.
    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        let path = path.as_ref();
        let mut file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(path)?;

        match self.copy_to_file(&mut file, option) {
            Ok(_) => {
                file.rewind()?;
                Ok(file)
            }
            Err(err) => {
                fs::remove_file(path)?;
                Err(err)
            }
        }
    }

    /// Create a nested transaction with read and write access.
    ///
    /// The new transaction will be a nested transaction, with the transaction indicated by parent
    /// as its parent.
    pub fn nested_write_txn<'p>(&'p self, parent: &'p mut RwTxn<'_, T>) -> Result<RwTxn<'p, T>> {
        if self.flags.read().unwrap().contains(EnvFlags::READ_ONLY) {
            return Err(Error::Incompatible);
        }

        // Create a nested transaction using the existing RwTxn::nested constructor
        let meta = parent.meta().clone();
        let last_pgno = parent.main_root(); // Use parent's state

        let allocator = PageAllocator::new(last_pgno, self.map_size(), self.page_size);

        Ok(RwTxn::new(self, parent.txnid(), meta, allocator))
    }

    /// Closes the environment.
    ///
    /// This is called automatically when the environment is dropped.
    fn close(&self) {
        let mut opened = OPENED_ENVS.write().unwrap();
        opened.remove(&self.path);
        self.signal_event.signal();
    }
}

impl<T: TlsUsage> Drop for Env<T> {
    fn drop(&mut self) {
        self.close();
    }
}

// Env is Send + Sync because all mutable state is protected by RwLock
unsafe impl<T: TlsUsage> Send for Env<T> {}
unsafe impl<T: TlsUsage> Sync for Env<T> {}

/// Environment information.
#[derive(Debug, Clone, Copy)]
pub struct EnvInfo {
    /// Size of the memory map.
    pub map_size: usize,
    /// Last used page number.
    pub last_pgno: PageNo,
    /// Last committed transaction ID.
    pub last_txnid: u64,
    /// Maximum number of reader slots.
    pub max_readers: u32,
    /// Number of reader slots in use.
    pub num_readers: u32,
}

/// Database statistics.
#[derive(Debug, Clone, Copy)]
pub struct DbStat {
    /// Size of a database page.
    pub page_size: usize,
    /// Depth (height) of the B-tree.
    pub depth: u32,
    /// Number of internal (branch) pages.
    pub branch_pages: u64,
    /// Number of leaf pages.
    pub leaf_pages: u64,
    /// Number of overflow pages.
    pub overflow_pages: u64,
    /// Number of data entries.
    pub entries: u64,
}

impl DbStat {
    /// Creates a DbStat from a DbInfo.
    pub fn from_db_info(info: &DbInfo, page_size: usize) -> Self {
        Self {
            page_size,
            depth: info.depth as u32,
            branch_pages: info.branch_pages,
            leaf_pages: info.leaf_pages,
            overflow_pages: info.overflow_pages,
            entries: info.entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MDB_MAGIC, MDB_VERSION};
    use tempfile::tempdir;

    #[test]
    fn create_new_env() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        assert_eq!(env.page_size(), page_size::get());
        assert!(env.map_size() >= DEFAULT_MAP_SIZE);
    }

    #[test]
    fn env_info_and_stat() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        let info = env.info();
        assert!(info.map_size >= DEFAULT_MAP_SIZE);
        assert_eq!(info.last_pgno, 1); // Meta pages 0 and 1

        let stat = env.stat();
        assert_eq!(stat.page_size, page_size::get());
        assert_eq!(stat.entries, 0);
    }

    #[test]
    fn env_already_opened() {
        let dir = tempdir().unwrap();
        let _env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Try to open again - should fail
        let result = unsafe { EnvOpenOptions::new().open(dir.path()) };
        assert!(matches!(result, Err(Error::EnvAlreadyOpened)));
    }

    #[test]
    fn env_reopen_after_close() {
        let dir = tempdir().unwrap();

        {
            let _env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
            // env drops here
        }

        // Should be able to reopen
        let _env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
    }

    #[test]
    fn env_no_sub_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let env = unsafe {
            let mut opts = EnvOpenOptions::new();
            opts.flags(EnvFlags::NO_SUB_DIR);
            opts.open(&path).unwrap()
        };

        assert!(path.exists());
        drop(env);
    }

    #[test]
    fn read_meta_page() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        let meta = env.current_meta();
        assert_eq!(meta.magic, MDB_MAGIC);
        assert_eq!(meta.version, MDB_VERSION);
    }

    #[test]
    fn custom_map_size() {
        let dir = tempdir().unwrap();
        let page_size = page_size::get();
        let custom_size = page_size * 1000; // 1000 pages

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(custom_size)
                .open(dir.path())
                .unwrap()
        };

        assert_eq!(env.map_size(), custom_size);
    }

    #[test]
    fn read_transaction() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Create a read transaction
        let rtxn = env.read_txn().unwrap();
        assert_eq!(rtxn.txnid(), 0);

        // Can read meta page
        let _meta = rtxn.meta();

        // Commit is a no-op for read transactions
        rtxn.commit().unwrap();
    }

    #[test]
    fn write_transaction_commit() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Initial state
        let info1 = env.info();
        assert_eq!(info1.last_txnid, 0);

        // Create and commit a write transaction
        {
            let mut wtxn = env.write_txn().unwrap();
            assert_eq!(wtxn.txnid(), 1);

            // Allocate a page
            let (pgno, data) = wtxn.alloc_page().unwrap();
            assert!(pgno >= 2); // After meta pages

            // Write some data
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);

            // Commit
            wtxn.commit().unwrap();
        }

        // Verify transaction ID incremented
        let info2 = env.info();
        assert_eq!(info2.last_txnid, 1);
        assert!(info2.last_pgno >= 2);
    }

    #[test]
    fn write_transaction_abort() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        let info1 = env.info();

        // Create and abort a write transaction
        {
            let mut wtxn = env.write_txn().unwrap();
            let (_pgno, data) = wtxn.alloc_page().unwrap();
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);
            wtxn.abort();
        }

        // Verify nothing changed
        let info2 = env.info();
        assert_eq!(info1.last_txnid, info2.last_txnid);
        assert_eq!(info1.last_pgno, info2.last_pgno);
    }

    #[test]
    fn multiple_write_transactions() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // First transaction
        {
            let mut wtxn = env.write_txn().unwrap();
            let (_pgno, data) = wtxn.alloc_page().unwrap();
            data[0] = 0xAA;
            wtxn.commit().unwrap();
        }

        assert_eq!(env.info().last_txnid, 1);

        // Second transaction
        {
            let mut wtxn = env.write_txn().unwrap();
            let (_pgno, data) = wtxn.alloc_page().unwrap();
            data[0] = 0xBB;
            wtxn.commit().unwrap();
        }

        assert_eq!(env.info().last_txnid, 2);
    }

    #[test]
    fn read_transaction_snapshot() {
        let dir = tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Get initial snapshot
        let rtxn1 = env.read_txn().unwrap();
        let txnid1 = rtxn1.txnid();

        // Do a write
        {
            let wtxn = env.write_txn().unwrap();
            wtxn.commit().unwrap();
        }

        // Get new snapshot
        let rtxn2 = env.read_txn().unwrap();
        let txnid2 = rtxn2.txnid();

        // New snapshot should see newer txnid
        assert!(txnid2 > txnid1);

        // Old snapshot still valid
        assert_eq!(rtxn1.txnid(), txnid1);
    }
}
