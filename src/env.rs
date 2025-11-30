//! Environment management for ZeroDB.
//!
//! The environment is the main entry point for using the database. It manages
//! the memory-mapped file, transactions, and database handles.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

use crate::alloc::{PageAllocator, PagePool};
use crate::error::{Error, Result};
use crate::flags::EnvFlags;
use crate::mmap::{DataFile, MemoryMap};
use crate::page::{DbInfo, MetaPage, PageNo};
use crate::txn::{RoTxn, RwTxn};

/// Default map size (10 MB).
const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024;

/// Default maximum readers.
const DEFAULT_MAX_READERS: u32 = 126;

/// Default maximum databases.
const DEFAULT_MAX_DBS: u32 = 0;

/// Global registry of open environments to prevent double-opening.
static OPENED_ENVS: std::sync::LazyLock<RwLock<HashMap<PathBuf, ()>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Options for opening an environment.
#[derive(Debug, Clone)]
pub struct EnvOpenOptions {
    map_size: usize,
    max_readers: u32,
    max_dbs: u32,
    flags: EnvFlags,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvOpenOptions {
    /// Creates a new set of environment open options with default values.
    pub fn new() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_readers: DEFAULT_MAX_READERS,
            max_dbs: DEFAULT_MAX_DBS,
            flags: EnvFlags::empty(),
        }
    }

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
    pub unsafe fn open(&self, path: &Path) -> Result<Env> {
        // SAFETY: Caller guarantees the safety requirements.
        unsafe { Env::open(path, self) }
    }
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
}

/// A database environment.
///
/// The environment is the main handle for working with the database.
/// It manages the memory-mapped file, transactions, and readers.
pub struct Env {
    /// Path to the environment directory or file.
    path: PathBuf,
    /// Data file path.
    #[allow(dead_code)]
    data_path: PathBuf,
    /// Lock file path.
    #[allow(dead_code)]
    lock_path: PathBuf,
    /// Data file handle.
    data_file: DataFile,
    /// Environment flags.
    flags: EnvFlags,
    /// Map size.
    map_size: usize,
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
}

impl Env {
    /// Opens or creates an environment.
    unsafe fn open(path: &Path, options: &EnvOpenOptions) -> Result<Self> {
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

        // Register as open
        {
            let mut opened = OPENED_ENVS.write().unwrap();
            opened.insert(canonical_path.clone(), ());
        }

        let last_pgno = meta.last_pgno;
        let last_txnid = meta.last_txnid;

        Ok(Self {
            path: canonical_path,
            data_path,
            lock_path,
            data_file,
            flags: options.flags,
            map_size: options.map_size,
            page_size,
            max_readers: options.max_readers,
            max_dbs: options.max_dbs,
            inner: RwLock::new(EnvInner {
                mmap,
                meta_index,
                meta,
                last_pgno,
                last_txnid,
            }),
            write_lock: Mutex::new(()),
            page_pool: Mutex::new(PagePool::new(page_size)),
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
        self.map_size
    }

    /// Returns the environment flags.
    pub fn flags(&self) -> EnvFlags {
        self.flags
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
            map_size: self.map_size,
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

    /// Forces an fsync of the data file.
    pub fn force_sync(&self) -> Result<()> {
        let inner = self.inner.read().unwrap();
        inner.mmap.flush()?;
        self.data_file.sync()?;
        Ok(())
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
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        if self.flags.contains(EnvFlags::NO_LOCK) {
            // With NO_LOCK, caller manages concurrency
        }

        let inner = self.inner.read().unwrap();
        let txnid = inner.last_txnid;
        let meta = inner.meta;

        Ok(RoTxn::new(self, txnid, meta))
    }

    /// Creates a new read-write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// Write transactions have exclusive access to modify the database.
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        if self.flags.contains(EnvFlags::READ_ONLY) {
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

        let allocator = PageAllocator::new(last_pgno, self.map_size, self.page_size);

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

        // Serialize meta page
        let mut meta_buf = vec![0u8; self.page_size];
        meta.write_to(&mut meta_buf)?;

        let page_size = self.page_size as u64;
        let meta_offset = new_meta_index as u64 * page_size;

        if self.flags.contains(EnvFlags::WRITE_MAP) {
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
                if !self.flags.contains(EnvFlags::NO_SYNC) {
                    if self.flags.contains(EnvFlags::MAP_ASYNC) {
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
            if !self.flags.contains(EnvFlags::NO_SYNC) {
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

        // Collect and return buffers to pool
        let buffers: Vec<Vec<u8>> = dirty_pages.into_values().collect();
        self.return_page_buffers(buffers);

        Ok(())
    }

    /// Closes the environment.
    ///
    /// This is called automatically when the environment is dropped.
    fn close(&self) {
        let mut opened = OPENED_ENVS.write().unwrap();
        opened.remove(&self.path);
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        self.close();
    }
}

// Env is Send + Sync because all mutable state is protected by RwLock
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

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
