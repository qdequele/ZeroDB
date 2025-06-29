//! Environment management with type-state pattern

use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use crate::error::{Error, PageId, Result, TransactionId};
use crate::io::{IoBackend, MmapBackend};
use crate::meta::{DbInfo, MetaPage, META_PAGE_1, META_PAGE_2};
use crate::page::{Page, PageHeader, PAGE_SIZE};
use crate::reader::ReaderTable;
use crate::txn::{Read, Transaction, Write};

/// Environment configuration
#[derive(Debug, Clone)]
pub(crate) struct EnvConfig {
    /// Use segregated freelist for better allocation performance
    pub use_segregated_freelist: bool,
    /// Use NUMA-aware allocation
    #[allow(dead_code)]
    pub use_numa: bool,
}

/// Environment state marker traits
pub mod state {
    /// Sealed trait for environment states
    mod sealed {
        pub trait Sealed {}
    }

    /// Environment state trait
    pub trait State: sealed::Sealed {}

    /// Closed environment state
    #[derive(Debug)]
    pub struct Closed;
    impl sealed::Sealed for Closed {}
    impl State for Closed {}

    /// Open environment state
    #[derive(Debug)]
    pub struct Open;
    impl sealed::Sealed for Open {}
    impl State for Open {}

    /// Read-only environment state
    #[derive(Debug)]
    pub struct ReadOnly;
    impl sealed::Sealed for ReadOnly {}
    impl State for ReadOnly {}
}

use state::*;

/// Maximum number of named databases
pub const MAX_DBS: u32 = 128;

/// Default map size (1GB)
pub const DEFAULT_MAP_SIZE: usize = 1 << 30;

/// Durability modes for write transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// No sync - fastest but no durability guarantees
    NoSync,
    /// Async sync - data is flushed asynchronously (default)
    AsyncFlush,
    /// Sync data pages only - metadata might be lost
    SyncData,
    /// Full sync - both data and metadata are synced (safest)
    FullSync,
}

/// Page pool for recycling allocations
pub(crate) struct PagePool {
    /// Pool of recycled pages
    pages: Mutex<Vec<Box<Page>>>,
    /// Maximum pages to keep in pool
    #[allow(dead_code)]
    max_pages: usize,
}

impl PagePool {
    /// Create a new page pool
    pub fn new(max_pages: usize) -> Self {
        Self { pages: Mutex::new(Vec::with_capacity(max_pages)), max_pages }
    }

    /// Get a page from the pool or allocate a new one
    pub fn get(&self, page_id: PageId, flags: crate::page::PageFlags) -> Box<Page> {
        let mut pool = self.pages.lock();
        if let Some(mut page) = pool.pop() {
            // Reuse existing page
            page.header = PageHeader::new(page_id.0, flags);
            page.data = [0; PAGE_SIZE - PageHeader::SIZE];
            page
        } else {
            // Allocate new page
            Page::new(page_id, flags)
        }
    }

    /// Return a page to the pool
    #[allow(dead_code)]
    pub fn put(&self, page: Box<Page>) {
        let mut pool = self.pages.lock();
        if pool.len() < self.max_pages {
            pool.push(page);
        }
        // Otherwise, let it be deallocated
    }
}

/// Shared environment data
pub(crate) struct EnvInner {
    /// Path to the database
    _path: PathBuf,
    /// I/O backend
    pub(crate) io: Box<dyn IoBackend>,
    /// Current map size
    _map_size: usize,
    /// Current transaction ID
    pub(crate) txn_id: AtomicU64,
    /// Write lock
    pub(crate) write_lock: Mutex<()>,
    /// Reader table
    pub(crate) readers: ReaderTable,
    /// Named databases
    pub(crate) databases: RwLock<HashMap<Option<String>, DbInfo>>,
    /// Free pages tracking
    pub(crate) _free_pages: RwLock<Vec<PageId>>,
    /// Durability mode
    pub(crate) durability: DurabilityMode,
    /// Checksum mode
    pub(crate) checksum_mode: crate::checksum::ChecksumMode,
    /// Next page ID to allocate
    pub(crate) next_page_id: AtomicU64,
    /// Page pool for COW operations
    pub(crate) page_pool: PagePool,
    /// Use segregated freelist for better allocation performance
    pub(crate) use_segregated_freelist: bool,
    /// NUMA-aware page allocator (if enabled)
    pub(crate) numa_allocator: Option<Arc<crate::numa::NumaPageAllocator>>,
    /// Maximum key size in bytes
    #[allow(dead_code)]
    pub(crate) max_key_size: usize,
    /// Maximum value size in bytes (for inline storage)
    #[allow(dead_code)]
    pub(crate) max_value_size: usize,
    /// Maximum database size in bytes
    pub(crate) max_database_size: Option<usize>,
}

// Safety: EnvInner is Send/Sync because IoBackend is Send/Sync
unsafe impl Send for EnvInner {}
unsafe impl Sync for EnvInner {}

impl EnvInner {
    /// Get the current meta page
    pub(crate) fn meta(&self) -> Result<MetaPage> {
        // Read both meta pages
        let meta0 = self.io.read_page(META_PAGE_1)?;
        let meta1 = self.io.read_page(META_PAGE_2)?;
        
        // Validate checksums if enabled (meta pages always validated in MetaOnly mode)
        if self.checksum_mode != crate::checksum::ChecksumMode::None {
            use crate::checksum::ChecksummedPage;
            // For meta pages, we try to validate but don't fail if checksum is missing (0)
            // This allows for backward compatibility
            if meta0.has_checksum() {
                meta0.validate_checksum()?;
            }
            if meta1.has_checksum() {
                meta1.validate_checksum()?;
            }
        }

        // Validate MetaPage fits in page data before casting
        if size_of::<MetaPage>() > meta0.data.len() || size_of::<MetaPage>() > meta1.data.len() {
            return Err(Error::Corruption {
                details: "MetaPage size exceeds page data".into(),
                page_id: None,
            });
        }
        
        // Validate alignment
        if meta0.data.as_ptr() as usize % std::mem::align_of::<MetaPage>() != 0 ||
           meta1.data.as_ptr() as usize % std::mem::align_of::<MetaPage>() != 0 {
            return Err(Error::Corruption {
                details: "MetaPage not properly aligned".into(),
                page_id: None,
            });
        }
        
        // Cast data area to MetaPage
        let meta0 = unsafe { &*(meta0.data.as_ptr() as *const MetaPage) };
        let meta1 = unsafe { &*(meta1.data.as_ptr() as *const MetaPage) };

        // Validate and return the most recent valid one
        let meta0_valid = meta0.validate().is_ok();
        let meta1_valid = meta1.validate().is_ok();

        match (meta0_valid, meta1_valid) {
            (true, true) => {
                // Both valid, use the one with higher transaction ID
                if meta0.last_txnid.0 >= meta1.last_txnid.0 {
                    Ok(*meta0)
                } else {
                    Ok(*meta1)
                }
            }
            (true, false) => Ok(*meta0),
            (false, true) => Ok(*meta1),
            (false, false) => Err(Error::Corrupted),
        }
    }

    /// Get the non-current meta page ID (for writing)
    pub(crate) fn next_meta_page_id(&self) -> Result<PageId> {
        let meta = self.meta()?;
        // If current is page 0, next is page 1
        if meta.last_txnid.0 % 2 == 0 {
            Ok(META_PAGE_2)
        } else {
            Ok(META_PAGE_1)
        }
    }

    /// Validate key size against configured maximum
    #[allow(dead_code)]
    pub(crate) fn validate_key_size(&self, key_size: usize) -> Result<()> {
        if key_size > self.max_key_size {
            return Err(Error::KeyTooLarge {
                size: key_size,
                max_size: self.max_key_size,
            });
        }
        Ok(())
    }

    /// Validate value size against configured maximum
    #[allow(dead_code)]
    pub(crate) fn validate_value_size(&self, value_size: usize) -> Result<()> {
        if value_size > self.max_value_size {
            return Err(Error::ValueTooLarge {
                size: value_size,
                max_size: self.max_value_size,
            });
        }
        Ok(())
    }

    /// Validate page ID is within allocated range
    #[allow(dead_code)]
    pub(crate) fn validate_page_id(&self, page_id: PageId) -> Result<()> {
        let max_pages = self.io.size_in_pages();
        
        if page_id.0 >= max_pages {
            return Err(Error::PageOutOfBounds {
                page_id,
                max_pages,
            });
        }
        Ok(())
    }

    /// Check if adding new pages would exceed database size limit
    pub(crate) fn check_database_size_limit(&self, pages_to_add: u64) -> Result<()> {
        if let Some(max_size) = self.max_database_size {
            let current_pages = self.io.size_in_pages();
            // Use checked multiplication to prevent overflow
            let current_size = current_pages
                .checked_mul(PAGE_SIZE as u64)
                .ok_or(Error::Custom("Current database size calculation overflow".into()))?;
            let additional_size = pages_to_add
                .checked_mul(PAGE_SIZE as u64)
                .ok_or(Error::Custom("Additional size calculation overflow".into()))?;
            let new_size = current_size
                .checked_add(additional_size)
                .ok_or(Error::Custom("Total size calculation overflow".into()))?;
            
            if new_size > max_size as u64 {
                return Err(Error::DatabaseFull {
                    current_size,
                    requested_size: new_size,
                    max_size: max_size as u64,
                });
            }
        }
        Ok(())
    }

    /// Check for integer overflow in size calculations
    #[allow(dead_code)]
    pub(crate) fn check_size_overflow(&self, size1: usize, size2: usize) -> Result<usize> {
        size1.checked_add(size2).ok_or_else(|| Error::IntegerOverflow {
            operation: "size addition".to_string(),
            values: format!("{} + {}", size1, size2),
        })
    }
}

/// Database environment
pub struct Environment<S: State = Closed> {
    inner: Option<Arc<EnvInner>>,
    _state: PhantomData<S>,
}

/// Builder for creating environments
pub struct EnvBuilder {
    map_size: usize,
    max_readers: u32,
    max_dbs: u32,
    _flags: u32,
    durability: DurabilityMode,
    checksum_mode: crate::checksum::ChecksumMode,
    use_segregated_freelist: bool,
    use_numa: bool,
    max_key_size: usize,
    max_value_size: usize,
    max_database_size: Option<usize>,
}

impl EnvBuilder {
    /// Create a new environment builder
    pub fn new() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_readers: 126,
            max_dbs: MAX_DBS,
            _flags: 0,
            durability: DurabilityMode::AsyncFlush,
            checksum_mode: crate::checksum::ChecksumMode::Full,
            use_segregated_freelist: false,
            use_numa: false,
            max_key_size: crate::DEFAULT_MAX_KEY_SIZE,
            max_value_size: crate::page::MAX_VALUE_SIZE,
            max_database_size: None, // No limit by default
        }
    }

    /// Set the map size
    pub fn map_size(mut self, size: usize) -> Self {
        self.map_size = size;
        self
    }

    /// Set the maximum number of readers
    pub fn max_readers(mut self, readers: u32) -> Self {
        self.max_readers = readers;
        self
    }

    /// Set the maximum number of named databases
    pub fn max_dbs(mut self, dbs: u32) -> Self {
        self.max_dbs = dbs.min(MAX_DBS);
        self
    }

    /// Set the durability mode
    pub fn durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    /// Set the checksum mode
    pub fn checksum_mode(mut self, mode: crate::checksum::ChecksumMode) -> Self {
        self.checksum_mode = mode;
        self
    }

    /// Enable segregated freelist for better allocation performance
    pub fn use_segregated_freelist(mut self, enabled: bool) -> Self {
        self.use_segregated_freelist = enabled;
        self
    }

    /// Enable NUMA-aware memory allocation for multi-socket systems
    pub fn use_numa(mut self, enabled: bool) -> Self {
        self.use_numa = enabled;
        self
    }

    /// Set the maximum key size in bytes
    pub fn max_key_size(mut self, size: usize) -> Self {
        self.max_key_size = size;
        self
    }

    /// Set the maximum value size in bytes (for inline storage)
    pub fn max_value_size(mut self, size: usize) -> Self {
        self.max_value_size = size;
        self
    }

    /// Set the maximum database size in bytes
    pub fn max_database_size(mut self, size: usize) -> Self {
        self.max_database_size = Some(size);
        self
    }

    /// Build and open the environment
    pub fn open(self, path: impl AsRef<Path>) -> Result<Environment<Open>> {
        let path = path.as_ref();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(path)?;

        let data_path = path.join("data.mdb");
        let _lock_path = path.join("lock.mdb");

        // Create I/O backend
        let mut io: Box<dyn IoBackend> =
            Box::new(MmapBackend::with_options(&data_path, self.map_size as u64)?);

        // Check if this is a new database by trying to read meta pages
        let is_new_db = match io.read_page(META_PAGE_1) {
            Ok(page) => {
                let meta = unsafe { &*(page.data.as_ptr() as *const MetaPage) };
                meta.magic != crate::meta::MAGIC
            }
            Err(_) => true,
        };

        let last_txn_id;
        let mut last_page_id = 3; // After two meta pages and two root pages
        let meta_info;

        if is_new_db {
            // Initialize new database
            let mut meta = MetaPage::new();
            meta.mapsize = self.map_size as u64;
            meta.maxreaders = self.max_readers;
            meta.dbs = self.max_dbs;
            meta.last_txnid = TransactionId(0);
            meta.free_db.root = PageId(2);
            meta.main_db.root = PageId(3);
            meta.last_pg = PageId(3);

            // Write meta page 0
            let meta_page0 = Page::from_meta(&meta, META_PAGE_1);
            io.write_page(&meta_page0)?;

            // Write meta page 1
            meta.last_txnid = TransactionId(1);
            let meta_page1 = Page::from_meta(&meta, META_PAGE_2);
            io.write_page(&meta_page1)?;

            // Initialize free DB root page (page 2)
            let free_page = Page::new(PageId(2), crate::page::PageFlags::LEAF);
            io.write_page(&free_page)?;

            // Initialize main DB root page (page 3)
            let main_page = Page::new(PageId(3), crate::page::PageFlags::LEAF);
            io.write_page(&main_page)?;

            // Sync to disk
            io.sync()?;

            last_txn_id = 1;
            meta_info = meta;
        } else {
            // Load existing meta info
            let inner = Arc::new(EnvInner {
                _path: path.to_path_buf(),
                io,
                _map_size: self.map_size,
                txn_id: AtomicU64::new(0),
                write_lock: Mutex::new(()),
                readers: ReaderTable::new(self.max_readers as usize),
                databases: RwLock::new(HashMap::new()),
                _free_pages: RwLock::new(Vec::new()),
                durability: self.durability,
                checksum_mode: self.checksum_mode,
                next_page_id: AtomicU64::new(0),
                page_pool: PagePool::new(128), // Keep up to 128 pages in pool
                use_segregated_freelist: self.use_segregated_freelist,
                numa_allocator: None, // Will be initialized later if needed
                max_key_size: self.max_key_size,
                max_value_size: self.max_value_size,
                max_database_size: self.max_database_size,
            });

            meta_info = inner.meta()?;
            last_txn_id = meta_info.last_txnid.0;
            last_page_id = meta_info.last_pg.0;

            // Recreate with correct values
            drop(inner);
            io = Box::new(MmapBackend::with_options(&data_path, self.map_size as u64)?);
        }

        // Initialize reader table
        let readers = ReaderTable::new(self.max_readers as usize);

        let inner = Arc::new(EnvInner {
            _path: path.to_path_buf(),
            io,
            _map_size: self.map_size,
            txn_id: AtomicU64::new(last_txn_id),
            write_lock: Mutex::new(()),
            readers,
            databases: RwLock::new(HashMap::new()),
            _free_pages: RwLock::new(Vec::new()),
            durability: self.durability,
            checksum_mode: self.checksum_mode,
            next_page_id: AtomicU64::new(last_page_id + 1),
            page_pool: PagePool::new(128), // Keep up to 128 pages in pool
            use_segregated_freelist: self.use_segregated_freelist,
            numa_allocator: if self.use_numa {
                match crate::numa::NumaPageAllocator::new(256) {
                    Ok(allocator) => Some(Arc::new(allocator)),
                    Err(_) => {
                        // NUMA initialization failed, fall back to regular allocation
                        None
                    }
                }
            } else {
                None
            },
            max_key_size: self.max_key_size,
            max_value_size: self.max_value_size,
            max_database_size: self.max_database_size,
        });

        // Initialize main database entry
        {
            let mut dbs = inner.databases.write()
                .expect("Failed to acquire database lock during initialization");
            dbs.insert(None, meta_info.main_db);
        }

        // Note: Named databases will be loaded on-demand from the catalog
        // We can't load them here because we'd need a transaction, but the
        // environment isn't fully constructed yet

        Ok(Environment { inner: Some(inner), _state: PhantomData })
    }
}

impl Default for EnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for Environment<Closed> {
    fn default() -> Self {
        Self::new()
    }
}

impl Environment<Closed> {
    /// Create a new closed environment
    pub fn new() -> Self {
        Self { inner: None, _state: PhantomData }
    }

    /// Open the environment
    pub fn open(self, path: impl AsRef<Path>) -> Result<Environment<Open>> {
        EnvBuilder::new().open(path)
    }
}

impl Environment<Open> {
    /// Begin a read transaction
    pub fn read_txn(&self) -> Result<Transaction<'_, Read>> {
        Transaction::new_read(self)
    }

    /// Begin a write transaction
    pub fn write_txn(&self) -> Result<Transaction<'_, Write>> {
        Transaction::new_write(self)
    }

    /// Get inner reference (for internal use)
    pub(crate) fn inner(&self) -> &Arc<EnvInner> {
        self.inner.as_ref().expect("Environment not open")
    }

    /// Get environment configuration (for internal use)
    pub(crate) fn config(&self) -> EnvConfig {
        let inner = self.inner();
        EnvConfig {
            use_segregated_freelist: inner.use_segregated_freelist,
            use_numa: inner.numa_allocator.is_some(),
        }
    }

    /// Get inner reference (for testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn inner_test(&self) -> &Arc<EnvInner> {
        self.inner()
    }

    /// Sync data to disk
    pub fn sync(&self) -> Result<()> {
        let inner = self.inner();
        match inner.durability {
            DurabilityMode::NoSync => {
                // No sync requested
                Ok(())
            }
            DurabilityMode::AsyncFlush | DurabilityMode::SyncData | DurabilityMode::FullSync => {
                inner.io.sync()?;
                Ok(())
            }
        }
    }

    /// Force a full synchronous sync regardless of durability mode
    pub fn force_sync(&self) -> Result<()> {
        let inner = self.inner();
        inner.io.sync()?;
        Ok(())
    }
    
    /// Get current space usage information
    pub fn space_info(&self) -> Result<crate::space_info::SpaceInfo> {
        let inner = self.inner();
        let total_pages = inner.io.size_in_pages();
        let next_page_id = inner.next_page_id.load(std::sync::atomic::Ordering::Acquire);
        
        // Get free pages count from a read transaction
        let free_pages = {
            let _txn = self.read_txn()?;
            // Access the transaction's data to get freelist info
            if inner.use_segregated_freelist {
                // Count segregated freelist pages
                // For now, estimate based on the difference
                total_pages.saturating_sub(next_page_id)
            } else {
                // Count regular freelist pages
                total_pages.saturating_sub(next_page_id)
            }
        };
        
        let used_pages = next_page_id;
        let map_size = inner._map_size as u64;
        
        Ok(crate::space_info::SpaceInfo::new(
            total_pages,
            used_pages,
            free_pages,
            map_size,
        ))
    }

    /// Get environment statistics
    pub fn stat(&self) -> Result<crate::meta::DbStats> {
        let inner = self.inner();
        let meta = inner.meta()?;

        Ok(crate::meta::DbStats {
            psize: meta.psize,
            depth: meta.main_db.depth,
            branch_pages: meta.main_db.branch_pages,
            leaf_pages: meta.main_db.leaf_pages,
            overflow_pages: meta.main_db.overflow_pages,
            entries: meta.main_db.entries,
        })
    }
}

impl<S: State> Drop for Environment<S> {
    fn drop(&mut self) {
        if let Some(_inner) = self.inner.take() {
            // Meta page alternation handled in commit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_SIZE;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_env_creation() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())
            .unwrap();

        let stats = env.stat().unwrap();
        assert_eq!(stats.psize, PAGE_SIZE as u32);
    }

    #[test]
    fn test_env_reopen() {
        let dir = TempDir::new().unwrap();

        // Create and close environment
        {
            let env = EnvBuilder::new().open(dir.path()).unwrap();
            let _txn = env.write_txn().unwrap();
            // Transaction commits on drop
        }

        // Reopen and verify
        {
            let env = EnvBuilder::new().open(dir.path()).unwrap();
            let stats = env.stat().unwrap();
            assert_eq!(stats.psize, PAGE_SIZE as u32);
        }
    }

    #[test]
    fn test_durability_modes() {
        use crate::db::Database;
        let dir = TempDir::new().unwrap();

        // Test with FullSync mode
        {
            let env = Arc::new(
                EnvBuilder::new()
                    .map_size(10 * 1024 * 1024)
                    .durability(DurabilityMode::FullSync)
                    .open(dir.path())
                    .unwrap(),
            );

            // Create database and insert data
            let db: Database<String, String> = {
                let mut txn = env.write_txn().unwrap();
                let db = env.create_database(&mut txn, None).unwrap();

                db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
                db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();

                // Commit with full sync
                txn.commit().unwrap();
                db
            };

            // Force drop to close mmap
            drop(db);
            drop(env);
        }

        // Reopen and verify data persisted
        {
            let env =
                Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap());

            let txn = env.read_txn().unwrap();
            let db: Database<String, String> = env.open_database(&txn, None).unwrap();

            assert_eq!(db.get(&txn, &"key1".to_string()).unwrap(), Some("value1".to_string()));
            assert_eq!(db.get(&txn, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        }
    }

    #[test]
    fn test_no_sync_mode() {
        let dir = TempDir::new().unwrap();

        // Test with NoSync mode - should be fastest
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .durability(DurabilityMode::NoSync)
                .open(dir.path())
                .unwrap(),
        );

        let start = std::time::Instant::now();

        // Perform many small transactions
        for _ in 0..100 {
            let mut txn = env.write_txn().unwrap();
            // Just allocate a page
            let _ = txn.alloc_page(crate::page::PageFlags::LEAF).unwrap();
            txn.commit().unwrap();
        }

        let no_sync_duration = start.elapsed();

        // Now test with FullSync mode
        let dir2 = TempDir::new().unwrap();
        let env2 = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .durability(DurabilityMode::FullSync)
                .open(dir2.path())
                .unwrap(),
        );

        let start = std::time::Instant::now();

        // Perform same transactions
        for _ in 0..100 {
            let mut txn = env2.write_txn().unwrap();
            let _ = txn.alloc_page(crate::page::PageFlags::LEAF).unwrap();
            txn.commit().unwrap();
        }

        let full_sync_duration = start.elapsed();

        // NoSync should be significantly faster
        println!("NoSync: {:?}, FullSync: {:?}", no_sync_duration, full_sync_duration);
        assert!(no_sync_duration < full_sync_duration);
    }
}
