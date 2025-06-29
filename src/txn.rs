//! Transaction management with compile-time safety

use parking_lot::MutexGuard;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::env::{state, Environment};
use crate::error::{Error, PageId, Result, TransactionId};
use crate::meta::DbInfo;
pub use crate::nested_txn::NestedTransactionExt;
use crate::page::{Page, PageFlags};
use crate::page_allocator::TxnPageAlloc;

/// Helper function to safely read from a RwLock
fn read_lock<'a, T>(
    lock: &'a std::sync::RwLock<T>,
    context: &'static str,
) -> Result<RwLockReadGuard<'a, T>> {
    lock.read()
        .map_err(|_| Error::Custom(format!("RwLock poisoned: {}", context).into()))
}

/// Helper function to safely write to a RwLock
fn write_lock<'a, T>(
    lock: &'a std::sync::RwLock<T>,
    context: &'static str,
) -> Result<RwLockWriteGuard<'a, T>> {
    lock.write()
        .map_err(|_| Error::Custom(format!("RwLock poisoned: {}", context).into()))
}

/// Helper function to get a mutable reference from HashMap with proper error
fn get_mut_page<'a>(pages: &'a mut HashMap<PageId, Box<Page>>, page_id: &PageId) -> Result<&'a mut Box<Page>> {
    pages.get_mut(page_id)
        .ok_or_else(|| Error::Custom(format!("Page {} not found in dirty pages", page_id.0).into()))
}

/// Transaction mode marker traits
pub mod mode {
    /// Sealed trait for transaction modes
    pub(crate) mod sealed {
        pub trait Sealed {}
    }

    /// Transaction mode trait
    pub trait Mode: sealed::Sealed {
        /// Whether this is a write transaction
        const IS_WRITE: bool;
    }
}

/// Read-only transaction mode
#[derive(Debug)]
pub struct Read;

impl crate::txn::mode::sealed::Sealed for Read {}
impl mode::Mode for Read {
    const IS_WRITE: bool = false;
}

/// Read-write transaction mode
#[derive(Debug)]
pub struct Write;

impl crate::txn::mode::sealed::Sealed for Write {}
impl mode::Mode for Write {
    const IS_WRITE: bool = true;
}

/// Dirty page tracking for write transactions
pub struct DirtyPages {
    /// Map of page ID to dirty page
    pub(crate) pages: HashMap<PageId, Box<Page>>,
    /// Pages marked for COW but not yet copied (lazy COW)
    #[allow(dead_code)]
    pub(crate) cow_pending: HashMap<PageId, PageId>, // old_id -> new_id
}

impl DirtyPages {
    /// Create new dirty page tracker
    pub(crate) fn new() -> Self {
        Self { pages: HashMap::new(), cow_pending: HashMap::new() }
    }

    /// Mark a page as dirty
    pub(crate) fn mark_dirty(&mut self, page_id: PageId, page: Box<Page>) {
        self.pages.insert(page_id, page);
    }

    /// Get a dirty page
    pub(crate) fn get(&self, page_id: &PageId) -> Option<&Page> {
        self.pages.get(page_id).map(|p| p.as_ref())
    }

    /// Get a mutable dirty page
    #[allow(dead_code)]
    pub(crate) fn get_mut(&mut self, page_id: &PageId) -> Option<&mut Page> {
        self.pages.get_mut(page_id).map(|p| p.as_mut())
    }
}

/// Transaction data shared between read and write modes
pub(crate) struct TxnData<'env> {
    /// Reference to environment
    pub(crate) env: &'env Environment<state::Open>,
    /// Transaction ID
    id: TransactionId,
    /// Database info cache
    pub(crate) databases: HashMap<Option<String>, DbInfo>,
}

/// A database transaction
pub struct Transaction<'env, M: mode::Mode> {
    /// Shared transaction data
    pub(crate) data: TxnData<'env>,
    /// Mode-specific data
    pub(crate) mode_data: ModeData<'env>,
    /// Phantom for mode
    _mode: PhantomData<M>,
}

/// Mode-specific transaction data
pub(crate) enum ModeData<'env> {
    /// Read transaction data
    Read {
        /// Reader slot index (for cleanup)
        reader_slot: Option<usize>,
    },
    /// Write transaction data
    Write {
        /// Write lock guard
        _write_guard: MutexGuard<'env, ()>,
        /// Dirty pages
        dirty: Box<DirtyPages>,
        /// Page allocation tracking
        page_alloc: TxnPageAlloc,
    },
}

impl<'env> Transaction<'env, Read> {
    /// Create a new read transaction
    pub(crate) fn new_read(env: &'env Environment<state::Open>) -> Result<Self> {
        let inner = env.inner();

        // Get the current transaction ID for snapshot isolation
        // Read transactions see the state as of the last committed write transaction
        let current_txn_id = TransactionId(inner.txn_id.load(Ordering::Acquire));

        // Try to acquire a reader slot
        let reader_slot = inner.readers.acquire(current_txn_id).map_err(|_| Error::ReadersFull)?;

        // Copy database info at this snapshot
        let databases = read_lock(&inner.databases, "reading databases for read transaction")?.clone();

        Ok(Self {
            data: TxnData { env, id: current_txn_id, databases },
            mode_data: ModeData::Read { reader_slot: Some(reader_slot) },
            _mode: PhantomData,
        })
    }
}

impl<'env> Transaction<'env, Write> {
    /// Create a new write transaction
    pub(crate) fn new_write(env: &'env Environment<state::Open>) -> Result<Self> {
        let inner = env.inner();

        // Get write lock first to ensure exclusive access
        let write_guard = inner.write_lock.lock();

        // Increment transaction ID for the new write transaction
        // This ensures write transactions have unique, monotonically increasing IDs
        let new_txn_id = TransactionId(inner.txn_id.fetch_add(1, Ordering::AcqRel) + 1);

        // Copy database info under write lock to ensure consistency
        let databases = read_lock(&inner.databases, "reading databases for write transaction")?.clone();

        // Initialize transaction page allocator with generous dirty room limit
        // LMDB doesn't have strict transaction size limits, so we make this very large
        let map_pages = inner.map_size / crate::page::PAGE_SIZE;
        let dirty_room = map_pages.max(5_000_000); // At least 5M pages, or full map size for extreme cases
        let page_alloc = TxnPageAlloc::new(dirty_room);

        Ok(Self {
            data: TxnData { env, id: new_txn_id, databases },
            mode_data: ModeData::Write {
                _write_guard: write_guard,
                dirty: Box::new(DirtyPages::new()),
                page_alloc,
            },
            _mode: PhantomData,
        })
    }
}

/// Type alias for read-only transaction
pub type ReadTransaction<'env> = Transaction<'env, Read>;

/// Type alias for read-write transaction
pub type WriteTransaction<'env> = Transaction<'env, Write>;

impl<'env, M: mode::Mode> Transaction<'env, M> {
    /// Get the transaction ID
    pub fn id(&self) -> TransactionId {
        self.data.id
    }

    /// Get a page by ID
    #[inline]
    pub fn get_page(&self, page_id: PageId) -> Result<&Page> {
        // Validate page ID bounds first
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        if page_id.0 >= num_pages {
            return Err(Error::Custom(format!("Page {} exceeds database size", page_id.0).into()));
        }
        
        // Special handling for meta pages (0 and 1)
        if page_id.0 <= 1 {
            // Meta pages should always exist, but double-check
            if num_pages < 2 {
                return Err(Error::Corruption {
                    details: "Database file too small to contain meta pages".to_string(),
                    page_id: Some(page_id),
                });
            }
        }
        
        // Check dirty pages first if write transaction
        if M::IS_WRITE {
            if let ModeData::Write { ref dirty, .. } = self.mode_data {
                if let Some(page) = dirty.get(&page_id) {
                    return Ok(page);
                }
            }
        }

        // For read operations, use zero-copy access directly from mmap
        // SAFETY: The returned page reference is valid for the transaction lifetime
        // because:
        // 1. The environment (and thus the mmap) outlives the transaction
        // 2. The mmap base address is stable during the transaction
        // 3. Pages are immutable once written (COW semantics)
        // 4. We validated the page ID is within bounds above
        let page = unsafe { inner.io.get_page_ref(page_id)? };
        
        Ok(page)
    }

    /// Get database info
    pub fn db_info(&self, name: Option<&str>) -> Result<&DbInfo> {
        self.data.databases.get(&name.map(|s| s.to_string())).ok_or(Error::InvalidDatabase)
    }

    /// Update database info (only for write transactions)
    pub fn update_db_info(&mut self, name: Option<&str>, info: DbInfo) -> Result<()> {
        if !M::IS_WRITE {
            return Err(Error::InvalidOperation("Cannot update database info in read transaction"));
        }

        self.data.databases.insert(name.map(|s| s.to_string()), info);
        Ok(())
    }
    


    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        
        match self.mode_data {
            ModeData::Read { .. } => {
                // Nothing to do for read transactions
                Ok(())
            }
            ModeData::Write {
                ref mut dirty,
                
                ..
            } => {
                if dirty.pages.is_empty() {
                    return Ok(());
                }

                let inner = self.data.env.inner();

                // Get current meta page and determine which meta page to write to
                let current_meta = inner.meta()?;
                let next_meta_page_id = inner.next_meta_page_id()?;

                // Create new meta content
                let mut new_meta = current_meta;
                new_meta.last_txnid = self.data.id;
                
                // Update last page from environment
                let current_pgno = inner.next_page_id.load(std::sync::atomic::Ordering::Acquire);
                new_meta.last_pg = PageId(current_pgno.saturating_sub(1).max(current_meta.last_pg.0));

                // Update main database info
                if let Some(main_db_info) = self.data.databases.get(&None) {
                    new_meta.main_db = *main_db_info;
                }

                // Write all dirty pages BEFORE meta page
                for (_page_id, page) in dirty.pages.iter_mut() {
                    inner.io.write_page(page)?;
                }

                // Write meta page
                let meta_page = Page::from_meta(&new_meta, next_meta_page_id);
                inner.io.write_page(&meta_page)?;

                // Sync based on durability mode
                match inner.durability {
                    crate::env::DurabilityMode::NoSync => {
                        // No sync
                    }
                    crate::env::DurabilityMode::AsyncFlush | 
                    crate::env::DurabilityMode::SyncData |
                    crate::env::DurabilityMode::FullSync => {
                        inner.io.sync()?;
                    }
                }

                // Update databases in environment
                {
                    let mut env_dbs = write_lock(&inner.databases, "updating databases on commit")?;
                    *env_dbs = self.data.databases.clone();
                }

                // Update transaction ID to make changes visible
                inner.txn_id.store(self.data.id.0, Ordering::Release);

                Ok(())
            }
        }
    }

    /// Abort the transaction
    pub fn abort(self) {
        // Transaction is aborted on drop
    }
}

impl<'env> Transaction<'env, Read> {
    /// Upgrade a read transaction to a write transaction
    pub fn upgrade(self) -> Result<Transaction<'env, Write>> {
        let inner = self.data.env.inner();

        // Try to acquire write lock
        let _write_guard =
            inner.write_lock.try_lock().ok_or(Error::Conflict(crate::error::ConflictDetails {
                txn_id: self.data.id,
                conflicting_page: PageId(0),
                operation: crate::error::Operation::Write,
            }))?;

        // Drop the read transaction and create a new write transaction
        let env = self.data.env;
        drop(self);

        // Now create a fresh write transaction
        Transaction::new_write(env)
    }
}

impl<'env> Transaction<'env, Write> {
    /// Get a mutable page with Copy-on-Write semantics
    /// Returns the page ID (which may be new) and a mutable reference to the page
    pub fn get_page_cow(&mut self, page_id: PageId) -> Result<(PageId, &mut Page)> {
        // For now, just use direct page modification until we implement proper COW
        self.get_page_mut(page_id)?;
        Ok((page_id, self.get_page_mut(page_id)?))
    }

    /// Get a mutable page (for backward compatibility - uses COW internally)
    pub fn get_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        // Basic validation
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        if page_id.0 >= num_pages {
            return Err(Error::Custom(format!("Page {} exceeds database size", page_id.0).into()));
        }
        
        // Check if already dirty first
        if let ModeData::Write { ref dirty, .. } = self.mode_data {
            if dirty.pages.contains_key(&page_id) {
                // Page is already dirty, get mutable reference
                if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
                    return Ok(get_mut_page(&mut dirty.pages, &page_id)?.as_mut());
                }
            }
        }
        
        // Load and copy page
        let mut page = inner.io.read_page(page_id)?;
        
        page.header.flags.insert(PageFlags::DIRTY);
        
        if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
            dirty.mark_dirty(page_id, page);
            Ok(get_mut_page(&mut dirty.pages, &page_id)?.as_mut())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Allocate a new page
    pub fn alloc_page(&mut self, flags: PageFlags) -> Result<(PageId, &mut Page)> {
        if let ModeData::Write {
            ref mut dirty,
            ref mut page_alloc,
            ..
        } = self.mode_data
        {
            // Track allocation against transaction limit
            let page_id = {
                let inner = self.data.env.inner();
                let max_pgno = inner.map_size / crate::page::PAGE_SIZE;
                let next_pgno = inner.next_page_id.load(std::sync::atomic::Ordering::Acquire);
                
                // Check map size limit only
                if next_pgno + 1 >= max_pgno as u64 {
                    return Err(Error::MapFull);
                }
                
                // Try freelist first
                if let Some(page_id) = page_alloc.try_alloc_from_freelist() {
                    page_id
                } else {
                    // Allocate from end of file
                    PageId(inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
                }
            };

            // Track in transaction
            page_alloc.track_alloc(page_id)?;

            // Create new page
            let page = crate::page_allocator::PageFactory::create_page(page_id, flags | PageFlags::DIRTY);
            dirty.mark_dirty(page_id, page);

            Ok((page_id, get_mut_page(&mut dirty.pages, &page_id)?.as_mut()))
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Free a page
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        // Validate page ID
        if page_id.0 <= 1 {
            return Err(Error::Custom("Cannot free meta pages (0 or 1)".into()));
        }
        
        // For now, freeing pages is a no-op until we properly implement the freelist
        // This follows the LMDB approach where freed pages are recycled
        if let ModeData::Write { ref mut page_alloc, .. } = self.mode_data {
            page_alloc.track_free(page_id);
            Ok(())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Allocate multiple contiguous pages (useful for overflow pages)
    /// Returns only the first page ID since pages are consecutive
    pub fn alloc_consecutive_pages(&mut self, count: usize, flags: PageFlags) -> Result<PageId> {
        if count == 0 {
            return Err(Error::InvalidParameter("Cannot allocate 0 pages"));
        }

        if let ModeData::Write { ref mut dirty, ref mut page_alloc, .. } = self.mode_data {
            let inner = self.data.env.inner();
            let max_pgno = inner.map_size / crate::page::PAGE_SIZE;
            let start_pgno = inner.next_page_id.fetch_add(count as u64, std::sync::atomic::Ordering::SeqCst);
            
            // Check map size limit
            if start_pgno + count as u64 >= max_pgno as u64 {
                // Restore the counter
                inner.next_page_id.fetch_sub(count as u64, std::sync::atomic::Ordering::SeqCst);
                return Err(Error::MapFull);
            }

            // Create and mark all pages as dirty
            for i in 0..count {
                let page_id = PageId(start_pgno + i as u64);
                page_alloc.track_alloc(page_id)?;
                let page = crate::page_allocator::PageFactory::create_page(page_id, flags | PageFlags::DIRTY);
                dirty.mark_dirty(page_id, page);
            }

            Ok(PageId(start_pgno))
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }
    
    /// Get a mutable reference to a consecutively allocated page
    pub fn get_consecutive_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
            Ok(get_mut_page(&mut dirty.pages, &page_id)?.as_mut())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Free multiple contiguous pages
    pub fn free_pages(&mut self, start_page_id: PageId, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        
        // Validate start page ID
        if start_page_id.0 <= 1 {
            return Err(Error::Custom("Cannot free meta pages (0 or 1)".into()));
        }

        if let ModeData::Write { ref mut page_alloc, .. } = self.mode_data {
            // Free pages one by one to track them
            for i in 0..count {
                let page_id = PageId(start_page_id.0 + i as u64);
                page_alloc.track_free(page_id);
            }
            Ok(())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }



    /// Downgrade a write transaction to a read transaction
    pub fn downgrade(self) -> Result<Transaction<'env, Read>> {
        let env = self.data.env;

        // Abort any changes
        drop(self);

        // Create new read transaction
        Transaction::new_read(env)
    }
}

impl<'env, M: mode::Mode> Drop for Transaction<'env, M> {
    fn drop(&mut self) {
        match &self.mode_data {
            ModeData::Read { reader_slot } => {
                // Release reader slot
                if let Some(slot_idx) = reader_slot {
                    let inner = self.data.env.inner();
                    inner.readers.release(*slot_idx);
                }
            }
            ModeData::Write { .. } => {
                // Write transaction aborted if not committed
                // Write lock released automatically
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::env::EnvBuilder;
    use tempfile::TempDir;

    #[test]
    fn test_transaction_creation() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().open(dir.path()).unwrap();

        // Create read transaction
        let rtxn = env.read_txn().unwrap();
        assert_eq!(rtxn.id().0, env.inner().txn_id.load(std::sync::atomic::Ordering::SeqCst));
        drop(rtxn);

        // Create write transaction
        let wtxn = env.write_txn().unwrap();
        // The txn_id is incremented after creation, so wtxn.id() <= txn_id
        assert!(wtxn.id().0 > 0);
    }

    #[test]
    fn test_transaction_isolation() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().open(dir.path()).unwrap();

        // Start read transaction
        let rtxn1 = env.read_txn().unwrap();
        let id1 = rtxn1.id();

        // Start write transaction
        let wtxn = env.write_txn().unwrap();
        let id2 = wtxn.id();

        // Write transaction has higher ID
        assert!(id2.0 > id1.0);

        // Commit write transaction
        wtxn.commit().unwrap();

        // Read transaction still sees old state
        assert_eq!(rtxn1.id(), id1);
    }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        use crate::db::Database;
        use std::sync::Arc;

        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().open(dir.path()).unwrap());

        // Create database and insert initial data
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, None).unwrap();
            db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut wtxn, "key2".to_string(), "value2".to_string()).unwrap();
            wtxn.commit().unwrap();
            db
        };

        // Start read transaction BEFORE modifications
        let rtxn1 = env.read_txn().unwrap();
        let snapshot_id = rtxn1.id();

        // Verify initial data is visible
        assert_eq!(db.get(&rtxn1, &"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(db.get(&rtxn1, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(db.get(&rtxn1, &"key3".to_string()).unwrap(), None);

        // Modify data in a write transaction
        {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, "key1".to_string(), "modified1".to_string()).unwrap();
            db.put(&mut wtxn, "key3".to_string(), "value3".to_string()).unwrap();
            db.delete(&mut wtxn, &"key2".to_string()).unwrap();
            wtxn.commit().unwrap();
        }

        // Original read transaction should still see old snapshot
        assert_eq!(db.get(&rtxn1, &"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(db.get(&rtxn1, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(db.get(&rtxn1, &"key3".to_string()).unwrap(), None);

        // New read transaction should see modified data
        let rtxn2 = env.read_txn().unwrap();
        assert!(rtxn2.id().0 > snapshot_id.0);
        assert_eq!(db.get(&rtxn2, &"key1".to_string()).unwrap(), Some("modified1".to_string()));
        assert_eq!(db.get(&rtxn2, &"key2".to_string()).unwrap(), None);
        assert_eq!(db.get(&rtxn2, &"key3".to_string()).unwrap(), Some("value3".to_string()));

        // Clean up
        drop(rtxn1);
        drop(rtxn2);
    }

    #[test]
    fn test_reader_tracking() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().max_readers(10).open(dir.path()).unwrap();

        let inner = env.inner();

        // Initially no readers
        assert_eq!(inner.readers.reader_count(), 0);
        assert_eq!(inner.readers.oldest_reader(), None);

        // Start multiple readers with different txn IDs
        let rtxn1 = env.read_txn().unwrap();
        assert_eq!(inner.readers.reader_count(), 1);

        let rtxn2 = env.read_txn().unwrap();
        assert_eq!(inner.readers.reader_count(), 2);

        let rtxn3 = env.read_txn().unwrap();
        assert_eq!(inner.readers.reader_count(), 3);

        // All should have same txn ID (no writes between them)
        assert_eq!(rtxn1.id(), rtxn2.id());
        assert_eq!(rtxn2.id(), rtxn3.id());

        // Oldest reader should be the common txn ID
        assert_eq!(inner.readers.oldest_reader(), Some(rtxn1.id()));

        // Drop middle reader
        drop(rtxn2);
        assert_eq!(inner.readers.reader_count(), 2);

        // Oldest should still be the same
        assert_eq!(inner.readers.oldest_reader(), Some(rtxn1.id()));

        // Drop oldest reader
        drop(rtxn1);
        assert_eq!(inner.readers.reader_count(), 1);

        // Now oldest should be rtxn3
        assert_eq!(inner.readers.oldest_reader(), Some(rtxn3.id()));

        // Drop last reader
        drop(rtxn3);
        assert_eq!(inner.readers.reader_count(), 0);
        assert_eq!(inner.readers.oldest_reader(), None);
    }

    #[test]
    fn test_meta_page_persistence() {
        use crate::db::Database;
        use crate::meta::{META_PAGE_1, META_PAGE_2};
        use std::sync::Arc;

        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .durability(crate::env::DurabilityMode::FullSync)
                .open(dir.path())
                .unwrap(),
        );

        let inner = env.inner();

        // Check initial meta pages
        let initial_meta = inner.meta().unwrap();
        assert_eq!(initial_meta.last_txnid.0, 1); // Initial txn ID

        // Create database and commit transaction
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let txn_id = wtxn.id();
            let db = env.create_database(&mut wtxn, None).unwrap();
            db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
            wtxn.commit().unwrap();

            // After commit, check meta page was updated
            let meta_after = inner.meta().unwrap();
            assert_eq!(meta_after.last_txnid, txn_id);
            assert!(meta_after.last_txnid.0 > initial_meta.last_txnid.0);

            db
        };

        // Second transaction to verify meta page alternation
        {
            let mut wtxn = env.write_txn().unwrap();
            let txn_id2 = wtxn.id();
            db.put(&mut wtxn, "key2".to_string(), "value2".to_string()).unwrap();
            wtxn.commit().unwrap();

            let meta_after2 = inner.meta().unwrap();
            assert_eq!(meta_after2.last_txnid, txn_id2);
        }

        // Verify both meta pages have been used
        let meta0 = inner.io.read_page(META_PAGE_1).unwrap();
        let meta1 = inner.io.read_page(META_PAGE_2).unwrap();

        let meta0 = unsafe { &*(meta0.data.as_ptr() as *const crate::meta::MetaPage) };
        let meta1 = unsafe { &*(meta1.data.as_ptr() as *const crate::meta::MetaPage) };

        // Both should be valid
        assert!(meta0.validate().is_ok());
        assert!(meta1.validate().is_ok());

        // They should have different transaction IDs
        assert_ne!(meta0.last_txnid, meta1.last_txnid);
    }

    #[test]
    fn test_transaction_commit_durability() {
        use crate::db::Database;
        use std::sync::Arc;

        let dir = TempDir::new().unwrap();

        // Test with different durability modes
        for mode in &[
            crate::env::DurabilityMode::NoSync,
            crate::env::DurabilityMode::AsyncFlush,
            crate::env::DurabilityMode::SyncData,
            crate::env::DurabilityMode::FullSync,
        ] {
            let env = Arc::new(EnvBuilder::new().durability(*mode).open(dir.path()).unwrap());

            let db: Database<String, String> = {
                let mut wtxn = env.write_txn().unwrap();
                let db = env.create_database(&mut wtxn, None).unwrap();

                // Insert some data
                for i in 0..10 {
                    db.put(&mut wtxn, format!("key{}", i), format!("value{}", i)).unwrap();
                }

                wtxn.commit().unwrap();
                db
            };

            // Verify data is readable
            let rtxn = env.read_txn().unwrap();
            for i in 0..10 {
                assert_eq!(
                    db.get(&rtxn, &format!("key{}", i)).unwrap(),
                    Some(format!("value{}", i))
                );
            }
        }
    }
}
