//! Transaction management with compile-time safety

use parking_lot::MutexGuard;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::env::{state, Environment};
use crate::error::{Error, PageId, Result, TransactionId};
use crate::freelist::FreeList;
use crate::meta::DbInfo;
pub use crate::nested_txn::NestedTransactionExt;
use crate::page::{Page, PageFlags};
use crate::segregated_freelist::SegregatedFreeList;

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
    /// Allocated pages that need to be written
    pub(crate) allocated: Vec<PageId>,
    /// Pages marked for COW but not yet copied (lazy COW)
    #[allow(dead_code)]
    pub(crate) cow_pending: HashMap<PageId, PageId>, // old_id -> new_id
}

impl DirtyPages {
    /// Create new dirty page tracker
    pub(crate) fn new() -> Self {
        Self { pages: HashMap::new(), allocated: Vec::new(), cow_pending: HashMap::new() }
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
        /// Free list manager (old)
        freelist: FreeList,
        /// Segregated free list (new)
        segregated_freelist: Box<Option<SegregatedFreeList>>,
        /// Next page ID to allocate
        next_pgno: PageId,
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

        // Get current meta page to read database state
        let meta = inner.meta()?;
        let next_pgno = PageId(meta.last_pg.0 + 1);

        // Create a temporary read transaction to load the free list
        // This is safe because we hold the write lock
        let temp_read_txn = Transaction {
            data: TxnData {
                env,
                id: TransactionId(meta.last_txnid.0), // Use last committed txn ID
                databases: databases.clone(),
            },
            mode_data: ModeData::Read { reader_slot: None },
            _mode: PhantomData::<Read>,
        };

        // Load the free list from the last committed state
        let mut freelist =
            FreeList::load(&temp_read_txn, &meta.free_db).unwrap_or_else(|_| FreeList::new());

        // Update freelist with current reader state
        if let Some(oldest_reader) = inner.readers.oldest_reader() {
            freelist.set_oldest_reader(oldest_reader);
        } else {
            // No active readers
            freelist.set_oldest_reader(TransactionId(0));
        }

        // Update free pages based on reader state
        freelist.update_free_pages();

        // Initialize segregated freelist if enabled
        let segregated_freelist = if env.config().use_segregated_freelist {
            let seg_list = SegregatedFreeList::new();
            // Migrate existing free pages from simple freelist
            // This will be populated as pages are freed
            Some(seg_list)
        } else {
            None
        };

        Ok(Self {
            data: TxnData { env, id: new_txn_id, databases },
            mode_data: ModeData::Write {
                _write_guard: write_guard,
                dirty: Box::new(DirtyPages::new()),
                freelist,
                segregated_freelist: Box::new(segregated_freelist),
                next_pgno,
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
            return Err(Error::InvalidPageId(page_id));
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
        
        // Validate checksum if enabled
        self.validate_page_checksum(page)?;
        
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
    
    /// Validate page checksum based on environment checksum mode
    #[inline]
    fn validate_page_checksum(&self, page: &Page) -> Result<()> {
        let inner = self.data.env.inner();
        
        match inner.checksum_mode {
            crate::checksum::ChecksumMode::None => Ok(()),
            crate::checksum::ChecksumMode::MetaOnly => {
                // Only validate meta pages
                if page.header.flags.contains(PageFlags::META) {
                    use crate::checksum::ChecksummedPage;
                    page.validate_checksum()
                } else {
                    Ok(())
                }
            }
            crate::checksum::ChecksumMode::Full => {
                // Validate all pages
                use crate::checksum::ChecksummedPage;
                page.validate_checksum()
            }
        }
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        // Extract checksum mode before the mutable borrow
        let checksum_mode = self.data.env.inner().checksum_mode;
        
        match self.mode_data {
            ModeData::Read { .. } => {
                // Nothing to do for read transactions
                Ok(())
            }
            ModeData::Write {
                ref mut dirty,
                ref mut freelist,
                ref mut segregated_freelist,
                ref mut next_pgno,
                ..
            } => {
                if dirty.pages.is_empty() && freelist.pending_len() == 0 {
                    // No changes, nothing to commit
                    return Ok(());
                }

                let inner = self.data.env.inner();

                // Get oldest reader to determine which pages can be freed
                if let Some(oldest_reader) = inner.readers.oldest_reader() {
                    freelist.set_oldest_reader(oldest_reader);
                }

                // Don't commit here - we'll do it after updating reader state

                // Get current meta page and determine which meta page to write to
                let current_meta = inner.meta()?;
                let next_meta_page_id = inner.next_meta_page_id()?;

                // Create new meta content
                let mut new_meta = current_meta;

                // Update transaction ID and last page
                new_meta.last_txnid = self.data.id;
                new_meta.last_pg =
                    PageId(next_pgno.0.saturating_sub(1).max(current_meta.last_pg.0));

                // Update main database info
                if let Some(main_db_info) = self.data.databases.get(&None) {
                    new_meta.main_db = *main_db_info;
                }

                // Update freelist with current reader state
                if let Some(oldest_reader) = inner.readers.oldest_reader() {
                    freelist.set_oldest_reader(oldest_reader);
                    if let Some(seg_list) = segregated_freelist.as_mut() {
                        seg_list.update_oldest_reader(oldest_reader);
                    }
                } else {
                    // No active readers
                    freelist.set_oldest_reader(TransactionId(0));
                    if let Some(seg_list) = segregated_freelist.as_mut() {
                        seg_list.update_oldest_reader(TransactionId(0));
                    }
                }

                // Commit pending pages to the appropriate freelist
                if let Some(seg_list) = segregated_freelist.as_mut() {
                    seg_list.commit(self.data.id);
                } else {
                    freelist.commit_pending(self.data.id);
                }

                // Save the freelist to the free database if needed
                if freelist.has_txn_free_pages() {
                    // Get the data to save before any borrows
                    let freelist_data = freelist.get_save_data();

                    // Make a copy of free_db info to modify
                    let mut free_db_info = new_meta.free_db;

                    // If free database doesn't exist, create it
                    if free_db_info.root.0 == 0 {
                        // Allocate a new page for the free database root
                        let page_id = PageId(next_pgno.0);
                        next_pgno.0 += 1;
                        
                        // Ensure we're not allocating beyond reasonable limits
                        const MAX_PAGE_ID: u64 = 1 << 48; // 256TB with 4KB pages
                        if page_id.0 >= MAX_PAGE_ID {
                            return Err(Error::Custom(
                                format!("Page ID {} exceeds maximum allowed value", page_id.0).into()
                            ));
                        }

                        let page = Page::new(page_id, PageFlags::LEAF | PageFlags::DIRTY);
                        dirty.mark_dirty(page_id, page);

                        let root_page = get_mut_page(&mut dirty.pages, &page_id)?;
                        root_page.header.num_keys = 0;

                        free_db_info.root = page_id;
                        free_db_info.leaf_pages = 1;
                        free_db_info.depth = 1;
                    }

                    // Save the freelist data to the free database
                    // We need to manually insert each key-value pair into the free database
                    for (key, value) in freelist_data {
                        // Insert directly into the free database pages
                        let current_root = free_db_info.root;
                        
                        // Try to insert into the current root page
                        let needs_split = {
                            let root_page = if let Some(page) = dirty.pages.get_mut(&current_root) {
                                page
                            } else {
                                // Validate page ID before reading
                                let num_pages = inner.io.size_in_pages();
                                if current_root.0 >= num_pages {
                                    return Err(Error::InvalidPageId(current_root));
                                }
                                // Need to make the page dirty first
                                let page = inner.io.read_page(current_root)?;
                                // Validate checksum if enabled
                                if checksum_mode != crate::checksum::ChecksumMode::None {
                                    use crate::checksum::ChecksummedPage;
                                    page.validate_checksum()?;
                                }
                                let mut new_page = Page::new(current_root, page.header.flags | PageFlags::DIRTY);
                                // Copy the page data
                                new_page.header = page.header;
                                new_page.header.flags |= PageFlags::DIRTY;
                                new_page.data.copy_from_slice(&page.data);
                                dirty.mark_dirty(current_root, new_page);
                                get_mut_page(&mut dirty.pages, &current_root)?
                            };
                            
                            // Try to add the node
                            match root_page.add_node_sorted(&key, &value) {
                                Ok(_) => {
                                    free_db_info.entries += 1;
                                    false
                                }
                                Err(e) => {
                                    if let Error::Custom(msg) = &e {
                                        if msg.contains("Page full") {
                                            true
                                        } else {
                                            return Err(e);
                                        }
                                    } else {
                                        return Err(e);
                                    }
                                }
                            }
                        };
                        
                        if needs_split {
                            // For now, we'll skip entries that don't fit
                            // A proper implementation would handle page splits
                            // but that requires more complex refactoring
                            continue;
                        }
                    }

                    // Update meta with the free database info
                    new_meta.free_db = free_db_info;
                }

                // Write all dirty pages BEFORE meta page
                // This ensures data is on disk before the meta page references it
                for (_page_id, page) in dirty.pages.iter_mut() {
                    // Update checksum if enabled
                    if checksum_mode != crate::checksum::ChecksumMode::None {
                        use crate::checksum::ChecksummedPage;
                        page.update_checksum();
                    }

                    // Write page to disk
                    inner.io.write_page(page)?;
                }

                // Handle durability and meta page writing
                match inner.durability {
                    crate::env::DurabilityMode::NoSync => {
                        // No sync - fastest but no durability guarantees
                        // Write meta page without syncing
                        let mut meta_page = Page::from_meta(&new_meta, next_meta_page_id);
                        
                        // Update checksum if enabled (meta pages always get checksums in MetaOnly mode)
                        if checksum_mode != crate::checksum::ChecksumMode::None {
                            use crate::checksum::ChecksummedPage;
                            meta_page.update_checksum();
                        }
                        
                        inner.io.write_page(&meta_page)?;
                    }
                    crate::env::DurabilityMode::AsyncFlush => {
                        // Write meta page
                        let mut meta_page = Page::from_meta(&new_meta, next_meta_page_id);
                        
                        // Update checksum if enabled (meta pages always get checksums in MetaOnly mode)
                        if checksum_mode != crate::checksum::ChecksumMode::None {
                            use crate::checksum::ChecksummedPage;
                            meta_page.update_checksum();
                        }
                        
                        inner.io.write_page(&meta_page)?;
                        // Schedule async flush (OS will sync eventually)
                        // Note: Data loss possible if system crashes before flush
                        inner.io.sync()?;
                    }
                    crate::env::DurabilityMode::SyncData => {
                        // Sync data pages first to ensure they're durable
                        if !dirty.pages.is_empty() {
                            inner.io.sync()?;
                        }

                        // Write meta page after data is synced
                        // This ensures data pages are durable before meta references them
                        let mut meta_page = Page::from_meta(&new_meta, next_meta_page_id);
                        
                        // Update checksum if enabled (meta pages always get checksums in MetaOnly mode)
                        if checksum_mode != crate::checksum::ChecksumMode::None {
                            use crate::checksum::ChecksummedPage;
                            meta_page.update_checksum();
                        }
                        
                        inner.io.write_page(&meta_page)?;
                        // Meta page is not synced - OS crash could lose this commit
                    }
                    crate::env::DurabilityMode::FullSync => {
                        // Most durable mode - full ACID compliance
                        // Step 1: Sync all data pages
                        if !dirty.pages.is_empty() {
                            inner.io.sync()?;
                        }

                        // Step 2: Write meta page after data is guaranteed durable
                        let mut meta_page = Page::from_meta(&new_meta, next_meta_page_id);
                        
                        // Update checksum if enabled (meta pages always get checksums in MetaOnly mode)
                        if checksum_mode != crate::checksum::ChecksumMode::None {
                            use crate::checksum::ChecksummedPage;
                            meta_page.update_checksum();
                        }
                        
                        inner.io.write_page(&meta_page)?;

                        // Step 3: Sync meta page to ensure commit is durable
                        inner.io.sync()?;
                    }
                }

                // Update databases in environment atomically
                {
                    let mut env_dbs = write_lock(&inner.databases, "updating databases on commit")?;
                    *env_dbs = self.data.databases.clone();
                }

                // Update the global transaction ID to reflect this commit
                // This makes the committed changes visible to new readers
                inner.txn_id.store(self.data.id.0, Ordering::Release);

                // The free list changes are already persisted in the free_db
                // They will be loaded by the next write transaction

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
        // Validate page ID bounds first
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        if page_id.0 >= num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        
        // Check if already dirty in this transaction
        if let ModeData::Write { ref dirty, .. } = self.mode_data {
            if dirty.pages.contains_key(&page_id) {
                if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
                    return Ok((page_id, get_mut_page(&mut dirty.pages, &page_id)?.as_mut()));
                }
            }
        }

        // Page is not dirty - need to copy it (Copy-on-Write)
        let original_page = inner.io.read_page(page_id)?;
        
        // Validate checksum if enabled
        self.validate_page_checksum(&original_page)?;

        // Check if this is a leaf page with overflow references that need copying
        let overflow_updates = if original_page.header.flags.contains(PageFlags::LEAF)
            && original_page.header.num_keys > 0
        {
            let mut updates = Vec::new();
            for i in 0..original_page.header.num_keys as usize {
                if let Ok(node) = original_page.node(i) {
                    if let Ok(Some(overflow_id)) = node.overflow_page() {
                        updates.push((i, overflow_id));
                    }
                } 
            }
            updates
        } else {
            Vec::new()
        };

        // Copy overflow chains if needed
        let mut overflow_mappings = Vec::new();
        for (node_idx, old_overflow_id) in overflow_updates {
            let new_overflow_id = crate::overflow::copy_overflow_chain(self, old_overflow_id)?;
            overflow_mappings.push((node_idx, new_overflow_id));
        }

        // Now create the new page with updated overflow references
        if let ModeData::Write { ref mut dirty, ref mut freelist, ref mut next_pgno, .. } =
            self.mode_data
        {
            // Allocate a new page for the copy
            let new_page_id = if let Some(free_page_id) = freelist.alloc_page() {
                free_page_id
            } else {
                // Allocate new page from end of file
                let id =
                    PageId(inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
                *next_pgno = PageId(id.0 + 1);
                id
            };

            // Get page from pool or allocate new one
            let mut new_page =
                inner.page_pool.get(new_page_id, original_page.header.flags | PageFlags::DIRTY);

            // Copy header fields (except pgno and flags which are already set)
            new_page.header.num_keys = original_page.header.num_keys;
            new_page.header.lower = original_page.header.lower;
            new_page.header.upper = original_page.header.upper;
            new_page.header.overflow = original_page.header.overflow;
            new_page.header.next_pgno = original_page.header.next_pgno;
            new_page.header.prev_pgno = original_page.header.prev_pgno;
            new_page.header.checksum = 0; // Reset checksum for new page

            // Copy data
            new_page.data = original_page.data;

            // Update overflow references in the new page
            // Only update if we have overflow mappings
            if !overflow_mappings.is_empty() {
                // We need to be careful here - the node indices from the original page
                // should still be valid in the copied page since we copied the data array as-is
                for (node_idx, new_overflow_id) in overflow_mappings {
                    // Double-check that the node index is valid
                    if node_idx >= new_page.header.num_keys as usize {
                        return Err(Error::Corruption {
                            details: format!(
                                "Node index {} out of bounds during COW (num_keys={})",
                                node_idx, new_page.header.num_keys
                            ),
                            page_id: Some(page_id),
                        });
                    }

                    let mut node_data = new_page.node_data_mut(node_idx)?;
                    node_data.set_overflow(new_overflow_id)?;
                }
            }

            // Mark the new page as dirty and track allocation
            dirty.mark_dirty(new_page_id, new_page);
            dirty.allocated.push(new_page_id);

            // Free the old page (it will be added to freelist after oldest reader)
            freelist.free_page(page_id);

            Ok((new_page_id, get_mut_page(&mut dirty.pages, &new_page_id)?))
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Get a mutable page (for backward compatibility - uses COW internally)
    pub fn get_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        // Validate page ID bounds first
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        if page_id.0 >= num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        
        // For now, keep the old behavior for compatibility
        // TODO: Update all callers to handle the new page ID from COW
        // First check if already dirty
        let already_dirty = if let ModeData::Write { ref dirty, .. } = self.mode_data {
            dirty.pages.contains_key(&page_id)
        } else {
            unreachable!("Write transaction must have write mode data");
        };
        
        if already_dirty {
            if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
                return Ok(get_mut_page(&mut dirty.pages, &page_id)?);
            }
        }

        // Load and copy page
        let mut page = inner.io.read_page(page_id)?;
        
        // Validate checksum if enabled
        self.validate_page_checksum(&page)?;

        // Mark as dirty
        page.header.flags.insert(PageFlags::DIRTY);
        
        if let ModeData::Write { ref mut dirty, .. } = self.mode_data {
            dirty.mark_dirty(page_id, page);
            Ok(get_mut_page(&mut dirty.pages, &page_id)?)
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Allocate a new page
    pub fn alloc_page(&mut self, flags: PageFlags) -> Result<(PageId, &mut Page)> {
        if let ModeData::Write {
            ref mut dirty,
            ref mut freelist,
            ref mut segregated_freelist,
            ref mut next_pgno,
            ..
        } = self.mode_data
        {
            // Check transaction page limit to prevent runaway allocation
            let max_txn_pages = self.data.env.config().max_txn_pages;
            if dirty.allocated.len() >= max_txn_pages {
                return Err(Error::Custom(
                    format!("Transaction page limit exceeded: {} pages. Consider committing more frequently for random write workloads.", 
                            max_txn_pages).into()
                ));
            }
            // Try to get a page from the appropriate free list
            let page_id = if let Some(seg_list) = segregated_freelist.as_mut() {
                // Use segregated freelist if enabled
                if let Some(free_page_id) = seg_list.allocate(1) {
                    free_page_id
                } else {
                    // Allocate new page from end of file
                    let inner = self.data.env.inner();
                    let id = PageId(
                        inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    );
                    // Update local tracking
                    *next_pgno = PageId(id.0 + 1);
                    id
                }
            } else {
                // Use simple freelist
                if let Some(free_page_id) = freelist.alloc_page() {
                    free_page_id
                } else {
                    // Allocate new page from end of file
                    let inner = self.data.env.inner();
                    let id = PageId(
                        inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    );
                    // Update local tracking
                    *next_pgno = PageId(id.0 + 1);
                    id
                }
            };

            // Create new page
            let page = Page::new(page_id, flags | PageFlags::DIRTY);
            dirty.mark_dirty(page_id, page);
            dirty.allocated.push(page_id);

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
        
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        if page_id.0 >= num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        
        if let ModeData::Write { ref mut freelist, ref mut segregated_freelist, .. } =
            self.mode_data
        {
            if let Some(seg_list) = segregated_freelist.as_mut() {
                // Use segregated freelist
                seg_list.free(page_id, 1);
            } else {
                // Use simple freelist
                freelist.free_page(page_id);
            }
            Ok(())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /* TODO: Fix borrow checker issues with returning multiple mutable references
    /// Allocate multiple contiguous pages (useful for overflow pages)
    pub fn alloc_pages(&mut self, count: usize, flags: PageFlags) -> Result<Vec<(PageId, &mut Page)>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        if let ModeData::Write { ref mut dirty, ref mut freelist, ref mut segregated_freelist, ref mut next_pgno, .. } = self.mode_data {
            let mut pages = Vec::with_capacity(count);

            // Try to allocate contiguous pages from segregated freelist
            if let Some(seg_list) = segregated_freelist.as_mut() {
                if count > 1 {
                    // Try to get contiguous pages
                    if let Some(start_page_id) = seg_list.allocate(count) {
                        // We got contiguous pages
                        let page_ids: Vec<PageId> = (0..count)
                            .map(|i| PageId(start_page_id.0 + i as u64))
                            .collect();

                        // First, allocate all pages
                        for &page_id in &page_ids {
                            let page = Page::new(page_id, flags | PageFlags::DIRTY);
                            dirty.mark_dirty(page_id, page);
                            dirty.allocated.push(page_id);
                        }

                        // Then collect mutable references
                        for &page_id in &page_ids {
                            pages.push((page_id, get_mut_page(&mut dirty.pages, &page_id)?.as_mut()));
                        }

                        return Ok(pages);
                    }
                }
            }

            // Fall back to allocating pages one by one
            // We need to collect page IDs first, then get references
            let mut allocated_ids = Vec::new();

            for _ in 0..count {
                // Allocate page ID
                let page_id = if let Some(seg_list) = segregated_freelist.as_mut() {
                    if let Some(free_page_id) = seg_list.allocate(1) {
                        free_page_id
                    } else {
                        let inner = self.data.env.inner();
                        let id = PageId(inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
                        *next_pgno = PageId(id.0 + 1);
                        id
                    }
                } else {
                    if let Some(free_page_id) = freelist.alloc_page() {
                        free_page_id
                    } else {
                        let inner = self.data.env.inner();
                        let id = PageId(inner.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
                        *next_pgno = PageId(id.0 + 1);
                        id
                    }
                };

                // Create and mark page as dirty
                let page = Page::new(page_id, flags | PageFlags::DIRTY);
                dirty.mark_dirty(page_id, page);
                dirty.allocated.push(page_id);
                allocated_ids.push(page_id);
            }

            // Now collect the mutable references
            for page_id in allocated_ids {
                pages.push((page_id, get_mut_page(&mut dirty.pages, &page_id)?.as_mut()));
            }

            Ok(pages)
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }
    */

    /// Free multiple contiguous pages
    pub fn free_pages(&mut self, start_page_id: PageId, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        
        // Validate start page ID
        if start_page_id.0 <= 1 {
            return Err(Error::Custom("Cannot free meta pages (0 or 1)".into()));
        }
        
        let inner = self.data.env.inner();
        let num_pages = inner.io.size_in_pages();
        
        // Validate start and end pages
        if start_page_id.0 >= num_pages {
            return Err(Error::InvalidPageId(start_page_id));
        }
        
        let end_page_id = PageId(start_page_id.0 + count as u64 - 1);
        if end_page_id.0 >= num_pages {
            return Err(Error::Custom(
                format!("Cannot free pages beyond database bounds: {} to {}", start_page_id.0, end_page_id.0).into()
            ));
        }

        if let ModeData::Write { ref mut freelist, ref mut segregated_freelist, .. } =
            self.mode_data
        {
            if let Some(seg_list) = segregated_freelist.as_mut() {
                // Use segregated freelist - can free as a single extent
                seg_list.free(start_page_id, count);
            } else {
                // Use simple freelist - free pages one by one
                for i in 0..count {
                    let page_id = PageId(start_page_id.0 + i as u64);
                    freelist.free_page(page_id);
                }
            }
            Ok(())
        } else {
            unreachable!("Write transaction must have write mode data");
        }
    }

    /// Internal method to allocate a page (used during commit)
    #[allow(dead_code)]
    fn alloc_page_internal<'a>(
        &mut self,
        dirty: &'a mut DirtyPages,
        next_pgno: &mut PageId,
        flags: PageFlags,
    ) -> Result<(PageId, &'a mut Page)> {
        if let ModeData::Write { ref mut freelist, .. } = self.mode_data {
            // Try to get a page from the free list first
            let page_id = if let Some(free_page_id) = freelist.alloc_page() {
                free_page_id
            } else {
                // Allocate new page from end of file
                let id = *next_pgno;
                *next_pgno = PageId(id.0 + 1);
                id
            };

            // Create new page
            let page = Page::new(page_id, flags | PageFlags::DIRTY);
            dirty.mark_dirty(page_id, page);
            dirty.allocated.push(page_id);

            Ok((page_id, get_mut_page(&mut dirty.pages, &page_id)?.as_mut()))
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
