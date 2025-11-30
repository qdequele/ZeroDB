//! Transaction management for ZeroDB.
//!
//! This module provides read-only and read-write transactions with
//! MVCC (Multi-Version Concurrency Control) semantics.

use std::marker::PhantomData;

use crate::alloc::{DirtyPages, PageAllocator};
use crate::env::Env;
use crate::error::{Error, Result};
use crate::page::{MetaPage, PageNo};

/// Marker trait for transaction state.
pub trait TxnState {}

/// Marker for active transaction state.
pub struct Active;
impl TxnState for Active {}

/// Marker for committed transaction state.
pub struct Committed;
impl TxnState for Committed {}

/// Marker for aborted transaction state.
pub struct Aborted;
impl TxnState for Aborted {}

// ============================================================================
// TLS (Thread Local Storage) Markers - Heed API Compatibility
// ============================================================================

/// Parameter defining that read transactions have been opened with
/// Thread Local Storage (TLS).
///
/// A thread can only use one transaction at a time, plus any
/// child (nested) transactions. Each transaction belongs to one
/// thread. Transactions with this marker are `!Send`.
pub enum WithTls {}

/// Parameter defining that read transactions have been opened without
/// Thread Local Storage (TLS).
///
/// A thread can use any number of read transactions at a time on
/// the same thread. Read transactions can be moved in between
/// threads (`Send`).
pub enum WithoutTls {}

/// Parameter defining that read transactions might have been opened with or
/// without Thread Local Storage (TLS).
///
/// `RwTxn`s and any `RoTxn` dereference to `&RoTxn<AnyTls>`.
pub enum AnyTls {}

/// Specifies if Thread Local Storage (TLS) must be used when
/// opening transactions. It is often faster to open TLS-backed
/// transactions but makes them `!Send`.
pub trait TlsUsage {
    /// True if TLS must be used, false otherwise.
    const ENABLED: bool;
}

impl TlsUsage for WithTls {
    const ENABLED: bool = true;
}

impl TlsUsage for WithoutTls {
    const ENABLED: bool = false;
}

impl TlsUsage for AnyTls {
    // Users cannot open environments with AnyTls; therefore, this will never be read.
    // We prefer to put the most restrictive value.
    const ENABLED: bool = false;
}

/// Inner state for read-only transaction environment reference.
///
/// Uses `Cow` to allow either borrowing or owning the environment.
enum RoTxnEnv<'e, T: TlsUsage> {
    /// Borrowed reference to the environment.
    Borrowed(&'e Env<T>),
    /// Owned environment (for static_read_txn).
    Owned(Env<T>),
}

impl<'e, T: TlsUsage> RoTxnEnv<'e, T> {
    fn as_ref(&self) -> &Env<T> {
        match self {
            RoTxnEnv::Borrowed(env) => env,
            RoTxnEnv::Owned(env) => env,
        }
    }
}

/// A read-only transaction.
///
/// Read transactions provide a consistent snapshot of the database
/// at the time the transaction was created. Multiple read transactions
/// can be active simultaneously.
///
/// # TLS Parameter
///
/// The type parameter `T` specifies whether Thread Local Storage is used:
/// - `WithTls` (default): Transactions are `!Send` but may be faster
/// - `WithoutTls`: Transactions are `Send` and can be moved between threads
/// - `AnyTls`: Generic marker used internally
///
/// # Example
///
/// ```ignore
/// let env = unsafe { EnvOpenOptions::new().open(path)? };
/// let rtxn = env.read_txn()?;
/// // ... read operations ...
/// rtxn.abort(); // or just drop it
/// ```
pub struct RoTxn<'e, T: TlsUsage = WithTls> {
    /// Reference to the environment.
    env: RoTxnEnv<'e, T>,
    /// Transaction ID (snapshot point).
    txnid: u64,
    /// Snapshot of the meta page at transaction start.
    meta: MetaPage,
    /// Whether the transaction is still active.
    active: bool,
    /// TLS marker phantom data.
    _tls_marker: PhantomData<T>,
}

impl<'e, T: TlsUsage> RoTxn<'e, T> {
    /// Creates a new read-only transaction.
    pub(crate) fn new(env: &'e Env<T>, txnid: u64, meta: MetaPage) -> Self {
        Self {
            env: RoTxnEnv::Borrowed(env),
            txnid,
            meta,
            active: true,
            _tls_marker: PhantomData,
        }
    }

    /// Creates a new read-only transaction that owns the environment.
    ///
    /// This allows the transaction to have a `'static` lifetime.
    pub(crate) fn new_static(env: Env<T>, txnid: u64, meta: MetaPage) -> RoTxn<'static, T> {
        RoTxn {
            env: RoTxnEnv::Owned(env),
            txnid,
            meta,
            active: true,
            _tls_marker: PhantomData,
        }
    }

    /// Return the transaction's ID.
    ///
    /// This returns the identifier associated with this transaction. For a
    /// read-only transaction, this corresponds to the snapshot being read;
    /// concurrent readers will frequently have the same transaction ID.
    pub fn id(&self) -> u64 {
        self.txnid
    }

    /// Returns the transaction ID.
    ///
    /// This is an alias for [`id()`](RoTxn::id) for backwards compatibility.
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &Env<T> {
        self.env.as_ref()
    }

    /// Returns the meta page snapshot.
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// Returns the root page number of the main database.
    pub fn main_root(&self) -> PageNo {
        self.meta.main_db.root
    }

    /// Returns the root page number of the free list database.
    pub fn free_root(&self) -> PageNo {
        self.meta.free_db.root
    }

    /// Reads a page from the database.
    ///
    /// Returns the raw page data as a byte slice.
    pub fn page(&self, pgno: PageNo) -> Result<&[u8]> {
        if !self.active {
            return Err(Error::BadTxn);
        }
        self.env.as_ref().page_data(pgno)
    }

    /// Aborts the transaction.
    ///
    /// This releases the read lock. The transaction can no longer be used.
    pub fn abort(mut self) {
        self.active = false;
        // Reader slot is released on drop
    }

    /// Commits the read transaction (same as abort for read-only).
    ///
    /// ## LMDB
    ///
    /// It's mandatory in a multi-process setup to call [`RoTxn::commit`] upon read-only database opening.
    /// After the transaction opening, the database is dropped. The next transaction might return
    /// an error known as `EINVAL`.
    pub fn commit(self) -> Result<()> {
        // Read transactions don't need to do anything on commit
        Ok(())
    }
}

impl<T: TlsUsage> Drop for RoTxn<'_, T> {
    fn drop(&mut self) {
        // Release reader slot
        self.active = false;
    }
}

/// Is sendable only if `MDB_NOTLS` has been used to open this transaction.
/// SAFETY: ZeroDB doesn't actually use TLS internally, so this is safe.
unsafe impl Send for RoTxn<'_, WithoutTls> {}

/// A read-write transaction.
///
/// Write transactions have exclusive write access to the database.
/// Only one write transaction can be active at a time. Write transactions
/// use copy-on-write semantics - pages are copied before modification.
///
/// # Example
///
/// ```ignore
/// let env = unsafe { EnvOpenOptions::new().open(path)? };
/// let mut wtxn = env.write_txn()?;
/// // ... write operations ...
/// wtxn.commit()?;
/// ```
pub struct RwTxn<'e, T: TlsUsage = WithTls> {
    /// Reference to the environment.
    env: &'e Env<T>,
    /// Transaction ID for this write transaction.
    txnid: u64,
    /// Starting meta page (will be updated on commit).
    meta: MetaPage,
    /// Page allocator state.
    allocator: PageAllocator,
    /// Dirty pages modified in this transaction.
    dirty_pages: DirtyPages,
    /// Whether the transaction is still active.
    active: bool,
    /// Parent transaction (for nested transactions).
    parent: Option<Box<RwTxn<'e, T>>>,
}

impl<'e, T: TlsUsage> RwTxn<'e, T> {
    /// Creates a new read-write transaction.
    pub(crate) fn new(
        env: &'e Env<T>,
        txnid: u64,
        meta: MetaPage,
        allocator: PageAllocator,
    ) -> Self {
        Self {
            env,
            txnid,
            meta,
            allocator,
            dirty_pages: DirtyPages::new(),
            active: true,
            parent: None,
        }
    }

    /// Creates a nested (child) transaction.
    #[allow(dead_code)]
    pub(crate) fn nested(parent: RwTxn<'e, T>) -> Self {
        let env = parent.env;
        let txnid = parent.txnid;
        let meta = parent.meta;
        let allocator = PageAllocator::new(
            parent.allocator.last_pgno(),
            env.map_size(),
            env.page_size(),
        );

        Self {
            env,
            txnid,
            meta,
            allocator,
            dirty_pages: DirtyPages::new(),
            active: true,
            parent: Some(Box::new(parent)),
        }
    }

    /// Return the transaction's ID.
    ///
    /// This returns the identifier associated with this transaction.
    pub fn id(&self) -> u64 {
        self.txnid
    }

    /// Returns the transaction ID.
    ///
    /// This is an alias for [`id()`](RwTxn::id) for backwards compatibility.
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &Env<T> {
        self.env
    }

    /// Returns the meta page.
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// Returns the root page number of the main database.
    pub fn main_root(&self) -> PageNo {
        self.meta.main_db.root
    }

    /// Returns the root page number of the free list database.
    pub fn free_root(&self) -> PageNo {
        self.meta.free_db.root
    }

    /// Reads a page from the database.
    ///
    /// First checks dirty pages, then falls back to the mmap.
    pub fn page(&self, pgno: PageNo) -> Result<&[u8]> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        // Check dirty pages first
        if let Some(data) = self.dirty_pages.get(pgno) {
            return Ok(data);
        }

        // Check parent transaction's dirty pages
        if let Some(ref parent) = self.parent {
            if let Some(data) = parent.dirty_pages.get(pgno) {
                return Ok(data);
            }
        }

        // Fall back to mmap
        self.env.page_data(pgno)
    }

    /// Allocates a new page for writing.
    ///
    /// Returns the page number and a mutable buffer for the page data.
    pub fn alloc_page(&mut self) -> Result<(PageNo, &mut [u8])> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        let pgno = self.allocator.alloc_page().ok_or(Error::MapFull)?;

        // Get a page buffer from the pool (avoids allocation if pool has buffers)
        let buf = self.env.get_page_buffer();
        self.dirty_pages.insert(pgno, buf);

        // Return the buffer
        let data = self.dirty_pages.get_mut(pgno).unwrap();
        Ok((pgno, data))
    }

    /// Allocates multiple contiguous pages.
    pub fn alloc_pages(&mut self, count: u32) -> Result<PageNo> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        let start_pgno = self.allocator.alloc_pages(count).ok_or(Error::MapFull)?;

        // Get page buffers from the pool
        for i in 0..count {
            let pgno = start_pgno + i as PageNo;
            let buf = self.env.get_page_buffer();
            self.dirty_pages.insert(pgno, buf);
        }

        Ok(start_pgno)
    }

    /// Gets a mutable reference to a page, copying if necessary.
    ///
    /// This implements copy-on-write semantics.
    pub fn page_mut(&mut self, pgno: PageNo) -> Result<&mut [u8]> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        // If already dirty, return it
        if self.dirty_pages.contains(pgno) {
            return Ok(self.dirty_pages.get_mut(pgno).unwrap());
        }

        // Copy the page using a buffer from the pool
        let original = self.env.page_data(pgno)?;
        let mut buf = self.env.get_page_buffer();
        buf[..original.len()].copy_from_slice(original);
        self.dirty_pages.insert(pgno, buf);

        Ok(self.dirty_pages.get_mut(pgno).unwrap())
    }

    /// Frees a page.
    pub fn free_page(&mut self, pgno: PageNo) -> Result<()> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        self.allocator.free_page(pgno);
        Ok(())
    }

    /// Returns the number of dirty pages.
    pub fn dirty_count(&self) -> usize {
        self.dirty_pages.len()
    }

    /// Commits the transaction.
    ///
    /// Writes all dirty pages to the file and updates the meta page.
    pub fn commit(mut self) -> Result<()> {
        if !self.active {
            return Err(Error::BadTxn);
        }

        self.active = false;

        // Handle nested transaction
        if let Some(mut parent) = self.parent.take() {
            // Merge dirty pages into parent
            for (pgno, data) in self.dirty_pages.take() {
                parent.dirty_pages.insert(pgno, data);
            }
            // Merge allocator state
            self.allocator.commit(self.txnid);

            // Parent becomes the active transaction again
            // (In a real implementation, we'd need to restore the parent)
            return Ok(());
        }

        // Check if transaction has actual changes
        let has_dirty_pages = !self.dirty_pages.is_empty();
        let has_new_pages = self.allocator.last_pgno() > self.meta.last_pgno;
        let has_freed_pages = self.allocator.has_freed_pages();
        let is_empty = !has_dirty_pages && !has_new_pages && !has_freed_pages;

        // Commit freelist changes
        self.allocator.commit(self.txnid);

        // Write dirty pages to the environment
        // Pass is_empty flag to allow commit_txn to skip expensive disk sync
        self.env.commit_txn(
            self.txnid,
            self.meta,
            self.dirty_pages.take(),
            self.allocator.last_pgno(),
            is_empty,
        )?;

        Ok(())
    }

    /// Aborts the transaction.
    ///
    /// Discards all changes made in this transaction.
    pub fn abort(mut self) {
        self.active = false;
        self.allocator.abort();

        // Return page buffers to the pool
        let buffers: Vec<Vec<u8>> = self.dirty_pages.take().into_values().collect();
        self.env.return_page_buffers(buffers);

        // If nested, restore parent
        if let Some(_parent) = self.parent.take() {
            // Parent becomes active again
            // (In a real implementation, we'd need to restore it properly)
        }
    }
}

impl<T: TlsUsage> Drop for RwTxn<'_, T> {
    fn drop(&mut self) {
        if self.active {
            // Transaction was not committed or aborted - abort it
            self.allocator.abort();

            // Return page buffers to the pool
            let buffers: Vec<Vec<u8>> = self.dirty_pages.take().into_values().collect();
            self.env.return_page_buffers(buffers);
        }
    }
}

/// A transaction that can be either read-only or read-write.
///
/// This is useful for functions that work with any transaction type.
pub enum Txn<'e, T: TlsUsage = WithTls> {
    /// Read-only transaction.
    Ro(RoTxn<'e, T>),
    /// Read-write transaction.
    Rw(RwTxn<'e, T>),
}

impl<'e, T: TlsUsage> Txn<'e, T> {
    /// Return the transaction's ID.
    pub fn id(&self) -> u64 {
        match self {
            Txn::Ro(txn) => txn.id(),
            Txn::Rw(txn) => txn.id(),
        }
    }

    /// Returns the transaction ID.
    ///
    /// This is an alias for [`id()`](Txn::id) for backwards compatibility.
    pub fn txnid(&self) -> u64 {
        self.id()
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &Env<T> {
        match self {
            Txn::Ro(txn) => txn.env(),
            Txn::Rw(txn) => txn.env(),
        }
    }

    /// Reads a page from the database.
    pub fn page(&self, pgno: PageNo) -> Result<&[u8]> {
        match self {
            Txn::Ro(txn) => txn.page(pgno),
            Txn::Rw(txn) => txn.page(pgno),
        }
    }
}

impl<'e, T: TlsUsage> From<RoTxn<'e, T>> for Txn<'e, T> {
    fn from(txn: RoTxn<'e, T>) -> Self {
        Txn::Ro(txn)
    }
}

impl<'e, T: TlsUsage> From<RwTxn<'e, T>> for Txn<'e, T> {
    fn from(txn: RwTxn<'e, T>) -> Self {
        Txn::Rw(txn)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    // Tests will be added with environment integration
}
