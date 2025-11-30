//! Transaction management for ZeroDB.
//!
//! This module provides read-only and read-write transactions with
//! MVCC (Multi-Version Concurrency Control) semantics.


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

/// A read-only transaction.
///
/// Read transactions provide a consistent snapshot of the database
/// at the time the transaction was created. Multiple read transactions
/// can be active simultaneously.
///
/// # Example
///
/// ```ignore
/// let env = unsafe { EnvOpenOptions::new().open(path)? };
/// let rtxn = env.read_txn()?;
/// // ... read operations ...
/// rtxn.abort(); // or just drop it
/// ```
pub struct RoTxn<'e> {
    /// Reference to the environment.
    env: &'e Env,
    /// Transaction ID (snapshot point).
    txnid: u64,
    /// Snapshot of the meta page at transaction start.
    meta: MetaPage,
    /// Whether the transaction is still active.
    active: bool,
}

impl<'e> RoTxn<'e> {
    /// Creates a new read-only transaction.
    pub(crate) fn new(env: &'e Env, txnid: u64, meta: MetaPage) -> Self {
        Self {
            env,
            txnid,
            meta,
            active: true,
        }
    }

    /// Returns the transaction ID.
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &'e Env {
        self.env
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
        self.env.page_data(pgno)
    }

    /// Aborts the transaction.
    ///
    /// This releases the read lock. The transaction can no longer be used.
    pub fn abort(mut self) {
        self.active = false;
        // Reader slot is released on drop
    }

    /// Commits the read transaction (same as abort for read-only).
    pub fn commit(self) -> Result<()> {
        // Read transactions don't need to do anything on commit
        Ok(())
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        // Release reader slot
        self.active = false;
    }
}

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
pub struct RwTxn<'e> {
    /// Reference to the environment.
    env: &'e Env,
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
    parent: Option<Box<RwTxn<'e>>>,
}

impl<'e> RwTxn<'e> {
    /// Creates a new read-write transaction.
    pub(crate) fn new(
        env: &'e Env,
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
    pub(crate) fn nested(parent: RwTxn<'e>) -> Self {
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

    /// Returns the transaction ID.
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &'e Env {
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

        // Commit freelist changes
        self.allocator.commit(self.txnid);

        // Write dirty pages to the environment
        self.env.commit_txn(
            self.txnid,
            self.meta,
            self.dirty_pages.take(),
            self.allocator.last_pgno(),
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

impl Drop for RwTxn<'_> {
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
pub enum Txn<'e> {
    /// Read-only transaction.
    Ro(RoTxn<'e>),
    /// Read-write transaction.
    Rw(RwTxn<'e>),
}

impl<'e> Txn<'e> {
    /// Returns the transaction ID.
    pub fn txnid(&self) -> u64 {
        match self {
            Txn::Ro(txn) => txn.txnid(),
            Txn::Rw(txn) => txn.txnid(),
        }
    }

    /// Returns a reference to the environment.
    pub fn env(&self) -> &'e Env {
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

impl<'e> From<RoTxn<'e>> for Txn<'e> {
    fn from(txn: RoTxn<'e>) -> Self {
        Txn::Ro(txn)
    }
}

impl<'e> From<RwTxn<'e>> for Txn<'e> {
    fn from(txn: RwTxn<'e>) -> Self {
        Txn::Rw(txn)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    // Tests will be added with environment integration
}
