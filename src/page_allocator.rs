//! Core page allocation subsystem
//!
//! This module provides efficient page allocation and management,
//! following a proven design approach similar to LMDB but optimized for Rust.

use crate::error::{Error, PageId, Result};
use crate::freelist::FreeList;
use crate::page::{Page, PageFlags};

use std::sync::atomic::{AtomicU64, Ordering};

/// Core page allocation engine
pub struct PageAllocator {
    /// Next page number to allocate
    next_pgno: AtomicU64,
    /// Maximum allowed page number (map_size / page_size)
    max_pgno: u64,
    /// Primary freelist for recycling pages
    freelist: FreeList,
}

impl PageAllocator {
    /// Create a new page allocator
    pub fn new(max_pgno: u64, initial_pgno: u64) -> Self {
        Self {
            next_pgno: AtomicU64::new(initial_pgno),
            max_pgno,
            freelist: FreeList::new(),
        }
    }

    /// Allocate a single page
    pub fn alloc_page(&mut self) -> Result<PageId> {
        self.alloc_pages(1)
    }

    /// Allocate multiple consecutive pages
    pub fn alloc_pages(&mut self, count: usize) -> Result<PageId> {
        if count == 0 {
            return Err(Error::InvalidParameter("Cannot allocate 0 pages"));
        }

        // Try freelist first for single pages
        if count == 1 {
            if let Some(page_id) = self.freelist.alloc_page() {
                return Ok(page_id);
            }
        }

        // Allocate from end of file
        let start_pgno = self.next_pgno.fetch_add(count as u64, Ordering::SeqCst);
        
        // Check map size limit only
        if start_pgno + count as u64 >= self.max_pgno {
            // Restore the counter
            self.next_pgno.fetch_sub(count as u64, Ordering::SeqCst);
            return Err(Error::MapFull);
        }

        Ok(PageId(start_pgno))
    }

    /// Free a single page
    pub fn free_page(&mut self, page_id: PageId) {
        self.free_pages(page_id, 1);
    }

    /// Free multiple consecutive pages
    pub fn free_pages(&mut self, start_page_id: PageId, count: usize) {
        if count == 0 {
            return;
        }

        // Don't free meta pages
        if start_page_id.0 <= 1 {
            return;
        }

        // Free pages one by one to simple freelist
        for i in 0..count {
            let page_id = PageId(start_page_id.0 + i as u64);
            self.freelist.free_page(page_id);
        }
    }

    /// Get current allocation state
    pub fn current_pgno(&self) -> u64 {
        self.next_pgno.load(Ordering::Acquire)
    }

    /// Get maximum allowed page number
    pub fn max_pgno(&self) -> u64 {
        self.max_pgno
    }

    /// Check if page allocation would exceed limits
    pub fn would_exceed_limit(&self, pages_needed: usize) -> bool {
        let current = self.next_pgno.load(Ordering::Acquire);
        current + pages_needed as u64 >= self.max_pgno
    }

    /// Update the page allocator for a committed transaction
    pub fn commit(&mut self, txn_id: crate::error::TransactionId) {
        // Update freelist state after commit
        self.freelist.commit_pending(txn_id);
    }

    /// Set the oldest reader for freelist management
    pub fn set_oldest_reader(&mut self, txn_id: crate::error::TransactionId) {
        self.freelist.set_oldest_reader(txn_id);
    }
}

/// Transaction-local page allocation tracking
pub struct TxnPageAlloc {
    /// Pages allocated in this transaction
    allocated: Vec<PageId>,
    /// Pages freed in this transaction
    freed: Vec<PageId>,
    /// Dirty room remaining (limits transaction size)
    dirty_room: usize,
}

impl TxnPageAlloc {
    /// Create new transaction allocation tracker
    pub fn new(initial_dirty_room: usize) -> Self {
        Self {
            allocated: Vec::new(),
            freed: Vec::new(),
            dirty_room: initial_dirty_room,
        }
    }

    /// Track a page allocation
    pub fn track_alloc(&mut self, page_id: PageId) -> Result<()> {
        if self.dirty_room == 0 {
            return Err(Error::TxnFull { size: self.allocated.len() });
        }
        
        self.allocated.push(page_id);
        self.dirty_room -= 1;
        Ok(())
    }

    /// Track a page free
    pub fn track_free(&mut self, page_id: PageId) {
        self.freed.push(page_id);
    }

    /// Get allocated pages
    pub fn allocated_pages(&self) -> &[PageId] {
        &self.allocated
    }

    /// Get freed pages
    pub fn freed_pages(&self) -> &[PageId] {
        &self.freed
    }

    /// Get remaining dirty room
    pub fn dirty_room(&self) -> usize {
        self.dirty_room
    }

    /// Try to allocate a page from freelist (placeholder for now)
    pub fn try_alloc_from_freelist(&mut self) -> Option<PageId> {
        // For now, return None to always allocate from end of file
        // This will be implemented properly when we integrate the freelist
        None
    }
}

/// Page creation utilities
pub struct PageFactory;

impl PageFactory {
    /// Create a new page with proper initialization
    pub fn create_page(page_id: PageId, flags: PageFlags) -> Box<Page> {
        Page::new(page_id, flags)
    }

    /// Create multiple consecutive pages
    pub fn create_pages(start_id: PageId, count: usize, flags: PageFlags) -> Vec<Box<Page>> {
        (0..count)
            .map(|i| {
                let page_id = PageId(start_id.0 + i as u64);
                Page::new(page_id, flags)
            })
            .collect()
    }
} 