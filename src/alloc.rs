//! Page allocation and freelist management for ZeroDB.
//!
//! This module handles allocating new pages and tracking freed pages
//! that can be reused.

use std::collections::BTreeMap;

use crate::page::PageNo;

/// Invalid/null page number.
pub const P_INVALID: PageNo = PageNo::MAX;

/// Page allocator for managing database pages.
///
/// The allocator tracks:
/// - The last allocated page number
/// - Pages freed in the current transaction (loose pages)
/// - Pages available for reuse from committed transactions (freelist)
#[derive(Debug)]
pub struct PageAllocator {
    /// Last allocated page number.
    last_pgno: PageNo,
    /// Maximum page number allowed (based on map size).
    max_pgno: PageNo,
    /// Pages freed in the current transaction.
    /// These cannot be reused until the transaction commits.
    loose_pages: Vec<PageNo>,
    /// Freelist: pages from committed transactions available for reuse.
    /// Key is the transaction ID when pages were freed.
    /// Value is the list of page numbers.
    freelist: BTreeMap<u64, Vec<PageNo>>,
    /// Minimum reader transaction ID.
    /// Pages freed by transactions newer than this cannot be reused.
    min_reader_txnid: u64,
}

impl PageAllocator {
    /// Creates a new page allocator.
    ///
    /// # Arguments
    ///
    /// * `last_pgno` - The last allocated page number
    /// * `map_size` - Total size of the memory map
    /// * `page_size` - Size of each page
    pub fn new(last_pgno: PageNo, map_size: usize, page_size: usize) -> Self {
        let max_pgno = (map_size / page_size) as PageNo - 1;

        Self {
            last_pgno,
            max_pgno,
            loose_pages: Vec::new(),
            freelist: BTreeMap::new(),
            min_reader_txnid: 0,
        }
    }

    /// Returns the last allocated page number.
    pub fn last_pgno(&self) -> PageNo {
        self.last_pgno
    }

    /// Sets the minimum reader transaction ID.
    ///
    /// Pages from transactions with ID greater than this value cannot be reused.
    pub fn set_min_reader_txnid(&mut self, txnid: u64) {
        self.min_reader_txnid = txnid;
    }

    /// Allocates a new page.
    ///
    /// First tries to reuse a page from the freelist, then extends the file.
    ///
    /// # Returns
    ///
    /// The page number of the allocated page, or None if no space available.
    pub fn alloc_page(&mut self) -> Option<PageNo> {
        // First, try to reuse a page from the freelist
        if let Some(pgno) = self.alloc_from_freelist() {
            return Some(pgno);
        }

        // Otherwise, allocate a new page at the end
        if self.last_pgno < self.max_pgno {
            self.last_pgno += 1;
            Some(self.last_pgno)
        } else {
            None // Map is full
        }
    }

    /// Allocates multiple contiguous pages.
    ///
    /// This is used for overflow pages that span multiple pages.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of contiguous pages needed
    ///
    /// # Returns
    ///
    /// The starting page number, or None if no space available.
    pub fn alloc_pages(&mut self, count: u32) -> Option<PageNo> {
        if count == 0 {
            return None;
        }

        if count == 1 {
            return self.alloc_page();
        }

        // For multiple pages, we need contiguous space
        // First try from freelist (complex - skip for now)
        // Then allocate from the end
        let start = self.last_pgno + 1;
        let end = start + count as PageNo - 1;

        if end <= self.max_pgno {
            self.last_pgno = end;
            Some(start)
        } else {
            None
        }
    }

    /// Frees a page in the current transaction.
    ///
    /// The page is added to the loose pages list and will be added to the
    /// freelist when the transaction commits.
    pub fn free_page(&mut self, pgno: PageNo) {
        self.loose_pages.push(pgno);
    }

    /// Frees multiple contiguous pages.
    pub fn free_pages(&mut self, start_pgno: PageNo, count: u32) {
        for i in 0..count {
            self.loose_pages.push(start_pgno + i as PageNo);
        }
    }

    /// Commits the current transaction's freed pages to the freelist.
    ///
    /// # Arguments
    ///
    /// * `txnid` - The transaction ID of the committing transaction
    pub fn commit(&mut self, txnid: u64) {
        if !self.loose_pages.is_empty() {
            let mut pages = std::mem::take(&mut self.loose_pages);
            // Sort pages by page number for sequential allocation (better cache locality)
            pages.sort_unstable();
            self.freelist.insert(txnid, pages);
        }
    }

    /// Aborts the current transaction, discarding loose pages.
    pub fn abort(&mut self) {
        self.loose_pages.clear();
    }

    /// Returns the number of loose pages (freed in current transaction).
    pub fn loose_count(&self) -> usize {
        self.loose_pages.len()
    }

    /// Returns the total number of pages in the freelist.
    pub fn freelist_count(&self) -> usize {
        self.freelist.values().map(|v| v.len()).sum()
    }

    /// Returns an iterator over freelist entries.
    pub fn freelist_iter(&self) -> impl Iterator<Item = (&u64, &Vec<PageNo>)> {
        self.freelist.iter()
    }

    /// Loads the freelist from the database.
    ///
    /// This should be called during environment initialization.
    pub fn load_freelist(&mut self, entries: impl IntoIterator<Item = (u64, Vec<PageNo>)>) {
        for (txnid, pages) in entries {
            self.freelist.insert(txnid, pages);
        }
    }

    /// Clears the freelist.
    pub fn clear_freelist(&mut self) {
        self.freelist.clear();
    }

    /// Tries to allocate a page from the freelist.
    /// Allocates lowest page numbers first for better cache locality.
    fn alloc_from_freelist(&mut self) -> Option<PageNo> {
        // Find the lowest page number across all reusable freelist entries
        let mut best_pgno: Option<PageNo> = None;
        let mut best_txnid: Option<u64> = None;

        for (&txnid, pages) in &self.freelist {
            // Only consider pages from transactions older than min_reader_txnid
            if txnid <= self.min_reader_txnid {
                if let Some(&first_pgno) = pages.first() {
                    if best_pgno.is_none() || first_pgno < best_pgno.unwrap() {
                        best_pgno = Some(first_pgno);
                        best_txnid = Some(txnid);
                    }
                }
            }
        }

        // Remove and return the best page
        if let (Some(pgno), Some(txnid)) = (best_pgno, best_txnid) {
            if let Some(pages) = self.freelist.get_mut(&txnid) {
                pages.remove(0); // Remove first element (lowest page number)
                if pages.is_empty() {
                    self.freelist.remove(&txnid);
                }
            }
            return Some(pgno);
        }

        None
    }

    /// Returns information about space usage.
    pub fn space_info(&self) -> SpaceInfo {
        let total_pages = self.max_pgno + 1;
        let used_pages = self.last_pgno + 1;
        let free_pages = self.freelist_count() as u64;

        SpaceInfo {
            total_pages,
            used_pages,
            free_pages,
            available_pages: total_pages - used_pages + free_pages,
        }
    }
}

/// Information about database space usage.
#[derive(Debug, Clone, Copy)]
pub struct SpaceInfo {
    /// Total number of pages in the database.
    pub total_pages: u64,
    /// Number of pages currently in use (allocated).
    pub used_pages: u64,
    /// Number of pages in the freelist (available for reuse).
    pub free_pages: u64,
    /// Number of pages available for allocation.
    pub available_pages: u64,
}

/// Page buffer pool to avoid repeated allocations.
/// Reuses page buffers across transactions.
#[derive(Debug)]
pub struct PagePool {
    /// Free buffers available for reuse.
    free: Vec<Vec<u8>>,
    /// Page size for this pool.
    page_size: usize,
}

impl PagePool {
    /// Creates a new page pool.
    pub fn new(page_size: usize) -> Self {
        Self {
            free: Vec::new(),
            page_size,
        }
    }

    /// Gets a page buffer, reusing one from the pool if available.
    pub fn get(&mut self) -> Vec<u8> {
        self.free.pop().unwrap_or_else(|| vec![0u8; self.page_size])
    }

    /// Returns a page buffer to the pool for reuse.
    pub fn put(&mut self, mut buf: Vec<u8>) {
        // Only keep buffers of the correct size
        if buf.len() == self.page_size {
            // Clear the buffer for reuse
            buf.fill(0);
            self.free.push(buf);
        }
    }

    /// Returns multiple buffers to the pool.
    pub fn put_all(&mut self, buffers: impl IntoIterator<Item = Vec<u8>>) {
        for buf in buffers {
            self.put(buf);
        }
    }
}

/// Dirty page tracking for write transactions.
#[derive(Debug, Default)]
pub struct DirtyPages {
    /// Map from page number to dirty page data.
    pages: BTreeMap<PageNo, Vec<u8>>,
}

impl DirtyPages {
    /// Creates a new dirty pages tracker.
    pub fn new() -> Self {
        Self {
            pages: BTreeMap::new(),
        }
    }

    /// Marks a page as dirty with the given data.
    pub fn insert(&mut self, pgno: PageNo, data: Vec<u8>) {
        self.pages.insert(pgno, data);
    }

    /// Returns the dirty data for a page, if any.
    pub fn get(&self, pgno: PageNo) -> Option<&[u8]> {
        self.pages.get(&pgno).map(|v| v.as_slice())
    }

    /// Returns a mutable reference to the dirty data for a page.
    pub fn get_mut(&mut self, pgno: PageNo) -> Option<&mut Vec<u8>> {
        self.pages.get_mut(&pgno)
    }

    /// Returns true if the page is dirty.
    pub fn contains(&self, pgno: PageNo) -> bool {
        self.pages.contains_key(&pgno)
    }

    /// Returns the number of dirty pages.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Returns true if there are no dirty pages.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Returns an iterator over dirty pages.
    pub fn iter(&self) -> impl Iterator<Item = (&PageNo, &Vec<u8>)> {
        self.pages.iter()
    }

    /// Clears all dirty pages.
    pub fn clear(&mut self) {
        self.pages.clear();
    }

    /// Takes ownership of all dirty pages, clearing the tracker.
    pub fn take(&mut self) -> BTreeMap<PageNo, Vec<u8>> {
        std::mem::take(&mut self.pages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_pages() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);

        // Allocate some pages
        assert_eq!(alloc.alloc_page(), Some(2));
        assert_eq!(alloc.alloc_page(), Some(3));
        assert_eq!(alloc.alloc_page(), Some(4));

        assert_eq!(alloc.last_pgno(), 4);
    }

    #[test]
    fn allocate_multiple_pages() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);

        // Allocate 5 contiguous pages
        assert_eq!(alloc.alloc_pages(5), Some(2));
        assert_eq!(alloc.last_pgno(), 6);

        // Allocate another single page
        assert_eq!(alloc.alloc_page(), Some(7));
    }

    #[test]
    fn free_and_reuse_pages() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);
        alloc.set_min_reader_txnid(100);

        // Allocate some pages
        assert_eq!(alloc.alloc_page(), Some(2));
        assert_eq!(alloc.alloc_page(), Some(3));

        // Free a page
        alloc.free_page(2);
        assert_eq!(alloc.loose_count(), 1);

        // Commit transaction
        alloc.commit(50); // txnid 50 <= min_reader_txnid 100

        // Page should now be reusable
        assert_eq!(alloc.freelist_count(), 1);
        assert_eq!(alloc.alloc_page(), Some(2)); // Reuses freed page
    }

    #[test]
    fn freelist_respects_readers() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);
        alloc.set_min_reader_txnid(50);

        // Allocate and free
        let pgno = alloc.alloc_page().unwrap();
        alloc.free_page(pgno);
        alloc.commit(100); // txnid 100 > min_reader_txnid 50

        // Page should NOT be reusable yet (transaction too new)
        assert_eq!(alloc.freelist_count(), 1);
        assert_eq!(alloc.alloc_page(), Some(3)); // Allocates new page instead
    }

    #[test]
    fn abort_discards_loose_pages() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);

        alloc.free_page(10);
        alloc.free_page(20);
        assert_eq!(alloc.loose_count(), 2);

        alloc.abort();
        assert_eq!(alloc.loose_count(), 0);
        assert_eq!(alloc.freelist_count(), 0);
    }

    #[test]
    fn space_info() {
        let mut alloc = PageAllocator::new(1, 4096 * 100, 4096);
        alloc.set_min_reader_txnid(100);

        // Initial state: pages 0 and 1 are used (meta pages)
        let info = alloc.space_info();
        assert_eq!(info.total_pages, 100);
        assert_eq!(info.used_pages, 2);
        assert_eq!(info.free_pages, 0);

        // Allocate some pages
        alloc.alloc_page();
        alloc.alloc_page();

        let info = alloc.space_info();
        assert_eq!(info.used_pages, 4);

        // Free and commit
        alloc.free_page(2);
        alloc.commit(50);

        let info = alloc.space_info();
        assert_eq!(info.free_pages, 1);
    }

    #[test]
    fn dirty_pages() {
        let mut dirty = DirtyPages::new();

        dirty.insert(5, vec![1, 2, 3, 4]);
        dirty.insert(10, vec![5, 6, 7, 8]);

        assert!(dirty.contains(5));
        assert!(!dirty.contains(6));
        assert_eq!(dirty.len(), 2);
        assert_eq!(dirty.get(5), Some(&[1, 2, 3, 4][..]));

        let taken = dirty.take();
        assert!(dirty.is_empty());
        assert_eq!(taken.len(), 2);
    }
}
