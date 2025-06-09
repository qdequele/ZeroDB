//! Segregated free list with size classes for efficient allocation
//!
//! This module implements a segregated free list that groups free pages
//! by size classes to reduce fragmentation and improve allocation performance.

use crate::cache_aligned::CacheAlignedCounter;
use crate::error::{PageId, Result, TransactionId};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Size class for page allocation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SizeClass {
    /// Single page (4KB)
    Single = 1,
    /// Small extent (2-4 pages, 8-16KB)
    Small = 4,
    /// Medium extent (5-16 pages, 20-64KB)
    Medium = 16,
    /// Large extent (17-64 pages, 68-256KB)
    Large = 64,
    /// Huge extent (65+ pages, 260KB+)
    Huge = 256,
}

impl SizeClass {
    /// Get size class for a given number of pages
    pub fn from_page_count(pages: usize) -> Self {
        match pages {
            1 => SizeClass::Single,
            2..=4 => SizeClass::Small,
            5..=16 => SizeClass::Medium,
            17..=64 => SizeClass::Large,
            _ => SizeClass::Huge,
        }
    }

    /// Get the maximum pages in this size class
    pub fn max_pages(&self) -> usize {
        *self as usize
    }

    /// Check if a given size fits in this class
    pub fn fits(&self, pages: usize) -> bool {
        match self {
            SizeClass::Single => pages == 1,
            SizeClass::Small => (2..=4).contains(&pages),
            SizeClass::Medium => (5..=16).contains(&pages),
            SizeClass::Large => (17..=64).contains(&pages),
            SizeClass::Huge => pages >= 65,
        }
    }
}

/// Free extent information
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FreeExtent {
    /// Starting page ID
    pub start: PageId,
    /// Number of contiguous pages
    pub pages: usize,
}

impl FreeExtent {
    /// Split extent at the given offset
    pub fn split(self, at: usize) -> (FreeExtent, Option<FreeExtent>) {
        assert!(at > 0 && at <= self.pages);

        let first = FreeExtent { start: self.start, pages: at };

        let second = if at < self.pages {
            Some(FreeExtent { start: PageId(self.start.0 + at as u64), pages: self.pages - at })
        } else {
            None
        };

        (first, second)
    }

    /// Check if this extent is adjacent to another
    pub fn is_adjacent(&self, other: &FreeExtent) -> bool {
        self.start.0 + self.pages as u64 == other.start.0
            || other.start.0 + other.pages as u64 == self.start.0
    }

    /// Merge with an adjacent extent
    pub fn merge(self, other: FreeExtent) -> Result<FreeExtent> {
        if !self.is_adjacent(&other) {
            return Err(crate::error::Error::Custom("Extents are not adjacent".into()));
        }

        let start = PageId(self.start.0.min(other.start.0));
        let pages = self.pages + other.pages;

        Ok(FreeExtent { start, pages })
    }
}

/// Segregated free list manager
pub struct SegregatedFreeList {
    /// Free extents organized by size class
    size_classes: RwLock<HashMap<SizeClass, BTreeSet<FreeExtent>>>,
    /// Map of page ID to extent for fast lookup
    page_map: RwLock<BTreeMap<PageId, FreeExtent>>,
    /// Pending frees (not yet committed)
    pending: RwLock<Vec<FreeExtent>>,
    /// Transaction-specific free lists
    txn_frees: RwLock<BTreeMap<TransactionId, Vec<FreeExtent>>>,
    /// Oldest reader transaction
    oldest_reader: RwLock<TransactionId>,
    /// Statistics
    stats: Arc<SegregatedStats>,
}

/// Statistics for segregated free list
pub struct SegregatedStats {
    /// Allocations per size class
    allocs: HashMap<SizeClass, CacheAlignedCounter>,
    /// Frees per size class
    frees: HashMap<SizeClass, CacheAlignedCounter>,
    /// Fragmentation events
    fragmentation: CacheAlignedCounter,
    /// Coalescing events
    coalescing: CacheAlignedCounter,
    /// Total free pages
    free_pages: CacheAlignedCounter,
}

impl SegregatedStats {
    fn new() -> Self {
        let mut allocs = HashMap::new();
        let mut frees = HashMap::new();

        for &class in &[
            SizeClass::Single,
            SizeClass::Small,
            SizeClass::Medium,
            SizeClass::Large,
            SizeClass::Huge,
        ] {
            allocs.insert(class, CacheAlignedCounter::new(0));
            frees.insert(class, CacheAlignedCounter::new(0));
        }

        Self {
            allocs,
            frees,
            fragmentation: CacheAlignedCounter::new(0),
            coalescing: CacheAlignedCounter::new(0),
            free_pages: CacheAlignedCounter::new(0),
        }
    }
}

impl Default for SegregatedFreeList {
    fn default() -> Self {
        Self::new()
    }
}

impl SegregatedFreeList {
    /// Create a new segregated free list
    pub fn new() -> Self {
        let mut size_classes = HashMap::new();
        for &class in &[
            SizeClass::Single,
            SizeClass::Small,
            SizeClass::Medium,
            SizeClass::Large,
            SizeClass::Huge,
        ] {
            size_classes.insert(class, BTreeSet::new());
        }

        Self {
            size_classes: RwLock::new(size_classes),
            page_map: RwLock::new(BTreeMap::new()),
            pending: RwLock::new(Vec::new()),
            txn_frees: RwLock::new(BTreeMap::new()),
            oldest_reader: RwLock::new(TransactionId(0)),
            stats: Arc::new(SegregatedStats::new()),
        }
    }

    /// Allocate pages of the requested size
    pub fn allocate(&self, pages: usize) -> Option<PageId> {
        if pages == 0 {
            return None;
        }

        let target_class = SizeClass::from_page_count(pages);
        let mut size_classes = self.size_classes.write();

        // Try exact fit first
        if let Some(extents) = size_classes.get_mut(&target_class) {
            if let Some(extent) = extents.iter().find(|e| e.pages >= pages).cloned() {
                extents.remove(&extent);

                // Remove from page map
                let mut page_map = self.page_map.write();
                page_map.remove(&extent.start);

                // Handle splitting if needed
                if extent.pages > pages {
                    let (allocated, remainder) = extent.split(pages);

                    if let Some(rem) = remainder {
                        // Add remainder back to appropriate size class
                        let rem_class = SizeClass::from_page_count(rem.pages);
                        size_classes.entry(rem_class).or_default().insert(rem);
                        page_map.insert(rem.start, rem);

                        self.stats.fragmentation.increment();
                    }

                    self.stats.allocs.get(&target_class).unwrap().increment();
                    self.stats.free_pages.add(-(pages as i64) as u64);
                    return Some(allocated.start);
                } else {
                    self.stats.allocs.get(&target_class).unwrap().increment();
                    self.stats.free_pages.add(-(pages as i64) as u64);
                    return Some(extent.start);
                }
            }
        }

        // Try larger size classes
        for &class in &[SizeClass::Small, SizeClass::Medium, SizeClass::Large, SizeClass::Huge] {
            if class <= target_class {
                continue;
            }

            if let Some(extents) = size_classes.get_mut(&class) {
                if let Some(extent) = extents.iter().next().cloned() {
                    extents.remove(&extent);

                    // Remove from page map
                    let mut page_map = self.page_map.write();
                    page_map.remove(&extent.start);

                    // Split the extent
                    let (allocated, remainder) = extent.split(pages);

                    if let Some(rem) = remainder {
                        // Add remainder back
                        let rem_class = SizeClass::from_page_count(rem.pages);
                        size_classes.entry(rem_class).or_default().insert(rem);
                        page_map.insert(rem.start, rem);

                        self.stats.fragmentation.increment();
                    }

                    self.stats.allocs.get(&target_class).unwrap().increment();
                    self.stats.free_pages.add(-(pages as i64) as u64);
                    return Some(allocated.start);
                }
            }
        }

        None
    }

    /// Free a contiguous extent of pages
    pub fn free(&self, start: PageId, pages: usize) {
        if pages == 0 {
            return;
        }

        let extent = FreeExtent { start, pages };
        self.pending.write().push(extent);

        let class = SizeClass::from_page_count(pages);
        self.stats.frees.get(&class).unwrap().increment();
    }

    /// Commit pending frees for a transaction
    pub fn commit(&self, txn_id: TransactionId) {
        let mut pending = self.pending.write();
        if pending.is_empty() {
            return;
        }

        let extents = std::mem::take(&mut *pending);
        self.txn_frees.write().insert(txn_id, extents);
    }

    /// Update oldest reader and process committed frees
    pub fn update_oldest_reader(&self, reader: TransactionId) {
        *self.oldest_reader.write() = reader;
        self.process_committed_frees();
    }

    /// Process committed frees that are now safe to reuse
    fn process_committed_frees(&self) {
        let oldest = *self.oldest_reader.read();
        let mut txn_frees = self.txn_frees.write();
        let mut to_remove = Vec::new();

        for (&txn_id, extents) in txn_frees.iter() {
            if oldest.0 == 0 || txn_id.0 < oldest.0 {
                // Safe to reuse these extents
                for &extent in extents {
                    self.add_free_extent(extent);
                }
                to_remove.push(txn_id);
            }
        }

        for txn_id in to_remove {
            txn_frees.remove(&txn_id);
        }
    }

    /// Add a free extent, coalescing with neighbors if possible
    fn add_free_extent(&self, extent: FreeExtent) {
        let mut size_classes = self.size_classes.write();
        let mut page_map = self.page_map.write();

        // Check for adjacent extents to coalesce
        let mut coalesced = extent;
        let mut did_coalesce = false;

        // Check previous page
        if extent.start.0 > 0 {
            let prev_page = PageId(extent.start.0 - 1);
            if let Some(prev_extent) = page_map.get(&prev_page).cloned() {
                if prev_extent.is_adjacent(&coalesced) {
                    // Remove old extent
                    let prev_class = SizeClass::from_page_count(prev_extent.pages);
                    size_classes.get_mut(&prev_class).unwrap().remove(&prev_extent);
                    page_map.remove(&prev_extent.start);

                    // Merge
                    coalesced = coalesced.merge(prev_extent).unwrap();
                    did_coalesce = true;
                }
            }
        }

        // Check next page
        let next_page = PageId(extent.start.0 + extent.pages as u64);
        if let Some(next_extent) = page_map.get(&next_page).cloned() {
            if coalesced.is_adjacent(&next_extent) {
                // Remove old extent
                let next_class = SizeClass::from_page_count(next_extent.pages);
                size_classes.get_mut(&next_class).unwrap().remove(&next_extent);
                page_map.remove(&next_extent.start);

                // Merge
                coalesced = coalesced.merge(next_extent).unwrap();
                did_coalesce = true;
            }
        }

        if did_coalesce {
            self.stats.coalescing.increment();
        }

        // Add coalesced extent to appropriate size class
        let class = SizeClass::from_page_count(coalesced.pages);
        size_classes.entry(class).or_default().insert(coalesced);
        page_map.insert(coalesced.start, coalesced);

        self.stats.free_pages.add(coalesced.pages as u64);
    }

    /// Get statistics
    pub fn stats(&self) -> FreeListStats {
        let size_classes = self.size_classes.read();
        let mut class_stats = HashMap::new();

        for (&class, extents) in size_classes.iter() {
            let count = extents.len();
            let pages: usize = extents.iter().map(|e| e.pages).sum();
            class_stats.insert(class, (count, pages));
        }

        FreeListStats {
            total_free_pages: self.stats.free_pages.get() as usize,
            fragmentation_events: self.stats.fragmentation.get() as usize,
            coalescing_events: self.stats.coalescing.get() as usize,
            size_class_stats: class_stats,
        }
    }
}

/// Free list statistics
#[derive(Debug)]
pub struct FreeListStats {
    /// Total number of free pages
    pub total_free_pages: usize,
    /// Number of fragmentation events
    pub fragmentation_events: usize,
    /// Number of coalescing events
    pub coalescing_events: usize,
    /// Stats per size class: (extent_count, total_pages)
    pub size_class_stats: HashMap<SizeClass, (usize, usize)>,
}

/// Best-fit allocator using segregated free list
pub struct BestFitAllocator {
    /// Underlying segregated free list
    freelist: Arc<SegregatedFreeList>,
    /// Allocation strategy
    strategy: AllocationStrategy,
}

#[derive(Debug, Clone, Copy)]
/// Strategy for allocating pages from the segregated free list
pub enum AllocationStrategy {
    /// Always use best fit
    BestFit,
    /// Use first fit for small allocations, best fit for large
    Hybrid,
    /// Prefer allocations that minimize fragmentation
    AntiFragmentation,
}

impl BestFitAllocator {
    /// Create a new allocator
    pub fn new(strategy: AllocationStrategy) -> Self {
        Self { freelist: Arc::new(SegregatedFreeList::new()), strategy }
    }

    /// Allocate pages using the configured strategy
    pub fn allocate(&self, pages: usize) -> Option<PageId> {
        match self.strategy {
            AllocationStrategy::BestFit => self.freelist.allocate(pages),
            AllocationStrategy::Hybrid => {
                if pages <= 4 {
                    // Use first fit for small allocations
                    self.freelist.allocate(pages)
                } else {
                    // Use best fit for large allocations
                    self.best_fit_allocate(pages)
                }
            }
            AllocationStrategy::AntiFragmentation => {
                // Prefer allocations that leave useful remainders
                self.anti_frag_allocate(pages)
            }
        }
    }

    /// Best fit allocation
    fn best_fit_allocate(&self, pages: usize) -> Option<PageId> {
        // For now, delegate to default allocator
        // TODO: Implement true best-fit search
        self.freelist.allocate(pages)
    }

    /// Anti-fragmentation allocation
    fn anti_frag_allocate(&self, pages: usize) -> Option<PageId> {
        // Prefer allocations that leave power-of-2 remainders
        // TODO: Implement anti-fragmentation heuristics
        self.freelist.allocate(pages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_class_selection() {
        assert_eq!(SizeClass::from_page_count(1), SizeClass::Single);
        assert_eq!(SizeClass::from_page_count(3), SizeClass::Small);
        assert_eq!(SizeClass::from_page_count(10), SizeClass::Medium);
        assert_eq!(SizeClass::from_page_count(32), SizeClass::Large);
        assert_eq!(SizeClass::from_page_count(100), SizeClass::Huge);
    }

    #[test]
    fn test_extent_operations() {
        let extent = FreeExtent { start: PageId(100), pages: 10 };

        // Test splitting
        let (first, second) = extent.split(4);
        assert_eq!(first.start.0, 100);
        assert_eq!(first.pages, 4);
        assert_eq!(second.unwrap().start.0, 104);
        assert_eq!(second.unwrap().pages, 6);

        // Test adjacency
        let ext1 = FreeExtent { start: PageId(100), pages: 5 };
        let ext2 = FreeExtent { start: PageId(105), pages: 3 };
        assert!(ext1.is_adjacent(&ext2));

        // Test merging
        let merged = ext1.merge(ext2).unwrap();
        assert_eq!(merged.start.0, 100);
        assert_eq!(merged.pages, 8);
    }

    #[test]
    fn test_segregated_allocation() {
        let freelist = SegregatedFreeList::new();

        // Add some free extents
        freelist.free(PageId(100), 1);
        freelist.free(PageId(200), 5);
        freelist.free(PageId(300), 20);

        // Commit and make available
        freelist.commit(TransactionId(1));
        freelist.update_oldest_reader(TransactionId(0));

        // Test allocations
        assert_eq!(freelist.allocate(1), Some(PageId(100)));
        assert_eq!(freelist.allocate(4), Some(PageId(200)));
        assert_eq!(freelist.allocate(15), Some(PageId(300)));

        // Remainder should still be available
        assert_eq!(freelist.allocate(5), Some(PageId(315)));
    }
}
