//! Adaptive page sizing for different workloads
//!
//! This module implements dynamic page size selection based on workload
//! characteristics to optimize performance for different use cases.

use crate::cache_aligned::CacheAlignedCounter;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Supported page sizes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PageSize {
    /// 4KB - Default, good for random access
    Small = 4096,
    /// 16KB - Better for mixed workloads
    Medium = 16384,
    /// 64KB - Good for sequential scans
    Large = 65536,
    /// 256KB - Optimal for bulk operations
    Huge = 262144,
}

impl PageSize {
    /// Get the size in bytes
    pub fn bytes(self) -> usize {
        self as usize
    }

    /// Get optimal page size for average value size
    pub fn from_avg_value_size(avg_size: usize) -> Self {
        match avg_size {
            0..=128 => PageSize::Small,     // Small values, many per page
            129..=1024 => PageSize::Medium, // Medium values
            1025..=8192 => PageSize::Large, // Large values
            _ => PageSize::Huge,            // Very large values
        }
    }

    /// Get optimal page size for access pattern
    pub fn from_access_pattern(pattern: AccessPattern) -> Self {
        match pattern {
            AccessPattern::Random => PageSize::Small,
            AccessPattern::Sequential => PageSize::Large,
            AccessPattern::Mixed => PageSize::Medium,
            AccessPattern::Bulk => PageSize::Huge,
        }
    }
}

/// Access pattern detection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessPattern {
    /// Random access pattern
    Random,
    /// Sequential access pattern
    Sequential,
    /// Mixed access pattern
    Mixed,
    /// Bulk operations
    Bulk,
}

/// Adaptive page size selector
pub struct AdaptivePageSelector {
    /// Current page size
    current_size: AtomicUsize,
    /// Access pattern detector
    pattern_detector: AccessPatternDetector,
    /// Value size tracker
    value_size_tracker: ValueSizeTracker,
    /// Performance monitor
    perf_monitor: PerformanceMonitor,
    /// Last adaptation time
    last_adaptation: AtomicU64,
}

impl AdaptivePageSelector {
    /// Create a new adaptive page selector
    pub fn new() -> Self {
        Self {
            current_size: AtomicUsize::new(PageSize::Small as usize),
            pattern_detector: AccessPatternDetector::new(),
            value_size_tracker: ValueSizeTracker::new(),
            perf_monitor: PerformanceMonitor::new(),
            last_adaptation: AtomicU64::new(0),
        }
    }

    /// Record a page access
    pub fn record_access(&self, page_id: u64, is_sequential: bool) {
        self.pattern_detector.record_access(page_id, is_sequential);
    }

    /// Record a value size
    pub fn record_value(&self, key_size: usize, value_size: usize) {
        self.value_size_tracker.record(key_size, value_size);
    }

    /// Record operation performance
    pub fn record_operation(&self, duration_ns: u64, bytes: usize) {
        self.perf_monitor.record(duration_ns, bytes);
    }

    /// Get the current recommended page size
    pub fn get_page_size(&self) -> PageSize {
        let size = self.current_size.load(Ordering::Relaxed);
        match size {
            4096 => PageSize::Small,
            16384 => PageSize::Medium,
            65536 => PageSize::Large,
            262144 => PageSize::Huge,
            _ => PageSize::Small,
        }
    }

    /// Adapt page size based on workload
    pub fn adapt(&self) -> Option<PageSize> {
        // Only adapt every 10 seconds
        let now = Instant::now().elapsed().as_secs();
        let last = self.last_adaptation.load(Ordering::Relaxed);
        if now - last < 10 {
            return None;
        }

        // Get workload characteristics
        let pattern = self.pattern_detector.get_pattern();
        let avg_value_size = self.value_size_tracker.get_average();
        let throughput = self.perf_monitor.get_throughput();

        // Determine optimal page size
        let pattern_size = PageSize::from_access_pattern(pattern);
        let value_size = PageSize::from_avg_value_size(avg_value_size);

        // Choose based on priority
        let new_size = if throughput < 10_000_000 {
            // Low throughput, optimize for latency
            PageSize::Small
        } else if pattern == AccessPattern::Sequential || pattern == AccessPattern::Bulk {
            // Sequential/bulk access benefits from larger pages
            std::cmp::max(pattern_size, value_size)
        } else {
            // Mixed/random access
            value_size
        };

        // Update if changed
        let current = self.get_page_size();
        if new_size != current {
            self.current_size.store(new_size as usize, Ordering::Relaxed);
            self.last_adaptation.store(now, Ordering::Relaxed);
            Some(new_size)
        } else {
            None
        }
    }
}

/// Access pattern detector
struct AccessPatternDetector {
    /// Recent page accesses
    recent_pages: parking_lot::Mutex<Vec<u64>>,
    /// Sequential access count
    sequential_count: CacheAlignedCounter,
    /// Random access count
    random_count: CacheAlignedCounter,
    /// Bulk operation count
    bulk_count: CacheAlignedCounter,
}

impl AccessPatternDetector {
    fn new() -> Self {
        Self {
            recent_pages: parking_lot::Mutex::new(Vec::with_capacity(1000)),
            sequential_count: CacheAlignedCounter::new(0),
            random_count: CacheAlignedCounter::new(0),
            bulk_count: CacheAlignedCounter::new(0),
        }
    }

    fn record_access(&self, page_id: u64, is_bulk: bool) {
        if is_bulk {
            self.bulk_count.increment();
            return;
        }

        let mut recent = self.recent_pages.lock();

        // Check if sequential
        let is_sequential = if let Some(&last) = recent.last() {
            page_id == last + 1 || page_id == last - 1
        } else {
            false
        };

        if is_sequential {
            self.sequential_count.increment();
        } else {
            self.random_count.increment();
        }

        // Update recent pages
        recent.push(page_id);
        if recent.len() > 1000 {
            recent.remove(0);
        }
    }

    fn get_pattern(&self) -> AccessPattern {
        let seq = self.sequential_count.get();
        let rand = self.random_count.get();
        let bulk = self.bulk_count.get();
        let total = seq + rand + bulk;

        if total == 0 {
            return AccessPattern::Mixed;
        }

        let seq_ratio = seq as f64 / total as f64;
        let bulk_ratio = bulk as f64 / total as f64;

        if bulk_ratio > 0.5 {
            AccessPattern::Bulk
        } else if seq_ratio > 0.7 {
            AccessPattern::Sequential
        } else if seq_ratio < 0.3 {
            AccessPattern::Random
        } else {
            AccessPattern::Mixed
        }
    }
}

/// Value size tracker
struct ValueSizeTracker {
    /// Total key size
    total_key_size: CacheAlignedCounter,
    /// Total value size
    total_value_size: CacheAlignedCounter,
    /// Number of values
    count: CacheAlignedCounter,
}

impl ValueSizeTracker {
    fn new() -> Self {
        Self {
            total_key_size: CacheAlignedCounter::new(0),
            total_value_size: CacheAlignedCounter::new(0),
            count: CacheAlignedCounter::new(0),
        }
    }

    fn record(&self, key_size: usize, value_size: usize) {
        self.total_key_size.add(key_size as u64);
        self.total_value_size.add(value_size as u64);
        self.count.increment();
    }

    fn get_average(&self) -> usize {
        let count = self.count.get();
        if count == 0 {
            return 128; // Default
        }

        let total = self.total_key_size.get() + self.total_value_size.get();
        (total / count) as usize
    }
}

/// Performance monitor
struct PerformanceMonitor {
    /// Total operation time in nanoseconds
    total_time_ns: CacheAlignedCounter,
    /// Total bytes processed
    total_bytes: CacheAlignedCounter,
    /// Operation count
    ops_count: CacheAlignedCounter,
}

impl PerformanceMonitor {
    fn new() -> Self {
        Self {
            total_time_ns: CacheAlignedCounter::new(0),
            total_bytes: CacheAlignedCounter::new(0),
            ops_count: CacheAlignedCounter::new(0),
        }
    }

    fn record(&self, duration_ns: u64, bytes: usize) {
        self.total_time_ns.add(duration_ns);
        self.total_bytes.add(bytes as u64);
        self.ops_count.increment();
    }

    fn get_throughput(&self) -> u64 {
        let time_ns = self.total_time_ns.get();
        let bytes = self.total_bytes.get();

        if time_ns == 0 {
            return 0;
        }

        // Calculate bytes per second
        (bytes * 1_000_000_000) / time_ns
    }
}

/// Page size migration helper
pub struct PageSizeMigrator {
    /// Source page size
    from_size: PageSize,
    /// Target page size
    to_size: PageSize,
}

impl PageSizeMigrator {
    /// Create a new migrator
    pub fn new(from: PageSize, to: PageSize) -> Self {
        Self { from_size: from, to_size: to }
    }

    /// Calculate how many old pages fit in a new page
    pub fn pages_per_new_page(&self) -> usize {
        self.to_size.bytes() / self.from_size.bytes()
    }

    /// Calculate new page ID after migration
    pub fn map_page_id(&self, old_id: u64) -> (u64, usize) {
        let pages_per_new = self.pages_per_new_page() as u64;
        let new_id = old_id / pages_per_new;
        let offset = (old_id % pages_per_new) as usize;
        (new_id, offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size_selection() {
        assert_eq!(PageSize::from_avg_value_size(64), PageSize::Small);
        assert_eq!(PageSize::from_avg_value_size(512), PageSize::Medium);
        assert_eq!(PageSize::from_avg_value_size(4096), PageSize::Large);
        assert_eq!(PageSize::from_avg_value_size(16384), PageSize::Huge);
    }

    #[test]
    fn test_access_pattern_detection() {
        let detector = AccessPatternDetector::new();

        // Simulate sequential access
        for i in 0..100 {
            detector.record_access(i, false);
        }

        assert_eq!(detector.get_pattern(), AccessPattern::Sequential);

        // Simulate random access
        for _ in 0..100 {
            detector.record_access(rand::random::<u64>() % 1000, false);
        }

        // Pattern should now be mixed or random
        let pattern = detector.get_pattern();
        assert!(pattern == AccessPattern::Mixed || pattern == AccessPattern::Random);
    }

    #[test]
    fn test_page_migration() {
        let migrator = PageSizeMigrator::new(PageSize::Small, PageSize::Large);

        assert_eq!(migrator.pages_per_new_page(), 16); // 64KB / 4KB

        let (new_id, offset) = migrator.map_page_id(17);
        assert_eq!(new_id, 1);
        assert_eq!(offset, 1);
    }
}
