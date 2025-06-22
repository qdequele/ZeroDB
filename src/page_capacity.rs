//! Page capacity management and fill factor strategies
//!
//! This module provides strategies for managing page capacity to prevent
//! premature page full errors and improve overall database performance.

use crate::page::{PageHeader, PAGE_SIZE};

/// Default fill factor for pages (90%)
pub const DEFAULT_FILL_FACTOR: f32 = 0.90;

/// Fill factor for append-heavy workloads (95%)
pub const APPEND_FILL_FACTOR: f32 = 0.95;

/// Fill factor for random insert workloads (85%)
pub const RANDOM_FILL_FACTOR: f32 = 0.85;

/// Minimum free space to maintain in pages (bytes)
pub const MIN_FREE_SPACE: usize = 64;

/// Page capacity configuration
#[derive(Debug, Clone, Copy)]
pub struct PageCapacityConfig {
    /// Target fill factor (0.0 to 1.0)
    pub fill_factor: f32,
    /// Minimum free space to maintain
    pub min_free_space: usize,
    /// Whether to use adaptive fill factors
    pub adaptive: bool,
}

impl Default for PageCapacityConfig {
    fn default() -> Self {
        Self {
            fill_factor: DEFAULT_FILL_FACTOR,
            min_free_space: MIN_FREE_SPACE,
            adaptive: true,
        }
    }
}

impl PageCapacityConfig {
    /// Create a configuration for append-heavy workloads
    pub fn append_optimized() -> Self {
        Self {
            fill_factor: APPEND_FILL_FACTOR,
            min_free_space: MIN_FREE_SPACE,
            adaptive: false,
        }
    }

    /// Create a configuration for random insert workloads
    pub fn random_optimized() -> Self {
        Self {
            fill_factor: RANDOM_FILL_FACTOR,
            min_free_space: MIN_FREE_SPACE * 2,
            adaptive: false,
        }
    }
}

/// Check if a page has enough space for a new entry
pub fn has_space_for_entry(
    header: &PageHeader,
    entry_size: usize,
    config: &PageCapacityConfig,
) -> bool {
    let free_space = header.free_space();
    let total_data_space = PAGE_SIZE - PageHeader::SIZE;
    let usable_space = (total_data_space as f32 * config.fill_factor) as usize;
    let used_space = (header.lower as usize - PageHeader::SIZE) + (PAGE_SIZE - header.upper as usize);
    
    // Check against fill factor
    if used_space + entry_size > usable_space {
        return false;
    }
    
    // Check against minimum free space
    if free_space < entry_size + config.min_free_space {
        return false;
    }
    
    // Additional check for pointer array growth
    let pointer_size = std::mem::size_of::<u16>();
    if free_space < entry_size + pointer_size {
        return false;
    }
    
    true
}

/// Calculate the optimal split point for a page
pub fn calculate_split_point(
    num_entries: usize,
    is_append: bool,
    config: &PageCapacityConfig,
) -> usize {
    if is_append && config.adaptive {
        // For append operations, split at 75% to leave more space in the left page
        (num_entries * 3) / 4
    } else if !config.adaptive {
        // Use standard 50/50 split
        num_entries / 2
    } else {
        // Adaptive split based on fill factor
        let split_ratio = 0.5 + (1.0 - config.fill_factor) * 0.5;
        (num_entries as f32 * split_ratio) as usize
    }
}

/// Page capacity statistics for monitoring
#[derive(Debug, Default)]
pub struct PageCapacityStats {
    /// Total number of pages
    pub total_pages: usize,
    /// Number of pages at or above fill factor
    pub full_pages: usize,
    /// Average page utilization (0.0 to 1.0)
    pub avg_utilization: f32,
    /// Number of page splits
    pub splits: usize,
    /// Number of rejected insertions due to capacity
    pub rejections: usize,
}

impl PageCapacityStats {
    /// Update statistics with a page's current state
    pub fn update_page(&mut self, header: &PageHeader) {
        self.total_pages += 1;
        let utilization = 1.0 - (header.free_space() as f32 / (PAGE_SIZE - PageHeader::SIZE) as f32);
        self.avg_utilization = (self.avg_utilization * (self.total_pages - 1) as f32 + utilization) / self.total_pages as f32;
        
        if utilization >= DEFAULT_FILL_FACTOR {
            self.full_pages += 1;
        }
    }
    
    /// Record a page split
    pub fn record_split(&mut self) {
        self.splits += 1;
    }
    
    /// Record a rejected insertion
    pub fn record_rejection(&mut self) {
        self.rejections += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageFlags;

    #[test]
    fn test_has_space_for_entry() {
        let mut header = PageHeader::new(1, PageFlags::LEAF);
        let config = PageCapacityConfig::default();
        
        // Fresh page should have space
        assert!(has_space_for_entry(&header, 100, &config));
        
        // Simulate filling the page
        header.lower = PageHeader::SIZE as u16 + 1000;
        header.upper = PAGE_SIZE as u16 - 2000;
        
        // Should reject if entry would exceed fill factor
        assert!(!has_space_for_entry(&header, 2000, &config));
    }
    
    #[test]
    fn test_split_points() {
        let config = PageCapacityConfig::default();
        
        // Standard split (adaptive mode gives 55)
        assert_eq!(calculate_split_point(100, false, &config), 55);
        
        // Append split
        assert_eq!(calculate_split_point(100, true, &config), 75);
        
        // Random optimized config
        let random_config = PageCapacityConfig::random_optimized();
        assert_eq!(calculate_split_point(100, false, &random_config), 50);
    }
}