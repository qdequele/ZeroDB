//! Space usage monitoring and estimation utilities

use crate::page::PAGE_SIZE;
use std::fmt;

/// Information about database space usage
#[derive(Debug, Clone, Copy)]
pub struct SpaceInfo {
    /// Total pages allocated in the database file
    pub total_pages: u64,
    /// Pages currently in use (allocated but not free)
    pub used_pages: u64,
    /// Pages available in the freelist
    pub free_pages: u64,
    /// Database file size in bytes
    pub db_size_bytes: u64,
    /// Maximum possible database size based on map size
    pub max_db_size_bytes: u64,
    /// Percentage of database space used
    pub percent_used: f64,
    /// Percentage of map size used
    pub percent_of_map_used: f64,
}

impl SpaceInfo {
    /// Create a new SpaceInfo instance
    pub fn new(
        total_pages: u64,
        used_pages: u64,
        free_pages: u64,
        map_size: u64,
    ) -> Self {
        let db_size_bytes = total_pages * PAGE_SIZE as u64;
        let percent_used = if total_pages > 0 {
            (used_pages as f64 / total_pages as f64) * 100.0
        } else {
            0.0
        };
        let percent_of_map_used = (db_size_bytes as f64 / map_size as f64) * 100.0;
        
        Self {
            total_pages,
            used_pages,
            free_pages,
            db_size_bytes,
            max_db_size_bytes: map_size,
            percent_used,
            percent_of_map_used,
        }
    }
    
    /// Check if the database is approaching capacity
    pub fn is_near_capacity(&self, threshold_percent: f64) -> bool {
        self.percent_of_map_used >= threshold_percent
    }
    
    /// Get the number of pages remaining before hitting the limit
    pub fn pages_remaining(&self) -> u64 {
        let max_pages = self.max_db_size_bytes / PAGE_SIZE as u64;
        max_pages.saturating_sub(self.total_pages)
    }
    
    /// Estimate how many more entries of a given size can be stored
    pub fn estimate_entries_remaining(&self, avg_entry_size: usize) -> u64 {
        let pages_per_entry = (avg_entry_size + PAGE_SIZE - 1) / PAGE_SIZE;
        self.pages_remaining() / pages_per_entry as u64
    }
}

impl fmt::Display for SpaceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Database Space Usage:")?;
        writeln!(f, "  Total pages: {} ({} MB)", 
                 self.total_pages, 
                 self.db_size_bytes / (1024 * 1024))?;
        writeln!(f, "  Used pages: {} ({:.1}%)", 
                 self.used_pages, 
                 self.percent_used)?;
        writeln!(f, "  Free pages: {}", self.free_pages)?;
        writeln!(f, "  Database size: {} MB / {} MB ({:.1}%)",
                 self.db_size_bytes / (1024 * 1024),
                 self.max_db_size_bytes / (1024 * 1024),
                 self.percent_of_map_used)?;
        writeln!(f, "  Pages remaining: {}", self.pages_remaining())?;
        Ok(())
    }
}

/// Estimate the map size needed for a given workload
#[derive(Debug)]
pub struct MapSizeEstimator {
    /// Number of entries expected
    pub num_entries: u64,
    /// Average key size in bytes
    pub avg_key_size: usize,
    /// Average value size in bytes
    pub avg_value_size: usize,
    /// B-tree overhead factor (typically 1.5-2.0)
    pub btree_overhead_factor: f64,
    /// Additional space for metadata, freelist, etc.
    pub metadata_overhead_pages: u64,
    /// Safety margin (e.g., 1.2 for 20% extra space)
    pub safety_margin: f64,
}

impl Default for MapSizeEstimator {
    fn default() -> Self {
        Self {
            num_entries: 0,
            avg_key_size: 0,
            avg_value_size: 0,
            btree_overhead_factor: 1.5,
            metadata_overhead_pages: 1000,
            safety_margin: 1.2,
        }
    }
}

impl MapSizeEstimator {
    /// Create a new estimator with the given parameters
    pub fn new(num_entries: u64, avg_key_size: usize, avg_value_size: usize) -> Self {
        Self {
            num_entries,
            avg_key_size,
            avg_value_size,
            ..Default::default()
        }
    }
    
    /// Estimate the required map size
    pub fn estimate(&self) -> u64 {
        // Calculate data size per entry
        let entry_size = self.avg_key_size + self.avg_value_size;
        
        // Calculate pages needed for data
        let pages_per_entry = if entry_size <= PAGE_SIZE / 2 {
            // Multiple entries per page
            1.0 / ((PAGE_SIZE / 2) as f64 / entry_size as f64)
        } else {
            // Overflow pages needed
            (entry_size as f64 / PAGE_SIZE as f64).ceil()
        };
        
        let data_pages = (self.num_entries as f64 * pages_per_entry) as u64;
        
        // Apply B-tree overhead
        let btree_pages = (data_pages as f64 * self.btree_overhead_factor) as u64;
        
        // Add metadata overhead
        let total_pages = btree_pages + self.metadata_overhead_pages;
        
        // Apply safety margin
        let final_pages = (total_pages as f64 * self.safety_margin) as u64;
        
        // Convert to bytes and round up to nearest GB
        let bytes = final_pages * PAGE_SIZE as u64;
        let gb = 1024 * 1024 * 1024;
        ((bytes + gb - 1) / gb) * gb
    }
    
    /// Get a detailed breakdown of the estimation
    pub fn breakdown(&self) -> String {
        let entry_size = self.avg_key_size + self.avg_value_size;
        let pages_per_entry = if entry_size <= PAGE_SIZE / 2 {
            1.0 / ((PAGE_SIZE / 2) as f64 / entry_size as f64)
        } else {
            (entry_size as f64 / PAGE_SIZE as f64).ceil()
        };
        
        let data_pages = (self.num_entries as f64 * pages_per_entry) as u64;
        let btree_pages = (data_pages as f64 * self.btree_overhead_factor) as u64;
        let total_pages = btree_pages + self.metadata_overhead_pages;
        let final_pages = (total_pages as f64 * self.safety_margin) as u64;
        let bytes = final_pages * PAGE_SIZE as u64;
        
        format!(
            "Map Size Estimation:\n\
             - Entries: {}\n\
             - Entry size: {} bytes (key: {}, value: {})\n\
             - Pages per entry: {:.2}\n\
             - Data pages: {}\n\
             - With B-tree overhead ({}x): {} pages\n\
             - With metadata overhead: {} pages\n\
             - With safety margin ({}x): {} pages\n\
             - Recommended map size: {} GB",
            self.num_entries,
            entry_size,
            self.avg_key_size,
            self.avg_value_size,
            pages_per_entry,
            data_pages,
            self.btree_overhead_factor,
            btree_pages,
            total_pages,
            self.safety_margin,
            final_pages,
            (bytes + (1024 * 1024 * 1024) - 1) / (1024 * 1024 * 1024)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_space_info() {
        let info = SpaceInfo::new(1000, 800, 200, 1024 * 1024 * 1024);
        assert_eq!(info.total_pages, 1000);
        assert_eq!(info.used_pages, 800);
        assert_eq!(info.free_pages, 200);
        assert_eq!(info.percent_used, 80.0);
        assert!(info.pages_remaining() > 0);
    }
    
    #[test]
    fn test_map_size_estimator() {
        // Test case from the bug: 100KB values, 100K entries
        let estimator = MapSizeEstimator::new(100_000, 16, 100_000);
        let estimate = estimator.estimate();
        
        // Should recommend more than 2GB
        assert!(estimate > 2 * 1024 * 1024 * 1024);
        
        println!("{}", estimator.breakdown());
    }
}