//! Pre-flight space checks for database operations

use crate::page::PAGE_SIZE;
use crate::space_info::SpaceInfo;

/// Pre-flight check for bulk operations
pub struct PreflightCheck {
    /// Available pages in the database
    pub available_pages: u64,
    /// Estimated pages needed for operation
    pub estimated_pages: u64,
    /// Whether the operation can proceed
    pub can_proceed: bool,
    /// Warning message if space is tight
    pub warning: Option<String>,
}

impl PreflightCheck {
    /// Check if a bulk insert operation has enough space
    pub fn check_bulk_insert(
        space_info: &SpaceInfo,
        num_entries: usize,
        avg_key_size: usize,
        avg_value_size: usize,
    ) -> Self {
        let available_pages = space_info.pages_remaining();
        let entry_size = avg_key_size + avg_value_size;
        
        // Estimate pages needed
        let pages_per_entry = if entry_size <= PAGE_SIZE / 2 {
            // Multiple entries per page, but account for B-tree overhead
            0.5
        } else {
            // Overflow pages needed
            ((entry_size + PAGE_SIZE - 1) / PAGE_SIZE) as f64
        };
        
        // Add B-tree overhead (nodes, splits, etc.)
        let btree_overhead = 1.3;
        let estimated_pages = ((num_entries as f64 * pages_per_entry) * btree_overhead) as u64;
        
        // Safety margin
        let required_pages = (estimated_pages as f64 * 1.2) as u64;
        
        let can_proceed = required_pages <= available_pages;
        
        let warning = if !can_proceed {
            Some(format!(
                "Insufficient space: need ~{} pages but only {} available. \
                 Consider increasing map_size by at least {} MB.",
                required_pages,
                available_pages,
                ((required_pages - available_pages) * PAGE_SIZE as u64) / (1024 * 1024)
            ))
        } else if required_pages > available_pages * 80 / 100 {
            Some(format!(
                "Warning: Operation will use ~{}% of remaining space",
                (required_pages * 100) / available_pages
            ))
        } else {
            None
        };
        
        PreflightCheck {
            available_pages,
            estimated_pages: required_pages,
            can_proceed,
            warning,
        }
    }
    
    /// Check if a single large value has enough space
    pub fn check_large_value(
        space_info: &SpaceInfo,
        value_size: usize,
    ) -> Self {
        let available_pages = space_info.pages_remaining();
        let pages_needed = ((value_size + PAGE_SIZE - 1) / PAGE_SIZE) as u64;
        
        // Add some overhead for B-tree operations
        let required_pages = pages_needed + 10;
        
        let can_proceed = required_pages <= available_pages;
        
        let warning = if !can_proceed {
            Some(format!(
                "Insufficient space for {} byte value: need {} pages but only {} available",
                value_size, required_pages, available_pages
            ))
        } else {
            None
        };
        
        PreflightCheck {
            available_pages,
            estimated_pages: required_pages,
            can_proceed,
            warning,
        }
    }
    
    /// Check if a transaction with estimated changes has enough space
    pub fn check_transaction(
        space_info: &SpaceInfo,
        estimated_inserts: usize,
        estimated_updates: usize,
        avg_entry_size: usize,
    ) -> Self {
        let available_pages = space_info.pages_remaining();
        
        // Calculate pages for inserts
        let pages_per_entry = ((avg_entry_size + PAGE_SIZE - 1) / PAGE_SIZE).max(1) as f64;
        let insert_pages = (estimated_inserts as f64 * pages_per_entry) as u64;
        
        // Updates might need COW pages
        let update_pages = (estimated_updates as f64 * 0.5) as u64;
        
        // Add overhead for B-tree operations, splits, etc.
        let overhead_pages = ((insert_pages + update_pages) as f64 * 0.3) as u64;
        
        let estimated_pages = insert_pages + update_pages + overhead_pages;
        let required_pages = (estimated_pages as f64 * 1.2) as u64; // Safety margin
        
        let can_proceed = required_pages <= available_pages;
        
        let warning = if !can_proceed {
            Some(format!(
                "Transaction may require ~{} pages but only {} available",
                required_pages, available_pages
            ))
        } else if required_pages > available_pages * 70 / 100 {
            Some(format!(
                "Warning: Large transaction will use ~{}% of remaining space",
                (required_pages * 100) / available_pages
            ))
        } else {
            None
        };
        
        PreflightCheck {
            available_pages,
            estimated_pages: required_pages,
            can_proceed,
            warning,
        }
    }
}

/// Helper trait for types that can estimate their space requirements
pub trait SpaceEstimate {
    /// Estimate the number of pages this value will require
    fn estimate_pages(&self) -> u64;
}

impl SpaceEstimate for Vec<u8> {
    fn estimate_pages(&self) -> u64 {
        ((self.len() + PAGE_SIZE - 1) / PAGE_SIZE) as u64
    }
}

impl SpaceEstimate for &[u8] {
    fn estimate_pages(&self) -> u64 {
        ((self.len() + PAGE_SIZE - 1) / PAGE_SIZE) as u64
    }
}

impl SpaceEstimate for String {
    fn estimate_pages(&self) -> u64 {
        ((self.len() + PAGE_SIZE - 1) / PAGE_SIZE) as u64
    }
}

impl SpaceEstimate for &str {
    fn estimate_pages(&self) -> u64 {
        ((self.len() + PAGE_SIZE - 1) / PAGE_SIZE) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bulk_insert_check() {
        let space_info = SpaceInfo::new(100_000, 50_000, 50_000, 1024 * 1024 * 1024);
        
        // Small entries that should fit
        let check = PreflightCheck::check_bulk_insert(&space_info, 1000, 32, 100);
        assert!(check.can_proceed);
        assert!(check.warning.is_none());
        
        // Large entries that won't fit
        let check = PreflightCheck::check_bulk_insert(&space_info, 100_000, 32, 10_000);
        assert!(!check.can_proceed);
        assert!(check.warning.is_some());
        
        // Borderline case
        let check = PreflightCheck::check_bulk_insert(&space_info, 30_000, 32, 1000);
        assert!(check.can_proceed);
        assert!(check.warning.is_some()); // Should warn about high usage
    }
    
    #[test]
    fn test_large_value_check() {
        let space_info = SpaceInfo::new(1000, 900, 100, 1024 * 1024 * 1024);
        
        // Small value
        let check = PreflightCheck::check_large_value(&space_info, 4096);
        assert!(check.can_proceed);
        
        // Large value that won't fit
        let check = PreflightCheck::check_large_value(&space_info, 500 * 4096);
        assert!(!check.can_proceed);
    }
    
    #[test]
    fn test_space_estimate_trait() {
        let small_vec = vec![0u8; 100];
        assert_eq!(small_vec.estimate_pages(), 1);
        
        let large_vec = vec![0u8; 10_000];
        assert_eq!(large_vec.estimate_pages(), 3); // ceil(10000/4096)
        
        let string = "a".repeat(5000);
        assert_eq!(string.estimate_pages(), 2);
    }
}