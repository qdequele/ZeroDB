//! Automatic database growth mechanism

use crate::error::{Error, Result};
use crate::page::PAGE_SIZE;
use std::sync::atomic::{AtomicBool, Ordering};

/// Configuration for automatic database growth
#[derive(Debug, Clone)]
pub struct AutoGrowConfig {
    /// Enable automatic growth
    pub enabled: bool,
    /// Growth factor (e.g., 1.5 = grow by 50%)
    pub growth_factor: f64,
    /// Minimum growth in bytes
    pub min_growth: u64,
    /// Maximum growth in bytes
    pub max_growth: u64,
    /// Threshold percentage to trigger growth (e.g., 90.0)
    pub growth_threshold: f64,
    /// Maximum allowed database size
    pub max_size: u64,
}

impl Default for AutoGrowConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for backward compatibility
            growth_factor: 1.5,
            min_growth: 100 * 1024 * 1024, // 100MB minimum
            max_growth: 10 * 1024 * 1024 * 1024, // 10GB maximum per growth
            growth_threshold: 90.0,
            max_size: 1024 * 1024 * 1024 * 1024, // 1TB max
        }
    }
}

/// Auto-growth state tracker
pub struct AutoGrowState {
    /// Configuration
    config: AutoGrowConfig,
    /// Flag to prevent recursive growth attempts
    growing: AtomicBool,
}

impl AutoGrowState {
    /// Create a new auto-growth state
    pub fn new(config: AutoGrowConfig) -> Self {
        Self {
            config,
            growing: AtomicBool::new(false),
        }
    }
    
    /// Check if growth is needed based on current usage
    pub fn needs_growth(&self, used_pages: u64, total_pages: u64) -> bool {
        if !self.config.enabled {
            return false;
        }
        
        let percent_used = (used_pages as f64 / total_pages as f64) * 100.0;
        percent_used >= self.config.growth_threshold
    }
    
    /// Calculate new size based on growth policy
    pub fn calculate_new_size(&self, current_size: u64) -> Result<u64> {
        // Check if already growing (prevent recursion)
        if self.growing.swap(true, Ordering::SeqCst) {
            return Err(Error::Custom("Growth already in progress".into()));
        }
        
        // Ensure we release the growing flag on exit
        struct GrowingGuard<'a>(&'a AtomicBool);
        impl<'a> Drop for GrowingGuard<'a> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _guard = GrowingGuard(&self.growing);
        
        // Calculate growth amount
        let growth_amount = ((current_size as f64 * self.config.growth_factor) - current_size as f64) as u64;
        let growth_amount = growth_amount.max(self.config.min_growth);
        let growth_amount = growth_amount.min(self.config.max_growth);
        
        // Calculate new size
        let new_size = current_size.saturating_add(growth_amount);
        
        // Check against maximum
        if new_size > self.config.max_size {
            if current_size >= self.config.max_size {
                return Err(Error::DatabaseFull {
                    current_size,
                    requested_size: new_size,
                    max_size: self.config.max_size,
                });
            }
            // Cap at maximum
            Ok(self.config.max_size)
        } else {
            // Round up to page boundary
            let pages = new_size.div_ceil(PAGE_SIZE as u64);
            Ok(pages * PAGE_SIZE as u64)
        }
    }
}

/// Growth policy presets
impl AutoGrowConfig {
    /// Conservative growth policy
    pub fn conservative() -> Self {
        Self {
            enabled: true,
            growth_factor: 1.2, // 20% growth
            min_growth: 50 * 1024 * 1024, // 50MB
            max_growth: 1024 * 1024 * 1024, // 1GB
            growth_threshold: 95.0, // Wait until 95% full
            max_size: 100 * 1024 * 1024 * 1024, // 100GB max
        }
    }
    
    /// Moderate growth policy (recommended)
    pub fn moderate() -> Self {
        Self {
            enabled: true,
            growth_factor: 1.5, // 50% growth
            min_growth: 100 * 1024 * 1024, // 100MB
            max_growth: 5 * 1024 * 1024 * 1024, // 5GB
            growth_threshold: 90.0, // Grow at 90% full
            max_size: 500 * 1024 * 1024 * 1024, // 500GB max
        }
    }
    
    /// Aggressive growth policy
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            growth_factor: 2.0, // Double size
            min_growth: 500 * 1024 * 1024, // 500MB
            max_growth: 20 * 1024 * 1024 * 1024, // 20GB
            growth_threshold: 80.0, // Grow at 80% full
            max_size: 1024 * 1024 * 1024 * 1024, // 1TB max
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_growth_calculation() {
        let config = AutoGrowConfig::moderate();
        let state = AutoGrowState::new(config);
        
        // Test normal growth
        let new_size = state.calculate_new_size(1024 * 1024 * 1024).unwrap();
        assert_eq!(new_size, 1610612736); // 1.5GB rounded to page boundary
        
        // Test minimum growth
        let new_size = state.calculate_new_size(10 * 1024 * 1024).unwrap();
        assert!(new_size >= 110 * 1024 * 1024); // At least 100MB growth
        
        // Test maximum cap
        let config = AutoGrowConfig {
            max_size: 2 * 1024 * 1024 * 1024,
            ..AutoGrowConfig::moderate()
        };
        let state = AutoGrowState::new(config);
        let new_size = state.calculate_new_size(1900 * 1024 * 1024).unwrap();
        assert_eq!(new_size, 2 * 1024 * 1024 * 1024); // Capped at max
    }
    
    #[test]
    fn test_needs_growth() {
        let config = AutoGrowConfig::moderate();
        let state = AutoGrowState::new(config);
        
        assert!(!state.needs_growth(89, 100)); // 89% - no growth
        assert!(state.needs_growth(90, 100)); // 90% - needs growth
        assert!(state.needs_growth(95, 100)); // 95% - needs growth
    }
}