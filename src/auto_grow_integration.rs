//! Integration helpers for automatic database growth

use crate::auto_grow::{AutoGrowConfig, AutoGrowState};
use crate::env::EnvInner;
use crate::error::Result;
use std::sync::Arc;

/// Helper to check and perform auto-growth if needed
pub fn check_and_grow(inner: &Arc<EnvInner>, auto_grow: &AutoGrowState) -> Result<bool> {
    // Get current usage
    let total_pages = inner.io.size_in_pages();
    let used_pages = inner.next_page_id.load(std::sync::atomic::Ordering::Acquire);
    
    // Check if growth is needed
    if !auto_grow.needs_growth(used_pages, total_pages) {
        return Ok(false);
    }
    
    // Calculate new size
    let current_size = total_pages * crate::page::PAGE_SIZE as u64;
    let new_size_bytes = auto_grow.calculate_new_size(current_size)?;
    let new_size_pages = new_size_bytes / crate::page::PAGE_SIZE as u64;
    
    // Log the growth attempt
    eprintln!(
        "Auto-growing database: {} pages -> {} pages ({} MB -> {} MB)",
        total_pages,
        new_size_pages,
        current_size / (1024 * 1024),
        new_size_bytes / (1024 * 1024)
    );
    
    // Attempt to grow
    inner.io.grow(new_size_pages)?;
    
    Ok(true)
}

/// Builder extension for auto-growth configuration
pub trait EnvBuilderAutoGrowExt {
    /// Enable auto-growth with default configuration
    fn with_auto_grow(self) -> Self;
    
    /// Enable auto-growth with custom configuration
    fn with_auto_grow_config(self, config: AutoGrowConfig) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auto_grow::AutoGrowConfig;
    use tempfile::TempDir;
    
    #[test]
    fn test_growth_integration() {
        // This is a conceptual test - actual implementation would need
        // proper environment setup
        let config = AutoGrowConfig::moderate();
        let state = AutoGrowState::new(config);
        
        // Verify configuration
        assert!(state.needs_growth(91, 100));
        assert!(!state.needs_growth(89, 100));
    }
}