//! Test automatic database growth functionality

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{auto_grow::{AutoGrowConfig, AutoGrowState}, db::Database, EnvBuilder};

#[test]
fn test_auto_grow_disabled_by_default() {
    // By default, auto-grow should be disabled
    let config = AutoGrowConfig::default();
    assert!(!config.enabled);
}

#[test]
fn test_auto_grow_presets() {
    // Test conservative preset
    let config = AutoGrowConfig::conservative();
    assert!(config.enabled);
    assert_eq!(config.growth_factor, 1.2);
    assert_eq!(config.growth_threshold, 95.0);
    
    // Test moderate preset
    let config = AutoGrowConfig::moderate();
    assert!(config.enabled);
    assert_eq!(config.growth_factor, 1.5);
    assert_eq!(config.growth_threshold, 90.0);
    
    // Test aggressive preset
    let config = AutoGrowConfig::aggressive();
    assert!(config.enabled);
    assert_eq!(config.growth_factor, 2.0);
    assert_eq!(config.growth_threshold, 80.0);
}

#[test]
fn test_growth_calculation_scenarios() {
    let config = AutoGrowConfig::moderate();
    let state = AutoGrowState::new(config);
    
    // Test various database sizes
    let test_cases = vec![
        (100 * 1024 * 1024, "100MB"), // 100MB -> 150MB
        (1 * 1024 * 1024 * 1024, "1GB"), // 1GB -> 1.5GB
        (10 * 1024 * 1024 * 1024, "10GB"), // 10GB -> 15GB (capped at max_growth)
    ];
    
    for (current_size, label) in test_cases {
        let new_size = state.calculate_new_size(current_size).unwrap();
        println!("{}: {} -> {} (growth: {} MB)",
                 label,
                 current_size / (1024 * 1024),
                 new_size / (1024 * 1024),
                 (new_size - current_size) / (1024 * 1024));
        
        assert!(new_size > current_size);
        assert!(new_size % 4096 == 0); // Page aligned
    }
}

#[test]
fn test_growth_threshold_detection() {
    let config = AutoGrowConfig {
        growth_threshold: 85.0,
        ..AutoGrowConfig::moderate()
    };
    let state = AutoGrowState::new(config);
    
    // Test threshold detection
    assert!(!state.needs_growth(84, 100)); // 84% - no growth
    assert!(!state.needs_growth(849, 1000)); // 84.9% - no growth
    assert!(state.needs_growth(85, 100)); // 85% - needs growth
    assert!(state.needs_growth(90, 100)); // 90% - needs growth
    assert!(state.needs_growth(95, 100)); // 95% - needs growth
}

#[test]
fn test_growth_with_max_size_limit() {
    let config = AutoGrowConfig {
        max_size: 10 * 1024 * 1024 * 1024, // 10GB max
        growth_factor: 2.0,
        ..AutoGrowConfig::moderate()
    };
    let state = AutoGrowState::new(config);
    
    // Test growth near limit
    let result = state.calculate_new_size(9 * 1024 * 1024 * 1024).unwrap();
    assert_eq!(result, 10 * 1024 * 1024 * 1024); // Capped at max
    
    // Test at limit
    let result = state.calculate_new_size(10 * 1024 * 1024 * 1024);
    assert!(result.is_err()); // Should fail when at max
}

#[test]
fn test_minimum_growth_enforcement() {
    let config = AutoGrowConfig {
        min_growth: 500 * 1024 * 1024, // 500MB minimum
        growth_factor: 1.1, // Only 10% growth
        ..AutoGrowConfig::moderate()
    };
    let state = AutoGrowState::new(config);
    
    // With 1GB database and 1.1x growth, we'd get 100MB growth
    // But minimum is 500MB
    let new_size = state.calculate_new_size(1 * 1024 * 1024 * 1024).unwrap();
    let growth = new_size - 1 * 1024 * 1024 * 1024;
    assert!(growth >= 500 * 1024 * 1024);
}

#[test]
fn test_conceptual_auto_grow_during_inserts() {
    println!("\n=== Conceptual Auto-Growth Test ===");
    
    // This test demonstrates how auto-growth would work in practice
    // Note: Actual implementation would require modifying WriteTxn
    
    let dir = TempDir::new().unwrap();
    let initial_size = 10 * 1024 * 1024; // Start with 10MB
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(initial_size)
            .open(dir.path())
            .unwrap(),
    );
    
    // Create auto-grow configuration
    let auto_grow_config = AutoGrowConfig {
        enabled: true,
        growth_factor: 2.0,
        min_growth: 5 * 1024 * 1024, // 5MB min
        growth_threshold: 80.0, // Grow at 80%
        ..Default::default()
    };
    
    let auto_grow_state = AutoGrowState::new(auto_grow_config);
    
    // Simulate monitoring during inserts
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Track when growth would be triggered
    let value = vec![0u8; 1024]; // 1KB values
    let mut growth_points = Vec::new();
    
    for i in 0..5000 {
        // Simulate checking space before allocation
        if let Ok(info) = env.space_info() {
            if auto_grow_state.needs_growth(info.used_pages, info.total_pages) {
                growth_points.push((i, info.percent_of_map_used));
                println!("Would trigger growth at key {} ({}% full)",
                         i, info.percent_of_map_used as u32);
                
                // Calculate what the new size would be
                let current_size = info.total_pages * 4096;
                if let Ok(new_size) = auto_grow_state.calculate_new_size(current_size) {
                    println!("  Would grow from {} MB to {} MB",
                             current_size / (1024 * 1024),
                             new_size / (1024 * 1024));
                }
                
                break; // Stop simulation at first growth point
            }
        }
        
        // Try to insert
        let key = format!("key_{:08}", i).into_bytes();
        let mut txn = env.write_txn().unwrap();
        match db.put(&mut txn, key, value.clone()) {
            Ok(_) => {
                txn.commit().unwrap();
            }
            Err(_) => {
                println!("Insert failed at key {} (database full)", i);
                break;
            }
        }
    }
    
    assert!(!growth_points.is_empty(), "Should have detected growth points");
}