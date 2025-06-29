//! Example demonstrating space management features

use std::sync::Arc;
use zerodb::{
    db::Database,
    preflight_checks::PreflightCheck,
    space_info::MapSizeEstimator,
    EnvBuilder,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ZeroDB Space Management Example ===\n");
    
    // 1. Estimate required space before creating database
    estimate_space_requirements();
    
    // 2. Create database with monitoring and pre-flight checks
    create_and_monitor_database()?;
    
    // 4. Show auto-growth configuration (conceptual)
    show_auto_growth_options();
    
    Ok(())
}

fn estimate_space_requirements() {
    println!("1. Estimating Space Requirements\n");
    
    // Scenario 1: Small key-value cache
    let estimator = MapSizeEstimator::new(100_000, 64, 256);
    println!("Small KV Cache (100K entries, 64B keys, 256B values):");
    println!("{}\n", estimator.breakdown());
    
    // Scenario 2: Document store
    let estimator = MapSizeEstimator::new(50_000, 32, 10_000);
    println!("Document Store (50K docs, 32B keys, 10KB values):");
    println!("{}\n", estimator.breakdown());
    
    // Scenario 3: Large blob storage
    let estimator = MapSizeEstimator::new(1_000, 16, 1_000_000);
    println!("Blob Storage (1K blobs, 16B keys, 1MB values):");
    println!("{}\n", estimator.breakdown());
}

fn create_and_monitor_database() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Creating Database with Space Monitoring\n");
    
    let temp_dir = tempfile::tempdir()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024) // 100MB for demo
            .open(temp_dir.path())?
    );
    
    // Check initial space
    let info = env.space_info()?;
    println!("Initial database state:");
    println!("{}", info);
    
    // Create database and insert some data
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert data and monitor space usage
    println!("\nInserting data and monitoring space...");
    let value = vec![0u8; 10_000]; // 10KB values
    
    for batch in 0..5 {
        let mut txn = env.write_txn()?;
        
        for i in 0..100 {
            let key = format!("key_{:08}", batch * 100 + i).into_bytes();
            db.put(&mut txn, key, value.clone())?;
        }
        
        txn.commit()?;
        
        let info = env.space_info()?;
        println!("\nAfter batch {} ({}KB inserted):",
                 batch + 1,
                 (batch + 1) * 100 * 10);
        println!("  Database usage: {:.1}%", info.percent_of_map_used);
        println!("  Pages used: {} / {}", info.used_pages, info.total_pages);
        println!("  Remaining capacity: {} entries",
                 info.estimate_entries_remaining(10_000));
        
        if info.is_near_capacity(80.0) {
            println!("  ⚠️  WARNING: Approaching capacity!");
        }
    }
    
    // Demonstrate pre-flight checks
    println!("\n3. Pre-flight Space Checks\n");
    
    let info = env.space_info()?;
    
    // Check 1: Bulk insert
    println!("Checking bulk insert of 10,000 entries (32B keys, 1KB values):");
    let check = PreflightCheck::check_bulk_insert(&info, 10_000, 32, 1024);
    print_check_result(&check);
    
    // Check 2: Large value
    println!("\nChecking insertion of 5MB value:");
    let check = PreflightCheck::check_large_value(&info, 5 * 1024 * 1024);
    print_check_result(&check);
    
    // Check 3: Transaction
    println!("\nChecking transaction with 500 inserts and 200 updates:");
    let check = PreflightCheck::check_transaction(&info, 500, 200, 1024);
    print_check_result(&check);
    
    Ok(())
}

fn print_check_result(check: &PreflightCheck) {
    println!("  Available pages: {}", check.available_pages);
    println!("  Estimated needed: {}", check.estimated_pages);
    println!("  Can proceed: {}", if check.can_proceed { "✅ YES" } else { "❌ NO" });
    if let Some(warning) = &check.warning {
        println!("  ⚠️  {}", warning);
    }
}

fn show_auto_growth_options() {
    println!("\n4. Auto-Growth Configuration Options\n");
    
    use zerodb::auto_grow::AutoGrowConfig;
    
    println!("Conservative Growth Policy:");
    let config = AutoGrowConfig::conservative();
    println!("  Growth factor: {}x", config.growth_factor);
    println!("  Threshold: {}%", config.growth_threshold);
    println!("  Min growth: {} MB", config.min_growth / (1024 * 1024));
    println!("  Max growth: {} GB\n", config.max_growth / (1024 * 1024 * 1024));
    
    println!("Moderate Growth Policy (Recommended):");
    let config = AutoGrowConfig::moderate();
    println!("  Growth factor: {}x", config.growth_factor);
    println!("  Threshold: {}%", config.growth_threshold);
    println!("  Min growth: {} MB", config.min_growth / (1024 * 1024));
    println!("  Max growth: {} GB\n", config.max_growth / (1024 * 1024 * 1024));
    
    println!("Aggressive Growth Policy:");
    let config = AutoGrowConfig::aggressive();
    println!("  Growth factor: {}x", config.growth_factor);
    println!("  Threshold: {}%", config.growth_threshold);
    println!("  Min growth: {} MB", config.min_growth / (1024 * 1024));
    println!("  Max growth: {} GB", config.max_growth / (1024 * 1024 * 1024));
    
    println!("\nNote: Auto-growth is currently a design proposal.");
    println!("      Use appropriate initial map_size for your workload.");
}