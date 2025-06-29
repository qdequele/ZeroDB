//! Debug test for preflight checks

use zerodb::preflight_checks::PreflightCheck;
use zerodb::space_info::SpaceInfo;
use zerodb::page::PAGE_SIZE;

#[test]
fn debug_preflight_check() {
    // Create a consistent SpaceInfo: 1GB map, 100K pages total, 50K used
    let map_size = 1024 * 1024 * 1024; // 1GB
    let total_pages = map_size / PAGE_SIZE as u64; // 262144 pages
    let used_pages = 50_000;
    let free_pages = total_pages - used_pages;
    let space_info = SpaceInfo::new(total_pages, used_pages, free_pages, map_size);
    
    println!("Space info:");
    println!("  Map size: {} MB", map_size / (1024 * 1024));
    println!("  Total pages: {}", total_pages);
    println!("  Used pages: {}", used_pages);
    println!("  Free pages: {}", free_pages);
    println!("  Pages remaining: {}", space_info.pages_remaining());
    
    // Borderline case - 50K entries with 1KB each
    let check = PreflightCheck::check_bulk_insert(&space_info, 50_000, 32, 1000);
    println!("\nBorderline check:");
    println!("  Available pages: {}", check.available_pages);
    println!("  Estimated pages: {}", check.estimated_pages);
    println!("  Can proceed: {}", check.can_proceed);
    println!("  Warning: {:?}", check.warning);
    
    // Calculate usage percentage
    let usage_percent = (check.estimated_pages * 100) / check.available_pages;
    println!("  Usage percent: {}%", usage_percent);
}