//! Example demonstrating io_uring backend usage

use zerodb::error::{Result, PageId};
use zerodb::io::{IoBackend, BackendType, create_backend, create_auto_backend};
use zerodb::page::{Page, PageFlags};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("ZeroDB io_uring Backend Demo\n");
    
    // Create a temporary file for demonstration
    let temp_file = NamedTempFile::new().map_err(|e| zerodb::error::Error::Io(e.to_string()))?;
    let path = temp_file.path();
    
    // Example 1: Explicitly create io_uring backend
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        println!("1. Creating io_uring backend explicitly...");
        match create_backend(path, BackendType::IoUring) {
            Ok(backend) => {
                println!("✓ Successfully created io_uring backend!");
                test_backend(&*backend)?;
            }
            Err(e) => {
                eprintln!("✗ Failed to create io_uring backend: {}", e);
            }
        }
        println!();
    }
    
    // Example 2: Create mmap backend
    println!("2. Creating mmap backend...");
    let mmap_backend = create_backend(path, BackendType::Mmap)?;
    println!("✓ Successfully created mmap backend!");
    test_backend(&*mmap_backend)?;
    println!();
    
    // Example 3: Auto-select backend
    println!("3. Auto-selecting best backend for platform...");
    let auto_backend = create_auto_backend(path)?;
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    println!("✓ Auto-selected: io_uring backend (if available) or mmap");
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    println!("✓ Auto-selected: mmap backend");
    
    // Example 4: Create io_uring with custom configuration
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        println!("\n4. Creating io_uring backend with custom configuration...");
        use zerodb::io_uring_parallel::IoUringConfig;
        use zerodb::io::IoUringBackend;
        
        let config = IoUringConfig {
            sq_entries: 2048,      // Very large submission queue
            concurrent_ops: 512,   // Many concurrent operations
            kernel_poll: true,     // Enable kernel polling
            sq_poll: false,        // Disable SQ polling (requires CAP_SYS_NICE)
        };
        
        match IoUringBackend::with_config(path, config) {
            Ok(backend) => {
                println!("✓ Successfully created custom io_uring backend!");
                println!("  Configuration:");
                println!("    - Submission queue entries: {}", config.sq_entries);
                println!("    - Concurrent operations: {}", config.concurrent_ops);
                println!("    - Kernel polling: {}", config.kernel_poll);
                println!("    - SQ polling: {}", config.sq_poll);
            }
            Err(e) => {
                eprintln!("✗ Failed to create custom io_uring backend: {}", e);
            }
        }
    }
    
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        println!("\nNote: io_uring is not available on this platform.");
        println!("io_uring requires Linux kernel 5.1 or later.");
    }
    
    Ok(())
}

fn test_backend(backend: &dyn IoBackend) -> Result<()> {
    println!("  Testing backend operations...");
    
    // Get initial size
    let initial_size = backend.size_in_pages();
    println!("  - Initial size: {} pages", initial_size);
    
    // Grow the backend
    let new_size = initial_size + 10;
    backend.grow(new_size)?;
    println!("  - Grew to: {} pages", backend.size_in_pages());
    
    // Write a test page
    let test_page = Page::new(PageId(2), PageFlags::LEAF);
    backend.write_page(&test_page)?;
    println!("  - Wrote test page");
    
    // Read it back
    let read_page = backend.read_page(PageId(2))?;
    println!("  - Read page back, flags: {:?}", read_page.header.flags);
    
    // Sync to disk
    backend.sync()?;
    println!("  - Synced to disk");
    
    Ok(())
}