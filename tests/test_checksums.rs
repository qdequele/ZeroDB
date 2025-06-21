//! Tests for page checksum functionality

use zerodb::checksum::{ChecksumMode, ChecksummedPage};
use zerodb::env::{EnvBuilder, DurabilityMode};
use zerodb::error::Error;
use zerodb::page::{Page, PageFlags};
use zerodb::db::Database;
use tempfile::TempDir;
use std::sync::Arc;

#[test]
fn test_checksum_none_mode() {
    let tmp_dir = TempDir::new().unwrap();
    
    // Create environment with no checksums
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .checksum_mode(ChecksumMode::None)
        .open(&tmp_dir)
        .unwrap());
    
    let db: Database<String, String> = {
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
        wtxn.commit().unwrap();
        db
    };
    
    // Write and read data
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
    
    {
        let rtxn = env.read_txn().unwrap();
        let value = db.get(&rtxn, &"key1".to_string()).unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }
}

#[test]
fn test_checksum_meta_only_mode() {
    let tmp_dir = TempDir::new().unwrap();
    
    // Create environment with meta-only checksums
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .checksum_mode(ChecksumMode::MetaOnly)
        .durability(DurabilityMode::FullSync)
        .open(&tmp_dir)
        .unwrap());
    
    let db: Database<String, String> = {
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
        wtxn.commit().unwrap();
        db
    };
    
    // Write data - meta pages should get checksums
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
    
    // Read data
    {
        let rtxn = env.read_txn().unwrap();
        let value = db.get(&rtxn, &"key1".to_string()).unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }
}

#[test]
fn test_checksum_full_mode() {
    let tmp_dir = TempDir::new().unwrap();
    
    // Create environment with full checksums
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .checksum_mode(ChecksumMode::Full)
        .open(&tmp_dir)
        .unwrap());
    
    let db: Database<String, String> = {
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
        wtxn.commit().unwrap();
        db
    };
    
    // Write data - all pages should get checksums
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..100 {
            db.put(&mut wtxn, format!("key{}", i), format!("value{}", i)).unwrap();
        }
        wtxn.commit().unwrap();
    }
    
    // Read data
    {
        let rtxn = env.read_txn().unwrap();
        for i in 0..100 {
            let value = db.get(&rtxn, &format!("key{}", i)).unwrap();
            assert_eq!(value, Some(format!("value{}", i)));
        }
    }
}

#[test]
fn test_checksum_validation() {
    use zerodb::error::PageId;
    
    // Test checksum calculation and validation
    let mut page = Page::new(PageId(1), PageFlags::LEAF);
    
    // Initially no checksum
    assert_eq!(page.header.checksum, 0);
    assert!(!page.has_checksum());
    
    // Add some data
    page.add_node(b"key1", b"value1").unwrap();
    page.add_node(b"key2", b"value2").unwrap();
    
    // Update checksum
    page.update_checksum();
    assert_ne!(page.header.checksum, 0);
    assert!(page.has_checksum());
    
    // Validation should succeed
    assert!(page.validate_checksum().is_ok());
    
    // Store the checksum
    let original_checksum = page.header.checksum;
    
    // Modify the page data
    page.data[0] = 0xFF;  // Corrupt the data
    
    // Checksum should now be invalid
    let result = page.validate_checksum();
    assert!(result.is_err());
    
    match result {
        Err(Error::Corruption { details, page_id }) => {
            assert!(details.contains("Checksum mismatch"));
            assert_eq!(page_id, Some(PageId(1)));
        }
        _ => panic!("Expected Corruption error"),
    }
    
    // Fix the page
    page.data[0] = 0x00;  // Restore original value
    assert!(page.validate_checksum().is_err()); // Still invalid due to checksum
    
    // Update checksum again
    page.update_checksum();
    assert_ne!(page.header.checksum, original_checksum);
    assert!(page.validate_checksum().is_ok());
}

#[test]
fn test_checksum_corruption_detection_skip() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path();
    
    // Create environment with full checksums
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::Full)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
            wtxn.commit().unwrap();
            db
        };
        
        // Write data
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
    
    // Corrupt the database file by directly modifying a page
    {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};
        
        let mut file = OpenOptions::new()
            .write(true)
            .open(db_path.join("data.mdb"))
            .unwrap();
        
        // Seek to a data page (skip meta pages)
        file.seek(SeekFrom::Start(4096 * 3)).unwrap();
        
        // Write some garbage to corrupt the page
        file.write_all(b"CORRUPTED").unwrap();
        file.sync_all().unwrap();
    }
    
    // Try to read the corrupted database
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::Full)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
            wtxn.commit().unwrap();
            db
        };
        
        let rtxn = env.read_txn().unwrap();
        let result = db.get(&rtxn, &"key1".to_string());
        
        // Should fail with corruption error
        match result {
            Err(Error::Corruption { .. }) => {
                // Expected
            }
            Ok(_) => panic!("Expected corruption error"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
}

#[test]
fn test_checksum_write_read() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path();
    
    // Write with checksums enabled
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::Full)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();
            db
        };
        
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
    
    // Read back with checksums enabled
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::Full)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let rtxn = env.read_txn().unwrap();
            env.open_database(&rtxn, None).unwrap()
        };
        
        let rtxn = env.read_txn().unwrap();
        let value = db.get(&rtxn, &"key1".to_string()).unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }
}

#[test]
fn test_backward_compatibility_skip() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path();
    
    // Create database without checksums
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::None)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
            wtxn.commit().unwrap();
            db
        };
        
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
    
    // Reopen with checksum validation enabled
    {
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .checksum_mode(ChecksumMode::Full)
            .open(db_path)
            .unwrap());
        
        let db: Database<String, String> = {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
            wtxn.commit().unwrap();
            db
        };
        
        // Should be able to read old data (pages with no checksums)
        let rtxn = env.read_txn().unwrap();
        let value = db.get(&rtxn, &"key1".to_string()).unwrap();
        assert_eq!(value, Some("value1".to_string()));
        
        // New writes should get checksums
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "key2".to_string(), "value2".to_string()).unwrap();
        wtxn.commit().unwrap();
    }
}