//! Test environment copy/backup functionality

use std::sync::Arc;
use zerodb::copy::{copy_to_file, copy_with_callback, BackupCallback, CopyOptions};
use zerodb::error::Result;
use zerodb::{Database, EnvBuilder};

struct ProgressCallback {
    last_progress: u64,
    progress_count: usize,
}

impl BackupCallback for ProgressCallback {
    fn progress(&mut self, pages_copied: u64, _total_pages: u64) {
        if pages_copied > self.last_progress + 10 {
            self.progress_count += 1;
            self.last_progress = pages_copied;
        }
    }

    fn complete(&mut self, pages_copied: u64) {
        assert!(pages_copied > 0);
    }
}

#[test]
fn test_simple_copy() -> Result<()> {
    // Create source environment
    let source_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(source_dir.path())?);

    // Add test data
    {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;

        // Add some data
        for i in 0..50 {
            db.put(&mut txn, format!("key_{:03}", i), format!("value_{:03}", i))?;
        }

        txn.commit()?;
    }

    // Simple copy
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("backup.mdb");

    copy_to_file(&env, &backup_path, CopyOptions::default())?;

    // Check file exists and has reasonable size
    let metadata = std::fs::metadata(&backup_path)?;
    assert!(metadata.len() > 0);
    assert!(backup_path.exists());

    Ok(())
}

#[test]
fn test_compact_copy() -> Result<()> {
    let source_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(source_dir.path())?);

    // Add test data
    {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;

        for i in 0..30 {
            db.put(&mut txn, format!("key_{:02}", i), format!("value_{:02}", i))?;
        }
        txn.commit()?;
    }

    // Create a named database too
    {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, Some("test_db"))?;

        for i in 0..30 {
            db.put(&mut txn, format!("test_{:02}", i), format!("data_{:02}", i))?;
        }
        txn.commit()?;
    }

    let backup_dir = tempfile::tempdir().unwrap();
    let normal_path = backup_dir.path().join("normal.mdb");
    let compact_path = backup_dir.path().join("compact.mdb");

    // Normal copy
    copy_to_file(&env, &normal_path, CopyOptions::default())?;
    
    // Compact copy
    copy_to_file(&env, &compact_path, CopyOptions::compact())?;

    let normal_size = std::fs::metadata(&normal_path)?.len();
    let compact_size = std::fs::metadata(&compact_path)?.len();

    assert!(normal_size > 0);
    assert!(compact_size > 0);
    // Compact should be smaller or equal
    assert!(compact_size <= normal_size);

    Ok(())
}

#[test]
fn test_copy_with_progress() -> Result<()> {
    let source_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(source_dir.path())?);

    // Add enough data to trigger progress callbacks
    {
        let mut txn = env.write_txn()?;
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;

        // Add larger data to allocate more pages
        for i in 0..200 {
            let key = format!("bulk_{:04}", i).into_bytes();
            let value = vec![0u8; 500]; // Larger values
            db.put(&mut txn, key, value)?;
        }

        txn.commit()?;
    }

    let backup_dir = tempfile::tempdir().unwrap();
    let progress_path = backup_dir.path().join("progress.mdb");
    let mut callback = ProgressCallback { 
        last_progress: 0,
        progress_count: 0,
    };

    copy_with_callback(&env, &progress_path, CopyOptions::default(), &mut callback)?;

    // Verify file was created
    assert!(progress_path.exists());
    
    // Verify callback was called (complete is always called at least once)
    assert!(callback.last_progress > 0 || callback.progress_count > 0);

    Ok(())
}

#[test]
fn test_copy_multiple_databases() -> Result<()> {
    let source_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(5)
        .open(source_dir.path())?);

    // Create multiple named databases
    {
        let mut txn = env.write_txn()?;
        
        let db1: Database<String, String> = env.create_database(&mut txn, Some("db1"))?;
        let db2: Database<String, String> = env.create_database(&mut txn, Some("db2"))?;
        let db3: Database<String, String> = env.create_database(&mut txn, Some("db3"))?;

        for i in 0..10 {
            db1.put(&mut txn, format!("key1_{}", i), format!("val1_{}", i))?;
            db2.put(&mut txn, format!("key2_{}", i), format!("val2_{}", i))?;
            db3.put(&mut txn, format!("key3_{}", i), format!("val3_{}", i))?;
        }

        txn.commit()?;
    }

    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("multi.mdb");

    copy_to_file(&env, &backup_path, CopyOptions::default())?;

    // Verify backup exists and has content
    let metadata = std::fs::metadata(&backup_path)?;
    assert!(metadata.len() > 1024); // Should be at least a few KB

    Ok(())
}