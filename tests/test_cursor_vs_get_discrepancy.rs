//! Test discrepancy between get() and cursor iteration

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_cursor_vs_get_at_limit() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1024 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert exactly 60 entries (the limit we found)
    for i in 0..60 {
        let mut txn = env.write_txn()?;
        let key = format!("key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key, value)?;
        txn.commit()?;
    }
    
    eprintln!("Inserted 60 entries successfully");
    
    // Now insert the 61st entry
    let mut txn = env.write_txn()?;
    let problem_key = format!("key_{:04}", 60).into_bytes();
    let value = vec![42u8; 50];
    db.put(&mut txn, problem_key.clone(), value)?;
    txn.commit()?;
    
    eprintln!("Inserted 61st entry (key_0060)");
    
    // Check with get()
    let read_txn = env.read_txn()?;
    
    eprintln!("\nChecking all keys with get():");
    let mut get_found = 0;
    for i in 0..65 {
        let key = format!("key_{:04}", i.to_string()).into_bytes();
        match db.get(&read_txn, &key) {
            Ok(Some(_)) => {
                get_found += 1;
                if i >= 60 {
                    eprintln!("  get() found key_{:04} ✓", i.to_string());
                }
            }
            Ok(None) => {
                if i <= 60 {
                    eprintln!("  get() did NOT find key_{:04} ✗", i.to_string());
                }
            }
            Err(e) => eprintln!("  get() error for key_{:04}: {:?}", i, e),
        }
    }
    eprintln!("Total found with get(): {}", get_found);
    
    // Check with cursor
    eprintln!("\nChecking with cursor iteration:");
    let mut cursor = db.cursor(&read_txn)?;
    let mut cursor_count = 0;
    let mut last_key = None;
    
    if let Ok(Some((_k, _))) = cursor.first() {
        cursor_count += 1;
        while let Ok(Some((k, _))) = cursor.next_raw() {
            last_key = Some(k);
            cursor_count += 1;
        }
    }
    
    eprintln!("Total found with cursor: {}", cursor_count);
    if let Some(k) = last_key {
        eprintln!("Last key from cursor: {}", String::from_utf8_lossy(k));
    }
    
    // Try to seek to the problem key
    eprintln!("\nTrying to seek to key_0060:");
    let mut cursor = db.cursor(&read_txn)?;
    match cursor.seek(&problem_key) {
        Ok(Some((k, _))) => {
            eprintln!("  seek() found key: {}", String::from_utf8_lossy(&k));
        }
        Ok(None) => {
            eprintln!("  seek() returned None");
        }
        Err(e) => {
            eprintln!("  seek() error: {:?}", e);
        }
    }
    
    // Try inserting more entries to see what happens
    eprintln!("\nTrying to insert entry 62:");
    let mut txn = env.write_txn()?;
    let key_62 = format!("key_{:04}", 61).into_bytes();
    let value = vec![42u8; 50];
    db.put(&mut txn, key_62.clone(), value)?;
    txn.commit()?;
    
    // Check if key_62 is visible
    let read_txn = env.read_txn()?;
    match db.get(&read_txn, &key_62) {
        Ok(Some(_)) => eprintln!("  get() can find key_0061"),
        Ok(None) => eprintln!("  get() cannot find key_0061"),
        Err(e) => eprintln!("  get() error for key_0061: {:?}", e),
    }
    
    Ok(())
}