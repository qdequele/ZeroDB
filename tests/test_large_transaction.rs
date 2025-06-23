use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_large_transaction_without_limit() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Test large transaction without page limit
    let mut txn = env.write_txn()?;
    
    eprintln!("Starting large transaction test...");
    let target = 20000; // 20K entries
    for i in 0..target {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![i as u8; 1000]; // 1KB values
        
        match db.put(&mut txn, key, value) {
            Ok(_) => {
                if i % 1000 == 0 {
                    eprintln!("Inserted {} entries", i);
                }
            }
            Err(e) => {
                eprintln!("Error at insertion {}: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    eprintln!("Committing transaction with {} entries...", target);
    txn.commit()?;
    eprintln!("Transaction committed successfully!");
    
    // Verify data
    let txn = env.read_txn()?;
    let count = db.len(&txn)?;
    assert_eq!(count, target as u64);
    
    Ok(())
}