//! Example showing heed-compatible transaction method names

use zerodb::{
    cursor_iter,
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};

fn main() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    // Environment creation is the same
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3)
        .open(&temp_dir)?;
    
    println!("=== ZeroDB with heed-compatible transaction names ===\n");
    
    // Create database
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Write data using heed-style method names
    println!("1. Writing data with heed-style write_txn():");
    {
        let mut wtxn = env.write_txn()?;  // Instead of write_txn()
        
        for i in 0..5 {
            let key = format!("key_{:02}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            db.put(&mut wtxn, key, value)?;
        }
        
        wtxn.commit()?;
        println!("   ✓ Wrote 5 entries");
    }
    
    // Read data using heed-style method names
    println!("\n2. Reading data with heed-style read_txn():");
    {
        let rtxn = env.read_txn()?;  // Instead of read_txn()
        
        for result in cursor_iter::iter(&db, &rtxn)? {
            let (key, value) = result?;
            println!("   {:?} = {:?}", 
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );
        }
    }
    
    // Show that both APIs work together
    println!("\n3. Mixing both API styles:");
    {
        // Start with heed-style
        let rtxn = env.read_txn()?;
        
        // Use with ZeroDB's cursor API
        let mut cursor = db.cursor(&rtxn)?;
        if let Some((key, value)) = cursor.first()? {
            println!("   First: {:?} = {:?}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );
        }
    }
    
    // Note: ZeroDB requires &mut self for write_txn() for safety
    println!("\n4. Safety note:");
    println!("   Unlike heed, ZeroDB's write_txn() requires &mut self");
    println!("   This ensures Rust's borrowing rules are enforced");
    
    println!("\n✓ ZeroDB supports heed-compatible transaction method names!");
    println!("\nComparison:");
    println!("  heed:    env.read_txn()  / env.write_txn()");
    println!("  ZeroDB:  env.read_txn()  / env.write_txn()");
    println!("           env.read_txn() / env.write_txn() (original API)");
    
    Ok(())
}