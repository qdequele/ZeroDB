//! Example demonstrating database size limit enforcement

use zerodb::{env::EnvBuilder, db::Database, error::Error};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let temp_dir = tempfile::tempdir()?;
    
    println!("Creating database with 5MB size limit...");
    
    // Create environment with size limit
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(50 * 1024 * 1024)     // 50MB map size
            .max_database_size(5 * 1024 * 1024)  // 5MB limit
            .open(temp_dir.path())?
    );
    
    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Write data until we hit the size limit
    let value = vec![0u8; 10 * 1024]; // 10KB value
    let mut total_written = 0;
    let mut key_counter = 0;
    
    println!("Writing 10KB values until we hit the 5MB limit...");
    
    loop {
        let mut txn = env.write_txn()?;
        let mut batch_written = 0;
        
        // Try to write a batch of entries
        for _ in 0..10 {
            let key = format!("key_{:06}", key_counter);
            
            match db.put(&mut txn, key.clone(), value.clone()) {
                Ok(()) => {
                    key_counter += 1;
                    batch_written += value.len();
                }
                Err(Error::DatabaseFull { current_size, requested_size, max_size }) => {
                    println!("\nDatabase full!");
                    println!("  Current size: {} bytes ({:.2} MB)", current_size, current_size as f64 / 1024.0 / 1024.0);
                    println!("  Requested size: {} bytes ({:.2} MB)", requested_size, requested_size as f64 / 1024.0 / 1024.0);
                    println!("  Maximum size: {} bytes ({:.2} MB)", max_size, max_size as f64 / 1024.0 / 1024.0);
                    println!("  Total data written: {} bytes ({:.2} MB)", total_written, total_written as f64 / 1024.0 / 1024.0);
                    println!("  Number of entries: {}", key_counter);
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        
        // Commit the batch
        match txn.commit() {
            Ok(()) => {
                total_written += batch_written;
                if total_written % (100 * 1024) == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout())?;
                }
            }
            Err(Error::DatabaseFull { current_size, requested_size, max_size }) => {
                println!("\nDatabase full during commit!");
                println!("  Current size: {} bytes ({:.2} MB)", current_size, current_size as f64 / 1024.0 / 1024.0);
                println!("  Requested size: {} bytes ({:.2} MB)", requested_size, requested_size as f64 / 1024.0 / 1024.0);
                println!("  Maximum size: {} bytes ({:.2} MB)", max_size, max_size as f64 / 1024.0 / 1024.0);
                println!("  Total data written: {} bytes ({:.2} MB)", total_written, total_written as f64 / 1024.0 / 1024.0);
                println!("  Number of entries: {}", key_counter);
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}