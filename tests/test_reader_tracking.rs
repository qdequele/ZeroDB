//! Test reader tracking

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing reader tracking...");

    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Insert some data
    {
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            db.put(&mut txn, format!("key_{}", i.to_string()), format!("value_{}", i.to_string()))?;
        }
        txn.commit()?;
    }

    // Create a long-running reader in another thread
    let env_clone = env.clone();
    let reader_handle = thread::spawn(move || {
        println!("\nReader thread: Starting read transaction...");
        let txn = env_clone.read_txn().unwrap();

        // Reader is active
        println!("Reader thread: Read transaction active");

        // Hold the transaction for a while
        thread::sleep(Duration::from_secs(2));

        println!("Reader thread: Ending read transaction");
        drop(txn);
    });

    // Give the reader thread time to start
    thread::sleep(Duration::from_millis(500));

    // Try to create write transactions that would reuse pages
    println!("\nMain thread: Creating write transactions...");
    for i in 0..5 {
        let mut txn = env.write_txn()?;

        // The reader tracking happens internally
        println!("Main thread: Writing transaction {}", i.to_string());

        // Delete and re-insert to generate free pages
        let key = format!("key_{}", i.to_string());
        db.delete(&mut txn, &key)?;
        db.put(&mut txn, key, format!("new_value_{}", i.to_string()))?;

        txn.commit()?;

        thread::sleep(Duration::from_millis(200));
    }

    // Wait for reader thread to finish
    reader_handle.join().unwrap();

    // Now pages can be reused
    println!("\nAfter reader finished - pages can now be safely reused");

    // Verify data integrity
    println!("\nVerifying data:");
    {
        let txn = env.read_txn()?;
        for i in 0..10 {
            let key = format!("key_{}", i.to_string());
            match db.get(&txn, &key)? {
                Some(value) => {
                    let expected =
                        if i < 5 { format!("new_value_{}", i.to_string()) } else { format!("value_{}", i.to_string()) };
                    if value == expected {
                        println!("  ✓ {} = {}", key, value);
                    } else {
                        println!("  ✗ {} = {} (expected {})", key, value, expected);
                    }
                }
                None => println!("  ✗ {} not found", key),
            }
        }
    }

    println!("\nReader tracking test completed!");
    Ok(())
}
