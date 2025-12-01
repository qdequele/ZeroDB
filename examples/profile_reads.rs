//! Profiling binary for read operations.
//!
//! Run with: cargo flamegraph --example profile_reads

use zerodb::{Database, EnvOpenOptions};
use zerodb::types::{Str, Bytes};

fn main() -> zerodb::Result<()> {
    let dir = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .max_dbs(10)
            .open(dir.path())?
    };

    // Create database and populate
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("bench"))?;

    println!("Populating database with 100,000 entries...");
    for i in 0..100_000 {
        let key = format!("key-{:08}", i);
        let value = vec![0x42u8; 100];
        db.put(&mut wtxn, &key, &value)?;
    }
    wtxn.commit()?;
    println!("Population complete!");

    // Profile: Point lookups
    println!("Starting point lookups (1,000,000 iterations)...");
    let rtxn = env.read_txn()?;
    for i in 0..1_000_000 {
        let key = format!("key-{:08}", i % 100_000);
        let _ = db.get(&rtxn, &key)?;
    }
    drop(rtxn);
    println!("Point lookups done!");

    // Profile: Full iteration
    println!("Starting full iteration (10 times)...");
    for _ in 0..10 {
        let rtxn = env.read_txn()?;
        let mut count = 0;
        for result in db.iter(&rtxn)? {
            let _ = result?;
            count += 1;
        }
        assert_eq!(count, 100_000);
    }
    println!("Iteration done!");

    Ok(())
}
