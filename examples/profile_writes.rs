//! Profiling binary for write operations.
//!
//! Run with: cargo flamegraph --example profile_writes

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

    // Create database
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("bench"))?;
    wtxn.commit()?;

    // Profile: Sequential writes with commits
    println!("Starting sequential writes (1000 iterations, 100 items each)...");
    for i in 0..1000 {
        let mut wtxn = env.write_txn()?;
        for j in 0..100 {
            let key = format!("key-{:08}-{:04}", i, j);
            let value = vec![0x42u8; 100];
            db.put(&mut wtxn, &key, &value)?;
        }
        wtxn.commit()?;
    }
    println!("Done!");

    Ok(())
}
