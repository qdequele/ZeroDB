//! Simple example of using zerodb

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{EnvBuilder, Result};

fn main() -> Result<()> {
    // Create temporary directory
    let dir = TempDir::new().unwrap();

    // Open environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())?,
    );

    println!("Environment opened at: {:?}", dir.path());

    // Create a database
    let db = {
        let mut txn = env.begin_write_txn()?;
        let db: zerodb::db::Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        println!("Database created");
        db
    };

    // Insert some data
    {
        let mut txn = env.begin_write_txn()?;
        db.put(&mut txn, "hello".to_string(), "world".to_string())?;
        db.put(&mut txn, "foo".to_string(), "bar".to_string())?;
        txn.commit()?;
        println!("Data inserted");
    }

    // Read the data back
    {
        let txn = env.begin_txn()?;

        let val1 = db.get(&txn, &"hello".to_string())?;
        println!("hello => {:?}", val1);

        let val2 = db.get(&txn, &"foo".to_string())?;
        println!("foo => {:?}", val2);

        let val3 = db.get(&txn, &"missing".to_string())?;
        println!("missing => {:?}", val3);
    }

    Ok(())
}
