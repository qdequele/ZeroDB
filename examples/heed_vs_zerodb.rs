// API Comparison between heed and ZeroDB
// This shows how to migrate common operations

// Common imports
use std::fs;
use std::path::Path;

#[allow(dead_code)]
mod heed_style {
    
    // This is how you'd write code with heed
    pub fn example() -> Result<(), Box<dyn std::error::Error>> {
        // heed style
        /*
        let path = Path::new("target/heed.mdb");
        fs::create_dir_all(&path)?;
        
        // Environment creation
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024 * 1024) // 10GB
                .max_dbs(10)
                .open(path)?
        };
        
        // Create database
        let mut wtxn = env.write_txn()?;
        let db: heed::Database<heed::types::Str, heed::types::Bytes> = 
            env.create_database(&mut wtxn, Some("my-db"))?;
        wtxn.commit()?;
        
        // Write data
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, "hello", b"world")?;
        wtxn.commit()?;
        
        // Read data
        let rtxn = env.read_txn()?;
        let value = db.get(&rtxn, "hello")?;
        
        // Iterate
        for result in db.iter(&rtxn)? {
            let (key, value) = result?;
            println!("{}: {:?}", key, value);
        }
        
        // Range iteration
        for result in db.range(&rtxn, "a".."z")? {
            let (key, value) = result?;
            println!("{}: {:?}", key, value);
        }
        */
        
        Ok(())
    }
}

mod zerodb_style {
    use super::*;
    use zerodb::{
        db::{Database, DatabaseFlags},
        env::EnvBuilder,
    };
    
    // Equivalent code in ZeroDB
    pub fn example() -> Result<(), Box<dyn std::error::Error>> {
        let path = Path::new("target/zerodb");
        fs::create_dir_all(path)?;
        
        // Environment creation - no unsafe needed!
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .max_dbs(10)
            .open(path)?;
        
        // Create database - different approach
        let db = Database::<Vec<u8>, Vec<u8>>::open(
            &env,
            Some("my-db"),
            DatabaseFlags::CREATE
        )?;
        
        // Write data - similar but different method name
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, b"hello".to_vec(), b"world".to_vec())?;
        wtxn.commit()?;
        
        // Read data - almost identical
        let rtxn = env.read_txn()?;
        let value = db.get(&rtxn, &b"hello".to_vec())?;
        println!("Value: {:?}", value);
        
        // Iterate - very different approach
        let mut cursor = db.cursor(&rtxn)?;
        while let Some((key, value)) = cursor.next_raw()? {
            println!("{:?}: {} bytes", String::from_utf8_lossy(key), value.len());
        }
        
        // Range iteration - using seek
        let mut cursor = db.cursor(&rtxn)?;
        cursor.seek(&b"a".to_vec())?;
        while let Some((key, value)) = cursor.current()? {
            if key >= b"z".to_vec() {
                break;
            }
            println!("{:?}: {} bytes", String::from_utf8_lossy(&key), value.len());
            cursor.next_raw()?;
        }
        
        Ok(())
    }
}

// Migration helper showing API mappings
#[allow(dead_code)]
mod migration_guide {
    // heed -> ZeroDB mappings:
    
    // env.read_txn()          -> env.read_txn()
    // env.write_txn()         -> env.write_txn()
    // env.create_database()   -> Database::open()
    // db.put()                -> db.put() (same!)
    // db.get()                -> db.get() (same!)
    // db.delete()             -> db.delete() (same!)
    // db.clear()              -> db.clear() (same!)
    // db.iter()               -> db.cursor() + cursor.next_raw()
    // db.range()              -> db.cursor() + cursor.seek()
    // db.first()              -> cursor.first()
    // db.last()               -> cursor.last()
    
    // Type differences:
    // heed::types::Str        -> String (with trait impl)
    // heed::types::Bytes      -> Vec<u8> (with trait impl)
    // heed::types::U32<BE>    -> u32 (with custom serialization)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ZeroDB vs heed API Comparison ===\n");
    
    println!("Running ZeroDB example...");
    zerodb_style::example()?;
    
    println!("\n✓ ZeroDB provides similar functionality with a different API");
    println!("  - No unsafe blocks required");
    println!("  - Different method names for transactions");
    println!("  - Cursor-based iteration instead of iterators");
    println!("  - Type system based on traits instead of type wrappers");
    
    Ok(())
}