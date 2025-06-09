//! Example showing heed-compatible iterator API

use zerodb::{
    cursor_iter,
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};

fn main() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let env = EnvBuilder::new().open(&temp_dir)?;
    
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Insert some test data
    let mut txn = env.write_txn()?;
    for i in 0..20 {
        let key = format!("key_{:02}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        db.put(&mut txn, key, value)?;
    }
    txn.commit()?;
    
    println!("=== ZeroDB with heed-style iterators ===\n");
    
    let txn = env.read_txn()?;
    
    // 1. Basic iteration (like heed's db.iter())
    println!("1. Basic iteration:");
    for result in cursor_iter::iter(&db, &txn)? {
        let (key, value) = result?;
        println!("  {:?}: {} bytes", String::from_utf8_lossy(&key), value.len());
        if key == b"key_04".to_vec() { break; } // Just show first 5
    }
    
    // 2. Using iterator methods
    println!("\n2. Using iterator adapters:");
    let count = cursor_iter::iter(&db, &txn)?
        .filter_map(|r| r.ok())
        .filter(|(k, _)| k.ends_with(&vec![b'5']))
        .count();
    println!("  Keys ending with '5': {}", count);
    
    // 3. Collecting into Vec
    println!("\n3. Collecting results:");
    let items: Vec<_> = cursor_iter::iter(&db, &txn)?
        .take(3)
        .collect::<Result<Vec<_>, _>>()?;
    for (key, value) in items {
        println!("  {:?}: {:?}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
    }
    
    // 4. Range iteration
    println!("\n4. Range iteration:");
    let start = b"key_10".to_vec();
    let end = b"key_15".to_vec();
    for result in cursor_iter::range(&db, &txn, &start..&end)? {
        let (key, value) = result?;
        println!("  {:?}: {} bytes", String::from_utf8_lossy(&key), value.len());
    }
    
    // 5. Find specific item
    println!("\n5. Finding specific item:");
    let target = b"key_12".to_vec();
    let found = cursor_iter::iter(&db, &txn)?
        .find(|r| r.as_ref().map(|(k, _)| k == &target).unwrap_or(false));
    match found {
        Some(Ok((key, value))) => println!("  Found: {:?} = {:?}", 
            String::from_utf8_lossy(&key), 
            String::from_utf8_lossy(&value)),
        Some(Err(e)) => println!("  Error: {}", e),
        None => println!("  Not found"),
    }
    
    println!("\n✓ ZeroDB now supports heed-style iteration patterns!");
    
    Ok(())
}

// This shows how the API can be nearly identical to heed:
#[allow(dead_code)]
fn heed_style_code_example() {
    // With our iterator support, this code looks almost identical to heed:
    /*
    // heed style:
    for result in db.iter(&rtxn)? {
        let (key, value) = result?;
        process(key, value);
    }
    
    // ZeroDB with iterators:
    for result in db.iter(&rtxn)? {
        let (key, value) = result?;
        process(key, value);
    }
    
    // Range queries are also similar:
    // heed: db.range(&rtxn, "a".."z")?
    // ZeroDB: db.range(&rtxn, Some(&"a"), Some("z"))?
    */
}