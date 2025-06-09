use zerodb::{
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};

fn main() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    println!("Testing PageFull issue with sequential inserts...");
    
    // Create environment with large map size
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(&temp_dir)?;
    
    // Create database
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Try to insert 1000 items like the benchmark does
    let mut txn = env.begin_write_txn()?;
    
    for i in 0..1000 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 100]; // 100-byte value
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                if i % 100 == 0 {
                    println!("Inserted {} keys", i);
                }
            }
            Err(e) => {
                println!("Failed at key {}: {:?}", i, e);
                return Err(e.into());
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted 1000 keys!");
    
    // Verify data
    let txn = env.begin_txn()?;
    let mut count = 0;
    let mut cursor = db.cursor(&txn)?;
    while cursor.next()?.is_some() {
        count += 1;
    }
    println!("Database contains {} entries", count);
    
    Ok(())
}