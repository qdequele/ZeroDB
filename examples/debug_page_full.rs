use zerodb::{
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};

fn main() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    println!("Debugging PageFull issue...");
    
    // Create environment - match benchmark settings
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB like benchmark
        .open(&temp_dir)?;
    
    // Create database
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Generate data exactly like the benchmark
    let mut data = Vec::new();
    for i in 0..1000 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 150]; // Average of 50-200 bytes
        data.push((key, value));
    }
    
    println!("Starting batch insert of {} items", data.len());
    
    // Insert in a single transaction like the benchmark
    let mut txn = env.begin_write_txn()?;
    
    for (i, (key, value)) in data.iter().enumerate() {
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i % 50 == 0 && i > 0 {
                    println!("Progress: {} keys inserted", i);
                }
            }
            Err(e) => {
                println!("\nFailed at key {} (index {}): {:?}", 
                    String::from_utf8_lossy(key), i, e);
                
                // Debug info
                println!("Transaction state at failure:");
                println!("  Failed key index: {}", i);
                println!("  Keys successfully inserted: {}", i);
                
                return Err(e.into());
            }
        }
    }
    
    txn.commit()?;
    println!("\nSuccessfully inserted all {} keys!", data.len());
    
    Ok(())
}