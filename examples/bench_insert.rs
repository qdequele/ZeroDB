use std::time::Instant;
use zerodb::{EnvOpenOptions, Database};
use zerodb::types::{Str, Bytes};

fn main() -> zerodb::Result<()> {
    let dir = tempfile::tempdir()?;
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(10)
            .open(dir.path())?
    };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("bench"))?;
    wtxn.commit()?;

    // Warm up
    for i in 0..10 {
        let mut wtxn = env.write_txn()?;
        for j in 0..10 {
            let key = format!("warmup-{:04}-{:04}", i, j);
            let value = vec![0x42u8; 100];
            db.put(&mut wtxn, &key, &value)?;
        }
        wtxn.commit()?;
    }

    // Benchmark: 100 transactions x 100 items each
    let iterations = 100;
    let items_per_txn = 100;
    
    let start = Instant::now();
    for i in 0..iterations {
        let mut wtxn = env.write_txn()?;
        for j in 0..items_per_txn {
            let key = format!("key-{:08}-{:04}", i, j);
            let value = vec![0x42u8; 100];
            db.put(&mut wtxn, &key, &value)?;
        }
        wtxn.commit()?;
    }
    let elapsed = start.elapsed();
    
    let total_items = iterations * items_per_txn;
    let items_per_sec = total_items as f64 / elapsed.as_secs_f64();
    let ms_per_txn = elapsed.as_millis() as f64 / iterations as f64;
    
    println!("Total time: {:?}", elapsed);
    println!("Items: {}", total_items);
    println!("Throughput: {:.0} items/sec", items_per_sec);
    println!("Per txn: {:.2} ms", ms_per_txn);

    Ok(())
}
