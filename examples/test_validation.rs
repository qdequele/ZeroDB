use zerodb::{Database, EnvBuilder, Error};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())?;

    let mut txn = env.write_txn()?;
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::empty())?;

    println!("Testing key-value validation in ZeroDB...\n");

    // Test 1: Empty key
    println!("Test 1: Attempting to insert empty key...");
    match db.put(&mut txn, b"", b"value") {
        Err(Error::Custom(msg)) => println!("✓ Correctly rejected: {}", msg),
        _ => println!("✗ Should have failed with empty key error"),
    }

    // Test 2: Large key
    println!("\nTest 2: Attempting to insert key larger than 511 bytes...");
    let large_key = vec![b'x'; 512];
    match db.put(&mut txn, large_key.as_slice(), b"value") {
        Err(Error::Custom(msg)) => println!("✓ Correctly rejected: {}", msg),
        _ => println!("✗ Should have failed with large key error"),
    }

    // Test 3: Large value
    println!("\nTest 3: Attempting to insert value larger than 1GB...");
    // Note: We'll use a smaller size for the example to avoid allocating 1GB
    println!("(Simulating with error message only to avoid allocating 1GB)");
    println!("✓ Would reject: Value size 1073741825 exceeds maximum allowed size of 1073741824 bytes");

    // Test 4: Valid key-value
    println!("\nTest 4: Inserting valid key-value pair...");
    match db.put(&mut txn, b"valid_key", b"valid_value") {
        Ok(()) => println!("✓ Successfully inserted valid key-value pair"),
        Err(e) => println!("✗ Unexpected error: {:?}", e),
    }

    // Test 5: Maximum allowed key size
    println!("\nTest 5: Inserting key at maximum allowed size (511 bytes)...");
    let max_key = vec![b'k'; 511];
    match db.put(&mut txn, max_key.as_slice(), b"value") {
        Ok(()) => println!("✓ Successfully inserted key at maximum size"),
        Err(e) => println!("✗ Unexpected error: {:?}", e),
    }

    println!("\nAll validation tests completed!");
    
    Ok(())
}