use zerodb::{Database, EnvBuilder, Error};
use tempfile::TempDir;

#[test]
fn test_empty_key_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap();

    let mut txn = env.write_txn().unwrap();
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::empty()).unwrap();

    // Test empty key
    let result = db.put(&mut txn, b"", b"value");
    assert!(result.is_err());
    if let Err(Error::Custom(msg)) = result {
        assert!(msg.contains("Key cannot be empty"));
    } else {
        panic!("Expected Custom error for empty key");
    }
}

#[test]
fn test_large_key_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap();

    let mut txn = env.write_txn().unwrap();
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::empty()).unwrap();

    // Test key that exceeds max size (512 bytes)
    let large_key = vec![b'x'; 512];
    let result = db.put(&mut txn, large_key.as_slice(), b"value");
    assert!(result.is_err());
    if let Err(Error::Custom(msg)) = result {
        assert!(msg.contains("Key size"));
        assert!(msg.contains("exceeds maximum allowed size"));
    } else {
        panic!("Expected Custom error for large key");
    }
}

#[test]
fn test_large_value_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap();

    let mut txn = env.write_txn().unwrap();
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::empty()).unwrap();

    // Test value that exceeds max size (1GB + 1 byte)
    let large_value_size = 1024 * 1024 * 1024 + 1; // 1GB + 1 byte
    let large_value = vec![b'v'; large_value_size];
    let result = db.put(&mut txn, b"key", large_value.as_slice());
    assert!(result.is_err());
    if let Err(Error::Custom(msg)) = result {
        assert!(msg.contains("Value size"));
        assert!(msg.contains("exceeds maximum allowed size"));
    } else {
        panic!("Expected Custom error for large value");
    }
}

#[test]
fn test_valid_key_value() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap();

    let mut txn = env.write_txn().unwrap();
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::empty()).unwrap();

    // Test valid key and value
    let result = db.put(&mut txn, b"valid_key", b"valid_value");
    assert!(result.is_ok(), "Valid key-value pair should be accepted");

    // Test key at max size (511 bytes)
    let max_key = vec![b'k'; 511];
    let result = db.put(&mut txn, max_key.as_slice(), b"value");
    assert!(result.is_ok(), "Key at max size should be accepted");
}

#[test]
fn test_put_dup_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap();

    let mut txn = env.write_txn().unwrap();
    let db: Database<&[u8], &[u8]> = Database::open(&env, None, zerodb::DatabaseFlags::DUP_SORT).unwrap();

    // Test empty key with put_dup
    let result = db.put_dup(&mut txn, b"", b"value");
    assert!(result.is_err());
    if let Err(Error::Custom(msg)) = result {
        assert!(msg.contains("Key cannot be empty"));
    } else {
        panic!("Expected Custom error for empty key in put_dup");
    }

    // Test large key with put_dup
    let large_key = vec![b'x'; 512];
    let result = db.put_dup(&mut txn, large_key.as_slice(), b"value");
    assert!(result.is_err());
    if let Err(Error::Custom(msg)) = result {
        assert!(msg.contains("Key size"));
        assert!(msg.contains("exceeds maximum allowed size"));
    } else {
        panic!("Expected Custom error for large key in put_dup");
    }
}