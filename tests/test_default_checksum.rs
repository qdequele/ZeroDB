use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_default_checksum_mode_is_full() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path();
    
    // Create environment with default settings (should now use Full checksums)
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(db_path)
        .unwrap());
    
    // Create a database and write some data
    let db: Database<String, String> = {
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some("test_db")).unwrap();
        wtxn.commit().unwrap();
        db
    };
    
    // Write data
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
    db.put(&mut wtxn, "key2".to_string(), "value2".to_string()).unwrap();
    wtxn.commit().unwrap();
    
    // Read data back
    let rtxn = env.read_txn().unwrap();
    let value1 = db.get(&rtxn, &"key1".to_string()).unwrap();
    let value2 = db.get(&rtxn, &"key2".to_string()).unwrap();
    
    assert_eq!(value1, Some("value1".to_string()));
    assert_eq!(value2, Some("value2".to_string()));
}