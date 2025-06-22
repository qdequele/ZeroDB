#[cfg(test)]
mod tests {
    use crate::env::EnvBuilder;
    use crate::btree::BTree;
    use crate::comparator::LexicographicComparator;
    use crate::page::PageFlags;
    use crate::meta::DbInfo;
    use tempfile::TempDir;

    #[test]
    fn test_overflow_simple() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        
        // Create initial page
        let (root_id, root_page) = txn.alloc_page(PageFlags::LEAF).unwrap();
        root_page.header.num_keys = 0;
        
        let mut db_info = DbInfo {
            root: root_id,
            leaf_pages: 1,
            ..Default::default()
        };
        let mut root = root_id;
        
        // Save initial database info
        txn.update_db_info(None, db_info).unwrap();
        
        // Create a large value
        let large_value = vec![0xAB; 5000]; // 5KB
        
        println!("Inserting large value...");
        let old = BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key1",
            &large_value,
        ).unwrap();
        assert!(old.is_none());
        
        println!("Inserting small value...");
        // Try to insert a small value
        let result = BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key2",
            b"small",
        );
        
        match result {
            Ok(_) => println!("Small value inserted successfully"),
            Err(e) => println!("Error inserting small value: {:?}", e),
        }
        
        txn.commit().unwrap();
    }
}