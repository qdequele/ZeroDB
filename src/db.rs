//! Database operations for ZeroDB.
//!
//! This module provides the main Database struct for interacting with
//! key-value data stored in B+trees.

use std::marker::PhantomData;

use crate::btree::{
    insert_into_branch, insert_into_leaf, CursorOps, CursorState,
    LeafPage, Node, PageBuilder, SearchResult,
};
use crate::error::{Error, Result};
use crate::flags::DatabaseFlags;
use crate::page::{DbInfo, PageNo};

/// A database handle within an environment.
///
/// Databases are opened once and can be used across multiple transactions.
/// Each database is a separate B+tree within the same environment.
#[derive(Debug, Clone, Copy)]
pub struct Database {
    /// Database index (handle).
    pub(crate) dbi: u32,
    /// Database flags.
    pub(crate) flags: DatabaseFlags,
}

impl Database {
    /// Creates a new database handle.
    pub(crate) fn new(dbi: u32, flags: DatabaseFlags) -> Self {
        Self { dbi, flags }
    }

    /// Returns the database index.
    pub fn dbi(&self) -> u32 {
        self.dbi
    }

    /// Returns the database flags.
    pub fn flags(&self) -> DatabaseFlags {
        self.flags
    }
}

/// A cursor for iterating over database entries.
pub struct RoCursor<'txn> {
    /// Cursor state.
    state: CursorState,
    /// Page size.
    page_size: usize,
    /// Phantom data for lifetime.
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> RoCursor<'txn> {
    /// Creates a new read-only cursor.
    pub(crate) fn new(root: PageNo, page_size: usize) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            _marker: PhantomData,
        }
    }

    /// Positions the cursor at the first key.
    pub fn first(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        CursorOps::first(&mut self.state, self.page_size, get_page)
    }

    /// Positions the cursor at the last key.
    pub fn last(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        CursorOps::last(&mut self.state, self.page_size, get_page)
    }

    /// Moves to the next entry.
    pub fn next(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        CursorOps::next(&mut self.state, self.page_size, get_page)
    }

    /// Moves to the previous entry.
    pub fn prev(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        CursorOps::prev(&mut self.state, self.page_size, get_page)
    }

    /// Seeks to a key.
    pub fn seek(&mut self, key: &[u8], get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        let result = CursorOps::search(&mut self.state, key, self.page_size, get_page)?;
        Ok(result.is_found())
    }

    /// Seeks to a key or the next greater key.
    pub fn seek_range(&mut self, key: &[u8], get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        CursorOps::search(&mut self.state, key, self.page_size, get_page)?;
        Ok(self.state.is_valid())
    }

    /// Returns the current key and value.
    pub fn current<'a>(&self, page_data: &'a [u8]) -> Result<Option<(&'a [u8], &'a [u8])>> {
        CursorOps::get_current(&self.state, page_data, self.page_size)
    }

    /// Returns the current leaf page number.
    pub fn leaf_pgno(&self) -> Option<PageNo> {
        self.state.leaf_pgno()
    }

    /// Returns true if positioned at a valid entry.
    pub fn is_valid(&self) -> bool {
        self.state.is_valid()
    }
}

/// A writable cursor for iterating and modifying database entries.
pub struct RwCursor<'txn> {
    /// Inner read cursor.
    inner: RoCursor<'txn>,
}

impl<'txn> RwCursor<'txn> {
    /// Creates a new read-write cursor.
    pub(crate) fn new(root: PageNo, page_size: usize) -> Self {
        Self {
            inner: RoCursor::new(root, page_size),
        }
    }

    /// Positions the cursor at the first key.
    pub fn first(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        self.inner.first(get_page)
    }

    /// Positions the cursor at the last key.
    pub fn last(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        self.inner.last(get_page)
    }

    /// Moves to the next entry.
    pub fn next(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        self.inner.next(get_page)
    }

    /// Moves to the previous entry.
    pub fn prev(&mut self, get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        self.inner.prev(get_page)
    }

    /// Seeks to a key.
    pub fn seek(&mut self, key: &[u8], get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<bool> {
        self.inner.seek(key, get_page)
    }

    /// Returns the current key and value.
    pub fn current<'a>(&self, page_data: &'a [u8]) -> Result<Option<(&'a [u8], &'a [u8])>> {
        self.inner.current(page_data)
    }

    /// Returns true if positioned at a valid entry.
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}

/// Operations on a database within a read transaction.
pub struct DbReader<'db, 'txn> {
    /// Database handle.
    db: &'db Database,
    /// Database info.
    pub(crate) db_info: DbInfo,
    /// Page size.
    page_size: usize,
    /// Phantom marker for transaction lifetime.
    _marker: PhantomData<&'txn ()>,
}

impl<'db, 'txn> DbReader<'db, 'txn> {
    /// Creates a new database reader.
    pub(crate) fn new(db: &'db Database, db_info: DbInfo, page_size: usize) -> Self {
        Self {
            db,
            db_info,
            page_size,
            _marker: PhantomData,
        }
    }

    /// Gets a value by key.
    pub fn get(&self, key: &[u8], get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        if self.db_info.root == 0 {
            return Ok(None);
        }

        let mut state = CursorState::new(self.db_info.root);
        let result = CursorOps::search(&mut state, key, self.page_size, &get_page)?;

        match result {
            SearchResult::Found(_) => {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, value)) = CursorOps::get_current(&state, &page_data, self.page_size)? {
                        return Ok(Some(value.to_vec()));
                    }
                }
                Ok(None)
            }
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Returns the database info.
    pub fn info(&self) -> &DbInfo {
        &self.db_info
    }

    /// Opens a read cursor.
    pub fn cursor(&self) -> RoCursor<'txn> {
        RoCursor::new(self.db_info.root, self.page_size)
    }
}

/// Operations on a database within a write transaction.
pub struct DbWriter<'db, 'txn> {
    /// Database handle.
    db: &'db Database,
    /// Database info (mutable for updates).
    pub(crate) db_info: DbInfo,
    /// Page size.
    page_size: usize,
    /// Phantom marker for transaction lifetime.
    _marker: PhantomData<&'txn mut ()>,
}

impl<'db, 'txn> DbWriter<'db, 'txn> {
    /// Creates a new database writer.
    pub(crate) fn new(db: &'db Database, db_info: DbInfo, page_size: usize) -> Self {
        Self {
            db,
            db_info,
            page_size,
            _marker: PhantomData,
        }
    }

    /// Gets a value by key.
    pub fn get(&self, key: &[u8], get_page: impl Fn(PageNo) -> Result<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        if self.db_info.root == 0 {
            return Ok(None);
        }

        let mut state = CursorState::new(self.db_info.root);
        let result = CursorOps::search(&mut state, key, self.page_size, &get_page)?;

        match result {
            SearchResult::Found(_) => {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, value)) = CursorOps::get_current(&state, &page_data, self.page_size)? {
                        return Ok(Some(value.to_vec()));
                    }
                }
                Ok(None)
            }
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Puts a key-value pair into the database.
    ///
    /// Returns the new root page number and whether a new page was allocated.
    pub fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
        alloc_page: impl FnMut() -> Result<PageNo>,
        set_page: impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        // Validate key size
        if key.len() > crate::MAX_KEY_SIZE {
            return Err(Error::BadValSize);
        }

        if self.db_info.root == 0 {
            // Empty tree - create root leaf
            self.create_root_leaf(key, value, alloc_page, set_page)?;
        } else {
            // Insert into existing tree
            self.insert_into_tree(key, value, get_page, alloc_page, set_page)?;
        }

        Ok(())
    }

    /// Creates a new root leaf page for an empty tree.
    fn create_root_leaf(
        &mut self,
        key: &[u8],
        value: &[u8],
        mut alloc_page: impl FnMut() -> Result<PageNo>,
        mut set_page: impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let pgno = alloc_page()?;

        let mut builder = PageBuilder::new_leaf(pgno, self.page_size);
        builder.add_leaf(&Node::leaf(key.to_vec(), value.to_vec()))?;
        set_page(pgno, builder.finish())?;

        self.db_info.root = pgno;
        self.db_info.entries += 1;
        self.db_info.leaf_pages += 1;
        self.db_info.depth = 1;

        Ok(())
    }

    /// Inserts into an existing tree.
    fn insert_into_tree(
        &mut self,
        key: &[u8],
        value: &[u8],
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
        mut alloc_page: impl FnMut() -> Result<PageNo>,
        mut set_page: impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut state = CursorState::new(self.db_info.root);
        let result = CursorOps::search(&mut state, key, self.page_size, &get_page)?;

        // Get the insert position
        let insert_index = result.index();
        let is_update = result.is_found();

        // Get the leaf page
        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let leaf_data = get_page(leaf_pgno)?;

        // Create the new node
        let new_node = Node::leaf(key.to_vec(), value.to_vec());

        // If updating existing key, we need to replace rather than insert
        if is_update {
            // For now, we'll just do a simple insert-at-position which handles updates
            // by rebuilding the page without the old entry
            let (new_leaf_data, split) = self.insert_at_leaf_update(
                &leaf_data,
                new_node,
                insert_index,
                leaf_pgno,
            )?;

            set_page(leaf_pgno, new_leaf_data)?;

            if let Some((right_data, separator)) = split {
                let right_pgno = alloc_page()?;
                set_page(right_pgno, self.set_page_no(&right_data, right_pgno))?;
                self.db_info.leaf_pages += 1;

                // Propagate split up
                self.propagate_split(
                    &state,
                    separator,
                    right_pgno,
                    &get_page,
                    &mut alloc_page,
                    &mut set_page,
                )?;
            }
        } else {
            // New key - insert
            let (new_leaf_data, split) = insert_into_leaf(
                &leaf_data,
                new_node,
                insert_index,
                leaf_pgno,
                self.page_size,
            )?;

            set_page(leaf_pgno, new_leaf_data)?;
            self.db_info.entries += 1;

            if let Some((right_data, separator)) = split {
                let right_pgno = alloc_page()?;
                set_page(right_pgno, self.set_page_no(&right_data, right_pgno))?;
                self.db_info.leaf_pages += 1;

                // Propagate split up
                self.propagate_split(
                    &state,
                    separator,
                    right_pgno,
                    &get_page,
                    &mut alloc_page,
                    &mut set_page,
                )?;
            }
        }

        Ok(())
    }

    /// Inserts at a leaf position, handling updates.
    fn insert_at_leaf_update(
        &self,
        page_data: &[u8],
        new_node: Node,
        update_index: usize,
        page_no: PageNo,
    ) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
        let page = LeafPage::new(page_data, self.page_size)?;
        let num_keys = page.num_keys();

        // Collect all nodes, replacing the one at update_index
        let mut nodes: Vec<Node> = Vec::with_capacity(num_keys);

        for i in 0..num_keys {
            if i == update_index {
                nodes.push(new_node.clone());
            } else {
                let node_ref = page.node(i)?;
                nodes.push(Node::leaf(
                    node_ref.key().to_vec(),
                    node_ref.value().to_vec(),
                ));
            }
        }

        // Try to fit all nodes in one page
        let mut builder = PageBuilder::new_leaf(page_no, self.page_size);
        let mut fit_count = 0;

        for node in &nodes {
            if builder.can_fit(node.size()) {
                builder.add_leaf(node)?;
                fit_count += 1;
            } else {
                break;
            }
        }

        if fit_count == nodes.len() {
            return Ok((builder.finish(), None));
        }

        // Need to split
        let split_point = nodes.len() / 2;
        let split_point = split_point.max(1);

        let mut left_builder = PageBuilder::new_leaf(page_no, self.page_size);
        for node in &nodes[..split_point] {
            left_builder.add_leaf(node)?;
        }

        let mut right_builder = PageBuilder::new_leaf(0, self.page_size);
        for node in &nodes[split_point..] {
            right_builder.add_leaf(node)?;
        }

        Ok((
            left_builder.finish(),
            Some((right_builder.finish(), nodes[split_point].key.clone())),
        ))
    }

    /// Sets the page number in page data.
    fn set_page_no(&self, data: &[u8], pgno: PageNo) -> Vec<u8> {
        let mut result = data.to_vec();
        result[0..8].copy_from_slice(&pgno.to_le_bytes());
        result
    }

    /// Propagates a split up the tree.
    fn propagate_split(
        &mut self,
        state: &CursorState,
        mut separator: Vec<u8>,
        mut right_pgno: PageNo,
        get_page: &impl Fn(PageNo) -> Result<Vec<u8>>,
        alloc_page: &mut impl FnMut() -> Result<PageNo>,
        set_page: &mut impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        // Work up the tree from leaf to root
        let stack_len = state.stack.len();

        for level in (0..stack_len.saturating_sub(1)).rev() {
            let parent = &state.stack[level];
            let parent_data = get_page(parent.page_no)?;

            let (new_parent_data, split) = insert_into_branch(
                &parent_data,
                separator.clone(),
                right_pgno,
                parent.index,
                parent.page_no,
                self.page_size,
            )?;

            set_page(parent.page_no, new_parent_data)?;

            match split {
                Some((new_right_data, new_separator)) => {
                    right_pgno = alloc_page()?;
                    set_page(right_pgno, self.set_page_no(&new_right_data, right_pgno))?;
                    separator = new_separator;
                    self.db_info.branch_pages += 1;
                }
                None => return Ok(()),
            }
        }

        // Need a new root
        self.create_new_root(separator, right_pgno, alloc_page, set_page)?;

        Ok(())
    }

    /// Creates a new root page when the old root splits.
    fn create_new_root(
        &mut self,
        separator: Vec<u8>,
        right_pgno: PageNo,
        alloc_page: &mut impl FnMut() -> Result<PageNo>,
        set_page: &mut impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let new_root_pgno = alloc_page()?;

        let mut builder = PageBuilder::new_branch(new_root_pgno, self.page_size);

        // Left child (old root)
        builder.add_branch(&Node::branch(Vec::new(), self.db_info.root))?;
        // Right child (new split page)
        builder.add_branch(&Node::branch(separator, right_pgno))?;

        set_page(new_root_pgno, builder.finish())?;

        self.db_info.root = new_root_pgno;
        self.db_info.depth += 1;
        self.db_info.branch_pages += 1;

        Ok(())
    }

    /// Deletes a key from the database.
    pub fn delete(
        &mut self,
        key: &[u8],
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
        set_page: impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<bool> {
        if self.db_info.root == 0 {
            return Ok(false);
        }

        let mut state = CursorState::new(self.db_info.root);
        let result = CursorOps::search(&mut state, key, self.page_size, &get_page)?;

        if !result.is_found() {
            return Ok(false);
        }

        // Get the leaf page and remove the entry
        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let delete_index = result.index();

        self.delete_from_leaf(leaf_pgno, delete_index, get_page, set_page)?;
        self.db_info.entries -= 1;

        Ok(true)
    }

    /// Deletes an entry from a leaf page.
    fn delete_from_leaf(
        &mut self,
        pgno: PageNo,
        index: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
        mut set_page: impl FnMut(PageNo, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let page_data = get_page(pgno)?;
        let page = LeafPage::new(&page_data, self.page_size)?;
        let num_keys = page.num_keys();

        // Rebuild page without the deleted entry
        let mut builder = PageBuilder::new_leaf(pgno, self.page_size);

        for i in 0..num_keys {
            if i != index {
                let node_ref = page.node(i)?;
                builder.add_leaf(&Node::leaf(
                    node_ref.key().to_vec(),
                    node_ref.value().to_vec(),
                ))?;
            }
        }

        set_page(pgno, builder.finish())?;

        // Note: For simplicity, we don't handle underflow/merging yet
        // A full implementation would check if the page is underfull
        // and merge with siblings or redistribute keys

        Ok(())
    }

    /// Returns the database info.
    pub fn info(&self) -> &DbInfo {
        &self.db_info
    }

    /// Opens a write cursor.
    pub fn cursor(&self) -> RwCursor<'txn> {
        RwCursor::new(self.db_info.root, self.page_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// Test helper that provides page storage with RefCell for interior mutability.
    struct TestPageStore {
        pages: RefCell<HashMap<PageNo, Vec<u8>>>,
        next_pgno: RefCell<PageNo>,
    }

    impl TestPageStore {
        fn new() -> Self {
            Self {
                pages: RefCell::new(HashMap::new()),
                next_pgno: RefCell::new(2), // Start after meta pages
            }
        }

        fn get(&self, pgno: PageNo) -> Result<Vec<u8>> {
            self.pages
                .borrow()
                .get(&pgno)
                .cloned()
                .ok_or(Error::Corrupted)
        }

        fn alloc(&self) -> Result<PageNo> {
            let mut next = self.next_pgno.borrow_mut();
            let pgno = *next;
            *next += 1;
            Ok(pgno)
        }

        fn set(&self, pgno: PageNo, data: Vec<u8>) -> Result<()> {
            self.pages.borrow_mut().insert(pgno, data);
            Ok(())
        }
    }

    #[test]
    fn database_basic_operations() {
        let page_size = 4096;
        let store = TestPageStore::new();

        let db = Database::new(0, DatabaseFlags::empty());
        let db_info = DbInfo::default();

        let mut writer = DbWriter::new(&db, db_info, page_size);

        // Put first key
        writer.put(
            b"key1",
            b"value1",
            |pgno| store.get(pgno),
            || store.alloc(),
            |pgno, data| store.set(pgno, data),
        ).unwrap();

        assert_eq!(writer.db_info.entries, 1);
        assert_eq!(writer.db_info.leaf_pages, 1);

        // Put second key
        writer.put(
            b"key2",
            b"value2",
            |pgno| store.get(pgno),
            || store.alloc(),
            |pgno, data| store.set(pgno, data),
        ).unwrap();

        assert_eq!(writer.db_info.entries, 2);

        // Get keys
        let value = writer.get(b"key1", |pgno| store.get(pgno)).unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        let value = writer.get(b"key2", |pgno| store.get(pgno)).unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));

        // Get non-existent key
        let value = writer.get(b"key3", |pgno| store.get(pgno)).unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn database_update_value() {
        let page_size = 4096;
        let store = TestPageStore::new();

        let db = Database::new(0, DatabaseFlags::empty());
        let db_info = DbInfo::default();

        let mut writer = DbWriter::new(&db, db_info, page_size);

        // Put key
        writer.put(
            b"key",
            b"value1",
            |pgno| store.get(pgno),
            || store.alloc(),
            |pgno, data| store.set(pgno, data),
        ).unwrap();

        // Update key
        writer.put(
            b"key",
            b"value2",
            |pgno| store.get(pgno),
            || store.alloc(),
            |pgno, data| store.set(pgno, data),
        ).unwrap();

        // Entry count should not increase
        assert_eq!(writer.db_info.entries, 1);

        // Value should be updated
        let value = writer.get(b"key", |pgno| store.get(pgno)).unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn database_delete() {
        let page_size = 4096;
        let store = TestPageStore::new();

        let db = Database::new(0, DatabaseFlags::empty());
        let db_info = DbInfo::default();

        let mut writer = DbWriter::new(&db, db_info, page_size);

        // Put keys
        for i in 0..3 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            writer.put(
                key.as_bytes(),
                value.as_bytes(),
                |pgno| store.get(pgno),
                || store.alloc(),
                |pgno, data| store.set(pgno, data),
            ).unwrap();
        }

        assert_eq!(writer.db_info.entries, 3);

        // Delete middle key
        let deleted = writer.delete(
            b"key1",
            |pgno| store.get(pgno),
            |pgno, data| store.set(pgno, data),
        ).unwrap();
        assert!(deleted);
        assert_eq!(writer.db_info.entries, 2);

        // Verify deleted
        let value = writer.get(b"key1", |pgno| store.get(pgno)).unwrap();
        assert!(value.is_none());

        // Other keys still exist
        let value = writer.get(b"key0", |pgno| store.get(pgno)).unwrap();
        assert_eq!(value, Some(b"value0".to_vec()));

        let value = writer.get(b"key2", |pgno| store.get(pgno)).unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn database_many_keys() {
        let page_size = 4096;
        let store = TestPageStore::new();

        let db = Database::new(0, DatabaseFlags::empty());
        let db_info = DbInfo::default();

        let mut writer = DbWriter::new(&db, db_info, page_size);

        // Insert many keys to trigger splits
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            writer.put(
                key.as_bytes(),
                value.as_bytes(),
                |pgno| store.get(pgno),
                || store.alloc(),
                |pgno, data| store.set(pgno, data),
            ).unwrap();
        }

        assert_eq!(writer.db_info.entries, 100);

        // Verify all keys exist
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let expected = format!("value{:03}", i);
            let value = writer.get(key.as_bytes(), |pgno| store.get(pgno)).unwrap();
            assert_eq!(value, Some(expected.into_bytes()), "Key {} not found", key);
        }
    }
}
