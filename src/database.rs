//! Heed-compatible typed database API.
//!
//! This module provides a typed `Database<KC, DC>` wrapper that uses
//! `BytesEncode` and `BytesDecode` traits for type-safe key-value storage.

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

use crate::btree::{CursorOps, CursorState, LeafPage, Node, PageBuilder, SearchResult};
use crate::btree::{insert_into_branch, insert_into_leaf};
use crate::env::{Env, DefaultComparator};
use crate::error::{Error, Result};
use crate::flags::DatabaseFlags;
use crate::page::{DbInfo, PageNo};
use crate::types::{BytesEncode, OwnedDecode};
use crate::txn::{RoTxn, RwTxn};

// ============================================================================
// Unspecified type marker
// ============================================================================

/// Marker type for unspecified key or data types.
///
/// This is used in `DatabaseOpenOptions` before types are specified.
/// A database with `Unspecified` types requires a call to `remap_types`
/// before it can be used.
#[derive(Debug, Clone, Copy)]
pub enum Unspecified {}

// ============================================================================
// DatabaseOpenOptions Builder
// ============================================================================

/// Options and flags which can be used to configure how a [`Database`] is opened.
///
/// # Example
///
/// ```ignore
/// use zerodb::EnvOpenOptions;
/// use zerodb::types::*;
///
/// let env = unsafe { EnvOpenOptions::new().open(dir.path())? };
///
/// // Imagine you have an optional name
/// let conditional_name = Some("my-database");
///
/// let mut wtxn = env.write_txn()?;
/// let mut options = env.database_options().types::<Str, U32>();
/// if let Some(name) = conditional_name {
///    options.name(name);
/// }
/// let db = options.create(&mut wtxn)?;
///
/// db.put(&mut wtxn, "hello", &42)?;
/// wtxn.commit()?;
/// ```
pub struct DatabaseOpenOptions<'e, 'n, KC, DC, C = DefaultComparator> {
    env: &'e Env,
    _types: PhantomData<(KC, DC, C)>,
    name: Option<&'n str>,
    flags: DatabaseFlags,
}

impl<KC, DC, C> std::fmt::Debug for DatabaseOpenOptions<'_, '_, KC, DC, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseOpenOptions")
            .field("name", &self.name)
            .field("flags", &self.flags)
            .finish_non_exhaustive()
    }
}

impl<'e> DatabaseOpenOptions<'e, 'static, Unspecified, Unspecified, DefaultComparator> {
    /// Create an options struct to open/create a database with specific flags.
    pub fn new(env: &'e Env) -> Self {
        DatabaseOpenOptions {
            env,
            _types: PhantomData,
            name: None,
            flags: DatabaseFlags::empty(),
        }
    }
}

impl<'e, 'n, KC, DC, C> DatabaseOpenOptions<'e, 'n, KC, DC, C> {
    /// Change the type of the database.
    ///
    /// The default types are [`Unspecified`] and require a call to [`Database::remap_types`]
    /// to use the [`Database`].
    pub fn types<NKC, NDC>(self) -> DatabaseOpenOptions<'e, 'n, NKC, NDC, C> {
        DatabaseOpenOptions {
            env: self.env,
            _types: PhantomData,
            name: self.name,
            flags: self.flags,
        }
    }

    /// Change the customized key compare function of the database.
    ///
    /// By default no customized compare function will be set when opening a database.
    pub fn key_comparator<NC>(self) -> DatabaseOpenOptions<'e, 'n, KC, DC, NC> {
        DatabaseOpenOptions {
            env: self.env,
            _types: PhantomData,
            name: self.name,
            flags: self.flags,
        }
    }

    /// Change the name of the database.
    ///
    /// By default the database is unnamed and there only is a single unnamed database.
    pub fn name(&mut self, name: &'n str) -> &mut Self {
        self.name = Some(name);
        self
    }

    /// Specify the set of flags used to open the database.
    pub fn flags(&mut self, flags: DatabaseFlags) -> &mut Self {
        self.flags = flags;
        self
    }

    /// Opens a typed database that already exists in this environment.
    ///
    /// If the database was previously opened in this program run, types will be checked.
    ///
    /// ## Important Information
    ///
    /// LMDB has an important restriction on the unnamed database when named ones are opened.
    /// The names of the named databases are stored as keys in the unnamed one and are immutable,
    /// and these keys can only be read and not written.
    pub fn open<'txn>(&self, rtxn: &'txn RoTxn<'_>) -> Result<Option<Database<KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        self.env.open_database::<KC, DC>(rtxn, self.name)
    }

    /// Creates a typed database that can already exist in this environment.
    ///
    /// If the database was previously opened in this program run, types will be checked.
    ///
    /// ## Important Information
    ///
    /// LMDB has an important restriction on the unnamed database when named ones are opened.
    /// The names of the named databases are stored as keys in the unnamed one and are immutable,
    /// and these keys can only be read and not written.
    pub fn create(&self, wtxn: &mut RwTxn<'_>) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        self.env.create_database::<KC, DC>(wtxn, self.name)
    }
}

impl<KC, DC, C> Clone for DatabaseOpenOptions<'_, '_, KC, DC, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<KC, DC, C> Copy for DatabaseOpenOptions<'_, '_, KC, DC, C> {}

// ============================================================================
// DatabaseStat
// ============================================================================

/// Statistics about a database.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabaseStat {
    /// Depth (height) of the B+tree.
    pub depth: u32,
    /// Number of branch pages.
    pub branch_pages: u64,
    /// Number of leaf pages.
    pub leaf_pages: u64,
    /// Number of overflow pages.
    pub overflow_pages: u64,
    /// Number of data items.
    pub entries: u64,
}

/// Database index type (similar to LMDB's MDB_dbi).
pub type Dbi = u32;

/// A typed database that encodes keys and values using the specified codecs.
///
/// The type parameters `KC` and `DC` specify the key and data codecs respectively.
/// They must implement `BytesEncode` for writing and `BytesDecode` for reading.
///
/// # Example
///
/// ```ignore
/// use zerodb::{Database, EnvOpenOptions};
/// use zerodb::types::{Str, U32};
///
/// let env = unsafe { EnvOpenOptions::new().open(path)? };
/// let mut wtxn = env.write_txn()?;
///
/// // Create a database with string keys and u32 values
/// let db: Database<Str, U32> = env.create_database(&mut wtxn, None)?;
///
/// db.put(&mut wtxn, "hello", &42)?;
/// wtxn.commit()?;
///
/// let rtxn = env.read_txn()?;
/// assert_eq!(db.get(&rtxn, "hello")?, Some(42));
/// ```
pub struct Database<KC, DC> {
    /// Database index.
    dbi: Dbi,
    /// Database name (None for unnamed database).
    name: Option<String>,
    /// Database info (root page, stats, etc.)
    db_info: DbInfo,
    /// Database flags
    flags: DatabaseFlags,
    /// Phantom data for type parameters
    _phantom: PhantomData<(KC, DC)>,
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Self {
        Self {
            dbi: self.dbi,
            name: self.name.clone(),
            db_info: self.db_info,
            flags: self.flags,
            _phantom: PhantomData,
        }
    }
}

impl<KC, DC> Database<KC, DC> {
    /// Creates a new typed database wrapper.
    pub(crate) fn new(dbi: Dbi, name: Option<String>, db_info: DbInfo, flags: DatabaseFlags) -> Self {
        Self {
            dbi,
            name,
            db_info,
            flags,
            _phantom: PhantomData,
        }
    }

    /// Returns the database index.
    pub fn dbi(&self) -> Dbi {
        self.dbi
    }

    /// Returns the database name, or `None` for the unnamed database.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the database flags.
    pub fn flags(&self) -> DatabaseFlags {
        self.flags
    }

    /// Returns the database statistics.
    pub fn stat(&self, _txn: &RoTxn<'_>) -> Result<DatabaseStat> {
        let info = &self.db_info;
        Ok(DatabaseStat {
            depth: info.depth as u32,
            branch_pages: info.branch_pages as u64,
            leaf_pages: info.leaf_pages as u64,
            overflow_pages: info.overflow_pages as u64,
            entries: info.entries as u64,
        })
    }

    /// Returns the number of entries in the database.
    pub fn len(&self, _txn: &RoTxn<'_>) -> Result<u64> {
        Ok(self.db_info.entries as u64)
    }

    /// Returns `true` if the database contains no entries.
    pub fn is_empty(&self, _txn: &RoTxn<'_>) -> Result<bool> {
        Ok(self.db_info.entries == 0)
    }

    /// Remap the types of this database.
    ///
    /// This is useful when you want to work with a database using different codecs.
    pub fn remap_types<KC2, DC2>(&self) -> Database<KC2, DC2> {
        Database {
            dbi: self.dbi,
            name: self.name.clone(),
            db_info: self.db_info,
            flags: self.flags,
            _phantom: PhantomData,
        }
    }

    /// Remap the key type of this database.
    pub fn remap_key_type<KC2>(&self) -> Database<KC2, DC> {
        Database {
            dbi: self.dbi,
            name: self.name.clone(),
            db_info: self.db_info,
            flags: self.flags,
            _phantom: PhantomData,
        }
    }

    /// Remap the data type of this database.
    pub fn remap_data_type<DC2>(&self) -> Database<KC, DC2> {
        Database {
            dbi: self.dbi,
            name: self.name.clone(),
            db_info: self.db_info,
            flags: self.flags,
            _phantom: PhantomData,
        }
    }
}

// Read operations that work with any lifetime
impl<KC, DC> Database<KC, DC> {
    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::OwnedItem>>
    where
        KC: BytesEncode<'a>,
        DC: OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        // Get the most up-to-date db_info from the registry (for named databases)
        let db_info = if self.dbi >= 2 {
            txn.env().get_db_info(self.dbi).unwrap_or(self.db_info)
        } else {
            self.db_info
        };

        if db_info.root == 0 || db_info.root == PageNo::MAX {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        match result {
            SearchResult::Found(_) => {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        // Copy value to owned bytes for decoding
                        return Ok(Some(DC::decode_owned(value)?));
                    }
                }
                Ok(None)
            }
            SearchResult::NotFound(_) => Ok(None),
        }
    }

    /// Returns the first key-value pair in the database.
    pub fn first<'txn>(&self, txn: &'txn RoTxn<'_>) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: OwnedDecode,
        DC: OwnedDecode,
    {
        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        if !CursorOps::first(&mut state, page_size, &get_page)? {
            return Ok(None);
        }

        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((key, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                let key = KC::decode_owned(key)?;
                let value = DC::decode_owned(value)?;
                return Ok(Some((key, value)));
            }
        }

        Ok(None)
    }

    /// Returns the last key-value pair in the database.
    pub fn last<'txn>(&self, txn: &'txn RoTxn<'_>) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: OwnedDecode,
        DC: OwnedDecode,
    {
        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        if !CursorOps::last(&mut state, page_size, &get_page)? {
            return Ok(None);
        }

        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((key, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                let key = KC::decode_owned(key)?;
                let value = DC::decode_owned(value)?;
                return Ok(Some((key, value)));
            }
        }

        Ok(None)
    }

    /// Inserts a key-value pair into the database.
    ///
    /// If the key already exists, the value is updated.
    pub fn put<'a>(
        &self,
        txn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        value: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes = KC::bytes_encode(key)?;
        let value_bytes = DC::bytes_encode(value)?;

        // Validate key size
        if key_bytes.len() > crate::MAX_KEY_SIZE {
            return Err(Error::BadValSize);
        }

        let page_size = txn.env().page_size();

        // Get the most up-to-date db_info from the registry (for named databases)
        let mut db_info = if self.dbi >= 2 {
            // Named database - get current info from registry
            txn.env().get_db_info(self.dbi).unwrap_or(self.db_info)
        } else {
            self.db_info
        };

        // Check if tree is empty (root == 0 for legacy, or PageNo::MAX for P_INVALID)
        if db_info.root == 0 || db_info.root == PageNo::MAX {
            // Empty tree - create root leaf
            let (pgno, data) = txn.alloc_page()?;
            let mut builder = PageBuilder::new_leaf(pgno, page_size);
            builder.add_leaf(&Node::leaf(key_bytes.to_vec(), value_bytes.to_vec()))?;
            data.copy_from_slice(&builder.finish());

            db_info.root = pgno;
            db_info.entries += 1;
            db_info.leaf_pages += 1;
            db_info.depth = 1;
        } else {
            // Insert into existing tree
            self.insert_into_tree(txn, &key_bytes, &value_bytes, &mut db_info)?;
        }

        // Update the registry with the new db_info (for named databases)
        if self.dbi >= 2 {
            txn.env().update_db_info(self.dbi, db_info);
        }

        Ok(())
    }

    /// Internal method to insert into an existing tree.
    fn insert_into_tree(
        &self,
        txn: &mut RwTxn<'_>,
        key: &[u8],
        value: &[u8],
        db_info: &mut DbInfo,
    ) -> Result<()> {
        let page_size = txn.env().page_size();
        let mut state = CursorState::new(db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, key, page_size, &get_page)?;
        let insert_index = result.index();
        let is_update = result.is_found();

        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let leaf_data = txn.page_mut(leaf_pgno)?;
        let leaf_data_copy = leaf_data.to_vec();

        let new_node = Node::leaf(key.to_vec(), value.to_vec());

        let (new_leaf_data, split) = if is_update {
            self.insert_at_leaf_update(&leaf_data_copy, new_node, insert_index, leaf_pgno, page_size)?
        } else {
            db_info.entries += 1;
            insert_into_leaf(&leaf_data_copy, new_node, insert_index, leaf_pgno, page_size)?
        };

        // Write the new leaf data
        let leaf_data = txn.page_mut(leaf_pgno)?;
        leaf_data[..new_leaf_data.len()].copy_from_slice(&new_leaf_data);

        if let Some((right_data, separator)) = split {
            let (right_pgno, right_page) = txn.alloc_page()?;
            let mut right_data_with_pgno = right_data;
            right_data_with_pgno[0..8].copy_from_slice(&right_pgno.to_le_bytes());
            right_page[..right_data_with_pgno.len()].copy_from_slice(&right_data_with_pgno);
            db_info.leaf_pages += 1;

            // Propagate split up the tree
            self.propagate_split(txn, &state, separator, right_pgno, db_info)?;
        }

        Ok(())
    }

    /// Insert at a leaf position, handling updates.
    fn insert_at_leaf_update(
        &self,
        page_data: &[u8],
        new_node: Node,
        update_index: usize,
        page_no: PageNo,
        page_size: usize,
    ) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
        let page = LeafPage::new(page_data, page_size)?;
        let num_keys = page.num_keys();

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

        let mut builder = PageBuilder::new_leaf(page_no, page_size);
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

        let mut left_builder = PageBuilder::new_leaf(page_no, page_size);
        for node in &nodes[..split_point] {
            left_builder.add_leaf(node)?;
        }

        let mut right_builder = PageBuilder::new_leaf(0, page_size);
        for node in &nodes[split_point..] {
            right_builder.add_leaf(node)?;
        }

        Ok((
            left_builder.finish(),
            Some((right_builder.finish(), nodes[split_point].key.clone())),
        ))
    }

    /// Propagates a split up the tree.
    fn propagate_split(
        &self,
        txn: &mut RwTxn<'_>,
        state: &CursorState,
        mut separator: Vec<u8>,
        mut right_pgno: PageNo,
        db_info: &mut DbInfo,
    ) -> Result<()> {
        let page_size = txn.env().page_size();
        let stack_len = state.stack.len();

        for level in (0..stack_len.saturating_sub(1)).rev() {
            let parent = &state.stack[level];
            let parent_data = txn.page_mut(parent.page_no)?.to_vec();

            let (new_parent_data, split) = insert_into_branch(
                &parent_data,
                separator.clone(),
                right_pgno,
                parent.index,
                parent.page_no,
                page_size,
            )?;

            let parent_page = txn.page_mut(parent.page_no)?;
            parent_page[..new_parent_data.len()].copy_from_slice(&new_parent_data);

            match split {
                Some((new_right_data, new_separator)) => {
                    let (new_right_pgno, new_right_page) = txn.alloc_page()?;
                    let mut right_with_pgno = new_right_data;
                    right_with_pgno[0..8].copy_from_slice(&new_right_pgno.to_le_bytes());
                    new_right_page[..right_with_pgno.len()].copy_from_slice(&right_with_pgno);
                    right_pgno = new_right_pgno;
                    separator = new_separator;
                    db_info.branch_pages += 1;
                }
                None => return Ok(()),
            }
        }

        // Need a new root
        self.create_new_root(txn, separator, right_pgno, db_info)?;

        Ok(())
    }

    /// Creates a new root page when the old root splits.
    fn create_new_root(
        &self,
        txn: &mut RwTxn<'_>,
        separator: Vec<u8>,
        right_pgno: PageNo,
        db_info: &mut DbInfo,
    ) -> Result<()> {
        let page_size = txn.env().page_size();
        let (new_root_pgno, new_root_page) = txn.alloc_page()?;

        let mut builder = PageBuilder::new_branch(new_root_pgno, page_size);
        builder.add_branch(&Node::branch(Vec::new(), db_info.root))?;
        builder.add_branch(&Node::branch(separator, right_pgno))?;

        let root_data = builder.finish();
        new_root_page[..root_data.len()].copy_from_slice(&root_data);

        db_info.root = new_root_pgno;
        db_info.depth += 1;
        db_info.branch_pages += 1;

        Ok(())
    }

    /// Deletes the entry with the given key.
    ///
    /// Returns `true` if the key was found and deleted, `false` otherwise.
    pub fn delete<'a>(&self, txn: &mut RwTxn<'_>, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(false);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        if !result.is_found() {
            return Ok(false);
        }

        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let delete_index = result.index();

        // Get the leaf page data
        let leaf_data = txn.page_mut(leaf_pgno)?.to_vec();
        let page = LeafPage::new(&leaf_data, page_size)?;
        let num_keys = page.num_keys();

        // Rebuild page without the deleted entry
        let mut builder = PageBuilder::new_leaf(leaf_pgno, page_size);

        for i in 0..num_keys {
            if i != delete_index {
                let node_ref = page.node(i)?;
                builder.add_leaf(&Node::leaf(
                    node_ref.key().to_vec(),
                    node_ref.value().to_vec(),
                ))?;
            }
        }

        let new_data = builder.finish();
        let leaf_page = txn.page_mut(leaf_pgno)?;
        leaf_page[..new_data.len()].copy_from_slice(&new_data);

        Ok(true)
    }

    /// Clears all entries from the database.
    pub fn clear(&self, _txn: &mut RwTxn<'_>) -> Result<()> {
        if self.db_info.root == 0 {
            return Ok(());
        }

        // For now, we'll just create a new empty tree
        // A full implementation would free all pages
        // This is a simplified version that just resets the root

        // TODO: Properly free all pages in the tree

        Ok(())
    }

    /// Returns the entry with the largest key less than the given key.
    pub fn get_lower_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: BytesEncode<'a> + OwnedDecode,
        DC: OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        // Search for the key
        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        // Move to previous entry
        let found = if result.is_found() {
            // Key exists, move to previous
            CursorOps::prev(&mut state, page_size, &get_page)?
        } else {
            // Key doesn't exist, cursor is at insertion point
            // If we're at a valid position, check if current key is less than target
            if state.is_valid() {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((current_key, _)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        if current_key < key_bytes.as_ref() {
                            true
                        } else {
                            CursorOps::prev(&mut state, page_size, &get_page)?
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        };

        if !found {
            return Ok(None);
        }

        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((k, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                let key = KC::decode_owned(k)?;
                let value = DC::decode_owned(v)?;
                return Ok(Some((key, value)));
            }
        }

        Ok(None)
    }

    /// Returns the entry with the largest key less than or equal to the given key.
    pub fn get_lower_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: BytesEncode<'a> + OwnedDecode,
        DC: OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        let found = if result.is_found() {
            // Exact match
            true
        } else if state.is_valid() {
            // Check if current is less than or equal
            if let Some(pgno) = state.leaf_pgno() {
                let page_data = get_page(pgno)?;
                if let Some((current_key, _)) = CursorOps::get_current(&state, &page_data, page_size)? {
                    if current_key <= key_bytes.as_ref() {
                        true
                    } else {
                        CursorOps::prev(&mut state, page_size, &get_page)?
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !found {
            return Ok(None);
        }

        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((k, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                let key = KC::decode_owned(k)?;
                let value = DC::decode_owned(v)?;
                return Ok(Some((key, value)));
            }
        }

        Ok(None)
    }

    /// Returns the entry with the smallest key greater than the given key.
    pub fn get_greater_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: BytesEncode<'a> + OwnedDecode,
        DC: OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        let found = if result.is_found() {
            // Exact match, need to move to next
            CursorOps::next(&mut state, page_size, &get_page)?
        } else if state.is_valid() {
            // Cursor at insertion point, check if current is greater
            if let Some(pgno) = state.leaf_pgno() {
                let page_data = get_page(pgno)?;
                if let Some((current_key, _)) = CursorOps::get_current(&state, &page_data, page_size)? {
                    current_key > key_bytes.as_ref()
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !found {
            return Ok(None);
        }

        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((k, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                let key = KC::decode_owned(k)?;
                let value = DC::decode_owned(v)?;
                return Ok(Some((key, value)));
            }
        }

        Ok(None)
    }

    /// Returns the entry with the smallest key greater than or equal to the given key.
    pub fn get_greater_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::OwnedItem, DC::OwnedItem)>>
    where
        KC: BytesEncode<'a> + OwnedDecode,
        DC: OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(None);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        if result.is_found() || state.is_valid() {
            if let Some(pgno) = state.leaf_pgno() {
                let page_data = get_page(pgno)?;
                if let Some((k, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                    if k >= key_bytes.as_ref() {
                        let key = KC::decode_owned(k)?;
                        let value = DC::decode_owned(v)?;
                        return Ok(Some((key, value)));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Returns an iterator over all entries in the database.
    pub fn iter<'txn>(&self, txn: &'txn RoTxn<'txn>) -> Result<RoIter<'txn, KC, DC>> {
        let page_size = txn.env().page_size();
        Ok(RoIter::new(self.db_info.root, page_size, txn))
    }

    /// Returns a reverse iterator over all entries in the database.
    pub fn rev_iter<'txn>(&self, txn: &'txn RoTxn<'txn>) -> Result<RoRevIter<'txn, KC, DC>> {
        let page_size = txn.env().page_size();
        Ok(RoRevIter::new(self.db_info.root, page_size, txn))
    }

    /// Returns an iterator over a range of entries in the database.
    pub fn range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn<'txn>,
        range: &'a R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        R: RangeBounds<&'a KC::EItem>,
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();

        // Convert bounds to bytes
        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RoRange::new(self.db_info.root, page_size, txn, start_bound, end_bound))
    }

    /// Returns a reverse iterator over a range of entries in the database.
    pub fn rev_range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn<'txn>,
        range: &'a R,
    ) -> Result<RoRevRange<'txn, KC, DC>>
    where
        R: RangeBounds<&'a KC::EItem>,
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();

        // Convert bounds to bytes
        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RoRevRange::new(self.db_info.root, page_size, txn, start_bound, end_bound))
    }

    /// Returns an iterator over all entries with keys starting with the given prefix.
    pub fn prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        prefix: &'a KC::EItem,
    ) -> Result<RoPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();
        let prefix_bytes = KC::bytes_encode(prefix)?.to_vec();
        Ok(RoPrefix::new(self.db_info.root, page_size, txn, prefix_bytes))
    }

    /// Returns a reverse iterator over all entries with keys starting with the given prefix.
    pub fn rev_prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        prefix: &'a KC::EItem,
    ) -> Result<RoRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();
        let prefix_bytes = KC::bytes_encode(prefix)?.to_vec();
        Ok(RoRevPrefix::new(self.db_info.root, page_size, txn, prefix_bytes))
    }

    /// Returns a mutable iterator over all entries in the database.
    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn<'txn>) -> Result<RwIter<'txn, KC, DC>> {
        let page_size = txn.env().page_size();
        Ok(RwIter::new(self.db_info.root, page_size, txn))
    }

    /// Returns a mutable reverse iterator over all entries in the database.
    pub fn rev_iter_mut<'txn>(&self, txn: &'txn mut RwTxn<'txn>) -> Result<RwRevIter<'txn, KC, DC>> {
        let page_size = txn.env().page_size();
        Ok(RwRevIter::new(self.db_info.root, page_size, txn))
    }

    /// Returns a mutable iterator over a range of entries in the database.
    pub fn range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn mut RwTxn<'txn>,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        R: RangeBounds<&'a KC::EItem>,
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();

        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RwRange::new(self.db_info.root, page_size, txn, start_bound, end_bound))
    }

    /// Returns a mutable reverse iterator over a range of entries in the database.
    pub fn rev_range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn mut RwTxn<'txn>,
        range: &'a R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        R: RangeBounds<&'a KC::EItem>,
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();

        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RwRevRange::new(self.db_info.root, page_size, txn, start_bound, end_bound))
    }

    /// Returns a mutable iterator over all entries with keys starting with the given prefix.
    pub fn prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn<'txn>,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();
        let prefix_bytes = KC::bytes_encode(prefix)?.to_vec();
        Ok(RwPrefix::new(self.db_info.root, page_size, txn, prefix_bytes))
    }

    /// Returns a mutable reverse iterator over all entries with keys starting with the given prefix.
    pub fn rev_prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn<'txn>,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        let page_size = txn.env().page_size();
        let prefix_bytes = KC::bytes_encode(prefix)?.to_vec();
        Ok(RwRevPrefix::new(self.db_info.root, page_size, txn, prefix_bytes))
    }

    /// Inserts a key-value pair into the database with specific flags.
    pub fn put_with_flags<'a>(
        &self,
        txn: &mut RwTxn<'_>,
        flags: crate::flags::PutFlags,
        key: &'a KC::EItem,
        value: &'a DC::EItem,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes = KC::bytes_encode(key)?;
        let value_bytes = DC::bytes_encode(value)?;

        if key_bytes.len() > crate::MAX_KEY_SIZE {
            return Err(Error::BadValSize);
        }

        // Handle NOOVERWRITE flag
        if flags.contains(crate::flags::PutFlags::NO_OVERWRITE) {
            if self.db_info.root != 0 {
                let page_size = txn.env().page_size();
                let mut state = CursorState::new(self.db_info.root);
                let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
                    txn.page(pgno).map(|s| s.to_vec())
                };
                let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;
                if result.is_found() {
                    return Ok(false); // Key exists, don't overwrite
                }
            }
        }

        let page_size = txn.env().page_size();
        let mut db_info = self.db_info;

        if db_info.root == 0 {
            let (pgno, data) = txn.alloc_page()?;
            let mut builder = PageBuilder::new_leaf(pgno, page_size);
            builder.add_leaf(&Node::leaf(key_bytes.to_vec(), value_bytes.to_vec()))?;
            data.copy_from_slice(&builder.finish());

            db_info.root = pgno;
            db_info.entries += 1;
            db_info.leaf_pages += 1;
            db_info.depth = 1;
        } else {
            self.insert_into_tree(txn, &key_bytes, &value_bytes, &mut db_info)?;
        }

        Ok(true)
    }

    /// Gets the value if the key exists, otherwise inserts the provided value.
    ///
    /// Returns the existing value if found, or `None` if the value was inserted.
    pub fn get_or_put<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn<'_>,
        key: &'a KC::EItem,
        value: &'a DC::EItem,
    ) -> Result<Option<DC::OwnedItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        // Check if key exists
        if self.db_info.root != 0 {
            let page_size = txn.env().page_size();
            let mut state = CursorState::new(self.db_info.root);
            let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
                txn.page(pgno).map(|s| s.to_vec())
            };

            let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

            if result.is_found() {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        return Ok(Some(DC::decode_owned(v)?));
                    }
                }
            }
        }

        // Key doesn't exist, insert
        self.put(txn, key, value)?;
        Ok(None)
    }

    /// Gets the value if the key exists, otherwise inserts with specific flags.
    pub fn get_or_put_with_flags<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn<'_>,
        flags: crate::flags::PutFlags,
        key: &'a KC::EItem,
        value: &'a DC::EItem,
    ) -> Result<Option<DC::OwnedItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + OwnedDecode,
    {
        let key_bytes = KC::bytes_encode(key)?;

        // Check if key exists
        if self.db_info.root != 0 {
            let page_size = txn.env().page_size();
            let mut state = CursorState::new(self.db_info.root);
            let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
                txn.page(pgno).map(|s| s.to_vec())
            };

            let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

            if result.is_found() {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        return Ok(Some(DC::decode_owned(v)?));
                    }
                }
            }
        }

        // Key doesn't exist, insert with flags
        self.put_with_flags(txn, flags, key, value)?;
        Ok(None)
    }

    /// Deletes all entries within the given range.
    ///
    /// Returns the number of entries deleted.
    pub fn delete_range<'a, 'txn, R>(&self, txn: &'txn mut RwTxn<'_>, range: &'a R) -> Result<usize>
    where
        R: RangeBounds<&'a KC::EItem>,
        KC: BytesEncode<'a> + OwnedDecode,
    {
        if self.db_info.root == 0 {
            return Ok(0);
        }

        let page_size = txn.env().page_size();

        // Convert bounds to bytes
        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(KC::bytes_encode(*k)?.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(KC::bytes_encode(*k)?.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Collect keys to delete (we can't modify while iterating)
        let mut keys_to_delete = Vec::new();

        {
            let mut state = CursorState::new(self.db_info.root);
            let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
                txn.page(pgno).map(|s| s.to_vec())
            };

            // Position at start
            let positioned = match &start_bound {
                Bound::Included(start) | Bound::Excluded(start) => {
                    let result = CursorOps::search(&mut state, start, page_size, &get_page)?;
                    if matches!(&start_bound, Bound::Excluded(_)) && result.is_found() {
                        CursorOps::next(&mut state, page_size, &get_page)?
                    } else {
                        state.is_valid()
                    }
                }
                Bound::Unbounded => CursorOps::first(&mut state, page_size, &get_page)?,
            };

            if !positioned {
                return Ok(0);
            }

            // Collect keys
            loop {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((key, _)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        // Check end bound
                        let in_range = match &end_bound {
                            Bound::Included(end) => key <= end.as_slice(),
                            Bound::Excluded(end) => key < end.as_slice(),
                            Bound::Unbounded => true,
                        };

                        if !in_range {
                            break;
                        }

                        keys_to_delete.push(key.to_vec());
                    }
                }

                if !CursorOps::next(&mut state, page_size, &get_page)? {
                    break;
                }
            }
        }

        // Delete collected keys
        let count = keys_to_delete.len();
        for key in keys_to_delete {
            self.delete_by_raw_key(txn, &key)?;
        }

        Ok(count)
    }

    /// Internal method to delete by raw key bytes.
    fn delete_by_raw_key(&self, txn: &mut RwTxn<'_>, key: &[u8]) -> Result<bool> {
        if self.db_info.root == 0 {
            return Ok(false);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, key, page_size, &get_page)?;

        if !result.is_found() {
            return Ok(false);
        }

        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let delete_index = result.index();

        let leaf_data = txn.page_mut(leaf_pgno)?.to_vec();
        let page = LeafPage::new(&leaf_data, page_size)?;
        let num_keys = page.num_keys();

        let mut builder = PageBuilder::new_leaf(leaf_pgno, page_size);

        for i in 0..num_keys {
            if i != delete_index {
                let node_ref = page.node(i)?;
                builder.add_leaf(&Node::leaf(
                    node_ref.key().to_vec(),
                    node_ref.value().to_vec(),
                ))?;
            }
        }

        let new_data = builder.finish();
        let leaf_page = txn.page_mut(leaf_pgno)?;
        leaf_page[..new_data.len()].copy_from_slice(&new_data);

        Ok(true)
    }

    /// Lazily decode the data, returning a database that defers value decoding.
    pub fn lazily_decode_data(&self) -> Database<KC, LazyDecode<DC>> {
        Database {
            dbi: self.dbi,
            name: self.name.clone(),
            db_info: self.db_info,
            flags: self.flags,
            _phantom: PhantomData,
        }
    }

    /// Inserts a key-value pair with reserved space for the value.
    ///
    /// This method reserves `data_size` bytes for the value and calls `write_func`
    /// with a `ReservedSpace` that must be filled completely.
    ///
    /// This is an optimization for cases where you want to write the value directly
    /// to the database without an intermediate copy.
    ///
    /// # Safety
    ///
    /// The `write_func` must write exactly `data_size` bytes to the `ReservedSpace`.
    pub fn put_reserved<'a, F>(
        &self,
        txn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        F: FnOnce(&mut ReservedSpace) -> std::io::Result<()>,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if key_bytes.len() > crate::MAX_KEY_SIZE {
            return Err(Error::BadValSize);
        }

        // Create a buffer for the value
        let mut value_buffer = vec![0u8; data_size];
        let mut reserved = ReservedSpace::new(&mut value_buffer);

        // Let the user write to the reserved space
        write_func(&mut reserved).map_err(|e| Error::from(e))?;

        // Ensure all bytes were written
        if reserved.remaining() != 0 {
            return Err(Error::BadValSize);
        }

        // Now insert the value
        let page_size = txn.env().page_size();
        let mut db_info = self.db_info;

        if db_info.root == 0 {
            let (pgno, data) = txn.alloc_page()?;
            let mut builder = PageBuilder::new_leaf(pgno, page_size);
            builder.add_leaf(&Node::leaf(key_bytes.to_vec(), value_buffer))?;
            data.copy_from_slice(&builder.finish());

            db_info.root = pgno;
            db_info.entries += 1;
            db_info.leaf_pages += 1;
            db_info.depth = 1;
        } else {
            self.insert_into_tree(txn, &key_bytes, &value_buffer, &mut db_info)?;
        }

        Ok(())
    }

    /// Gets the value if the key exists, otherwise reserves space and inserts.
    ///
    /// If the key already exists, returns the existing value.
    /// If not, reserves `data_size` bytes and calls `write_func` to write the value.
    ///
    /// Returns `None` if a new value was inserted, or `Some(value)` if the key existed.
    pub fn get_or_put_reserved<'a, F>(
        &self,
        txn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<Option<DC::OwnedItem>>
    where
        KC: BytesEncode<'a>,
        DC: OwnedDecode,
        F: FnOnce(&mut ReservedSpace) -> std::io::Result<()>,
    {
        let key_bytes = KC::bytes_encode(key)?;

        // Check if key exists
        if self.db_info.root != 0 {
            let page_size = txn.env().page_size();
            let mut state = CursorState::new(self.db_info.root);
            let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
                txn.page(pgno).map(|s| s.to_vec())
            };

            let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

            if result.is_found() {
                if let Some(pgno) = state.leaf_pgno() {
                    let page_data = get_page(pgno)?;
                    if let Some((_, v)) = CursorOps::get_current(&state, &page_data, page_size)? {
                        return Ok(Some(DC::decode_owned(v)?));
                    }
                }
            }
        }

        // Key doesn't exist, insert with reserved space
        self.put_reserved(txn, key, data_size, write_func)?;
        Ok(None)
    }

    /// Gets the value if the key exists, otherwise reserves space and inserts with flags.
    ///
    /// If the key already exists, returns the existing value.
    /// If not, reserves `data_size` bytes and calls `write_func` to write the value.
    ///
    /// The `flags` parameter is currently unused but provided for API compatibility.
    pub fn get_or_put_reserved_with_flags<'a, F>(
        &self,
        txn: &mut RwTxn<'_>,
        _flags: crate::flags::PutFlags,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<Option<DC::OwnedItem>>
    where
        KC: BytesEncode<'a>,
        DC: OwnedDecode,
        F: FnOnce(&mut ReservedSpace) -> std::io::Result<()>,
    {
        // Currently we ignore flags for reserved space operations
        // as the RESERVE flag is implicit in this method
        self.get_or_put_reserved(txn, key, data_size, write_func)
    }

    /// Returns an iterator over duplicate values for the given key.
    ///
    /// This method is only meaningful for databases opened with `DUP_SORT` flag.
    /// For non-DUPSORT databases, this will return at most one value.
    ///
    /// # Note
    ///
    /// DUPSORT databases allow multiple values per key, stored in sorted order.
    /// This method returns an iterator over all values associated with the given key.
    pub fn get_duplicates<'a, 'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<RoDuplicates<'txn, DC>>
    where
        KC: BytesEncode<'a>,
    {
        let key_bytes = KC::bytes_encode(key)?;

        if self.db_info.root == 0 {
            return Ok(RoDuplicates::empty(txn));
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        if !result.is_found() {
            return Ok(RoDuplicates::empty(txn));
        }

        // For non-DUPSORT databases, we just return the single value
        // For DUPSORT databases, we would iterate over all duplicate values
        // Since our current B+tree doesn't support DUPSORT natively,
        // we return a single-value iterator
        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((_, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                return Ok(RoDuplicates::single(value.to_vec(), txn));
            }
        }

        Ok(RoDuplicates::empty(txn))
    }

    /// Deletes a specific duplicate value for the given key.
    ///
    /// This method is only meaningful for databases opened with `DUP_SORT` flag.
    /// It deletes the specific key/value pair if it exists.
    ///
    /// For non-DUPSORT databases, this behaves like `delete()` but also
    /// verifies the value matches before deleting.
    ///
    /// Returns `true` if the entry was found and deleted.
    pub fn delete_one_duplicate<'a>(
        &self,
        txn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes = KC::bytes_encode(key)?;
        let data_bytes = DC::bytes_encode(data)?;

        if self.db_info.root == 0 {
            return Ok(false);
        }

        let page_size = txn.env().page_size();
        let mut state = CursorState::new(self.db_info.root);

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            txn.page(pgno).map(|s| s.to_vec())
        };

        let result = CursorOps::search(&mut state, &key_bytes, page_size, &get_page)?;

        if !result.is_found() {
            return Ok(false);
        }

        // Verify the value matches
        if let Some(pgno) = state.leaf_pgno() {
            let page_data = get_page(pgno)?;
            if let Some((_, value)) = CursorOps::get_current(&state, &page_data, page_size)? {
                if value != data_bytes.as_ref() {
                    // Value doesn't match
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        // Value matches, delete it
        let leaf_pgno = state.leaf_pgno().ok_or(Error::Corrupted)?;
        let delete_index = result.index();

        let leaf_data = txn.page_mut(leaf_pgno)?.to_vec();
        let page = LeafPage::new(&leaf_data, page_size)?;
        let num_keys = page.num_keys();

        let mut builder = PageBuilder::new_leaf(leaf_pgno, page_size);

        for i in 0..num_keys {
            if i != delete_index {
                let node_ref = page.node(i)?;
                builder.add_leaf(&Node::leaf(
                    node_ref.key().to_vec(),
                    node_ref.value().to_vec(),
                ))?;
            }
        }

        let new_data = builder.finish();
        let leaf_page = txn.page_mut(leaf_pgno)?;
        leaf_page[..new_data.len()].copy_from_slice(&new_data);

        Ok(true)
    }
}

/// Marker type for lazy decoding of values.
pub struct LazyDecode<C>(PhantomData<C>);

impl<C> OwnedDecode for LazyDecode<C> {
    type OwnedItem = Vec<u8>;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        Ok(bytes.to_vec())
    }
}

/// A reserved space in the database for writing value data.
///
/// This is used by `put_reserved` to allow zero-copy writes of values.
/// The caller must write exactly the reserved number of bytes.
pub struct ReservedSpace<'a> {
    /// The buffer to write to
    buffer: &'a mut [u8],
    /// Current write position
    written: usize,
}

impl<'a> ReservedSpace<'a> {
    /// Creates a new reserved space wrapping the given buffer.
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, written: 0 }
    }

    /// Returns the total size of the reserved space.
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the number of bytes remaining to be written.
    pub fn remaining(&self) -> usize {
        self.buffer.len() - self.written
    }

    /// Returns a mutable slice of the previously written bytes.
    ///
    /// This can be used to modify previously written data, for example
    /// to add a checksum after writing the body.
    pub fn written_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.written]
    }

    /// Fills the remaining space with zeroes.
    pub fn fill_zeroes(&mut self) {
        self.buffer[self.written..].fill(0);
        self.written = self.buffer.len();
    }
}

impl<'a> std::io::Write for ReservedSpace<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let remaining = self.remaining();
        if remaining == 0 {
            return Ok(0);
        }

        let to_write = buf.len().min(remaining);
        self.buffer[self.written..self.written + to_write].copy_from_slice(&buf[..to_write]);
        self.written += to_write;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> std::io::Seek for ReservedSpace<'a> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            std::io::SeekFrom::Start(offset) => offset as i64,
            std::io::SeekFrom::End(offset) => self.written as i64 + offset,
            std::io::SeekFrom::Current(offset) => self.written as i64 + offset,
        };

        if new_pos < 0 || new_pos > self.written as i64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek position",
            ));
        }

        self.written = new_pos as usize;
        Ok(self.written as u64)
    }
}

/// An iterator over duplicate values for a key in a DUPSORT database.
///
/// For non-DUPSORT databases, this iterator will yield at most one value.
pub struct RoDuplicates<'txn, DC> {
    /// The single value (for non-DUPSORT databases)
    value: Option<Vec<u8>>,
    /// Whether we've returned the value
    returned: bool,
    /// Transaction reference (kept for lifetime consistency)
    _txn: &'txn RoTxn<'txn>,
    /// Phantom data for data codec
    _phantom: PhantomData<DC>,
}

impl<'txn, DC> RoDuplicates<'txn, DC> {
    /// Creates an empty duplicates iterator.
    pub(crate) fn empty(txn: &'txn RoTxn<'txn>) -> Self {
        Self {
            value: None,
            returned: true,
            _txn: txn,
            _phantom: PhantomData,
        }
    }

    /// Creates a single-value duplicates iterator.
    pub(crate) fn single(value: Vec<u8>, txn: &'txn RoTxn<'txn>) -> Self {
        Self {
            value: Some(value),
            returned: false,
            _txn: txn,
            _phantom: PhantomData,
        }
    }
}

impl<'txn, DC> Iterator for RoDuplicates<'txn, DC>
where
    DC: OwnedDecode,
{
    type Item = Result<DC::OwnedItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.returned {
            return None;
        }

        self.returned = true;

        if let Some(ref value) = self.value {
            Some(DC::decode_owned(value))
        } else {
            None
        }
    }
}

/// A read-only iterator over the entries of a database.
pub struct RoIter<'txn, KC, DC> {
    /// Cursor state
    state: CursorState,
    /// Page size
    page_size: usize,
    /// Transaction reference for page access
    txn: &'txn RoTxn<'txn>,
    /// Whether we've started iterating
    started: bool,
    /// Whether we've finished iterating
    finished: bool,
    /// Phantom data for type parameters
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoIter<'txn, KC, DC> {
    /// Creates a new iterator.
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RoTxn<'txn>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }
}

impl<'txn, KC, DC> Iterator for RoIter<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        // Position at first entry if not started
        if !self.started {
            self.started = true;
            match CursorOps::first(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            // Move to next entry
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // Get current entry
        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A reverse read-only iterator over the entries of a database.
pub struct RoRevIter<'txn, KC, DC> {
    /// Cursor state
    state: CursorState,
    /// Page size
    page_size: usize,
    /// Transaction reference for page access
    txn: &'txn RoTxn<'txn>,
    /// Whether we've started iterating
    started: bool,
    /// Whether we've finished iterating
    finished: bool,
    /// Phantom data for type parameters
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevIter<'txn, KC, DC> {
    /// Creates a new reverse iterator.
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RoTxn<'txn>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }
}

impl<'txn, KC, DC> Iterator for RoRevIter<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        // Position at last entry if not started
        if !self.started {
            self.started = true;
            match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            // Move to previous entry
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // Get current entry
        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A read-only range iterator over the entries of a database.
pub struct RoRange<'txn, KC, DC> {
    /// Cursor state
    state: CursorState,
    /// Page size
    page_size: usize,
    /// Transaction reference
    txn: &'txn RoTxn<'txn>,
    /// Start bound (encoded)
    start_bound: Bound<Vec<u8>>,
    /// End bound (encoded)
    end_bound: Bound<Vec<u8>>,
    /// Whether we've started iterating
    started: bool,
    /// Whether we've finished iterating
    finished: bool,
    /// Phantom data
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRange<'txn, KC, DC> {
    /// Creates a new range iterator.
    pub(crate) fn new(
        root: PageNo,
        page_size: usize,
        txn: &'txn RoTxn<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            start_bound,
            end_bound,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    /// Check if a key is within the end bound.
    fn check_end_bound(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Included(end) => key <= end.as_slice(),
            Bound::Excluded(end) => key < end.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

impl<'txn, KC, DC> Iterator for RoRange<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        // Position at start if not started
        if !self.started {
            self.started = true;

            // Position cursor based on start bound
            let positioned = match &self.start_bound {
                Bound::Included(start) | Bound::Excluded(start) => {
                    match CursorOps::search(&mut self.state, start, self.page_size, &get_page) {
                        Ok(result) => {
                            // If excluded and found exact match, move to next
                            if matches!(&self.start_bound, Bound::Excluded(_)) && result.is_found() {
                                match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            } else {
                                self.state.is_valid()
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Bound::Unbounded => {
                    match CursorOps::first(&mut self.state, self.page_size, &get_page) {
                        Ok(found) => found,
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            if !positioned {
                self.finished = true;
                return None;
            }
        } else {
            // Move to next entry
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // Get current entry and check bounds
        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            // Check end bound
                            if !self.check_end_bound(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A read-only reverse range iterator over the entries of a database.
pub struct RoRevRange<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RoTxn<'txn>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevRange<'txn, KC, DC> {
    pub(crate) fn new(
        root: PageNo,
        page_size: usize,
        txn: &'txn RoTxn<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            start_bound,
            end_bound,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn check_start_bound(&self, key: &[u8]) -> bool {
        match &self.start_bound {
            Bound::Included(start) => key >= start.as_slice(),
            Bound::Excluded(start) => key > start.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

impl<'txn, KC, DC> Iterator for RoRevRange<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;

            let positioned = match &self.end_bound {
                Bound::Included(end) | Bound::Excluded(end) => {
                    match CursorOps::search(&mut self.state, end, self.page_size, &get_page) {
                        Ok(result) => {
                            if matches!(&self.end_bound, Bound::Excluded(_)) && result.is_found() {
                                match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            } else if result.is_found() {
                                true
                            } else {
                                match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Bound::Unbounded => {
                    match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                        Ok(found) => found,
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            if !positioned {
                self.finished = true;
                return None;
            }
        } else {
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.check_start_bound(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A read-only prefix iterator over entries with a common prefix.
pub struct RoPrefix<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RoTxn<'txn>,
    prefix: Vec<u8>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoPrefix<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RoTxn<'txn>, prefix: Vec<u8>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            prefix,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn has_prefix(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }
}

impl<'txn, KC, DC> Iterator for RoPrefix<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            match CursorOps::search(&mut self.state, &self.prefix, self.page_size, &get_page) {
                Ok(_) => {
                    if !self.state.is_valid() {
                        self.finished = true;
                        return None;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.has_prefix(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A read-only reverse prefix iterator.
pub struct RoRevPrefix<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RoTxn<'txn>,
    prefix: Vec<u8>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevPrefix<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RoTxn<'txn>, prefix: Vec<u8>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            prefix,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn has_prefix(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }

    fn prefix_end(&self) -> Vec<u8> {
        let mut end = self.prefix.clone();
        // Increment the last byte that doesn't overflow
        for i in (0..end.len()).rev() {
            if end[i] < 255 {
                end[i] += 1;
                return end;
            }
            end.pop();
        }
        // All 255s - just search for last
        end
    }
}

impl<'txn, KC, DC> Iterator for RoRevPrefix<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            let end = self.prefix_end();
            match CursorOps::search(&mut self.state, &end, self.page_size, &get_page) {
                Ok(_) => {
                    // Move back to find last entry with prefix
                    loop {
                        if !self.state.is_valid() {
                            match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                                Ok(true) => {}
                                Ok(false) => {
                                    self.finished = true;
                                    return None;
                                }
                                Err(e) => return Some(Err(e)),
                            }
                            break;
                        }
                        if let Some(pgno) = self.state.leaf_pgno() {
                            match get_page(pgno) {
                                Ok(page_data) => {
                                    if let Ok(Some((key, _))) = CursorOps::get_current(&self.state, &page_data, self.page_size) {
                                        if self.has_prefix(key) {
                                            break;
                                        }
                                    }
                                }
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                            Ok(true) => {}
                            Ok(false) => {
                                self.finished = true;
                                return None;
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.has_prefix(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable iterator over the entries of a database.
pub struct RwIter<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwIter<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RwTxn<'txn>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }
}

impl<'txn, KC, DC> Iterator for RwIter<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            match CursorOps::first(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable reverse iterator over the entries of a database.
pub struct RwRevIter<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevIter<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RwTxn<'txn>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }
}

impl<'txn, KC, DC> Iterator for RwRevIter<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable range iterator over the entries of a database.
pub struct RwRange<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRange<'txn, KC, DC> {
    pub(crate) fn new(
        root: PageNo,
        page_size: usize,
        txn: &'txn RwTxn<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            start_bound,
            end_bound,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn check_end_bound(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Included(end) => key <= end.as_slice(),
            Bound::Excluded(end) => key < end.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

impl<'txn, KC, DC> Iterator for RwRange<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;

            let positioned = match &self.start_bound {
                Bound::Included(start) | Bound::Excluded(start) => {
                    match CursorOps::search(&mut self.state, start, self.page_size, &get_page) {
                        Ok(result) => {
                            if matches!(&self.start_bound, Bound::Excluded(_)) && result.is_found() {
                                match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            } else {
                                self.state.is_valid()
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Bound::Unbounded => {
                    match CursorOps::first(&mut self.state, self.page_size, &get_page) {
                        Ok(found) => found,
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            if !positioned {
                self.finished = true;
                return None;
            }
        } else {
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.check_end_bound(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable reverse range iterator over the entries of a database.
pub struct RwRevRange<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevRange<'txn, KC, DC> {
    pub(crate) fn new(
        root: PageNo,
        page_size: usize,
        txn: &'txn RwTxn<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            start_bound,
            end_bound,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn check_start_bound(&self, key: &[u8]) -> bool {
        match &self.start_bound {
            Bound::Included(start) => key >= start.as_slice(),
            Bound::Excluded(start) => key > start.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

impl<'txn, KC, DC> Iterator for RwRevRange<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;

            let positioned = match &self.end_bound {
                Bound::Included(end) | Bound::Excluded(end) => {
                    match CursorOps::search(&mut self.state, end, self.page_size, &get_page) {
                        Ok(result) => {
                            if matches!(&self.end_bound, Bound::Excluded(_)) && result.is_found() {
                                match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            } else if result.is_found() {
                                true
                            } else {
                                match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                                    Ok(found) => found,
                                    Err(e) => return Some(Err(e)),
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Bound::Unbounded => {
                    match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                        Ok(found) => found,
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            if !positioned {
                self.finished = true;
                return None;
            }
        } else {
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.check_start_bound(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable prefix iterator.
pub struct RwPrefix<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    prefix: Vec<u8>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwPrefix<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RwTxn<'txn>, prefix: Vec<u8>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            prefix,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn has_prefix(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }
}

impl<'txn, KC, DC> Iterator for RwPrefix<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            match CursorOps::search(&mut self.state, &self.prefix, self.page_size, &get_page) {
                Ok(_) => {
                    if !self.state.is_valid() {
                        self.finished = true;
                        return None;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::next(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.has_prefix(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

/// A mutable reverse prefix iterator.
pub struct RwRevPrefix<'txn, KC, DC> {
    state: CursorState,
    page_size: usize,
    txn: &'txn RwTxn<'txn>,
    prefix: Vec<u8>,
    started: bool,
    finished: bool,
    _phantom: PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevPrefix<'txn, KC, DC> {
    pub(crate) fn new(root: PageNo, page_size: usize, txn: &'txn RwTxn<'txn>, prefix: Vec<u8>) -> Self {
        Self {
            state: CursorState::new(root),
            page_size,
            txn,
            prefix,
            started: false,
            finished: false,
            _phantom: PhantomData,
        }
    }

    fn has_prefix(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }

    fn prefix_end(&self) -> Vec<u8> {
        let mut end = self.prefix.clone();
        for i in (0..end.len()).rev() {
            if end[i] < 255 {
                end[i] += 1;
                return end;
            }
            end.pop();
        }
        end
    }
}

impl<'txn, KC, DC> Iterator for RwRevPrefix<'txn, KC, DC>
where
    KC: OwnedDecode,
    DC: OwnedDecode,
{
    type Item = Result<(KC::OwnedItem, DC::OwnedItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let get_page = |pgno: PageNo| -> Result<Vec<u8>> {
            self.txn.page(pgno).map(|s| s.to_vec())
        };

        if !self.started {
            self.started = true;
            let end = self.prefix_end();
            match CursorOps::search(&mut self.state, &end, self.page_size, &get_page) {
                Ok(_) => {
                    loop {
                        if !self.state.is_valid() {
                            match CursorOps::last(&mut self.state, self.page_size, &get_page) {
                                Ok(true) => {}
                                Ok(false) => {
                                    self.finished = true;
                                    return None;
                                }
                                Err(e) => return Some(Err(e)),
                            }
                            break;
                        }
                        if let Some(pgno) = self.state.leaf_pgno() {
                            match get_page(pgno) {
                                Ok(page_data) => {
                                    if let Ok(Some((key, _))) = CursorOps::get_current(&self.state, &page_data, self.page_size) {
                                        if self.has_prefix(key) {
                                            break;
                                        }
                                    }
                                }
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                            Ok(true) => {}
                            Ok(false) => {
                                self.finished = true;
                                return None;
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        } else {
            match CursorOps::prev(&mut self.state, self.page_size, &get_page) {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if let Some(pgno) = self.state.leaf_pgno() {
            match get_page(pgno) {
                Ok(page_data) => {
                    match CursorOps::get_current(&self.state, &page_data, self.page_size) {
                        Ok(Some((key, value))) => {
                            if !self.has_prefix(key) {
                                self.finished = true;
                                return None;
                            }

                            match (KC::decode_owned(key), DC::decode_owned(value)) {
                                (Ok(k), Ok(v)) => Some(Ok((k, v))),
                                (Err(e), _) | (_, Err(e)) => Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.finished = true;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            self.finished = true;
            None
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests would go here, but require environment setup
}
