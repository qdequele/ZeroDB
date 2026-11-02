//! DUPSORT spike: the write side of duplicate-key databases.
//!
//! A key with one value is an ordinary leaf cell; a key with two or more
//! values is one `F_DUPDATA` cell whose value is an embedded sub-page
//! ([`crate::page::subpage`]). Every change rewrites that whole cell. A set
//! that would outgrow the leaf's inline bound (`max_node_size`) is refused
//! with `ErrorKind::Unsupported`: promotion to a sub-tree is not part of the
//! spike.

use super::*;
use crate::page::geometry::max_node_size;
use crate::page::subpage::{self, SubPage};
use crate::page::{DB_DUPSORT, F_DUPDATA};

/// The largest duplicate value: duplicates are bounded like keys
/// (SPEC 03 §12.1 O1).
const MAX_DUP_SIZE: usize = MAX_KEY_SIZE;

impl Env {
    /// `create_database` with persistent database flags (LMDB's
    /// `mdb_dbi_open` with `MDB_CREATE | flags`). Only [`DB_DUPSORT`] is
    /// accepted. The flags of a database that already exists are kept and
    /// `flags` is ignored, as in LMDB (SPEC 03 §12.1 O8).
    ///
    /// # Errors
    ///
    /// As [`Env::create_database`], plus `Io(InvalidInput)` for an unknown flag
    /// or the main database.
    pub fn create_database_with_flags(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: Option<&[u8]>,
        flags: u16,
    ) -> Result<Database> {
        if flags & !DB_DUPSORT != 0 || name.is_none() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the DUPSORT spike supports DB_DUPSORT on named databases only",
            )));
        }
        wtxn.guard_ok()?;
        let name = name.expect("checked above");
        if name.is_empty() || name.len() > MAX_DB_NAME {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        let db = self.open_slot(&wtxn.handles, name)?;
        let DbSel::Named(dbi) = db.sel() else {
            unreachable!("a named open makes a named handle")
        };
        if let Err(e) = wtxn.create_named(dbi, name, flags) {
            self.inner().handles().abandon(&wtxn.handles, dbi);
            return Err(e);
        }
        Ok(db)
    }

    /// Register the duplicate ordering of `db` (LMDB's `mdb_set_dupsort`).
    /// Same rules as a key comparator: named databases only, and every open
    /// of the database must pass the same ordering.
    ///
    /// # Errors
    ///
    /// `Io(InvalidInput)` for the main database or a different ordering
    /// already registered in this process.
    pub fn set_dup_comparator(
        &self,
        db: Database,
        cmp: Box<dyn crate::cmp::Comparator>,
    ) -> Result<()> {
        let invalid =
            |msg: String| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg));
        let DbSel::Named(dbi) = db.sel() else {
            return Err(invalid(
                crate::cmp::ComparatorError::MainDatabase.to_string(),
            ));
        };
        self.inner()
            .register_dup_comparator(dbi, cmp)
            .map_err(|e| invalid(e.to_string()))
    }
}

impl RwTxn<'_> {
    pub(super) fn is_dupsort(&self, tree: TreeId) -> bool {
        self.record(tree).flags & DB_DUPSORT != 0
    }

    /// The stored cell for `key` in `db`: its node flags and an owned copy of
    /// its value bytes.
    fn dup_cell(&self, db: &Database, key: &[u8]) -> Result<Option<(u16, Vec<u8>)>> {
        let tree = db.tree(self);
        Ok(tree
            .get_catalog_entry(key)
            .map_err(map_page_err)?
            .map(|(f, v)| (f, v.to_vec())))
    }

    /// Add the pair `(key, val)` to the DUPSORT tree `tree`. An existing pair
    /// is a no-op (SPEC 03 §12.1 O5); `NO_OVERWRITE` on an existing key is
    /// `KeyExist`.
    pub(super) fn put_dup(
        &mut self,
        db: &Database,
        tree: TreeId,
        key: &[u8],
        val: &[u8],
        flags: PutFlags,
    ) -> Result<()> {
        if val.len() > MAX_DUP_SIZE {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        let Some((node_flags, old)) = self.dup_cell(db, key)? else {
            return self
                .put_tree(tree, key, flags, ValSrc::Val(val))
                .map(|_| ());
        };
        if flags.contains(PutFlags::NO_OVERWRITE) {
            return Err(Error::Mdb(MdbError::KeyExist));
        }
        let mut vals: Vec<Vec<u8>> = if node_flags & F_DUPDATA != 0 {
            SubPage::parse(&old)
                .map_err(map_page_err)?
                .iter()
                .map(<[u8]>::to_vec)
                .collect()
        } else {
            vec![old]
        };
        let cmp = self.env.inner().dup_comparator_for(db.sel());
        let pos = match vals.binary_search_by(|v| cmp.compare(v, val)) {
            Ok(_) => return Ok(()),
            Err(pos) => pos,
        };
        vals.insert(pos, val.to_vec());
        let refs: Vec<&[u8]> = vals.iter().map(Vec::as_slice).collect();
        let mut cell = Vec::new();
        subpage::encode(&refs, &mut cell);
        if LEAF_NODE_HEADER + key.len() + cell.len() > max_node_size(self.psize) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "duplicate set larger than one leaf cell (sub-tree promotion is not implemented)",
            )));
        }
        // The node flag changes from plain to F_DUPDATA on the second value,
        // and an overwrite keeps node flags, so replace the cell outright.
        self.delete_tree(tree, key)?;
        self.put_tree_flagged(tree, key, PutFlags::EMPTY, ValSrc::Val(&cell), F_DUPDATA)?;
        sat_add(&mut self.record_mut(tree).entries, 1);
        Ok(())
    }

    /// Delete `key` and all its values from the DUPSORT tree `tree`.
    pub(super) fn delete_dup_key(
        &mut self,
        db: &Database,
        tree: TreeId,
        key: &[u8],
    ) -> Result<bool> {
        let count = match self.dup_cell(db, key)? {
            None => return Ok(false),
            Some((f, v)) if f & F_DUPDATA != 0 => {
                SubPage::parse(&v).map_err(map_page_err)?.len() as u64
            }
            Some(_) => 1,
        };
        let found = self.delete_tree(tree, key)?;
        let rec = self.record_mut(tree);
        rec.entries = rec.entries.saturating_sub(count - 1);
        Ok(found)
    }
}
