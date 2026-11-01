//! Named databases (SPEC 02 §6): creating, opening, clearing and dropping
//! them, and writing their records back to the catalog at commit.

use super::*;

/// Whether some pgno appears twice in `pages` — `clear`'s refusal of a
/// hostile image that aliases one page under several branch pointers (the
/// leaf-skipping walk never loads the leaves it frees). LMDB's `mdb_drop0`
/// has no such check; this keeps it linear in the page count. A well-formed
/// tree's pages spread over a span of pgnos rarely more than a small multiple
/// of their count, so a bitmap over that span (no larger than `pages` itself
/// when the span is at most 64 times the count) replaces the sort, which
/// still covers a sparse tree in a large file.
pub(super) fn has_duplicate(pages: &[u64]) -> bool {
    let (Some(&min), Some(&max)) = (pages.iter().min(), pages.iter().max()) else {
        return false;
    };
    let words = (max - min) / 64 + 1;
    if words <= pages.len() as u64 {
        let mut seen = vec![0u64; words as usize];
        for &p in pages {
            let bit = p - min;
            let (word, mask) = ((bit / 64) as usize, 1u64 << (bit % 64));
            if seen[word] & mask != 0 {
                return true;
            }
            seen[word] |= mask;
        }
        false
    } else {
        let mut sorted = pages.to_vec();
        sorted.sort_unstable();
        sorted.windows(2).any(|w| w[0] == w[1])
    }
}

/// The level guard of the page-collecting walks: `level` is the on-disk
/// `rec.depth` counting down, and they recurse one frame per level, so a
/// zero or a hostile `u16::MAX` must be a typed error, not a stack overflow.
fn check_walk_level(level: u16) -> Result<()> {
    if level == 0 || level as usize > crate::btree::CURSOR_STACK {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    Ok(())
}

/// A named DB's per-txn working state: the loaded/mutated `DBRecord`
/// plus its catalog name, kept in [`RwTxn::open`]. `dirty` marks that the
/// record changed and must be written back into the main catalog at commit
/// (SPEC 02 §6, before `freelist_save` — the LMDB sub-DB flush order).
pub(super) struct NamedTree {
    pub(super) name: Box<[u8]>,
    /// The handle generation the name was looked up for, when it came from
    /// a checked handle ([`RwTxn::ensure_open`]); `None` for a database
    /// this txn created or opened by name. A slot keeps one name per
    /// generation (ADR-0024), so the next write txn may reuse a name
    /// carried with its generation ([`WriterCarry`]) instead of looking it up.
    pub(super) generation: Option<u32>,
    pub(super) rec: DBRecord,
    pub(super) dirty: bool,
    /// This DB's sequential-writes setting (ADR-0015), resolved when the txn
    /// first touches it.
    pub(super) seq: bool,
}

/// The per-txn named-DB table, indexed directly by dbi. LMDB keeps the same
/// state in `txn->mt_dbs[dbi]`, an array read by index on every cursor init;
/// a hashed map cost a SipHash per probe and a named put probes several times.
/// dbi indices are small (slots below `max_dbs`, lowest free first), so the
/// table grows to the highest dbi this txn touched, never past it. An entry
/// exists only for a handle valid in this txn, and such a slot cannot change
/// name while the txn lives except through this txn's own `drop_database`,
/// which removes the entry (ADR-0024).
#[derive(Default)]
pub(super) struct OpenTable {
    pub(super) slots: Vec<Option<NamedTree>>,
}

impl OpenTable {
    #[inline]
    pub(super) fn get(&self, dbi: u32) -> Option<&NamedTree> {
        self.slots.get(dbi as usize)?.as_ref()
    }

    #[inline]
    pub(super) fn get_mut(&mut self, dbi: u32) -> Option<&mut NamedTree> {
        self.slots.get_mut(dbi as usize)?.as_mut()
    }

    #[inline]
    pub(super) fn contains(&self, dbi: u32) -> bool {
        self.get(dbi).is_some()
    }

    /// The slot for `dbi`, growing the table to reach it.
    pub(super) fn slot(&mut self, dbi: u32) -> &mut Option<NamedTree> {
        let i = dbi as usize;
        if i >= self.slots.len() {
            self.slots.resize_with(i + 1, || None);
        }
        &mut self.slots[i]
    }

    pub(super) fn insert(&mut self, dbi: u32, t: NamedTree) {
        *self.slot(dbi) = Some(t);
    }

    pub(super) fn remove(&mut self, dbi: u32) {
        if let Some(s) = self.slots.get_mut(dbi as usize) {
            *s = None;
        }
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &NamedTree> {
        self.slots.iter().flatten()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut NamedTree> {
        self.slots.iter_mut().flatten()
    }
}

impl Env {
    /// `create_database(wtxn, name)` (SPEC 00 rows 11/12, `mdb_dbi_open` +
    /// `MDB_CREATE`): open a database, creating it if absent, inside the active
    /// write txn. `None` name → the main DB (always present). `Some(name)`
    /// assigns/looks up the dbi and inserts an empty `F_SUBDATA` catalog entry
    /// if the name is new (SPEC 02 §6); an abort discards a just-created DB with
    /// the dirty set. Idempotent for an already-open name.
    ///
    /// # Errors
    ///
    /// - [`MdbError::BadValSize`] if `name` is empty or `> MAX_DB_NAME`.
    /// - [`MdbError::DbsFull`] if the catalog is full (`> max_dbs` named DBs).
    /// - [`MdbError::Incompatible`] if the name exists as a plain user key.
    /// - [`MdbError::BadTxn`] on a poisoned write txn.
    pub fn create_database(&self, wtxn: &mut RwTxn<'_>, name: Option<&[u8]>) -> Result<Database> {
        wtxn.guard_ok()?;
        let name = match name {
            None => return Ok(self.main_database()),
            Some(n) => n,
        };
        if name.is_empty() || name.len() > MAX_DB_NAME {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        let db = self.open_slot(&wtxn.handles, name)?;
        let DbSel::Named(dbi) = db.sel() else {
            unreachable!("a named open makes a named handle")
        };
        if let Err(e) = wtxn.create_named(dbi, name) {
            // No database came to exist under this handle (ADR-0024).
            self.inner().handles().abandon(&wtxn.handles, dbi);
            return Err(e);
        }
        Ok(db)
    }

    /// `create_database` with a **custom key comparator** (LMDB's
    /// `mdb_dbi_open` + `MDB_CREATE` followed by `mdb_set_compare`,
    /// which heed does not expose at all — SPEC 00 second table).
    ///
    /// The comparator is registered for this database on the environment and
    /// governs every subsequent search, insert, split, range bound, neighbor
    /// seek and `APPEND` check on it (SPEC 03 §2.0).
    ///
    /// # The comparator is not stored in the file
    ///
    /// **Every future open of this database — in this process or any other —
    /// must pass the same ordering, and nothing checks that it did.** A tree
    /// read under a different ordering than the one that built it silently
    /// returns wrong answers and, once written to, is permanently corrupt.
    /// LMDB has the identical hazard; ZeroDB cannot do better without an
    /// on-disk format change (`DBRecord` is full — see [`crate::cmp`] and
    /// docs/LMDB-COMPATIBILITY.md). Treat the comparator as part of your schema.
    ///
    /// # Errors
    ///
    /// - Everything [`Env::create_database`] returns.
    /// - [`Error::Io`] (`InvalidInput`) if `name` is `None`: the main/unnamed
    ///   database is also the named-DB catalog and is always memcmp-ordered
    ///   ([`crate::cmp::ComparatorError::MainDatabase`]).
    /// - [`Error::Io`] (`InvalidInput`) if a *different* comparator is already
    ///   registered for this database in this process
    ///   ([`crate::cmp::ComparatorError::Mismatch`]).
    pub fn create_database_with_comparator(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: Option<&[u8]>,
        cmp: Box<dyn crate::cmp::Comparator>,
    ) -> Result<Database> {
        let db = self.create_database(wtxn, name)?;
        self.register_comparator_on(db, cmp)?;
        Ok(db)
    }

    /// Register `cmp` for `db` on `env`, mapping [`crate::cmp::ComparatorError`]
    /// into the public taxonomy.
    ///
    /// `Io(InvalidInput)` is the error kind ZeroDB already uses for open-time
    /// argument rejection (cf. the `map_size` and `max_readers(0)` entries in
    /// docs/LMDB-COMPATIBILITY.md, and the `page_size` selector);
    /// LMDB has no error to mirror here because `mdb_set_compare` simply returns
    /// `EINVAL` for a bad dbi and otherwise cannot fail.
    pub(crate) fn register_comparator_on(
        &self,
        db: Database,
        cmp: Box<dyn crate::cmp::Comparator>,
    ) -> Result<()> {
        let dbi = match db.sel() {
            DbSel::Main => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    crate::cmp::ComparatorError::MainDatabase.to_string(),
                )))
            }
            DbSel::Named(dbi) => dbi,
        };
        self.inner().register_comparator(dbi, cmp).map_err(|e| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                e.to_string(),
            ))
        })
    }

    /// Open a nested read transaction parented to `parent` (SPEC 00 row 16).
    /// Equivalent to [`RwTxn::nested_read_txn`] — provided because heed
    /// exposes both entry points (`Env::nested_read_txn(&self, &wtxn)`
    /// delegates exactly like this, without an env-identity check).
    ///
    /// # Errors
    ///
    /// As [`RwTxn::nested_read_txn`].
    pub fn nested_read_txn<'p>(&'p self, parent: &'p RwTxn<'_>) -> Result<NestedRoTxn<'p>> {
        parent.nested_read_txn()
    }
}

impl<'env> RwTxn<'env> {
    /// `clear` (SPEC 00 row 38): free every page of `tree` and reset its working
    /// record to empty. Works for the main DB and any named DB (SPEC 02 §6). The
    /// named record is left in the open table marked dirty (by `record_mut`), so
    /// its catalog entry is rewritten empty at commit; the entry itself stays.
    pub(super) fn clear_tree(&mut self, tree: TreeId) -> Result<()> {
        self.guard_ok()?;
        // Every page of the tree is about to be freed (SPEC 03 §6.6 F3; the
        // `free_page` hook would also catch each fingered page one by one).
        self.finger_invalidate(tree);
        let rec = *self.record(tree);
        if rec.root != PGNO_INVALID {
            let mut pages = Vec::new();
            let mut runs = Vec::new();
            // Leaf-skipping fast path (SPEC 02 §6.1, LMDB `mdb_drop0`): a tree
            // whose record says it has no overflow pages frees its leaf pgnos
            // straight from the lowest branch level, never reading the leaves.
            // Any overflow forces the full walk, which reads every leaf to
            // find the runs.
            let res = if rec.overflow_pages == 0 {
                let mut r =
                    self.collect_tree_skip_leaves(rec.root, rec.depth, &mut pages, &mut runs);
                if r.is_ok() {
                    // The unread leaves were never loaded, so a hostile image
                    // aliasing one pgno under several branch pointers would
                    // free it twice (the full walk double-frees such a page
                    // too, but only after loading it). Refuse the whole clear
                    // instead — nothing has been freed yet.
                    if has_duplicate(&pages) {
                        r = Err(Error::Mdb(MdbError::Invalid));
                    }
                }
                r
            } else {
                self.collect_tree(rec.root, rec.depth, &mut pages, &mut runs)
            };
            if let Err(e) = res {
                self.errored = true;
                return Err(e);
            }
            for (head, n) in runs {
                self.free_run(head, n);
            }
            // A tree this txn has not touched frees every page into `freed`.
            work_mut!(self).freed.reserve(pages.len());
            for p in pages {
                self.free_page(p);
            }
        }
        // Reset to empty (persistent flags are always 0 today). `record_mut`
        // marks a named record dirty for the commit write-back.
        *self.record_mut(tree) = DBRecord::empty();
        Ok(())
    }

    /// `drop` (`mdb_drop(_, 1)`, SPEC 02 §6). Main DB → `clear` (no catalog
    /// entry to remove). Named DB → free every page, then delete the catalog
    /// entry from the main tree and forget the working record so commit does
    /// not write it back. The handle's slot is freed for every txn (ADR-0024,
    /// SPEC 04 DBI-5).
    pub(super) fn drop_database(&mut self, db: &Database) -> Result<()> {
        self.guard_ok()?;
        // TXN-68: the main DB's arm does not pass through `ensure_open`.
        self.maybe_spill()?;
        match db.sel() {
            DbSel::Main => self.clear_tree(TreeId::Main),
            DbSel::Named(dbi) => {
                let tree = self.ensure_open(db)?;
                self.clear_tree(tree)?;
                let name = self
                    .open
                    .get(dbi)
                    .expect("named record loaded")
                    .name
                    .clone();
                // Remove the catalog entry (decrements main_db.entries).
                let existed = self.delete_tree(TreeId::Main, &name)?;
                debug_assert!(existed, "dropped named DB had no catalog entry");
                // The DB no longer exists this txn: drop its working record so
                // `flush_catalog` does not re-create it.
                self.open.remove(dbi);
                // ADR-0024: the handle is closed at once, for every txn; an
                // abort of this one does not reopen it (LMDB `mdb_drop`).
                self.env
                    .inner()
                    .handles()
                    .remove(&self.handles, dbi, db.generation());
                Ok(())
            }
        }
    }

    /// A clean working record for named DB `dbi`, with its sequential-writes
    /// setting resolved (ADR-0015).
    pub(super) fn named_tree(
        &self,
        dbi: u32,
        name: Box<[u8]>,
        generation: Option<u32>,
        rec: DBRecord,
    ) -> NamedTree {
        NamedTree {
            name,
            generation,
            rec,
            dirty: false,
            seq: self.env.inner().sequential_writes_for(Some(dbi)),
        }
    }

    /// Create-or-open a named DB's catalog entry (SPEC 02 §6, `mdb_dbi_open` +
    /// `MDB_CREATE`): if the name already exists as an `F_SUBDATA` record, load
    /// it (idempotent open); if it exists as a plain user key, `Incompatible`;
    /// otherwise insert an empty `F_SUBDATA` record eagerly (LMDB `MDB_CREATE`
    /// creates the entry inside the write txn), so `open_database` sees it and
    /// abort discards it with the dirty set.
    pub(super) fn create_named(&mut self, dbi: u32, name: &[u8]) -> Result<()> {
        self.guard_ok()?;
        // TXN-68: creating a DB inserts a catalog record (a main-tree put).
        self.maybe_spill()?;
        enum Cat {
            Missing,
            SubDb(DBRecord),
            Collision,
        }
        let cat = {
            let tree = Tree::new(
                self.source(),
                self.psize,
                self.main_db.root,
                self.main_db.depth,
            );
            match tree.get_catalog_entry(name).map_err(map_page_err)? {
                Some((flags, val)) if flags & F_SUBDATA != 0 => {
                    Cat::SubDb(DBRecord::from_bytes(val).unwrap_or_else(DBRecord::empty))
                }
                Some(_) => Cat::Collision,
                None => Cat::Missing,
            }
        };
        match cat {
            Cat::Collision => Err(Error::Mdb(MdbError::Incompatible)),
            Cat::SubDb(rec) => {
                let t = self.named_tree(dbi, name.into(), None, rec);
                self.open.slot(dbi).get_or_insert(t);
                Ok(())
            }
            Cat::Missing => {
                let empty = DBRecord::empty();
                let bytes = empty.to_bytes();
                debug_assert_eq!(bytes.len(), DBRECORD_LEN);
                let res = self.put_tree_flagged(
                    TreeId::Main,
                    name,
                    PutFlags::EMPTY,
                    ValSrc::Val(&bytes),
                    F_SUBDATA,
                );
                if res.is_err() {
                    self.errored = true;
                    return res.map(|_| ());
                }
                let t = self.named_tree(dbi, name.into(), None, empty);
                self.open.insert(dbi, t);
                Ok(())
            }
        }
    }

    /// Write back every dirty named-DB working record into the main catalog
    /// (SPEC 02 §6), in ascending-name order for determinism. Runs at commit
    /// step **C1a — before `freelist_save`** (the LMDB sub-DB flush order): a
    /// record rewrite is a same-size 48-byte overwrite of an existing
    /// `F_SUBDATA` entry, but it COWs main-tree leaves and may free pages, all
    /// of which must be captured by the subsequent `freelist_save`.
    pub(super) fn flush_catalog(&mut self) -> Result<()> {
        let mut order = std::mem::take(&mut work_mut!(self).catalog);
        order.clear();
        order.extend(
            (0..self.open.slots.len() as u32)
                .filter(|&dbi| self.open.get(dbi).is_some_and(|t| t.dirty)),
        );
        let open = &self.open;
        // Names are unique, so the unstable sort is deterministic.
        order.sort_unstable_by(|&a, &b| {
            let name = |dbi| &open.get(dbi).expect("listed above").name;
            name(a).cmp(name(b))
        });
        for &dbi in &order {
            // The name moves out for the put (which reads no named-DB name)
            // and back after it, so the write borrows it without a copy.
            let t = self.open.get_mut(dbi).expect("listed above");
            let name = std::mem::take(&mut t.name);
            let bytes = t.rec.to_bytes();
            let r = self.put_tree_flagged(
                TreeId::Main,
                &name,
                PutFlags::EMPTY,
                ValSrc::Val(&bytes),
                F_SUBDATA,
            );
            self.open.get_mut(dbi).expect("listed above").name = name;
            r?;
        }
        work_mut!(self).catalog = order;
        for t in self.open.values_mut() {
            t.dirty = false;
        }
        Ok(())
    }

    pub(super) fn collect_tree(
        &self,
        pgno: u64,
        level: u16,
        pages: &mut Vec<u64>,
        runs: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        check_walk_level(level)?;
        let page = self.load(pgno)?;
        match page.page_type() {
            PageType::Leaf => self.collect_loaded_leaf(pgno, pages, runs),
            PageType::Branch => {
                let br = branch_view(self.source(), self.psize, pgno, Some(&self.validated))
                    .map_err(map_page_err)?;
                for i in 0..br.num_keys() {
                    self.collect_tree(br.child_pgno(i), level - 1, pages, runs)?;
                }
                pages.push(pgno);
                Ok(())
            }
            _ => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// Leaf-skipping variant of [`collect_tree`](Self::collect_tree) for a
    /// tree whose record says `overflow_pages == 0` (SPEC 02 §6.1, LMDB
    /// `mdb_drop0`): branch pages are read as usual, but the children of the
    /// lowest branch level — and a root-is-leaf root — are collected WITHOUT
    /// loading the leaf pages, so `clear`/`drop` reads only the branches.
    ///
    /// An unread pgno bypasses the loader's own bounds check, so it is
    /// classified here before it may reach the free list: it must be dirty in
    /// this txn (a this-txn allocation, legitimately past the committed
    /// high-water) or lie in `[FIRST_DATA_PGNO, committed_last_pg]`. Anything
    /// else — a meta slot (0/1) or a pgno past the committed high-water —
    /// gets the same typed `Invalid` the full walk's loader returns, and the
    /// caller then frees nothing (collect-before-free). What is NOT checked is
    /// the page's type: a corrupt record that understates the depth frees the
    /// mis-typed page (in-bounds, so non-corrupting) and leaks its subtree,
    /// where the full walk returns `Invalid`.
    pub(super) fn collect_tree_skip_leaves(
        &self,
        pgno: u64,
        level: u16,
        pages: &mut Vec<u64>,
        runs: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        check_walk_level(level)?;
        if level == 1 {
            // A root that is the leaf level (a one-page tree).
            return self.collect_unread_leaf(pgno, pages);
        }
        let page = self.load(pgno)?;
        match page.page_type() {
            PageType::Branch => {
                let br = branch_view(self.source(), self.psize, pgno, Some(&self.validated))
                    .map_err(map_page_err)?;
                if level == 2 {
                    // Every child is an unread leaf: room for all of them at
                    // once, and one loop over the child pointers (LMDB's
                    // `mdb_drop0` with `mdb_midl_need` per branch).
                    pages.reserve(br.num_keys() + 1);
                    for i in 0..br.num_keys() {
                        self.collect_unread_leaf(br.child_pgno(i), pages)?;
                    }
                } else {
                    for i in 0..br.num_keys() {
                        self.collect_tree_skip_leaves(br.child_pgno(i), level - 1, pages, runs)?;
                    }
                }
                pages.push(pgno);
                Ok(())
            }
            // A leaf above the leaf level (ragged — only a hostile image):
            // handled exactly as the full walk handles a loaded leaf.
            PageType::Leaf => self.collect_loaded_leaf(pgno, pages, runs),
            _ => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// Collect loaded leaf `pgno` and the overflow runs its values head.
    fn collect_loaded_leaf(
        &self,
        pgno: u64,
        pages: &mut Vec<u64>,
        runs: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        let leaf = leaf_view(self.source(), self.psize, pgno, Some(&self.validated))
            .map_err(map_page_err)?;
        for i in 0..leaf.num_keys() {
            if let LeafValue::Overflow { head_pgno, dsize } = leaf.value(i) {
                runs.push((head_pgno, overflow_page_count(dsize as u64, self.psize)));
            }
        }
        pages.push(pgno);
        Ok(())
    }

    /// Collect leaf `pgno` for [`collect_tree_skip_leaves`](Self::collect_tree_skip_leaves)
    /// without loading it, after the bound check that stands in for the load
    /// (see there). Committed leaves (the common case) answer on the range
    /// test; the dirty and spilled lookups run only for this-txn pages.
    #[inline]
    pub(super) fn collect_unread_leaf(&self, pgno: u64, pages: &mut Vec<u64>) -> Result<()> {
        if (FIRST_DATA_PGNO..=self.committed_last_pg).contains(&pgno)
            || self.dirty.contains(pgno)
            || self.dirty.spilled_pages(pgno).is_some()
        {
            pages.push(pgno);
            return Ok(());
        }
        Err(Error::Mdb(MdbError::Invalid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_duplicate_matches_a_sort_on_both_arms() {
        assert!(!has_duplicate(&[]));
        assert!(!has_duplicate(&[7]));
        // Dense span: the bitmap arm (words across a 64-bit boundary).
        assert!(!has_duplicate(&[130, 3, 64, 65, 2]));
        assert!(has_duplicate(&[130, 3, 64, 65, 130]));
        assert!(has_duplicate(&[5, 5]));
        // Sparse span: the sort arm.
        assert!(!has_duplicate(&[2, 1 << 40, 9]));
        assert!(has_duplicate(&[1 << 40, 2, 1 << 40]));
        assert!(!has_duplicate(&[0, u64::MAX]));
        assert!(has_duplicate(&[u64::MAX, 0, u64::MAX]));
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 16 } else { 256 },
            ..Default::default()
        })]

        /// `has_duplicate` agrees with sort-and-dedup on dense and sparse
        /// pgno sets.
        #[test]
        fn has_duplicate_matches_model(
            pages in proptest::collection::vec(0u64..2_000, 0..200),
            sparse in proptest::prelude::any::<bool>(),
        ) {
            let pages: Vec<u64> = if sparse {
                pages.iter().map(|p| p << 30).collect()
            } else {
                pages
            };
            let mut sorted = pages.clone();
            sorted.sort_unstable();
            sorted.dedup();
            proptest::prop_assert_eq!(has_duplicate(&pages), sorted.len() != pages.len());
        }
    }
}
