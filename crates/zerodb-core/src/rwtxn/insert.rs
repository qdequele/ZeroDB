//! Puts: leaf inserts and replaces, overflow values, and leaf and branch
//! splits (SPEC 03 §6, §9).

use super::*;

/// Leaf node header size (SPEC 02 §4.2).
pub(super) const LEAF_NODE_HEADER: usize = 8;

/// Branch node header size (SPEC 02 §4.1).
pub(super) const BRANCH_NODE_HEADER: usize = 10;

/// Put flags (SPEC 01 Table 3 subset). Hand-rolled bitset like
/// [`crate::env::Env`]'s flags — no `bitflags` dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PutFlags(u32);

impl PutFlags {
    /// No flags: plain upsert.
    pub const EMPTY: PutFlags = PutFlags(0);
    /// `MDB_APPEND` (SPEC 01 §S1, SPEC 03 §6.3): key must be strictly greater
    /// than the current last key; equal-or-less is `KeyExist`.
    pub const APPEND: PutFlags = PutFlags(0x2_0000);
    /// `MDB_NOOVERWRITE` (SPEC 01 §S2): insert only if the key is absent.
    pub const NO_OVERWRITE: PutFlags = PutFlags(0x10);

    /// Whether all bits of `other` are set.
    #[must_use]
    pub fn contains(self, other: PutFlags) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for PutFlags {
    type Output = PutFlags;
    fn bitor(self, rhs: PutFlags) -> PutFlags {
        PutFlags(self.0 | rhs.0)
    }
}

/// The value source of a put: caller bytes, or a RESERVE of `n` bytes
/// (TXN-47 — the engine places the cell and the caller fills it).
#[derive(Clone, Copy)]
pub(super) enum ValSrc<'v> {
    Val(&'v [u8]),
    Reserve(usize),
}

impl ValSrc<'_> {
    pub(super) fn len(&self) -> usize {
        match self {
            ValSrc::Val(v) => v.len(),
            ValSrc::Reserve(n) => *n,
        }
    }
}

/// Where a reserved value landed (so `put_reserved` can hand out the slice).
pub(super) enum ReserveLoc {
    /// Inline in the leaf. `at` carries the settled `(leaf pgno, cell index)`
    /// when the insert path can prove it final — every no-split arm, where
    /// the receiving leaf and slot are in scope — so the common
    /// `put_reserved` needs no second descent. `None` (a split moved cells)
    /// falls back to a key search.
    Inline { at: Option<(u64, usize)> },
    /// On the overflow run headed at this pgno.
    Big(u64),
}

/// A leaf split's new-cell payload, **borrowed** from the caller:
/// `split_leaf` packs it straight into the target frame without ever
/// materializing an owned cell.
pub(super) enum NewLeafVal<'v> {
    /// A plain inline value.
    Inline(&'v [u8]),
    /// RESERVE (TXN-47): a zero-filled inline region of this length (fresh
    /// frames are zero-initialized, so packing reserves bytes without a
    /// zeroed scratch vec).
    ZeroReserve(usize),
    /// A BIGDATA pointer cell (the overflow run is already written).
    Big { head: u64, dsize: u32 },
}

impl NewLeafVal<'_> {
    /// §6.4 `used` term for the new cell (mirrors [`leaf_cell_used`]).
    pub(super) fn used(&self, key_len: usize) -> usize {
        let varea = match self {
            NewLeafVal::Inline(v) => v.len(),
            NewLeafVal::ZeroReserve(n) => *n,
            NewLeafVal::Big { .. } => 8,
        };
        even(LEAF_NODE_HEADER + key_len + varea) + 2
    }
}

/// §6.4 `used` term of existing cell `i` of a leaf view.
pub(super) fn leaf_cell_used(leaf: &LeafRef<'_>, i: usize) -> usize {
    let varea = match leaf.value(i) {
        LeafValue::Inline(v) => v.len(),
        LeafValue::Overflow { .. } => 8,
    };
    even(LEAF_NODE_HEADER + leaf.key(i).len() + varea) + 2
}

/// An owned branch cell (separator + child). `key` is empty for node 0.
pub(super) struct OwnedBranchCell {
    pub(super) key: Vec<u8>,
    pub(super) child: u64,
}

impl OwnedBranchCell {
    pub(super) fn used(&self) -> usize {
        even(BRANCH_NODE_HEADER + self.key.len()) + 2
    }
}

/// §6.4 split-point policy: median then fit-adjust, over the post-insert
/// sequence's `used` sizes (`cell + 2` each). Index `s` belongs to the right
/// page (deterministic tie-break). Cited, not re-derived (ADR-0004); the
/// monotonicity/termination argument is SPEC 03 §6.4's.
pub(super) fn choose_split(sizes: &[usize], cap: usize) -> usize {
    let n = sizes.len();
    debug_assert!(n >= 2, "splitting fewer than two cells");
    let total: usize = sizes.iter().sum();
    // prefix[s] = used(L(s)).
    let mut prefix = Vec::with_capacity(n + 1);
    let mut acc = 0usize;
    prefix.push(0);
    for &sz in sizes {
        acc += sz;
        prefix.push(acc);
    }
    let mut s = n / 2; // (nkeys + 1) / 2 over the post-insert sequence
    loop {
        let left = prefix[s];
        let right = total - left;
        if left <= cap && right <= cap {
            debug_assert!(s >= 1 && s < n, "split must leave both sides non-empty");
            return s;
        }
        if left > cap {
            s -= 1;
        } else {
            s += 1;
        }
    }
}

impl<'env> RwTxn<'env> {
    pub(super) fn put_tree(
        &mut self,
        tree: TreeId,
        key: &[u8],
        flags: PutFlags,
        val: ValSrc<'_>,
    ) -> Result<ReserveLoc> {
        self.put_tree_flagged(tree, key, flags, val, 0)
    }

    /// As [`RwTxn::put_tree`] but with an explicit leaf-node flag (`F_SUBDATA`
    /// for a named-DB catalog entry, SPEC 02 §6). `node_flags` is applied only
    /// when the entry is *inserted* fresh; a same-size in-place overwrite keeps
    /// the existing node flags (so a catalog record update never disturbs
    /// `F_SUBDATA`).
    pub(super) fn put_tree_flagged(
        &mut self,
        tree: TreeId,
        key: &[u8],
        flags: PutFlags,
        val: ValSrc<'_>,
        node_flags: u16,
    ) -> Result<ReserveLoc> {
        self.guard_ok()?;
        // SPEC 03 §6 / §2.1: writes validate up front — empty or > 511-byte
        // key, oversized value → BadValSize (the split machinery's termination
        // proof relies on the key bound).
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        if val.len() as u64 > MAX_DATA_SIZE as u64 {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        // Sequential-writes trees (ADR-0015) take the finger-aware copy of
        // this path, out of line, so a tree with the setting off pays only
        // this one branch.
        if self.seq_writes_on(tree) {
            return self.put_tree_seq(tree, key, flags, val, node_flags);
        }
        if flags.contains(PutFlags::APPEND) {
            return self.append_tree(tree, key, val, false);
        }
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let found = match self.search_path_into(tree, key, &mut path) {
            Ok(found) => found,
            Err(e) => {
                work_mut!(self).path_buf = path;
                return Err(e);
            }
        };
        if found && flags.contains(PutFlags::NO_OVERWRITE) {
            // §S2: no mutation, nothing dirtied.
            work_mut!(self).path_buf = path;
            return Err(Error::Mdb(MdbError::KeyExist));
        }
        let res = self.put_apply(tree, &mut path, found, key, val, node_flags);
        work_mut!(self).path_buf = path;
        if res.is_err() {
            // Mid-mutation failure (MapFull in a split cascade, corrupt page):
            // the working tree may be partial — poison the txn (TXN-59 clean
            // abort is the only exit).
            self.errored = true;
        }
        res
    }

    /// [`RwTxn::put_tree_flagged`] for a tree with the sequential-writes
    /// setting on (SPEC 03 §6.6; ADR-0015), after validation. The finger is
    /// tested before the APPEND dispatch: a hit proves `key >` the tree's
    /// last key, which is APPEND's §6.3 validation and (for plain puts) proof
    /// the key is absent, so one fast path serves every flag combination. A
    /// miss runs the same descent as the default path and establishes the
    /// finger from it.
    #[inline(never)]
    pub(super) fn put_tree_seq(
        &mut self,
        tree: TreeId,
        key: &[u8],
        flags: PutFlags,
        val: ValSrc<'_>,
        node_flags: u16,
    ) -> Result<ReserveLoc> {
        if let Some(res) = self.put_finger(tree, key, flags, val, node_flags) {
            return res;
        }
        if flags.contains(PutFlags::APPEND) {
            return self.append_tree(tree, key, val, true);
        }
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let found = match self.search_path_into(tree, key, &mut path) {
            Ok(found) => found,
            Err(e) => {
                work_mut!(self).path_buf = path;
                return Err(e);
            }
        };
        if found && flags.contains(PutFlags::NO_OVERWRITE) {
            work_mut!(self).path_buf = path;
            return Err(Error::Mdb(MdbError::KeyExist));
        }
        let before = *self.record(tree);
        let res = self.put_apply(tree, &mut path, found, key, val, node_flags);
        if res.is_ok() && !found {
            // A fresh insert may have landed at the end of the tree:
            // establish the finger from this descent (§6.6 F2).
            self.finger_consider(tree, &path, &before);
        }
        work_mut!(self).path_buf = path;
        if res.is_err() {
            self.errored = true;
        }
        res
    }

    /// APPEND (§6.3): compare against the **last** key only; the cursor/search
    /// position is irrelevant. Equal-to-last is `KeyExist`, not an overwrite.
    ///
    /// `seq` is the tree's sequential-writes setting (ADR-0015): when on, a
    /// successful append establishes the rightmost-leaf finger.
    pub(super) fn append_tree(
        &mut self,
        tree: TreeId,
        key: &[u8],
        val: ValSrc<'_>,
        seq: bool,
    ) -> Result<ReserveLoc> {
        if self.record(tree).root == PGNO_INVALID {
            let res = self.insert_first(tree, key, val, 0);
            match res {
                Ok(_) => sat_add(&mut self.record_mut(tree).entries, 1),
                Err(_) => self.errored = true,
            }
            return res;
        }
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let res = match self.rightmost_path_into(tree, key, &mut path) {
            Ok(Some(owned)) if seq => {
                let before = *self.record(tree);
                let res = self.append_apply(tree, &mut path, owned, key, val);
                if res.is_ok() {
                    // A fully-touched right-spine descent that did not split
                    // is exactly the finger invariant (§6.6 F2).
                    self.finger_consider(tree, &path, &before);
                }
                res
            }
            Ok(Some(owned)) => self.append_apply(tree, &mut path, owned, key, val),
            Ok(None) => {
                work_mut!(self).path_buf = path;
                return Err(Error::Mdb(MdbError::KeyExist));
            }
            Err(e) => {
                work_mut!(self).path_buf = path;
                return Err(e);
            }
        };
        work_mut!(self).path_buf = path;
        if res.is_err() {
            self.errored = true;
        }
        res
    }

    /// `owned`: every page on `path` is already a frame of this txn
    /// ([`rightmost_path_into`](Self::rightmost_path_into)), so there is
    /// nothing to copy on write.
    pub(super) fn append_apply(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        owned: bool,
        key: &[u8],
        val: ValSrc<'_>,
    ) -> Result<ReserveLoc> {
        if !owned {
            self.touch_path(tree, path)?;
        }
        // The rightmost descent left the leaf frame on its last cell, and a
        // COW copy is byte-identical: the append slot is one past it, as
        // LMDB's APPEND bumps `mc_ki` after `mdb_cursor_last`, with no second
        // view of the leaf.
        let leaf = path.last_mut().expect("non-empty path");
        leaf.1 += 1;
        let (lpg, n) = *leaf;
        debug_assert_eq!(
            LeafRef::new_prevalidated(self.dirty.bytes(lpg).expect("leaf touched"), self.psize)
                .map(|l| l.num_keys())
                .ok(),
            Some(n),
            "the rightmost descent named the leaf's last cell"
        );
        let loc = self.insert_into_leaf(tree, path, n, key, val, true, 0)?;
        sat_add(&mut self.record_mut(tree).entries, 1);
        Ok(loc)
    }

    pub(super) fn put_apply(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        found: bool,
        key: &[u8],
        val: ValSrc<'_>,
        node_flags: u16,
    ) -> Result<ReserveLoc> {
        if path.is_empty() {
            // Empty tree: first insert allocates the root leaf (§9 grow).
            let loc = self.insert_first(tree, key, val, node_flags)?;
            sat_add(&mut self.record_mut(tree).entries, 1);
            return Ok(loc);
        }
        self.touch_path(tree, path)?;
        let (lpg, ki) = *path.last().expect("non-empty path");
        if !found {
            let loc = self.insert_into_leaf(tree, path, ki, key, val, false, node_flags)?;
            sat_add(&mut self.record_mut(tree).entries, 1);
            return Ok(loc);
        }
        // §6.1 replace. Read the old value's shape first: `Err(dsize)` for an
        // inline value, `Ok((head, dsize))` for a BIGDATA one.
        let (old_big, new_inline) = {
            let frame = self.dirty.bytes(lpg).expect("leaf touched");
            let leaf = LeafRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
            let old_big = match leaf.value(ki) {
                LeafValue::Inline(v) => Err(v.len() as u32),
                LeafValue::Overflow { head_pgno, dsize } => Ok((head_pgno, dsize)),
            };
            let new_inline = value_is_inline(key.len(), val.len() as u64, self.psize);
            (old_big, new_inline)
        };
        match old_big {
            Err(old_dsize) if new_inline && old_dsize as usize == val.len() => {
                // Same-size inline fast path: overwrite the value bytes in
                // place (§6.1; the `MDB_RESERVE`/`put_current` fast path).
                let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
                let (off, dsize) = {
                    let leaf =
                        LeafMut::from_valid(&mut *frame, self.psize).map_err(map_page_err)?;
                    leaf.inline_value_at(ki).map_err(map_page_err)?
                };
                if let ValSrc::Val(v) = val {
                    frame[off..off + dsize as usize].copy_from_slice(v);
                }
                return Ok(ReserveLoc::Inline {
                    at: Some((lpg, ki)),
                });
            }
            _ => {}
        }
        // Different size/shape: delete the old node (freeing an old overflow
        // run, §8) and insert the new one at the same slot (§6.1).
        if let Ok((head, dsize)) = old_big {
            let n = overflow_page_count(dsize as u64, self.psize);
            self.free_run(head, n);
            sat_sub(&mut self.record_mut(tree).overflow_pages, n);
        }
        {
            let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
            LeafMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .remove(ki)
                .map_err(map_page_err)?;
        }
        // entries unchanged: replace, not insert.
        self.insert_into_leaf(tree, path, ki, key, val, false, node_flags)
    }

    /// First insert into an empty tree (§9 grow: empty → 1 leaf).
    pub(super) fn insert_first(
        &mut self,
        tree: TreeId,
        key: &[u8],
        val: ValSrc<'_>,
        node_flags: u16,
    ) -> Result<ReserveLoc> {
        let pg = self.allocate(1)?;
        {
            let frame = self.dirty.insert_tree_frame(pg);
            LeafMut::init(frame, self.psize, pg, self.txnid).map_err(map_page_err)?;
        }
        let rec = self.record_mut(tree);
        rec.root = pg;
        rec.depth = 1;
        rec.leaf_pages = 1;
        let mut path = vec![(pg, 0usize)];
        self.insert_into_leaf(tree, &mut path, 0, key, val, false, node_flags)
    }

    /// Insert a `(key, val)` cell at slot `ki` of the (dirty) leaf at
    /// `path.last()` (§6.2), splitting when full (§6.4; `append` forces the
    /// append split policy §6.3). BIGDATA values allocate their run first
    /// (SPEC 02 §4.2 inline rule).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert_into_leaf(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        ki: usize,
        key: &[u8],
        val: ValSrc<'_>,
        append: bool,
        node_flags: u16,
    ) -> Result<ReserveLoc> {
        let psize = self.psize;
        if value_is_inline(key.len(), val.len() as u64, psize) {
            let (lpg, _) = *path.last().expect("non-empty path");
            let r = {
                let frame = self.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let mut leaf = LeafMut::from_valid(frame, psize).map_err(map_page_err)?;
                match &val {
                    ValSrc::Val(v) => leaf.insert_inline(ki, key, node_flags, v),
                    ValSrc::Reserve(n) => {
                        leaf.insert_inline_reserved(ki, key, *n as u32).map(|_| ())
                    }
                }
            };
            match r {
                Ok(()) => Ok(ReserveLoc::Inline {
                    at: Some((lpg, ki)),
                }),
                Err(PageError::PageFull { .. }) => {
                    // Split (§6.2/§6.4). A RESERVE that splits packs as a
                    // zero-filled placeholder the caller then overwrites
                    // (TXN-47: fresh frames are zero-initialized, so the
                    // region is zeroed without a scratch buffer).
                    let nv = match &val {
                        ValSrc::Val(v) => NewLeafVal::Inline(v),
                        ValSrc::Reserve(n) => NewLeafVal::ZeroReserve(*n),
                    };
                    self.split_leaf(tree, path, ki, key, nv, node_flags, append)?;
                    // The cell may have landed on either split half.
                    Ok(ReserveLoc::Inline { at: None })
                }
                Err(e) => Err(map_page_err(e)),
            }
        } else {
            // Overflow run first (§6.2), then the BIGDATA pointer cell. The
            // zeroed run frame comes from the store: a contiguous heap box
            // (TXN-41), or the map region at `head` under in-place WRITE_MAP
            // (ADR-0021 — the payload is written once, at its final offset).
            let dsize = val.len() as u32;
            let n = overflow_page_count(dsize as u64, psize);
            let head = self.allocate(n)?;
            let ps = psize as usize;
            let payload: &[u8] = match &val {
                ValSrc::Val(v) => v,
                ValSrc::Reserve(_) => &[],
            };
            {
                let run = self.dirty.insert_run_frame(head, n);
                let written = write_overflow_head(run, psize, head, self.txnid, n as u32, payload)
                    .map_err(map_page_err)?;
                if payload.len() > written {
                    let rest = &payload[written..];
                    run[ps..ps + rest.len()].copy_from_slice(rest);
                }
            }
            sat_add(&mut self.record_mut(tree).overflow_pages, n);
            let (lpg, _) = *path.last().expect("non-empty path");
            let r = {
                let frame = self.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let mut leaf = LeafMut::from_valid(frame, psize).map_err(map_page_err)?;
                leaf.insert_bigdata(ki, key, dsize, head)
            };
            match r {
                Ok(()) => Ok(ReserveLoc::Big(head)),
                Err(PageError::PageFull { .. }) => {
                    self.split_leaf(
                        tree,
                        path,
                        ki,
                        key,
                        NewLeafVal::Big { head, dsize },
                        node_flags,
                        append,
                    )?;
                    Ok(ReserveLoc::Big(head))
                }
                Err(e) => Err(map_page_err(e)),
            }
        }
    }

    pub(super) fn extract_branch_cells(&self, pgno: u64) -> Result<Vec<OwnedBranchCell>> {
        let frame = self.dirty.bytes(pgno).expect("page is dirty");
        let br = BranchRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
        let mut cells = Vec::with_capacity(br.num_keys());
        for i in 0..br.num_keys() {
            cells.push(OwnedBranchCell {
                key: br.key(i).to_vec(),
                child: br.child_pgno(i),
            });
        }
        Ok(cells)
    }

    /// Rewrite the (dirty) branch frame at `pgno` from owned cells. `cells[0]`
    /// carries the empty separator (node 0).
    pub(super) fn write_branch_frame(
        &mut self,
        pgno: u64,
        cells: &[OwnedBranchCell],
    ) -> Result<()> {
        let psize = self.psize;
        let txnid = self.txnid;
        let frame = self.dirty.bytes_mut(pgno).expect("page is dirty");
        let mut br = BranchMut::init(frame, psize, pgno, txnid).map_err(map_page_err)?;
        for (i, c) in cells.iter().enumerate() {
            let sep: &[u8] = if i == 0 { &[] } else { &c.key };
            br.insert(i, sep, c.child).map_err(map_page_err)?;
        }
        Ok(())
    }

    /// Leaf split (§6.2): distribute the post-insert cells around `s` (§6.4),
    /// keep the left half in the existing (dirty) frame, put the right half on
    /// a fresh page, and rise the right page's first key into the parent.
    ///
    /// **End-of-page rules** (§6.4), as the fork's `mdb_page_split`, for a new
    /// cell that lands at the end of the page (`newindx == nkeys`):
    /// - an APPEND forces `s = nkeys`: all existing cells stay on the left
    ///   (dirty) frame and the new cell alone starts the right page;
    /// - a plain put forces `s = nkeys - 1`: the old last cell and the new one
    ///   form the right page, every other cell stays left.
    ///
    /// Both keep ascending loads' leaves nearly full (milli's dominant
    /// pattern). All non-end inserts keep the median-fit-adjust rule
    /// (`choose_split`).
    #[allow(clippy::too_many_arguments)] // house pattern (insert_into_leaf): pre-resolved scalars
    pub(super) fn split_leaf(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        newindx: usize,
        key: &[u8],
        val: NewLeafVal<'_>,
        node_flags: u16,
        append: bool,
    ) -> Result<()> {
        let psize = self.psize;
        let top = path.len() - 1;
        let (lpg, _) = path[top];
        let n_old = {
            let frame = self.dirty.bytes(lpg).expect("leaf is dirty");
            LeafRef::new_prevalidated(frame, psize)
                .map_err(map_page_err)?
                .num_keys()
        };

        // A plain put at the end moves the old last cell along with the new
        // one (§6.4). It needs two old cells, and both cells must fit on one
        // page, which `max_node_size` guarantees (SPEC 02 §4.2); otherwise
        // the new cell goes alone, as for an APPEND.
        let end_pair = !append && newindx == n_old && n_old >= 2 && {
            let frame = self.dirty.bytes(lpg).expect("leaf is dirty");
            let leaf = LeafRef::new_prevalidated(frame, psize).map_err(map_page_err)?;
            leaf_cell_used(&leaf, n_old - 1) + val.used(key.len()) <= body_size(psize)
        };
        // End split (§6.4) without repacking the left frame. APPEND, or a
        // plain put whose pair does not fit: the new cell ALONE forms the
        // right page and the left frame is not touched at all. A plain put:
        // the old last cell moves to the right page first, removed from the
        // left frame in place. On an ascending page that cell is the top of
        // the cell heap, so removing it moves no other cell.
        if append || newindx == n_old {
            enum Moved {
                Inline(Vec<u8>),
                Big { head: u64, dsize: u32 },
            }
            let moved = if end_pair {
                let frame = self.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let i = n_old - 1;
                let (mkey, mflags, mval) = {
                    let leaf = LeafRef::new_prevalidated(&*frame, psize).map_err(map_page_err)?;
                    // Preserve non-BIGDATA node flags (F_SUBDATA) across the
                    // move (SPEC 02 §6); F_BIGDATA is re-derived on write.
                    let flags = leaf.node_flags(i) & !crate::page::F_BIGDATA;
                    let v = match leaf.value(i) {
                        LeafValue::Inline(v) => Moved::Inline(v.to_vec()),
                        LeafValue::Overflow { head_pgno, dsize } => Moved::Big {
                            head: head_pgno,
                            dsize,
                        },
                    };
                    (leaf.key(i).to_vec(), flags, v)
                };
                LeafMut::from_valid(&mut *frame, psize)
                    .map_err(map_page_err)?
                    .remove(i)
                    .map_err(map_page_err)?;
                Some((mkey, mflags, mval))
            } else {
                None
            };
            let rpg = self.allocate(1)?;
            {
                let txnid = self.txnid;
                let frame = self.dirty.insert_tree_frame(rpg);
                let mut leaf = LeafMut::init(frame, psize, rpg, txnid).map_err(map_page_err)?;
                let slot = match &moved {
                    Some((mkey, mflags, Moved::Inline(v))) => {
                        leaf.insert_inline(0, mkey, *mflags, v)
                            .map_err(map_page_err)?;
                        1
                    }
                    Some((mkey, _, Moved::Big { head, dsize })) => {
                        leaf.insert_bigdata(0, mkey, *dsize, *head)
                            .map_err(map_page_err)?;
                        1
                    }
                    None => 0,
                };
                match &val {
                    NewLeafVal::Inline(v) => leaf.insert_inline(slot, key, node_flags, v),
                    NewLeafVal::ZeroReserve(n) => leaf
                        .insert_inline_reserved(slot, key, *n as u32)
                        .map(|_| ()),
                    NewLeafVal::Big { head, dsize } => {
                        leaf.insert_bigdata(slot, key, *dsize, *head)
                    }
                }
                .map_err(map_page_err)?;
            }
            sat_add(&mut self.record_mut(tree).leaf_pages, 1);
            // sep = first key of R (§6.4): the moved old last key, or the new key.
            let (sep, slot) = match moved {
                Some((mkey, _, _)) => (mkey, 1),
                None => (key.to_vec(), 0),
            };
            // The new cell is on the right page, after the moved cell if any.
            return self.rise_leaf_split(tree, path, sep, rpg, (rpg, slot));
        }

        // General split: pack both frames from cells **borrowed** out of the
        // old frame — removed from the store as an owned, address-stable
        // `Box` — plus the caller's new cell. No owned per-cell copies.
        let rpg = self.allocate(1)?;
        let old = self.dirty.remove(lpg).expect("leaf is dirty");
        let oldleaf = LeafRef::new_prevalidated(&old, psize).map_err(map_page_err)?;
        let total = n_old + 1;
        let mut sizes = Vec::with_capacity(total);
        for j in 0..total {
            sizes.push(match j.cmp(&newindx) {
                Ordering::Less => leaf_cell_used(&oldleaf, j),
                Ordering::Equal => val.used(key.len()),
                Ordering::Greater => leaf_cell_used(&oldleaf, j - 1),
            });
        }
        let cap = body_size(psize);
        let s = choose_split(&sizes, cap);
        // sep = first key of R (§6.4).
        let sep = match s.cmp(&newindx) {
            Ordering::Less => oldleaf.key(s).to_vec(),
            Ordering::Equal => key.to_vec(),
            Ordering::Greater => oldleaf.key(s - 1).to_vec(),
        };
        self.pack_leaf_range(lpg, &oldleaf, newindx, key, &val, node_flags, 0, s)?;
        self.pack_leaf_range(rpg, &oldleaf, newindx, key, &val, node_flags, s, total)?;
        drop(old);
        sat_add(&mut self.record_mut(tree).leaf_pages, 1);
        // Post-insert item `newindx` is the new cell: slot `newindx` of the
        // left half, or `newindx - s` of the right one.
        let cell = if newindx < s {
            (lpg, newindx)
        } else {
            (rpg, newindx - s)
        };
        self.rise_leaf_split(tree, path, sep, rpg, cell)
    }

    /// Rise a leaf split's separator into the parent (or grow the root), then
    /// point the leaf frame of `path` at `cell`, the `(pgno, slot)` the split
    /// gave the cell just inserted. Every branch frame above follows it
    /// through any split the insert cascades into (SPEC 03 §5.4b — LMDB fixes
    /// up the splitting cursor the same way after `mdb_page_split`), so
    /// `path` stays the root-to-leaf path to that cell; only a root grow
    /// leaves the new root's frame for the caller to add
    /// ([`put_at_path`](Self::put_at_path)).
    pub(super) fn rise_leaf_split(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        sep: Vec<u8>,
        rpg: u64,
        cell: (u64, usize),
    ) -> Result<()> {
        let top = path.len() - 1;
        let to_right = cell.0 == rpg;
        if top == 0 {
            self.insert_into_branch(tree, path, -1, 0, sep, rpg, Some(to_right))?;
        } else {
            let at = path[top - 1].1 + 1;
            self.insert_into_branch(tree, path, top as isize - 1, at, sep, rpg, Some(to_right))?;
        }
        // Written last: a root grow above reads `path[0]` as the old root.
        path[top] = cell;
        Ok(())
    }

    /// Pack post-insert items `[from, to)` into a fresh frame at `pgno`
    /// (no owned-cell materialization): item `j` is the caller's new cell when `j == newindx`,
    /// else old cell `j`/`j-1` borrowed from `old`. The frame is
    /// zero-initialized by `insert_tree_frame`, so `ZeroReserve` regions are
    /// zero-filled without a scratch buffer (TXN-47).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn pack_leaf_range(
        &mut self,
        pgno: u64,
        old: &LeafRef<'_>,
        newindx: usize,
        key: &[u8],
        val: &NewLeafVal<'_>,
        node_flags: u16,
        from: usize,
        to: usize,
    ) -> Result<()> {
        let psize = self.psize;
        let txnid = self.txnid;
        let frame = self.dirty.insert_tree_frame(pgno);
        let mut leaf = LeafMut::init(frame, psize, pgno, txnid).map_err(map_page_err)?;
        for (slot, j) in (from..to).enumerate() {
            let r = match j.cmp(&newindx) {
                Ordering::Equal => match val {
                    NewLeafVal::Inline(v) => leaf.insert_inline(slot, key, node_flags, v),
                    NewLeafVal::ZeroReserve(n) => leaf
                        .insert_inline_reserved(slot, key, *n as u32)
                        .map(|_| ()),
                    NewLeafVal::Big { head, dsize } => {
                        leaf.insert_bigdata(slot, key, *dsize, *head)
                    }
                },
                other => {
                    let i = if other == Ordering::Less { j } else { j - 1 };
                    // Preserve non-BIGDATA node flags (F_SUBDATA) across the
                    // rewrite (SPEC 02 §6); F_BIGDATA is re-derived on write.
                    let flags = old.node_flags(i) & !crate::page::F_BIGDATA;
                    match old.value(i) {
                        LeafValue::Inline(v) => leaf.insert_inline(slot, old.key(i), flags, v),
                        LeafValue::Overflow { head_pgno, dsize } => {
                            leaf.insert_bigdata(slot, old.key(i), dsize, head_pgno)
                        }
                    }
                }
            };
            r.map_err(map_page_err)?;
        }
        Ok(())
    }

    /// Insert `(key -> child)` at index `at` of the branch at `path[level]`,
    /// splitting recursively (§6.5) up to a root split (§9 grow) at
    /// `level < 0`. Returns whether any page split (or the root grew) on the
    /// way — the signal that a kept cursor path can no longer be proven
    /// correct (SPEC 03 §5.4a).
    ///
    /// Optionally keeps `path` on a put's new cell (SPEC 03 §5.4b). `follow` is `None` for
    /// callers whose path does not survive a split (it is then left as is),
    /// or `Some(new)` when the cell is under the inserted `child` (`new`) or
    /// under the frame's own child, whose index the insert does not move
    /// (`at` is right after it). The frame at `level` is then rewritten to
    /// the slot, on whichever half, of the child holding the cell.
    #[allow(clippy::too_many_arguments)] // house pattern (insert_into_leaf): pre-resolved scalars
    pub(super) fn insert_into_branch(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        level: isize,
        at: usize,
        key: Vec<u8>,
        child: u64,
        follow: Option<bool>,
    ) -> Result<bool> {
        if level < 0 {
            // Root split (§9): new root branch over (old root, risen key).
            let old_root = path[0].0;
            let np = self.allocate(1)?;
            {
                let psize = self.psize;
                let txnid = self.txnid;
                let frame = self.dirty.insert_tree_frame(np);
                let mut br = BranchMut::init(frame, psize, np, txnid).map_err(map_page_err)?;
                br.insert(0, &[], old_root).map_err(map_page_err)?;
                br.insert(1, &key, child).map_err(map_page_err)?;
            }
            let rec = self.record_mut(tree);
            rec.root = np;
            rec.depth = rec.depth.saturating_add(1);
            sat_add(&mut rec.branch_pages, 1);
            return Ok(true);
        }
        let lvl = level as usize;
        debug_assert!(at >= 1, "branch inserts never target node 0");
        let (ppg, pki) = path[lvl];
        // The followed child's index among the post-insert cells.
        let follow = follow.map(|new| {
            debug_assert!(
                new || at > pki,
                "the insert must not shift the followed child"
            );
            if new {
                at
            } else {
                pki
            }
        });
        let r = {
            let frame = self.dirty.bytes_mut(ppg).expect("branch is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .insert(at, &key, child)
        };
        match r {
            Ok(()) => {
                if let Some(x) = follow {
                    path[lvl].1 = x;
                }
                Ok(false)
            }
            Err(PageError::PageFull { .. }) => {
                self.split_branch(tree, path, lvl, at, key, child, follow)
            }
            Err(e) => Err(map_page_err(e)),
        }
    }

    /// Branch split (§6.5): the split key rises and is **removed from both
    /// children** — the right page's node 0 keeps the rising key's child under
    /// the empty separator. Always returns `true` (it *is* a split), after
    /// propagating the rising key.
    ///
    /// **End-of-page insert-point rule** (§6.4): when the new cell lands at
    /// the end of the page (`at == nkeys`, the pattern every leaf split of an
    /// ascending load produces in its parent) the split point is
    /// `s = nkeys - 1`: the old last cell rises and its child becomes the
    /// right page's node 0, the new cell is node 1 — exactly two children on
    /// the right page, all other children stay on the left. This is the
    /// fork's `mdb_page_split` split point for an end insert on a branch, and
    /// it keeps branches packed on ascending workloads where the median rule
    /// leaves them about half full. A branch must keep two children (INV-8),
    /// so the new cell cannot start the right page alone as it does for an
    /// APPEND on a leaf. Non-end inserts keep the median-fit-adjust rule (`choose_split`).
    ///
    /// `follow` is the post-insert index of the child a put's path follows
    /// ([`insert_into_branch`](Self::insert_into_branch)), or
    /// `None` to leave `path` as is.
    #[allow(clippy::too_many_arguments)] // house pattern (insert_into_leaf): pre-resolved scalars
    pub(super) fn split_branch(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        lvl: usize,
        at: usize,
        key: Vec<u8>,
        child: u64,
        follow: Option<usize>,
    ) -> Result<bool> {
        let (ppg, _) = path[lvl];
        let mut cells = self.extract_branch_cells(ppg)?;
        let n_old = cells.len();
        cells.insert(at, OwnedBranchCell { key, child });
        // End insert (§6.4): left keeps the first `n_old - 1` children, the
        // right page gets the old last child plus the new one. Both sides fit
        // by construction: the left is a subset of the full page, and the
        // right holds one key-less node 0 plus one cell whose key is at most
        // `MAX_KEY_SIZE` bytes, far below an empty page's capacity. The
        // `n_old >= 3` guard keeps the left at two or more children (INV-8);
        // a full branch always holds more than that at supported page sizes,
        // but the median rule is the safe fallback.
        let s = if at == n_old && n_old >= 3 {
            n_old - 1
        } else {
            let cap = body_size(self.psize);
            let sizes: Vec<usize> = cells.iter().map(OwnedBranchCell::used).collect();
            choose_split(&sizes, cap)
        };
        let rising = cells[s].key.clone();
        let rpg = self.allocate(1)?;
        self.dirty.insert_tree_frame(rpg);
        // Right page: node 0 = (empty key, rising cell's child), then the tail.
        let mut right: Vec<OwnedBranchCell> = Vec::with_capacity(cells.len() - s);
        right.push(OwnedBranchCell {
            key: Vec::new(),
            child: cells[s].child,
        });
        for c in cells.drain(s + 1..) {
            right.push(c);
        }
        cells.truncate(s); // left keeps [0, s)
        self.write_branch_frame(ppg, &cells)?;
        self.write_branch_frame(rpg, &right)?;
        sat_add(&mut self.record_mut(tree).branch_pages, 1);
        // Children `[0, s)` stay left at their index; child `s` (the rising
        // cell's) is the right page's node 0, and the tail follows it.
        let followed = follow.map(|x| if x < s { (ppg, x) } else { (rpg, x - s) });
        let up = followed.map(|(pg, _)| pg == rpg);
        if lvl == 0 {
            self.insert_into_branch(tree, path, -1, 0, rising, rpg, up)?;
        } else {
            let at2 = path[lvl - 1].1 + 1;
            self.insert_into_branch(tree, path, lvl as isize - 1, at2, rising, rpg, up)?;
        }
        // Written last: a root grow above reads `path[0]` as the old root.
        if let Some(frame) = followed {
            path[lvl] = frame;
        }
        Ok(true)
    }
}
