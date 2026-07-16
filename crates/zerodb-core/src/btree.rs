//! B+tree read path — search, `get`, and the full cursor state machine
//! ([SPEC 03](../../../../docs/SPEC/03-btree.md) §2–§4). Milestones 1.3/1.4.
//!
//! Everything here operates over a [`Source`] — either the immutable mapped
//! env file (`Source::Map`, the M1.3 read path) or a write txn's view
//! (`Source::Writer`: the dirty-page store first, the map for untouched pages
//! — SPEC 04 TXN-38, ADR-0004 D2). Reads are **zero-copy**: keys and values
//! are `&'a [u8]` borrowed straight from the backing bytes (SPEC 04 TXN-37/41),
//! and a `F_BIGDATA` value resolves to one contiguous slice spanning its
//! overflow run (SPEC 03 §3; a dirty run is one contiguous frame, TXN-41).
//! This module contains **no** `unsafe` (the crate is `#![forbid(unsafe_code)]`)
//! and no I/O — it is pure logic over borrowed bytes, so `miri` exercises it.
//!
//! The cursor is a root-to-leaf path (`stack` of `(pgno, ki)` frames) plus the
//! `INITIALIZED`/`EOF` flags of SPEC 03 §4. Each public op documents the §4
//! subsection whose positioning/EOF/empty-DB semantics it implements.

use super::dirty::DirtyStore;
use super::page::{LeafRef, LeafValue, OverflowRef, PageError, PageRef, PageType, PGNO_INVALID};

/// An entry `(key, value)` borrowed from the map for the view's lifetime `'a`.
pub type Entry<'a> = (&'a [u8], &'a [u8]);

/// Result of a cursor/positioning op: `Ok(Some(entry))`, `Ok(None)` (EOF /
/// empty / no-such), or a decode error on a structurally-corrupt tree.
pub type PosResult<'a> = Result<Option<Entry<'a>>, PageError>;

// ---------------------------------------------------------------------------
// Page source (ADR-0004 D2): where a page's bytes come from
// ---------------------------------------------------------------------------

/// Where tree pages are read from (SPEC 04 TXN-37/38).
///
/// `Map` is a read snapshot over the mapped file; `Writer` is a write txn's
/// view, which resolves the **dirty-page store first** and falls back to the
/// map for pages the txn has not touched. A caller cannot tell which backing a
/// borrow came from; the lifetime rules are identical (SPEC 04 §6.2). This is
/// also the M1.9 seam (a nested reader is the writer's source, read-only) and
/// the M1.10 seam (WRITE_MAP swaps the backing, TXN-46).
#[derive(Clone, Copy)]
pub enum Source<'a> {
    /// The read-only mapped env file.
    Map {
        /// The whole mapped region.
        bytes: &'a [u8],
    },
    /// A write txn's view: dirty frames first, then the map.
    Writer {
        /// The txn's dirty-page store (checked first).
        dirty: &'a DirtyStore,
        /// The mapped region (fallback for untouched pages).
        bytes: &'a [u8],
    },
}

impl<'a> Source<'a> {
    /// Bytes beginning at page `pgno`: at least one page; for an overflow head
    /// resolved from this source, the returned slice covers the whole run
    /// (a dirty run is its full contiguous frame; a mapped run extends to the
    /// end of the map, bounded by the overflow decoder).
    pub(crate) fn bytes_from(&self, psize: u32, pgno: u64) -> Result<&'a [u8], PageError> {
        let ps = psize as usize;
        let map_slice = |bytes: &'a [u8]| -> Result<&'a [u8], PageError> {
            let base = (pgno as usize)
                .checked_mul(ps)
                .ok_or(PageError::BufferTooSmall { got: 0, psize: ps })?;
            bytes
                .get(base..)
                .filter(|s| s.len() >= ps)
                .ok_or(PageError::BufferTooSmall { got: 0, psize: ps })
        };
        match self {
            Source::Map { bytes } => map_slice(bytes),
            Source::Writer { dirty, bytes } => match dirty.bytes(pgno) {
                Some(frame) => Ok(frame),
                None => map_slice(bytes),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Page loading + value resolution
// ---------------------------------------------------------------------------

/// Load the `psize`-byte page `pgno` from the source as a validated [`PageRef`].
fn load_page<'a>(src: Source<'a>, psize: u32, pgno: u64) -> Result<PageRef<'a>, PageError> {
    PageRef::new(src.bytes_from(psize, pgno)?, psize)
}

/// Resolve the value of leaf entry `i` to a contiguous `&'a [u8]` (SPEC 03 §3):
/// inline values borrow the leaf page; `F_BIGDATA` values borrow the overflow
/// run, sliced from the head page across the whole run.
fn resolve_value<'a>(
    src: Source<'a>,
    psize: u32,
    leaf: &LeafRef<'a>,
    i: usize,
) -> Result<&'a [u8], PageError> {
    match leaf.value(i) {
        LeafValue::Inline(v) => Ok(v),
        LeafValue::Overflow { head_pgno, dsize } => {
            let run = src.bytes_from(psize, head_pgno)?;
            OverflowRef::new(run, psize)?.payload(dsize)
        }
    }
}

// ---------------------------------------------------------------------------
// Tree — the immutable handle over one B+tree
// ---------------------------------------------------------------------------

/// An immutable view of one B+tree, rooted at `root` with height `depth`
/// (SPEC 03 §1). Cheap to copy; carries no owned state.
#[derive(Clone, Copy)]
pub struct Tree<'a> {
    src: Source<'a>,
    psize: u32,
    root: u64,
    depth: u16,
}

impl<'a> Tree<'a> {
    /// Build a tree view over `src` with the given page size, root page number
    /// (`PGNO_INVALID` for an empty tree), and height.
    #[must_use]
    pub fn new(src: Source<'a>, psize: u32, root: u64, depth: u16) -> Tree<'a> {
        Tree {
            src,
            psize,
            root,
            depth,
        }
    }

    /// Whether the tree is empty (`root == PGNO_INVALID`).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root == PGNO_INVALID
    }

    /// `get(key)` (SPEC 03 §2 tail): the value for `key` if present, else `None`
    /// (a missing key is `Ok(None)`, never an error — SPEC 00 rows 14/30). The
    /// value is resolved zero-copy, spanning an overflow run if `F_BIGDATA`.
    ///
    /// # Errors
    ///
    /// A [`PageError`] only if the tree is structurally corrupt.
    pub fn get(&self, key: &[u8]) -> Result<Option<&'a [u8]>, PageError> {
        let mut c = Cursor::new(*self);
        c.search(key)?;
        if !c.initialized {
            return Ok(None);
        }
        let (pgno, ki) = *c.stack.last().expect("initialized cursor has a leaf frame");
        let page = load_page(self.src, self.psize, pgno)?;
        let leaf = page.as_leaf()?;
        if ki < leaf.num_keys() && leaf.key(ki) == key {
            Ok(Some(resolve_value(self.src, self.psize, &leaf, ki)?))
        } else {
            Ok(None)
        }
    }

    /// Catalog lookup for the named-DB resolver (SPEC 02 §6): like [`Tree::get`]
    /// but also returns the leaf **node flags** of the matched entry, so the
    /// caller can tell an `F_SUBDATA` sub-DB record from a plain user key
    /// (a name collision → `Incompatible`). `Ok(None)` if the key is absent.
    ///
    /// # Errors
    ///
    /// A [`PageError`] only if the tree is structurally corrupt.
    pub fn get_catalog_entry(&self, key: &[u8]) -> Result<Option<(u16, &'a [u8])>, PageError> {
        let mut c = Cursor::new(*self);
        c.search(key)?;
        if !c.initialized {
            return Ok(None);
        }
        let (pgno, ki) = *c.stack.last().expect("initialized cursor has a leaf frame");
        let page = load_page(self.src, self.psize, pgno)?;
        let leaf = page.as_leaf()?;
        if ki < leaf.num_keys() && leaf.key(ki) == key {
            let flags = leaf.node_flags(ki);
            Ok(Some((
                flags,
                resolve_value(self.src, self.psize, &leaf, ki)?,
            )))
        } else {
            Ok(None)
        }
    }

    /// Open a fresh, unpositioned [`Cursor`] over this tree.
    #[must_use]
    pub fn cursor(&self) -> Cursor<'a> {
        Cursor::new(*self)
    }
}

// ---------------------------------------------------------------------------
// Cursor — the read positioning state machine (SPEC 03 §4)
// ---------------------------------------------------------------------------

/// A read cursor: a root-to-leaf path plus the `INITIALIZED`/`EOF` flags of
/// SPEC 03 §4. All positioning ops return the entry at the new position (or
/// `None` at an edge), borrowing key/value `&'a [u8]` from the source.
#[derive(Clone)]
pub struct Cursor<'a> {
    src: Source<'a>,
    psize: u32,
    root: u64,
    depth: u16,
    /// `(pgno, ki)` from root (index 0) to the current leaf (`top`). Empty when
    /// the cursor is unpositioned.
    stack: Vec<(u64, usize)>,
    /// The `INITIALIZED` flag (SPEC 03 §4): the cursor has been positioned.
    /// Cleared on before-begin (`prev` past the minimum) and on a failed exact
    /// `set`.
    initialized: bool,
    /// The `EOF` flag (SPEC 03 §4): the cursor sits past the maximum entry.
    eof: bool,
}

impl<'a> Cursor<'a> {
    fn new(t: Tree<'a>) -> Cursor<'a> {
        Cursor {
            src: t.src,
            psize: t.psize,
            root: t.root,
            depth: t.depth,
            stack: Vec::new(),
            initialized: false,
            eof: false,
        }
    }

    // -- page helpers ------------------------------------------------------

    fn page(&self, pgno: u64) -> Result<PageRef<'a>, PageError> {
        load_page(self.src, self.psize, pgno)
    }

    /// The entry at the current leaf position, or `None` if unpositioned / EOF /
    /// parked past the end of a leaf. Shared by every positioning op.
    fn current(&self) -> PosResult<'a> {
        if !self.initialized || self.eof {
            return Ok(None);
        }
        let (pgno, ki) = match self.stack.last() {
            Some(f) => *f,
            None => return Ok(None),
        };
        let leaf = self.page(pgno)?.as_leaf()?;
        if ki >= leaf.num_keys() {
            return Ok(None);
        }
        let k = leaf.key(ki);
        let v = resolve_value(self.src, self.psize, &leaf, ki)?;
        Ok(Some((k, v)))
    }

    // -- descents ----------------------------------------------------------

    /// Descend from `start` taking child 0 at every branch to the leftmost leaf,
    /// pushing frames with `ki = 0`.
    fn descend_min(&mut self, start: u64) -> Result<(), PageError> {
        let mut pgno = start;
        for _ in 0..=(self.depth as usize + 1) {
            let page = self.page(pgno)?;
            match page.page_type() {
                PageType::Leaf => {
                    self.stack.push((pgno, 0));
                    return Ok(());
                }
                PageType::Branch => {
                    let br = page.as_branch()?;
                    self.stack.push((pgno, 0));
                    pgno = br.child_pgno(0);
                }
                other => return Err(wrong_type(other)),
            }
        }
        Err(depth_exceeded())
    }

    /// Descend from `start` taking the last child at every branch to the
    /// rightmost leaf, pushing frames with `ki = num_keys − 1`.
    fn descend_max(&mut self, start: u64) -> Result<(), PageError> {
        let mut pgno = start;
        for _ in 0..=(self.depth as usize + 1) {
            let page = self.page(pgno)?;
            match page.page_type() {
                PageType::Leaf => {
                    let leaf = page.as_leaf()?;
                    let n = leaf.num_keys();
                    self.stack.push((pgno, n.saturating_sub(1)));
                    return Ok(());
                }
                PageType::Branch => {
                    let br = page.as_branch()?;
                    let last = br.num_keys().saturating_sub(1);
                    self.stack.push((pgno, last));
                    pgno = br.child_pgno(last);
                }
                other => return Err(wrong_type(other)),
            }
        }
        Err(depth_exceeded())
    }

    /// Position at the leaf that would contain `key`, `ki` = lower-bound slot
    /// (may equal `num_keys`). Leaves the cursor uninitialized on an empty tree
    /// (SPEC 03 §2).
    fn search(&mut self, key: &[u8]) -> Result<(), PageError> {
        self.stack.clear();
        self.eof = false;
        if self.root == PGNO_INVALID {
            self.initialized = false;
            return Ok(());
        }
        let mut pgno = self.root;
        for _ in 0..=(self.depth as usize + 1) {
            let page = self.page(pgno)?;
            match page.page_type() {
                PageType::Leaf => {
                    let leaf = page.as_leaf()?;
                    let ki = match leaf.lookup(key) {
                        Ok(i) | Err(i) => i,
                    };
                    self.stack.push((pgno, ki));
                    self.initialized = true;
                    return Ok(());
                }
                PageType::Branch => {
                    let br = page.as_branch()?;
                    let i = br.child_index(key);
                    self.stack.push((pgno, i));
                    pgno = br.child_pgno(i);
                }
                other => return Err(wrong_type(other)),
            }
        }
        Err(depth_exceeded())
    }

    // -- ascend helpers (shared by next / prev / set_range) ----------------

    /// From the current leaf, move to the first entry of the next leaf, or set
    /// `EOF` if there is none. Pops to the first ancestor with a further child,
    /// then descends leftmost. Does **not** read the leaf's own `ki`, so it is
    /// also used by `set_range` when parked past a leaf's end.
    fn ascend_next(&mut self) -> PosResult<'a> {
        loop {
            if self.stack.len() <= 1 {
                self.eof = true;
                return Ok(None);
            }
            self.stack.pop();
            let (bp, bki) = *self.stack.last().expect("len > 1");
            let br = self.page(bp)?.as_branch()?;
            if bki + 1 < br.num_keys() {
                self.stack.last_mut().expect("len > 1").1 = bki + 1;
                let child = br.child_pgno(bki + 1);
                self.descend_min(child)?;
                return self.current();
            }
        }
    }

    /// Symmetric to [`ascend_next`](Self::ascend_next): move to the last entry of
    /// the previous leaf, or go before-begin (uninitialized) if there is none.
    fn ascend_prev(&mut self) -> PosResult<'a> {
        loop {
            if self.stack.len() <= 1 {
                self.initialized = false;
                self.stack.clear();
                return Ok(None);
            }
            self.stack.pop();
            let (bp, bki) = *self.stack.last().expect("len > 1");
            let br = self.page(bp)?.as_branch()?;
            if bki > 0 {
                self.stack.last_mut().expect("len > 1").1 = bki - 1;
                let child = br.child_pgno(bki - 1);
                self.descend_max(child)?;
                return self.current();
            }
        }
    }

    // -- public positioning ops (SPEC 03 §4) -------------------------------

    /// `first` (`MDB_FIRST`) — SPEC 03 §4: leftmost entry; empty tree → `None`,
    /// cursor stays uninitialized. Clears `EOF`.
    pub fn first(&mut self) -> PosResult<'a> {
        self.stack.clear();
        self.eof = false;
        if self.root == PGNO_INVALID {
            self.initialized = false;
            return Ok(None);
        }
        self.descend_min(self.root)?;
        self.initialized = true;
        self.current()
    }

    /// `last` (`MDB_LAST`) — SPEC 03 §4: rightmost entry; empty tree → `None`.
    pub fn last(&mut self) -> PosResult<'a> {
        self.stack.clear();
        self.eof = false;
        if self.root == PGNO_INVALID {
            self.initialized = false;
            return Ok(None);
        }
        self.descend_max(self.root)?;
        self.initialized = true;
        self.current()
    }

    /// `next` (`MDB_NEXT`) — SPEC 03 §4: from an uninitialized cursor behaves as
    /// [`first`](Self::first); at `EOF` keeps returning `None`; otherwise
    /// advances one entry, hopping to the next leaf when the current one is
    /// exhausted.
    ///
    /// Named after `MDB_NEXT` (not [`Iterator::next`]): this is a cursor
    /// positioning op that returns a `Result`, and the SPEC 03 `next`/`prev`
    /// pair is the readable naming.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> PosResult<'a> {
        if !self.initialized {
            return self.first();
        }
        if self.eof {
            return Ok(None);
        }
        let (pgno, ki) = *self
            .stack
            .last()
            .expect("initialized cursor has a leaf frame");
        let leaf = self.page(pgno)?.as_leaf()?;
        if ki + 1 < leaf.num_keys() {
            self.stack.last_mut().expect("leaf frame").1 = ki + 1;
            return self.current();
        }
        self.ascend_next()
    }

    /// `prev` (`MDB_PREV`) — SPEC 03 §4: from an uninitialized cursor behaves as
    /// [`last`](Self::last); from `EOF` returns the maximum entry (`last`);
    /// otherwise steps back one entry, hopping to the previous leaf. Stepping
    /// past the minimum leaves the cursor before-begin (uninitialized), so a
    /// following `next` yields `first`.
    pub fn prev(&mut self) -> PosResult<'a> {
        if !self.initialized {
            return self.last();
        }
        if self.eof {
            // Past the maximum: the entry before "past the end" is the global
            // maximum (SPEC 03 §4 EOF/empty summary).
            self.eof = false;
            return self.last();
        }
        let (_, ki) = *self
            .stack
            .last()
            .expect("initialized cursor has a leaf frame");
        if ki > 0 {
            self.stack.last_mut().expect("leaf frame").1 = ki - 1;
            return self.current();
        }
        self.ascend_prev()
    }

    /// `set` (`MDB_SET`) — SPEC 03 §4: exact match. Positions on `key` and
    /// returns its entry, or `None` (LMDB `MDB_NOTFOUND`) leaving the cursor
    /// unpositioned for iteration.
    pub fn set_exact(&mut self, key: &[u8]) -> PosResult<'a> {
        self.search(key)?;
        if !self.initialized {
            return Ok(None);
        }
        let (pgno, ki) = *self
            .stack
            .last()
            .expect("initialized cursor has a leaf frame");
        let leaf = self.page(pgno)?.as_leaf()?;
        if ki < leaf.num_keys() && leaf.key(ki) == key {
            return self.current();
        }
        // Not found: leave unpositioned for iteration (SPEC 03 §4 `set`).
        self.initialized = false;
        Ok(None)
    }

    /// `set_range` (`MDB_SET_RANGE`, `≥`) — SPEC 03 §4: the first entry `≥ key`,
    /// hopping to the following leaf when `key` is past the searched leaf's end;
    /// empty tree / nothing `≥ key` → `None`.
    pub fn set_range(&mut self, key: &[u8]) -> PosResult<'a> {
        self.search(key)?;
        if !self.initialized {
            return Ok(None);
        }
        let (pgno, ki) = *self
            .stack
            .last()
            .expect("initialized cursor has a leaf frame");
        let leaf = self.page(pgno)?.as_leaf()?;
        if ki < leaf.num_keys() {
            return self.current();
        }
        // `key` is past the end of this leaf: advance to the next leaf's first
        // entry (SPEC 03 §4 `set_range`).
        self.ascend_next()
    }

    /// `get_greater_than` (`>`) — SPEC 03 §4: the least entry strictly greater
    /// than `key`. `set_range` then skip an equal hit.
    pub fn get_greater_than(&mut self, key: &[u8]) -> PosResult<'a> {
        match self.set_range(key)? {
            None => Ok(None),
            Some((k, _)) if k == key => self.next(),
            some => Ok(some),
        }
    }

    /// `get_greater_than_or_equal_to` (`≥`) — SPEC 00 SHOULD; identical to
    /// [`set_range`](Self::set_range).
    pub fn get_greater_than_or_equal_to(&mut self, key: &[u8]) -> PosResult<'a> {
        self.set_range(key)
    }

    /// `get_lower_than_or_equal_to` (`≤`) — SPEC 03 §4: the greatest entry `≤
    /// key`. `set_range`; if it overshot (or hit end) step back / take `last`.
    pub fn get_lower_than_or_equal_to(&mut self, key: &[u8]) -> PosResult<'a> {
        match self.set_range(key)? {
            None => self.last(),
            Some((k, _)) if k == key => self.current(),
            Some(_) => self.prev(),
        }
    }

    /// `get_lower_than` (`<`) — SPEC 00 SHOULD: the greatest entry strictly less
    /// than `key`. `set_range` then step back once (the first `≥ key`, whether
    /// `==` or `>`, is stepped over).
    pub fn get_lower_than(&mut self, key: &[u8]) -> PosResult<'a> {
        match self.set_range(key)? {
            None => self.last(),
            Some(_) => self.prev(),
        }
    }

    /// `get_current` (`MDB_GET_CURRENT`) — SPEC 03 §4: the entry at the current
    /// position without moving. `None` if unpositioned or at `EOF`.
    pub fn get_current(&self) -> PosResult<'a> {
        self.current()
    }

    /// The leaf **node flags** of the entry at the current position (M1.12
    /// tools/copy): lets `dump`/`copy_to_file` tell an `F_SUBDATA` named-DB
    /// catalog record apart from a plain user-data key during an in-order scan
    /// (SPEC 02 §6). `None` if unpositioned, at `EOF`, or parked past a leaf's
    /// last entry — the same positions for which [`Cursor::get_current`] yields
    /// `None`.
    ///
    /// # Errors
    ///
    /// A [`PageError`] only if the tree is structurally corrupt.
    pub fn current_flags(&self) -> Result<Option<u16>, PageError> {
        if !self.initialized || self.eof {
            return Ok(None);
        }
        let (pgno, ki) = match self.stack.last() {
            Some(f) => *f,
            None => return Ok(None),
        };
        let leaf = self.page(pgno)?.as_leaf()?;
        if ki >= leaf.num_keys() {
            return Ok(None);
        }
        Ok(Some(leaf.node_flags(ki)))
    }
}

// ---------------------------------------------------------------------------
// Prefix helper (SPEC 03 §4 prefix iteration)
// ---------------------------------------------------------------------------

/// The smallest key strictly greater than every key with prefix `p`
/// (`prefix_successor`, SPEC 03 §4): increment the last non-`0xFF` byte and drop
/// the trailing `0xFF`s. `None` when `p` is empty or all `0xFF` (no successor —
/// the prefix range extends to the end of key space).
#[must_use]
pub fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut s = p.to_vec();
    while let Some(&last) = s.last() {
        if last == 0xFF {
            s.pop();
        } else {
            let n = s.len();
            s[n - 1] = last + 1;
            return Some(s);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Error helpers (structural corruption)
// ---------------------------------------------------------------------------

fn wrong_type(found: PageType) -> PageError {
    PageError::WrongPageType {
        expected: PageType::Leaf,
        found,
    }
}

fn depth_exceeded() -> PageError {
    // A descent deeper than `depth + 1` means a cycle / bad `depth`: reject
    // rather than loop forever (SPEC 03 §11 INV-7 height balance).
    PageError::BadBounds {
        lower: 0,
        upper: 0,
        body_size: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::build_single_db_image;
    use crate::page::{select_meta, MetaChoice, MetaPage, META_A_PGNO, META_B_PGNO};

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 20;

    /// Build an env image from `entries` and return `(image, main_root, depth)`.
    fn build(entries: &[(Vec<u8>, Vec<u8>)]) -> (Vec<u8>, u64, u16) {
        let img = build_single_db_image(PS, MAP, 1, entries, 900).expect("build");
        let s0 = MetaPage::validate(&img[0..PS as usize], PS).unwrap();
        let s1 = MetaPage::validate(&img[PS as usize..2 * PS as usize], PS).unwrap();
        let meta = match select_meta(&s0, &s1, false) {
            MetaChoice::Both { meta, .. } | MetaChoice::OnlyOne { meta, .. } => meta,
            MetaChoice::None => panic!("built image has no valid meta"),
        };
        let _ = (META_A_PGNO, META_B_PGNO);
        (img, meta.main_db.root, meta.main_db.depth)
    }

    fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), v.to_vec())
    }

    fn collect_fwd(tree: &Tree<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut c = tree.cursor();
        let mut out = Vec::new();
        let mut e = c.first().unwrap();
        while let Some((k, v)) = e {
            out.push((k.to_vec(), v.to_vec()));
            e = c.next().unwrap();
        }
        out
    }

    fn collect_rev(tree: &Tree<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut c = tree.cursor();
        let mut out = Vec::new();
        let mut e = c.last().unwrap();
        while let Some((k, v)) = e {
            out.push((k.to_vec(), v.to_vec()));
            e = c.prev().unwrap();
        }
        out
    }

    #[test]
    fn empty_tree_reads() {
        let (img, root, depth) = build(&[]);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        assert!(t.is_empty());
        assert_eq!(t.get(b"x").unwrap(), None);
        let mut c = t.cursor();
        assert_eq!(c.first().unwrap(), None);
        assert_eq!(c.last().unwrap(), None);
        assert_eq!(c.next().unwrap(), None);
        assert_eq!(c.prev().unwrap(), None);
        assert_eq!(c.set_range(b"x").unwrap(), None);
        assert_eq!(c.get_greater_than(b"x").unwrap(), None);
        assert_eq!(c.get_lower_than_or_equal_to(b"x").unwrap(), None);
    }

    #[test]
    fn single_leaf_get_and_iter() {
        let entries: Vec<_> = (0u16..20)
            .map(|i| kv(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes()))
            .collect();
        let (img, root, depth) = build(&entries);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        assert_eq!(depth, 1, "20 tiny entries fit one leaf");
        for (k, v) in &entries {
            assert_eq!(t.get(k).unwrap(), Some(v.as_slice()));
        }
        assert_eq!(t.get(b"missing").unwrap(), None);
        assert_eq!(collect_fwd(&t), entries);
        let mut rev = entries.clone();
        rev.reverse();
        assert_eq!(collect_rev(&t), rev);
    }

    #[test]
    fn multi_level_iter_matches_sorted() {
        // Enough entries with large-ish values to force several leaves + a
        // branch level.
        let entries: Vec<_> = (0u16..2000)
            .map(|i| {
                kv(
                    format!("key{i:05}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                )
            })
            .collect();
        let (img, root, depth) = build(&entries);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        assert!(
            depth >= 2,
            "2000 entries need a branch level, got depth {depth}"
        );
        for (k, v) in &entries {
            assert_eq!(t.get(k).unwrap(), Some(v.as_slice()), "get {k:?}");
        }
        assert_eq!(collect_fwd(&t), entries);
        let mut rev = entries.clone();
        rev.reverse();
        assert_eq!(collect_rev(&t), rev);
    }

    #[test]
    fn overflow_values_resolve_zero_copy() {
        let big = vec![0xABu8; 9000]; // > 2 pages
        let bigger = vec![0x5Cu8; 200_000];
        let entries = vec![
            kv(b"a", b"small"),
            kv(b"b", &big),
            kv(b"c", &bigger),
            kv(b"d", b"tiny"),
        ];
        let (img, root, depth) = build(&entries);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        assert_eq!(t.get(b"b").unwrap(), Some(big.as_slice()));
        assert_eq!(t.get(b"c").unwrap(), Some(bigger.as_slice()));
        assert_eq!(collect_fwd(&t), entries);
    }

    #[test]
    fn neighbor_seeks() {
        let entries: Vec<_> = [10u32, 20, 30, 40, 50]
            .iter()
            .map(|n| kv(format!("{n:03}").as_bytes(), b"x"))
            .collect();
        let (img, root, depth) = build(&entries);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        let mut c = t.cursor();

        // set_range (>=)
        assert_eq!(
            c.set_range(b"025").unwrap().map(|(k, _)| k.to_vec()),
            Some(b"030".to_vec())
        );
        assert_eq!(
            c.set_range(b"030").unwrap().map(|(k, _)| k.to_vec()),
            Some(b"030".to_vec())
        );
        assert_eq!(c.set_range(b"055").unwrap(), None);
        assert_eq!(
            c.set_range(b"005").unwrap().map(|(k, _)| k.to_vec()),
            Some(b"010".to_vec())
        );

        // get_greater_than (>)
        assert_eq!(
            c.get_greater_than(b"030").unwrap().map(|(k, _)| k.to_vec()),
            Some(b"040".to_vec())
        );
        assert_eq!(c.get_greater_than(b"050").unwrap(), None);
        assert_eq!(
            c.get_greater_than(b"005").unwrap().map(|(k, _)| k.to_vec()),
            Some(b"010".to_vec())
        );

        // get_lower_than_or_equal_to (<=)
        assert_eq!(
            c.get_lower_than_or_equal_to(b"035")
                .unwrap()
                .map(|(k, _)| k.to_vec()),
            Some(b"030".to_vec())
        );
        assert_eq!(
            c.get_lower_than_or_equal_to(b"030")
                .unwrap()
                .map(|(k, _)| k.to_vec()),
            Some(b"030".to_vec())
        );
        assert_eq!(c.get_lower_than_or_equal_to(b"005").unwrap(), None);
        assert_eq!(
            c.get_lower_than_or_equal_to(b"999")
                .unwrap()
                .map(|(k, _)| k.to_vec()),
            Some(b"050".to_vec())
        );
    }

    #[test]
    fn next_prev_edges() {
        let entries: Vec<_> = (0u16..500)
            .map(|i| kv(format!("k{i:04}").as_bytes(), b"v"))
            .collect();
        let (img, root, depth) = build(&entries);
        let t = Tree::new(Source::Map { bytes: &img }, PS, root, depth);
        let mut c = t.cursor();
        // Walk to EOF.
        c.first().unwrap();
        while c.next().unwrap().is_some() {}
        // At EOF: next stays None, prev returns last.
        assert_eq!(c.next().unwrap(), None);
        assert_eq!(
            c.prev().unwrap().map(|(k, _)| k.to_vec()),
            Some(b"k0499".to_vec())
        );
        // Walk to before-begin.
        c.last().unwrap();
        while c.prev().unwrap().is_some() {}
        // Before-begin: next resumes at first.
        assert_eq!(
            c.next().unwrap().map(|(k, _)| k.to_vec()),
            Some(b"k0000".to_vec())
        );
    }

    #[test]
    fn prefix_successor_cases() {
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"\xff\xff"), None);
        assert_eq!(prefix_successor(b""), None);
        assert_eq!(prefix_successor(b"a\xff\xff"), Some(b"b".to_vec()));
    }
}
