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
//! This module contains **no** `unsafe` (the crate is `#![deny(unsafe_code)]`,
//! opened only in `page::raw` — PERF-GAP A3)
//! and no I/O — it is pure logic over borrowed bytes, so `miri` exercises it.
//!
//! The cursor is a root-to-leaf path (`stack` of `(pgno, ki)` frames) plus the
//! `INITIALIZED`/`EOF` flags of SPEC 03 §4. Each public op documents the §4
//! subsection whose positioning/EOF/empty-DB semantics it implements.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

use super::cmp::KeyCmp;
use super::dirty::DirtyStore;
use super::page::{
    BranchRef, LeafRef, LeafValue, OverflowRef, PageError, PageRef, PageType, PGNO_INVALID,
};

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
        self.bytes_from_classified(psize, pgno).map(|(b, _)| b)
    }

    /// As [`Source::bytes_from`], additionally reporting whether the bytes
    /// came from the **map** (`true`) or from a **dirty frame** (`false`).
    ///
    /// The distinction gates [`ValidatedPages`]: map bytes are immutable for
    /// the owning txn's life (a reader's pinned snapshot is GC-protected,
    /// TXN-20/21; a writer buffers every mutation in the dirty store until
    /// commit C2 — including WRITE_MAP, TXN-45a — so the map never changes
    /// under a live txn). Dirty frames mutate mid-txn and must never be
    /// trusted from a memo.
    pub(crate) fn bytes_from_classified(
        &self,
        psize: u32,
        pgno: u64,
    ) -> Result<(&'a [u8], bool), PageError> {
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
            Source::Map { bytes } => map_slice(bytes).map(|b| (b, true)),
            Source::Writer { dirty, bytes } => match dirty.bytes(pgno) {
                Some(frame) => Ok((frame, false)),
                None => map_slice(bytes).map(|b| (b, true)),
            },
        }
    }
}

/// Geometric growth levels of the memo (level `i` holds
/// `MEMO_BASE_SLOTS << (2 * i)` slots): 7 levels ≈ 5.6 M slots ≈ 2.8 M
/// memoized pages at the ½ load-factor gate — a ~45 GB env of 16 K pages
/// touched by ONE txn before the memo saturates and degrades to plain
/// revalidation (correct, just slower).
const MEMO_LEVELS: usize = 7;
/// Slots in level 0 (8 KiB) — sized so short txns allocate once and small.
const MEMO_BASE_SLOTS: usize = 1024;

/// Which validated shape a memo entry vouches for (PERF-GAP A8). The kind is
/// part of the memo **key**, so a hit can hand out a fully *trusted* typed
/// view (`new_trusted`, zero checks) while a pgno validated as one kind can
/// never be trusted as the other — the mismatched lookup simply misses and
/// revalidates, failing loudly on the type check.
#[derive(Clone, Copy)]
pub(crate) enum PageKind {
    Leaf,
    Branch,
}

/// Txn-scoped memo of pages whose **cells** have already passed full
/// validation this txn (docs/PERF-GAP-VS-LMDB.md A2; lock-free since A7).
///
/// `LeafRef::new`/`BranchRef::new` validate every cell — O(`num_keys`) per
/// view construction — which multiplied every descent (get, seek, put
/// search). A page recorded here is re-wrapped via the zero-check
/// `new_trusted` constructors (the memo key carries the page kind).
///
/// Soundness: entries are only recorded for **map-sourced** bytes
/// ([`Source::bytes_from_classified`]), which are immutable for the owning
/// txn's life; dirty frames never enter the memo.
///
/// Concurrency (A7): milli shares one `RoTxn` across rayon workers, and the
/// previous `Mutex<HashSet>` probe was the second-hottest zerodb frame in the
/// milli indexing profile. Now: insert-only open addressing over `AtomicU64`
/// slots (`0` = empty, else `key`), in geometrically growing levels published
/// through `OnceLock` — levels are never moved or rehashed, `contains` probes
/// every initialized level, and nothing here can affect correctness: any
/// degradation (stale level counter, saturated last level, racing duplicate
/// insert) is at worst a miss, which revalidates.
pub struct ValidatedPages {
    levels: [OnceLock<Box<[AtomicU64]>>; MEMO_LEVELS],
    /// Highest level inserts currently target.
    cur: AtomicUsize,
    /// Per-level advisory fill counts (½ load-factor gate only).
    counts: [AtomicUsize; MEMO_LEVELS],
}

impl ValidatedPages {
    pub(crate) fn new() -> ValidatedPages {
        ValidatedPages {
            levels: std::array::from_fn(|_| OnceLock::new()),
            cur: AtomicUsize::new(0),
            counts: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }

    /// One slot's stored key: `(pgno | kind_tag) + 1`, so `0` stays "empty".
    /// Bit 63 tags the kind; pgnos are bounded far below that
    /// (`map_size / page_size`), and the `+1` cannot wrap.
    fn key_of(pgno: u64, kind: PageKind) -> u64 {
        let tag = match kind {
            PageKind::Leaf => 0,
            PageKind::Branch => 1u64 << 63,
        };
        (pgno | tag) + 1
    }

    /// splitmix64 finalizer — cheap, well-mixed slot index (the memo's
    /// previous SipHash was measurable in the milli profile).
    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn contains(&self, pgno: u64, kind: PageKind) -> bool {
        let key = Self::key_of(pgno, kind);
        // Ordering: `Acquire` here pairs with the `Release` slot publication
        // in `insert`, so a hit happens-after the completed validation that
        // program-order preceded that insert (required on ARM's weak model;
        // the page bytes themselves are immutable and were readable before).
        // A stale `cur` or slot read is only ever a miss → revalidation.
        let top = self.cur.load(Ordering::Acquire).min(MEMO_LEVELS - 1);
        for level in self.levels.iter().take(top + 1) {
            let Some(lvl) = level.get() else { continue };
            let mask = lvl.len() - 1;
            let mut i = (Self::mix(key) as usize) & mask;
            // Bounded probe: the ½ load-factor gate guarantees empty slots,
            // so the first `0` ends this level; the bound is belt-and-braces.
            for _ in 0..lvl.len() {
                match lvl[i].load(Ordering::Acquire) {
                    0 => break,
                    s if s == key => return true,
                    _ => i = (i + 1) & mask,
                }
            }
        }
        false
    }

    fn insert(&self, pgno: u64, kind: PageKind) {
        let key = Self::key_of(pgno, kind);
        loop {
            // Ordering: `Acquire` on `cur` sees the latest published level
            // index; `get_or_init` does its own synchronization for the
            // allocation itself.
            let li = self.cur.load(Ordering::Acquire).min(MEMO_LEVELS - 1);
            let lvl = self.levels[li].get_or_init(|| {
                (0..MEMO_BASE_SLOTS << (2 * li))
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            });
            // ½ load-factor gate: keeps probes short and guarantees empty
            // slots so `contains` terminates on the first `0`.
            if self.counts[li].load(Ordering::Relaxed) * 2 >= lvl.len() {
                if li + 1 < MEMO_LEVELS {
                    // Ordering: `AcqRel` — the winning bump publishes the new
                    // level index; losers reload and retry.
                    let _ =
                        self.cur
                            .compare_exchange(li, li + 1, Ordering::AcqRel, Ordering::Acquire);
                    continue;
                }
                return; // saturated: degrade to revalidation, stay correct
            }
            let mask = lvl.len() - 1;
            let mut i = (Self::mix(key) as usize) & mask;
            for _ in 0..lvl.len() {
                // Ordering: `Release` on success publishes "this page's full
                // validation completed" (all validation reads are
                // program-ordered before this CAS) to `contains`'s `Acquire`
                // loads; `Acquire` on failure so an observed equal key is a
                // completed publication by a racing thread.
                match lvl[i].compare_exchange(0, key, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => {
                        // Advisory only (gates the load factor); `Relaxed`
                        // suffices — no data is published through it.
                        self.counts[li].fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    Err(s) if s == key => return,
                    Err(_) => i = (i + 1) & mask,
                }
            }
            // Level filled under racing inserts before the gate caught it:
            // advance (or saturate) and retry.
            if li + 1 < MEMO_LEVELS {
                let _ = self
                    .cur
                    .compare_exchange(li, li + 1, Ordering::AcqRel, Ordering::Acquire);
            } else {
                return;
            }
        }
    }
}

/// The validated leaf view of `pgno` from `src`: memo hit on a map-sourced
/// page skips the O(`num_keys`) cell walk; every other case fully validates
/// (and records map-sourced pages for the rest of the txn).
pub(crate) fn leaf_view<'a>(
    src: Source<'a>,
    psize: u32,
    pgno: u64,
    valid: Option<&ValidatedPages>,
) -> Result<LeafRef<'a>, PageError> {
    let (bytes, from_map) = src.bytes_from_classified(psize, pgno)?;
    leaf_view_over(bytes, from_map, psize, pgno, valid)
}

/// [`leaf_view`] over bytes the caller already resolved (PERF-GAP issue #9:
/// the descent resolves each page's bytes exactly once — see [`node_view`]).
fn leaf_view_over<'a>(
    bytes: &'a [u8],
    from_map: bool,
    psize: u32,
    pgno: u64,
    valid: Option<&ValidatedPages>,
) -> Result<LeafRef<'a>, PageError> {
    match valid {
        Some(v) => {
            if from_map {
                if v.contains(pgno, PageKind::Leaf) {
                    // Kind-tagged hit: zero checks (PERF-GAP A8).
                    Ok(LeafRef::new_trusted(bytes))
                } else {
                    let leaf = LeafRef::new(bytes, psize)?;
                    v.insert(pgno, PageKind::Leaf);
                    Ok(leaf)
                }
            } else {
                // Engine-authored dirty frame (PERF-GAP batch 3): a frame is
                // either a COW copy of a page fully validated on its first
                // map access this txn, or the output of this txn's own page
                // encoders — never raw disk bytes, which always enter through
                // the `from_map` arm above. Structural O(1) checks (type,
                // bounds) still run; the per-cell walk over the engine's own
                // output is skipped — LMDB's model for its dirty pages.
                LeafRef::new_prevalidated(bytes, psize)
            }
        }
        None => LeafRef::new(bytes, psize),
    }
}

/// As [`leaf_view`], for branch pages.
pub(crate) fn branch_view<'a>(
    src: Source<'a>,
    psize: u32,
    pgno: u64,
    valid: Option<&ValidatedPages>,
) -> Result<BranchRef<'a>, PageError> {
    let (bytes, from_map) = src.bytes_from_classified(psize, pgno)?;
    branch_view_over(bytes, from_map, psize, pgno, valid)
}

/// [`branch_view`] over bytes the caller already resolved (see [`node_view`]).
fn branch_view_over<'a>(
    bytes: &'a [u8],
    from_map: bool,
    psize: u32,
    pgno: u64,
    valid: Option<&ValidatedPages>,
) -> Result<BranchRef<'a>, PageError> {
    match valid {
        Some(v) => {
            if from_map {
                if v.contains(pgno, PageKind::Branch) {
                    // Kind-tagged hit: zero checks (PERF-GAP A8).
                    Ok(BranchRef::new_trusted(bytes))
                } else {
                    let br = BranchRef::new(bytes, psize)?;
                    v.insert(pgno, PageKind::Branch);
                    Ok(br)
                }
            } else {
                // Engine-authored dirty frame — see [`leaf_view`].
                BranchRef::new_prevalidated(bytes, psize)
            }
        }
        None => BranchRef::new(bytes, psize),
    }
}

/// A tree page as its typed view, resolved and dispatched in **one** source
/// resolution (PERF-GAP issue #9).
///
/// The descent previously resolved every page's bytes twice — once through
/// [`load_page`] for the type dispatch, then again inside
/// [`leaf_view`]/[`branch_view`] — and in a write txn each resolution probes
/// the dirty store first ([`Source::bytes_from_classified`]), which the
/// hannoy-build call tree showed as a top descent cost. Any non-tree page
/// type fails with the same [`PageError::WrongPageType`] the two-step
/// dispatch produced.
pub(crate) enum NodeView<'a> {
    Leaf(LeafRef<'a>),
    Branch(BranchRef<'a>),
}

pub(crate) fn node_view<'a>(
    src: Source<'a>,
    psize: u32,
    pgno: u64,
    valid: Option<&ValidatedPages>,
) -> Result<NodeView<'a>, PageError> {
    let (bytes, from_map) = src.bytes_from_classified(psize, pgno)?;
    let page = PageRef::new_trusted_psize(bytes, psize)?;
    match page.page_type() {
        PageType::Leaf => leaf_view_over(bytes, from_map, psize, pgno, valid).map(NodeView::Leaf),
        PageType::Branch => {
            branch_view_over(bytes, from_map, psize, pgno, valid).map(NodeView::Branch)
        }
        other => Err(wrong_type(other)),
    }
}

// ---------------------------------------------------------------------------
// Page loading + value resolution
// ---------------------------------------------------------------------------

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
    /// The ordering this tree is stored under (milestone 2.4, SPEC 03 §2.0).
    /// Carried by the `Tree` rather than looked up per comparison so every
    /// descent, seek and hit-test in this module uses the same ordering by
    /// construction — the only way to be sure none of them silently falls back
    /// to memcmp.
    cmp: KeyCmp<'a>,
    /// The owning txn's validated-pages memo, if it provides one
    /// ([`ValidatedPages`]); `None` (always fully validate) for tests, tools
    /// and the GC tree.
    valid: Option<&'a ValidatedPages>,
}

impl<'a> Tree<'a> {
    /// Build a tree view over `src` with the given page size, root page number
    /// (`PGNO_INVALID` for an empty tree), and height.
    #[must_use]
    pub fn new(src: Source<'a>, psize: u32, root: u64, depth: u16) -> Tree<'a> {
        Tree::with_comparator(src, psize, root, depth, KeyCmp::Default)
    }

    /// As [`Tree::new`], under an explicit ordering (**milestone 2.4**). Every
    /// engine path that serves a *named* database builds its tree through here;
    /// [`Tree::new`] (memcmp) remains correct for the main/catalog tree and the
    /// GC tree, which are memcmp by construction (SPEC 03 §2.0).
    #[must_use]
    pub fn with_comparator(
        src: Source<'a>,
        psize: u32,
        root: u64,
        depth: u16,
        cmp: KeyCmp<'a>,
    ) -> Tree<'a> {
        Tree {
            src,
            psize,
            root,
            depth,
            cmp,
            valid: None,
        }
    }

    /// Attach the owning txn's validated-pages memo (PERF-GAP A2). Descents
    /// through this tree then skip re-validating cells of map-sourced pages
    /// already validated this txn; without it every view fully validates.
    #[must_use]
    pub(crate) fn with_validation_memo(mut self, valid: Option<&'a ValidatedPages>) -> Tree<'a> {
        self.valid = valid;
        self
    }

    /// The ordering this tree is stored under (milestone 2.4).
    #[must_use]
    pub fn comparator(&self) -> KeyCmp<'a> {
        self.cmp
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
        // The search just cached this leaf's view — no re-resolution.
        let leaf = c.leaf_at(pgno)?;
        if ki < leaf.num_keys() && self.cmp.eq(leaf.key(ki), key) {
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
        // The search just cached this leaf's view — no re-resolution.
        let leaf = c.leaf_at(pgno)?;
        if ki < leaf.num_keys() && self.cmp.eq(leaf.key(ki), key) {
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

    /// The entry at a known `(leaf pgno, cell index)` position (PERF-GAP B1):
    /// used by the write cursor to re-materialize the borrows of a position it
    /// computed while holding a *different* borrow of the txn. Goes through the
    /// txn's validated-pages memo, so no re-validation on the hot path.
    ///
    /// The caller vouches that `(pgno, ki)` is a live leaf position in **this**
    /// tree state (the write cursor only replays positions it just derived,
    /// with no mutation in between — it holds the `&mut RwTxn` exclusively).
    ///
    /// # Errors
    ///
    /// A [`PageError`] if the page is not a valid leaf or `ki` is out of range.
    pub(crate) fn entry_at(&self, pgno: u64, ki: usize) -> Result<(&'a [u8], &'a [u8]), PageError> {
        let leaf = leaf_view(self.src, self.psize, pgno, self.valid)?;
        let n = leaf.num_keys();
        if ki >= n {
            // Unreachable under the caller contract; surfaced as an
            // out-of-bounds cell rather than a panic so a logic error degrades
            // to `Invalid`, not UB-adjacent behavior.
            debug_assert!(false, "entry_at({pgno}, {ki}) past num_keys={n}");
            return Err(PageError::CellOutOfBounds {
                offset: ki,
                needed: 1,
                body_size: n,
            });
        }
        let k = leaf.key(ki);
        let v = resolve_value(self.src, self.psize, &leaf, ki)?;
        Ok((k, v))
    }
}

// ---------------------------------------------------------------------------
// PathStack — inline root-to-leaf frame stack (PERF-GAP A6, issue #19)
// ---------------------------------------------------------------------------

/// Maximum root-to-leaf frames a cursor path can hold — LMDB's
/// `CURSOR_STACK` bound. With the B+tree's minimum branch fanout of 2,
/// depth 32 already addresses 2^31 leaf pages (8 TB at the smallest page
/// size); any deeper descent means a corrupt `depth`/cycle and fails with
/// the same typed error the per-descent iteration guards produce.
const CURSOR_STACK: usize = 32;

/// A `Vec`-shaped fixed-capacity `(pgno, ki)` stack. Descents are the
/// engine's hottest loop, and the previous heap `Vec` cost one alloc + free
/// per `Tree::get` (the `grow_one` frame in the hannoy-build profile).
/// Inline storage makes cursor construction allocation-free; `push` reports
/// overflow as a typed corruption error instead of growing.
#[derive(Clone, Debug)]
pub(crate) struct PathStack {
    /// Frames `0..len`; slots past `len` are dead space.
    buf: [(u64, usize); CURSOR_STACK],
    len: usize,
}

impl PathStack {
    fn new() -> PathStack {
        PathStack {
            buf: [(0, 0); CURSOR_STACK],
            len: 0,
        }
    }

    fn push(&mut self, frame: (u64, usize)) -> Result<(), PageError> {
        match self.buf.get_mut(self.len) {
            Some(slot) => {
                *slot = frame;
                self.len += 1;
                Ok(())
            }
            // Deeper than any legal tree: reject like the descent iteration
            // guards do (SPEC 03 §11 INV-7), never overflow.
            None => Err(depth_exceeded()),
        }
    }

    fn pop(&mut self) -> Option<(u64, usize)> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            Some(self.buf[self.len])
        }
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn last(&self) -> Option<&(u64, usize)> {
        self.buf[..self.len].last()
    }

    fn last_mut(&mut self) -> Option<&mut (u64, usize)> {
        self.buf[..self.len].last_mut()
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
    stack: PathStack,
    /// The `INITIALIZED` flag (SPEC 03 §4): the cursor has been positioned.
    /// Cleared on before-begin (`prev` past the minimum) and on a failed exact
    /// `set`.
    initialized: bool,
    /// The `EOF` flag (SPEC 03 §4): the cursor sits past the maximum entry.
    eof: bool,
    /// The tree's ordering (milestone 2.4), copied from the [`Tree`] this
    /// cursor was opened on so every seek uses it.
    cmp: KeyCmp<'a>,
    /// Memoized current leaf view, keyed by pgno.
    ///
    /// [`LeafRef::new`] validates **every cell** on the page (O(`num_keys`)), so
    /// re-deriving the view on each step made a full scan O(`num_keys`²) per
    /// page instead of O(`num_keys`) — the dominant cost in cursor iteration.
    /// Caching keeps the validation (every page is still fully validated before
    /// any access) and just stops repeating it while the cursor stays on one
    /// page.
    ///
    /// Soundness: the cursor owns a [`Source<'a>`], whose `&'a [u8]` map and
    /// `&'a DirtyStore` are *immutable* borrows for `'a`. No mutation can occur
    /// while this cursor is alive, so a cached view can never go stale.
    leaf_cache: Cell<Option<(u64, LeafRef<'a>)>>,
    /// The owning txn's validated-pages memo (PERF-GAP A2), copied from the
    /// [`Tree`] this cursor was opened on.
    valid: Option<&'a ValidatedPages>,
}

impl<'a> Cursor<'a> {
    fn new(t: Tree<'a>) -> Cursor<'a> {
        Cursor {
            src: t.src,
            psize: t.psize,
            root: t.root,
            depth: t.depth,
            stack: PathStack::new(),
            initialized: false,
            eof: false,
            cmp: t.cmp,
            leaf_cache: Cell::new(None),
            valid: t.valid,
        }
    }

    // -- page helpers ------------------------------------------------------

    /// The validated leaf view for `pgno`, reusing the memoized one while the
    /// cursor stays on the same page (see [`Cursor::leaf_cache`]). A miss goes
    /// through the txn's validated-pages memo ([`leaf_view`]).
    fn leaf_at(&self, pgno: u64) -> Result<LeafRef<'a>, PageError> {
        if let Some((cached, leaf)) = self.leaf_cache.get() {
            if cached == pgno {
                return Ok(leaf);
            }
        }
        let leaf = leaf_view(self.src, self.psize, pgno, self.valid)?;
        self.leaf_cache.set(Some((pgno, leaf)));
        Ok(leaf)
    }

    /// The validated branch view for `pgno`, through the txn's memo
    /// ([`branch_view`]).
    fn branch_at(&self, pgno: u64) -> Result<BranchRef<'a>, PageError> {
        branch_view(self.src, self.psize, pgno, self.valid)
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
        let leaf = self.leaf_at(pgno)?;
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
            match node_view(self.src, self.psize, pgno, self.valid)? {
                NodeView::Leaf(leaf) => {
                    self.stack.push((pgno, 0))?;
                    self.leaf_cache.set(Some((pgno, leaf)));
                    return Ok(());
                }
                NodeView::Branch(br) => {
                    self.stack.push((pgno, 0))?;
                    pgno = br.child_pgno(0);
                }
            }
        }
        Err(depth_exceeded())
    }

    /// Descend from `start` taking the last child at every branch to the
    /// rightmost leaf, pushing frames with `ki = num_keys − 1`.
    fn descend_max(&mut self, start: u64) -> Result<(), PageError> {
        let mut pgno = start;
        for _ in 0..=(self.depth as usize + 1) {
            match node_view(self.src, self.psize, pgno, self.valid)? {
                NodeView::Leaf(leaf) => {
                    let n = leaf.num_keys();
                    self.stack.push((pgno, n.saturating_sub(1)))?;
                    self.leaf_cache.set(Some((pgno, leaf)));
                    return Ok(());
                }
                NodeView::Branch(br) => {
                    let last = br.num_keys().saturating_sub(1);
                    self.stack.push((pgno, last))?;
                    pgno = br.child_pgno(last);
                }
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
            match node_view(self.src, self.psize, pgno, self.valid)? {
                NodeView::Leaf(leaf) => {
                    let ki = match leaf.lookup_with(key, self.cmp) {
                        Ok(i) | Err(i) => i,
                    };
                    self.stack.push((pgno, ki))?;
                    self.leaf_cache.set(Some((pgno, leaf)));
                    self.initialized = true;
                    return Ok(());
                }
                NodeView::Branch(br) => {
                    let i = br.child_index_with(key, self.cmp);
                    self.stack.push((pgno, i))?;
                    pgno = br.child_pgno(i);
                }
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
            let br = self.branch_at(bp)?;
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
            let br = self.branch_at(bp)?;
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
        let leaf = self.leaf_at(pgno)?;
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
        let leaf = self.leaf_at(pgno)?;
        if ki < leaf.num_keys() && self.cmp.eq(leaf.key(ki), key) {
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
        let leaf = self.leaf_at(pgno)?;
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
            Some((k, _)) if self.cmp.eq(k, key) => self.next(),
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
            Some((k, _)) if self.cmp.eq(k, key) => self.current(),
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
        let leaf = self.leaf_at(pgno)?;
        if ki >= leaf.num_keys() {
            return Ok(None);
        }
        Ok(Some(leaf.node_flags(ki)))
    }

    // -- park / resume (PERF-GAP B1: the write cursor's stack persistence) --

    /// Detach this cursor's position as plain data (no borrows), so a write
    /// cursor can persist it across `&mut RwTxn` calls and [`resume`]
    /// (Self::resume) later without a re-descent. The inline [`PathStack`]
    /// moves by value, so a park/resume round-trip never allocates.
    pub(crate) fn park(self) -> SavedCursor {
        SavedCursor {
            stack: self.stack,
            initialized: self.initialized,
            eof: self.eof,
        }
    }

    /// Rebuild a cursor over `t` from a parked position.
    ///
    /// Caller contract (PERF-GAP B1): `saved` must have been parked from a
    /// cursor over the **same tree state** — same root, same pages, no
    /// mutation in between. The write cursor guarantees this by holding the
    /// `&mut RwTxn` exclusively and dropping its parked state on every
    /// mutation.
    pub(crate) fn resume(t: Tree<'a>, saved: SavedCursor) -> Cursor<'a> {
        Cursor {
            src: t.src,
            psize: t.psize,
            root: t.root,
            depth: t.depth,
            stack: saved.stack,
            initialized: saved.initialized,
            eof: saved.eof,
            cmp: t.cmp,
            leaf_cache: Cell::new(None),
            valid: t.valid,
        }
    }

    /// The `(leaf pgno, cell index)` under the cursor, for callers that just
    /// received `Some(..)` from a positioning op and need the position as
    /// plain data (PERF-GAP B1 two-phase yield). `None` when unpositioned/EOF
    /// (mirrors [`get_current`](Self::get_current)'s `None` conditions except
    /// the past-leaf-end park, which positioning ops never yield `Some` from).
    pub(crate) fn entry_pos(&self) -> Option<(u64, usize)> {
        if !self.initialized || self.eof {
            return None;
        }
        self.stack.last().copied()
    }
}

/// A [`Cursor`]'s position detached from its borrows (PERF-GAP B1): the
/// root-to-leaf `(pgno, child/cell index)` stack plus the SPEC 03 §4
/// `INITIALIZED`/`EOF` flags. Only meaningful for the exact tree state it was
/// parked from (see [`Cursor::resume`]).
#[derive(Debug)]
pub(crate) struct SavedCursor {
    stack: PathStack,
    initialized: bool,
    eof: bool,
}

impl SavedCursor {
    /// The parked `(leaf pgno, cell index)` if the parked cursor sat on an
    /// entry — same conditions as [`Cursor::entry_pos`], evaluated on the
    /// detached state (used by the write cursor's mutations to read the
    /// current key without resuming).
    pub(crate) fn entry_pos(&self) -> Option<(u64, usize)> {
        if !self.initialized || self.eof {
            return None;
        }
        self.stack.last().copied()
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

    #[test]
    fn path_stack_is_vec_shaped_and_rejects_overflow() {
        // PERF-GAP A6 (#19): the inline stack must behave like the Vec it
        // replaced and fail typed (never grow, never panic) past the
        // CURSOR_STACK bound.
        let mut s = PathStack::new();
        assert!(s.last().is_none());
        assert!(s.pop().is_none());
        for i in 0..CURSOR_STACK as u64 {
            s.push((i, i as usize)).expect("within capacity");
        }
        assert_eq!(s.len(), CURSOR_STACK);
        assert!(
            s.push((99, 0)).is_err(),
            "frame {CURSOR_STACK} must fail typed, not grow"
        );
        assert_eq!(s.pop(), Some((CURSOR_STACK as u64 - 1, CURSOR_STACK - 1)));
        s.last_mut().expect("nonempty").1 = 7;
        assert_eq!(s.last(), Some(&(CURSOR_STACK as u64 - 2, 7)));
        s.clear();
        assert_eq!(s.len(), 0);
        assert!(s.last().is_none());
    }

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

    /// PERF-GAP A8: the lock-free memo under concurrent insert/contains.
    /// 4 threads × 2,000 keys with heavy overlap (every key inserted by two
    /// threads, both kinds) force level growth (level 0 holds 512 at the ½
    /// gate), CAS races on duplicate keys, and probes racing publications.
    /// Afterwards every inserted key must be a hit under its own kind and a
    /// miss under the other (bit-63 tag). Runs natively and under miri
    /// (miri's weak-memory machinery checks the Acquire/Release pairs); loom
    /// is deliberately not wired: the memo's contract is advisory (any race
    /// outcome is at worst a miss → revalidation), unlike the reader table's.
    #[test]
    fn validated_pages_concurrent_insert_contains() {
        let vp = ValidatedPages::new();
        let n_threads = 4usize;
        // Full size natively (forces two level-growths); small under miri —
        // its weak-memory simulation is ~1000× slower and the orderings it
        // checks are the same at any size. Level growth still triggers with
        // 640 distinct keys > level 0's 512-insert gate.
        #[cfg(not(miri))]
        let per_thread = 2_000u64;
        #[cfg(miri)]
        let per_thread = 320u64;
        std::thread::scope(|s| {
            for t in 0..n_threads {
                let vp = &vp;
                s.spawn(move || {
                    for i in 0..per_thread {
                        // Overlap: thread t and thread (t+1)%4 share keys.
                        let pgno = (t as u64 % 2) * 10_000 + i;
                        let kind = if i % 2 == 0 {
                            PageKind::Leaf
                        } else {
                            PageKind::Branch
                        };
                        vp.insert(pgno, kind);
                        assert!(vp.contains(pgno, kind), "own insert must hit");
                    }
                });
            }
        });
        for pgno in 0..per_thread {
            let (own, other) = if pgno % 2 == 0 {
                (PageKind::Leaf, PageKind::Branch)
            } else {
                (PageKind::Branch, PageKind::Leaf)
            };
            assert!(vp.contains(pgno, own));
            assert!(vp.contains(10_000 + pgno, own));
            assert!(!vp.contains(pgno, other), "kind tag must separate");
            assert!(!vp.contains(10_000 + pgno, other));
        }
        assert!(!vp.contains(999_999, PageKind::Leaf));
    }
}
