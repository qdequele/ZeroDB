//! The write transaction: single-writer `RwTxn`, COW mutation, and the commit
//! pipeline (SPEC 04 §2/§6/§8/§9, SPEC 03 §5–§10, ADR-0004). Milestone 1.4.
//!
//! ## Shape (ADR-0004 D2)
//!
//! [`Env::write_txn`] acquires the env's in-process write mutex (TXN-6) and
//! holds the guard for the txn's whole life. All mutation runs against the
//! txn-private [`DirtyStore`] (TXN-41 stable frames); reads on the txn resolve
//! dirty frames first, then the read-only map (TXN-38), through the same
//! [`Database`] read API as a `RoTxn` (via [`TxnRead`]). heed's borrow model is
//! the compile-time enforcement of the value-borrow contract: reads take
//! `&RwTxn`, mutations `&mut RwTxn`, so no `&'txn [u8]` can span a mutation
//! (TXN-39).
//!
//! ## COW and allocation (SPEC 03 §5/§8; M1.4 scope per ADR-0004 D6)
//!
//! First touch copies a committed page into a fresh frame under a **new** pgno
//! and rewrites the parent chain top-down (§5.1/§5.3). Allocation draws from
//! the loose-page list (GC-7/8) for single pages, else extends the file
//! (`next_pgno`, GC-15/16) with the GC-17 `MapFull` bound. **No GC-DB draw
//! exists yet** (M1.5); `freelist_save` is a stub at the C1 hook, so pages
//! freed by committed txns leak on disk until M1.5 (ADR-0004 OQ4, accepted).
//!
//! ## Commit (SPEC 04 §9, SPEC 06 §2)
//!
//! [`RwTxn::commit`] runs the C0–C6 steps in **one function**
//! ([`RwTxn::commit_pipeline`]) with the always-compiled H0–H4 crash hooks
//! between steps (ADR-0004 D3/OQ5). The single load-bearing ordering (REC-7):
//! dirty pages are written (C2) and fsynced (C3) strictly before the meta is
//! written (C4) to slot `txnid & 1` (TXN-63) and fsynced (C5); the in-memory
//! snapshot is published last (C6, TXN-18/19 order). A failed fsync poisons
//! the env (REC-13).
//!
//! ## txnid assignment note (TXN-2 vs TXN-63)
//!
//! `writer_txnid` is always `last_committed_txnid + 1`, so an aborted txn's id
//! is **reused** by the next writer. TXN-2 explicitly permits this (no page or
//! meta ever recorded the aborted id), and the slot-parity scheme **requires**
//! it: slot `N & 1` must be the older slot (TXN-63), which only holds when
//! commit ids are consecutive. Non-reuse would make a post-abort commit
//! overwrite the *live* snapshot's slot. (SPEC 04 §1 clarified in this change.)

use std::sync::{Arc, MutexGuard};

use crate::btree::Source;
use crate::dirty::DirtyStore;
use crate::env::{Env, HookPoint, Snapshot};
use crate::error::{Error, MdbError, Result};
use crate::page::geometry::{
    body_size, is_map_full, map_pages, overflow_page_count, value_is_inline,
};
use crate::page::{
    write_overflow_head, BranchMut, BranchRef, DBRecord, LeafMut, LeafRef, LeafValue, MetaPage,
    PageError, PageRef, PageType, FILL_THRESHOLD_PERMILLE, FORMAT_VERSION, HEADER_SIZE, MAGIC,
    MAX_DATA_SIZE, MAX_KEY_SIZE, MIN_KEYS_BRANCH, MIN_KEYS_LEAF, PGNO_INVALID,
};
use crate::rotxn::{map_page_err, Database, TxnRead};

/// Round `n` up to the next even number (2-byte cell alignment, SPEC 02 §2.2).
#[inline]
fn even(n: usize) -> usize {
    (n + 1) & !1
}

/// A root-to-leaf descent path: `(pgno, ki)` per level (SPEC 03 §1 cursor
/// shape). `ki` is the chosen child index on branches and the entry/insertion
/// slot on the leaf.
type Path = Vec<(u64, usize)>;

/// Leaf node header size (SPEC 02 §4.2).
const LEAF_NODE_HEADER: usize = 8;
/// Branch node header size (SPEC 02 §4.1).
const BRANCH_NODE_HEADER: usize = 10;

/// Put flags (SPEC 01 Table 3 subset in M1.4 scope). Hand-rolled bitset like
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
enum ValSrc<'v> {
    Val(&'v [u8]),
    Reserve(usize),
}

impl ValSrc<'_> {
    fn len(&self) -> usize {
        match self {
            ValSrc::Val(v) => v.len(),
            ValSrc::Reserve(n) => *n,
        }
    }
}

/// Where a reserved value landed (so `put_reserved` can hand out the slice).
enum ReserveLoc {
    /// Inline in the leaf — relocate by key after the insert settles.
    Inline,
    /// On the overflow run headed at this pgno.
    Big(u64),
}

/// An owned leaf-cell value extracted during a split/borrow/merge rewrite.
enum OwnedVal {
    Inline(Vec<u8>),
    Big { head: u64, dsize: u32 },
}

/// An owned leaf cell (key + value), the currency of page rewrites.
struct OwnedLeafCell {
    key: Vec<u8>,
    val: OwnedVal,
}

impl OwnedLeafCell {
    /// `cell + 2` bytes this cell consumes on a page (§6.4 `used` term).
    fn used(&self) -> usize {
        let varea = match &self.val {
            OwnedVal::Inline(v) => v.len(),
            OwnedVal::Big { .. } => 8,
        };
        even(LEAF_NODE_HEADER + self.key.len() + varea) + 2
    }
}

/// An owned branch cell (separator + child). `key` is empty for node 0.
struct OwnedBranchCell {
    key: Vec<u8>,
    child: u64,
}

impl OwnedBranchCell {
    fn used(&self) -> usize {
        even(BRANCH_NODE_HEADER + self.key.len()) + 2
    }
}

/// §6.4 split-point policy: median then fit-adjust, over the post-insert
/// sequence's `used` sizes (`cell + 2` each). Index `s` belongs to the right
/// page (deterministic tie-break). Cited, not re-derived (ADR-0004 D5); the
/// monotonicity/termination argument is SPEC 03 §6.4's.
fn choose_split(sizes: &[usize], cap: usize) -> usize {
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

fn corrupt(e: PageError) -> Error {
    map_page_err(e)
}

fn poisoned_error() -> Error {
    Error::Io(std::io::Error::other(
        "environment poisoned by a failed durability barrier (SPEC 06 REC-13)",
    ))
}

// ---------------------------------------------------------------------------
// RwTxn
// ---------------------------------------------------------------------------

/// The single write transaction (SPEC 04 §2/§8). Holds the env's write-mutex
/// guard for its whole life (TXN-6); dropping it without [`RwTxn::commit`] is
/// an abort (TXN-59/60 — the dirty set vanishes, the disk is untouched).
pub struct RwTxn<'env> {
    env: &'env Env,
    /// TXN-6: released when the txn drops (commit or abort).
    _guard: MutexGuard<'env, ()>,
    /// The mapped region (fallback source for untouched pages, TXN-38).
    bytes: &'env [u8],
    /// The snapshot this txn grew from (`writer_txnid = base.txnid + 1`).
    base: Arc<Snapshot>,
    txnid: u64,
    psize: u32,
    dirty: DirtyStore,
    /// Committed pages obsoleted by this txn (GC-6). Persisted to the GC DB
    /// from M1.5 on; **leaked** in M1.4 (ADR-0004 OQ4).
    freed: Vec<u64>,
    /// Pages allocated *and* freed by this txn (GC-7): the reuse fast path
    /// (GC-8) and the trailing-shrink set (GC-10).
    loose: Vec<u64>,
    /// Next never-allocated pgno (GC-15); persisted as `last_pg = next_pgno-1`.
    next_pgno: u64,
    /// `base.last_pg` — the committed high-water, used to classify freed pages
    /// as loose (`> committed_last_pg` = this-txn allocation) and to assert
    /// TXN-62 at C2.
    committed_last_pg: u64,
    /// Working roots/stats (TXN-56); written to the meta at commit.
    main_db: DBRecord,
    free_db: DBRecord,
    /// LMDB `MDB_TXN_ERROR` parity: a mid-mutation failure (e.g. `MapFull`
    /// inside a split cascade) leaves the working tree partial, so every later
    /// mutation and `commit` returns `BadTxn`; only abort is valid.
    errored: bool,
}

impl Env {
    /// Begin the write transaction (SPEC 00 row 27, SPEC 04 TXN-6/56). Blocks
    /// until any current writer finishes (TXN-7); readers are unaffected
    /// (TXN-9).
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the env is poisoned by an earlier fsync failure
    /// (SPEC 06 REC-13).
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        let inner = self.inner();
        if inner.is_poisoned() {
            return Err(poisoned_error());
        }
        let guard = inner.lock_writer();
        // Recheck under the lock: a concurrent commit may have failed while we
        // blocked on the mutex.
        if inner.is_poisoned() {
            return Err(poisoned_error());
        }
        let base = inner.snapshot();
        Ok(RwTxn {
            txnid: base.txnid + 1, // TXN-2 (+ the reuse note in the module docs)
            next_pgno: base.last_pg + 1,
            committed_last_pg: base.last_pg,
            main_db: base.main_db,
            free_db: base.free_db,
            psize: inner.page_size(),
            dirty: DirtyStore::new(inner.page_size()),
            freed: Vec::new(),
            loose: Vec::new(),
            errored: false,
            bytes: inner.backing_bytes(),
            base,
            _guard: guard,
            env: self,
        })
    }
}

impl TxnRead for RwTxn<'_> {
    fn source(&self) -> Source<'_> {
        Source::Writer {
            dirty: &self.dirty,
            bytes: self.bytes,
        }
    }
    fn main_record(&self) -> &DBRecord {
        &self.main_db
    }
    fn page_size(&self) -> u32 {
        self.psize
    }
}

impl<'env> RwTxn<'env> {
    /// This writer's txnid (`last committed + 1`, SPEC 04 TXN-2/5).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Abort: drop the dirty set and freed lists, change nothing on disk,
    /// release the write mutex (SPEC 04 TXN-59). Equivalent to dropping.
    pub fn abort(self) {
        drop(self);
    }

    // -- internal plumbing ---------------------------------------------------

    fn guard_ok(&self) -> Result<()> {
        if self.errored {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        Ok(())
    }

    fn load(&self, pgno: u64) -> Result<PageRef<'_>> {
        PageRef::new(
            self.source()
                .bytes_from(self.psize, pgno)
                .map_err(corrupt)?,
            self.psize,
        )
        .map_err(corrupt)
    }

    /// `allocate(n)` (SPEC 05 GC-16, M1.4 subset — ADR-0004 D4): loose-page
    /// fast path for single pages, else file extend with the GC-17 bound. No
    /// GC-DB draw until M1.5.
    fn allocate(&mut self, n: u64) -> Result<u64> {
        if n == 1 {
            if let Some(p) = self.loose.pop() {
                return Ok(p);
            }
        }
        let mp = map_pages(self.env.inner().map_size(), self.psize);
        if is_map_full(self.next_pgno, n, mp) {
            return Err(Error::Mdb(MdbError::MapFull));
        }
        let p = self.next_pgno;
        self.next_pgno += n;
        Ok(p)
    }

    /// Record `pgno` as freed (GC-6), dropping its dirty frame if it has one.
    /// A page this txn allocated (`> committed_last_pg`) is loose (GC-7);
    /// dropping the frame here is sound because freeing only happens inside
    /// `&mut` ops, where no borrow into the frame can be live (TXN-39/43).
    fn free_page(&mut self, pgno: u64) {
        let _ = self.dirty.remove(pgno);
        if pgno > self.committed_last_pg {
            self.loose.push(pgno);
        } else {
            self.freed.push(pgno);
        }
    }

    /// Free a whole overflow run (SPEC 03 §8: all `n` pgnos).
    fn free_run(&mut self, head: u64, n: u64) {
        let _ = self.dirty.remove(head);
        for p in head..head + n {
            if p > self.committed_last_pg {
                self.loose.push(p);
            } else {
                self.freed.push(p);
            }
        }
    }

    /// COW first-touch (SPEC 03 §5.1/§5.2): an already-dirty page is edited in
    /// place; a committed page is copied into a fresh frame under a **new**
    /// pgno, stamped with this txn's id, and its old pgno freed. Returns the
    /// (possibly new) pgno; the caller rewrites the parent pointer (§5.3).
    fn touch(&mut self, pgno: u64) -> Result<u64> {
        if self.dirty.contains(pgno) {
            return Ok(pgno);
        }
        let ps = self.psize as usize;
        let base = (pgno as usize)
            .checked_mul(ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let src = self
            .bytes
            .get(base..base + ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let mut frame: Box<[u8]> = src.into();
        let np = self.allocate(1)?;
        // Restamp the copy's identity (SPEC 02 §2: pgno + writer txnid).
        let mut hdr = crate::page::CommonHeader::read(&frame);
        hdr.pgno = np;
        hdr.txnid = self.txnid;
        hdr.write(&mut frame);
        self.dirty.insert(np, frame);
        self.free_page(pgno);
        Ok(np)
    }

    /// Read-only root-to-key descent (SPEC 03 §2), returning the path of
    /// `(pgno, ki)` frames and whether the key is present at `path.last()`.
    /// No page is touched — COW happens only when a mutation is decided
    /// (`touch_path`), so a `NO_OVERWRITE` miss or a `del` of an absent key
    /// dirties nothing (LMDB parity).
    fn search_path(&self, key: &[u8]) -> Result<(Path, bool)> {
        let mut path = Vec::new();
        if self.main_db.root == PGNO_INVALID {
            return Ok((path, false));
        }
        let mut pgno = self.main_db.root;
        for _ in 0..=self.main_db.depth {
            let page = self.load(pgno)?;
            match page.page_type() {
                PageType::Leaf => {
                    let leaf = page.as_leaf().map_err(corrupt)?;
                    let (ki, found) = match leaf.lookup(key) {
                        Ok(i) => (i, true),
                        Err(i) => (i, false),
                    };
                    path.push((pgno, ki));
                    return Ok((path, found));
                }
                PageType::Branch => {
                    let br = page.as_branch().map_err(corrupt)?;
                    let i = br.child_index(key);
                    path.push((pgno, i));
                    pgno = br.child_pgno(i);
                }
                _ => return Err(Error::Mdb(MdbError::Invalid)),
            }
        }
        Err(Error::Mdb(MdbError::Invalid)) // deeper than depth: corrupt
    }

    /// Read-only descent to the rightmost entry (APPEND's last-key compare,
    /// §6.3). Returns the path (leaf `ki = num_keys - 1`) and the owned last
    /// key. The tree must be non-empty.
    fn rightmost_path(&self) -> Result<(Path, Vec<u8>)> {
        let mut path = Vec::new();
        let mut pgno = self.main_db.root;
        for _ in 0..=self.main_db.depth {
            let page = self.load(pgno)?;
            match page.page_type() {
                PageType::Leaf => {
                    let leaf = page.as_leaf().map_err(corrupt)?;
                    let n = leaf.num_keys();
                    if n == 0 {
                        return Err(Error::Mdb(MdbError::Invalid));
                    }
                    let key = leaf.key(n - 1).to_vec();
                    path.push((pgno, n - 1));
                    return Ok((path, key));
                }
                PageType::Branch => {
                    let br = page.as_branch().map_err(corrupt)?;
                    let last = br.num_keys() - 1;
                    path.push((pgno, last));
                    pgno = br.child_pgno(last);
                }
                _ => return Err(Error::Mdb(MdbError::Invalid)),
            }
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// COW every page on `path` top-down (SPEC 03 §5.3): copying a child
    /// rewrites the (already dirty) parent's child pointer; copying the root
    /// updates the working `DBRecord.root`. `ki` values stay valid — the copy
    /// is byte-identical apart from its header identity.
    fn touch_path(&mut self, path: &mut [(u64, usize)]) -> Result<()> {
        for level in 0..path.len() {
            let (pgno, _) = path[level];
            let np = self.touch(pgno)?;
            if np != pgno {
                path[level].0 = np;
                if level == 0 {
                    self.main_db.root = np;
                } else {
                    let (ppg, pki) = path[level - 1];
                    let frame = self.dirty.bytes_mut(ppg).expect("parent already touched");
                    BranchMut::from_valid(frame, self.psize)
                        .map_err(corrupt)?
                        .set_child_pgno(pki, np);
                }
            }
        }
        Ok(())
    }

    // -- put ------------------------------------------------------------------

    fn put_main(&mut self, key: &[u8], flags: PutFlags, val: ValSrc<'_>) -> Result<ReserveLoc> {
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
        if flags.contains(PutFlags::APPEND) {
            return self.append_main(key, val);
        }
        let (mut path, found) = self.search_path(key)?;
        if found && flags.contains(PutFlags::NO_OVERWRITE) {
            // §S2: no mutation, nothing dirtied.
            return Err(Error::Mdb(MdbError::KeyExist));
        }
        let res = self.put_apply(&mut path, found, key, val);
        if res.is_err() {
            // Mid-mutation failure (MapFull in a split cascade, corrupt page):
            // the working tree may be partial — poison the txn (TXN-59 clean
            // abort is the only exit).
            self.errored = true;
        }
        res
    }

    /// APPEND (§6.3): compare against the **last** key only; the cursor/search
    /// position is irrelevant. Equal-to-last is `KeyExist`, not an overwrite.
    fn append_main(&mut self, key: &[u8], val: ValSrc<'_>) -> Result<ReserveLoc> {
        if self.main_db.root == PGNO_INVALID {
            let res = self.insert_first(key, val);
            match res {
                Ok(_) => self.main_db.entries += 1,
                Err(_) => self.errored = true,
            }
            return res;
        }
        let (mut path, last_key) = self.rightmost_path()?;
        if key <= last_key.as_slice() {
            return Err(Error::Mdb(MdbError::KeyExist));
        }
        let res = self.append_apply(&mut path, key, val);
        if res.is_err() {
            self.errored = true;
        }
        res
    }

    fn append_apply(
        &mut self,
        path: &mut [(u64, usize)],
        key: &[u8],
        val: ValSrc<'_>,
    ) -> Result<ReserveLoc> {
        self.touch_path(path)?;
        let (lpg, _) = *path.last().expect("non-empty path");
        let n = {
            let frame = self.dirty.bytes(lpg).expect("leaf touched");
            LeafRef::new(frame, self.psize).map_err(corrupt)?.num_keys()
        };
        path.last_mut().expect("non-empty path").1 = n;
        let loc = self.insert_into_leaf(path, n, key, val, true)?;
        self.main_db.entries += 1;
        Ok(loc)
    }

    fn put_apply(
        &mut self,
        path: &mut [(u64, usize)],
        found: bool,
        key: &[u8],
        val: ValSrc<'_>,
    ) -> Result<ReserveLoc> {
        if path.is_empty() {
            // Empty tree: first insert allocates the root leaf (§9 grow).
            let loc = self.insert_first(key, val)?;
            self.main_db.entries += 1;
            return Ok(loc);
        }
        self.touch_path(path)?;
        let (lpg, ki) = *path.last().expect("non-empty path");
        if !found {
            let loc = self.insert_into_leaf(path, ki, key, val, false)?;
            self.main_db.entries += 1;
            return Ok(loc);
        }
        // §6.1 replace. Read the old value's shape first: `Err(dsize)` for an
        // inline value, `Ok((head, dsize))` for a BIGDATA one.
        let (old_big, new_inline) = {
            let frame = self.dirty.bytes(lpg).expect("leaf touched");
            let leaf = LeafRef::new(frame, self.psize).map_err(corrupt)?;
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
                    let leaf = LeafMut::from_valid(&mut *frame, self.psize).map_err(corrupt)?;
                    leaf.inline_value_at(ki).map_err(corrupt)?
                };
                if let ValSrc::Val(v) = val {
                    frame[off..off + dsize as usize].copy_from_slice(v);
                }
                return Ok(ReserveLoc::Inline);
            }
            _ => {}
        }
        // Different size/shape: delete the old node (freeing an old overflow
        // run, §8) and insert the new one at the same slot (§6.1).
        if let Ok((head, dsize)) = old_big {
            let n = overflow_page_count(dsize as u64, self.psize);
            self.free_run(head, n);
            self.main_db.overflow_pages -= n;
        }
        {
            let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
            LeafMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .remove(ki);
        }
        // entries unchanged: replace, not insert.
        self.insert_into_leaf(path, ki, key, val, false)
    }

    /// First insert into an empty tree (§9 grow: empty → 1 leaf).
    fn insert_first(&mut self, key: &[u8], val: ValSrc<'_>) -> Result<ReserveLoc> {
        let pg = self.allocate(1)?;
        {
            let frame = self.dirty.insert_tree_frame(pg);
            LeafMut::init(frame, self.psize, pg, self.txnid).map_err(corrupt)?;
        }
        self.main_db.root = pg;
        self.main_db.depth = 1;
        self.main_db.leaf_pages = 1;
        let mut path = vec![(pg, 0usize)];
        self.insert_into_leaf(&mut path, 0, key, val, false)
    }

    /// Insert a `(key, val)` cell at slot `ki` of the (dirty) leaf at
    /// `path.last()` (§6.2), splitting when full (§6.4; `append` forces the
    /// append split policy §6.3). BIGDATA values allocate their run first
    /// (SPEC 02 §4.2 inline rule).
    fn insert_into_leaf(
        &mut self,
        path: &mut [(u64, usize)],
        ki: usize,
        key: &[u8],
        val: ValSrc<'_>,
        append: bool,
    ) -> Result<ReserveLoc> {
        let psize = self.psize;
        if value_is_inline(key.len(), val.len() as u64, psize) {
            let (lpg, _) = *path.last().expect("non-empty path");
            let r = {
                let frame = self.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let mut leaf = LeafMut::from_valid(frame, psize).map_err(corrupt)?;
                match &val {
                    ValSrc::Val(v) => leaf.insert_inline(ki, key, 0, v),
                    ValSrc::Reserve(n) => {
                        leaf.insert_inline_reserved(ki, key, *n as u32).map(|_| ())
                    }
                }
            };
            match r {
                Ok(()) => Ok(ReserveLoc::Inline),
                Err(PageError::PageFull { .. }) => {
                    // Split (§6.2/§6.4). A RESERVE that splits materializes as
                    // a zero-filled placeholder the caller then overwrites
                    // (TXN-47 note: fresh frames have no prior content to
                    // preserve; the closure must fully write the region).
                    let cell = OwnedLeafCell {
                        key: key.to_vec(),
                        val: OwnedVal::Inline(match &val {
                            ValSrc::Val(v) => v.to_vec(),
                            ValSrc::Reserve(n) => vec![0u8; *n],
                        }),
                    };
                    self.split_leaf(path, ki, cell, append)?;
                    Ok(ReserveLoc::Inline)
                }
                Err(e) => Err(corrupt(e)),
            }
        } else {
            // Overflow run first (§6.2), then the BIGDATA pointer cell.
            let dsize = val.len() as u32;
            let n = overflow_page_count(dsize as u64, psize);
            let head = self.allocate(n)?;
            let ps = psize as usize;
            let mut run = vec![0u8; n as usize * ps].into_boxed_slice();
            let payload: &[u8] = match &val {
                ValSrc::Val(v) => v,
                ValSrc::Reserve(_) => &[],
            };
            let written = write_overflow_head(&mut run, psize, head, self.txnid, n as u32, payload)
                .map_err(corrupt)?;
            if payload.len() > written {
                let rest = &payload[written..];
                run[ps..ps + rest.len()].copy_from_slice(rest);
            }
            self.dirty.insert(head, run);
            self.main_db.overflow_pages += n;
            let (lpg, _) = *path.last().expect("non-empty path");
            let r = {
                let frame = self.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let mut leaf = LeafMut::from_valid(frame, psize).map_err(corrupt)?;
                leaf.insert_bigdata(ki, key, dsize, head)
            };
            match r {
                Ok(()) => Ok(ReserveLoc::Big(head)),
                Err(PageError::PageFull { .. }) => {
                    let cell = OwnedLeafCell {
                        key: key.to_vec(),
                        val: OwnedVal::Big { head, dsize },
                    };
                    self.split_leaf(path, ki, cell, append)?;
                    Ok(ReserveLoc::Big(head))
                }
                Err(e) => Err(corrupt(e)),
            }
        }
    }

    // -- split machinery (§6.2/§6.4/§6.5, ADR-0004 D5) ------------------------

    fn extract_leaf_cells(&self, pgno: u64) -> Result<Vec<OwnedLeafCell>> {
        let frame = self.dirty.bytes(pgno).expect("page is dirty");
        let leaf = LeafRef::new(frame, self.psize).map_err(corrupt)?;
        let mut cells = Vec::with_capacity(leaf.num_keys());
        for i in 0..leaf.num_keys() {
            let val = match leaf.value(i) {
                LeafValue::Inline(v) => OwnedVal::Inline(v.to_vec()),
                LeafValue::Overflow { head_pgno, dsize } => OwnedVal::Big {
                    head: head_pgno,
                    dsize,
                },
            };
            cells.push(OwnedLeafCell {
                key: leaf.key(i).to_vec(),
                val,
            });
        }
        Ok(cells)
    }

    fn extract_branch_cells(&self, pgno: u64) -> Result<Vec<OwnedBranchCell>> {
        let frame = self.dirty.bytes(pgno).expect("page is dirty");
        let br = BranchRef::new(frame, self.psize).map_err(corrupt)?;
        let mut cells = Vec::with_capacity(br.num_keys());
        for i in 0..br.num_keys() {
            cells.push(OwnedBranchCell {
                key: br.key(i).to_vec(),
                child: br.child_pgno(i),
            });
        }
        Ok(cells)
    }

    /// Rewrite the (dirty) leaf frame at `pgno` from owned cells.
    fn write_leaf_frame(&mut self, pgno: u64, cells: &[OwnedLeafCell]) -> Result<()> {
        let psize = self.psize;
        let txnid = self.txnid;
        let frame = self.dirty.bytes_mut(pgno).expect("page is dirty");
        let mut leaf = LeafMut::init(frame, psize, pgno, txnid).map_err(corrupt)?;
        for (i, c) in cells.iter().enumerate() {
            match &c.val {
                OwnedVal::Inline(v) => leaf.insert_inline(i, &c.key, 0, v).map_err(corrupt)?,
                OwnedVal::Big { head, dsize } => leaf
                    .insert_bigdata(i, &c.key, *dsize, *head)
                    .map_err(corrupt)?,
            }
        }
        Ok(())
    }

    /// Rewrite the (dirty) branch frame at `pgno` from owned cells. `cells[0]`
    /// carries the empty separator (node 0).
    fn write_branch_frame(&mut self, pgno: u64, cells: &[OwnedBranchCell]) -> Result<()> {
        let psize = self.psize;
        let txnid = self.txnid;
        let frame = self.dirty.bytes_mut(pgno).expect("page is dirty");
        let mut br = BranchMut::init(frame, psize, pgno, txnid).map_err(corrupt)?;
        for (i, c) in cells.iter().enumerate() {
            let sep: &[u8] = if i == 0 { &[] } else { &c.key };
            br.insert(i, sep, c.child).map_err(corrupt)?;
        }
        Ok(())
    }

    /// Leaf split (§6.2): distribute the post-insert cells around `s`
    /// (§6.4; `append` forces `s = nkeys`), keep the left half in the existing
    /// (dirty) frame, put the right half on a fresh page, and rise the right
    /// page's first key into the parent.
    fn split_leaf(
        &mut self,
        path: &mut [(u64, usize)],
        newindx: usize,
        newcell: OwnedLeafCell,
        append: bool,
    ) -> Result<()> {
        let top = path.len() - 1;
        let (lpg, _) = path[top];
        let mut cells = self.extract_leaf_cells(lpg)?;
        cells.insert(newindx, newcell);
        let cap = body_size(self.psize);
        let s = if append {
            cells.len() - 1 // §6.4 append split: new cell alone on the right
        } else {
            let sizes: Vec<usize> = cells.iter().map(OwnedLeafCell::used).collect();
            choose_split(&sizes, cap)
        };
        let sep = cells[s].key.clone(); // leaf split: sep = first key of R (§6.4)
        let rpg = self.allocate(1)?;
        self.dirty.insert_tree_frame(rpg);
        self.write_leaf_frame(lpg, &cells[..s])?;
        self.write_leaf_frame(rpg, &cells[s..])?;
        self.main_db.leaf_pages += 1;
        if top == 0 {
            self.insert_into_branch(path, -1, 0, sep, rpg)
        } else {
            let at = path[top - 1].1 + 1;
            self.insert_into_branch(path, top as isize - 1, at, sep, rpg)
        }
    }

    /// Insert `(key -> child)` at index `at` of the branch at `path[level]`,
    /// splitting recursively (§6.5) up to a root split (§9 grow) at
    /// `level < 0`.
    fn insert_into_branch(
        &mut self,
        path: &mut [(u64, usize)],
        level: isize,
        at: usize,
        key: Vec<u8>,
        child: u64,
    ) -> Result<()> {
        if level < 0 {
            // Root split (§9): new root branch over (old root, risen key).
            let old_root = path[0].0;
            let np = self.allocate(1)?;
            {
                let psize = self.psize;
                let txnid = self.txnid;
                let frame = self.dirty.insert_tree_frame(np);
                let mut br = BranchMut::init(frame, psize, np, txnid).map_err(corrupt)?;
                br.insert(0, &[], old_root).map_err(corrupt)?;
                br.insert(1, &key, child).map_err(corrupt)?;
            }
            self.main_db.root = np;
            self.main_db.depth += 1;
            self.main_db.branch_pages += 1;
            return Ok(());
        }
        let lvl = level as usize;
        debug_assert!(at >= 1, "branch inserts never target node 0");
        let (ppg, _) = path[lvl];
        let r = {
            let frame = self.dirty.bytes_mut(ppg).expect("branch is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .insert(at, &key, child)
        };
        match r {
            Ok(()) => Ok(()),
            Err(PageError::PageFull { .. }) => self.split_branch(path, lvl, at, key, child),
            Err(e) => Err(corrupt(e)),
        }
    }

    /// Branch split (§6.5): the median rises and is **removed from both
    /// children** — the right page's node 0 keeps the rising key's child under
    /// the empty separator.
    fn split_branch(
        &mut self,
        path: &mut [(u64, usize)],
        lvl: usize,
        at: usize,
        key: Vec<u8>,
        child: u64,
    ) -> Result<()> {
        let (ppg, _) = path[lvl];
        let mut cells = self.extract_branch_cells(ppg)?;
        cells.insert(at, OwnedBranchCell { key, child });
        let cap = body_size(self.psize);
        let sizes: Vec<usize> = cells.iter().map(OwnedBranchCell::used).collect();
        let s = choose_split(&sizes, cap);
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
        self.main_db.branch_pages += 1;
        if lvl == 0 {
            self.insert_into_branch(path, -1, 0, rising, rpg)
        } else {
            let at2 = path[lvl - 1].1 + 1;
            self.insert_into_branch(path, lvl as isize - 1, at2, rising, rpg)
        }
    }

    // -- delete + rebalance (§7/§10, §9 shrink) --------------------------------

    fn delete_main(&mut self, key: &[u8]) -> Result<bool> {
        self.guard_ok()?;
        // Read-side key leniency (§2.1): an oversized key simply finds
        // nothing (`Ok(false)`); the empty-key `BadValSize` is an API-boundary
        // concern (oracle adapter / heed adapter).
        let (mut path, found) = self.search_path(key)?;
        if !found {
            return Ok(false);
        }
        match self.delete_apply(&mut path) {
            Ok(()) => {
                self.main_db.entries -= 1;
                Ok(true)
            }
            Err(e) => {
                self.errored = true;
                Err(e)
            }
        }
    }

    fn delete_apply(&mut self, path: &mut [(u64, usize)]) -> Result<()> {
        self.touch_path(path)?;
        let (lpg, ki) = *path.last().expect("non-empty path");
        let big = {
            let frame = self.dirty.bytes(lpg).expect("leaf touched");
            let leaf = LeafRef::new(frame, self.psize).map_err(corrupt)?;
            match leaf.value(ki) {
                LeafValue::Inline(_) => None,
                LeafValue::Overflow { head_pgno, dsize } => Some((head_pgno, dsize)),
            }
        };
        if let Some((head, dsize)) = big {
            let n = overflow_page_count(dsize as u64, self.psize);
            self.free_run(head, n);
            self.main_db.overflow_pages -= n;
        }
        {
            let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
            LeafMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .remove(ki);
        }
        let top = path.len() - 1;
        self.rebalance(path, top)
    }

    /// `(is_leaf, num_keys, used_bytes)` of the dirty page at `pgno`.
    fn page_stats(&self, pgno: u64) -> Result<(bool, usize, usize)> {
        let frame = self.dirty.bytes(pgno).expect("page is dirty");
        let page = PageRef::new(frame, self.psize).map_err(corrupt)?;
        let body = body_size(self.psize);
        match page.page_type() {
            PageType::Leaf => {
                let l = page.as_leaf().map_err(corrupt)?;
                Ok((true, l.num_keys(), body - l.free_space()))
            }
            PageType::Branch => {
                let b = page.as_branch().map_err(corrupt)?;
                Ok((false, b.num_keys(), body - b.free_space()))
            }
            _ => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// §10 rebalance at `path[level]` after a delete/merge: root shrink at the
    /// root (§9); otherwise, when below threshold, borrow from (or merge with)
    /// a sibling, recursing upward on merge.
    fn rebalance(&mut self, path: &mut [(u64, usize)], level: usize) -> Result<()> {
        let (pgno, _) = path[level];
        let (is_leaf, nkeys, used) = self.page_stats(pgno)?;
        let body = body_size(self.psize);
        if level == 0 {
            // §9 root shrink.
            if is_leaf && nkeys == 0 {
                self.free_page(pgno);
                self.main_db.root = PGNO_INVALID;
                self.main_db.depth = 0;
                self.main_db.leaf_pages -= 1;
            } else if !is_leaf && nkeys == 1 {
                let child = {
                    let frame = self.dirty.bytes(pgno).expect("root is dirty");
                    BranchRef::new(frame, self.psize)
                        .map_err(corrupt)?
                        .child_pgno(0)
                };
                self.free_page(pgno);
                self.main_db.root = child;
                self.main_db.depth -= 1;
                self.main_db.branch_pages -= 1;
            }
            return Ok(());
        }
        // §10 thresholds: leaf = 25 % fill (FILL_THRESHOLD) or below min_keys;
        // branch = below min_keys (2 children) — its fill threshold is
        // "effectively > 0".
        let below = if is_leaf {
            nkeys < MIN_KEYS_LEAF || used * 1000 < body * FILL_THRESHOLD_PERMILLE as usize
        } else {
            nkeys < MIN_KEYS_BRANCH
        };
        if !below {
            return Ok(());
        }
        let (ppg, pki) = path[level - 1];
        let parent_nkeys = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new(frame, self.psize)
                .map_err(corrupt)?
                .num_keys()
        };
        if parent_nkeys < 2 {
            // Cannot happen on an INV-8-conforming tree; degrade gracefully.
            debug_assert!(false, "parent branch with < 2 children mid-rebalance");
            return Ok(());
        }
        // §10 sibling choice: leftmost child pairs with its right neighbor;
        // every other child pairs with its left neighbor.
        let (sib_idx, fromleft) = if pki == 0 {
            (1usize, false)
        } else {
            (pki - 1, true)
        };
        let sib_old = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new(frame, self.psize)
                .map_err(corrupt)?
                .child_pgno(sib_idx)
        };
        let sib = self.touch(sib_old)?;
        if sib != sib_old {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .set_child_pgno(sib_idx, sib);
        }
        let (s_leaf, s_nkeys, s_used) = self.page_stats(sib)?;
        if s_leaf != is_leaf {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let can_borrow = if is_leaf {
            s_nkeys > MIN_KEYS_LEAF && s_used * 1000 >= body * FILL_THRESHOLD_PERMILLE as usize
        } else {
            s_nkeys > MIN_KEYS_BRANCH
        };
        if can_borrow {
            self.borrow_entry(path, level, sib, fromleft, is_leaf)
        } else {
            self.merge_pages(path, level, sib, fromleft, is_leaf)?;
            self.rebalance(path, level - 1)
        }
    }

    /// §10 BORROW (`node_move`): move the boundary entry from the fuller
    /// sibling and rewrite the parent separator.
    fn borrow_entry(
        &mut self,
        path: &mut [(u64, usize)],
        level: usize,
        sib: u64,
        fromleft: bool,
        is_leaf: bool,
    ) -> Result<()> {
        let (pg, _) = path[level];
        let (_, pki) = path[level - 1];
        let psize = self.psize;
        if is_leaf {
            if fromleft {
                // Sibling's last entry becomes P's first; P's separator = the
                // moved key.
                let cell = {
                    let frame = self.dirty.bytes(sib).expect("sibling touched");
                    let leaf = LeafRef::new(frame, psize).map_err(corrupt)?;
                    let i = leaf.num_keys() - 1;
                    OwnedLeafCell {
                        key: leaf.key(i).to_vec(),
                        val: match leaf.value(i) {
                            LeafValue::Inline(v) => OwnedVal::Inline(v.to_vec()),
                            LeafValue::Overflow { head_pgno, dsize } => OwnedVal::Big {
                                head: head_pgno,
                                dsize,
                            },
                        },
                    }
                };
                {
                    let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                    let mut l = LeafMut::from_valid(frame, psize).map_err(corrupt)?;
                    let i = l.num_keys() - 1;
                    l.remove(i);
                }
                self.insert_owned_leaf_cell(pg, 0, &cell)?;
                let newkey = cell.key.clone();
                self.update_parent_key(path, level - 1, pki, newkey)
            } else {
                // Sibling's first entry becomes P's last; the sibling's
                // separator = its new first key.
                let cell = {
                    let frame = self.dirty.bytes(sib).expect("sibling touched");
                    let leaf = LeafRef::new(frame, psize).map_err(corrupt)?;
                    OwnedLeafCell {
                        key: leaf.key(0).to_vec(),
                        val: match leaf.value(0) {
                            LeafValue::Inline(v) => OwnedVal::Inline(v.to_vec()),
                            LeafValue::Overflow { head_pgno, dsize } => OwnedVal::Big {
                                head: head_pgno,
                                dsize,
                            },
                        },
                    }
                };
                {
                    let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                    LeafMut::from_valid(frame, psize)
                        .map_err(corrupt)?
                        .remove(0);
                }
                let new_first = {
                    let frame = self.dirty.bytes(sib).expect("sibling touched");
                    LeafRef::new(frame, psize).map_err(corrupt)?.key(0).to_vec()
                };
                let p_n = {
                    let frame = self.dirty.bytes(pg).expect("page is dirty");
                    LeafRef::new(frame, psize).map_err(corrupt)?.num_keys()
                };
                self.insert_owned_leaf_cell(pg, p_n, &cell)?;
                self.update_parent_key(path, level - 1, pki + 1, new_first)
            }
        } else if fromleft {
            // Branch borrow from the left sibling: its last child becomes P's
            // new node 0; P's old node 0 gets P's old parent separator; the
            // parent separator becomes the moved child's key.
            let (k_m, c) = {
                let frame = self.dirty.bytes(sib).expect("sibling touched");
                let br = BranchRef::new(frame, psize).map_err(corrupt)?;
                let i = br.num_keys() - 1;
                (br.key(i).to_vec(), br.child_pgno(i))
            };
            {
                let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                let mut b = BranchMut::from_valid(frame, psize).map_err(corrupt)?;
                let i = b.num_keys() - 1;
                b.remove(i);
            }
            let old_sep = {
                let (ppg, _) = path[level - 1];
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new(frame, psize)
                    .map_err(corrupt)?
                    .key(pki)
                    .to_vec()
            };
            // P is underful (a single child, the branch rebalance trigger).
            // Rebuild it as: node 0 = (empty, c), node 1 = (old_sep, c0).
            let c0 = {
                let frame = self.dirty.bytes(pg).expect("page is dirty");
                let br = BranchRef::new(frame, psize).map_err(corrupt)?;
                debug_assert_eq!(br.num_keys(), 1, "underful branch has one child");
                br.child_pgno(0)
            };
            {
                let frame = self.dirty.bytes_mut(pg).expect("page is dirty");
                let mut b = BranchMut::from_valid(frame, psize).map_err(corrupt)?;
                b.remove(0);
                b.insert(0, &[], c).map_err(corrupt)?;
                b.insert(1, &old_sep, c0).map_err(corrupt)?;
            }
            self.update_parent_key(path, level - 1, pki, k_m)
        } else {
            // Branch borrow from the right sibling: its node 0 moves to P's
            // end carrying the sibling's old parent separator; the sibling's
            // node 1 becomes its new (empty-key) node 0 and its key rises to
            // the parent.
            let (c, k1, c1) = {
                let frame = self.dirty.bytes(sib).expect("sibling touched");
                let br = BranchRef::new(frame, psize).map_err(corrupt)?;
                (br.child_pgno(0), br.key(1).to_vec(), br.child_pgno(1))
            };
            {
                let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                let mut b = BranchMut::from_valid(frame, psize).map_err(corrupt)?;
                // Remove at explicit, unshifted indices, HIGHEST FIRST: after
                // `remove(0)` the real-keyed old node 1 would shift into
                // index 0, where the node-0 sentinel rule (empty separator,
                // SPEC 02 §4.1) rejects its key — the M1.4 coverage-pass bug
                // (repeated `remove(0)`). So: drop old node 1 first (its child
                // is re-inserted as the new sentinel), then the old node 0
                // sentinel, then install the new sentinel.
                b.remove(1); // old node 1 (real key; child c1 survives below)
                b.remove(0); // old node 0 (the empty-key sentinel)
                b.insert(0, &[], c1).map_err(corrupt)?;
            }
            let old_sep = {
                let (ppg, _) = path[level - 1];
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new(frame, psize)
                    .map_err(corrupt)?
                    .key(pki + 1)
                    .to_vec()
            };
            {
                let frame = self.dirty.bytes_mut(pg).expect("page is dirty");
                let mut b = BranchMut::from_valid(frame, psize).map_err(corrupt)?;
                let n = b.num_keys();
                b.insert(n, &old_sep, c).map_err(corrupt)?;
            }
            self.update_parent_key(path, level - 1, pki + 1, k1)
        }
    }

    fn insert_owned_leaf_cell(
        &mut self,
        pgno: u64,
        idx: usize,
        cell: &OwnedLeafCell,
    ) -> Result<()> {
        let frame = self.dirty.bytes_mut(pgno).expect("page is dirty");
        let mut leaf = LeafMut::from_valid(frame, self.psize).map_err(corrupt)?;
        match &cell.val {
            OwnedVal::Inline(v) => leaf.insert_inline(idx, &cell.key, 0, v).map_err(corrupt),
            OwnedVal::Big { head, dsize } => leaf
                .insert_bigdata(idx, &cell.key, *dsize, *head)
                .map_err(corrupt),
        }
    }

    /// Rewrite the parent separator for `child_idx` (never node 0): remove the
    /// old node and re-insert with the new key through the generic branch
    /// insert, which splits the parent if the longer key no longer fits.
    fn update_parent_key(
        &mut self,
        path: &mut [(u64, usize)],
        parent_level: usize,
        child_idx: usize,
        new_key: Vec<u8>,
    ) -> Result<()> {
        debug_assert!(child_idx >= 1, "node 0's separator is always empty");
        let (ppg, _) = path[parent_level];
        let child = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new(frame, self.psize)
                .map_err(corrupt)?
                .child_pgno(child_idx)
        };
        {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .remove(child_idx);
        }
        self.insert_into_branch(path, parent_level as isize, child_idx, new_key, child)
    }

    /// §10 MERGE, always right-into-left: append the right page's entries to
    /// the left page, drop the parent's separator node for the right page,
    /// free the right page. The caller then rebalances the parent.
    fn merge_pages(
        &mut self,
        path: &mut [(u64, usize)],
        level: usize,
        sib: u64,
        fromleft: bool,
        is_leaf: bool,
    ) -> Result<()> {
        let (pg, _) = path[level];
        let (ppg, pki) = path[level - 1];
        let (left, right, right_idx) = if fromleft {
            (sib, pg, pki)
        } else {
            (pg, sib, pki + 1)
        };
        if is_leaf {
            let cells = self.extract_leaf_cells(right)?;
            let base = {
                let frame = self.dirty.bytes(left).expect("left is dirty");
                LeafRef::new(frame, self.psize).map_err(corrupt)?.num_keys()
            };
            for (j, c) in cells.iter().enumerate() {
                self.insert_owned_leaf_cell(left, base + j, c)?;
            }
            self.main_db.leaf_pages -= 1;
        } else {
            // The right branch's node 0 regains its explicit key: the parent
            // separator being dropped (§10 merge / §6.5 inverse).
            let sep = {
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new(frame, self.psize)
                    .map_err(corrupt)?
                    .key(right_idx)
                    .to_vec()
            };
            let cells = self.extract_branch_cells(right)?;
            {
                let frame = self.dirty.bytes_mut(left).expect("left is dirty");
                let mut b = BranchMut::from_valid(frame, self.psize).map_err(corrupt)?;
                let base = b.num_keys();
                b.insert(base, &sep, cells[0].child).map_err(corrupt)?;
                for (j, c) in cells.iter().enumerate().skip(1) {
                    b.insert(base + j, &c.key, c.child).map_err(corrupt)?;
                }
            }
            self.main_db.branch_pages -= 1;
        }
        {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(corrupt)?
                .remove(right_idx);
        }
        self.free_page(right);
        // Keep the path coherent for the parent-level recursion: the surviving
        // page is the pair's left page.
        path[level].0 = left;
        if fromleft {
            path[level - 1].1 = pki - 1;
        }
        Ok(())
    }

    // -- clear ------------------------------------------------------------------

    /// `clear` (SPEC 00 row 38): free every page of the tree and reset the
    /// working record to empty.
    fn clear_main(&mut self) -> Result<()> {
        self.guard_ok()?;
        if self.main_db.root == PGNO_INVALID {
            self.main_db = DBRecord::empty();
            return Ok(());
        }
        let mut pages = Vec::new();
        let mut runs = Vec::new();
        let res = self.collect_tree(self.main_db.root, self.main_db.depth, &mut pages, &mut runs);
        if let Err(e) = res {
            self.errored = true;
            return Err(e);
        }
        for (head, n) in runs {
            self.free_run(head, n);
        }
        for p in pages {
            self.free_page(p);
        }
        self.main_db = DBRecord::empty();
        Ok(())
    }

    fn collect_tree(
        &self,
        pgno: u64,
        level: u16,
        pages: &mut Vec<u64>,
        runs: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        if level == 0 {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let page = self.load(pgno)?;
        match page.page_type() {
            PageType::Leaf => {
                let leaf = page.as_leaf().map_err(corrupt)?;
                for i in 0..leaf.num_keys() {
                    if let LeafValue::Overflow { head_pgno, dsize } = leaf.value(i) {
                        runs.push((head_pgno, overflow_page_count(dsize as u64, self.psize)));
                    }
                }
                pages.push(pgno);
                Ok(())
            }
            PageType::Branch => {
                let br = page.as_branch().map_err(corrupt)?;
                for i in 0..br.num_keys() {
                    self.collect_tree(br.child_pgno(i), level - 1, pages, runs)?;
                }
                pages.push(pgno);
                Ok(())
            }
            _ => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    // -- commit pipeline (SPEC 04 §9 TXN-61..64, SPEC 06 §2 REC-6/7) -----------

    /// Whether this txn changed nothing observable (no dirty pages, no frees,
    /// no allocation, identical records): commit is then a pure mutex release,
    /// no meta write, no txnid consumption (LMDB parity).
    fn is_unchanged(&self) -> bool {
        self.dirty.is_empty()
            && self.freed.is_empty()
            && self.loose.is_empty()
            && self.next_pgno == self.base.last_pg + 1
            && self.main_db == self.base.main_db
            && self.free_db == self.base.free_db
    }

    /// GC-10 trailing shrink: loose pages that are the highest-numbered pages
    /// of the file and were never written (nothing is written before C2, so
    /// every loose page qualifies) are dropped and `next_pgno` rolls back past
    /// them, so the file does not grow by holes at its tail.
    fn release_trailing_loose(&mut self) {
        if self.loose.is_empty() {
            return;
        }
        let mut set: std::collections::BTreeSet<u64> = self.loose.iter().copied().collect();
        while self.next_pgno > self.committed_last_pg + 1 && set.remove(&(self.next_pgno - 1)) {
            debug_assert!(!self.dirty.contains(self.next_pgno - 1));
            self.next_pgno -= 1;
        }
        self.loose = set.into_iter().collect();
    }

    /// Commit (SPEC 04 TXN-58/61): run the pipeline; on success the new meta
    /// is durable and published. Consumes the txn; the write mutex is released
    /// on return (drop).
    ///
    /// # Errors
    ///
    /// - [`MdbError::BadTxn`] if a mid-mutation failure poisoned the txn (the
    ///   commit is an implicit abort).
    /// - [`Error::Io`] on a write/fsync failure — a failed **fsync** also
    ///   poisons the env (SPEC 06 REC-13).
    pub fn commit(mut self) -> Result<()> {
        if self.errored {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        if self.env.inner().is_poisoned() {
            return Err(poisoned_error());
        }
        if self.is_unchanged() {
            return Ok(());
        }
        self.commit_pipeline()
    }

    /// The one commit function (ADR-0004 D3): steps C1–C6 in textual order,
    /// H0–H4 hooks between steps. Crash-safety invariant per cut point is the
    /// SPEC 06 REC-6 table; the load-bearing ordering (REC-7) is that C3
    /// completes before C4 begins, and C4 targets only the older slot
    /// (`txnid & 1`, TXN-63).
    fn commit_pipeline(&mut self) -> Result<()> {
        let inner = self.env.inner();
        let psize = self.psize;

        // ----- C0: no nested readers exist before M1.9 (child_count ≡ 0);
        // the freed-page list is already accumulated (GC-6). -----

        // ----- C1: freelist_save (SPEC 05 §4) — M1.4 stub (ADR-0004 D6/OQ4).
        // The GC-10 trailing shrink runs; the freed/loose sets are NOT written
        // to the GC DB and leak on disk until M1.5 fills this stub in. -----
        self.release_trailing_loose();
        // freelist_save(self) — M1.5 lands here (C1 hook point).
        inner.run_hook(HookPoint::H0);
        // Crash here: nothing written — disk is byte-identical to snapshot
        // `N-1` (REC-6 H0).

        // ----- C2: write dirty pages, ascending pgno. Not yet durable. -----
        {
            let backing = inner.backing_ref();
            for pgno in self.dirty.sorted_pgnos() {
                let data = self.dirty.bytes(pgno).expect("sorted pgno present");
                // TXN-62: C2 may only write pages the live meta `N-1` does not
                // reference. In M1.4 every allocation is beyond the committed
                // high-water (extend-only + this-txn loose reuse), so:
                debug_assert!(
                    pgno > self.committed_last_pg,
                    "TXN-62 violation: writing page {pgno} referenced by the live snapshot"
                );
                backing.write_at_page(pgno, psize, data)?;
            }
        }
        inner.run_hook(HookPoint::H1);
        // Crash here: meta slots untouched → `N-1` selected; the written (or
        // torn) pages are unreferenced garbage (REC-6 H1).

        // ----- C3: fsync(data) — MUST complete before C4 (REC-7). -----
        if let Err(e) = inner.backing_ref().sync_data() {
            inner.poison(); // REC-13
            return Err(e.into());
        }
        inner.run_hook(HookPoint::H2);
        // Crash here: `N`'s data durable but unreferenced → `N-1` (REC-6 H2).

        // ----- C4: write the meta to slot `txnid & 1` (TXN-63: the older
        // slot; the intact `N-1` slot is the crash fallback). -----
        let last_pg = self.next_pgno - 1;
        let slot = self.txnid & 1;
        let meta = MetaPage {
            pgno: slot,
            txnid: self.txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: psize,
            env_flags: 0,
            map_size: inner.map_size(),
            last_pg,
            free_db: self.free_db,
            main_db: self.main_db,
        };
        let mut buf = vec![0u8; psize as usize];
        meta.encode(&mut buf).map_err(corrupt)?;
        inner.backing_ref().write_at_page(slot, psize, &buf)?;
        inner.run_hook(HookPoint::H3);
        // Crash here: recovered snapshot is `N-1` (meta missing/torn → CRC
        // rejects → older slot wins) or `N` (meta intact — safe because C3
        // already made `N`'s data durable). Never a torn meta accepted
        // (REC-6 H3, REC-8).

        // ----- C5: fsync(meta). -----
        if let Err(e) = inner.backing_ref().sync_data() {
            inner.poison(); // REC-13
            return Err(e.into());
        }
        inner.run_hook(HookPoint::H4);
        // Crash here: `N` durable and selected (REC-6 H4).

        // ----- C6: publish the snapshot (TXN-18/19 order: swap the object,
        // then store the commit point SeqCst) — only after C5, so no reader
        // can pin `N` before it is durable (TXN-64). -----
        inner.publish_snapshot(Arc::new(Snapshot {
            txnid: self.txnid,
            last_pg,
            main_db: self.main_db,
            free_db: self.free_db,
        }));
        Ok(())
        // Drop of `self` releases the write mutex — the tail of C6.
    }
}

// ---------------------------------------------------------------------------
// Database write API (heed-shaped wrappers over RwTxn internals)
// ---------------------------------------------------------------------------

impl Database {
    /// `put(txn, key, value)` (SPEC 00 row 31): plain upsert.
    ///
    /// # Errors
    ///
    /// [`MdbError::BadValSize`] (empty/oversized key, oversized value),
    /// [`MdbError::MapFull`], [`MdbError::BadTxn`] on a poisoned txn.
    pub fn put(&self, txn: &mut RwTxn<'_>, key: &[u8], value: &[u8]) -> Result<()> {
        txn.put_main(key, PutFlags::EMPTY, ValSrc::Val(value))
            .map(|_| ())
    }

    /// `put_with_flags(txn, flags, key, value)` (SPEC 00 row 32; SPEC 01
    /// §S1/§S2).
    ///
    /// # Errors
    ///
    /// As [`Database::put`], plus [`MdbError::KeyExist`] for `NO_OVERWRITE` on
    /// an existing key or `APPEND` with `key <=` the current last key.
    pub fn put_with_flags(
        &self,
        txn: &mut RwTxn<'_>,
        flags: PutFlags,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        txn.put_main(key, flags, ValSrc::Val(value)).map(|_| ())
    }

    /// `put_reserved(txn, key, len, f)` (SPEC 00 row 35, `MDB_RESERVE`,
    /// SPEC 04 TXN-47): the engine places a `len`-byte value slot and hands
    /// `f` the mutable slice into the dirty page (or overflow run frame) to
    /// fill. The closure must fully write the region; the slice is valid only
    /// for the closure's duration (the compile-time form of TXN-47's "until
    /// the next op").
    ///
    /// # Errors
    ///
    /// As [`Database::put`].
    pub fn put_reserved(
        &self,
        txn: &mut RwTxn<'_>,
        key: &[u8],
        len: usize,
        f: impl FnOnce(&mut [u8]),
    ) -> Result<()> {
        let loc = txn.put_main(key, PutFlags::EMPTY, ValSrc::Reserve(len))?;
        match loc {
            ReserveLoc::Big(head) => {
                let frame = txn.dirty.bytes_mut(head).expect("run frame present");
                f(&mut frame[HEADER_SIZE..HEADER_SIZE + len]);
            }
            ReserveLoc::Inline => {
                // Locate the settled cell (it may have moved through a split).
                let (path, found) = txn.search_path(key)?;
                debug_assert!(found, "reserved key must be present");
                let (lpg, ki) = *path.last().expect("non-empty path");
                let frame = txn.dirty.bytes_mut(lpg).expect("leaf is dirty");
                let (off, dsize) = {
                    let leaf = LeafMut::from_valid(&mut *frame, txn.psize).map_err(corrupt)?;
                    leaf.inline_value_at(ki).map_err(corrupt)?
                };
                debug_assert_eq!(dsize as usize, len);
                f(&mut frame[off..off + len]);
            }
        }
        Ok(())
    }

    /// `delete(txn, key)` (SPEC 00 row 36): whether the key existed.
    ///
    /// # Errors
    ///
    /// [`MdbError::BadTxn`] on a poisoned txn; [`MdbError::MapFull`] if the
    /// COW/rebalance ran out of map (which also poisons the txn).
    pub fn delete(&self, txn: &mut RwTxn<'_>, key: &[u8]) -> Result<bool> {
        txn.delete_main(key)
    }

    /// `delete_range(txn, lower, upper)` (SPEC 00 row 37): delete every entry
    /// in the bound pair; returns the number deleted.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn delete_range(
        &self,
        txn: &mut RwTxn<'_>,
        lower: std::ops::Bound<&[u8]>,
        upper: std::ops::Bound<&[u8]>,
    ) -> Result<u64> {
        let keys: Vec<Vec<u8>> = {
            let mut out = Vec::new();
            for item in self.range(&*txn, lower, upper) {
                let (k, _) = item?;
                out.push(k.to_vec());
            }
            out
        };
        let mut n = 0u64;
        for k in &keys {
            if txn.delete_main(k)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// `clear(txn)` (SPEC 00 row 38): empty the database.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn clear(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        txn.clear_main()
    }

    /// A mutable cursor over this database (the `iter_mut` /
    /// `prefix_iter_mut` primitive, SPEC 00 rows 33/34; SPEC 03 §7).
    #[must_use]
    pub fn rw_cursor<'t, 'env>(&self, txn: &'t mut RwTxn<'env>) -> RwCursor<'t, 'env> {
        RwCursor {
            txn,
            pos: CurPos::Start,
        }
    }
}

// ---------------------------------------------------------------------------
// RwCursor — the mutable cursor (SPEC 03 §7, ADR-0004 D5)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum CurPos {
    Start,
    At(Vec<u8>),
    AfterDelete(Vec<u8>),
    End,
}

/// A write cursor: forward iteration with `put_current`/`del_current` at the
/// current entry (SPEC 03 §7). It holds the `&mut RwTxn` exclusively, so **at
/// most one cursor exists across any mutation** — cursor fix-up therefore
/// reduces to this cursor's own position (ADR-0004 D5), which is tracked by
/// key: after a structural change the cursor re-seeks its key, so splits and
/// merges cannot leave it dangling. Entries are yielded as owned pairs in
/// M1.4 (the heed adapter revisits zero-copy yields at M1.13).
pub struct RwCursor<'t, 'env> {
    txn: &'t mut RwTxn<'env>,
    pos: CurPos,
}

impl RwCursor<'_, '_> {
    /// Advance to the next entry (ascending; SPEC 03 §4 `next` semantics over
    /// the writer's view). After a `del_current`, yields the entry that
    /// followed the deleted one (§7).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn move_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let entry: Option<(Vec<u8>, Vec<u8>)> = {
            let tree = crate::rotxn::tree_of(&*self.txn);
            let mut c = tree.cursor();
            let r = match &self.pos {
                CurPos::Start => c.first(),
                CurPos::At(k) => c.get_greater_than(k),
                CurPos::AfterDelete(k) => c.set_range(k),
                CurPos::End => return Ok(None),
            };
            r.map_err(map_page_err)?
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
        };
        match entry {
            Some((k, v)) => {
                self.pos = CurPos::At(k.clone());
                Ok(Some((k, v)))
            }
            None => {
                self.pos = CurPos::End;
                Ok(None)
            }
        }
    }

    /// Rewrite the value of the current entry, keeping the key (`MDB_CURRENT`,
    /// SPEC 03 §7): same-size rewrites happen in place; a different size
    /// deletes and re-inserts (which may split). Returns `false` if the cursor
    /// is not positioned on an entry.
    ///
    /// # Errors
    ///
    /// As [`Database::put`].
    pub fn put_current(&mut self, value: &[u8]) -> Result<bool> {
        match &self.pos {
            CurPos::At(k) => {
                let k = k.clone();
                self.txn
                    .put_main(&k, PutFlags::EMPTY, ValSrc::Val(value))
                    .map(|_| ())?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Delete the current entry (SPEC 03 §7). The cursor is left so that a
    /// following [`RwCursor::move_next`] yields the entry that followed the
    /// deleted one. Returns `false` if not positioned.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn del_current(&mut self) -> Result<bool> {
        match &self.pos {
            CurPos::At(k) => {
                let k = k.clone();
                let existed = self.txn.delete_main(&k)?;
                self.pos = CurPos::AfterDelete(k);
                Ok(existed)
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::testutil::mem_env;
    use crate::error::MdbError;

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 20;

    fn kv(i: u32) -> (Vec<u8>, Vec<u8>) {
        (
            format!("key{i:05}").into_bytes(),
            format!("value-{i}-{}", "x".repeat((i % 40) as usize)).into_bytes(),
        )
    }

    /// Everything visible through the txn, in key order.
    fn dump(txn: &RwTxn<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let tree = crate::rotxn::tree_of(txn);
        let mut c = tree.cursor();
        let mut out = Vec::new();
        let mut e = c.first().unwrap();
        while let Some((k, v)) = e {
            out.push((k.to_vec(), v.to_vec()));
            e = c.next().unwrap();
        }
        out
    }

    #[test]
    fn put_get_iter_with_splits() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        // Insert in a shuffled-ish order to exercise mid-page inserts.
        let n = 300u32;
        let mut order: Vec<u32> = (0..n).collect();
        order.reverse();
        order.sort_by_key(|i| (i * 7919) % n); // deterministic shuffle
        for &i in &order {
            let (k, v) = kv(i);
            db.put(&mut txn, &k, &v).unwrap();
        }
        assert_eq!(db.len(&txn).unwrap(), n as u64);
        for i in 0..n {
            let (k, v) = kv(i);
            assert_eq!(db.get(&txn, &k).unwrap(), Some(v.as_slice()), "get {i}");
        }
        let expected: Vec<_> = (0..n).map(kv).collect();
        assert_eq!(dump(&txn), expected);
        // The tree must have split beyond one leaf.
        assert!(txn.main_record().leaf_pages > 1, "expected leaf splits");
        assert!(txn.main_record().depth >= 2, "expected a branch level");
    }

    /// TXN-49 item 1: get-then-put — a value copied out before a put survives;
    /// the put cannot invalidate it (compile-time: the borrow ended).
    #[test]
    fn txn49_get_then_put() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        db.put(&mut txn, b"alpha", b"first-value").unwrap();
        let copied: Vec<u8> = db.get(&txn, b"alpha").unwrap().unwrap().to_vec();
        // Mutate: same leaf gets rewritten (in-place same-size would not, so
        // use a different size to force remove+reinsert).
        db.put(&mut txn, b"alpha", b"a-second-longer-value")
            .unwrap();
        db.put(&mut txn, b"beta", b"x").unwrap();
        assert_eq!(copied, b"first-value");
        assert_eq!(
            db.get(&txn, b"alpha").unwrap(),
            Some(b"a-second-longer-value".as_slice())
        );
    }

    /// TXN-49 item 2: reserve, then mutate until the leaf splits; the reserved
    /// value must have landed intact and no stale pointer is retained.
    #[test]
    fn txn49_reserve_then_split() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let payload = vec![0xCDu8; 1000];
        db.put_reserved(&mut txn, b"reserved-key", payload.len(), |buf| {
            buf.copy_from_slice(&payload);
        })
        .unwrap();
        // Force splits of the leaf holding the reserved cell.
        for i in 0..30u32 {
            let k = format!("reserved-neighbor-{i:03}").into_bytes();
            db.put(&mut txn, &k, &vec![0x11u8; 900]).unwrap();
        }
        assert_eq!(
            db.get(&txn, b"reserved-key").unwrap(),
            Some(payload.as_slice())
        );
        assert!(txn.main_record().leaf_pages > 1);
    }

    /// TXN-49 item 4: dirty enough pages to grow the pgno→frame index; frame
    /// addresses must be stable (TXN-44) — an untouched leaf's bytes do not
    /// move while other subtrees split and the HashMap rehashes.
    #[test]
    fn txn49_frame_stability_across_index_growth() {
        let env = mem_env(PS, 4 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        for i in 0..120u32 {
            let k = format!("a{i:04}").into_bytes();
            db.put(&mut txn, &k, b"stable-value").unwrap();
        }
        let addr_before = db.get(&txn, b"a0000").unwrap().unwrap().as_ptr() as usize;
        // Grow the dirty index with many later-subtree pages.
        for i in 0..600u32 {
            let k = format!("z{i:04}").into_bytes();
            db.put(&mut txn, &k, &[0x22u8; 64]).unwrap();
        }
        let addr_after = db.get(&txn, b"a0000").unwrap().unwrap().as_ptr() as usize;
        assert_eq!(addr_before, addr_after, "frame moved under the index");
    }

    #[test]
    fn append_semantics() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        // APPEND into an empty tree succeeds (§6.3).
        db.put_with_flags(&mut txn, PutFlags::APPEND, b"a", b"1")
            .unwrap();
        db.put_with_flags(&mut txn, PutFlags::APPEND, b"b", b"2")
            .unwrap();
        // Equal-to-last and less-than-last are KeyExist, not overwrites.
        for k in [b"b".as_slice(), b"a"] {
            let e = db
                .put_with_flags(&mut txn, PutFlags::APPEND, k, b"nope")
                .unwrap_err();
            assert!(matches!(e, Error::Mdb(MdbError::KeyExist)));
        }
        assert_eq!(db.get(&txn, b"b").unwrap(), Some(b"2".as_slice()));
        // Bulk append keeps pages ~full (append split policy §6.4).
        for i in 0..400u32 {
            let k = format!("c{i:05}").into_bytes();
            db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &[0x33u8; 50])
                .unwrap();
        }
        assert_eq!(db.len(&txn).unwrap(), 402);
    }

    #[test]
    fn no_overwrite_semantics() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        db.put(&mut txn, b"k", b"v1").unwrap();
        let e = db
            .put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, b"k", b"v2")
            .unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::KeyExist)));
        assert_eq!(db.get(&txn, b"k").unwrap(), Some(b"v1".as_slice()));
        db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, b"k2", b"v2")
            .unwrap();
        assert_eq!(db.len(&txn).unwrap(), 2);
    }

    #[test]
    fn key_and_value_bounds() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        for bad in [vec![], vec![0u8; 512]] {
            let e = db.put(&mut txn, &bad, b"v").unwrap_err();
            assert!(matches!(e, Error::Mdb(MdbError::BadValSize)));
        }
        db.put(&mut txn, &vec![7u8; 511], b"v").unwrap(); // max key ok
                                                          // Oversized delete key: not an error, finds nothing (§2.1).
        assert!(!db.delete(&mut txn, &vec![0u8; 600]).unwrap());
    }

    #[test]
    fn delete_with_rebalance_and_root_shrink() {
        let env = mem_env(PS, 4 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let n = 400u32;
        for i in 0..n {
            let (k, v) = kv(i);
            db.put(&mut txn, &k, &v).unwrap();
        }
        assert!(txn.main_record().depth >= 2);
        // Delete everything except a handful, in a merge-provoking order.
        for i in 0..n {
            if i % 97 == 0 {
                continue;
            }
            let (k, _) = kv(i);
            assert!(db.delete(&mut txn, &k).unwrap(), "delete {i}");
        }
        let remaining: Vec<_> = (0..n).filter(|i| i % 97 == 0).map(kv).collect();
        assert_eq!(dump(&txn), remaining);
        assert_eq!(db.len(&txn).unwrap(), remaining.len() as u64);
        // Delete the rest: the tree must collapse to empty (§9 shrink).
        for (k, _) in &remaining {
            assert!(db.delete(&mut txn, k).unwrap());
        }
        assert_eq!(db.len(&txn).unwrap(), 0);
        assert_eq!(txn.main_record().root, PGNO_INVALID);
        assert_eq!(txn.main_record().depth, 0);
        assert_eq!(txn.main_record().leaf_pages, 0);
        assert_eq!(txn.main_record().branch_pages, 0);
        // And it is reusable.
        db.put(&mut txn, b"again", b"works").unwrap();
        assert_eq!(db.get(&txn, b"again").unwrap(), Some(b"works".as_slice()));
    }

    #[test]
    fn overflow_values_in_txn() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let big = vec![0xABu8; 3 * PS as usize]; // multi-page run
        db.put(&mut txn, b"big", &big).unwrap();
        assert_eq!(db.get(&txn, b"big").unwrap(), Some(big.as_slice()));
        assert!(txn.main_record().overflow_pages >= 3);
        // Replace with a different-size overflow value: old run freed.
        let big2 = vec![0x44u8; 2 * PS as usize];
        db.put(&mut txn, b"big", &big2).unwrap();
        assert_eq!(db.get(&txn, b"big").unwrap(), Some(big2.as_slice()));
        // Replace with inline.
        db.put(&mut txn, b"big", b"small-now").unwrap();
        assert_eq!(db.get(&txn, b"big").unwrap(), Some(b"small-now".as_slice()));
        assert_eq!(txn.main_record().overflow_pages, 0);
        // Reserve into an overflow run.
        let big3 = vec![0x77u8; PS as usize + 100];
        db.put_reserved(&mut txn, b"big", big3.len(), |buf| {
            buf.copy_from_slice(&big3);
        })
        .unwrap();
        assert_eq!(db.get(&txn, b"big").unwrap(), Some(big3.as_slice()));
        // Delete frees the run.
        assert!(db.delete(&mut txn, b"big").unwrap());
        assert_eq!(txn.main_record().overflow_pages, 0);
    }

    #[test]
    fn clear_resets_everything() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        for i in 0..100u32 {
            let (k, v) = kv(i);
            db.put(&mut txn, &k, &v).unwrap();
        }
        db.put(&mut txn, b"big", &vec![9u8; 9000]).unwrap();
        db.clear(&mut txn).unwrap();
        assert_eq!(db.len(&txn).unwrap(), 0);
        assert_eq!(dump(&txn), Vec::new());
        assert_eq!(txn.main_record().root, PGNO_INVALID);
        db.put(&mut txn, b"post-clear", b"v").unwrap();
        assert_eq!(db.len(&txn).unwrap(), 1);
    }

    #[test]
    fn rw_cursor_put_and_del_current() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        for i in 0..10u32 {
            db.put(
                &mut txn,
                format!("k{i}").as_bytes(),
                format!("v{i}").as_bytes(),
            )
            .unwrap();
        }
        {
            let mut cur = db.rw_cursor(&mut txn);
            // Advance to the 4th entry (k3) and rewrite it (bigger value).
            for _ in 0..4 {
                cur.move_next().unwrap();
            }
            assert!(cur.put_current(b"rewritten-much-longer-value").unwrap());
            // Continue: next must be k4.
            let (k, _) = cur.move_next().unwrap().unwrap();
            assert_eq!(k, b"k4");
            // Delete k4; next yields k5 (the successor of the deleted entry).
            assert!(cur.del_current().unwrap());
            let (k, _) = cur.move_next().unwrap().unwrap();
            assert_eq!(k, b"k5");
        }
        assert_eq!(
            db.get(&txn, b"k3").unwrap(),
            Some(b"rewritten-much-longer-value".as_slice())
        );
        assert_eq!(db.get(&txn, b"k4").unwrap(), None);
        assert_eq!(db.len(&txn).unwrap(), 9);
        // Unpositioned cursor: mutations are no-ops returning false.
        let mut cur = db.rw_cursor(&mut txn);
        assert!(!cur.put_current(b"x").unwrap());
        assert!(!cur.del_current().unwrap());
    }

    #[test]
    fn delete_range_counts() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        for i in 0..20u32 {
            db.put(&mut txn, format!("k{i:02}").as_bytes(), b"v")
                .unwrap();
        }
        let n = db
            .delete_range(
                &mut txn,
                std::ops::Bound::Included(b"k05".as_slice()),
                std::ops::Bound::Excluded(b"k15".as_slice()),
            )
            .unwrap();
        assert_eq!(n, 10);
        assert_eq!(db.len(&txn).unwrap(), 10);
        assert_eq!(db.get(&txn, b"k05").unwrap(), None);
        assert_eq!(db.get(&txn, b"k15").unwrap(), Some(b"v".as_slice()));
    }

    #[test]
    fn map_full_poisons_txn() {
        // 16-page map: 2 metas + 14 usable pages.
        let env = mem_env(PS, 16 * PS as u64);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut hit_full = false;
        for i in 0..64u32 {
            let k = format!("k{i:02}").into_bytes();
            match db.put(&mut txn, &k, &vec![0u8; 3000]) {
                Ok(()) => {}
                Err(Error::Mdb(MdbError::MapFull)) => {
                    hit_full = true;
                    break;
                }
                Err(e) => panic!("unexpected error {e:?}"),
            }
        }
        assert!(hit_full, "expected MapFull on a 16-page map");
        // The txn is poisoned: further mutations and commit refuse.
        let e = db.put(&mut txn, b"more", b"v").unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadTxn)));
        let e = txn.commit().unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadTxn)));
    }

    #[test]
    fn abort_discards_everything() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        {
            let mut txn = env.write_txn().unwrap();
            db.put(&mut txn, b"k", b"v").unwrap();
            txn.abort();
        }
        // A fresh reader sees the (empty) committed snapshot.
        let rtxn = env.read_txn().unwrap();
        assert_eq!(db.get(&rtxn, b"k").unwrap(), None);
        assert_eq!(rtxn.txnid(), 0);
        // The write mutex was released: a new writer can begin.
        let txn2 = env.write_txn().unwrap();
        assert_eq!(txn2.txnid(), 1, "aborted id is reused (TXN-2/TXN-63)");
    }

    #[test]
    fn unchanged_commit_is_a_noop() {
        let env = mem_env(PS, MAP);
        let txn = env.write_txn().unwrap();
        // Nothing mutated: commit must not attempt I/O (the mem backing would
        // error) and must not advance the commit point.
        txn.commit().unwrap();
        assert_eq!(env.txnid(), 0);
    }

    #[test]
    fn reader_snapshot_isolated_from_writer() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let rtxn = env.read_txn().unwrap();
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        // The reader's pinned snapshot must not see uncommitted writes
        // (TXN-11); the writer sees its own (TXN-38).
        assert_eq!(db.get(&rtxn, b"k").unwrap(), None);
        assert_eq!(db.get(&wtxn, b"k").unwrap(), Some(b"v".as_slice()));
    }
}
