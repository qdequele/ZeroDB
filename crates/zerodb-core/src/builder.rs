//! Bottom-up B+tree builder: the streaming [`EnvStream`] behind compacting
//! copy. This crate's tests also compile a batch builder (the `batch`
//! submodule) that packs a whole image in one call, as fixture loader and as
//! the reference the streaming builder is compared against.
//!
//! Given a stream of pre-sorted, unique `(key, value)` pairs it packs leaves
//! at a configurable fill factor, builds branch levels upward, stores big
//! values on overflow runs (SPEC 02 §5), and emits a complete env-file image
//! (two meta slots and the data pages) whose `main_db` `DBRecord` points at
//! the built tree. The result is a valid file: [`crate::env::open_with_backing`]
//! opens it, and the [`crate::btree`] read path and `zerodb-tools check`
//! treat it like a tree the write path built.
//!
//! The builder writes through a [`PageSink`] and has no I/O and no `unsafe`,
//! so miri can run all of it.

use crate::page::geometry::{body_size, overflow_page_count, validate_page_size, value_is_inline};
use crate::page::{
    even, write_overflow_head, BranchMut, DBRecord, LeafMut, MetaPage, PageError,
    BIGDATA_VALUE_LEN, BRANCH_NODE_HEADER, FORMAT_VERSION, F_SUBDATA, LEAF_NODE_HEADER, MAGIC,
    MIN_KEYS_BRANCH,
};

#[cfg(test)]
mod batch;
#[cfg(test)]
pub(crate) use batch::{build_multi_db_image, build_single_db_image, NamedDbData};

/// Default leaf/branch fill factor in permille (90 %): each page keeps
/// about 10 % free for later inserts.
pub const DEFAULT_FILL_PERMILLE: u32 = 900;

/// Write the little-endian `pgno` into a scratch page's header (offset 0).
fn set_pgno(page: &mut [u8], pgno: u64) {
    page[0..8].copy_from_slice(&pgno.to_le_bytes());
}

/// The plan for one leaf entry: its padded cell length, and — if the value goes
/// to overflow — the run's page count.
struct LeafCellPlan {
    cell_len: usize,
    overflow_pages: Option<u64>,
}

fn plan_leaf_cell(psize: u32, key: &[u8], val: &[u8]) -> LeafCellPlan {
    let ksize = key.len();
    let dsize = val.len() as u64;
    if value_is_inline(ksize, dsize, psize) {
        LeafCellPlan {
            cell_len: even(LEAF_NODE_HEADER + ksize + val.len()),
            overflow_pages: None,
        }
    } else {
        LeafCellPlan {
            cell_len: even(LEAF_NODE_HEADER + ksize + BIGDATA_VALUE_LEN),
            overflow_pages: Some(overflow_page_count(dsize, psize)),
        }
    }
}

/// Total bytes a branch page holding `group`'s children would use (node 0 has
/// an empty separator key).
fn branch_group_bytes(group: &[usize], children: &[(Vec<u8>, u64)]) -> usize {
    let mut used = 0usize;
    for (j, &cidx) in group.iter().enumerate() {
        let ksize = if j == 0 { 0 } else { children[cidx].0.len() };
        used += even(BRANCH_NODE_HEADER + ksize) + 2;
    }
    used
}

// ===========================================================================
// Streaming builder: compaction in bounded memory
// ===========================================================================

/// Where a streaming build writes its pages. `zerodb-core` does no I/O, so
/// the file-backed implementation lives in the `zerodb` crate
/// (`zerodb::copy`); tests use an in-memory one. Each data page is emitted
/// exactly once, at the pgno `alloc` returned for it. The two meta slots
/// are emitted last.
pub trait PageSink {
    /// Allocate `n` contiguous page numbers, returning the first.
    fn alloc(&mut self, n: u64) -> u64;
    /// The next pgno `alloc` would return (== 1 + highest allocated).
    fn next_pgno(&self) -> u64;
    /// Write one `psize`-byte frame at `pgno`.
    ///
    /// # Errors
    ///
    /// I/O errors from file-backed sinks; an in-memory sink never fails.
    fn emit(&mut self, pgno: u64, frame: &[u8]) -> std::io::Result<()>;
}

/// A [`PageSink`] over a growable in-memory image (pages 0 and 1 reserved
/// for the meta slots).
#[cfg(test)]
pub(crate) struct VecSink {
    psize: u32,
    buf: Vec<u8>,
    next: u64,
}

#[cfg(test)]
impl VecSink {
    /// A fresh image sink: two zeroed meta slots, data from
    /// [`FIRST_DATA_PGNO`](crate::page::FIRST_DATA_PGNO).
    #[must_use]
    pub fn new(psize: u32) -> VecSink {
        VecSink {
            psize,
            buf: vec![0u8; 2 * psize as usize],
            next: crate::page::FIRST_DATA_PGNO,
        }
    }

    /// The finished image bytes.
    #[must_use]
    pub fn into_image(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
impl PageSink for VecSink {
    fn alloc(&mut self, n: u64) -> u64 {
        let p = self.next;
        self.next += n;
        let need = self.next as usize * self.psize as usize;
        if self.buf.len() < need {
            self.buf.resize(need, 0);
        }
        p
    }
    fn next_pgno(&self) -> u64 {
        self.next
    }
    fn emit(&mut self, pgno: u64, frame: &[u8]) -> std::io::Result<()> {
        let ps = self.psize as usize;
        let base = pgno as usize * ps;
        self.buf[base..base + ps].copy_from_slice(&frame[..ps]);
        Ok(())
    }
}

/// A streaming-build failure: a page-encoding error, or the sink's I/O error.
#[derive(Debug)]
pub enum StreamBuildError {
    /// Page-encoding failure (invalid geometry, oversized cell, …).
    Page(PageError),
    /// The sink failed to write a page (file-backed sinks only).
    Io(std::io::Error),
}

impl From<PageError> for StreamBuildError {
    fn from(e: PageError) -> StreamBuildError {
        StreamBuildError::Page(e)
    }
}
impl From<std::io::Error> for StreamBuildError {
    fn from(e: std::io::Error) -> StreamBuildError {
        StreamBuildError::Io(e)
    }
}

/// One pending (closed but unemitted) branch group: its children `(min_key,
/// pgno)`. Kept unemitted until the *next* group at the level closes, so the
/// end-of-level rebalance (the last-two rule in [`TreeStream::finish`]) can
/// still merge or re-split it.
struct PendingGroup {
    children: Vec<(Vec<u8>, u64)>,
}

/// One branch level of the streaming bulk-loader: the last closed group (kept
/// for the rebalance) and the currently-filling one.
struct StreamLevel {
    pending: Option<PendingGroup>,
    cur: Vec<(Vec<u8>, u64)>,
    cur_used: usize,
}

/// A bottom-up B+tree bulk loader fed one entry at a time. It applies the
/// batch builder's rules (the same greedy fill targets, rebalance of the
/// last two branch groups, and overflow threshold) and emits every finished
/// page to a [`PageSink`] at once. Peak memory is one leaf scratch frame,
/// one overflow scratch frame and at most two child lists per branch level
/// (about two pages of separators each): O(depth × psize), independent of
/// the tree's size.
///
/// Contract: entries are pushed in strictly ascending, unique key order.
/// The order is memcmp, so the caller must refuse an env that has a custom
/// comparator.
pub struct TreeStream<'s, S: PageSink> {
    sink: &'s mut S,
    psize: u32,
    txnid: u64,
    /// Leaf/branch fill target in bytes (from the permille factor).
    target: usize,
    // -- leaf accumulator --
    leaf_scratch: Vec<u8>,
    leaf_count: usize,
    leaf_used: usize,
    leaf_first_key: Vec<u8>,
    // -- overflow interior scratch (one frame, reused) --
    ovf_scratch: Vec<u8>,
    // -- branch levels, bottom-up --
    levels: Vec<StreamLevel>,
    // -- record counters --
    leaf_pages: u64,
    branch_pages: u64,
    overflow_pages: u64,
    entries: u64,
    #[cfg(debug_assertions)]
    last_key: Option<Vec<u8>>,
}

impl<'s, S: PageSink> TreeStream<'s, S> {
    /// Start a tree build over `sink`.
    ///
    /// # Errors
    ///
    /// [`PageError::InvalidPageSize`] if `psize` is not a valid page size.
    pub fn new(
        sink: &'s mut S,
        psize: u32,
        txnid: u64,
        fill_permille: u32,
    ) -> Result<TreeStream<'s, S>, PageError> {
        validate_page_size(psize)?;
        let fill = fill_permille.clamp(1, 1000);
        let target = ((body_size(psize) as u64 * fill as u64) / 1000) as usize;
        Ok(TreeStream {
            sink,
            psize,
            txnid,
            target,
            leaf_scratch: vec![0u8; psize as usize],
            leaf_count: 0,
            leaf_used: 0,
            leaf_first_key: Vec::new(),
            ovf_scratch: vec![0u8; psize as usize],
            levels: Vec::new(),
            leaf_pages: 0,
            branch_pages: 0,
            overflow_pages: 0,
            entries: 0,
            #[cfg(debug_assertions)]
            last_key: None,
        })
    }

    /// Append one entry (strictly greater key than every prior push).
    ///
    /// # Errors
    ///
    /// [`StreamBuildError`] on a page-encoding failure or sink I/O error.
    pub fn push(&mut self, key: &[u8], flags: u16, val: &[u8]) -> Result<(), StreamBuildError> {
        #[cfg(debug_assertions)]
        {
            if let Some(last) = &self.last_key {
                debug_assert!(
                    last.as_slice() < key,
                    "TreeStream requires strictly-ascending, unique keys"
                );
            }
            self.last_key = Some(key.to_vec());
        }
        let plan = plan_leaf_cell(self.psize, key, val);
        let need = plan.cell_len + 2;
        if self.leaf_count >= 1 && self.leaf_used + need > self.target {
            self.flush_leaf()?;
        }
        if self.leaf_count == 0 {
            self.leaf_first_key.clear();
            self.leaf_first_key.extend_from_slice(key);
            // (Re)initialize the scratch as an empty leaf.
            LeafMut::init(&mut self.leaf_scratch, self.psize, 0, self.txnid)?;
        }
        let mut leaf = LeafMut::from_valid(&mut self.leaf_scratch, self.psize)?;
        match plan.overflow_pages {
            Some(n) => {
                let opg = self.sink.alloc(n);
                // Head page: header + first body bytes, from a scratch frame.
                let ps = self.psize as usize;
                self.ovf_scratch.fill(0);
                let head_written = write_overflow_head(
                    &mut self.ovf_scratch,
                    self.psize,
                    opg,
                    self.txnid,
                    n as u32,
                    val,
                )?;
                self.sink.emit(opg, &self.ovf_scratch)?;
                // Interior pages hold only payload, written one scratch frame
                // at a time; the last page is zero-padded.
                let mut off = head_written;
                for i in 1..n {
                    let take = (val.len() - off).min(ps);
                    self.ovf_scratch[..take].copy_from_slice(&val[off..off + take]);
                    self.ovf_scratch[take..].fill(0);
                    self.sink.emit(opg + i, &self.ovf_scratch)?;
                    off += take;
                }
                debug_assert_eq!(off, val.len(), "overflow run must consume the value");
                self.overflow_pages += n;
                leaf.insert_bigdata(self.leaf_count, key, val.len() as u32, opg)?;
            }
            None => leaf.insert_inline(self.leaf_count, key, flags, val)?,
        }
        self.leaf_used += need;
        self.leaf_count += 1;
        self.entries += 1;
        Ok(())
    }

    /// Emit the open leaf and hand `(first_key, pgno)` to level 0.
    fn flush_leaf(&mut self) -> Result<(), StreamBuildError> {
        debug_assert!(self.leaf_count > 0);
        let pgno = self.sink.alloc(1);
        set_pgno(&mut self.leaf_scratch, pgno);
        self.sink.emit(pgno, &self.leaf_scratch)?;
        self.leaf_pages += 1;
        let first = std::mem::take(&mut self.leaf_first_key);
        self.leaf_count = 0;
        self.leaf_used = 0;
        self.add_child(0, first, pgno)
    }

    /// Append a child to branch level `lvl`, closing the current group when
    /// the fill target is reached. As in the batch builder, the child that
    /// does not fit starts the next group as its node 0, with the empty
    /// separator.
    fn add_child(&mut self, lvl: usize, key: Vec<u8>, pgno: u64) -> Result<(), StreamBuildError> {
        if self.levels.len() == lvl {
            self.levels.push(StreamLevel {
                pending: None,
                cur: Vec::new(),
                cur_used: 0,
            });
        }
        let ksize = if self.levels[lvl].cur.is_empty() {
            0
        } else {
            key.len()
        };
        let need = even(BRANCH_NODE_HEADER + ksize) + 2;
        if !self.levels[lvl].cur.is_empty() && self.levels[lvl].cur_used + need > self.target {
            // Close the current group. The previous pending one, if any, is
            // now final and is emitted.
            let closed = PendingGroup {
                children: std::mem::take(&mut self.levels[lvl].cur),
            };
            if let Some(prev) = self.levels[lvl].pending.take() {
                self.emit_group(lvl, prev)?;
            }
            self.levels[lvl].pending = Some(closed);
            self.levels[lvl].cur_used = even(BRANCH_NODE_HEADER) + 2; // this child is node 0
            self.levels[lvl].cur.push((key, pgno));
        } else {
            self.levels[lvl].cur_used += need;
            self.levels[lvl].cur.push((key, pgno));
        }
        Ok(())
    }

    /// Build and emit one finalized branch group, handing its `(min_key,
    /// pgno)` to the level above.
    fn emit_group(&mut self, lvl: usize, group: PendingGroup) -> Result<(), StreamBuildError> {
        debug_assert!(!group.children.is_empty());
        let mut scratch = vec![0u8; self.psize as usize];
        let mut br = BranchMut::init(&mut scratch, self.psize, 0, self.txnid)?;
        for (j, (key, cpgno)) in group.children.iter().enumerate() {
            let sep: &[u8] = if j == 0 { &[] } else { key.as_slice() };
            br.insert(j, sep, *cpgno)?;
        }
        let pgno = self.sink.alloc(1);
        set_pgno(&mut scratch, pgno);
        self.sink.emit(pgno, &scratch)?;
        self.branch_pages += 1;
        let min_key = group.children[0].0.clone();
        self.add_child(lvl + 1, min_key, pgno)
    }

    /// Finish the tree: flush the open leaf, close every level (rebalancing
    /// its last two groups so that no branch has fewer than
    /// [`MIN_KEYS_BRANCH`] children), and return the tree's [`DBRecord`].
    ///
    /// # Errors
    ///
    /// As [`TreeStream::push`].
    pub fn finish(mut self) -> Result<DBRecord, StreamBuildError> {
        if self.entries == 0 {
            return Ok(DBRecord::empty());
        }
        if self.leaf_count > 0 {
            self.flush_leaf()?;
        }
        // Cascade upward. `levels` may grow while we walk it.
        let mut lvl = 0usize;
        while lvl < self.levels.len() {
            let is_top = lvl + 1 == self.levels.len();
            let level = &mut self.levels[lvl];
            let pending = level.pending.take();
            let cur = std::mem::take(&mut level.cur);
            match (pending, cur) {
                (None, cur) if cur.len() == 1 && is_top => {
                    // A single child at the top level is the root, with no
                    // branch page over it.
                    let depth = 1 + lvl as u16;
                    return Ok(self.record(cur[0].1, depth));
                }
                (None, cur) => {
                    debug_assert!(!cur.is_empty(), "level exists but has no children");
                    self.emit_group(lvl, PendingGroup { children: cur })?;
                }
                (Some(prev), cur) => {
                    // Only the last group can have too few children: merge it
                    // with the previous one if they fit one page, else split
                    // their union evenly.
                    if cur.len() >= MIN_KEYS_BRANCH {
                        self.emit_group(lvl, prev)?;
                        self.emit_group(lvl, PendingGroup { children: cur })?;
                    } else {
                        let mut merged = prev.children;
                        merged.extend(cur);
                        let idx: Vec<usize> = (0..merged.len()).collect();
                        if branch_group_bytes(&idx, &merged) <= body_size(self.psize) {
                            self.emit_group(lvl, PendingGroup { children: merged })?;
                        } else {
                            let mid = merged.len() / 2;
                            let tail = merged.split_off(mid);
                            self.emit_group(lvl, PendingGroup { children: merged })?;
                            self.emit_group(lvl, PendingGroup { children: tail })?;
                        }
                    }
                }
            }
            lvl += 1;
        }
        // Unreachable: `flush_leaf` always creates level 0, every non-top
        // level that stops growing has emitted upward, and the cascade only
        // exits through the single-child-top return above.
        unreachable!("TreeStream cascade always terminates at a single-child top level")
    }

    fn record(&self, root: u64, depth: u16) -> DBRecord {
        DBRecord {
            root,
            branch_pages: self.branch_pages,
            leaf_pages: self.leaf_pages,
            overflow_pages: self.overflow_pages,
            entries: self.entries,
            depth,
            flags: 0,
            leaf2_ksize: 0,
        }
    }
}

/// Builds a whole env image with several databases through a [`PageSink`],
/// in [`TreeStream`]'s bounded memory: the named databases first, then the
/// main tree with their `F_SUBDATA` catalog records merged in key order,
/// then both meta slots.
///
/// Usage: [`named_db`](Self::named_db) once per named database, in ascending
/// name order, then [`finish_main`](Self::finish_main), which returns the
/// sink.
pub struct EnvStream<S: PageSink> {
    sink: S,
    psize: u32,
    txnid: u64,
    fill: u32,
    catalog: Vec<(Vec<u8>, DBRecord)>,
}

/// The handle [`EnvStream::finish_main`] passes to its `feed` callback: it
/// merges the named-database catalog records (`F_SUBDATA`) with the pushed
/// user entries in one ascending memcmp order, without collecting the user
/// entries.
pub struct MainStream<'s, S: PageSink> {
    ts: TreeStream<'s, S>,
    catalog: std::iter::Peekable<std::vec::IntoIter<(Vec<u8>, DBRecord)>>,
}

impl<S: PageSink> MainStream<'_, S> {
    /// Push one main-DB **user** entry (flags 0), key strictly ascending.
    ///
    /// # Errors
    ///
    /// As [`TreeStream::push`].
    pub fn push(&mut self, key: &[u8], val: &[u8]) -> Result<(), StreamBuildError> {
        while let Some((name, _)) = self.catalog.peek() {
            debug_assert!(
                name.as_slice() != key,
                "named-DB name collides with a main user key"
            );
            if name.as_slice() > key {
                break;
            }
            let (name, rec) = self.catalog.next().expect("peeked");
            self.ts.push(&name, F_SUBDATA, &rec.to_bytes())?;
        }
        self.ts.push(key, 0, val)
    }

    /// Drain any catalog records greater than every pushed user key.
    fn drain(&mut self) -> Result<(), StreamBuildError> {
        for (name, rec) in self.catalog.by_ref() {
            self.ts.push(&name, F_SUBDATA, &rec.to_bytes())?;
        }
        Ok(())
    }
}

impl<S: PageSink> EnvStream<S> {
    /// Start an env-image stream over `sink`.
    ///
    /// # Errors
    ///
    /// [`PageError::InvalidPageSize`].
    pub fn new(
        sink: S,
        psize: u32,
        txnid: u64,
        fill_permille: u32,
    ) -> Result<EnvStream<S>, PageError> {
        validate_page_size(psize)?;
        Ok(EnvStream {
            sink,
            psize,
            txnid,
            fill: fill_permille.clamp(1, 1000),
            catalog: Vec::new(),
        })
    }

    /// Build one named DB: `feed` pushes its entries (ascending, unique).
    /// Named DBs must arrive in ascending name order.
    ///
    /// # Errors
    ///
    /// As [`TreeStream::push`].
    pub fn named_db(
        &mut self,
        name: &[u8],
        feed: impl FnOnce(&mut TreeStream<'_, S>) -> Result<(), StreamBuildError>,
    ) -> Result<(), StreamBuildError> {
        debug_assert!(
            self.catalog.last().is_none_or(|(n, _)| n.as_slice() < name),
            "named DBs must be added in ascending name order"
        );
        let mut ts = TreeStream::new(&mut self.sink, self.psize, self.txnid, self.fill)?;
        feed(&mut ts)?;
        let rec = ts.finish()?;
        self.catalog.push((name.to_vec(), rec));
        Ok(())
    }

    /// Build the main tree (`feed` pushes the **user** entries; catalog
    /// records interleave automatically), then write both meta slots and
    /// return the sink.
    ///
    /// # Errors
    ///
    /// As [`TreeStream::push`].
    pub fn finish_main(
        self,
        map_size: u64,
        feed: impl FnOnce(&mut MainStream<'_, S>) -> Result<(), StreamBuildError>,
    ) -> Result<S, StreamBuildError> {
        let EnvStream {
            mut sink,
            psize,
            txnid,
            fill,
            catalog,
        } = self;
        let ts = TreeStream::new(&mut sink, psize, txnid, fill)?;
        let mut ms = MainStream {
            ts,
            catalog: catalog.into_iter().peekable(),
        };
        feed(&mut ms)?;
        ms.drain()?;
        let main_db = ms.ts.finish()?;

        let last_pg = sink.next_pgno() - 1;
        let ps = psize as usize;
        let mut frame = vec![0u8; ps];
        let mut meta = MetaPage {
            pgno: 0,
            txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: psize,
            env_flags: 0,
            map_size,
            last_pg,
            free_db: DBRecord::empty(),
            main_db,
            fl_count: 0,
        };
        meta.encode(&mut frame)?;
        sink.emit(0, &frame)?;
        meta.pgno = 1;
        frame.fill(0);
        meta.encode(&mut frame)?;
        sink.emit(1, &frame)?;
        Ok(sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{select_meta, MetaChoice, PageRef, PageType, PGNO_INVALID};

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 20;

    fn main_record(img: &[u8]) -> DBRecord {
        let s0 = MetaPage::validate(&img[0..PS as usize], PS).unwrap();
        let s1 = MetaPage::validate(&img[PS as usize..2 * PS as usize], PS).unwrap();
        match select_meta(&s0, &s1, false) {
            MetaChoice::Both { meta, .. } | MetaChoice::OnlyOne { meta, .. } => meta.main_db,
            MetaChoice::None => panic!("no valid meta"),
        }
    }

    /// Walk every reachable page, asserting: correct type per level, strictly
    /// ascending keys within a page (INV-5), and non-root branches with
    /// `>= 2` children (INV-8). Returns the set of entries in key order.
    fn walk(img: &[u8], rec: &DBRecord) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        if rec.root == PGNO_INVALID {
            assert_eq!(rec.depth, 0);
            return out;
        }
        walk_node(img, rec.root, rec.depth, true, &mut out);
        out
    }

    fn page(img: &[u8], pgno: u64) -> PageRef<'_> {
        let base = pgno as usize * PS as usize;
        PageRef::new(&img[base..base + PS as usize], PS).unwrap()
    }

    fn walk_node(
        img: &[u8],
        pgno: u64,
        level: u16,
        is_root: bool,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let p = page(img, pgno);
        match p.page_type() {
            PageType::Leaf => {
                assert_eq!(level, 1, "leaf must be at level 1");
                let leaf = p.as_leaf().unwrap();
                let mut prev: Option<Vec<u8>> = None;
                for i in 0..leaf.num_keys() {
                    let k = leaf.key(i).to_vec();
                    if let Some(pk) = &prev {
                        assert!(
                            pk.as_slice() < k.as_slice(),
                            "leaf keys must ascend (INV-5)"
                        );
                    }
                    let v = match leaf.value(i) {
                        crate::page::LeafValue::Inline(v) => v.to_vec(),
                        crate::page::LeafValue::Overflow { head_pgno, dsize } => {
                            let base = head_pgno as usize * PS as usize;
                            let run = &img[base..];
                            crate::page::OverflowRef::new(run, PS)
                                .unwrap()
                                .payload(dsize)
                                .unwrap()
                                .to_vec()
                        }
                    };
                    out.push((k.clone(), v));
                    prev = Some(k);
                }
            }
            PageType::Branch => {
                let br = p.as_branch().unwrap();
                if !is_root {
                    assert!(
                        br.num_keys() >= 2,
                        "non-root branch needs >= 2 children (INV-8)"
                    );
                }
                for i in 0..br.num_keys() {
                    walk_node(img, br.child_pgno(i), level - 1, false, out);
                }
            }
            other => panic!("unexpected page type {other:?} in tree"),
        }
    }

    fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), v.to_vec())
    }

    #[test]
    fn empty_image_opens_as_empty_main_db() {
        let img = build_single_db_image(PS, MAP, 1, &[], DEFAULT_FILL_PERMILLE).unwrap();
        let rec = main_record(&img);
        assert_eq!(rec.root, PGNO_INVALID);
        assert_eq!(rec.depth, 0);
        assert_eq!(rec.entries, 0);
        assert_eq!(walk(&img, &rec), Vec::new());
    }

    #[test]
    fn single_leaf_stats_and_walk() {
        let entries: Vec<_> = (0u16..10)
            .map(|i| kv(format!("k{i:02}").as_bytes(), b"v"))
            .collect();
        let img = build_single_db_image(PS, MAP, 7, &entries, DEFAULT_FILL_PERMILLE).unwrap();
        let rec = main_record(&img);
        assert_eq!(rec.depth, 1);
        assert_eq!(rec.leaf_pages, 1);
        assert_eq!(rec.branch_pages, 0);
        assert_eq!(rec.entries, 10);
        assert_eq!(walk(&img, &rec), entries);
    }

    #[test]
    fn multi_level_stats_and_invariants() {
        let entries: Vec<_> = (0u32..5000)
            .map(|i| {
                kv(
                    format!("key{i:06}").as_bytes(),
                    format!("val{i}").as_bytes(),
                )
            })
            .collect();
        let img = build_single_db_image(PS, 8 << 20, 3, &entries, DEFAULT_FILL_PERMILLE).unwrap();
        let rec = main_record(&img);
        assert!(rec.depth >= 2, "5000 entries need branch levels");
        assert!(rec.leaf_pages > 1);
        assert!(rec.branch_pages >= 1);
        assert_eq!(rec.entries, 5000);
        assert_eq!(walk(&img, &rec), entries);
    }

    #[test]
    fn overflow_stats() {
        let entries = vec![
            kv(b"a", &[1u8; 10]),
            kv(b"b", &[2u8; 9000]),   // 3 pages
            kv(b"c", &[3u8; 70_000]), // ~18 pages
        ];
        let img = build_single_db_image(PS, 1 << 20, 2, &entries, DEFAULT_FILL_PERMILLE).unwrap();
        let rec = main_record(&img);
        assert_eq!(rec.overflow_pages, 3 + 18);
        assert_eq!(walk(&img, &rec), entries);
    }

    #[test]
    fn large_keys_force_thin_branches() {
        // 511-byte keys: few children per branch page, so the last branch
        // groups need rebalancing.
        let entries: Vec<_> = (0u16..300)
            .map(|i| {
                let mut k = vec![b'a'; 509];
                k.extend_from_slice(&i.to_be_bytes());
                (k, b"v".to_vec())
            })
            .collect();
        let img = build_single_db_image(PS, 8 << 20, 1, &entries, DEFAULT_FILL_PERMILLE).unwrap();
        let rec = main_record(&img);
        assert!(rec.depth >= 2);
        assert_eq!(walk(&img, &rec), entries);
    }

    #[test]
    fn multi_db_image_is_check_clean() {
        // Main user data + two named DBs (one with multi-page overflow, one
        // empty), all packed into one image; the invariant walker follows every
        // F_SUBDATA catalog entry into its sub-tree.
        let main_user: Vec<_> = (0u32..2000)
            .map(|i| kv(format!("m{i:06}").as_bytes(), b"x"))
            .collect();
        let posts: Vec<_> = (0u32..1500)
            .map(|i| {
                kv(
                    format!("p{i:06}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                )
            })
            .collect();
        let big = vec![
            kv(b"blob-a", &[7u8; 40_000]),
            kv(b"blob-b", &[9u8; 100_000]),
        ];
        let named = vec![
            NamedDbData {
                name: b"posts",
                entries: &posts,
            },
            NamedDbData {
                name: b"blobs",
                entries: &big,
            },
            NamedDbData {
                name: b"empty",
                entries: &[],
            },
        ];
        let img = build_multi_db_image(PS, 16 << 20, 4, &main_user, &named, DEFAULT_FILL_PERMILLE)
            .unwrap();
        assert_eq!(
            crate::check::check_image(&img, PS),
            Vec::<String>::new(),
            "multi-DB image must be invariant-clean"
        );
    }

    #[test]
    fn empty_multi_db_image_is_check_clean() {
        let img = build_multi_db_image(PS, 1 << 20, 0, &[], &[], DEFAULT_FILL_PERMILLE).unwrap();
        assert_eq!(crate::check::check_image(&img, PS), Vec::<String>::new());
    }

    /// The streaming builder against the batch builder on the same mixed
    /// data (multi-level trees, a single-leaf tree, empty named databases on
    /// both sides of the user keys, multi-page overflow values). The page
    /// numbering may differ. Both images must pass the checker, hold the same
    /// logical content (catalog records are compared by following them, since
    /// their root pgnos differ), and have the same page counts per kind and
    /// the same depth, since both use the same packing rules.
    #[test]
    fn stream_builder_matches_batch_builder() {
        use std::collections::HashSet;

        let mk = |i: u32| format!("key{i:05}").into_bytes();
        let big = vec![0xEEu8; 9000]; // multi-page overflow at PS=4096
        let main_user: Vec<(Vec<u8>, Vec<u8>)> = (0..800)
            .map(|i| (mk(i * 3), format!("val-{i}").into_bytes()))
            .collect();
        let db_a: Vec<(Vec<u8>, Vec<u8>)> = (0..1200)
            .map(|i| (mk(i * 2), vec![b'a'; (i % 200) as usize + 1]))
            .collect();
        let db_b: Vec<(Vec<u8>, Vec<u8>)> =
            vec![(b"one".to_vec(), big), (b"two".to_vec(), b"small".to_vec())];
        let named = [
            NamedDbData {
                name: b"aempty",
                entries: &[],
            },
            NamedDbData {
                name: b"alpha",
                entries: &db_a,
            },
            NamedDbData {
                name: b"beta",
                entries: &db_b,
            },
            // Sorts after every "keyNNNNN" user key, so the stream must emit
            // this catalog record after the last user entry.
            NamedDbData {
                name: b"zeta",
                entries: &[],
            },
        ];

        let batch =
            build_multi_db_image(PS, MAP, 7, &main_user, &named, DEFAULT_FILL_PERMILLE).unwrap();

        let mut es = EnvStream::new(VecSink::new(PS), PS, 7, DEFAULT_FILL_PERMILLE).unwrap();
        for nd in &named {
            es.named_db(nd.name, |ts| {
                for (k, v) in nd.entries.iter() {
                    ts.push(k, 0, v)?;
                }
                Ok(())
            })
            .unwrap();
        }
        let sink = es
            .finish_main(MAP, |ms| {
                for (k, v) in &main_user {
                    ms.push(k, v)?;
                }
                Ok(())
            })
            .unwrap();
        let stream = sink.into_image();

        assert_eq!(crate::check::check_image(&batch, PS), Vec::<String>::new());
        assert_eq!(crate::check::check_image(&stream, PS), Vec::<String>::new());

        let br = main_record(&batch);
        let sr = main_record(&stream);
        assert_eq!(
            (
                br.entries,
                br.branch_pages,
                br.leaf_pages,
                br.overflow_pages,
                br.depth
            ),
            (
                sr.entries,
                sr.branch_pages,
                sr.leaf_pages,
                sr.overflow_pages,
                sr.depth
            ),
            "main-tree page economy must match"
        );

        let bm = walk(&batch, &br);
        let sm = walk(&stream, &sr);
        assert_eq!(bm.len(), sm.len(), "main entry counts");
        let names: HashSet<&[u8]> = named.iter().map(|n| n.name).collect();
        for ((bk, bv), (sk, sv)) in bm.iter().zip(&sm) {
            assert_eq!(bk, sk, "main key order");
            if names.contains(bk.as_slice()) {
                let brc = DBRecord::from_bytes(bv).unwrap();
                let src = DBRecord::from_bytes(sv).unwrap();
                assert_eq!(
                    (
                        brc.entries,
                        brc.branch_pages,
                        brc.leaf_pages,
                        brc.overflow_pages,
                        brc.depth
                    ),
                    (
                        src.entries,
                        src.branch_pages,
                        src.leaf_pages,
                        src.overflow_pages,
                        src.depth
                    ),
                    "named DB {:?} page economy",
                    String::from_utf8_lossy(bk)
                );
                assert_eq!(
                    walk(&batch, &brc),
                    walk(&stream, &src),
                    "named DB {:?} content",
                    String::from_utf8_lossy(bk)
                );
            } else {
                assert_eq!(
                    bv,
                    sv,
                    "main user value for {:?}",
                    String::from_utf8_lossy(bk)
                );
            }
        }
    }
}
