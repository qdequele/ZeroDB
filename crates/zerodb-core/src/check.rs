//! Tree invariant checker (SPEC 03 §11 INV-1..21 + SPEC 05 §9 INV-22..26).
//! The `zerodb-tools check` command wraps this walk.
//!
//! [`check_image`] validates a whole env-file image against the live meta's
//! snapshot and returns every violation found, each tagged with its INV id.
//! The **reachable-XOR-free** partition (INV-10/INV-22) is checked
//! unconditionally: every page in `[FIRST_DATA_PGNO, last_pg]` is either
//! reachable exactly once through a tree or listed exactly once as free in the
//! GC DB — never both (reuse-while-referenced), never neither (leak).
//! (INV-27, the `non_free_pages_size` identity, is asserted at the API level —
//! this walk has no `fstat`.)
//!
//! [`check_snapshot`] runs the same walk over one committed snapshot of a
//! live environment, read through a read txn's map, without the meta slots
//! (ADR-0023).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::page::geometry::{gc_key_decode, overflow_page_count};
use crate::page::{
    select_meta, DBRecord, LeafValue, MetaPage, OverflowRef, PageRef, PageType, DBRECORD_LEN,
    FIRST_DATA_PGNO, F_SUBDATA, MAX_DB_NAME, MAX_KEY_SIZE, MIN_KEYS_BRANCH, MIN_KEYS_LEAF,
    PGNO_INVALID,
};

/// Accumulated walk statistics, compared against a `DBRecord` (INV-18).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WalkStats {
    branch_pages: u64,
    leaf_pages: u64,
    overflow_pages: u64,
    entries: u64,
}

impl std::ops::AddAssign for WalkStats {
    fn add_assign(&mut self, o: WalkStats) {
        self.branch_pages += o.branch_pages;
        self.leaf_pages += o.leaf_pages;
        self.overflow_pages += o.overflow_pages;
        self.entries += o.entries;
    }
}

/// A set of page numbers in `[0, last_pg]`, one bit per page, that several
/// walking threads insert into concurrently.
///
/// Its size is bounded by the image: callers build it only after checking
/// that `(last_pg + 1) * psize` bytes are present, so it takes at most one
/// bit per page of the file. A bitmap rather than a hash set also means a
/// hostile file cannot pick page ids that collide.
struct PageSet {
    words: Vec<AtomicU64>,
}

impl PageSet {
    fn new(last_pg: u64) -> PageSet {
        let n = usize::try_from(last_pg / 64 + 1).expect("bounded by the image length");
        PageSet {
            words: (0..n).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Add `pgno` (which must be `<= last_pg`); `false` if it was already in.
    fn insert(&self, pgno: u64) -> bool {
        let bit = 1u64 << (pgno % 64);
        // Relaxed: the set is only a membership record. A read-modify-write
        // on one word is atomic at any ordering, so exactly one of several
        // threads inserting the same page sees its bit clear. Nothing else
        // is published through the bitmap; it is read back (`word`) only
        // after every walking thread has been joined, and the join orders
        // all their writes before that read.
        let old = self.words[(pgno / 64) as usize].fetch_or(bit, Ordering::Relaxed);
        old & bit == 0
    }

    /// The 64 membership bits for pages `64 * i .. 64 * i + 64`.
    fn word(&self, i: usize) -> u64 {
        // Relaxed: called only after the walking threads were joined (see
        // `insert`).
        self.words[i].load(Ordering::Relaxed)
    }
}

/// What every walk over one image shares: the image and the snapshot it
/// checks, and the set of pages reached so far.
struct Shared<'a> {
    bytes: &'a [u8],
    psize: u32,
    meta_txnid: u64,
    last_pg: u64,
    /// The selected meta's annex id count (INV-28/GC-29: `> 0` forbids a GC
    /// tree entry keyed `BE(meta_txnid)`).
    annex_count: usize,
    /// Pages reached by the tree walks (INV-16, INV-11, INV-22).
    visited: PageSet,
}

/// Work a parallel walk hands back to the coordinator instead of doing it
/// in place (see [`check_state`]).
enum Deferred {
    /// A named database's record found in the main catalog.
    Record { label: String, rec: DBRecord },
    /// One child subtree of a record's root branch page.
    Subtree(SubtreeJob),
}

/// A child subtree of a record's root branch, with the separator bounds the
/// root gives it (INV-6).
struct SubtreeJob {
    /// Index of the record this subtree belongs to, in the coordinator's
    /// record table.
    rec_idx: usize,
    pgno: u64,
    level: u16,
    low: Option<Vec<u8>>,
    high: Option<Vec<u8>>,
    gc_tree: bool,
    catalog: bool,
}

struct Checker<'s, 'a> {
    shared: &'s Shared<'a>,
    bytes: &'a [u8],
    psize: u32,
    meta_txnid: u64,
    last_pg: u64,
    annex_count: usize,
    /// Free page ids collected from every GC PIL **and the selected meta's
    /// free-list annex** → occurrence count (INV-22/INV-24/INV-28; SPEC 05
    /// §9/§2a). **Randomly seeded** (std `RandomState`), unlike the engine's
    /// unseeded pgno hasher: its keys are *on-disk* page ids, so a hostile
    /// file could otherwise choose ids that all collide (HashDoS).
    free: HashMap<u64, u64>,
    violations: Vec<String>,
    /// `None` for the serial walk, which walks everything in place, in the
    /// order the report lists findings. `Some` for one job of a parallel
    /// walk: a record's root branch defers its children, and a catalog leaf
    /// defers its named databases, to this list.
    deferred: Option<Vec<Deferred>>,
    /// The record being walked, for the subtrees it defers.
    rec_idx: usize,
}

impl<'s, 'a> Checker<'s, 'a> {
    fn new(shared: &'s Shared<'a>, deferred: bool) -> Self {
        Checker {
            shared,
            bytes: shared.bytes,
            psize: shared.psize,
            meta_txnid: shared.meta_txnid,
            last_pg: shared.last_pg,
            annex_count: shared.annex_count,
            free: HashMap::new(),
            violations: Vec::new(),
            deferred: deferred.then(Vec::new),
            rec_idx: 0,
        }
    }

    fn fail(&mut self, inv: &str, msg: String) {
        self.violations.push(format!("{inv}: {msg}"));
    }

    fn page(&self, pgno: u64) -> Option<PageRef<'a>> {
        let ps = self.psize as usize;
        let base = (pgno as usize).checked_mul(ps)?;
        let slice = self.bytes.get(base..)?;
        if slice.len() < ps {
            return None;
        }
        PageRef::new(slice, self.psize).ok()
    }

    /// Common per-page checks: in range (INV-17/25), self pgno (INV-4),
    /// writer stamp (INV-20), single parent (INV-16). Returns the page view or
    /// records why not.
    fn visit(&mut self, pgno: u64, what: &str) -> Option<PageRef<'a>> {
        if pgno < FIRST_DATA_PGNO || pgno > self.last_pg {
            self.fail(
                "INV-17",
                format!("{what} page {pgno} outside [2, last_pg={}]", self.last_pg),
            );
            return None;
        }
        if !self.shared.visited.insert(pgno) {
            self.fail(
                "INV-16",
                format!("{what} page {pgno} referenced more than once"),
            );
            return None;
        }
        let Some(page) = self.page(pgno) else {
            self.fail(
                "INV-9",
                format!("{what} page {pgno} is undecodable or beyond the file"),
            );
            return None;
        };
        if page.pgno() != pgno {
            self.fail(
                "INV-4",
                format!(
                    "page {pgno} self-identifies as {} (written pages must match)",
                    page.pgno()
                ),
            );
        }
        if page.txnid() > self.meta_txnid {
            self.fail(
                "INV-20",
                format!(
                    "page {pgno} stamped by future txn {} > live meta txnid {}",
                    page.txnid(),
                    self.meta_txnid
                ),
            );
        }
        Some(page)
    }

    /// Walk one tree (INV-5..9, 11–13, 16–20), accumulating stats.
    /// `low`/`high` are the separator bounds inherited from the parent
    /// (INV-6): every key in this subtree must be in `[low, high)`.
    #[allow(clippy::too_many_arguments)]
    fn walk(
        &mut self,
        pgno: u64,
        level: u16,
        is_root: bool,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        stats: &mut WalkStats,
        gc_tree: bool,
        catalog: bool,
    ) {
        let Some(page) = self.visit(pgno, if gc_tree { "GC" } else { "tree" }) else {
            return;
        };
        match page.page_type() {
            PageType::Leaf => {
                if level != 1 {
                    self.fail(
                        "INV-7",
                        format!("leaf {pgno} at level {level} (all leaves must be at depth)"),
                    );
                }
                stats.leaf_pages += 1;
                let Ok(leaf) = page.as_leaf() else {
                    self.fail("INV-9", format!("leaf {pgno} fails body validation"));
                    return;
                };
                if !is_root && leaf.num_keys() < MIN_KEYS_LEAF {
                    self.fail(
                        "INV-8",
                        format!("non-root leaf {pgno} holds {} < 1 keys", leaf.num_keys()),
                    );
                }
                let mut prev: Option<&[u8]> = None;
                // Named-DB catalog entries found on this leaf (main tree only);
                // their sub-trees are walked after the `leaf` borrow ends.
                let mut subdbs: Vec<(Vec<u8>, DBRecord)> = Vec::new();
                for i in 0..leaf.num_keys() {
                    stats.entries += 1;
                    let k = leaf.key(i);
                    if k.is_empty() || k.len() > MAX_KEY_SIZE {
                        self.fail(
                            "INV-12",
                            format!("leaf {pgno} entry {i} key len {}", k.len()),
                        );
                    }
                    if let Some(p) = prev {
                        if p >= k {
                            self.fail("INV-5", format!("leaf {pgno} keys not ascending at {i}"));
                        }
                    }
                    if let Some(lo) = low {
                        if k < lo {
                            self.fail("INV-6", format!("leaf {pgno} key below separator bound"));
                        }
                    }
                    if let Some(hi) = high {
                        if k >= hi {
                            self.fail("INV-6", format!("leaf {pgno} key at/above separator bound"));
                        }
                    }
                    prev = Some(k);
                    let is_subdata = leaf.node_flags(i) & F_SUBDATA != 0;
                    match leaf.value(i) {
                        LeafValue::Inline(v) => {
                            // Catalog entry (SPEC 02 §6): an `F_SUBDATA` value is
                            // a 48-byte sub-DB record on the main tree only.
                            if is_subdata {
                                if !catalog {
                                    self.fail(
                                        "INV-21",
                                        format!(
                                            "leaf {pgno} entry {i}: F_SUBDATA outside the main catalog"
                                        ),
                                    );
                                } else if k.len() > MAX_DB_NAME {
                                    self.fail(
                                        "INV-12",
                                        format!("catalog {pgno} entry {i}: DB name too long"),
                                    );
                                } else if let Some(rec) = DBRecord::from_bytes(v) {
                                    subdbs.push((k.to_vec(), rec));
                                } else {
                                    self.fail(
                                        "INV-21",
                                        format!(
                                            "catalog {pgno} entry {i}: F_SUBDATA value is {} bytes, not {DBRECORD_LEN}",
                                            v.len()
                                        ),
                                    );
                                }
                            }
                        }
                        LeafValue::Overflow { head_pgno, dsize } => {
                            if is_subdata {
                                self.fail(
                                    "INV-21",
                                    format!("catalog {pgno} entry {i}: sub-DB record on overflow"),
                                );
                            }
                            self.check_overflow(pgno, head_pgno, dsize, stats);
                        }
                    }
                    if gc_tree {
                        self.check_gc_entry(pgno, k, leaf.value(i));
                    }
                }
                // Walk each named-DB sub-tree: its pages join `visited`
                // (reachable-XOR-free) and its stats are validated against its
                // own record (INV-18). `leaf` borrows the immutable image (not `&mut self`),
                // so these `&mut self` calls are sound while it is in scope.
                for (name, rec) in subdbs {
                    let label = format!("subdb {:?}", String::from_utf8_lossy(&name));
                    match &mut self.deferred {
                        Some(d) => d.push(Deferred::Record { label, rec }),
                        None => self.check_record(&label, &rec, false),
                    }
                }
            }
            PageType::Branch => {
                stats.branch_pages += 1;
                let Ok(br) = page.as_branch() else {
                    self.fail("INV-9", format!("branch {pgno} fails body validation"));
                    return;
                };
                if level <= 1 {
                    self.fail("INV-7", format!("branch {pgno} at leaf level"));
                    return;
                }
                let n = br.num_keys();
                if (!is_root && n < MIN_KEYS_BRANCH) || (is_root && n < 2) {
                    self.fail("INV-8", format!("branch {pgno} holds {n} < 2 children"));
                }
                // Owned copies of separators so recursion doesn't hold a
                // borrow of `self`.
                let seps: Vec<Vec<u8>> = (0..n).map(|i| br.key(i).to_vec()).collect();
                let children: Vec<u64> = (0..n).map(|i| br.child_pgno(i)).collect();
                if n > 0 && !seps[0].is_empty() {
                    self.fail("INV-6", format!("branch {pgno} node 0 separator not empty"));
                }
                for i in 0..n {
                    // INV-6: child i covers [sep(i), sep(i+1)), with sep(0)
                    // inheriting the parent's low bound.
                    let lo: Option<&[u8]> = if i == 0 { low } else { Some(&seps[i]) };
                    let hi: Option<&[u8]> = if i + 1 < n { Some(&seps[i + 1]) } else { high };
                    if let (Some(l), Some(h)) = (lo, hi) {
                        if l >= h {
                            self.fail("INV-6", format!("branch {pgno} separators not ascending"));
                        }
                    }
                    // A parallel walk hands the root's children to other
                    // threads; their stats join this record's at the end.
                    if is_root {
                        if let Some(d) = &mut self.deferred {
                            d.push(Deferred::Subtree(SubtreeJob {
                                rec_idx: self.rec_idx,
                                pgno: children[i],
                                level: level - 1,
                                low: lo.map(<[u8]>::to_vec),
                                high: hi.map(<[u8]>::to_vec),
                                gc_tree,
                                catalog,
                            }));
                            continue;
                        }
                    }
                    self.walk(
                        children[i],
                        level - 1,
                        false,
                        lo,
                        hi,
                        stats,
                        gc_tree,
                        catalog,
                    );
                }
            }
            other => {
                self.fail(
                    "INV-9",
                    format!("unexpected page type {other:?} at tree page {pgno}"),
                );
            }
        }
    }

    /// INV-11: overflow head + run integrity.
    fn check_overflow(&mut self, leaf: u64, head: u64, dsize: u32, stats: &mut WalkStats) {
        let expect = overflow_page_count(dsize as u64, self.psize);
        // Checked: `head + expect - 1` on hostile values must not wrap.
        let run_end = expect
            .checked_sub(1)
            .and_then(|e| head.checked_add(e))
            .unwrap_or(u64::MAX);
        if head < FIRST_DATA_PGNO || run_end > self.last_pg {
            self.fail(
                "INV-11",
                format!("leaf {leaf}: overflow run [{head}, +{expect}) outside high-water"),
            );
            return;
        }
        // Every page of the run is claimed once (INV-11/16); interior pages
        // carry no header, so only the head decodes.
        for p in head..head + expect {
            if !self.shared.visited.insert(p) {
                self.fail("INV-11", format!("overflow page {p} shared/re-referenced"));
            }
        }
        let ps = self.psize as usize;
        // `head <= last_pg <= bytes.len()/ps` here, but keep it checked.
        let Some(base) = (head as usize).checked_mul(ps) else {
            self.fail("INV-11", format!("overflow head {head} beyond the file"));
            return;
        };
        let Some(run) = self.bytes.get(base..) else {
            self.fail("INV-11", format!("overflow head {head} beyond the file"));
            return;
        };
        match OverflowRef::new(run, self.psize) {
            Ok(ovf) => {
                if u64::from(ovf.ovf_pages()) != expect {
                    self.fail(
                        "INV-11",
                        format!(
                            "overflow head {head}: ovf_pages {} != ceil((32+{dsize})/psize) = {expect}",
                            ovf.ovf_pages()
                        ),
                    );
                }
                if ovf.txnid() > self.meta_txnid {
                    self.fail(
                        "INV-20",
                        format!("overflow head {head} stamped by future txn"),
                    );
                }
                if ovf.payload(dsize).is_err() {
                    self.fail(
                        "INV-11",
                        format!("overflow run at {head} cannot serve {dsize} bytes"),
                    );
                }
            }
            Err(e) => self.fail("INV-11", format!("overflow head {head} invalid: {e}")),
        }
        stats.overflow_pages += expect;
    }

    /// GC-entry well-formedness (INV-23/25/26): 8-byte big-endian txnid key
    /// `<=` the live meta txnid; PIL = count prefix + strictly-ascending ids in
    /// `[2, last_pg]`. Every id is also collected into `self.free` for the
    /// INV-22/INV-24 partition check.
    fn check_gc_entry(&mut self, leaf: u64, key: &[u8], val: LeafValue<'_>) {
        let Some(txnid) = gc_key_decode(key) else {
            self.fail("INV-23", format!("GC leaf {leaf}: key is not 8 bytes"));
            return;
        };
        if txnid > self.meta_txnid {
            self.fail("INV-23", format!("GC entry keyed by future txn {txnid}"));
        }
        // INV-28/GC-29 (format v2): a meta with a non-empty annex holds the
        // newest freeing-txn's PIL itself — a tree entry under the same
        // txnid would be the forbidden split placement (GC-32).
        if txnid == self.meta_txnid && self.annex_count > 0 {
            self.fail(
                "INV-28",
                format!("GC entry {txnid} coexists with a non-empty meta annex (GC-29/GC-32)"),
            );
        }
        let pil: Vec<u8> = match val {
            LeafValue::Inline(v) => v.to_vec(),
            LeafValue::Overflow { head_pgno, dsize } => {
                let ps = self.psize as usize;
                // Checked: `head_pgno` is raw on-disk data; the multiply must
                // not wrap into a bogus in-bounds offset.
                let Some(base) = (head_pgno as usize).checked_mul(ps) else {
                    self.fail("INV-26", format!("GC entry {txnid}: unreadable PIL"));
                    return;
                };
                match self
                    .bytes
                    .get(base..)
                    .and_then(|run| OverflowRef::new(run, self.psize).ok())
                    .and_then(|o| o.payload(dsize).ok())
                {
                    Some(p) => p.to_vec(),
                    None => {
                        self.fail("INV-26", format!("GC entry {txnid}: unreadable PIL"));
                        return;
                    }
                }
            }
        };
        if pil.len() < 8 || !pil.len().is_multiple_of(8) {
            self.fail(
                "INV-26",
                format!("GC entry {txnid}: PIL length {}", pil.len()),
            );
            return;
        }
        let count = u64::from_le_bytes(pil[0..8].try_into().expect("8 bytes"));
        if count as usize != pil.len() / 8 - 1 {
            self.fail("INV-26", format!("GC entry {txnid}: count prefix mismatch"));
            return;
        }
        let mut prev: Option<u64> = None;
        for chunk in pil[8..].as_chunks::<8>().0 {
            let id = u64::from_le_bytes(*chunk);
            if id < FIRST_DATA_PGNO || id > self.last_pg {
                self.fail(
                    "INV-25",
                    format!("GC entry {txnid}: free id {id} out of range"),
                );
            }
            if let Some(p) = prev {
                if p >= id {
                    self.fail("INV-26", format!("GC entry {txnid}: ids not ascending"));
                }
            }
            prev = Some(id);
            *self.free.entry(id).or_insert(0) += 1;
        }
    }

    /// INV-22 (the GC side of INV-10) + INV-24: after both tree walks, every
    /// page in `[FIRST_DATA_PGNO, last_pg]` is reachable XOR free, and no page
    /// id is GC-listed more than once.
    /// Reported in ascending page order.
    fn check_reachable_xor_free(&mut self) {
        let mut twice: Vec<(u64, u64)> = self
            .free
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(id, count)| (*id, *count))
            .collect();
        twice.sort_unstable();
        for (id, count) in twice {
            self.violations.push(format!(
                "INV-24: page {id} listed free {count} times across GC entries"
            ));
        }
        // The free ids as a bitmap over [0, last_pg]; ids outside it were
        // reported (INV-25/INV-28) and take no part in the partition.
        let words = self.shared.visited.words.len();
        let mut free_bits = vec![0u64; words];
        for &id in self.free.keys() {
            if (FIRST_DATA_PGNO..=self.last_pg).contains(&id) {
                free_bits[(id / 64) as usize] |= 1 << (id % 64);
            }
        }
        for (i, &free) in free_bits.iter().enumerate() {
            let reachable = self.shared.visited.word(i);
            // Pages of this word inside [FIRST_DATA_PGNO, last_pg].
            let first = (i as u64) * 64;
            let mut in_range = u64::MAX;
            if first < FIRST_DATA_PGNO {
                in_range &= u64::MAX << FIRST_DATA_PGNO;
            }
            if self.last_pg - first < 63 {
                in_range &= u64::MAX >> (63 - (self.last_pg - first));
            }
            let both = reachable & free & in_range;
            let mut odd = (both | !(reachable | free)) & in_range;
            while odd != 0 {
                let bit = odd.trailing_zeros();
                odd &= odd - 1;
                let pgno = first + u64::from(bit);
                self.violations.push(if both & (1 << bit) != 0 {
                    format!("INV-22: page {pgno} both reachable and GC-listed free")
                } else {
                    format!("INV-22: page {pgno} neither reachable nor GC-listed free (leaked)")
                });
            }
        }
    }

    /// Validate a `DBRecord` (`name` labels it in violation messages) and walk
    /// its tree. `gc_tree` marks the free/GC DB; `catalog` marks the main DB
    /// (so its `F_SUBDATA` leaf entries are followed as named-DB records,
    /// SPEC 02 §6). A named DB's own tree is neither (`catalog = false`).
    fn check_record(&mut self, name: &str, rec: &DBRecord, gc_tree: bool) {
        self.check_record_inner(name, rec, gc_tree, false);
    }

    /// The main catalog DB: `catalog = true` so `F_SUBDATA` entries are walked.
    fn check_catalog_record(&mut self, name: &str, rec: &DBRecord) {
        self.check_record_inner(name, rec, false, true);
    }

    fn check_record_inner(&mut self, name: &str, rec: &DBRecord, gc_tree: bool, catalog: bool) {
        if let Some(stats) = self.walk_record(name, rec, gc_tree, catalog) {
            self.check_stats(name, rec, stats);
        }
    }

    /// Check a record's own fields and walk its tree (deferring the root's
    /// children in a parallel walk). Returns the stats it counted, or `None`
    /// when the record is malformed and its tree was not walked.
    fn walk_record(
        &mut self,
        name: &str,
        rec: &DBRecord,
        gc_tree: bool,
        catalog: bool,
    ) -> Option<WalkStats> {
        // INV-19 root shape.
        if rec.depth == 0 && rec.root != PGNO_INVALID {
            self.fail("INV-19", format!("{name}: depth 0 but root {}", rec.root));
            return None;
        }
        if rec.depth > 0 && rec.root == PGNO_INVALID {
            self.fail("INV-19", format!("{name}: depth {} but no root", rec.depth));
            return None;
        }
        // INV-21 (record-level reserved fields).
        if rec.flags != 0 || rec.leaf2_ksize != 0 {
            self.fail("INV-21", format!("{name}: reserved DBRecord fields set"));
        }
        // Hostile-depth guard: `walk` recurses one frame per level, and
        // `rec.depth` is raw on-disk data (u16 → up to 65 535 frames → stack
        // overflow). Anything deeper than the read path's CURSOR_STACK bound
        // (32 — minimum fanout 2 already addresses 2^31 leaves there) is
        // corruption; report it and do not walk.
        if rec.depth as usize > crate::btree::CURSOR_STACK {
            self.fail(
                "INV-7",
                format!(
                    "{name}: depth {} exceeds the maximum legal tree depth {}",
                    rec.depth,
                    crate::btree::CURSOR_STACK
                ),
            );
            return None;
        }
        let mut stats = WalkStats::default();
        if rec.root != PGNO_INVALID {
            let root_page_type = self.page(rec.root).map(|p| p.page_type());
            match (rec.depth, root_page_type) {
                (1, Some(PageType::Leaf)) | (2.., Some(PageType::Branch)) => {}
                (d, t) => self.fail("INV-19", format!("{name}: depth {d} but root is {t:?}")),
            }
            self.walk(
                rec.root, rec.depth, true, None, None, &mut stats, gc_tree, catalog,
            );
        }
        Some(stats)
    }

    /// INV-18: the stats a record's walk counted match the record.
    fn check_stats(&mut self, name: &str, rec: &DBRecord, stats: WalkStats) {
        let expect = WalkStats {
            branch_pages: rec.branch_pages,
            leaf_pages: rec.leaf_pages,
            overflow_pages: rec.overflow_pages,
            entries: rec.entries,
        };
        if stats != expect {
            self.fail(
                "INV-18",
                format!("{name}: record says {expect:?}, walk found {stats:?}"),
            );
        }
    }
}

/// Check a whole env-file image against the live meta's snapshot. Returns the
/// list of violations (empty = clean). `bytes` should be the **file** contents
/// (not an over-long map): file-length invariants (INV-3/INV-17) are asserted
/// against `bytes.len()`.
///
/// A large image is walked on several threads; the report is the same as a
/// single-threaded walk's (see [`check_image_with_threads`]).
#[must_use]
pub fn check_image(bytes: &[u8], psize: u32) -> Vec<String> {
    check_image_inner(bytes, psize, None)
}

/// [`check_image`] on exactly `threads` threads (1 = the serial walk),
/// whatever the image size. Not part of the stable API: it exists so tests
/// can compare the serial and parallel walks. The report never depends on
/// `threads`: a parallel walk that finds anything is redone serially.
#[doc(hidden)]
#[must_use]
pub fn check_image_with_threads(bytes: &[u8], psize: u32, threads: usize) -> Vec<String> {
    check_image_inner(bytes, psize, Some(threads))
}

fn check_image_inner(bytes: &[u8], psize: u32, threads: Option<usize>) -> Vec<String> {
    let ps = psize as usize;
    let mut violations = Vec::new();
    if bytes.len() < 2 * ps {
        return vec!["INV-1: file shorter than the two meta slots".into()];
    }
    // INV-1/INV-2: slot validation + selection (SPEC 02 §3.2 owns the rules).
    let v0 = MetaPage::validate(&bytes[0..ps], psize);
    let v1 = MetaPage::validate(&bytes[ps..2 * ps], psize);
    let (v0, v1) = match (v0, v1) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return vec!["INV-1: meta slots undecodable".into()],
    };
    let (meta, chosen) = match select_meta(&v0, &v1, false) {
        crate::page::MetaChoice::Both { meta, chosen }
        | crate::page::MetaChoice::OnlyOne { meta, chosen } => (meta, chosen),
        crate::page::MetaChoice::None => {
            return vec!["INV-2: no valid meta slot (both torn/foreign)".into()]
        }
    };
    // INV-3: whole-page file.
    if !bytes.len().is_multiple_of(ps) {
        violations.push(format!(
            "INV-3: file length {} not a multiple of psize {psize}",
            bytes.len()
        ));
    }
    // INV-17: the file covers the high-water. Checked arithmetic — a hostile
    // `last_pg` near `u64::MAX` must not wrap the multiply — and an **early
    // return**: every later phase (the walks, and especially the
    // reachable-XOR-free sweep over `FIRST_DATA_PGNO..=last_pg`) is bounded
    // by `last_pg`, so a high-water the image cannot back would otherwise
    // loop ~2^64 times allocating a violation `String` per page.
    let needed = meta
        .last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(ps as u64));
    match needed {
        Some(n) if n <= bytes.len() as u64 => {}
        _ => {
            violations.push(format!(
                "INV-17: file length {} < (last_pg {} + 1) * psize",
                bytes.len(),
                meta.last_pg
            ));
            return violations;
        }
    }
    // Format v2 (ADR-0022): the selected meta's free-list annex ids are free
    // pages (SPEC 05 §2a).
    let annex = MetaPage::read_annex(&bytes[chosen * ps..(chosen + 1) * ps], psize)
        .expect("annex count was bounds-checked by MetaPage::validate");
    check_state(
        bytes,
        psize,
        &SnapshotRoots {
            txnid: meta.txnid,
            last_pg: meta.last_pg,
            main_db: &meta.main_db,
            free_db: &meta.free_db,
            annex: &annex,
        },
        violations,
        threads.unwrap_or_else(|| default_threads(meta.last_pg)),
    )
}

/// Check one committed snapshot of a live environment (ADR-0023): its roots,
/// its free tree, its free-list annex and its `last_pg`, read through `map`,
/// the mapped data file a read txn pinning `snap` borrows
/// ([`crate::rotxn::RoTxn::map_bytes`]). Returns the violations (empty =
/// clean), as [`check_image`].
///
/// Unlike [`check_image`] it never reads the meta slots, which another
/// process may be overwriting, and it reads only pages the snapshot
/// references: the pages it lists as free may be rewritten by a writer while
/// the walk runs. `map` may be longer than the file; the file-length
/// invariants are asserted against `(last_pg + 1) * psize` instead.
#[must_use]
pub fn check_snapshot(map: &[u8], psize: u32, snap: &crate::env::Snapshot) -> Vec<String> {
    let needed = snap
        .last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(u64::from(psize)))
        .and_then(|n| usize::try_from(n).ok());
    let bytes = match needed {
        Some(n) if n <= map.len() => &map[..n],
        _ => {
            return vec![format!(
                "INV-17: map length {} < (last_pg {} + 1) * psize",
                map.len(),
                snap.last_pg
            )]
        }
    };
    check_state(
        bytes,
        psize,
        &SnapshotRoots {
            txnid: snap.txnid,
            last_pg: snap.last_pg,
            main_db: &snap.main_db,
            free_db: &snap.free_db,
            annex: &snap.free_annex,
        },
        Vec::new(),
        default_threads(snap.last_pg),
    )
}

/// The parts of a committed state the walk starts from.
struct SnapshotRoots<'a> {
    txnid: u64,
    last_pg: u64,
    main_db: &'a DBRecord,
    free_db: &'a DBRecord,
    annex: &'a [u64],
}

/// Images smaller than this many pages (64 MiB at 4 KiB pages) are walked on
/// the calling thread: their walk takes milliseconds, less than starting
/// threads would save.
const PARALLEL_MIN_PAGES: u64 = 16_384;

/// The most threads one check uses.
const MAX_THREADS: usize = 16;

/// How many threads to walk an image of `last_pg + 1` pages with.
fn default_threads(last_pg: u64) -> usize {
    if last_pg < PARALLEL_MIN_PAGES {
        return 1;
    }
    std::thread::available_parallelism().map_or(1, |n| n.get().min(MAX_THREADS))
}

/// Walk the state `roots` describes over `bytes`, which covers at least
/// pages `0..=roots.last_pg`, appending to `violations`.
///
/// With `threads > 1` and no violation found so far, the trees are first
/// walked in parallel. A clean parallel walk is the answer. If it finds
/// anything, the state is walked again on this thread and that walk's
/// findings are returned, so the report (which findings, in which order) is
/// always the serial walk's. The two walks agree on whether the state is
/// clean: they run the same per-page and per-record checks over the same
/// pages, and the only check that depends on visiting order, "page reached
/// twice" (INV-16, INV-11), fires in both whenever some page is reachable
/// twice, since the reached-pages set is shared by all threads.
fn check_state(
    bytes: &[u8],
    psize: u32,
    roots: &SnapshotRoots<'_>,
    mut violations: Vec<String>,
    threads: usize,
) -> Vec<String> {
    // Validate the annex's GC-29 shape (the INV-25/26 analogues, labelled
    // INV-28) and count its ids into the free set for the INV-22/24
    // partition.
    let mut free: HashMap<u64, u64> = HashMap::new();
    let mut prev: Option<u64> = None;
    for &id in roots.annex {
        if id < FIRST_DATA_PGNO || id > roots.last_pg {
            violations.push(format!("INV-28: annex free id {id} out of range"));
        }
        if let Some(p) = prev {
            if p >= id {
                violations.push("INV-28: annex ids not strictly ascending".into());
            }
        }
        prev = Some(id);
        *free.entry(id).or_insert(0) += 1;
    }
    let shared = || Shared {
        bytes,
        psize,
        meta_txnid: roots.txnid,
        last_pg: roots.last_pg,
        annex_count: roots.annex.len(),
        visited: PageSet::new(roots.last_pg),
    };
    if threads > 1 && violations.is_empty() {
        let found = walk_parallel(&shared(), roots, free.clone(), threads);
        if found.is_empty() {
            return found;
        }
    }
    let shared = shared();
    let mut checker = Checker::new(&shared, false);
    checker.free = free;
    checker.violations = violations;
    checker.check_catalog_record("main_db", roots.main_db);
    checker.check_record("free_db", roots.free_db, true);
    // INV-10 / INV-22 + INV-24 (reachable XOR free, unconditional).
    checker.check_reachable_xor_free();
    checker.violations
}

/// A database record in a parallel walk, with the stats its jobs counted.
struct RecordSlot {
    label: String,
    rec: DBRecord,
    stats: WalkStats,
    /// Its tree was walked (the record is well formed), so INV-18 applies.
    walked: bool,
}

/// One unit of a parallel walk.
enum Job {
    /// Check record `idx`'s fields and its root page; defer the root's
    /// children.
    Record {
        idx: usize,
        gc_tree: bool,
        catalog: bool,
    },
    /// Walk a whole subtree.
    Subtree(SubtreeJob),
}

/// What one job found.
struct JobOut {
    violations: Vec<String>,
    free: HashMap<u64, u64>,
    deferred: Vec<Deferred>,
    rec_idx: usize,
    /// The job's share of its record's stats; `None` for a malformed
    /// record, whose tree is not walked.
    stats: Option<WalkStats>,
}

/// The parallel walk: rounds of independent jobs. The first round checks the
/// main and free-tree records and their root pages; each later round walks
/// the root children and named databases the previous round deferred. A
/// record's stats are the sum of its jobs' and are compared (INV-18) once
/// every round is done; then the reachable-XOR-free partition is checked
/// over the shared reached-pages set.
fn walk_parallel(
    shared: &Shared<'_>,
    roots: &SnapshotRoots<'_>,
    mut free: HashMap<u64, u64>,
    threads: usize,
) -> Vec<String> {
    let slot = |label: &str, rec: &DBRecord| RecordSlot {
        label: label.to_owned(),
        rec: *rec,
        stats: WalkStats::default(),
        walked: false,
    };
    let mut records = vec![
        slot("main_db", roots.main_db),
        slot("free_db", roots.free_db),
    ];
    let mut jobs = vec![
        Job::Record {
            idx: 0,
            gc_tree: false,
            catalog: true,
        },
        Job::Record {
            idx: 1,
            gc_tree: true,
            catalog: false,
        },
    ];
    let mut violations = Vec::new();
    while !jobs.is_empty() {
        let outs = run_round(shared, &records, &jobs, threads);
        jobs.clear();
        for out in outs {
            violations.extend(out.violations);
            for (id, n) in out.free {
                *free.entry(id).or_insert(0) += n;
            }
            if let Some(stats) = out.stats {
                let r = &mut records[out.rec_idx];
                r.stats += stats;
                r.walked = true;
            }
            for d in out.deferred {
                match d {
                    Deferred::Record { label, rec } => {
                        jobs.push(Job::Record {
                            idx: records.len(),
                            gc_tree: false,
                            catalog: false,
                        });
                        records.push(slot(&label, &rec));
                    }
                    Deferred::Subtree(s) => jobs.push(Job::Subtree(s)),
                }
            }
        }
    }
    let mut checker = Checker::new(shared, false);
    checker.free = free;
    checker.violations = violations;
    for r in records.iter().filter(|r| r.walked) {
        checker.check_stats(&r.label, &r.rec, r.stats);
    }
    checker.check_reachable_xor_free();
    checker.violations
}

/// Run one round's `jobs` on up to `threads` threads; the results come back
/// in job order.
fn run_round(
    shared: &Shared<'_>,
    records: &[RecordSlot],
    jobs: &[Job],
    threads: usize,
) -> Vec<JobOut> {
    let run = |job: &Job| {
        let mut c = Checker::new(shared, true);
        let stats = match job {
            Job::Record {
                idx,
                gc_tree,
                catalog,
            } => {
                c.rec_idx = *idx;
                let r = &records[*idx];
                c.walk_record(&r.label, &r.rec, *gc_tree, *catalog)
            }
            Job::Subtree(j) => {
                c.rec_idx = j.rec_idx;
                let mut stats = WalkStats::default();
                c.walk(
                    j.pgno,
                    j.level,
                    false,
                    j.low.as_deref(),
                    j.high.as_deref(),
                    &mut stats,
                    j.gc_tree,
                    j.catalog,
                );
                Some(stats)
            }
        };
        JobOut {
            violations: c.violations,
            free: c.free,
            deferred: c.deferred.unwrap_or_default(),
            rec_idx: c.rec_idx,
            stats,
        }
    };
    let workers = threads.min(jobs.len());
    if workers <= 1 {
        return jobs.iter().map(run).collect();
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                s.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        // Relaxed: the counter only hands out distinct job
                        // indexes; the jobs themselves were written before
                        // the threads were spawned.
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(job) = jobs.get(i) else { break };
                        done.push((i, run(job)));
                    }
                    done
                })
            })
            .collect();
        let mut all: Vec<(usize, JobOut)> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect();
        all.sort_unstable_by_key(|(i, _)| *i);
        all.into_iter().map(|(_, out)| out).collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::build_single_db_image;

    const PS: u32 = 4096;

    #[test]
    fn builder_images_are_clean() {
        let entries: Vec<_> = (0u32..3000)
            .map(|i| {
                (
                    format!("key{i:06}").into_bytes(),
                    format!("val{i}").into_bytes(),
                )
            })
            .collect();
        let img = build_single_db_image(PS, 8 << 20, 3, &entries, 900).unwrap();
        assert_eq!(check_image(&img, PS), Vec::<String>::new());
    }

    /// The snapshot walk reads the same state as the image walk, from a map
    /// that may run past the file, and never the meta slots.
    #[test]
    fn snapshot_walk_matches_the_image_walk() {
        let entries: Vec<_> = (0u32..3000)
            .map(|i| (format!("key{i:06}").into_bytes(), b"v".to_vec()))
            .collect();
        let img = build_single_db_image(PS, 8 << 20, 3, &entries, 900).unwrap();
        let ps = PS as usize;
        let slot = |i: usize| match MetaPage::validate(&img[i * ps..(i + 1) * ps], PS) {
            Ok(crate::page::MetaValidity::Valid(m)) => Some(m),
            _ => None,
        };
        let (meta, i) = match (slot(0), slot(1)) {
            (Some(a), Some(b)) if b.txnid > a.txnid => (b, 1),
            (Some(a), _) => (a, 0),
            (None, Some(b)) => (b, 1),
            (None, None) => panic!("no valid meta slot"),
        };
        let annex = MetaPage::read_annex(&img[i * ps..(i + 1) * ps], PS).unwrap();
        let snap = crate::env::Snapshot::from_meta(&meta, annex);

        // A map longer than the file, with both meta slots zeroed: still clean.
        let mut map = img.clone();
        map.resize(img.len() + 4 * ps, 0);
        map[..2 * ps].fill(0);
        assert_eq!(check_snapshot(&map, PS, &snap), Vec::<String>::new());

        // A map that does not reach last_pg.
        let short = &img[..img.len() - ps];
        let v = check_snapshot(short, PS, &snap);
        assert!(
            v.iter().any(|s| s.starts_with("INV-17")),
            "expected INV-17, got {v:?}"
        );

        // A reachable page corrupted in the map is reported.
        let mut bad = img.clone();
        bad[2 * ps + 8..2 * ps + 16].copy_from_slice(&999u64.to_le_bytes());
        assert!(!check_snapshot(&bad, PS, &snap).is_empty());
    }

    #[test]
    fn detects_stat_drift() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let mut img = build_single_db_image(PS, 1 << 20, 1, &entries, 900).unwrap();
        // Corrupt main_db.entries in both slots (and refresh their CRCs) so
        // selection still succeeds but INV-18 must fire.
        for slot in 0..2usize {
            let base = slot * PS as usize;
            let e_off = base + 120 + 32; // main_db record + entries offset
            img[e_off] = 99;
            // Format v2 CRC: fl_count (offset 168) is 0 in builder images,
            // so the coverage is exactly [0, 172); the CRC field sits at 172.
            let crc = crate::page::crc32c(&img[base..base + 172]);
            img[base + 172..base + 176].copy_from_slice(&crc.to_le_bytes());
        }
        let v = check_image(&img, PS);
        assert!(
            v.iter().any(|s| s.starts_with("INV-18")),
            "expected INV-18, got {v:?}"
        );
    }

    #[test]
    fn detects_future_txnid() {
        let entries = vec![(b"a".to_vec(), b"1".to_vec())];
        let mut img = build_single_db_image(PS, 1 << 20, 5, &entries, 900).unwrap();
        // Stamp the root leaf (page 2) with a future txnid.
        let base = 2 * PS as usize;
        img[base + 8..base + 16].copy_from_slice(&999u64.to_le_bytes());
        let v = check_image(&img, PS);
        assert!(
            v.iter().any(|s| s.starts_with("INV-20")),
            "expected INV-20, got {v:?}"
        );
    }

    /// Entries per database in the parallel-walk tests: enough for branch
    /// roots (about 85 entries fit a leaf), small under miri.
    const TEST_ENTRIES: u32 = if cfg!(miri) { 120 } else { 1500 };

    /// A multi-DB image whose main tree and named databases have branch
    /// roots and overflow values, so a parallel walk defers both subtrees
    /// and named databases.
    fn multi_db_image(n: u32) -> Vec<u8> {
        let mk = |prefix: &str| -> Vec<(Vec<u8>, Vec<u8>)> {
            (0..n)
                .map(|i| {
                    let len = if i % 61 == 0 {
                        5000
                    } else {
                        8 + (i % 40) as usize
                    };
                    (format!("{prefix}{i:06}").into_bytes(), vec![b'v'; len])
                })
                .collect()
        };
        let main = mk("m");
        let (a, b, c) = (mk("a"), mk("b"), mk("c"));
        let named = [
            crate::builder::NamedDbData {
                name: b"db-a",
                entries: &a,
            },
            crate::builder::NamedDbData {
                name: b"db-b",
                entries: &b,
            },
            crate::builder::NamedDbData {
                name: b"db-c",
                entries: &c,
            },
        ];
        crate::builder::build_multi_db_image(PS, 64 << 20, 7, &main, &named, 900).unwrap()
    }

    /// The selected meta of a clean image and its annex.
    fn meta_of(img: &[u8]) -> (MetaPage, Vec<u64>) {
        let ps = PS as usize;
        let v0 = MetaPage::validate(&img[..ps], PS).unwrap();
        let v1 = MetaPage::validate(&img[ps..2 * ps], PS).unwrap();
        match select_meta(&v0, &v1, false) {
            crate::page::MetaChoice::Both { meta, chosen }
            | crate::page::MetaChoice::OnlyOne { meta, chosen } => {
                let annex = MetaPage::read_annex(&img[chosen * ps..(chosen + 1) * ps], PS).unwrap();
                (meta, annex)
            }
            crate::page::MetaChoice::None => panic!("no valid meta"),
        }
    }

    /// Whether the parallel walk alone (without the serial redo) finds the
    /// state of `img` clean, given the meta of the uncorrupted image.
    fn parallel_alone_is_clean(img: &[u8], meta: &MetaPage, annex: &[u64]) -> bool {
        let shared = Shared {
            bytes: img,
            psize: PS,
            meta_txnid: meta.txnid,
            last_pg: meta.last_pg,
            annex_count: annex.len(),
            visited: PageSet::new(meta.last_pg),
        };
        let roots = SnapshotRoots {
            txnid: meta.txnid,
            last_pg: meta.last_pg,
            main_db: &meta.main_db,
            free_db: &meta.free_db,
            annex,
        };
        let mut free = HashMap::new();
        for &id in annex {
            *free.entry(id).or_insert(0) += 1;
        }
        walk_parallel(&shared, &roots, free, 4).is_empty()
    }

    /// The branch pages of `img` and their child page numbers.
    fn branches(img: &[u8], last_pg: u64) -> Vec<(u64, Vec<u64>)> {
        let ps = PS as usize;
        (FIRST_DATA_PGNO..=last_pg)
            .filter_map(|pgno| {
                let page = PageRef::new(&img[pgno as usize * ps..], PS).ok()?;
                if page.page_type() != PageType::Branch {
                    return None;
                }
                let br = page.as_branch().ok()?;
                Some((pgno, (0..br.num_keys()).map(|i| br.child_pgno(i)).collect()))
            })
            .collect()
    }

    #[test]
    fn parallel_walk_of_a_clean_image_is_clean() {
        let img = multi_db_image(TEST_ENTRIES);
        let (meta, annex) = meta_of(&img);
        assert!(meta.main_db.depth >= 2, "the main tree needs a branch root");
        assert!(parallel_alone_is_clean(&img, &meta, &annex));
        for threads in [1, 2, 4] {
            assert_eq!(
                check_image_with_threads(&img, PS, threads),
                Vec::<String>::new()
            );
        }
    }

    /// A page reached from two subtrees that different threads walk is
    /// found by the parallel walk too, and the report is the serial one.
    #[test]
    fn page_shared_across_parallel_subtrees_is_reported() {
        let mut img = multi_db_image(TEST_ENTRIES);
        let (meta, annex) = meta_of(&img);
        let ps = PS as usize;
        let root = meta.main_db.root;
        let (_, children) = branches(&img, meta.last_pg)
            .into_iter()
            .find(|(p, _)| *p == root)
            .expect("the main root is a branch");
        // Point the root's last child at its first: two subtrees, one page.
        let (first, last) = (children[0], *children.last().unwrap());
        let page = &mut img[root as usize * ps..(root as usize + 1) * ps];
        let at = page
            .windows(8)
            .position(|w| w == last.to_le_bytes())
            .expect("child pointer bytes");
        page[at..at + 8].copy_from_slice(&first.to_le_bytes());
        let serial = check_image_with_threads(&img, PS, 1);
        assert!(
            serial.iter().any(|v| v.starts_with("INV-16")),
            "expected INV-16, got {serial:?}"
        );
        assert!(!parallel_alone_is_clean(&img, &meta, &annex));
        assert_eq!(check_image_with_threads(&img, PS, 4), serial);
    }

    fn prop_config() -> proptest::test_runner::Config {
        proptest::test_runner::Config {
            failure_persistence: None,
            cases: if cfg!(miri) { 16 } else { 256 },
            ..proptest::test_runner::Config::default()
        }
    }

    /// The proptest's uncorrupted image and its branch pages, built once.
    static PROP_IMAGE: std::sync::OnceLock<(Vec<u8>, Vec<Branch>)> = std::sync::OnceLock::new();

    /// A branch page number and its child page numbers.
    type Branch = (u64, Vec<u64>);

    proptest::proptest! {
        #![proptest_config(prop_config())]
        /// Over random corruptions of the data pages (byte flips, redirected
        /// child pointers, pages copied over others), the parallel walk alone
        /// finds the state clean exactly when the serial walk does, and the
        /// report does not depend on the thread count.
        #[test]
        fn parallel_and_serial_walks_agree(
            edits in proptest::collection::vec(
                (0u8..3, proptest::prelude::any::<u64>(), proptest::prelude::any::<u64>(), proptest::prelude::any::<u8>()),
                1..4,
            )
        ) {
            let (clean, branch_list) = PROP_IMAGE.get_or_init(|| {
                let img = multi_db_image(TEST_ENTRIES);
                let last_pg = meta_of(&img).0.last_pg;
                let branch_list = branches(&img, last_pg);
                (img, branch_list)
            });
            let mut img = clean.clone();
            let (meta, annex) = meta_of(&img);
            let ps = PS as usize;
            let data_pages = meta.last_pg - FIRST_DATA_PGNO + 1;
            for (kind, a, b, byte) in edits {
                match kind {
                    // Flip one byte of a data page.
                    0 => {
                        let at = (FIRST_DATA_PGNO + a % data_pages) as usize * ps + (b % PS as u64) as usize;
                        img[at] ^= byte | 1;
                    }
                    // Redirect one branch child to any data page.
                    1 => {
                        let (pgno, children) = &branch_list[(a % branch_list.len() as u64) as usize];
                        let old = children[(b % children.len() as u64) as usize];
                        let new = FIRST_DATA_PGNO + (b / 7 + u64::from(byte)) % data_pages;
                        let page = &mut img[*pgno as usize * ps..(*pgno as usize + 1) * ps];
                        if let Some(at) = page.windows(8).position(|w| w == old.to_le_bytes()) {
                            page[at..at + 8].copy_from_slice(&new.to_le_bytes());
                        }
                    }
                    // Copy one data page over another.
                    _ => {
                        let from = (FIRST_DATA_PGNO + a % data_pages) as usize;
                        let to = (FIRST_DATA_PGNO + b % data_pages) as usize;
                        img.copy_within(from * ps..(from + 1) * ps, to * ps);
                    }
                }
            }
            let serial = check_image_with_threads(&img, PS, 1);
            proptest::prop_assert_eq!(serial.is_empty(), parallel_alone_is_clean(&img, &meta, &annex));
            proptest::prop_assert_eq!(check_image_with_threads(&img, PS, 4), serial);
        }
    }
}
