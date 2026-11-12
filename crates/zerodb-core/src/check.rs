//! Tree invariant checker (SPEC 03 §11 INV-1..21 + SPEC 05 §9 INV-22..29).
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
//!
//! Key order (INV-5, INV-6) is evaluated in byte order, which is the order
//! of the main database, the free tree and every named database without a
//! custom comparator. A file does not record a named database's comparator
//! ([`crate::cmp`]), so a caller that knows it says so through
//! [`check_image_with_key_order`] / [`check_snapshot_with_key_order`];
//! otherwise a database written under a custom comparator reports INV-5 and
//! INV-6 violations.

use std::cmp::Ordering as KeyOrdering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::cmp::Comparator;
use crate::page::geometry::{gc_chunk_cap, gc_key_decode, overflow_page_count, pil_decode, GcKey};
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

/// The ordering `check` evaluates a named database's keys under (INV-5,
/// INV-6). The main database and the free tree are always byte-ordered.
#[derive(Clone, Copy)]
pub enum KeyOrder<'a> {
    /// Unsigned lexicographic byte order: the order of every database that
    /// has no custom comparator.
    Bytewise,
    /// The comparator the database was written under. INV-5 and INV-6 are
    /// evaluated with it, at full strength.
    Custom(&'a dyn Comparator),
    /// The database was written under a comparator the caller cannot supply.
    /// Its key-order checks are skipped, except that two adjacent keys of a
    /// page with identical bytes are still an INV-5 violation, since they are
    /// equal under every ordering. Every other invariant is checked as usual.
    Unknown,
}

impl KeyOrder<'_> {
    /// How `a` orders against `b`, or `None` when the ordering is unknown.
    fn compare(&self, a: &[u8], b: &[u8]) -> Option<KeyOrdering> {
        match self {
            KeyOrder::Bytewise => Some(a.cmp(b)),
            KeyOrder::Custom(c) => Some(c.compare(a, b)),
            KeyOrder::Unknown => None,
        }
    }

    /// Whether `a` is known not to sort strictly before `b`.
    fn not_before(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b).is_some_and(|o| o != KeyOrdering::Less)
    }
}

/// Maps a named database's name to the ordering its keys were written under
/// ([`check_image_with_key_order`]).
pub type NamedKeyOrder<'a> = dyn Fn(&[u8]) -> KeyOrder<'a> + Sync + 'a;

/// What every walk over one image shares: the image and the snapshot it
/// checks, and the set of pages reached so far.
struct Shared<'a> {
    bytes: &'a [u8],
    psize: u32,
    meta_txnid: u64,
    last_pg: u64,
    /// The selected meta's annex id count (INV-28/GC-29: `> 0` forbids a GC
    /// tree record of `meta_txnid`).
    annex_count: usize,
    /// Pages reached by the tree walks (INV-16, INV-11, INV-22).
    visited: PageSet,
    /// The key order of each named database; `None` means byte order for
    /// all of them.
    order_of: Option<&'a NamedKeyOrder<'a>>,
}

/// Work a parallel walk hands back to the coordinator instead of doing it
/// in place (see [`check_state`]).
enum Deferred<'a> {
    /// A named database's record found in the main catalog.
    Record {
        label: String,
        rec: DBRecord,
        order: KeyOrder<'a>,
    },
    /// One child subtree of a record's root branch page.
    Subtree(SubtreeJob<'a>),
}

/// Which kind of tree a walk is in: it decides how leaf entries are read and
/// which ordering the keys follow.
#[derive(Clone, Copy)]
enum TreeKind<'a> {
    /// The main DB: its `F_SUBDATA` entries are named-DB records, followed
    /// as trees of their own (SPEC 02 §6).
    Catalog,
    /// The free/GC DB: its entries are free-page lists (SPEC 05).
    Gc,
    /// A named database's own tree, with the ordering of its keys.
    Named(KeyOrder<'a>),
}

impl<'a> TreeKind<'a> {
    /// The ordering this tree's keys follow: only a named database's can
    /// differ from byte order.
    fn order(self) -> KeyOrder<'a> {
        match self {
            TreeKind::Catalog | TreeKind::Gc => KeyOrder::Bytewise,
            TreeKind::Named(order) => order,
        }
    }
}

/// A child subtree of a record's root branch, with the separator bounds the
/// root gives it (INV-6).
struct SubtreeJob<'a> {
    /// Index of the record this subtree belongs to, in the coordinator's
    /// record table.
    rec_idx: usize,
    pgno: u64,
    level: u16,
    low: Option<Vec<u8>>,
    high: Option<Vec<u8>>,
    kind: TreeKind<'a>,
}

struct Checker<'s, 'a> {
    shared: &'s Shared<'a>,
    bytes: &'a [u8],
    psize: u32,
    meta_txnid: u64,
    last_pg: u64,
    annex_count: usize,
    /// Free page ids collected from every GC PIL and from the selected
    /// meta's free-list annex, with their occurrence counts
    /// (INV-22/INV-24/INV-28; SPEC 05 §9/§2a). The map is randomly seeded
    /// (std `RandomState`): its keys are page ids read from the file, so
    /// under a fixed hash a hostile file could choose ids that all collide.
    free: HashMap<u64, u64>,
    /// The shape of every GC record walked, for the INV-29 range check once
    /// the whole GC tree has been read.
    gc_records: Vec<GcRecordShape>,
    violations: Vec<String>,
    /// `None` for the serial walk, which walks everything in place, in the
    /// order the report lists findings. `Some` for one job of a parallel
    /// walk: a record's root branch defers its children, and a catalog leaf
    /// defers its named databases, to this list.
    deferred: Option<Vec<Deferred<'a>>>,
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
            gc_records: Vec::new(),
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
    /// `(low, high)` are the separator bounds inherited from the parent
    /// (INV-6): every key in this subtree must be in `[low, high)`.
    fn walk(
        &mut self,
        pgno: u64,
        level: u16,
        is_root: bool,
        (low, high): (Option<&[u8]>, Option<&[u8]>),
        stats: &mut WalkStats,
        kind: TreeKind<'a>,
    ) {
        let what = if matches!(kind, TreeKind::Gc) {
            "GC"
        } else {
            "tree"
        };
        let order = kind.order();
        let Some(page) = self.visit(pgno, what) else {
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
                        // Under an unknown ordering only identical bytes are
                        // known to be out of order: no ordering separates them.
                        let out_of_order = match order.compare(p, k) {
                            Some(o) => o != KeyOrdering::Less,
                            None => p == k,
                        };
                        if out_of_order {
                            self.fail("INV-5", format!("leaf {pgno} keys not ascending at {i}"));
                        }
                    }
                    if let Some(lo) = low {
                        if order.compare(k, lo) == Some(KeyOrdering::Less) {
                            self.fail("INV-6", format!("leaf {pgno} key below separator bound"));
                        }
                    }
                    if let Some(hi) = high {
                        if order.not_before(k, hi) {
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
                                if !matches!(kind, TreeKind::Catalog) {
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
                    if matches!(kind, TreeKind::Gc) {
                        self.check_gc_entry(pgno, k, leaf.value(i));
                    }
                }
                // Walk each named-DB sub-tree: its pages join `visited`
                // (reachable-XOR-free) and its stats are validated against its
                // own record (INV-18). `leaf` borrows the image, not `self`, so
                // these `&mut self` calls are allowed while it is in scope.
                for (name, rec) in subdbs {
                    let label = format!("subdb {:?}", String::from_utf8_lossy(&name));
                    let order = self
                        .shared
                        .order_of
                        .map_or(KeyOrder::Bytewise, |order_of| order_of(&name));
                    match &mut self.deferred {
                        Some(d) => d.push(Deferred::Record { label, rec, order }),
                        None => self.check_record(&label, &rec, TreeKind::Named(order)),
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
                if n < MIN_KEYS_BRANCH {
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
                        if order.not_before(l, h) {
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
                                kind,
                            }));
                            continue;
                        }
                    }
                    self.walk(children[i], level - 1, false, (lo, hi), stats, kind);
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
        // `head <= last_pg <= bytes.len()/ps` here, but keep it checked.
        let Some(run) = self.run_at(head) else {
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

    /// GC-record well-formedness (INV-23/25/26/29): a 16-byte big-endian
    /// `(txnid, seq)` key whose txnid is `<=` the live meta txnid; an inline
    /// PIL (GC-5) = count prefix + at most [`gc_chunk_cap`] strictly
    /// ascending ids in `[2, last_pg]`. Every id is also collected into
    /// `self.free` for the INV-22/INV-24 partition check, and the record's
    /// shape into `self.gc_records` for the INV-29 range check.
    fn check_gc_entry(&mut self, leaf: u64, key: &[u8], val: LeafValue<'_>) {
        let Some(key) = gc_key_decode(key) else {
            self.fail(
                "INV-23",
                format!("GC leaf {leaf}: key of {} bytes, not 16", key.len()),
            );
            return;
        };
        if key.txnid > self.meta_txnid {
            self.fail("INV-23", format!("GC record {key} keyed by a future txn"));
        }
        // INV-28/GC-29 (format v2): a meta with a non-empty annex holds the
        // newest freeing-txn's PIL itself — a tree record of the same txnid
        // would be the forbidden split placement (GC-32).
        if key.txnid == self.meta_txnid && self.annex_count > 0 {
            self.fail(
                "INV-28",
                format!("GC record {key} coexists with a non-empty meta annex (GC-29/GC-32)"),
            );
        }
        let pil: Vec<u8> = match val {
            LeafValue::Inline(v) => v.to_vec(),
            LeafValue::Overflow { head_pgno, dsize } => {
                self.fail(
                    "INV-29",
                    format!("GC record {key}: value on an overflow run at {head_pgno} (GC-5)"),
                );
                match self
                    .run_at(head_pgno)
                    .and_then(|run| OverflowRef::new(run, self.psize).ok())
                    .and_then(|o| o.payload(dsize).ok())
                {
                    Some(p) => p.to_vec(),
                    None => {
                        self.fail("INV-26", format!("GC record {key}: unreadable PIL"));
                        return;
                    }
                }
            }
        };
        let Some(ids) = pil_decode(&pil) else {
            self.fail(
                "INV-26",
                format!(
                    "GC record {key}: PIL of {} bytes: bad length or count prefix",
                    pil.len()
                ),
            );
            return;
        };
        for fault in check_free_ids(&ids, self.last_pg, &mut self.free) {
            match fault {
                FreeIdFault::OutOfRange(id) => self.fail(
                    "INV-25",
                    format!("GC record {key}: free id {id} out of range"),
                ),
                FreeIdFault::NotAscending => {
                    self.fail("INV-26", format!("GC record {key}: ids not ascending"));
                }
            }
        }
        self.gc_records.push(GcRecordShape {
            key,
            count: ids.len(),
            span: ids.first().zip(ids.last()).map(|(a, b)| (*a, *b)),
        });
    }

    /// INV-29 (ADR-0037): every GC record holds at least one and at most
    /// [`gc_chunk_cap`] ids, and the records of one freeing txn cover
    /// disjoint ranges: a record keyed `(F, s)` lists only ids in `[s, s′)`,
    /// `s′` the key of the next record of `F`. Run once the whole GC tree has
    /// been read (`gc_records` in any order).
    fn check_gc_ranges(&mut self) {
        let cap = gc_chunk_cap(self.psize);
        let mut recs = std::mem::take(&mut self.gc_records);
        recs.sort_unstable_by_key(|r| r.key);
        for (i, r) in recs.iter().enumerate() {
            if r.count == 0 || r.count > cap {
                self.fail(
                    "INV-29",
                    format!("GC record {}: {} ids, not 1..={cap}", r.key, r.count),
                );
            }
            let Some((first, last)) = r.span else {
                continue;
            };
            if first < r.key.seq {
                self.fail(
                    "INV-29",
                    format!("GC record {}: id {first} below its key", r.key),
                );
            }
            if let Some(next) = recs.get(i + 1).filter(|n| n.key.txnid == r.key.txnid) {
                if last >= next.key.seq {
                    self.fail(
                        "INV-29",
                        format!(
                            "GC record {}: id {last} at or past the next record {}",
                            r.key, next.key
                        ),
                    );
                }
            }
        }
    }

    /// The bytes from page `pgno` to the end of the file; `None` past it.
    /// Checked: `pgno` is raw on-disk data, and the multiply must not wrap
    /// into a bogus in-bounds offset.
    fn run_at(&self, pgno: u64) -> Option<&'a [u8]> {
        let base = (pgno as usize).checked_mul(self.psize as usize)?;
        self.bytes.get(base..)
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

    /// Validate a `DBRecord` (`name` labels it in violation messages), walk
    /// its tree as a `kind` tree and check its stats.
    fn check_record(&mut self, name: &str, rec: &DBRecord, kind: TreeKind<'a>) {
        if let Some(stats) = self.walk_record(name, rec, kind) {
            self.check_stats(name, rec, stats);
        }
    }

    /// Check a record's own fields and walk its tree (deferring the root's
    /// children in a parallel walk). Returns the stats it counted, or `None`
    /// when the record is malformed and its tree was not walked.
    fn walk_record(&mut self, name: &str, rec: &DBRecord, kind: TreeKind<'a>) -> Option<WalkStats> {
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
        // `walk` recurses one frame per level, and `rec.depth` comes from
        // the file: a hostile depth of up to 65,535 would overflow the stack.
        // A depth beyond the read path's `CURSOR_STACK` bound is corruption;
        // report it and do not walk.
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
            self.walk(rec.root, rec.depth, true, (None, None), &mut stats, kind);
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
    check_image_inner(bytes, psize, None, None)
}

/// [`check_image`] for a file whose named databases are not all byte-ordered:
/// `order_of` is called with each named database's name and returns the
/// ordering its keys were written under. Return [`KeyOrder::Custom`] with the
/// comparator the database is opened with, or [`KeyOrder::Unknown`] when that
/// comparator is not available, so that a healthy database is not reported
/// for INV-5 or INV-6. The main database and the free tree are byte-ordered
/// whatever `order_of` says.
///
/// A comparator supplied here must be the one the database was written
/// under: any other one makes a healthy tree report violations.
#[must_use]
pub fn check_image_with_key_order<'a>(
    bytes: &'a [u8],
    psize: u32,
    order_of: &'a NamedKeyOrder<'a>,
) -> Vec<String> {
    check_image_inner(bytes, psize, None, Some(order_of))
}

/// [`check_image`] on exactly `threads` threads (1 = the serial walk),
/// whatever the image size. Not part of the stable API: it exists so tests
/// can compare the serial and parallel walks. The report never depends on
/// `threads`: a parallel walk that finds anything is redone serially.
#[doc(hidden)]
#[must_use]
pub fn check_image_with_threads(bytes: &[u8], psize: u32, threads: usize) -> Vec<String> {
    check_image_inner(bytes, psize, Some(threads), None)
}

fn check_image_inner<'a>(
    bytes: &'a [u8],
    psize: u32,
    threads: Option<usize>,
    order_of: Option<&'a NamedKeyOrder<'a>>,
) -> Vec<String> {
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
    // INV-17: the file covers the high-water. The arithmetic is checked, so
    // that a hostile `last_pg` near `u64::MAX` cannot wrap the multiply. A
    // failure returns at once: every later phase is bounded by `last_pg`,
    // and the reachable-XOR-free sweep over `FIRST_DATA_PGNO..=last_pg`
    // would otherwise report one violation per page the image lacks.
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
        order_of,
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
    check_snapshot_inner(map, psize, snap, None)
}

/// [`check_snapshot`] with the key order of each named database given by
/// `order_of`, as [`check_image_with_key_order`].
#[must_use]
pub fn check_snapshot_with_key_order<'a>(
    map: &'a [u8],
    psize: u32,
    snap: &crate::env::Snapshot,
    order_of: &'a NamedKeyOrder<'a>,
) -> Vec<String> {
    check_snapshot_inner(map, psize, snap, Some(order_of))
}

fn check_snapshot_inner<'a>(
    map: &'a [u8],
    psize: u32,
    snap: &crate::env::Snapshot,
    order_of: Option<&'a NamedKeyOrder<'a>>,
) -> Vec<String> {
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
        order_of,
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
/// the calling thread: the walk is too short for extra threads to pay off.
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

/// What INV-29 needs of one GC record ([`Checker::check_gc_ranges`]).
struct GcRecordShape {
    key: GcKey,
    count: usize,
    /// Its lowest and highest id, when it lists any.
    span: Option<(u64, u64)>,
}

/// A fault in a list of free-page ids ([`check_free_ids`]).
enum FreeIdFault {
    /// An id outside `[FIRST_DATA_PGNO, last_pg]`.
    OutOfRange(u64),
    /// An id not above the one before it.
    NotAscending,
}

/// The shape every list of free-page ids must have, a GC entry's (GC-4) and
/// the meta annex's (GC-29): each id in `[FIRST_DATA_PGNO, last_pg]` and
/// strictly ascending. Counts each id into `free` (for the INV-22/24
/// partition) and returns every fault, in list order.
fn check_free_ids(ids: &[u64], last_pg: u64, free: &mut HashMap<u64, u64>) -> Vec<FreeIdFault> {
    let mut faults = Vec::new();
    let mut prev: Option<u64> = None;
    for &id in ids {
        if id < FIRST_DATA_PGNO || id > last_pg {
            faults.push(FreeIdFault::OutOfRange(id));
        }
        if prev.is_some_and(|p| p >= id) {
            faults.push(FreeIdFault::NotAscending);
        }
        prev = Some(id);
        *free.entry(id).or_insert(0) += 1;
    }
    faults
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
fn check_state<'a>(
    bytes: &'a [u8],
    psize: u32,
    roots: &SnapshotRoots<'_>,
    mut violations: Vec<String>,
    threads: usize,
    order_of: Option<&'a NamedKeyOrder<'a>>,
) -> Vec<String> {
    // Validate the annex's GC-29 shape (the INV-25/26 analogues, labelled
    // INV-28) and count its ids into the free set for the INV-22/24
    // partition.
    let mut free: HashMap<u64, u64> = HashMap::new();
    for fault in check_free_ids(roots.annex, roots.last_pg, &mut free) {
        violations.push(match fault {
            FreeIdFault::OutOfRange(id) => format!("INV-28: annex free id {id} out of range"),
            FreeIdFault::NotAscending => "INV-28: annex ids not strictly ascending".into(),
        });
    }
    let shared = || Shared {
        bytes,
        psize,
        meta_txnid: roots.txnid,
        last_pg: roots.last_pg,
        annex_count: roots.annex.len(),
        visited: PageSet::new(roots.last_pg),
        order_of,
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
    checker.check_record("main_db", roots.main_db, TreeKind::Catalog);
    checker.check_record("free_db", roots.free_db, TreeKind::Gc);
    checker.check_gc_ranges();
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
enum Job<'a> {
    /// Check record `idx`'s fields and its root page; defer the root's
    /// children.
    Record { idx: usize, kind: TreeKind<'a> },
    /// Walk a whole subtree.
    Subtree(SubtreeJob<'a>),
}

/// What one job found.
struct JobOut<'a> {
    violations: Vec<String>,
    free: HashMap<u64, u64>,
    gc_records: Vec<GcRecordShape>,
    deferred: Vec<Deferred<'a>>,
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
fn walk_parallel<'a>(
    shared: &Shared<'a>,
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
            kind: TreeKind::Catalog,
        },
        Job::Record {
            idx: 1,
            kind: TreeKind::Gc,
        },
    ];
    let mut violations = Vec::new();
    let mut gc_records = Vec::new();
    while !jobs.is_empty() {
        let outs = run_round(shared, &records, &jobs, threads);
        jobs.clear();
        for out in outs {
            violations.extend(out.violations);
            for (id, n) in out.free {
                *free.entry(id).or_insert(0) += n;
            }
            gc_records.extend(out.gc_records);
            if let Some(stats) = out.stats {
                let r = &mut records[out.rec_idx];
                r.stats += stats;
                r.walked = true;
            }
            for d in out.deferred {
                match d {
                    Deferred::Record { label, rec, order } => {
                        jobs.push(Job::Record {
                            idx: records.len(),
                            kind: TreeKind::Named(order),
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
    checker.gc_records = gc_records;
    checker.violations = violations;
    for r in records.iter().filter(|r| r.walked) {
        checker.check_stats(&r.label, &r.rec, r.stats);
    }
    checker.check_gc_ranges();
    checker.check_reachable_xor_free();
    checker.violations
}

/// Run one round's `jobs` on up to `threads` threads; the results come back
/// in job order.
fn run_round<'a>(
    shared: &Shared<'a>,
    records: &[RecordSlot],
    jobs: &[Job<'a>],
    threads: usize,
) -> Vec<JobOut<'a>> {
    let run = |job: &Job<'a>| -> JobOut<'a> {
        let mut c = Checker::new(shared, true);
        let stats = match job {
            Job::Record { idx, kind } => {
                c.rec_idx = *idx;
                let r = &records[*idx];
                c.walk_record(&r.label, &r.rec, *kind)
            }
            Job::Subtree(j) => {
                c.rec_idx = j.rec_idx;
                let mut stats = WalkStats::default();
                c.walk(
                    j.pgno,
                    j.level,
                    false,
                    (j.low.as_deref(), j.high.as_deref()),
                    &mut stats,
                    j.kind,
                );
                Some(stats)
            }
        };
        JobOut {
            violations: c.violations,
            free: c.free,
            gc_records: c.gc_records,
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
            order_of: None,
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

    /// The key order given for a named database decides its INV-5/INV-6
    /// verdict, on the serial walk and on the parallel one alike: the
    /// databases of a byte-ordered image are clean under byte order, under
    /// the default comparator and under an unknown order, and one checked
    /// under the opposite order reports exactly those two invariants.
    #[test]
    fn named_key_order_reaches_every_walk() {
        use crate::cmp::{DefaultComparator, FnComparator};

        let img = multi_db_image(TEST_ENTRIES);
        let reverse = FnComparator::new("test.reverse.v1", |a: &[u8], b: &[u8]| b.cmp(a));
        let clean = |name: &[u8]| match name {
            b"db-a" => KeyOrder::Custom(&DefaultComparator),
            b"db-b" => KeyOrder::Unknown,
            _ => KeyOrder::Bytewise,
        };
        let wrong = |name: &[u8]| match name {
            b"db-a" => KeyOrder::Custom(&reverse),
            _ => KeyOrder::Bytewise,
        };
        for threads in [1, 4] {
            assert_eq!(
                check_image_inner(&img, PS, Some(threads), Some(&clean)),
                Vec::<String>::new(),
                "threads = {threads}"
            );
        }
        let serial = check_image_inner(&img, PS, Some(1), Some(&wrong));
        assert!(!serial.is_empty(), "db-a is not in reverse order");
        assert!(
            serial
                .iter()
                .all(|v| v.starts_with("INV-5") || v.starts_with("INV-6")),
            "{serial:#?}"
        );
        assert_eq!(check_image_inner(&img, PS, Some(4), Some(&wrong)), serial);
        // The main database is byte-ordered whatever the caller says.
        let all_reverse = |_: &[u8]| KeyOrder::Custom(&reverse);
        let v = check_image_with_key_order(&img, PS, &all_reverse);
        assert!(!v.is_empty());
        let only_main = build_single_db_image(
            PS,
            8 << 20,
            3,
            &[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ],
            900,
        )
        .unwrap();
        assert_eq!(
            check_image_with_key_order(&only_main, PS, &all_reverse),
            Vec::<String>::new()
        );
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
