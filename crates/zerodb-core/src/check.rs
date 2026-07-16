//! Tree invariant checker (SPEC 03 §11 INV-1..21 + SPEC 05 §9 INV-22..26).
//! Milestones 1.4/1.5 (the M1.12 `zerodb-tools check` command wraps this walk).
//!
//! [`check_image`] validates a whole env-file image against the live meta's
//! snapshot and returns every violation found, each tagged with its INV id.
//! Since M1.5 the **reachable-XOR-free** partition (INV-10/INV-22) is checked
//! unconditionally: every page in `[FIRST_DATA_PGNO, last_pg]` is either
//! reachable exactly once through a tree or listed exactly once as free in the
//! GC DB — never both (reuse-while-referenced), never neither (leak).
//! (INV-27, the `non_free_pages_size` identity, is asserted at the API level —
//! this walk has no `fstat`.)

use std::collections::{HashMap, HashSet};

use crate::page::geometry::{gc_key_decode, overflow_page_count};
use crate::page::{
    select_meta, DBRecord, LeafValue, MetaPage, OverflowRef, PageRef, PageType, FIRST_DATA_PGNO,
    MAX_KEY_SIZE, MIN_KEYS_BRANCH, MIN_KEYS_LEAF, PGNO_INVALID,
};

/// Accumulated walk statistics, compared against a `DBRecord` (INV-18).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WalkStats {
    branch_pages: u64,
    leaf_pages: u64,
    overflow_pages: u64,
    entries: u64,
}

struct Checker<'a> {
    bytes: &'a [u8],
    psize: u32,
    meta_txnid: u64,
    last_pg: u64,
    visited: HashSet<u64>,
    /// Free page ids collected from every GC PIL → occurrence count
    /// (INV-22/INV-24; SPEC 05 §9).
    free: HashMap<u64, u64>,
    violations: Vec<String>,
}

impl<'a> Checker<'a> {
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
        if !self.visited.insert(pgno) {
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
                    match leaf.value(i) {
                        LeafValue::Inline(_) => {}
                        LeafValue::Overflow { head_pgno, dsize } => {
                            self.check_overflow(pgno, head_pgno, dsize, stats);
                        }
                    }
                    if gc_tree {
                        self.check_gc_entry(pgno, k, leaf.value(i));
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
                    self.walk(children[i], level - 1, false, lo, hi, stats, gc_tree);
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
        if head < FIRST_DATA_PGNO || head + expect - 1 > self.last_pg {
            self.fail(
                "INV-11",
                format!("leaf {leaf}: overflow run [{head}, +{expect}) outside high-water"),
            );
            return;
        }
        // Every page of the run is claimed once (INV-11/16); interior pages
        // carry no header, so only the head decodes.
        for p in head..head + expect {
            if !self.visited.insert(p) {
                self.fail("INV-11", format!("overflow page {p} shared/re-referenced"));
            }
        }
        let ps = self.psize as usize;
        let base = head as usize * ps;
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
        let pil: Vec<u8> = match val {
            LeafValue::Inline(v) => v.to_vec(),
            LeafValue::Overflow { head_pgno, dsize } => {
                let ps = self.psize as usize;
                let base = head_pgno as usize * ps;
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
        if pil.len() < 8 || pil.len() % 8 != 0 {
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
        for chunk in pil[8..].chunks_exact(8) {
            let id = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
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
    fn check_reachable_xor_free(&mut self) {
        for (id, count) in &self.free {
            if *count > 1 {
                self.violations.push(format!(
                    "INV-24: page {id} listed free {count} times across GC entries"
                ));
            }
        }
        for pgno in FIRST_DATA_PGNO..=self.last_pg {
            let reachable = self.visited.contains(&pgno);
            let free = self.free.contains_key(&pgno);
            match (reachable, free) {
                (true, true) => self.violations.push(format!(
                    "INV-22: page {pgno} both reachable and GC-listed free"
                )),
                (false, false) => self.violations.push(format!(
                    "INV-22: page {pgno} neither reachable nor GC-listed free (leaked)"
                )),
                _ => {}
            }
        }
    }

    fn check_record(&mut self, name: &str, rec: &DBRecord, gc_tree: bool) {
        // INV-19 root shape.
        if rec.depth == 0 && rec.root != PGNO_INVALID {
            self.fail("INV-19", format!("{name}: depth 0 but root {}", rec.root));
            return;
        }
        if rec.depth > 0 && rec.root == PGNO_INVALID {
            self.fail("INV-19", format!("{name}: depth {} but no root", rec.depth));
            return;
        }
        // INV-21 (record-level reserved fields).
        if rec.flags != 0 || rec.leaf2_ksize != 0 {
            self.fail("INV-21", format!("{name}: reserved DBRecord fields set"));
        }
        let mut stats = WalkStats::default();
        if rec.root != PGNO_INVALID {
            let root_page_type = self.page(rec.root).map(|p| p.page_type());
            match (rec.depth, root_page_type) {
                (1, Some(PageType::Leaf)) | (2.., Some(PageType::Branch)) => {}
                (d, t) => self.fail("INV-19", format!("{name}: depth {d} but root is {t:?}")),
            }
            self.walk(rec.root, rec.depth, true, None, None, &mut stats, gc_tree);
        }
        // INV-18 stat accuracy.
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
#[must_use]
pub fn check_image(bytes: &[u8], psize: u32) -> Vec<String> {
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
    let meta = match select_meta(&v0, &v1, false) {
        crate::page::MetaChoice::Both { meta, .. }
        | crate::page::MetaChoice::OnlyOne { meta, .. } => meta,
        crate::page::MetaChoice::None => {
            return vec!["INV-2: no valid meta slot (both torn/foreign)".into()]
        }
    };
    // INV-3: whole-page file.
    if bytes.len() % ps != 0 {
        violations.push(format!(
            "INV-3: file length {} not a multiple of psize {psize}",
            bytes.len()
        ));
    }
    // INV-17: the file covers the high-water.
    if bytes.len() < (meta.last_pg as usize + 1) * ps {
        violations.push(format!(
            "INV-17: file length {} < (last_pg {} + 1) * psize",
            bytes.len(),
            meta.last_pg
        ));
    }
    let mut checker = Checker {
        bytes,
        psize,
        meta_txnid: meta.txnid,
        last_pg: meta.last_pg,
        visited: HashSet::new(),
        free: HashMap::new(),
        violations,
    };
    checker.check_record("main_db", &meta.main_db, false);
    checker.check_record("free_db", &meta.free_db, true);
    // INV-10 / INV-22 + INV-24 (reachable XOR free, unconditional since M1.5).
    checker.check_reachable_xor_free();
    checker.violations
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
            let crc = crate::page::crc32c(&img[base..base + 168]);
            img[base + 168..base + 172].copy_from_slice(&crc.to_le_bytes());
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
}
