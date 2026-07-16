//! Bottom-up B+tree builder — the M1.3 test loader and the **prototype of
//! milestone 3.4's `Database::bulk_load`** (PLAN §1.3/§3.4). Milestone 1.3.
//!
//! Given a stream of **pre-sorted, unique** `(key, value)` pairs it packs leaves
//! at a configurable fill factor, builds branch levels upward, spills big values
//! to overflow runs (SPEC 02 §5), and emits a complete env-file image (two meta
//! slots + data pages) whose `main_db` `DBRecord` points at the built tree. The
//! result is a byte-for-byte valid file that [`crate::env::open_with_backing`]
//! opens and the [`crate::btree`] read path (and the M1.12 `check` tool) treat
//! identically to a write-path tree — which is what lets M1.4 differential-test
//! the write path against this loader.
//!
//! It is written to be **promoted, not discarded** (PLAN §1.3): the packing and
//! upward-build logic is exactly what 3.4 needs; only the page sink changes
//! (an in-memory image here; the txn dirty set + writer there).
//!
//! Pure logic over a growable `Vec<u8>` page buffer — no I/O, no `unsafe`, so
//! `miri` exercises it.

use crate::page::geometry::{body_size, overflow_page_count, validate_page_size, value_is_inline};
use crate::page::{
    write_overflow_head, BranchMut, DBRecord, LeafMut, MetaPage, PageError, FIRST_DATA_PGNO,
    FORMAT_VERSION, F_SUBDATA, MAGIC, MIN_KEYS_BRANCH,
};

/// Default leaf/branch fill factor in permille (~90 %). Sequentially-packed
/// pages are left ~10 % slack, matching a typical bulk-load target.
pub const DEFAULT_FILL_PERMILLE: u32 = 900;

/// One entry fed to the tree builder: `(key, value, leaf-node flags)`. The flags
/// are `0` for plain user data and [`F_SUBDATA`] for a named-DB catalog record
/// on the main tree (SPEC 02 §6). Layout is decided by key/value size only —
/// the flags do not affect a cell's size (an `F_SUBDATA` record is a fixed
/// 48-byte inline value).
type FEntry = (Vec<u8>, Vec<u8>, u16);

/// A named database to place in a multi-DB image ([`build_multi_db_image`]): its
/// catalog name and its pre-sorted, unique entries.
pub struct NamedDbData<'a> {
    /// The database name (the catalog key; 1..=`MAX_DB_NAME` bytes).
    pub name: &'a [u8],
    /// The database's entries, strictly ascending and unique by key.
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
}

/// Round `n` up to the next even number (2-byte cell alignment; SPEC 02 §2.2).
#[inline]
fn even(n: usize) -> usize {
    (n + 1) & !1
}

/// A page sink: a growable buffer addressed by page number, with pages 0/1
/// reserved for the meta slots and data pages allocated from
/// [`FIRST_DATA_PGNO`] upward.
struct PageStore {
    psize: u32,
    buf: Vec<u8>,
    next_pgno: u64,
    overflow_pages: u64,
}

impl PageStore {
    fn new(psize: u32) -> PageStore {
        PageStore {
            psize,
            buf: vec![0u8; 2 * psize as usize],
            next_pgno: FIRST_DATA_PGNO,
            overflow_pages: 0,
        }
    }

    /// Allocate `n` contiguous pages, returning the first pgno.
    fn alloc(&mut self, n: u64) -> u64 {
        let p = self.next_pgno;
        self.next_pgno += n;
        let need = self.next_pgno as usize * self.psize as usize;
        if self.buf.len() < need {
            self.buf.resize(need, 0);
        }
        p
    }

    fn copy_page(&mut self, pgno: u64, page: &[u8]) {
        let ps = self.psize as usize;
        let base = pgno as usize * ps;
        self.buf[base..base + ps].copy_from_slice(&page[..ps]);
    }

    /// Write a `payload` value across the `n`-page overflow run starting at
    /// `pgno` (SPEC 02 §5): head page carries the header + first body bytes;
    /// interior pages are pure payload.
    fn write_run(
        &mut self,
        pgno: u64,
        n: u64,
        txnid: u64,
        payload: &[u8],
    ) -> Result<(), PageError> {
        let ps = self.psize as usize;
        let base = pgno as usize * ps;
        let region = &mut self.buf[base..base + n as usize * ps];
        let head_written = write_overflow_head(region, self.psize, pgno, txnid, n as u32, payload)?;
        let rest = &payload[head_written..];
        region[ps..ps + rest.len()].copy_from_slice(rest);
        self.overflow_pages += n;
        Ok(())
    }
}

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
            cell_len: even(8 + ksize + val.len()),
            overflow_pages: None,
        }
    } else {
        LeafCellPlan {
            cell_len: even(8 + ksize + 8), // BIGDATA pointer
            overflow_pages: Some(overflow_page_count(dsize, psize)),
        }
    }
}

/// Pack the leaf level, returning `(first_key, leaf_pgno)` per leaf in order.
fn build_leaves(
    store: &mut PageStore,
    psize: u32,
    txnid: u64,
    entries: &[FEntry],
    fill: u32,
) -> Result<Vec<(Vec<u8>, u64)>, PageError> {
    let body = body_size(psize);
    let target = ((body as u64 * fill as u64) / 1000) as usize;
    let mut leaves = Vec::new();
    let mut i = 0usize;
    while i < entries.len() {
        let mut scratch = vec![0u8; psize as usize];
        let mut leaf = LeafMut::init(&mut scratch, psize, 0, txnid)?;
        let first_key = entries[i].0.clone();
        let mut used = 0usize;
        let mut count = 0usize;
        while i < entries.len() {
            let (k, v, flags) = &entries[i];
            let plan = plan_leaf_cell(psize, k, v);
            let need = plan.cell_len + 2;
            if count >= 1 && used + need > target {
                break;
            }
            match plan.overflow_pages {
                Some(n) => {
                    let opg = store.alloc(n);
                    store.write_run(opg, n, txnid, v)?;
                    leaf.insert_bigdata(count, k, v.len() as u32, opg)?;
                }
                None => leaf.insert_inline(count, k, *flags, v)?,
            }
            used += need;
            count += 1;
            i += 1;
        }
        let lpg = store.alloc(1);
        set_pgno(&mut scratch, lpg);
        store.copy_page(lpg, &scratch);
        leaves.push((first_key, lpg));
    }
    Ok(leaves)
}

/// Total bytes a branch page holding `group`'s children would use (node 0 has
/// an empty separator key).
fn branch_group_bytes(group: &[usize], children: &[(Vec<u8>, u64)], _psize: u32) -> usize {
    let mut used = 0usize;
    for (j, &cidx) in group.iter().enumerate() {
        let ksize = if j == 0 { 0 } else { children[cidx].0.len() };
        used += even(10 + ksize) + 2;
    }
    used
}

/// Ensure every branch group has `>= MIN_KEYS_BRANCH` children (SPEC 03 §11
/// INV-8). Greedy packing can only leave the **last** group underful; rebalance
/// the last two groups (merge if they fit one page, else split evenly).
fn fix_branch_groups(groups: &mut Vec<Vec<usize>>, children: &[(Vec<u8>, u64)], psize: u32) {
    if groups.len() < 2 {
        return;
    }
    let body = body_size(psize);
    let last = groups.len() - 1;
    if groups[last].len() >= MIN_KEYS_BRANCH {
        return;
    }
    let mut merged = groups[last - 1].clone();
    merged.extend_from_slice(&groups[last]);
    if branch_group_bytes(&merged, children, psize) <= body {
        groups[last - 1] = merged;
        groups.pop();
    } else {
        let mid = merged.len() / 2;
        groups[last - 1] = merged[..mid].to_vec();
        groups[last] = merged[mid..].to_vec();
    }
}

/// Build one branch page over `group` (indices into `children`); node 0 carries
/// the empty separator, node j≥1 the child's minimum key.
fn build_one_branch(
    store: &mut PageStore,
    psize: u32,
    txnid: u64,
    children: &[(Vec<u8>, u64)],
    group: &[usize],
) -> Result<u64, PageError> {
    let mut scratch = vec![0u8; psize as usize];
    let mut br = BranchMut::init(&mut scratch, psize, 0, txnid)?;
    for (j, &cidx) in group.iter().enumerate() {
        let (key, cpgno) = &children[cidx];
        let sep: &[u8] = if j == 0 { &[] } else { key.as_slice() };
        br.insert(j, sep, *cpgno)?;
    }
    let pgno = store.alloc(1);
    set_pgno(&mut scratch, pgno);
    store.copy_page(pgno, &scratch);
    Ok(pgno)
}

/// Pack one level of `children` into branch pages, returning the parent level
/// `(min_key, branch_pgno)`.
fn pack_branch_level(
    store: &mut PageStore,
    psize: u32,
    txnid: u64,
    children: &[(Vec<u8>, u64)],
    fill: u32,
    branch_pages: &mut u64,
) -> Result<Vec<(Vec<u8>, u64)>, PageError> {
    let body = body_size(psize);
    let target = ((body as u64 * fill as u64) / 1000) as usize;
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut cur: Vec<usize> = Vec::new();
    let mut used = 0usize;
    for (idx, (key, _)) in children.iter().enumerate() {
        let ksize = if cur.is_empty() { 0 } else { key.len() };
        let need = even(10 + ksize) + 2;
        if !cur.is_empty() && used + need > target {
            groups.push(std::mem::take(&mut cur));
            used = even(10) + 2; // this child is node 0 of the new group (empty key)
            cur.push(idx);
        } else {
            used += need;
            cur.push(idx);
        }
    }
    if !cur.is_empty() {
        groups.push(cur);
    }
    fix_branch_groups(&mut groups, children, psize);

    let mut parents = Vec::with_capacity(groups.len());
    for group in &groups {
        let pgno = build_one_branch(store, psize, txnid, children, group)?;
        *branch_pages += 1;
        parents.push((children[group[0]].0.clone(), pgno));
    }
    Ok(parents)
}

/// Build one tree into a shared [`PageStore`], returning its `DBRecord`. The
/// store's `next_pgno` advances so several trees (named DBs + the main catalog)
/// pack into one env image ([`build_multi_db_image`]). `entries` are flagged
/// ([`FEntry`]) so the main catalog can carry `F_SUBDATA` sub-DB records
/// alongside user data; a per-DB tree passes flags `0`.
fn build_tree_into(
    store: &mut PageStore,
    psize: u32,
    txnid: u64,
    entries: &[FEntry],
    fill: u32,
) -> Result<DBRecord, PageError> {
    validate_page_size(psize)?;
    debug_assert!(
        entries.windows(2).all(|w| w[0].0 < w[1].0),
        "build_tree_into requires strictly-ascending, unique keys"
    );
    if entries.is_empty() {
        return Ok(DBRecord::empty());
    }

    // The store's overflow counter is shared across trees in a multi-DB image,
    // so snapshot it and diff to get *this* tree's overflow-page count.
    let overflow_before = store.overflow_pages;
    let leaves = build_leaves(store, psize, txnid, entries, fill)?;
    let leaf_pages = leaves.len() as u64;

    let (root, depth, branch_pages) = if leaves.len() == 1 {
        (leaves[0].1, 1u16, 0u64)
    } else {
        let mut level = leaves;
        let mut branch_pages = 0u64;
        let mut branch_levels = 0u16;
        while level.len() > 1 {
            level = pack_branch_level(store, psize, txnid, &level, fill, &mut branch_pages)?;
            branch_levels += 1;
        }
        (level[0].1, 1 + branch_levels, branch_pages)
    };

    Ok(DBRecord {
        root,
        branch_pages,
        leaf_pages,
        overflow_pages: store.overflow_pages - overflow_before,
        entries: entries.len() as u64,
        depth,
        flags: 0,
        leaf2_ksize: 0,
    })
}

/// Finalize a filled [`PageStore`] into a two-meta env image whose `main_db`
/// roots `main`, `free_db` is empty, and `map_size`/`last_pg` describe the file.
fn finalize_image(
    store: PageStore,
    psize: u32,
    map_size: u64,
    txnid: u64,
    main_db: DBRecord,
) -> Result<Vec<u8>, PageError> {
    let last_pg = store.next_pgno - 1;
    let mut buf = store.buf;
    if buf.len() < 2 * psize as usize {
        buf.resize(2 * psize as usize, 0);
    }
    let m0 = MetaPage {
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
    };
    m0.encode(&mut buf[0..psize as usize])?;
    let mut m1 = m0;
    m1.pgno = 1;
    m1.encode(&mut buf[psize as usize..2 * psize as usize])?;
    Ok(buf)
}

/// Build a complete single-DB env-file image from pre-sorted, unique `entries`:
/// two identical valid meta slots at `txnid` whose `main_db` roots the built
/// tree (SPEC 02 §3). `free_db` is empty (the builder produces no GC state). The
/// whole dataset lives in the main/unnamed DB; for named DBs use
/// [`build_multi_db_image`].
///
/// The returned bytes can be written to an env's `zerodb.dat` and opened.
///
/// # Errors
///
/// [`PageError`] on an invalid page size or (never, for valid inputs) a
/// page-encoding failure.
///
/// # Panics (debug only)
///
/// If `entries` are not strictly ascending / unique.
pub fn build_single_db_image(
    psize: u32,
    map_size: u64,
    txnid: u64,
    entries: &[(Vec<u8>, Vec<u8>)],
    fill_permille: u32,
) -> Result<Vec<u8>, PageError> {
    let fill = fill_permille.clamp(1, 1000);
    let mut store = PageStore::new(psize);
    let flagged: Vec<FEntry> = entries
        .iter()
        .map(|(k, v)| (k.clone(), v.clone(), 0u16))
        .collect();
    let main_db = build_tree_into(&mut store, psize, txnid, &flagged, fill)?;
    finalize_image(store, psize, map_size, txnid, main_db)
}

/// Build a complete **multi-DB** env-file image: the main/unnamed DB
/// (`main_user` = its plain user entries) plus every named DB in `named`, each
/// referenced from the main catalog by an inline `F_SUBDATA` record (SPEC 02
/// §6). This is the compaction primitive for `Env::copy_to_file(Enabled)` and
/// the `zerodb-tools load` reload path (M1.12): every tree is packed bottom-up
/// and densely, dropping fragmentation and all stale GC state.
///
/// Build order: each named DB's tree is packed first (so its root pgno is
/// known), then the main catalog is packed over `main_user` merged with one
/// `F_SUBDATA` entry per named DB, sorted by key. `free_db` is empty in the
/// result (a fresh compact env owns no free pages).
///
/// Requirements (as for [`build_single_db_image`]): within each DB the entries
/// are strictly ascending and unique, and the named-DB names are disjoint from
/// the main DB's user keys (guaranteed for any image produced from a valid env
/// — a main-tree key is either a user key or a sub-DB pointer, never both).
///
/// # Errors
///
/// [`PageError`] on an invalid page size or a page-encoding failure.
///
/// # Panics (debug only)
///
/// If any DB's entries are not strictly ascending / unique, or a name collides
/// with a main user key.
pub fn build_multi_db_image(
    psize: u32,
    map_size: u64,
    txnid: u64,
    main_user: &[(Vec<u8>, Vec<u8>)],
    named: &[NamedDbData<'_>],
    fill_permille: u32,
) -> Result<Vec<u8>, PageError> {
    let fill = fill_permille.clamp(1, 1000);
    let mut store = PageStore::new(psize);

    // Pack each named DB's tree; collect its catalog record keyed by name.
    let mut catalog: Vec<FEntry> = Vec::with_capacity(named.len());
    for nd in named {
        let flagged: Vec<FEntry> = nd
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone(), 0u16))
            .collect();
        let rec = build_tree_into(&mut store, psize, txnid, &flagged, fill)?;
        catalog.push((nd.name.to_vec(), rec.to_bytes().to_vec(), F_SUBDATA));
    }

    // Merge main user data (flags 0) with the catalog records (F_SUBDATA) and
    // sort by key: the main tree holds both, in one ascending order.
    let mut main_entries: Vec<FEntry> = main_user
        .iter()
        .map(|(k, v)| (k.clone(), v.clone(), 0u16))
        .collect();
    main_entries.extend(catalog);
    main_entries.sort_by(|a, b| a.0.cmp(&b.0));

    let main_db = build_tree_into(&mut store, psize, txnid, &main_entries, fill)?;
    finalize_image(store, psize, map_size, txnid, main_db)
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
        // 511-byte keys: few children per branch page, exercising fix_branch_groups.
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
}
