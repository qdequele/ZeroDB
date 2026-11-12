//! The batch tree builder: packs a whole pre-sorted entry set into an in-memory
//! env image in one call. Compiled for this crate's tests only, where it
//! builds fixtures and serves as the reference the streaming
//! [`EnvStream`](super::EnvStream) is compared against.

use super::{branch_group_bytes, plan_leaf_cell, set_pgno};
use crate::page::geometry::{body_size, validate_page_size};
use crate::page::{
    even, write_overflow_head, BranchMut, DBRecord, LeafMut, MetaPage, PageError,
    BRANCH_NODE_HEADER, FIRST_DATA_PGNO, FORMAT_VERSION, F_SUBDATA, MAGIC, MIN_KEYS_BRANCH,
};

/// One entry fed to the tree builder: `(key, value, leaf-node flags)`. The flags
/// are `0` for plain user data and [`F_SUBDATA`] for a named-DB catalog record
/// on the main tree (SPEC 02 §6). The flags do not affect a cell's size: an
/// `F_SUBDATA` record is a 48-byte inline value.
type FEntry = (Vec<u8>, Vec<u8>, u16);

/// A named database to place in a multi-DB image ([`build_multi_db_image`]): its
/// catalog name and its pre-sorted, unique entries.
pub(crate) struct NamedDbData<'a> {
    /// The database name (the catalog key; 1..=`MAX_DB_NAME` bytes).
    pub name: &'a [u8],
    /// The database's entries, strictly ascending and unique by key.
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
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

/// Ensure every branch group has `>= MIN_KEYS_BRANCH` children (SPEC 03 §11
/// INV-8). Greedy packing can leave only the last group with too few; the
/// last two groups are then merged if they fit one page, else split evenly.
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
    if branch_group_bytes(&merged, children) <= body {
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
        let need = even(BRANCH_NODE_HEADER + ksize) + 2;
        if !cur.is_empty() && used + need > target {
            groups.push(std::mem::take(&mut cur));
            used = even(BRANCH_NODE_HEADER) + 2; // this child is node 0 of the new group (empty key)
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

    // The store's overflow counter is shared by every tree of the image, so
    // this tree's overflow-page count is the difference across its build.
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
        fl_count: 0,
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
/// [`PageError`] on an invalid page size or a page-encoding failure.
///
/// # Panics (debug only)
///
/// If `entries` are not strictly ascending / unique.
pub(crate) fn build_single_db_image(
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
/// §6). Every tree is packed bottom-up and densely, with no free pages.
///
/// Build order: each named DB's tree is packed first (so its root pgno is
/// known), then the main catalog is packed over `main_user` merged with one
/// `F_SUBDATA` entry per named DB, sorted by key. `free_db` is empty in the
/// result (a fresh compact env owns no free pages).
///
/// Requirements (as for [`build_single_db_image`]): within each DB the entries
/// are strictly ascending and unique, and the named-DB names are disjoint from
/// the main DB's user keys. Entries read from a valid env satisfy this: a
/// main-tree key is either a user key or a named-database record.
///
/// # Errors
///
/// [`PageError`] on an invalid page size or a page-encoding failure.
///
/// # Panics (debug only)
///
/// If any DB's entries are not strictly ascending / unique, or a name collides
/// with a main user key.
pub(crate) fn build_multi_db_image(
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
