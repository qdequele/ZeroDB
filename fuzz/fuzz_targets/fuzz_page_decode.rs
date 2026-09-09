#![no_main]
//! Fuzz target `fuzz_page_decode` (PLAN §1.1 acceptance).
//!
//! Feeds arbitrary bytes to the SPEC 02 page decoder under a page-size sweep
//! (4096 / 8192 / 65536), forcing every page-type interpretation, and walks
//! every accessor on any view that constructs. The decoder must return typed
//! errors only — it must never panic — for untrusted input.

use libfuzzer_sys::fuzz_target;
use zerodb_core::page::{
    BranchRef, LeafRef, MetaPage, OverflowRef, PageRef, P_BRANCH, P_LEAF, P_META, P_OVERFLOW,
};

const PSIZES: [u32; 3] = [4096, 8192, 65536];

/// Walk every accessor reachable from a generic page view.
fn walk_page(p: &PageRef<'_>) {
    let _ = p.pgno();
    let _ = p.txnid();
    let _ = p.flags();
    let _ = p.checksum();
    let _ = p.page_type();
    let _ = p.page_size();
    if let Ok(leaf) = p.as_leaf() {
        walk_leaf(&leaf);
    }
    if let Ok(branch) = p.as_branch() {
        walk_branch(&branch);
    }
    if let Ok(ovf) = p.as_overflow() {
        let _ = ovf.pgno();
        let _ = ovf.txnid();
        let _ = ovf.ovf_pages();
        let _ = ovf.payload(0);
        let _ = ovf.payload(4096);
        let _ = ovf.payload(u32::MAX);
    }
    let _ = p.as_meta();
}

fn walk_leaf(leaf: &LeafRef<'_>) {
    for i in 0..leaf.num_keys() {
        let _ = leaf.key(i);
        let _ = leaf.value(i);
        let _ = leaf.node_flags(i);
    }
    let _ = leaf.lookup(b"probe");
    let _ = leaf.free_space();
}

fn walk_branch(branch: &BranchRef<'_>) {
    for i in 0..branch.num_keys() {
        let _ = branch.key(i);
        let _ = branch.child_pgno(i);
    }
    // H2 regression (2026-09 security review): the descent dereferences
    // `child_pgno(child_index(..))` unconditionally, so exercise that exact
    // pair on every constructed view — a zero-key branch used to construct
    // and then panic here (`child_index` returns 0, `child_pgno(0)` asserts).
    // Since the fix a zero-child branch never constructs (EmptyBranch), so
    // these calls are total.
    let i = branch.child_index(b"probe");
    let _ = branch.child_pgno(i);
    let i0 = branch.child_index(&[]);
    let _ = branch.child_pgno(i0);
    let _ = branch.free_space();
}

fuzz_target!(|data: &[u8]| {
    for &psize in &PSIZES {
        // Build a page-sized buffer from the arbitrary input (zero-padded).
        let mut buf = vec![0u8; psize as usize];
        let n = data.len().min(psize as usize);
        buf[..n].copy_from_slice(&data[..n]);

        // 1. Decode with whatever flags the input carries.
        if let Ok(p) = PageRef::new(&buf, psize) {
            walk_page(&p);
        }

        // 2. Force each structural interpretation and re-decode + walk.
        for flag in [P_LEAF, P_BRANCH, P_OVERFLOW, P_META] {
            buf[16..18].copy_from_slice(&flag.to_le_bytes());
            if let Ok(p) = PageRef::new(&buf, psize) {
                walk_page(&p);
            }
            // Also drive the type-specific constructors directly.
            let _ = LeafRef::new(&buf, psize).map(|l| walk_leaf(&l));
            let _ = BranchRef::new(&buf, psize).map(|b| walk_branch(&b));
            let _ = OverflowRef::new(&buf, psize).map(|o| o.payload(1234));
            let _ = MetaPage::validate(&buf, psize);
        }
    }
});
