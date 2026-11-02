#![no_main]
//! Fuzz target `fuzz_page_decode`.
//!
//! Feeds arbitrary bytes to the SPEC 02 page decoder under a page-size sweep
//! (4096 / 8192 / 65536), forcing every page-type interpretation, and walks
//! every accessor on any view that constructs. Every leaf entry is also
//! decoded as a GC record (16-byte key, page-id list; SPEC 05 GC-1..5). The
//! decoder must return typed errors only — it must never panic — for
//! untrusted input.

use libfuzzer_sys::fuzz_target;
use zerodb_core::page::geometry::{
    gc_chunk_cap, gc_key_decode, gc_key_encode, pil_count, pil_decode, pil_encode_into, pil_size,
    GC_KEY_SIZE,
};
use zerodb_core::page::{
    BranchRef, LeafRef, LeafValue, MetaPage, OverflowRef, PageRef, P_BRANCH, P_LEAF, P_META,
    P_OVERFLOW,
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
        walk_leaf(&leaf, p.page_size());
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

fn walk_leaf(leaf: &LeafRef<'_>, psize: u32) {
    for i in 0..leaf.num_keys() {
        let key = leaf.key(i);
        let value = leaf.value(i);
        let _ = leaf.node_flags(i);
        if let LeafValue::Inline(val) = value {
            walk_gc_record(key, val, psize);
        }
    }
    let _ = leaf.lookup(b"probe");
    let _ = leaf.free_space();
}

/// Decode a leaf entry as a GC record (SPEC 05 GC-1..5): the key decodes
/// exactly when it is 16 bytes and then re-encodes to itself, the two PIL
/// decoders agree, and a list the writer would store (at most
/// `gc_chunk_cap` ids) re-encodes to the same bytes.
fn walk_gc_record(key: &[u8], val: &[u8], psize: u32) {
    match gc_key_decode(key) {
        Some(k) => assert_eq!(&gc_key_encode(k)[..], key),
        None => assert_ne!(key.len(), GC_KEY_SIZE),
    }
    let ids = pil_decode(val);
    assert_eq!(pil_count(val), ids.as_ref().map(|v| v.len() as u64));
    if let Some(ids) = ids {
        if ids.len() <= gc_chunk_cap(psize) {
            let mut buf = vec![0u8; pil_size(ids.len())];
            pil_encode_into(&ids, &mut buf);
            assert_eq!(buf, val);
        }
    }
}

fn walk_branch(branch: &BranchRef<'_>) {
    for i in 0..branch.num_keys() {
        let _ = branch.key(i);
        let _ = branch.child_pgno(i);
    }
    // Regression (H2, SPEC 02 §4.1): the descent dereferences
    // `child_pgno(child_index(..))` unconditionally, so exercise that exact
    // pair on every constructed view. A zero-child branch never constructs
    // (`EmptyBranch`), so these calls must be total.
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
            let _ = LeafRef::new(&buf, psize).map(|l| walk_leaf(&l, psize));
            let _ = BranchRef::new(&buf, psize).map(|b| walk_branch(&b));
            let _ = OverflowRef::new(&buf, psize).map(|o| o.payload(1234));
            let _ = MetaPage::validate(&buf, psize);
        }
    }
});
