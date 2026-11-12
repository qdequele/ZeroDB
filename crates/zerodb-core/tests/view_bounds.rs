//! The safe page-view API never reads outside the buffer it is given.
//!
//! `LeafRef` and `BranchRef` accessors read without bounds checks, relying
//! on their validating constructors. These tests hand those constructors,
//! and the `LeafMut` operations that follow the O(1) `from_valid`, buffers
//! and pages crafted so that a missing check would read past the buffer.
//! They run under miri, which reports such a read.

use zerodb_core::page::{BranchMut, BranchRef, LeafMut, LeafRef, PageError};

const PS: u32 = 4096;
/// Page header size, and the offsets of the tree header's `lower` and
/// `upper` fields (SPEC 02 §2.1).
const HEADER: usize = 32;
const OFF_LOWER: usize = 24;
const OFF_UPPER: usize = 26;

fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

/// The first 64 bytes of an empty leaf page, edited to hold one entry whose
/// cell header lies inside those 64 bytes and whose 400-byte key would lie
/// past them. Within a 4096-byte page every bound checks.
fn short_leaf() -> Vec<u8> {
    let mut page = vec![0u8; PS as usize];
    LeafMut::init(&mut page, PS, 2, 1).unwrap();
    let mut buf = page[..64].to_vec();
    put_u16(&mut buf, OFF_LOWER, 2);
    put_u16(&mut buf, OFF_UPPER, 20);
    put_u16(&mut buf, HEADER, 20); // pointer 0 -> body offset 20
    let cell = HEADER + 20;
    put_u16(&mut buf, cell, 0); // node flags
    put_u16(&mut buf, cell + 2, 400); // ksize
    buf[cell + 4..cell + 8].copy_from_slice(&0u32.to_le_bytes()); // dsize
    buf
}

/// As [`short_leaf`], for a branch: child 0 with the empty separator, and
/// child 1 whose 300-byte separator would lie past the 64 bytes.
fn short_branch() -> Vec<u8> {
    let mut page = vec![0u8; PS as usize];
    BranchMut::init(&mut page, PS, 2, 1).unwrap();
    let mut buf = page[..64].to_vec();
    put_u16(&mut buf, OFF_LOWER, 4);
    put_u16(&mut buf, OFF_UPPER, 6);
    put_u16(&mut buf, HEADER, 6); // pointer 0 -> body offset 6
    put_u16(&mut buf, HEADER + 2, 16); // pointer 1 -> body offset 16
    let cell0 = HEADER + 6;
    buf[cell0..cell0 + 8].copy_from_slice(&3u64.to_le_bytes()); // child pgno
    put_u16(&mut buf, cell0 + 8, 0); // ksize
    let cell1 = HEADER + 16;
    buf[cell1..cell1 + 8].copy_from_slice(&4u64.to_le_bytes());
    put_u16(&mut buf, cell1 + 8, 300);
    buf
}

#[test]
fn leaf_ref_rejects_a_buffer_shorter_than_the_page() {
    let buf = short_leaf();
    match LeafRef::new(&buf, PS) {
        Err(PageError::BufferTooSmall { got: 64, psize }) => assert_eq!(psize, PS as usize),
        Err(e) => panic!("expected BufferTooSmall, got {e:?}"),
        Ok(leaf) => panic!(
            "a 64-byte buffer validated as a {PS}-byte leaf with {} keys",
            leaf.num_keys()
        ),
    }
    // The same bytes at the head of a whole page are a valid leaf.
    let mut page = vec![0u8; PS as usize];
    page[..64].copy_from_slice(&buf);
    let leaf = LeafRef::new(&page, PS).unwrap();
    assert_eq!(leaf.key(0).len(), 400);
}

#[test]
fn branch_ref_rejects_a_buffer_shorter_than_the_page() {
    let buf = short_branch();
    match BranchRef::new(&buf, PS) {
        Err(PageError::BufferTooSmall { got: 64, psize }) => assert_eq!(psize, PS as usize),
        Err(e) => panic!("expected BufferTooSmall, got {e:?}"),
        Ok(br) => panic!(
            "a 64-byte buffer validated as a {PS}-byte branch with {} keys",
            br.num_keys()
        ),
    }
    let mut page = vec![0u8; PS as usize];
    page[..64].copy_from_slice(&buf);
    let br = BranchRef::new(&page, PS).unwrap();
    assert_eq!(br.key(1).len(), 300);
}

#[test]
fn tree_views_reject_an_empty_buffer_and_an_invalid_page_size() {
    assert!(matches!(
        LeafRef::new(&[], PS),
        Err(PageError::BufferTooSmall { got: 0, .. })
    ));
    assert!(matches!(
        BranchRef::new(&[], PS),
        Err(PageError::BufferTooSmall { got: 0, .. })
    ));
    let page = vec![0u8; PS as usize];
    for psize in [0u32, 16, 100, 4095, 3 * 4096] {
        assert!(
            matches!(
                LeafRef::new(&page, psize),
                Err(PageError::InvalidPageSize(p)) if p == psize
            ),
            "leaf, psize {psize}"
        );
        assert!(
            matches!(
                BranchRef::new(&page, psize),
                Err(PageError::InvalidPageSize(p)) if p == psize
            ),
            "branch, psize {psize}"
        );
    }
}

/// `LeafMut::from_valid` checks the header only, so `lookup` on the view it
/// returns must not trust the cells: here the one pointer names a cell whose
/// header straddles the end of the page.
#[test]
fn leaf_mut_lookup_reads_nothing_past_an_unvalidated_cell() {
    let mut page = vec![0u8; PS as usize];
    LeafMut::init(&mut page, PS, 2, 1).unwrap();
    put_u16(&mut page, OFF_LOWER, 2);
    put_u16(&mut page, OFF_UPPER, 4000);
    put_u16(&mut page, HEADER, 4062); // cell header would end past the page
    let leaf = LeafMut::from_valid(&mut page, PS).unwrap();
    let found = leaf.lookup(b"key");
    assert!(matches!(found, Ok(0) | Err(0 | 1)), "{found:?}");

    // A cell inside the page whose key length runs past it.
    put_u16(&mut page, HEADER, 4050);
    put_u16(&mut page, HEADER + 4050 + 2, u16::MAX);
    let leaf = LeafMut::from_valid(&mut page, PS).unwrap();
    let found = leaf.lookup(b"key");
    assert!(matches!(found, Ok(0) | Err(0 | 1)), "{found:?}");
}

/// `LeafMut::lookup` on a well-formed page agrees with `LeafRef::lookup`.
#[test]
fn leaf_mut_lookup_matches_leaf_ref_lookup() {
    let mut page = vec![0u8; PS as usize];
    {
        let mut leaf = LeafMut::init(&mut page, PS, 2, 1).unwrap();
        for i in 0..40u8 {
            leaf.insert_inline(i as usize, &[b'k', 2 * i], 0, &[i; 9])
                .unwrap();
        }
    }
    let expected: Vec<_> = {
        let leaf = LeafRef::new(&page, PS).unwrap();
        (0..90u8).map(|b| leaf.lookup(&[b'k', b])).collect()
    };
    let leaf = LeafMut::from_valid(&mut page, PS).unwrap();
    let got: Vec<_> = (0..90u8).map(|b| leaf.lookup(&[b'k', b])).collect();
    assert_eq!(got, expected);
}

/// `remove_span` on a page whose surviving cells overlap and add up to more
/// than the body returns a typed error and leaves every byte as it was.
#[test]
fn remove_span_error_leaves_the_page_unmodified() {
    let mut page = vec![0u8; PS as usize];
    LeafMut::init(&mut page, PS, 2, 1).unwrap();
    // Three pointers: two name the same 3018-byte cell at body offset 1000,
    // the third a 14-byte cell at body offset 4040.
    put_u16(&mut page, OFF_LOWER, 6);
    put_u16(&mut page, OFF_UPPER, 1000);
    put_u16(&mut page, HEADER, 1000);
    put_u16(&mut page, HEADER + 2, 1000);
    put_u16(&mut page, HEADER + 4, 4040);
    let big = HEADER + 1000;
    put_u16(&mut page, big, 0);
    put_u16(&mut page, big + 2, 10); // ksize
    page[big + 4..big + 8].copy_from_slice(&3000u32.to_le_bytes()); // dsize
    for (i, b) in page[big + 8..big + 3018].iter_mut().enumerate() {
        *b = i as u8;
    }
    let small = HEADER + 4040;
    put_u16(&mut page, small, 0);
    put_u16(&mut page, small + 2, 2);
    page[small + 4..small + 8].copy_from_slice(&4u32.to_le_bytes());

    let before = page.clone();
    let mut leaf = LeafMut::from_valid(&mut page, PS).unwrap();
    let e = leaf.remove_span(2, 3).unwrap_err();
    assert!(matches!(e, PageError::CellOutOfBounds { .. }), "got {e:?}");
    assert!(page == before, "the failed remove_span modified the page");
}
