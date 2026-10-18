//! The ADR-0028 page primitives against the operations they replace.
//!
//! - Moving a leaf cell verbatim (`cell_bytes` + `insert_cell_bytes`) gives
//!   the destination exactly the entries, flags, values and free space that
//!   decoding the entry and re-inserting it through `insert_inline` /
//!   `insert_bigdata` gives.
//! - Rewriting a branch separator in place (`set_key`) gives exactly the
//!   logical page that `remove` + `insert` gives, and declines (page
//!   untouched) in exactly the cases where that re-insert returns `PageFull`.
//!
//! Every resulting page also passes the full per-cell validation
//! (`LeafRef::new` / `BranchRef::new`), so a heap shift that overlapped or
//! left a cell out of bounds fails here. Runs under miri (no failure
//! persistence: it reads the working directory, which miri isolation blocks).

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::Config;
use zerodb_core::page::{BranchMut, BranchRef, LeafMut, LeafRef, LeafValue, PageError};

const PSIZES: [u32; 2] = [4096, 16384];

fn config() -> Config {
    Config {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..Config::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Val {
    Inline(Vec<u8>),
    Big { head: u64, dsize: u32 },
}

type LeafEntry = (Vec<u8>, u16, Val);

fn leaf_entries(buf: &[u8], psize: u32) -> (Vec<LeafEntry>, usize) {
    let l = LeafRef::new(buf, psize).expect("leaf validates");
    let mut out = Vec::new();
    for i in 0..l.num_keys() {
        let v = match l.value(i) {
            LeafValue::Inline(v) => Val::Inline(v.to_vec()),
            LeafValue::Overflow { head_pgno, dsize } => Val::Big {
                head: head_pgno,
                dsize,
            },
        };
        out.push((l.key(i).to_vec(), l.node_flags(i), v));
    }
    (out, l.free_space())
}

fn branch_entries(buf: &[u8], psize: u32) -> (Vec<(Vec<u8>, u64)>, usize) {
    let b = BranchRef::new(buf, psize).expect("branch validates");
    let out = (0..b.num_keys())
        .map(|i| (b.key(i).to_vec(), b.child_pgno(i)))
        .collect();
    (out, b.free_space())
}

/// A leaf with up to `n` entries in order; `big` marks entries stored as
/// overflow references instead of inline values.
fn build_leaf(psize: u32, pgno: u64, items: &[(Vec<u8>, Vec<u8>, bool)]) -> Vec<u8> {
    let mut buf = vec![0u8; psize as usize];
    let mut leaf = LeafMut::init(&mut buf, psize, pgno, 1).unwrap();
    let mut idx = 0;
    for (k, v, big) in items {
        let r = if *big {
            leaf.insert_bigdata(idx, k, 70_000 + v.len() as u32, 900 + idx as u64)
        } else {
            leaf.insert_inline(idx, k, 0, v)
        };
        match r {
            Ok(()) => idx += 1,
            Err(PageError::PageFull { .. }) => break,
            Err(e) => panic!("unexpected insert error {e:?}"),
        }
    }
    buf
}

fn leaf_items() -> impl Strategy<Value = Vec<(Vec<u8>, Vec<u8>, bool)>> {
    vec(
        (
            vec(any::<u8>(), 1..40),
            vec(any::<u8>(), 0..1600),
            any::<bool>(),
        ),
        1..30,
    )
}

proptest! {
    #![proptest_config(config())]

    /// A verbatim cell move equals decode + remove + re-insert.
    #[test]
    fn leaf_cell_move_equals_decode_and_reinsert(
        psize_idx in 0usize..2,
        src_items in leaf_items(),
        dst_items in leaf_items(),
        pick in any::<prop::sample::Index>(),
        at in any::<prop::sample::Index>(),
    ) {
        let psize = PSIZES[psize_idx];
        let mut src = build_leaf(psize, 2, &src_items);
        let dst = build_leaf(psize, 3, &dst_items);
        let n_src = LeafRef::new(&src, psize).unwrap().num_keys();
        let n_dst = LeafRef::new(&dst, psize).unwrap().num_keys();
        prop_assume!(n_src > 0);
        let i = pick.index(n_src);
        let j = at.index(n_dst + 1);

        // Model: decode the entry and insert it through the typed inserts.
        let (key, flags, val) = {
            let l = LeafRef::new(&src, psize).unwrap();
            (l.key(i).to_vec(), l.node_flags(i), l.value(i))
        };
        let mut model = dst.clone();
        let model_r = {
            let mut m = LeafMut::from_valid(&mut model, psize).unwrap();
            match val {
                LeafValue::Inline(v) => m.insert_inline(j, &key, flags, v),
                LeafValue::Overflow { head_pgno, dsize } => m.insert_bigdata(j, &key, dsize, head_pgno),
            }
        };

        // Primitive: copy the encoded cell, remove it from the source, insert
        // the bytes verbatim.
        let cell = LeafMut::from_valid(&mut src, psize).unwrap().cell_bytes(i).unwrap().to_vec();
        LeafMut::from_valid(&mut src, psize).unwrap().remove(i).unwrap();
        let mut moved = dst.clone();
        let moved_r = LeafMut::from_valid(&mut moved, psize).unwrap().insert_cell_bytes(j, &cell);

        prop_assert_eq!(model_r.is_ok(), moved_r.is_ok());
        if moved_r.is_ok() {
            prop_assert_eq!(leaf_entries(&moved, psize), leaf_entries(&model, psize));
        } else {
            prop_assert!(matches!(moved_r, Err(PageError::PageFull { .. })), "expected PageFull");
            prop_assert_eq!(&moved, &dst, "a refused insert leaves the page untouched");
        }
        // The source stays a valid leaf one entry shorter.
        prop_assert_eq!(leaf_entries(&src, psize).0.len(), n_src - 1);
    }

    /// An in-place separator rewrite equals remove + insert, and declines
    /// exactly when that insert would not fit.
    #[test]
    fn branch_set_key_equals_remove_and_insert(
        psize_idx in 0usize..2,
        keys in vec(vec(any::<u8>(), 1..200), 1..120),
        new_key in vec(any::<u8>(), 1..512),
        pick in any::<prop::sample::Index>(),
    ) {
        let psize = PSIZES[psize_idx];
        let mut page = vec![0u8; psize as usize];
        let mut n = 0usize;
        {
            let mut b = BranchMut::init(&mut page, psize, 2, 1).unwrap();
            b.insert(0, &[], 100).unwrap();
            n += 1;
            for k in &keys {
                if b.insert(n, k, 100 + n as u64).is_err() {
                    break;
                }
                n += 1;
            }
        }
        prop_assume!(n >= 2);
        let idx = 1 + pick.index(n - 1);
        let child = BranchRef::new(&page, psize).unwrap().child_pgno(idx);

        let mut model = page.clone();
        let model_r = {
            let mut m = BranchMut::from_valid(&mut model, psize).unwrap();
            m.remove(idx).unwrap();
            m.insert(idx, &new_key, child)
        };

        let mut inplace = page.clone();
        let fitted = BranchMut::from_valid(&mut inplace, psize).unwrap().set_key(idx, &new_key).unwrap();

        prop_assert_eq!(fitted, model_r.is_ok());
        if fitted {
            prop_assert_eq!(branch_entries(&inplace, psize), branch_entries(&model, psize));
        } else {
            prop_assert!(matches!(model_r, Err(PageError::PageFull { .. })), "expected PageFull");
            prop_assert_eq!(&inplace, &page, "a declined rewrite leaves the page untouched");
        }
    }
}

#[test]
fn set_key_rejects_keys_invalid_for_the_slot() {
    let psize = 4096;
    let mut page = vec![0u8; psize as usize];
    let mut b = BranchMut::init(&mut page, psize, 2, 1).unwrap();
    b.insert(0, &[], 10).unwrap();
    b.insert(1, b"m", 11).unwrap();
    assert!(matches!(
        b.set_key(0, b"x"),
        Err(PageError::BadKeySize { .. })
    ));
    assert!(matches!(
        b.set_key(1, b""),
        Err(PageError::BadKeySize { .. })
    ));
    assert!(matches!(
        b.set_key(1, &[b'k'; 512]),
        Err(PageError::BadKeySize { .. })
    ));
    assert!(b.set_key(0, b"").unwrap(), "node 0 keeps its empty key");
    assert!(b.set_key(1, b"zz").unwrap());
    let r = BranchRef::new(&page, psize).unwrap();
    assert_eq!(r.key(1), b"zz");
    assert_eq!(r.child_pgno(1), 11);
}

#[test]
fn insert_cell_bytes_rejects_a_slice_that_is_not_one_cell() {
    let psize = 4096;
    let mut src = vec![0u8; psize as usize];
    {
        let mut l = LeafMut::init(&mut src, psize, 2, 1).unwrap();
        l.insert_inline(0, b"key", 0, b"value").unwrap();
    }
    let cell = LeafRef::new(&src, psize)
        .unwrap()
        .cell_bytes(0, psize)
        .unwrap()
        .to_vec();
    let mut dst = vec![0u8; psize as usize];
    let mut l = LeafMut::init(&mut dst, psize, 3, 1).unwrap();
    assert!(l.insert_cell_bytes(0, &cell[..cell.len() - 2]).is_err());
    let mut longer = cell.clone();
    longer.extend_from_slice(&[0, 0]);
    assert!(l.insert_cell_bytes(0, &longer).is_err());
    assert!(l.insert_cell_bytes(0, &cell[..4]).is_err());
    l.insert_cell_bytes(0, &cell).unwrap();
    assert_eq!(l.num_keys(), 1);
}
