//! Property-based encode -> decode round-trips for every page/node type
//! (PLAN §1.1 acceptance). A test-writer pass will extend these.
//!
//! Excluded under Miri: the proptest harness itself reads `current_dir` for
//! failure persistence, which Miri's isolation blocks (a harness limitation, not
//! engine code). The deterministic `spec02_format` test exercises the identical
//! codec paths under Miri.
#![cfg(not(miri))]

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use zerodb_core::page::{
    geometry, write_overflow_head, BranchMut, BranchRef, DBRecord, LeafMut, LeafRef, LeafValue,
    MetaPage, MetaValidity, OverflowRef, PageRef,
};

const PSIZES: [u32; 3] = [4096, 8192, 65536];

proptest! {
    /// Leaf: insert a sorted map, read every key/value back unchanged.
    #[test]
    fn leaf_roundtrip(
        psize_idx in 0usize..3,
        entries in btree_map(vec(any::<u8>(), 1..12), vec(any::<u8>(), 0..24), 0..40),
    ) {
        let psize = PSIZES[psize_idx];
        let mut buf = vec![0u8; psize as usize];
        let mut leaf = LeafMut::init(&mut buf, psize, 2, 1).unwrap();
        let mut inserted: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for (k, v) in &entries {
            // Only insert values that fit inline and while the page has room.
            if !geometry::value_is_inline(k.len(), v.len() as u64, psize) {
                continue;
            }
            match leaf.lookup(k) {
                Ok(_) => continue, // duplicate key: skip
                Err(i) => {
                    if leaf.insert_inline(i, k, 0, v).is_ok() {
                        inserted.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        let leaf = LeafRef::new(&buf, psize).unwrap();
        prop_assert_eq!(leaf.num_keys(), inserted.len());
        for (i, (k, v)) in inserted.iter().enumerate() {
            prop_assert_eq!(leaf.key(i), &k[..]);
            prop_assert_eq!(leaf.value(i), LeafValue::Inline(v));
            prop_assert_eq!(leaf.lookup(k), Ok(i));
        }
    }

    /// Branch: insert index-0 empty key + sorted separators, read back.
    #[test]
    fn branch_roundtrip(
        psize_idx in 0usize..3,
        seps in btree_map(vec(any::<u8>(), 1..12), any::<u64>(), 0..30),
        child0 in any::<u64>(),
    ) {
        let psize = PSIZES[psize_idx];
        let mut buf = vec![0u8; psize as usize];
        let mut branch = BranchMut::init(&mut buf, psize, 5, 7).unwrap();
        branch.insert(0, b"", child0).unwrap();
        let mut children = vec![(Vec::new(), child0)];
        for (i, (sep, child)) in seps.iter().enumerate() {
            if branch.insert(i + 1, sep, *child).is_ok() {
                children.push((sep.clone(), *child));
            }
        }
        let branch = BranchRef::new(&buf, psize).unwrap();
        prop_assert_eq!(branch.num_keys(), children.len());
        for (i, (sep, child)) in children.iter().enumerate() {
            prop_assert_eq!(branch.key(i), &sep[..]);
            prop_assert_eq!(branch.child_pgno(i), *child);
        }
    }

    /// Meta: arbitrary field values encode and decode identically and validate.
    #[test]
    fn meta_roundtrip(
        psize_idx in 0usize..3,
        txnid in any::<u64>(),
        map_size in any::<u64>(),
        last_pg in any::<u64>(),
        root0 in any::<u64>(),
        entries0 in any::<u64>(),
        depth0 in any::<u16>(),
    ) {
        let psize = PSIZES[psize_idx];
        let mut meta = MetaPage::create(txnid & 1, psize, map_size);
        meta.txnid = txnid;
        meta.last_pg = last_pg;
        meta.main_db = DBRecord {
            root: root0,
            entries: entries0,
            depth: depth0,
            ..DBRecord::empty()
        };
        let mut buf = vec![0u8; psize as usize];
        meta.encode(&mut buf).unwrap();
        match MetaPage::validate(&buf, psize).unwrap() {
            MetaValidity::Valid(decoded) => prop_assert_eq!(decoded, meta),
            other => return Err(TestCaseError::fail(format!("not valid: {other:?}"))),
        }
    }

    /// Overflow: any value round-trips through a run of the right length.
    #[test]
    fn overflow_roundtrip(
        psize_idx in 0usize..3,
        value in vec(any::<u8>(), 0..20_000),
    ) {
        let psize = PSIZES[psize_idx];
        let n = geometry::overflow_page_count(value.len() as u64, psize);
        let mut run = vec![0u8; n as usize * psize as usize];
        let written = write_overflow_head(
            &mut run[..psize as usize], psize, 8, 42, n as u32, &value,
        ).unwrap();
        run[psize as usize..psize as usize + (value.len() - written)]
            .copy_from_slice(&value[written..]);
        let ovf = OverflowRef::new(&run, psize).unwrap();
        prop_assert_eq!(ovf.ovf_pages(), n as u32);
        prop_assert_eq!(ovf.payload(value.len() as u32).unwrap(), &value[..]);
    }

    /// Robustness: arbitrary bytes interpreted as a page never panic — the
    /// decoder returns a typed error or a valid view, for every psize.
    #[test]
    fn arbitrary_bytes_never_panic(
        psize_idx in 0usize..3,
        flags in any::<u16>(),
        mut data in vec(any::<u8>(), 4096..=65536),
    ) {
        let psize = PSIZES[psize_idx];
        data.resize(psize as usize, 0);
        // Patch the flags field to exercise all type interpretations.
        data[16..18].copy_from_slice(&flags.to_le_bytes());
        if let Ok(p) = PageRef::new(&data, psize) {
            // Walk each accessor path; none may panic.
            let _ = p.pgno();
            let _ = p.txnid();
            let _ = p.checksum();
            let _ = p.as_leaf().map(|l| {
                for i in 0..l.num_keys() { let _ = l.key(i); let _ = l.value(i); }
            });
            let _ = p.as_branch().map(|b| {
                for i in 0..b.num_keys() { let _ = b.key(i); let _ = b.child_pgno(i); }
            });
            let _ = p.as_overflow().map(|o| o.payload(1024));
            let _ = p.as_meta();
        }
    }
}

// ---------------------------------------------------------------------------
// M1.1 test-writer pass additions (items 8-9 of the coverage task): strategies
// biased toward SPEC 02 boundaries, plus the GC-DB big-endian key codec. These
// are added alongside the implementer's strategies above, not in place of
// them.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Leaf round-trip biased toward MAX_KEY_SIZE-adjacent keys (1, 2, 510,
    /// 511 bytes) and inline/overflow-threshold-adjacent values (±2 bytes
    /// around `max_node_size`), rather than the original strategy's uniform
    /// small sizes (item 8). Also checks `free_space` against the exact
    /// theoretical accounting, which the original `leaf_roundtrip` does not.
    #[test]
    fn leaf_roundtrip_boundary_biased(
        psize_idx in 0usize..3,
        klen_choice in 0usize..4,
        pairs in vec((any::<u8>(), 0usize..5, vec(any::<u8>(), 0..8)), 1..40),
    ) {
        let psize = PSIZES[psize_idx];
        let klens = [1usize, 2, 510, 511];
        let klen = klens[klen_choice];
        let threshold = geometry::max_node_size(psize);
        let deltas: [i64; 5] = [-2, -1, 0, 1, 2];

        let mut buf = vec![0u8; psize as usize];
        let mut leaf = LeafMut::init(&mut buf, psize, 2, 1).unwrap();
        let mut inserted: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut expected_free = leaf.free_space();

        for (kb, delta_choice, perturb) in pairs {
            let mut key = vec![kb; klen];
            for (i, b) in perturb.iter().take(klen.saturating_sub(1)).enumerate() {
                key[i + 1] = *b;
            }
            let base = threshold as i64 - 8 - klen as i64;
            let vlen = (base + deltas[delta_choice]).clamp(0, threshold as i64) as usize;
            if !geometry::value_is_inline(klen, vlen as u64, psize) {
                continue;
            }
            let value = vec![kb; vlen];
            match leaf.lookup(&key) {
                Ok(_) => continue, // duplicate key: skip
                Err(i) => {
                    let cost = ((8 + klen + vlen + 1) & !1) + 2;
                    if leaf.insert_inline(i, &key, 0, &value).is_ok() {
                        inserted.insert(key, value);
                        expected_free -= cost;
                    }
                }
            }
        }

        let leaf_ref = LeafRef::new(&buf, psize).unwrap();
        prop_assert_eq!(leaf_ref.num_keys(), inserted.len());
        prop_assert_eq!(leaf_ref.free_space(), expected_free);
        for (i, (k, v)) in inserted.iter().enumerate() {
            prop_assert_eq!(leaf_ref.key(i), &k[..]);
            prop_assert_eq!(leaf_ref.value(i), LeafValue::Inline(&v[..]));
        }
    }

    /// Branch round-trip biased toward MAX_KEY_SIZE-adjacent separator keys
    /// (item 8), filling the page much fuller than the original strategy's
    /// `0..30` uniform-small-key entries.
    #[test]
    fn branch_roundtrip_boundary_biased(
        psize_idx in 0usize..3,
        klen_choice in 0usize..3,
        fillers in 0usize..200,
        child0 in any::<u64>(),
    ) {
        let psize = PSIZES[psize_idx];
        let klens = [1usize, 510, 511];
        let klen = klens[klen_choice];

        let mut buf = vec![0u8; psize as usize];
        let mut branch = BranchMut::init(&mut buf, psize, 5, 7).unwrap();
        branch.insert(0, b"", child0).unwrap();
        let mut children: Vec<(Vec<u8>, u64)> = vec![(Vec::new(), child0)];

        for i in 0..fillers {
            // Vary the key content so entries are distinguishable while
            // holding the length at the boundary-adjacent `klen`.
            let mut sep = vec![(i % 251) as u8; klen];
            if klen > 1 {
                sep[klen - 1] = ((i / 251) % 251) as u8;
            }
            let idx = children.len();
            if branch.insert(idx, &sep, i as u64).is_ok() {
                children.push((sep, i as u64));
            } else {
                break; // page full
            }
        }

        let branch_ref = BranchRef::new(&buf, psize).unwrap();
        prop_assert_eq!(branch_ref.num_keys(), children.len());
        for (i, (sep, child)) in children.iter().enumerate() {
            prop_assert_eq!(branch_ref.key(i), &sep[..]);
            prop_assert_eq!(branch_ref.child_pgno(i), *child);
        }
    }

    /// Overflow round-trip biased toward exact page-multiple boundaries
    /// (`N*psize - HEADER_SIZE` +/- a small delta) rather than the original
    /// strategy's `0..20_000` uniform range (item 8).
    #[test]
    fn overflow_roundtrip_boundary_biased(
        psize_idx in 0usize..3,
        n in 1u64..4,
        delta in -2i64..=2,
        fill in any::<u8>(),
    ) {
        let psize = PSIZES[psize_idx];
        let exact = n * psize as u64 - 32;
        let dsize = (exact as i64 + delta).clamp(0, (4 * psize as u64) as i64) as u64;
        let value: Vec<u8> = (0..dsize).map(|i| fill.wrapping_add((i % 253) as u8)).collect();

        let n_actual = geometry::overflow_page_count(dsize, psize);
        let mut run = vec![0u8; n_actual as usize * psize as usize];
        let written = write_overflow_head(
            &mut run[..psize as usize], psize, 8, 42, n_actual as u32, &value,
        ).unwrap();
        run[psize as usize..psize as usize + (value.len() - written)]
            .copy_from_slice(&value[written..]);
        let ovf = OverflowRef::new(&run, psize).unwrap();
        prop_assert_eq!(ovf.ovf_pages(), n_actual as u32);
        prop_assert_eq!(ovf.payload(dsize as u32).unwrap(), &value[..]);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The GC-DB txnid key codec (SPEC 02 §7) is big-endian byte-for-byte
    /// (item 9), not merely "ordered like" big-endian.
    #[test]
    fn gc_key_encode_is_byte_exact_big_endian(txnid in any::<u64>()) {
        prop_assert_eq!(geometry::gc_key_encode(txnid), txnid.to_be_bytes());
    }

    /// memcmp order over the encoded bytes equals numeric order over the
    /// txnid, in both directions (item 9) — the property SPEC 05's
    /// reclamation scan depends on.
    #[test]
    fn gc_key_order_matches_numeric_order(a in any::<u64>(), b in any::<u64>()) {
        let ea = geometry::gc_key_encode(a);
        let eb = geometry::gc_key_encode(b);
        prop_assert_eq!(a < b, ea < eb);
        prop_assert_eq!(a == b, ea == eb);
    }
}
