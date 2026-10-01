//! Roadmap #10 — `free_page_count` counts from each GC entry's PIL `count`
//! prefix (`pil_count`) instead of decoding the ids (`pil_decode`) just to take
//! their length (SPEC 05 GC-23).
//!
//! `free_page_count`'s body is a fold of one per-entry function over the GC
//! cursor walk, and the walk itself is unchanged. So the arithmetic is
//! preserved exactly iff, for every entry value, `pil_count` returns the same
//! number the old `pil_decode(..).len()` did, and rejects the same malformed
//! values (a rejection is what makes `free_page_count` return the identical
//! `MdbError::Invalid`). These tests pin that equivalence — including the
//! zero-ids entry and the malformed-length entry the task calls out — over an
//! entry list standing in for a GC tree's several entries. The real B+tree walk
//! is covered end to end in `crates/zerodb/tests/non_free_fragmented.rs`.

use proptest::prelude::*;
use zerodb_core::page::geometry::{pil_count, pil_decode};

/// Lay out a PIL by hand (SPEC 05 GC-3): an 8-byte LE `count` prefix followed by
/// `ids` as 8-byte LE words. Built independently of the encoder (no sort/unique
/// requirement) so the test does not lean on the code it is checking.
fn make_pil(ids: &[u64]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 8 * ids.len());
    v.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for &id in ids {
        v.extend_from_slice(&id.to_le_bytes());
    }
    v
}

/// The old decode-based per-entry contribution: decode the ids, take the length.
fn decode_len(bytes: &[u8]) -> Option<u64> {
    pil_decode(bytes).map(|ids| ids.len() as u64)
}

#[test]
fn count_matches_decode_over_gc_entries() {
    // Several entries as a GC tree's walk would yield them, including an entry
    // with zero ids (representable: an 8-byte all-zero count prefix).
    let entries = [
        make_pil(&[]),                               // zero-ids entry
        make_pil(&[2]),                              // single id
        make_pil(&[7, 9, 10, 4096]),                 // a small run + a scattered id
        make_pil(&[3, 5, 8, 13, 21, 34, 55]),        // seven ids
        make_pil(&(100..350).collect::<Vec<u64>>()), // 250 ids (inline-PIL cap at 4 KiB)
    ];

    let mut old_total = 0u64;
    let mut new_total = 0u64;
    for e in &entries {
        let old = decode_len(e).expect("valid PIL decodes");
        let new = pil_count(e).expect("valid PIL counts");
        assert_eq!(new, old, "per-entry count disagreed with decode length");
        old_total += old;
        new_total += new;
    }
    assert_eq!(new_total, old_total, "summed free-page count changed");
    // The zero-ids entry contributes 0, the rest sum to their id counts:
    // 0 + 1 + 4 + 7 + 250.
    assert_eq!(new_total, 262);
}

#[test]
fn malformed_pil_rejected_like_decode() {
    // Each of these is malformed in the length/count shape. `free_page_count`
    // turns a `None` here into `MdbError::Invalid`, exactly as it did through
    // `pil_decode` before — so both codecs MUST reject the same bytes.
    let bad: &[Vec<u8>] = &[
        vec![],        // empty (< 8)
        vec![0u8; 4],  // shorter than the prefix
        vec![0u8; 12], // not a multiple of 8
        {
            // count prefix says 3 ids, but only 1 follows
            let mut v = 3u64.to_le_bytes().to_vec();
            v.extend_from_slice(&9u64.to_le_bytes());
            v
        },
        {
            // count prefix says 0 ids, but one follows
            let mut v = 0u64.to_le_bytes().to_vec();
            v.extend_from_slice(&1u64.to_le_bytes());
            v
        },
    ];
    for b in bad {
        assert!(pil_count(b).is_none(), "pil_count accepted malformed {b:?}");
        assert!(
            pil_decode(b).is_none(),
            "pil_decode accepted malformed {b:?}"
        );
    }
}

fn cfg() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(cfg())]

    /// Over ARBITRARY bytes (well-formed or not), `pil_count` agrees with the
    /// old `pil_decode(..).len()` on both the value and the accept/reject
    /// decision — the exact substitution `free_page_count` makes.
    #[test]
    fn count_equiv_decode_arbitrary(bytes in proptest::collection::vec(any::<u8>(), 0..80)) {
        prop_assert_eq!(pil_count(&bytes), decode_len(&bytes));
    }

    /// Over well-formed PILs of arbitrary id vectors, the count equals the id
    /// vector's length.
    #[test]
    fn count_equiv_decode_wellformed(ids in proptest::collection::vec(any::<u64>(), 0..40)) {
        let bytes = make_pil(&ids);
        prop_assert_eq!(pil_count(&bytes), Some(ids.len() as u64));
        prop_assert_eq!(pil_count(&bytes), decode_len(&bytes));
    }
}
