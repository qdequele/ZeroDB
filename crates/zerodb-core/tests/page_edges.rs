//! Adversarial edge-case coverage for the M1.1 page model (SPEC 02), added by
//! a test-writer pass on top of the implementer's own `spec02_format.rs` /
//! `proptest_roundtrip.rs`. These are deterministic (no proptest macro use),
//! so — unlike `proptest_roundtrip.rs` — this file runs under `cargo miri
//! test` too.
//!
//! Section numbering below matches the M1.1 test-coverage task list:
//! 1. Page-full boundary exactness (leaf + branch, min/max cell, both psizes)
//! 2. Insert-at-every-position ordering
//! 3. Remove + heap compaction adversarial
//! 4. Meta validation rejection matrix
//! 5. Meta double-buffer selection truth-table completion (tie-break)
//! 6. 511-boundary keys / inline-overflow threshold in page context
//! 7. Overflow run math (exact page-multiple boundaries, typed errors)

use zerodb_core::page::{
    geometry, select_meta, write_overflow_head, BranchMut, BranchRef, DBRecord, LeafMut, LeafRef,
    LeafValue, MetaChoice, MetaPage, MetaValidity, OverflowRef, PageError,
};

const PSIZES: [u32; 2] = [4096, 65536];

// ===========================================================================
// Shared byte-cost helpers (mirror SPEC 02 §2.2/§4, independent of the
// crate's private `tree::even`/`*_cell_len` internals — this file is a
// black-box consumer of the public page API).
// ===========================================================================

fn even_up(n: usize) -> usize {
    (n + 1) & !1
}

/// Insert cost (cell + 2-byte pointer) of a leaf cell (SPEC 02 §4.2).
fn leaf_cost(ksize: usize, vsize: usize) -> usize {
    even_up(8 + ksize + vsize) + 2
}

/// Insert cost (cell + 2-byte pointer) of a branch cell (SPEC 02 §4.1).
fn branch_cost(ksize: usize) -> usize {
    even_up(10 + ksize) + 2
}

/// Solve for the value length that makes a leaf cell's insert cost exactly
/// `target_cost` bytes, for a given key length. `None` if unattainable.
fn leaf_value_len_for_exact_cost(ksize: usize, target_cost: usize) -> Option<usize> {
    if target_cost < 2 {
        return None;
    }
    let clen = target_cost - 2;
    if clen % 2 != 0 || clen < 8 + ksize {
        return None;
    }
    Some(clen - (8 + ksize))
}

// ===========================================================================
// 1. Page-full boundary exactness
// ===========================================================================

/// Leaf boundary: fill to exactly `remainder` bytes free with one filler
/// cell, then prove the exact-fit cell of key length `klen` succeeds while
/// the same cell one value-byte larger does not, from the identical starting
/// state (rebuilt deterministically for the second attempt).
fn leaf_full_boundary(psize: u32, klen: usize) {
    let body = psize as usize - 32;
    let min_cost = leaf_cost(klen, 0);
    let remainder = even_up(min_cost + 64);
    assert!(
        remainder < body,
        "psize {psize} too small for klen {klen} boundary test"
    );

    let build = |buf: &mut [u8]| {
        let mut leaf = LeafMut::init(buf, psize, 2, 1).unwrap();
        let filler_cost = body - remainder;
        let filler_klen = klen.clamp(1, 4);
        let filler_vlen = leaf_value_len_for_exact_cost(filler_klen, filler_cost)
            .expect("filler cost must be achievable");
        leaf.insert_inline(0, &vec![b'f'; filler_klen], 0, &vec![0u8; filler_vlen])
            .unwrap();
        assert_eq!(leaf.free_space(), remainder);
    };

    let vlen_last = leaf_value_len_for_exact_cost(klen, remainder)
        .expect("remainder must be large enough for an exact-fit cell of this key length");
    let last_key = vec![b'z'; klen];

    // (a) exact-fit insert succeeds and consumes all remaining space.
    let mut buf_a = vec![0u8; psize as usize];
    build(&mut buf_a);
    {
        let mut leaf_a = LeafMut::from_valid(&mut buf_a, psize).unwrap();
        leaf_a
            .insert_inline(leaf_a.num_keys(), &last_key, 0, &vec![1u8; vlen_last])
            .unwrap();
        assert_eq!(
            leaf_a.free_space(),
            0,
            "exact-fit cell must zero out free space"
        );
    }
    let leaf_a_ref = LeafRef::new(&buf_a, psize).unwrap();
    let last_idx = leaf_a_ref.num_keys() - 1;
    assert_eq!(leaf_a_ref.key(last_idx), &last_key[..]);
    assert_eq!(
        leaf_a_ref.value(last_idx),
        LeafValue::Inline(&vec![1u8; vlen_last][..])
    );

    // (b) same starting state (rebuilt deterministically), one value byte
    // more flips the identical insert to PageFull.
    let mut buf_b = vec![0u8; psize as usize];
    build(&mut buf_b);
    let mut leaf_b = LeafMut::from_valid(&mut buf_b, psize).unwrap();
    let err = leaf_b
        .insert_inline(leaf_b.num_keys(), &last_key, 0, &vec![1u8; vlen_last + 1])
        .unwrap_err();
    match err {
        PageError::PageFull { needed, available } => {
            assert_eq!(available, remainder);
            assert!(
                needed > remainder,
                "one extra value byte must need more than available"
            );
        }
        other => panic!("expected PageFull, got {other:?}"),
    }
}

#[test]
fn leaf_full_boundary_4096_min_key() {
    leaf_full_boundary(4096, 1);
}
#[test]
fn leaf_full_boundary_4096_max_key() {
    leaf_full_boundary(4096, 511);
}
#[test]
fn leaf_full_boundary_65536_min_key() {
    leaf_full_boundary(65536, 1);
}
#[test]
fn leaf_full_boundary_65536_max_key() {
    leaf_full_boundary(65536, 511);
}

fn branch_init_with_index0(buf: &mut [u8], psize: u32) -> BranchMut<'_> {
    let mut branch = BranchMut::init(buf, psize, 5, 7).unwrap();
    branch.insert(0, b"", 0).unwrap();
    branch
}

/// Insert max-cost (`ksize = 511`) filler cells while more than `stop_below`
/// bytes of free space remain. Cheap (O(body / max_cost) iterations)
/// regardless of psize, so this stays fast under Miri.
fn branch_fill_with_large_filler(branch: &mut BranchMut<'_>, stop_below: usize) {
    let key = vec![b'f'; 511];
    while branch.free_space() >= stop_below {
        let idx = branch.num_keys();
        branch.insert(idx, &key, idx as u64).unwrap();
    }
}

/// Branch boundary, min-size separator keys: find the largest key length that
/// still fits the remaining space and the smallest that does not, and prove
/// both from the identical starting state.
fn branch_min_key_boundary(psize: u32) {
    let min_cost = branch_cost(1);

    let build = |buf: &mut [u8]| -> usize {
        let mut branch = branch_init_with_index0(buf, psize);
        branch_fill_with_large_filler(&mut branch, branch_cost(511));
        while branch.free_space() >= 2 * min_cost {
            let idx = branch.num_keys();
            branch.insert(idx, &[b'm'; 1], idx as u64).unwrap();
        }
        branch.free_space()
    };

    // `build` fully re-derives the "almost full" state deterministically, so
    // it is called once per live `BranchMut` we need rather than trying to
    // reopen a previously-populated buffer (there is no `BranchMut::from_valid`
    // reopen API, unlike `LeafMut`).
    let mut probe_buf = vec![0u8; psize as usize];
    let remainder = build(&mut probe_buf);
    assert!(
        (min_cost..2 * min_cost).contains(&remainder),
        "remainder={remainder}"
    );

    let fits_ksize = (1..=511usize)
        .filter(|&k| branch_cost(k) <= remainder)
        .max()
        .expect("ksize=1 must fit: remainder >= min_cost");
    let overflow_ksize = (fits_ksize + 1..=511usize)
        .find(|&k| branch_cost(k) > remainder)
        .expect("some larger ksize must overflow the remainder");

    // (a) largest still-fitting cell succeeds, consuming exactly its cost.
    let mut buf_a = vec![0u8; psize as usize];
    let mut branch_a = branch_init_with_index0(&mut buf_a, psize);
    branch_fill_with_large_filler(&mut branch_a, branch_cost(511));
    while branch_a.free_space() >= 2 * min_cost {
        let idx = branch_a.num_keys();
        branch_a.insert(idx, &[b'm'; 1], idx as u64).unwrap();
    }
    assert_eq!(
        branch_a.free_space(),
        remainder,
        "deterministic rebuild must match"
    );
    let fits_cost = branch_cost(fits_ksize);
    let fits_key = vec![b'x'; fits_ksize];
    branch_a.insert(branch_a.num_keys(), &fits_key, 1).unwrap();
    assert_eq!(branch_a.free_space(), remainder - fits_cost);

    // (b) same starting remainder (rebuilt again), a bigger key is rejected.
    let mut buf_b = vec![0u8; psize as usize];
    let mut branch_b = branch_init_with_index0(&mut buf_b, psize);
    branch_fill_with_large_filler(&mut branch_b, branch_cost(511));
    while branch_b.free_space() >= 2 * min_cost {
        let idx = branch_b.num_keys();
        branch_b.insert(idx, &[b'm'; 1], idx as u64).unwrap();
    }
    assert_eq!(
        branch_b.free_space(),
        remainder,
        "deterministic rebuild must match"
    );
    let overflow_key = vec![b'y'; overflow_ksize];
    let err = branch_b
        .insert(branch_b.num_keys(), &overflow_key, 1)
        .unwrap_err();
    match err {
        PageError::PageFull { needed, available } => {
            assert_eq!(needed, branch_cost(overflow_ksize));
            assert_eq!(available, remainder);
        }
        other => panic!("expected PageFull, got {other:?}"),
    }
}

/// Branch boundary, max-size (511-byte) separator keys: the last max-size
/// cell that fits succeeds; a further one does not (a key cannot grow past
/// MAX_KEY_SIZE, so the "+1 byte" framing does not apply here — the boundary
/// is instead demonstrated by successive max-size inserts).
fn branch_max_key_boundary(psize: u32) {
    let max_cost = branch_cost(511);
    let mut buf = vec![0u8; psize as usize];
    let mut branch = branch_init_with_index0(&mut buf, psize);
    // Stop with [max_cost, 2*max_cost) free so exactly one more max cell is
    // guaranteed to fit, and a second is guaranteed not to.
    branch_fill_with_large_filler(&mut branch, 2 * max_cost);
    let remainder = branch.free_space();
    assert!(
        (max_cost..2 * max_cost).contains(&remainder),
        "remainder={remainder}"
    );

    let key = vec![b'M'; 511];
    branch.insert(branch.num_keys(), &key, 111).unwrap();
    let remainder_after = branch.free_space();
    assert_eq!(remainder_after, remainder - max_cost);
    assert!(
        remainder_after < max_cost,
        "must not fit another max-size cell"
    );

    let err = branch.insert(branch.num_keys(), &key, 222).unwrap_err();
    match err {
        PageError::PageFull { needed, available } => {
            assert_eq!(needed, max_cost);
            assert_eq!(available, remainder_after);
        }
        other => panic!("expected PageFull, got {other:?}"),
    }

    // Byte-level integrity of the successfully-inserted max-size cell.
    let branch_ref = BranchRef::new(&buf, psize).unwrap();
    let last = branch_ref.num_keys() - 1;
    assert_eq!(branch_ref.key(last), &key[..]);
    assert_eq!(branch_ref.child_pgno(last), 111);
}

#[test]
fn branch_full_boundary_4096_min_key() {
    branch_min_key_boundary(4096);
}
#[test]
fn branch_full_boundary_4096_max_key() {
    branch_max_key_boundary(4096);
}
#[test]
fn branch_full_boundary_65536_min_key() {
    branch_min_key_boundary(65536);
}
#[test]
fn branch_full_boundary_65536_max_key() {
    branch_max_key_boundary(65536);
}

// ===========================================================================
// 2. Insert-at-every-position ordering
// ===========================================================================

/// Tiny deterministic LCG (no `rand` dependency needed for a fixed-seed
/// shuffle) — avoids widening the dependency allowlist for a test-only need.
fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn shuffled_indices(n: usize, seed: u64) -> Vec<usize> {
    let mut v: Vec<usize> = (0..n).collect();
    let mut s = seed;
    for i in (1..n).rev() {
        let j = (lcg_next(&mut s) % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
    v
}

fn middle_out_indices(n: usize) -> Vec<usize> {
    let mut out = Vec::with_capacity(n);
    let mid = n / 2;
    out.push(mid);
    let mut lo = mid;
    let mut hi = mid;
    while out.len() < n {
        if hi + 1 < n {
            hi += 1;
            out.push(hi);
        }
        if out.len() < n && lo > 0 {
            lo -= 1;
            out.push(lo);
        }
    }
    out
}

/// Insert `n` distinct 4-byte big-endian keys in the given order via
/// lookup-based sorted insertion (mirrors real B+tree leaf usage), asserting
/// after every single insert that (a) iteration order is sorted memcmp order
/// and (b) `free_space` matches the theoretical `body - sum(costs)` exactly.
fn leaf_insert_order_test(psize: u32, order: &[usize]) {
    let body = psize as usize - 32;
    let mut buf = vec![0u8; psize as usize];
    LeafMut::init(&mut buf, psize, 2, 1).unwrap();
    let val = [0xABu8, 0xCD];
    let cost = leaf_cost(4, val.len());
    let mut consumed = 0usize;

    for (step, &k) in order.iter().enumerate() {
        let key = (k as u32).to_be_bytes();
        {
            let mut leaf = LeafMut::from_valid(&mut buf, psize).unwrap();
            let idx = leaf.lookup(&key).expect_err("keys are distinct");
            leaf.insert_inline(idx, &key, 0, &val).unwrap();
            consumed += cost;
            assert_eq!(leaf.num_keys(), step + 1);
            assert_eq!(
                leaf.free_space(),
                body - consumed,
                "free-space accounting mismatch after step {step}"
            );
        }

        // Re-open as a read view and verify strict sorted (memcmp) order.
        let view = LeafRef::new(&buf, psize).unwrap();
        for i in 1..view.num_keys() {
            assert!(
                view.key(i - 1) < view.key(i),
                "iteration order must be strictly sorted after step {step}"
            );
        }
    }
}

#[test]
fn leaf_insert_reverse_order_4096() {
    let order: Vec<usize> = (0..60).rev().collect();
    leaf_insert_order_test(4096, &order);
}

#[test]
fn leaf_insert_reverse_order_65536() {
    let order: Vec<usize> = (0..60).rev().collect();
    leaf_insert_order_test(65536, &order);
}

#[test]
fn leaf_insert_shuffled_order_4096() {
    leaf_insert_order_test(4096, &shuffled_indices(60, 0x5EED_5EED));
}

#[test]
fn leaf_insert_shuffled_order_65536() {
    leaf_insert_order_test(65536, &shuffled_indices(60, 0x1234_5678));
}

#[test]
fn leaf_insert_middle_out_order_4096() {
    leaf_insert_order_test(4096, &middle_out_indices(60));
}

#[test]
fn leaf_insert_middle_out_order_65536() {
    leaf_insert_order_test(65536, &middle_out_indices(60));
}

// ===========================================================================
// 3. Remove + heap compaction adversarial
// ===========================================================================

/// Interleave inserts and removes across varying cell sizes to repeatedly
/// churn the cell heap, tracking the expected surviving set in a BTreeMap.
/// After the churn, assert byte-level integrity of every surviving cell, then
/// insert a cell sized to consume *all* remaining free space exactly —
/// proving the heap is always fully compacted (no internal fragmentation)
/// after `LeafMut::remove`.
fn leaf_remove_compaction_adversarial(psize: u32) {
    let body = psize as usize - 32;
    let mut buf = vec![0u8; psize as usize];
    let mut leaf = LeafMut::init(&mut buf, psize, 2, 1).unwrap();
    let mut expected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        std::collections::BTreeMap::new();
    let mut consumed = 0usize;

    // Varying cell sizes so removals leave heterogeneous gaps.
    let sizes = [1usize, 40, 3, 200, 7, 90, 2, 500, 15];
    for (i, &vsize) in sizes.iter().enumerate() {
        let key = vec![b'a' + (i as u8 % 26), (i as u8)];
        let val = vec![(i as u8).wrapping_mul(7); vsize];
        let idx = leaf.lookup(&key).unwrap_err();
        leaf.insert_inline(idx, &key, 0, &val).unwrap();
        consumed += leaf_cost(key.len(), val.len());
        expected.insert(key, val);
    }
    assert_eq!(leaf.free_space(), body - consumed);

    // Remove every other entry (by key, via lookup) to fragment the heap.
    let keys_to_remove: Vec<Vec<u8>> = expected.keys().step_by(2).cloned().collect();
    for key in &keys_to_remove {
        let idx = leaf.lookup(key).unwrap();
        let removed_val = expected.remove(key).unwrap();
        consumed -= leaf_cost(key.len(), removed_val.len());
        leaf.remove(idx).unwrap();
        assert_eq!(leaf.free_space(), body - consumed);
    }

    // Insert two more varied-size cells into the now-fragmented heap.
    for (i, vsize) in [(100usize, 60usize), (101, 4)] {
        let key = vec![b'z', i as u8];
        let val = vec![9u8; vsize];
        let idx = leaf.lookup(&key).unwrap_err();
        leaf.insert_inline(idx, &key, 0, &val).unwrap();
        consumed += leaf_cost(key.len(), val.len());
        expected.insert(key, val);
    }
    assert_eq!(leaf.free_space(), body - consumed);

    // Byte-level integrity of every surviving cell, in sorted order.
    let view = LeafRef::new(&buf, psize).unwrap();
    assert_eq!(view.num_keys(), expected.len());
    for (i, (k, v)) in expected.iter().enumerate() {
        assert_eq!(view.key(i), &k[..], "key mismatch at surviving index {i}");
        assert_eq!(
            view.value(i),
            LeafValue::Inline(&v[..]),
            "value mismatch at surviving index {i}"
        );
    }

    // Compaction proof: a cell sized to consume exactly the remaining free
    // space must succeed, i.e. free space is fully contiguous, not merely
    // fully accounted for.
    let mut leaf = LeafMut::from_valid(&mut buf, psize).unwrap();
    let remaining = leaf.free_space();
    // Use a 1-byte key so the whole remainder becomes value bytes.
    let vlen = leaf_value_len_for_exact_cost(1, remaining)
        .expect("remaining free space must be constructible as a single cell");
    leaf.insert_inline(leaf.num_keys(), b"\xFF", 0, &vec![0x11u8; vlen])
        .unwrap();
    assert_eq!(
        leaf.free_space(),
        0,
        "post-compaction insert must exactly fill the page"
    );
}

#[test]
fn leaf_remove_compaction_adversarial_4096() {
    leaf_remove_compaction_adversarial(4096);
}

#[test]
fn leaf_remove_compaction_adversarial_65536() {
    leaf_remove_compaction_adversarial(65536);
}

// ===========================================================================
// 4. Meta validation rejection matrix
// ===========================================================================

fn built_valid_meta(psize: u32) -> Vec<u8> {
    let mut meta = MetaPage::create(0, psize, 4 * 1024 * 1024);
    meta.txnid = 5;
    meta.main_db = DBRecord {
        root: 9,
        entries: 3,
        depth: 1,
        ..DBRecord::empty()
    };
    let mut buf = vec![0u8; psize as usize];
    meta.encode(&mut buf).unwrap();
    assert!(MetaPage::validate(&buf, psize).unwrap().is_valid());
    buf
}

/// Every single-byte flip in the CRC-covered region [0, META_CONTENT_LEN)
/// must invalidate the slot; every single-byte flip in the excluded reserved
/// tail [172, psize) must NOT (SPEC 02 §3.3 — the sector-tear reality of
/// REC-22). The `meta_crc` field itself [168, 172) is not part of the hashed
/// range but flipping it desyncs stored-vs-recomputed CRC, so it also
/// invalidates.
fn meta_validation_rejection_matrix(psize: u32) {
    let buf = built_valid_meta(psize);

    for i in 0..168usize {
        let mut b = buf.clone();
        b[i] ^= 0xFF;
        let v = MetaPage::validate(&b, psize).unwrap();
        assert!(
            !v.is_valid(),
            "byte {i} (CRC-covered) flip must invalidate meta"
        );
    }
    for i in 168..172usize {
        let mut b = buf.clone();
        b[i] ^= 0xFF;
        let v = MetaPage::validate(&b, psize).unwrap();
        assert!(
            !v.is_valid(),
            "crc field byte {i} flip must invalidate meta (stored != recomputed)"
        );
    }
    // Sample the reserved tail rather than iterating every byte (up to ~65KB)
    // to stay within the ~30s test-runtime budget; every sampled byte must
    // NOT invalidate, documenting that this range is unprotected by the CRC.
    let stride = ((psize as usize - 172) / 40).max(1);
    for i in (172..psize as usize).step_by(stride) {
        let mut b = buf.clone();
        b[i] ^= 0xFF;
        let v = MetaPage::validate(&b, psize).unwrap();
        assert!(
            v.is_valid(),
            "reserved-tail byte {i} flip must NOT invalidate meta"
        );
    }
}

#[test]
fn meta_validation_rejection_matrix_4096() {
    meta_validation_rejection_matrix(4096);
}
#[test]
fn meta_validation_rejection_matrix_65536() {
    meta_validation_rejection_matrix(65536);
}

#[test]
fn meta_validation_specific_variants() {
    let buf = built_valid_meta(4096);

    // magic (offset 32..36) -> BadMagic.
    let mut b = buf.clone();
    b[32] ^= 0xFF;
    assert_eq!(
        MetaPage::validate(&b, 4096).unwrap(),
        MetaValidity::BadMagic
    );

    // format_version (offset 36..40) -> BadVersion.
    let mut b = buf.clone();
    b[36] ^= 0xFF;
    assert!(matches!(
        MetaPage::validate(&b, 4096).unwrap(),
        MetaValidity::BadVersion(_)
    ));

    // page_size (offset 40..44): flip to a non-power-of-two -> BadPageSize.
    let mut b = buf.clone();
    b[40] ^= 0x01; // 4096 -> 4097
    assert!(matches!(
        MetaPage::validate(&b, 4096).unwrap(),
        MetaValidity::BadPageSize(4097)
    ));

    // Header txnid (offset 8) vs body txnid (offset 64) mismatch.
    let mut b = buf.clone();
    b[8] ^= 0x01;
    assert!(matches!(
        MetaPage::validate(&b, 4096).unwrap(),
        MetaValidity::TxnidMismatch { .. }
    ));

    // A CRC-covered byte elsewhere (inside free_db/main_db, offset 90) that
    // does not touch magic/version/page_size/txnid -> BadCrc.
    let mut b = buf.clone();
    b[90] ^= 0xFF;
    assert!(matches!(
        MetaPage::validate(&b, 4096).unwrap(),
        MetaValidity::BadCrc { .. }
    ));
}

// ===========================================================================
// 5. Meta double-buffer selection truth table completion
// ===========================================================================

fn valid_slot_at(slot: u64, txnid: u64, psize: u32) -> MetaValidity {
    let mut m = MetaPage::create(slot, psize, 1024 * 1024);
    m.txnid = txnid;
    let mut buf = vec![0u8; psize as usize];
    m.encode(&mut buf).unwrap();
    MetaPage::validate(&buf, psize).unwrap()
}

fn invalid_slot() -> MetaValidity {
    MetaValidity::BadCrc {
        stored: 1,
        computed: 2,
    }
}

/// Equal-txnid + both valid is the env-creation state (SPEC 02 §3.4): the tie
/// must resolve to a deterministic, documented choice rather than panicking
/// or being arbitrary run-to-run. Per `select`'s formula
/// `(txnid[0] < txnid[1]) XOR prev_snapshot`, an exact tie makes the first
/// term `false`, so the choice is exactly `prev_snapshot` as a slot index.
#[test]
fn selection_tie_equal_txnid_is_deterministic() {
    let s0 = valid_slot_at(0, 5, 4096);
    let s1 = valid_slot_at(1, 5, 4096);
    match select_meta(&s0, &s1, false) {
        MetaChoice::Both { chosen, meta } => {
            assert_eq!(chosen, 0);
            assert_eq!(meta.txnid, 5);
        }
        other => panic!("{other:?}"),
    }
    match select_meta(&s0, &s1, true) {
        MetaChoice::Both { chosen, meta } => {
            assert_eq!(chosen, 1);
            assert_eq!(meta.txnid, 5);
        }
        other => panic!("{other:?}"),
    }
}

/// All 8 combinations of (slot0 valid?, slot1 valid?, prev_snapshot), with
/// the both-valid rows covering equal txnid, slot0-newer and slot1-newer.
#[test]
fn selection_truth_table_all_combinations() {
    let v0_newer0 = valid_slot_at(0, 5, 4096);
    let v1_older0 = valid_slot_at(1, 3, 4096);
    let inv = invalid_slot();

    // (valid, valid, false) -> higher txnid (slot 0).
    match select_meta(&v0_newer0, &v1_older0, false) {
        MetaChoice::Both { chosen, .. } => assert_eq!(chosen, 0),
        o => panic!("{o:?}"),
    }
    // (valid, valid, true) -> lower txnid (slot 1).
    match select_meta(&v0_newer0, &v1_older0, true) {
        MetaChoice::Both { chosen, .. } => assert_eq!(chosen, 1),
        o => panic!("{o:?}"),
    }
    // (valid, invalid, false) -> OnlyOne(0), regardless of flag.
    match select_meta(&v0_newer0, &inv, false) {
        MetaChoice::OnlyOne { chosen, .. } => assert_eq!(chosen, 0),
        o => panic!("{o:?}"),
    }
    // (valid, invalid, true) -> still OnlyOne(0): no older slot to pick.
    match select_meta(&v0_newer0, &inv, true) {
        MetaChoice::OnlyOne { chosen, .. } => assert_eq!(chosen, 0),
        o => panic!("{o:?}"),
    }
    // (invalid, valid, false) -> OnlyOne(1).
    match select_meta(&inv, &v1_older0, false) {
        MetaChoice::OnlyOne { chosen, .. } => assert_eq!(chosen, 1),
        o => panic!("{o:?}"),
    }
    // (invalid, valid, true) -> still OnlyOne(1).
    match select_meta(&inv, &v1_older0, true) {
        MetaChoice::OnlyOne { chosen, .. } => assert_eq!(chosen, 1),
        o => panic!("{o:?}"),
    }
    // (invalid, invalid, false) -> None.
    assert_eq!(select_meta(&inv, &inv, false), MetaChoice::None);
    // (invalid, invalid, true) -> None.
    assert_eq!(select_meta(&inv, &inv, true), MetaChoice::None);
}

// ===========================================================================
// 6. 511-boundary keys in page context / inline-overflow threshold
// ===========================================================================

/// `value_is_inline` must flip exactly at `dsize == max_node_size - 8 - ksize`
/// for every tested key length, at both psizes (SPEC 02 §4.2, ADR-0002 §D5).
#[test]
fn inline_overflow_flip_point_both_psizes() {
    for &psize in &PSIZES {
        for &ksize in &[1usize, 2, 510, 511] {
            let threshold = geometry::max_node_size(psize);
            assert!(
                threshold > 8 + ksize,
                "threshold too small for ksize {ksize}"
            );
            let dsize_at = (threshold - 8 - ksize) as u64;
            assert!(
                geometry::value_is_inline(ksize, dsize_at, psize),
                "psize={psize} ksize={ksize}: at-threshold value must be inline"
            );
            assert!(
                !geometry::value_is_inline(ksize, dsize_at + 1, psize),
                "psize={psize} ksize={ksize}: one byte over threshold must overflow"
            );
        }
    }
}

/// SPEC 02 §4.2's stated guarantee ("the largest cell that still guarantees
/// two entries fit on a page") exercised in actual page bytes: two
/// max-node-size cells must fit on a fresh page; a third must not.
#[test]
fn two_max_node_size_cells_fit_a_third_does_not() {
    for &psize in &PSIZES {
        let ksize = 2usize;
        let threshold = geometry::max_node_size(psize);
        let dsize = threshold - 8 - ksize;
        let value = vec![7u8; dsize];

        let mut buf = vec![0u8; psize as usize];
        let mut leaf = LeafMut::init(&mut buf, psize, 2, 1).unwrap();
        leaf.insert_inline(0, b"k1", 0, &value).unwrap();
        leaf.insert_inline(1, b"k2", 0, &value).unwrap();
        let err = leaf.insert_inline(2, b"k3", 0, &value).unwrap_err();
        assert!(matches!(err, PageError::PageFull { .. }));

        let view = LeafRef::new(&buf, psize).unwrap();
        assert_eq!(view.num_keys(), 2);
        assert_eq!(view.value(0), LeafValue::Inline(&value[..]));
        assert_eq!(view.value(1), LeafValue::Inline(&value[..]));
    }
}

/// A 511-byte (MAX_KEY_SIZE) key round-trips through a real page at both
/// psizes, including via `lookup`.
#[test]
fn max_size_key_roundtrip_both_psizes() {
    for &psize in &PSIZES {
        let mut buf = vec![0u8; psize as usize];
        let mut leaf = LeafMut::init(&mut buf, psize, 2, 1).unwrap();
        let key = vec![0x42u8; 511];
        leaf.insert_inline(0, &key, 0, b"v").unwrap();
        let view = LeafRef::new(&buf, psize).unwrap();
        assert_eq!(view.key(0), &key[..]);
        assert_eq!(view.lookup(&key), Ok(0));
    }
}

// ===========================================================================
// 7. Overflow run math
// ===========================================================================

#[test]
fn overflow_page_count_exact_multiples_and_off_by_one() {
    for &psize in &PSIZES {
        // Exactly the head page's capacity (psize - HEADER_SIZE) -> 1 page.
        assert_eq!(geometry::overflow_page_count((psize - 32) as u64, psize), 1);
        // One byte over -> 2 pages.
        assert_eq!(geometry::overflow_page_count((psize - 31) as u64, psize), 2);
        for n in [1u64, 2, 5] {
            let exact = n * psize as u64 - 32;
            assert_eq!(geometry::overflow_page_count(exact, psize), n);
            assert_eq!(geometry::overflow_page_count(exact + 1, psize), n + 1);
            assert_eq!(geometry::overflow_page_count(exact - 1, psize), n);
        }
    }
}

fn build_overflow_run(psize: u32, dsize: u32) -> (Vec<u8>, Vec<u8>) {
    let n = geometry::overflow_page_count(dsize as u64, psize);
    let mut run = vec![0u8; n as usize * psize as usize];
    let value: Vec<u8> = (0..dsize).map(|i| (i % 253) as u8).collect();
    let written =
        write_overflow_head(&mut run[..psize as usize], psize, 3, 9, n as u32, &value).unwrap();
    run[psize as usize..psize as usize + (value.len() - written)]
        .copy_from_slice(&value[written..]);
    (run, value)
}

#[test]
fn overflow_payload_extraction_at_page_multiple_boundaries() {
    for &psize in &PSIZES {
        for &dsize in &[psize - 32, psize - 31, psize, 2 * psize - 32] {
            let (run, value) = build_overflow_run(psize, dsize);
            let ovf = OverflowRef::new(&run, psize).unwrap();
            assert_eq!(
                ovf.ovf_pages(),
                geometry::overflow_page_count(dsize as u64, psize) as u32
            );
            assert_eq!(ovf.payload(dsize).unwrap(), &value[..]);
        }
    }
}

/// A run whose declared `ovf_pages` claims more pages than the provided
/// buffer actually covers must return a typed error on `payload()`, never
/// panic or read out of bounds.
#[test]
fn overflow_declared_pages_exceeds_buffer_is_typed_error_not_panic() {
    let psize = 4096u32;
    let mut buf = vec![0u8; psize as usize];
    // ovf_pages claims a 5-page run, but the buffer backing this OverflowRef
    // is only 1 page.
    write_overflow_head(&mut buf, psize, 1, 1, 5, &[0xAAu8; 100]).unwrap();
    let ovf = OverflowRef::new(&buf, psize).unwrap();
    assert_eq!(ovf.ovf_pages(), 5);
    let big_dsize = 5 * psize - 32; // within the *declared* capacity...
    let err = ovf.payload(big_dsize).unwrap_err();
    // ...but the backing buffer cannot supply it: BufferTooSmall, not a panic.
    assert!(matches!(err, PageError::BufferTooSmall { .. }));
}

/// A `dsize` that exceeds even the declared run's capacity is a distinct
/// typed error (`BadValueSize`) from the buffer-too-small case above.
#[test]
fn overflow_dsize_exceeding_declared_capacity_is_bad_value_size() {
    let psize = 4096u32;
    let (run, _value) = build_overflow_run(psize, 100);
    let ovf = OverflowRef::new(&run, psize).unwrap();
    let err = ovf.payload(u32::MAX).unwrap_err();
    assert!(matches!(err, PageError::BadValueSize(_)));
}
