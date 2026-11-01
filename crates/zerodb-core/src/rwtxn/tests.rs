use super::*;
use crate::env::testutil::{mem_env, mem_env_sequential};
use crate::error::MdbError;

const PS: u32 = 4096;
const MAP: u64 = 1 << 20;

fn kv(i: u32) -> (Vec<u8>, Vec<u8>) {
    (
        format!("key{i:05}").into_bytes(),
        format!("value-{i}-{}", "x".repeat((i % 40) as usize)).into_bytes(),
    )
}

/// Everything visible through the txn, in key order.
fn dump(txn: &RwTxn<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let tree = crate::rotxn::tree_of(txn);
    let mut c = tree.cursor();
    let mut out = Vec::new();
    let mut e = c.first().unwrap();
    while let Some((k, v)) = e {
        out.push((k.to_vec(), v.to_vec()));
        e = c.next().unwrap();
    }
    out
}

#[test]
fn put_get_iter_with_splits() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    // Insert in a shuffled-ish order to exercise mid-page inserts.
    let n = 300u32;
    let mut order: Vec<u32> = (0..n).collect();
    order.reverse();
    order.sort_by_key(|i| (i * 7919) % n); // deterministic shuffle
    for &i in &order {
        let (k, v) = kv(i);
        db.put(&mut txn, &k, &v).unwrap();
    }
    assert_eq!(db.len(&txn).unwrap(), n as u64);
    for i in 0..n {
        let (k, v) = kv(i);
        assert_eq!(db.get(&txn, &k).unwrap(), Some(v.as_slice()), "get {i}");
    }
    let expected: Vec<_> = (0..n).map(kv).collect();
    assert_eq!(dump(&txn), expected);
    // The tree must have split beyond one leaf.
    assert!(txn.main_record().leaf_pages > 1, "expected leaf splits");
    assert!(txn.main_record().depth >= 2, "expected a branch level");
}

/// TXN-49 item 1: get-then-put — a value copied out before a put survives;
/// the put cannot invalidate it (compile-time: the borrow ended).
#[test]
fn txn49_get_then_put() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    db.put(&mut txn, b"alpha", b"first-value").unwrap();
    let copied: Vec<u8> = db.get(&txn, b"alpha").unwrap().unwrap().to_vec();
    // Mutate: same leaf gets rewritten (in-place same-size would not, so
    // use a different size to force remove+reinsert).
    db.put(&mut txn, b"alpha", b"a-second-longer-value")
        .unwrap();
    db.put(&mut txn, b"beta", b"x").unwrap();
    assert_eq!(copied, b"first-value");
    assert_eq!(
        db.get(&txn, b"alpha").unwrap(),
        Some(b"a-second-longer-value".as_slice())
    );
}

/// TXN-49 item 2: reserve, then mutate until the leaf splits; the reserved
/// value must have landed intact and no stale pointer is retained.
#[test]
fn txn49_reserve_then_split() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let payload = vec![0xCDu8; 1000];
    db.put_reserved(&mut txn, b"reserved-key", payload.len(), |buf| {
        buf.copy_from_slice(&payload);
    })
    .unwrap();
    // Force splits of the leaf holding the reserved cell.
    for i in 0..30u32 {
        let k = format!("reserved-neighbor-{i:03}").into_bytes();
        db.put(&mut txn, &k, &vec![0x11u8; 900]).unwrap();
    }
    assert_eq!(
        db.get(&txn, b"reserved-key").unwrap(),
        Some(payload.as_slice())
    );
    assert!(txn.main_record().leaf_pages > 1);
}

/// TXN-49 item 4: dirty enough pages to grow the pgno→frame index; frame
/// addresses must be stable (TXN-44) — an untouched leaf's bytes do not
/// move while other subtrees split and the HashMap rehashes.
#[test]
fn txn49_frame_stability_across_index_growth() {
    let env = mem_env(PS, 4 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for i in 0..120u32 {
        let k = format!("a{i:04}").into_bytes();
        db.put(&mut txn, &k, b"stable-value").unwrap();
    }
    let addr_before = db.get(&txn, b"a0000").unwrap().unwrap().as_ptr() as usize;
    // Grow the dirty index with many later-subtree pages.
    for i in 0..600u32 {
        let k = format!("z{i:04}").into_bytes();
        db.put(&mut txn, &k, &[0x22u8; 64]).unwrap();
    }
    let addr_after = db.get(&txn, b"a0000").unwrap().unwrap().as_ptr() as usize;
    assert_eq!(addr_before, addr_after, "frame moved under the index");
}

/// Write-cursor referee: the stack-carrying write cursor against a
/// `BTreeMap` model. Deterministic pseudo-random walks (forward and
/// reverse, each opened by a random seek — the consumer envelope: no
/// mid-walk direction reversal) with mutations interleaved at yielded
/// entries: `put_current` (resized values, so in-place *and*
/// delete+reinsert *and* splits happen mid-walk), `del_current`, and
/// repositioning `put`. Every mutation drops the parked stack, so the
/// walk continuously alternates the resume fast path with the CurPos
/// re-seek fallback — the equivalence this test pins. Model semantics per
/// SPEC 03 §4/§7: from `At(k)` next is strictly greater / prev strictly
/// lower; from `AfterDelete(k)` next is `>= k` / prev strictly lower.
#[test]
fn rw_cursor_walks_match_btreemap_model_under_mutation() {
    use std::collections::BTreeMap;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    #[derive(Clone)]
    enum MPos {
        Start,
        At(Vec<u8>),
        AfterDel(Vec<u8>),
    }

    fn m_next(m: &BTreeMap<Vec<u8>, Vec<u8>>, p: &MPos) -> Option<(Vec<u8>, Vec<u8>)> {
        let r = match p {
            MPos::Start => m.iter().next(),
            MPos::At(k) => m.range((Excluded(k.clone()), Unbounded)).next(),
            MPos::AfterDel(k) => m.range((Included(k.clone()), Unbounded)).next(),
        };
        r.map(|(k, v)| (k.clone(), v.clone()))
    }
    fn m_prev(m: &BTreeMap<Vec<u8>, Vec<u8>>, p: &MPos) -> Option<(Vec<u8>, Vec<u8>)> {
        let r = match p {
            MPos::Start => m.iter().next_back(),
            MPos::At(k) | MPos::AfterDel(k) => {
                m.range((Unbounded, Excluded(k.clone()))).next_back()
            }
        };
        r.map(|(k, v)| (k.clone(), v.clone()))
    }

    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Deterministic xorshift64*.
    let mut state: u64 = 0x243F_6A88_85A3_08D3;
    let mut rnd = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let key_of = |n: u64| format!("k{:05}", n % 3000).into_bytes();
    let val_of = |n: u64| vec![b'a' + (n % 23) as u8; 4 + (n % 400) as usize];

    // Seed store + model identically.
    for _ in 0..500 {
        let (k, v) = (key_of(rnd()), val_of(rnd()));
        db.put(&mut txn, &k, &v).unwrap();
        model.insert(k, v);
    }

    // 40 walks of up to 60 steps each, mutating ~1 step in 3.
    for _ in 0..40 {
        let fwd = rnd() % 2 == 0;
        let mut cur = db.rw_cursor(&mut txn);
        let mut pos = MPos::Start;

        // Open with a random seek (or plain first step from Start).
        let sk = key_of(rnd());
        let (got, want): (Option<(Vec<u8>, Vec<u8>)>, _) = match rnd() % 4 {
            0 => (
                cur.seek_ge(&sk)
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec())),
                model
                    .range((Included(sk.clone()), Unbounded))
                    .next()
                    .map(|(k, v)| (k.clone(), v.clone())),
            ),
            1 => (
                cur.seek_gt(&sk)
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec())),
                model
                    .range((Excluded(sk.clone()), Unbounded))
                    .next()
                    .map(|(k, v)| (k.clone(), v.clone())),
            ),
            2 => (
                cur.seek_le(&sk)
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec())),
                model
                    .range((Unbounded, Included(sk.clone())))
                    .next_back()
                    .map(|(k, v)| (k.clone(), v.clone())),
            ),
            _ => (
                cur.seek_lt(&sk)
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec())),
                model
                    .range((Unbounded, Excluded(sk.clone())))
                    .next_back()
                    .map(|(k, v)| (k.clone(), v.clone())),
            ),
        };
        assert_eq!(got, want, "seek divergence");
        let mut on_entry = match got {
            Some((k, _)) => {
                pos = MPos::At(k);
                true
            }
            None => false,
        };

        for _ in 0..60 {
            // Maybe mutate at the current entry.
            if on_entry {
                match rnd() % 6 {
                    0 => {
                        // put_current with a resized value.
                        let nv = val_of(rnd());
                        assert!(cur.put_current(&nv).unwrap());
                        if let MPos::At(k) = &pos {
                            model.insert(k.clone(), nv);
                        } else {
                            unreachable!("on_entry implies At");
                        }
                    }
                    1 => {
                        assert!(cur.del_current().unwrap());
                        let k = match &pos {
                            MPos::At(k) => k.clone(),
                            _ => unreachable!("on_entry implies At"),
                        };
                        model.remove(&k);
                        pos = MPos::AfterDel(k);
                        // (No `on_entry = false` needed: the step below is
                        // unconditional and re-derives it.)
                    }
                    2 => {
                        // Repositioning put at an arbitrary key.
                        let (k, v) = (key_of(rnd()), val_of(rnd()));
                        cur.put(PutFlags::EMPTY, &k, &v).unwrap();
                        model.insert(k.clone(), v);
                        pos = MPos::At(k);
                    }
                    _ => {}
                }
            }
            // Step.
            let got = if fwd {
                cur.move_next()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            } else {
                cur.move_prev()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            };
            let want = if fwd {
                m_next(&model, &pos)
            } else {
                m_prev(&model, &pos)
            };
            assert_eq!(got, want, "step divergence (fwd={fwd})");
            match got {
                Some((k, _)) => {
                    pos = MPos::At(k);
                    on_entry = true;
                }
                None => break, // walk over; next walk re-seeks
            }
        }
    }

    // Final ground truth: the store must equal the model exactly.
    let mut cur = db.rw_cursor(&mut txn);
    let mut store: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while let Some((k, v)) = cur.move_next().unwrap() {
        store.push((k.to_vec(), v.to_vec()));
    }
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(store, want, "final store/model divergence");
}

#[test]
fn append_semantics() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    // APPEND into an empty tree succeeds (§6.3).
    db.put_with_flags(&mut txn, PutFlags::APPEND, b"a", b"1")
        .unwrap();
    db.put_with_flags(&mut txn, PutFlags::APPEND, b"b", b"2")
        .unwrap();
    // Equal-to-last and less-than-last are KeyExist, not overwrites.
    for k in [b"b".as_slice(), b"a"] {
        let e = db
            .put_with_flags(&mut txn, PutFlags::APPEND, k, b"nope")
            .unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::KeyExist)));
    }
    assert_eq!(db.get(&txn, b"b").unwrap(), Some(b"2".as_slice()));
    // Bulk append keeps pages ~full (append split policy §6.4).
    for i in 0..400u32 {
        let k = format!("c{i:05}").into_bytes();
        db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &[0x33u8; 50])
            .unwrap();
    }
    assert_eq!(db.len(&txn).unwrap(), 402);
}

#[test]
fn no_overwrite_semantics() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    db.put(&mut txn, b"k", b"v1").unwrap();
    let e = db
        .put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, b"k", b"v2")
        .unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::KeyExist)));
    assert_eq!(db.get(&txn, b"k").unwrap(), Some(b"v1".as_slice()));
    db.put_with_flags(&mut txn, PutFlags::NO_OVERWRITE, b"k2", b"v2")
        .unwrap();
    assert_eq!(db.len(&txn).unwrap(), 2);
}

#[test]
fn key_and_value_bounds() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for bad in [vec![], vec![0u8; 512]] {
        let e = db.put(&mut txn, &bad, b"v").unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadValSize)));
    }
    db.put(&mut txn, &vec![7u8; 511], b"v").unwrap(); // max key ok
                                                      // Oversized delete key: not an error, finds nothing (§2.1).
    assert!(!db.delete(&mut txn, &vec![0u8; 600]).unwrap());
}

#[test]
fn delete_with_rebalance_and_root_shrink() {
    let env = mem_env(PS, 4 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let n = 400u32;
    for i in 0..n {
        let (k, v) = kv(i);
        db.put(&mut txn, &k, &v).unwrap();
    }
    assert!(txn.main_record().depth >= 2);
    // Delete everything except a handful, in a merge-provoking order.
    for i in 0..n {
        if i % 97 == 0 {
            continue;
        }
        let (k, _) = kv(i);
        assert!(db.delete(&mut txn, &k).unwrap(), "delete {i}");
    }
    let remaining: Vec<_> = (0..n).filter(|i| i % 97 == 0).map(kv).collect();
    assert_eq!(dump(&txn), remaining);
    assert_eq!(db.len(&txn).unwrap(), remaining.len() as u64);
    // Delete the rest: the tree must collapse to empty (§9 shrink).
    for (k, _) in &remaining {
        assert!(db.delete(&mut txn, k).unwrap());
    }
    assert_eq!(db.len(&txn).unwrap(), 0);
    assert_eq!(txn.main_record().root, PGNO_INVALID);
    assert_eq!(txn.main_record().depth, 0);
    assert_eq!(txn.main_record().leaf_pages, 0);
    assert_eq!(txn.main_record().branch_pages, 0);
    // And it is reusable.
    db.put(&mut txn, b"again", b"works").unwrap();
    assert_eq!(db.get(&txn, b"again").unwrap(), Some(b"works".as_slice()));
}

#[test]
fn overflow_values_in_txn() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let big = vec![0xABu8; 3 * PS as usize]; // multi-page run
    db.put(&mut txn, b"big", &big).unwrap();
    assert_eq!(db.get(&txn, b"big").unwrap(), Some(big.as_slice()));
    assert!(txn.main_record().overflow_pages >= 3);
    // Replace with a different-size overflow value: old run freed.
    let big2 = vec![0x44u8; 2 * PS as usize];
    db.put(&mut txn, b"big", &big2).unwrap();
    assert_eq!(db.get(&txn, b"big").unwrap(), Some(big2.as_slice()));
    // Replace with inline.
    db.put(&mut txn, b"big", b"small-now").unwrap();
    assert_eq!(db.get(&txn, b"big").unwrap(), Some(b"small-now".as_slice()));
    assert_eq!(txn.main_record().overflow_pages, 0);
    // Reserve into an overflow run.
    let big3 = vec![0x77u8; PS as usize + 100];
    db.put_reserved(&mut txn, b"big", big3.len(), |buf| {
        buf.copy_from_slice(&big3);
    })
    .unwrap();
    assert_eq!(db.get(&txn, b"big").unwrap(), Some(big3.as_slice()));
    // Delete frees the run.
    assert!(db.delete(&mut txn, b"big").unwrap());
    assert_eq!(txn.main_record().overflow_pages, 0);
}

#[test]
fn clear_resets_everything() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for i in 0..100u32 {
        let (k, v) = kv(i);
        db.put(&mut txn, &k, &v).unwrap();
    }
    db.put(&mut txn, b"big", &vec![9u8; 9000]).unwrap();
    db.clear(&mut txn).unwrap();
    assert_eq!(db.len(&txn).unwrap(), 0);
    assert_eq!(dump(&txn), Vec::new());
    assert_eq!(txn.main_record().root, PGNO_INVALID);
    db.put(&mut txn, b"post-clear", b"v").unwrap();
    assert_eq!(db.len(&txn).unwrap(), 1);
}

#[test]
fn rw_cursor_put_and_del_current() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for i in 0..10u32 {
        db.put(
            &mut txn,
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        )
        .unwrap();
    }
    {
        let mut cur = db.rw_cursor(&mut txn);
        // Advance to the 4th entry (k3) and rewrite it (bigger value).
        for _ in 0..4 {
            cur.move_next().unwrap();
        }
        assert!(cur.put_current(b"rewritten-much-longer-value").unwrap());
        // Continue: next must be k4.
        let (k, _) = cur.move_next().unwrap().unwrap();
        assert_eq!(k, b"k4");
        // Delete k4; next yields k5 (the successor of the deleted entry).
        assert!(cur.del_current().unwrap());
        let (k, _) = cur.move_next().unwrap().unwrap();
        assert_eq!(k, b"k5");
    }
    assert_eq!(
        db.get(&txn, b"k3").unwrap(),
        Some(b"rewritten-much-longer-value".as_slice())
    );
    assert_eq!(db.get(&txn, b"k4").unwrap(), None);
    assert_eq!(db.len(&txn).unwrap(), 9);
    // Unpositioned cursor: mutations are no-ops returning false.
    let mut cur = db.rw_cursor(&mut txn);
    assert!(!cur.put_current(b"x").unwrap());
    assert!(!cur.del_current().unwrap());
}

#[test]
fn delete_range_counts() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for i in 0..20u32 {
        db.put(&mut txn, format!("k{i:02}").as_bytes(), b"v")
            .unwrap();
    }
    let n = db
        .delete_range(
            &mut txn,
            std::ops::Bound::Included(b"k05".as_slice()),
            std::ops::Bound::Excluded(b"k15".as_slice()),
        )
        .unwrap();
    assert_eq!(n, 10);
    assert_eq!(db.len(&txn).unwrap(), 10);
    assert_eq!(db.get(&txn, b"k05").unwrap(), None);
    assert_eq!(db.get(&txn, b"k15").unwrap(), Some(b"v".as_slice()));
}

#[test]
fn map_full_poisons_txn() {
    // 16-page map: 2 metas + 14 usable pages.
    let env = mem_env(PS, 16 * PS as u64);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let mut hit_full = false;
    for i in 0..64u32 {
        let k = format!("k{i:02}").into_bytes();
        match db.put(&mut txn, &k, &vec![0u8; 3000]) {
            Ok(()) => {}
            Err(Error::Mdb(MdbError::MapFull)) => {
                hit_full = true;
                break;
            }
            Err(e) => panic!("unexpected error {e:?}"),
        }
    }
    assert!(hit_full, "expected MapFull on a 16-page map");
    // The txn is poisoned: further mutations and commit refuse.
    let e = db.put(&mut txn, b"more", b"v").unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::BadTxn)));
    let e = txn.commit().unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::BadTxn)));
}

#[test]
fn abort_discards_everything() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    {
        let mut txn = env.write_txn().unwrap();
        db.put(&mut txn, b"k", b"v").unwrap();
        txn.abort();
    }
    // A fresh reader sees the (empty) committed snapshot.
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), None);
    assert_eq!(rtxn.txnid(), 0);
    // The write mutex was released: a new writer can begin.
    let txn2 = env.write_txn().unwrap();
    assert_eq!(txn2.txnid(), 1, "aborted id is reused (TXN-2/TXN-63)");
}

#[test]
fn unchanged_commit_is_a_noop() {
    let env = mem_env(PS, MAP);
    let txn = env.write_txn().unwrap();
    // Nothing mutated: commit must not attempt I/O (the mem backing would
    // error) and must not advance the commit point.
    txn.commit().unwrap();
    assert_eq!(env.txnid(), 0);
}

#[test]
fn named_db_in_txn_create_put_read_clear_drop() {
    // Exercises the named-DB write path under mem_env (miri-clean, no
    // commit): create → put → read via the catalog-resolved record →
    // clear → drop, plus F_SUBDATA on the catalog entry.
    let env = mem_env(PS, MAP);
    let mut txn = env.write_txn().unwrap();
    let named = env.create_database(&mut txn, Some(b"users")).unwrap();
    // The main catalog now has one F_SUBDATA entry keyed by the name.
    let main = env.main_database();
    assert_eq!(main.len(&txn).unwrap(), 1);
    assert_eq!(
        main.get(&txn, b"users").unwrap().map(<[u8]>::len),
        Some(48) // the 48-byte DBRecord
    );
    // Writes land in the named tree, isolated from main.
    for i in 0..800u32 {
        named
            .put(&mut txn, format!("u{i:04}").as_bytes(), b"value")
            .unwrap();
    }
    assert_eq!(named.len(&txn).unwrap(), 800);
    assert_eq!(main.len(&txn).unwrap(), 1); // still just the catalog entry
    assert_eq!(
        named.get(&txn, b"u0100").unwrap(),
        Some(b"value".as_slice())
    );
    assert!(main.get(&txn, b"u0100").unwrap().is_none());
    let st = named.stat(&txn).unwrap();
    assert_eq!(st.entries, 800);
    assert!(st.depth >= 2, "expected a split, depth {}", st.depth);
    // clear empties the named tree but keeps the catalog entry.
    named.clear(&mut txn).unwrap();
    assert_eq!(named.len(&txn).unwrap(), 0);
    assert_eq!(main.len(&txn).unwrap(), 1);
    // drop removes the catalog entry.
    named.drop_db(&mut txn).unwrap();
    assert_eq!(main.len(&txn).unwrap(), 0);
    // Reusable name after a re-create.
    let named2 = env.create_database(&mut txn, Some(b"users")).unwrap();
    named2.put(&mut txn, b"again", b"1").unwrap();
    assert_eq!(named2.get(&txn, b"again").unwrap(), Some(b"1".as_slice()));
    assert_eq!(main.len(&txn).unwrap(), 1);
}

#[test]
fn named_db_bad_name_rejected() {
    // (DbsFull is covered on real files in crates/zerodb/tests/named_db.rs;
    // mem_env's capacity is generous.)
    let env = mem_env(PS, MAP);
    let mut txn = env.write_txn().unwrap();
    // Empty / oversized names → BadValSize.
    for bad in [b"".as_slice(), &[0u8; 512]] {
        let e = env.create_database(&mut txn, Some(bad)).unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::BadValSize)));
    }
    // 511-byte name is fine.
    env.create_database(&mut txn, Some(&[7u8; 511])).unwrap();
    // None → main DB, always Ok.
    env.create_database(&mut txn, None).unwrap();
}

#[test]
fn reader_snapshot_isolated_from_writer() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    // The reader's pinned snapshot must not see uncommitted writes
    // (TXN-11); the writer sees its own (TXN-38).
    assert_eq!(db.get(&rtxn, b"k").unwrap(), None);
    assert_eq!(db.get(&wtxn, b"k").unwrap(), Some(b"v".as_slice()));
}

// -- del_current path repair (SPEC 03 §5.4a repair table) -----------------

/// A wide key/value pair: at 4 K pages the leaves hold ~6 entries and the
/// branches ~8 children, so a depth-3 tree needs only ~120 entries and a
/// front-to-back drain rebalances on most deletes (the structural case the
/// §5.4a repair table exists for).
fn wide_kv(i: u32) -> (Vec<u8>, Vec<u8>) {
    (
        format!("{i:05}{}", "k".repeat(455)).into_bytes(),
        format!("v{i}-{}", "x".repeat(96)).into_bytes(),
    )
}

/// Drain the whole DB front-to-back through the write cursor (milli's
/// `iter_mut` + `del_current` pattern) and return the keys in the order
/// the drain yielded them.
fn drain_all(db: &Database, txn: &mut RwTxn<'_>) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    let mut cur = db.rw_cursor(txn);
    while let Some((k, _v)) = cur.move_next().unwrap() {
        seen.push(k.to_vec());
        assert!(cur.del_current().unwrap());
    }
    seen
}

/// Tree sizes are miri-scaled like proptest case counts: the shadow check
/// re-descends per delete, which the interpreter pays ~1000x for.
fn miri_scaled(native: u32, miri: u32) -> u32 {
    if cfg!(miri) {
        miri
    } else {
        native
    }
}

/// A `first/del_current/next` drain over depths 1, 2 and 3+ must yield
/// every key exactly once, in order — none skipped, none repeated — and
/// leave the tree empty. The depth-2/3 runs exercise the kept-path repair
/// (borrow-from-right, merge-with-right, root shrink); the debug shadow
/// check re-derives every kept path against a fresh descent.
#[test]
fn cursor_drain_matches_model_at_depths() {
    for (n, min_depth) in [(4u32, 1u16), (12, 2), (miri_scaled(150, 80), 3)] {
        let env = mem_env(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut model = std::collections::BTreeMap::new();
        for i in 0..n {
            let (k, v) = wide_kv(i);
            db.put(&mut txn, &k, &v).unwrap();
            model.insert(k, v);
        }
        assert!(
            txn.main_record().depth >= min_depth,
            "wanted depth >= {min_depth}, got {} for n = {n}",
            txn.main_record().depth
        );
        let seen = drain_all(&db, &mut txn);
        let expected: Vec<Vec<u8>> = model.keys().cloned().collect();
        assert_eq!(
            seen, expected,
            "drain skipped or repeated entries (n = {n})"
        );
        assert_eq!(db.len(&txn).unwrap(), 0);
        assert_eq!(txn.main_record().root, PGNO_INVALID);
    }
}

/// A backward drain (`prev/del_current`) deletes at `pki != 0`, so its
/// rebalances pair with the LEFT sibling and `prev` resumes from the
/// repaired path. It must still visit every key exactly once, in reverse
/// order.
#[test]
fn cursor_reverse_drain_matches_model() {
    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let n = miri_scaled(130, 72);
    let mut model = std::collections::BTreeMap::new();
    for i in 0..n {
        let (k, v) = wide_kv(i);
        db.put(&mut txn, &k, &v).unwrap();
        model.insert(k, v);
    }
    assert!(txn.main_record().depth >= 3);
    let mut seen = Vec::new();
    {
        let mut cur = db.rw_cursor(&mut txn);
        while let Some((k, _v)) = cur.move_prev().unwrap() {
            seen.push(k.to_vec());
            assert!(cur.del_current().unwrap());
        }
    }
    let expected: Vec<Vec<u8>> = model.keys().rev().cloned().collect();
    assert_eq!(seen, expected);
    assert_eq!(db.len(&txn).unwrap(), 0);
}

/// Drain in segments with point `get`s in between (the cursor cannot
/// overlap a shared borrow of the txn, so milli interleaves exactly this
/// way): deleted keys must read back `None`, survivors `Some`, and the
/// resumed cursor must not skip or repeat across the seam.
#[test]
fn cursor_drain_interleaved_with_gets() {
    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let n = miri_scaled(120, 72);
    for i in 0..n {
        let (k, v) = wide_kv(i);
        db.put(&mut txn, &k, &v).unwrap();
    }
    assert!(txn.main_record().depth >= 3);
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut segment = 0u32;
    loop {
        let mut deleted_now = 0u32;
        {
            let mut cur = db.rw_cursor(&mut txn);
            // Resume strictly after the last drained key.
            let start = match seen.last() {
                Some(k) => cur.seek_gt(k).unwrap(),
                None => cur.move_next().unwrap(),
            };
            let mut entry = start.map(|(k, _)| k.to_vec());
            while let Some(k) = entry {
                seen.push(k);
                assert!(cur.del_current().unwrap());
                deleted_now += 1;
                if deleted_now == 17 {
                    break; // segment boundary: drop the cursor, go get()
                }
                entry = cur.move_next().unwrap().map(|(k, _)| k.to_vec());
            }
        }
        for k in &seen {
            assert_eq!(db.get(&txn, k).unwrap(), None, "segment {segment}");
        }
        let survivors = n as usize - seen.len();
        assert_eq!(db.len(&txn).unwrap() as usize, survivors);
        if deleted_now < 17 {
            break;
        }
        segment += 1;
    }
    let expected: Vec<Vec<u8>> = (0..n).map(|i| wide_kv(i).0).collect();
    assert_eq!(seen, expected);
    assert_eq!(db.len(&txn).unwrap(), 0);
}

/// The lever must actually fire: a front-to-back drain over a deep tree
/// performs structural deletes whose path is KEPT (repaired), not
/// re-derived. Counted in debug builds only, where the shadow check also
/// re-validates every kept path.
#[cfg(debug_assertions)]
#[test]
fn cursor_drain_keeps_repaired_paths() {
    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let n = miri_scaled(150, 80);
    for i in 0..n {
        let (k, v) = wide_kv(i);
        db.put(&mut txn, &k, &v).unwrap();
    }
    assert!(txn.main_record().depth >= 3);
    let mut cur = db.rw_cursor(&mut txn);
    while let Some((_k, _v)) = cur.move_next().unwrap() {
        assert!(cur.del_current().unwrap());
    }
    // At ~6 wide entries per 4 K leaf, a 150-entry front drain rebalances
    // dozens of times; the leftmost pairing keeps all of those except the
    // rare separator-split fallback, so "many" is the honest bound.
    assert!(
        cur.repaired_keeps >= 10,
        "kept-path repair never fired: {} structural keeps",
        cur.repaired_keeps
    );
}

/// A drain that starts mid-tree deletes from leaves that are not their
/// parent's child 0, so each rebalance borrows from or merges with the
/// LEFT sibling (SPEC 03 §10). The cursor must keep its repaired path
/// across those (the debug shadow check re-derives every kept one), and
/// must still yield exactly the span, in order. A backward drain over the
/// same span then runs the same repairs under `prev`.
#[cfg(debug_assertions)]
#[test]
fn cursor_mid_drain_keeps_left_pairing_paths() {
    for backward in [false, true] {
        let env = mem_env(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let n = miri_scaled(150, 80);
        let mut model = std::collections::BTreeMap::new();
        for i in 0..n {
            let (k, v) = wide_kv(i);
            db.put(&mut txn, &k, &v).unwrap();
            model.insert(k, v);
        }
        assert!(txn.main_record().depth >= 3);
        let (lo, hi) = (wide_kv(n / 4).0, wide_kv(3 * n / 4).0);
        let mut seen = Vec::new();
        let keeps = {
            let mut cur = db.rw_cursor(&mut txn);
            let mut kv = if backward {
                cur.seek_lt(&hi).unwrap()
            } else {
                cur.seek_ge(&lo).unwrap()
            };
            while let Some((k, _v)) = kv {
                if k < lo.as_slice() || k >= hi.as_slice() {
                    break;
                }
                seen.push(k.to_vec());
                assert!(cur.del_current().unwrap());
                kv = if backward {
                    cur.move_prev().unwrap()
                } else {
                    cur.move_next().unwrap()
                };
            }
            cur.repaired_keeps
        };
        let mut expected: Vec<Vec<u8>> = model
            .range(lo.clone()..hi.clone())
            .map(|(k, _)| k.clone())
            .collect();
        if backward {
            expected.reverse();
        }
        assert_eq!(seen, expected, "backward = {backward}");
        model.retain(|k, _| k < &lo || k >= &hi);
        assert_eq!(dump(&txn), model.into_iter().collect::<Vec<_>>());
        // ~6 wide entries per 4 K leaf: a 75-entry span rebalances dozens
        // of times, nearly all of them with the left sibling.
        assert!(
            keeps >= 10,
            "left-pairing repair never fired: {keeps} keeps"
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..Default::default()
    })]

    /// Random key sets, random partial drains: delete every `k`-th entry
    /// through the cursor, then check the survivors equal a `BTreeMap`
    /// model and the visited sequence was every key exactly once, in
    /// order. Small keys keep the tree shallow *and* mixed-`pki` (both
    /// repair and fallback paths fire); the wide-entry unit tests above
    /// pin the deep-tree shapes. Sizes are miri-scaled like the case
    /// count.
    #[test]
    fn cursor_partial_drain_matches_model(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(proptest::prelude::any::<u8>(), 1..24),
            proptest::collection::vec(
                proptest::prelude::any::<u8>(),
                0..if cfg!(miri) { 80 } else { 400 },
            ),
            1..if cfg!(miri) { 40usize } else { 120 },
        ),
        k in 1..5u32,
    ) {
        let env = mem_env(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut model = entries.clone();
        for (key, val) in &entries {
            db.put(&mut txn, key, val).unwrap();
        }
        let mut seen = Vec::new();
        {
            let mut cur = db.rw_cursor(&mut txn);
            let mut i = 0u32;
            while let Some((key, _v)) = cur.move_next().unwrap() {
                let key = key.to_vec();
                if i.is_multiple_of(k) {
                    proptest::prop_assert!(cur.del_current().unwrap());
                    model.remove(&key);
                }
                seen.push(key);
                i += 1;
            }
        }
        // Every key visited exactly once, in order (nothing skipped or
        // repeated around the deletes).
        let all: Vec<Vec<u8>> = entries.keys().cloned().collect();
        proptest::prop_assert_eq!(&seen, &all);
        // Survivors match the model, by full dump and by point gets.
        let left: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(a, b)| (a.clone(), b.clone())).collect();
        proptest::prop_assert_eq!(dump(&txn), left);
        for key in entries.keys() {
            let want = model.get(key).map(Vec::as_slice);
            proptest::prop_assert_eq!(db.get(&txn, key).unwrap(), want);
        }
        // Drain the rest: still every survivor exactly once, then empty.
        let rest = drain_all(&db, &mut txn);
        let survivors: Vec<Vec<u8>> = model.keys().cloned().collect();
        proptest::prop_assert_eq!(rest, survivors);
        proptest::prop_assert_eq!(db.len(&txn).unwrap(), 0);
    }
}

// -- put at the cursor position (SPEC 03 §5.4b) ---------------------------

/// A rewrite of every entry through the cursor over a depth-3 tree, in
/// both directions and through both put entry points, with values that
/// keep their size, shrink, and grow past what the leaf can hold (so some
/// puts split and drop the path). Every entry must be visited exactly
/// once, in order, and end with its new value; the debug shadow check
/// re-derives every kept path.
#[test]
fn cursor_rewrite_visits_every_entry_once() {
    let n = miri_scaled(150, 80);
    for backward in [false, true] {
        let env = mem_env(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut model = std::collections::BTreeMap::new();
        for i in 0..n {
            let (k, v) = wide_kv(i);
            db.put(&mut txn, &k, &v).unwrap();
            model.insert(k, v);
        }
        assert!(txn.main_record().depth >= 3);
        let mut seen = Vec::new();
        {
            let mut cur = db.rw_cursor(&mut txn);
            let mut i = 0usize;
            loop {
                let e = if backward {
                    cur.move_prev().unwrap()
                } else {
                    cur.move_next().unwrap()
                };
                let Some((k, v)) = e else { break };
                let (k, old_len) = (k.to_vec(), v.len());
                let new_len = match i % 4 {
                    0 | 1 => old_len,
                    2 => old_len / 2,
                    _ => old_len * 3,
                };
                let nv = vec![b'a' + (i % 26) as u8; new_len];
                if i.is_multiple_of(2) {
                    assert!(cur.put_current(&nv).unwrap());
                } else {
                    cur.put(PutFlags::EMPTY, &k, &nv).unwrap();
                }
                model.insert(k.clone(), nv);
                seen.push(k);
                i += 1;
            }
            #[cfg(debug_assertions)]
            assert!(
                cur.repaired_keeps >= n as u64 / 2,
                "cursor puts kept only {} paths",
                cur.repaired_keeps
            );
        }
        let mut expected: Vec<Vec<u8>> = model.keys().cloned().collect();
        if backward {
            expected.reverse();
        }
        assert_eq!(seen, expected, "rewrite skipped or repeated entries");
        let want: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
        assert_eq!(dump(&txn), want);
    }
}

/// A cursor put of a key other than the one under the cursor is a keyed
/// put that moves the cursor there; a flagged put of the current key
/// keeps its flag semantics.
#[test]
fn cursor_put_other_key_and_flags() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for k in [b"a", b"c", b"e"] {
        db.put(&mut txn, k, b"0").unwrap();
    }
    let mut cur = db.rw_cursor(&mut txn);
    assert_eq!(cur.move_next().unwrap().unwrap().0, b"a");
    cur.put(PutFlags::EMPTY, b"d", b"new").unwrap();
    assert_eq!(cur.move_next().unwrap().unwrap().0, b"e");
    assert_eq!(cur.move_prev().unwrap().unwrap().0, b"d");
    let r = cur.put(PutFlags::NO_OVERWRITE, b"d", b"x");
    assert!(matches!(r, Err(Error::Mdb(MdbError::KeyExist))));
    cur.put(PutFlags::EMPTY, b"d", b"same").unwrap();
    assert_eq!(cur.move_next().unwrap().unwrap(), (&b"e"[..], &b"0"[..]));
    assert_eq!(cur.move_prev().unwrap().unwrap(), (&b"d"[..], &b"same"[..]));
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..Default::default()
    })]

    /// Random trees, random walk mixing rewrites of random sizes (some
    /// split), deletes and plain steps through one cursor: the visited
    /// sequence must be every key exactly once, in order, and the final
    /// content must equal a `BTreeMap` model.
    #[test]
    fn cursor_rewrite_walk_matches_model(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(proptest::prelude::any::<u8>(), 1..24),
            proptest::collection::vec(
                proptest::prelude::any::<u8>(),
                0..if cfg!(miri) { 80 } else { 400 },
            ),
            1..if cfg!(miri) { 40usize } else { 120 },
        ),
        actions in proptest::collection::vec((0..4u8, 0..600usize), 1..64),
    ) {
        let env = mem_env(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut model = entries.clone();
        for (key, val) in &entries {
            db.put(&mut txn, key, val).unwrap();
        }
        let mut seen = Vec::new();
        {
            let mut cur = db.rw_cursor(&mut txn);
            let mut i = 0usize;
            while let Some((key, _v)) = cur.move_next().unwrap() {
                let key = key.to_vec();
                let (act, len) = actions[i % actions.len()];
                let nv = vec![(i % 251) as u8; len];
                match act {
                    0 => {
                        proptest::prop_assert!(cur.put_current(&nv).unwrap());
                        model.insert(key.clone(), nv);
                    }
                    1 => {
                        cur.put(PutFlags::EMPTY, &key, &nv).unwrap();
                        model.insert(key.clone(), nv);
                    }
                    2 => {
                        proptest::prop_assert!(cur.del_current().unwrap());
                        model.remove(&key);
                    }
                    _ => {}
                }
                seen.push(key);
                i += 1;
            }
        }
        let all: Vec<Vec<u8>> = entries.keys().cloned().collect();
        proptest::prop_assert_eq!(&seen, &all);
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(a, b)| (a.clone(), b.clone())).collect();
        proptest::prop_assert_eq!(dump(&txn), want);
    }
}

// -- delete_range (leaf-granular, SPEC 00 r37) ----------------------------

/// Reference model: the keys of `model` covered by the bound pair under
/// memcmp order (the main DB's ordering).
fn covered_keys(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    lower: &std::ops::Bound<Vec<u8>>,
    upper: &std::ops::Bound<Vec<u8>>,
) -> Vec<Vec<u8>> {
    use std::ops::Bound;
    model
        .keys()
        .filter(|k| {
            let lo_ok = match lower {
                Bound::Unbounded => true,
                Bound::Included(l) => *k >= l,
                Bound::Excluded(l) => *k > l,
            };
            let hi_ok = match upper {
                Bound::Unbounded => true,
                Bound::Included(h) => *k <= h,
                Bound::Excluded(h) => *k < h,
            };
            lo_ok && hi_ok
        })
        .cloned()
        .collect()
}

/// Drive one leaf-granular `delete_range` against a fresh env seeded with
/// `entries`, and assert every observable against the `BTreeMap` model:
/// the returned count, the survivors by full dump and by point gets, the
/// `entries` stat, and exact `overflow_pages` accounting (only surviving
/// big values may keep runs).
fn check_delete_range_case(
    entries: &BTreeMap<Vec<u8>, Vec<u8>>,
    lower: std::ops::Bound<Vec<u8>>,
    upper: std::ops::Bound<Vec<u8>>,
) {
    use std::ops::Bound;
    let env = mem_env(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    for (k, v) in entries {
        db.put(&mut txn, k, v).unwrap();
    }
    let mut model = entries.clone();
    let removed = covered_keys(&model, &lower, &upper);
    for k in &removed {
        model.remove(k);
    }
    fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
        match b {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => Bound::Included(k.as_slice()),
            Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        }
    }
    let n = db
        .delete_range(&mut txn, as_ref(&lower), as_ref(&upper))
        .unwrap();
    assert_eq!(n, removed.len() as u64, "returned count");
    assert_eq!(db.len(&txn).unwrap(), model.len() as u64, "entries stat");
    let left: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(a, b)| (a.clone(), b.clone())).collect();
    assert_eq!(dump(&txn), left, "survivors by dump");
    for k in entries.keys() {
        let want = model.get(k).map(Vec::as_slice);
        assert_eq!(db.get(&txn, k).unwrap(), want, "point get");
    }
    let want_ovf: u64 = model
        .iter()
        .filter(|(k, v)| !value_is_inline(k.len(), v.len() as u64, PS))
        .map(|(_, v)| overflow_page_count(v.len() as u64, PS))
        .sum();
    assert_eq!(
        db.stat(&txn).unwrap().overflow_pages,
        want_ovf,
        "overflow_pages stat"
    );
}

#[test]
fn delete_range_bound_combinations_and_edges() {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    // Wide cells keep leaf fanout low, so interior ranges cross whole
    // leaves, partially-covered leaves, and both tree edges. Sizes are
    // miri-scaled like the proptest case counts.
    let n = miri_scaled(120, 30);
    let vlen = if cfg!(miri) { 40 } else { 300 } as usize;
    let mut entries = BTreeMap::new();
    for i in 0..n {
        entries.insert(
            format!("k{i:03}").into_bytes(),
            vec![b'v'; vlen + (i % 7) as usize],
        );
    }
    // Bound keys as fractions of the key space, so the shapes survive
    // the miri scaling; `kx` lands between two keys.
    let k = |i: u32| format!("k{i:03}").into_bytes();
    let kx = |i: u32| format!("k{i:03}x").into_bytes();
    // Empty db.
    check_delete_range_case(&BTreeMap::new(), Unbounded, Unbounded);
    // Full wipe across many leaves.
    check_delete_range_case(&entries, Unbounded, Unbounded);
    // Interior range spanning several leaves.
    check_delete_range_case(&entries, Included(k(n / 12)), Excluded(k(5 * n / 6)));
    // Both ends exclusive, bounds falling between keys.
    check_delete_range_case(&entries, Excluded(kx(n / 12)), Excluded(kx(5 * n / 6)));
    // Single key, surrounded by every bound kind.
    check_delete_range_case(&entries, Included(k(n / 2)), Included(k(n / 2)));
    check_delete_range_case(&entries, Excluded(k(n / 2)), Included(k(n / 2 + 1)));
    check_delete_range_case(&entries, Included(k(n / 2 + 1)), Excluded(k(n / 2 + 2)));
    // Empty ranges: Excluded/Excluded on one key, inverted bounds.
    check_delete_range_case(&entries, Excluded(k(n / 2)), Excluded(k(n / 2)));
    check_delete_range_case(&entries, Included(k(5 * n / 6)), Included(k(n / 12)));
    // Tree edges: a prefix drain and a suffix drain.
    check_delete_range_case(&entries, Unbounded, Excluded(k(n / 6)));
    check_delete_range_case(&entries, Included(k(5 * n / 6)), Unbounded);
    // Bounds entirely outside the key space.
    check_delete_range_case(&entries, Included(b"x".to_vec()), Unbounded);
    check_delete_range_case(&entries, Unbounded, Excluded(b"a".to_vec()));
}

#[test]
fn delete_range_frees_covered_overflow_runs_only() {
    use std::ops::Bound::{Excluded, Included};
    // Every 5th value overflows; the range covers some of each kind and
    // leaves overflow values alive on both sides.
    let n = miri_scaled(40, 15);
    let mut entries = BTreeMap::new();
    for i in 0..n {
        let len = if i % 5 == 0 { 3000 + i as usize } else { 200 };
        entries.insert(format!("k{i:03}").into_bytes(), vec![b'o'; len]);
    }
    check_delete_range_case(
        &entries,
        Included(format!("k{:03}", n / 8).into_bytes()),
        Excluded(format!("k{:03}", 3 * n / 4).into_bytes()),
    );
}

#[test]
fn delete_range_poisoned_txn_refuses() {
    // Same poisoning recipe as `map_full_poisons_txn`.
    let env = mem_env(PS, 16 * PS as u64);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let mut hit_full = false;
    for i in 0..64u32 {
        let key = format!("k{i:02}").into_bytes();
        if db.put(&mut txn, &key, &vec![0u8; 3000]).is_err() {
            hit_full = true;
            break;
        }
    }
    assert!(hit_full, "expected MapFull on a 16-page map");
    let e = db
        .delete_range(
            &mut txn,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
        )
        .unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::BadTxn)));
}

/// Child-1 separator of the root branch. On a freshly split, delete-free
/// tree it **equals** the second leaf's first key (SPEC 03 §11 INV-6:
/// equality holds for freshly built/split branches), which makes it an
/// exact leaf boundary for the shape pins below.
fn root_separator_1(txn: &RwTxn<'_>) -> Vec<u8> {
    let rec = txn.main_db;
    let frame = txn
        .source()
        .bytes_from(txn.psize, rec.root)
        .expect("root readable");
    BranchRef::new(frame, txn.psize)
        .expect("root is a branch")
        .key(1)
        .to_vec()
}

/// §10 empty-page rule, walk mode (SPEC 03): a leaf that
/// `delete_range` empties is merged away by the very next rebalance —
/// never borrowed into. Deleting exactly the first leaf's keys of a
/// two-leaf tree must therefore collapse it to one full leaf in that same
/// call; the borrow arm would instead pull one (uncovered) entry across
/// and leave both leaves standing. Point deletes keep the default
/// neighbor policy (the gc_reclaim churn-flatness pin depends on it).
#[test]
fn delete_range_merges_emptied_leaf_instead_of_borrowing() {
    let env = mem_env(PS, MAP);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    // Descending inserts avoid the append-split skew; wide entries keep
    // the tree at exactly two leaves.
    let total = 6u64;
    for i in (0..total).rev() {
        db.put(&mut txn, format!("k{i}").as_bytes(), &vec![b'w'; 1100])
            .unwrap();
    }
    let pre = db.stat(&txn).unwrap();
    assert_eq!(pre.depth, 2, "want a root branch over leaves");
    assert!(pre.leaf_pages >= 2, "want at least two leaves: {pre:?}");
    let boundary = root_separator_1(&txn);
    // Exactly the first leaf: [-inf, second leaf's first key).
    let n = db
        .delete_range(
            &mut txn,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Excluded(boundary.as_slice()),
        )
        .unwrap();
    assert!(n >= 1);
    let stat = db.stat(&txn).unwrap();
    assert_eq!(stat.entries, total - n);
    // The emptied leaf merged with its (untouched, full) right sibling in
    // the same call: one leaf gone. The borrow arm would have pulled one
    // uncovered entry across and kept both leaves standing.
    assert_eq!(
        stat.leaf_pages,
        pre.leaf_pages - 1,
        "emptied leaf must merge away, not borrow"
    );
    let survivor = vec![b'w'; 1100];
    assert_eq!(db.get(&txn, &boundary).unwrap(), Some(survivor.as_slice()));
}

/// Bound strategy for [`delete_range_matches_model`]: anchor on an
/// existing key (or its "between keys" successor, a 0-byte appended) and
/// a bound kind, independently per side — inverted and empty ranges
/// included.
fn bound_from(keys: &[Vec<u8>], pick: usize, kind: u8) -> std::ops::Bound<Vec<u8>> {
    use std::ops::Bound;
    if keys.is_empty() {
        return Bound::Unbounded;
    }
    let k = keys[pick % keys.len()].clone();
    match kind % 4 {
        0 => Bound::Unbounded,
        1 => Bound::Included(k),
        2 => Bound::Excluded(k),
        _ => {
            let mut between = k;
            between.push(0);
            Bound::Included(between)
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..Default::default()
    })]

    /// Random key sets (a slice of the values overflow-sized), random
    /// bound pairs anchored on existing keys: the leaf-granular
    /// `delete_range` must match the `BTreeMap` model on count,
    /// survivors, and stats. Small keys keep leaves wide; the wide-entry
    /// unit tests above pin the many-leaf shapes. Sizes are miri-scaled
    /// like the case count.
    #[test]
    fn delete_range_matches_model(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(proptest::prelude::any::<u8>(), 1..24),
            proptest::prelude::prop_oneof![
                // Inline values, element-random.
                4 => proptest::collection::vec(
                    proptest::prelude::any::<u8>(),
                    0..if cfg!(miri) { 40usize } else { 200 },
                ),
                // Overflow values (>= 2030 always overflows at 4 K for
                // keys < 24 bytes): a (len, fill) pair, not a per-element
                // vec — element-random 2.5 KB values would make this the
                // miri long pole for no extra coverage.
                1 => proptest::prelude::Strategy::prop_map(
                    (
                        2030..if cfg!(miri) { 2060usize } else { 3300 },
                        proptest::prelude::any::<u8>(),
                    ),
                    |(len, b)| vec![b; len],
                ),
            ],
            1..if cfg!(miri) { 16usize } else { 100 },
        ),
        lo_pick in 0usize..1024,
        lo_kind in 0u8..4,
        hi_pick in 0usize..1024,
        hi_kind in 0u8..4,
    ) {
        let keys: Vec<Vec<u8>> = entries.keys().cloned().collect();
        let lower = bound_from(&keys, lo_pick, lo_kind);
        let upper = bound_from(&keys, hi_pick, hi_kind);
        check_delete_range_case(&entries, lower, upper);
    }
}

// -- rightmost-leaf finger (SPEC 03 §6.6) ----------------------------------

/// Everything visible in `db` through the txn, in key order.
fn dump_db(db: &Database, txn: &RwTxn<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(txn)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// The main DB doubles as the named-DB catalog (SPEC 02 §6): a model
/// comparison of user entries must skip the `F_SUBDATA` records.
fn dump_main_user(txn: &RwTxn<'_>, names: &[&[u8]]) -> Vec<(Vec<u8>, Vec<u8>)> {
    dump(txn)
        .into_iter()
        .filter(|(k, _)| !names.contains(&k.as_slice()))
        .collect()
}

/// The fast path actually fires: an APPEND load of a multi-level tree
/// pays one descent per split and hits the finger for every other put
/// (each hit shadow-checked against a fresh descent), and a plain
/// ascending load fires it just the same. Contents stay exact.
#[test]
fn finger_fires_on_append_and_seq_loads() {
    let env = mem_env_sequential(PS, 8 << 20);
    let db = env.main_database();
    let mut txn = env.write_txn().unwrap();
    // Wide entries so the tree splits into a branch level even at the
    // miri-scaled size.
    let n = miri_scaled(600, 60);
    for i in 0..n {
        let (k, v) = wide_kv(i);
        db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
            .unwrap();
    }
    assert!(
        txn.main_record().depth >= 2,
        "load must build a branch level"
    );
    // Every put except the re-establishing descent after each split (and
    // the very first) is a hit; >= n/2 is the loose, honest bound.
    assert!(
        txn.finger_hits >= u64::from(n) / 2,
        "APPEND load took the fast path only {} times of {n}",
        txn.finger_hits
    );
    let expected: Vec<_> = (0..n).map(wide_kv).collect();
    assert_eq!(dump(&txn), expected);

    // Plain sequential puts (no APPEND flag) fire it too.
    let named = env.create_database(&mut txn, Some(b"seq")).unwrap();
    let before = txn.finger_hits;
    for i in 0..n {
        let (k, v) = wide_kv(i);
        named.put(&mut txn, &k, &v).unwrap();
    }
    assert!(
        txn.finger_hits - before >= u64::from(n) / 2,
        "sequential load took the fast path only {} times of {n}",
        txn.finger_hits - before
    );
    assert_eq!(dump_db(&named, &txn), expected);
}

/// ADR-0015: the finger runs only where the sequential-writes setting is
/// on. Off by default (an APPEND load never hits); a per-database
/// override wins over the env default in both directions; `None` goes
/// back to the default. The override applies from the next write txn.
#[test]
fn finger_follows_the_sequential_writes_setting() {
    fn load(env: &Env, db: &Database, n: u32) -> u64 {
        let mut txn = env.write_txn().unwrap();
        for i in 0..n {
            let (k, v) = wide_kv(i);
            db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
                .unwrap();
        }
        let hits = txn.finger_hits;
        txn.abort();
        hits
    }
    let n = miri_scaled(200, 40);

    let off = mem_env(PS, 8 << 20);
    let main = off.main_database();
    assert!(!off.sequential_writes(&main), "default must be off");
    assert_eq!(load(&off, &main, n), 0, "finger ran with the setting off");
    off.set_sequential_writes(&main, Some(true));
    assert!(load(&off, &main, n) > 0, "override on did not enable it");
    off.set_sequential_writes(&main, None);
    assert_eq!(load(&off, &main, n), 0, "None must restore the default");

    // Named DBs, created inside the measured txn (a mem env cannot
    // commit); an aborted create keeps its dbi, so the override set
    // between txns applies when the next txn creates it again.
    let on = mem_env_sequential(PS, 8 << 20);
    let named_load = |env: &Env| {
        let mut txn = env.write_txn().unwrap();
        let db = env.create_database(&mut txn, Some(b"ids")).unwrap();
        for i in 0..n {
            let (k, v) = wide_kv(i);
            db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
                .unwrap();
        }
        let hits = txn.finger_hits;
        txn.abort();
        (db, hits)
    };
    let (named, hits) = named_load(&on);
    assert!(hits > 0, "env default on did not reach a named DB");
    on.set_sequential_writes(&named, Some(false));
    assert!(!on.sequential_writes(&named));
    assert_eq!(named_load(&on).1, 0, "override off did not disable it");
    assert!(
        on.sequential_writes(&on.main_database()),
        "main keeps the env default"
    );
}

/// The finger under adversarial interleaving, all in one txn: sequential
/// and APPEND runs on the main DB and a named DB, broken up by
/// out-of-order puts, tail deletes, a `delete_range`, `clear`, and
/// drop + recreate — every phase checked against a `BTreeMap` model
/// (and every fast-path hit shadow-checked against a fresh descent).
#[test]
fn finger_model_interleaved_ops() {
    let env = mem_env_sequential(PS, 8 << 20);
    let main = env.main_database();
    let mut txn = env.write_txn().unwrap();
    let named = env.create_database(&mut txn, Some(b"aux")).unwrap();
    let mut mm: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut mn: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let n = miri_scaled(400, 48);
    // Phase 1: interleaved ascending loads — plain puts on main, APPEND
    // on the named DB — so both trees keep a live finger at once.
    for i in 0..n {
        let (k, v) = kv(i);
        main.put(&mut txn, &k, &v).unwrap();
        mm.insert(k.clone(), v.clone());
        named
            .put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
            .unwrap();
        mn.insert(k, v);
    }
    // Phase 2: out-of-order puts into main (finger misses, then
    // re-establishes on the next end insert).
    for i in (0..n).rev().step_by(3) {
        let (k, _) = kv(i);
        let v = format!("rewritten-{i}").into_bytes();
        main.put(&mut txn, &k, &v).unwrap();
        mm.insert(k, v);
    }
    assert_eq!(
        dump_main_user(&txn, &[b"aux"]),
        mm.clone().into_iter().collect::<Vec<_>>()
    );
    // Phase 3: delete the tail of main (kills the right spine), then
    // append past the old maximum again.
    for i in n.saturating_sub(24)..n {
        let (k, _) = kv(i);
        assert!(main.delete(&mut txn, &k).unwrap());
        mm.remove(&k);
    }
    for i in n..n + 32 {
        let (k, v) = kv(i);
        main.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
            .unwrap();
        mm.insert(k, v);
    }
    // Phase 4: range-delete a middle band of main, then more end inserts.
    let (lo, _) = kv(n / 4);
    let (hi, _) = kv(n / 2);
    let deleted = main
        .delete_range(
            &mut txn,
            std::ops::Bound::Included(lo.as_slice()),
            std::ops::Bound::Excluded(hi.as_slice()),
        )
        .unwrap();
    let covered: Vec<Vec<u8>> = mm
        .range::<[u8], _>((
            std::ops::Bound::Included(lo.as_slice()),
            std::ops::Bound::Excluded(hi.as_slice()),
        ))
        .map(|(k, _)| k.clone())
        .collect();
    assert_eq!(deleted, covered.len() as u64);
    for k in covered {
        mm.remove(&k);
    }
    for i in n + 32..n + 48 {
        let (k, v) = kv(i);
        main.put(&mut txn, &k, &v).unwrap();
        mm.insert(k, v);
    }
    assert_eq!(
        dump_main_user(&txn, &[b"aux"]),
        mm.clone().into_iter().collect::<Vec<_>>()
    );
    // Phase 5: clear the named DB, refill ascending.
    named.clear(&mut txn).unwrap();
    mn.clear();
    for i in 0..n / 2 {
        let (k, v) = kv(i);
        named
            .put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
            .unwrap();
        mn.insert(k, v);
    }
    assert_eq!(
        dump_db(&named, &txn),
        mn.clone().into_iter().collect::<Vec<_>>()
    );
    // Phase 6: drop + recreate the named DB, refill with plain puts.
    named.drop_db(&mut txn).unwrap();
    mn.clear();
    let named = env.create_database(&mut txn, Some(b"aux")).unwrap();
    for i in 0..n / 2 {
        let (k, v) = kv(i);
        named.put(&mut txn, &k, &v).unwrap();
        mn.insert(k, v);
    }
    assert_eq!(dump_db(&named, &txn), mn.into_iter().collect::<Vec<_>>());
    assert_eq!(
        dump_main_user(&txn, &[b"aux"]),
        mm.into_iter().collect::<Vec<_>>()
    );
    assert!(txn.finger_hits > 0, "the interleaving never hit the finger");
}

/// The stale-finger hazard the F3 ownership hook exists for: build main
/// so its rightmost pages are dirty and fingered, delete the tail so the
/// rightmost leaf merges away (its dirty pgno goes to the loose list),
/// fill a named DB so allocation re-serves that pgno as a page of the
/// OTHER tree (still dirty!), then append to main again. A finger keyed
/// on dirtiness alone would descend into the named DB's page; here the
/// delete invalidates, `free_page` forgets, the hit re-verifies the
/// spine, and the debug shadow check re-descends — the append must land
/// correctly and both trees stay exact.
#[test]
fn finger_survives_freed_page_reuse_by_another_tree() {
    let env = mem_env_sequential(PS, 8 << 20);
    let main = env.main_database();
    let mut txn = env.write_txn().unwrap();
    // Named "!thief" so the catalog entry sorts BEFORE the digit-prefixed
    // user keys — otherwise the entry itself is the main tree's last key
    // and every APPEND below would rightly refuse with KeyExist.
    let named = env.create_database(&mut txn, Some(b"!thief")).unwrap();
    let n = miri_scaled(300, 60);
    let mut mm: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..n {
        let (k, v) = wide_kv(i);
        main.put(&mut txn, &k, &v).unwrap();
        mm.insert(k, v);
    }
    assert!(txn.main_record().depth >= 2);
    let leaves_before = txn.main_record().leaf_pages;
    // Tail deletes until at least one rightmost leaf merged away — its
    // dirty, this-txn pgno lands on the loose list.
    let mut i = n;
    while txn.main_record().leaf_pages == leaves_before {
        i -= 1;
        let (k, _) = wide_kv(i);
        assert!(main.delete(&mut txn, &k).unwrap());
        mm.remove(&k);
        assert!(i > 0, "never merged a rightmost leaf");
    }
    assert!(
        !work!(txn).loose.is_empty(),
        "merged leaf pgno must be loose"
    );
    // The named DB's growth pops the loose list first (GC-8): the freed
    // main-tree page is re-served as a page of the named tree.
    for j in 0..n {
        let (k, v) = wide_kv(j);
        named.put(&mut txn, &k, &v).unwrap();
    }
    // Append to main past its old maximum: any surviving finger for main
    // would have to re-verify through the reused page and MUST miss.
    for j in n..n + 40 {
        let (k, v) = wide_kv(j);
        main.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v)
            .unwrap();
        mm.insert(k, v);
    }
    assert_eq!(
        dump_main_user(&txn, &[b"!thief"]),
        mm.into_iter().collect::<Vec<_>>()
    );
    let expected: Vec<_> = (0..n).map(wide_kv).collect();
    assert_eq!(dump_db(&named, &txn), expected);
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        failure_persistence: None,
        cases: if cfg!(miri) { 16 } else { 256 },
        ..Default::default()
    })]

    /// Random op tapes over the main DB — APPEND bursts, plain ascending
    /// bursts, out-of-order puts, point deletes, range deletes, clears —
    /// must match a `BTreeMap` model exactly. The bursts keep the finger
    /// hot; the other ops exercise every invalidation edge between hits,
    /// and each hit runs the debug shadow re-descent. Sizes are
    /// miri-scaled like the case count.
    #[test]
    fn finger_random_ops_match_model(
        ops in proptest::collection::vec(
            (0u8..6, proptest::prelude::any::<u16>(), proptest::prelude::any::<u16>()),
            1..if cfg!(miri) { 12usize } else { 48 },
        ),
    ) {
        let env = mem_env_sequential(PS, 8 << 20);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        // Ascending key counter for the sequential/APPEND bursts.
        let mut next: u32 = 0;
        let mkkey = |i: u32| format!("k{i:08}").into_bytes();
        let mkval = |a: u16, b: u16| {
            let mut v = format!("v{a}-{b}-").into_bytes();
            v.resize(8 + (b % 160) as usize, b'x');
            v
        };
        for &(kind, a, b) in &ops {
            match kind {
                // APPEND burst past the current maximum.
                0 => {
                    for _ in 0..(a % 24) + 1 {
                        let k = mkkey(next);
                        let v = mkval(a, b);
                        db.put_with_flags(&mut txn, PutFlags::APPEND, &k, &v).unwrap();
                        model.insert(k, v);
                        next += 1;
                    }
                }
                // Plain ascending burst past the current maximum.
                1 => {
                    for _ in 0..(a % 24) + 1 {
                        let k = mkkey(next);
                        let v = mkval(b, a);
                        db.put(&mut txn, &k, &v).unwrap();
                        model.insert(k, v);
                        next += 1;
                    }
                }
                // Out-of-order put (insert or replace, possibly past the
                // max — `next` then moves so APPEND bursts stay legal).
                2 => {
                    let idx = u32::from(a) % (next + 40);
                    let k = mkkey(idx);
                    let v = mkval(a, b);
                    db.put(&mut txn, &k, &v).unwrap();
                    model.insert(k, v);
                    next = next.max(idx + 1);
                }
                // Point delete of an existing key (when any).
                3 => {
                    if let Some(k) = model.keys().nth(a as usize % model.len().max(1)).cloned() {
                        proptest::prop_assert!(db.delete(&mut txn, &k).unwrap());
                        model.remove(&k);
                    }
                }
                // Range delete between two existing keys.
                4 => {
                    if !model.is_empty() {
                        let ka = model.keys().nth(a as usize % model.len()).cloned().unwrap();
                        let kb = model.keys().nth(b as usize % model.len()).cloned().unwrap();
                        let (lo, hi) = if ka <= kb { (ka, kb) } else { (kb, ka) };
                        let got = db.delete_range(
                            &mut txn,
                            std::ops::Bound::Included(lo.as_slice()),
                            std::ops::Bound::Included(hi.as_slice()),
                        ).unwrap();
                        let covered: Vec<Vec<u8>> = model
                            .range(lo..=hi)
                            .map(|(k, _)| k.clone())
                            .collect();
                        proptest::prop_assert_eq!(got, covered.len() as u64);
                        for k in covered {
                            model.remove(&k);
                        }
                    }
                }
                // Clear (rare: kind == 5 only).
                _ => {
                    db.clear(&mut txn).unwrap();
                    model.clear();
                }
            }
        }
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        proptest::prop_assert_eq!(dump(&txn), want);
        proptest::prop_assert_eq!(db.len(&txn).unwrap(), model.len() as u64);
    }
}
