//! Bounded free-page records (SPEC 05 GC-5, GC-11 step (c), INV-29;
//! ADR-0037): a transaction that frees more pages than one record holds
//! (`K` ids, 249 at 4 KiB pages) writes its list as several records keyed
//! `(txnid, seq)`, and later commits drain them record by record.
//!
//! The cases put the freeing transaction's list on either side of a multiple
//! of `K` (from `2K` up: a list of at most 490 ids rides in the meta annex,
//! SPEC 05 §2a), with the meta annex carried into it, an older freed list drawn
//! from in the same transaction (so `freelist_save` rewrites drained records
//! and its own records grow and split while it runs), and a reader pinned
//! across it. After every commit:
//! - the image passes `zerodb::check::check_image` (INV-22..29: reachable
//!   XOR free, no record longer than `K` or on an overflow run, the records
//!   of one transaction in disjoint ranges);
//! - the database holds exactly the model's contents;
//! - a pinned reader still reads the contents of its commit.

use std::collections::BTreeMap;
use std::path::Path;

use proptest::prelude::*;
use zerodb::{check, Env, EnvOpenOptions};
use zerodb_core::page::geometry::{gc_chunk_cap, gc_key_decode, pil_decode, GcKey};
use zerodb_core::page::{BranchRef, LeafRef, LeafValue, MetaPage, MetaValidity, PGNO_INVALID};
use zerodb_oracle::tempdir::TempDir;

const PS: u32 = 4096;

fn k() -> usize {
    gc_chunk_cap(PS)
}

fn open(dir: &Path) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(256 << 20);
    o.page_size(PS);
    o.open(dir).expect("open env")
}

/// A value on exactly one overflow page.
fn one_page(fill: u8) -> Vec<u8> {
    vec![fill; PS as usize / 2]
}

fn key(i: u32) -> Vec<u8> {
    format!("v{i:06}").into_bytes()
}

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

fn contents(env: &Env, rtxn: &zerodb::RoTxn<'_>) -> Contents {
    env.main_database()
        .iter(rtxn)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// The free-page records of the live meta's free tree, in key order.
fn gc_records(dir: &Path) -> Vec<(GcKey, Vec<u64>)> {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).unwrap();
    let ps = PS as usize;
    let slot = |i: usize| match MetaPage::validate(&bytes[i * ps..(i + 1) * ps], PS) {
        Ok(MetaValidity::Valid(m)) => Some(m),
        _ => None,
    };
    let meta = match (slot(0), slot(1)) {
        (Some(a), Some(b)) => {
            if a.txnid >= b.txnid {
                a
            } else {
                b
            }
        }
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => panic!("no valid meta slot"),
    };
    let mut out = Vec::new();
    if meta.free_db.root != PGNO_INVALID {
        walk(&bytes, meta.free_db.root, meta.free_db.depth, &mut out);
    }
    out
}

fn walk(file: &[u8], pgno: u64, depth: u16, out: &mut Vec<(GcKey, Vec<u64>)>) {
    let ps = PS as usize;
    let page = &file[pgno as usize * ps..(pgno as usize + 1) * ps];
    if depth > 1 {
        let b = BranchRef::new(page, PS).expect("GC branch decodes");
        for i in 0..b.num_keys() {
            walk(file, b.child_pgno(i), depth - 1, out);
        }
    } else {
        let l = LeafRef::new(page, PS).expect("GC leaf decodes");
        for i in 0..l.num_keys() {
            let key = gc_key_decode(l.key(i)).expect("16-byte GC key");
            let LeafValue::Inline(v) = l.value(i) else {
                panic!("GC record {key} on an overflow run");
            };
            out.push((key, pil_decode(v).expect("PIL decodes")));
        }
    }
}

/// The ids txn `t` freed, summed over its records, and how many records.
fn list_of(dir: &Path, t: u64) -> (usize, usize) {
    let recs: Vec<_> = gc_records(dir)
        .into_iter()
        .filter(|(k, _)| k.txnid == t)
        .collect();
    (recs.iter().map(|(_, ids)| ids.len()).sum(), recs.len())
}

fn last_txnid(env: &Env) -> u64 {
    env.read_txn().unwrap().txnid()
}

/// Commit hook: the image is clean and holds the model; a pinned reader
/// still reads its commit.
fn verify(
    dir: &Path,
    env: &Env,
    model: &Contents,
    pinned: &Option<(zerodb::RoTxn<'_>, Contents)>,
    at: &str,
) -> Result<(), TestCaseError> {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).unwrap();
    let v = check::check_image(&bytes, PS);
    prop_assert!(v.is_empty(), "{}: {:#?}", at, v);
    let r = env.read_txn().unwrap();
    prop_assert_eq!(&contents(env, &r), model, "{}", at);
    drop(r);
    if let Some((r, want)) = pinned {
        prop_assert_eq!(&contents(env, r), want, "pinned reader, {}", at);
    }
    Ok(())
}

/// One case. `chunks * K + offset` one-page values are deleted in one
/// transaction (`del`), so its list lands near a multiple of `K` ids.
/// `older` values are deleted in an earlier transaction and the `del`
/// transaction first re-inserts `reuse` values, drawing from that older
/// list. `annex` small writes before `del` leave a meta annex for it to
/// carry. `pin` keeps a reader open from before `del` until `small` small
/// commits later.
#[derive(Debug, Clone)]
struct Case {
    chunks: usize,
    offset: i32,
    older: u32,
    reuse: u32,
    annex: u32,
    pin: bool,
    small: u32,
}

fn run_case(c: &Case) -> Result<(), TestCaseError> {
    let dir = TempDir::new().unwrap();
    let env = open(dir.path());
    let db = env.main_database();
    let mut model = Contents::new();
    let target = (c.chunks * k()) as i64 + i64::from(c.offset);
    let del = u32::try_from(target.max(1)).unwrap();
    let total = del + c.older;

    let mut w = env.write_txn().unwrap();
    for i in 0..total {
        let v = one_page(i as u8);
        db.put(&mut w, &key(i), &v).unwrap();
        model.insert(key(i), v);
    }
    w.commit().unwrap();
    verify(dir.path(), &env, &model, &None, "load")?;

    // An older freed list: the top `older` values.
    if c.older > 0 {
        let mut w = env.write_txn().unwrap();
        for i in del..total {
            db.delete(&mut w, &key(i)).unwrap();
            model.remove(&key(i));
        }
        w.commit().unwrap();
        verify(dir.path(), &env, &model, &None, "older delete")?;
    }
    for j in 0..c.annex {
        let mut w = env.write_txn().unwrap();
        let k = format!("a{j:04}").into_bytes();
        db.put(&mut w, &k, b"annex").unwrap();
        model.insert(k, b"annex".to_vec());
        w.commit().unwrap();
    }
    let mut pinned = c.pin.then(|| {
        let r = env.read_txn().unwrap();
        let want = contents(&env, &r);
        (r, want)
    });

    // The big delete, after re-inserting `reuse` values (drawn from the
    // older list, so the save rewrites drained records too).
    let mut w = env.write_txn().unwrap();
    for j in 0..c.reuse {
        let k = format!("r{j:04}").into_bytes();
        let v = one_page(j as u8 ^ 0x5A);
        db.put(&mut w, &k, &v).unwrap();
        model.insert(k, v);
    }
    for i in 0..del {
        db.delete(&mut w, &key(i)).unwrap();
        model.remove(&key(i));
    }
    w.commit().unwrap();
    let big = last_txnid(&env);
    verify(dir.path(), &env, &model, &pinned, "big delete")?;
    let (ids, records) = list_of(dir.path(), big);
    prop_assert!(
        ids > k(),
        "the big delete's list fits one record: {} ids",
        ids
    );
    prop_assert!(
        records >= ids.div_ceil(k()),
        "{} ids in {} records",
        ids,
        records
    );

    // Small commits drain the records one at a time.
    for s in 0..c.small {
        if s == c.small / 2 {
            pinned = None;
        }
        let mut w = env.write_txn().unwrap();
        let k = format!("s{s:04}").into_bytes();
        let v = if s % 3 == 0 {
            one_page(s as u8)
        } else {
            vec![s as u8; 64]
        };
        db.put(&mut w, &k, &v).unwrap();
        model.insert(k, v);
        w.commit().unwrap();
        verify(
            dir.path(),
            &env,
            &model,
            &pinned,
            &format!("small commit {s}"),
        )?;
    }
    Ok(())
}

fn case() -> impl Strategy<Value = Case> {
    (
        2usize..=5,
        -4i32..=4,
        prop_oneof![1 => Just(0u32), 2 => 1u32..600],
        0u32..40,
        0u32..3,
        any::<bool>(),
        4u32..24,
    )
        .prop_map(|(chunks, offset, older, reuse, annex, pin, small)| Case {
            chunks,
            offset,
            older,
            reuse: if older == 0 { 0 } else { reuse },
            annex,
            pin,
            small,
        })
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 48,
        ..ProptestConfig::default()
    })]

    #[test]
    fn freed_lists_across_record_boundaries_stay_bounded_and_whole(c in case()) {
        run_case(&c)?;
    }
}

/// With no older free list and no reader, the big delete's save never grows
/// its own list (nothing committed in the free tree to copy on write), so it
/// writes exactly `ceil(ids / K)` records. Sweeping the deleted count across
/// each multiple of `K` must show both sides of every boundary (the list also
/// holds the freed leaf and branch pages, about one per 150 values, so the
/// window reaches further below the boundary than above it). A list short
/// enough for the meta annex never reaches the tree and is skipped.
#[test]
fn record_count_follows_the_list_length_across_boundaries() {
    for chunks in 2..=4usize {
        let mut sides = (false, false);
        for offset in -24..=6i32 {
            let dir = TempDir::new().unwrap();
            let env = open(dir.path());
            let db = env.main_database();
            let n = (chunks * k()) as i32 + offset;
            let mut w = env.write_txn().unwrap();
            for i in 0..n as u32 {
                db.put(&mut w, &key(i), &one_page(i as u8)).unwrap();
            }
            w.commit().unwrap();
            let mut w = env.write_txn().unwrap();
            db.clear(&mut w).unwrap();
            w.commit().unwrap();
            let (ids, records) = list_of(dir.path(), last_txnid(&env));
            let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
            assert_eq!(check::check_image(&bytes, PS), Vec::<String>::new());
            if ids == 0 {
                // At most 490 ids: the list rode in the meta annex.
                continue;
            }
            assert_eq!(records, ids.div_ceil(k()), "{ids} ids");
            if ids <= chunks * k() {
                sides.0 = true;
            } else {
                sides.1 = true;
            }
        }
        assert_eq!(
            sides,
            (true, true),
            "the sweep missed a side of {chunks} * K"
        );
    }
}
