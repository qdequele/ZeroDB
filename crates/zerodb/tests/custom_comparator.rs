//! Milestone 2.4 — custom key comparators.
//!
//! ## Why there is no differential half, and what replaces it
//!
//! Phase 2's acceptance line asks for "differential semantics tests where LMDB
//! has the feature". LMDB *has* `mdb_set_compare` — but **heed never calls
//! it**, and the oracle's LMDB engine drives C LMDB *through heed 0.22.1*
//! (`zerodb-oracle/src/lmdb.rs`, ADR-0001). There is therefore no way to make
//! the oracle's LMDB side use a custom comparator without either (a) bypassing
//! heed to call `lmdb_master_sys::mdb_set_compare` on a raw dbi, which means
//! standing up a second, hand-rolled LMDB driver whose txn/dbi lifetimes are
//! managed outside the harness that the rest of Phase 1 was validated with, or
//! (b) changing the oracle's engine abstraction. Neither is worth it here,
//! because a differential would only be checking that *our* comparator and
//! *their* comparator — the same closure, expressed twice — sort the same way.
//! That is a tautology, not a parity risk: the parity risk in this milestone is
//! entirely about whether **every** engine path routes through the comparator,
//! which a cross-engine diff would not expose any better than the tests below.
//!
//! So this file pins the property that actually matters, three ways:
//!
//!   1. **Exact reversal.** Under a reverse comparator, iteration must be
//!      exactly the reverse of the memcmp iteration over the same key set —
//!      not "sorted somehow", but element-for-element reversed.
//!   2. **Every op routes through it.** `get`, `range`, `rev_range`, all four
//!      neighbor seeks, `first`/`last`, `delete`, `delete_range` and `APPEND`
//!      are each asserted against the comparator's order, because each is a
//!      separate opportunity to fall back to memcmp.
//!   3. **The tree is still structurally valid**, checked by walking the
//!      committed image, plus a memcmp control DB in the same env that must be
//!      unaffected.
//!
//! Do not weaken these (CLAUDE.md rule 2).

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use zerodb::{
    CompactionOption, Comparator, CopyToFile, Env, EnvOpenOptions, FnComparator, PutFlags,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-cmp-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(32 << 20);
    opts.max_dbs(16);
    opts.max_readers(32);
    opts.open(dir).expect("open env")
}

/// Descending byte order — the sharpest possible test ordering, because every
/// memcmp fallback anywhere in the engine flips a result.
fn reverse() -> Box<dyn Comparator> {
    Box::new(FnComparator::new(
        "test.reverse.v1",
        |a: &[u8], b: &[u8]| b.cmp(a),
    ))
}

/// Order by the *last* byte, then by the whole key. Shares no prefix structure
/// with memcmp, so it catches paths that assume prefix-ordering properties
/// (branch descent, `set_range` leaf-hopping) rather than just a flipped sign.
fn by_last_byte() -> Box<dyn Comparator> {
    Box::new(FnComparator::new(
        "test.by-last-byte.v1",
        |a: &[u8], b: &[u8]| match a.last().cmp(&b.last()) {
            Ordering::Equal => a.cmp(b),
            o => o,
        },
    ))
}

/// A key set big enough to force branch pages (multi-level descent), so the
/// comparator is exercised in `child_index`, not only in the leaf binary
/// search.
fn keys(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("key{i:05}").into_bytes()).collect()
}

fn collect(db: &zerodb::Database, r: &zerodb::RoTxn<'_>) -> Vec<Vec<u8>> {
    db.iter(r).map(|e| e.unwrap().0.to_vec()).collect()
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, 4096);
    // `check_image` is memcmp-defined (SPEC 03 §2.0): it walks every tree
    // asserting byte-ascending order, so a custom-comparator DB legitimately
    // trips INV-5/INV-6. Everything *else* must still be clean — that is the
    // part that proves the engine did not corrupt page structure, accounting,
    // or reachability while reordering.
    let unrelated: Vec<&String> = v
        .iter()
        .filter(|s| !s.starts_with("INV-5") && !s.starts_with("INV-6"))
        .collect();
    assert!(
        unrelated.is_empty(),
        "structural invariant violations unrelated to key order: {unrelated:#?}"
    );
}

// ---------------------------------------------------------------------------
// 1. Exact reversal
// ---------------------------------------------------------------------------

#[test]
fn reverse_comparator_produces_exactly_reversed_iteration() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let ks = keys(500);

    let (plain, rev) = {
        let mut w = env.write_txn().unwrap();
        let plain = env.create_database(&mut w, Some(b"plain")).unwrap();
        let rev = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        // Insert in a shuffled-ish order so neither DB gets a free pass from
        // insertion order matching its ordering.
        for i in (0..ks.len()).rev() {
            plain.put(&mut w, &ks[i], b"v").unwrap();
        }
        for k in &ks {
            rev.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
        (plain, rev)
    };

    let r = env.read_txn().unwrap();
    let plain_order = collect(&plain, &r);
    let rev_order = collect(&rev, &r);

    assert_eq!(plain_order.len(), ks.len());
    assert_eq!(rev_order.len(), ks.len());
    let mut expected = plain_order.clone();
    expected.reverse();
    assert_eq!(
        rev_order, expected,
        "a reverse comparator must yield EXACTLY the reverse sequence, \
         element for element — not merely 'some other order'"
    );

    // The memcmp control DB in the same env is untouched by the registration.
    let mut sorted = ks.clone();
    sorted.sort();
    assert_eq!(
        plain_order, sorted,
        "registering a comparator on one named DB must not affect any other"
    );

    // Reverse iteration of a reverse-ordered DB is forward memcmp order.
    let rev_rev: Vec<Vec<u8>> = rev
        .rev_iter(&r)
        .map(|e| e.unwrap().0.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        rev_rev, sorted,
        "rev_iter walks the comparator order backwards"
    );

    drop(r);
    assert_clean(dir.path());
}

// ---------------------------------------------------------------------------
// 2. Every operation routes through the comparator
// ---------------------------------------------------------------------------

#[test]
fn point_lookups_find_every_key_under_a_custom_comparator() {
    // The most basic failure mode: descent uses the comparator but the tree
    // was built with it too, so a memcmp `get` lands on the wrong leaf and
    // reports a present key as absent.
    let dir = TempDir::new();
    let env = open(dir.path());
    let ks = keys(400);

    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        for (i, k) in ks.iter().enumerate() {
            db.put(&mut w, k, format!("v{i}").as_bytes()).unwrap();
        }
        w.commit().unwrap();
        db
    };

    let r = env.read_txn().unwrap();
    for (i, k) in ks.iter().enumerate() {
        assert_eq!(
            db.get(&r, k).unwrap(),
            Some(format!("v{i}").as_bytes()),
            "every inserted key must be findable: {k:?}"
        );
    }
    assert_eq!(db.get(&r, b"absent").unwrap(), None);
    assert_eq!(db.len(&r).unwrap(), ks.len() as u64);
}

#[test]
fn first_last_and_neighbor_seeks_follow_the_comparator() {
    let dir = TempDir::new();
    let env = open(dir.path());
    // Small, hand-checkable set.
    let ks: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];

    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        for k in &ks {
            db.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
        db
    };

    let r = env.read_txn().unwrap();
    // Under reverse order the sequence is e d c b a.
    assert_eq!(
        db.first(&r).unwrap().map(|(k, _)| k.to_vec()),
        Some(b"e".to_vec())
    );
    assert_eq!(
        db.last(&r).unwrap().map(|(k, _)| k.to_vec()),
        Some(b"a".to_vec())
    );

    // "greater than c" in reverse order means bytewise LESS than c → "b".
    assert_eq!(
        db.get_greater_than(&r, b"c")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        Some(b"b".to_vec()),
        "get_greater_than is 'next in the comparator's order', not 'next byte-wise'"
    );
    assert_eq!(
        db.get_greater_than_or_equal_to(&r, b"c")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        Some(b"c".to_vec())
    );
    assert_eq!(
        db.get_lower_than(&r, b"c")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        Some(b"d".to_vec())
    );
    assert_eq!(
        db.get_lower_than_or_equal_to(&r, b"c")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        Some(b"c".to_vec())
    );

    // Seeks on absent keys, and past both ends of the comparator order.
    assert_eq!(
        db.get_greater_than(&r, b"a")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        None,
        "'a' is the comparator-maximum, so nothing is greater"
    );
    assert_eq!(
        db.get_lower_than(&r, b"e")
            .unwrap()
            .map(|(k, _)| k.to_vec()),
        None,
        "'e' is the comparator-minimum, so nothing is lower"
    );
}

#[test]
fn ranges_use_the_comparator_for_both_seek_and_termination() {
    // The bug this catches: the cursor seeks with the comparator but the
    // iterator's upper-bound test uses memcmp, truncating the scan at the
    // first element the byte order disagrees about.
    use std::ops::Bound;

    let dir = TempDir::new();
    let env = open(dir.path());
    let ks: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];

    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        for k in &ks {
            db.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
        db
    };

    let r = env.read_txn().unwrap();
    // In comparator order (e d c b a), the range [d, b] is d, c, b.
    let got: Vec<Vec<u8>> = db
        .range(
            &r,
            Bound::Included(b"d".as_slice()),
            Bound::Included(b"b".as_slice()),
        )
        .map(|e| e.unwrap().0.to_vec())
        .collect();
    assert_eq!(
        got,
        vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec()],
        "range bounds are comparator-ordered: lo=d, hi=b is a NON-empty range here"
    );

    // Exclusive upper bound drops `b`.
    let got: Vec<Vec<u8>> = db
        .range(
            &r,
            Bound::Included(b"d".as_slice()),
            Bound::Excluded(b"b".as_slice()),
        )
        .map(|e| e.unwrap().0.to_vec())
        .collect();
    assert_eq!(got, vec![b"d".to_vec(), b"c".to_vec()]);

    // Reverse range over the same bounds walks it backwards.
    let got: Vec<Vec<u8>> = db
        .rev_range(
            &r,
            Bound::Included(b"d".as_slice()),
            Bound::Included(b"b".as_slice()),
        )
        .map(|e| e.unwrap().0.to_vec())
        .collect();
    assert_eq!(got, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
}

#[test]
fn append_accepts_comparator_ascending_and_rejects_otherwise() {
    // APPEND compares against the tree's last key. Under memcmp this DB's keys
    // arrive in DESCENDING byte order, so a memcmp APPEND check would reject
    // every one of them with KeyExist.
    let dir = TempDir::new();
    let env = open(dir.path());

    let mut w = env.write_txn().unwrap();
    let db = env
        .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
        .unwrap();
    for k in [b"e", b"d", b"c", b"b", b"a"] {
        db.put_with_flags(&mut w, PutFlags::APPEND, k, b"v")
            .unwrap_or_else(|e| panic!("APPEND of {k:?} in comparator order must succeed: {e:?}"));
    }
    // Now a key that is NOT greater in the comparator's order must be refused.
    let err = db
        .put_with_flags(&mut w, PutFlags::APPEND, b"z", b"v")
        .expect_err("'z' is comparator-less-than 'a', so APPEND must fail");
    assert!(
        matches!(err, zerodb::Error::Mdb(zerodb::MdbError::KeyExist)),
        "APPEND misuse is KeyExist (SPEC 00 row 58), got {err:?}"
    );
    // Equal-to-last is also KeyExist.
    let err = db.put_with_flags(&mut w, PutFlags::APPEND, b"a", b"v");
    assert!(matches!(
        err,
        Err(zerodb::Error::Mdb(zerodb::MdbError::KeyExist))
    ));
    w.commit().unwrap();

    let r = env.read_txn().unwrap();
    assert_eq!(
        collect(&db, &r),
        vec![
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec()
        ]
    );
}

#[test]
fn delete_and_delete_range_use_the_comparator() {
    use std::ops::Bound;

    let dir = TempDir::new();
    let env = open(dir.path());
    let ks = keys(200);

    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        for k in &ks {
            db.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
        db
    };

    {
        let mut w = env.write_txn().unwrap();
        // Point delete must find its key.
        assert!(
            db.delete(&mut w, &ks[100]).unwrap(),
            "point delete finds the key"
        );
        assert!(
            !db.delete(&mut w, &ks[100]).unwrap(),
            "second delete is a miss"
        );
        w.commit().unwrap();
    }

    {
        let mut w = env.write_txn().unwrap();
        // In comparator order the DB runs from key00199 down to key00000.
        // [key00010, key00005] is a non-empty comparator range of 6 keys.
        let n = db
            .delete_range(
                &mut w,
                Bound::Included(ks[10].as_slice()),
                Bound::Included(ks[5].as_slice()),
            )
            .unwrap();
        assert_eq!(
            n, 6,
            "delete_range spans the comparator-ordered interval [key00010 .. key00005]"
        );
        w.commit().unwrap();
    }

    let r = env.read_txn().unwrap();
    assert_eq!(db.len(&r).unwrap(), 200 - 1 - 6);
    for (i, k) in ks.iter().enumerate().take(11).skip(5) {
        assert_eq!(db.get(&r, k).unwrap(), None, "ks[{i}] was range-deleted");
    }
    assert!(db.get(&r, &ks[11]).unwrap().is_some());
    assert!(db.get(&r, &ks[4]).unwrap().is_some());
    drop(r);
    assert_clean(dir.path());
}

#[test]
fn a_non_sign_flipped_comparator_also_works_end_to_end() {
    // `reverse` is a sign flip, which a buggy implementation could accidentally
    // satisfy. `by_last_byte` shares no structure with memcmp at all.
    let dir = TempDir::new();
    let env = open(dir.path());
    let ks = keys(300);

    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"lastbyte"), by_last_byte())
            .unwrap();
        for k in &ks {
            db.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
        db
    };

    let r = env.read_txn().unwrap();
    let got = collect(&db, &r);
    let mut expected = ks.clone();
    expected.sort_by(|a, b| match a.last().cmp(&b.last()) {
        Ordering::Equal => a.cmp(b),
        o => o,
    });
    assert_eq!(got, expected, "iteration follows the by-last-byte order");
    // Every key still resolves.
    for k in &ks {
        assert!(db.get(&r, k).unwrap().is_some(), "missing {k:?}");
    }
    drop(r);
    assert_clean(dir.path());
}

#[test]
fn comparator_survives_reopen_when_the_same_one_is_supplied() {
    // The supported lifecycle: pass the same comparator on every open. This is
    // the *only* way to use the feature correctly, since nothing is persisted.
    let dir = TempDir::new();
    let ks = keys(300);

    {
        let env = open(dir.path());
        let mut w = env.write_txn().unwrap();
        let db = env
            .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
            .unwrap();
        for k in &ks {
            db.put(&mut w, k, b"v").unwrap();
        }
        w.commit().unwrap();
    }

    let env = open(dir.path());
    let r = env.read_txn().unwrap();
    let db = env
        .open_database_with_comparator(&r, Some(b"rev"), reverse())
        .unwrap()
        .expect("the database exists");
    let got = collect(&db, &r);
    let mut expected = ks.clone();
    expected.sort();
    expected.reverse();
    assert_eq!(got, expected);
    for k in &ks {
        assert!(
            db.get(&r, k).unwrap().is_some(),
            "missing after reopen: {k:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. The refusals
// ---------------------------------------------------------------------------

#[test]
fn the_main_database_refuses_a_comparator() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let mut w = env.write_txn().unwrap();
    let err = env
        .create_database_with_comparator(&mut w, None, reverse())
        .expect_err("the main DB is the named-DB catalog and must stay memcmp");
    match err {
        zerodb::Error::Io(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                e.to_string().contains("catalog"),
                "the error should say why, got: {e}"
            );
        }
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
}

#[test]
fn a_conflicting_in_process_registration_is_refused_and_the_first_one_stands() {
    let dir = TempDir::new();
    let env = open(dir.path());

    let mut w = env.write_txn().unwrap();
    let db = env
        .create_database_with_comparator(&mut w, Some(b"d"), reverse())
        .unwrap();
    db.put(&mut w, b"a", b"v").unwrap();
    db.put(&mut w, b"b", b"v").unwrap();
    w.commit().unwrap();

    // A second module in the same process disagreeing about this DB.
    let r = env.read_txn().unwrap();
    let err = env
        .open_database_with_comparator(&r, Some(b"d"), by_last_byte())
        .expect_err("a different ordering for the same DB must be refused");
    match err {
        zerodb::Error::Io(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
            let msg = e.to_string();
            assert!(
                msg.contains("test.reverse.v1"),
                "names the registered one: {msg}"
            );
            assert!(
                msg.contains("test.by-last-byte.v1"),
                "names the rejected one: {msg}"
            );
        }
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    // The refusal must not have swapped the ordering in.
    let db2 = env
        .open_database_with_comparator(&r, Some(b"d"), reverse())
        .unwrap()
        .unwrap();
    assert_eq!(
        collect(&db2, &r),
        vec![b"b".to_vec(), b"a".to_vec()],
        "the original registration survives a rejected conflicting one"
    );
}

#[test]
fn re_registering_the_identical_comparator_is_idempotent() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let mut w = env.write_txn().unwrap();
    env.create_database_with_comparator(&mut w, Some(b"d"), reverse())
        .unwrap();
    w.commit().unwrap();

    let r = env.read_txn().unwrap();
    env.open_database_with_comparator(&r, Some(b"d"), reverse())
        .expect("re-opening with the same ordering is the normal lifecycle");
}

#[test]
fn compacting_copy_refuses_an_env_with_a_custom_comparator() {
    // Documented scope boundary (SPEC 03 §2.0): the bulk builder and the
    // dump/load format are memcmp-defined end to end, so compaction must
    // refuse rather than silently rebuild the tree in byte order.
    let dir = TempDir::new();
    let env = open(dir.path());
    let mut w = env.write_txn().unwrap();
    let db = env
        .create_database_with_comparator(&mut w, Some(b"rev"), reverse())
        .unwrap();
    for k in keys(50) {
        db.put(&mut w, &k, b"v").unwrap();
    }
    w.commit().unwrap();

    let out = dir.path().join("compact.dat");
    let err = env
        .copy_to_file(&out, CompactionOption::Enabled)
        .expect_err("compaction is memcmp-only in 2.4");
    match err {
        zerodb::Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    assert!(!out.exists(), "the refusal writes nothing");

    // The raw copy is byte-level and stays supported.
    let raw = dir.path().join("raw.dat");
    env.copy_to_file(&raw, CompactionOption::Disabled)
        .expect("a raw page copy preserves whatever order is on disk");
    assert!(raw.exists());
}

// ---------------------------------------------------------------------------
// Isolation from the engine-internal trees
// ---------------------------------------------------------------------------

#[test]
fn gc_and_catalog_stay_memcmp_under_heavy_custom_comparator_churn() {
    // The GC tree is keyed by big-endian txnids whose memcmp order IS their
    // numeric order, and the catalog is keyed by DB names. Neither may pick up
    // the caller's ordering. Drive enough churn to allocate, free and reclaim
    // pages, then check the image: a comparator leaking into the GC tree shows
    // up as an INV-26 (GC ids not ascending) or a reachability failure, and one
    // leaking into the catalog shows up as the named DBs going missing.
    let dir = TempDir::new();
    let env = open(dir.path());

    let names: Vec<Vec<u8>> = (0..6).map(|i| format!("db{i}").into_bytes()).collect();
    {
        let mut w = env.write_txn().unwrap();
        for (i, n) in names.iter().enumerate() {
            if i % 2 == 0 {
                env.create_database_with_comparator(&mut w, Some(n), reverse())
                    .unwrap();
            } else {
                env.create_database(&mut w, Some(n)).unwrap();
            }
        }
        w.commit().unwrap();
    }

    for round in 0..6 {
        let mut w = env.write_txn().unwrap();
        for n in &names {
            let db = env.create_database(&mut w, Some(n)).unwrap();
            for i in 0..150 {
                let k = format!("k{:04}-{round}", i);
                db.put(&mut w, k.as_bytes(), &[b'x'; 64]).unwrap();
            }
            if round % 2 == 1 {
                for i in 0..120 {
                    let k = format!("k{:04}-{}", i, round - 1);
                    db.delete(&mut w, k.as_bytes()).unwrap();
                }
            }
        }
        w.commit().unwrap();
    }

    // The catalog still resolves every name, in both flavors.
    let r = env.read_txn().unwrap();
    let listed = zerodb::named_databases(&r).unwrap();
    assert_eq!(listed.len(), names.len(), "the catalog is intact");
    let mut sorted_names = names.clone();
    sorted_names.sort();
    assert_eq!(
        listed, sorted_names,
        "catalog enumeration stays in memcmp name order regardless of any \
         comparator registered on the databases it points to"
    );
    drop(r);

    assert_clean(dir.path());
}
