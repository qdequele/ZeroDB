//! `check` and databases written under a custom key comparator.
//!
//! A file records no comparator, so the default walk evaluates key order
//! (INV-5, INV-6) in byte order and reports a healthy custom-comparator
//! database for exactly those two invariants (SPEC 03 §2.0). A caller that
//! knows the ordering passes it to `check_image_with_key_order` /
//! `check_snapshot_with_key_order`, and the same database is then clean.

mod common;
use common::TempDir;

use std::cmp::Ordering;

use zerodb::check::{self, KeyOrder};
use zerodb::{Comparator, EnvOpenOptions, FnComparator};

const PS: u32 = 4096;

fn reverse() -> FnComparator<impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static> {
    FnComparator::new("test.reverse.v1", |a: &[u8], b: &[u8]| b.cmp(a))
}

fn by_last_byte() -> FnComparator<impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static> {
    FnComparator::new("test.by-last-byte.v1", |a: &[u8], b: &[u8]| {
        match a.last().cmp(&b.last()) {
            Ordering::Equal => a.cmp(b),
            o => o,
        }
    })
}

/// The data file of an environment holding a byte-ordered database `plain`
/// and two custom-comparator ones, `rev` and `lastbyte`, each deep enough to
/// have branch pages, after inserts and deletes.
fn image() -> Vec<u8> {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(64 << 20);
    opts.page_size(PS);
    opts.max_dbs(8);
    // SAFETY: only ZeroDB writes this environment's files.
    let env = unsafe { opts.open(dir.path()) }.expect("open env");
    let keys: Vec<Vec<u8>> = (0..3000u32)
        .map(|i| format!("key{:05}", i.wrapping_mul(7919) % 100_000).into_bytes())
        .collect();
    let mut w = env.write_txn().unwrap();
    let plain = env.create_database(&mut w, Some(b"plain")).unwrap();
    let rev = env
        .create_database_with_comparator(&mut w, Some(b"rev"), Box::new(reverse()))
        .unwrap();
    let last = env
        .create_database_with_comparator(&mut w, Some(b"lastbyte"), Box::new(by_last_byte()))
        .unwrap();
    for db in [&plain, &rev, &last] {
        for k in &keys {
            db.put(&mut w, k, &[b'v'; 24]).unwrap();
        }
    }
    w.commit().unwrap();
    let mut w = env.write_txn().unwrap();
    for db in [&plain, &rev, &last] {
        for k in keys.iter().step_by(3) {
            db.delete(&mut w, k).unwrap();
        }
    }
    w.commit().unwrap();
    let r = env.read_txn().unwrap();
    for db in [&plain, &rev, &last] {
        assert!(
            db.stat(&r).unwrap().depth >= 2,
            "each tree has branch pages"
        );
    }
    drop(r);
    drop(env);
    std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).expect("read data file")
}

fn only_key_order(v: &[String]) -> bool {
    v.iter()
        .all(|s| s.starts_with("INV-5") || s.starts_with("INV-6"))
}

/// The default walk is byte-ordered: the healthy custom-comparator databases
/// are reported, for INV-5 and INV-6 only.
#[test]
fn default_walk_reports_custom_comparator_databases_for_key_order_only() {
    let img = image();
    let v = check::check_image(&img, PS);
    assert!(!v.is_empty(), "byte order does not hold in `rev`");
    assert!(only_key_order(&v), "{v:#?}");
}

/// With each database's comparator supplied, the same image is clean, and
/// the key-order checks ran under those comparators.
#[test]
fn supplied_comparators_make_a_healthy_image_clean() {
    let img = image();
    let (rev, last) = (reverse(), by_last_byte());
    let order_of = |name: &[u8]| match name {
        b"rev" => KeyOrder::Custom(&rev as &dyn Comparator),
        b"lastbyte" => KeyOrder::Custom(&last),
        _ => KeyOrder::Bytewise,
    };
    assert_eq!(
        check::check_image_with_key_order(&img, PS, &order_of),
        Vec::<String>::new()
    );

    // The comparators are really applied: swapped, both databases fail.
    let swapped = |name: &[u8]| match name {
        b"rev" => KeyOrder::Custom(&last as &dyn Comparator),
        b"lastbyte" => KeyOrder::Custom(&rev),
        _ => KeyOrder::Bytewise,
    };
    let v = check::check_image_with_key_order(&img, PS, &swapped);
    assert!(!v.is_empty());
    assert!(only_key_order(&v), "{v:#?}");

    // A byte-ordered database checked under another order fails too.
    let plain_reversed = |name: &[u8]| match name {
        b"plain" => KeyOrder::Custom(&rev as &dyn Comparator),
        _ => KeyOrder::Unknown,
    };
    let v = check::check_image_with_key_order(&img, PS, &plain_reversed);
    assert!(!v.is_empty());
    assert!(only_key_order(&v), "{v:#?}");
}

/// A caller without the comparators marks those databases' order unknown:
/// no false violation, and the byte-ordered database stays strictly checked.
#[test]
fn unknown_order_reports_no_false_violation() {
    let img = image();
    let order_of = |name: &[u8]| match name {
        b"plain" => KeyOrder::Bytewise,
        _ => KeyOrder::Unknown,
    };
    assert_eq!(
        check::check_image_with_key_order(&img, PS, &order_of),
        Vec::<String>::new()
    );
}

/// The snapshot walk of a live environment takes the same key orders.
#[test]
fn snapshot_walk_takes_the_key_orders() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(16 << 20);
    opts.page_size(PS);
    opts.max_dbs(4);
    // SAFETY: only ZeroDB writes this environment's files.
    let env = unsafe { opts.open(dir.path()) }.expect("open env");
    let mut w = env.write_txn().unwrap();
    let db = env
        .create_database_with_comparator(&mut w, Some(b"rev"), Box::new(reverse()))
        .unwrap();
    for i in 0..1000u32 {
        db.put(&mut w, format!("key{i:05}").as_bytes(), b"v")
            .unwrap();
    }
    w.commit().unwrap();

    let r = env.read_txn().unwrap();
    let rev = reverse();
    let v = check::check_snapshot(r.map_bytes(), PS, r.snapshot());
    assert!(!v.is_empty() && only_key_order(&v), "{v:#?}");
    let order_of = |_: &[u8]| KeyOrder::Custom(&rev as &dyn Comparator);
    assert_eq!(
        check::check_snapshot_with_key_order(r.map_bytes(), PS, r.snapshot(), &order_of),
        Vec::<String>::new()
    );
}
