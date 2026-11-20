//! The adapter's `unsafe` surface under miri, through the public heed API.
//!
//! Miri cannot see an OS mapping, so these tests open the engine on the
//! heap-backed writable map (`zerodb_io::testmap::TestWriteMap`, ADR-0021),
//! where miri checks every access, and wrap it in a heed [`Env`].
//!
//! Each test runs twice: with dirty pages written in place in the map
//! (`WRITE_MAP`) and with dirty pages in heap frames (the default). In heap
//! mode `TestWriteMap` imitates the commit's `pwrite` with a pointer store
//! into pages no borrow covers, since every view of the map is bounded to
//! the pages it reads (ADR-0021 amendment 1). See
//! `docs/maintainers/UNSAFE-AUDIT.md`.
//!
//! What they reach, by `unsafe` site:
//!
//! - the `RoTxn` TLS-marker retags (`txn.rs`, the two `Deref` impls), on
//!   every read through a `RoTxn<WithTls>` or `RoTxn<WithoutTls>`;
//! - the `Send` impl of `RoTxn<WithoutTls>` (`txn.rs`), by reading through a
//!   read transaction on another thread;
//! - the `ReservedSpace` uninitialized view (`reserved_space.rs`) and
//!   `assume_written`, through `Database::put_reserved`;
//! - the lifetime-erased write cursor (`iterator.rs`) and its public
//!   `unsafe fn`s `put_current` and `del_current`, through
//!   `Database::iter_mut`. `iterator.rs` also drives the cursor's internals
//!   directly.
//!
//! Run: `cargo +nightly miri test -p heed --lib`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb_core::env::{open_with_backing_policy, DurabilityFlags};
use zerodb_core::page::FileTrust;
use zerodb_io::testmap::TestWriteMap;

use crate::types::Bytes;
use crate::{Database, Env, TlsUsage, WithTls, WithoutTls};

const PS: u32 = 4096;
const MAP: usize = 2 << 20;

/// The `write_map` modes a test runs in; see the module docs.
const MODES: &[bool] = &[true, false];

/// A heed env over a fresh heap-backed map; `write_map` selects in-place
/// dirty pages.
fn heap_env<T: TlsUsage>(write_map: bool) -> Env<T> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // Relaxed: only uniqueness of the value matters.
    let seq = NEXT.fetch_add(1, Ordering::Relaxed);
    let inner = open_with_backing_policy(
        PathBuf::from(format!("/virtual/heed-miri-{}-{seq}", std::process::id())),
        Box::new(TestWriteMap::fresh_env(PS, MAP)),
        PS,
        false,
        4,
        8,
        DurabilityFlags {
            write_map,
            ..DurabilityFlags::default()
        },
        FileTrust::VALIDATE,
        false,
        None,
    )
    .expect("open heap-backed env");
    Env::from_zdb_for_tests(inner)
}

fn main_db<T>(env: &Env<T>) -> Database<Bytes, Bytes> {
    let mut wtxn = env.write_txn().expect("write txn");
    let db = env
        .create_database::<Bytes, Bytes>(&mut wtxn, None)
        .expect("main database");
    wtxn.commit().expect("commit");
    db
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

/// Values of every size class: inline, near the node limit, and overflow.
fn value(i: u32) -> Vec<u8> {
    let len = match i % 5 {
        0 => 5_000,
        1 => 1_200,
        _ => 16 + i as usize % 40,
    };
    vec![(i % 251) as u8; len]
}

fn fill<T>(env: &Env<T>, db: Database<Bytes, Bytes>, n: u32) {
    let mut wtxn = env.write_txn().expect("write txn");
    for i in 0..n {
        db.put(&mut wtxn, &key(i), &value(i)).expect("put");
    }
    wtxn.commit().expect("commit");
}

/// Reads through `RoTxn<WithTls>` and `RoTxn<WithoutTls>` go through the
/// marker retag to `RoTxn<AnyTls>`; the borrowed values point into the map.
#[test]
fn tls_retag_reads_both_markers() {
    for &write_map in MODES {
        let env = heap_env::<WithTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 60);
        let rtxn = env.read_txn().expect("read txn");
        for i in (0..60).step_by(7) {
            assert_eq!(db.get(&rtxn, &key(i)).expect("get"), Some(&value(i)[..]));
        }
        assert_eq!(db.iter(&rtxn).expect("iter").count(), 60);
        drop(rtxn);

        let env = heap_env::<WithoutTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 60);
        let rtxn = env.read_txn().expect("read txn");
        let last = db.last(&rtxn).expect("last").expect("non-empty");
        assert_eq!(last.0, &key(59)[..]);
    }
}

/// A `RoTxn<WithoutTls>` is `Send`: one is moved to another thread and read
/// there, then the writer commits once the reader is done.
#[test]
fn without_tls_read_txn_reads_on_another_thread() {
    for &write_map in MODES {
        let env = heap_env::<WithoutTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 40);
        let rtxn = env.clone().static_read_txn().expect("static read txn");
        let reader = std::thread::spawn(move || {
            for i in 0..40 {
                let v = db.get(&rtxn, &key(i)).expect("get").expect("present");
                assert_eq!(v, &value(i)[..]);
            }
            assert_eq!(db.len(&rtxn).expect("len"), 40);
            // Dropped here, on the reader's thread.
        });
        reader.join().expect("reader thread");
        let mut wtxn = env.write_txn().expect("write txn");
        db.delete(&mut wtxn, &key(0)).expect("delete");
        wtxn.commit().expect("commit");
        let rtxn = env.read_txn().expect("read txn");
        assert_eq!(db.len(&rtxn).expect("len"), 39);
    }
}

/// As above, but the writer commits while the other thread reads. The
/// reader must see its own snapshot throughout.
///
/// The writer stores into free pages below the reader's high-water, in place
/// under `WRITE_MAP` and through the imitated `pwrite` otherwise. The
/// reader's borrows cover only the pages it reads (ADR-0021 amendment 1);
/// before that, miri reported this test as UB under both aliasing models
/// (`docs/maintainers/UNSAFE-AUDIT.md`, F1).
#[test]
fn without_tls_read_txn_reads_on_another_thread_during_commits() {
    for &write_map in MODES {
        let env = heap_env::<WithoutTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 40);
        let rtxn = env.clone().static_read_txn().expect("static read txn");
        let reader = std::thread::spawn(move || {
            let mut total = 0usize;
            for _ in 0..3 {
                for i in 0..40 {
                    let v = db.get(&rtxn, &key(i)).expect("get").expect("present");
                    assert_eq!(v, &value(i)[..], "snapshot changed under the reader");
                    total += v.len();
                }
            }
            assert_eq!(db.len(&rtxn).expect("len"), 40);
            total
        });
        for round in 0..3u32 {
            let mut wtxn = env.write_txn().expect("write txn");
            for i in 0..40 {
                db.put(&mut wtxn, &key(i), format!("round-{round}").as_bytes())
                    .expect("put");
            }
            db.delete(&mut wtxn, &key(round)).expect("delete");
            wtxn.commit().expect("commit");
        }
        assert!(reader.join().expect("reader thread") > 0);
        // Each round rewrites every key, so only the last round's delete
        // remains.
        let rtxn = env.read_txn().expect("read txn");
        assert_eq!(db.len(&rtxn).expect("len"), 39);
    }
}

/// `put_reserved` hands the closure a `ReservedSpace` over the engine's
/// slot. The uninitialized view and `assume_written` are exercised, and the
/// unwritten tail must read back as zeroes even where the slot held stale
/// bytes of the copied page.
#[test]
fn put_reserved_views_and_zero_tail() {
    for &write_map in MODES {
        let env = heap_env::<WithTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 30);
        let mut wtxn = env.write_txn().expect("write txn");
        // Fully written through the uninitialized view.
        db.put_reserved(&mut wtxn, b"reserved-uninit", 64, |space| {
            for (i, b) in space.as_uninit_mut().iter_mut().enumerate() {
                b.write(i as u8);
            }
            // SAFETY: every byte was written through the view above.
            unsafe { space.assume_written(64) };
            Ok(())
        })
        .expect("reserved put");
        // Partly written (an overflow-sized slot): `UnexpectedEof`, and the
        // tail is zeroed.
        let err = db
            .put_reserved(&mut wtxn, &key(0), 6_000, |space| {
                std::io::Write::write_all(space, b"head")
            })
            .expect_err("a short write is an error");
        assert!(
            matches!(err, crate::Error::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        // Written, then rewritten through `written_mut`.
        db.put_reserved(&mut wtxn, b"reserved-rewrite", 16, |space| {
            std::io::Write::write_all(space, &[7u8; 16])?;
            space.written_mut()[..4].copy_from_slice(b"abcd");
            Ok(())
        })
        .expect("reserved put");
        wtxn.commit().expect("commit");

        let rtxn = env.read_txn().expect("read txn");
        let v = db
            .get(&rtxn, b"reserved-uninit")
            .expect("get")
            .expect("present");
        assert!(v.iter().enumerate().all(|(i, &b)| b == i as u8));
        let v = db.get(&rtxn, &key(0)).expect("get").expect("present");
        assert_eq!(v.len(), 6_000);
        assert_eq!(&v[..4], b"head");
        assert!(v[4..].iter().all(|&b| b == 0), "unwritten tail is not zero");
        let v = db
            .get(&rtxn, b"reserved-rewrite")
            .expect("get")
            .expect("present");
        assert_eq!(&v[..4], b"abcd");
        assert!(v[4..].iter().all(|&b| b == 7));
    }
}

/// The public `unsafe fn`s of the write iterator, used as their contract
/// says: no borrow from the iterator is held across a call.
#[test]
fn rw_iterator_put_and_del_current() {
    for &write_map in MODES {
        let env = heap_env::<WithTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 50);
        let mut wtxn = env.write_txn().expect("write txn");
        let mut iter = db.iter_mut(&mut wtxn).expect("iter_mut");
        let mut deleted = 0;
        while let Some(entry) = iter.next() {
            let (k, _v) = entry.expect("entry");
            let i: u32 = std::str::from_utf8(&k[4..]).unwrap().parse().unwrap();
            let k = k.to_vec();
            if i.is_multiple_of(3) {
                // SAFETY: `k` is an owned copy; no borrow from the iterator
                // is live across the call.
                assert!(unsafe { iter.del_current() }.expect("del_current"));
                deleted += 1;
            } else if i % 3 == 1 {
                // SAFETY: as above; the new value is owned by this frame.
                unsafe { iter.put_current(&k, b"rewritten") }.expect("put_current");
            }
        }
        drop(iter);
        wtxn.commit().expect("commit");

        let rtxn = env.read_txn().expect("read txn");
        assert_eq!(db.len(&rtxn).expect("len"), 50 - deleted);
        for i in 0..50 {
            let got = db.get(&rtxn, &key(i)).expect("get");
            match i % 3 {
                0 => assert_eq!(got, None),
                1 => assert_eq!(got, Some(&b"rewritten"[..])),
                _ => assert_eq!(got, Some(&value(i)[..])),
            }
        }
    }
}

/// A nested read transaction reads the parent's uncommitted pages, which
/// live in the map in `WRITE_MAP` mode.
#[test]
fn nested_read_txn_sees_uncommitted_pages() {
    for &write_map in MODES {
        let env = heap_env::<WithTls>(write_map);
        let db = main_db(&env);
        fill(&env, db, 20);
        let mut wtxn = env.write_txn().expect("write txn");
        for i in 20..40 {
            db.put(&mut wtxn, &key(i), &value(i)).expect("put");
        }
        let a = env.nested_read_txn(&wtxn).expect("nested");
        let b = wtxn.nested_read_txn().expect("nested");
        for i in 0..40 {
            assert_eq!(db.get(&a, &key(i)).expect("get"), Some(&value(i)[..]));
        }
        assert_eq!(db.len(&b).expect("len"), 40);
        drop((a, b));
        db.delete(&mut wtxn, &key(25)).expect("delete");
        wtxn.commit().expect("commit");
        let rtxn = env.read_txn().expect("read txn");
        assert_eq!(db.len(&rtxn).expect("len"), 39);
    }
}
