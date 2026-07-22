//! Error-precedence sweep for the M2.9 dbi-handle lifetime work (ADR-0013,
//! SPEC 04 TXN-68) across the FULL adapter read surface.
//!
//! `crates/zerodb-oracle/tests/dbi_handle_lifetime.rs` already pins that
//! `get`/`del`/neighbor-seeks/`iter`/`iter_mut` prefer `BadDbi` over any other
//! error for a stale handle. What is NOT yet pinned anywhere:
//!
//! 1. The **empty-key** precedence combined with a **stale handle** — the
//!    adapter's `check_read_key` (SPEC 03 §2.1, the "empty key → `BadValSize`
//!    on get/del/neighbor-seeks/forward-`prefix_iter`" shim) runs strictly
//!    AFTER `validate` in every read method's source (see the
//!    `// TXN-68 before §2.1` comments in `heed-zerodb/src/database.rs`), so a
//!    stale handle queried with an empty key must yield `BadDbi`, never
//!    `BadValSize` — for every op class that has both checks, not just the
//!    ones already exercised with non-empty keys.
//! 2. The four range/prefix **constructors** (`range`, `rev_range`,
//!    `prefix_iter`, `rev_prefix_iter`) validate EAGERLY at construction
//!    (`Result`-returning, unlike the native core's infallible-by-signature
//!    iterator constructors which defer to the first `next()` — SPEC 04
//!    TXN-68 note). Only `iter`/`rev_iter`/`iter_mut` open-time validation is
//!    pinned today (`dbi_handle_lifetime.rs` `adapter_boundary`); this file
//!    extends that to all four range/prefix constructors.
//! 3. The documented `rev_prefix_iter` asymmetry (empty prefix is NOT
//!    rejected, unlike forward `prefix_iter`) still holds for a LIVE handle,
//!    so a regression that starts rejecting it — or that stops rejecting the
//!    forward direction — would be caught here too.

use heed_zerodb::types::Bytes;
use heed_zerodb::{Database, Env, EnvOpenOptions, Error, MdbError, WithoutTls};

type Db = Database<Bytes, Bytes>;

fn env_opts() -> EnvOpenOptions<WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

fn open_env(dir: &std::path::Path) -> Env<WithoutTls> {
    let mut opts = env_opts();
    opts.map_size(1 << 20).max_dbs(16);
    // SAFETY: heed's `EnvOpenOptions::open` contract — the path is a fresh
    // private tempdir, so no other env maps it in this process.
    unsafe { opts.open(dir) }.expect("open env")
}

#[track_caller]
fn assert_bad_dbi<T: std::fmt::Debug>(label: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Io(io)) => {
            assert_eq!(
                io.raw_os_error(),
                Some(libc::EINVAL),
                "{label}: expected the BadDbi->EINVAL mapping, got {io:?}"
            );
        }
        other => panic!("{label}: expected Io(EINVAL) (BadDbi), got {other:?}"),
    }
}

#[track_caller]
fn assert_bad_val_size<T: std::fmt::Debug>(label: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Mdb(MdbError::BadValSize)) => {}
        other => panic!("{label}: expected Mdb(BadValSize), got {other:?}"),
    }
}

/// Build an env with one LIVE named db (`"live"`, containing one entry) and
/// one STALE handle (`"dead"`, created then aborted) — a single fixture
/// shared by every sweep below.
struct Fixture {
    dir: tempfile::TempDir,
    env: Env<WithoutTls>,
    live: Db,
    dead: Db,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let live: Db = env.create_database(&mut wtxn, Some("live")).unwrap();
    live.put(&mut wtxn, b"k".as_slice(), b"v".as_slice())
        .unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let dead: Db = env.create_database(&mut wtxn, Some("dead")).unwrap();
    wtxn.abort(); // TXN-68 event 1: the handle is now stale.

    Fixture {
        dir,
        env,
        live,
        dead,
    }
}

// ---------------------------------------------------------------------------
// 1. Empty key + stale handle: BadDbi wins, never BadValSize.
// ---------------------------------------------------------------------------

#[test]
fn stale_handle_beats_empty_key_on_every_neighbor_seek_and_get() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();
    let empty: &[u8] = b"";

    assert_bad_dbi("get", f.dead.get(&rtxn, empty));
    assert_bad_dbi("get_lower_than", f.dead.get_lower_than(&rtxn, empty));
    assert_bad_dbi(
        "get_lower_than_or_equal_to",
        f.dead.get_lower_than_or_equal_to(&rtxn, empty),
    );
    assert_bad_dbi("get_greater_than", f.dead.get_greater_than(&rtxn, empty));
    assert_bad_dbi(
        "get_greater_than_or_equal_to",
        f.dead.get_greater_than_or_equal_to(&rtxn, empty),
    );
    assert_bad_dbi(
        "prefix_iter (forward, has a check_read_key)",
        f.dead.prefix_iter(&rtxn, empty).map(|_| ()),
    );
    drop(rtxn);

    // Delete, through a write txn, has the identical precedence.
    let mut wtxn = f.env.write_txn().unwrap();
    assert_bad_dbi("delete", f.dead.delete(&mut wtxn, empty));
    wtxn.commit().expect("BadDbi must not poison the txn");

    let _ = f.dir; // keep the tempdir alive for the whole test
}

/// Control: the SAME empty-key ops against the LIVE handle land on
/// `BadValSize` instead — proving the sweep above is really exercising the
/// precedence order, not merely "empty key always errors somehow".
#[test]
fn empty_key_alone_on_a_live_handle_is_bad_val_size() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();
    let empty: &[u8] = b"";

    assert_bad_val_size("live get", f.live.get(&rtxn, empty));
    assert_bad_val_size("live get_lower_than", f.live.get_lower_than(&rtxn, empty));
    assert_bad_val_size(
        "live get_greater_than_or_equal_to",
        f.live.get_greater_than_or_equal_to(&rtxn, empty),
    );
    assert_bad_val_size(
        "live prefix_iter (forward)",
        f.live.prefix_iter(&rtxn, empty).map(|_| ()),
    );
}

// ---------------------------------------------------------------------------
// 2. All four range/prefix constructors validate EAGERLY (error-at-open),
//    not deferred to the first `next()`.
// ---------------------------------------------------------------------------

#[test]
fn all_four_range_and_prefix_constructors_error_at_open_for_a_stale_handle() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();

    assert_bad_dbi("range constructor", f.dead.range(&rtxn, &(..)).map(|_| ()));
    assert_bad_dbi(
        "rev_range constructor",
        f.dead.rev_range(&rtxn, &(..)).map(|_| ()),
    );
    assert_bad_dbi(
        "prefix_iter constructor",
        f.dead.prefix_iter(&rtxn, b"p".as_slice()).map(|_| ()),
    );
    assert_bad_dbi(
        "rev_prefix_iter constructor",
        f.dead.rev_prefix_iter(&rtxn, b"p".as_slice()).map(|_| ()),
    );
}

/// Every constructor above returns `Result`, so the failure must be visible
/// from the `?`/`match` on the constructor call itself — proving there is no
/// live `RoRange`/`RoPrefix` iterator object to (mis-)drive at all. This
/// distinguishes the adapter's eager validation from the native core's
/// infallible-by-signature constructors, which defer to the first `next()`
/// (SPEC 04 TXN-68 note; native coverage already exists in
/// `dbi_handle_lifetime_native.rs`).
#[test]
fn range_constructor_failure_prevents_any_iterator_from_existing() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();
    match f.dead.range(&rtxn, &(..)) {
        Err(_) => {} // correct: no RoRange value was ever constructed
        Ok(_) => panic!("range() must fail at construction for a stale handle"),
    }
}

// ---------------------------------------------------------------------------
// 3. The rev_prefix_iter empty-prefix asymmetry, pinned against a LIVE
//    handle (a regression here would silently change SPEC 03 §2.1 behavior).
// ---------------------------------------------------------------------------

#[test]
fn rev_prefix_iter_empty_prefix_is_not_rejected_on_a_live_handle() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();
    let empty: &[u8] = b"";

    // Forward prefix_iter DOES reject the empty prefix (BadValSize).
    assert_bad_val_size(
        "live forward prefix_iter(empty)",
        f.live.prefix_iter(&rtxn, empty).map(|_| ()),
    );

    // rev_prefix_iter does NOT — construction succeeds and yields the whole
    // database in reverse order (documented asymmetry, SPEC 03 §2.1 note in
    // `heed-zerodb/src/database.rs::check_read_key`'s doc comment).
    let mut it = f
        .live
        .rev_prefix_iter(&rtxn, empty)
        .expect("rev_prefix_iter(empty) must NOT error on a live handle");
    let (k, v) = it
        .next()
        .expect("iterator must yield")
        .expect("no per-item error");
    assert_eq!(k, b"k".as_slice());
    assert_eq!(v, b"v".as_slice());
    assert!(it.next().is_none());
}

/// And for the STALE handle, `BadDbi` still wins over the "empty prefix is
/// fine" leniency — the dbi gate applies before ANY key-size reasoning is
/// even reached, empty-prefix-tolerant path included.
#[test]
fn rev_prefix_iter_stale_handle_beats_the_empty_prefix_leniency() {
    let f = fixture();
    let rtxn = f.env.read_txn().unwrap();
    let empty: &[u8] = b"";
    assert_bad_dbi(
        "stale rev_prefix_iter(empty)",
        f.dead.rev_prefix_iter(&rtxn, empty).map(|_| ()),
    );
}
