//! Where a write cursor is left after `put_current`, observed against the fork.
//!
//! SPEC 03 §7: a `put_current` leaves the cursor on the rewritten entry, so the
//! next `next` yields its successor and the next `prev` its predecessor. ZeroDB
//! keeps the cursor's root-to-leaf path across the put when the rewrite stays
//! inside its leaf (SPEC 03 §5.4b) instead of searching for the key again; these
//! tests pin the resulting *position* — through same-size, shrinking and
//! growing rewrites, the last of which split leaves mid-walk — so that change of
//! mechanism cannot change what the cursor yields. Do not weaken them to let an
//! optimization pass.
//!
//! Each case records an event trace through the **heed surface consumers use**
//! (`iter_mut` / `rev_iter_mut` / `range_mut`, the shapes milli and hannoy
//! rewrite entries through) and asserts the two engines produce the same trace
//! *and* the same final content.

use zerodb_oracle::tempdir::TempDir;

/// Entries per tree: enough to span many leaves at any supported page size.
const N: usize = 2_000;
/// Seed value width — with the 8-byte keys this puts ~70 entries on a 16 K page.
const VAL: usize = 200;

fn keys() -> Vec<Vec<u8>> {
    (0..N as u64).map(|i| i.to_be_bytes().to_vec()).collect()
}

/// The replacement width for the `i`-th rewrite under each shape.
fn width(shape: u8, i: usize) -> usize {
    match shape {
        // Same size: every rewrite stays where it sits.
        0 => VAL,
        // Shrink, same size and grow in turn: the growing ones split leaves.
        1 => [VAL / 2, VAL, VAL * 3][i % 3],
        // Always grow: a split every few entries.
        _ => VAL * 4,
    }
}

/// Generate one probe module per engine from a single body, so neither engine
/// can be given a different sequence of calls.
macro_rules! probes {
    ($m:ident, $heed:ident, $setpage:tt) => {
        mod $m {
            #[allow(unused_imports)] // only the `set` arm needs the page size
            use super::{width, N, VAL};
            use std::ops::Bound;
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvOpenOptions, WithoutTls};

            type Db = Database<Bytes, Bytes>;

            fn open(dir: &Path) -> (Env<WithoutTls>, Db) {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(256 << 20);
                o.max_dbs(4);
                probes!(@setpage o, $setpage);
                // SAFETY: single-process, single-threaded, private temp dir; no
                // flags are set at all.
                let env = unsafe { o.open(dir) }.expect("open env");
                let mut w = env.write_txn().expect("write_txn");
                let db = env
                    .create_database::<Bytes, Bytes>(&mut w, Some("t"))
                    .expect("create_database");
                w.commit().expect("commit");
                (env, db)
            }

            fn fill(env: &Env<WithoutTls>, db: Db, keys: &[Vec<u8>]) {
                let v = vec![0xABu8; VAL];
                let mut w = env.write_txn().expect("write_txn");
                for k in keys {
                    db.put(&mut w, k.as_slice(), &v).expect("put");
                }
                w.commit().expect("commit");
            }

            /// Every `(key, value length, first value byte)`, read in a fresh txn.
            fn content(env: &Env<WithoutTls>, db: Db) -> Vec<(u64, usize, u8)> {
                let r = env.read_txn().expect("read_txn");
                db.iter(&r)
                    .expect("iter")
                    .map(|kv| {
                        let (k, v) = kv.expect("entry");
                        (key_of(k), v.len(), v.first().copied().unwrap_or(0))
                    })
                    .collect()
            }

            fn key_of(kv: &[u8]) -> u64 {
                u64::from_be_bytes(kv.try_into().expect("8-byte key"))
            }

            /// Advance to entry `nth`, `put_current` a value of `len` bytes,
            /// then report what the next `next()` yields.
            pub fn put_then_next(dir: &Path, keys: &[Vec<u8>], nth: usize, len: usize) -> (Vec<String>, Vec<(u64, usize, u8)>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut trace = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    let mut key = None;
                    for i in 0..=nth {
                        match it.next() {
                            Some(Ok((k, _))) => {
                                if i == nth {
                                    trace.push(format!("at {}", key_of(k)));
                                    key = Some(k.to_vec());
                                }
                            }
                            Some(Err(e)) => panic!("iter: {e}"),
                            None => break,
                        }
                    }
                    let key = key.expect("tree holds nth");
                    let v = vec![0x11u8; len];
                    // SAFETY: the key was copied out; no borrow is live.
                    let ok = unsafe { it.put_current(&key, &v) }.expect("put_current");
                    trace.push(format!("put -> {ok}"));
                    match it.next() {
                        Some(Ok((k, v))) => trace.push(format!("next -> {} ({} bytes)", key_of(k), v.len())),
                        Some(Err(e)) => panic!("next: {e}"),
                        None => trace.push("next -> None".into()),
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (trace, c)
            }

            /// Rewrite every entry through the cursor, forward (`iter_mut`) or
            /// backward (`rev_iter_mut`), with the widths of `shape`: the
            /// read-modify-write loop, recording the visit order.
            pub fn rewrite_all(dir: &Path, keys: &[Vec<u8>], shape: u8, backward: bool) -> (Vec<String>, Vec<(u64, usize, u8)>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut seen = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut fwd;
                    let mut rev;
                    let it: &mut dyn RwWalk = if backward {
                        rev = db.rev_iter_mut(&mut w).expect("rev_iter_mut");
                        &mut rev
                    } else {
                        fwd = db.iter_mut(&mut w).expect("iter_mut");
                        &mut fwd
                    };
                    let mut i = 0usize;
                    while let Some(k) = it.step() {
                        seen.push(key_of(&k));
                        let v = vec![(i % 251) as u8; width(shape, i)];
                        it.rewrite(&k, &v);
                        i += 1;
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (walk_trace(&seen, backward), c)
            }

            /// Rewrite every entry of a `range_mut` span, then report where
            /// the cursor stops — the span's end must still bound it.
            pub fn rewrite_range(dir: &Path, keys: &[Vec<u8>], lo: u64, hi: u64, shape: u8) -> (Vec<String>, Vec<(u64, usize, u8)>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut seen = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let (l, h) = (lo.to_be_bytes(), hi.to_be_bytes());
                    let span = (Bound::Included(&l[..]), Bound::Excluded(&h[..]));
                    let mut it = db.range_mut(&mut w, &span).expect("range_mut");
                    let mut i = 0usize;
                    while let Some(kv) = it.next() {
                        let k = kv.expect("entry").0.to_vec();
                        seen.push(key_of(&k));
                        let v = vec![(i % 251) as u8; width(shape, i)];
                        // SAFETY: the key was copied out; no borrow is live.
                        unsafe { it.put_current(&k, &v) }.expect("put_current");
                        i += 1;
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (walk_trace(&seen, false), c)
            }

            fn walk_trace(seen: &[u64], backward: bool) -> Vec<String> {
                let ordered = if backward {
                    seen.windows(2).all(|w| w[0] > w[1])
                } else {
                    seen.windows(2).all(|w| w[0] < w[1])
                };
                vec![
                    format!("visited {} entries", seen.len()),
                    format!("first {:?}", seen.first()),
                    format!("last {:?}", seen.last()),
                    format!("strictly ordered: {ordered}"),
                ]
            }

            /// One step and one rewrite, over either iterator direction.
            trait RwWalk {
                fn step(&mut self) -> Option<Vec<u8>>;
                fn rewrite(&mut self, k: &[u8], v: &[u8]);
            }
            impl RwWalk for $heed::RwIter<'_, Bytes, Bytes> {
                fn step(&mut self) -> Option<Vec<u8>> {
                    self.next().map(|kv| kv.expect("entry").0.to_vec())
                }
                fn rewrite(&mut self, k: &[u8], v: &[u8]) {
                    // SAFETY: `step` copied the key out; no borrow is live.
                    unsafe { self.put_current(k, v) }.expect("put_current");
                }
            }
            impl RwWalk for $heed::RwRevIter<'_, Bytes, Bytes> {
                fn step(&mut self) -> Option<Vec<u8>> {
                    self.next().map(|kv| kv.expect("entry").0.to_vec())
                }
                fn rewrite(&mut self, k: &[u8], v: &[u8]) {
                    // SAFETY: `step` copied the key out; no borrow is live.
                    unsafe { self.put_current(k, v) }.expect("put_current");
                }
            }
        }
    };
    (@setpage $o:ident, set) => { $o.page_size(zerodb_oracle::os_page_size()); };
    (@setpage $o:ident, noset) => {};
}

probes!(lmdb, heed, noset);
probes!(zerodb, heed_zerodb, set);

/// Run one probe on both engines and require identical trace AND content.
macro_rules! agree {
    ($probe:ident, $label:expr $(, $arg:expr)*) => {{
        let ks = keys();
        let l_dir = TempDir::new().expect("tempdir");
        let z_dir = TempDir::new().expect("tempdir");
        let (l_trace, l_content) = lmdb::$probe(l_dir.path(), &ks $(, $arg)*);
        let (z_trace, z_content) = zerodb::$probe(z_dir.path(), &ks $(, $arg)*);
        assert_eq!(
            l_trace, z_trace,
            "{}: cursor-position trace diverges from the fork (SPEC 03 §7)",
            $label
        );
        assert_eq!(
            l_content, z_content,
            "{}: final content diverges from the fork",
            $label
        );
        l_trace
    }};
}

#[test]
fn same_size_put_then_next_yields_the_successor() {
    let t = agree!(put_then_next, "same size", 10usize, VAL);
    // Pin the fork's answer literally, so a future reader sees the observation
    // and not just that the two agreed.
    assert_eq!(
        t,
        vec!["at 10", "put -> true", "next -> 11 (200 bytes)"],
        "observed fork behaviour changed"
    );
}

#[test]
fn growing_put_then_next_yields_the_successor() {
    // Big enough that the leaf cannot hold it in place at any page size up to
    // 16 K, so the put splits (or moves the value to overflow pages).
    let t = agree!(put_then_next, "grow", 199usize, VAL * 40);
    assert_eq!(t, vec!["at 199", "put -> true", "next -> 200 (200 bytes)"]);
}

#[test]
fn put_on_the_last_entry_then_next_is_none() {
    let t = agree!(put_then_next, "EOF edge", N - 1, VAL);
    assert_eq!(
        t,
        vec![
            format!("at {}", N - 1),
            "put -> true".into(),
            "next -> None".into()
        ]
    );
}

#[test]
fn rewriting_every_entry_forward_visits_each_once() {
    for shape in [0u8, 1, 2] {
        let t = agree!(rewrite_all, format!("forward shape {shape}"), shape, false);
        assert_eq!(t[0], format!("visited {N} entries"));
        assert_eq!(t[3], "strictly ordered: true");
    }
}

#[test]
fn rewriting_every_entry_backward_visits_each_once() {
    for shape in [0u8, 1, 2] {
        let t = agree!(rewrite_all, format!("backward shape {shape}"), shape, true);
        assert_eq!(t[0], format!("visited {N} entries"));
        assert_eq!(t[3], "strictly ordered: true");
    }
}

#[test]
fn rewriting_a_range_stops_at_its_end() {
    for shape in [0u8, 1, 2] {
        let t = agree!(
            rewrite_range,
            format!("range shape {shape}"),
            300u64,
            1_300u64,
            shape
        );
        assert_eq!(t[0], "visited 1000 entries");
    }
}
