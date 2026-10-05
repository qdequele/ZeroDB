//! Where a write cursor is left after `del_current`, observed against the fork.
//!
//! SPEC 03 §7 states the contract ("a following `next` yields the entry that
//! followed the deleted one") and §5 rule 4 currently *mandates the mechanism*:
//! the write cursor "tracks its position by key and re-seeks after each of its
//! own mutations". docs/PERF-GAP-VS-LMDB.md (the `del_current` re-descent)
//! proposes replacing that re-seek with a retained
//! path (LMDB's `C_DEL`), which is a change of mechanism, not of contract.
//!
//! Nothing pinned the contract. The existing `Op::IterMutDelCurrent` deletes and
//! **stops** — it never iterates afterwards — so only the resulting content was
//! differential, never the resulting position. These tests pin the position, so
//! that change has something to be correct against (rule 1: observe the fork,
//! do not reason about it; rule 2: this test may not be weakened to let an
//! optimization pass).
//!
//! Each case records an event trace through the **heed surface consumers use**
//! (`iter_mut` / `prefix_iter_mut` / `range_mut`, the shapes milli writes at its
//! nine `del_current` call sites) and asserts the two engines produce the same
//! trace *and* the same surviving content.

use zerodb_oracle::tempdir::TempDir;

/// Entries per tree: enough to span many leaves at any supported page size, so
/// the drain cases cross leaf boundaries and force merges (SPEC 03 §10).
const N: usize = 2_000;
/// Value width — with the 8-byte keys this puts ~70 entries on a 16 K page.
const VAL: usize = 200;

fn keys() -> Vec<Vec<u8>> {
    (0..N as u64).map(|i| i.to_be_bytes().to_vec()).collect()
}

/// The OS page size, clamped to zerodb's supported range. LMDB is locked to it
/// and exposes no selector, so zerodb is pinned to the same value: a geometry
/// mismatch would change which deletes trigger a merge and make the traces
/// incomparable for reasons that have nothing to do with cursor position.
fn os_page_size() -> u32 {
    // SAFETY: `sysconf(_SC_PAGESIZE)` is a pure query with no preconditions and
    // no side effects. FFI in the oracle crate is sanctioned by the AGENTS.md
    // unsafe policy.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    match u32::try_from(v) {
        Ok(p) if (4096..=65536).contains(&p) && p.is_power_of_two() => p,
        _ => 4096,
    }
}

/// Generate one probe module per engine from a single body, so neither engine
/// can be given a different sequence of calls.
macro_rules! probes {
    ($m:ident, $heed:ident, $setpage:tt) => {
        mod $m {
            #[allow(unused_imports)] // only the `set` arm needs the page size
            use super::{os_page_size, N, VAL};
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvOpenOptions, WithoutTls};

            type Db = Database<Bytes, Bytes>;

            fn open(dir: &Path) -> (Env<WithoutTls>, Db) {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(64 << 20);
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

            /// Surviving keys, as `u64`s, read in a fresh txn.
            fn content(env: &Env<WithoutTls>, db: Db) -> Vec<u64> {
                let r = env.read_txn().expect("read_txn");
                db.iter(&r)
                    .expect("iter")
                    .map(|kv| {
                        let (k, _) = kv.expect("entry");
                        u64::from_be_bytes(k.try_into().expect("8-byte key"))
                    })
                    .collect()
            }

            fn key_of(kv: &[u8]) -> u64 {
                u64::from_be_bytes(kv.try_into().expect("8-byte key"))
            }

            /// Advance to entry `nth`, `del_current`, then report what the very
            /// next `next()` yields. The single-step question §7 answers.
            pub fn delete_then_next(dir: &Path, keys: &[Vec<u8>], nth: usize) -> (Vec<String>, Vec<u64>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut trace = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    for i in 0..=nth {
                        match it.next() {
                            Some(Ok((k, _))) => {
                                if i == nth {
                                    trace.push(format!("at {}", key_of(k)));
                                }
                            }
                            Some(Err(e)) => panic!("iter: {e}"),
                            None => {
                                trace.push("exhausted before nth".into());
                                break;
                            }
                        }
                    }
                    // SAFETY: the borrow from the last `next()` was dropped above.
                    let existed = unsafe { it.del_current() }.expect("del_current");
                    trace.push(format!("del -> {existed}"));
                    match it.next() {
                        Some(Ok((k, _))) => trace.push(format!("next -> {}", key_of(k))),
                        Some(Err(e)) => panic!("next: {e}"),
                        None => trace.push("next -> None".into()),
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (trace, c)
            }

            /// From entry `nth`, delete every remaining entry through the
            /// cursor — milli's `while iter.next() { del_current }` shape, and
            /// the one that forces merges mid-walk (SPEC 03 §10).
            pub fn drain_from(dir: &Path, keys: &[Vec<u8>], nth: usize) -> (Vec<String>, Vec<u64>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut seen = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    let mut i = 0usize;
                    while let Some(kv) = it.next() {
                        let (k, _) = kv.expect("entry");
                        let k = key_of(k);
                        if i >= nth {
                            seen.push(k);
                            // SAFETY: `k` is a copied u64; no borrow is live.
                            unsafe { it.del_current() }.expect("del_current");
                        }
                        i += 1;
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                let trace = vec![
                    format!("visited {} entries", seen.len()),
                    format!("first {:?}", seen.first()),
                    format!("last {:?}", seen.last()),
                    format!("strictly ascending: {}", seen.windows(2).all(|w| w[0] < w[1])),
                ];
                (trace, c)
            }

            /// Delete every other entry through the cursor: the cursor must
            /// survive alternating delete / advance, not just a delete run.
            pub fn delete_every_other(dir: &Path, keys: &[Vec<u8>]) -> (Vec<String>, Vec<u64>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut deleted = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    let mut i = 0usize;
                    while let Some(kv) = it.next() {
                        let (k, _) = kv.expect("entry");
                        let k = key_of(k);
                        if i % 2 == 0 {
                            deleted.push(k);
                            // SAFETY: `k` is a copied u64; no borrow is live.
                            unsafe { it.del_current() }.expect("del_current");
                        }
                        i += 1;
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (vec![format!("deleted {:?}", deleted)], c)
            }

            /// The same drain through `prefix_iter_mut` — milli's actual call
            /// at `DeletingFromAllFilters` and `delete_old_fid_word_count_docids`.
            pub fn drain_prefix(dir: &Path, keys: &[Vec<u8>], prefix: &[u8]) -> (Vec<String>, Vec<u64>) {
                let (env, db) = open(dir);
                fill(&env, db, keys);
                let mut seen = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.prefix_iter_mut(&mut w, prefix).expect("prefix_iter_mut");
                    while let Some(kv) = it.next() {
                        let (k, _) = kv.expect("entry");
                        seen.push(key_of(k));
                        // SAFETY: the key was copied out; no borrow is live.
                        unsafe { it.del_current() }.expect("del_current");
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db);
                drop(env);
                (vec![format!("prefix drained {:?}", seen)], c)
            }

            /// Delete the final entry, then ask for `next` — the EOF edge.
            pub fn delete_last_then_next(dir: &Path, keys: &[Vec<u8>]) -> (Vec<String>, Vec<u64>) {
                delete_then_next(dir, keys, N - 1)
            }
        }
    };
    (@setpage $o:ident, set) => { $o.page_size(os_page_size()); };
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
            "{}: surviving content diverges from the fork",
            $label
        );
        l_trace
    }};
}

#[test]
fn delete_then_next_yields_the_successor_midpage() {
    let t = agree!(delete_then_next, "mid-page", 10usize);
    // Pin the fork's answer literally, so a future reader sees the observation
    // and not just that the two agreed.
    assert_eq!(
        t,
        vec!["at 10", "del -> true", "next -> 11"],
        "observed fork behaviour changed"
    );
}

#[test]
fn delete_then_next_yields_the_successor_across_a_leaf_boundary() {
    // Some index deep enough that a 16 K page has ended before it; the exact
    // boundary is geometry-dependent, so the value below is the *contract*
    // (successor), not the boundary.
    let t = agree!(delete_then_next, "leaf boundary", 199usize);
    assert_eq!(t, vec!["at 199", "del -> true", "next -> 200"]);
}

#[test]
fn delete_last_entry_then_next_is_none() {
    let t = agree!(delete_last_then_next, "EOF edge");
    assert_eq!(
        t,
        vec![
            format!("at {}", N - 1),
            "del -> true".into(),
            "next -> None".into()
        ]
    );
}

#[test]
fn draining_the_whole_tree_through_the_cursor_visits_every_key_once() {
    let t = agree!(drain_from, "full drain", 0usize);
    assert_eq!(t[0], format!("visited {N} entries"));
    assert_eq!(t[3], "strictly ascending: true");
}

#[test]
fn draining_the_tail_through_the_cursor_leaves_the_head() {
    agree!(drain_from, "tail drain", 1_000usize);
}

#[test]
fn deleting_every_other_entry_keeps_the_cursor_aligned() {
    agree!(delete_every_other, "alternating");
}

#[test]
fn draining_a_prefix_through_the_cursor_matches_the_fork() {
    // 8-byte big-endian keys below 2^16: bytes 0..6 are zero and byte 6 is the
    // bucket, so a 7-byte prefix selects the 256 keys sharing it.
    let prefix = 3u64.to_be_bytes()[..7].to_vec();
    agree!(drain_prefix, "prefix drain", prefix.as_slice());
}
