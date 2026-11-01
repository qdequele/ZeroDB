//! Where the Meilisearch LMDB fork places a multi-page value when the free
//! pages that could hold it were freed by different transactions, observed
//! through heed 0.22.1. Input for issue #29 (the free-page run search) and
//! its design record, docs/adr/0029-free-page-run-search.md.
//! AGENTS.md rule 1: observed, not guessed.
//!
//! A value larger than a page is stored in a run of contiguous overflow
//! pages. Deleting it frees the run, and the freed pages are recorded under
//! the id of the deleting transaction: one freed-page record per
//! transaction. Each script lays out two overflow values `a` and `b` side by
//! side in the file, deletes them (in one transaction or in two), commits a
//! number of small "spacer" transactions, then writes a value `c` exactly as
//! long as `a` and `b` together, and records:
//!
//! 1. `b-a=adjacent` when `b`'s run starts right after `a`'s (the
//!    precondition: the freed pages form one contiguous run), else the
//!    distance in pages;
//! 2. `c=reused` when `c`'s first page lies inside the file as it was before
//!    `c`'s transaction, `c=end` when `c` was placed past it;
//! 3. `grew=<n>`: how many pages `c`'s transaction added to the end of the
//!    file (`EnvInfo::last_page_number` after minus before), written
//!    `grew=c+<k>` when it is at least `c`'s own length, so the pins do not
//!    depend on the page size.
//!
//! Each value starts with a unique marker, so its first page is found by
//! scanning the data file; the page number is the marker's offset divided by
//! the page size, whatever the engine's page-header size. Both engines run at
//! the OS page size (ZeroDB's default, as LMDB's).
//!
//! What the pins show about LMDB:
//! - Pages freed by the transaction just before the writer are not reused
//!   (`freed_by_previous_txn`): LMDB only draws from records older than the
//!   previous transaction, so one spacer commit makes them reusable.
//! - Once reusable, a run is found whether its pages come from one record or
//!   from two: LMDB merges the records it reads into one in-memory list
//!   before searching it for a run, for small and for large records alike.
//!
//! The `OBS_*` constants pin LMDB's outcomes; a diff means the fork changed or
//! the pin was wrong, never edit a pin to make ZeroDB pass. Pins observed on
//! macOS (Darwin 27.0.0, aarch64, 16 KiB pages) and Linux aarch64 (4 KiB
//! pages), 2026-10-19, identical on both; the two `_CUSHIONED` pins on macOS
//! only. ZeroDB runs the
//! same scripts and must match each pin, except in the cases of divergence
//! D-027 (a) (proposed, issue #29), where it is held to its current outcome so
//! that any change to the free-page search shows up here. Both outcomes are
//! printed side by side with `--nocapture`.
//!
//! Since ZeroDB's run search also merges records (SPEC 05 GC-21), the
//! `_CUSHIONED` cases, where the run of `a` and `b` stays whole until `c`,
//! match LMDB; before it, ZeroDB placed `c` at the end of the file in both.

use zerodb_oracle::tempdir::TempDir;

/// `a` and `b` (3 pages each) deleted in one transaction, `c` (6 pages)
/// written in the next one, with no spacer.
const OBS_FREED_BY_PREVIOUS_TXN: &[&str] = &["b-a=adjacent", "c=end", "grew=c+1"];
/// `a` and `b` (3 pages each) deleted in one transaction, one spacer, then
/// `c` (6 pages).
const OBS_RUN_WITHIN_ONE_RECORD: &[&str] = &["b-a=adjacent", "c=reused", "grew=0"];
/// `a` and `b` (3 pages each) deleted by two transactions, one spacer, then
/// `c` (6 pages).
const OBS_RUN_ACROSS_TWO_RECORDS: &[&str] = &["b-a=adjacent", "c=reused", "grew=0"];
/// As `OBS_RUN_ACROSS_TWO_RECORDS` with `a` and `b` of `BIG` pages each, more
/// than a freed-page list ZeroDB keeps in its meta page holds, so ZeroDB
/// stores both records in its free-page database.
const OBS_BIG_RUN_ACROSS_TWO_RECORDS: &[&str] = &["b-a=adjacent", "c=reused", "grew=1"];
/// `a` of `BIG` pages and `b` of 3 pages deleted by two transactions, one
/// spacer, then `c` of `BIG + 3` pages.
const OBS_BIG_AND_SMALL_ACROSS_TWO_RECORDS: &[&str] = &["b-a=adjacent", "c=reused", "grew=1"];
/// As `OBS_BIG_RUN_ACROSS_TWO_RECORDS`, with a cushion `x` of 64 pages
/// freed first (`Script::cushion`).
const OBS_BIG_RUN_ACROSS_TWO_RECORDS_CUSHIONED: &[&str] = &["b-a=adjacent", "c=reused", "grew=0"];
/// As `OBS_BIG_AND_SMALL_ACROSS_TWO_RECORDS`, with a cushion `x` of 64 pages
/// freed first.
const OBS_BIG_AND_SMALL_ACROSS_TWO_RECORDS_CUSHIONED: &[&str] =
    &["b-a=adjacent", "c=reused", "grew=0"];

/// Marker prefix of value `name`; 16 bytes, never produced by the filler.
fn marker(name: u8) -> [u8; 16] {
    let mut m = *b"\xffZDB-RUN-OBS-?\xfe\xff";
    m[13] = name;
    m
}

/// A value of `pages` overflow pages at page size `ps` for both engines:
/// `pages - 1` full pages plus half a page leaves room for either engine's
/// overflow-page header without spilling into one more page.
fn value(name: u8, pages: usize, ps: usize) -> Vec<u8> {
    let len = (pages - 1) * ps + ps / 2;
    let mut v = vec![name; len];
    v[..16].copy_from_slice(&marker(name));
    v
}

/// The page holding the start of value `name` in the data file at `path`.
fn first_page(path: &std::path::Path, name: u8, ps: usize) -> usize {
    let bytes = std::fs::read(path).expect("read data file");
    let m = marker(name);
    let hits: Vec<usize> = bytes
        .windows(m.len())
        .enumerate()
        .filter(|(_, w)| *w == m)
        .map(|(i, _)| i / ps)
        .collect();
    assert_eq!(hits.len(), 1, "value {} found {hits:?}", name as char);
    hits[0]
}

/// Three pages, a freed list that fits in ZeroDB's meta page.
fn small(_ps: usize) -> usize {
    3
}

/// More freed ids than ZeroDB's meta page holds (`(ps - 176) / 8`, SPEC 05
/// §2a), so the freed list goes to the free-page database.
fn big(ps: usize) -> usize {
    (ps - 176) / 8 + 64
}

/// One script: the sizes of `a` and `b`, the key sets each deleting
/// transaction removes, and the number of spacer commits before `c`.
struct Script {
    a: fn(usize) -> usize,
    b: fn(usize) -> usize,
    frees: &'static [&'static [&'static [u8]]],
    spacers: usize,
    /// Pages of a value `x` written before `a` and `b` and deleted just
    /// before them (0: no `x`). Its freed pages are older and lower than
    /// `a`'s and `b`'s, so the pages the engines take for their own
    /// bookkeeping until `c` (tree pages, freed-page records) come from `x`
    /// rather than from the front of `a`. A two-page value `y`, written
    /// right after `x` and never deleted, keeps `x`'s pages apart from
    /// `a`'s.
    cushion: usize,
}

const FREED_BY_PREVIOUS_TXN: Script = Script {
    a: small,
    b: small,
    frees: &[&[b"a", b"b"]],
    spacers: 0,
    cushion: 0,
};
const RUN_WITHIN_ONE_RECORD: Script = Script {
    a: small,
    b: small,
    frees: &[&[b"a", b"b"]],
    spacers: 1,
    cushion: 0,
};
const RUN_ACROSS_TWO_RECORDS: Script = Script {
    a: small,
    b: small,
    frees: &[&[b"a"], &[b"b"]],
    spacers: 1,
    cushion: 0,
};
const BIG_RUN_ACROSS_TWO_RECORDS: Script = Script {
    a: big,
    b: big,
    frees: &[&[b"a"], &[b"b"]],
    spacers: 1,
    cushion: 0,
};
const BIG_AND_SMALL_ACROSS_TWO_RECORDS: Script = Script {
    a: big,
    b: small,
    frees: &[&[b"a"], &[b"b"]],
    spacers: 1,
    cushion: 0,
};

const BIG_RUN_ACROSS_TWO_RECORDS_CUSHIONED: Script = Script {
    cushion: 64,
    ..BIG_RUN_ACROSS_TWO_RECORDS
};
const BIG_AND_SMALL_ACROSS_TWO_RECORDS_CUSHIONED: Script = Script {
    cushion: 64,
    ..BIG_AND_SMALL_ACROSS_TWO_RECORDS
};

macro_rules! cases {
    ($m:ident, $heed:ident) => {
        mod $m {
            use super::{first_page, value, Script};
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvOpenOptions, WithoutTls};

            type Db = Database<Bytes, Bytes>;

            fn open(dir: &Path) -> (Env<WithoutTls>, Db, usize) {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(1 << 30);
                // SAFETY: a private temp dir, one process.
                let env = unsafe { o.open(dir) }.expect("open env");
                let mut w = env.write_txn().unwrap();
                let db: Db = env.create_database(&mut w, None).unwrap();
                // A small entry keeps the tree's leaf alive once `a` and `b`
                // are deleted.
                db.put(&mut w, b"k", b"v").unwrap();
                w.commit().unwrap();
                let ps = db.stat(&env.read_txn().unwrap()).unwrap().page_size as usize;
                (env, db, ps)
            }

            fn commit_puts(env: &Env<WithoutTls>, db: Db, puts: &[(&[u8], Vec<u8>)]) {
                let mut w = env.write_txn().unwrap();
                for (k, v) in puts {
                    db.put(&mut w, k, v).unwrap();
                }
                w.commit().unwrap();
            }

            pub fn run(dir: &Path, s: &Script) -> Vec<String> {
                let (env, db, ps) = open(dir);
                let (na, nb) = ((s.a)(ps), (s.b)(ps));
                let data = dir.join("data.mdb");
                if s.cushion > 0 {
                    // `y` stays: it keeps `x`'s pages apart from `a`'s,
                    // so no run of `c`'s length lies in `x` and `a` alone.
                    commit_puts(
                        &env,
                        db,
                        &[
                            (b"x", value(b'x', s.cushion, ps)),
                            (b"y", value(b'y', 2, ps)),
                        ],
                    );
                }
                commit_puts(
                    &env,
                    db,
                    &[(b"a", value(b'a', na, ps)), (b"b", value(b'b', nb, ps))],
                );
                let (a, b) = (first_page(&data, b'a', ps), first_page(&data, b'b', ps));
                if s.cushion > 0 {
                    let mut w = env.write_txn().unwrap();
                    assert!(db.delete(&mut w, b"x").unwrap());
                    w.commit().unwrap();
                }
                for keys in s.frees {
                    let mut w = env.write_txn().unwrap();
                    for k in *keys {
                        assert!(db.delete(&mut w, k).unwrap());
                    }
                    w.commit().unwrap();
                }
                for i in 0..s.spacers {
                    commit_puts(&env, db, &[(format!("s{i}").as_bytes(), b"v".to_vec())]);
                }
                let before = env.info().last_page_number;
                commit_puts(&env, db, &[(b"c", value(b'c', na + nb, ps))]);
                let after = env.info().last_page_number;
                let c = first_page(&data, b'c', ps);
                vec![
                    if b == a + na {
                        "b-a=adjacent".to_string()
                    } else {
                        format!("b-a={}", b as i64 - a as i64)
                    },
                    if c <= before { "c=reused" } else { "c=end" }.to_string(),
                    match (after - before).checked_sub(na + nb) {
                        Some(k) => format!("grew=c+{k}"),
                        None => format!("grew={}", after - before),
                    },
                ]
            }
        }
    };
}

cases!(lmdb, heed);
cases!(zerodb, heed_zerodb);

macro_rules! observe {
    // ZeroDB must match LMDB's pin.
    ($test:ident, $script:ident, $pin:ident) => {
        observe!($test, $script, $pin, $pin);
    };
    // ZeroDB is held to its current outcome: a tracked divergence.
    ($test:ident, $script:ident, $pin:ident, $zerodb:expr) => {
        #[test]
        fn $test() {
            let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
            let l = lmdb::run(a.path(), &$script);
            let z = zerodb::run(b.path(), &$script);
            println!(
                "{:34} lmdb {:?}  zerodb {:?}{}",
                stringify!($test),
                l,
                z,
                if l == z { "" } else { "  DIFFERS" }
            );
            assert_eq!(l, $pin, "LMDB outcome changed for {}", stringify!($test));
            let want: &[&str] = $zerodb;
            assert_eq!(z, want, "ZeroDB outcome for {}", stringify!($test));
        }
    };
}

// D-027 (a), proposed: ZeroDB reuses pages freed by the previous transaction.
observe!(
    freed_by_previous_txn,
    FREED_BY_PREVIOUS_TXN,
    OBS_FREED_BY_PREVIOUS_TXN,
    &["b-a=adjacent", "c=reused", "grew=0"]
);
observe!(
    run_within_one_record,
    RUN_WITHIN_ONE_RECORD,
    OBS_RUN_WITHIN_ONE_RECORD
);
// Matches: ZeroDB carries a small freed list forward into the next
// transaction's own list (SPEC 05 GC-30), so both runs end up in one record.
observe!(
    run_across_two_records,
    RUN_ACROSS_TWO_RECORDS,
    OBS_RUN_ACROSS_TWO_RECORDS
);
// D-027 (a), proposed: the run spans two records of the free-page database,
// and ZeroDB's run search finds such runs (the `_cushioned` case below), but
// here `a`'s front is gone before `c`. ZeroDB takes the pages for its own
// bookkeeping (the tree's leaf, the free-page database's leaf and its
// records) from the lowest free ids, which are `a`'s first pages, and it
// starts doing so in the transaction right after the one that freed `a`,
// which LMDB's gate does not allow. Traced at 16 KiB pages: 10 of `a`'s
// pages are taken over three commits, leaving a free run 7 pages short of
// `c`, so `c` goes to the end of the file.
observe!(
    big_run_across_two_records,
    BIG_RUN_ACROSS_TWO_RECORDS,
    OBS_BIG_RUN_ACROSS_TWO_RECORDS,
    &["b-a=adjacent", "c=end", "grew=c+0"]
);
// D-027 (a), proposed: as above, with one record in the free-page database
// and the other in the meta page's freed list.
observe!(
    big_and_small_across_two_records,
    BIG_AND_SMALL_ACROSS_TWO_RECORDS,
    OBS_BIG_AND_SMALL_ACROSS_TWO_RECORDS,
    &["b-a=adjacent", "c=end", "grew=c+0"]
);
// Matches: the run spans two records of the free-page database (SPEC 05
// GC-21's merged search).
observe!(
    big_run_across_two_records_cushioned,
    BIG_RUN_ACROSS_TWO_RECORDS_CUSHIONED,
    OBS_BIG_RUN_ACROSS_TWO_RECORDS_CUSHIONED
);
// Matches: the run spans a record of the free-page database and the meta
// page's freed list.
observe!(
    big_and_small_across_two_records_cushioned,
    BIG_AND_SMALL_ACROSS_TWO_RECORDS_CUSHIONED,
    OBS_BIG_AND_SMALL_ACROSS_TWO_RECORDS_CUSHIONED
);
