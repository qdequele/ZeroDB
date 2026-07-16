//! `migrate-from-lmdb` acceptance (M1.12, PLAN §1.12) — needs `migrate-lmdb`.
//!
//! (a) Round-trip: build a real LMDB env (multi-DB, overflow values, varied
//!     sizes), migrate it into a fresh zerodb env, and assert `our dump of the
//!     zerodb env` is **byte-identical** to `LMDB's own content rendered in the
//!     same dump format`.
//! (b) Real-Meilisearch-index proxy: a milli-shaped env (BE-u32-keyed docids →
//!     roaring-bitmap-like blobs, str-keyed word DB, a large overflow value),
//!     migrated, compared by full iteration (the dump equality) **and** by point
//!     queries — byte-identical values. A true milli index replay is a 1.14-gate
//!     item; this proxy stands in for it here.
#![cfg(feature = "migrate-lmdb")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use heed::types::Bytes;
use heed::EnvOpenOptions as LEnvOpenOptions;

use zerodb::{EnvFlags, EnvOpenOptions as ZEnvOpenOptions};
use zerodb_tools::dump_format::{render, DumpDb};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-migrate-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Logical content collected from a heed env, in the same order `cmd_dump`
/// produces: main user data (subdb pointers excluded), then named DBs by name,
/// every section sorted by key.
struct Content {
    main_user: Vec<zerodb_tools::common::Kv>,
    named: Vec<zerodb_tools::common::NamedDb>,
}

fn collect_lmdb(dir: &Path, map_size: usize) -> Content {
    let mut opts = LEnvOpenOptions::new();
    opts.max_dbs(64);
    opts.map_size(map_size);
    // SAFETY: read-only-style access to a private temp env, single-threaded,
    // no cross-process flags. Test-only.
    let env = unsafe { opts.open(dir).unwrap() };
    let rtxn = env.read_txn().unwrap();
    let main: heed::Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();

    let mut main_user = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for item in main.iter(&rtxn).unwrap() {
        let (k, v) = item.unwrap();
        let is_subdb = std::str::from_utf8(k)
            .ok()
            .map(|name| {
                env.open_database::<Bytes, Bytes>(&rtxn, Some(name)).is_ok()
                    && env
                        .open_database::<Bytes, Bytes>(&rtxn, Some(name))
                        .unwrap()
                        .is_some()
            })
            .unwrap_or(false);
        if is_subdb {
            names.push(std::str::from_utf8(k).unwrap().to_string());
        } else {
            main_user.push((k.to_vec(), v.to_vec()));
        }
    }
    names.sort();
    let mut named = Vec::new();
    for name in names {
        let db: heed::Database<Bytes, Bytes> =
            env.open_database(&rtxn, Some(&name)).unwrap().unwrap();
        let entries: Vec<_> = db
            .iter(&rtxn)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        named.push((name.into_bytes(), entries));
    }
    Content { main_user, named }
}

fn render_content(c: &Content) -> String {
    let mut dbs = vec![DumpDb {
        name: None,
        entries: c.main_user.clone(),
    }];
    for (name, entries) in &c.named {
        dbs.push(DumpDb {
            name: Some(name.clone()),
            entries: entries.clone(),
        });
    }
    render(&dbs)
}

/// Build a representative LMDB env: str-keyed main data, a BE-u32-keyed DB with
/// roaring-like blobs (varied sizes, some > a page = overflow), a str-keyed word
/// DB, an empty DB, and a single very large overflow value.
fn build_lmdb(dir: &Path, map_size: usize) {
    let mut opts = LEnvOpenOptions::new();
    opts.max_dbs(64);
    opts.map_size(map_size);
    // SAFETY: fresh private temp env, single-threaded, no cross-process flags.
    let env = unsafe { opts.open(dir).unwrap() };
    let mut wtxn = env.write_txn().unwrap();

    let main: heed::Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for i in 0u32..300 {
        main.put(&mut wtxn, format!("setting-{i:04}").as_bytes(), b"1")
            .unwrap();
    }
    main.put(&mut wtxn, b"empty-value", b"").unwrap();

    // milli-shaped: BE-u32 docid -> roaring-bitmap-like blob (varied sizes).
    let docids: heed::Database<Bytes, Bytes> = env
        .create_database(&mut wtxn, Some("docid-bitmaps"))
        .unwrap();
    for i in 0u32..1500 {
        let blob = vec![(i % 251) as u8; 8 + (i as usize % 4000)];
        docids.put(&mut wtxn, &i.to_be_bytes(), &blob).unwrap();
    }

    let words: heed::Database<Bytes, Bytes> =
        env.create_database(&mut wtxn, Some("word-fst")).unwrap();
    for w in ["apple", "banana", "cherry", "date", "elder"] {
        words
            .put(&mut wtxn, w.as_bytes(), format!("posting:{w}").as_bytes())
            .unwrap();
    }

    let big: heed::Database<Bytes, Bytes> =
        env.create_database(&mut wtxn, Some("large-blobs")).unwrap();
    big.put(&mut wtxn, b"huge", &vec![0xABu8; 250_000]).unwrap();

    let _empty: heed::Database<Bytes, Bytes> =
        env.create_database(&mut wtxn, Some("empty-db")).unwrap();

    wtxn.commit().unwrap();
}

#[test]
fn round_trip_lmdb_migrate_dump_is_byte_identical() {
    let map_size = 128 << 20;
    let src = tmp_dir("rt-lmdb");
    build_lmdb(&src, map_size);

    // LMDB's own logical content, rendered in our dump format.
    let content = collect_lmdb(&src, map_size);
    let lmdb_dump = render_content(&content);

    // Migrate into a fresh zerodb env, then dump it.
    let dst = tmp_dir("rt-zerodb");
    let report =
        zerodb_tools::migrate::cmd_migrate(&src, &dst, map_size, 4096).expect("migrate failed");
    assert!(report.contains("verified"), "migrate report:\n{report}");
    let zerodb_dump = zerodb_tools::commands::cmd_dump(&dst).unwrap();

    assert_eq!(
        lmdb_dump, zerodb_dump,
        "migrated zerodb dump differs from the LMDB-rendered dump"
    );

    // The migrated env is invariant-clean.
    let (creport, clean) = zerodb_tools::commands::cmd_check(&dst).unwrap();
    assert!(clean, "migrated env failed check:\n{creport}");
}

#[test]
fn real_index_proxy_point_queries_match() {
    let map_size = 128 << 20;
    let src = tmp_dir("proxy-lmdb");
    build_lmdb(&src, map_size);
    let content = collect_lmdb(&src, map_size);

    let dst = tmp_dir("proxy-zerodb");
    zerodb_tools::migrate::cmd_migrate(&src, &dst, map_size, 4096).unwrap();

    // Reopen the migrated zerodb env and point-query several keys per DB,
    // comparing the exact value bytes to the LMDB source.
    let env = ZEnvOpenOptions::new()
        .max_dbs(64)
        .flags(EnvFlags::READ_ONLY)
        .open(&dst)
        .unwrap();
    let rtxn = env.read_txn().unwrap();

    // main
    let main = env.main_database();
    for (k, v) in &content.main_user {
        assert_eq!(
            main.get(&rtxn, k).unwrap(),
            Some(v.as_slice()),
            "main key {k:?}"
        );
    }
    // named DBs
    for (name, entries) in &content.named {
        let db = env.open_database(&rtxn, Some(name)).unwrap().unwrap();
        for (k, v) in entries {
            assert_eq!(
                db.get(&rtxn, k).unwrap(),
                Some(v.as_slice()),
                "db {:?} key {:?}",
                String::from_utf8_lossy(name),
                k
            );
        }
    }
}
