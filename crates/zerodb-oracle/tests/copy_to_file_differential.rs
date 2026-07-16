//! `copy_to_file` differential: heed (the LMDB fork) vs zerodb, both compaction
//! options (M1.12, PLAN §1.12; SPEC 00 rows 17/59). We copy an equivalent env
//! through each engine's `copy_to_file`, reopen the **copies**, and assert their
//! logical content is identical — dump-equality, not byte-equality (the on-disk
//! formats differ, D-002).
//!
//! copy_to_file ops are deliberately NOT added to the fuzz op model (they take
//! their own internal read txn and produce a file, not an in-env state change);
//! these direct differential tests cover them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use heed::types::Bytes;
use heed::EnvOpenOptions as LEnvOpenOptions;

use zerodb::{
    collect_entries_flagged, named_databases, CompactionOption as ZCompaction, CopyToFile,
    EnvFlags as ZEnvFlags, EnvOpenOptions as ZEnvOpenOptions, DATA_FILE_NAME,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-copydiff-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Canonical logical content: main user data + named DBs by name, all sorted.
type Content = Vec<(Option<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>)>;

/// One dataset row: `(dbname, key, value)`, `None` dbname = main DB.
type Row = (Option<&'static [u8]>, Vec<u8>, Vec<u8>);

/// The shared dataset applied identically to both engines. Includes
/// overflow-sized values and varied sizes.
fn dataset() -> Vec<Row> {
    let mut out: Vec<Row> = Vec::new();
    for i in 0u32..400 {
        out.push((None, format!("main{i:05}").into_bytes(), b"m".to_vec()));
    }
    out.push((None, b"empty".to_vec(), Vec::new()));
    for i in 0u32..800 {
        let v = vec![(i % 100) as u8; 4 + (i as usize % 3000)];
        out.push((Some(b"docids"), i.to_be_bytes().to_vec(), v));
    }
    for w in ["alpha", "beta", "gamma"] {
        out.push((
            Some(b"words"),
            w.as_bytes().to_vec(),
            format!("p:{w}").into_bytes(),
        ));
    }
    out.push((Some(b"big"), b"huge".to_vec(), vec![0x5Au8; 200_000]));
    out.push((Some(b"empty_db"), b"__seed__".to_vec(), b"x".to_vec()));
    out
}

fn subdb_names() -> Vec<&'static [u8]> {
    vec![b"big", b"docids", b"empty_db", b"words"]
}

// ---- heed side ----------------------------------------------------------

fn build_heed(dir: &Path, map_size: usize) {
    let mut opts = LEnvOpenOptions::new();
    opts.max_dbs(64);
    opts.map_size(map_size);
    // SAFETY: fresh private temp env, single-threaded, no cross-process flags.
    let env = unsafe { opts.open(dir).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let main: heed::Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    let mut dbs: std::collections::HashMap<&[u8], heed::Database<Bytes, Bytes>> =
        std::collections::HashMap::new();
    for name in subdb_names() {
        let d = env
            .create_database(&mut wtxn, Some(std::str::from_utf8(name).unwrap()))
            .unwrap();
        dbs.insert(name, d);
    }
    for (db, k, v) in dataset() {
        match db {
            None => main.put(&mut wtxn, &k, &v).unwrap(),
            Some(name) => dbs[name].put(&mut wtxn, &k, &v).unwrap(),
        }
    }
    wtxn.commit().unwrap();
}

fn collect_heed_file(copy_file: &Path, map_size: usize) -> Content {
    let mut opts = LEnvOpenOptions::new();
    opts.max_dbs(64);
    opts.map_size(map_size);
    // SAFETY: NO_SUB_DIR opens the single copied data file; private, single-threaded.
    let env = unsafe {
        opts.flags(heed::EnvFlags::NO_SUB_DIR);
        opts.open(copy_file).unwrap()
    };
    let rtxn = env.read_txn().unwrap();
    let main: heed::Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();

    let mut content: Content = Vec::new();
    let mut main_user = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for item in main.iter(&rtxn).unwrap() {
        let (k, v) = item.unwrap();
        let is_sub = std::str::from_utf8(k)
            .ok()
            .and_then(|n| {
                env.open_database::<Bytes, Bytes>(&rtxn, Some(n))
                    .ok()
                    .flatten()
            })
            .is_some();
        if is_sub {
            names.push(std::str::from_utf8(k).unwrap().to_string());
        } else {
            main_user.push((k.to_vec(), v.to_vec()));
        }
    }
    content.push((None, main_user));
    names.sort();
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
        content.push((Some(name.into_bytes()), entries));
    }
    content
}

// ---- zerodb side --------------------------------------------------------

fn build_zerodb(dir: &Path, map_size: u64) {
    let env = ZEnvOpenOptions::new()
        .map_size(map_size as usize)
        .max_dbs(64)
        .open(dir)
        .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let main = env.main_database();
    let mut dbs = std::collections::HashMap::new();
    for name in subdb_names() {
        let d = env.create_database(&mut wtxn, Some(name)).unwrap();
        dbs.insert(name, d);
    }
    for (db, k, v) in dataset() {
        match db {
            None => main.put(&mut wtxn, &k, &v).unwrap(),
            Some(name) => dbs[name].put(&mut wtxn, &k, &v).unwrap(),
        }
    }
    wtxn.commit().unwrap();
}

fn collect_zerodb_file(copy_file: &Path, tag: &str) -> Content {
    let dir = tmp(tag);
    std::fs::copy(copy_file, dir.join(DATA_FILE_NAME)).unwrap();
    let env = ZEnvOpenOptions::new()
        .max_dbs(64)
        .flags(ZEnvFlags::READ_ONLY)
        .open(&dir)
        .unwrap();
    let rtxn = env.read_txn().unwrap();
    let mut content: Content = Vec::new();
    let main = env.main_database();
    let main_user: Vec<_> = collect_entries_flagged(&main, &rtxn)
        .unwrap()
        .into_iter()
        .filter(|(_, f, _)| f & 0x0002 == 0)
        .map(|(k, _, v)| (k, v))
        .collect();
    content.push((None, main_user));
    for name in named_databases(&rtxn).unwrap() {
        let db = env.open_database(&rtxn, Some(&name)).unwrap().unwrap();
        let entries: Vec<_> = collect_entries_flagged(&db, &rtxn)
            .unwrap()
            .into_iter()
            .map(|(k, _, v)| (k, v))
            .collect();
        content.push((Some(name), entries));
    }
    content
}

// ---- the differential ---------------------------------------------------

fn run_case(zerodb_opt: ZCompaction, heed_opt: heed::CompactionOption, tag: &str) {
    let map_size: usize = 128 << 20;

    // heed source -> copy -> reopen -> content.
    let hsrc = tmp(&format!("{tag}-hsrc"));
    build_heed(&hsrc, map_size);
    let hcopy = tmp(&format!("{tag}-hcopy")).join("copy.mdb");
    {
        let mut opts = LEnvOpenOptions::new();
        opts.max_dbs(64);
        opts.map_size(map_size);
        // SAFETY: reopen the private source env to copy it; single-threaded.
        let env = unsafe { opts.open(&hsrc).unwrap() };
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&hcopy)
            .unwrap();
        env.copy_to_file(&mut f, heed_opt).unwrap();
        f.sync_all().unwrap();
    }
    let heed_content = collect_heed_file(&hcopy, map_size);

    // zerodb source -> copy -> reopen -> content.
    let zsrc = tmp(&format!("{tag}-zsrc"));
    build_zerodb(&zsrc, map_size as u64);
    let zcopy = tmp(&format!("{tag}-zcopy")).join("copy.dat");
    {
        let env = ZEnvOpenOptions::new()
            .map_size(map_size)
            .max_dbs(64)
            .open(&zsrc)
            .unwrap();
        env.copy_to_file(&zcopy, zerodb_opt).unwrap();
    }
    let zerodb_content = collect_zerodb_file(&zcopy, &format!("{tag}-zread"));

    assert_eq!(
        heed_content, zerodb_content,
        "{tag}: copy logical content differs between LMDB and zerodb"
    );
}

#[test]
fn copy_enabled_matches_lmdb() {
    run_case(
        ZCompaction::Enabled,
        heed::CompactionOption::Enabled,
        "enabled",
    );
}

#[test]
fn copy_disabled_matches_lmdb() {
    run_case(
        ZCompaction::Disabled,
        heed::CompactionOption::Disabled,
        "disabled",
    );
}
