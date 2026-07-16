//! `Env::copy_to_file` (M1.12): both compaction options produce a copy whose
//! logical content equals the source and which passes the invariant walker.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{
    check::check_image, collect_entries_flagged, named_databases, CompactionOption, CopyToFile,
    Database, Env, EnvFlags, EnvOpenOptions, RoTxn, DATA_FILE_NAME,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-copy-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Full logical content of an env: (main user entries, named DBs' entries).
type Logical = (
    Vec<(Vec<u8>, Vec<u8>)>,
    Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
);

fn logical(env: &Env, txn: &RoTxn<'_>) -> Logical {
    let main = env.main_database();
    let main_user: Vec<_> = collect_entries_flagged(&main, txn)
        .unwrap()
        .into_iter()
        .filter(|(_, f, _)| f & 0x0002 == 0) // F_SUBDATA
        .map(|(k, _, v)| (k, v))
        .collect();
    let mut named = Vec::new();
    for name in named_databases(txn).unwrap() {
        let dbh: Database = env.open_database(txn, Some(&name)).unwrap().unwrap();
        let entries: Vec<_> = collect_entries_flagged(&dbh, txn)
            .unwrap()
            .into_iter()
            .map(|(k, _, v)| (k, v))
            .collect();
        named.push((name, entries));
    }
    (main_user, named)
}

/// Build a source env with main user data, three named DBs (one with big
/// overflow values, one empty), across several commits.
fn build_source(dir: &Path) -> Env {
    let env = EnvOpenOptions::new()
        .map_size(64 << 20)
        .max_dbs(64)
        .open(dir)
        .unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let main = env.main_database();
    for i in 0u32..1500 {
        main.put(&mut wtxn, format!("main{i:05}").as_bytes(), b"payload")
            .unwrap();
    }
    let posts = env.create_database(&mut wtxn, Some(b"posts")).unwrap();
    for i in 0u32..2000 {
        posts
            .put(&mut wtxn, &i.to_be_bytes(), format!("post-{i}").as_bytes())
            .unwrap();
    }
    let blobs = env.create_database(&mut wtxn, Some(b"blobs")).unwrap();
    blobs.put(&mut wtxn, b"a", &vec![7u8; 40_000]).unwrap();
    blobs.put(&mut wtxn, b"b", &vec![9u8; 130_000]).unwrap();
    let _empty = env.create_database(&mut wtxn, Some(b"empty")).unwrap();
    wtxn.commit().unwrap();

    // A second commit with deletes, to leave some free pages behind (so the
    // non-compact copy exercises a non-empty freelist).
    let mut wtxn = env.write_txn().unwrap();
    for i in 0u32..500 {
        main.delete(&mut wtxn, format!("main{i:05}").as_bytes())
            .unwrap();
    }
    wtxn.commit().unwrap();
    env
}

/// Open the single-file copy at `copy_file` as an env dir and read it.
fn open_copy(copy_file: &Path, tag: &str) -> (PathBuf, Env) {
    let dir = tmp_dir(tag);
    std::fs::copy(copy_file, dir.join(DATA_FILE_NAME)).unwrap();
    let env = EnvOpenOptions::new()
        .max_dbs(64)
        .flags(EnvFlags::READ_ONLY)
        .open(&dir)
        .unwrap();
    (dir, env)
}

fn assert_copy_matches(src_dir: &Path, option: CompactionOption, tag: &str) {
    let src = build_source(src_dir);
    let want = {
        let txn = src.read_txn().unwrap();
        logical(&src, &txn)
    };

    let copy_file = tmp_dir(&format!("{tag}-out")).join("copy.dat");
    src.copy_to_file(&copy_file, option).unwrap();

    // The produced file is a valid, invariant-clean env image.
    let bytes = std::fs::read(&copy_file).unwrap();
    let psize = src.page_size();
    assert_eq!(
        check_image(&bytes, psize),
        Vec::<String>::new(),
        "{tag}: copy failed the invariant check"
    );

    let (_cdir, copy_env) = open_copy(&copy_file, tag);
    let got = {
        let txn = copy_env.read_txn().unwrap();
        logical(&copy_env, &txn)
    };
    assert_eq!(want, got, "{tag}: copy logical content differs from source");
}

#[test]
fn copy_disabled_preserves_content() {
    let dir = tmp_dir("src-disabled");
    assert_copy_matches(&dir, CompactionOption::Disabled, "disabled");
}

#[test]
fn copy_enabled_compacts_and_preserves_content() {
    let dir = tmp_dir("src-enabled");
    assert_copy_matches(&dir, CompactionOption::Enabled, "enabled");
}

#[test]
fn compacted_copy_is_no_larger_than_raw_copy() {
    // Compaction drops free pages, so the Enabled copy must not exceed the
    // Disabled copy's size for the same source.
    let dir = tmp_dir("src-size");
    let src = build_source(&dir);

    let raw = tmp_dir("raw-out").join("copy.dat");
    let compact = tmp_dir("compact-out").join("copy.dat");
    src.copy_to_file(&raw, CompactionOption::Disabled).unwrap();
    src.copy_to_file(&compact, CompactionOption::Enabled)
        .unwrap();

    let raw_len = std::fs::metadata(&raw).unwrap().len();
    let compact_len = std::fs::metadata(&compact).unwrap().len();
    assert!(
        compact_len <= raw_len,
        "compacted copy ({compact_len}) larger than raw copy ({raw_len})"
    );
}
