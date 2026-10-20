//! `dump` -> `load` -> `dump` round-trip over a zerodb env (tools acceptance,
//! pure zerodb — no heed): the two dumps are byte-identical and the reloaded env
//! is invariant-clean.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("zerodb-tools-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Build an env with main user data + named DBs (one with overflow, one empty).
fn build(dir: &Path) {
    let env: Env = EnvOpenOptions::new()
        .map_size(64 << 20)
        .max_dbs(64)
        .open(dir)
        .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let main = env.main_database();
    for i in 0u32..1200 {
        main.put(&mut wtxn, format!("m{i:05}").as_bytes(), b"v")
            .unwrap();
    }
    let posts = env.create_database(&mut wtxn, Some(b"posts")).unwrap();
    for i in 0u32..900 {
        posts
            .put(&mut wtxn, &i.to_be_bytes(), format!("post-{i}").as_bytes())
            .unwrap();
    }
    let blobs = env.create_database(&mut wtxn, Some(b"blobs")).unwrap();
    blobs.put(&mut wtxn, b"big", &vec![3u8; 90_000]).unwrap();
    let _empty = env.create_database(&mut wtxn, Some(b"empty")).unwrap();
    wtxn.commit().unwrap();
    // env dropped here (same-process registry frees the path for the tool).
}

#[test]
fn dump_load_dump_is_byte_identical() {
    let a = tmp_dir("src");
    build(&a);

    let dump1 = zerodb_tools::commands::cmd_dump(&a).unwrap();

    let dump_file = tmp_dir("dumpfile").join("out.dump");
    std::fs::write(&dump_file, dump1.as_bytes()).unwrap();

    let b = tmp_dir("dst");
    zerodb_tools::commands::cmd_load(&dump_file, &b, 4096, 64 << 20).unwrap();

    // The reloaded env is invariant-clean...
    let (report, clean) = zerodb_tools::commands::cmd_check(&b).unwrap();
    assert!(clean, "reloaded env failed check:\n{report}");

    // ...and dumping it again yields byte-identical text.
    let dump2 = zerodb_tools::commands::cmd_dump(&b).unwrap();
    assert_eq!(dump1, dump2, "round-trip dump differs");
}

#[test]
fn stat_runs_on_a_closed_env() {
    let a = tmp_dir("stat-src");
    build(&a);
    let report = zerodb_tools::commands::cmd_stat(&a).unwrap();
    assert!(report.contains("database posts:"), "stat report:\n{report}");
    assert!(report.contains("database <main>:"));
    assert!(report.contains("free_pages:"));
}

/// Dump `src`, load the dump into a fresh env, check it, dump it again and
/// require byte-identical text. Returns the dump.
fn round_trip(src: &Path, page_size: u32) -> String {
    let dump1 = zerodb_tools::commands::cmd_dump(src).unwrap();
    let dump_file = tmp_dir("rt-dumpfile").join("out.dump");
    std::fs::write(&dump_file, dump1.as_bytes()).unwrap();
    let dst = tmp_dir("rt-dst");
    zerodb_tools::commands::cmd_load(&dump_file, &dst, page_size, 1 << 20).unwrap();
    let (report, clean) = zerodb_tools::commands::cmd_check(&dst).unwrap();
    assert!(clean, "reloaded env failed check:\n{report}");
    let dump2 = zerodb_tools::commands::cmd_dump(&dst).unwrap();
    assert_eq!(dump1, dump2, "round-trip dump differs");
    dump1
}

fn open_env(dir: &Path) -> Env {
    EnvOpenOptions::new()
        .map_size(256 << 20)
        .max_dbs(1024)
        .open(dir)
        .unwrap()
}

#[test]
fn round_trip_empty_env() {
    let a = tmp_dir("rt-empty");
    let env = open_env(&a);
    env.write_txn().unwrap().commit().unwrap();
    drop(env);
    let dump = round_trip(&a, 4096);
    assert_eq!(
        dump,
        "VERSION=3\nformat=bytevalue\ntype=btree\nHEADER=END\nDATA=END\n"
    );
}

#[test]
fn round_trip_many_named_dbs() {
    let a = tmp_dir("rt-many");
    let env = open_env(&a);
    let mut wtxn = env.write_txn().unwrap();
    for d in 0u32..300 {
        let db = env
            .create_database(&mut wtxn, Some(format!("db-{d:03}").as_bytes()))
            .unwrap();
        for i in 0..(d % 7) {
            db.put(&mut wtxn, &i.to_be_bytes(), &d.to_le_bytes())
                .unwrap();
        }
    }
    wtxn.commit().unwrap();
    drop(env);
    let dump = round_trip(&a, 4096);
    assert_eq!(dump.matches("database=").count(), 300);
}

#[test]
fn round_trip_large_values_and_special_keys() {
    let a = tmp_dir("rt-special");
    let env = open_env(&a);
    let mut wtxn = env.write_txn().unwrap();
    let main = env.main_database();
    // Keys made of bytes that mean something in the text format or in C
    // strings, and the longest key the engine accepts.
    let keys: Vec<Vec<u8>> = vec![
        vec![0],
        vec![0, 0, 0],
        vec![b'\n'],
        b"\r\n".to_vec(),
        b" DATA=END".to_vec(),
        vec![0xff; 3],
        vec![b'k'; 511],
    ];
    for (i, k) in keys.iter().enumerate() {
        main.put(&mut wtxn, k, &[i as u8; 3]).unwrap();
    }
    let blobs = env
        .create_database(&mut wtxn, Some(&[0, b'\n', 0xff]))
        .unwrap();
    blobs.put(&mut wtxn, b"empty", b"").unwrap();
    blobs
        .put(&mut wtxn, b"one-mib", &vec![0xa5; 1 << 20])
        .unwrap();
    blobs.put(&mut wtxn, b"page", &vec![1; 4096]).unwrap();
    wtxn.commit().unwrap();
    drop(env);
    round_trip(&a, 4096);
    round_trip(&a, 65536);
}
