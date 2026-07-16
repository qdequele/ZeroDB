//! `dump` -> `load` -> `dump` round-trip over a zerodb env (M1.12 acceptance,
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
