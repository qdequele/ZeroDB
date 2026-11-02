//! Duplicate-key (DUPSORT) spike: `dump` then `load` keeps every duplicate and
//! the database's DUPSORT flag.
//!
//! A dump lists one record per `(key, value)` pair, so a key with three
//! duplicates appears on three records, and the block header carries the
//! flag lines `mdb_dump` prints for a DUPSORT database (`duplicates=1`, then
//! `dupsort=1`; read from the vendored `mdb_dump.c`). `load` must recreate the
//! database with that flag and add every pair.
//!
//! Only built with the `dupsort-spike` feature: duplicate keys are not
//! supported by default (docs/LMDB-COMPATIBILITY.md).

#![cfg(feature = "dupsort-spike")]

mod common;
use common::TempDir;

use std::path::Path;

use zerodb::{collect_pairs, Env, EnvOpenOptions, DB_DUPSORT};

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Every pair stored in `dups`, in key then duplicate order.
fn expected_pairs() -> Pairs {
    let mut out = Vec::new();
    for i in 0u32..300 {
        for j in 0..=(i % 4) {
            out.push((
                format!("k{i:05}").into_bytes(),
                format!("v{j}").into_bytes(),
            ));
        }
    }
    out
}

fn build(dir: &Path) {
    let env: Env = EnvOpenOptions::new()
        .map_size(64 << 20)
        .max_dbs(8)
        .open(dir)
        .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let dups = env
        .create_database_with_flags(&mut wtxn, Some(b"dups"), DB_DUPSORT)
        .unwrap();
    // Inserted in reverse so the engine, not the caller, orders duplicates.
    for (k, v) in expected_pairs().into_iter().rev() {
        dups.put(&mut wtxn, &k, &v).unwrap();
    }
    let plain = env.create_database(&mut wtxn, Some(b"plain")).unwrap();
    plain.put(&mut wtxn, b"only", b"one").unwrap();
    wtxn.commit().unwrap();
}

fn read_pairs(dir: &Path) -> (Pairs, u64) {
    let env: Env = EnvOpenOptions::new()
        .map_size(64 << 20)
        .max_dbs(8)
        .open(dir)
        .unwrap();
    let rtxn = env.read_txn().unwrap();
    let dups = env.open_database(&rtxn, Some(b"dups")).unwrap().unwrap();
    (
        collect_pairs(&dups, &rtxn).unwrap(),
        dups.len(&rtxn).unwrap(),
    )
}

#[test]
fn dump_then_load_keeps_duplicates() {
    let src = TempDir::new("dup-src");
    build(&src);
    let expected = expected_pairs();
    assert_eq!(read_pairs(&src), (expected.clone(), expected.len() as u64));

    let dump1 = zerodb_tools::commands::cmd_dump(&src).unwrap();
    let dups_block = dump1
        .split("DATA=END\n")
        .find(|b| b.contains(&format!("database={}", hex(b"dups"))))
        .expect("dups block");
    assert!(
        dups_block.contains("type=btree\nduplicates=1\ndupsort=1\nHEADER=END\n"),
        "DUPSORT header lines missing:\n{dups_block}"
    );
    let records = dups_block
        .split("HEADER=END\n")
        .nth(1)
        .unwrap()
        .lines()
        .count();
    assert_eq!(
        records,
        2 * expected.len(),
        "one key line and one value line per pair"
    );

    let dump_dir = TempDir::new("dup-dumpfile");
    let dump_file = dump_dir.join("out.dump");
    std::fs::write(&dump_file, dump1.as_bytes()).unwrap();
    let dst = TempDir::new("dup-dst");
    zerodb_tools::commands::cmd_load(&dump_file, &dst, 4096, 64 << 20).unwrap();

    let (report, clean) = zerodb_tools::commands::cmd_check(&dst).unwrap();
    assert!(clean, "reloaded env failed check:\n{report}");
    assert_eq!(read_pairs(&dst), (expected.clone(), expected.len() as u64));
    let dump2 = zerodb_tools::commands::cmd_dump(&dst).unwrap();
    assert_eq!(dump1, dump2, "round-trip dump differs");

    // The reloaded database is still DUPSORT.
    let env: Env = EnvOpenOptions::new()
        .map_size(64 << 20)
        .max_dbs(8)
        .open(&dst)
        .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let dups = env.open_database(&wtxn, Some(b"dups")).unwrap().unwrap();
    dups.put(&mut wtxn, b"k00000", b"v9").unwrap();
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(dups.len(&rtxn).unwrap(), expected.len() as u64 + 1);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
