//! The parallel `check` walk reports exactly what the serial walk reports,
//! over real environments with free-page state (a free tree, the meta's
//! free-list annex, overflow values, named databases) and over corrupted
//! copies of them.

mod common;
use common::TempDir;

use zerodb::{check, EnvOpenOptions};

const PS: u32 = 4096;

/// xorshift64: a deterministic stream for the workload and the corruptions.
fn next(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

/// An environment after a churning workload over the main database and four
/// named ones, with inline and overflow values and deletes, so it has a
/// free tree and free pages.
fn churned_image() -> Vec<u8> {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(256 << 20);
    opts.page_size(PS);
    opts.max_dbs(8);
    let env = opts.open(dir.path()).expect("open env");
    let mut dbs = vec![env.main_database()];
    {
        let mut w = env.write_txn().unwrap();
        for i in 0..4 {
            dbs.push(
                env.create_database(&mut w, Some(format!("db{i}").as_bytes()))
                    .unwrap(),
            );
        }
        w.commit().unwrap();
    }
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for round in 0..12 {
        let mut w = env.write_txn().unwrap();
        for _ in 0..2_000 {
            let r = next(&mut x);
            let db = &dbs[(r % dbs.len() as u64) as usize];
            let key = (r % 20_000).to_be_bytes();
            if round > 2 && r.is_multiple_of(5) {
                db.delete(&mut w, &key).unwrap();
            } else {
                let len = if r.is_multiple_of(37) {
                    9_000
                } else {
                    16 + (r % 300) as usize
                };
                db.put(&mut w, &key, &vec![(r & 0xff) as u8; len]).unwrap();
            }
        }
        w.commit().unwrap();
    }
    drop(env);
    std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).expect("read data file")
}

#[test]
fn clean_environment_is_clean_at_every_thread_count() {
    let img = churned_image();
    for threads in [1, 2, 4, 8] {
        assert_eq!(
            check::check_image_with_threads(&img, PS, threads),
            Vec::<String>::new(),
            "threads = {threads}"
        );
    }
    assert_eq!(check::check_image(&img, PS), Vec::<String>::new());
}

/// Corrupt copies (byte flips in page headers and bodies, pages copied over
/// others) get the same report, in the same order, at every thread count.
#[test]
fn corrupted_environments_get_the_serial_report() {
    let img = churned_image();
    let pages = (img.len() / PS as usize) as u64;
    let mut x = 0xD1B5_4A32_D192_ED03u64;
    let mut dirty = 0;
    for case in 0..200 {
        let mut bad = img.clone();
        for _ in 0..1 + case % 3 {
            let page = 2 + next(&mut x) % (pages - 2);
            let base = (page * u64::from(PS)) as usize;
            if case % 4 == 3 {
                let to = 2 + next(&mut x) % (pages - 2);
                bad.copy_within(base..base + PS as usize, (to * u64::from(PS)) as usize);
            } else {
                // Mostly the header and the first cells, where pointers live.
                let off = (next(&mut x) % if case % 2 == 0 { 64 } else { u64::from(PS) }) as usize;
                bad[base + off] ^= (next(&mut x) as u8) | 1;
            }
        }
        let serial = check::check_image_with_threads(&bad, PS, 1);
        if !serial.is_empty() {
            dirty += 1;
        }
        for threads in [2, 4] {
            assert_eq!(
                check::check_image_with_threads(&bad, PS, threads),
                serial,
                "case {case}, threads {threads}"
            );
        }
    }
    assert!(dirty > 50, "too few corruptions were detected ({dirty})");
}
