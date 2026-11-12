//! Commits when the file is within a few pages of the map size and holds
//! more free pages than the meta annex lists (SPEC 05 GC-13a, GC-17): once
//! the free-list save can no longer extend the file, every page it needs
//! must come from the free pages it already holds, and the commit must
//! still succeed. The case matters when the save has stopped drawing from
//! its pools, which `just save-no-draw` forces on every commit.

mod common;
use common::TempDir;

use zerodb::{check, EnvOpenOptions};

const PS: u32 = 4096;
const MAP_PAGES: u64 = 1024;
/// Pages left between the file's end and the map's end.
const HEADROOM: u64 = 6;

fn key(i: u32) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

#[test]
fn commits_succeed_when_the_file_cannot_grow() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size((MAP_PAGES * u64::from(PS)) as usize);
    opts.page_size(PS);
    // SAFETY: only ZeroDB writes this environment's files.
    let env = unsafe { opts.open(dir.path()) }.expect("open env");
    let db = {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        for i in 0..60 {
            db.put(&mut w, &key(i), &[0xA5u8; 500]).unwrap();
        }
        w.commit().unwrap();
        db
    };

    // One value on an overflow run takes the file to `HEADROOM` pages from
    // the map's end; deleting it leaves the file that long and the run free.
    // The run is longer than the meta annex, so it is listed in free-page
    // records, which later commits drain and rewrite.
    let used = env.info().last_pgno + 1;
    let run = MAP_PAGES - used - HEADROOM - 4;
    {
        let mut w = env.write_txn().unwrap();
        db.put(
            &mut w,
            b"big",
            &vec![0x5Au8; (run * u64::from(PS)) as usize - 64],
        )
        .unwrap();
        w.commit().unwrap();
    }
    {
        let mut w = env.write_txn().unwrap();
        assert!(db.delete(&mut w, b"big").unwrap());
        w.commit().unwrap();
    }
    assert!(
        env.info().last_pgno + 1 + HEADROOM + 4 >= MAP_PAGES,
        "the file is not near the map's end: last page {}",
        env.info().last_pgno
    );

    // Each commit copies tree pages and rewrites free-page records. A save
    // that extends the file uses up the headroom within a few commits.
    for round in 0u8..80 {
        let mut w = env.write_txn().unwrap();
        for i in (0..60).filter(|i| i % 4 == u32::from(round) % 4) {
            db.put(&mut w, &key(i), &[round; 500]).unwrap();
        }
        w.commit().unwrap_or_else(|e| panic!("round {round}: {e}"));
    }

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.len(&rtxn).unwrap(), 60);
    drop(rtxn);
    drop(env);
    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    let violations = check::check_image(&bytes, PS);
    assert!(
        violations.is_empty(),
        "invariant violations: {violations:#?}"
    );
}
