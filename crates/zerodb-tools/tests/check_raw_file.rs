//! `check` on an environment that does not open walks the raw data file
//! through a read-only map of it. These tests damage a closed environment so
//! that it no longer opens, then check that the raw walk reports what the
//! file holds and leaves it byte for byte as it was.

mod common;
use common::TempDir;

use std::path::{Path, PathBuf};

use zerodb::{EnvOpenOptions, DATA_FILE_NAME};
use zerodb_core::page::{MetaPage, MetaValidity};

const PS: u32 = 4096;

/// A closed environment with a multi-level main tree, a named database and
/// overflow values. Returns its data file's path.
fn closed_env(dir: &Path) -> PathBuf {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(16 << 20);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.data_file_name(DATA_FILE_NAME);
    // SAFETY: only ZeroDB writes this environment's files.
    let env = unsafe { opts.open(dir) }.unwrap();
    let mut w = env.write_txn().unwrap();
    let named = env.create_database(&mut w, Some(b"named")).unwrap();
    for i in 0..2000u32 {
        let key = format!("key{i:05}");
        let len = if i % 97 == 0 { 9000 } else { 40 };
        env.main_database()
            .put(&mut w, key.as_bytes(), &vec![b'v'; len])
            .unwrap();
        named.put(&mut w, key.as_bytes(), b"n").unwrap();
    }
    w.commit().unwrap();
    env.prepare_for_closing().wait();
    dir.join(DATA_FILE_NAME)
}

/// Rewrite both meta slots of `data` with `edit` applied, keeping each
/// slot's free-list annex and a valid CRC.
fn edit_metas(data: &Path, edit: impl Fn(&mut MetaPage)) {
    let ps = PS as usize;
    let mut bytes = std::fs::read(data).unwrap();
    for slot in 0..2 {
        let page = &mut bytes[slot * ps..(slot + 1) * ps];
        let MetaValidity::Valid(mut meta) = MetaPage::validate(page, PS).unwrap() else {
            panic!("slot {slot} of a fresh env is valid");
        };
        let annex = MetaPage::read_annex(page, PS).unwrap();
        edit(&mut meta);
        meta.encode_with_annex(page, &annex).unwrap();
    }
    std::fs::write(data, bytes).unwrap();
}

/// Run `check`, asserting it took the raw-file walk and left the data file
/// unchanged. Returns the report.
fn raw_check(dir: &Path, data: &Path) -> String {
    let before = std::fs::read(data).unwrap();
    let (report, clean) = zerodb_tools::commands::cmd_check(dir).expect("check runs");
    assert!(report.contains("raw file"), "not the raw walk: {report}");
    assert!(!clean, "a damaged env checked clean: {report}");
    assert!(
        std::fs::read(data).unwrap() == before,
        "check modified the data file"
    );
    report
}

/// The walk reaches the trees through the map: an env whose metas name a
/// main-database root outside the file does not open, and the raw walk
/// reports that root, having read both metas and the free tree.
#[test]
fn raw_walk_reads_the_file_through_the_map() {
    let dir = TempDir::new("raw-walk");
    let data = closed_env(&dir);
    let (report, clean) = zerodb_tools::commands::cmd_check(&dir).unwrap();
    assert!(clean && report.contains("txnid="), "{report}");

    edit_metas(&data, |m| m.main_db.root = m.last_pg + 7);
    let report = raw_check(&dir, &data);
    assert!(report.contains("INV-17: tree page"), "{report}");
    // The pages of the main tree are now neither reachable nor free.
    assert!(report.contains("INV-22: page 2 "), "{report}");
}

/// Metas that decode but name a map size above the supported maximum: the
/// file is refused before it is mapped, and reported.
#[test]
fn raw_walk_reports_an_oversized_persisted_map() {
    let dir = TempDir::new("raw-mapsize");
    let data = closed_env(&dir);
    edit_metas(&data, |m| m.map_size = u64::MAX / 2);
    let report = raw_check(&dir, &data);
    assert!(report.contains("INV-1:"), "{report}");
}

/// Both meta slots torn: the walk maps the file and finds no valid meta.
#[test]
fn raw_walk_reports_torn_metas() {
    let dir = TempDir::new("raw-torn");
    let data = closed_env(&dir);
    let mut bytes = std::fs::read(&data).unwrap();
    for slot in 0..2 {
        bytes[slot * PS as usize + 64] ^= 0xFF; // the body txnid, under the CRC
    }
    std::fs::write(&data, bytes).unwrap();
    let report = raw_check(&dir, &data);
    assert!(report.contains("INV-2"), "{report}");
}

/// A file cut to less than two pages is too short to map as an env; it is
/// read instead and reported.
#[test]
fn raw_walk_reports_a_file_shorter_than_the_meta_slots() {
    let dir = TempDir::new("raw-short");
    let data = closed_env(&dir);
    let bytes = std::fs::read(&data).unwrap();
    std::fs::write(&data, &bytes[..PS as usize + 100]).unwrap();
    let report = raw_check(&dir, &data);
    assert!(report.contains("INV-1:"), "{report}");
}

/// A file cut in the middle of its pages no longer backs its high-water.
#[test]
fn raw_walk_reports_a_truncated_file() {
    let dir = TempDir::new("raw-truncated");
    let data = closed_env(&dir);
    let bytes = std::fs::read(&data).unwrap();
    assert!(bytes.len() > 8 * PS as usize);
    std::fs::write(&data, &bytes[..bytes.len() / 2 + 123]).unwrap();
    let report = raw_check(&dir, &data);
    assert!(report.contains("INV-17"), "{report}");
    assert!(report.contains("INV-3"), "{report}");
}
