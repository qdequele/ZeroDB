//! Reading an environment in on-disk format 2, the format of the 0.2.0
//! release: `dump` produces the dump 0.2.0's own `zerodb-tools dump` wrote,
//! and `upgrade` produces a current-format environment holding the same data.
//!
//! The fixture `tests/fixtures/format-v2/env/zerodb.dat` was written by the
//! 0.2.0 engine (the generator is `gen_v2_fixture.rs.txt` beside it, run from
//! a checkout of the `v0.2.0` tag), and `dump-by-0.2.0.txt` is that
//! release's `zerodb-tools dump` of it. It holds a main database with inline
//! and overflow values, a multi-level named database, an empty one, one with
//! a binary name and overflow values, and a free database with format 2
//! records, one of them on an overflow run, left by commits made while a
//! read transaction pinned the first snapshot.

mod common;
use common::TempDir;

use std::path::{Path, PathBuf};

use zerodb_core::page::{MetaPage, MetaValidity, FORMAT_VERSION};
use zerodb_tools::commands::{cmd_check, cmd_dump, cmd_stat};
use zerodb_tools::legacy::{cmd_upgrade, LegacyEnv, LEGACY_FORMAT_VERSION};

const PSIZE: u32 = 4096;
const MAP: u64 = 64 << 20;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-v2")
}

/// The fixture's data file copied into a fresh directory under `name`, so no
/// test touches the checked-in file.
fn fixture_copy(tag: &str, name: &str) -> TempDir {
    let dir = TempDir::new(tag);
    std::fs::copy(fixture_dir().join("env/zerodb.dat"), dir.join(name)).unwrap();
    dir
}

fn expected_dump() -> String {
    std::fs::read_to_string(fixture_dir().join("dump-by-0.2.0.txt")).unwrap()
}

fn format_version(data: &Path) -> u32 {
    zerodb_tools::naming::probe_magic(data).unwrap().unwrap().1
}

#[test]
fn fixture_is_format_2() {
    assert_eq!(LEGACY_FORMAT_VERSION, 2);
    assert_ne!(FORMAT_VERSION, LEGACY_FORMAT_VERSION);
    assert_eq!(
        format_version(&fixture_dir().join("env/zerodb.dat")),
        LEGACY_FORMAT_VERSION
    );
    // The fixture's free database holds records, so the reader is shown to
    // skip a tree whose format changed.
    let env = LegacyEnv::open(&fixture_dir().join("env/zerodb.dat")).unwrap();
    assert!(env.meta().free_db.entries > 0);
    assert!(env.meta().free_db.overflow_pages > 0);
}

#[test]
fn dump_of_format_2_matches_the_0_2_0_dump() {
    let src = fixture_copy("v2dump", zerodb::DATA_FILE_NAME);
    assert_eq!(cmd_dump(&src).unwrap(), expected_dump());
    // Reading never writes: the file is unchanged and no lock file appears.
    assert_eq!(
        std::fs::read(src.join(zerodb::DATA_FILE_NAME)).unwrap(),
        std::fs::read(fixture_dir().join("env/zerodb.dat")).unwrap()
    );
    assert_eq!(std::fs::read_dir(&*src).unwrap().count(), 1);
}

#[test]
fn upgrade_produces_a_current_env_with_the_same_data() {
    let src = fixture_copy("v2src", zerodb::DATA_FILE_NAME);
    let dst = TempDir::new("v2dst");
    let report = cmd_upgrade(&src, &dst, None, MAP).unwrap();
    assert!(report.contains("alpha: 300 entries"), "{report}");
    assert!(report.contains("<main>: 303 entries"), "{report}");

    let data = dst.join(zerodb::DATA_FILE_NAME);
    assert_eq!(format_version(&data), FORMAT_VERSION);
    assert_eq!(cmd_dump(&dst).unwrap(), expected_dump());
    let (check, clean) = cmd_check(&dst).unwrap();
    assert!(clean, "{check}");
    let stat = cmd_stat(&dst).unwrap();
    assert!(
        stat.contains(&format!("page_size:            {PSIZE}")),
        "{stat}"
    );
}

#[test]
fn upgrade_keeps_the_heed_data_file_name() {
    let src = fixture_copy("v2heed", "data.mdb");
    let dst = TempDir::new("v2heeddst");
    cmd_upgrade(&src, &dst, None, MAP).unwrap();
    assert!(dst.join("data.mdb").exists());
    assert!(dst.join("lock.mdb").exists());
    assert!(!dst.join(zerodb::DATA_FILE_NAME).exists());
    assert_eq!(cmd_dump(&dst).unwrap(), expected_dump());
}

#[test]
fn upgrade_to_another_page_size() {
    let src = fixture_copy("v2ps", zerodb::DATA_FILE_NAME);
    let dst = TempDir::new("v2psdst");
    cmd_upgrade(&src, &dst, Some(16384), MAP).unwrap();
    assert!(cmd_stat(&dst)
        .unwrap()
        .contains("page_size:            16384"));
    assert_eq!(cmd_dump(&dst).unwrap(), expected_dump());
}

#[test]
fn upgrade_refuses_a_non_empty_destination() {
    let src = fixture_copy("v2ne", zerodb::DATA_FILE_NAME);
    let dst = TempDir::new("v2nedst");
    cmd_upgrade(&src, &dst, None, MAP).unwrap();
    let err = cmd_upgrade(&src, &dst, None, MAP).unwrap_err().to_string();
    assert!(
        err.contains("refusing to upgrade into a non-empty env"),
        "{err}"
    );
}

#[test]
fn upgrade_refuses_a_current_or_unknown_format() {
    // A current-format source: the upgrade of the fixture.
    let src = fixture_copy("v2cur", zerodb::DATA_FILE_NAME);
    let current = TempDir::new("v2curdst");
    cmd_upgrade(&src, &current, None, MAP).unwrap();
    let dst = TempDir::new("v2curdst2");
    let err = cmd_upgrade(&current, &dst, None, MAP)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("already in the current on-disk format"),
        "{err}"
    );
    assert!(!dst.join(zerodb::DATA_FILE_NAME).exists());

    // A version this tool does not read.
    let old = fixture_copy("v1", zerodb::DATA_FILE_NAME);
    set_version(&old.join(zerodb::DATA_FILE_NAME), 1);
    let err = cmd_upgrade(&old, &dst, None, MAP).unwrap_err().to_string();
    assert!(err.contains("on-disk format 1"), "{err}");
    let err = cmd_dump(&old).unwrap_err().to_string();
    assert!(!err.is_empty());
}

/// Overwrite both meta slots' `format_version` field (SPEC 02 §3, offset 36).
fn set_version(data: &Path, version: u32) {
    let mut bytes = std::fs::read(data).unwrap();
    for slot in 0..2 {
        let off = slot * PSIZE as usize + 36;
        bytes[off..off + 4].copy_from_slice(&version.to_le_bytes());
    }
    std::fs::write(data, bytes).unwrap();
}

#[test]
fn a_corrupt_page_is_an_error_not_a_panic() {
    let src = fixture_copy("v2bad", zerodb::DATA_FILE_NAME);
    let data = src.join(zerodb::DATA_FILE_NAME);
    let env = LegacyEnv::open(&data).unwrap();
    let root = env.meta().main_db.root;
    drop(env);
    // Stamp the main tree's root page with another page number.
    let mut bytes = std::fs::read(&data).unwrap();
    let off = root as usize * PSIZE as usize;
    bytes[off..off + 8].copy_from_slice(&(root + 1).to_le_bytes());
    std::fs::write(&data, bytes).unwrap();

    let err = cmd_dump(&src).unwrap_err().to_string();
    assert!(err.contains("records page number"), "{err}");
    let dst = TempDir::new("v2baddst");
    assert!(cmd_upgrade(&src, &dst, None, MAP).is_err());
}

/// A commit landing while the reader walks the trees makes the read fail:
/// the walk may have read pages that commit reused.
#[test]
fn a_commit_during_the_read_is_detected() {
    let src = fixture_copy("v2race", zerodb::DATA_FILE_NAME);
    let data = src.join(zerodb::DATA_FILE_NAME);
    let env = LegacyEnv::open(&data).unwrap();
    env.ensure_unchanged().unwrap();

    // Write a newer valid format 2 meta into the older slot, as the next
    // commit of a 0.2.0 writer would.
    let newer = env.meta().txnid + 1;
    let slot = newer & 1;
    let mut bytes = std::fs::read(&data).unwrap();
    let current = (env.meta().pgno * u64::from(PSIZE)) as usize;
    let annex = MetaPage::read_annex(&bytes[current..current + PSIZE as usize], PSIZE).unwrap();
    let page = &mut bytes[(slot * u64::from(PSIZE)) as usize..][..PSIZE as usize];
    let mut meta = *env.meta();
    meta.pgno = slot;
    meta.txnid = newer;
    meta.encode_with_annex(page, &annex).unwrap();
    assert!(matches!(
        MetaPage::validate_version(page, PSIZE, LEGACY_FORMAT_VERSION).unwrap(),
        MetaValidity::Valid(_)
    ));
    std::fs::write(&data, bytes).unwrap();

    let err = env.ensure_unchanged().unwrap_err().to_string();
    assert!(err.contains("changed while it was read"), "{err}");
}

/// A truncated file is refused before any page is read: its meta names pages
/// past the end of the file.
#[test]
fn a_truncated_file_is_refused() {
    let src = fixture_copy("v2short", zerodb::DATA_FILE_NAME);
    let data = src.join(zerodb::DATA_FILE_NAME);
    let file = std::fs::OpenOptions::new().write(true).open(&data).unwrap();
    file.set_len(file.metadata().unwrap().len() / 2).unwrap();
    let err = cmd_dump(&src).unwrap_err().to_string();
    assert!(err.contains("beyond the file"), "{err}");
}
