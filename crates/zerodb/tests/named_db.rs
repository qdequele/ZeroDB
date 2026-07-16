//! Milestone 1.6 named-database integration tests over **real files**: the
//! catalog (main DB) holding `F_SUBDATA` sub-DB records (SPEC 02 §6), the
//! per-DB `stat`, `clear`/`drop`, `DbsFull`, and — critically — that the
//! `check_image` invariant walk now follows every named-DB sub-tree from the
//! catalog (M1.6), so `F_SUBDATA` preservation across catalog-leaf splits is
//! verified structurally.
//!
//! Every committed image is validated with `zerodb::check::check_image`. Do not
//! weaken (CLAUDE.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvOpenOptions, Error, MdbError};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-named-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

const PS: u32 = 4096;
const MAP: usize = 16 << 20;

fn open(dir: &Path, max_dbs: u32) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(max_dbs);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

#[test]
fn create_named_dbs_persist_and_are_isolated() {
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    {
        let mut w = env.write_txn().unwrap();
        let a = env.create_database(&mut w, Some(b"alpha")).unwrap();
        let b = env.create_database(&mut w, Some(b"beta")).unwrap();
        for i in 0..200u32 {
            a.put(&mut w, format!("k{i:04}").as_bytes(), b"A").unwrap();
            b.put(&mut w, format!("k{i:04}").as_bytes(), b"B").unwrap();
        }
        w.commit().unwrap();
    }
    assert_clean(dir.path());
    // Reopen and read back through the committed catalog.
    let env2 = {
        drop(env);
        open(dir.path(), 8)
    };
    let r = env2.read_txn().unwrap();
    let a = env2.open_database(&r, Some(b"alpha")).unwrap().unwrap();
    let b = env2.open_database(&r, Some(b"beta")).unwrap().unwrap();
    assert_eq!(a.len(&r).unwrap(), 200);
    assert_eq!(b.len(&r).unwrap(), 200);
    assert_eq!(a.get(&r, b"k0100").unwrap(), Some(b"A".as_slice()));
    assert_eq!(b.get(&r, b"k0100").unwrap(), Some(b"B".as_slice()));
    // The main/catalog DB reports both sub-DB entries as its own len.
    assert_eq!(env2.main_database().len(&r).unwrap(), 2);
    // A never-created name resolves to None.
    assert!(env2.open_database(&r, Some(b"gamma")).unwrap().is_none());
}

#[test]
fn many_named_dbs_split_the_catalog_leaf_preserving_f_subdata() {
    // Create enough named DBs (with long names) to split the main/catalog leaf
    // into a multi-level tree. The check walk must still follow every sub-DB
    // (F_SUBDATA preserved across the catalog split) — a leak or a lost flag
    // would trip INV-22 / INV-18.
    let dir = TempDir::new();
    let env = open(dir.path(), 200);
    let names: Vec<String> = (0..150)
        .map(|i| format!("database-number-{i:04}"))
        .collect();
    {
        let mut w = env.write_txn().unwrap();
        for (i, n) in names.iter().enumerate() {
            let db = env.create_database(&mut w, Some(n.as_bytes())).unwrap();
            // Put a handful into each so sub-trees are non-empty.
            for j in 0..(i % 5 + 1) {
                db.put(&mut w, format!("k{j}").as_bytes(), n.as_bytes())
                    .unwrap();
            }
        }
        w.commit().unwrap();
    }
    assert_clean(dir.path());
    // The catalog (main DB) must have grown past a single leaf.
    let r = env.read_txn().unwrap();
    let main_stat = env.main_database().stat(&r).unwrap();
    assert_eq!(main_stat.entries, 150);
    assert!(
        main_stat.depth >= 2,
        "expected the catalog to split (depth {} < 2)",
        main_stat.depth
    );
    // Every DB still resolves and has its data.
    for (i, n) in names.iter().enumerate() {
        let db = env.open_database(&r, Some(n.as_bytes())).unwrap().unwrap();
        assert_eq!(db.len(&r).unwrap(), (i % 5 + 1) as u64);
    }
}

#[test]
fn stat_matches_full_walk() {
    // stat() fields are maintained counters (SPEC 02 §3.1); check_image walks
    // the tree and validates them (INV-18). A clean image after a stat read is
    // the cross-check that the counters equal the walk.
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    let mut w = env.write_txn().unwrap();
    let db = env.create_database(&mut w, Some(b"stats")).unwrap();
    for i in 0..1000u32 {
        db.put(
            &mut w,
            format!("key{i:05}").as_bytes(),
            &vec![0u8; (i % 50) as usize],
        )
        .unwrap();
    }
    let s = db.stat(&w).unwrap();
    assert_eq!(s.entries, 1000);
    assert!(s.depth >= 2, "expected a branch level, depth {}", s.depth);
    w.commit().unwrap();
    assert_clean(dir.path());
}

#[test]
fn clear_keeps_entry_drop_removes_it() {
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    {
        let mut w = env.write_txn().unwrap();
        let a = env.create_database(&mut w, Some(b"a")).unwrap();
        let b = env.create_database(&mut w, Some(b"b")).unwrap();
        for i in 0..100u32 {
            a.put(&mut w, format!("k{i:03}").as_bytes(), b"v").unwrap();
            b.put(&mut w, format!("k{i:03}").as_bytes(), b"v").unwrap();
        }
        w.commit().unwrap();
    }
    // clear "a": emptied, but the catalog entry (and thus open_database) stays.
    {
        let mut w = env.write_txn().unwrap();
        let a = env.open_database(&w, Some(b"a")).unwrap().unwrap();
        a.clear(&mut w).unwrap();
        w.commit().unwrap();
    }
    assert_clean(dir.path());
    {
        let r = env.read_txn().unwrap();
        let a = env.open_database(&r, Some(b"a")).unwrap().unwrap();
        assert_eq!(a.len(&r).unwrap(), 0);
        assert!(a.is_empty(&r).unwrap());
        // main still lists both catalog entries.
        assert_eq!(env.main_database().len(&r).unwrap(), 2);
    }
    // drop "b": entry removed, main len drops to 1, name resolves to None.
    {
        let mut w = env.write_txn().unwrap();
        let b = env.open_database(&w, Some(b"b")).unwrap().unwrap();
        b.drop_db(&mut w).unwrap();
        w.commit().unwrap();
    }
    assert_clean(dir.path());
    let r = env.read_txn().unwrap();
    assert!(env.open_database(&r, Some(b"b")).unwrap().is_none());
    assert_eq!(env.main_database().len(&r).unwrap(), 1);
}

#[test]
fn create_in_txn_then_abort_discards_it() {
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    {
        let mut w = env.write_txn().unwrap();
        let d = env.create_database(&mut w, Some(b"ephemeral")).unwrap();
        d.put(&mut w, b"k", b"v").unwrap();
        w.abort();
    }
    // Not committed: absent.
    let r = env.read_txn().unwrap();
    assert!(env.open_database(&r, Some(b"ephemeral")).unwrap().is_none());
    assert_eq!(env.main_database().len(&r).unwrap(), 0);
    drop(r);
    // Re-create the same name and commit: the dbi is reused, the entry persists.
    {
        let mut w = env.write_txn().unwrap();
        let d = env.create_database(&mut w, Some(b"ephemeral")).unwrap();
        d.put(&mut w, b"k", b"v2").unwrap();
        w.commit().unwrap();
    }
    assert_clean(dir.path());
    let r = env.read_txn().unwrap();
    let d = env.open_database(&r, Some(b"ephemeral")).unwrap().unwrap();
    assert_eq!(d.get(&r, b"k").unwrap(), Some(b"v2".as_slice()));
}

#[test]
fn dbs_full_when_catalog_capacity_exhausted() {
    // max_dbs = 3 named DBs; the 4th distinct name is DbsFull.
    let dir = TempDir::new();
    let env = open(dir.path(), 3);
    let mut w = env.write_txn().unwrap();
    env.create_database(&mut w, Some(b"one")).unwrap();
    env.create_database(&mut w, Some(b"two")).unwrap();
    env.create_database(&mut w, Some(b"three")).unwrap();
    // Re-opening an existing name is fine (no new slot).
    env.create_database(&mut w, Some(b"two")).unwrap();
    // A 4th distinct name overflows.
    let e = env.create_database(&mut w, Some(b"four")).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::DbsFull)), "got {e:?}");
    w.commit().unwrap();
    assert_clean(dir.path());
}

#[test]
fn user_key_name_collision_is_incompatible() {
    // A plain user key in the main DB that later collides with a create_database
    // name → Incompatible (SPEC 02 §6): the entry is not an F_SUBDATA record.
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    let mut w = env.write_txn().unwrap();
    env.main_database()
        .put(&mut w, b"collide", b"user")
        .unwrap();
    let e = env.create_database(&mut w, Some(b"collide")).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Incompatible)), "got {e:?}");
    // open_database on the same collision is also Incompatible.
    let e2 = env.open_database(&w, Some(b"collide")).unwrap_err();
    assert!(
        matches!(e2, Error::Mdb(MdbError::Incompatible)),
        "got {e2:?}"
    );
    w.abort();
}
