//! M1.10 — write flags and modes: WRITE_MAP write path, durability flags,
//! READ_ONLY envs, and `force_sync`, exercised on real files (SPEC 01 Table 1,
//! §S6/§S7; SPEC 04 §6.4; SPEC 06 §3). Oracle *parity* lives in
//! `crates/zerodb-oracle/tests/`; these are zerodb-only end-to-end checks that
//! the modes actually read/write/persist correctly.

use zerodb::{Env, EnvFlags, EnvOpenOptions};

fn tmpdir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("zerodb-m110-{n}-{:p}", &p));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn open(dir: &std::path::Path, flags: EnvFlags) -> zerodb::Result<Env> {
    let mut o = EnvOpenOptions::new();
    o.map_size(4 << 20);
    o.max_dbs(8);
    o.page_size(4096);
    o.flags(flags);
    o.open(dir)
}

#[test]
fn writemap_put_get_reserved_persist() {
    let dir = tmpdir();
    {
        let env = open(&dir, EnvFlags::WRITE_MAP).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        for i in 0u32..500 {
            let k = i.to_be_bytes();
            db.put(&mut w, &k, format!("val-{i}").as_bytes()).unwrap();
        }
        // put_reserved into the writable map (SPEC 04 §6.4 / TXN-47).
        db.put_reserved(&mut w, b"reserved", 8, |slot| {
            slot.copy_from_slice(b"ABCDEFGH")
        })
        .unwrap();
        // large value → overflow run written into the map
        db.put(&mut w, b"big", &vec![0xEE; 40_000]).unwrap();
        w.commit().unwrap();

        let r = env.read_txn().unwrap();
        assert_eq!(
            db.get(&r, &7u32.to_be_bytes()).unwrap(),
            Some(&b"val-7"[..])
        );
        assert_eq!(db.get(&r, b"reserved").unwrap(), Some(&b"ABCDEFGH"[..]));
        assert_eq!(db.get(&r, b"big").unwrap().unwrap().len(), 40_000);
        assert_eq!(db.len(&r).unwrap(), 502);
        drop(r);
        drop(env);
    }
    // Reopen (default, non-writemap) and confirm persistence + structural check.
    {
        let env = open(&dir, EnvFlags::EMPTY).unwrap();
        let r = env.read_txn().unwrap();
        let db = env.open_database(&r, None).unwrap().unwrap();
        assert_eq!(
            db.get(&r, &123u32.to_be_bytes()).unwrap(),
            Some(&b"val-123"[..])
        );
        assert_eq!(db.len(&r).unwrap(), 502);
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn writemap_reopened_as_writemap_sees_data() {
    let dir = tmpdir();
    {
        let env = open(&dir, EnvFlags::WRITE_MAP).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"main")).unwrap();
        db.put(&mut w, b"a", b"1").unwrap();
        w.commit().unwrap();
    }
    {
        let env = open(&dir, EnvFlags::WRITE_MAP).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.open_database(&w, Some(b"main")).unwrap().unwrap();
        db.put(&mut w, b"b", b"2").unwrap();
        w.commit().unwrap();
        let r = env.read_txn().unwrap();
        assert_eq!(db.get(&r, b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(db.get(&r, b"b").unwrap(), Some(&b"2"[..]));
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn durability_nosync_and_nometasync_commit_and_persist() {
    for flags in [EnvFlags::NO_SYNC, EnvFlags::NO_META_SYNC] {
        let dir = tmpdir();
        {
            let env = open(&dir, flags).unwrap();
            let mut w = env.write_txn().unwrap();
            let db = env.create_database(&mut w, None).unwrap();
            db.put(&mut w, b"k", b"v").unwrap();
            w.commit().unwrap();
            // force_sync restores durability (SPEC 01 §S6).
            env.force_sync().unwrap();
            let r = env.read_txn().unwrap();
            assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn mapasync_writemap_commit_and_force_sync() {
    let dir = tmpdir();
    {
        let env = open(&dir, EnvFlags::WRITE_MAP | EnvFlags::MAP_ASYNC).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
        env.force_sync().unwrap();
        let r = env.read_txn().unwrap();
        assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_only_env_rejects_write_txn_and_force_sync() {
    let dir = tmpdir();
    // Seed a store first (RDONLY cannot create).
    {
        let env = open(&dir, EnvFlags::EMPTY).unwrap();
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    // Reopen RDONLY: reads work, writes are EACCES.
    {
        let env = open(&dir, EnvFlags::READ_ONLY).unwrap();
        assert!(env.is_read_only());
        let r = env.read_txn().unwrap();
        let db = env.open_database(&r, None).unwrap().unwrap();
        assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
        drop(r);

        // `RwTxn` is not `Debug`, so use `let-else` rather than `expect_err`.
        let Err(e) = env.write_txn() else {
            panic!("write_txn on RDONLY must fail");
        };
        match e {
            zerodb::Error::Io(io) => {
                assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied)
            }
            other => panic!("expected Io(PermissionDenied), got {other:?}"),
        }
        let e = env
            .force_sync()
            .expect_err("force_sync on RDONLY must fail");
        assert!(
            matches!(e, zerodb::Error::Io(ref io) if io.kind() == std::io::ErrorKind::PermissionDenied)
        );
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_only_open_of_missing_store_errors() {
    let dir = tmpdir();
    let e = open(&dir, EnvFlags::READ_ONLY).expect_err("no store to open");
    assert!(matches!(e, zerodb::Error::Io(_)));
    std::fs::remove_dir_all(&dir).ok();
}
