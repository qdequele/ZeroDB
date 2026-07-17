//! End-to-end smoke test of the adapter over ZeroDB (milestone 1.13). Exercises
//! the enum-dispatch read path, the pointer-based write cursor, nested readers,
//! and the env lifecycle — the load-bearing pieces of the 1:1 surface.

use heed_zerodb::types::{Bytes, Str, U32};
use heed_zerodb::{byteorder::BigEndian, Database, EnvOpenOptions};

fn env_opts() -> EnvOpenOptions<heed_zerodb::WithoutTls> {
    EnvOpenOptions::new().read_txn_without_tls()
}

#[test]
fn put_get_iter_delete() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, U32<BigEndian>> = env.create_database(&mut wtxn, Some("nums")).unwrap();
    db.put(&mut wtxn, "seven", &7).unwrap();
    db.put(&mut wtxn, "five", &5).unwrap();
    db.put(&mut wtxn, "zero", &0).unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, "five").unwrap(), Some(5));
    assert_eq!(db.get(&rtxn, "absent").unwrap(), None);
    assert_eq!(db.len(&rtxn).unwrap(), 3);
    // Ascending key order.
    let keys: Vec<String> = db
        .iter(&rtxn)
        .unwrap()
        .map(|r| r.unwrap().0.to_string())
        .collect();
    assert_eq!(keys, vec!["five", "seven", "zero"]);
    drop(rtxn);

    let mut wtxn = env.write_txn().unwrap();
    assert!(db.delete(&mut wtxn, "five").unwrap());
    assert!(!db.delete(&mut wtxn, "five").unwrap());
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.len(&rtxn).unwrap(), 2);
}

#[test]
fn read_through_write_txn_and_nested() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    db.put(&mut wtxn, b"a", b"1").unwrap();
    db.put(&mut wtxn, b"b", b"2").unwrap();

    // Reads through &wtxn see uncommitted state (RwTxn -> RoTxn deref).
    assert_eq!(db.get(&wtxn, b"a").unwrap(), Some(b"1".as_slice()));

    // A nested reader sees the same uncommitted state and is Send.
    let nested = wtxn.nested_read_txn().unwrap();
    assert_eq!(db.get(&nested, b"b").unwrap(), Some(b"2".as_slice()));
    fn assert_send<T: Send>(_: &T) {}
    assert_send(&nested);
    drop(nested);

    wtxn.commit().unwrap();
}

#[test]
fn iter_mut_del_current() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for i in 0u8..6 {
        db.put(&mut wtxn, &[i], b"v").unwrap();
    }
    // Delete even keys through the mutable cursor.
    let mut it = db.iter_mut(&mut wtxn).unwrap();
    while let Some(entry) = it.next().transpose().unwrap() {
        let (k, _) = entry;
        if k[0] % 2 == 0 {
            // SAFETY: no borrow of the entry is held across the call.
            let k0 = k[0];
            let _ = k;
            unsafe { it.del_current().unwrap() };
            let _ = k0;
        }
    }
    drop(it);
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let keys: Vec<u8> = db.iter(&rtxn).unwrap().map(|r| r.unwrap().0[0]).collect();
    assert_eq!(keys, vec![1, 3, 5]);
}

#[test]
fn static_read_txn_and_closing_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = env_opts();
    opts.map_size(1024 * 1024).max_dbs(4);
    let env = unsafe { opts.open(dir.path()).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let srtxn = env.clone().static_read_txn().unwrap();
    assert_eq!(db.get(&srtxn, b"k").unwrap(), Some(b"v".as_slice()));
    fn assert_send<T: Send>(_: &T) {}
    assert_send(&srtxn);

    let event = env.prepare_for_closing();
    // A live static read txn keeps the env open.
    assert!(!event.wait_timeout(std::time::Duration::from_millis(0)));
    drop(srtxn);
    event.wait();
}
