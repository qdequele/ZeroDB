//! Named-database differential tests: `LmdbEngine` vs the native
//! `ZerodbEngine`.
//!
//! Covers the named-database acceptance surface: multi-DB workloads
//! interleaved across the primary DB + several named DBs, create-in-txn-then-
//! abort parity, create-then-reopen persistence, `clear`/`drop` parity, and
//! overflow values in named DBs — all verified by exact `OpResult`
//! comparison against the LMDB fork. (Per-DB `stat` parity and name-edge cases
//! are `stat_differential.rs` / `name_edge_differential.rs`.)

use zerodb_oracle::{assert_matches_lmdb, DbName, Op, ZerodbEngine};

/// The five databases the harness addresses: primary ("main") + db0..db3. The
/// `db` index selects modulo the number of open DBs, so after opening all five
/// in creation order, index `i` targets creation-order DB `i`.
fn create_all() -> Vec<Op> {
    vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::create_db(DbName::Named(0)),
        Op::create_db(DbName::Named(1)),
        Op::create_db(DbName::Named(2)),
        Op::create_db(DbName::Named(3)),
    ]
}

#[test]
fn interleaved_writes_are_isolated_per_db() {
    let mut ops = create_all();
    // Interleave puts across all five DBs; each DB's keyspace is independent.
    for round in 0..40u32 {
        for db in 0..5u8 {
            let key = format!("k{round:03}");
            let val = format!("db{db}-r{round}");
            ops.push(Op::put(db, key.as_bytes(), val.as_bytes()));
        }
    }
    ops.push(Op::Commit);
    // Read every DB back: each must have exactly its own 40 entries.
    for db in 0..5u8 {
        ops.push(Op::Len { db });
        ops.push(Op::Iter { db });
        ops.push(Op::First { db });
        ops.push(Op::Last { db });
    }
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn deletes_and_clears_are_isolated() {
    let mut ops = create_all();
    for db in 0..5u8 {
        for i in 0..30u32 {
            ops.push(Op::put(db, format!("k{i:03}").as_bytes(), b"x"));
        }
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    // Clear db2 entirely; delete a band from db0; leave db1/db3/main alone.
    ops.push(Op::ClearDb { db: 3 }); // creation order: main,db0,db1,db2,db3 -> idx3 = db2
    for i in 5..15u32 {
        // db index 1 is db0
        ops.push(Op::del(1, format!("k{i:03}").as_bytes()));
    }
    ops.push(Op::Commit);
    for db in 0..5u8 {
        ops.push(Op::Len { db });
        ops.push(Op::Iter { db });
    }
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn create_in_txn_then_abort_removes_the_db() {
    // Create db0 in a txn, write to it, then abort. Re-create + commit proves
    // the name is reusable and stays absent until the create commits.
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::put(0, b"a", b"1"),
        Op::Abort,
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::verify_get(0, b"a"), // still None: not committed
        Op::put(0, b"a", b"2"),
        Op::Commit,
        Op::verify_get(0, b"a"), // now Some("2")
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn create_then_reopen_persists() {
    let mut ops = vec![Op::BeginRw, Op::create_db(DbName::Named(1))];
    for i in 0..50u32 {
        ops.push(Op::put(
            0,
            format!("key{i:04}").as_bytes(),
            format!("val{i}").as_bytes(),
        ));
    }
    ops.push(Op::Commit);
    ops.push(Op::reopen(0));
    // Reopen clears the handle table on both engines; re-open the DB by name
    // (idempotent create over the committed catalog) so later ops can address
    // it, then read every entry back.
    ops.push(Op::BeginRw);
    ops.push(Op::create_db(DbName::Named(1)));
    ops.push(Op::Commit);
    for i in 0..50u32 {
        ops.push(Op::verify_get(0, format!("key{i:04}").as_bytes()));
    }
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn drop_removes_the_named_db_and_its_data() {
    let mut ops = create_all();
    for db in 0..5u8 {
        for i in 0..20u32 {
            ops.push(Op::put(db, format!("k{i:02}").as_bytes(), b"v"));
        }
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    ops.push(Op::DropDb { db: 2 }); // creation order idx2 = db1
    ops.push(Op::Commit);
    // db1 is gone; the remaining four DBs are intact. After the drop the dbs
    // list shrinks, so indices shift — read all remaining by index.
    for db in 0..4u8 {
        ops.push(Op::Len { db });
        ops.push(Op::Iter { db });
    }
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn drop_then_recreate_same_name() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::put(0, b"old", b"1"),
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Commit,
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::put(0, b"new", b"2"),
        Op::Commit,
        Op::verify_get(0, b"old"), // gone
        Op::verify_get(0, b"new"), // present
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn clear_keeps_the_db_but_empties_it() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::put(0, b"a", b"1"),
        Op::put(0, b"b", b"2"),
        Op::Commit,
        Op::BeginRw,
        Op::ClearDb { db: 0 },
        Op::Commit,
        Op::Len { db: 0 },
        Op::IsEmpty { db: 0 },
        // Re-open by name after clear must still succeed (entry kept).
        Op::verify_get(0, b"a"),
        Op::BeginRw,
        Op::put(0, b"c", b"3"),
        Op::Commit,
        Op::Len { db: 0 },
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn overflow_values_in_named_dbs() {
    // Large (overflow) values in named DBs, to exercise the BIGDATA path
    // through the catalog-resolved record.
    let big = vec![0xABu8; 9000];
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(2)),
        Op::put(0, b"big", big.clone()),
        Op::Commit,
        Op::verify_get(0, b"big"),
        Op::get(0, b"big"),
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}
