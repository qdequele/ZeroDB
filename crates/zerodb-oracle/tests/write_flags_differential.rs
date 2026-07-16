//! M1.10 — differential tests for write flags and modes (SPEC 01 Table 1,
//! §S6/§S7). Every MUST/SHOULD-Phase-1 flag from SPEC 01 Table 1 gets a
//! differential test here (the flag-matrix acceptance in PLAN §1.10):
//!
//! | Flag | Slug (test fn) |
//! |------|----------------|
//! | `WRITE_MAP` | `env_writemap_put_get_parity`, `env_writemap_put_reserved` |
//! | `MAP_ASYNC` (+WRITE_MAP) | `durability_mapasync_writemap` |
//! | `NO_SYNC` | `durability_nosync_no_fsync` |
//! | `NO_META_SYNC` | `durability_nometasync` |
//! | `RDONLY` | `env_rdonly_rejects_write` |
//! | `NOTLS` | `flag_notls_rotxn_is_send` |
//!
//! `WRITE_MAP` / durability flags are exercised through the op harness in the
//! matching [`EngineMode`] (`run_in_mode`): both engines open with the identical
//! flag combination, so the whole read/write/reserve/iterate storm is compared
//! at parity. `RDONLY` and `NOTLS` are not expressible as op sequences (a
//! write-rejection / a `Send` move), so they are direct two-engine / compile
//! tests. The Meilisearch indexing flag-combo replay is
//! `milli_indexing_flag_combo_replay`.

use zerodb_oracle::{
    run, run_in_mode, DbName, EngineMode, Key, LmdbEngine, Op, PutFlag, Value, ZerodbEngine,
};

fn k(b: &[u8]) -> Key {
    Key(b.to_vec())
}
fn v(b: &[u8]) -> Value {
    Value(b.to_vec())
}

/// A read/write/reserve/iterate storm exercising the whole surface, used by the
/// `WRITE_MAP` parity tests. `db = 0` is the sole (unnamed→"main") DB.
fn storm() -> Vec<Op> {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
    ];
    // Ascending APPEND (SPEC 01 §S1) — the bulk-insert path writemap must match.
    for i in 0u16..80 {
        ops.push(Op::PutFlagged {
            db: 0,
            key: k(&i.to_be_bytes()),
            val: v(format!("append-{i}").as_bytes()),
            flag: PutFlag::Append,
        });
    }
    // Plain puts (overwrites + fresh), a reserved value, and a large overflow
    // value — all landing in the writable map under WRITE_MAP.
    ops.push(Op::Put {
        db: 0,
        key: k(b"zzz"),
        val: v(b"plain"),
    });
    ops.push(Op::Put {
        db: 0,
        key: k(&5u16.to_be_bytes()),
        val: v(b"overwrite"),
    });
    ops.push(Op::PutReserved {
        db: 0,
        key: k(b"reserved"),
        val: v(b"RESERVEDBYTES"),
    });
    ops.push(Op::Put {
        db: 0,
        key: k(b"big"),
        val: v(&vec![0xAB; 20_000]),
    });
    ops.push(Op::Del {
        db: 0,
        key: k(&3u16.to_be_bytes()),
    });
    ops.push(Op::Commit);
    // Read back through a fresh read txn.
    ops.push(Op::BeginRo);
    ops.push(Op::Len { db: 0 });
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Get {
        db: 0,
        key: k(&7u16.to_be_bytes()),
    });
    ops.push(Op::Get {
        db: 0,
        key: k(b"reserved"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k(b"big"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k(&3u16.to_be_bytes()),
    }); // deleted → None
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Abort);
    ops
}

#[test]
fn env_writemap_put_get_parity() {
    // Both engines under WRITE_MAP: full read/write storm compared op-by-op.
    if let Err(d) = run_in_mode::<LmdbEngine, ZerodbEngine>(&storm(), EngineMode::WRITE_MAP) {
        panic!("WRITE_MAP put/get divergence:\n{d}");
    }
}

#[test]
fn env_writemap_put_reserved() {
    // Focused on `put_reserved` into the writable map (SPEC 04 §6.4 / TXN-47):
    // reserve, overwrite same key with a new reserved value, read back.
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::PutReserved {
            db: 0,
            key: k(b"a"),
            val: v(b"first-reserved"),
        },
        Op::PutReserved {
            db: 0,
            key: k(b"b"),
            val: v(&vec![0x7F; 9000]),
        }, // overflow-sized reserve
        Op::Get {
            db: 0,
            key: k(b"a"),
        },
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"replaced-plain"),
        },
        Op::Get {
            db: 0,
            key: k(b"a"),
        },
        Op::Commit,
        Op::BeginRo,
        Op::Get {
            db: 0,
            key: k(b"a"),
        },
        Op::Get {
            db: 0,
            key: k(b"b"),
        },
        Op::Len { db: 0 },
        Op::Abort,
    ];
    if let Err(d) = run_in_mode::<LmdbEngine, ZerodbEngine>(&ops, EngineMode::WRITE_MAP) {
        panic!("WRITE_MAP put_reserved divergence:\n{d}");
    }
}

#[test]
fn durability_mapasync_writemap() {
    // WRITE_MAP | MAP_ASYNC: both engines accept the flags and commit; the
    // observable data is identical (crash windows are M1.11's concern).
    if let Err(d) = run_in_mode::<LmdbEngine, ZerodbEngine>(&storm(), EngineMode::WRITE_MAP_ASYNC) {
        panic!("WRITE_MAP|MAP_ASYNC divergence:\n{d}");
    }
}

#[test]
fn durability_nosync_no_fsync() {
    // NO_SYNC (heap+pwrite mode): both engines accept the flag and commit; the
    // committed data is identical. (That C3+C5 are skipped is asserted by the
    // CommitHook control-flow test `durability_barriers_per_flag`.)
    let mode = EngineMode {
        no_sync: true,
        ..EngineMode::DEFAULT
    };
    if let Err(d) = run_in_mode::<LmdbEngine, ZerodbEngine>(&storm(), mode) {
        panic!("NO_SYNC divergence:\n{d}");
    }
}

#[test]
fn durability_nometasync() {
    // NO_META_SYNC: fsync data, skip meta fsync (SPEC 01 §S6). Control-flow
    // parity — both commit; data identical.
    let mode = EngineMode {
        no_meta_sync: true,
        ..EngineMode::DEFAULT
    };
    if let Err(d) = run_in_mode::<LmdbEngine, ZerodbEngine>(&storm(), mode) {
        panic!("NO_META_SYNC divergence:\n{d}");
    }
}

// ---------------------------------------------------------------------------
// RDONLY — direct two-engine parity (a write-rejection is not an op sequence).
// ---------------------------------------------------------------------------

#[test]
fn env_rdonly_rejects_write() {
    use zerodb_oracle::tempdir::TempDir;

    // --- reference (heed / the fork) ---
    let ref_kind = {
        use heed::types::Bytes;
        use heed::{Database, EnvFlags, EnvOpenOptions};
        let dir = TempDir::new().unwrap();
        {
            let mut o = EnvOpenOptions::new().read_txn_without_tls();
            o.map_size(1 << 20);
            o.max_dbs(4);
            let env = unsafe { o.open(dir.path()) }.unwrap();
            let mut w = env.write_txn().unwrap();
            let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
            db.put(&mut w, b"k", b"v").unwrap();
            w.commit().unwrap();
        }
        let mut o = EnvOpenOptions::new().read_txn_without_tls();
        o.map_size(1 << 20);
        o.max_dbs(4);
        unsafe { o.flags(EnvFlags::READ_ONLY) };
        let env = unsafe { o.open(dir.path()) }.unwrap();
        // reads still work
        {
            let r = env.read_txn().unwrap();
            let db: Database<Bytes, Bytes> = env.open_database(&r, None).unwrap().unwrap();
            assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
        }
        let kind = match env.write_txn() {
            Ok(_) => panic!("fork: RDONLY write_txn unexpectedly succeeded"),
            Err(heed::Error::Io(io)) => io.kind(),
            Err(other) => panic!("fork: unexpected RDONLY write error {other:?}"),
        };
        kind
    };

    // --- native (zerodb) ---
    let zerodb_kind = {
        use zerodb::{EnvFlags, EnvOpenOptions};
        let dir = TempDir::new().unwrap();
        {
            let mut o = EnvOpenOptions::new();
            o.map_size(1 << 20);
            o.max_dbs(4);
            o.page_size(4096);
            let env = o.open(dir.path()).unwrap();
            let mut w = env.write_txn().unwrap();
            let db = env.create_database(&mut w, None).unwrap();
            db.put(&mut w, b"k", b"v").unwrap();
            w.commit().unwrap();
        }
        let mut o = EnvOpenOptions::new();
        o.map_size(1 << 20);
        o.max_dbs(4);
        o.page_size(4096);
        o.flags(EnvFlags::READ_ONLY);
        let env = o.open(dir.path()).unwrap();
        {
            let r = env.read_txn().unwrap();
            let db = env.open_database(&r, None).unwrap().unwrap();
            assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
        }
        let kind = match env.write_txn() {
            Ok(_) => panic!("zerodb: RDONLY write_txn unexpectedly succeeded"),
            Err(zerodb::Error::Io(io)) => io.kind(),
            Err(other) => panic!("zerodb: unexpected RDONLY write error {other:?}"),
        };
        kind
    };

    // Both surface the fork's EACCES as Io(PermissionDenied) (os error 13).
    assert_eq!(ref_kind, std::io::ErrorKind::PermissionDenied);
    assert_eq!(
        zerodb_kind, ref_kind,
        "RDONLY write-rejection error kind must match the fork"
    );
}

// ---------------------------------------------------------------------------
// NOTLS — RoTxn is Send (SPEC 01 Table 1, TXN-13). Compile assert + real move.
// ---------------------------------------------------------------------------

#[test]
fn flag_notls_rotxn_is_send() {
    fn assert_send<T: Send>() {}
    // The fork opens WithoutTls everywhere (SPEC 00 rows 2/29); zerodb makes it
    // the default/only mode, so `RoTxn: Send`.
    assert_send::<zerodb::RoTxn<'static>>();
    assert_send::<heed::RoTxn<'static, heed::WithoutTls>>();

    // Real move across a thread boundary: begin a read txn, hand it to another
    // thread, read there.
    use zerodb_oracle::tempdir::TempDir;
    let dir = TempDir::new().unwrap();
    let mut o = zerodb::EnvOpenOptions::new();
    o.map_size(1 << 20);
    o.max_dbs(4);
    o.page_size(4096);
    let env = o.open(dir.path()).unwrap();
    {
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        db.put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    // `static_read_txn` yields an env-owning `RoTxn<'static>` (SPEC 04 TXN-24),
    // which — being `Send` under NOTLS — can move into a `'static` thread.
    let r = env.clone().static_read_txn().unwrap();
    let db = {
        let probe = env.read_txn().unwrap();
        env.open_database(&probe, None).unwrap().unwrap()
    };
    let handle = std::thread::spawn(move || db.get(&r, b"k").unwrap().map(<[u8]>::to_vec));
    assert_eq!(handle.join().unwrap().as_deref(), Some(&b"v"[..]));
}

// ---------------------------------------------------------------------------
// PLAN §1.10 acceptance: Meilisearch's exact indexing flag combo replayed.
// ---------------------------------------------------------------------------

#[test]
fn milli_indexing_flag_combo_replay() {
    // milli opens the env `WithoutTls` (both engines' default), with a large
    // `map_size` and many named DBs, then indexes: `put` / `put_with_flags`
    // (APPEND) in sorted order, `del`, `clear`, and **nested reads mid-txn**
    // (fanned out to rayon workers, SPEC 04 §5). The op harness runs this exact
    // combo on both engines and compares every result (values, order, errors,
    // nested-read snapshots). The harness is `WithoutTls` + default durability —
    // milli's real indexing flag combination (WRITE_MAP is milli's *experimental*
    // gate, covered separately by the WRITE_MAP tests above).
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(0),
        }, // e.g. "word-docids"
        Op::CreateDb {
            name: DbName::Named(1),
        }, // e.g. "documents"
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // milli's named "main"
    ];
    // Bulk APPEND word postings in strict ascending order (milli's grenad-sorted
    // output → APPEND loop).
    for i in 0u16..64 {
        ops.push(Op::PutFlagged {
            db: 0,
            key: k(&i.to_be_bytes()),
            val: v(format!("posting-{i}").as_bytes()),
            flag: PutFlag::Append,
        });
    }
    // Documents DB: plain puts (out-of-order keys).
    for &d in &[9u16, 2, 40, 3, 17, 8] {
        ops.push(Op::Put {
            db: 1,
            key: k(&d.to_be_bytes()),
            val: v(b"doc"),
        });
    }
    // Mid-txn nested reads (the fan-out): open a nested reader, read uncommitted
    // state from both DBs, close it, resume the writer.
    ops.push(Op::BeginNestedRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Get {
        db: 1,
        key: k(&40u16.to_be_bytes()),
    });
    ops.push(Op::EndNestedRo);
    // Deletes + a clear (prune a DB), then more writes.
    ops.push(Op::Del {
        db: 0,
        key: k(&10u16.to_be_bytes()),
    });
    ops.push(Op::Del {
        db: 1,
        key: k(&2u16.to_be_bytes()),
    });
    ops.push(Op::ClearDb { db: 2 });
    ops.push(Op::Put {
        db: 2,
        key: k(b"key"),
        val: v(b"main-val"),
    });
    // Second nested-read fan-out after mutations.
    ops.push(Op::BeginNestedRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Iter { db: 1 });
    ops.push(Op::Get {
        db: 0,
        key: k(&10u16.to_be_bytes()),
    }); // deleted → None
    ops.push(Op::EndNestedRo);
    ops.push(Op::Commit);
    // Post-commit verification through fresh read txns.
    ops.push(Op::BeginRo);
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Len { db: 1 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Iter { db: 1 });
    ops.push(Op::Iter { db: 2 });
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Abort);

    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("milli indexing flag-combo divergence:\n{d}");
    }
}
