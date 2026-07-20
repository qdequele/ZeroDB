//! Milestone 2.8a — the ADR-0011 Q5 pre-implementation DUPSORT pin list
//! (heed-reachable portion; the FFI-only portion is `dup_pin_ffi.rs`).
//!
//! Every test here observes the Meilisearch LMDB fork (heed =0.22.1 /
//! lmdb-master-sys 0.2.6, `mdb.master.nested-rtxns`) and pins exactly what it
//! returns, BEFORE any zerodb dup code exists (CLAUDE.md rule 1; ADR-0011
//! Decision 7.1 / Q5). The observed tables are transcribed into
//! `docs/SPEC/03-btree.md` §12 in the same change. Do NOT weaken these
//! assertions: they encode observed LMDB behavior, and a mismatch is a
//! zerodb/spec bug for a human to adjudicate (CLAUDE.md rule 2).
//!
//! Pin protocol: each test builds an observation table (one `id = value` line
//! per probe) and compares it against the pinned constant. A behavior change in
//! the fork (or a wrong guess) shows up as a full-table diff.

#![allow(deprecated)] // INTEGER_DUP/INTEGER_KEY: the persisted-bit path is exactly what 2.8a pins

use heed::types::Bytes;
use heed::{Database, DatabaseFlags, EnvOpenOptions, PutFlags};
use zerodb_oracle::tempdir::TempDir;

type BDb = Database<Bytes, Bytes>;

const MAP_SIZE: usize = 32 << 20; // multiple of the 16 KiB macOS page (D-006)

fn open_env(dir: &std::path::Path) -> heed::Env<heed::WithoutTls> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(MAP_SIZE);
    opts.max_dbs(8);
    unsafe { opts.open(dir) }.expect("open env")
}

/// Render a heed result compactly and stably for table pinning.
fn obs<T>(r: heed::Result<T>, ok: impl FnOnce(T) -> String) -> String {
    match r {
        Ok(v) => ok(v),
        Err(heed::Error::Mdb(e)) => format!("Err(Mdb({e:?}))"),
        Err(heed::Error::Io(e)) => format!("Err(Io({:?}))", e.kind()),
        Err(other) => format!("Err({other:?})"),
    }
}

fn ok_unit(_: ()) -> String {
    "Ok".into()
}

fn ok_bool(b: bool) -> String {
    format!("Ok({b})")
}

fn ok_val(v: Option<&[u8]>) -> String {
    match v {
        None => "Ok(None)".into(),
        Some(b) if b.len() <= 16 => format!("Ok({})", String::from_utf8_lossy(b)),
        Some(b) => format!("Ok(len={})", b.len()),
    }
}

fn stat_line(s: &heed::DatabaseStat) -> String {
    format!(
        "entries={} depth={} branch={} leaf={} overflow={}",
        s.entries, s.depth, s.branch_pages, s.leaf_pages, s.overflow_pages
    )
}

struct Table(Vec<String>);

impl Table {
    fn new() -> Self {
        Table(Vec::new())
    }
    fn row(&mut self, id: &str, val: impl Into<String>) {
        self.0.push(format!("{id} = {}", val.into()));
    }
    fn assert_pinned(&self, expected: &str) {
        let got = self.0.join("\n");
        let expected = expected.trim();
        assert_eq!(
            got, expected,
            "\n--- observed (fork) ---\n{got}\n--- pinned ---\n{expected}\n"
        );
    }
}

fn dup_db(env: &heed::Env<heed::WithoutTls>, wtxn: &mut heed::RwTxn, name: &str) -> BDb {
    env.database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT)
        .name(name)
        .create(wtxn)
        .expect("create dup db")
}

fn plain_db(env: &heed::Env<heed::WithoutTls>, wtxn: &mut heed::RwTxn, name: &str) -> BDb {
    env.create_database(wtxn, Some(name)).expect("create db")
}

fn dups(db: &BDb, txn: &heed::RoTxn, key: &[u8]) -> String {
    match db.get_duplicates(txn, key) {
        Ok(None) => "None".into(),
        Ok(Some(it)) => {
            let mut vals: Vec<String> = Vec::new();
            for r in it {
                match r {
                    Ok((_, v)) => vals.push(String::from_utf8_lossy(v).into_owned()),
                    // A mid-iteration error (e.g. FIRST_DUP on a non-dup DB
                    // -> Incompatible) is itself a pinned observation.
                    Err(e) => {
                        vals.push(obs::<()>(Err(e), |_| unreachable!()));
                        break;
                    }
                }
            }
            format!("[{}]", vals.join(","))
        }
        Err(e) => obs::<()>(Err(e), |_| unreachable!()),
    }
}

// ---------------------------------------------------------------------------
// Pin 1 — dup value size bound (ADR-0011 Decision 1: expected = the 511-byte
// key bound because dup values become sub-tree keys; SPEC 01 §S4 dup row).
// ---------------------------------------------------------------------------
#[test]
fn pin_dup_value_size_bound() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = dup_db(&env, &mut wtxn, "dup");
    let plain = plain_db(&env, &mut wtxn, "plain");

    let mut t = Table::new();
    for len in [0usize, 1, 510, 511, 512, 513, 4096, 65536] {
        let val = vec![b'v'; len];
        t.row(
            &format!("dup_put_val_len_{len}"),
            obs(db.put(&mut wtxn, b"k", &val), ok_unit),
        );
    }
    // Control: the same lengths are fine on a non-dup DB (inline or overflow).
    for len in [512usize, 65536] {
        let val = vec![b'v'; len];
        t.row(
            &format!("plain_put_val_len_{len}"),
            obs(plain.put(&mut wtxn, b"k", &val), ok_unit),
        );
    }
    // Max key together with max dup value.
    let k511 = vec![b'k'; 511];
    let v511 = vec![b'w'; 511];
    t.row(
        "dup_put_key511_val511",
        obs(db.put(&mut wtxn, &k511, &v511), ok_unit),
    );
    wtxn.abort();

    t.assert_pinned(
        r#"
dup_put_val_len_0 = Ok
dup_put_val_len_1 = Ok
dup_put_val_len_510 = Ok
dup_put_val_len_511 = Ok
dup_put_val_len_512 = Err(Mdb(BadValSize))
dup_put_val_len_513 = Err(Mdb(BadValSize))
dup_put_val_len_4096 = Err(Mdb(BadValSize))
dup_put_val_len_65536 = Err(Mdb(BadValSize))
plain_put_val_len_512 = Ok
plain_put_val_len_65536 = Ok
dup_put_key511_val511 = Ok
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 2 — Database::stat folding: entries = PAIRS or KEYS? How do
// depth/branch/leaf counts move as one key's dup set grows across the
// sub-page → sub-tree promotion boundary, and back down (demotion)?
// (ADR-0011 Decision 1 "Stat accounting" + the demotion open item.)
// ---------------------------------------------------------------------------
#[test]
fn pin_stat_folding_and_growth() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = dup_db(&env, &mut wtxn, "dup");

    let mut t = Table::new();

    // Sub-page scale: k1 -> 3 dups, k2 -> 1 dup.
    for v in [b"a1", b"a2", b"a3"] {
        db.put(&mut wtxn, b"k1", v).unwrap();
    }
    db.put(&mut wtxn, b"k2", b"b1").unwrap();
    t.row("small_stat", stat_line(&db.stat(&wtxn).unwrap()));
    t.row("small_len", format!("{}", db.len(&wtxn).unwrap()));

    // Grow k1's dup set well past a 4 KiB sub-page (the fork's page size on
    // Linux; 16 KiB on macOS — use ~40 KiB of dups so promotion happens on
    // both): 400 dups x ~100 bytes.
    for i in 0..400u32 {
        let v = format!("value-{i:06}-{}", "x".repeat(90));
        db.put(&mut wtxn, b"k1", v.as_bytes()).unwrap();
    }
    t.row("grown_stat", stat_line(&db.stat(&wtxn).unwrap()));
    t.row("grown_len", format!("{}", db.len(&wtxn).unwrap()));

    // Demotion probe: delete the grown dups again (keep the original 3).
    for i in 0..400u32 {
        let v = format!("value-{i:06}-{}", "x".repeat(90));
        assert!(db
            .delete_one_duplicate(&mut wtxn, b"k1", v.as_bytes())
            .unwrap());
    }
    t.row("shrunk_stat", stat_line(&db.stat(&wtxn).unwrap()));

    // All the way down to a single dup for k1.
    db.delete_one_duplicate(&mut wtxn, b"k1", b"a2").unwrap();
    db.delete_one_duplicate(&mut wtxn, b"k1", b"a3").unwrap();
    t.row("single_dup_stat", stat_line(&db.stat(&wtxn).unwrap()));
    t.row("single_dup_get", obs(db.get(&wtxn, b"k1"), ok_val));
    wtxn.abort();

    // PINNED FINDING (ADR-0011 Decision 1 "Stat accounting"): `entries`
    // counts PAIRS, but branch/leaf/overflow page counts and `depth` cover
    // the MAIN tree only — promoted dup sub-tree pages (~40 KiB of dups
    // here) are NOT folded into the parent DB's stat, and `depth` stays the
    // main-tree depth.
    t.assert_pinned(
        r#"
small_stat = entries=4 depth=1 branch=0 leaf=1 overflow=0
small_len = 4
grown_stat = entries=404 depth=1 branch=0 leaf=1 overflow=0
grown_len = 404
shrunk_stat = entries=4 depth=1 branch=0 leaf=1 overflow=0
single_dup_stat = entries=2 depth=1 branch=0 leaf=1 overflow=0
single_dup_get = Ok(a1)
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 3 — error taxonomy: dup-only ops issued on a NON-dup DB, and
// non-dup-compatible ops on a dup DB (ADR-0011 Decision 4/5 "pinned, not
// guessed"; the fuzz driver classifies these as COMPARED errors).
// ---------------------------------------------------------------------------
#[test]
fn pin_error_taxonomy_nondup_vs_dup() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let dup = dup_db(&env, &mut wtxn, "dup");
    let plain = plain_db(&env, &mut wtxn, "plain");
    plain.put(&mut wtxn, b"k", b"v0").unwrap();
    plain.put(&mut wtxn, b"m", b"z0").unwrap();
    dup.put(&mut wtxn, b"k", b"v0").unwrap();
    dup.put(&mut wtxn, b"k", b"v1").unwrap();

    let mut t = Table::new();

    // get_duplicates on a non-dup DB with an EXISTING key. heed drives
    // MDB_SET + (GET_CURRENT, NEXT_DUP...); the FFI pin shows NEXT_DUP on a
    // non-dup DB degenerates to NEXT — does the "dup iteration" therefore
    // walk into the following KEYS?
    t.row("nondup_get_duplicates_existing", dups(&plain, &wtxn, b"k"));
    t.row(
        "nondup_get_duplicates_missing",
        dups(&plain, &wtxn, b"missing"),
    );

    // NODUPDATA put on a NON-dup DB (lmdb.h says "may only be specified if
    // MDB_DUPSORT" — what actually happens): fresh key / exact pair /
    // existing key with a new value.
    t.row(
        "nondup_put_nodupdata_fresh_key",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k2", b"v"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_put_nodupdata_exact_pair",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k2", b"v"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_put_nodupdata_new_value",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k2", b"OTHER"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_get_after_nodupdata_new_value",
        obs(plain.get(&wtxn, b"k2"), ok_val),
    );

    // APPENDDUP on a NON-dup DB: ascending fresh key / out-of-order key /
    // exact pair on the last key.
    t.row(
        "nondup_put_appenddup_ascending",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::APPEND_DUP, b"zz", b"v"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_put_appenddup_out_of_order",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::APPEND_DUP, b"aa", b"v"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_put_appenddup_exact_last_pair",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::APPEND_DUP, b"zz", b"v"),
            ok_unit,
        ),
    );
    t.row(
        "nondup_put_appenddup_last_key_new_value",
        obs(
            plain.put_with_flags(&mut wtxn, PutFlags::APPEND_DUP, b"zz", b"w"),
            ok_unit,
        ),
    );
    t.row("nondup_get_zz", obs(plain.get(&wtxn, b"zz"), ok_val));

    // mdb_del with a data argument on a non-dup DB ("data is ignored" per
    // lmdb.h — pinned: the key is deleted regardless of the value bytes).
    t.row(
        "nondup_delete_one_dup_matching_val",
        obs(plain.delete_one_duplicate(&mut wtxn, b"k", b"v0"), ok_bool),
    );
    plain.put(&mut wtxn, b"k", b"v0").unwrap();
    t.row(
        "nondup_delete_one_dup_wrong_val",
        obs(
            plain.delete_one_duplicate(&mut wtxn, b"k", b"NOT-THE-VALUE"),
            ok_bool,
        ),
    );
    t.row(
        "nondup_get_after_del_wrong_val",
        obs(plain.get(&wtxn, b"k"), ok_val),
    );

    // RESERVE on a DUP db: lmdb.h says "must not be specified if DUPSORT",
    // but the fork does not reject it — pin what it stores.
    t.row(
        "dup_put_reserved",
        obs(
            dup.put_reserved(&mut wtxn, b"r", 4, |b| {
                use std::io::Write;
                b.write_all(b"abcd")
            }),
            ok_unit,
        ),
    );
    t.row("dup_put_reserved_readback", dups(&dup, &wtxn, b"r"));
    wtxn.abort();

    // PINNED FINDINGS: on a NON-dup DB the dup-only put flags NODUPDATA and
    // APPENDDUP are silently IGNORED (the put behaves as a plain overwrite
    // put — no EINVAL, no KeyExist, no order check), and `mdb_del`'s data
    // argument is ignored (the key is deleted whatever bytes are passed).
    // heed's `get_duplicates` on a non-dup DB surfaces the fork's
    // FIRST_DUP -> MDB_INCOMPATIBLE as a mid-iteration error. RESERVE on a
    // dup DB (documented "must not be specified with DUPSORT") is accepted
    // and stores the reserved bytes as an ordinary dup value.
    t.assert_pinned(
        r#"
nondup_get_duplicates_existing = [Err(Mdb(Incompatible))]
nondup_get_duplicates_missing = None
nondup_put_nodupdata_fresh_key = Ok
nondup_put_nodupdata_exact_pair = Ok
nondup_put_nodupdata_new_value = Ok
nondup_get_after_nodupdata_new_value = Ok(OTHER)
nondup_put_appenddup_ascending = Ok
nondup_put_appenddup_out_of_order = Ok
nondup_put_appenddup_exact_last_pair = Ok
nondup_put_appenddup_last_key_new_value = Ok
nondup_get_zz = Ok(w)
nondup_delete_one_dup_matching_val = Ok(true)
nondup_delete_one_dup_wrong_val = Ok(true)
nondup_get_after_del_wrong_val = Ok(None)
dup_put_reserved = Ok
dup_put_reserved_readback = [abcd]
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 4 — NODUPDATA put + delete(key) = delete-ALL-dups semantics
// (ADR-0011 Decision 5).
// ---------------------------------------------------------------------------
#[test]
fn pin_nodupdata_put_del() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = dup_db(&env, &mut wtxn, "dup");

    let mut t = Table::new();
    t.row(
        "nodup_first_pair",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k", b"v1"),
            ok_unit,
        ),
    );
    t.row(
        "nodup_second_distinct",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k", b"v2"),
            ok_unit,
        ),
    );
    t.row(
        "nodup_exact_pair_again",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::NO_DUP_DATA, b"k", b"v1"),
            ok_unit,
        ),
    );
    t.row("len_after", format!("{}", db.len(&wtxn).unwrap()));

    // Plain put of the exact existing pair (no flags): error or no-op?
    t.row(
        "plain_put_exact_pair",
        obs(db.put(&mut wtxn, b"k", b"v1"), ok_unit),
    );
    t.row("len_after_exact_put", format!("{}", db.len(&wtxn).unwrap()));

    // NOOVERWRITE on a dup DB with an existing key but a NEW value.
    t.row(
        "nooverwrite_existing_key_new_val",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::NO_OVERWRITE, b"k", b"v3"),
            ok_unit,
        ),
    );

    // delete(key) with no value: removes ALL dups.
    db.put(&mut wtxn, b"other", b"x").unwrap();
    t.row("delete_key", obs(db.delete(&mut wtxn, b"k"), ok_bool));
    t.row(
        "len_after_delete_all",
        format!("{}", db.len(&wtxn).unwrap()),
    );
    t.row("get_after_delete_all", obs(db.get(&wtxn, b"k"), ok_val));

    // delete_one_duplicate: exact pair vs absent pair.
    db.put(&mut wtxn, b"k", b"v1").unwrap();
    db.put(&mut wtxn, b"k", b"v2").unwrap();
    t.row(
        "del_one_dup_exact",
        obs(db.delete_one_duplicate(&mut wtxn, b"k", b"v1"), ok_bool),
    );
    t.row(
        "del_one_dup_absent",
        obs(db.delete_one_duplicate(&mut wtxn, b"k", b"nope"), ok_bool),
    );
    t.row("dups_after_del_one", dups(&db, &wtxn, b"k"));
    wtxn.abort();

    t.assert_pinned(
        r#"
nodup_first_pair = Ok
nodup_second_distinct = Ok
nodup_exact_pair_again = Err(Mdb(KeyExist))
len_after = 2
plain_put_exact_pair = Ok
len_after_exact_put = 2
nooverwrite_existing_key_new_val = Err(Mdb(KeyExist))
delete_key = Ok(true)
len_after_delete_all = 1
get_after_delete_all = Ok(None)
del_one_dup_exact = Ok(true)
del_one_dup_absent = Ok(false)
dups_after_del_one = [v2]
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 5 — APPENDDUP: the §S1 analogue — compares only against the CURRENT
// LAST dup of the key; `new <= last` (equal included) -> KeyExist; the first
// dup of a key always succeeds (ADR-0011 Decision 5).
// ---------------------------------------------------------------------------
#[test]
fn pin_appenddup_rules() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = dup_db(&env, &mut wtxn, "dup");

    let mut t = Table::new();
    let ad = PutFlags::APPEND_DUP;
    t.row(
        "first_dup_new_key",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"m"), ok_unit),
    );
    t.row(
        "greater_than_last",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"p"), ok_unit),
    );
    t.row(
        "equal_to_last",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"p"), ok_unit),
    );
    t.row(
        "less_than_last",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"a"), ok_unit),
    );
    t.row(
        "less_than_last_but_gt_first",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"n"), ok_unit),
    );
    t.row("dups_now", dups(&db, &wtxn, b"k"));

    // APPENDDUP on an EARLIER key than the last key in the DB (cursor position
    // question: does APPENDDUP alone seek to the key's own dup set?).
    db.put(&mut wtxn, b"z", b"zv").unwrap();
    t.row(
        "appenddup_on_earlier_key",
        obs(db.put_with_flags(&mut wtxn, ad, b"k", b"q"), ok_unit),
    );

    // APPEND (key-level) with equal key on a dup DB: allowed with a greater
    // dup value? (fork §S1 note: APPEND on dupsort + equal key.)
    t.row(
        "append_equal_key_greater_dup",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::APPEND, b"z", b"zw"),
            ok_unit,
        ),
    );
    t.row(
        "append_equal_key_smaller_dup",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::APPEND, b"z", b"aa"),
            ok_unit,
        ),
    );
    // APPEND|APPENDDUP combination.
    t.row(
        "append_and_appenddup_new_key",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::APPEND | ad, b"zz", b"1"),
            ok_unit,
        ),
    );
    t.row(
        "append_and_appenddup_equal_key_greater_dup",
        obs(
            db.put_with_flags(&mut wtxn, PutFlags::APPEND | ad, b"zz", b"2"),
            ok_unit,
        ),
    );
    wtxn.abort();

    t.assert_pinned(
        r#"
first_dup_new_key = Ok
greater_than_last = Ok
equal_to_last = Err(Mdb(KeyExist))
less_than_last = Err(Mdb(KeyExist))
less_than_last_but_gt_first = Err(Mdb(KeyExist))
dups_now = [m,p]
appenddup_on_earlier_key = Ok
append_equal_key_greater_dup = Err(Mdb(KeyExist))
append_equal_key_smaller_dup = Err(Mdb(KeyExist))
append_and_appenddup_new_key = Ok
append_and_appenddup_equal_key_greater_dup = Err(Mdb(KeyExist))
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 6 — persisted-flag mismatch at open (SPEC 01 §S8 item 8): exact error
// for open-without-flags of a DUPSORT DB, open-with-DUPSORT of a plain DB,
// and reopen across env restarts.
// ---------------------------------------------------------------------------
#[test]
fn pin_flags_persistence_open_mismatch() {
    let dir = TempDir::new().unwrap();
    let mut t = Table::new();
    {
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let dup = dup_db(&env, &mut wtxn, "dup");
        dup.put(&mut wtxn, b"k", b"v").unwrap();
        plain_db(&env, &mut wtxn, "plain");
        wtxn.commit().unwrap();

        // Same env instance, same process: open with mismatched flags.
        let rtxn = env.read_txn().unwrap();
        t.row(
            "same_env_open_dup_without_flags",
            obs(env.open_database::<Bytes, Bytes>(&rtxn, Some("dup")), |o| {
                format!("Ok(is_some={})", o.is_some())
            }),
        );
        drop(rtxn);
        let mut wtxn = env.write_txn().unwrap();
        t.row(
            "same_env_create_dup_without_flags",
            obs(
                env.create_database::<Bytes, Bytes>(&mut wtxn, Some("dup")),
                |_| "Ok".into(),
            ),
        );
        t.row(
            "same_env_create_plain_with_dupsort",
            obs(
                env.database_options()
                    .types::<Bytes, Bytes>()
                    .flags(DatabaseFlags::DUP_SORT)
                    .name("plain")
                    .create(&mut wtxn),
                |_| "Ok".into(),
            ),
        );
        wtxn.abort();
    }
    // Fresh env open (fresh dbi table — the persisted-flag path).
    {
        let env = open_env(dir.path());
        let rtxn = env.read_txn().unwrap();
        t.row(
            "reopen_open_dup_without_flags",
            obs(env.open_database::<Bytes, Bytes>(&rtxn, Some("dup")), |o| {
                format!("Ok(is_some={})", o.is_some())
            }),
        );
        drop(rtxn);
        let mut wtxn = env.write_txn().unwrap();
        t.row(
            "reopen_create_dup_without_flags",
            obs(
                env.create_database::<Bytes, Bytes>(&mut wtxn, Some("dup")),
                |_| "Ok".into(),
            ),
        );
        t.row(
            "reopen_create_plain_with_dupsort",
            obs(
                env.database_options()
                    .types::<Bytes, Bytes>()
                    .flags(DatabaseFlags::DUP_SORT)
                    .name("plain")
                    .create(&mut wtxn),
                |_| "Ok".into(),
            ),
        );
        // Matching flags on reopen: fine, and data is there.
        let redup = env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT)
            .name("dup")
            .create(&mut wtxn)
            .unwrap();
        t.row(
            "reopen_matching_flags_get",
            obs(redup.get(&wtxn, b"k"), ok_val),
        );
        // Subset/superset probe: DUPSORT DB opened as DUPSORT|REVERSEDUP.
        t.row(
            "reopen_dup_with_extra_reversedup",
            obs(
                env.database_options()
                    .types::<Bytes, Bytes>()
                    .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::REVERSE_DUP)
                    .name("dup")
                    .create(&mut wtxn),
                |_| "Ok".into(),
            ),
        );
        wtxn.abort();
    }
    // WHICH flags govern the handle after a mismatched open? (The fork's
    // `mdb_dbi_open` copies the PERSISTED MDB_db into the slot and ignores
    // the requested flags for an existing DB — probe the observable side.)
    {
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        // "plain" requested as DUPSORT: does a double-put make 1 or 2 entries?
        let plain_as_dup = env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT)
            .name("plain")
            .create(&mut wtxn)
            .unwrap();
        plain_as_dup.put(&mut wtxn, b"p", b"1").unwrap();
        plain_as_dup.put(&mut wtxn, b"p", b"2").unwrap();
        t.row(
            "plain_opened_as_dup_double_put_len",
            format!("{}", plain_as_dup.len(&wtxn).unwrap()),
        );
        t.row(
            "plain_opened_as_dup_get",
            obs(plain_as_dup.get(&wtxn, b"p"), ok_val),
        );
        wtxn.abort();
    }
    {
        // "dup" opened with NO flags: does it still behave DUPSORT?
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let dup_as_plain = env
            .create_database::<Bytes, Bytes>(&mut wtxn, Some("dup"))
            .unwrap();
        dup_as_plain.put(&mut wtxn, b"q", b"1").unwrap();
        dup_as_plain.put(&mut wtxn, b"q", b"2").unwrap();
        t.row(
            "dup_opened_plain_double_put_count",
            dups(&dup_as_plain, &wtxn, b"q"),
        );
        wtxn.abort();
    }

    // PINNED FINDING (contradicts SPEC 01 §S8 item 8 and the ADR-0011
    // context/Decision-3 assumption): the fork's `mdb_dbi_open` performs NO
    // persistent-flags mismatch check on an existing named DB — the
    // persisted `md_flags` are silently adopted and the caller's requested
    // flags are silently ignored (`MDB_INCOMPATIBLE` on flag mismatch does
    // not exist in this fork). Human adjudication required before 2.8a
    // implements open-time flag semantics (see the 2.8a stop-report).
    t.assert_pinned(
        r#"
same_env_open_dup_without_flags = Ok(is_some=true)
same_env_create_dup_without_flags = Ok
same_env_create_plain_with_dupsort = Ok
reopen_open_dup_without_flags = Ok(is_some=true)
reopen_create_dup_without_flags = Ok
reopen_create_plain_with_dupsort = Ok
reopen_matching_flags_get = Ok(v)
reopen_dup_with_extra_reversedup = Ok
plain_opened_as_dup_double_put_len = 1
plain_opened_as_dup_get = Ok(2)
dup_opened_plain_double_put_count = [1,2]
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 7 — main-DB DUPSORT (§S8 items 3/4): flags on the unnamed DB, then a
// named open attempt.
// ---------------------------------------------------------------------------
#[test]
fn pin_main_db_dupsort() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();

    let mut t = Table::new();
    let main = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT)
        .create(&mut wtxn);
    t.row("create_main_dupsort", obs(main, |_| "Ok".into()));
    let main = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT)
        .create(&mut wtxn)
        .unwrap();
    main.put(&mut wtxn, b"k", b"v1").unwrap();
    main.put(&mut wtxn, b"k", b"v2").unwrap();
    t.row("main_dups", dups(&main, &wtxn, b"k"));
    t.row(
        "named_create_while_main_dupsort",
        obs(
            env.create_database::<Bytes, Bytes>(&mut wtxn, Some("sub")),
            |_| "Ok".into(),
        ),
    );
    t.row(
        "named_open_while_main_dupsort",
        obs(env.open_database::<Bytes, Bytes>(&wtxn, Some("sub")), |o| {
            format!("Ok(is_some={})", o.is_some())
        }),
    );
    wtxn.commit().unwrap();
    drop(env);

    // Are the main DB's flags PERSISTED? Reopen the env, open main with NO
    // flags, and check both the dup behavior and the named-open refusal.
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let main = env
        .create_database::<Bytes, Bytes>(&mut wtxn, None)
        .unwrap();
    t.row("reopen_main_get", obs(main.get(&wtxn, b"k"), ok_val));
    t.row("reopen_main_dups", dups(&main, &wtxn, b"k"));
    t.row(
        "reopen_named_create_still_refused",
        obs(
            env.create_database::<Bytes, Bytes>(&mut wtxn, Some("sub2")),
            |_| "Ok".into(),
        ),
    );
    wtxn.abort();

    t.assert_pinned(
        r#"
create_main_dupsort = Ok
main_dups = [v1,v2]
named_create_while_main_dupsort = Err(Mdb(Incompatible))
named_open_while_main_dupsort = Ok(is_some=false)
reopen_main_get = Ok(v1)
reopen_main_dups = [v1,v2]
reopen_named_create_still_refused = Err(Mdb(Incompatible))
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 8 — DUPFIXED size discipline: what a mismatched-size put returns
// (decides whether DBRecord.leaf2_ksize can ever reset; ADR-0011 Decision 2).
// ---------------------------------------------------------------------------
#[test]
fn pin_dupfixed_size_discipline() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
        .name("dupfix")
        .create(&mut wtxn)
        .unwrap();

    let mut t = Table::new();
    t.row(
        "first_item_len4",
        obs(db.put(&mut wtxn, b"k", b"aaaa"), ok_unit),
    );
    t.row("same_len4", obs(db.put(&mut wtxn, b"k", b"bbbb"), ok_unit));
    t.row(
        "longer_len5",
        obs(db.put(&mut wtxn, b"k", b"ccccc"), ok_unit),
    );
    t.row(
        "shorter_len3",
        obs(db.put(&mut wtxn, b"k", b"ddd"), ok_unit),
    );
    // The exact stored bytes after mismatched-size puts are garbage (packed
    // sub-page arithmetic applied to wrong-size items). Pin the SHAPE, not
    // the garbage bytes: item count, and whether the logical multiset
    // round-trips (it does not — silent corruption).
    let stored: Vec<Vec<u8>> = db
        .get_duplicates(&wtxn, b"k")
        .unwrap()
        .map(|it| it.map(|r| r.unwrap().1.to_vec()).collect())
        .unwrap_or_default();
    let clean: std::collections::BTreeSet<&[u8]> =
        [b"aaaa".as_slice(), b"bbbb", b"ccccc", b"ddd"].into();
    let stored_set: std::collections::BTreeSet<&[u8]> =
        stored.iter().map(|v| v.as_slice()).collect();
    t.row(
        "dups_now_shape",
        format!("count={} round_trips={}", stored.len(), stored_set == clean),
    );
    // A different KEY with a different item size (is the size per-key or
    // per-DB?).
    t.row(
        "other_key_len2",
        obs(db.put(&mut wtxn, b"m", b"ee"), ok_unit),
    );
    t.row("other_key_dups", dups(&db, &wtxn, b"m"));
    wtxn.abort();

    // PINNED FINDING (ADR-0011 Decision 2 open item): the fork does NOT
    // enforce DUPFIXED item-size uniformity on put — mismatched sizes are
    // silently ACCEPTED and the stored items become garbage (silent
    // corruption; neither a clean error nor an "un-fixing" of the page).
    // 2.8c must adjudicate whether to replicate this (a DIVERGENCES
    // candidate: BadValSize instead) before any DUPFIXED code lands.
    t.assert_pinned(
        r#"
first_item_len4 = Ok
same_len4 = Ok
longer_len5 = Ok
shorter_len3 = Ok
dups_now_shape = count=4 round_trips=false
other_key_len2 = Ok
other_key_dups = [ee]
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 9 — INTEGERDUP / INTEGERKEY built-in comparators on 4- and 8-byte
// items (ADR-0011 Decision 3: `mdb_cmp_cint` family, native word size), and
// REVERSEDUP ordering.
// ---------------------------------------------------------------------------
#[test]
fn pin_integer_and_reverse_orders() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();

    let mut t = Table::new();

    // INTEGERDUP with 4-byte native-endian items inserted out of numeric order.
    let idup = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_DUP)
        .name("idup4")
        .create(&mut wtxn)
        .unwrap();
    for n in [300u32, 2, 70000, 1] {
        idup.put(&mut wtxn, b"k", &n.to_ne_bytes()).unwrap();
    }
    let order4: Vec<u32> = idup
        .get_duplicates(&wtxn, b"k")
        .unwrap()
        .unwrap()
        .map(|r| u32::from_ne_bytes(r.unwrap().1.try_into().unwrap()))
        .collect();
    t.row("integerdup_4byte_order", format!("{order4:?}"));

    // INTEGERDUP with 8-byte items.
    let idup8 = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::INTEGER_DUP)
        .name("idup8")
        .create(&mut wtxn)
        .unwrap();
    for n in [1u64 << 40, 5, 1 << 33, 2] {
        idup8.put(&mut wtxn, b"k", &n.to_ne_bytes()).unwrap();
    }
    let order8: Vec<u64> = idup8
        .get_duplicates(&wtxn, b"k")
        .unwrap()
        .unwrap()
        .map(|r| u64::from_ne_bytes(r.unwrap().1.try_into().unwrap()))
        .collect();
    t.row("integerdup_8byte_order", format!("{order8:?}"));

    // Mixed-size items in an INTEGERDUP db: accepted? ordering? (lmdb.h
    // calls same-size mandatory for INTEGERKEY; `mdb_cmp_cint` on mixed
    // sizes is formally undefined — pin what actually happens.)
    t.row(
        "integerdup_mixed_size_put",
        obs(idup.put(&mut wtxn, b"k", &7u64.to_ne_bytes()), ok_unit),
    );
    let mixed: Vec<u64> = idup
        .get_duplicates(&wtxn, b"k")
        .unwrap()
        .unwrap()
        .map(|r| {
            let v = r.unwrap().1;
            match v.len() {
                4 => u32::from_ne_bytes(v.try_into().unwrap()) as u64,
                8 => u64::from_ne_bytes(v.try_into().unwrap()),
                n => panic!("unexpected item len {n}"),
            }
        })
        .collect();
    t.row("integerdup_mixed_size_order", format!("{mixed:?}"));

    // INTEGERKEY main ordering with 4-byte keys.
    let ikey = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::INTEGER_KEY)
        .name("ikey")
        .create(&mut wtxn)
        .unwrap();
    for n in [300u32, 2, 70000, 1] {
        ikey.put(&mut wtxn, &n.to_ne_bytes(), b"v").unwrap();
    }
    let korder: Vec<u32> = ikey
        .iter(&wtxn)
        .unwrap()
        .map(|r| u32::from_ne_bytes(r.unwrap().0.try_into().unwrap()))
        .collect();
    t.row("integerkey_4byte_order", format!("{korder:?}"));

    // REVERSEDUP: dup order = reverse-byte memcmp.
    let rdup = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::REVERSE_DUP)
        .name("rdup")
        .create(&mut wtxn)
        .unwrap();
    for v in [b"az".as_slice(), b"by", b"ax", b"c"] {
        rdup.put(&mut wtxn, b"k", v).unwrap();
    }
    t.row("reversedup_order", dups(&rdup, &wtxn, b"k"));

    // REVERSEKEY: key order = reverse-byte memcmp.
    let rkey = env
        .database_options()
        .types::<Bytes, Bytes>()
        .flags(DatabaseFlags::REVERSE_KEY)
        .name("rkey")
        .create(&mut wtxn)
        .unwrap();
    for k in [b"az".as_slice(), b"by", b"ax", b"c"] {
        rkey.put(&mut wtxn, k, b"v").unwrap();
    }
    let rkorder: Vec<String> = rkey
        .iter(&wtxn)
        .unwrap()
        .map(|r| String::from_utf8_lossy(r.unwrap().0).into_owned())
        .collect();
    t.row("reversekey_order", format!("{rkorder:?}"));
    wtxn.abort();

    // Reverse orders compare from the END of the byte string toward the
    // front ("c"(63) < "ax"(78,61 reversed) < "by"(79,62) < "az"(7a,61)).
    t.assert_pinned(
        r#"
integerdup_4byte_order = [1, 2, 300, 70000]
integerdup_8byte_order = [2, 5, 8589934592, 1099511627776]
integerdup_mixed_size_put = Ok
integerdup_mixed_size_order = [1, 2, 7, 300, 70000]
integerkey_4byte_order = [1, 2, 300, 70000]
reversedup_order = [c,ax,by,az]
reversekey_order = ["c", "ax", "by", "az"]
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin 10 — plain get on a dup DB returns the FIRST dup; iteration over a dup
// DB yields the key once PER PAIR (whole-scan shape the oracle Iter op sees).
// ---------------------------------------------------------------------------
#[test]
fn pin_get_and_iter_shape_on_dup_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = dup_db(&env, &mut wtxn, "dup");
    db.put(&mut wtxn, b"b", b"2").unwrap();
    db.put(&mut wtxn, b"a", b"9").unwrap();
    db.put(&mut wtxn, b"a", b"1").unwrap();
    db.put(&mut wtxn, b"a", b"5").unwrap();

    let mut t = Table::new();
    t.row("get_returns_first_dup", obs(db.get(&wtxn, b"a"), ok_val));
    let full: Vec<String> = db
        .iter(&wtxn)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            format!(
                "{}={}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            )
        })
        .collect();
    t.row("full_iter", format!("[{}]", full.join(",")));
    let rev: Vec<String> = db
        .rev_iter(&wtxn)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            format!(
                "{}={}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            )
        })
        .collect();
    t.row("rev_iter", format!("[{}]", rev.join(",")));
    t.row("first", obs(db.first(&wtxn), |o| format!("{o:?}")));
    t.row("last", obs(db.last(&wtxn), |o| format!("{o:?}")));
    wtxn.abort();

    t.assert_pinned(
        r#"
get_returns_first_dup = Ok(1)
full_iter = [a=1,a=5,a=9,b=2]
rev_iter = [b=2,a=9,a=5,a=1]
first = Some(([97], [49]))
last = Some(([98], [50]))
"#,
    );
}
