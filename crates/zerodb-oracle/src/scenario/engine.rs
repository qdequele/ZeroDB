//! The scenario runners, expanded once over heed (LMDB) and once over
//! `heed-zerodb` (ZeroDB), so both engines run exactly the same calls.
//!
//! Every runner returns a transcript: one `(step, result)` entry per
//! operation, with results normalized to strings both engines can produce.
//! Transactions live in local variables of one function call, so several
//! read transactions and a write transaction coexist without any `unsafe`.

/// FNV-1a 64 of `bytes`, so a long value is compared in full while its
/// transcript entry stays short.
fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// A key or value as a transcript string: hex when short, else its length
/// and digest.
pub(super) fn bytes_str(bytes: &[u8]) -> String {
    if bytes.len() <= 24 {
        let mut s = String::from("0x");
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    } else {
        format!("<{}B:{:016x}>", bytes.len(), fnv(bytes))
    }
}

/// The transcript label of the post-`MapFull` commit result.
pub(super) const POST_FAIL: &str = "commit after MapFull";
/// That entry's value when the map never filled.
pub(super) const NEVER_FULL: &str = "never full";

/// Round a map size in bytes to 64 KiB units, a multiple of every OS page
/// size, as heed requires.
fn units(n: u8) -> usize {
    usize::from(n) * 64 * 1024
}

macro_rules! scenario_engine {
    ($mod:ident, $heed:ident) => {
        pub(super) mod $mod {
            use std::collections::BTreeMap;
            use std::ops::Bound;
            use std::path::Path;

            use $heed::types::Bytes;
            use $heed::{
                Comparator, Database, Env, EnvFlags, EnvOpenOptions, PutFlags, RoTxn, RwTxn,
                WithoutTls,
            };

            use super::{bytes_str, units};
            use crate::scenario::{
                fill_value, CmpKind, Entry, KeyShape, LenThenBytesComparator, MapFullCase,
                OpenFlags, OrderedCase, OrderedOp, ReadOp, ReopenCase, ReopenOp, ReverseComparator,
                SKey, SnapshotCase, SnapshotOp, WriteOp, REOPEN_BASE_MAP, SNAPSHOT_MAX_READERS,
                SNAPSHOT_SLOTS,
            };
            use crate::tempdir::TempDir;

            type Db<C> = Database<Bytes, Bytes, C>;

            /// A heed error, normalized. Only the variant and the error code
            /// or I/O kind are kept: messages differ between the engines.
            fn e(err: $heed::Error) -> String {
                match err {
                    $heed::Error::Mdb(m) => format!("Err(mdb:{m:?})"),
                    $heed::Error::Io(io) => format!("Err(io:{:?})", io.kind()),
                    $heed::Error::Encoding(_) => "Err(encoding)".into(),
                    $heed::Error::Decoding(_) => "Err(decoding)".into(),
                    $heed::Error::EnvAlreadyOpened => "Err(env-already-opened)".into(),
                }
            }

            fn res<T: std::fmt::Debug>(r: $heed::Result<T>) -> String {
                match r {
                    Ok(v) => format!("Ok({v:?})"),
                    Err(err) => e(err),
                }
            }

            fn entry(kv: Option<(&[u8], &[u8])>) -> String {
                match kv {
                    Some((k, v)) => format!("Some({}={})", bytes_str(k), bytes_str(v)),
                    None => "None".into(),
                }
            }

            fn rb(b: &Bound<SKey>) -> Bound<&[u8]> {
                match b {
                    Bound::Included(k) => Bound::Included(&k.0[..]),
                    Bound::Excluded(k) => Bound::Excluded(&k.0[..]),
                    Bound::Unbounded => Bound::Unbounded,
                }
            }

            /// Drain an iterator into a transcript string, stopping at the
            /// first error.
            fn drain<'t>(
                it: $heed::Result<impl Iterator<Item = $heed::Result<(&'t [u8], &'t [u8])>>>,
            ) -> String {
                let it = match it {
                    Ok(it) => it,
                    Err(err) => return e(err),
                };
                let mut out = String::from("[");
                for item in it {
                    match item {
                        Ok((k, v)) => {
                            out.push_str(&bytes_str(k));
                            out.push('=');
                            out.push_str(&bytes_str(v));
                            out.push(',');
                        }
                        Err(err) => {
                            out.push_str(&e(err));
                            break;
                        }
                    }
                }
                out.push(']');
                out
            }

            fn read<C: Comparator + 'static>(db: &Db<C>, txn: &RoTxn<'_>, op: &ReadOp) -> String {
                match op {
                    ReadOp::Get(k) => match db.get(txn, &k.0) {
                        Ok(v) => format!("Ok({})", v.map_or("None".into(), bytes_str)),
                        Err(err) => e(err),
                    },
                    ReadOp::Len => res(db.len(txn)),
                    ReadOp::Iter => drain(db.iter(txn)),
                    ReadOp::RevIter => drain(db.rev_iter(txn)),
                    ReadOp::Range(lo, hi) => drain(db.range(txn, &(rb(lo), rb(hi)))),
                    ReadOp::RevRange(lo, hi) => drain(db.rev_range(txn, &(rb(lo), rb(hi)))),
                    ReadOp::First => db.first(txn).map_or_else(e, entry),
                    ReadOp::Last => db.last(txn).map_or_else(e, entry),
                    ReadOp::Gt(k) => db.get_greater_than(txn, &k.0).map_or_else(e, entry),
                    ReadOp::Ge(k) => db
                        .get_greater_than_or_equal_to(txn, &k.0)
                        .map_or_else(e, entry),
                    ReadOp::Lt(k) => db.get_lower_than(txn, &k.0).map_or_else(e, entry),
                    ReadOp::Le(k) => db
                        .get_lower_than_or_equal_to(txn, &k.0)
                        .map_or_else(e, entry),
                }
            }

            fn write<C: Comparator + 'static>(
                db: &Db<C>,
                wtxn: &mut RwTxn<'_>,
                op: &WriteOp,
                shape: KeyShape,
            ) -> String {
                match op {
                    WriteOp::Put(k, v) => res(db.put(wtxn, &k.0, &v.0)),
                    WriteOp::PutNoOverwrite(k, v) => {
                        res(db.put_with_flags(wtxn, PutFlags::NO_OVERWRITE, &k.0, &v.0))
                    }
                    WriteOp::Append(k, v) => {
                        res(db.put_with_flags(wtxn, PutFlags::APPEND, &k.0, &v.0))
                    }
                    WriteOp::Del(k) => res(db.delete(wtxn, &k.0)),
                    WriteOp::DeleteRange(lo, hi) => res(db.delete_range(wtxn, &(rb(lo), rb(hi)))),
                    WriteOp::Fill { start, n, vlen } => {
                        for i in 0..u32::from(*n) {
                            let idx = u32::from(*start) + i;
                            let k = shape.index_key(idx);
                            if let Err(err) = db.put(wtxn, &k.0, &fill_value(idx, *vlen)) {
                                return format!("at {i}: {}", e(err));
                            }
                        }
                        "Ok(filled)".into()
                    }
                    WriteOp::Clear => res(db.clear(wtxn)),
                }
            }

            fn open(
                dir: &Path,
                flags: OpenFlags,
                map_size: Option<usize>,
                max_readers: u32,
            ) -> $heed::Result<Env<WithoutTls>> {
                let mut opts = EnvOpenOptions::new().read_txn_without_tls();
                if let Some(m) = map_size {
                    opts.map_size(m);
                }
                opts.max_dbs(4).max_readers(max_readers);
                // Durability never changes an observable result, only what a
                // crash keeps; skipping the syncs keeps a case fast.
                let mut f = EnvFlags::NO_SYNC;
                if flags.read_only {
                    f |= EnvFlags::READ_ONLY;
                }
                if flags.prev_snapshot {
                    f |= EnvFlags::PREV_SNAPSHOT;
                }
                if flags.no_lock {
                    f |= EnvFlags::NO_LOCK;
                }
                if flags.write_map {
                    f |= EnvFlags::WRITE_MAP;
                }
                // SAFETY: the directory is a private temp dir that only this
                // function call's engine opens, from one thread; `NO_LOCK` is
                // therefore sound, and no other process maps the file.
                unsafe {
                    opts.flags(f);
                    opts.open(dir)
                }
            }

            /// Close `env` and wait until the engine has released it, so the
            /// next open of the directory starts from the files alone.
            fn close(env: Env<WithoutTls>) {
                env.prepare_for_closing().wait();
            }

            const WRITABLE: OpenFlags = OpenFlags {
                read_only: false,
                prev_snapshot: false,
                no_lock: false,
                write_map: false,
            };

            // ---- ordered ----------------------------------------------------

            pub(crate) fn ordered(case: &OrderedCase) -> Vec<Entry> {
                match case.cmp {
                    CmpKind::Default => ordered_with::<$heed::DefaultComparator>(case),
                    CmpKind::Reverse => ordered_with::<ReverseComparator>(case),
                    CmpKind::Integer { .. } => ordered_with::<$heed::IntegerComparator>(case),
                    CmpKind::LenThenBytes => ordered_with::<LenThenBytesComparator>(case),
                }
            }

            fn ordered_with<C: Comparator + 'static>(case: &OrderedCase) -> Vec<Entry> {
                let shape = KeyShape::for_cmp(case.cmp);
                let dir = TempDir::new().expect("temp dir");
                let env = open(dir.path(), WRITABLE, Some(64 << 20), 8).expect("open env");
                let db: Db<C> = {
                    let mut w = env.write_txn().expect("write txn");
                    let db = env
                        .database_options()
                        .types::<Bytes, Bytes>()
                        .key_comparator::<C>()
                        .name("t")
                        .create(&mut w)
                        .expect("create db");
                    w.commit().expect("commit");
                    db
                };
                let mut out = Vec::new();
                let mut wtxn: Option<RwTxn<'_>> = None;
                for op in &case.ops {
                    let r = match op {
                        OrderedOp::Write(w) => {
                            let t = match wtxn.as_mut() {
                                Some(t) => t,
                                None => wtxn.insert(env.write_txn().expect("write txn")),
                            };
                            write(&db, t, w, shape)
                        }
                        OrderedOp::Read(r) => {
                            let t = match wtxn.as_mut() {
                                Some(t) => t,
                                None => wtxn.insert(env.write_txn().expect("write txn")),
                            };
                            read(&db, t, r)
                        }
                        OrderedOp::ReadCommitted(r) => match env.read_txn() {
                            Ok(t) => read(&db, &t, r),
                            Err(err) => e(err),
                        },
                        OrderedOp::Commit => match wtxn.take() {
                            Some(t) => res(t.commit()),
                            None => "no txn".into(),
                        },
                        OrderedOp::Abort => match wtxn.take() {
                            Some(t) => {
                                t.abort();
                                "aborted".into()
                            }
                            None => "no txn".into(),
                        },
                    };
                    out.push((format!("{op:?}"), r));
                }
                drop(wtxn);
                close(env);
                out
            }

            // ---- snapshots --------------------------------------------------

            pub(crate) fn snapshots(case: &SnapshotCase) -> Vec<Entry> {
                let shape = KeyShape::for_cmp(CmpKind::Default);
                let dir = TempDir::new().expect("temp dir");
                let env = open(dir.path(), WRITABLE, Some(64 << 20), SNAPSHOT_MAX_READERS)
                    .expect("open env");
                let db: Db<$heed::DefaultComparator> = {
                    let mut w = env.write_txn().expect("write txn");
                    let db = env
                        .create_database::<Bytes, Bytes>(&mut w, Some("main"))
                        .expect("create db");
                    w.commit().expect("commit");
                    db
                };
                let mut out = Vec::new();
                let mut wtxn: Option<RwTxn<'_>> = None;
                let mut readers: Vec<Option<RoTxn<'_, WithoutTls>>> =
                    (0..SNAPSHOT_SLOTS).map(|_| None).collect();
                for op in &case.ops {
                    let r = match op {
                        SnapshotOp::Write(w) => {
                            let t = match wtxn.as_mut() {
                                Some(t) => t,
                                None => wtxn.insert(env.write_txn().expect("write txn")),
                            };
                            write(&db, t, w, shape)
                        }
                        SnapshotOp::WriterRead(r) => match wtxn.as_ref() {
                            Some(t) => read(&db, t, r),
                            None => "no txn".into(),
                        },
                        SnapshotOp::Commit => match wtxn.take() {
                            Some(t) => res(t.commit()),
                            None => "no txn".into(),
                        },
                        SnapshotOp::Abort => match wtxn.take() {
                            Some(t) => {
                                t.abort();
                                "aborted".into()
                            }
                            None => "no txn".into(),
                        },
                        SnapshotOp::OpenReader(s) => {
                            let slot = &mut readers[usize::from(*s)];
                            if slot.is_some() {
                                "already open".into()
                            } else {
                                match env.read_txn() {
                                    Ok(t) => {
                                        *slot = Some(t);
                                        "Ok".into()
                                    }
                                    Err(err) => e(err),
                                }
                            }
                        }
                        SnapshotOp::CloseReader(s) => match readers[usize::from(*s)].take() {
                            Some(t) => res(t.commit()),
                            None => "no reader".into(),
                        },
                        SnapshotOp::Read(s, r) => match readers[usize::from(*s)].as_ref() {
                            Some(t) => read(&db, t, r),
                            None => "no reader".into(),
                        },
                        SnapshotOp::ReaderCount => {
                            format!("{}", env.info().number_of_readers)
                        }
                        SnapshotOp::ClearStale => res(env.clear_stale_readers()),
                    };
                    out.push((format!("{op:?}"), r));
                }
                drop(readers);
                drop(wtxn);
                close(env);
                out
            }

            // ---- reopen -----------------------------------------------------

            fn unnamed(
                env: &Env<WithoutTls>,
            ) -> $heed::Result<Option<Db<$heed::DefaultComparator>>> {
                let r = env.read_txn()?;
                let db = env.open_database::<Bytes, Bytes>(&r, None)?;
                r.commit()?;
                Ok(db)
            }

            pub(crate) fn reopen(case: &ReopenCase) -> Vec<Entry> {
                let shape = KeyShape::for_cmp(CmpKind::Default);
                let dir = TempDir::new().expect("temp dir");
                let mut env =
                    Some(open(dir.path(), WRITABLE, Some(REOPEN_BASE_MAP), 8).expect("open env"));
                let mut out = Vec::new();
                for op in &case.ops {
                    let r = match op {
                        ReopenOp::Commit(writes) => match env.as_ref() {
                            None => "closed".into(),
                            Some(env) => match env.write_txn() {
                                Err(err) => format!("begin: {}", e(err)),
                                Ok(mut w) => {
                                    let mut s = String::new();
                                    match env.create_database::<Bytes, Bytes>(&mut w, None) {
                                        Err(err) => s.push_str(&format!("db: {}", e(err))),
                                        Ok(db) => {
                                            for op in writes {
                                                s.push_str(&write(&db, &mut w, op, shape));
                                                s.push(';');
                                            }
                                        }
                                    }
                                    s.push_str(&format!(" commit: {}", res(w.commit())));
                                    s
                                }
                            },
                        },
                        ReopenOp::Reopen { flags, map_units } => {
                            if let Some(old) = env.take() {
                                close(old);
                            }
                            match open(dir.path(), *flags, map_units.map(units), 8) {
                                Ok(new) => {
                                    let s = format!("Ok(map {})", new.info().map_size);
                                    env = Some(new);
                                    s
                                }
                                Err(err) => e(err),
                            }
                        }
                        ReopenOp::Close => match env.take() {
                            Some(old) => {
                                close(old);
                                "closed".into()
                            }
                            None => "not open".into(),
                        },
                        ReopenOp::Read(r) => match env.as_ref() {
                            None => "closed".into(),
                            Some(env) => match unnamed(env) {
                                Err(err) => format!("open db: {}", e(err)),
                                Ok(None) => "no db".into(),
                                Ok(Some(db)) => match env.read_txn() {
                                    Ok(t) => read(&db, &t, r),
                                    Err(err) => e(err),
                                },
                            },
                        },
                        ReopenOp::Resize(n) => match env.as_ref() {
                            None => "closed".into(),
                            // SAFETY: no transaction of this environment is
                            // live: every transaction above is scoped to its
                            // own step.
                            Some(env) => match unsafe { env.resize(units(*n)) } {
                                Ok(()) => format!("Ok(map {})", env.info().map_size),
                                Err(err) => e(err),
                            },
                        },
                        ReopenOp::Info => match env.as_ref() {
                            None => "closed".into(),
                            Some(env) => {
                                let i = env.info();
                                format!(
                                    "map {} txn {} readers {}/{}",
                                    i.map_size,
                                    i.last_txn_id,
                                    i.number_of_readers,
                                    i.maximum_number_of_readers
                                )
                            }
                        },
                        ReopenOp::Flags => match env.as_ref() {
                            None => "closed".into(),
                            // The adapter reports these six bits only, a
                            // difference listed under `flags()` in
                            // docs/LMDB-COMPATIBILITY.md.
                            Some(env) => match env.flags() {
                                Ok(f) => {
                                    let shown = EnvFlags::WRITE_MAP
                                        | EnvFlags::READ_ONLY
                                        | EnvFlags::NO_SYNC
                                        | EnvFlags::NO_META_SYNC
                                        | EnvFlags::MAP_ASYNC
                                        | EnvFlags::NO_LOCK;
                                    format!("{:#x}", f.map_or(0, |f| (f & shown).bits()))
                                }
                                Err(err) => e(err),
                            },
                        },
                    };
                    out.push((format!("{op:?}"), r));
                }
                if let Some(env) = env {
                    close(env);
                }
                out
            }

            // ---- map full ---------------------------------------------------

            /// Apply one batch in one write transaction. `Ok(())` when it
            /// committed; `Err(MapFull)` after checking the failure is clean.
            fn batch(
                env: &Env<WithoutTls>,
                db: &Db<$heed::DefaultComparator>,
                b: &[(u16, Option<u16>)],
                post_fail: &mut Vec<String>,
                violations: &mut Vec<String>,
            ) -> Result<(), ()> {
                let mut w = match env.write_txn() {
                    Ok(w) => w,
                    Err($heed::Error::Mdb($heed::MdbError::MapFull)) => return Err(()),
                    Err(err) => {
                        violations.push(format!("write_txn: {}", e(err)));
                        return Err(());
                    }
                };
                for (key, v) in b {
                    let k = format!("k{key:04}");
                    let r = match v {
                        Some(len) => db
                            .put(&mut w, k.as_bytes(), &fill_value(u32::from(*key), *len))
                            .map(|()| true),
                        None => db.delete(&mut w, k.as_bytes()),
                    };
                    match r {
                        Ok(_) => {}
                        Err($heed::Error::Mdb($heed::MdbError::MapFull)) => {
                            // The failed transaction must not commit.
                            let c = w.commit();
                            if c.is_ok() {
                                violations.push("commit after MapFull succeeded".into());
                            }
                            post_fail.push(res(c));
                            return Err(());
                        }
                        Err(err) => {
                            violations.push(format!("put: {}", e(err)));
                            return Err(());
                        }
                    }
                }
                match w.commit() {
                    Ok(()) => Ok(()),
                    Err($heed::Error::Mdb($heed::MdbError::MapFull)) => Err(()),
                    Err(err) => {
                        violations.push(format!("commit: {}", e(err)));
                        Err(())
                    }
                }
            }

            fn contents(
                env: &Env<WithoutTls>,
                db: &Db<$heed::DefaultComparator>,
            ) -> $heed::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
                let r = env.read_txn()?;
                let mut m = BTreeMap::new();
                for kv in db.iter(&r)? {
                    let (k, v) = kv?;
                    m.insert(k.to_vec(), v.to_vec());
                }
                Ok(m)
            }

            /// The `MapFull` property on one engine. The transcript holds what
            /// both engines must agree on: no violations, the result of a
            /// commit attempted after `MapFull`, and the final contents.
            pub(crate) fn map_full(case: &MapFullCase) -> Vec<Entry> {
                let dir = TempDir::new().expect("temp dir");
                let flags = OpenFlags {
                    write_map: case.write_map,
                    ..WRITABLE
                };
                let mut map = units(case.map_units);
                let env = match open(dir.path(), flags, Some(map), 8) {
                    Ok(env) => env,
                    Err(err) => return vec![("open".into(), e(err))],
                };
                let mut violations = Vec::new();
                let mut post_fail = Vec::new();
                let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
                let mut db_slot = None;
                for (bi, b) in case.batches.iter().enumerate() {
                    // The database itself is created in the first batch that
                    // fits, so a full map can also fail its creation.
                    loop {
                        let db = match db_slot {
                            Some(db) => db,
                            None => {
                                let mut w = match env.write_txn() {
                                    Ok(w) => w,
                                    Err(err) => {
                                        violations.push(format!("begin: {}", e(err)));
                                        break;
                                    }
                                };
                                let created = env
                                    .create_database::<Bytes, Bytes>(&mut w, Some("t"))
                                    .and_then(|db| w.commit().map(|()| db));
                                match created {
                                    Ok(db) => *db_slot.insert(db),
                                    Err($heed::Error::Mdb($heed::MdbError::MapFull)) => {
                                        map *= 2;
                                        // SAFETY: no transaction is live here.
                                        if let Err(err) = unsafe { env.resize(map) } {
                                            violations.push(format!("resize: {}", e(err)));
                                            break;
                                        }
                                        continue;
                                    }
                                    Err(err) => {
                                        violations.push(format!("create: {}", e(err)));
                                        break;
                                    }
                                }
                            }
                        };
                        let before = violations.len();
                        if batch(&env, &db, b, &mut post_fail, &mut violations).is_ok() {
                            for (key, v) in b {
                                let k = format!("k{key:04}").into_bytes();
                                match v {
                                    Some(len) => {
                                        model.insert(k, fill_value(u32::from(*key), *len));
                                    }
                                    None => {
                                        model.remove(&k);
                                    }
                                }
                            }
                            break;
                        }
                        if violations.len() > before {
                            break;
                        }
                        // A clean MapFull: the committed state is intact and
                        // the environment still serves reads.
                        match contents(&env, &db) {
                            Ok(c) if c == model => {}
                            Ok(_) => violations.push(format!(
                                "batch {bi}: committed state changed by a failed txn"
                            )),
                            Err(err) => violations.push(format!("batch {bi}: read: {}", e(err))),
                        }
                        if map >= 256 << 20 {
                            violations.push(format!("batch {bi}: MapFull at 256 MiB"));
                            break;
                        }
                        map *= 2;
                        // SAFETY: no transaction is live here.
                        if let Err(err) = unsafe { env.resize(map) } {
                            violations.push(format!("resize: {}", e(err)));
                            break;
                        }
                    }
                    if !violations.is_empty() {
                        break;
                    }
                }
                let mut out = vec![("violations".to_string(), format!("{violations:?}"))];
                // Which batches fill the map differs between engines, so only
                // the set of post-failure commit results is compared.
                post_fail.sort();
                post_fail.dedup();
                out.push((
                    super::POST_FAIL.into(),
                    if post_fail.is_empty() {
                        super::NEVER_FULL.into()
                    } else {
                        format!("{post_fail:?}")
                    },
                ));
                let fin = match db_slot {
                    Some(db) => match contents(&env, &db) {
                        Ok(c) => {
                            if c != model {
                                out.push((
                                    "model".into(),
                                    "final contents differ from model".into(),
                                ));
                            }
                            format!("{} entries, {:016x}", c.len(), {
                                let mut all = Vec::new();
                                for (k, v) in &c {
                                    all.extend_from_slice(k);
                                    all.extend_from_slice(v);
                                }
                                super::fnv(&all)
                            })
                        }
                        Err(err) => e(err),
                    },
                    None => "no db".into(),
                };
                out.push(("final contents".into(), fin));
                close(env);
                out
            }
        }
    };
}

scenario_engine!(lmdb, heed);
scenario_engine!(zdb, heed_zerodb);
