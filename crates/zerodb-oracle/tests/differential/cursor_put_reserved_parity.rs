//! The write cursor's reserved put, `put_current_reserved_with_flags`
//! (`mdb_cursor_put` with `MDB_RESERVE`), observed against the fork through
//! heed and compared with ZeroDB through `heed-zerodb`.
//!
//! heed 0.22.1 reserves the slot inside the page first and runs the caller's
//! closure on it afterwards. Each case records, through the `iter_mut` /
//! `rev_iter_mut` surface: the call's result, whether the closure ran, the
//! size of the space it was handed, the bytes that space holds before the
//! closure writes (on a same-size overwrite only, where the slot is the old
//! value), where the cursor is left, and the committed content. Bytes a
//! closure did not write are never compared: LMDB leaves page garbage there.
//!
//! Every case runs with and without `WRITE_MAP`. Do not weaken these pins to
//! let an implementation pass.

use zerodb_oracle::tempdir::TempDir;

/// Entries per tree: enough to span many leaves at any supported page size.
const N: usize = 2_000;
/// Seed value width — with the 8-byte keys this puts ~70 entries on a 16 K page.
const VAL: usize = 200;
/// The seed value's byte.
const SEED: u8 = 0xAB;
/// A value too large to be stored inline at any page size up to 64 K.
const BIG: usize = 40_000;

fn keys() -> Vec<Vec<u8>> {
    (0..N as u64).map(|i| i.to_be_bytes().to_vec()).collect()
}

/// What one reserved put through the cursor does: the key it names, the
/// flags it passes, the space it reserves and how much of it the closure
/// writes before returning.
#[derive(Clone, Copy, Debug)]
pub enum Target {
    /// The key under the cursor.
    Current,
    /// A key that does not exist yet and sorts right after the current one.
    NextNew,
    /// The existing key five entries after the current one.
    Ahead,
    /// A key greater than every stored key.
    PastLast,
    /// The empty key.
    Empty,
}

#[derive(Clone, Copy, Debug)]
pub enum Flag {
    None,
    NoOverwrite,
    Append,
}

#[derive(Clone, Copy, Debug)]
pub enum Fill {
    /// Write every reserved byte.
    Full,
    /// Write this many bytes, then return `Ok`.
    Short(usize),
    /// Write this many bytes, then return an error.
    Fail(usize),
}

#[derive(Clone, Copy, Debug)]
pub struct Case {
    pub target: Target,
    pub flag: Flag,
    pub len: usize,
    pub fill: Fill,
}

impl Case {
    const fn full(len: usize) -> Case {
        Case {
            target: Target::Current,
            flag: Flag::None,
            len,
            fill: Fill::Full,
        }
    }
}

/// The byte the `i`-th reserved put of a walk writes.
fn byte(i: usize) -> u8 {
    (i % 251) as u8
}

/// The replacement width for the `i`-th rewrite under each shape.
fn width(shape: u8, i: usize) -> usize {
    match shape {
        // Same size: every slot is the old value's.
        0 => VAL,
        // Shrink, same size and grow in turn: the growing ones split leaves.
        1 => [VAL / 2, VAL, VAL * 3][i % 3],
        // Always grow: a split every few entries.
        2 => VAL * 4,
        // Inline and overflow values in turn.
        _ => [VAL, BIG, VAL / 2][i % 3],
    }
}

/// FNV-1a over a value, so content comparisons cover every byte.
fn digest(v: &[u8]) -> u64 {
    v.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Generate one probe module per engine from a single body, so neither engine
/// can be given a different sequence of calls.
macro_rules! probes {
    ($m:ident, $heed:ident, $setpage:tt) => {
        mod $m {
            #[allow(unused_imports)] // only the `set` arm needs the page size
            use super::{byte, digest, width, Case, Fill, Flag, Target, SEED, VAL};
            use std::io::{Seek as _, Write as _};
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvFlags, EnvOpenOptions, PutFlags, WithoutTls};

            type Db = Database<Bytes, Bytes>;
            /// `(key, value length, digest of the compared bytes)`.
            type Content = Vec<(Vec<u8>, usize, u64)>;

            fn open(dir: &Path, writemap: bool) -> (Env<WithoutTls>, Db) {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(512 << 20);
                o.max_dbs(4);
                probes!(@setpage o, $setpage);
                if writemap {
                    // SAFETY: WRITE_MAP only changes where dirty pages live;
                    // this process is the env's only user.
                    unsafe { o.flags(EnvFlags::WRITE_MAP) };
                }
                // SAFETY: single-process, single-threaded, private temp dir.
                let env = unsafe { o.open(dir) }.expect("open env");
                let mut w = env.write_txn().expect("write_txn");
                let db = env
                    .create_database::<Bytes, Bytes>(&mut w, Some("t"))
                    .expect("create_database");
                let v = vec![SEED; VAL];
                for k in super::keys() {
                    db.put(&mut w, k.as_slice(), &v).expect("put");
                }
                w.commit().expect("commit");
                (env, db)
            }

            /// Every entry, read in a fresh txn. `partial` names the one
            /// entry whose bytes are compared only up to a prefix.
            fn content(env: &Env<WithoutTls>, db: Db, partial: Option<(&[u8], usize)>) -> Content {
                let r = env.read_txn().expect("read_txn");
                db.iter(&r)
                    .expect("iter")
                    .map(|kv| {
                        let (k, v) = kv.expect("entry");
                        let cmp = match partial {
                            Some((pk, n)) if pk == k => &v[..n.min(v.len())],
                            _ => v,
                        };
                        (k.to_vec(), v.len(), digest(cmp))
                    })
                    .collect()
            }

            fn class(e: &$heed::Error) -> String {
                match e {
                    $heed::Error::Io(e) => format!("Io({:?})", e.kind()),
                    $heed::Error::Mdb(e) => format!("Mdb({e:?})"),
                    e => format!("other: {e}"),
                }
            }

            fn show(kv: Option<$heed::Result<(&[u8], &[u8])>>) -> String {
                match kv {
                    Some(Ok((k, v))) => format!("{} ({} bytes)", hex(k), v.len()),
                    Some(Err(e)) => format!("error {}", class(&e)),
                    None => "None".into(),
                }
            }

            fn hex(k: &[u8]) -> String {
                k.iter().map(|b| format!("{b:02x}")).collect()
            }

            /// Advance to entry `nth`, run `case` there, then report what
            /// the next `next()` yields.
            pub fn reserve_then_next(dir: &Path, writemap: bool, nth: usize, case: Case) -> (Vec<String>, Content) {
                let (env, db) = open(dir, writemap);
                let mut trace = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                let target;
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    let mut cur = Vec::new();
                    for _ in 0..=nth {
                        let (k, _) = it.next().expect("tree holds nth").expect("entry");
                        cur = k.to_vec();
                    }
                    trace.push(format!("at {}", hex(&cur)));
                    target = match case.target {
                        Target::Current => cur,
                        Target::NextNew => [cur.as_slice(), &[0]].concat(),
                        Target::Ahead => (u64::from_be_bytes(cur.as_slice().try_into().expect("8-byte key")) + 5)
                            .to_be_bytes()
                            .to_vec(),
                        Target::PastLast => u64::MAX.to_be_bytes().to_vec(),
                        Target::Empty => Vec::new(),
                    };
                    let flags = match case.flag {
                        Flag::None => PutFlags::empty(),
                        Flag::NoOverwrite => PutFlags::NO_OVERWRITE,
                        Flag::Append => PutFlags::APPEND,
                    };
                    let mut seen = None;
                    let fill = |sp: &mut $heed::ReservedSpace| {
                        let size = sp.size();
                        let remaining = sp.remaining();
                        // A same-size overwrite hands back the old value's
                        // bytes, which are initialized; anything else may be
                        // garbage and is not read.
                        let old = (matches!(case.target, Target::Current) && case.len == VAL)
                            .then(|| {
                                // SAFETY: every byte of this slot is the
                                // initialized old value.
                                unsafe { sp.assume_written(size) };
                                let old = sp.written_mut().iter().all(|&b| b == SEED);
                                sp.seek(std::io::SeekFrom::Start(0)).map(|_| old)
                            })
                            .transpose()?;
                        seen = Some((size, remaining, old));
                        match case.fill {
                            Fill::Full => sp.write_all(&vec![0x5A; size]),
                            Fill::Short(n) => sp.write_all(&vec![0x5A; n]),
                            Fill::Fail(n) => {
                                sp.write_all(&vec![0x5A; n])?;
                                Err(std::io::Error::other("closure failure"))
                            }
                        }
                    };
                    // SAFETY: the key was copied out; no borrow is live.
                    let r = unsafe { it.put_current_reserved_with_flags(flags, &target, case.len, fill) };
                    trace.push(match seen {
                        None => "closure did not run".into(),
                        Some((size, remaining, old)) => {
                            format!("closure saw size {size}, remaining {remaining}, old value {old:?}")
                        }
                    });
                    trace.push(match &r {
                        Ok(b) => format!("put -> Ok({b})"),
                        Err(e) => format!("put -> Err({})", class(e)),
                    });
                    trace.push(format!("next -> {}", show(it.next())));
                }
                w.commit().expect("commit");
                let written = match case.fill {
                    Fill::Full => case.len,
                    Fill::Short(n) | Fill::Fail(n) => n,
                };
                let c = content(&env, db, Some((&target, written)));
                drop(env);
                (trace, c)
            }

            /// Reserve the `nth` entry's value with each of `lens` in turn,
            /// filling it fully every time, then report what `next()` yields.
            pub fn reserve_repeatedly(dir: &Path, writemap: bool, nth: usize, lens: &[usize]) -> (Vec<String>, Content) {
                let (env, db) = open(dir, writemap);
                let mut trace = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut it = db.iter_mut(&mut w).expect("iter_mut");
                    let mut cur = Vec::new();
                    for _ in 0..=nth {
                        cur = it.next().expect("tree holds nth").expect("entry").0.to_vec();
                    }
                    for (i, &len) in lens.iter().enumerate() {
                        // SAFETY: the key was copied out; no borrow is live.
                        let r = unsafe {
                            it.put_current_reserved_with_flags(PutFlags::empty(), &cur, len, |sp| {
                                sp.write_all(&vec![byte(i); len])
                            })
                        };
                        trace.push(format!("put {len} -> {:?}", r.map_err(|e| class(&e))));
                    }
                    trace.push(format!("next -> {}", show(it.next())));
                }
                w.commit().expect("commit");
                let c = content(&env, db, None);
                drop(env);
                (trace, c)
            }

            /// Rewrite every entry through the cursor with a reserved put,
            /// forward (`iter_mut`) or backward (`rev_iter_mut`), with the
            /// widths of `shape`: the read-modify-write loop.
            pub fn rewrite_all(dir: &Path, writemap: bool, shape: u8, backward: bool) -> (Vec<String>, Content) {
                let (env, db) = open(dir, writemap);
                let mut seen = Vec::new();
                let mut w = env.write_txn().expect("write_txn");
                {
                    let mut fwd;
                    let mut rev;
                    let it: &mut dyn RwWalk = if backward {
                        rev = db.rev_iter_mut(&mut w).expect("rev_iter_mut");
                        &mut rev
                    } else {
                        fwd = db.iter_mut(&mut w).expect("iter_mut");
                        &mut fwd
                    };
                    let mut i = 0usize;
                    while let Some(k) = it.step() {
                        seen.push(k.clone());
                        it.reserve(&k, width(shape, i), byte(i));
                        i += 1;
                    }
                }
                w.commit().expect("commit");
                let c = content(&env, db, None);
                drop(env);
                let ordered = if backward {
                    seen.windows(2).all(|w| w[0] > w[1])
                } else {
                    seen.windows(2).all(|w| w[0] < w[1])
                };
                let trace = vec![
                    format!("visited {} entries", seen.len()),
                    format!("strictly ordered: {ordered}"),
                ];
                (trace, c)
            }

            /// One step and one reserved rewrite, over either iterator
            /// direction.
            trait RwWalk {
                fn step(&mut self) -> Option<Vec<u8>>;
                fn reserve(&mut self, k: &[u8], len: usize, b: u8);
            }
            impl RwWalk for $heed::RwIter<'_, Bytes, Bytes> {
                fn step(&mut self) -> Option<Vec<u8>> {
                    self.next().map(|kv| kv.expect("entry").0.to_vec())
                }
                fn reserve(&mut self, k: &[u8], len: usize, b: u8) {
                    // SAFETY: `step` copied the key out; no borrow is live.
                    let r = unsafe {
                        self.put_current_reserved_with_flags(PutFlags::empty(), k, len, |sp| {
                            sp.write_all(&vec![b; len])
                        })
                    };
                    assert!(r.expect("reserved put"), "reserved put reports the entry");
                }
            }
            impl RwWalk for $heed::RwRevIter<'_, Bytes, Bytes> {
                fn step(&mut self) -> Option<Vec<u8>> {
                    self.next().map(|kv| kv.expect("entry").0.to_vec())
                }
                fn reserve(&mut self, k: &[u8], len: usize, b: u8) {
                    // SAFETY: `step` copied the key out; no borrow is live.
                    let r = unsafe {
                        self.put_current_reserved_with_flags(PutFlags::empty(), k, len, |sp| {
                            sp.write_all(&vec![b; len])
                        })
                    };
                    assert!(r.expect("reserved put"), "reserved put reports the entry");
                }
            }
        }
    };
    (@setpage $o:ident, set) => { $o.page_size(zerodb_oracle::os_page_size()); };
    (@setpage $o:ident, noset) => {};
}

probes!(lmdb, heed, noset);
probes!(zerodb, heed_zerodb, set);

/// Run one probe on both engines, with and without `WRITE_MAP`, and require
/// identical trace AND content. Returns the fork's trace.
macro_rules! agree {
    ($probe:ident, $label:expr $(, $arg:expr)*) => {{
        let mut first: Option<Vec<String>> = None;
        for writemap in [false, true] {
            let l_dir = TempDir::new().expect("tempdir");
            let z_dir = TempDir::new().expect("tempdir");
            let (l_trace, l_content) = lmdb::$probe(l_dir.path(), writemap $(, $arg)*);
            let (z_trace, z_content) = zerodb::$probe(z_dir.path(), writemap $(, $arg)*);
            assert_eq!(
                l_trace, z_trace,
                "{} (WRITE_MAP {writemap}): trace diverges from the fork",
                $label
            );
            assert!(
                l_content == z_content,
                "{} (WRITE_MAP {writemap}): committed content diverges from the fork",
                $label
            );
            match &first {
                None => first = Some(l_trace),
                Some(t) => assert_eq!(t, &l_trace, "{}: WRITE_MAP changes the fork's trace", $label),
            }
        }
        first.expect("ran")
    }};
}

const AT: usize = 10;
const AT_KEY: &str = "000000000000000a";
const NEXT_KEY: &str = "000000000000000b";

#[test]
fn same_size_reserve_starts_from_the_old_value() {
    let t = agree!(reserve_then_next, "same size", AT, Case::full(VAL));
    // Pin the fork's answer literally, so a reader sees the observation and
    // not just that the two engines agreed.
    assert_eq!(
        t,
        vec![
            format!("at {AT_KEY}"),
            "closure saw size 200, remaining 200, old value Some(true)".into(),
            "put -> Ok(true)".into(),
            format!("next -> {NEXT_KEY} (200 bytes)"),
        ]
    );
}

#[test]
fn resized_reserves_leave_the_cursor_on_the_entry() {
    for len in [0, 1, VAL / 2, VAL * 40, BIG] {
        let t = agree!(reserve_then_next, format!("len {len}"), AT, Case::full(len));
        assert_eq!(
            t[1],
            format!("closure saw size {len}, remaining {len}, old value None")
        );
        assert_eq!(t[2], "put -> Ok(true)");
        assert_eq!(t[3], format!("next -> {NEXT_KEY} (200 bytes)"));
    }
}

#[test]
fn reserve_on_the_last_entry_then_next_is_none() {
    let t = agree!(reserve_then_next, "last entry", N - 1, Case::full(VAL));
    assert_eq!(t[2], "put -> Ok(true)");
    assert_eq!(t[3], "next -> None");
}

#[test]
fn short_write_keeps_the_reserved_entry_and_returns_eof() {
    for (len, n) in [(64, 7), (VAL, 1), (BIG, 100)] {
        let case = Case {
            fill: Fill::Short(n),
            ..Case::full(len)
        };
        let t = agree!(reserve_then_next, format!("short {n} of {len}"), AT, case);
        assert_eq!(t[2], "put -> Err(Io(UnexpectedEof))");
        assert_eq!(t[3], format!("next -> {NEXT_KEY} (200 bytes)"));
    }
}

#[test]
fn failing_closure_keeps_the_reserved_entry_and_returns_its_error() {
    for (len, n) in [(64, 7), (VAL, 3), (BIG, 100)] {
        let case = Case {
            fill: Fill::Fail(n),
            ..Case::full(len)
        };
        let t = agree!(
            reserve_then_next,
            format!("fail after {n} of {len}"),
            AT,
            case
        );
        assert_eq!(t[2], "put -> Err(Io(Other))");
        assert_eq!(t[3], format!("next -> {NEXT_KEY} (200 bytes)"));
    }
}

#[test]
fn no_overwrite_on_an_existing_key_never_runs_the_closure() {
    let case = Case {
        flag: Flag::NoOverwrite,
        ..Case::full(VAL)
    };
    let t = agree!(reserve_then_next, "NO_OVERWRITE existing", AT, case);
    assert_eq!(t[1], "closure did not run");
    assert_eq!(t[2], "put -> Err(Mdb(KeyExist))");
}

#[test]
fn no_overwrite_on_a_new_key_inserts_it() {
    let case = Case {
        target: Target::NextNew,
        flag: Flag::NoOverwrite,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "NO_OVERWRITE new", AT, case);
    assert_eq!(t[2], "put -> Ok(true)");
    assert_eq!(t[3], format!("next -> {NEXT_KEY} (200 bytes)"));
}

#[test]
fn plain_reserve_of_another_key_moves_the_cursor_there() {
    let case = Case {
        target: Target::PastLast,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "other key", AT, case);
    assert_eq!(t[2], "put -> Ok(true)");
    assert_eq!(t[3], "next -> None");
}

#[test]
fn append_past_the_last_key_inserts_it() {
    let case = Case {
        target: Target::PastLast,
        flag: Flag::Append,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "APPEND past last", AT, case);
    assert_eq!(t[2], "put -> Ok(true)");
}

#[test]
fn append_of_a_lower_key_never_runs_the_closure() {
    let case = Case {
        flag: Flag::Append,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "APPEND lower", AT, case);
    assert_eq!(t[1], "closure did not run");
    assert_eq!(t[2], "put -> Err(Mdb(KeyExist))");
}

#[test]
fn empty_key_never_runs_the_closure() {
    let case = Case {
        target: Target::Empty,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "empty key", AT, case);
    assert_eq!(t[1], "closure did not run");
    assert_eq!(t[2], "put -> Err(Mdb(BadValSize))");
}

#[test]
fn repeated_reserves_of_one_entry_move_between_inline_and_overflow() {
    let lens = [BIG, BIG, VAL, BIG + 5_000, 0, VAL];
    let t = agree!(reserve_repeatedly, "repeated", AT, &lens[..]);
    assert!(
        t[..lens.len()].iter().all(|l| l.ends_with("-> Ok(true)")),
        "{t:?}"
    );
    assert_eq!(t[lens.len()], format!("next -> {NEXT_KEY} (200 bytes)"));
}

#[test]
fn reserving_every_entry_forward_visits_each_once() {
    for shape in [0u8, 1, 2, 3] {
        let t = agree!(rewrite_all, format!("forward shape {shape}"), shape, false);
        assert_eq!(
            t,
            vec![
                format!("visited {N} entries"),
                "strictly ordered: true".into()
            ]
        );
    }
}

#[test]
fn reserving_every_entry_backward_visits_each_once() {
    for shape in [0u8, 1, 2, 3] {
        let t = agree!(rewrite_all, format!("backward shape {shape}"), shape, true);
        assert_eq!(
            t,
            vec![
                format!("visited {N} entries"),
                "strictly ordered: true".into()
            ]
        );
    }
}

#[test]
fn no_overwrite_on_another_existing_key_leaves_the_cursor_there() {
    let case = Case {
        target: Target::Ahead,
        flag: Flag::NoOverwrite,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "NO_OVERWRITE ahead", AT, case);
    assert_eq!(
        t,
        vec![
            format!("at {AT_KEY}"),
            "closure did not run".into(),
            "put -> Err(Mdb(KeyExist))".into(),
            "next -> 0000000000000010 (200 bytes)".into(),
        ]
    );
}

#[test]
fn rejected_append_leaves_the_cursor_on_the_last_entry() {
    let case = Case {
        flag: Flag::Append,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "APPEND lower", AT, case);
    assert_eq!(t[3], "next -> None");
}

#[test]
fn plain_reserve_of_an_existing_key_ahead_moves_the_cursor_there() {
    let case = Case {
        target: Target::Ahead,
        ..Case::full(64)
    };
    let t = agree!(reserve_then_next, "plain ahead", AT, case);
    assert_eq!(t[2], "put -> Ok(true)");
    assert_eq!(t[3], "next -> 0000000000000010 (200 bytes)");
}
