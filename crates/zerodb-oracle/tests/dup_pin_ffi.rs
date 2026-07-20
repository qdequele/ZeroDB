//! Milestone 2.8a — the ADR-0011 Q5 DUPSORT pin list, FFI-only portion.
//!
//! heed 0.22.1 exposes no surface for `MDB_GET_BOTH`/`MDB_GET_BOTH_RANGE`,
//! raw `mdb_dbi_open` flag bits, positioned dup-cursor steps, or the §S2-style
//! out-data contract, so this file drives the vendored fork directly through
//! `lmdb-master-sys` (sanctioned: ADR-0011 Q2 — FFI differentials live in
//! `zerodb-oracle`, the one crate allowed to link C). Same pin protocol as
//! `dup_pin_semantics.rs`: observation tables compared against pinned
//! constants; a diff means the fork changed or a guess was wrong — never
//! weaken, adjudicate (CLAUDE.md rules 1/2).

use std::ffi::CString;
use std::os::raw::{c_int, c_uint, c_void};
use std::ptr;

use lmdb_master_sys as ffi;
use zerodb_oracle::tempdir::TempDir;

const MDB_DUPSORT: c_uint = 0x04;
const MDB_CREATE: c_uint = 0x40000;
const MDB_NOOVERWRITE: c_uint = 0x10;
const MDB_NODUPDATA: c_uint = 0x20;

const MDB_FIRST_DUP: ffi::MDB_cursor_op = 1;
const MDB_GET_BOTH: ffi::MDB_cursor_op = 2;
const MDB_GET_BOTH_RANGE: ffi::MDB_cursor_op = 3;
const MDB_GET_CURRENT: ffi::MDB_cursor_op = 4;
const MDB_LAST_DUP: ffi::MDB_cursor_op = 7;
const MDB_NEXT: ffi::MDB_cursor_op = 8;
const MDB_NEXT_DUP: ffi::MDB_cursor_op = 9;
const MDB_NEXT_NODUP: ffi::MDB_cursor_op = 11;
const MDB_PREV_DUP: ffi::MDB_cursor_op = 13;
const MDB_PREV_NODUP: ffi::MDB_cursor_op = 14;
const MDB_SET: ffi::MDB_cursor_op = 15;

fn rc_name(rc: c_int) -> String {
    match rc {
        0 => "OK".into(),
        ffi::MDB_KEYEXIST => "KEYEXIST".into(),
        ffi::MDB_NOTFOUND => "NOTFOUND".into(),
        ffi::MDB_INCOMPATIBLE => "INCOMPATIBLE".into(),
        ffi::MDB_BAD_VALSIZE => "BAD_VALSIZE".into(),
        22 => "EINVAL".into(),
        other => format!("rc({other})"),
    }
}

// SAFETY helpers around the raw fork API. All pointers are used within the
// lifetime of the env/txn that produced them, single-threaded, exactly as the
// C API requires.

struct Fork {
    env: *mut ffi::MDB_env,
    _dir: TempDir,
}

impl Fork {
    fn new() -> Fork {
        let dir = TempDir::new().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        // SAFETY: standard env-creation sequence; the env pointer outlives all
        // txns/cursors created below and is closed in Drop.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxdbs(env, 8), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, 32 << 20), 0);
            assert_eq!(ffi::mdb_env_open(env, path.as_ptr(), 0, 0o664), 0);
        }
        Fork { env, _dir: dir }
    }

    fn txn(&self) -> *mut ffi::MDB_txn {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        // SAFETY: env is live; single-threaded write txn.
        unsafe {
            assert_eq!(
                ffi::mdb_txn_begin(self.env, ptr::null_mut(), 0, &mut txn),
                0
            );
        }
        txn
    }

    fn dbi(&self, txn: *mut ffi::MDB_txn, name: &str, flags: c_uint) -> (c_int, ffi::MDB_dbi) {
        let name = CString::new(name).unwrap();
        let mut dbi: ffi::MDB_dbi = 0;
        // SAFETY: txn is live and belongs to self.env.
        let rc = unsafe { ffi::mdb_dbi_open(txn, name.as_ptr(), flags, &mut dbi) };
        (rc, dbi)
    }
}

impl Drop for Fork {
    fn drop(&mut self) {
        // SAFETY: all txns/cursors have been closed by the tests before drop.
        unsafe { ffi::mdb_env_close(self.env) }
    }
}

fn val(bytes: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_size: bytes.len(),
        mv_data: bytes.as_ptr() as *mut c_void,
    }
}

/// Render an `MDB_val` written by the fork back into printable bytes.
///
/// # Safety
/// `v` must have been filled in by a successful fork call within the current
/// txn's lifetime.
unsafe fn render_val(v: &ffi::MDB_val) -> String {
    let bytes = unsafe { std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size) };
    String::from_utf8_lossy(bytes).into_owned()
}

fn put(txn: *mut ffi::MDB_txn, dbi: ffi::MDB_dbi, k: &[u8], v: &[u8], flags: c_uint) -> c_int {
    let mut key = val(k);
    let mut data = val(v);
    // SAFETY: key/data point at live slices for the duration of the call.
    unsafe { ffi::mdb_put(txn, dbi, &mut key, &mut data, flags) }
}

/// One cursor-get probe: returns "RC key=.. data=.." with the out-vals as the
/// fork left them (the GET_BOTH_RANGE return-quirk question).
fn cursor_probe(
    cursor: *mut ffi::MDB_cursor,
    k: Option<&[u8]>,
    d: Option<&[u8]>,
    op: ffi::MDB_cursor_op,
) -> String {
    let mut key = k.map(val).unwrap_or(ffi::MDB_val {
        mv_size: 0,
        mv_data: ptr::null_mut(),
    });
    let mut data = d.map(val).unwrap_or(ffi::MDB_val {
        mv_size: 0,
        mv_data: ptr::null_mut(),
    });
    // SAFETY: cursor is live; in/out vals are valid for the call and read back
    // immediately, inside the owning txn.
    let rc = unsafe { ffi::mdb_cursor_get(cursor, &mut key, &mut data, op) };
    if rc == 0 {
        // SAFETY: on success both vals point into the map, valid in-txn.
        unsafe { format!("OK key={} data={}", render_val(&key), render_val(&data)) }
    } else {
        rc_name(rc)
    }
}

// ---------------------------------------------------------------------------
// Pin F1 — GET_BOTH / GET_BOTH_RANGE exact positioning, EOF edges, and the
// out-val quirks (ADR-0011 Decision 4 table rows).
// ---------------------------------------------------------------------------
#[test]
fn pin_get_both_and_range() {
    let fork = Fork::new();
    let txn = fork.txn();
    let (rc, dbi) = fork.dbi(txn, "dup", MDB_DUPSORT | MDB_CREATE);
    assert_eq!(rc, 0);
    for (k, v) in [
        (b"k1".as_slice(), b"d1".as_slice()),
        (b"k1", b"d3"),
        (b"k1", b"d5"),
        (b"k2", b"e1"),
    ] {
        assert_eq!(put(txn, dbi, k, v, 0), 0);
    }
    let mut cur: *mut ffi::MDB_cursor = ptr::null_mut();
    // SAFETY: txn/dbi live; cursor closed before txn abort.
    unsafe { assert_eq!(ffi::mdb_cursor_open(txn, dbi, &mut cur), 0) };

    let mut t: Vec<String> = Vec::new();
    let mut row = |id: &str, s: String| t.push(format!("{id} = {s}"));

    row(
        "get_both_exact",
        cursor_probe(cur, Some(b"k1"), Some(b"d3"), MDB_GET_BOTH),
    );
    row(
        "get_both_absent_dup",
        cursor_probe(cur, Some(b"k1"), Some(b"d2"), MDB_GET_BOTH),
    );
    row(
        "get_both_below_first_dup",
        cursor_probe(cur, Some(b"k1"), Some(b"d0"), MDB_GET_BOTH),
    );
    row(
        "get_both_above_last_dup",
        cursor_probe(cur, Some(b"k1"), Some(b"d9"), MDB_GET_BOTH),
    );
    row(
        "get_both_missing_key",
        cursor_probe(cur, Some(b"kX"), Some(b"d1"), MDB_GET_BOTH),
    );

    row(
        "get_both_range_between",
        cursor_probe(cur, Some(b"k1"), Some(b"d2"), MDB_GET_BOTH_RANGE),
    );
    row(
        "get_both_range_exact",
        cursor_probe(cur, Some(b"k1"), Some(b"d5"), MDB_GET_BOTH_RANGE),
    );
    row(
        "get_both_empty_data",
        cursor_probe(cur, Some(b"k1"), Some(b""), MDB_GET_BOTH),
    );
    row(
        "get_both_range_empty_data",
        cursor_probe(cur, Some(b"k1"), Some(b""), MDB_GET_BOTH_RANGE),
    );
    row(
        "get_both_range_past_last_dup",
        cursor_probe(cur, Some(b"k1"), Some(b"d9"), MDB_GET_BOTH_RANGE),
    );
    // Cursor state after the EOF-edge miss: what is current, what does NEXT do?
    row(
        "after_range_miss_get_current",
        cursor_probe(cur, None, None, MDB_GET_CURRENT),
    );
    row(
        "after_range_miss_next",
        cursor_probe(cur, None, None, MDB_NEXT),
    );
    row(
        "get_both_range_missing_key",
        cursor_probe(cur, Some(b"kX"), Some(b"d1"), MDB_GET_BOTH_RANGE),
    );

    // SAFETY: close cursor before ending the txn.
    unsafe {
        ffi::mdb_cursor_close(cur);
        ffi::mdb_txn_abort(txn);
    }

    assert_pinned(
        &t,
        r#"
get_both_exact = OK key=k1 data=d3
get_both_absent_dup = NOTFOUND
get_both_below_first_dup = NOTFOUND
get_both_above_last_dup = NOTFOUND
get_both_missing_key = NOTFOUND
get_both_range_between = OK key=k1 data=d3
get_both_range_exact = OK key=k1 data=d5
get_both_empty_data = BAD_VALSIZE
get_both_range_empty_data = BAD_VALSIZE
get_both_range_past_last_dup = NOTFOUND
after_range_miss_get_current = NOTFOUND
after_range_miss_next = OK key=k2 data=e1
get_both_range_missing_key = NOTFOUND
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin F2 — dup cursor ops on a NON-dup DB, and unpositioned dup ops on a dup
// DB (the Table 5 error taxonomy; ADR-0011 Decision 4 "pinned, not guessed").
// ---------------------------------------------------------------------------
#[test]
fn pin_dup_cursor_ops_taxonomy() {
    let fork = Fork::new();
    let txn = fork.txn();
    let (rc, plain) = fork.dbi(txn, "plain", MDB_CREATE);
    assert_eq!(rc, 0);
    let (rc, dup) = fork.dbi(txn, "dup", MDB_DUPSORT | MDB_CREATE);
    assert_eq!(rc, 0);
    for (k, v) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b", b"2"),
        (b"c", b"3"),
    ] {
        assert_eq!(put(txn, plain, k, v, 0), 0);
    }
    for (k, v) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"a", b"2"),
        (b"b", b"9"),
    ] {
        assert_eq!(put(txn, dup, k, v, 0), 0);
    }

    let mut t: Vec<String> = Vec::new();
    let mut cur: *mut ffi::MDB_cursor = ptr::null_mut();
    // SAFETY: txn/dbi live; cursor closed below.
    unsafe { assert_eq!(ffi::mdb_cursor_open(txn, plain, &mut cur), 0) };
    {
        let mut row = |id: &str, s: String| t.push(format!("{id} = {s}"));
        // Position on "b" first, then try every dup op on the non-dup DB.
        row("nondup_set_b", cursor_probe(cur, Some(b"b"), None, MDB_SET));
        row(
            "nondup_first_dup",
            cursor_probe(cur, None, None, MDB_FIRST_DUP),
        );
        row(
            "nondup_last_dup",
            cursor_probe(cur, None, None, MDB_LAST_DUP),
        );
        row(
            "nondup_next_dup",
            cursor_probe(cur, None, None, MDB_NEXT_DUP),
        );
        row(
            "nondup_prev_dup",
            cursor_probe(cur, None, None, MDB_PREV_DUP),
        );
        row(
            "nondup_next_nodup",
            cursor_probe(cur, None, None, MDB_NEXT_NODUP),
        );
        row(
            "nondup_prev_nodup",
            cursor_probe(cur, None, None, MDB_PREV_NODUP),
        );
        row(
            "nondup_get_both",
            cursor_probe(cur, Some(b"b"), Some(b"2"), MDB_GET_BOTH),
        );
        row(
            "nondup_get_both_range",
            cursor_probe(cur, Some(b"b"), Some(b"1"), MDB_GET_BOTH_RANGE),
        );
    }
    // SAFETY: cursor belongs to txn, closed before reuse of the txn below.
    unsafe { ffi::mdb_cursor_close(cur) };

    let mut cur: *mut ffi::MDB_cursor = ptr::null_mut();
    // SAFETY: txn/dbi live; cursor closed below.
    unsafe { assert_eq!(ffi::mdb_cursor_open(txn, dup, &mut cur), 0) };
    {
        let mut row = |id: &str, s: String| t.push(format!("{id} = {s}"));
        // Unpositioned dup ops on a dup DB (fresh cursor).
        row(
            "dup_unpos_first_dup",
            cursor_probe(cur, None, None, MDB_FIRST_DUP),
        );
        row(
            "dup_unpos_next_dup",
            cursor_probe(cur, None, None, MDB_NEXT_DUP),
        );
        row(
            "dup_unpos_prev_dup",
            cursor_probe(cur, None, None, MDB_PREV_DUP),
        );
        // Positioned walk: NEXT crosses dups then keys; NEXT_DUP refuses the
        // key boundary; NEXT_NODUP skips it; PREV_NODUP lands on the LAST dup.
        row("dup_set_a", cursor_probe(cur, Some(b"a"), None, MDB_SET));
        row("dup_next_from_a1", cursor_probe(cur, None, None, MDB_NEXT));
        row(
            "dup_next_dup_at_last_dup",
            cursor_probe(cur, None, None, MDB_NEXT_DUP),
        );
        row(
            "dup_next_at_last_dup",
            cursor_probe(cur, None, None, MDB_NEXT),
        );
        row(
            "dup_next_dup_at_b",
            cursor_probe(cur, None, None, MDB_NEXT_DUP),
        );
        row(
            "dup_set_a_again",
            cursor_probe(cur, Some(b"a"), None, MDB_SET),
        );
        row(
            "dup_next_nodup_from_a",
            cursor_probe(cur, None, None, MDB_NEXT_NODUP),
        );
        row(
            "dup_prev_nodup_from_b",
            cursor_probe(cur, None, None, MDB_PREV_NODUP),
        );
    }
    // SAFETY: close cursor then abort txn (reverse creation order).
    unsafe {
        ffi::mdb_cursor_close(cur);
        ffi::mdb_txn_abort(txn);
    }

    assert_pinned(
        &t,
        r#"
nondup_set_b = OK key=b data=2
nondup_first_dup = INCOMPATIBLE
nondup_last_dup = INCOMPATIBLE
nondup_next_dup = OK key=c data=3
nondup_prev_dup = OK key=b data=2
nondup_next_nodup = OK key=c data=3
nondup_prev_nodup = OK key=b data=2
nondup_get_both = INCOMPATIBLE
nondup_get_both_range = INCOMPATIBLE
dup_unpos_first_dup = EINVAL
dup_unpos_next_dup = OK key=a data=1
dup_unpos_prev_dup = NOTFOUND
dup_set_a = OK key=a data=1
dup_next_from_a1 = OK key=a data=2
dup_next_dup_at_last_dup = NOTFOUND
dup_next_at_last_dup = OK key=b data=9
dup_next_dup_at_b = NOTFOUND
dup_set_a_again = OK key=a data=1
dup_next_nodup_from_a = OK key=b data=9
dup_prev_nodup_from_b = OK key=a data=2
"#,
    );
}

// ---------------------------------------------------------------------------
// Pin F3 — raw dbi-open flag validation (unknown bits → EINVAL, Table 2
// note) and the §S2-analogue out-data contract for NOOVERWRITE / NODUPDATA
// on a dup DB (does the fork hand back the existing item?).
// ---------------------------------------------------------------------------
#[test]
fn pin_dbi_flags_and_put_out_contract() {
    let fork = Fork::new();
    let txn = fork.txn();
    let mut t: Vec<String> = Vec::new();

    // Unknown flag bit (0x1000 is not in VALID_FLAGS).
    let (rc, _) = fork.dbi(txn, "x1", 0x1000 | MDB_CREATE);
    t.push(format!("dbi_open_unknown_bit = {}", rc_name(rc)));
    // A dup-modifier without DUPSORT itself (REVERSEDUP alone).
    let (rc, _) = fork.dbi(txn, "x2", 0x40 | MDB_CREATE);
    t.push(format!("dbi_open_reversedup_alone = {}", rc_name(rc)));
    // DUPFIXED alone (0x10) without DUPSORT.
    let (rc, _) = fork.dbi(txn, "x3", 0x10 | MDB_CREATE);
    t.push(format!("dbi_open_dupfixed_alone = {}", rc_name(rc)));

    let (rc, dup) = fork.dbi(txn, "dup", MDB_DUPSORT | MDB_CREATE);
    assert_eq!(rc, 0);
    assert_eq!(put(txn, dup, b"k", b"v1", 0), 0);
    assert_eq!(put(txn, dup, b"k", b"v5", 0), 0);

    // NOOVERWRITE on an existing key of a dup DB: rc + what data points at.
    {
        let mut key = val(b"k");
        let mut data = val(b"v3");
        // SAFETY: vals point at live slices; out-val read back inside the txn.
        let rc = unsafe { ffi::mdb_put(txn, dup, &mut key, &mut data, MDB_NOOVERWRITE) };
        let shown = if rc == ffi::MDB_KEYEXIST {
            // SAFETY: on KEYEXIST LMDB documents data -> the existing item.
            unsafe { format!("KEYEXIST data_out={}", render_val(&data)) }
        } else {
            rc_name(rc)
        };
        t.push(format!("nooverwrite_existing_key_out = {shown}"));
    }
    // NODUPDATA on an exactly-existing pair: rc + whether data is rewritten.
    {
        let mut key = val(b"k");
        let mut data = val(b"v5");
        // SAFETY: as above.
        let rc = unsafe { ffi::mdb_put(txn, dup, &mut key, &mut data, MDB_NODUPDATA) };
        let shown = if rc == ffi::MDB_KEYEXIST {
            // SAFETY: as above.
            unsafe { format!("KEYEXIST data_out={}", render_val(&data)) }
        } else {
            rc_name(rc)
        };
        t.push(format!("nodupdata_exact_pair_out = {shown}"));
    }

    // SAFETY: no cursors open; abort ends the txn.
    unsafe { ffi::mdb_txn_abort(txn) };

    assert_pinned(
        &t,
        r#"
dbi_open_unknown_bit = EINVAL
dbi_open_reversedup_alone = OK
dbi_open_dupfixed_alone = OK
nooverwrite_existing_key_out = KEYEXIST data_out=v1
nodupdata_exact_pair_out = KEYEXIST data_out=v5
"#,
    );
}

fn assert_pinned(rows: &[String], expected: &str) {
    let got = rows.join("\n");
    let expected = expected.trim();
    assert_eq!(
        got, expected,
        "\n--- observed (fork) ---\n{got}\n--- pinned ---\n{expected}\n"
    );
}
