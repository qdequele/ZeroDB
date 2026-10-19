//! `Iterator::nth` on the read iterators skips leaf by leaf (SPEC 03 §4
//! "advance_by / retreat_by"). It must yield exactly what `n` calls to `next`
//! followed by one more yield, for every iterator kind, bound shape and
//! direction, on committed and dirty trees. `Iterator::skip` reaches it, so
//! `skip(s).take(l)` is checked against slicing the full scan too.

#![cfg(not(feature = "unique-txns"))]

use std::ops::Bound;

use heed_zerodb::types::Bytes;
use heed_zerodb::{Database, EnvOpenOptions, RoTxn, WithoutTls};

/// xorshift64* — deterministic, no new dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn bytes(&mut self, max_len: u64, alphabet: u8) -> Vec<u8> {
        let len = 1 + self.below(max_len);
        (0..len)
            .map(|_| b'a' + self.below(alphabet as u64) as u8)
            .collect()
    }
}

type Item = Option<(Vec<u8>, Vec<u8>)>;

fn own(r: Option<heed_zerodb::Result<(&[u8], &[u8])>>) -> Item {
    r.map(|r| {
        let (k, v) = r.unwrap();
        (k.to_vec(), v.to_vec())
    })
}

fn bound(rng: &mut Rng, keys: &[Vec<u8>]) -> Bound<Vec<u8>> {
    let k = if !keys.is_empty() && rng.below(2) == 0 {
        keys[rng.below(keys.len() as u64) as usize].clone()
    } else {
        rng.bytes(6, 6)
    };
    match rng.below(3) {
        0 => Bound::Unbounded,
        1 => Bound::Included(k),
        _ => Bound::Excluded(k),
    }
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
    }
}

/// Build one iterator of `kind` twice (reference + under test) and drive both
/// with the same random op sequence: `next`, or `nth(k)` on the tested one vs
/// `k` `next`s plus one on the reference.
fn check_one(
    rng: &mut Rng,
    txn: &RoTxn<'_, WithoutTls>,
    db: Database<Bytes, Bytes>,
    keys: &[Vec<u8>],
    total: usize,
) {
    let kind = rng.below(6);
    let lo = bound(rng, keys);
    let hi = bound(rng, keys);
    let prefix = rng.bytes(3, 6);
    macro_rules! mk {
        () => {{
            let it: Box<dyn Iterator<Item = heed_zerodb::Result<(&[u8], &[u8])>>> = match kind {
                0 => Box::new(db.iter(txn).unwrap()),
                1 => Box::new(db.rev_iter(txn).unwrap()),
                2 => Box::new(db.range(txn, &(as_ref(&lo), as_ref(&hi))).unwrap()),
                3 => Box::new(db.rev_range(txn, &(as_ref(&lo), as_ref(&hi))).unwrap()),
                4 => Box::new(db.prefix_iter(txn, &prefix).unwrap()),
                _ => Box::new(db.rev_prefix_iter(txn, &prefix).unwrap()),
            };
            it
        }};
    }
    let mut reference = mk!();
    let mut tested = mk!();
    for _ in 0..12 {
        let k = match rng.below(5) {
            0 => 0,
            1 => rng.below(4) as usize,
            2 => rng.below(64) as usize,
            3 => rng.below(total as u64 + 4) as usize,
            _ => total + rng.below(4) as usize,
        };
        let mut want = None;
        for _ in 0..=k {
            want = own(reference.next());
        }
        let got = own(tested.nth(k));
        assert_eq!(
            got, want,
            "kind {kind} lo {lo:?} hi {hi:?} prefix {prefix:?} nth({k})"
        );
        if rng.below(2) == 0 {
            assert_eq!(own(tested.next()), own(reference.next()), "follow-up next");
        }
    }
}

fn run(seed: u64) {
    let mut rng = Rng(seed | 1);
    let dir = tempfile::tempdir().unwrap();
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(256 << 20).max_dbs(1).page_size(4096);
    let env = unsafe { o.open(dir.path()) }.unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    let n = match rng.below(4) {
        0 => rng.below(8),
        1 => rng.below(300),
        _ => rng.below(6000),
    } as usize;
    let mut keys = Vec::new();
    for _ in 0..n {
        let k = rng.bytes(24, 8);
        let vlen = if rng.below(50) == 0 {
            6000
        } else {
            rng.below(120)
        };
        let v: Vec<u8> = (0..vlen).map(|i| i as u8).collect();
        db.put(&mut wtxn, &k, &v).unwrap();
        keys.push(k);
    }
    // Delete a slice of keys so leaves are unevenly filled.
    for k in keys.iter().step_by(7) {
        db.delete(&mut wtxn, k).unwrap();
    }
    wtxn.commit().unwrap();
    keys.sort();
    keys.dedup();
    let total = db.len(&env.read_txn().unwrap()).unwrap() as usize;

    let rtxn = env.read_txn().unwrap();
    for _ in 0..60 {
        check_one(&mut rng, &rtxn, db, &keys, total);
    }
    drop(rtxn);

    // Dirty frames: iterate inside a write txn that has modified the tree.
    let mut wtxn = env.write_txn().unwrap();
    for _ in 0..(n / 3) {
        let k = rng.bytes(24, 8);
        db.put(&mut wtxn, &k, b"dirty").unwrap();
    }
    let total = db.len(&wtxn).unwrap() as usize;
    for _ in 0..30 {
        check_one(&mut rng, &wtxn, db, &keys, total);
    }
}

#[test]
fn nth_matches_repeated_next() {
    for seed in 0..64u64 {
        run(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(17));
    }
}

/// `skip(s).take(l)` — the crud-bench shape — equals slicing the full scan.
#[test]
fn skip_take_matches_slice() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(256 << 20).max_dbs(1);
    let env = unsafe { o.open(dir.path()) }.unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    let mut rng = Rng(42);
    for _ in 0..20_000 {
        db.put(&mut wtxn, &rng.bytes(26, 26), b"v").unwrap();
    }
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let all: Vec<_> = db
        .iter(&rtxn)
        .unwrap()
        .map(|r| r.unwrap().0.to_vec())
        .collect();
    let rev: Vec<_> = all.iter().rev().cloned().collect();
    for s in [
        0,
        1,
        99,
        100,
        5000,
        19_000,
        all.len() - 1,
        all.len(),
        all.len() + 5,
    ] {
        let got: Vec<_> = db
            .iter(&rtxn)
            .unwrap()
            .skip(s)
            .take(100)
            .map(|r| r.unwrap().0.to_vec())
            .collect();
        let want: Vec<_> = all.iter().skip(s).take(100).cloned().collect();
        assert_eq!(got, want, "fwd skip {s}");
        let got: Vec<_> = db
            .rev_iter(&rtxn)
            .unwrap()
            .skip(s)
            .take(100)
            .map(|r| r.unwrap().0.to_vec())
            .collect();
        let want: Vec<_> = rev.iter().skip(s).take(100).cloned().collect();
        assert_eq!(got, want, "rev skip {s}");
        assert_eq!(
            db.iter(&rtxn).unwrap().skip(s).count(),
            all.len().saturating_sub(s)
        );
    }
}

// ---------------------------------------------------------------------------
// Corruption: what a skip reads and what it reports (SPEC 03 §4)
// ---------------------------------------------------------------------------

const PS: usize = 4096;
/// Page-type flags and reserved header fields (SPEC 02 §1, §2.1).
const OFF_FLAGS: usize = 16;
const P_LEAF: u16 = 0x0001;
const P_OVERFLOW: u16 = 0x0004;
/// Overflow head: `reserved2` (offset 28, MUST be 0).
const OFF_OVF_RESERVED2: usize = 28;
/// Leaf: `leaf2_ksize` (offset 28, MUST be 0).
const OFF_LEAF2_KSIZE: usize = 28;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

/// 1000 keys on 4 KiB pages; key 500 holds `big` (an overflow value when it
/// exceeds a page), every other key a short value. The env is closed on return.
fn build_corruptible(dir: &std::path::Path, big: &[u8]) {
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(16 << 20).max_dbs(1).page_size(PS as u32);
    let env = unsafe { o.open(dir) }.unwrap();
    let mut w = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
    for i in 0..1000 {
        let v: &[u8] = if i == 500 { big } else { b"small" };
        db.put(&mut w, &key(i), v).unwrap();
    }
    w.commit().unwrap();
}

fn reopen(dir: &std::path::Path) -> (heed_zerodb::Env<WithoutTls>, Database<Bytes, Bytes>) {
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(16 << 20).max_dbs(1);
    let env = unsafe { o.open(dir) }.unwrap();
    let r = env.read_txn().unwrap();
    let db = env.open_database(&r, None).unwrap().unwrap();
    drop(r);
    (env, db)
}

/// Set a 4-byte field of the first page of type `flag` whose bytes contain
/// `needle`, and return that page's number.
fn damage_page(dir: &std::path::Path, flag: u16, needle: &[u8], off: usize) -> usize {
    let path = dir.join(zerodb::HEED_DATA_FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();
    let pgno = (2..bytes.len() / PS)
        .find(|&p| {
            let page = &bytes[p * PS..(p + 1) * PS];
            u16::from_le_bytes([page[OFF_FLAGS], page[OFF_FLAGS + 1]]) & flag != 0
                && page.windows(needle.len()).any(|w| w == needle)
        })
        .expect("page to damage");
    bytes[pgno * PS + off..pgno * PS + off + 4].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    pgno
}

/// A skip never reads a skipped entry's value: a damaged overflow page behind
/// a skipped entry goes unnoticed, while landing on that entry, or stepping
/// over it with `next`, reports it.
#[test]
fn skip_does_not_read_skipped_overflow_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut big = b"OVERFLOW-MARKER".to_vec();
    big.resize(3 * PS, 0x7A);
    build_corruptible(dir.path(), &big);
    damage_page(
        dir.path(),
        P_OVERFLOW,
        b"OVERFLOW-MARKER",
        OFF_OVF_RESERVED2,
    );
    let (env, db) = reopen(dir.path());
    let r = env.read_txn().unwrap();

    // The damage is real: stepping onto entry 500 reports it.
    assert!(db.iter(&r).unwrap().nth(500).unwrap().is_err());
    let stepped: Vec<_> = db.iter(&r).unwrap().take(501).collect();
    assert!(stepped[500].is_err(), "next reads every value");

    // Skipping past it does not read it.
    let (k, v) = db.iter(&r).unwrap().nth(600).unwrap().unwrap();
    assert_eq!((k, v), (&key(600)[..], &b"small"[..]));
    let (k, _) = db.rev_iter(&r).unwrap().nth(999 - 400).unwrap().unwrap();
    assert_eq!(k, &key(400)[..]);
}

/// A page error met while a skip crosses leaves is the item `nth` returns,
/// and the iterator ends after it.
#[test]
fn skip_reports_a_damaged_leaf_it_crosses() {
    let dir = tempfile::tempdir().unwrap();
    build_corruptible(dir.path(), b"small");
    damage_page(dir.path(), P_LEAF, &key(500), OFF_LEAF2_KSIZE);
    let (env, db) = reopen(dir.path());
    let r = env.read_txn().unwrap();

    for rev in [false, true] {
        let mut it: Box<dyn Iterator<Item = heed_zerodb::Result<(&[u8], &[u8])>>> = if rev {
            Box::new(db.rev_iter(&r).unwrap())
        } else {
            Box::new(db.iter(&r).unwrap())
        };
        assert!(
            it.nth(900).unwrap().is_err(),
            "rev={rev}: crossing the damaged leaf"
        );
        assert!(
            it.next().is_none(),
            "rev={rev}: the iterator ends after the error"
        );
    }
}
