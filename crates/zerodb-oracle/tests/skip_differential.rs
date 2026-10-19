//! `Iterator::nth` / `skip` on the read iterators, real heed (LMDB fork)
//! against `heed-zerodb`. heed over LMDB takes std's default `nth` (one
//! `MDB_NEXT`/`MDB_PREV` per skipped entry); ZeroDB skips leaf by leaf
//! (SPEC 03 §4 "advance_by / retreat_by"). Both must yield the same entries
//! for every iterator kind, bound shape and direction.
//!
//! ZeroDB runs on 4 KiB pages so a skip crosses many leaves; the results do
//! not depend on page size.

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A throwaway directory (no `tempfile` dependency in this crate).
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-skip-diff-{pid}-{seq}"));
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

/// xorshift64* — deterministic, no `rand`.
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
            .map(|_| b'a' + self.below(u64::from(alphabet)) as u8)
            .collect()
    }
}

/// One query: an iterator kind and its bounds, then a list of `nth` arguments
/// applied in turn, then a `skip(s).take(t)` on a fresh iterator.
#[derive(Debug, Clone)]
struct Query {
    kind: u8,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    prefix: Vec<u8>,
    nths: Vec<usize>,
    skip: usize,
    take: usize,
}

type Pair = (Vec<u8>, Vec<u8>);

/// What one engine yields for a [`Query`]: the `nth` results, then the
/// `skip/take` window.
type Answer = (Vec<Option<Pair>>, Vec<Pair>);

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
    }
}

/// The same body over both crates, so neither side can be given a different
/// query.
macro_rules! engine {
    ($m:ident, $heed:ident, $page:expr) => {
        mod $m {
            use super::*;
            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvOpenOptions, RoTxn, WithoutTls};

            pub fn build(
                dir: &Path,
                keys: &[Vec<u8>],
                vals: &[Vec<u8>],
            ) -> (Env<WithoutTls>, Database<Bytes, Bytes>) {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(256 << 20).max_dbs(1);
                $page(&mut o);
                // SAFETY: a private temp dir opened by this process only.
                let env = unsafe { o.open(dir) }.expect("open");
                let mut w = env.write_txn().expect("write_txn");
                let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).expect("create");
                for (k, v) in keys.iter().zip(vals) {
                    db.put(&mut w, k, v).expect("put");
                }
                // Uneven leaves: delete every 7th key.
                for k in keys.iter().step_by(7) {
                    db.delete(&mut w, k).expect("delete");
                }
                w.commit().expect("commit");
                (env, db)
            }

            fn iter<'t>(
                r: &'t RoTxn<'_, WithoutTls>,
                db: Database<Bytes, Bytes>,
                q: &Query,
            ) -> Box<dyn Iterator<Item = $heed::Result<(&'t [u8], &'t [u8])>> + 't> {
                let span = (as_ref(&q.lo), as_ref(&q.hi));
                match q.kind {
                    0 => Box::new(db.iter(r).expect("iter")),
                    1 => Box::new(db.rev_iter(r).expect("rev_iter")),
                    2 => Box::new(db.range(r, &span).expect("range")),
                    3 => Box::new(db.rev_range(r, &span).expect("rev_range")),
                    4 => Box::new(db.prefix_iter(r, &q.prefix).expect("prefix_iter")),
                    _ => Box::new(db.rev_prefix_iter(r, &q.prefix).expect("rev_prefix_iter")),
                }
            }

            pub fn answer(env: &Env<WithoutTls>, db: Database<Bytes, Bytes>, q: &Query) -> Answer {
                let r = env.read_txn().expect("read_txn");
                let own = |kv: $heed::Result<(&[u8], &[u8])>| {
                    let (k, v) = kv.expect("entry");
                    (k.to_vec(), v.to_vec())
                };
                let mut it = iter(&r, db, q);
                let nths = q.nths.iter().map(|&n| it.nth(n).map(own)).collect();
                let window = iter(&r, db, q).skip(q.skip).take(q.take).map(own).collect();
                (nths, window)
            }
        }
    };
}

engine!(lmdb, heed, |_: &mut heed::EnvOpenOptions<
    heed::WithoutTls,
>| {});
engine!(zdb, heed_zerodb, |o: &mut heed_zerodb::EnvOpenOptions<
    heed_zerodb::WithoutTls,
>| {
    o.page_size(4096);
});

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

fn count(rng: &mut Rng, total: usize) -> usize {
    match rng.below(5) {
        0 => 0,
        1 => rng.below(4) as usize,
        2 => rng.below(64) as usize,
        3 => rng.below(total as u64 + 4) as usize,
        _ => total + rng.below(4) as usize,
    }
}

fn run(seed: u64) {
    let mut rng = Rng(seed | 1);
    let n = match rng.below(4) {
        0 => rng.below(8),
        1 => rng.below(300),
        _ => rng.below(5000),
    } as usize;
    let keys: Vec<Vec<u8>> = (0..n).map(|_| rng.bytes(24, 8)).collect();
    let vals: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            // Some values overflow a 4 KiB page.
            let len = if rng.below(50) == 0 {
                6000
            } else {
                rng.below(120)
            };
            (0..len).map(|i| i as u8).collect()
        })
        .collect();
    let (dl, dz) = (TempDir::new(), TempDir::new());
    let (le, ldb) = lmdb::build(dl.path(), &keys, &vals);
    let (ze, zdb_) = zdb::build(dz.path(), &keys, &vals);

    for _ in 0..80 {
        let q = Query {
            kind: rng.below(6) as u8,
            lo: bound(&mut rng, &keys),
            hi: bound(&mut rng, &keys),
            prefix: rng.bytes(3, 6),
            nths: (0..1 + rng.below(6)).map(|_| count(&mut rng, n)).collect(),
            skip: count(&mut rng, n),
            take: rng.below(200) as usize,
        };
        let want = lmdb::answer(&le, ldb, &q);
        let got = zdb::answer(&ze, zdb_, &q);
        assert_eq!(got, want, "seed {seed} query {q:?}");
    }
}

#[test]
fn nth_and_skip_match_lmdb() {
    for seed in 0..48u64 {
        run(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(3));
    }
}
