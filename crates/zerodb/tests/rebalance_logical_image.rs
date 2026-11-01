//! Golden logical tree images for delete-driven rebalancing (ADR-0028).
//!
//! Each workload below runs a fixed, seeded sequence of puts and deletes and,
//! after every commit, folds the **logical** image of the main tree into a
//! digest: for every page reached from the root, its page number, kind, free
//! space and ordered entries (key, node flags, inline value bytes or overflow
//! head and length, child page number), plus the tree's `stat` record. The
//! digest also covers a dump of the entries and the bytes of a compacting
//! copy of the final state.
//!
//! What it pins: the tree *shape* a rebalance leaves — which page each entry
//! lands on, which page numbers are allocated and freed, and when parents
//! split — not the order of cells inside a page's heap, which ADR-0028 lets
//! an in-place separator rewrite change. The digests were recorded before
//! the ADR-0028 change, so the new cell-move primitives must reproduce them
//! exactly.
//!
//! Every workload also runs in a `WRITE_MAP` env and must give the same
//! digest as the default env, and every commit passes `check::check_image`.
//!
//! A deliberate change to the tree shape (a split or rebalance policy change)
//! changes these digests. Regenerate them with
//! `ZERODB_PRINT_GOLDEN=1 cargo test -p zerodb --test rebalance_logical_image -- --nocapture`
//! and say in the change why the shape moved.

mod common;
use common::TempDir;

use std::path::PathBuf;

use zerodb::{check, CompactionOption, CopyToFile, Database, Env, EnvFlags, EnvOpenOptions};
use zerodb::{RwTxn, TxnRead};
use zerodb_core::page::{BranchRef, LeafRef, LeafValue, PGNO_INVALID};

/// FNV-1a, 64-bit: a fixed, dependency-free digest, stable across Rust
/// releases and architectures (unlike `DefaultHasher`).
struct Fnv(u64);

impl Fnv {
    fn new() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
    fn bytes(&mut self, b: &[u8]) {
        for &x in b {
            self.0 ^= u64::from(x);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
    /// A length-prefixed byte string, so adjacent fields cannot alias.
    fn field(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.bytes(b);
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            v.swap(i, j);
        }
    }
}

/// One workload run: its env, its page size, and the running digest.
struct Run {
    _dir: TempDir,
    dir: PathBuf,
    env: Env,
    db: Database,
    psize: u32,
    digest: Fnv,
}

impl Run {
    fn new(psize: u32, flags: EnvFlags) -> Run {
        let dir = TempDir::new();
        let mut opts = EnvOpenOptions::new();
        opts.map_size(64 << 20).page_size(psize).flags(flags);
        let env = opts.open(dir.path()).expect("open env");
        let db = env.main_database();
        let path = dir.path().to_path_buf();
        Run {
            _dir: dir,
            dir: path,
            env,
            db,
            psize,
            digest: Fnv::new(),
        }
    }

    /// Run `f` in one write txn, commit, then fold the committed image in.
    fn txn(&mut self, f: impl FnOnce(&Database, &mut RwTxn<'_>)) {
        let mut w = self.env.write_txn().expect("write txn");
        f(&self.db, &mut w);
        w.commit().expect("commit");
        self.fold_image();
    }

    fn fold_image(&mut self) {
        let bytes = std::fs::read(self.dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
        let v = check::check_image(&bytes, self.psize);
        assert!(v.is_empty(), "invariant violations: {v:#?}");
        let rec = {
            let r = self.env.read_txn().expect("read txn");
            *TxnRead::main_record(&r)
        };
        let d = &mut self.digest;
        d.u64(rec.root);
        d.u64(u64::from(rec.depth));
        d.u64(rec.entries);
        d.u64(rec.leaf_pages);
        d.u64(rec.branch_pages);
        d.u64(rec.overflow_pages);
        if rec.root != PGNO_INVALID {
            walk(d, &bytes, self.psize, rec.root, rec.depth);
        }
    }

    /// Fold a dump of every entry and the bytes of a compacting copy.
    fn finish(mut self) -> u64 {
        {
            let r = self.env.read_txn().expect("read txn");
            for item in self.db.iter(&r) {
                let (k, v) = item.expect("entry");
                self.digest.field(k);
                self.digest.field(v);
            }
        }
        let out = self.dir.join("compact.dat");
        self.env
            .copy_to_file(&out, CompactionOption::Enabled)
            .expect("compacting copy");
        let copied = std::fs::read(&out).expect("read copy");
        self.digest.field(&copied);
        self.digest.0
    }
}

/// Depth-first, child order: every reachable tree page's logical content.
fn walk(d: &mut Fnv, file: &[u8], psize: u32, pgno: u64, depth: u16) {
    let ps = psize as usize;
    let page = &file[pgno as usize * ps..(pgno as usize + 1) * ps];
    d.u64(pgno);
    if depth > 1 {
        let b = BranchRef::new(page, psize).expect("branch page decodes");
        d.u64(2);
        d.u64(b.free_space() as u64);
        d.u64(b.num_keys() as u64);
        for i in 0..b.num_keys() {
            d.field(b.key(i));
            d.u64(b.child_pgno(i));
        }
        for i in 0..b.num_keys() {
            walk(d, file, psize, b.child_pgno(i), depth - 1);
        }
    } else {
        let l = LeafRef::new(page, psize).expect("leaf page decodes");
        d.u64(1);
        d.u64(l.free_space() as u64);
        d.u64(l.num_keys() as u64);
        for i in 0..l.num_keys() {
            d.field(l.key(i));
            d.u64(u64::from(l.node_flags(i)));
            match l.value(i) {
                LeafValue::Inline(v) => d.field(v),
                LeafValue::Overflow { head_pgno, dsize } => {
                    d.u64(head_pgno);
                    d.u64(u64::from(dsize));
                }
            }
        }
    }
}

fn fixed_key(i: u64) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

/// A 26-byte key from a random id, as crud-bench's string keys.
fn crud_key(id: u64) -> Vec<u8> {
    format!("{id:026x}").into_bytes()
}

fn val(i: u64, len: usize) -> Vec<u8> {
    let mut v = vec![(i % 251) as u8; len];
    v[..8.min(len)].copy_from_slice(&i.to_le_bytes()[..8.min(len)]);
    v
}

/// The workloads, each a function of the env flags so it runs twice.
type Workload = fn(EnvFlags) -> u64;

/// Ascending point deletes of the first half (child-0 / right-sibling
/// pairing, the `del/bulk/half` shape), then the rest.
fn ascending_drain(flags: EnvFlags) -> u64 {
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..6000 {
            db.put(w, &fixed_key(i), &val(i, 256)).unwrap();
        }
    });
    r.txn(|db, w| {
        for i in 0..3000 {
            assert!(db.delete(w, &fixed_key(i)).unwrap());
        }
    });
    r.txn(|db, w| {
        for i in 3000..6000 {
            assert!(db.delete(w, &fixed_key(i)).unwrap());
        }
    });
    r.finish()
}

/// Descending point deletes (left-sibling pairing: borrow and merge from
/// the left at leaf and branch levels).
fn descending_drain(flags: EnvFlags) -> u64 {
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..6000 {
            db.put(w, &fixed_key(i), &val(i, 200)).unwrap();
        }
    });
    r.txn(|db, w| {
        for i in (1000..6000).rev() {
            assert!(db.delete(w, &fixed_key(i)).unwrap());
        }
    });
    r.finish()
}

/// Front-to-back write-cursor drains: the whole first half, then every other
/// entry of the rest (the `del_current` kept-path repairs, SPEC 03 §5.4a).
fn cursor_drain(flags: EnvFlags) -> u64 {
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..6000 {
            db.put(w, &fixed_key(i), &val(i, 256)).unwrap();
        }
    });
    r.txn(|db, w| {
        let mut cur = db.rw_cursor(w);
        let mut n = 0;
        while let Some((_k, _v)) = cur.move_next().unwrap() {
            if n == 3000 {
                break;
            }
            assert!(cur.del_current().unwrap());
            n += 1;
        }
    });
    r.txn(|db, w| {
        let mut cur = db.rw_cursor(w);
        let mut n = 0u64;
        while let Some((_k, _v)) = cur.move_next().unwrap() {
            if n.is_multiple_of(2) {
                assert!(cur.del_current().unwrap());
            }
            n += 1;
        }
    });
    r.finish()
}

/// crud-bench's shape: random 26-byte keys, ~1.5 KB values, 16 KiB pages,
/// deleted in random order over several commits.
fn crud_random(flags: EnvFlags) -> u64 {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut ids: Vec<u64> = (0..4000).map(|_| rng.next()).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut r = Run::new(16384, flags);
    let mut order = ids.clone();
    rng.shuffle(&mut order);
    r.txn(|db, w| {
        for &id in &order {
            db.put(w, &crud_key(id), &val(id, 1500)).unwrap();
        }
    });
    rng.shuffle(&mut order);
    for chunk in order[..3600].chunks(600) {
        r.txn(|db, w| {
            for &id in chunk {
                assert!(db.delete(w, &crud_key(id)).unwrap());
            }
        });
    }
    r.finish()
}

/// Variable-length keys (1..=200 bytes) with small values: separators change
/// size when rewritten, so some rewrites grow into a full parent and split it.
fn variable_keys(flags: EnvFlags) -> u64 {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut keys: Vec<Vec<u8>> = (0..5000)
        .map(|_| {
            let len = 1 + rng.below(200) as usize;
            (0..len).map(|_| b'a' + rng.below(26) as u8).collect()
        })
        .collect();
    keys.sort();
    keys.dedup();
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for (i, k) in keys.iter().enumerate() {
            db.put(w, k, &val(i as u64, 16)).unwrap();
        }
    });
    let mut order: Vec<usize> = (0..keys.len()).collect();
    rng.shuffle(&mut order);
    let cut = order.len() * 4 / 5;
    for chunk in order[..cut].chunks(1000) {
        r.txn(|db, w| {
            for &i in chunk {
                assert!(db.delete(w, &keys[i]).unwrap());
            }
        });
    }
    r.finish()
}

/// ~500-byte keys keep branch fanout near 7, so the tree reaches depth 3 and
/// deletes cascade merges and branch borrows through every level.
fn wide_keys_deep(flags: EnvFlags) -> u64 {
    let wide = |i: u64| {
        let mut k = format!("wk{i:06}").into_bytes();
        k.resize(500, b'.');
        k
    };
    let mut rng = Rng(0x5851_f42d_4c95_7f2d);
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..600 {
            db.put(w, &wide(i), &val(i, 24)).unwrap();
        }
    });
    let mut order: Vec<u64> = (0..600).collect();
    rng.shuffle(&mut order);
    for chunk in order.chunks(150) {
        r.txn(|db, w| {
            for &i in chunk {
                assert!(db.delete(w, &wide(i)).unwrap());
            }
        });
    }
    r.finish()
}

/// Overflow values mixed with inline ones: a moved `F_BIGDATA` cell must
/// carry its run untouched.
fn overflow_mix(flags: EnvFlags) -> u64 {
    let mut rng = Rng(0x1405_7b7e_f767_814f);
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..3000u64 {
            let len = if i.is_multiple_of(5) { 6000 } else { 300 };
            db.put(w, &fixed_key(i), &val(i, len)).unwrap();
        }
    });
    let mut order: Vec<u64> = (0..3000).collect();
    rng.shuffle(&mut order);
    for chunk in order[..2400].chunks(400) {
        r.txn(|db, w| {
            for &i in chunk {
                assert!(db.delete(w, &fixed_key(i)).unwrap());
            }
        });
    }
    r.finish()
}

/// Delete and re-insert the same slice repeatedly (the `del/churn/reinsert`
/// shape): freed pages are reclaimed and handed straight back out.
fn churn_reinsert(flags: EnvFlags) -> u64 {
    let mut r = Run::new(4096, flags);
    r.txn(|db, w| {
        for i in 0..6000 {
            db.put(w, &fixed_key(i), &val(i, 256)).unwrap();
        }
    });
    for round in 0..3u64 {
        r.txn(|db, w| {
            for i in 0..1500 {
                assert!(db.delete(w, &fixed_key(i)).unwrap());
            }
        });
        r.txn(|db, w| {
            for i in 0..1500 {
                db.put(w, &fixed_key(i), &val(i + round, 256)).unwrap();
            }
        });
    }
    r.finish()
}

/// Recorded on the engine before the ADR-0028 change (main at f98cd08), at a
/// 64 MiB map (see the module docs to regenerate).
const GOLDEN: &[(&str, Workload, u64)] = &[
    ("ascending_drain", ascending_drain, 0x0bbfd378eedf36aa),
    ("descending_drain", descending_drain, 0x6738322c1de4c585),
    ("cursor_drain", cursor_drain, 0x641245be963b8e41),
    ("crud_random", crud_random, 0xb6f370b6839111ab),
    ("variable_keys", variable_keys, 0x615ffce7ec930f9f),
    ("wide_keys_deep", wide_keys_deep, 0xf631cac21fb2d132),
    ("overflow_mix", overflow_mix, 0x1d2d5cccd96c3ca4),
    ("churn_reinsert", churn_reinsert, 0xdb7fa7ecf7640449),
];

#[test]
fn delete_rebalance_logical_images_match_golden() {
    let print = std::env::var_os("ZERODB_PRINT_GOLDEN").is_some();
    let mut failures = Vec::new();
    for &(name, workload, want) in GOLDEN {
        let heap = workload(EnvFlags::EMPTY);
        let in_map = workload(EnvFlags::WRITE_MAP);
        assert_eq!(
            heap, in_map,
            "{name}: WRITE_MAP and default env give different logical images"
        );
        if print {
            println!("    (\"{name}\", {name}, {heap:#018x}),");
        }
        if heap != want {
            failures.push(format!("{name}: got {heap:#018x}, golden {want:#018x}"));
        }
    }
    assert!(
        print || failures.is_empty(),
        "logical tree images changed:\n{}",
        failures.join("\n")
    );
}
