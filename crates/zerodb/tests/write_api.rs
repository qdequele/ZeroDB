//! Milestone 1.4 write-path integration tests over **real files**: the commit
//! pipeline end-to-end (SPEC 04 §9), read-after-commit through the fixed
//! full-`map_size` map (ADR-0004 D4), reopen durability, meta slot
//! alternation (TXN-63), PREV_SNAPSHOT over real commits (TXN-65..67), split
//! exact-fit boundaries at both page-size extremes (ADR-0004 D7 risk 2), and
//! the multi-MB overflow value the coordinator scoped in.
//!
//! Every committed image is validated with `zerodb::check::check_image`
//! (SPEC 03 §11, minus INV-10 per ADR-0004 OQ4). Do not weaken (CLAUDE.md
//! rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Env, EnvFlags, EnvOpenOptions};

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-write-{pid}-{nanos}-{seq}"));
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

fn open(dir: &Path, map_size: usize, page_size: u32) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(map_size);
    opts.page_size(page_size);
    opts.open(dir).expect("open env")
}

/// Assert the on-disk image passes the invariant walk (SPEC 03 §11 subset).
fn assert_clean(dir: &Path, psize: u32) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, psize);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

const PS: u32 = 4096;
const MAP: usize = 8 << 20;

#[test]
fn put_commit_read_back_same_env() {
    // Read-after-commit through the same env exercises the fixed map seeing
    // the commit's pwrites (ADR-0004 D4) — no remap, no reopen.
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..500u32 {
        db.put(
            &mut wtxn,
            format!("key{i:04}").as_bytes(),
            format!("val{i}").as_bytes(),
        )
        .unwrap();
    }
    wtxn.commit().unwrap();
    assert_eq!(env.txnid(), 1);
    let rtxn = env.read_txn().unwrap();
    assert_eq!(rtxn.txnid(), 1);
    assert_eq!(db.len(&rtxn).unwrap(), 500);
    for i in (0..500u32).step_by(37) {
        assert_eq!(
            db.get(&rtxn, format!("key{i:04}").as_bytes()).unwrap(),
            Some(format!("val{i}").as_bytes())
        );
    }
    let all: Vec<_> = db.iter(&rtxn).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all.len(), 500);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
    drop(rtxn);
    assert_clean(dir.path(), PS);
}

#[test]
fn commit_then_reopen_durable() {
    let dir = TempDir::new();
    {
        let env = open(dir.path(), MAP, PS);
        let db = env.main_database();
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"persist", b"me").unwrap();
        db.put(&mut wtxn, b"big", &vec![0x5Au8; 100_000]).unwrap();
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    let env = open(dir.path(), MAP, PS);
    assert_eq!(env.txnid(), 1);
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"persist").unwrap(), Some(b"me".as_slice()));
    assert_eq!(
        db.get(&rtxn, b"big").unwrap().map(<[u8]>::len),
        Some(100_000)
    );
}

#[test]
fn multi_mb_overflow_value_through_commit() {
    // Coordinator scope: unit-test a multi-MB overflow value through
    // commit/reopen/read (the fuzz harness caps values at 64 KiB).
    let dir = TempDir::new();
    let big: Vec<u8> = (0..5_000_000u32).map(|i| (i % 251) as u8).collect();
    {
        let env = open(dir.path(), 16 << 20, PS);
        let db = env.main_database();
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"five-megabytes", &big).unwrap();
        db.put(&mut wtxn, b"small", b"s").unwrap();
        // In-txn readback of the dirty run (contiguous frame, TXN-41).
        assert_eq!(
            db.get(&wtxn, b"five-megabytes").unwrap(),
            Some(big.as_slice())
        );
        wtxn.commit().unwrap();
        // Same-env readback through the map.
        let rtxn = env.read_txn().unwrap();
        assert_eq!(
            db.get(&rtxn, b"five-megabytes").unwrap(),
            Some(big.as_slice())
        );
    }
    assert_clean(dir.path(), PS);
    let env = open(dir.path(), 16 << 20, PS);
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"five-megabytes").unwrap(),
        Some(big.as_slice())
    );
}

#[test]
fn abort_leaves_disk_untouched() {
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"committed", b"1").unwrap();
        wtxn.commit().unwrap();
    }
    let before = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"aborted", b"2").unwrap();
        db.delete(&mut wtxn, b"committed").unwrap();
        wtxn.abort();
    }
    let after = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    assert_eq!(before, after, "abort must not change the file (TXN-59)");
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"committed").unwrap(), Some(b"1".as_slice()));
    assert_eq!(db.get(&rtxn, b"aborted").unwrap(), None);
}

#[test]
fn meta_slots_alternate_and_prev_snapshot_rolls_back() {
    // TXN-63: commit N writes slot N&1; TXN-65..67: PREV_SNAPSHOT opens the
    // older snapshot and its first commit overwrites the abandoned slot.
    let dir = TempDir::new();
    {
        let env = open(dir.path(), MAP, PS);
        let db = env.main_database();
        for (i, v) in [b"one", b"two"].iter().enumerate() {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, b"state", *v).unwrap();
            wtxn.commit().unwrap();
            assert_eq!(env.txnid(), i as u64 + 1);
        }
        assert_clean(dir.path(), PS);
    }
    // Normal reopen sees txn 2.
    {
        let env = open(dir.path(), MAP, PS);
        assert_eq!(env.txnid(), 2);
        let rtxn = env.read_txn().unwrap();
        assert_eq!(
            env.main_database().get(&rtxn, b"state").unwrap(),
            Some(b"two".as_slice())
        );
    }
    // PREV_SNAPSHOT reopen sees txn 1 ("one"); one commit makes it live.
    {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(MAP);
        opts.flags(EnvFlags::PREV_SNAPSHOT);
        let env = opts.open(dir.path()).unwrap();
        assert_eq!(env.txnid(), 1);
        let db = env.main_database();
        {
            let rtxn = env.read_txn().unwrap();
            assert_eq!(db.get(&rtxn, b"state").unwrap(), Some(b"one".as_slice()));
        }
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, b"state", b"rolled-back-then-committed")
            .unwrap();
        wtxn.commit().unwrap();
        assert_eq!(
            env.txnid(),
            2,
            "first post-rollback commit is older+1 (TXN-67)"
        );
    }
    assert_clean(dir.path(), PS);
    // Normal reopen: the rolled-back-then-committed state is live; the
    // abandoned branch is gone (TXN-67 self-resolution).
    let env = open(dir.path(), MAP, PS);
    assert_eq!(env.txnid(), 2);
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        env.main_database().get(&rtxn, b"state").unwrap(),
        Some(b"rolled-back-then-committed".as_slice())
    );
}

#[test]
fn multi_commit_churn_stays_clean() {
    // put/del/clear churn across several commits; the invariant walk runs
    // after each (freed-page leak is sanctioned and excluded, ADR-0004 OQ4).
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    for round in 0..6u32 {
        let mut wtxn = env.write_txn().unwrap();
        if round == 3 {
            db.clear(&mut wtxn).unwrap();
        }
        for i in 0..200u32 {
            let k = format!("r{round}k{i:04}");
            db.put(
                &mut wtxn,
                k.as_bytes(),
                &vec![round as u8; (i % 300) as usize],
            )
            .unwrap();
        }
        for i in (0..200u32).step_by(3) {
            let k = format!("r{round}k{i:04}");
            assert!(db.delete(&mut wtxn, k.as_bytes()).unwrap());
        }
        wtxn.commit().unwrap();
        assert_clean(dir.path(), PS);
    }
    // Cross-check final content by full iteration on a reopen.
    drop(env);
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    let n = db.iter(&rtxn).count();
    assert_eq!(db.len(&rtxn).unwrap(), n as u64);
}

/// D7 risk 2 guard: exact page-full boundary at the split, at both page-size
/// extremes. Fill a leaf to exactly zero free bytes, then insert once more
/// and assert the split leaves a valid, fully-readable tree.
fn split_boundary_case(psize: u32) {
    let dir = TempDir::new();
    let env = open(dir.path(), 32 << 20, psize);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();

    let body = psize as usize - 32;
    // Fixed geometry: key 8 bytes, value 102 bytes → cell even(8+8+102)=118,
    // slot cost 120.
    let unit = 120usize;
    let full_count = body / unit;
    let leftover = body - full_count * unit;
    for i in 0..full_count {
        let k = format!("k{i:07}");
        db.put(&mut wtxn, k.as_bytes(), &[0xEEu8; 102]).unwrap();
    }
    // Top up to EXACTLY zero free bytes with one tailored entry, if the
    // leftover can host one (needs >= 2 (ptr) + 8 (hdr) + 8 (key) rounded).
    if leftover >= 20 {
        let vlen = leftover - 2 - 8 - 8; // even cell: leftover-2 total cell len
        let k = format!("k{full_count:07}");
        db.put(&mut wtxn, k.as_bytes(), &vec![0xEEu8; vlen])
            .unwrap();
    }
    let rec_before = *zerodb::TxnRead::main_record(&wtxn);
    assert_eq!(rec_before.leaf_pages, 1, "setup must fill exactly one leaf");
    // The next insert cannot fit: the leaf must split.
    db.put(&mut wtxn, b"k9999999", &[0xEEu8; 102]).unwrap();
    let rec_after = *zerodb::TxnRead::main_record(&wtxn);
    assert_eq!(rec_after.leaf_pages, 2, "exact-fit insert must split once");
    assert_eq!(rec_after.branch_pages, 1);
    assert_eq!(rec_after.depth, 2);
    let expect_entries = rec_before.entries + 1;
    assert_eq!(rec_after.entries, expect_entries);
    wtxn.commit().unwrap();
    assert_clean(dir.path(), psize);
    // Every entry readable after the split.
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.len(&rtxn).unwrap(), expect_entries);
    let all: Vec<_> = db.iter(&rtxn).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all.len() as u64, expect_entries);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn split_exact_fit_boundary_4k() {
    split_boundary_case(4096);
}

#[test]
fn split_exact_fit_boundary_64k() {
    split_boundary_case(65536);
}

#[test]
fn append_split_packs_pages_full() {
    // §6.3/§6.4 append policy: sequential APPEND loads leave leaves ~full, so
    // the leaf count is near the packing optimum.
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    let n = 2000u32;
    // 8-byte key + 50-byte value → cell even(66)=66, +2 = 68; 4064/68 = 59/leaf.
    for i in 0..n {
        let k = format!("{i:08}");
        db.put_with_flags(
            &mut wtxn,
            zerodb::PutFlags::APPEND,
            k.as_bytes(),
            &[0x7Fu8; 50],
        )
        .unwrap();
    }
    let rec = *zerodb::TxnRead::main_record(&wtxn);
    let per_leaf_optimum = (4096usize - 32) / 68;
    let min_leaves = (n as usize).div_ceil(per_leaf_optimum) as u64;
    assert!(
        rec.leaf_pages <= min_leaves + 1,
        "append must pack pages ~100% full: {} leaves for optimum {min_leaves}",
        rec.leaf_pages
    );
    wtxn.commit().unwrap();
    assert_clean(dir.path(), PS);
}

#[test]
fn put_reserved_through_commit() {
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    let inline_val = vec![0x21u8; 500];
    let big_val = vec![0x42u8; 20_000];
    db.put_reserved(&mut wtxn, b"inline", inline_val.len(), |buf| {
        buf.copy_from_slice(&inline_val);
    })
    .unwrap();
    db.put_reserved(&mut wtxn, b"big", big_val.len(), |buf| {
        buf.copy_from_slice(&big_val);
    })
    .unwrap();
    wtxn.commit().unwrap();
    assert_clean(dir.path(), PS);
    let rtxn = env.read_txn().unwrap();
    assert_eq!(
        db.get(&rtxn, b"inline").unwrap(),
        Some(inline_val.as_slice())
    );
    assert_eq!(db.get(&rtxn, b"big").unwrap(), Some(big_val.as_slice()));
}

#[test]
fn iter_mut_ops_through_commit() {
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..50u32 {
        db.put(&mut wtxn, format!("k{i:02}").as_bytes(), b"orig")
            .unwrap();
    }
    {
        let mut cur = db.rw_cursor(&mut wtxn);
        // Rewrite every third entry, delete every seventh (iter_mut pass).
        let mut i = 0u32;
        while let Some((_k, _v)) = cur.move_next().unwrap() {
            if i % 7 == 0 {
                cur.del_current().unwrap();
            } else if i % 3 == 0 {
                cur.put_current(format!("rewritten-{i}").as_bytes())
                    .unwrap();
            }
            i += 1;
        }
    }
    wtxn.commit().unwrap();
    assert_clean(dir.path(), PS);
    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k00").unwrap(), None); // i=0 deleted
    assert_eq!(
        db.get(&rtxn, b"k03").unwrap(),
        Some(b"rewritten-3".as_slice())
    );
    assert_eq!(db.get(&rtxn, b"k01").unwrap(), Some(b"orig".as_slice()));
}

#[test]
fn delete_heavy_shrinks_to_empty_and_recovers() {
    let dir = TempDir::new();
    let env = open(dir.path(), MAP, PS);
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..1000u32 {
            db.put(&mut wtxn, format!("k{i:05}").as_bytes(), &[1u8; 100])
                .unwrap();
        }
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..1000u32 {
            assert!(db.delete(&mut wtxn, format!("k{i:05}").as_bytes()).unwrap());
        }
        assert_eq!(db.len(&wtxn).unwrap(), 0);
        wtxn.commit().unwrap();
    }
    assert_clean(dir.path(), PS);
    let rtxn = env.read_txn().unwrap();
    assert!(db.is_empty(&rtxn).unwrap());
    assert_eq!(db.first(&rtxn).unwrap(), None);
}
