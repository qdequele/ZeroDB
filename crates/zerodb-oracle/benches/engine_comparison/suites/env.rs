//! Suite `env` — the per-lifetime and per-transaction floors.
//!
//! Everything here is cost paid *before* a single key is touched. milli opens a
//! read txn per search and a write txn per indexing batch, so a txn-begin
//! regression multiplies across a workload without showing up in any `get`
//! rung.
//!
//! Ladder:
//! * `open/create` → `open/reopen` separates writing a fresh image from mapping
//!   and validating an existing one.
//! * `txn/ro_begin_abort` → `txn/rw_empty_commit` separates the reader-table
//!   slot protocol from the commit path with zero dirty pages.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput};
use zerodb_oracle::tempdir::TempDir;

use crate::data::{ascending_keys, MAP, N, N_PROBE, VAL};
use crate::harness::{Backend, Fixture, Group};
use crate::{pair_shape, Cfg};

/// Envs opened per timed iteration — a single open is too quick to time.
const N_OPEN: usize = 20;
/// Empty write txns per timed iteration.
const N_EMPTY_COMMIT: usize = 1_000;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    open_create(c, cfg);
    reopen(c, cfg);
    ro_txn(c, cfg);
    rw_empty_commit(c, cfg);
}

/// Create a brand-new env and one named DB, `N_OPEN` times: file creation, the
/// initial meta pages, and the catalog write.
fn open_create(c: &mut Criterion, cfg: &Cfg) {
    let mut g = c.benchmark_group("env/open/create");
    g.throughput(Throughput::Elements(N_OPEN as u64));
    crate::heavy(&mut g);
    pair_shape!(case_open_create, &mut g, cfg.page);
    g.finish();
}

fn case_open_create<B: Backend>(g: &mut Group<'_>, page: u32) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                // One directory per env, all made here so the timed region is
                // engine work only. Reusing one path would make 19 of the 20
                // "creations" reopens, which is the next rung's job.
                let t = TempDir::new().expect("tempdir");
                let dirs: Vec<_> = (0..N_OPEN)
                    .map(|i| {
                        let d = t.path().join(format!("env{i}"));
                        std::fs::create_dir_all(&d).expect("mkdir");
                        d
                    })
                    .collect();
                (t, dirs)
            },
            |(t, dirs)| {
                B::open_create_close(&dirs, MAP, page);
                drop(t);
            },
            BatchSize::PerIteration,
        )
    });
}

/// Reopen an env already holding `N` entries, `N_OPEN` times. The delta against
/// `open/create` is what mapping + geometry validation costs on an existing
/// image (zerodb validates rather more of it than LMDB does — D-017).
fn reopen(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];
    let mut g = c.benchmark_group("env/open/reopen");
    g.throughput(Throughput::Elements(N_OPEN as u64));
    crate::heavy(&mut g);
    pair_shape!(case_reopen, &mut g, cfg.page, &keys, &val);
    g.finish();
}

fn case_reopen<B: Backend>(g: &mut Group<'_>, page: u32, keys: &[Vec<u8>], val: &[u8]) {
    // The populating env must be CLOSED before the rung runs: an environment
    // may only be open once per process, and this rung times `open`.
    let dir = Fixture::<B>::single(page, Some("bench"), keys, val).into_dir();
    g.bench_function(B::NAME, |b| {
        b.iter(|| B::reopen(dir.path(), MAP, page, N_OPEN))
    });
}

/// Begin + drop a read txn, repeatedly: the reader-table slot pin/unpin
/// protocol and nothing else. zerodb's is a lock-free atomic slot (ADR-0006);
/// LMDB's is the mmap'd lock table.
fn ro_txn(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];
    let mut g = c.benchmark_group("env/txn/ro_begin_abort");
    g.throughput(Throughput::Elements(N_PROBE as u64));
    pair_shape!(case_ro_txn, &mut g, cfg.page, &keys, &val);
    g.finish();
}

fn case_ro_txn<B: Backend>(g: &mut Group<'_>, page: u32, keys: &[Vec<u8>], val: &[u8]) {
    let f = Fixture::<B>::single(page, Some("bench"), keys, val);
    g.bench_function(B::NAME, |b| {
        b.iter(|| black_box(B::rotxn_churn(&f.env, N_PROBE)))
    });
}

/// Begin + commit an EMPTY write txn, repeatedly, fsync off. Zero dirty pages,
/// so this is the writer-lock handoff plus the meta-page update — the floor
/// every `commit/*` rung sits on top of.
fn rw_empty_commit(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];
    let mut g = c.benchmark_group("env/txn/rw_empty_commit");
    g.throughput(Throughput::Elements(N_EMPTY_COMMIT as u64));
    pair_shape!(case_empty_commit, &mut g, cfg.page, &keys, &val);
    g.finish();
}

fn case_empty_commit<B: Backend>(g: &mut Group<'_>, page: u32, keys: &[Vec<u8>], val: &[u8]) {
    let f = Fixture::<B>::single(page, Some("bench"), keys, val);
    g.bench_function(B::NAME, |b| {
        b.iter(|| B::empty_commit_churn(&f.env, N_EMPTY_COMMIT))
    });
}
