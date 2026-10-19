//! Property test for the free-page run search across freed-page records
//! (SPEC 05 GC-21): random commits of multi-page values that are written,
//! resized and deleted, large enough that freed pages land in several
//! records of the free-page database, so later runs are drawn across them.
//!
//! After every commit:
//! - the committed image passes `zerodb::check::check_image`, whose partition
//!   walk fails on a page that is both reachable and listed free (handed out
//!   twice, or drawn without leaving its record) and on a page that is
//!   neither (leaked);
//! - the database holds exactly the model's contents;
//! - a reader opened at a chosen earlier commit still reads that commit's
//!   contents, so no page it can reach was reused (the reuse gate holds for
//!   every record a run is drawn from).

use std::collections::BTreeMap;

use proptest::prelude::*;
use zerodb::{check, Env, EnvOpenOptions};
use zerodb_oracle::tempdir::TempDir;

const PS: u32 = 4096;

/// One put (`Some(pages)`, `0` = a small inline value) or delete (`None`) of
/// key `k`.
type Op = (u8, Option<u16>);

fn value(fill: u8, pages: u16) -> Vec<u8> {
    if pages == 0 {
        return vec![fill; 40];
    }
    let ps = PS as usize;
    vec![fill; (usize::from(pages) - 1) * ps + ps / 2]
}

fn open(dir: &std::path::Path) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(256 << 20);
    o.page_size(PS);
    o.open(dir).expect("open env")
}

/// A database's contents, key to value.
type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

fn contents(env: &Env, rtxn: &zerodb::RoTxn<'_>) -> Contents {
    let db = env.main_database();
    db.iter(rtxn)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

fn op() -> impl Strategy<Value = Op> {
    (
        0u8..32,
        prop_oneof![
            1 => Just(None),
            1 => Just(Some(0u16)),
            4 => (2u16..64).prop_map(Some),
        ],
    )
}

/// One random case: `commits` write txns, a reader pinned after commit
/// `pin_at`, and a spacer commit after every `spacer_every`-th one.
fn run_case(commits: &[Vec<Op>], pin_at: usize, spacer_every: usize) -> Result<(), TestCaseError> {
    let dir = TempDir::new().unwrap();
    let env = open(dir.path());
    let db = env.main_database();
    let mut model = Contents::new();
    let mut pinned: Option<(zerodb::RoTxn<'_>, Contents)> = None;
    for (i, ops) in commits.iter().enumerate() {
        let mut w = env.write_txn().unwrap();
        for (n, &(k, put)) in ops.iter().enumerate() {
            let key = vec![b'k', k];
            match put {
                Some(pages) => {
                    let v = value((i * 31 + n) as u8, pages);
                    db.put(&mut w, &key, &v).unwrap();
                    model.insert(key, v);
                }
                None => {
                    let had = db.delete(&mut w, &key).unwrap();
                    prop_assert_eq!(had, model.remove(&key).is_some());
                }
            }
        }
        w.commit().unwrap();
        // Spacer commits let freed records pass the reuse gate.
        if i % spacer_every == 0 {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, b"spacer", &[i as u8]).unwrap();
            w.commit().unwrap();
            model.insert(b"spacer".to_vec(), vec![i as u8]);
        }
        let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
        let v = check::check_image(&bytes, PS);
        prop_assert!(v.is_empty(), "commit {}: {:#?}", i, v);
        let r = env.read_txn().unwrap();
        prop_assert_eq!(&contents(&env, &r), &model, "commit {}", i);
        drop(r);
        if let Some((r, want)) = &pinned {
            prop_assert_eq!(&contents(&env, r), want, "pinned reader, commit {}", i);
        }
        if i == pin_at {
            let r = env.read_txn().unwrap();
            pinned = Some((r, model.clone()));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn runs_drawn_across_records_never_leak_double_or_cross_a_reader(
        commits in proptest::collection::vec(proptest::collection::vec(op(), 8..40), 4..16),
        pin_at in 0usize..16,
        spacer_every in 1usize..4,
    ) {
        run_case(&commits, pin_at, spacer_every)?;
    }
}

/// The case the property test shrank to on Linux CI: commit 4 (whose
/// allocations drew runs across many records) failed to reach the
/// `freelist_save` fixed point (GC-13). Kept as a fixed regression case.
#[test]
fn freelist_save_converges_after_cross_record_draws_regression() {
    let commits: Vec<Vec<Op>> = vec![
        vec![
            (29, Some(48)),
            (17, Some(55)),
            (14, Some(19)),
            (3, Some(43)),
            (1, Some(37)),
            (2, Some(41)),
            (0, Some(47)),
            (21, Some(26)),
        ],
        vec![
            (30, None),
            (0, Some(31)),
            (29, None),
            (0, None),
            (0, None),
            (11, Some(2)),
            (0, None),
            (18, None),
        ],
        vec![
            (3, None),
            (5, Some(17)),
            (23, Some(13)),
            (3, None),
            (19, Some(18)),
            (7, Some(52)),
            (1, Some(8)),
            (15, Some(47)),
            (7, None),
        ],
        vec![
            (9, Some(9)),
            (10, Some(29)),
            (20, Some(45)),
            (5, Some(18)),
            (27, Some(53)),
            (22, Some(63)),
            (29, Some(31)),
            (26, Some(40)),
            (4, None),
            (12, Some(0)),
            (0, Some(0)),
            (21, None),
            (16, Some(17)),
            (0, None),
            (29, Some(21)),
            (19, Some(8)),
            (29, Some(32)),
            (22, Some(28)),
            (29, Some(40)),
            (8, Some(33)),
            (19, None),
            (17, None),
            (24, Some(33)),
            (8, Some(49)),
            (17, None),
            (4, Some(33)),
            (16, Some(30)),
            (19, Some(0)),
            (15, Some(29)),
            (22, Some(2)),
            (20, Some(56)),
            (14, Some(38)),
        ],
        vec![
            (24, Some(0)),
            (23, None),
            (6, None),
            (26, Some(62)),
            (8, Some(32)),
            (21, Some(45)),
            (14, Some(51)),
            (16, Some(54)),
            (2, Some(0)),
            (27, Some(46)),
            (8, Some(10)),
        ],
    ];
    run_case(&commits, 4, 3).unwrap();
}
