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
    }
}
