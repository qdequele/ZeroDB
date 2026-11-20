//! What the Meilisearch LMDB fork does when a named database written under a
//! custom key comparator is reopened, in a new environment session, under a
//! different one, observed through heed 0.22.1. Input for divergence D-014.
//! CONTRIBUTING.md rule 1: observed, not guessed.
//!
//! The script creates the named database `c` with a descending comparator,
//! puts eight one-letter keys and commits, closes the environment, reopens
//! it, and opens `c` with heed's default (ascending) comparator. It records:
//!
//! 1. the result of `open_database` (`some`, `none`, or the error);
//! 2. the keys in iteration order;
//! 3. how many of the eight keys `get` still finds.
//!
//! `OBS_REOPEN_WITH_OTHER_COMPARATOR` pins LMDB's outcome: a diff means the
//! fork changed or the pin was wrong; never edit it to make ZeroDB pass.
//! Observed on macOS (Darwin 27.2.0, aarch64), 2026-11-20. ZeroDB runs the
//! same script and must match, since D-014 records the same hazard. Both
//! outcomes are printed side by side with `--nocapture`.

use zerodb_oracle::tempdir::TempDir;

const KEYS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];

/// The open succeeds, iteration shows the stored (descending) order, and
/// lookups under the wrong comparator miss keys: nothing reports the
/// mismatch.
const OBS_REOPEN_WITH_OTHER_COMPARATOR: &[&str] = &["some", "hgfedcba", "found 1 of 8"];

macro_rules! script {
    ($name:ident, $heed:ident) => {
        fn $name(dir: &std::path::Path) -> Vec<String> {
            use $heed::types::Str;
            use $heed::{Comparator, Database, EnvOpenOptions};

            enum Descending {}
            impl Comparator for Descending {
                fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                    b.cmp(a)
                }
            }

            let open = || {
                let mut opts = EnvOpenOptions::new().read_txn_without_tls();
                opts.map_size(1 << 20);
                opts.max_dbs(4);
                // SAFETY: a private temporary directory, opened by one
                // environment at a time in this single-threaded test; for
                // ZeroDB, only ZeroDB writes its files.
                unsafe { opts.open(dir) }.unwrap()
            };

            {
                let env = open();
                let mut w = env.write_txn().unwrap();
                let db: Database<Str, Str, Descending> = env
                    .database_options()
                    .types::<Str, Str>()
                    .key_comparator::<Descending>()
                    .name("c")
                    .create(&mut w)
                    .unwrap();
                for k in KEYS {
                    db.put(&mut w, k, k).unwrap();
                }
                w.commit().unwrap();
                env.prepare_for_closing().wait();
            }

            let env = open();
            let r = env.read_txn().unwrap();
            let mut out = Vec::new();
            let db: Database<Str, Str> = match env.open_database(&r, Some("c")) {
                Ok(Some(db)) => {
                    out.push("some".to_string());
                    db
                }
                Ok(None) => return vec!["none".into()],
                Err(e) => return vec![format!("{e:?}")],
            };
            out.push(db.iter(&r).unwrap().map(|e| e.unwrap().0).collect());
            let found = KEYS
                .iter()
                .filter(|k| db.get(&r, k).unwrap().is_some())
                .count();
            out.push(format!("found {found} of {}", KEYS.len()));
            out
        }
    };
}

script!(fork, heed);
script!(adapter, heed_zerodb);

#[test]
fn reopen_with_another_comparator_is_not_detected() {
    let l = fork(TempDir::new().unwrap().path());
    let z = adapter(TempDir::new().unwrap().path());
    println!("fork:   {l:?}\nzerodb: {z:?}");
    assert_eq!(l, OBS_REOPEN_WITH_OTHER_COMPARATOR, "LMDB pin");
    assert_eq!(
        z, OBS_REOPEN_WITH_OTHER_COMPARATOR,
        "ZeroDB must match LMDB (D-014)"
    );
}
