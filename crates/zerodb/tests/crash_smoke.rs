//! M1.4 commit-pipeline crash smoke test (ADR-0004 D7 risk 3; the full
//! ≥10k-cycle harness is M1.11). For each hook point H0..H4 (SPEC 04 §9), a
//! **child process** runs: commit txn 1 normally, then commit txn 2 with a
//! [`CommitHook`] that aborts the process at the target point. The parent
//! reopens the env and asserts the SPEC 06 REC-6 crash-stage row:
//!
//! | killed at | recovers to |
//! |-----------|-------------|
//! | H0/H1/H2  | txn 1       |
//! | H3        | txn 1 or 2  |
//! | H4        | txn 2       |
//!
//! plus data consistency for the recovered txn and a clean invariant walk.
//!
//! Mechanism note (REC-17): process death does not tear writes — the OS page
//! cache survives — so this validates the pipeline's *control-flow ordering*
//! (steps happen in order, the meta is not written early, the publish does not
//! precede durability). Torn/reordered-write coverage is M1.11's
//! fault-injection backend (REC-19).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use zerodb::{check, CommitHook, Env, EnvOpenOptions, HookPoint};

const PS: u32 = 4096;
const MAP: usize = 4 << 20;

const CHILD_ENV_VAR: &str = "ZDB_CRASH_CHILD_HOOK";
const DIR_ENV_VAR: &str = "ZDB_CRASH_CHILD_DIR";

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

/// A commit hook that aborts the process at the target point.
struct AbortAt(HookPoint);

impl CommitHook for AbortAt {
    fn at(&self, point: HookPoint) {
        if point == self.0 {
            // SIGABRT: die without unwinding or flushing anything further.
            std::process::abort();
        }
    }
}

fn hook_from_name(name: &str) -> HookPoint {
    match name {
        "H0" => HookPoint::H0,
        "H1" => HookPoint::H1,
        "H2" => HookPoint::H2,
        "H3" => HookPoint::H3,
        "H4" => HookPoint::H4,
        other => panic!("unknown hook {other}"),
    }
}

/// The child body. Runs only when spawned by the parent test (env vars set);
/// as a regular test it is a no-op.
#[test]
fn crash_child_runner() {
    let Ok(hook_name) = std::env::var(CHILD_ENV_VAR) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(DIR_ENV_VAR).expect("child dir env var"));
    let env = open(&dir);
    let db = env.main_database();
    // Txn 1: the baseline commit (completes normally).
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..300u32 {
        db.put(&mut wtxn, format!("base{i:04}").as_bytes(), &[1u8; 64])
            .unwrap();
    }
    wtxn.commit().unwrap();
    assert_eq!(env.txnid(), 1);
    // Txn 2: dies at the target hook point.
    env.set_commit_hook(Some(Arc::new(AbortAt(hook_from_name(&hook_name)))));
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..300u32 {
        db.put(&mut wtxn, format!("crash{i:04}").as_bytes(), &[2u8; 64])
            .unwrap();
    }
    db.delete(&mut wtxn, b"base0000").unwrap();
    let _ = wtxn.commit(); // aborts the process at the hook
    unreachable!("the commit hook must have killed the process");
}

#[test]
fn h0_to_h4_kill_matrix_recovers_per_rec6() {
    // Not the child (no env var): drive the matrix.
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        return;
    }
    let exe = std::env::current_exe().expect("test executable path");
    for hook in ["H0", "H1", "H2", "H3", "H4"] {
        let dir =
            std::env::temp_dir().join(format!("zerodb-crash-smoke-{}-{hook}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create crash dir");

        let status = Command::new(&exe)
            .args(["crash_child_runner", "--exact", "--nocapture"])
            .env(CHILD_ENV_VAR, hook)
            .env(DIR_ENV_VAR, &dir)
            .status()
            .expect("spawn child");
        assert!(
            !status.success(),
            "{hook}: the child must die mid-commit, got {status:?}"
        );

        // Recovery: reopen and assert the REC-6 row.
        let env = open(&dir);
        let recovered = env.txnid();
        match hook {
            "H0" | "H1" | "H2" => assert_eq!(
                recovered, 1,
                "{hook}: pre-meta crash must recover to txn 1 (REC-6)"
            ),
            "H3" => assert!(
                recovered == 1 || recovered == 2,
                "{hook}: crash after the un-fsynced meta write recovers to 1 or 2, got {recovered}"
            ),
            "H4" => assert_eq!(
                recovered, 2,
                "{hook}: the meta was durable — txn 2 must never disappear (REC-18.4)"
            ),
            _ => unreachable!(),
        }
        // Data consistency for the recovered snapshot — all-or-nothing per
        // txn (REC-16): never a partial txn 2.
        let db = env.main_database();
        let rtxn = env.read_txn().unwrap();
        let base_first = db.get(&rtxn, b"base0000").unwrap();
        let crash_first = db.get(&rtxn, b"crash0000").unwrap();
        let crash_last = db.get(&rtxn, b"crash0299").unwrap();
        if recovered == 1 {
            assert_eq!(base_first, Some(vec![1u8; 64].as_slice()), "{hook}");
            assert_eq!(crash_first, None, "{hook}: txn 2 must be invisible");
            assert_eq!(crash_last, None, "{hook}");
            assert_eq!(db.len(&rtxn).unwrap(), 300, "{hook}");
        } else {
            assert_eq!(base_first, None, "{hook}: txn 2 deleted base0000");
            assert_eq!(crash_first, Some(vec![2u8; 64].as_slice()), "{hook}");
            assert_eq!(crash_last, Some(vec![2u8; 64].as_slice()), "{hook}");
            assert_eq!(db.len(&rtxn).unwrap(), 599, "{hook}");
        }
        drop(rtxn);
        // The recovered image passes the invariant walk (REC-18.3, M1.4 scope).
        let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).unwrap();
        let v = check::check_image(&bytes, PS);
        assert!(v.is_empty(), "{hook}: invariant violations: {v:#?}");
        drop(env);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
