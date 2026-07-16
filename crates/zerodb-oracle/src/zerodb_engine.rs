//! The native [`Engine`] backed by the `zerodb` crate.
//!
//! ## Milestone scope (M1.2 — env lifecycle only)
//!
//! zerodb has no read/write path yet (M1.3/M1.4), so this engine implements only
//! the **environment-lifecycle** ops: it opens a real `zerodb` env in a private
//! temp dir and services [`Op::Reopen`] (create / open / reopen, incl.
//! reopen-with-larger-map-size). Every other op is reported as
//! [`Engine::implements`]` == false`, so [`crate::run`] skips it symmetrically on
//! both engines (see the driver). As later milestones land, this engine grows a
//! tracked [`TxnState`](crate::driver::TxnState)/db model and dispatches through
//! the shared [`classify`](crate::driver::classify) exactly like
//! [`LmdbEngine`](crate::LmdbEngine) — the seam is designed so M1.3+ just fills
//! ops in.

use crate::result::OracleError;
use crate::tempdir::TempDir;
use crate::{Engine, Op, OpResult, Skip};

use zerodb::{Env, EnvOpenOptions, Error, MdbError};

/// 1 MiB base map size; growth layered on in 4 KiB units (mirrors
/// [`LmdbEngine`](crate::LmdbEngine)).
const BASE_MAP_SIZE: usize = 1 << 20;
/// Catalog capacity (stored; consumed when named DBs land in M1.6).
const MAX_DBS: u32 = 16;

/// The native engine under differential test.
pub struct ZerodbEngine {
    /// Always `Some` between ops; `None` only briefly during a reopen.
    env: Option<Env>,
    map_size: usize,
    dir: TempDir,
}

impl ZerodbEngine {
    fn open(dir: &std::path::Path, map_size: usize) -> Result<Env, Error> {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(map_size);
        opts.max_dbs(MAX_DBS);
        opts.open(dir)
    }

    fn desired_map_size(&self, kib: u16) -> usize {
        let want = BASE_MAP_SIZE + (kib as usize) * 4096;
        // Monotonic: never shrink below the current size (mirrors LmdbEngine, so
        // a reopen can never fail by cutting below live data).
        want.max(self.map_size)
    }

    fn reopen(&mut self, kib: u16) -> OpResult {
        // Drop the env first so the same-process registry (TXN-51) frees the
        // path before we reopen it.
        self.env = None;
        let new_size = self.desired_map_size(kib);
        match Self::open(self.dir.path(), new_size) {
            Ok(env) => {
                self.env = Some(env);
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                // Restore at the previous (known-good) size so the engine stays
                // usable, then report — same shape as LmdbEngine::reopen.
                if let Ok(env) = Self::open(self.dir.path(), self.map_size) {
                    self.env = Some(env);
                }
                OpResult::Err(to_oracle(e))
            }
        }
    }
}

/// Map a `zerodb::Error` into the oracle's normalized taxonomy, matching
/// `LmdbEngine`'s mapping so error **kinds** compare equal across engines.
fn to_oracle(e: Error) -> OracleError {
    match e {
        Error::Mdb(m) => match m {
            MdbError::NotFound => OracleError::NotFound,
            MdbError::KeyExist => OracleError::KeyExist,
            MdbError::MapFull => OracleError::MapFull,
            MdbError::BadValSize => OracleError::BadValSize,
            MdbError::Invalid => OracleError::Invalid,
            other => OracleError::Other(format!("mdb:{other:?}")),
        },
        Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        Error::EnvAlreadyOpened => OracleError::Other("env-already-opened".into()),
        other => OracleError::Other(format!("{other:?}")),
    }
}

impl Engine for ZerodbEngine {
    fn new() -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let env = ZerodbEngine::open(dir.path(), BASE_MAP_SIZE).expect("open zerodb env");
        ZerodbEngine {
            env: Some(env),
            map_size: BASE_MAP_SIZE,
            dir,
        }
    }

    fn name(&self) -> &'static str {
        "zerodb"
    }

    fn implements(&self, op: &Op) -> bool {
        // M1.2: only the environment-lifecycle op. Everything else is gated out
        // of the differential until its milestone builds it.
        matches!(op, Op::Reopen { .. })
    }

    fn apply(&mut self, op: &Op) -> OpResult {
        match op {
            Op::Reopen { map_size_kib } => self.reopen(*map_size_kib),
            // Defensive: `run` gates unimplemented ops before calling `apply`,
            // so this arm is not reached in a differential run.
            _ => OpResult::Skipped(Skip::NotImplemented),
        }
    }
}
