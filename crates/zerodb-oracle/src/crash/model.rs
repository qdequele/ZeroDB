//! The crash harness's data-model oracle (ADR-0008 D2): a pure shadow model
//! replaying the seeded op stream, executed either **against** a live env
//! (mechanisms A and B-child — every op's outcome is cross-checked against
//! the model's prediction, so model↔engine drift fails loudly *before* any
//! crash is injected, ADR-0008 D6.3) or **standalone** (mechanism B parent
//! replay, reconstructing the expected committed state for any recovered
//! txnid).
//!
//! Validity gating goes through [`crate::driver::classify`] — the same single
//! authority the differential harness uses — with `cleared_in_txn = false`:
//! the FORK-1 `KnownForkBug` guard protects the vendored C fork, which is not
//! in this loop (crash cycles are zerodb-only).
//!
//! Executed surface (ADR-0008 D2 / REC-21): `BeginRw`, `Commit`, `Abort`,
//! `CreateDb`, `ClearDb`, `DropDb`, `Put`, `PutFlagged`, `PutReserved`,
//! `Del`, plus `Get`/`Len` as continuous in-txn cross-checks. Read-only
//! positioning/iteration ops are skipped (the differential fuzz owns read
//! semantics); keys outside `1..=511` bytes are skipped (key-size parity is
//! SPEC 03 §2.1 territory, not crash territory).

use std::collections::BTreeMap;
use std::sync::Arc;

use zerodb::{Database, Env, PutFlags, RwTxn};
use zerodb_core::error::{Error, MdbError};

use crate::driver::{classify, TxnState};
use crate::{Op, PutFlag};

/// One database's expected contents.
pub type DbMap = BTreeMap<Vec<u8>, Arc<[u8]>>;
/// The expected committed state: catalog name → contents. Only named DBs —
/// the workload's `Unnamed` resolves to milli's named `"main"` (see
/// [`crate::DbName`]), so the true root stays a pure catalog.
pub type World = BTreeMap<String, DbMap>;

/// What one [`Exec::step`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum StepOutcome {
    /// Executed against model (and env, if attached).
    Executed,
    /// Structurally skipped (unexecuted surface, `classify` skip, or key-size
    /// filter).
    Skipped,
    /// A `Commit` op completed; the payload is the committed txnid.
    Committed(u64),
}

/// Why a cycle cannot continue.
#[derive(Debug)]
pub enum ExecErr {
    /// Model and engine disagreed — a pre-crash cross-check failure. Always a
    /// violation (harness-model or engine bug; never attributable to crash
    /// recovery, which has not happened yet).
    Drift(String),
    /// Environmental pressure the model deliberately does not predict
    /// (`MapFull`-class errors). The cycle is abandoned and counted.
    Abandon(String),
}

struct View {
    world: World,
    /// Whether this write txn dirtied any page (mirrors the engine's
    /// unchanged-commit skip: `commit()` bumps the txnid iff the dirty set is
    /// non-empty — SPEC 04 §9 / M1.4). Set on: successful put (any, even
    /// same-value — COW dirties), `del` that removed, any `clear` (the record
    /// dirties even when the db is already empty — probed), create of a new
    /// db, drop of an existing db.
    effective: bool,
}

/// The workload executor: shadow model + optional live env.
pub struct Exec<'e> {
    env: Option<&'e Env>,
    wtxn: Option<RwTxn<'e>>,
    /// Committed shadow state.
    pub world: World,
    view: Option<View>,
    /// Predicted committed txnid (cross-checked against `env.txnid()` at
    /// every commit when an env is attached).
    pub txnid: u64,
    /// Committed state per txnid — the verification oracle's state table.
    /// Seeded with `{0: empty}` (recovery to the pre-first-commit state is
    /// legal, REC-6).
    pub states: BTreeMap<u64, World>,
    /// Txnid after each executed `Commit` op, in order (the mechanism-B
    /// sidecar mirror; unchanged commits repeat the previous value).
    pub commit_log: Vec<u64>,
}

impl<'e> Exec<'e> {
    /// New executor over a fresh env (`txnid` must be 0) or standalone.
    #[must_use]
    pub fn new(env: Option<&'e Env>) -> Exec<'e> {
        debug_assert!(
            env.map_or(true, |e| e.txnid() == 0),
            "Exec expects a fresh env"
        );
        let mut states = BTreeMap::new();
        states.insert(0, World::new());
        Exec {
            env,
            wtxn: None,
            world: World::new(),
            view: None,
            txnid: 0,
            states,
            commit_log: Vec::new(),
        }
    }

    /// Abort any open write txn (end-of-workload cleanup).
    pub fn finish(&mut self) {
        if let Some(t) = self.wtxn.take() {
            t.abort();
        }
        self.view = None;
    }

    /// Last acknowledged-committed txnid (0 if none).
    #[must_use]
    pub fn acked(&self) -> u64 {
        self.commit_log.last().copied().unwrap_or(0)
    }

    fn active_world(&self) -> &World {
        self.view.as_ref().map_or(&self.world, |v| &v.world)
    }

    /// Resolve a `u8` op index to a db name over the current world's sorted
    /// names — always an existing db, identically in live and replay runs.
    fn resolve_db(&self, idx: u8) -> String {
        let w = self.active_world();
        debug_assert!(!w.is_empty(), "classify guarantees a db exists");
        let names: Vec<&String> = w.keys().collect();
        names[idx as usize % names.len()].clone()
    }

    fn open_handle(&self, name: &str) -> Result<Database, ExecErr> {
        let env = self.env.expect("open_handle only with an env");
        let wtxn = self.wtxn.as_ref().expect("open_handle inside a write txn");
        match env.open_database(wtxn, Some(name.as_bytes())) {
            Ok(Some(db)) => Ok(db),
            Ok(None) => Err(ExecErr::Drift(format!(
                "model says db {name:?} exists, engine catalog says absent"
            ))),
            Err(e) => Err(ExecErr::Drift(format!("open_database({name:?}): {e}"))),
        }
    }

    /// Execute one op. See the module docs for the executed surface.
    ///
    /// # Errors
    ///
    /// [`ExecErr::Drift`] on model/engine disagreement, [`ExecErr::Abandon`]
    /// on unmodeled environmental pressure.
    #[allow(clippy::too_many_lines)] // one match arm per executed op, linear
    pub fn step(&mut self, op: &Op) -> Result<StepOutcome, ExecErr> {
        let executed = matches!(
            op,
            Op::BeginRw
                | Op::Commit
                | Op::Abort
                | Op::CreateDb { .. }
                | Op::ClearDb { .. }
                | Op::DropDb { .. }
                | Op::Put { .. }
                | Op::PutFlagged { .. }
                | Op::PutReserved { .. }
                | Op::Del { .. }
                | Op::Get { .. }
                | Op::Len { .. }
        );
        if !executed {
            return Ok(StepOutcome::Skipped);
        }
        let txn_state = if self.view.is_some() {
            TxnState::Rw
        } else {
            TxnState::None
        };
        // `cleared_in_txn = false`: see module docs (FORK-1 is fork-only).
        if classify(op, txn_state, self.active_world().is_empty(), false).is_some() {
            return Ok(StepOutcome::Skipped);
        }
        // Key-size filter (module docs): crash coverage is orthogonal to the
        // SPEC 03 §2.1 key-size parity rules.
        if let Op::Put { key, .. }
        | Op::PutFlagged { key, .. }
        | Op::PutReserved { key, .. }
        | Op::Del { key, .. }
        | Op::Get { key, .. } = op
        {
            if key.0.is_empty() || key.0.len() > 511 {
                return Ok(StepOutcome::Skipped);
            }
        }

        match op {
            Op::BeginRw => {
                if let Some(env) = self.env {
                    match env.write_txn() {
                        Ok(t) => self.wtxn = Some(t),
                        Err(e) => return Err(ExecErr::Abandon(format!("write_txn: {e}"))),
                    }
                }
                self.view = Some(View {
                    world: self.world.clone(),
                    effective: false,
                });
                Ok(StepOutcome::Executed)
            }
            Op::Commit => {
                let view = self.view.take().expect("classify guaranteed a txn");
                if let Some(t) = self.wtxn.take() {
                    if let Err(e) = t.commit() {
                        return Err(ExecErr::Abandon(format!("commit: {e}")));
                    }
                }
                if view.effective {
                    self.txnid += 1;
                }
                self.world = view.world;
                if let Some(env) = self.env {
                    if env.txnid() != self.txnid {
                        return Err(ExecErr::Drift(format!(
                            "txnid drift after commit: engine {} vs model {}",
                            env.txnid(),
                            self.txnid
                        )));
                    }
                }
                self.states.insert(self.txnid, self.world.clone());
                self.commit_log.push(self.txnid);
                Ok(StepOutcome::Committed(self.txnid))
            }
            Op::Abort => {
                if let Some(t) = self.wtxn.take() {
                    t.abort();
                }
                self.view = None;
                Ok(StepOutcome::Executed)
            }
            Op::CreateDb { name } => {
                let nm = name.resolve().expect("DbName always resolves");
                let is_new = !self.view.as_ref().unwrap().world.contains_key(&nm);
                if let Some(env) = self.env {
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    if let Err(e) = env.create_database(wtxn, Some(nm.as_bytes())) {
                        return Err(ExecErr::Abandon(format!("create_database({nm}): {e}")));
                    }
                }
                let view = self.view.as_mut().unwrap();
                if is_new {
                    view.world.insert(nm, DbMap::new());
                    view.effective = true;
                }
                Ok(StepOutcome::Executed)
            }
            Op::ClearDb { db } => {
                let nm = self.resolve_db(*db);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    if let Err(e) = h.clear(wtxn) {
                        return Err(ExecErr::Abandon(format!("clear({nm}): {e}")));
                    }
                }
                let view = self.view.as_mut().unwrap();
                view.world.get_mut(&nm).expect("resolved over view").clear();
                // Effective even on an already-empty db: `clear` dirties the
                // working record unconditionally (probed 2026-07-16 — the
                // txnid bumps; matches LMDB's always-dirty `mdb_drop(dbi,0)`),
                // so the catalog write-back COWs a main-tree leaf at C1a.
                view.effective = true;
                Ok(StepOutcome::Executed)
            }
            Op::DropDb { db } => {
                let nm = self.resolve_db(*db);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    if let Err(e) = h.drop_db(wtxn) {
                        return Err(ExecErr::Abandon(format!("drop_db({nm}): {e}")));
                    }
                }
                let view = self.view.as_mut().unwrap();
                view.world.remove(&nm);
                // Removing the catalog entry dirties the main tree even for an
                // empty db (the eager F_SUBDATA record exists) — effective.
                view.effective = true;
                Ok(StepOutcome::Executed)
            }
            Op::Put { db, key, val } | Op::PutReserved { db, key, val } => {
                let nm = self.resolve_db(*db);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    let res = if matches!(op, Op::PutReserved { .. }) {
                        h.put_reserved(wtxn, &key.0, val.0.len(), |buf| {
                            buf.copy_from_slice(&val.0);
                        })
                    } else {
                        h.put(wtxn, &key.0, &val.0)
                    };
                    if let Err(e) = res {
                        return Err(ExecErr::Abandon(format!("put({nm}): {e}")));
                    }
                }
                let view = self.view.as_mut().unwrap();
                view.world
                    .get_mut(&nm)
                    .expect("resolved over view")
                    .insert(key.0.clone(), Arc::from(val.0.as_slice()));
                // Even an identical-value overwrite COWs the leaf — effective.
                view.effective = true;
                Ok(StepOutcome::Executed)
            }
            Op::PutFlagged { db, key, val, flag } => {
                let nm = self.resolve_db(*db);
                let pred_ok = {
                    let dbm = self.active_world().get(&nm).expect("resolved over view");
                    match flag {
                        // SPEC 01 §S1: APPEND requires key strictly above the
                        // current last key (empty db always accepts).
                        PutFlag::Append => dbm.last_key_value().map_or(true, |(k, _)| key.0 > *k),
                        // SPEC 01 §S2: NO_OVERWRITE requires absence.
                        PutFlag::NoOverwrite => !dbm.contains_key(&key.0),
                    }
                };
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    let flags = match flag {
                        PutFlag::Append => PutFlags::APPEND,
                        PutFlag::NoOverwrite => PutFlags::NO_OVERWRITE,
                    };
                    match (pred_ok, h.put_with_flags(wtxn, flags, &key.0, &val.0)) {
                        (true, Ok(())) => {}
                        (false, Err(Error::Mdb(MdbError::KeyExist))) => {}
                        (true, Err(Error::Mdb(MdbError::KeyExist))) => {
                            return Err(ExecErr::Drift(format!(
                                "put_with_flags({flag:?}) KeyExist but model predicted Ok"
                            )));
                        }
                        (false, Ok(())) => {
                            return Err(ExecErr::Drift(format!(
                                "put_with_flags({flag:?}) Ok but model predicted KeyExist"
                            )));
                        }
                        (_, Err(e)) => {
                            return Err(ExecErr::Abandon(format!("put_with_flags: {e}")));
                        }
                    }
                }
                if pred_ok {
                    let view = self.view.as_mut().unwrap();
                    view.world
                        .get_mut(&nm)
                        .expect("resolved over view")
                        .insert(key.0.clone(), Arc::from(val.0.as_slice()));
                    view.effective = true;
                }
                Ok(StepOutcome::Executed)
            }
            Op::Del { db, key } => {
                let nm = self.resolve_db(*db);
                let pred = self
                    .active_world()
                    .get(&nm)
                    .expect("resolved over view")
                    .contains_key(&key.0);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_mut().expect("txn open");
                    match h.delete(wtxn, &key.0) {
                        Ok(b) if b == pred => {}
                        Ok(b) => {
                            return Err(ExecErr::Drift(format!(
                                "delete returned {b}, model predicted {pred}"
                            )));
                        }
                        Err(e) => return Err(ExecErr::Abandon(format!("delete: {e}"))),
                    }
                }
                if pred {
                    let view = self.view.as_mut().unwrap();
                    view.world
                        .get_mut(&nm)
                        .expect("resolved over view")
                        .remove(&key.0);
                    view.effective = true;
                }
                Ok(StepOutcome::Executed)
            }
            Op::Get { db, key } => {
                let nm = self.resolve_db(*db);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_ref().expect("txn open");
                    let pred = self
                        .active_world()
                        .get(&nm)
                        .expect("resolved over view")
                        .get(&key.0)
                        .cloned();
                    match h.get(wtxn, &key.0) {
                        Ok(got) => {
                            let same = match (&got, &pred) {
                                (None, None) => true,
                                (Some(g), Some(p)) => *g == &p[..],
                                _ => false,
                            };
                            if !same {
                                return Err(ExecErr::Drift(format!(
                                    "in-txn get mismatch on key {:02x?}",
                                    &key.0[..key.0.len().min(8)]
                                )));
                            }
                        }
                        Err(e) => return Err(ExecErr::Abandon(format!("get: {e}"))),
                    }
                }
                Ok(StepOutcome::Executed)
            }
            Op::Len { db } => {
                let nm = self.resolve_db(*db);
                if self.env.is_some() {
                    let h = self.open_handle(&nm)?;
                    let wtxn = self.wtxn.as_ref().expect("txn open");
                    let pred = self.active_world().get(&nm).expect("view").len() as u64;
                    match h.len(wtxn) {
                        Ok(l) if l == pred => {}
                        Ok(l) => {
                            return Err(ExecErr::Drift(format!(
                                "in-txn len mismatch: engine {l} vs model {pred} for {nm}"
                            )));
                        }
                        Err(e) => return Err(ExecErr::Abandon(format!("len: {e}"))),
                    }
                }
                Ok(StepOutcome::Executed)
            }
            _ => unreachable!("filtered by the executed-surface match"),
        }
    }
}
