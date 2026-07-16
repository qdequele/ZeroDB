//! Per-cycle recovery verification (SPEC 06 REC-18; ADR-0008 D2/D4).
//!
//! Full verification (obligations REC-18.1–.5):
//! 1. open succeeds (a single-torn image MUST open, REC-3);
//! 2. the recovered txnid is within the cut's legal `[floor, ceil]` window
//!    (floor/ceil = the txnid selected over durable-only / all-applied
//!    images — self-adapting to the cut point, encoding the REC-6 rows);
//! 3. `check_image` (SPEC 03 §11 + SPEC 05 §9) is clean;
//! 4. the recovered contents equal the model's committed state for exactly
//!    that txnid, bidirectionally (catalog count + per-db full compare);
//! 5. monotonic durability is asserted by the caller via the floor
//!    (`floor ≥ acked` in strict modes — an acked commit can never sit above
//!    the durable floor).
//!
//! Adversarial probing (`NO_SYNC`/`MAP_ASYNC` reorder sub-model, ratified
//! OQ3): assert only *no panic* and the designed error taxonomy; walk/open
//! outcomes are counted for characterization, never gated (REC-11 promises
//! nothing stronger).

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

use zerodb::check::check_image;
use zerodb::Env;
use zerodb_core::env::{open_with_backing, testutil::VecBacking, DurabilityFlags};
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::{MetaPage, MetaValidity};

use super::model::World;

/// The txnid recovery would select on `bytes`: the higher-txnid **valid** slot
/// (REC-2, `PREV_SNAPSHOT` off), or `None` if neither slot validates.
#[must_use]
pub fn image_txnid(bytes: &[u8], psize: u32) -> Option<u64> {
    let ps = psize as usize;
    let mut best: Option<u64> = None;
    for slot in [0usize, 1] {
        let (a, b) = (slot * ps, (slot + 1) * ps);
        if bytes.len() < b {
            continue;
        }
        if let Ok(MetaValidity::Valid(m)) = MetaPage::validate(&bytes[a..b], psize) {
            best = Some(best.map_or(m.txnid, |t| t.max(m.txnid)));
        }
    }
    best
}

/// Pad a materialized image with zeros so every page a *valid* slot's
/// `last_pg` references is in-bounds. A crash image's length can legally sit
/// below a stale slot's high-water only when that slot loses selection
/// (REC-14 guarantees the *selected* meta's pages are durable); padding keeps
/// the walker/read path on the in-bounds path so a genuine REC-14 violation
/// surfaces as a check failure, not an out-of-bounds panic.
pub fn pad_image(img: &mut Vec<u8>, psize: u32) {
    let ps = psize as usize;
    let mut need = 2 * ps;
    for slot in [0usize, 1] {
        let (a, b) = (slot * ps, (slot + 1) * ps);
        if img.len() < b {
            continue;
        }
        if let Ok(MetaValidity::Valid(m)) = MetaPage::validate(&img[a..b], psize) {
            need = need.max(((m.last_pg + 1) as usize) * ps);
        }
    }
    if img.len() < need {
        img.resize(need, 0);
    }
}

/// Bidirectional data comparison: recovered env contents == `world`, exactly.
///
/// # Errors
///
/// A human-readable mismatch description.
pub fn compare_env_world(env: &Env, world: &World) -> Result<(), String> {
    let rtxn = env.read_txn().map_err(|e| format!("read_txn: {e}"))?;
    // The true root is a pure catalog (the workload writes only named DBs):
    // its entry count is the db count — extra/missing DBs surface here.
    let root = env.main_database();
    let catalog_len = root.len(&rtxn).map_err(|e| format!("catalog len: {e}"))?;
    if catalog_len != world.len() as u64 {
        return Err(format!(
            "catalog count mismatch: recovered {catalog_len} dbs, model {}",
            world.len()
        ));
    }
    for (name, entries) in world {
        let db = match env.open_database(&rtxn, Some(name.as_bytes())) {
            Ok(Some(db)) => db,
            Ok(None) => return Err(format!("model db {name:?} missing after recovery")),
            Err(e) => return Err(format!("open_database({name:?}): {e}")),
        };
        let mut got = 0u64;
        let mut model_iter = entries.iter();
        for pair in db.iter(&rtxn) {
            let (k, v) = pair.map_err(|e| format!("iter({name:?}): {e}"))?;
            got += 1;
            match model_iter.next() {
                None => {
                    return Err(format!(
                        "extra recovered key in {name:?}: {:02x?}…",
                        &k[..k.len().min(12)]
                    ))
                }
                Some((mk, mv)) => {
                    if k != mk.as_slice() {
                        return Err(format!(
                            "key order/content mismatch in {name:?}: recovered {:02x?}… vs model {:02x?}…",
                            &k[..k.len().min(12)],
                            &mk[..mk.len().min(12)]
                        ));
                    }
                    if v != &mv[..] {
                        return Err(format!(
                            "value mismatch in {name:?} for key {:02x?}… ({} vs model {} bytes)",
                            &k[..k.len().min(12)],
                            v.len(),
                            mv.len()
                        ));
                    }
                }
            }
        }
        if let Some((mk, _)) = model_iter.next() {
            return Err(format!(
                "missing recovered key in {name:?}: {:02x?}… (recovered {got} entries, model {})",
                &mk[..mk.len().min(12)],
                entries.len()
            ));
        }
    }
    Ok(())
}

/// Context for full REC-18 verification of one image.
pub struct FullChecks<'a> {
    /// Legal window lower bound (durable-only selection).
    pub floor: u64,
    /// Legal window upper bound (all-applied selection).
    pub ceil: u64,
    /// Committed state per txnid (from the model).
    pub states: &'a BTreeMap<u64, World>,
    /// Env page size.
    pub psize: u32,
    /// Env map size (for the reopen).
    pub map_size: u64,
    /// Registry path for the verification env. Reused sequentially per
    /// worker: a failed deregistration on the previous drop surfaces as
    /// `EnvAlreadyOpened` here (the ADR-0008 D6.2 residue tripwire).
    pub verify_path: &'a Path,
    /// **`NO_META_SYNC` reclaim-clobber window** (REC-10 as amended M1.11;
    /// harness find, seed 15797139550980166469): set by the caller only when
    /// the plan persisted at least one in-flight **data** write of a cut that
    /// also has the (single) previous commit's meta pending. If recovery then
    /// falls **below** `ceil` (that meta torn/dropped), the recovered
    /// snapshot's pages may have been legally clobbered — pages freed by txn
    /// `ceil` belong to snapshot `ceil − 1`, and txn `ceil + 1` may reclaim
    /// them (GC-18) with nothing barrier-ordering its writes against the
    /// un-fsynced meta. LMDB shares this window (`MDB_NOMETASYNC`; libmdbx's
    /// steady/weak metas exist precisely to close it — Phase 3 candidate).
    /// Walk/data obligations are waived for exactly those images; the window
    /// and txnid-taxonomy obligations still hold. Never set for
    /// default/`WRITE_MAP` — REC-6's full guarantees stand there.
    pub stale_data_exempt: bool,
}

/// The verdict of a full verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullOutcome {
    /// Every REC-18 obligation held.
    Verified,
    /// The image landed in the `NO_META_SYNC` reclaim-clobber window
    /// (`stale_data_exempt` and recovered < ceil): window/taxonomy
    /// obligations held; walk/data waived per REC-10 as amended. Counted
    /// separately for characterization.
    StaleFallback,
}

/// Full REC-18 verification (see module docs). Panics inside recovery are
/// converted to violations.
///
/// # Errors
///
/// The violated obligation, human-readable.
pub fn verify_image_full(mut img: Vec<u8>, c: &FullChecks<'_>) -> Result<FullOutcome, String> {
    pad_image(&mut img, c.psize);
    let psize = c.psize;
    let map_size = c.map_size;
    let path = c.verify_path.to_path_buf();
    let (floor, ceil) = (c.floor, c.ceil);
    let (states, exempt) = (c.states, c.stale_data_exempt);
    let result = catch_unwind(AssertUnwindSafe(move || -> Result<FullOutcome, String> {
        // Obligation 1: a legally materialized post-commit image must open.
        let env = open_with_backing(
            path,
            Box::new(VecBacking(img)),
            psize,
            map_size,
            false,
            16,
            126,
            DurabilityFlags::default(),
        )
        .map_err(|e| format!("recovery open failed: {e}"))?;
        // Obligation 2: recovered txnid within the cut's legal window, and a
        // real committed txnid (both hold even in the exempt window — the
        // double buffer still yields a valid meta of a committed txn).
        let r = env.txnid();
        if r < floor || r > ceil {
            return Err(format!(
                "recovered txnid {r} outside legal window [{floor}, {ceil}]"
            ));
        }
        let world = states
            .get(&r)
            .ok_or_else(|| format!("recovered txnid {r} has no recorded committed state"))?;
        // REC-10 as amended: falling below the newest issued meta while a
        // younger txn's data persisted exempts walk/data (see FullChecks).
        if exempt && r < ceil {
            return Ok(FullOutcome::StaleFallback);
        }
        // Obligation 3: full walk on the recovered bytes.
        let violations = check_image(env.inner().backing_bytes(), psize);
        if !violations.is_empty() {
            return Err(format!("check_image failed: {violations:?}"));
        }
        // Obligation 4: exact committed state for that txnid.
        compare_env_world(&env, world)?;
        Ok(FullOutcome::Verified)
    }));
    match result {
        Ok(r) => r,
        Err(_) => Err("panic during recovery verification".to_string()),
    }
}

/// Adversarial-probe outcome (characterization only, ADR-0008 D4).
#[derive(Debug, Clone, Copy)]
pub struct AdvOutcome {
    /// Recovered txnid when the image opened.
    pub opened: Option<u64>,
    /// Open failed with the designed `MdbError::Invalid`.
    pub invalid: bool,
    /// The image also passed the full walk (logged, not gated).
    pub walk_clean: bool,
}

/// Probe an adversarial (`NO_SYNC`/`MAP_ASYNC` reorder) image: no panic, and
/// only the designed taxonomy (`Ok` or `Invalid`). Nothing else is asserted —
/// REC-11 makes no stronger promise under reordering (ratified OQ3).
///
/// # Errors
///
/// A panic or a non-designed error — those ARE violations even here.
pub fn probe_image_adversarial(
    mut img: Vec<u8>,
    psize: u32,
    map_size: u64,
    verify_path: &Path,
) -> Result<AdvOutcome, String> {
    pad_image(&mut img, psize);
    let walk_clean = check_image(&img, psize).is_empty();
    let path = verify_path.to_path_buf();
    let result = catch_unwind(AssertUnwindSafe(move || {
        open_with_backing(
            path,
            Box::new(VecBacking(img)),
            psize,
            map_size,
            false,
            16,
            126,
            DurabilityFlags::default(),
        )
        .map(|env| env.txnid())
    }));
    match result {
        Err(_) => Err("panic while opening adversarial image".to_string()),
        Ok(Ok(txnid)) => Ok(AdvOutcome {
            opened: Some(txnid),
            invalid: false,
            walk_clean,
        }),
        Ok(Err(Error::Mdb(MdbError::Invalid))) => Ok(AdvOutcome {
            opened: None,
            invalid: true,
            walk_clean,
        }),
        Ok(Err(other)) => Err(format!(
            "non-designed error opening adversarial image: {other}"
        )),
    }
}
