//! Crash-consistency harness (M1.11, ADR-0008; SPEC 06 §5 REC-17..21).
//!
//! Two mechanisms over one shared seeded workload and one shared verifier:
//!
//! - [`image`] — **mechanism A** (REC-19/REC-20): the workload runs on a
//!   [`zerodb_io::fault::FaultBacking`]; a simulated power cut freezes the
//!   `(durable, pending)` pair mid-pipeline (via a capture [`CommitHook`]) or
//!   at an op boundary; N fault-plan images are materialized, reopened, and
//!   verified. This is the mechanism that tears/reorders/drops writes.
//! - [`sigkill`] — **mechanism B** (REC-17): a child process runs the same
//!   workload on a real file env and dies (deterministic `abort()` at a
//!   seeded `(commit, hook)`, or an asynchronous SIGKILL from the parent);
//!   the parent reopens and verifies. The OS page cache survives, so this
//!   validates control-flow ordering on the real mmap/pwrite path.
//!
//! One **cycle** = one recovered-and-verified crash state (ratified ADR-0008
//! OQ1): each materialized image variant and each SIGKILL recovery counts as
//! one toward the ≥10k acceptance bar.
//!
//! [`CommitHook`]: zerodb::CommitHook

pub mod image;
pub mod model;
pub mod sigkill;
pub mod verify;
pub mod workload;

pub use image::{run_image_cut, ImageOpts};
pub use model::{Exec, ExecErr, StepOutcome, World};
pub use sigkill::{child_run, run_sigkill_cycle, SigkillOpts};
pub use workload::{gen_spec, Mode, Spec};

/// The outcome of one cut (mechanism A) or one child crash (mechanism B).
#[derive(Debug, Default)]
pub struct CutReport {
    /// Recovered-and-verified crash states (the ADR-0008 OQ1 cycle unit).
    pub verified: u64,
    /// The cycle could not run to a verifiable cut (env pressure such as
    /// `MapFull`, spawn failure, …) — counted and reported, never silently
    /// retried; a spike would flag a harness/model gap.
    pub abandoned: bool,
    /// First verification failure, with enough context to reproduce.
    pub violation: Option<String>,
    /// Durability mode of the cycle (for reporting).
    pub mode: Option<Mode>,
    /// Adversarial-model characterization (ADR-0008 D4: logged, not gated):
    /// probes attempted / opened OK / designed-`Invalid` / walk-clean count.
    pub adv_probes: u64,
    /// Adversarial probes that opened successfully.
    pub adv_opened: u64,
    /// Adversarial probes that failed with the designed `Invalid`.
    pub adv_invalid: u64,
    /// Adversarial probes whose image also passed the full walk.
    pub adv_walk_clean: u64,
    /// Images that landed in the `NO_META_SYNC` reclaim-clobber window
    /// (REC-10 as amended M1.11): window/taxonomy obligations verified,
    /// walk/data waived. Counted for characterization.
    pub stale_fallback: u64,
}
