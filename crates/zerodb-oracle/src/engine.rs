//! The engine-agnostic trait both sides of the differential harness implement.

use crate::{EngineMode, Op, OpResult};

/// A storage engine the oracle can drive.
///
/// The LMDB reference side is [`crate::LmdbEngine`]. The native zerodb side
/// lands in milestone 1.2+ as a second implementor; wiring it in only requires
/// calling [`crate::run::<LmdbEngine, ZerodbEngine>`](crate::run).
///
/// Implementors own a private, freshly-created environment (e.g. a temp dir) so
/// two instances never share state. [`Engine::new`] must therefore be
/// independent per call.
pub trait Engine {
    /// Create a fresh, empty engine backed by its own private storage.
    fn new() -> Self
    where
        Self: Sized;

    /// Create a fresh engine opened in `mode` (M1.10, SPEC 01 Table 1):
    /// `WRITE_MAP` and/or durability flags. The default delegates to
    /// [`Engine::new`] (the default all-durable heap mode), so an engine that
    /// does not care about modes needs no override; the LMDB and zerodb engines
    /// override it to open with the matching env flags. Used by
    /// [`crate::run_in_mode`] and the M1.10 differential tests / fuzz dimension.
    fn new_in_mode(mode: EngineMode) -> Self
    where
        Self: Sized,
    {
        let _ = mode;
        Self::new()
    }

    /// A short human-readable name used in divergence reports.
    fn name(&self) -> &'static str;

    /// Apply one operation and return its normalized result.
    fn apply(&mut self, op: &Op) -> OpResult;

    /// Whether this engine implements `op` in the current milestone.
    ///
    /// The default is `true` (the LMDB reference implements the whole SPEC 00
    /// surface). A partially-built native engine (M1.2's `ZerodbEngine`, which
    /// covers only the env-lifecycle subset) overrides this to return `false`
    /// for ops it does not yet execute. [`crate::run`] gates on **both** engines
    /// implementing an op, so a differential run is symmetrically restricted to
    /// the ops both sides support — letting M1.3+ fill ops in one at a time
    /// without spurious divergences.
    fn implements(&self, _op: &Op) -> bool {
        true
    }

    /// The engine's actual on-disk data-file size, if it can report one.
    ///
    /// Feeds the M1.5 file-size tripwire in [`crate::run`] (ADR-0005 D5): after
    /// every committed op, the native engine's file must stay within a fixed
    /// band of the reference's, catching unbounded GC growth on every fuzz
    /// case. Default `None` (no check).
    fn real_disk_size(&self) -> Option<u64> {
        None
    }
}
