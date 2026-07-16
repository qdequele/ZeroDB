//! The engine-agnostic trait both sides of the differential harness implement.

use crate::{Op, OpResult};

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
}
