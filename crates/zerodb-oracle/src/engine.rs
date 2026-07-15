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
}
