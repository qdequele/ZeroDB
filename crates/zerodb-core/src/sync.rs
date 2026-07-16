//! Concurrency-primitive shim so the reader-table protocol (`crate::readers`)
//! compiles against `std::sync` natively (including under miri, which runs the
//! native path) and against `loom::sync` under `RUSTFLAGS="--cfg loom"`
//! (`just loom`) — the **same source** is model-checked and shipped
//! (ADR-0006 §loom).
//!
//! Scope: only `crate::readers`, `crate::nested` (the M1.9 child counter,
//! ADR-0007 D3) and the `EnvInner` fields they own import from here. The rest
//! of the crate uses `std::sync` directly — loom tests drive the
//! table/cell/commit-point and child-counter protocols in isolation, not a
//! whole env.
//!
//! `Arc` is deliberately **not** shimmed: the protocol under test never relies
//! on `Arc`'s internal refcount ordering (the cell mutex serializes clone vs
//! swap), so `std::sync::Arc` is used everywhere, including in loom models.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::Mutex;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::Mutex;
