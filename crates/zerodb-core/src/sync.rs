//! Concurrency-primitive shim so the reader-table protocol (`crate::readers`)
//! compiles against `std::sync` natively (including under miri, which runs the
//! native path) and against `loom::sync` under `RUSTFLAGS="--cfg loom"`
//! (`just loom`) — the **same source** is model-checked and shipped
//! (ADR-0006 §loom).
//!
//! Scope: only `crate::readers`, `crate::nested` (the nested-txn child counter,
//! ADR-0007), `crate::stamps` (the ADR-0018 stamp cache), `crate::handles`'s
//! `MemoCell` (the ADR-0024 check memo, a seqlock), the in-process writer lock
//! (`env::writer_lock`, TXN-6), `crate::writer_word` (the cross-process
//! writer lock's word, LCK-4) and the `EnvInner` fields they own import from
//! here. The rest of the crate uses `std::sync`
//! directly: the loom tests drive these protocols in isolation, not a whole
//! env.
//!
//! `Arc` is deliberately **not** shimmed: the protocol under test never relies
//! on `Arc`'s internal refcount ordering (the cell mutex serializes clone vs
//! swap), so `std::sync::Arc` is used everywhere, including in loom models.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Condvar, Mutex};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Condvar, Mutex};
