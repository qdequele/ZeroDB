//! zerodb-core — pages, B+tree, txns, GC, reader table. No I/O policy.
//!
//! See `PLAN.md` and `docs/SPEC/` for the design.
//!
//! - Milestone 1.1: the [`page`] module — on-disk page formats (SPEC 02).
//! - Milestone 1.2: the [`env`] module — environment open/close, meta selection,
//!   and the same-process registry (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7), plus
//!   the [`error`] taxonomy (SPEC 00 rows 54–58). All mmap `unsafe` lives in
//!   `zerodb-io` behind [`env::Backing`]; this crate contains none.

#![forbid(unsafe_code)]

pub mod btree;
pub mod builder;
pub mod env;
pub mod error;
pub mod page;
pub mod rotxn;

pub use rotxn::{Database, RoRange, RoTxn};

pub use error::{Error, MdbError, Result};
