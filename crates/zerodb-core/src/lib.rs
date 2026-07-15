//! zerodb-core — pages, B+tree, txns, GC, reader table. No I/O policy.
//!
//! See `PLAN.md` and `docs/SPEC/` for the design. Milestone 1.1 implements the
//! [`page`] module: the on-disk page formats (SPEC 02).

#![forbid(unsafe_code)]

pub mod page;
