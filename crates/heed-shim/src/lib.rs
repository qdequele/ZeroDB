//! `heed` — a verbatim re-export of the `heed-zerodb` adapter (milestone 1.13).
//!
//! This crate exists only so a consumer's `[patch.crates-io] heed = { path =
//! ".../crates/heed-shim" }` resolves (cargo `[patch]` matches by crate name).
//! All types, traits, modules (`types`, `byteorder`, `iteration_method`), and
//! the `Result`/`Error` aliases come straight from `heed-zerodb`, so consumers
//! that name `heed::…` get the adapter surface unchanged.
pub use heed_zerodb::*;
