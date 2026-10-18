//! `heed` — a verbatim re-export of the `heed-zerodb` adapter with its
//! `unique-txns` feature on: the API of Meilisearch's heed fork
//! (`meilisearch/heed@mutexify-open-dbi`, ADR-0027).
//!
//! The patch target for consumers built against that fork; consumers on
//! stock heed 0.22.1 use `crates/heed-shim` instead.
pub use heed_zerodb::*;
