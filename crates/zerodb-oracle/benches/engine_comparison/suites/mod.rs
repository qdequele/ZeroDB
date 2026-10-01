//! One module per suite. Every suite exposes `run(&mut Criterion, &Cfg)` and
//! names its groups `suite/case/variant`, with `lmdb` / `zerodb` as the two
//! functions inside each group — so `cargo bench -- '^get/'` selects a suite and
//! `scripts/bench-report.py` can pair the two engines mechanically.

pub mod commit;
pub mod concurrent;
pub mod del;
pub mod env;
pub mod get;
pub mod maint;
pub mod mixed;
pub mod put;
pub mod scan;
pub mod seek;
