//! Print what a `diff_ops` fuzz artifact decodes to: the engine mode (from
//! the first byte) and the op sequence (from the rest), exactly as
//! `fuzz/fuzz_targets/diff_ops.rs` reads its input. The engine pair is chosen
//! by `ZERODB_FUZZ_PAIR` at fuzz time, not by the artifact.
//!
//! `cargo run -p zerodb-oracle --example decode_artifact -- <artifact>`
use zerodb_oracle::{decode_ops, EngineMode};

fn main() {
    let path = std::env::args().nth(1).expect("artifact path");
    let bytes = std::fs::read(path).expect("read artifact");
    let (mode, rest) = match bytes.split_first() {
        Some((seed, rest)) => (EngineMode::from_fuzz_byte(*seed), rest),
        None => (EngineMode::DEFAULT, &bytes[..]),
    };
    println!("mode: {mode:?}");
    for (i, op) in decode_ops(rest, 64).iter().enumerate() {
        println!("{i:3}: {op:?}");
    }
}
