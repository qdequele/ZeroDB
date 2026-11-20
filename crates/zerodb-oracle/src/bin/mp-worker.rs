//! A worker process of the generated multi-process sequences
//! (`zerodb_oracle::mp`): `mp-worker <lmdb|zerodb> <dir>` runs the commands
//! it reads on standard input against that engine's environment in `dir`,
//! one answer line per command.

use std::path::PathBuf;

use zerodb_oracle::mp::{serve, MpEngine};

fn main() {
    let mut args = std::env::args().skip(1);
    let engine = match args.next().as_deref() {
        Some("lmdb") => MpEngine::Lmdb,
        Some("zerodb") => MpEngine::Zerodb,
        other => panic!("usage: mp-worker <lmdb|zerodb> <dir> (got {other:?})"),
    };
    let dir = PathBuf::from(args.next().expect("usage: mp-worker <lmdb|zerodb> <dir>"));
    serve(engine, &dir);
}
