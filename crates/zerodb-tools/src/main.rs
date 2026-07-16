//! `zerodb-tools` binary entry point (M1.12). Parses argv and dispatches
//! through [`zerodb_tools::run`]; all logic lives in the library so it is
//! testable.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    zerodb_tools::run(&args)
}
