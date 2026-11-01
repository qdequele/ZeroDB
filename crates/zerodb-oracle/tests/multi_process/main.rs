//! Multi-process behavior: the test binary re-runs itself as a child process
//! (see `zerodb_oracle::child`), on the LMDB fork and on ZeroDB with a lock file.

mod lmdb_reaping_observation;
mod lmdb_stage3_observation;
mod multi_process_differential;
