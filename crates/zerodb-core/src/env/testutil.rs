//! Test-only helpers: a heap-backed [`Backing`] and an in-memory env
//! constructor, so txn/mutation logic runs under `miri` (no mmap, no file
//! I/O). **Not part of the stable API** — used by this crate's tests, the
//! crate's integration tests, and nothing else. The backing is read-only
//! (commit I/O keeps the erroring `Backing` defaults), so in-memory envs
//! exercise mutation and in-txn reads but never the commit pipeline — commits
//! are tested against real files in `crates/zerodb`.

use super::{next_env_id, open_with_backing_policy, Backing, Env, MapView};
use crate::page::FileTrust;
use crate::page::MetaPage;
use std::path::PathBuf;

/// A heap-backed read-only [`Backing`]: the bytes model the file, and the
/// logical map size is fixed at construction.
pub struct VecBacking {
    bytes: Vec<u8>,
    map_size: u64,
}

impl VecBacking {
    /// A backing over `bytes` (the file's contents) with a logical map
    /// size of `map_size`, which may exceed `bytes.len()`: pages past the
    /// file are never read, as with a real map past the end of the file.
    #[must_use]
    pub fn new(bytes: Vec<u8>, map_size: u64) -> VecBacking {
        VecBacking { bytes, map_size }
    }

    /// The file's contents, for a test that edits them in place.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl Backing for VecBacking {
    fn view(&self) -> MapView<'_> {
        MapView {
            bytes: &self.bytes,
            map_size: self.map_size,
        }
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "vec backing has no fd",
        ))
    }
}

/// A fresh in-memory env: both meta slots valid at txnid 0 (SPEC 02 §3.4),
/// the backing vector sized to `map_size` (as a real map would be,
/// ADR-0004), registered under a unique virtual path.
///
/// # Panics
///
/// On an invalid `page_size` (test helper).
#[must_use]
pub fn mem_env(page_size: u32, map_size: u64) -> Env {
    mem_env_with(page_size, map_size, false)
}

/// [`mem_env`] with the sequential-writes default on (ADR-0015), for the
/// rightmost-leaf finger tests.
///
/// # Panics
///
/// On an invalid `page_size` (test helper).
#[must_use]
pub fn mem_env_sequential(page_size: u32, map_size: u64) -> Env {
    mem_env_with(page_size, map_size, true)
}

fn mem_env_with(page_size: u32, map_size: u64, sequential_writes: bool) -> Env {
    let ps = page_size as usize;
    let mut buf = vec![0u8; map_size as usize];
    for slot in [0u64, 1] {
        let meta = MetaPage::create(slot, page_size, map_size);
        let base = slot as usize * ps;
        meta.encode(&mut buf[base..base + ps]).expect("valid meta");
    }
    let path = PathBuf::from(format!("/virtual/mem-env-{}", next_env_id()));
    // A generous named-DB capacity for tests (real envs pass the caller's
    // `max_dbs`; SPEC 02 §6); max_readers = 126, the TXN-14
    // default.
    match open_with_backing_policy(
        path,
        Box::new(VecBacking::new(buf, map_size)),
        page_size,
        false,
        128,
        126,
        super::DurabilityFlags::default(),
        FileTrust::VALIDATE,
        sequential_writes,
        None,
    ) {
        Ok(env) => env,
        Err(e) => panic!("mem_env open failed: {e}"),
    }
}
