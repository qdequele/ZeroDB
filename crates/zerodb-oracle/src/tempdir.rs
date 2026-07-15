//! A minimal self-cleaning temporary directory.
//!
//! Deliberately dependency-free: `tempfile` is not on the CLAUDE.md allowlist,
//! and the oracle only needs a unique, auto-removed directory to host an env.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

// Monotonic disambiguator for directory names. `Relaxed` is correct here: the
// counter guards nothing but its own uniqueness — no other memory is published
// or acquired through it, and each `fetch_add` returns a distinct value on every
// architecture regardless of ordering.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary directory removed (best-effort) on drop.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a new unique temporary directory under the system temp dir.
    pub fn new() -> std::io::Result<TempDir> {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-oracle-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path)?;
        Ok(TempDir { path })
    }

    /// The directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
