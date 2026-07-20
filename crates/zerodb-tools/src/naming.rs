//! Resolving which data file an env directory holds (**ADR-0010**, D-012).
//!
//! Since ADR-0010 the data-file name depends on *which stack created the env*:
//! a natively-opened env holds [`zerodb::DATA_FILE_NAME`] (`zerodb.dat`), while
//! one created through the `heed-zerodb` adapter holds
//! [`zerodb::HEED_DATA_FILE_NAME`] (`data.mdb`) so that heed consumers which
//! hardcode LMDB's name — Meilisearch does, in production compaction and
//! snapshot paths — find the file they expect.
//!
//! The **engine** deliberately does no fallback probing: it opens exactly the
//! name it was configured with, so the adapter always reads the name it writes
//! (ADR-0010 §Options D3). The **tools** are read paths where a wrong guess is
//! recoverable and an operator should not have to know which stack wrote the
//! env, so they probe both names — and treat *both present* as a hard error
//! rather than silently picking one, because the two files would be two
//! different databases and no pick is defensible.
//!
//! Write-side tools (`load`, `migrate-from-lmdb`) do not probe: they create
//! native envs under `zerodb.dat`. They refuse to write into a directory that
//! already holds a non-empty env under *either* name ([`existing_data_files`]).

use std::io;
use std::path::{Path, PathBuf};

use zerodb::{DATA_FILE_NAME, HEED_DATA_FILE_NAME};

/// The names a zerodb env directory may use, in probe order (adapter name
/// first — it is the one Meilisearch produces).
pub const CANDIDATE_NAMES: [&str; 2] = [HEED_DATA_FILE_NAME, DATA_FILE_NAME];

/// A resolved env data file.
#[derive(Debug, Clone)]
pub struct EnvDataFile {
    /// The file name inside the env directory (one of [`CANDIDATE_NAMES`]).
    pub name: &'static str,
    /// The full path to the data file.
    pub path: PathBuf,
}

/// Why resolving an env directory's data file failed.
#[derive(Debug)]
pub enum NameError {
    /// The directory holds neither candidate name — there is no env here.
    Missing(PathBuf),
    /// The directory holds **both** candidate names. Two different databases;
    /// refusing to guess (ADR-0010).
    Ambiguous(PathBuf),
}

impl std::fmt::Display for NameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NameError::Missing(p) => write!(
                f,
                "no zerodb env at {}: neither {HEED_DATA_FILE_NAME} nor {DATA_FILE_NAME} found",
                p.display()
            ),
            NameError::Ambiguous(p) => write!(
                f,
                "ambiguous env at {}: it holds BOTH {HEED_DATA_FILE_NAME} and {DATA_FILE_NAME}, \
                 which are two different databases (ADR-0010). Remove or move one, or point the \
                 tool at a directory holding exactly one.",
                p.display()
            ),
        }
    }
}

impl std::error::Error for NameError {}

/// Every candidate data file that currently exists in `env_dir`, in probe
/// order. Used by the write tools to refuse clobbering an existing env under
/// either name.
#[must_use]
pub fn existing_data_files(env_dir: &Path) -> Vec<EnvDataFile> {
    CANDIDATE_NAMES
        .iter()
        .map(|&name| EnvDataFile {
            name,
            path: env_dir.join(name),
        })
        .filter(|c| c.path.exists())
        .collect()
}

/// Resolve the single data file in `env_dir` (ADR-0010 two-name probe).
///
/// # Errors
///
/// [`NameError::Missing`] if neither name is present, [`NameError::Ambiguous`]
/// if both are.
pub fn resolve(env_dir: &Path) -> Result<EnvDataFile, NameError> {
    let mut found = existing_data_files(env_dir);
    match found.len() {
        0 => Err(NameError::Missing(env_dir.to_path_buf())),
        1 => Ok(found.remove(0)),
        _ => Err(NameError::Ambiguous(env_dir.to_path_buf())),
    }
}

/// The `ZDB1` magic and format version read straight out of a data file's meta
/// slot 0 (SPEC 02 §3: `magic` at byte 32, `format_version` at 36).
///
/// Returns `None` if the file is too short to hold a meta header. Used for
/// engine identification in `stat` and for the `migrate-from-lmdb` mismatch
/// error — the magic, not the file name, is the authoritative discriminator
/// (ADR-0010 §Options D2).
///
/// # Errors
///
/// Propagates any read error other than a short file.
pub fn probe_magic(path: &Path) -> io::Result<Option<([u8; 4], u32)>> {
    use std::os::unix::fs::FileExt;

    let file = std::fs::File::open(path)?;
    let mut buf = [0u8; 8];
    match file.read_exact_at(&mut buf, 32) {
        Ok(()) => {
            let magic = [buf[0], buf[1], buf[2], buf[3]];
            let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            Ok(Some((magic, version)))
        }
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether `path` starts with ZeroDB's `ZDB1` meta magic (SPEC 02 §3).
///
/// # Errors
///
/// Propagates read errors other than a short file.
pub fn is_zerodb_image(path: &Path) -> io::Result<bool> {
    Ok(probe_magic(path)?.is_some_and(|(m, _)| m == zerodb_core::page::MAGIC))
}
