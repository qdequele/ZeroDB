//! `EnvFlags::NO_READ_AHEAD` (`MDB_NORDAHEAD`, SPEC 01 Table 1): the map is
//! advised `MADV_RANDOM`, as LMDB does, so a page fault reads only its page
//! instead of a readahead window — what keeps random reads over a dataset
//! larger than memory from thrashing (added 2026-09-29). The flag changes no
//! result; on Linux the advice is visible as the `rr` VmFlag of the data file's
//! mapping in `/proc/self/smaps`. Do not weaken (AGENTS.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{EnvFlags, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-nordahead-{pid}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The `VmFlags` of the mapping of `file` in this process, if found.
#[cfg(target_os = "linux")]
fn vm_flags_of(file: &Path) -> Option<String> {
    let file = file.canonicalize().ok()?;
    let smaps = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut in_mapping = false;
    for line in smaps.lines() {
        let first = line.split_whitespace().next().unwrap_or("");
        if first.contains('-') && first.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            in_mapping = line.trim_end().ends_with(file.to_str()?);
        } else if in_mapping && line.starts_with("VmFlags:") {
            return Some(line.to_string());
        }
    }
    None
}

#[test]
fn no_read_ahead_env_works_and_advises_random() {
    for write_map in [false, true] {
        for nordahead in [false, true] {
            let dir = TempDir::new();
            let mut flags = EnvFlags::EMPTY;
            if write_map {
                flags |= EnvFlags::WRITE_MAP;
            }
            if nordahead {
                flags |= EnvFlags::NO_READ_AHEAD;
            }
            let env = EnvOpenOptions::new()
                .map_size(64 << 20)
                .flags(flags)
                .open(dir.path())
                .expect("open");
            let db = env.main_database();
            let mut w = env.write_txn().unwrap();
            for i in 0u32..2000 {
                db.put(&mut w, &i.to_be_bytes(), &[7u8; 64]).unwrap();
            }
            w.commit().unwrap();
            let r = env.read_txn().unwrap();
            assert_eq!(
                db.get(&r, &1234u32.to_be_bytes()).unwrap(),
                Some(&[7u8; 64][..])
            );
            drop(r);

            #[cfg(target_os = "linux")]
            {
                let flags = vm_flags_of(&dir.path().join(zerodb::DATA_FILE_NAME))
                    .expect("the data file is mapped");
                let random = flags.split_whitespace().any(|f| f == "rr");
                assert_eq!(
                    random, nordahead,
                    "write_map={write_map} NO_READ_AHEAD={nordahead}: {flags}"
                );
            }
            drop(env);
        }
    }
}
