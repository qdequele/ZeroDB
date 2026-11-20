//! Power-loss harness below the file system (SPEC 06 REC-18 obligations,
//! checked on block-device states a power cut can leave).
//!
//! The image mechanism ([`super::image`]) models what a disk may keep, and the
//! SIGKILL mechanisms ([`super::sigkill`], [`super::multiproc`]) kill a real
//! process, which leaves the kernel page cache intact. Neither drops writes
//! that the file system accepted but the device never received. This module
//! does, on Linux, with the `dm-log-writes` device-mapper target:
//!
//! 1. A file system (ext4, XFS, ...) is created on a `dm-log-writes` device,
//!    which records on a separate log device every flush, every FUA write,
//!    and every other write once a flush that covers it has completed.
//! 2. [`run_workload`] runs a seeded crash workload on that file system. After
//!    each commit returns it inserts a mark into the log (`zdb-C-<txnid>`),
//!    so every log position is tied to the commits acknowledged before it.
//! 3. The verifier ([`LogReader`] + [`replay_to`]) rebuilds the device as it
//!    stood at chosen log positions, by default right after every flush, which
//!    is the state a power cut leaves once the device honors its flushes:
//!    every write before the flush, nothing after it. The caller mounts each
//!    rebuilt state (letting the file system replay its journal) and runs
//!    [`verify_point`]: the env opens, `check_image` passes, every commit
//!    acknowledged in a durable mode is present, and the recovered state is
//!    exactly one committed state, never part of a later commit.
//!
//! What this does not model: a device that reorders writes between two
//! flushes, or tears a sector. A log prefix keeps writes in log order.
//! The image mechanism covers reordering and tearing against its model.
//!
//! Only the modes whose recovery is promised corruption-free by REC-9 run
//! here: default, `WRITE_MAP` and `NO_META_SYNC`. `NO_SYNC` and `MAP_ASYNC`
//! depend on the file system's write order (REC-11), which a power cut does
//! not preserve.
//!
//! The log format read here is the one the kernel documents in
//! `Documentation/admin-guide/device-mapper/log-writes.rst` and writes in
//! `drivers/md/dm-log-writes.c`: a super block in the first logical block,
//! then one header block per entry, followed by the entry's data for writes.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::process::Command;

use zerodb::check::check_image;
use zerodb::{CompactionOption, CopyToFile, EnvOpenOptions};

use super::model::{Exec, ExecErr, StepOutcome, World};
use super::verify::compare_env_world;
use zerodb_io::fault::{splitmix64, Rng};

use super::workload::{gen_spec, Mode, Spec};
use crate::{DbName, Key, Op, Value};

// ---------------------------------------------------------------------------
// dm-log-writes log format
// ---------------------------------------------------------------------------

/// `WRITE_LOG_MAGIC` of the log super block.
pub const LOG_MAGIC: u64 = 0x006A_7366_7773_6872;
/// The only log version the kernel writes.
pub const LOG_VERSION: u64 = 1;
/// The entry is a flush: every entry before it reached stable storage.
pub const LOG_FLUSH: u64 = 1 << 0;
/// The entry is a FUA write: its own data reached stable storage.
pub const LOG_FUA: u64 = 1 << 1;
/// The entry discards `nr_sectors` sectors; no data follows it.
pub const LOG_DISCARD: u64 = 1 << 2;
/// The entry is a mark inserted with `dmsetup message <dev> 0 mark <name>`.
pub const LOG_MARK: u64 = 1 << 3;
/// The write carried `REQ_META`; informational only.
pub const LOG_METADATA: u64 = 1 << 4;

/// Size of the fixed part of an entry header (`struct log_write_entry`).
const ENTRY_HEADER: usize = 32;
/// Size of the fixed part of the super block (`struct log_write_super`).
const SUPER_LEN: usize = 28;

/// One entry of the log, without its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Position in the log, from 0.
    pub index: u64,
    /// First sector written or discarded, in units of the log's sector size.
    pub sector: u64,
    /// Sectors written or discarded, in units of the log's sector size.
    pub nr_sectors: u64,
    /// `LOG_*` flags.
    pub flags: u64,
    /// The mark name, for a mark entry.
    pub mark: Option<String>,
    /// Byte offset of the entry's data in the log, for a write.
    pub data_offset: u64,
}

impl LogEntry {
    /// Whether the entry is a write that carries data.
    #[must_use]
    pub fn is_write(&self) -> bool {
        self.flags & (LOG_DISCARD | LOG_MARK) == 0 && self.nr_sectors > 0
    }

    /// Whether the entry is a durability point: a flush, or a FUA write.
    #[must_use]
    pub fn is_barrier(&self) -> bool {
        self.flags & (LOG_FLUSH | LOG_FUA) != 0
    }
}

/// A parsed log: its sector size and every entry header.
#[derive(Debug)]
pub struct LogReader {
    file: File,
    /// The log's sector size in bytes (the logged device's logical block
    /// size).
    pub sectorsize: u64,
    /// Every entry, in log order.
    pub entries: Vec<LogEntry>,
}

fn le64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

fn le32(b: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(a)
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

impl LogReader {
    /// Parse the super block and every entry header of the log at `path`
    /// (the log device, or a file copied from it).
    ///
    /// # Errors
    ///
    /// An I/O error, or `InvalidData` when the log is not a version-1
    /// `dm-log-writes` log or an entry runs past the end of the log.
    pub fn open(path: &Path) -> io::Result<LogReader> {
        let file = File::open(path)?;
        let mut sup = [0u8; SUPER_LEN];
        file.read_exact_at(&mut sup, 0)?;
        let magic = le64(&sup, 0);
        let version = le64(&sup, 8);
        let nr_entries = le64(&sup, 16);
        let sectorsize = u64::from(le32(&sup, 24));
        if magic != LOG_MAGIC {
            return Err(invalid(format!("bad log magic {magic:#x}")));
        }
        if version != LOG_VERSION {
            return Err(invalid(format!("unsupported log version {version}")));
        }
        if !(512..=65536).contains(&sectorsize) || !sectorsize.is_power_of_two() {
            return Err(invalid(format!("bad log sector size {sectorsize}")));
        }
        let mut entries = Vec::with_capacity(usize::try_from(nr_entries).unwrap_or(0));
        let mut header = vec![0u8; sectorsize as usize];
        let mut offset = sectorsize;
        for index in 0..nr_entries {
            file.read_exact_at(&mut header, offset)
                .map_err(|e| invalid(format!("entry {index} header at byte {offset}: {e}")))?;
            let sector = le64(&header, 0);
            let nr_sectors = le64(&header, 8);
            let flags = le64(&header, 16);
            let data_len = le64(&header, 24);
            let mark = if flags & LOG_MARK != 0 {
                let len = usize::try_from(data_len)
                    .ok()
                    .filter(|l| ENTRY_HEADER + l <= header.len())
                    .ok_or_else(|| invalid(format!("entry {index}: mark length {data_len}")))?;
                let raw = &header[ENTRY_HEADER..ENTRY_HEADER + len];
                // The kernel stores the name without its terminator; stop at
                // a NUL in case a writer included one.
                let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
                Some(String::from_utf8_lossy(&raw[..end]).into_owned())
            } else {
                None
            };
            let data_offset = offset + sectorsize;
            // Writes carry their data right after the header; discards,
            // flushes and marks carry none.
            let data = if flags & (LOG_DISCARD | LOG_MARK) == 0 {
                nr_sectors
                    .checked_mul(sectorsize)
                    .ok_or_else(|| invalid(format!("entry {index}: size overflow")))?
            } else {
                0
            };
            entries.push(LogEntry {
                index,
                sector,
                nr_sectors,
                flags,
                mark,
                data_offset,
            });
            offset = data_offset
                .checked_add(data)
                .ok_or_else(|| invalid(format!("entry {index}: offset overflow")))?;
        }
        Ok(LogReader {
            file,
            sectorsize,
            entries,
        })
    }

    /// Index of the first entry carrying mark `name`.
    #[must_use]
    pub fn find_mark(&self, name: &str) -> Option<u64> {
        self.entries
            .iter()
            .find(|e| e.mark.as_deref() == Some(name))
            .map(|e| e.index)
    }
}

/// Apply log entries `from..=to` to `target`, the device being rebuilt.
/// Writes copy their data; discards write zeros; flushes and marks change
/// nothing. Returns the index of the next entry to apply.
///
/// # Errors
///
/// An I/O error reading the log or writing the target.
pub fn replay_to(log: &LogReader, target: &File, from: u64, to: u64) -> io::Result<u64> {
    let ss = log.sectorsize;
    let mut buf = Vec::new();
    let end = to.min(log.entries.len() as u64);
    let mut next = from;
    while next <= end && next < log.entries.len() as u64 {
        let e = &log.entries[next as usize];
        if e.flags & LOG_DISCARD != 0 {
            let zeros = vec![0u8; (ss as usize).max(1 << 16)];
            let mut left = e.nr_sectors * ss;
            let mut at = e.sector * ss;
            while left > 0 {
                let n = left.min(zeros.len() as u64);
                target.write_all_at(&zeros[..n as usize], at)?;
                left -= n;
                at += n;
            }
        } else if e.is_write() {
            let len = usize::try_from(e.nr_sectors * ss)
                .map_err(|_| invalid(format!("entry {}: write too large", e.index)))?;
            buf.resize(len, 0);
            log.file.read_exact_at(&mut buf, e.data_offset)?;
            target.write_all_at(&buf, e.sector * ss)?;
        }
        next += 1;
    }
    Ok(next)
}

/// Encode a log, for tests of the reader and of the replayer. Each item is
/// `(sector, flags, data, mark)`: a write when `data` is non-empty, a discard
/// of `sector..sector+n` when flags carry [`LOG_DISCARD`] (`n` in `data[0]`
/// sectors), a mark when `mark` is set.
#[doc(hidden)]
#[must_use]
pub fn encode_log(sectorsize: u64, items: &[(u64, u64, Vec<u8>, Option<&str>)]) -> Vec<u8> {
    let ss = sectorsize as usize;
    let mut out = vec![0u8; ss];
    out[0..8].copy_from_slice(&LOG_MAGIC.to_le_bytes());
    out[8..16].copy_from_slice(&LOG_VERSION.to_le_bytes());
    out[16..24].copy_from_slice(&(items.len() as u64).to_le_bytes());
    out[24..28].copy_from_slice(&(sectorsize as u32).to_le_bytes());
    for (sector, flags, data, mark) in items {
        let mut header = vec![0u8; ss];
        let (nr, data_len) = if let Some(m) = mark {
            header[ENTRY_HEADER..ENTRY_HEADER + m.len()].copy_from_slice(m.as_bytes());
            (0u64, m.len() as u64)
        } else if flags & LOG_DISCARD != 0 {
            (u64::from(data[0]), 0)
        } else {
            assert_eq!(data.len() % ss, 0, "write data must be whole sectors");
            ((data.len() / ss) as u64, 0)
        };
        let flags = if mark.is_some() {
            flags | LOG_MARK
        } else {
            *flags
        };
        header[0..8].copy_from_slice(&sector.to_le_bytes());
        header[8..16].copy_from_slice(&nr.to_le_bytes());
        header[16..24].copy_from_slice(&flags.to_le_bytes());
        header[24..32].copy_from_slice(&data_len.to_le_bytes());
        out.extend_from_slice(&header);
        if mark.is_none() && flags & LOG_DISCARD == 0 {
            out.extend_from_slice(data);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Marks
// ---------------------------------------------------------------------------

/// Mark inserted once the workload starts, before the env is created. Cut
/// points before it are not checked.
pub const MARK_START: &str = "zdb-start";
/// Mark inserted right before a `Commit` op runs.
pub const MARK_BEGIN: &str = "zdb-B";
/// Prefix of the mark inserted after a commit returned: `zdb-C-<txnid>`.
pub const MARK_COMMIT: &str = "zdb-C-";
/// Mark inserted when a `Commit` op committed nothing (no txn was open).
pub const MARK_SKIP: &str = "zdb-X";
/// Prefix of the mark inserted once a compacted copy of snapshot `<txnid>`
/// and its directory entries are synced: `zdb-K-<txnid>`.
pub const MARK_COMPACT: &str = "zdb-K-";
/// Mark inserted after the workload finished and closed the env.
pub const MARK_END: &str = "zdb-end";

/// What the marks up to a cut point say about the workload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkState {
    /// [`MARK_START`] was seen.
    pub started: bool,
    /// Highest txnid acknowledged by a returned commit (0 if none).
    pub acked: u64,
    /// Every acknowledged txnid, in order.
    pub acks: Vec<u64>,
    /// A commit had begun and had not returned at the cut.
    pub pending: bool,
    /// Snapshot txnid of a compacted copy durable at the cut.
    pub compacted: Option<u64>,
    /// [`MARK_END`] was seen.
    pub ended: bool,
}

impl MarkState {
    /// Fold one mark into the state. Marks of other tools are ignored.
    ///
    /// # Errors
    ///
    /// A mark of this harness that does not parse.
    pub fn apply(&mut self, name: &str) -> Result<(), String> {
        if name == MARK_START {
            self.started = true;
        } else if name == MARK_BEGIN {
            self.pending = true;
        } else if name == MARK_SKIP {
            self.pending = false;
        } else if name == MARK_END {
            self.ended = true;
        } else if let Some(t) = name.strip_prefix(MARK_COMMIT) {
            let t: u64 = t.parse().map_err(|_| format!("bad commit mark {name:?}"))?;
            self.acked = t;
            self.acks.push(t);
            self.pending = false;
        } else if let Some(t) = name.strip_prefix(MARK_COMPACT) {
            let t: u64 = t
                .parse()
                .map_err(|_| format!("bad compaction mark {name:?}"))?;
            self.compacted = Some(t);
        }
        Ok(())
    }

    /// The lowest txnid recovery may select in `mode`.
    ///
    /// Default and `WRITE_MAP` make a commit durable before it returns
    /// (REC-6). Under `NO_META_SYNC` the meta of the last acknowledged commit
    /// may be lost, but each commit's data barrier also covers the previous
    /// commit's meta (REC-10), so at most that one commit is lost.
    #[must_use]
    pub fn floor(&self, mode: Mode) -> u64 {
        if mode.strict_ack() {
            self.acked
        } else {
            self.acked.saturating_sub(1)
        }
    }

    /// The highest txnid recovery may select: the in-flight commit, if any.
    #[must_use]
    pub fn ceil(&self) -> u64 {
        self.acked + u64::from(self.pending)
    }
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

/// Sub-directory of the mounted file system that holds the env.
pub const ENV_DIR: &str = "env";
/// Sub-directory of the mounted file system that holds the compacted copy.
pub const COMPACT_DIR: &str = "compact";

/// Parameters of one power-loss run, shared by the workload and the verifier.
#[derive(Debug, Clone)]
pub struct PowerSpec {
    /// The crash workload spec (`gen_spec(seed)` with the mode and spill
    /// choice below forced).
    pub spec: Spec,
    /// Workload rounds ([`Spec::ops_for_round`]).
    pub rounds: u32,
    /// Round after which the workload closes its open txn and writes a
    /// compacted copy; `None` for no compaction.
    pub compact_after: Option<u32>,
    /// Small commits (one to three small puts or deletes each) appended to
    /// every round.
    pub small_commits: u32,
    /// Whether round 0 opens with one large commit of multi-page values.
    pub large_commit: bool,
}

impl PowerSpec {
    /// The ops of round `round`: on round 0 the large commit, then the crash
    /// workload's round ([`Spec::ops_for_round`]), then the small commits.
    /// The seeded rounds alone commit about once per round, too rarely to put
    /// many cut points between acknowledged commits.
    #[must_use]
    pub fn ops_for_round(&self, round: u32) -> Vec<Op> {
        let mut rng = Rng::new(splitmix64(
            self.spec.seed ^ (0x9077_E400_0000 + u64::from(round)),
        ));
        let mut ops = Vec::new();
        if round == 0 && self.large_commit {
            ops.extend(large_phase(&mut rng));
        }
        ops.extend(self.spec.ops_for_round(round));
        ops.extend(small_phase(&mut rng, round, self.small_commits));
        ops
    }
}

fn val(rng: &mut Rng, len: usize) -> Value {
    let mut v = vec![0u8; len];
    rng.fill(&mut v);
    Value(v)
}

/// One committed txn of 48 overflow values of 64 to 256 KiB (about 8 MiB),
/// written in many device writes and several file-system extents.
fn large_phase(rng: &mut Rng) -> Vec<Op> {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
    ];
    for i in 0..48u32 {
        let len = (64 << 10) + rng.below(192 << 10);
        ops.push(Op::Put {
            db: 0,
            key: Key(format!("large-{i:03}").into_bytes()),
            val: val(rng, len),
        });
    }
    ops.push(Op::Commit);
    ops
}

/// `n` committed txns of one to three small puts, or a delete of a key an
/// earlier small commit of the round wrote.
fn small_phase(rng: &mut Rng, round: u32, n: u32) -> Vec<Op> {
    let mut ops = Vec::new();
    for i in 0..n {
        ops.push(Op::BeginRw);
        // Puts resolve over existing databases; idempotent once created.
        ops.push(Op::CreateDb {
            name: DbName::Unnamed,
        });
        for j in 0..=rng.below(3) {
            if i > 0 && rng.ratio(1, 5) {
                let k = rng.below(i as usize);
                ops.push(Op::Del {
                    db: 0,
                    key: Key(format!("small-{round}-{k:03}-0").into_bytes()),
                });
            } else {
                let len = 16 + rng.below(400);
                ops.push(Op::Put {
                    db: 0,
                    key: Key(format!("small-{round}-{i:03}-{j}").into_bytes()),
                    val: val(rng, len),
                });
            }
        }
        ops.push(Op::Commit);
    }
    ops
}

/// Modes this harness runs (see the module docs).
#[must_use]
pub fn power_mode(name: &str) -> Option<Mode> {
    match name {
        "default" => Some(Mode::Default),
        "writemap" => Some(Mode::WriteMap),
        "nometasync" => Some(Mode::NoMetaSync),
        _ => None,
    }
}

/// Build the spec of one run. `spill` forces a small dirty limit (large
/// commits that spill, ADR-0017) or none.
#[must_use]
pub fn power_spec(
    seed: u64,
    mode: Mode,
    spill: bool,
    rounds: u32,
    compact_after: Option<u32>,
) -> PowerSpec {
    let mut spec = gen_spec(seed);
    spec.mode = mode;
    // The public option raises a smaller limit to its floor (as in the
    // SIGKILL child); 64 pages is below it, so the floor applies.
    spec.dirty_limit = spill.then_some(64);
    // Room for the large commit, a multi-MB value and the spill phase.
    spec.map_size = 256 << 20;
    PowerSpec {
        spec,
        rounds,
        compact_after,
        small_commits: 16,
        large_commit: true,
    }
}

/// What the workload did.
#[derive(Debug, Default)]
pub struct WorkloadSummary {
    /// Acknowledged txnids, in order.
    pub acks: Vec<u64>,
    /// Snapshot txnid of the compacted copy, if one was written.
    pub compacted: Option<u64>,
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Run the workload in `root` (a directory on the file system under test),
/// calling `mark` at each point listed in the [`MARK_START`].. constants.
/// The env lives in `root/env`, the compacted copy in `root/compact`.
///
/// # Errors
///
/// A description of what failed: an env error, a model/engine drift, or a
/// failed mark.
pub fn run_workload(
    ps: &PowerSpec,
    root: &Path,
    mark: &mut dyn FnMut(&str) -> io::Result<()>,
) -> Result<WorkloadSummary, String> {
    let spec = &ps.spec;
    let mut mark = |m: &str| mark(m).map_err(|e| format!("mark {m}: {e}"));
    mark(MARK_START)?;
    let dir = root.join(ENV_DIR);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    sync_dir(root).map_err(|e| format!("sync {}: {e}", root.display()))?;
    let mut opts = EnvOpenOptions::new();
    opts.map_size(spec.map_size as usize)
        .page_size(spec.page_size)
        .max_dbs(16)
        .flags(spec.mode.env_flags());
    if let Some(pages) = spec.dirty_limit {
        opts.max_dirty_bytes(pages as usize * spec.page_size as usize);
    }
    // SAFETY: the directory was just created by this function and holds no
    // env yet; nothing else modifies the file while it is mapped.
    let env = unsafe { opts.open(&dir) }.map_err(|e| format!("open: {e}"))?;
    let mut exec = Exec::new(Some(&env));
    let mut summary = WorkloadSummary::default();
    for round in 0..ps.rounds {
        for op in ps.ops_for_round(round) {
            let commit = matches!(op, Op::Commit);
            if commit {
                mark(MARK_BEGIN)?;
            }
            match exec.step(&op) {
                Ok(StepOutcome::Committed(t)) => {
                    mark(&format!("{MARK_COMMIT}{t}"))?;
                    summary.acks.push(t);
                }
                Ok(_) if commit => mark(MARK_SKIP)?,
                Ok(_) => {}
                Err(ExecErr::Drift(d)) => return Err(format!("model/engine drift: {d}")),
                Err(ExecErr::Abandon(a)) => return Err(format!("abandoned: {a}")),
            }
        }
        if ps.compact_after == Some(round) {
            exec.finish();
            let t = env.txnid();
            let cdir = root.join(COMPACT_DIR);
            std::fs::create_dir_all(&cdir)
                .map_err(|e| format!("create {}: {e}", cdir.display()))?;
            let cfile = cdir.join(zerodb::DATA_FILE_NAME);
            env.copy_to_file(&cfile, CompactionOption::Enabled)
                .map_err(|e| format!("compaction: {e}"))?;
            File::open(&cfile)
                .and_then(|f| f.sync_all())
                .and_then(|()| sync_dir(&cdir))
                .and_then(|()| sync_dir(root))
                .map_err(|e| format!("sync compacted copy: {e}"))?;
            mark(&format!("{MARK_COMPACT}{t}"))?;
            summary.compacted = Some(t);
        }
    }
    exec.finish();
    drop(exec);
    drop(env);
    mark(MARK_END)?;
    Ok(summary)
}

/// The committed state of every txnid the workload can reach, rebuilt by
/// replaying the same ops without an env.
#[derive(Debug)]
pub struct Model {
    /// Committed state per txnid.
    pub states: BTreeMap<u64, World>,
    /// Txnid after each `Commit` op that committed, in order.
    pub commit_log: Vec<u64>,
}

/// Replay `ps`'s ops on the model alone, exactly as [`run_workload`] runs
/// them (including the txn close before compaction).
#[must_use]
pub fn build_model(ps: &PowerSpec) -> Model {
    let mut exec = Exec::new(None);
    for round in 0..ps.rounds {
        for op in ps.ops_for_round(round) {
            // A standalone replay has no env to disagree with.
            let _ = exec.step(&op);
        }
        if ps.compact_after == Some(round) {
            exec.finish();
        }
    }
    exec.finish();
    Model {
        states: std::mem::take(&mut exec.states),
        commit_log: std::mem::take(&mut exec.commit_log),
    }
}

// ---------------------------------------------------------------------------
// Verification of one cut
// ---------------------------------------------------------------------------

/// The verdict on one cut point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointOutcome {
    /// Every obligation held; recovery selected `txnid`.
    Verified {
        /// The recovered txnid.
        txnid: u64,
    },
    /// The cut fell before the first commit was acknowledged and the env
    /// file is absent or not yet a complete env (SPEC 06 §5, env-creation
    /// window): no obligation applies.
    CreationWindow,
    /// `NO_META_SYNC` only: recovery fell below the newest issued commit and
    /// the walk or the data comparison failed. This is the reclaim-clobber
    /// window REC-10 documents (shared with LMDB); it is counted, not failed.
    StaleFallback {
        /// The recovered txnid.
        txnid: u64,
    },
}

fn open_validated(dir: &Path) -> Result<zerodb::Env, String> {
    let mut opts = EnvOpenOptions::new();
    opts.max_dbs(16);
    // SAFETY: opened with `FileTrust::VALIDATE`, so a damaged file yields an
    // error; nothing else modifies the file while it is mapped.
    unsafe { opts.file_trust(zerodb::FileTrust::VALIDATE).open(dir) }.map_err(|e| e.to_string())
}

fn walk_file(dir: &Path, psize: u32) -> Result<(), String> {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME))
        .map_err(|e| format!("reading {}: {e}", dir.display()))?;
    let v = check_image(&bytes, psize);
    if v.is_empty() {
        Ok(())
    } else {
        Err(format!("check_image failed: {v:?}"))
    }
}

fn run_tools_check(tools: &Path, dir: &Path) -> Result<(), String> {
    let out = Command::new(tools)
        .arg("check")
        .arg(dir)
        .output()
        .map_err(|e| format!("running {} check: {e}", tools.display()))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "zerodb-tools check {} failed ({}): {}{}",
            dir.display(),
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

/// Verify the file system state of one cut, mounted at `root`, against the
/// marks seen up to the cut.
///
/// Obligations: the env opens; the recovered txnid lies in
/// `[floor, ceil]` (every commit acknowledged in a durable mode is present,
/// nothing past the in-flight commit is); `check_image` passes, as does
/// `zerodb-tools check` when `tools` is given; the contents equal the
/// model's committed state for exactly the recovered txnid (no later commit
/// is partly visible). When `final_state` is set (the cut is the end of the
/// log, after the file system was unmounted cleanly) recovery must select
/// the last acknowledged commit. When the marks show a durable compacted
/// copy, it must open, pass the same checks and equal its snapshot's state.
///
/// # Errors
///
/// The violated obligation.
pub fn verify_point(
    root: &Path,
    ps: &PowerSpec,
    model: &Model,
    marks: &MarkState,
    final_state: bool,
    tools: Option<&Path>,
) -> Result<PointOutcome, String> {
    let r = catch_unwind(AssertUnwindSafe(|| {
        verify_point_inner(root, ps, model, marks, final_state, tools)
    }));
    r.unwrap_or_else(|_| Err("panic during verification".to_string()))
}

fn verify_point_inner(
    root: &Path,
    ps: &PowerSpec,
    model: &Model,
    marks: &MarkState,
    final_state: bool,
    tools: Option<&Path>,
) -> Result<PointOutcome, String> {
    let mode = ps.spec.mode;
    let psize = ps.spec.page_size;
    // The acknowledged commits must be the ones the model predicts.
    if marks.acks.len() > model.commit_log.len()
        || marks.acks[..] != model.commit_log[..marks.acks.len()]
    {
        return Err(format!(
            "acknowledged txnids {:?} are not a prefix of the model's {:?}",
            &marks.acks[..marks.acks.len().min(8)],
            &model.commit_log[..model.commit_log.len().min(8)]
        ));
    }
    let dir = root.join(ENV_DIR);
    let env = match open_validated(&dir) {
        Ok(env) => env,
        Err(e) => {
            if marks.acks.is_empty() {
                return Ok(PointOutcome::CreationWindow);
            }
            return Err(format!("recovery open failed: {e}"));
        }
    };
    let r = env.txnid();
    let (floor, ceil) = (marks.floor(mode), marks.ceil());
    if r < floor || r > ceil {
        return Err(format!(
            "recovered txnid {r} outside [{floor}, {ceil}] (acked {}, pending {})",
            marks.acked, marks.pending
        ));
    }
    if final_state && r != marks.acked {
        return Err(format!(
            "after a clean unmount recovery selected {r}, not the last acknowledged {}",
            marks.acked
        ));
    }
    let world = model
        .states
        .get(&r)
        .ok_or_else(|| format!("recovered txnid {r} has no modeled state"))?;
    // A cut inside env creation leaves a file shorter than two meta pages,
    // which opens (REC-4) but which the walker rightly rejects for any env
    // past creation; no commit can have been acknowledged then.
    if marks.acks.is_empty() && r == 0 {
        let len = std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME)).map_or(0, |m| m.len());
        if len < 2 * u64::from(psize) {
            return Ok(PointOutcome::CreationWindow);
        }
    }
    let checks = walk_file(&dir, psize)
        .and_then(|()| compare_env_world(&env, world))
        .map_err(|e| format!("recovered txnid {r}: {e}"));
    drop(env);
    if let Err(e) = checks {
        if mode == Mode::NoMetaSync && r < ceil {
            return Ok(PointOutcome::StaleFallback { txnid: r });
        }
        return Err(e);
    }
    if let Some(tools) = tools {
        run_tools_check(tools, &dir)?;
    }
    if let Some(t) = marks.compacted {
        let cdir = root.join(COMPACT_DIR);
        let cworld = model
            .states
            .get(&t)
            .ok_or_else(|| format!("compacted snapshot {t} has no modeled state"))?;
        let cenv = open_validated(&cdir).map_err(|e| format!("compacted copy open failed: {e}"))?;
        let res = walk_file(&cdir, psize).and_then(|()| compare_env_world(&cenv, cworld));
        drop(cenv);
        res.map_err(|e| format!("compacted copy of snapshot {t}: {e}"))?;
        if let Some(tools) = tools {
            run_tools_check(tools, &cdir)?;
        }
    }
    Ok(PointOutcome::Verified { txnid: r })
}

/// Choose the cut points to check: every barrier entry after the start mark,
/// plus every `every`-th entry when `every > 0`, plus the last entry; then,
/// when there are more than `max_points`, an evenly spaced subset that keeps
/// the last entry.
#[must_use]
pub fn select_points(log: &LogReader, every: u64, max_points: usize) -> Vec<u64> {
    let Some(start) = log.find_mark(MARK_START) else {
        return Vec::new();
    };
    let mut pts: Vec<u64> = log
        .entries
        .iter()
        .filter(|e| e.index > start)
        .filter(|e| e.is_barrier() || (every > 0 && e.index % every == 0))
        .map(|e| e.index)
        .collect();
    if let Some(last) = log.entries.last() {
        if last.index > start && pts.last() != Some(&last.index) {
            pts.push(last.index);
        }
    }
    if max_points > 0 && pts.len() > max_points {
        let n = pts.len();
        let last = pts[n - 1];
        let mut chosen: Vec<u64> = (0..max_points.saturating_sub(1))
            .map(|i| pts[i * (n - 1) / (max_points - 1).max(1)])
            .collect();
        chosen.dedup();
        chosen.push(last);
        pts = chosen;
    }
    pts
}

/// The marks up to and including entry `to`.
///
/// # Errors
///
/// A mark of this harness that does not parse.
pub fn marks_until(log: &LogReader, to: u64) -> Result<MarkState, String> {
    let mut m = MarkState::default();
    for e in log.entries.iter().take_while(|e| e.index <= to) {
        if let Some(name) = &e.mark {
            m.apply(name)?;
        }
    }
    Ok(m)
}

#[cfg(test)]
mod tests;
