//! Generated multi-process sequences.
//!
//! An [`MpCase`] drives two or three worker processes on one environment:
//! read transactions held in reader slots, writes, bulk growth past another
//! worker's map, `resize` (including `resize(0)`, which adopts the size
//! another process persisted), `MapResized`, `SIGKILL` of a worker while it
//! holds readers, stale-reader cleanup, and respawning a worker into the
//! running environment. The case runs once with every worker on LMDB (heed)
//! and once with every worker on ZeroDB (`heed-zerodb`, with its lock file),
//! and the two transcripts must be equal.
//!
//! Each worker is the `mp-worker` binary, serving the line protocol of
//! [`Cmd`] on its standard input and output ([`serve`]). The parent waits for
//! every answer before sending the next command, so the interleaving is the
//! generated one, and it waits for a killed worker to be reaped before going
//! on, so a dead reader is dead by the next step.
//!
//! **Sizes.** Where a map fills, and whether a reader's map is too small for
//! the newest snapshot, depend on each engine's page allocation, as
//! [`crate::scenario::MapFullCase`] explains. The sizes here are far apart so
//! no outcome sits near such a boundary: maps are [`SMALL_MAP`] or
//! [`BIG_MAP`]; small writes overwrite one fixed set of keys, so they never
//! come close to the small map even with every reader slot pinning an older
//! version; a bulk write adds [`BULK_BYTES`] of new keys, well past the small
//! map, and a case holds at most [`MAX_BULKS`] of them, well under the big
//! one. The comparison also stops at the first `MapFull` either engine
//! reports, as the reopen scenarios do.
//!
//! Not generated: `WRITE_MAP` (a joining LMDB process truncates the data
//! file to its own map size, which can leave another process's mapping past
//! the end of the file, D-024), `PREV_SNAPSHOT` (the two approved differences
//! of D-025) and `NO_LOCK` (single-process by definition). A worker never
//! holds a write transaction between commands, so no worker ever waits on
//! another's writer lock, and it refuses `resize` while it holds readers
//! (heed's `resize` contract).

use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};

use crate::child::ChildGuard;
use crate::scenario::ScenarioDivergence;
use crate::tempdir::TempDir;

/// Reader slots per worker.
pub const MP_SLOTS: u8 = 3;
/// `max_readers` of the environment: fewer than the slots of all workers
/// together, so `ReadersFull` is reachable, and dead readers matter.
pub const MP_MAX_READERS: u32 = 4;
/// The small map: 2 MiB.
pub const SMALL_MAP: usize = 2 << 20;
/// The big map: 32 MiB.
pub const BIG_MAP: usize = 32 << 20;
/// Bytes of values one bulk write adds: 4 MiB, twice the small map.
pub const BULK_BYTES: usize = 4 << 20;
/// Bulk writes per case at most.
pub const MAX_BULKS: u8 = 2;
/// Puts per write transaction of a bulk write. Every commit copies the path
/// to its leaves, and a reader pinning an old snapshot keeps every copy, so
/// few large commits keep that waste far below the big map.
pub const BULK_BATCH: usize = 128;
/// Keys a small write overwrites, from a fixed set of 128.
pub const SMALL_KEYS: u16 = 128;

/// A map size choice.
#[derive(Arbitrary, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapChoice {
    /// [`SMALL_MAP`].
    Small,
    /// [`BIG_MAP`].
    Big,
}

impl MapChoice {
    fn bytes(self) -> usize {
        match self {
            MapChoice::Small => SMALL_MAP,
            MapChoice::Big => BIG_MAP,
        }
    }
}

/// One command a worker runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    /// Close the environment, if open, and open it again.
    Open {
        /// The map size.
        map: MapChoice,
        /// `READ_ONLY`.
        read_only: bool,
    },
    /// Close the environment.
    Close,
    /// Begin a read transaction into a slot.
    BeginRead(u8),
    /// End the read transaction of a slot.
    EndRead(u8),
    /// Read through a slot: the entry count and a digest of every entry.
    Scan(u8),
    /// `get` of a small-write key through a slot.
    Get(u8, u16),
    /// One write transaction: put `n` of the small keys from `start`, with
    /// values of `vlen` bytes tagged `tag`, then commit.
    Write {
        /// First key.
        start: u16,
        /// Number of keys.
        n: u8,
        /// Value length.
        vlen: u8,
        /// Value byte.
        tag: u8,
    },
    /// Delete small keys `start..start + n` in one write transaction.
    Delete {
        /// First key.
        start: u16,
        /// Number of keys.
        n: u8,
    },
    /// Add [`BULK_BYTES`] of new keys, in fixed batches of [`BULK_BATCH`]
    /// puts per write transaction. `id` keeps two bulks' keys apart.
    Bulk {
        /// Which bulk this is.
        id: u8,
    },
    /// `Env::resize`: to a map choice, or 0 to adopt the persisted size.
    Resize(Option<MapChoice>),
    /// `Env::info()`: map size, last txn id, reader high-water mark.
    Info,
    /// `Env::clear_stale_readers`.
    ClearStale,
}

impl Cmd {
    /// The command as one protocol line.
    #[must_use]
    pub fn encode(&self) -> String {
        let map = |m: &MapChoice| match m {
            MapChoice::Small => "small",
            MapChoice::Big => "big",
        };
        match self {
            Cmd::Open { map: m, read_only } => format!("open {} {}", map(m), u8::from(*read_only)),
            Cmd::Close => "close".into(),
            Cmd::BeginRead(s) => format!("begin {s}"),
            Cmd::EndRead(s) => format!("end {s}"),
            Cmd::Scan(s) => format!("scan {s}"),
            Cmd::Get(s, k) => format!("get {s} {k}"),
            Cmd::Write {
                start,
                n,
                vlen,
                tag,
            } => format!("write {start} {n} {vlen} {tag}"),
            Cmd::Delete { start, n } => format!("delete {start} {n}"),
            Cmd::Bulk { id } => format!("bulk {id}"),
            Cmd::Resize(None) => "resize 0".into(),
            Cmd::Resize(Some(m)) => format!("resize {}", map(m)),
            Cmd::Info => "info".into(),
            Cmd::ClearStale => "stale".into(),
        }
    }

    /// Parse a protocol line.
    ///
    /// # Errors
    ///
    /// On a malformed line.
    pub fn decode(line: &str) -> Result<Cmd, String> {
        let w: Vec<&str> = line.split_whitespace().collect();
        let num = |i: usize| -> Result<u64, String> {
            w.get(i)
                .ok_or_else(|| format!("missing field {i}: {line}"))?
                .parse::<u64>()
                .map_err(|e| format!("{e}: {line}"))
        };
        let map = |i: usize| match w.get(i) {
            Some(&"small") => Ok(MapChoice::Small),
            Some(&"big") => Ok(MapChoice::Big),
            _ => Err(format!("bad map: {line}")),
        };
        let slot = |i: usize| num(i).map(|v| v as u8);
        Ok(match w.first().copied() {
            Some("open") => Cmd::Open {
                map: map(1)?,
                read_only: num(2)? != 0,
            },
            Some("close") => Cmd::Close,
            Some("begin") => Cmd::BeginRead(slot(1)?),
            Some("end") => Cmd::EndRead(slot(1)?),
            Some("scan") => Cmd::Scan(slot(1)?),
            Some("get") => Cmd::Get(slot(1)?, num(2)? as u16),
            Some("write") => Cmd::Write {
                start: num(1)? as u16,
                n: num(2)? as u8,
                vlen: num(3)? as u8,
                tag: num(4)? as u8,
            },
            Some("delete") => Cmd::Delete {
                start: num(1)? as u16,
                n: num(2)? as u8,
            },
            Some("bulk") => Cmd::Bulk { id: num(1)? as u8 },
            Some("resize") if w.get(1) == Some(&"0") => Cmd::Resize(None),
            Some("resize") => Cmd::Resize(Some(map(1)?)),
            Some("info") => Cmd::Info,
            Some("stale") => Cmd::ClearStale,
            _ => return Err(format!("unknown command: {line}")),
        })
    }
}

/// One step of an [`MpCase`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MpOp {
    /// Run a command in a worker.
    Run(u8, Cmd),
    /// `SIGKILL` a worker and reap it.
    Kill(u8),
    /// Start a dead worker again and open the environment in it.
    Respawn(u8, MapChoice),
}

/// A generated multi-process sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MpCase {
    /// Number of workers, 2 or 3; each starts with the environment open,
    /// writable, with its map.
    pub workers: Vec<MapChoice>,
    /// The steps.
    pub ops: Vec<MpOp>,
}

impl<'a> Arbitrary<'a> for MpCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let n = u.int_in_range(2u8..=3)?;
        let mut workers = Vec::new();
        for _ in 0..n {
            workers.push(u.arbitrary()?);
        }
        let slot = |u: &mut Unstructured<'_>| u.int_in_range(0..=MP_SLOTS - 1);
        let mut bulks = 0u8;
        let mut ops = Vec::new();
        while ops.len() < 40 && !u.is_empty() {
            let w = u.int_in_range(0..=n - 1)?;
            let cmd = match u.int_in_range(0u8..=23)? {
                0..=4 => Cmd::BeginRead(slot(u)?),
                5 | 6 => Cmd::EndRead(slot(u)?),
                7 | 8 => Cmd::Scan(slot(u)?),
                9 => Cmd::Get(slot(u)?, u.int_in_range(0..=SMALL_KEYS - 1)?),
                10..=12 => Cmd::Write {
                    start: u.int_in_range(0..=SMALL_KEYS - 1)?,
                    n: u.int_in_range(1u8..=32)?,
                    vlen: u.arbitrary()?,
                    tag: u.arbitrary()?,
                },
                13 => Cmd::Delete {
                    start: u.int_in_range(0..=SMALL_KEYS - 1)?,
                    n: u.int_in_range(1u8..=32)?,
                },
                14 if bulks < MAX_BULKS => {
                    bulks += 1;
                    Cmd::Bulk { id: bulks }
                }
                14 | 15 => Cmd::Resize(if u.arbitrary()? {
                    None
                } else {
                    Some(u.arbitrary()?)
                }),
                16 => Cmd::Open {
                    map: u.arbitrary()?,
                    read_only: u.ratio(1u8, 3u8)?,
                },
                17 => Cmd::Close,
                18 | 19 => Cmd::Info,
                20 => Cmd::ClearStale,
                21 => {
                    ops.push(MpOp::Kill(w));
                    continue;
                }
                _ => {
                    ops.push(MpOp::Respawn(w, u.arbitrary()?));
                    continue;
                }
            };
            ops.push(MpOp::Run(w, cmd));
        }
        Ok(MpCase { workers, ops })
    }
}

/// Decode an [`MpCase`] from bytes.
///
/// # Errors
///
/// When the bytes run out before the workers are chosen.
pub fn decode_mp_case(data: &[u8]) -> arbitrary::Result<MpCase> {
    MpCase::arbitrary(&mut Unstructured::new(data))
}

/// Which engine a worker runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MpEngine {
    /// heed over the LMDB fork.
    Lmdb,
    /// `heed-zerodb`.
    Zerodb,
}

impl MpEngine {
    fn arg(self) -> &'static str {
        match self {
            MpEngine::Lmdb => "lmdb",
            MpEngine::Zerodb => "zerodb",
        }
    }
}

/// How long a worker may take to answer one command.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);

struct Worker {
    guard: ChildGuard,
    stdin: std::process::ChildStdin,
    lines: mpsc::Receiver<String>,
}

impl Worker {
    fn spawn(exe: &Path, engine: MpEngine, dir: &Path) -> Worker {
        let child = Command::new(exe)
            .arg(engine.arg())
            .arg(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mp-worker");
        let mut guard = ChildGuard(child);
        let stdin = guard.0.stdin.take().expect("worker stdin");
        let stdout = guard.0.stdout.take().expect("worker stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Worker {
            guard,
            stdin,
            lines,
        }
    }

    fn ask(&mut self, cmd: &Cmd) -> String {
        if writeln!(self.stdin, "{}", cmd.encode()).is_err() || self.stdin.flush().is_err() {
            return "worker gone".into();
        }
        match self.lines.recv_timeout(ANSWER_TIMEOUT) {
            Ok(line) => line,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = self.guard.0.kill();
                "timeout".into()
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => "worker died".into(),
        }
    }

    fn kill(mut self) {
        let _ = self.guard.0.kill();
        let _ = self.guard.0.wait();
    }
}

/// Run `case` with every worker on `engine`; return the transcript.
#[must_use]
pub fn transcript(case: &MpCase, engine: MpEngine, exe: &Path) -> Vec<(String, String)> {
    let dir = TempDir::new().expect("temp dir");
    let mut out = Vec::new();
    let open = |m: MapChoice| Cmd::Open {
        map: m,
        read_only: false,
    };
    let mut workers: Vec<Option<Worker>> = Vec::new();
    for (i, m) in case.workers.iter().enumerate() {
        let mut w = Worker::spawn(exe, engine, dir.path());
        let r = w.ask(&open(*m));
        out.push((format!("start {i} {m:?}"), r));
        workers.push(Some(w));
    }
    for op in &case.ops {
        let (what, r) = match op {
            MpOp::Run(i, cmd) => {
                let r = match workers[usize::from(*i)].as_mut() {
                    Some(w) => w.ask(cmd),
                    None => "dead".into(),
                };
                (format!("{i}: {}", cmd.encode()), r)
            }
            MpOp::Kill(i) => {
                let r = match workers[usize::from(*i)].take() {
                    Some(w) => {
                        w.kill();
                        "killed".into()
                    }
                    None => "dead".into(),
                };
                (format!("kill {i}"), r)
            }
            MpOp::Respawn(i, m) => {
                let slot = &mut workers[usize::from(*i)];
                let r = if slot.is_some() {
                    "alive".to_string()
                } else {
                    let mut w = Worker::spawn(exe, engine, dir.path());
                    let r = w.ask(&open(*m));
                    *slot = Some(w);
                    r
                };
                (format!("respawn {i} {m:?}"), r)
            }
        };
        out.push((what, r));
    }
    for w in workers.into_iter().flatten() {
        w.kill();
    }
    out
}

/// Run `case` on LMDB and on ZeroDB, with `exe` the `mp-worker` binary, and
/// return the first difference. The comparison stops at the first `MapFull`
/// either engine reports (see the module docs).
///
/// # Errors
///
/// The first step where the transcripts differ.
pub fn run_mp(case: &MpCase, exe: &Path) -> Result<(), Box<ScenarioDivergence>> {
    let mut a = transcript(case, MpEngine::Lmdb, exe);
    let mut b = transcript(case, MpEngine::Zerodb, exe);
    let full = |t: &[(String, String)]| t.iter().position(|e| e.1.contains("MapFull"));
    if let Some(cut) = [full(&a), full(&b)].into_iter().flatten().min() {
        a.truncate(cut);
        b.truncate(cut);
    }
    crate::scenario::compare(&a, &b)
}

/// Shrink a diverging case: drop steps one at a time while it still
/// diverges at the same kind of step.
#[must_use]
pub fn minimize_mp(case: &MpCase, exe: &Path) -> MpCase {
    let kind = |d: &ScenarioDivergence| {
        let w = d.what.split_whitespace().collect::<Vec<_>>();
        w.get(1).or(w.first()).map(|s| (*s).to_string())
    };
    let Err(d) = run_mp(case, exe) else {
        return case.clone();
    };
    let k = kind(&d);
    let mut best = case.clone();
    loop {
        let mut shrunk = false;
        for i in (0..best.ops.len()).rev() {
            let mut cand = best.clone();
            cand.ops.remove(i);
            if matches!(run_mp(&cand, exe), Err(d) if kind(&d) == k) {
                best = cand;
                shrunk = true;
            }
        }
        if !shrunk {
            return best;
        }
    }
}

impl fmt::Display for MpCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "workers: {:?}", self.workers)?;
        for op in &self.ops {
            match op {
                MpOp::Run(i, c) => writeln!(f, "  {i}: {}", c.encode())?,
                MpOp::Kill(i) => writeln!(f, "  kill {i}")?,
                MpOp::Respawn(i, m) => writeln!(f, "  respawn {i} {m:?}")?,
            }
        }
        Ok(())
    }
}

/// The worker loop: run [`Cmd`]s from standard input on `engine` in `dir`,
/// one answer line each, until standard input closes.
pub fn serve(engine: MpEngine, dir: &Path) {
    match engine {
        MpEngine::Lmdb => worker::lmdb::serve(dir),
        MpEngine::Zerodb => worker::zdb::serve(dir),
    }
}

/// A map size as a transcript string. A map set smaller than the data is
/// raised to the pages in use, a size that depends on each engine's page
/// allocation; both engines must raise it, but only the fact is compared.
/// `asked` is the requested size, or 0 when none was.
fn map_str(size: usize, asked: usize) -> String {
    match size {
        SMALL_MAP => "small".into(),
        BIG_MAP => "big".into(),
        s if asked != 0 && s > asked => "raised".into(),
        s => format!("other {s}"),
    }
}

/// The value bytes of small key `k` written with `tag`.
fn small_value(k: u16, vlen: u8, tag: u8) -> Vec<u8> {
    (0..usize::from(vlen))
        .map(|i| tag.wrapping_add(k as u8).wrapping_add(i as u8))
        .collect()
}

mod worker {
    macro_rules! mp_worker {
        ($mod:ident, $heed:ident) => {
            pub(super) mod $mod {
                use std::io::{BufRead, Write};
                use std::path::Path;

                use $heed::types::Bytes;
                use $heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, WithoutTls};

                use crate::mp::{
                    map_str, small_value, Cmd, MapChoice, BULK_BATCH, BULK_BYTES, MP_MAX_READERS,
                    MP_SLOTS,
                };

                fn e(err: $heed::Error) -> String {
                    match err {
                        $heed::Error::Mdb(m) => format!("Err(mdb:{m:?})"),
                        $heed::Error::Io(io) => format!("Err(io:{:?})", io.kind()),
                        $heed::Error::Encoding(_) => "Err(encoding)".into(),
                        $heed::Error::Decoding(_) => "Err(decoding)".into(),
                        $heed::Error::EnvAlreadyOpened => "Err(env-already-opened)".into(),
                    }
                }

                fn open(
                    dir: &Path,
                    map: MapChoice,
                    read_only: bool,
                ) -> $heed::Result<Env<WithoutTls>> {
                    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
                    opts.map_size(map.bytes()).max_readers(MP_MAX_READERS);
                    let mut f = EnvFlags::NO_SYNC;
                    if read_only {
                        f |= EnvFlags::READ_ONLY;
                    }
                    // SAFETY: only the workers of one case open this
                    // directory, each with a lock file, so every process
                    // coordinates through it; no flag here makes a
                    // process write outside its transactions.
                    unsafe {
                        opts.flags(f);
                        opts.open(dir)
                    }
                }

                fn main_db(env: &Env<WithoutTls>) -> $heed::Result<Option<Database<Bytes, Bytes>>> {
                    let r = env.read_txn()?;
                    let db = env.open_database::<Bytes, Bytes>(&r, None)?;
                    r.commit()?;
                    Ok(db)
                }

                fn fnv(h: &mut u64, bytes: &[u8]) {
                    for b in bytes {
                        *h ^= u64::from(*b);
                        *h = h.wrapping_mul(0x0100_0000_01b3);
                    }
                }

                fn write_txn(
                    env: &Env<WithoutTls>,
                    f: impl FnOnce(&mut $heed::RwTxn<'_>, Database<Bytes, Bytes>) -> $heed::Result<()>,
                ) -> String {
                    let mut w = match env.write_txn() {
                        Ok(w) => w,
                        Err(err) => return format!("begin: {}", e(err)),
                    };
                    let db = match env.create_database::<Bytes, Bytes>(&mut w, None) {
                        Ok(db) => db,
                        Err(err) => return format!("db: {}", e(err)),
                    };
                    match f(&mut w, db) {
                        Ok(()) => match w.commit() {
                            Ok(()) => "Ok".into(),
                            Err(err) => format!("commit: {}", e(err)),
                        },
                        Err(err) => e(err),
                    }
                }

                /// Run commands against one open environment until a
                /// command closes or reopens it, which is returned.
                fn session(
                    env: &Env<WithoutTls>,
                    dir: &Path,
                    mut asked: usize,
                    lines: &mut impl Iterator<Item = String>,
                    out: &mut impl Write,
                ) -> Option<Cmd> {
                    let mut readers: Vec<Option<RoTxn<'_, WithoutTls>>> =
                        (0..MP_SLOTS).map(|_| None).collect();
                    for line in lines {
                        let cmd = match Cmd::decode(&line) {
                            Ok(c) => c,
                            Err(err) => panic!("{err}"),
                        };
                        let r = match &cmd {
                            Cmd::Open { .. } | Cmd::Close => return Some(cmd),
                            Cmd::BeginRead(s) => {
                                let slot = &mut readers[usize::from(*s)];
                                if slot.is_some() {
                                    "already open".into()
                                } else {
                                    match env.read_txn() {
                                        Ok(t) => {
                                            *slot = Some(t);
                                            "Ok".into()
                                        }
                                        Err(err) => e(err),
                                    }
                                }
                            }
                            Cmd::EndRead(s) => match readers[usize::from(*s)].take() {
                                Some(t) => match t.commit() {
                                    Ok(()) => "Ok".into(),
                                    Err(err) => e(err),
                                },
                                None => "no reader".into(),
                            },
                            Cmd::Scan(s) => match readers[usize::from(*s)].as_ref() {
                                None => "no reader".into(),
                                Some(t) => match main_db_in(env, t) {
                                    Err(err) => e(err),
                                    Ok(None) => "no db".into(),
                                    Ok(Some(db)) => scan(&db, t),
                                },
                            },
                            Cmd::Get(s, k) => match readers[usize::from(*s)].as_ref() {
                                None => "no reader".into(),
                                Some(t) => match main_db_in(env, t) {
                                    Err(err) => e(err),
                                    Ok(None) => "no db".into(),
                                    Ok(Some(db)) => match db.get(t, format!("s{k:03}").as_bytes()) {
                                        Ok(v) => format!("Ok({:?})", v.map(|v| v.len())),
                                        Err(err) => e(err),
                                    },
                                },
                            },
                            Cmd::Write { start, n, vlen, tag } => write_txn(env, |w, db| {
                                for i in 0..u16::from(*n) {
                                    let k = (start + i) % crate::mp::SMALL_KEYS;
                                    db.put(w, format!("s{k:03}").as_bytes(), &small_value(k, *vlen, *tag))?;
                                }
                                Ok(())
                            }),
                            Cmd::Delete { start, n } => write_txn(env, |w, db| {
                                for i in 0..u16::from(*n) {
                                    let k = (start + i) % crate::mp::SMALL_KEYS;
                                    db.delete(w, format!("s{k:03}").as_bytes())?;
                                }
                                Ok(())
                            }),
                            // A bulk into a map below the big one would fill
                            // it at an allocation-dependent point; such a
                            // worker skips it, on both engines alike.
                            Cmd::Bulk { .. } if env.info().map_size < crate::mp::BIG_MAP => {
                                "skipped: map below big".into()
                            }
                            Cmd::Bulk { id } => {
                                let value = vec![*id; 1000];
                                let puts = BULK_BYTES / 1000;
                                let mut r = String::from("Ok");
                                for batch in 0..puts.div_ceil(BULK_BATCH) {
                                    let res = write_txn(env, |w, db| {
                                        for j in 0..BULK_BATCH {
                                            let k = format!("b{id}:{:06}", batch * BULK_BATCH + j);
                                            db.put(w, k.as_bytes(), &value)?;
                                        }
                                        Ok(())
                                    });
                                    if res != "Ok" {
                                        r = res;
                                        break;
                                    }
                                }
                                r
                            }
                            Cmd::Resize(m) => {
                                if readers.iter().any(Option::is_some) {
                                    "busy".into()
                                } else {
                                    let size = m.map_or(0, MapChoice::bytes);
                                    // SAFETY: this process holds no
                                    // transaction of `env` (checked above;
                                    // write transactions never outlive a
                                    // command).
                                    match unsafe { env.resize(size) } {
                                        Ok(()) => {
                                            asked = size;
                                            format!("Ok(map {})", map_str(env.info().map_size, size))
                                        }
                                        Err(err) => e(err),
                                    }
                                }
                            }
                            Cmd::Info => {
                                let i = env.info();
                                // While the data file is larger than this
                                // process's map, ZeroDB reports the txnid
                                // of its own last snapshot where LMDB
                                // reports the newest (D-029).
                                let file = std::fs::metadata(dir.join("data.mdb"))
                                    .map_or(0, |m| m.len());
                                let txn = if file > i.map_size as u64 {
                                    "beyond-map".to_string()
                                } else {
                                    i.last_txn_id.to_string()
                                };
                                format!(
                                    "map {} txn {txn} readers {}",
                                    map_str(i.map_size, asked),
                                    i.number_of_readers
                                )
                            }
                            Cmd::ClearStale => match env.clear_stale_readers() {
                                Ok(n) => format!("Ok({n})"),
                                Err(err) => e(err),
                            },
                        };
                        writeln!(out, "{r}").unwrap();
                        out.flush().unwrap();
                    }
                    None
                }

                fn main_db_in(
                    env: &Env<WithoutTls>,
                    t: &RoTxn<'_, WithoutTls>,
                ) -> $heed::Result<Option<Database<Bytes, Bytes>>> {
                    env.open_database::<Bytes, Bytes>(t, None)
                }

                fn scan(db: &Database<Bytes, Bytes>, t: &RoTxn<'_, WithoutTls>) -> String {
                    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
                    let mut n = 0u64;
                    let it = match db.iter(t) {
                        Ok(it) => it,
                        Err(err) => return e(err),
                    };
                    for kv in it {
                        match kv {
                            Ok((k, v)) => {
                                n += 1;
                                fnv(&mut h, k);
                                fnv(&mut h, v);
                            }
                            Err(err) => return e(err),
                        }
                    }
                    format!("{n} entries {h:016x}")
                }

                pub(crate) fn serve(dir: &Path) {
                    let stdin = std::io::stdin();
                    let mut lines = stdin.lock().lines().map_while(Result::ok);
                    let mut out = std::io::stdout();
                    let mut next: Option<Cmd> = None;
                    loop {
                        let cmd = match next.take() {
                            Some(c) => c,
                            None => match lines.next() {
                                Some(l) => Cmd::decode(&l).unwrap_or_else(|err| panic!("{err}")),
                                None => return,
                            },
                        };
                        match cmd {
                            Cmd::Open { map, read_only } => match open(dir, map, read_only) {
                                Err(err) => {
                                    writeln!(out, "{}", e(err)).unwrap();
                                    out.flush().unwrap();
                                }
                                Ok(env) => {
                                    let r = match main_db(&env) {
                                        Ok(_) => format!(
                                            "Ok(map {})",
                                            map_str(env.info().map_size, map.bytes())
                                        ),
                                        Err(err) => format!("opened, {}", e(err)),
                                    };
                                    writeln!(out, "{r}").unwrap();
                                    out.flush().unwrap();
                                    next = session(&env, dir, map.bytes(), &mut lines, &mut out);
                                    env.prepare_for_closing().wait();
                                    if matches!(next, Some(Cmd::Close)) {
                                        next = None;
                                        writeln!(out, "closed").unwrap();
                                        out.flush().unwrap();
                                    } else if next.is_none() {
                                        return;
                                    }
                                }
                            },
                            Cmd::Close => {
                                writeln!(out, "not open").unwrap();
                                out.flush().unwrap();
                            }
                            _ => {
                                writeln!(out, "not open").unwrap();
                                out.flush().unwrap();
                            }
                        }
                    }
                }
            }
        };
    }

    mp_worker!(lmdb, heed);
    mp_worker!(zdb, heed_zerodb);
}
