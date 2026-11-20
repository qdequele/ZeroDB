//! Generated scenarios the linear [`Op`](crate::Op) model cannot express.
//!
//! The main op model drives one transaction at a time against one database
//! with memcmp ordering. A [`Scenario`] covers the rest of the heed surface
//! that consumers rely on, each kind with its own small operation language:
//!
//! * [`OrderedCase`]: one named database opened with a key comparator
//!   ([`CmpKind`]: memcmp, reverse, native-endian integer, length then bytes),
//!   driven with puts, `APPEND`, deletes, `delete_range`, range scans and every
//!   positioning call. On LMDB, heed's `delete_range` is a cursor walk that
//!   deletes each entry of the range; ZeroDB deletes the range natively.
//! * [`SnapshotCase`]: several read transactions opened at different
//!   snapshots, held while a write transaction mutates and commits, each
//!   read compared, plus the reader-slot limit (`ReadersFull`).
//! * [`ReopenCase`]: close and reopen with `READ_ONLY`, `PREV_SNAPSHOT`,
//!   `NO_LOCK` and `WRITE_MAP` in any combination, with or without a map
//!   size, `resize`, and environment info.
//! * [`MapFullCase`]: a small map filled until it returns `MapFull`. Where
//!   each engine fills depends on its page allocation, so this compares a
//!   weaker property: every failure is a clean `MapFull`, the failed
//!   transaction cannot commit, the committed state survives intact, the
//!   environment grows with `resize` and accepts the failed batch, and both
//!   engines end with the same contents.
//!
//! Both sides go through the heed API: LMDB through heed 0.22.1, ZeroDB
//! through the `heed-zerodb` adapter, expanded from one source by the macro in
//! `engine.rs`, so both engines run exactly the same calls. Each run produces
//! a transcript of normalized results, one entry per operation, and the two
//! transcripts must be equal.
//!
//! Multi-process sequences live in [`crate::mp`]: they need child processes,
//! so they run from a test, not from the fuzzer.

use std::fmt;
use std::ops::Bound;

use arbitrary::{Arbitrary, Unstructured};

mod engine;

/// A key comparator, set on the scenario's database at creation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpKind {
    /// memcmp, LMDB's default.
    Default,
    /// The exact reverse of memcmp ([`ReverseComparator`]).
    Reverse,
    /// heed's `IntegerComparator`: native-endian unsigned integers. heed sets
    /// `MDB_INTEGERKEY` on LMDB, whose contract is that every key of the
    /// database has the same size, so a case uses one width for all its keys.
    Integer {
        /// 8-byte keys when true, 4-byte keys otherwise.
        wide: bool,
    },
    /// Shorter keys first, then memcmp ([`LenThenBytesComparator`]).
    LenThenBytes,
}

impl<'a> Arbitrary<'a> for CmpKind {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0u8..=4)? {
            0 => CmpKind::Default,
            1 => CmpKind::Reverse,
            2 => CmpKind::Integer { wide: false },
            3 => CmpKind::Integer { wide: true },
            _ => CmpKind::LenThenBytes,
        })
    }
}

// heed and `heed-zerodb` both re-export `heed_traits::Comparator` from the
// same `heed-traits 0.20.0`, so one implementation serves both engines.

/// The reverse of memcmp: `b.cmp(a)`.
pub enum ReverseComparator {}

impl heed::Comparator for ReverseComparator {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
}

/// Shorter keys sort first; keys of one length sort by memcmp.
pub enum LenThenBytesComparator {}

impl heed::Comparator for LenThenBytesComparator {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.len().cmp(&b.len()).then_with(|| a.cmp(b))
    }
}

/// A scenario key. Generated keys come from a small alphabet so that puts,
/// deletes and seeks collide often.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SKey(pub Vec<u8>);

/// A scenario value: short, or occasionally a few overflow pages long.
#[derive(Clone, PartialEq, Eq)]
pub struct SVal(pub Vec<u8>);

fn hex(bytes: &[u8]) -> String {
    let mut s = String::from("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

impl fmt::Debug for SKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex(&self.0))
    }
}

impl fmt::Debug for SVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() > 32 {
            write!(f, "<{} bytes, {}..>", self.0.len(), hex(&self.0[..8]))
        } else {
            write!(f, "{}", hex(&self.0))
        }
    }
}

/// Bytes keys are drawn from: both ends of the byte range and a few in
/// between, so memcmp, reverse and length orders all get ties and neighbors.
const ALPHABET: [u8; 8] = [0x00, 0x01, 0x02, 0x41, 0x7f, 0x80, 0xfe, 0xff];

/// The keys of one case: their shape follows the case's comparator.
#[derive(Clone, Copy, Debug)]
pub struct KeyShape {
    /// Every key has exactly this many bytes (the integer comparator).
    fixed: Option<usize>,
}

impl KeyShape {
    /// The key shape for a comparator.
    #[must_use]
    pub fn for_cmp(cmp: CmpKind) -> KeyShape {
        match cmp {
            CmpKind::Integer { wide } => KeyShape {
                fixed: Some(if wide { 8 } else { 4 }),
            },
            _ => KeyShape { fixed: None },
        }
    }

    fn key(self, u: &mut Unstructured<'_>) -> arbitrary::Result<SKey> {
        match self.fixed {
            // Small integers, so keys collide and fills interleave with them.
            Some(_) => Ok(self.index_key(u.int_in_range(0u32..=300)?)),
            None => {
                // About 1 in 48 empty, which both engines must refuse.
                let len = if u.ratio(1u8, 48u8)? {
                    0
                } else {
                    u.int_in_range(1u8..=4)? as usize
                };
                let mut k = Vec::with_capacity(len);
                for _ in 0..len {
                    k.push(*u.choose(&ALPHABET)?);
                }
                Ok(SKey(k))
            }
        }
    }

    /// The `i`-th key of a deterministic fill. Integer keys are `i` in native
    /// byte order; byte keys are `'f'` and `i` big-endian, so fills sort in
    /// index order under memcmp and in reverse under the reverse comparator.
    #[must_use]
    pub fn index_key(self, i: u32) -> SKey {
        match self.fixed {
            Some(8) => SKey(u64::from(i).to_ne_bytes().to_vec()),
            Some(_) => SKey(i.to_ne_bytes().to_vec()),
            None => {
                let mut k = vec![b'f'];
                k.extend_from_slice(&(i as u16).to_be_bytes());
                SKey(k)
            }
        }
    }

    fn bound(self, u: &mut Unstructured<'_>) -> arbitrary::Result<Bound<SKey>> {
        Ok(match u.int_in_range(0u8..=4)? {
            0 => Bound::Unbounded,
            1 | 2 => Bound::Included(self.key(u)?),
            _ => Bound::Excluded(self.key(u)?),
        })
    }
}

fn value(u: &mut Unstructured<'_>) -> arbitrary::Result<SVal> {
    let len = if u.ratio(1u8, 24u8)? {
        u.int_in_range(2_000u16..=9_000)? as usize
    } else {
        u.int_in_range(0u8..=16)? as usize
    };
    let seed: u8 = u.arbitrary()?;
    Ok(SVal((0..len).map(|i| seed.wrapping_add(i as u8)).collect()))
}

/// The value of the `i`-th entry of a fill whose values are `vlen` bytes.
#[must_use]
pub fn fill_value(i: u32, vlen: u16) -> Vec<u8> {
    vec![(i % 251) as u8; usize::from(vlen)]
}

/// A write in a scenario's write transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOp {
    /// `put` (overwrite allowed).
    Put(SKey, SVal),
    /// `put_with_flags(NO_OVERWRITE)`.
    PutNoOverwrite(SKey, SVal),
    /// `put_with_flags(APPEND)`: the key must sort after the last key, in the
    /// database's comparator order.
    Append(SKey, SVal),
    /// `delete`.
    Del(SKey),
    /// `delete_range`.
    DeleteRange(Bound<SKey>, Bound<SKey>),
    /// Put `n` deterministic entries, from index `start`, so trees reach
    /// several levels and ranges span leaves.
    Fill {
        /// First fill index.
        start: u16,
        /// Number of entries.
        n: u8,
        /// Value length of every entry.
        vlen: u16,
    },
    /// `clear`.
    Clear,
}

impl WriteOp {
    /// A write whose fill is small: at most 40 entries of 40 bytes.
    fn gen_small(u: &mut Unstructured<'_>, shape: KeyShape) -> arbitrary::Result<WriteOp> {
        Ok(match WriteOp::gen(u, shape)? {
            WriteOp::Fill { start, n, vlen } => WriteOp::Fill {
                start,
                n: n % 41,
                vlen: vlen % 41,
            },
            w => w,
        })
    }

    fn gen(u: &mut Unstructured<'_>, shape: KeyShape) -> arbitrary::Result<WriteOp> {
        Ok(match u.int_in_range(0u8..=13)? {
            0..=3 => WriteOp::Put(shape.key(u)?, value(u)?),
            4 => WriteOp::PutNoOverwrite(shape.key(u)?, value(u)?),
            5 => WriteOp::Append(shape.key(u)?, value(u)?),
            6 | 7 => WriteOp::Del(shape.key(u)?),
            8..=10 => WriteOp::DeleteRange(shape.bound(u)?, shape.bound(u)?),
            11 | 12 => WriteOp::Fill {
                start: u.int_in_range(0u16..=400)?,
                n: u.arbitrary()?,
                vlen: if u.ratio(1u8, 8u8)? {
                    u.int_in_range(1_000u16..=3_000)?
                } else {
                    u.int_in_range(0u16..=40)?
                },
            },
            _ => WriteOp::Clear,
        })
    }
}

/// A read, served by some transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadOp {
    /// `get`.
    Get(SKey),
    /// `len`.
    Len,
    /// Forward scan.
    Iter,
    /// Reverse scan.
    RevIter,
    /// `range`.
    Range(Bound<SKey>, Bound<SKey>),
    /// `rev_range`.
    RevRange(Bound<SKey>, Bound<SKey>),
    /// `first`.
    First,
    /// `last`.
    Last,
    /// `get_greater_than`.
    Gt(SKey),
    /// `get_greater_than_or_equal_to`.
    Ge(SKey),
    /// `get_lower_than`.
    Lt(SKey),
    /// `get_lower_than_or_equal_to`.
    Le(SKey),
}

impl ReadOp {
    fn gen(u: &mut Unstructured<'_>, shape: KeyShape) -> arbitrary::Result<ReadOp> {
        Ok(match u.int_in_range(0u8..=11)? {
            0 => ReadOp::Get(shape.key(u)?),
            1 => ReadOp::Len,
            2 => ReadOp::Iter,
            3 => ReadOp::RevIter,
            4 => ReadOp::Range(shape.bound(u)?, shape.bound(u)?),
            5 => ReadOp::RevRange(shape.bound(u)?, shape.bound(u)?),
            6 => ReadOp::First,
            7 => ReadOp::Last,
            8 => ReadOp::Gt(shape.key(u)?),
            9 => ReadOp::Ge(shape.key(u)?),
            10 => ReadOp::Lt(shape.key(u)?),
            _ => ReadOp::Le(shape.key(u)?),
        })
    }
}

/// One step of an [`OrderedCase`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderedOp {
    /// A write in the current write transaction (begun on demand).
    Write(WriteOp),
    /// A read in the current write transaction.
    Read(ReadOp),
    /// A read in a fresh read transaction: the committed state.
    ReadCommitted(ReadOp),
    /// Commit the write transaction.
    Commit,
    /// Abort the write transaction.
    Abort,
}

/// Puts, deletes, `delete_range` and reads on one database opened with a
/// key comparator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderedCase {
    /// The database's comparator.
    pub cmp: CmpKind,
    /// The steps.
    pub ops: Vec<OrderedOp>,
}

impl<'a> Arbitrary<'a> for OrderedCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let cmp: CmpKind = u.arbitrary()?;
        let shape = KeyShape::for_cmp(cmp);
        let mut ops = Vec::new();
        while ops.len() < 64 && !u.is_empty() {
            ops.push(match u.int_in_range(0u8..=19)? {
                0..=8 => OrderedOp::Write(WriteOp::gen(u, shape)?),
                9..=14 => OrderedOp::Read(ReadOp::gen(u, shape)?),
                15 | 16 => OrderedOp::ReadCommitted(ReadOp::gen(u, shape)?),
                17 | 18 => OrderedOp::Commit,
                _ => OrderedOp::Abort,
            });
        }
        Ok(OrderedCase { cmp, ops })
    }
}

/// Reader slots a [`SnapshotCase`] addresses; more than
/// [`SNAPSHOT_MAX_READERS`] so `ReadersFull` is reachable.
pub const SNAPSHOT_SLOTS: u8 = 6;
/// `max_readers` of a [`SnapshotCase`] environment.
pub const SNAPSHOT_MAX_READERS: u32 = 4;

/// One step of a [`SnapshotCase`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotOp {
    /// A write in the write transaction (begun on demand).
    Write(WriteOp),
    /// A read in the write transaction.
    WriterRead(ReadOp),
    /// Commit the write transaction.
    Commit,
    /// Abort the write transaction.
    Abort,
    /// Begin a read transaction into a slot (a no-op if the slot holds one).
    OpenReader(u8),
    /// End the read transaction of a slot.
    CloseReader(u8),
    /// Read through the read transaction of a slot.
    Read(u8, ReadOp),
    /// `Env::info().number_of_readers`.
    ReaderCount,
    /// `Env::clear_stale_readers`.
    ClearStale,
}

/// Several live read transactions at different snapshots while a writer
/// mutates and commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotCase {
    /// The steps.
    pub ops: Vec<SnapshotOp>,
}

impl<'a> Arbitrary<'a> for SnapshotCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let shape = KeyShape::for_cmp(CmpKind::Default);
        let slot = |u: &mut Unstructured<'_>| u.int_in_range(0..=SNAPSHOT_SLOTS - 1);
        let mut ops = Vec::new();
        while ops.len() < 64 && !u.is_empty() {
            ops.push(match u.int_in_range(0u8..=19)? {
                0..=5 => SnapshotOp::Write(WriteOp::gen(u, shape)?),
                6 => SnapshotOp::WriterRead(ReadOp::gen(u, shape)?),
                7 | 8 => SnapshotOp::Commit,
                9 => SnapshotOp::Abort,
                10..=12 => SnapshotOp::OpenReader(slot(u)?),
                13 => SnapshotOp::CloseReader(slot(u)?),
                14..=17 => SnapshotOp::Read(slot(u)?, ReadOp::gen(u, shape)?),
                18 => SnapshotOp::ReaderCount,
                _ => SnapshotOp::ClearStale,
            });
        }
        Ok(SnapshotCase { ops })
    }
}

/// Environment flags for a reopen.
#[derive(Arbitrary, Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenFlags {
    /// `READ_ONLY`.
    pub read_only: bool,
    /// `PREV_SNAPSHOT`.
    pub prev_snapshot: bool,
    /// `NO_LOCK`.
    pub no_lock: bool,
    /// `WRITE_MAP`.
    pub write_map: bool,
}

/// One step of a [`ReopenCase`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReopenOp {
    /// Begin a write transaction, apply the writes, commit.
    Commit(Vec<WriteOp>),
    /// Close the environment, if open, and open it again. `map_units` sets
    /// the map size to that many 64 KiB units, or leaves it unset (the
    /// persisted size) for `None`.
    Reopen {
        /// The flags of the new open.
        flags: OpenFlags,
        /// The map size, in 64 KiB units, if set.
        map_units: Option<u8>,
    },
    /// Close the environment.
    Close,
    /// A read in a fresh read transaction.
    Read(ReadOp),
    /// `Env::resize` to that many 64 KiB units, with no transaction open.
    Resize(u8),
    /// `Env::info()`: map size, last transaction id, reader counts.
    Info,
    /// `Env::flags()`.
    Flags,
}

/// Map size of a fresh [`ReopenCase`] environment: 4 MiB.
pub const REOPEN_BASE_MAP: usize = 4 << 20;

/// The smallest map a [`ReopenCase`] sets, in 64 KiB units: 1 MiB, well
/// above what its bounded writes use. A map set below the data is raised to
/// the pages in use, and how many pages are in use depends on each engine's
/// page allocation, as the point where a map fills does; so reopen cases
/// keep their maps above the data, and the comparison stops at a `MapFull`.
pub const REOPEN_MIN_MAP_UNITS: u8 = 16;

/// Close and reopen sequences with `READ_ONLY`, `PREV_SNAPSHOT`, `NO_LOCK` and
/// `WRITE_MAP`, map sizes and `resize`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReopenCase {
    /// The steps. The environment starts open, writable, with
    /// [`REOPEN_BASE_MAP`].
    pub ops: Vec<ReopenOp>,
}

impl<'a> Arbitrary<'a> for ReopenCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let shape = KeyShape::for_cmp(CmpKind::Default);
        let mut ops = Vec::new();
        while ops.len() < 32 && !u.is_empty() {
            ops.push(match u.int_in_range(0u8..=11)? {
                0..=3 => {
                    let n = u.int_in_range(1u8..=6)?;
                    let mut w = Vec::new();
                    for _ in 0..n {
                        w.push(WriteOp::gen_small(u, shape)?);
                    }
                    ReopenOp::Commit(w)
                }
                4..=6 => ReopenOp::Reopen {
                    flags: u.arbitrary()?,
                    map_units: if u.arbitrary()? {
                        Some(u.int_in_range(REOPEN_MIN_MAP_UNITS..=255)?)
                    } else {
                        None
                    },
                },
                7 => ReopenOp::Close,
                8 | 9 => ReopenOp::Read(ReadOp::gen(u, shape)?),
                10 => ReopenOp::Resize(u.int_in_range(REOPEN_MIN_MAP_UNITS..=255)?),
                _ => {
                    if u.arbitrary()? {
                        ReopenOp::Info
                    } else {
                        ReopenOp::Flags
                    }
                }
            });
        }
        Ok(ReopenCase { ops })
    }
}

/// Batches written into a small map until it is full.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapFullCase {
    /// Initial map size, in 64 KiB units.
    pub map_units: u8,
    /// `WRITE_MAP`.
    pub write_map: bool,
    /// Each batch is one write transaction: `(key index, value length)`
    /// puts, and `None` for a delete of the key.
    pub batches: Vec<Vec<(u16, Option<u16>)>>,
}

impl<'a> Arbitrary<'a> for MapFullCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let map_units = u.int_in_range(4u8..=24)?;
        let write_map = u.arbitrary()?;
        let mut batches = Vec::new();
        while batches.len() < 24 && !u.is_empty() {
            let n = u.int_in_range(1u8..=24)?;
            let mut b = Vec::new();
            for _ in 0..n {
                let key = u.int_in_range(0u16..=999)?;
                let v = if u.ratio(1u8, 6u8)? {
                    None
                } else if u.ratio(1u8, 3u8)? {
                    Some(u.int_in_range(2_000u16..=20_000)?)
                } else {
                    Some(u.int_in_range(1u16..=600)?)
                };
                b.push((key, v));
            }
            batches.push(b);
        }
        Ok(MapFullCase {
            map_units,
            write_map,
            batches,
        })
    }
}

/// One generated scenario.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scenario {
    /// Key comparators, `delete_range` and every positioning call.
    Ordered(OrderedCase),
    /// Live readers at different snapshots while a writer commits.
    Snapshots(SnapshotCase),
    /// Read-only and reopen sequences.
    Reopen(ReopenCase),
    /// The `MapFull` property.
    MapFull(MapFullCase),
}

impl<'a> Arbitrary<'a> for Scenario {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0u8..=9)? {
            0..=3 => Scenario::Ordered(u.arbitrary()?),
            4..=6 => Scenario::Snapshots(u.arbitrary()?),
            7 | 8 => Scenario::Reopen(u.arbitrary()?),
            _ => Scenario::MapFull(u.arbitrary()?),
        })
    }
}

/// Decode a [`Scenario`] from fuzz bytes.
///
/// # Errors
///
/// When the bytes run out before a scenario kind is chosen.
pub fn decode_scenario(data: &[u8]) -> arbitrary::Result<Scenario> {
    Scenario::arbitrary(&mut Unstructured::new(data))
}

/// The first point where LMDB and ZeroDB disagree on a scenario.
#[derive(Clone, PartialEq, Eq)]
pub struct ScenarioDivergence {
    /// Index of the step in the scenario's transcript.
    pub step: usize,
    /// What the step did.
    pub what: String,
    /// LMDB's result.
    pub lmdb: String,
    /// ZeroDB's result.
    pub zerodb: String,
}

impl fmt::Debug for ScenarioDivergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "scenario divergence at step #{}:", self.step)?;
        writeln!(f, "  step:   {}", self.what)?;
        writeln!(f, "  lmdb:   {}", self.lmdb)?;
        writeln!(f, "  zerodb: {}", self.zerodb)
    }
}

impl fmt::Display for ScenarioDivergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for ScenarioDivergence {}

/// One transcript entry: what the step did and its normalized result.
pub(crate) type Entry = (String, String);

pub(crate) fn compare(a: &[Entry], b: &[Entry]) -> Result<(), Box<ScenarioDivergence>> {
    for (step, (ea, eb)) in a.iter().zip(b.iter()).enumerate() {
        if ea != eb {
            return Err(Box::new(ScenarioDivergence {
                step,
                what: ea.0.clone(),
                lmdb: ea.1.clone(),
                zerodb: eb.1.clone(),
            }));
        }
    }
    if a.len() != b.len() {
        let step = a.len().min(b.len());
        let at = |t: &[Entry]| {
            t.get(step)
                .map_or("<end>".to_string(), |e| format!("{} => {}", e.0, e.1))
        };
        return Err(Box::new(ScenarioDivergence {
            step,
            what: "transcript length".into(),
            lmdb: at(a),
            zerodb: at(b),
        }));
    }
    Ok(())
}

/// Run `scenario` on LMDB and on ZeroDB (through the heed adapter) and
/// return the first difference.
///
/// # Errors
///
/// The first step where the two transcripts differ.
pub fn run_scenario(scenario: &Scenario) -> Result<(), Box<ScenarioDivergence>> {
    let (a, b) = match scenario {
        Scenario::Ordered(c) => (engine::lmdb::ordered(c), engine::zdb::ordered(c)),
        Scenario::Snapshots(c) => (engine::lmdb::snapshots(c), engine::zdb::snapshots(c)),
        Scenario::Reopen(c) => {
            let (mut a, mut b) = (engine::lmdb::reopen(c), engine::zdb::reopen(c));
            // A reopen case may set a map too small for its writes. Where a
            // map fills depends on each engine's page allocation, so the
            // comparison stops at the first step where either engine
            // returned `MapFull`; [`MapFullCase`] covers what follows a full
            // map.
            let full = |t: &[Entry]| t.iter().position(|e| e.1.contains("MapFull"));
            if let Some(cut) = [full(&a), full(&b)].into_iter().flatten().min() {
                a.truncate(cut);
                b.truncate(cut);
            }
            (a, b)
        }
        Scenario::MapFull(c) => {
            let (mut a, mut b) = (engine::lmdb::map_full(c), engine::zdb::map_full(c));
            // Whether a case fills the map at all depends on each engine's
            // page allocation; the result of a commit after `MapFull` is only
            // compared when both engines filled it.
            let never = |t: &[Entry]| t.iter().any(|e| e.1 == engine::NEVER_FULL);
            if never(&a) || never(&b) {
                a.retain(|e| e.0 != engine::POST_FAIL);
                b.retain(|e| e.0 != engine::POST_FAIL);
            }
            (a, b)
        }
    };
    compare(&a, &b)
}

/// Run `scenario` on LMDB twice: the harness's own determinism check.
///
/// # Errors
///
/// The first step where the two LMDB transcripts differ.
pub fn run_scenario_self_test(scenario: &Scenario) -> Result<(), Box<ScenarioDivergence>> {
    let run = |s: &Scenario| match s {
        Scenario::Ordered(c) => engine::lmdb::ordered(c),
        Scenario::Snapshots(c) => engine::lmdb::snapshots(c),
        Scenario::Reopen(c) => engine::lmdb::reopen(c),
        Scenario::MapFull(c) => engine::lmdb::map_full(c),
    };
    compare(&run(scenario), &run(scenario))
}

/// The kind of step a divergence happened at: its text up to the first
/// space or parenthesis.
fn step_kind(d: &ScenarioDivergence) -> String {
    d.what
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Shrink a diverging scenario: drop steps (and batches) one at a time while
/// the scenario still diverges at the same kind of step. Returns `scenario`
/// unchanged when it does not diverge.
#[must_use]
pub fn minimize(scenario: &Scenario) -> Scenario {
    let Err(d) = run_scenario(scenario) else {
        return scenario.clone();
    };
    let kind = step_kind(&d);
    let still = |s: &Scenario| matches!(run_scenario(s), Err(d) if step_kind(&d) == kind);
    let mut best = scenario.clone();
    loop {
        let len = match &best {
            Scenario::Ordered(c) => c.ops.len(),
            Scenario::Snapshots(c) => c.ops.len(),
            Scenario::Reopen(c) => c.ops.len(),
            Scenario::MapFull(c) => c.batches.len(),
        };
        let mut shrunk = false;
        for i in (0..len).rev() {
            let mut cand = best.clone();
            match &mut cand {
                Scenario::Ordered(c) => {
                    c.ops.remove(i);
                }
                Scenario::Snapshots(c) => {
                    c.ops.remove(i);
                }
                Scenario::Reopen(c) => {
                    c.ops.remove(i);
                }
                Scenario::MapFull(c) => {
                    c.batches.remove(i);
                }
            }
            if still(&cand) {
                best = cand;
                shrunk = true;
            }
        }
        if !shrunk {
            return best;
        }
    }
}
