//! The cross-process coordination seam (ADR-0023).
//!
//! When several processes may open one environment, they coordinate through a
//! shared lock file: the published txnid `T`, the reader slots and the writer
//! lock. `zerodb-io` maps that file and implements [`SharedRegion`] over it;
//! this crate sees only safe atomics and lock calls, so it stays free of
//! `unsafe`.

use std::sync::atomic::{AtomicU64, AtomicU8};

/// Reader-slot txnid value meaning "free" (SPEC 04 TXN-14). A lock-file
/// initializer writes it into every slot.
pub const READER_SLOT_FREE: u64 = crate::readers::RDR_FREE;

/// The size in bytes of one [`ReaderSlot`], which is also the stride between
/// slots in the lock file (ADR-0023 §3).
pub const READER_SLOT_SIZE: usize = 128;

/// One reader slot (SPEC 04 TXN-14), with the same fixed layout in this
/// process's heap and in a shared lock file (ADR-0023 §3):
///
/// | offset | field |
/// |---|---|
/// | 0 | txnid `u64`: the pinned snapshot txnid, or a sentinel (TXN-14) |
/// | 8 | owner `u64`: low 32 bits the owner PID (`0` = none), high 32 bits the claim generation; written only in a lock file |
/// | 16 | padding up to 128 bytes, unused by the slot protocol |
///
/// The owner word's generation is incremented by every write that stores a
/// non-zero PID (a claim or a reaper's adoption) and kept by every release
/// (which stores PID 0). A word with a non-zero PID therefore never recurs: a
/// reaper that compare-and-swaps the whole word cannot be fooled by a PID
/// that was freed and then claimed again under the same PID
/// (ADR-0023 amendment 14). See [`owner_pid`], [`owner_generation`] and
/// [`owner_word`].
///
/// The padding is atomic bytes too, so the whole slot is interior-mutable:
/// another process may still write those bytes (a later initializer that
/// truncates and re-extends the file, for example) while this one holds a
/// `&ReaderSlot` over the mapping, and that must not be a data race on plain
/// memory.
///
/// The 64-byte alignment and 128-byte size keep a reader's stores to its own
/// slot and the writer's scan of the table off each other's cache lines
/// (including adjacent-line prefetch). A lock file stores the slot table at a
/// 64-byte-aligned offset with this exact stride, so `zerodb-io` can view it
/// as a `&[ReaderSlot]`.
///
/// The atomics are always `std`'s, even under loom: the layout is shared with
/// other processes. loom builds model the reader table over a separate local
/// slot type with loom atomics (`crate::readers`).
#[repr(C, align(64))]
pub struct ReaderSlot {
    txnid: AtomicU64,
    owner: AtomicU64,
    _pad: [AtomicU8; 112],
}

// The lock-file layout depends on these (ADR-0023 §3).
const _: () = {
    assert!(std::mem::size_of::<ReaderSlot>() == READER_SLOT_SIZE);
    assert!(std::mem::align_of::<ReaderSlot>() == 64);
    assert!(std::mem::offset_of!(ReaderSlot, txnid) == 0);
    assert!(std::mem::offset_of!(ReaderSlot, owner) == 8);
};

/// The owner PID in an owner word (`0` = none).
pub const fn owner_pid(word: u64) -> u32 {
    word as u32
}

/// The claim generation in an owner word.
pub const fn owner_generation(word: u64) -> u32 {
    (word >> 32) as u32
}

/// An owner word from a generation and a PID.
pub const fn owner_word(generation: u32, pid: u32) -> u64 {
    ((generation as u64) << 32) | pid as u64
}

impl ReaderSlot {
    /// A free slot: txnid [`READER_SLOT_FREE`], no owner.
    pub fn new_free() -> ReaderSlot {
        ReaderSlot {
            txnid: AtomicU64::new(READER_SLOT_FREE),
            owner: AtomicU64::new(0),
            _pad: [const { AtomicU8::new(0) }; 112],
        }
    }

    /// The slot's txnid word, with the in-process encoding (TXN-14).
    pub fn txnid(&self) -> &AtomicU64 {
        &self.txnid
    }

    /// The slot's owner word: the owner PID in the low 32 bits (`0` while
    /// free, being claimed or released) and the claim generation in the high
    /// 32 bits.
    pub fn owner(&self) -> &AtomicU64 {
        &self.owner
    }
}

/// The shared region an environment coordinates through when opened with a
/// lock file (ADR-0023). Every reference returned lives as long as `self`.
pub trait SharedRegion: Send + Sync {
    /// The published txnid `T`: the last committed txnid, stored by the
    /// committing writer after its meta write (ADR-0023 §4).
    fn published_txnid(&self) -> &AtomicU64;
    /// The reader slots; the length is `max_readers` as fixed by the first
    /// opener. Callers resolve this once per operation and then index it
    /// directly.
    fn slots(&self) -> &[ReaderSlot];
    /// This process's PID as written into slots and the writer lock.
    fn pid(&self) -> u32;
    /// Whether this process initialized the region (no other process had the
    /// environment open), and so must seed `T` from the data file.
    fn is_initializer(&self) -> bool;
    /// Whether process `pid` currently has this environment open (holds its
    /// liveness lock, ADR-0023 §3). Immune to PID reuse for other processes.
    /// For this process's own PID it is always `true`, even when the PID was
    /// recorded by a dead predecessor with the same PID; the reader table
    /// frees those slots at open instead (ADR-0023 amendment 9).
    ///
    /// # Errors
    ///
    /// The I/O error of the underlying probe.
    fn is_alive(&self, pid: u32) -> std::io::Result<bool>;
    /// Block until this process holds the cross-process writer lock. Called
    /// only while the in-process writer lock is held, so at most one thread
    /// per process waits here.
    ///
    /// Returns `true` when the lock was taken over from a dead owner (or from
    /// a stale word naming this process's PID, left by a dead predecessor),
    /// `false` when it was acquired normally. An implementation that cannot
    /// tell (a POSIX record lock reports no previous owner) returns `false`.
    /// The environment reaps dead readers after a takeover, as LMDB does
    /// (ADR-0023 amendment 12).
    ///
    /// # Errors
    ///
    /// The I/O error of the underlying lock call.
    fn lock_writer(&self) -> std::io::Result<bool>;
    /// Release the cross-process writer lock. Callable from any thread.
    fn unlock_writer(&self);
    /// Let other processes join (ADR-0023 §1, LCK-2): the initializer
    /// publishes the finished header and downgrades its exclusive election
    /// lock to a shared one. A no-op for a joiner, which already holds it
    /// shared. The environment calls it once, at the end of its open, or,
    /// for a `PREV_SNAPSHOT` open, at its first commit (amendment 19).
    ///
    /// # Errors
    ///
    /// The I/O error of the lock conversion.
    fn finish_open(&self) -> std::io::Result<()>;
}

/// An in-process [`SharedRegion`] for unit tests and miri: heap atomics and a
/// flag lock. Not a cross-process implementation.
#[cfg(all(test, not(loom)))]
pub(crate) struct HeapRegion {
    /// The published txnid and the slots, shared with every region made by
    /// [`HeapRegion::rejoin`] (which plays another opener of the same file).
    table: std::sync::Arc<HeapTable>,
    pub(crate) pid: u32,
    pub(crate) initializer: bool,
    pub(crate) writer: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
    dead: std::sync::Mutex<Vec<u32>>,
    /// When set, the next [`SharedRegion::lock_writer`] reports a takeover
    /// from a dead owner (and clears the flag).
    next_lock_is_takeover: std::sync::atomic::AtomicBool,
    /// When set, every [`SharedRegion::is_alive`] probe fails.
    probes_fail: std::sync::atomic::AtomicBool,
    /// When set, the next [`SharedRegion::lock_writer`] fails with an I/O
    /// error without taking the lock (and clears the flag).
    next_lock_fails: std::sync::atomic::AtomicBool,
    /// How many times [`SharedRegion::finish_open`] ran.
    pub(crate) finish_opens: std::sync::atomic::AtomicU32,
    /// When set, the next [`SharedRegion::finish_open`] fails with an I/O
    /// error (and clears the flag).
    next_finish_fails: std::sync::atomic::AtomicBool,
}

/// The part of a [`HeapRegion`] that models the lock file's contents.
#[cfg(all(test, not(loom)))]
struct HeapTable {
    t: AtomicU64,
    slots: Box<[ReaderSlot]>,
}

#[cfg(all(test, not(loom)))]
impl HeapRegion {
    pub(crate) fn new(slots: u32, pid: u32, initializer: bool) -> HeapRegion {
        HeapRegion {
            table: std::sync::Arc::new(HeapTable {
                t: AtomicU64::new(0),
                slots: (0..slots).map(|_| ReaderSlot::new_free()).collect(),
            }),
            pid,
            initializer,
            writer: std::sync::Mutex::new(false),
            cv: std::sync::Condvar::new(),
            dead: std::sync::Mutex::new(Vec::new()),
            next_lock_is_takeover: std::sync::atomic::AtomicBool::new(false),
            probes_fail: std::sync::atomic::AtomicBool::new(false),
            next_lock_fails: std::sync::atomic::AtomicBool::new(false),
            finish_opens: std::sync::atomic::AtomicU32::new(0),
            next_finish_fails: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// A region over the same published txnid and slots, with the same PID,
    /// that did not initialize them: a later opener of the same lock file.
    pub(crate) fn rejoin(&self) -> std::sync::Arc<HeapRegion> {
        std::sync::Arc::new(HeapRegion {
            table: std::sync::Arc::clone(&self.table),
            pid: self.pid,
            initializer: false,
            writer: std::sync::Mutex::new(false),
            cv: std::sync::Condvar::new(),
            dead: std::sync::Mutex::new(Vec::new()),
            next_lock_is_takeover: std::sync::atomic::AtomicBool::new(false),
            probes_fail: std::sync::atomic::AtomicBool::new(false),
            next_lock_fails: std::sync::atomic::AtomicBool::new(false),
            finish_opens: std::sync::atomic::AtomicU32::new(0),
            next_finish_fails: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Mark `pid` dead for [`SharedRegion::is_alive`].
    pub(crate) fn kill(&self, pid: u32) {
        self.dead.lock().expect("test region poisoned").push(pid);
    }

    /// Make the next [`SharedRegion::lock_writer`] report that it took the
    /// lock over from a dead owner, as the lock-file region does on Linux.
    pub(crate) fn simulate_takeover_on_next_lock(&self) {
        // Relaxed: a test knob set and read on the same thread.
        self.next_lock_is_takeover
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Make the next [`SharedRegion::lock_writer`] call fail with an I/O
    /// error, without taking the lock.
    pub(crate) fn fail_next_lock(&self) {
        // Relaxed: a test knob set and read on the same thread.
        self.next_lock_fails
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Make the next [`SharedRegion::finish_open`] call fail with an I/O
    /// error.
    pub(crate) fn fail_next_finish_open(&self) {
        // Relaxed: a test knob set and read on the same thread.
        self.next_finish_fails
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Make every later [`SharedRegion::is_alive`] probe fail with an I/O
    /// error (`true`) or answer normally again (`false`).
    pub(crate) fn set_liveness_probes_fail(&self, fail: bool) {
        // Relaxed: a test knob set and read on the same thread.
        self.probes_fail
            .store(fail, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(all(test, not(loom)))]
impl SharedRegion for HeapRegion {
    fn published_txnid(&self) -> &AtomicU64 {
        &self.table.t
    }
    fn slots(&self) -> &[ReaderSlot] {
        &self.table.slots
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn is_initializer(&self) -> bool {
        self.initializer
    }
    fn is_alive(&self, pid: u32) -> std::io::Result<bool> {
        // Relaxed: a test knob, see `set_liveness_probes_fail`.
        if self.probes_fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(std::io::Error::other("test probe failure"));
        }
        Ok(pid == self.pid
            || !self
                .dead
                .lock()
                .expect("test region poisoned")
                .contains(&pid))
    }
    fn lock_writer(&self) -> std::io::Result<bool> {
        // Relaxed: a test knob, set on the thread that takes the lock.
        if self
            .next_lock_fails
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(std::io::Error::other("test lock failure"));
        }
        let mut held = self.writer.lock().expect("test region poisoned");
        while *held {
            held = self.cv.wait(held).expect("test region poisoned");
        }
        *held = true;
        // Relaxed: a test knob, set on the thread that takes the lock.
        Ok(self
            .next_lock_is_takeover
            .swap(false, std::sync::atomic::Ordering::Relaxed))
    }
    fn unlock_writer(&self) {
        *self.writer.lock().expect("test region poisoned") = false;
        self.cv.notify_one();
    }
    fn finish_open(&self) -> std::io::Result<()> {
        // Relaxed: test knobs and a counter, set and read on the test thread.
        if self
            .next_finish_fails
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(std::io::Error::other("test finish_open failure"));
        }
        self.finish_opens
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
