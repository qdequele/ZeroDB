//! The cross-process coordination seam (ADR-0023).
//!
//! When several processes may open one environment, they coordinate through a
//! shared lock file: the published txnid `T`, the reader slots and the writer
//! lock. `zerodb-io` maps that file and implements [`SharedRegion`] over it;
//! this crate sees only safe atomics and lock calls, so it stays free of
//! `unsafe`.

use std::sync::atomic::{AtomicU32, AtomicU64};

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
/// | 8 | owner PID `u32`: written only in a lock file; `0` while free |
/// | 12 | padding up to 128 bytes, never read or written |
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
    pid: AtomicU32,
    _pad: [u8; 116],
}

// The lock-file layout depends on these (ADR-0023 §3).
const _: () = {
    assert!(std::mem::size_of::<ReaderSlot>() == READER_SLOT_SIZE);
    assert!(std::mem::align_of::<ReaderSlot>() == 64);
    assert!(std::mem::offset_of!(ReaderSlot, txnid) == 0);
    assert!(std::mem::offset_of!(ReaderSlot, pid) == 8);
};

impl ReaderSlot {
    /// A free slot: txnid [`READER_SLOT_FREE`], no owner.
    pub fn new_free() -> ReaderSlot {
        ReaderSlot {
            txnid: AtomicU64::new(READER_SLOT_FREE),
            pid: AtomicU32::new(0),
            _pad: [0; 116],
        }
    }

    /// The slot's txnid word, with the in-process encoding (TXN-14).
    pub fn txnid(&self) -> &AtomicU64 {
        &self.txnid
    }

    /// The slot's owner PID; `0` while free, being claimed or released.
    pub fn pid(&self) -> &AtomicU32 {
        &self.pid
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
    /// Block until this process holds the cross-process writer lock. Called
    /// only while the in-process writer lock is held, so at most one thread
    /// per process waits here.
    ///
    /// # Errors
    ///
    /// The I/O error of the underlying lock call.
    fn lock_writer(&self) -> std::io::Result<()>;
    /// Release the cross-process writer lock. Callable from any thread.
    fn unlock_writer(&self);
}

/// An in-process [`SharedRegion`] for unit tests and miri: heap atomics and a
/// flag lock. Not a cross-process implementation.
#[cfg(all(test, not(loom)))]
pub(crate) struct HeapRegion {
    pub(crate) t: AtomicU64,
    pub(crate) slots: Box<[ReaderSlot]>,
    pub(crate) pid: u32,
    pub(crate) initializer: bool,
    pub(crate) writer: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

#[cfg(all(test, not(loom)))]
impl HeapRegion {
    pub(crate) fn new(slots: u32, pid: u32, initializer: bool) -> HeapRegion {
        HeapRegion {
            t: AtomicU64::new(0),
            slots: (0..slots).map(|_| ReaderSlot::new_free()).collect(),
            pid,
            initializer,
            writer: std::sync::Mutex::new(false),
            cv: std::sync::Condvar::new(),
        }
    }
}

#[cfg(all(test, not(loom)))]
impl SharedRegion for HeapRegion {
    fn published_txnid(&self) -> &AtomicU64 {
        &self.t
    }
    fn slots(&self) -> &[ReaderSlot] {
        &self.slots
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn is_initializer(&self) -> bool {
        self.initializer
    }
    fn lock_writer(&self) -> std::io::Result<()> {
        let mut held = self.writer.lock().expect("test region poisoned");
        while *held {
            held = self.cv.wait(held).expect("test region poisoned");
        }
        *held = true;
        Ok(())
    }
    fn unlock_writer(&self) {
        *self.writer.lock().expect("test region poisoned") = false;
        self.cv.notify_one();
    }
}
