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

/// The shared region an environment coordinates through when opened with a
/// lock file (ADR-0023). Every reference returned lives as long as `self`.
pub trait SharedRegion: Send + Sync {
    /// The published txnid `T`: the last committed txnid, stored by the
    /// committing writer after its meta write (ADR-0023 §4).
    fn published_txnid(&self) -> &AtomicU64;
    /// The number of reader slots (`max_readers` fixed by the first opener).
    fn slot_count(&self) -> u32;
    /// Reader slot `i`'s txnid word, with the in-process encoding (TXN-14).
    fn slot_txnid(&self, i: u32) -> &AtomicU64;
    /// Reader slot `i`'s owner PID; `0` while being claimed or released.
    fn slot_pid(&self, i: u32) -> &AtomicU32;
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
#[cfg(test)]
pub(crate) struct HeapRegion {
    pub(crate) t: AtomicU64,
    pub(crate) txnids: Box<[AtomicU64]>,
    pub(crate) pids: Box<[AtomicU32]>,
    pub(crate) pid: u32,
    pub(crate) initializer: bool,
    writer: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

#[cfg(test)]
impl HeapRegion {
    pub(crate) fn new(slots: u32, pid: u32, initializer: bool) -> HeapRegion {
        HeapRegion {
            t: AtomicU64::new(0),
            txnids: (0..slots)
                .map(|_| AtomicU64::new(READER_SLOT_FREE))
                .collect(),
            pids: (0..slots).map(|_| AtomicU32::new(0)).collect(),
            pid,
            initializer,
            writer: std::sync::Mutex::new(false),
            cv: std::sync::Condvar::new(),
        }
    }
}

#[cfg(test)]
impl SharedRegion for HeapRegion {
    fn published_txnid(&self) -> &AtomicU64 {
        &self.t
    }
    fn slot_count(&self) -> u32 {
        self.txnids.len() as u32
    }
    fn slot_txnid(&self, i: u32) -> &AtomicU64 {
        &self.txnids[i as usize]
    }
    fn slot_pid(&self, i: u32) -> &AtomicU32 {
        &self.pids[i as usize]
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
