//! The single-writer lock and the close signal.

use super::*;

/// The single-writer lock (SPEC 04 TXN-6), thread-agnostic by construction:
/// an `occupied` flag under a short-critical-section mutex plus a condvar.
///
/// It deliberately does **not** hand out the `Mutex`'s own `MutexGuard` for
/// the write txn's lifetime: `std::sync::MutexGuard` is `!Send` (and on some
/// platforms unlocking from a foreign thread aborts), while `zerodb::RwTxn`
/// must be genuinely `Send` — heed's `WithoutTls` mode moves write txns
/// across threads, and the txn's drop (commit or abort) may run on any
/// thread. Here the guard only *clears the flag* under a fresh short lock
/// taken on whichever thread drops it, which is sound on every platform.
#[derive(Debug)]
pub(super) struct WriterLock {
    occupied: Mutex<WriterSlot>,
    cv: Condvar,
}

/// The state behind [`WriterLock`]'s mutex.
#[derive(Debug, Default)]
pub(super) struct WriterSlot {
    /// `true` while a write txn is live.
    occupied: bool,
    /// Threads parked in [`WriterLock::acquire`]'s wait. The release notifies
    /// only when this is non-zero: on Linux, std's futex `Condvar` makes every
    /// `notify_one` a `futex_wake` system call even with nobody waiting.
    waiters: u32,
    /// What one write txn leaves for the next: the dirty-frame
    /// pool (LMDB's `me_dpages`), the emptied containers a write txn fills,
    /// and the snapshot the last one published ([`crate::rwtxn::WriterCarry`]).
    /// Kept under the writer lock's own mutex, as LMDB keeps `me_dpages`
    /// under its writer mutex: the carry moves out with the acquire and back
    /// with the release, so it costs no lock operation of its own. `None`
    /// until the first write txn ends.
    carry: Option<Box<crate::rwtxn::WriterCarry>>,
}

/// Ownership of the writer slot; releases it on drop, from any thread.
/// Held by `RwTxn` for its whole life (TXN-6).
pub(crate) struct WriterGuard<'env> {
    lock: &'env WriterLock,
    /// The carry: taken from the slot at acquire, handed to the write txn,
    /// given back at txn end, stored at release.
    carry: Option<Box<crate::rwtxn::WriterCarry>>,
    /// The cross-process writer lock, when this guard also holds it
    /// (ADR-0023 §2; set by [`EnvInner::lock_writer_shared`]). Released
    /// before the in-process slot on drop.
    pub(super) shared: Option<&'env dyn crate::shared::SharedRegion>,
}

impl WriterGuard<'_> {
    /// The carry taken with the writer slot, allocated on the first write
    /// txn of the env.
    pub(crate) fn carry(&mut self) -> &mut crate::rwtxn::WriterCarry {
        self.carry.get_or_insert_with(Box::default)
    }

    /// Move the carry out, for the new write txn. O(1): the `Box` moves.
    pub(crate) fn take_carry(&mut self) -> Box<crate::rwtxn::WriterCarry> {
        self.carry.take().unwrap_or_default()
    }

    /// The carry to store back into the slot when this guard releases it.
    pub(crate) fn give_back_carry(&mut self, carry: Box<crate::rwtxn::WriterCarry>) {
        self.carry = Some(carry);
    }
}

impl WriterLock {
    pub(super) fn new() -> WriterLock {
        WriterLock {
            occupied: Mutex::new(WriterSlot::default()),
            cv: Condvar::new(),
        }
    }

    /// Claim the writer slot if it is free; `None` while a write txn of
    /// this env is live. Never waits (`Env::resize`, ADR-0023).
    pub(super) fn try_acquire(&self) -> Option<WriterGuard<'_>> {
        // Poison recovery as in `acquire`.
        let mut g = self
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if g.occupied {
            return None;
        }
        g.occupied = true;
        let carry = g.carry.take();
        Some(WriterGuard {
            lock: self,
            carry,
            shared: None,
        })
    }

    /// Block until the writer slot is free, then claim it.
    pub(super) fn acquire(&self) -> WriterGuard<'_> {
        // Poison recovery: the lock guards no data — the dirty set lives in
        // the RwTxn and is dropped during unwind (TXN-60 implicit abort) — so
        // clearing a poison is sound and keeps the env usable after a writer
        // panic. The flag mutex is only ever held for the flag flip below,
        // but the same recovery applies.
        let mut g = self
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while g.occupied {
            // Registered under the mutex before `wait` releases it, so a
            // release that reads `waiters == 0` under the same mutex cannot be
            // racing a thread about to park: no lost wakeup.
            g.waiters += 1;
            g = self
                .cv
                .wait(g)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            g.waiters -= 1;
        }
        g.occupied = true;
        let carry = g.carry.take();
        WriterGuard {
            lock: self,
            carry,
            shared: None,
        }
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        // Cross-process lock first, then the in-process slot: a thread of this
        // process woken by the slot release then competes fairly with other
        // processes for the shared lock.
        if let Some(region) = self.shared.take() {
            region.unlock_writer();
        }
        let mut g = self
            .lock
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.occupied = false;
        g.carry = self.carry.take();
        // One waiter at most can make progress (single writer), so
        // `notify_one` suffices; drop the flag lock before notify is not
        // required for correctness (the waiter re-checks under the lock).
        // Skipped when nobody is parked, which is the common, uncontended case.
        if g.waiters > 0 {
            self.lock.cv.notify_one();
        }
    }
}

/// A cross-thread one-shot signal: fires once, when the last [`EnvInner`]
/// reference is dropped (SPEC 04 TXN-53). Waiters block until then.
#[derive(Debug)]
pub(super) struct SignalEvent {
    fired: Mutex<bool>,
    cv: Condvar,
}

impl SignalEvent {
    pub(super) fn new() -> SignalEvent {
        SignalEvent {
            fired: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    pub(super) fn signal(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        *g = true;
        self.cv.notify_all();
    }

    pub(super) fn wait(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        while !*g {
            g = self.cv.wait(g).expect("signal mutex poisoned");
        }
    }

    /// Wait up to `dur`. Returns `true` if the event has fired.
    pub(super) fn wait_timeout(&self, dur: Duration) -> bool {
        let g = self.fired.lock().expect("signal mutex poisoned");
        if *g {
            return true;
        }
        let (g2, _res) = self.cv.wait_timeout(g, dur).expect("signal mutex poisoned");
        *g2
    }
}
