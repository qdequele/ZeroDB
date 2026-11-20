//! The cross-process writer lock's word protocol on Linux (SPEC 04 LCK-4,
//! ADR-0023 §2).
//!
//! The word lives in the shared lock file and holds the owner's PID, with
//! [`WAITERS`] set while some process sleeps on it. `zerodb-io` maps the
//! word and supplies the futex calls and the liveness probe; the state
//! machine itself is plain atomics, so it lives here, where loom checks the
//! same source that ships (`just loom`, ADR-0006). Nothing here sleeps or
//! makes a system call.

#[cfg(test)]
use crate::sync::AtomicU32;
use crate::sync::Ordering;

/// Writer-word bit set while a process sleeps on the word's futex.
pub const WAITERS: u32 = 1 << 31;

/// The atomic operations the protocol uses. `zerodb-io` passes the mapped
/// lock-file word, a `std::sync::atomic::AtomicU32`; the loom models pass
/// loom's `AtomicU32`. Under `--cfg loom` both types exist in one build, so
/// the protocol is generic over them instead of naming either.
pub trait Word {
    /// As `AtomicU32::load`.
    fn load(&self, order: Ordering) -> u32;
    /// As `AtomicU32::compare_exchange`.
    ///
    /// # Errors
    ///
    /// The current value, when it is not `current`.
    fn compare_exchange(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32>;
    /// As `AtomicU32::swap`.
    fn swap(&self, val: u32, order: Ordering) -> u32;
}

impl Word for std::sync::atomic::AtomicU32 {
    #[inline]
    fn load(&self, order: Ordering) -> u32 {
        self.load(order)
    }
    #[inline]
    fn compare_exchange(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        self.compare_exchange(current, new, success, failure)
    }
    #[inline]
    fn swap(&self, val: u32, order: Ordering) -> u32 {
        self.swap(val, order)
    }
}

#[cfg(loom)]
impl Word for loom::sync::atomic::AtomicU32 {
    fn load(&self, order: Ordering) -> u32 {
        self.load(order)
    }
    fn compare_exchange(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        self.compare_exchange(current, new, success, failure)
    }
    fn swap(&self, val: u32, order: Ordering) -> u32 {
        self.swap(val, order)
    }
}

/// Take the writer lock held in `word` for process `pid` (LCK-4).
///
/// `wait(expected)` sleeps while the word equals `expected`, for at most the
/// owner-probe interval, and returns `true` if that interval ran out; it may
/// return early for any reason. `is_alive(pid)` is the liveness probe of
/// LCK-5. Both are called only while the lock is contended.
///
/// The caller holds the in-process writer lock (TXN-6), so no other thread
/// of this process can own the word. A word naming `pid` was therefore left
/// by a dead process with the same PID, and is taken over at once.
///
/// Returns `true` when the lock was taken over (from a dead owner, or from a
/// stale word naming `pid`), `false` when it was acquired normally.
///
/// # Errors
///
/// The error of `is_alive`.
pub fn lock(
    word: &impl Word,
    pid: u32,
    mut wait: impl FnMut(u32) -> bool,
    mut is_alive: impl FnMut(u32) -> std::io::Result<bool>,
) -> std::io::Result<bool> {
    debug_assert!(pid != 0 && pid & WAITERS == 0, "PID {pid} cannot be stored");
    loop {
        // Acquire: synchronizes with the previous owner's Release in
        // `unlock`, so its T store and meta write are visible.
        let cur = match word.compare_exchange(0, pid, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => return Ok(false),
            Err(cur) => cur,
        };
        let owner = cur & !WAITERS;
        if owner == pid {
            // The word names this process, but this process does not hold
            // the lock (see above): the word is stale. Waiting would never
            // end (`is_alive` reports this PID alive, and other processes see
            // this process's liveness lock), so take it over at once.
            // Acquire as above; the waiters bit is kept so this owner's
            // release wakes the others.
            if word
                .compare_exchange(
                    cur,
                    pid | (cur & WAITERS),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Ok(true);
            }
            continue; // the word changed; re-read it
        }
        // Relaxed: setting the waiters bit publishes no data; it only tells
        // the owner's release to issue a wake. The futex syscall re-checks
        // the word against `cur | WAITERS` in the kernel.
        if cur & WAITERS == 0
            && word
                .compare_exchange(cur, cur | WAITERS, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            continue; // the word changed; re-read it
        }
        let timed_out = wait(cur | WAITERS);
        if timed_out && !is_alive(owner)? {
            // The owner died holding the lock (ADR-0023): take it over.
            // Relaxed load: the CAS below re-validates the value. Acquire on
            // the CAS as above. The waiters bit is kept so this owner's
            // release wakes the others.
            let now = word.load(Ordering::Relaxed);
            if now & !WAITERS == owner
                && word
                    .compare_exchange(
                        now,
                        pid | (now & WAITERS),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                return Ok(true);
            }
        }
    }
}

/// Release the writer lock held in `word` (LCK-4). Returns whether a process
/// may be sleeping on the word, in which case the caller wakes every waiter.
/// Callable from any thread.
#[must_use]
pub fn unlock(word: &impl Word) -> bool {
    // Release: publishes this writer's T store and meta write to the next
    // owner's Acquire.
    word.swap(0, Ordering::Release) & WAITERS != 0
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn free_word_is_taken_without_waiting() {
        let w = AtomicU32::new(0);
        let r = lock(&w, 7, |_| panic!("no wait"), |_| panic!("no probe"));
        assert!(!r.unwrap());
        assert_eq!(w.load(Ordering::Relaxed), 7);
        assert!(!unlock(&w), "nobody waited");
        assert_eq!(w.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn stale_own_pid_is_taken_over_keeping_the_waiters_bit() {
        for stale in [7, 7 | WAITERS] {
            let w = AtomicU32::new(stale);
            let r = lock(&w, 7, |_| panic!("no wait"), |_| panic!("no probe"));
            assert!(r.unwrap());
            assert_eq!(w.load(Ordering::Relaxed), stale);
            assert_eq!(unlock(&w), stale & WAITERS != 0);
        }
    }

    #[test]
    fn dead_owner_is_taken_over_after_a_timed_out_wait() {
        let w = AtomicU32::new(9);
        let waits = Cell::new(0);
        let r = lock(
            &w,
            7,
            |expected| {
                assert_eq!(
                    expected,
                    9 | WAITERS,
                    "waits on the word with its waiters bit"
                );
                waits.set(waits.get() + 1);
                true
            },
            |pid| {
                assert_eq!(pid, 9);
                Ok(false)
            },
        );
        assert!(r.unwrap());
        assert_eq!(waits.get(), 1);
        assert_eq!(w.load(Ordering::Relaxed), 7 | WAITERS);
        assert!(unlock(&w), "the waiters bit asks for a wake");
    }

    #[test]
    fn live_owner_is_waited_for_and_probe_errors_propagate() {
        let w = AtomicU32::new(9);
        let mut probes = 0;
        let r = lock(
            &w,
            7,
            |_| true,
            |_| {
                probes += 1;
                if probes < 3 {
                    Ok(true)
                } else {
                    Err(std::io::Error::other("probe failed"))
                }
            },
        );
        assert_eq!(r.unwrap_err().to_string(), "probe failed");
        assert_eq!(probes, 3);
        assert_eq!(
            w.load(Ordering::Relaxed),
            9 | WAITERS,
            "the owner keeps the lock"
        );
    }
}

// ---------------------------------------------------------------------------
// Loom models (`just loom`). Each thread plays one process: `pid` is its PID
// and the word is the shared lock-file word. The futex is modelled by a mutex
// and a condvar: `wait` checks the word under the mutex and sleeps on the
// condvar, which the kernel's futex hash-bucket lock makes atomic with
// respect to a wake; `wake_all` takes the mutex and notifies every sleeper.
//
// The data a writer publishes (its meta write and the store of T) is
// modelled by a Relaxed load-then-store increment of a shared counter inside
// the critical section. Two owners at once, or an acquisition that does not
// happen-after the previous release, can lose an increment, and the final
// count shows it; overlapping critical sections are also caught by a flag.
// ---------------------------------------------------------------------------

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::sync::{Arc, Condvar, Mutex};
    use loom::thread;

    /// The futex: see the section comment above.
    #[derive(Default)]
    struct Futex {
        bucket: Mutex<()>,
        cv: Condvar,
    }

    impl Futex {
        /// Sleep while `word == expected`, with no timeout. Returns `false`
        /// (not a timeout). A lost wakeup leaves the thread asleep forever,
        /// which loom reports as a deadlock; on Linux it would be a stall of
        /// one owner-probe interval.
        fn wait_untimed(&self, word: &AtomicU32, expected: u32) -> bool {
            let g = self.bucket.lock().unwrap();
            // Relaxed: the bucket mutex orders this load against a waker's
            // swap, as the hash-bucket lock does in the kernel.
            if word.load(Ordering::Relaxed) == expected {
                drop(self.cv.wait(g).unwrap());
            }
            false
        }

        fn wake_all(&self) {
            let _g = self.bucket.lock().unwrap();
            self.cv.notify_all();
        }
    }

    /// Shared state of one model: the word, the futex and the critical
    /// section's witnesses.
    #[derive(Default)]
    struct World {
        word: AtomicU32,
        futex: Futex,
        /// Incremented with a Relaxed load and store by each owner.
        published: AtomicU32,
        /// Set for the length of each critical section.
        inside: loom::sync::atomic::AtomicBool,
    }

    impl World {
        fn critical_section(&self) {
            // Relaxed: a swap is a read-modify-write, so two overlapping
            // sections cannot both read `false`.
            assert!(
                !self.inside.swap(true, Ordering::Relaxed),
                "two owners at once"
            );
            // Relaxed on purpose: only the lock's Acquire/Release orders this
            // against the previous owner's store.
            let v = self.published.load(Ordering::Relaxed);
            self.published.store(v + 1, Ordering::Relaxed);
            self.inside.store(false, Ordering::Relaxed);
        }

        fn release(&self) {
            if unlock(&self.word) {
                self.futex.wake_all();
            }
        }
    }

    fn bounded() -> loom::model::Builder {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(3);
        b
    }

    /// Two live processes contend: each gets the lock once, never both at
    /// once, each sees the other's publication, and no wakeup is lost
    /// (the waiter sleeps with no timeout).
    #[test]
    fn loom_ww1_two_live_writers_exclude_and_hand_off() {
        bounded().check(|| {
            let world = Arc::new(World::default());
            let threads: Vec<_> = [1u32, 2]
                .into_iter()
                .map(|pid| {
                    let world = Arc::clone(&world);
                    thread::spawn(move || {
                        let w = &*world;
                        let took_over = lock(
                            &w.word,
                            pid,
                            |expected| w.futex.wait_untimed(&w.word, expected),
                            |_| panic!("no probe without a timeout"),
                        )
                        .unwrap();
                        assert!(!took_over);
                        w.critical_section();
                        w.release();
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
            assert_eq!(world.published.load(Ordering::Relaxed), 2);
            assert_eq!(world.word.load(Ordering::Relaxed), 0);
        });
    }

    /// One live process takes the lock twice in a row while another waits:
    /// the waiters bit set by the sleeper survives the first owner's
    /// re-acquisition, so the second release still wakes it.
    #[test]
    fn loom_ww2_reacquire_keeps_the_sleeper_awake() {
        bounded().check(|| {
            let world = Arc::new(World::default());
            let w2 = Arc::clone(&world);
            let other = thread::spawn(move || {
                let w = &*w2;
                lock(
                    &w.word,
                    2,
                    |e| w.futex.wait_untimed(&w.word, e),
                    |_| panic!("no probe without a timeout"),
                )
                .unwrap();
                w.critical_section();
                w.release();
            });
            let w = &*world;
            for _ in 0..2 {
                lock(
                    &w.word,
                    1,
                    |e| w.futex.wait_untimed(&w.word, e),
                    |_| panic!("no probe without a timeout"),
                )
                .unwrap();
                w.critical_section();
                w.release();
            }
            other.join().unwrap();
            assert_eq!(world.published.load(Ordering::Relaxed), 3);
        });
    }

    /// The owner (PID 9) died holding the lock. Two live processes wait on
    /// it; their waits time out, the probe reports PID 9 dead, and exactly
    /// one of them takes the lock over. The other then gets it from the new
    /// owner, by an ordinary acquisition or, if it reads the word before the
    /// takeover, not at all until the new owner releases.
    #[test]
    fn loom_ww3_dead_owner_taken_over_once() {
        bounded().check(|| {
            let world = Arc::new(World::default());
            world.word.store(9 | WAITERS, Ordering::Relaxed);
            let threads: Vec<_> = [1u32, 2]
                .into_iter()
                .map(|pid| {
                    let world = Arc::clone(&world);
                    thread::spawn(move || {
                        let w = &*world;
                        let took_over = lock(
                            &w.word,
                            pid,
                            // A timed wait that runs out: the dead owner never
                            // wakes anyone. Yield so the other thread runs.
                            |_| {
                                thread::yield_now();
                                true
                            },
                            |p| Ok(p != 9),
                        )
                        .unwrap();
                        w.critical_section();
                        w.release();
                        took_over
                    })
                })
                .collect();
            let takeovers = threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(|&t| t)
                .count();
            assert_eq!(takeovers, 1, "exactly one takeover of the dead owner");
            assert_eq!(world.published.load(Ordering::Relaxed), 2);
            assert_eq!(world.word.load(Ordering::Relaxed), 0);
        });
    }

    /// A dead predecessor with this process's PID left the word set. This
    /// process takes it over at once, while a live process waiting on the
    /// same stale word gets the lock only after this one releases it.
    #[test]
    fn loom_ww4_stale_own_pid_vs_live_waiter() {
        bounded().check(|| {
            let world = Arc::new(World::default());
            world.word.store(1, Ordering::Relaxed);
            let w2 = Arc::clone(&world);
            let other = thread::spawn(move || {
                let w = &*w2;
                let took_over = lock(
                    &w.word,
                    2,
                    |e| w.futex.wait_untimed(&w.word, e),
                    |_| panic!("PID 1 is alive: no wait times out"),
                )
                .unwrap();
                w.critical_section();
                w.release();
                took_over
            });
            let w = &*world;
            let took_over =
                lock(&w.word, 1, |_| panic!("no wait"), |_| panic!("no probe")).unwrap();
            assert!(took_over, "a stale word naming this PID is a takeover");
            w.critical_section();
            w.release();
            assert!(!other.join().unwrap(), "the live waiter acquires normally");
            assert_eq!(world.published.load(Ordering::Relaxed), 2);
        });
    }
}
