# ADR-0035: One atomic word for the in-process writer lock

- Status: Proposed (awaiting maintainer approval; no code changed yet)
- Related: issue #146 (per-commit CPU), ADR-0023 §2 (the two-layer writer
  lock), SPEC 04 TXN-6/TXN-7, the `unsafe` policy in `CONTRIBUTING.md`
- Date: 2026-11-02

## Context

A write transaction that changes nothing costs ZeroDB 3.52× what it costs
LMDB (`env/txn/rw_empty_commit`, Graviton4, October 2026), and the fixed
cost of beginning and ending a write transaction is most of the remaining
gap on small no-sync commits (issue #146). Meilisearch's task queue and auth
environments run many such transactions.

### What the writer lock costs today

SPEC 04 TXN-6 gives ZeroDB a two-layer writer lock (ADR-0023 §2):

1. an **in-process lock**, `WriterLock` in
   `crates/zerodb-core/src/env/writer_lock.rs`: a `std::sync::Mutex` over an
   `occupied` flag, a waiter count and the writer carry (the containers one
   write transaction leaves for the next, LMDB's `me_txn0`), plus a
   `Condvar`. The mutex is **not** held for the transaction's life, because
   `std::sync::MutexGuard` is `!Send` while `RwTxn` must be `Send` (heed's
   `WithoutTls` mode moves write transactions across threads). Instead the
   acquire locks the mutex, sets the flag, takes the carry and unlocks; the
   release locks it again, clears the flag, stores the carry back and
   unlocks.
2. in a lock-file environment, the **cross-process lock**: a word in the
   shared lock file, compare-and-swapped from 0 to the owner's PID on Linux
   (a futex word), an `fcntl` record lock on macOS. It is always taken inside
   the in-process lock, so threads of one process never contend on it.

Counting atomic read-modify-write operations (RMW) per write transaction, on
Linux, in a lock-file environment:

| Step | ZeroDB | LMDB |
|---|---:|---:|
| In-process acquire (mutex lock + unlock) | 2 | — |
| Cross-process acquire (CAS on the shared word) | 1 | 1 (robust mutex lock) |
| Cross-process release (swap) | 1 | 1 (robust mutex unlock) |
| In-process release (mutex lock + unlock) | 2 | — |
| **Writer lock total** | **6** | **2** |

LMDB has one lock: a process-shared robust `pthread_mutex_t` in its lock
file serializes threads and processes alike. Without a lock file
(`MDB_NOLOCK`) it takes no lock at all and leaves serialization to the
caller; ZeroDB's `NO_LOCK` keeps the in-process lock (4 RMW).

On the default `aarch64-unknown-linux-gnu` target, which does not enable LSE
atomics at compile time, each RMW is a call to an out-of-line helper
(`__aarch64_cas4_acq`, `__aarch64_swp4_rel`, …). Issue #146 measured those
helpers at about 18 % of an empty write transaction's cycles on Graviton4,
for about 8 RMW per transaction: the 6 above plus the reference-count
increment and decrement of the base snapshot's free-list annex, which the
write transaction shares rather than copies.

The commit path had two more: a `Mutex` around the reusable meta-page buffer.
That buffer now lives in the writer carry (issue #146, no ADR needed: it is
only ever used by the lock holder), so a commit takes no lock of its own
before it publishes its snapshot.

### What a profile of an empty write transaction shows

On a Linux aarch64 VM (`perf record -e cpu-clock --all-user`, 2 million
empty write transactions through heed, October 2026 `main`), the release
side of the lock (`WriterGuard::drop` and `__aarch64_swp4_rel`) is about 5 %
of user time and the acquire is inlined into `Env::write_txn`; the largest
single item is
`memcpy` (about 20 %), from moving the 856-byte `RwTxn` by value between
`Env::write_txn`, heed's wrapper and `commit`. That move is a separate, safe
fix (see Consequences). The VM has no hardware counters and its atomics are
inlined LSE instructions, so it understates what the out-of-line helpers cost
on Graviton; the 18 % figure above is the Graviton measurement.

### Constraints

- `RwTxn` must stay `Send` and its drop may run on any thread (TXN-6).
- A writer that panics must not wedge the environment: today the mutex
  poison is recovered because it guards no transaction data (TXN-60).
- Waiting writers must sleep, not spin (TXN-7: a second `write_txn` blocks).
- The cross-process layer is unchanged by this ADR: its owner word must
  name a process, not a thread, because a write transaction may move between
  threads and its first thread may exit while it is live.
- The writer carry must be reachable only by the lock holder.

## Options

### Option A — An owned lock cell in `zerodb-core` (new `unsafe`)

Replace `WriterLock` with a cell that owns the carry and is locked by one
atomic word:

```rust
pub(super) struct WriterCell {
    /// FREE, HELD, or HELD_WITH_WAITERS.
    state: AtomicU32,
    /// The writer carry; reachable only through a `WriterGuard`, which
    /// exists only while `state` is not FREE and this guard set it.
    carry: UnsafeCell<Option<Box<WriterCarry>>>,
    /// Slow path only: where waiting writers sleep.
    park: Mutex<()>,
    cv: Condvar,
}
```

- **Acquire, uncontended:** one `compare_exchange(FREE, HELD, Acquire)`.
  The guard then has exclusive access to `carry`.
- **Release, uncontended:** one `swap(FREE, Release)`. Only when the old
  value was `HELD_WITH_WAITERS` does it lock `park` and `notify_one`.
- **Contended:** a waiter locks `park`, then in a loop sets the state to
  `HELD_WITH_WAITERS` (or takes the lock if it finds it `FREE`, leaving it
  `HELD_WITH_WAITERS` in case others wait) and waits on `cv`. The releaser
  takes `park` before notifying, so a waiter is either still before its
  check (and will see `FREE`) or already parked (and is woken): no lost
  wakeup. This is the classic three-state futex mutex, with std's
  `Condvar` (a futex on Linux) as the sleep.
- **`unsafe`:** `unsafe impl Sync for WriterCell`, and the two dereferences
  of `carry.get()` in the guard's accessor and drop, each with a `// SAFETY:`
  comment stating that `state` was moved off `FREE` by this guard's acquire
  and is moved back only by its drop, so no other guard exists. Three
  `unsafe` sites, about 80 lines in all.
- **Panics:** there is no poison. A panicking writer unwinds through the
  `RwTxn` drop, which hands the carry back and releases the word, exactly as
  today; the carry holds only emptied containers.
- **Verification:** a loom model (`loom_writer_cell_*`) of two and three
  threads acquiring and releasing, through the same `crate::sync` shim the
  reader table uses (it gains `UnsafeCell`, `Condvar`, `AtomicU32` is already
  there), checking mutual exclusion through the cell and that every waiter
  wakes. Plus the existing multi-process tests, crash harness and stress.
- **Cost:** in-process 2 RMW, the same as LMDB's mutex. With the
  cross-process layer: 4 RMW per write transaction instead of 6.
- **Policy:** amends the `unsafe` policy with one bullet: "the writer-lock
  cell in `zerodb-core::env::writer_lock`: the `Sync` impl and the
  dereferences of its carry cell while its lock word is held".
- **ARM:** the Acquire CAS and the Release swap order every write the
  previous holder made to the carry before every read the next holder makes
  of it, on a weak memory model; nothing relies on x86 ordering.

### Option A′ — The carry pointer as the lock (new `unsafe`)

An `AtomicPtr<WriterCarry>`: null means held. Acquire swaps it with null and
owns the `Box` it got (`Box::from_raw`); release stores the pointer back.
Waiters still need a state bit and a park/notify path, which brings back a
second atomic or a tagged pointer. Same RMW count as Option A, more delicate
`unsafe` (raw `Box` ownership through an integer-tagged pointer), and loom
models raw-pointer ownership less directly. No advantage over A.

### Option B — A dependency: `parking_lot` with `send_guard`

`parking_lot::Mutex<WriterSlot>`, its guard held by the `RwTxn` for the
transaction's life. With the `send_guard` feature the guard is `Send`, so the
release may run on another thread. `parking_lot`'s uncontended lock is one
CAS and its unlock one CAS: 2 RMW in-process, as Option A, and no `unsafe` in
ZeroDB.

- **Dependencies:** `parking_lot`, `lock_api`, `parking_lot_core`,
  `scopeguard`, `smallvec`, `cfg-if` (and `redox_syscall`/`windows-link` on
  those targets). Meilisearch already builds `parking_lot` 0.12.5
  transitively, so its build gains no new crate; ZeroDB's own allowlist gains
  six.
- **Feature unification:** `send_guard` is a crate-wide feature, so it would
  turn on for every `parking_lot` user in a consumer's build, and it cannot
  be combined with `parking_lot`'s `deadlock_detection` feature (a compile
  error). Any consumer that enables `deadlock_detection` could no longer
  build with ZeroDB.
- **Verification:** loom cannot model `parking_lot`. The writer lock is not
  loom-modeled today either, so nothing is lost, but nothing is gained.
- Driving `parking_lot`'s `RawMutex` directly instead of through the guard
  avoids `send_guard` but needs `unsafe { raw.unlock() }`, which is Option
  A's policy question again, with a dependency on top.

### Option C — Safe and dependency-free

Within safe Rust and the current allowlist there is no lock-free way to hand
a `Box` from one thread to another, so the carry must stay behind a std
`Mutex`, and a std `Mutex` cannot be held across threads. Variants exist (an
atomic flag beside the mutex, the carry moved into the snapshot cell's
critical section at publish) but none takes fewer than 4 in-process RMW for
an empty transaction, which is today's count. What safe code can do it has
done: the meta-page buffer's mutex is gone (above), and the annex
reference-count pair can go by reading the base snapshot's annex in place
instead of cloning its `Arc` (Consequences). Option C keeps the lock as it is.

### Option D — Later: one lock word for threads and processes

LMDB's 2 RMW come from having a single lock. ZeroDB could get there in a
lock-file environment by letting threads of this process wait on the shared
word too. The word names the owning process, so "the word holds my PID" would
mean either "another thread of mine holds it" or "a dead process with my
recycled PID left it" (the takeover case in `lockfile.rs`), and telling them
apart needs a process-local record of the holder, published in an order that
a racing thread cannot misread. That is a change to ADR-0023's protocol and
to the macOS `fcntl` path, worth its own ADR once Option A has landed and been
measured. It is listed so the choice here does not close it: Option A's cell
would become the process-local record.

## Recommendation

**Option A.** It reaches LMDB's in-process cost (2 RMW), keeps the
dependency list unchanged, keeps `RwTxn: Send` without `send_guard`'s
feature-unification hazard, and is the only option that can be model-checked
with the loom setup the reader table already uses. The `unsafe` it adds is
three sites in one module, guarding one invariant ("only the guard that moved
the word off `FREE` touches the carry"), the same shape as the existing
policy exceptions.

| | Faster | Reliable | Durable | Secure |
|---|---|---|---|---|
| **A** owned cell | 6 → 4 RMW per write txn with a lock file (4 → 2 without); the expected share is about a quarter of the atomic-helper cycles issue #146 measured on Graviton | Loom model of exclusion and wakeups; no poison state; same panic behaviour | No change: commit ordering and the cross-process lock are untouched | Three new `unsafe` sites under a written invariant; policy amended |
| **A′** pointer token | Same as A | Harder to model; raw `Box` ownership | No change | More delicate `unsafe` than A |
| **B** `parking_lot` | Same as A | Mature crate, not loom-checkable; `send_guard` conflicts with `deadlock_detection` in consumers | No change | No new `unsafe` in ZeroDB; six more crates on the allowlist |
| **C** status quo | None beyond the safe fixes (meta buffer, annex reference count) | Unchanged | No change | Unchanged |
| **D** single lock word | 2 RMW total, LMDB parity (after A) | Changes ADR-0023's takeover protocol; needs its own ADR | No change | Same as A |

## Consequences

If Option A is approved:

- `crates/zerodb-core/src/env/writer_lock.rs` gets the cell; `WriterGuard`
  keeps its API (`carry`, `take_carry`, `give_back_carry`, the `shared`
  field), so `RwTxn` and `EnvInner` are unchanged.
- `crate::sync` gains loom-switchable `UnsafeCell` and `Condvar`; a
  `loom_writer_cell_*` model joins `just loom`.
- SPEC 04 TXN-6's "Lock construction" paragraph is rewritten for the cell
  (its rule ID stays); TXN-7's waiting behaviour is unchanged.
- `CONTRIBUTING.md`'s `unsafe` policy gains the bullet above.
- Gates: `just loom`, the multi-process tests in `crates/zerodb/tests`,
  `just crash-test-quick`, `just stress`, and a three-column `just bench-ab`
  on `env/txn/rw_empty_commit` and `commit/batch/n1` on Graviton, where the
  out-of-line helpers are.

Whatever the decision, two safe fixes found while profiling for this ADR
need no design record and cut more of an empty write transaction than the
lock does:

- **The `RwTxn` move.** `zerodb::RwTxn` is 856 bytes and heed's `RoTxn` and
  `RwTxn` embed it by value, so `write_txn` and `commit` copy it several
  times (about 20 % of an empty write transaction in the profile above, and
  every heed `RoTxn` carries the same size). Moving the cold per-transaction
  state (the validated-pages memo, the drain and run-search bookkeeping) into
  the writer carry, which already moves as one pointer, shrinks it the way
  LMDB's `MDB_txn` handle is a pointer to `me_txn0`.
- **The annex reference count.** A write transaction can read the base
  snapshot's annex through its own `base` field instead of cloning the
  annex `Arc` into its allocator state: 2 RMW fewer per transaction whose
  base has a non-empty annex, which is most of them.

## Open questions for human review

1. Approve Option A and the `unsafe` policy amendment, or prefer Option B's
   dependency?
2. Should Option D be scheduled after A is measured on Graviton, or is A's
   4 RMW enough?
