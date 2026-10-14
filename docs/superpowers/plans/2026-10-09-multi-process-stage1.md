# Multi-process access — Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let several OS processes open one ZeroDB environment, read each other's commits and take turns writing, behind an opt-in `lock_file` option, then measure what it costs a single process.

**Architecture:** `zerodb-io` gains a `LockFile`: a `MAP_SHARED` lock file holding the published txnid `T`, a writer-lock word and the reader slots, plus an `fcntl` first-opener election and per-PID liveness locks. `zerodb-core` sees it only through a safe `SharedRegion` trait made of plain atomics and lock calls. When a region is present, the core uses it in three places. Reader slots live in the region. Readers pin `T` and decode meta slot `T & 1`, with an in-process cache as the fast path. Writers take the cross-process lock, rebuild their base from `T`, and store `T` at commit.

**Tech Stack:** Rust 2021 (MSRV 1.98); `libc` (`fcntl`, `futex` syscall); `memmap2::MmapRaw`. No new dependencies.

**Spec:** `docs/adr/0023-multi-process.md` (approved 2026-10-09). Stage 1 of its staging section, as amended by Task 1.

## Global Constraints

- Branch `qdequele/adr-0023-multi-process`. Stage 1 does **not** merge to `main` (ADR-0023, resolved question 3).
- `unsafe` only in `zerodb-io`, every block with a `// SAFETY:` comment. `zerodb-core` stays `#![forbid(unsafe_code)]`.
- Every atomic uses an explicit `Ordering` with a comment justifying it. Assume a weak memory model (ARM).
- No `#[repr(C)]` casts. Lock-file fields are accessed by explicit byte offset.
- Default behavior is unchanged in stage 1: `lock_file` defaults to **false**, so every existing test runs the old path. Stage 3 flips the default.
- No new dependency. No `TODO` in code. Never weaken, `#[ignore]` or delete a test.
- Native lock-file name `zerodb.lock`; heed adapter name `lock.mdb`.
- Comments describe what code does for an outside reader; spec rule IDs and ADR numbers are fine.
- Commit messages carry **no** `Co-Authored-By` line (user's global instruction).
- Before calling a task done: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and on macOS also `cargo clippy --workspace --all-targets --target x86_64-unknown-linux-gnu --exclude zerodb-oracle -- -D warnings`. That second run catches Linux-only code such as the futex path.

## Review Focus

1. **The same env opened twice in one process with `lock_file(true)`.** The second open must return `EnvAlreadyOpened`, and the first env must keep working. POSIX `fcntl` locks belong to the process, and closing *any* descriptor of the lock file drops them all, so the second open must be refused before it opens a descriptor. Pinned in Task 5 (`second_open_in_process_is_refused_before_any_fd`) and Task 7 (`double_open_in_process_keeps_first_env_working`).
2. **A process joins while a writer is mid-commit**, with meta `N` written but `T` still `N-1`. The joiner must seed its cache from `T`, not from the newest meta. Otherwise it caches a snapshot that may never be published, and whose txnid the next writer reuses after a failed commit (TXN-2). Pinned in Task 4 (`joiner_seeds_from_published_txnid_not_newest_meta`).
3. **A meta slot being overwritten while another process reads it.** The reader copies the page with `pread`, never as a borrowed map slice, and treats a CRC failure or a txnid mismatch as "retry". Pinned in Task 3 (`overwritten_slot_reads_as_none`, `torn_slot_reads_as_none`).
4. **A writer process killed while holding the writer lock.** The next writer takes over within about one probe interval, and the killed transaction's writes are absent. Pinned in Task 7 (`killed_writer_lock_is_taken_over`).
5. **A reader in one process holding a snapshot while another process rewrites and frees its pages.** The old values must stay readable, which shows the writer's GC scan sees foreign reader slots. Pinned in Task 7 (`foreign_reader_holds_back_page_reuse`).

---

### Task 1: Record the stage-1 refinements in ADR-0023 and amend the `unsafe` policy

Writing the plan turned up five refinements to the approved ADR. They go into the ADR before any code.

**Files:**
- Modify: `docs/adr/0023-multi-process.md`
- Modify: `AGENTS.md` (the `## \`unsafe\` policy` list)

- [ ] **Step 1: Add an "Amendments (stage-1 planning, 2026-10-09)" section to ADR-0023, just above `## Consequences`**

```markdown
## Amendments (stage-1 planning, 2026-10-09)

1. **Shared reader slots move into stage 1.** Without them a writer in one
   process can reuse pages a reader in another process still holds, so
   cross-process tests would be unsound. Stage 1 claims, pins and releases
   shared slots (with the `pid` word); stage 2 adds liveness-based reaping and
   its loom models.
2. **POSIX `fcntl` record locks on both platforms** for the election (byte 0),
   liveness (byte `LIVENESS_BASE + pid`) and the macOS writer lock (byte 1),
   instead of OFD locks on Linux. One code path, LMDB's own choice, atomic
   exclusive→shared conversion. The per-process pitfall (closing any descriptor
   drops every lock) is closed by refusing a second in-process open of the same
   lock file *before* opening a descriptor.
3. **macOS writer lock is an `fcntl` write lock on byte 1**, not `flock`: on BSD
   systems `flock` and `fcntl` locks on one file may interact. POSIX locks are
   per process, so any thread may release it; the in-process writer lock
   already serializes threads.
4. **Meta reads use `pread`** (`Backing::read_meta_copy`), not a slice of the
   map: another process may be rewriting the slot, and a kernel copy avoids a
   data race on borrowed memory. A torn copy fails its CRC and is retried.
5. **A joining process seeds its snapshot cache from `T`**, never from the
   newest valid meta: a meta written but not yet published (or whose commit then
   failed) must never enter the cache.
6. **Stage 1 is opt-in** (`EnvOpenOptions::lock_file(true)`, default off) so the
   existing suite is untouched; stage 3 flips the default and updates the two
   tests that assert the single-process behavior.
```

- [ ] **Step 2: In AGENTS.md, add this bullet to the `unsafe` policy list, after the `zerodb-io` mmap bullet**

```markdown
- shared-memory synchronization primitives in `zerodb-io::lockfile`
  (ADR-0023): atomic views over the mapped lock file
  (`AtomicU64::from_ptr` / `AtomicU32::from_ptr`), the Linux `futex`
  syscall, and `fcntl` record locks. They are exposed to `zerodb-core` only
  through the safe `SharedRegion` trait;
```

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0023-multi-process.md AGENTS.md
git commit -m "ADR-0023: stage-1 planning amendments; unsafe policy for lock-file primitives"
```

---

### Task 2: `SharedRegion` seam and shared reader slots in `zerodb-core`

**Files:**
- Create: `crates/zerodb-core/src/shared.rs`
- Modify: `crates/zerodb-core/src/lib.rs` (add `pub mod shared;`)
- Modify: `crates/zerodb-core/src/readers.rs` (slot storage enum, pid on claim/release)
- Test: unit tests in `crates/zerodb-core/src/readers.rs` (`mod tests`) and `crates/zerodb-core/src/shared.rs`

**Interfaces:**
- Produces (used by Tasks 4 and 5):
  - `pub trait zerodb_core::shared::SharedRegion: Send + Sync`, with methods `published_txnid(&self) -> &AtomicU64`, `slot_count(&self) -> u32`, `slot_txnid(&self, i: u32) -> &AtomicU64`, `slot_pid(&self, i: u32) -> &AtomicU32`, `pid(&self) -> u32`, `is_initializer(&self) -> bool`, `lock_writer(&self) -> std::io::Result<()>`, `unlock_writer(&self)`.
  - `pub const zerodb_core::shared::READER_SLOT_FREE: u64` (= `readers::RDR_FREE`).
  - `pub(crate) fn ReaderTable::new_shared(region: Arc<dyn SharedRegion>) -> ReaderTable` (not under `cfg(loom)`).
  - `#[cfg(test)] pub(crate) struct shared::HeapRegion` (an in-process test implementation).

- [ ] **Step 1: Write `shared.rs` with the trait, the constant and the test region**

```rust
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
            txnids: (0..slots).map(|_| AtomicU64::new(READER_SLOT_FREE)).collect(),
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
```

Add `pub mod shared;` to `crates/zerodb-core/src/lib.rs`, next to the other `pub mod` lines. Make `RDR_FREE` in `readers.rs` `pub(crate)` if it isn't already.

- [ ] **Step 2: Write the failing reader-table tests, appended to `readers.rs` `mod tests`**

```rust
    #[test]
    fn shared_claim_records_pid_and_release_clears_it_first() {
        use crate::shared::{HeapRegion, SharedRegion};
        let region = Arc::new(HeapRegion::new(2, 4242, true));
        let t = ReaderTable::new_shared(region.clone());
        assert_eq!(t.capacity(), 2);
        let s = t.claim().expect("slot");
        assert_eq!(region.slot_pid(s).load(Ordering::SeqCst), 4242);
        assert_eq!(region.slot_txnid(s).load(Ordering::SeqCst), RDR_CLAIMED);
        t.store_pin(s, 7);
        assert_eq!(t.oldest(), Some(7));
        t.release(s);
        assert_eq!(region.slot_pid(s).load(Ordering::SeqCst), 0);
        assert_eq!(region.slot_txnid(s).load(Ordering::SeqCst), RDR_FREE);
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn shared_oldest_sees_slots_written_by_another_process() {
        use crate::shared::{HeapRegion, SharedRegion};
        let region = Arc::new(HeapRegion::new(3, 1, true));
        // Slot 2 pinned at txnid 5 by "another process" (written directly).
        region.slot_pid(2).store(99, Ordering::SeqCst);
        region.slot_txnid(2).store(5, Ordering::SeqCst);
        let t = ReaderTable::new_shared(region.clone());
        let mine = t.claim().expect("slot");
        assert_eq!(mine, 0);
        t.store_pin(mine, 9);
        assert_eq!(t.oldest(), Some(5));
    }

    #[test]
    fn shared_exhaustion_returns_none() {
        use crate::shared::HeapRegion;
        let t = ReaderTable::new_shared(Arc::new(HeapRegion::new(1, 1, true)));
        let _a = t.claim().expect("slot");
        assert!(t.claim().is_none());
    }
```

- [ ] **Step 3: Run them and confirm they fail**

Run: `cargo test -p zerodb-core --lib readers::tests::shared`
Expected: compile error, because `new_shared` is not defined.

- [ ] **Step 4: Implement the slot storage enum in `readers.rs`**

Replace the `slots` field and its uses:

```rust
/// Where the reader slots live: on this process's heap (the default), or in a
/// lock file shared with other processes (ADR-0023).
enum Slots {
    Local(Box<[CachePadded<AtomicU64>]>),
    // loom models the in-process protocol only; the shared region holds
    // `std` atomics that loom cannot instrument.
    #[cfg(not(loom))]
    Shared(std::sync::Arc<dyn crate::shared::SharedRegion>),
}

pub(crate) struct ReaderTable {
    slots: Slots,
    high_water: AtomicU64,
}

impl ReaderTable {
    pub(crate) fn new(max_readers: u32) -> ReaderTable {
        let slots = (0..max_readers)
            .map(|_| CachePadded::new(AtomicU64::new(RDR_FREE)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ReaderTable { slots: Slots::Local(slots), high_water: AtomicU64::new(0) }
    }

    /// A table whose slots live in a cross-process region (ADR-0023 §3). The
    /// claim, pin and release protocol is the same; claims also record the
    /// owner PID so a later stage can reap a dead process's slots.
    #[cfg(not(loom))]
    pub(crate) fn new_shared(region: std::sync::Arc<dyn crate::shared::SharedRegion>) -> ReaderTable {
        ReaderTable { slots: Slots::Shared(region), high_water: AtomicU64::new(0) }
    }

    fn slot(&self, i: usize) -> &AtomicU64 {
        match &self.slots {
            Slots::Local(s) => &s[i],
            #[cfg(not(loom))]
            Slots::Shared(r) => r.slot_txnid(i as u32),
        }
    }

    fn len(&self) -> usize {
        match &self.slots {
            Slots::Local(s) => s.len(),
            #[cfg(not(loom))]
            Slots::Shared(r) => r.slot_count() as usize,
        }
    }
```

Then rewrite `claim`, `store_pin`, `release`, `oldest` and `capacity` over `self.slot(i)` and `self.len()` (`for i in 0..self.len()`). Keep every existing ordering and comment. Add the PID handling:

In `claim`, right after the successful CAS and before `high_water.fetch_max`:

```rust
                #[cfg(not(loom))]
                if let Slots::Shared(r) = &self.slots {
                    // Release: a later reaper (ADR-0023 §3, stage 2) loads the
                    // PID with Acquire before judging the slot; this store
                    // must be visible no later than the pin that follows it.
                    r.slot_pid(i as u32).store(r.pid(), Ordering::Release);
                }
```

In `release`, before the existing `store(RDR_FREE, Release)`:

```rust
        #[cfg(not(loom))]
        if let Slots::Shared(r) = &self.slots {
            // PID cleared *before* the slot reads FREE (ADR-0023 §3): the FREE
            // store below is Release, so anyone who observes FREE also
            // observes PID 0, and a reaper can never attribute a re-claimed
            // slot to this (possibly later dead) process.
            r.slot_pid(slot).store(0, Ordering::Release);
        }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p zerodb-core --lib readers`
Expected: PASS, both the new tests and every existing `readers` test.

Run: `RUSTFLAGS="--cfg loom" cargo test -p zerodb-core --release --lib readers::loom` (or `just loom`)
Expected: PASS. The `Shared` variant is compiled out under loom.

- [ ] **Step 6: Commit**

```bash
git add crates/zerodb-core/src/shared.rs crates/zerodb-core/src/lib.rs crates/zerodb-core/src/readers.rs
git commit -m "zerodb-core: SharedRegion seam and shared reader slots (ADR-0023 stage 1)"
```

---

### Task 3: Decode a snapshot for a given txnid from a meta copy

**Files:**
- Modify: `crates/zerodb-core/src/env.rs` (new `Backing::read_meta_copy` default method, new `pub(crate) fn read_snapshot_at`)
- Modify: `crates/zerodb-io/src/lib.rs` (`MmapBacking` and `WriteMapBacking` override `read_meta_copy` with `pread`)
- Test: `crates/zerodb-core/src/env.rs` (`#[cfg(test)] mod shared_meta_tests`)

**Interfaces:**
- Produces:
  - `fn Backing::read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()>`. The default copies from `self.bytes()`.
  - `pub(crate) fn read_snapshot_at(backing: &dyn Backing, page_size: u32, t: u64) -> Result<Option<Snapshot>, Error>`. It returns `Ok(None)` when the slot no longer holds `t` or is torn.

- [ ] **Step 1: Write the failing tests at the end of `env.rs`**

```rust
#[cfg(test)]
mod shared_meta_tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};

    const PS: u32 = 4096;

    fn meta(txnid: u64, last_pg: u64) -> MetaPage {
        MetaPage {
            pgno: txnid & 1,
            txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: PS,
            env_flags: 0,
            map_size: 1 << 20,
            last_pg,
            free_db: DBRecord::empty(),
            main_db: DBRecord::empty(),
            fl_count: 0,
        }
    }

    /// Two meta slots plus `extra` zeroed data pages.
    fn image(m0: &MetaPage, m1: &MetaPage, extra: usize) -> VecBacking {
        let ps = PS as usize;
        let mut bytes = vec![0u8; ps * (2 + extra)];
        m0.encode_with_annex(&mut bytes[..ps], &[]).unwrap();
        m1.encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
        VecBacking(bytes)
    }

    #[test]
    fn reads_the_slot_holding_t() {
        let b = image(&meta(4, 2), &meta(5, 3), 2);
        let s = read_snapshot_at(&b, PS, 5).unwrap().expect("slot 1 holds 5");
        assert_eq!((s.txnid, s.last_pg), (5, 3));
        let s = read_snapshot_at(&b, PS, 4).unwrap().expect("slot 0 holds 4");
        assert_eq!(s.txnid, 4);
    }

    #[test]
    fn overwritten_slot_reads_as_none() {
        // Slot 1 was overwritten by txnid 7 while the caller pinned 5.
        let b = image(&meta(6, 2), &meta(7, 3), 2);
        assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
    }

    #[test]
    fn torn_slot_reads_as_none() {
        let mut b = image(&meta(4, 2), &meta(5, 3), 2);
        b.0[PS as usize + 100] ^= 0xFF; // corrupt slot 1 → CRC fails
        assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
    }

    #[test]
    fn root_beyond_last_pg_is_invalid() {
        let mut m = meta(5, 3);
        m.main_db.root = 50;
        let b = image(&meta(4, 2), &m, 2);
        assert!(matches!(
            read_snapshot_at(&b, PS, 5),
            Err(Error::Mdb(MdbError::Invalid))
        ));
    }
}
```

If `DBRecord::root` is not a public field, set the root the way `crates/zerodb-core/src/page/meta.rs` tests build a record with a root. Check those tests before writing this one.

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p zerodb-core --lib shared_meta_tests`
Expected: compile error, because `read_snapshot_at` is not defined.

- [ ] **Step 3: Add the `Backing` method**

Add it inside `pub trait Backing`, after `real_disk_size`:

```rust
    /// Copy meta slot `slot` (0 or 1) into `buf` (`psize` bytes). Readers of
    /// a lock-file environment (ADR-0023 §4) decode metas from this copy,
    /// never from a borrowed map slice: another process may be rewriting the
    /// slot, and a torn copy is caught by its CRC. The default copies from
    /// [`Backing::bytes`] (heap test backings); file backings use `pread`.
    ///
    /// # Errors
    ///
    /// The read error, or `UnexpectedEof` if the slot is outside the backing.
    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        let ps = psize as usize;
        let start = slot as usize * ps;
        let src = self
            .bytes()
            .get(start..start + ps)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        buf[..ps].copy_from_slice(src);
        Ok(())
    }
```

- [ ] **Step 4: Add `read_snapshot_at` next to `read_slot` in `env.rs`**

```rust
/// The snapshot committed as txnid `t`, decoded from a copy of meta slot
/// `t & 1` (ADR-0023 §4). `Ok(None)` means the slot no longer holds `t`: it
/// was overwritten by `t + 2`, or the copy was torn by a concurrent rewrite.
/// The caller re-reads the published txnid and retries. Geometry is checked
/// as at open (SPEC 06 REC-1a), against the file as it is now.
pub(crate) fn read_snapshot_at(
    backing: &dyn Backing,
    page_size: u32,
    t: u64,
) -> Result<Option<Snapshot>, Error> {
    let mut buf = vec![0u8; page_size as usize];
    backing
        .read_meta_copy(t & 1, page_size, &mut buf)
        .map_err(Error::Io)?;
    let meta = match MetaPage::validate(&buf, page_size) {
        Ok(MetaValidity::Valid(m)) => m,
        // A failed CRC is a torn copy of a slot being rewritten; retry.
        Ok(_) => return Ok(None),
        Err(_) => return Err(Error::Mdb(MdbError::Invalid)),
    };
    if meta.txnid != t {
        return Ok(None);
    }
    if meta.txnid > crate::readers::MAX_COMMITTED_TXNID {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    for rec in [&meta.main_db, &meta.free_db] {
        if rec.root != crate::page::PGNO_INVALID
            && (rec.root < crate::page::FIRST_DATA_PGNO || rec.root > meta.last_pg)
        {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }
    let annex = MetaPage::read_annex(&buf, page_size).ok_or(Error::Mdb(MdbError::Invalid))?;
    Ok(Some(Snapshot::from_meta(&meta, annex)))
}
```

- [ ] **Step 5: Override `read_meta_copy` with `pread` in `zerodb-io`**

In `crates/zerodb-io/src/file.rs`:

```rust
/// Positioned read of exactly `buf.len()` bytes at page `pgno`.
pub fn read_page_into(file: &File, pgno: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, pgno * u64::from(psize))
}
```

In `crates/zerodb-io/src/lib.rs`, in both `impl Backing for MmapBacking` and `impl Backing for WriteMapBacking`:

```rust
    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        // A kernel copy rather than a map slice (ADR-0023 §4): another process
        // may be rewriting this slot, and borrowing bytes under a concurrent
        // write would be a data race. A torn copy fails its CRC.
        file::read_page_into(&self.file, slot, psize, &mut buf[..psize as usize])
    }
```

If `WriteMapBacking` names its `File` field differently, use that field. `FaultBacking` in `fault.rs` forwards every method to the wrapped backing; forward this one too.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p zerodb-core --lib shared_meta_tests && cargo test -p zerodb-io`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/zerodb-core/src/env.rs crates/zerodb-io/src/lib.rs crates/zerodb-io/src/file.rs crates/zerodb-io/src/fault.rs
git commit -m "zerodb-core: decode a snapshot for a published txnid from a pread meta copy"
```

---

### Task 4: Wire the shared region into the environment

**Files:**
- Modify: `crates/zerodb-core/src/env.rs` (the `shared` field, `open_with_backing_shared`, `pin_reader`, `current_snapshot`, `txnid`, `publish_snapshot`, writer guard)
- Modify: `crates/zerodb-core/src/readers.rs` (`SnapshotCell::install_if_newer`)
- Modify: `crates/zerodb-core/src/rwtxn.rs` (`RwTxn::new` takes the cross-process lock and uses `current_snapshot`)
- Test: `crates/zerodb-core/src/env.rs` (`mod shared_env_tests`)

**Interfaces:**
- Consumes: `SharedRegion`, `ReaderTable::new_shared` (Task 2) and `read_snapshot_at` (Task 3).
- Produces (used by Task 6): `pub fn zerodb_core::env::open_with_backing_shared(<same args as open_with_backing_policy>, shared: Option<Arc<dyn SharedRegion>>) -> Result<Env, Error>`. `open_with_backing_policy` becomes a wrapper that passes `None`.

- [ ] **Step 1: Write the failing tests at the end of `env.rs`**

These tests use `testutil::VecBacking`, so they can pin, refresh and seed but cannot commit. Commits are covered in Task 7.

```rust
#[cfg(test)]
mod shared_env_tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};
    use crate::shared::{HeapRegion, SharedRegion};
    use std::sync::atomic::Ordering as O;

    const PS: u32 = 4096;

    fn meta(txnid: u64) -> MetaPage {
        MetaPage {
            pgno: txnid & 1, txnid, magic: MAGIC, format_version: FORMAT_VERSION,
            page_size: PS, env_flags: 0, map_size: 1 << 20, last_pg: 3,
            free_db: DBRecord::empty(), main_db: DBRecord::empty(), fl_count: 0,
        }
    }

    fn backing(t0: u64, t1: u64) -> Box<dyn Backing> {
        let ps = PS as usize;
        let mut bytes = vec![0u8; ps * 4];
        meta(t0).encode_with_annex(&mut bytes[..ps], &[]).unwrap();
        meta(t1).encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
        Box::new(VecBacking(bytes))
    }

    fn open(b: Box<dyn Backing>, region: Arc<dyn SharedRegion>) -> Env {
        let dir = std::env::temp_dir().join(format!("zerodb-shared-{}", next_env_id()));
        open_with_backing_shared(
            dir, b, PS, 1 << 20, false, 4, 4, DurabilityFlags::default(),
            crate::page::FileTrust::VALIDATE, false, None, Some(region),
        )
        .expect("open")
    }

    #[test]
    fn initializer_publishes_the_newest_meta_as_t() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        assert_eq!(region.published_txnid().load(O::SeqCst), 5);
        assert_eq!(env.inner.txnid(), 5);
    }

    #[test]
    fn joiner_seeds_from_published_txnid_not_newest_meta() {
        // Slot 1 holds txnid 5, written but not yet published: T is still 4.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        assert_eq!(env.inner.snapshot().txnid, 4);
        let (snap, slot) = env.inner.pin_reader().expect("pin");
        assert_eq!(snap.txnid, 4);
        assert_eq!(region.slot_txnid(slot).load(O::SeqCst), 4);
        env.inner.release_reader(slot);
    }

    #[test]
    fn pin_follows_t_published_by_another_process() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        // Another process publishes 5.
        region.published_txnid().store(5, O::SeqCst);
        let (snap, slot) = env.inner.pin_reader().expect("pin");
        assert_eq!(snap.txnid, 5);
        env.inner.release_reader(slot);
        // The cache was refreshed: the fast path now serves 5.
        assert_eq!(env.inner.snapshot().txnid, 5);
    }

    #[test]
    fn current_snapshot_follows_t() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        region.published_txnid().store(5, O::SeqCst);
        assert_eq!(env.inner.current_snapshot().unwrap().txnid, 5);
    }
}
```

If `DurabilityFlags` has no `Default`, build it field by field as `crates/zerodb/src/lib.rs` does, with every flag false.

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p zerodb-core --lib shared_env_tests`
Expected: compile error, because `open_with_backing_shared` and `current_snapshot` are not defined.

- [ ] **Step 3: Add `install_if_newer` to `SnapshotCell` in `readers.rs`**

```rust
    /// Replace the cached snapshot with `snap` if it is newer (ADR-0023 §4:
    /// a snapshot decoded after another process committed). Only snapshots
    /// validated against the published txnid are ever passed here. Does not
    /// touch `commit_point`, which lock-file environments do not pin against.
    pub(crate) fn install_if_newer(&self, snap: Arc<Snapshot>) {
        let old = {
            let mut g = self.cell.lock().expect("snapshot cell poisoned");
            if snap.txnid <= g.txnid {
                return;
            }
            std::mem::replace(&mut *g, snap)
        };
        drop(old);
    }
```

- [ ] **Step 4: Add the `shared` field and the open entry point in `env.rs`**

Add the field to `EnvInner`:

```rust
    /// The cross-process region when opened with a lock file (ADR-0023);
    /// `None` keeps the single-process path unchanged.
    shared: Option<Arc<dyn crate::shared::SharedRegion>>,
```

Rename the body of `open_with_backing_policy` into `open_with_backing_shared`, which takes one extra trailing parameter `shared: Option<Arc<dyn crate::shared::SharedRegion>>`. `open_with_backing_policy` keeps its exact signature and calls it with `None`.

In `open_with_backing_shared`, after the geometry checks and before building `EnvInner`, choose the seed snapshot:

```rust
    let seed = match &shared {
        // A joining process seeds from the published txnid, never from the
        // newest valid meta: that meta may belong to a commit still in flight
        // or one that failed, whose txnid the next writer reuses (TXN-2;
        // ADR-0023 amendment 5).
        Some(region) if !region.is_initializer() => {
            seed_from_published(backing.as_ref(), page_size, region.as_ref())?
        }
        _ => Snapshot::from_meta(&meta, annex),
    };
```

Then use `SnapshotCell::new(Arc::new(seed))`, plus:

```rust
        reader_table: match &shared {
            Some(region) => ReaderTable::new_shared(Arc::clone(region)),
            None => ReaderTable::new(max_readers),
        },
        shared: shared.clone(),
```

Right after the `EnvInner` is built, and before registry insertion:

```rust
    if let Some(region) = &shared {
        if region.is_initializer() {
            // SeqCst: the same total order as every later T store and
            // every reader's publish-and-verify (SPEC 04 TXN-17/19).
            region.published_txnid().store(inner.snapshot().txnid, Ordering::SeqCst);
        }
    }
```

In shared mode `max_readers` comes from the region; Task 5 validates that it matches. Add the helper:

```rust
/// Bound on re-reads of the published txnid when its meta slot keeps changing
/// underneath (ADR-0023 §4). Each retry means a commit landed or a slot was
/// mid-rewrite; a slot that never decodes is a corrupt file.
const META_RETRY_LIMIT: u32 = 10_000;

fn seed_from_published(
    backing: &dyn Backing,
    page_size: u32,
    region: &dyn crate::shared::SharedRegion,
) -> Result<Snapshot, Error> {
    for _ in 0..META_RETRY_LIMIT {
        // SeqCst: pairs with the committing writer's SeqCst store of T.
        let t = region.published_txnid().load(Ordering::SeqCst);
        if let Some(s) = read_snapshot_at(backing, page_size, t)? {
            return Ok(s);
        }
        std::thread::yield_now();
    }
    Err(Error::Mdb(MdbError::Invalid))
}
```

- [ ] **Step 5: Shared pin, `current_snapshot`, `txnid` and publish**

Replace `pin_reader`:

```rust
    pub(crate) fn pin_reader(&self) -> Result<(Arc<Snapshot>, u32), Error> {
        let Some(region) = &self.shared else {
            return self
                .snap_cell
                .pin(&self.reader_table)
                .ok_or(Error::Mdb(MdbError::ReadersFull));
        };
        let slot = self.reader_table.claim().ok_or(Error::Mdb(MdbError::ReadersFull))?;
        for _ in 0..META_RETRY_LIMIT {
            // TXN-17 publish-and-verify against the shared T: once verified,
            // no writer in any process can reclaim pages of snapshot t.
            let t = self.reader_table.publish_and_verify(slot, region.published_txnid());
            match self.snapshot_at(t) {
                Ok(Some(snap)) => return Ok((snap, slot)),
                Ok(None) => std::thread::yield_now(),
                Err(e) => {
                    self.reader_table.release(slot);
                    return Err(e);
                }
            }
        }
        self.reader_table.release(slot);
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// The snapshot for published txnid `t`: the cached one when it matches
    /// (the single-process fast path), else decoded from the data file and
    /// cached. `None` when meta slot `t & 1` has moved on.
    fn snapshot_at(&self, t: u64) -> Result<Option<Arc<Snapshot>>, Error> {
        let cached = self.snap_cell.clone_snapshot();
        if cached.txnid == t {
            return Ok(Some(cached));
        }
        let Some(snap) = read_snapshot_at(self.backing_ref(), self.page_size, t)? else {
            return Ok(None);
        };
        // This process's map must cover the snapshot. Another process with a
        // larger map_size could have grown the file past it. Stage 3 maps
        // this to MapResized after observing LMDB.
        let needed = snap
            .last_pg
            .checked_add(1)
            .and_then(|n| n.checked_mul(u64::from(self.page_size)))
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        if needed > self.backing_ref().bytes().len() as u64 {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let snap = Arc::new(snap);
        self.snap_cell.install_if_newer(Arc::clone(&snap));
        Ok(Some(snap))
    }

    /// The latest published snapshot. In a lock-file environment it follows
    /// the shared T, so it includes other processes' commits (ADR-0023 §4);
    /// otherwise it is [`EnvInner::snapshot`].
    pub(crate) fn current_snapshot(&self) -> Result<Arc<Snapshot>, Error> {
        let Some(region) = &self.shared else {
            return Ok(self.snapshot());
        };
        for _ in 0..META_RETRY_LIMIT {
            let t = region.published_txnid().load(Ordering::SeqCst);
            if let Some(s) = self.snapshot_at(t)? {
                return Ok(s);
            }
            std::thread::yield_now();
        }
        Err(Error::Mdb(MdbError::Invalid))
    }
```

Change `txnid()`:

```rust
    pub fn txnid(&self) -> u64 {
        match &self.shared {
            // SeqCst: the same total order as the T stores (TXN-17/19).
            Some(region) => region.published_txnid().load(Ordering::SeqCst),
            None => self.snap_cell.commit_point(),
        }
    }
```

Change `publish_snapshot`:

```rust
    pub(crate) fn publish_snapshot(&self, snap: Arc<Snapshot>) {
        let txnid = snap.txnid;
        self.snap_cell.publish(snap);
        if let Some(region) = &self.shared {
            // ADR-0023 §4: T is stored after the meta write (and its fsync in
            // the durable modes, C4/C5), and after the cache swap (TXN-19
            // object-before-counter). SeqCst pairs with every reader's
            // publish-and-verify and with the writer's SeqCst slot scan,
            // across processes. Only the cross-process writer-lock holder
            // advances T.
            region.published_txnid().store(txnid, Ordering::SeqCst);
        }
    }
```

- [ ] **Step 6: Cross-process writer guard**

In `WriterGuard`, add `shared: Option<&'env dyn crate::shared::SharedRegion>` (initialized to `None` in `WriterLock::acquire`). Add a method to `EnvInner`:

```rust
    /// Take the cross-process writer lock on top of the in-process one
    /// (ADR-0023 §2); a no-op without a lock file.
    pub(crate) fn lock_writer_shared<'a>(&'a self, guard: &mut WriterGuard<'a>) -> Result<(), Error> {
        if let Some(region) = &self.shared {
            region.lock_writer().map_err(Error::Io)?;
            guard.shared = Some(region.as_ref());
        }
        Ok(())
    }
```

At the start of `impl Drop for WriterGuard`:

```rust
        // Cross-process lock first, then the in-process slot: a thread of this
        // process woken by the slot release then competes fairly with other
        // processes for the shared lock.
        if let Some(region) = self.shared.take() {
            region.unlock_writer();
        }
```

`Env::sync` keeps calling `lock_writer()` alone. An fsync needs no cross-process exclusion.

- [ ] **Step 7: `RwTxn::new` uses both**

In `crates/zerodb-core/src/rwtxn.rs`, replace

```rust
        let mut guard = inner.lock_writer();
```

with

```rust
        let mut guard = inner.lock_writer();
        inner.lock_writer_shared(&mut guard)?;
```

and

```rust
        let base = inner.snapshot();
```

with

```rust
        // The latest published snapshot, which in a lock-file env may be
        // another process's commit (ADR-0023 §4): never the process-local cell.
        let base = inner.current_snapshot()?;
```

- [ ] **Step 8: Run the tests**

Run: `cargo test -p zerodb-core`
Expected: PASS, the new `shared_env_tests` and every existing test.

Run: `cargo +nightly miri test -p zerodb-core --lib shared`
Expected: PASS.

Run: `just loom`
Expected: PASS. Under `cfg(loom)`, `crate::sync::AtomicU64` is loom's type while `SharedRegion` returns `std` atomics. If `env.rs` fails to compile under loom, gate the `shared` field, `seed_from_published`, `snapshot_at`'s region use and the shared branches of `pin_reader`, `current_snapshot`, `txnid`, `publish_snapshot` and `lock_writer_shared` with `#[cfg(not(loom))]`, keeping the single-process branch as the loom build. Do not change what the loom models check.

- [ ] **Step 9: Commit**

```bash
git add crates/zerodb-core/src
git commit -m "zerodb-core: pin, refresh and publish through the shared txnid; cross-process writer guard"
```

---

### Task 5: `LockFile` in `zerodb-io`

**Files:**
- Create: `crates/zerodb-io/src/lockfile.rs`
- Modify: `crates/zerodb-io/src/lib.rs` (`pub mod lockfile;`, module doc names the new `unsafe` home)
- Test: `crates/zerodb-io/src/lockfile.rs` (`mod tests`)

**Interfaces:**
- Consumes: `zerodb_core::shared::{SharedRegion, READER_SLOT_FREE}`.
- Produces (used by Task 6):
  - `pub struct LockFile` implementing `SharedRegion`.
  - `pub fn LockFile::open(path: &Path, max_readers: u32) -> Result<Arc<LockFile>, LockOpenError>`.
  - `pub fn LockFile::bind_page_size(&self, page_size: u32) -> std::io::Result<()>`.
  - `pub fn LockFile::finish_open(&self) -> std::io::Result<()>`.
  - `pub enum LockOpenError { AlreadyOpen, Mismatch(String), Io(std::io::Error) }`.
  - `pub fn LockFile::is_alive(&self, pid: u32) -> std::io::Result<bool>`.

- [ ] **Step 1: Write the failing in-process tests**

The multi-process cases are in Task 7.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "zerodb-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p.join("zerodb.lock")
    }

    #[test]
    fn first_opener_initializes_every_slot_free() {
        let path = tmp();
        let l = LockFile::open(&path, 8).expect("open");
        assert!(l.is_initializer());
        assert_eq!(l.slot_count(), 8);
        for i in 0..8 {
            assert_eq!(l.slot_txnid(i).load(Ordering::SeqCst), READER_SLOT_FREE);
            assert_eq!(l.slot_pid(i).load(Ordering::SeqCst), 0);
        }
        assert_eq!(l.published_txnid().load(Ordering::SeqCst), 0);
        assert_eq!(l.pid(), std::process::id());
        l.bind_page_size(4096).unwrap();
        l.finish_open().unwrap();
    }

    #[test]
    fn second_open_in_process_is_refused_before_any_fd() {
        let path = tmp();
        let a = LockFile::open(&path, 4).expect("open");
        assert!(matches!(LockFile::open(&path, 4), Err(LockOpenError::AlreadyOpen)));
        // `a` still holds its locks and works.
        a.lock_writer().unwrap();
        a.unlock_writer();
        drop(a);
        // After the first handle is gone, the path can be opened again.
        LockFile::open(&path, 4).expect("reopen");
    }

    #[test]
    fn writer_lock_releases_from_another_thread() {
        let path = tmp();
        let l = LockFile::open(&path, 4).expect("open");
        l.lock_writer().unwrap();
        let l2 = Arc::clone(&l);
        std::thread::spawn(move || l2.unlock_writer()).join().unwrap();
        l.lock_writer().unwrap();
        l.unlock_writer();
    }

    #[test]
    fn page_size_rebind_with_another_value_fails() {
        let path = tmp();
        let l = LockFile::open(&path, 4).expect("open");
        l.bind_page_size(4096).unwrap();
        assert!(l.bind_page_size(8192).is_err());
    }
}
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p zerodb-io lockfile`
Expected: compile error, because the module is missing.

- [ ] **Step 3: Write `lockfile.rs`**

```rust
//! The shared lock file of a multi-process environment (ADR-0023).
//!
//! Layout, by byte offset (all fields little-endian atomics; the map is
//! page-aligned and every offset below is a multiple of its field's size):
//!
//! | offset | field |
//! |---|---|
//! | 0 | magic `u64` (`LOCK_MAGIC`, stored last by the initializer) |
//! | 8 | lock-format version `u32` |
//! | 12 | data page size `u32` (0 until bound) |
//! | 16 | `max_readers` `u32` |
//! | 64 | published txnid `T` `u64` (own cache line) |
//! | 128 | writer-lock word `u32`: owner PID, bit 31 = waiters (Linux) |
//! | 192 + i·128 | reader slot `i`: txnid `u64` at +0, owner PID `u32` at +8 |
//!
//! Record locks (POSIX `fcntl`, per process; ADR-0023 amendment 2): byte 0 is
//! the first-opener election, byte 1 the macOS writer lock, and byte
//! `LIVENESS_BASE + pid` each process's liveness lock.
//!
//! The `unsafe` here is the sanctioned shared-memory category of the AGENTS.md
//! policy: atomic views over the mapping, `fcntl`, and the Linux `futex`.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use zerodb_core::shared::{SharedRegion, READER_SLOT_FREE};

const LOCK_MAGIC: u64 = u64::from_le_bytes(*b"ZDBLOCK1");
const LOCK_FORMAT: u32 = 1;
const OFF_MAGIC: usize = 0;
const OFF_FORMAT: usize = 8;
const OFF_PAGE_SIZE: usize = 12;
const OFF_MAX_READERS: usize = 16;
const OFF_T: usize = 64;
const OFF_WRITER: usize = 128;
const OFF_SLOTS: usize = 192;
const SLOT_STRIDE: usize = 128;
/// Record-lock offset of the election byte.
const ELECTION_BYTE: i64 = 0;
/// Record-lock offset of the macOS writer lock.
#[cfg(not(target_os = "linux"))]
const WRITER_BYTE: i64 = 1;
/// Record-lock offset base of the per-PID liveness locks. Far above the
/// mapped size, so PIDs never collide with bytes 0 and 1.
const LIVENESS_BASE: i64 = 1 << 32;
/// Writer-word bit set while a process sleeps on the futex (Linux).
#[cfg(target_os = "linux")]
const WAITERS: u32 = 1 << 31;
/// How long a waiting writer sleeps before checking the owner is alive.
#[cfg(target_os = "linux")]
const OWNER_PROBE: std::time::Duration = std::time::Duration::from_millis(100);

/// Why [`LockFile::open`] failed.
#[derive(Debug)]
pub enum LockOpenError {
    /// This process already has the lock file open. A second descriptor would
    /// drop the first one's record locks when closed.
    AlreadyOpen,
    /// The lock file was initialized with incompatible parameters.
    Mismatch(String),
    /// An I/O error.
    Io(std::io::Error),
}

impl From<std::io::Error> for LockOpenError {
    fn from(e: std::io::Error) -> Self {
        LockOpenError::Io(e)
    }
}

fn open_paths() -> &'static Mutex<HashSet<PathBuf>> {
    static PATHS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// The mapped lock file. Field order matters for `Drop`: the map is
/// unmapped before the descriptor closes, and closing the descriptor
/// releases this process's record locks.
pub struct LockFile {
    map: memmap2::MmapRaw,
    file: File,
    path: PathBuf,
    pid: u32,
    initializer: bool,
    slots: u32,
}

impl LockFile {
    /// Open or create the lock file at `path` and join the environment
    /// (ADR-0023 §1). The first opener (no other process alive) initializes
    /// the file and holds the election byte exclusively until
    /// [`LockFile::finish_open`]; later openers wait for that, then validate.
    ///
    /// # Errors
    ///
    /// [`LockOpenError::AlreadyOpen`] if this process has it open,
    /// [`LockOpenError::Mismatch`] on a format or `max_readers` mismatch, and
    /// I/O errors.
    pub fn open(path: &Path, max_readers: u32) -> Result<Arc<LockFile>, LockOpenError> {
        // Claimed before any descriptor exists: POSIX record locks belong to
        // the process, and closing a second descriptor would drop the first
        // handle's locks (ADR-0023 amendment 2).
        if !open_paths().lock().expect("lock registry poisoned").insert(path.to_path_buf()) {
            return Err(LockOpenError::AlreadyOpen);
        }
        match Self::open_registered(path, max_readers) {
            Ok(l) => Ok(l),
            Err(e) => {
                open_paths().lock().expect("lock registry poisoned").remove(path);
                Err(e)
            }
        }
    }

    fn open_registered(path: &Path, max_readers: u32) -> Result<Arc<LockFile>, LockOpenError> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let initializer = record_lock(&file, libc::F_SETLK, libc::F_WRLCK as i16, ELECTION_BYTE)?;
        if !initializer {
            // Blocks until the initializer downgrades in `finish_open`.
            record_lock(&file, libc::F_SETLKW, libc::F_RDLCK as i16, ELECTION_BYTE)?;
        }
        let slots = if initializer { max_readers } else { read_u32_at(&file, OFF_MAX_READERS)? };
        let len = OFF_SLOTS + SLOT_STRIDE * slots as usize;
        if initializer {
            // Truncate to zero first so no field survives from a previous life.
            file.set_len(0)?;
            file.set_len(len as u64)?;
        } else if (file.metadata()?.len() as usize) < len {
            return Err(LockOpenError::Mismatch("lock file shorter than its header says".into()));
        }
        let map = memmap2::MmapOptions::new().len(len).map_raw(&file)?;
        let pid = std::process::id();
        let lock = LockFile { map, file, path: path.to_path_buf(), pid, initializer, slots };
        if initializer {
            for i in 0..slots {
                // Relaxed: published by the Release store of the magic below,
                // and no other process can map the file before the election
                // byte is downgraded.
                lock.slot_txnid(i).store(READER_SLOT_FREE, Ordering::Relaxed);
                lock.slot_pid(i).store(0, Ordering::Relaxed);
            }
            lock.u32_at(OFF_FORMAT).store(LOCK_FORMAT, Ordering::Relaxed);
            lock.u32_at(OFF_MAX_READERS).store(slots, Ordering::Relaxed);
            // Release: every field above is visible to a joiner that loads the
            // magic with Acquire.
            lock.u64_at(OFF_MAGIC).store(LOCK_MAGIC, Ordering::Release);
        } else {
            // Acquire: pairs with the initializer's Release store of the magic.
            if lock.u64_at(OFF_MAGIC).load(Ordering::Acquire) != LOCK_MAGIC
                || lock.u32_at(OFF_FORMAT).load(Ordering::Relaxed) != LOCK_FORMAT
            {
                return Err(LockOpenError::Mismatch("not a ZeroDB lock file of this version".into()));
            }
            if slots != max_readers {
                return Err(LockOpenError::Mismatch(format!(
                    "max_readers {max_readers} differs from the running environment's {slots}"
                )));
            }
        }
        // This process's liveness lock (ADR-0023 §3), held for the handle's life.
        if !record_lock(&lock.file, libc::F_SETLK, libc::F_WRLCK as i16, LIVENESS_BASE + i64::from(pid))? {
            return Err(LockOpenError::Io(std::io::Error::other("liveness lock already held")));
        }
        Ok(Arc::new(lock))
    }

    /// Record the data page size (initializer) or check it (joiner).
    ///
    /// # Errors
    ///
    /// `InvalidData` when the running environment uses another page size.
    pub fn bind_page_size(&self, page_size: u32) -> std::io::Result<()> {
        let cell = self.u32_at(OFF_PAGE_SIZE);
        // AcqRel/Acquire: the bound value is read by joiners after the
        // election downgrade; the CAS makes a re-bind with another value fail.
        match cell.compare_exchange(0, page_size, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(cur) if cur == page_size => Ok(()),
            Err(cur) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("page size {page_size} differs from the running environment's {cur}"),
            )),
        }
    }

    /// Let other processes in: the initializer converts its exclusive
    /// election lock to a shared one (atomic for POSIX locks). No-op for a
    /// joiner, which already holds it shared.
    ///
    /// # Errors
    ///
    /// The `fcntl` error.
    pub fn finish_open(&self) -> std::io::Result<()> {
        if self.initializer {
            record_lock(&self.file, libc::F_SETLK, libc::F_RDLCK as i16, ELECTION_BYTE)?;
        }
        Ok(())
    }

    /// Whether process `pid` currently has this environment open (holds its
    /// liveness lock). Immune to PID reuse: a recycled PID holds no lock.
    ///
    /// # Errors
    ///
    /// The `fcntl` error.
    pub fn is_alive(&self, pid: u32) -> std::io::Result<bool> {
        if pid == self.pid {
            return Ok(true);
        }
        probe_locked(&self.file, LIVENESS_BASE + i64::from(pid))
    }

    fn u64_at(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off % 8 == 0 && off + 8 <= self.map.len());
        // SAFETY: the map is page-aligned and `off` is a multiple of 8 within
        // its length, so the pointer is aligned and in bounds. The mapping
        // lives as long as `self` and is never remapped, so the reference
        // cannot outlive it. Every access to these bytes, in this and every
        // other process, goes through atomics, so there is no non-atomic data
        // race.
        unsafe { AtomicU64::from_ptr(self.map.as_mut_ptr().add(off).cast()) }
    }

    fn u32_at(&self, off: usize) -> &AtomicU32 {
        debug_assert!(off % 4 == 0 && off + 4 <= self.map.len());
        // SAFETY: as `u64_at`, for a 4-aligned in-bounds offset.
        unsafe { AtomicU32::from_ptr(self.map.as_mut_ptr().add(off).cast()) }
    }

    fn writer_word(&self) -> &AtomicU32 {
        self.u32_at(OFF_WRITER)
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        // The map and descriptor drop after this body; closing the descriptor
        // releases the election and liveness locks.
        open_paths().lock().expect("lock registry poisoned").remove(&self.path);
    }
}

impl SharedRegion for LockFile {
    fn published_txnid(&self) -> &AtomicU64 {
        self.u64_at(OFF_T)
    }
    fn slot_count(&self) -> u32 {
        self.slots
    }
    fn slot_txnid(&self, i: u32) -> &AtomicU64 {
        assert!(i < self.slots, "reader slot out of range");
        self.u64_at(OFF_SLOTS + i as usize * SLOT_STRIDE)
    }
    fn slot_pid(&self, i: u32) -> &AtomicU32 {
        assert!(i < self.slots, "reader slot out of range");
        self.u32_at(OFF_SLOTS + i as usize * SLOT_STRIDE + 8)
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn is_initializer(&self) -> bool {
        self.initializer
    }

    #[cfg(target_os = "linux")]
    fn lock_writer(&self) -> std::io::Result<()> {
        let w = self.writer_word();
        loop {
            // Acquire: synchronizes with the previous owner's Release in
            // `unlock_writer`, so its T store and meta write are visible.
            let cur = match w.compare_exchange(0, self.pid, Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => return Ok(()),
                Err(cur) => cur,
            };
            let owner = cur & !WAITERS;
            if cur & WAITERS == 0
                && w.compare_exchange(cur, cur | WAITERS, Ordering::Relaxed, Ordering::Relaxed).is_err()
            {
                continue; // the word changed; re-read it
            }
            let timed_out = futex_wait(w, cur | WAITERS, OWNER_PROBE);
            if timed_out && !self.is_alive(owner)? {
                // The owner died holding the lock (ADR-0023 §2): take it over.
                // Acquire as above. The waiters bit is kept so this owner's
                // release wakes the others.
                let now = w.load(Ordering::Relaxed);
                if now & !WAITERS == owner
                    && w
                        .compare_exchange(now, self.pid | (now & WAITERS), Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    return Ok(());
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn unlock_writer(&self) {
        // Release: publishes this writer's T store and meta write to the next
        // owner's Acquire.
        let old = self.writer_word().swap(0, Ordering::Release);
        if old & WAITERS != 0 {
            futex_wake_all(self.writer_word());
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn lock_writer(&self) -> std::io::Result<()> {
        // POSIX record locks belong to the process, so any thread may release
        // it, and the kernel drops it if the process dies (ADR-0023 §2).
        record_lock(&self.file, libc::F_SETLKW, libc::F_WRLCK as i16, WRITER_BYTE).map(|_| ())
    }

    #[cfg(not(target_os = "linux"))]
    fn unlock_writer(&self) {
        // An unlock cannot fail for a lock this process holds. A failure here
        // would leave other processes blocked, so it is fatal.
        record_lock(&self.file, libc::F_SETLK, libc::F_UNLCK as i16, WRITER_BYTE)
            .expect("release the cross-process writer lock");
    }
}

/// Set or clear a one-byte POSIX record lock at `start`. Returns `Ok(false)`
/// when a non-blocking `F_SETLK` finds the byte held by another process.
fn record_lock(file: &File, cmd: libc::c_int, kind: i16, start: i64) -> std::io::Result<bool> {
    let mut fl: libc::flock = unsafe_zeroed_flock();
    fl.l_type = kind as _;
    fl.l_whence = libc::SEEK_SET as _;
    fl.l_start = start as _;
    fl.l_len = 1;
    loop {
        // SAFETY: `file` is an open descriptor for the call's duration and
        // `fl` is a valid, initialized `flock` that outlives the call.
        let r = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &fl as *const libc::flock) };
        if r == 0 {
            return Ok(true);
        }
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) | Some(libc::EACCES) if cmd == libc::F_SETLK => return Ok(false),
            _ => return Err(e),
        }
    }
}

/// Whether another process holds a write lock on byte `start`.
fn probe_locked(file: &File, start: i64) -> std::io::Result<bool> {
    let mut fl: libc::flock = unsafe_zeroed_flock();
    fl.l_type = libc::F_WRLCK as _;
    fl.l_whence = libc::SEEK_SET as _;
    fl.l_start = start as _;
    fl.l_len = 1;
    // SAFETY: as in `record_lock`; F_GETLK writes the result into `fl`.
    let r = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut fl as *mut libc::flock) };
    if r != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fl.l_type != libc::F_UNLCK as _)
}

fn unsafe_zeroed_flock() -> libc::flock {
    // SAFETY: `flock` is a plain C struct of integers, for which all-zero bytes
    // are a valid value.
    unsafe { std::mem::zeroed() }
}

fn read_u32_at(file: &File, off: usize) -> std::io::Result<u32> {
    use std::os::unix::fs::FileExt;
    let mut b = [0u8; 4];
    file.read_exact_at(&mut b, off as u64)?;
    Ok(u32::from_le_bytes(b))
}

/// Sleep while `word == expected`, at most `timeout`. Returns `true` on timeout.
#[cfg(target_os = "linux")]
fn futex_wait(word: &AtomicU32, expected: u32, timeout: std::time::Duration) -> bool {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as _,
        tv_nsec: timeout.subsec_nanos() as _,
    };
    // SAFETY: `word` is a live, 4-aligned u32 in this lock file's MAP_SHARED
    // mapping, which outlives the call. FUTEX_WAIT without the PRIVATE flag
    // keys on the physical page, so waiters in other processes match. `ts`
    // lives on the stack across the call. The kernel only reads both.
    let r = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
            std::ptr::null::<u32>(),
            0u32,
        )
    };
    r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ETIMEDOUT)
}

/// Wake every process sleeping on `word`.
#[cfg(target_os = "linux")]
fn futex_wake_all(word: &AtomicU32) {
    // SAFETY: as `futex_wait`; FUTEX_WAKE only uses the address as a key.
    unsafe {
        libc::syscall(libc::SYS_futex, word.as_ptr(), libc::FUTEX_WAKE, i32::MAX, 0usize, 0usize, 0u32);
    }
}
```

Joiner subtlety: the joiner reads `max_readers` with `read_u32_at` (`pread`) *before* mapping. That is safe because it already holds the shared election lock, so the initializer has finished. If `unsafe_zeroed_flock` trips clippy's naming lints, rename it `zeroed_flock`.

- [ ] **Step 4: Register the module**

In `crates/zerodb-io/src/lib.rs`, add `pub mod lockfile;`. Then extend the crate doc's list of `unsafe` homes with: "the shared-memory primitives in [`lockfile`] (atomic views over the lock file, `fcntl` record locks, the Linux futex; ADR-0023)".

- [ ] **Step 5: Run the tests and both lint targets**

Run: `cargo test -p zerodb-io lockfile`
Expected: PASS.

Run: `cargo clippy -p zerodb-io --all-targets -- -D warnings && cargo clippy -p zerodb-io --all-targets --target x86_64-unknown-linux-gnu -- -D warnings`
Expected: clean. The second command compiles the futex path.

- [ ] **Step 6: Commit**

```bash
git add crates/zerodb-io/src/lockfile.rs crates/zerodb-io/src/lib.rs
git commit -m "zerodb-io: shared lock file — election, liveness locks, PID-owner futex writer lock"
```

---

### Task 6: Public options and open wiring

**Files:**
- Modify: `crates/zerodb/src/lib.rs` (`EnvOpenOptions::lock_file`, `lock_file_name`, `LOCK_FILE_NAME`, open path)
- Modify: `crates/heed-zerodb/src/env.rs` (extension `lock_file(bool)`, which uses the name `lock.mdb`)
- Modify: `crates/zerodb-oracle/benches/engine_comparison/backend.rs` (the `ZERODB_BENCH_LOCK_FILE=1` axis)
- Test: `crates/zerodb/tests/lock_file_single_process.rs`

**Interfaces:**
- Consumes: `LockFile` (Task 5) and `open_with_backing_shared` (Task 4).
- Produces (used by Task 7):
  - `pub fn zerodb::EnvOpenOptions::lock_file(&mut self, on: bool) -> &mut Self`.
  - `pub fn zerodb::EnvOpenOptions::lock_file_name(&mut self, name: impl Into<OsString>) -> &mut Self`.
  - `pub const zerodb::LOCK_FILE_NAME: &str = "zerodb.lock"`.

- [ ] **Step 1: Write the failing single-process test**

Create `crates/zerodb/tests/lock_file_single_process.rs`. Copy the `TempDir` helper from `env_lifecycle.rs`, including its `Drop` impl, then add:

```rust
use zerodb::{EnvOpenOptions, Error, LOCK_FILE_NAME};

fn opts() -> EnvOpenOptions {
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).page_size(4096).lock_file(true);
    o
}

#[test]
fn lock_file_env_reads_and_writes_like_the_default() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(dir.path().join(LOCK_FILE_NAME).exists());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
}

#[test]
fn default_env_creates_no_lock_file() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20);
    let _env = o.open(dir.path()).expect("open");
    assert!(!dir.path().join(LOCK_FILE_NAME).exists());
}

#[test]
fn double_open_in_process_keeps_first_env_working() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(matches!(opts().open(dir.path()), Err(Error::EnvAlreadyOpened)));
    let db = env.main_database();
    let mut w = env.write_txn().expect("first env still writes");
    db.put(&mut w, b"a", b"1").unwrap();
    w.commit().unwrap();
}

#[test]
fn reopen_after_close_reinitializes() {
    let dir = TempDir::new();
    {
        let env = opts().open(dir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"x", b"1").unwrap();
        w.commit().unwrap();
    }
    let env = opts().open(dir.path()).unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"x").unwrap(), Some(&b"1"[..]));
}
```

Before writing the tests, check the exact method names (`main_database`, chaining on the setters) against `crates/zerodb/tests/concurrency_smoke.rs`.

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p zerodb --test lock_file_single_process`
Expected: compile error, because `lock_file` is not defined.

- [ ] **Step 3: Implement the options and open path in `crates/zerodb/src/lib.rs`**

Add fields `lock_file: bool` (default `false`) and `lock_file_name: OsString` (default `LOCK_FILE_NAME`). Add the constant and the setters:

```rust
/// The native lock-file name of a multi-process environment (ADR-0023).
pub const LOCK_FILE_NAME: &str = "zerodb.lock";

    /// Let other processes open this environment at the same time, through a
    /// shared lock file next to the data file (ADR-0023). Off by default in
    /// this release; the heed adapter maps LMDB's default to it.
    pub fn lock_file(&mut self, on: bool) -> &mut EnvOpenOptions {
        self.lock_file = on;
        self
    }

    /// The lock file's name inside the environment directory: a single path
    /// component. Default [`LOCK_FILE_NAME`]; the heed adapter uses `lock.mdb`.
    pub fn lock_file_name(&mut self, name: impl Into<OsString>) -> &mut EnvOpenOptions {
        self.lock_file_name = name.into();
        self
    }
```

In `open`, validate `lock_file_name` with `is_single_component`, exactly as for `data_file_name`. Then, after `canonical_dir`, and before `open_or_create_with_advice`, so a joiner waits for the initializer before touching the data file:

```rust
        let lock = if self.lock_file {
            let path = canonical_dir.join(&self.lock_file_name);
            match zerodb_io::lockfile::LockFile::open(&path, self.max_readers) {
                Ok(l) => Some(l),
                Err(zerodb_io::lockfile::LockOpenError::AlreadyOpen) => {
                    return Err(Error::EnvAlreadyOpened)
                }
                Err(zerodb_io::lockfile::LockOpenError::Mismatch(m)) => {
                    return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, m)))
                }
                Err(zerodb_io::lockfile::LockOpenError::Io(e)) => return Err(Error::Io(e)),
            }
        } else {
            None
        };
```

After `open_or_create_with_advice` returns `opened`:

```rust
        if let Some(l) = &lock {
            l.bind_page_size(opened.page_size).map_err(Error::Io)?;
        }
```

Replace the `open_with_backing_policy(...)` call with `open_with_backing_shared(..., lock.clone().map(|l| l as Arc<dyn zerodb_core::shared::SharedRegion>))`, bound to `let env = ...?;`. Then:

```rust
        if let Some(l) = &lock {
            // Let joiners in only after T is seeded (ADR-0023 §1).
            l.finish_open().map_err(Error::Io)?;
        }
        Ok(env)
```

- [ ] **Step 4: heed adapter extension**

In `crates/heed-zerodb/src/env.rs`, add a `lock_file: bool` field (default `false`) and:

```rust
    /// Let other processes open this environment at the same time
    /// (ADR-0023), with LMDB's lock-file name `lock.mdb`.
    ///
    /// **ZeroDB extension.** In this release it is opt-in; a later release
    /// makes it the default and honors heed's `NO_LOCK` to turn it off.
    pub fn lock_file(&mut self, on: bool) -> &mut Self {
        self.lock_file = on;
        self
    }
```

In `open`, where the engine `opts` are built: `if self.lock_file { opts.lock_file(true).lock_file_name("lock.mdb"); }`.

- [ ] **Step 5: Bench axis**

In `backend.rs`, inside `(@setpage $opts:ident, $page:ident, set)`, after the trust-file block:

```rust
        // ADR-0023 bench axis: `ZERODB_BENCH_LOCK_FILE=1` opens every zerodb
        // env with the multi-process lock file, to measure its single-process
        // cost against the default. Off unless set.
        if std::env::var("ZERODB_BENCH_LOCK_FILE").as_deref() == Ok("1") {
            $opts.lock_file(true);
        }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p zerodb --test lock_file_single_process && cargo test --workspace`
Expected: PASS. The default is unchanged, so every existing test passes as before.

- [ ] **Step 7: Commit**

```bash
git add crates/zerodb crates/heed-zerodb crates/zerodb-oracle/benches
git commit -m "zerodb: opt-in lock_file option; heed adapter extension; bench axis"
```

---

### Task 7: Two-process tests

**Files:**
- Create: `crates/zerodb/tests/multi_process.rs`

**Interfaces:**
- Consumes: `EnvOpenOptions::lock_file` (Task 6).

The test binary re-runs itself as a child. `child(role)` spawns `current_exe()` filtered to the single test `child_entry`, passing the role and directory in environment variables. `child_entry` does nothing when those variables are absent, so it passes trivially in a normal run. Children and parent coordinate through marker files in the env directory, polled with a deadline.

- [ ] **Step 1: Write the tests**

```rust
//! Two-process tests for lock-file environments (ADR-0023 stage 1). The test
//! binary re-runs itself as the second process (see `child_entry`).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb::{Env, EnvOpenOptions};

// <copy the TempDir helper from env_lifecycle.rs here, including Drop>

const ROLE: &str = "ZERODB_MP_ROLE";
const DIR: &str = "ZERODB_MP_DIR";

fn open(dir: &Path) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(64 << 20).page_size(4096).max_readers(16).lock_file(true);
    o.open(dir).expect("open lock-file env")
}

fn child(role: &str, dir: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .spawn()
        .expect("spawn child")
}

fn wait_for(path: &Path, deadline: Duration) {
    let end = Instant::now() + deadline;
    while !path.exists() {
        assert!(Instant::now() < end, "timed out waiting for {}", path.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn mark(dir: &Path, name: &str) {
    std::fs::write(dir.join(name), b"").unwrap();
}

/// Child entry point: a no-op unless run by `child()`.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else { return };
    let dir = PathBuf::from(dir);
    let env = open(&dir);
    let db = env.main_database();
    match role.as_str() {
        "write_one" => {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, b"from-child", b"1").unwrap();
            w.commit().unwrap();
        }
        "churn" => {
            // Rewrite and delete every key many times, so freed pages would be
            // reused if the parent's reader slot were invisible here.
            for round in 0..50u32 {
                let mut w = env.write_txn().unwrap();
                for i in 0..200u32 {
                    let k = format!("k{i:04}");
                    if round % 2 == 0 {
                        db.delete(&mut w, k.as_bytes()).unwrap();
                    } else {
                        db.put(&mut w, k.as_bytes(), format!("r{round}").repeat(40).as_bytes()).unwrap();
                    }
                }
                w.commit().unwrap();
            }
        }
        "write_when_free" => {
            mark(&dir, "child-waiting");
            let mut w = env.write_txn().unwrap(); // blocks while the parent writes
            mark(&dir, "child-got-lock");
            db.put(&mut w, b"child", b"2").unwrap();
            w.commit().unwrap();
        }
        "hold_and_die" => {
            let mut w = env.write_txn().unwrap();
            db.put(&mut w, b"doomed", b"x").unwrap();
            mark(&dir, "child-holds-lock");
            std::thread::sleep(Duration::from_secs(600)); // killed by the parent
        }
        other => panic!("unknown role {other}"),
    }
}

#[test]
fn commit_in_one_process_is_visible_in_another() {
    let dir = TempDir::new();
    let env = open(dir.path());
    assert!(child("write_one", dir.path()).wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"from-child").unwrap(), Some(&b"1"[..]));
}

#[test]
fn foreign_reader_holds_back_page_reuse() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    for i in 0..200u32 {
        db.put(&mut w, format!("k{i:04}").as_bytes(), format!("orig{i}").repeat(40).as_bytes())
            .unwrap();
    }
    w.commit().unwrap();
    let r = env.read_txn().unwrap(); // pinned in a shared slot
    assert!(child("churn", dir.path()).wait().unwrap().success());
    for i in 0..200u32 {
        let want = format!("orig{i}").repeat(40);
        assert_eq!(
            db.get(&r, format!("k{i:04}").as_bytes()).unwrap(),
            Some(want.as_bytes()),
            "key {i} changed under a pinned snapshot"
        );
    }
    drop(r);
    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).unwrap();
    assert!(zerodb::check::check_image(&bytes, 4096).is_empty());
}

#[test]
fn writers_in_two_processes_exclude_each_other() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    let mut c = child("write_when_free", dir.path());
    wait_for(&dir.path().join("child-waiting"), Duration::from_secs(30));
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !dir.path().join("child-got-lock").exists(),
        "child took the writer lock while the parent held it"
    );
    db.put(&mut w, b"parent", b"1").unwrap();
    w.commit().unwrap();
    assert!(c.wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"parent").unwrap(), Some(&b"1"[..]));
    assert_eq!(db.get(&r, b"child").unwrap(), Some(&b"2"[..]));
}

#[test]
fn killed_writer_lock_is_taken_over() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let mut c = child("hold_and_die", dir.path());
    wait_for(&dir.path().join("child-holds-lock"), Duration::from_secs(30));
    c.kill().unwrap(); // SIGKILL
    c.wait().unwrap();
    let start = Instant::now();
    let w = env.write_txn().expect("take over the dead writer's lock");
    assert!(start.elapsed() < Duration::from_secs(5), "takeover took {:?}", start.elapsed());
    drop(w);
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"doomed").unwrap(), None);
}

#[test]
fn second_process_with_other_max_readers_is_refused() {
    let dir = TempDir::new();
    let _env = open(dir.path()); // max_readers 16
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry_other_readers", "--nocapture"])
        .env(DIR, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
}

/// Child entry point for the `max_readers` mismatch case.
#[test]
fn child_entry_other_readers() {
    let Ok(dir) = std::env::var(DIR) else { return };
    let mut o = EnvOpenOptions::new();
    o.map_size(64 << 20).page_size(4096).max_readers(32).lock_file(true);
    match o.open(Path::new(&dir)) {
        Err(zerodb::Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected a max_readers mismatch error, got {other:?}"),
    }
}
```

Check the names `db.delete`, `max_readers`, `zerodb::check::check_image` and `DATA_FILE_NAME` against `concurrency_smoke.rs` and `write_api.rs`. `child_entry_other_readers` must ignore `ZERODB_MP_ROLE`: it returns early only when `DIR` is absent, and the parent sets `DIR` only for this child.

- [ ] **Step 2: Run them**

Run: `cargo test -p zerodb --test multi_process -- --test-threads=1`
Expected: PASS. If a test fails, fix the implementation, never the test (rule 2). If a test itself looks wrong, stop and report.

Run the same command five times in a row (`for i in 1 2 3 4 5; do ...; done`). Expected: PASS every time. Timing-sensitive flakiness is a bug.

- [ ] **Step 3: Commit**

```bash
git add crates/zerodb/tests/multi_process.rs
git commit -m "tests: two-process visibility, GC, writer exclusion and takeover (ADR-0023 stage 1)"
```

---

### Task 8: Full gate, three-column bench, stage-1 report

**Files:**
- Create: `benches/results/2026-10-XX-multi-process-stage1.md` (the date the run finishes)
- Modify: `benches/results/perf-ledger.jsonl` (append one entry with `scripts/perf-ledger.py`)

- [ ] **Step 1: Full local gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --target x86_64-unknown-linux-gnu --exclude zerodb-oracle -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core
just loom
just fuzz-quick
just crash-test-quick
```

Expected: all green. Report any failure with its output and do not continue past it.

- [ ] **Step 2: Linux run of the two-process tests**

The futex path only compiles and runs on Linux. Run `cargo test -p zerodb --test multi_process -- --test-threads=1` five times on the bench server (`root@62.210.92.143`, x86-64) and once on a Graviton host if one is up. Use an isolated `CARGO_HOME` and target dir in the scratchpad (see memory notes), never a shared target dir between checkouts.

- [ ] **Step 3: Three-column bench on the bench server**

Columns: LMDB, ZeroDB before (`main`), ZeroDB after with the lock file on (this branch, `ZERODB_BENCH_LOCK_FILE=1`). Also run a fourth, "after with the lock file off", to show the default path did not regress. Rungs: `env/txn/*`, `get/*`, `commit/batch/n1`, `commit/batch/n100`, `commit/sync/*`. Use `just bench-ab` / `scripts/bench-ab.sh` with `ROUNDS=5` and `CODEGEN_UNITS=1`, as for ADR-0022.

- [ ] **Step 4: Write the report and the ledger entry**

The report records:
- per-rung ratios against LMDB and before/after;
- the Linux test-run counts;
- whether stage 1's riskiest assumption held. That assumption is two-part: cross-process visibility and handoff work, and single-process cost stays within noise on `txn/begin` and `get/*` and within a few percent on `commit/batch/n1`.

Append the ledger line with `scripts/perf-ledger.py`, following existing entries.

- [ ] **Step 5: Commit and stop**

```bash
git add benches/results
git commit -m "bench: ADR-0023 stage 1 — single-process cost of the lock file"
```

Then report to the maintainer and **stop before stage 2**, per the ADR. The user pushes. The assistant does not push.
