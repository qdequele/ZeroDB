//! Named-database handle lifetime (ADR-0024; SPEC 02 §6.2): the environment's
//! slot table and each transaction's view of it.
//!
//! A named database handle is `(slot, generation)`. A slot is *pending* while
//! the transactions that opened it are live and none has committed,
//! *published* once one commits, and *free* once every opener ended without a
//! commit or the database was removed. A handle is checked against the
//! transaction using it before every operation; the rules and their order are
//! ADR-0024's (`HandleTable::check`). They reproduce LMDB's `EINVAL` and
//! `MDB_BAD_DBI` outcomes, except where ADR-0024's divergence D-026 is stricter.
//!
//! The table sits behind one mutex, taken when a database is opened, created
//! or removed, when a transaction that opened one ends, and on the first check
//! of a slot after any slot change. Every other check is lock-free: the env's
//! epoch, bumped on every slot change, tells a transaction whether the
//! verdict it memoized for a slot still holds.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::error::{Error, MdbError, Result};

/// The `EINVAL` (raw OS error 22) LMDB returns for a handle that is not valid
/// in a transaction; heed surfaces it as `Error::Io`.
pub(crate) fn einval() -> Error {
    Error::Io(std::io::Error::from_raw_os_error(22))
}

/// "Never": the publication epoch of an instance that was never published.
const NEVER: u64 = u64::MAX;

/// Slots below this get the lock-free check. Meilisearch opens about 30 named
/// databases per environment and hannoy a handful; a slot past it is checked
/// under the table lock every time.
pub(crate) const DENSE_SLOT_LIMIT: u32 = 256;

#[derive(Debug)]
enum State {
    /// Opened by these live transactions (owner ids), none committed yet.
    Pending(Vec<u64>),
    /// Published at this epoch.
    Published(u64),
    /// Not open. The slot keeps its name, so reopening the same name reuses
    /// it with the same generation, as LMDB reuses the slot.
    Free,
}

#[derive(Debug)]
struct Slot {
    name: Box<[u8]>,
    /// Bumped when the slot is reassigned to another name. A handle with an
    /// older generation is never valid again (D-026).
    generation: u32,
    /// Bumped on every (re)assignment, so a transaction's record of what it
    /// opened names one instance of the slot, not a later one.
    inc: u64,
    state: State,
    /// Epoch of the most recent free; 0 if never freed.
    freed_at: u64,
    /// Owner id of the transaction that did the most recent free; 0 if none.
    freed_by: u64,
    /// When the instance freed most recently had been published; [`NEVER`]
    /// if it never was.
    freed_published_at: u64,
}

#[derive(Debug)]
struct Table {
    slots: Vec<Slot>,
    /// Name → slot, for every slot, free ones included.
    by_name: HashMap<Box<[u8]>, u32>,
    /// The next owner id handed to a transaction that opens or removes a
    /// database; 0 is "none".
    next_owner: u64,
}

/// The environment's named-database slot table (ADR-0024). Engine-internal;
/// public only because [`crate::rotxn::TxnRead`] names it.
#[doc(hidden)]
#[derive(Debug)]
pub struct HandleTable {
    table: Mutex<Table>,
    /// Bumped, under the table lock, on every slot change.
    epoch: AtomicU64,
    /// The catalog capacity (`max_dbs`): the number of slots.
    max_dbs: u32,
}

/// One transaction's state in the slot table. Engine-internal; public only
/// because [`crate::rotxn::TxnRead`] names it.
#[doc(hidden)]
#[derive(Debug)]
pub struct TxnHandles {
    /// The epoch when the transaction began.
    begin: u64,
    /// The owner id, assigned on the transaction's first open or remove; 0
    /// until then. Written under the table lock.
    owner: AtomicU64,
    /// What only a transaction that opens or removes a database, or a write
    /// or nested read txn that checks one, needs; most read txns never
    /// allocate it.
    rare: OnceLock<Box<Rare>>,
}

#[derive(Debug, Default)]
pub(crate) struct Rare {
    /// `(slot, inc)` of every slot instance this transaction opened.
    opened: Mutex<Vec<(u32, u64)>>,
    /// Slot → `epoch + 1` of the last check that found the slot's current
    /// generation valid here; 0 = none. Allocated on the first such check.
    /// Read txns keep theirs beside their record memo instead
    /// ([`HandleTable::check_with`]).
    memo: OnceLock<Box<[MemoCell]>>,
}

/// One slot's memo of a valid check: the epoch it held at and the handle
/// generation it was for (ADR-0024; SPEC 04 DBI-10). Written only under the
/// table lock, read lock-free.
#[doc(hidden)]
#[derive(Debug)]
pub struct MemoCell {
    /// `epoch + 1` of the verdict; 0 = none.
    epoch: crate::sync::AtomicU64,
    generation: crate::sync::AtomicU32,
}

impl Default for MemoCell {
    fn default() -> MemoCell {
        MemoCell {
            epoch: crate::sync::AtomicU64::new(0),
            generation: crate::sync::AtomicU32::new(0),
        }
    }
}

impl MemoCell {
    /// Whether a handle of `generation` was found valid at epoch `e`.
    #[inline]
    fn holds(&self, e: u64, generation: u32) -> bool {
        // A seqlock read of the pair: `set` clears `epoch`, writes the
        // generation, then writes `epoch`. Epochs only grow, so a rewrite
        // never brings an old value back: the same `e + 1` before and after
        // the generation load means that generation was written with it.
        // Acquire (all three): pair with `set`'s Release stores, and keep
        // the second epoch load after the generation load.
        self.epoch.load(crate::sync::Ordering::Acquire) == e + 1
            && self.generation.load(crate::sync::Ordering::Acquire) == generation
            && self.epoch.load(crate::sync::Ordering::Acquire) == e + 1
    }

    /// Forget the verdict. Only on a memo no other thread can reach
    /// ([`TxnHandles::recycle`] holds it by `&mut`), so Relaxed suffices.
    fn reset(&self) {
        self.epoch.store(0, crate::sync::Ordering::Relaxed);
    }

    /// Record a valid verdict for `generation` at epoch `e`. Called with the
    /// table lock held, so writers never interleave.
    fn set(&self, e: u64, generation: u32) {
        // Release (all three): pair with `holds`' Acquire loads. The clear is
        // ordered before the generation store by that store's Release.
        self.epoch.store(0, crate::sync::Ordering::Release);
        self.generation
            .store(generation, crate::sync::Ordering::Release);
        self.epoch.store(e + 1, crate::sync::Ordering::Release);
    }
}

impl TxnHandles {
    /// Whether this transaction never opened or removed a database, so its
    /// end has nothing to do. `&mut`: called from a commit or a drop.
    pub(crate) fn is_idle(&mut self) -> bool {
        *self.owner.get_mut() == 0
    }

    fn rare(&self) -> &Rare {
        self.rare.get_or_init(Box::default)
    }

    /// Take this transaction's [`Rare`] allocation, emptied, for the next
    /// write transaction ([`HandleTable::begin_reusing`]; issue #8). Called
    /// after [`HandleTable::end`], with `&mut`, so no check can be reading
    /// the memo. Every memo cell is reset to "none": the next transaction
    /// checks each slot under the table lock once, exactly as with a fresh
    /// allocation. `None` if there was none, or its list lock was poisoned.
    pub(crate) fn recycle(&mut self) -> Option<Box<Rare>> {
        let mut rare = self.rare.take()?;
        rare.opened.get_mut().ok()?.clear();
        if let Some(memo) = rare.memo.get_mut() {
            for cell in memo.iter() {
                cell.reset();
            }
        }
        Some(rare)
    }

    /// Whether this transaction opened instance `inc` of `slot`.
    fn has_opened(&self, slot: u32, inc: u64) -> bool {
        self.rare.get().is_some_and(|r| {
            r.opened
                .lock()
                .expect("opened list poisoned")
                .contains(&(slot, inc))
        })
    }
}

impl HandleTable {
    /// An empty table for `max_dbs` named databases.
    pub(crate) fn new(max_dbs: u32) -> HandleTable {
        HandleTable {
            table: Mutex::new(Table {
                slots: Vec::new(),
                by_name: HashMap::new(),
                next_owner: 1,
            }),
            epoch: AtomicU64::new(0),
            max_dbs,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table.lock().expect("handle table poisoned")
    }

    /// A transaction's begin: record the epoch.
    pub(crate) fn begin(&self) -> TxnHandles {
        self.begin_reusing(None)
    }

    /// [`HandleTable::begin`] for a write transaction, reusing the previous
    /// one's [`Rare`] allocation ([`TxnHandles::recycle`]) when there is one.
    pub(crate) fn begin_reusing(&self, rare: Option<Box<Rare>>) -> TxnHandles {
        TxnHandles {
            // Acquire: pairs with `bump`'s Release, so a slot published before
            // this load is seen published by this transaction's checks.
            begin: self.epoch.load(Ordering::Acquire),
            owner: AtomicU64::new(0),
            rare: rare.map_or_else(OnceLock::new, OnceLock::from),
        }
    }

    /// Advance the epoch. Called with the table lock held (`_t` proves it),
    /// before the slot change it stamps, so a check that reads the old epoch
    /// and finds its memo is ordered before the change.
    fn bump(&self, _t: &mut Table) -> u64 {
        // Relaxed load: only writers holding the lock store the epoch.
        let e = self.epoch.load(Ordering::Relaxed) + 1;
        // Release: pairs with the Acquire loads in `begin` and `check`.
        self.epoch.store(e, Ordering::Release);
        e
    }

    /// The name of `slot`, if assigned.
    pub(crate) fn name(&self, slot: u32) -> Option<Box<[u8]>> {
        self.lock().slots.get(slot as usize).map(|s| s.name.clone())
    }

    /// The transaction's owner id, assigning one if it has none.
    fn owner_of(t: &mut Table, txn: &TxnHandles) -> u64 {
        // Relaxed: the owner id is only written under the table lock, which
        // the caller holds.
        let o = txn.owner.load(Ordering::Relaxed);
        if o != 0 {
            return o;
        }
        let o = t.next_owner;
        t.next_owner += 1;
        txn.owner.store(o, Ordering::Relaxed);
        o
    }

    /// `open_database` / `create_database` of `name`, a database that exists
    /// in the transaction's catalog view (or is being created by it). Returns
    /// the handle's `(slot, generation)` and whether the slot changed name,
    /// or `None` when every slot is in use (`DbsFull`). `reusable(slot)` says
    /// whether a free slot may take a new name; a slot with a custom
    /// comparator registered may not, since the registration is per slot and
    /// permanent.
    pub(crate) fn open(
        &self,
        txn: &TxnHandles,
        name: &[u8],
        reusable: impl Fn(u32) -> bool,
    ) -> Option<(u32, u32, bool)> {
        let mut guard = self.lock();
        let t = &mut *guard;
        let owner = Self::owner_of(t, txn);
        let mut opened = txn.rare().opened.lock().expect("opened list poisoned");
        if let Some(&i) = t.by_name.get(name) {
            self.bump(t);
            let s = &mut t.slots[i as usize];
            match &mut s.state {
                State::Pending(owners) => {
                    if !owners.contains(&owner) {
                        owners.push(owner);
                    }
                }
                State::Published(_) => {}
                State::Free => {
                    s.inc += 1;
                    s.state = State::Pending(vec![owner]);
                }
            }
            if !opened.contains(&(i, s.inc)) {
                opened.push((i, s.inc));
            }
            return Some((i, s.generation, false));
        }
        let free = t
            .slots
            .iter()
            .enumerate()
            .position(|(i, s)| matches!(s.state, State::Free) && reusable(i as u32));
        let i = match free {
            Some(i) => {
                self.bump(t);
                let s = &mut t.slots[i];
                let old = std::mem::replace(&mut s.name, name.into());
                t.by_name.remove(&old);
                s.generation = s.generation.wrapping_add(1);
                s.inc += 1;
                s.state = State::Pending(vec![owner]);
                i
            }
            None => {
                if t.slots.len() >= self.max_dbs as usize {
                    return None;
                }
                self.bump(t);
                t.slots.push(Slot {
                    name: name.into(),
                    generation: 0,
                    inc: 1,
                    state: State::Pending(vec![owner]),
                    freed_at: 0,
                    freed_by: 0,
                    freed_published_at: NEVER,
                });
                t.slots.len() - 1
            }
        };
        let i32_ = i as u32;
        t.by_name.insert(name.into(), i32_);
        let s = &t.slots[i];
        opened.push((i32_, s.inc));
        Some((i32_, s.generation, free.is_some()))
    }

    /// Undo an `open` whose create then failed before the database existed:
    /// the transaction stops being an opener of `slot`, and a slot left with
    /// no opener is freed.
    pub(crate) fn abandon(&self, txn: &TxnHandles, slot: u32) {
        let mut guard = self.lock();
        let t = &mut *guard;
        // Relaxed: written under the table lock, which is held.
        let owner = txn.owner.load(Ordering::Relaxed);
        let mut opened = txn.rare().opened.lock().expect("opened list poisoned");
        let e = self.bump(t);
        let Some(s) = t.slots.get_mut(slot as usize) else {
            return;
        };
        opened.retain(|&(i, inc)| (i, inc) != (slot, s.inc));
        if let State::Pending(owners) = &mut s.state {
            owners.retain(|&o| o != owner);
            if owners.is_empty() {
                Self::free(s, e, 0);
            }
        }
    }

    fn free(s: &mut Slot, e: u64, by: u64) {
        s.freed_published_at = match s.state {
            State::Published(p) => p,
            _ => NEVER,
        };
        s.freed_at = e;
        s.freed_by = by;
        s.state = State::Free;
    }

    /// `Database::remove` (`mdb_drop(.., 1)`): free the slot at once. An abort
    /// of `txn` does not undo it, as in LMDB.
    pub(crate) fn remove(&self, txn: &TxnHandles, slot: u32, generation: u32) {
        let mut guard = self.lock();
        let t = &mut *guard;
        let owner = Self::owner_of(t, txn);
        let live = t
            .slots
            .get(slot as usize)
            .is_some_and(|s| s.generation == generation && !matches!(s.state, State::Free));
        if live {
            let e = self.bump(t);
            Self::free(&mut t.slots[slot as usize], e, owner);
        }
    }

    /// The end of `txn`: a commit publishes the pending slots it opened; an
    /// end without one removes it from their openers, freeing a slot left
    /// with none. Takes `&mut` (every caller is a commit or a drop), so a
    /// transaction that opened nothing returns without touching the lock.
    pub(crate) fn end(&self, txn: &mut TxnHandles, committed: bool) {
        let Some(rare) = txn.rare.get_mut() else {
            return;
        };
        let opened = std::mem::take(rare.opened.get_mut().expect("opened list poisoned"));
        if opened.is_empty() {
            return;
        }
        let owner = *txn.owner.get_mut();
        let mut guard = self.lock();
        let t = &mut *guard;
        let e = self.bump(t);
        for (slot, inc) in opened {
            let s = &mut t.slots[slot as usize];
            if s.inc != inc {
                continue;
            }
            if let State::Pending(owners) = &mut s.state {
                if committed {
                    s.state = State::Published(e);
                } else {
                    owners.retain(|&o| o != owner);
                    if owners.is_empty() {
                        Self::free(s, e, owner);
                    }
                }
            }
        }
    }

    /// Check handle `(slot, generation)` in `txn` (ADR-0024 "The rules", check
    /// order 1–6). A nested read child passes its parent: it accepts every
    /// handle the parent accepts.
    ///
    /// # Errors
    ///
    /// `Error::Io` with raw OS error `EINVAL` (22), or [`MdbError::BadDbi`].
    #[inline]
    pub(crate) fn check(
        &self,
        txn: &TxnHandles,
        parent: Option<&TxnHandles>,
        slot: u32,
        generation: u32,
    ) -> Result<()> {
        if let Some(cell) = txn
            .rare
            .get()
            .and_then(|r| r.memo.get())
            .and_then(|m| m.get(slot as usize))
        {
            if cell.holds(self.epoch(), generation) {
                return Ok(());
            }
        }
        self.check_locked(txn, parent, slot, generation, |epoch| {
            let memo = txn.rare().memo.get_or_init(|| {
                (0..self.max_dbs.min(DENSE_SLOT_LIMIT))
                    .map(|_| MemoCell::default())
                    .collect()
            });
            if let Some(m) = memo.get(slot as usize) {
                m.set(epoch, generation);
            }
        })
    }

    /// [`HandleTable::check`] with the memo cell supplied by the caller: a
    /// read txn keeps it beside its record memo for the slot, so a check
    /// costs no lookup of its own.
    #[inline]
    pub(crate) fn check_with(
        &self,
        txn: &TxnHandles,
        slot: u32,
        generation: u32,
        cell: &MemoCell,
    ) -> Result<()> {
        if cell.holds(self.epoch(), generation) {
            return Ok(());
        }
        self.check_locked(txn, None, slot, generation, |epoch| {
            cell.set(epoch, generation);
        })
    }

    /// The current epoch.
    #[inline]
    fn epoch(&self) -> u64 {
        // Acquire: pairs with `bump`'s Release. The same epoch as when a memo
        // was written means no slot has changed since.
        self.epoch.load(Ordering::Acquire)
    }

    #[cold]
    fn check_locked(
        &self,
        txn: &TxnHandles,
        parent: Option<&TxnHandles>,
        slot: u32,
        generation: u32,
        on_valid: impl FnOnce(u64),
    ) -> Result<()> {
        let t = self.lock();
        let Some(s) = t.slots.get(slot as usize) else {
            return Err(einval());
        };
        // Rule 1: the slot changed name since the handle was made.
        if s.generation != generation {
            return Err(einval());
        }
        let mut verdict = Self::verdict(s, slot, txn);
        if verdict.is_err() {
            if let Some(p) = parent {
                if Self::verdict(s, slot, p).is_ok() {
                    verdict = Ok(());
                }
            }
        }
        if verdict.is_ok() && slot < DENSE_SLOT_LIMIT {
            // Memoized under the lock, at the epoch the verdict holds for.
            // Relaxed load: the epoch only changes under the lock, held.
            on_valid(self.epoch.load(Ordering::Relaxed));
        }
        verdict
    }

    /// Rules 2–6 for one transaction.
    fn verdict(s: &Slot, slot: u32, txn: &TxnHandles) -> Result<()> {
        let free = matches!(s.state, State::Free);
        let opened = txn.has_opened(slot, s.inc);
        // Rule 2: this transaction opened the slot's current instance.
        if opened && !free {
            return Ok(());
        }
        // Relaxed: written under the table lock, which the caller holds.
        let owner = txn.owner.load(Ordering::Relaxed);
        // Rule 3: this transaction removed it.
        if free && owner != 0 && s.freed_by == owner {
            return Err(einval());
        }
        // Rule 4: valid when this transaction began, freed since.
        if s.freed_at > txn.begin && s.freed_published_at <= txn.begin {
            return Err(Error::Mdb(MdbError::BadDbi));
        }
        // Rule 5: published before this transaction began, not freed since.
        if let State::Published(p) = s.state {
            if p <= txn.begin && s.freed_at <= txn.begin {
                return Ok(());
            }
        }
        // Rule 6.
        Err(einval())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_einval(r: Result<()>) -> bool {
        matches!(r, Err(Error::Io(e)) if e.raw_os_error() == Some(22))
    }

    fn is_bad_dbi(r: Result<()>) -> bool {
        matches!(r, Err(Error::Mdb(MdbError::BadDbi)))
    }

    #[test]
    fn a_recycled_memo_starts_empty() {
        // Issue #8: a write txn reuses the previous one's memo allocation,
        // but none of its verdicts.
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, true);
        let mut w1 = h.begin();
        assert!(h.check(&w1, None, s, g).is_ok());
        let memo = w1.rare.get().and_then(|r| r.memo.get()).expect("memo");
        assert!(memo[s as usize].holds(h.epoch(), g));
        let rare = w1.recycle().expect("w1 allocated one");
        let memo = rare.memo.get().expect("memo kept");
        assert!(memo.iter().all(|c| !c.holds(h.epoch(), g)));
        assert!(rare.opened.lock().unwrap().is_empty());
        let w2 = h.begin_reusing(Some(rare));
        assert!(h.check(&w2, None, s, g).is_ok());

        // An aborted create leaves no verdict behind for the next txn.
        let mut w3 = h.begin_reusing(None);
        let (s3, g3, _) = h.open(&w3, b"b", |_| true).unwrap();
        assert!(h.check(&w3, None, s3, g3).is_ok());
        h.end(&mut w3, false);
        let w4 = h.begin_reusing(w3.recycle());
        assert!(is_einval(h.check(&w4, None, s3, g3)));
    }

    #[test]
    fn create_then_abort_frees_the_slot() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        assert!(h.check(&w, None, s, g).is_ok());
        h.end(&mut w, false);
        let w2 = h.begin();
        assert!(is_einval(h.check(&w2, None, s, g)));
    }

    #[test]
    fn commit_publishes_for_later_transactions_only() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        let before = h.begin();
        assert!(is_einval(h.check(&before, None, s, g)));
        h.end(&mut w, true);
        assert!(is_einval(h.check(&before, None, s, g)));
        let after = h.begin();
        assert!(h.check(&after, None, s, g).is_ok());
    }

    #[test]
    fn remove_under_a_live_reader_is_bad_dbi_then_einval() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, true);
        let r = h.begin();
        assert!(h.check(&r, None, s, g).is_ok());
        let mut w = h.begin();
        h.remove(&w, s, g);
        assert!(is_einval(h.check(&w, None, s, g)), "rule 3, same txn");
        h.end(&mut w, false);
        assert!(is_bad_dbi(h.check(&r, None, s, g)), "rule 4, memo bypassed");
        assert!(is_einval(h.check(&h.begin(), None, s, g)));
    }

    #[test]
    fn same_name_reuses_the_slot_and_generation() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s1, g1, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, false);
        let mut w = h.begin();
        let (s2, g2, renamed) = h.open(&w, b"a", |_| true).unwrap();
        assert_eq!((s1, g1, false), (s2, g2, renamed));
        h.end(&mut w, true);
        assert!(h.check(&h.begin(), None, s1, g1).is_ok());
    }

    #[test]
    fn another_name_bumps_the_generation() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (sa, ga, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, false);
        let mut w = h.begin();
        let (sb, gb, renamed) = h.open(&w, b"b", |_| true).unwrap();
        assert_eq!(sa, sb);
        assert_ne!(ga, gb);
        assert!(renamed);
        h.end(&mut w, true);
        let r = h.begin();
        assert!(is_einval(h.check(&r, None, sa, ga)), "D-026");
        assert!(h.check(&r, None, sb, gb).is_ok());
    }

    #[test]
    fn a_slot_that_is_not_reusable_is_skipped() {
        let h = HandleTable::new(2);
        let mut w = h.begin();
        let (sa, _, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, false);
        let w = h.begin();
        let (sb, _, _) = h.open(&w, b"b", |i| i != sa).unwrap();
        assert_ne!(sa, sb);
        assert!(h.open(&w, b"c", |i| i != sa).is_none(), "DbsFull");
    }

    #[test]
    fn dbs_full_counts_open_slots() {
        let h = HandleTable::new(1);
        let mut w = h.begin();
        h.open(&w, b"a", |_| true).unwrap();
        assert!(h.open(&w, b"b", |_| true).is_none());
        h.end(&mut w, false);
        let w = h.begin();
        assert!(h.open(&w, b"b", |_| true).is_some());
    }

    #[test]
    fn co_openers_keep_the_slot_until_one_commits() {
        let h = HandleTable::new(4);
        let mut r1 = h.begin();
        let mut r2 = h.begin();
        let (s, g, _) = h.open(&r1, b"a", |_| true).unwrap();
        assert_eq!(h.open(&r2, b"a", |_| true).unwrap().0, s);
        h.end(&mut r1, false);
        assert!(h.check(&r2, None, s, g).is_ok());
        h.end(&mut r2, true);
        assert!(h.check(&h.begin(), None, s, g).is_ok(), "D-026");
    }

    #[test]
    fn a_child_accepts_its_parents_handles() {
        let h = HandleTable::new(4);
        let w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        let child = h.begin();
        assert!(is_einval(h.check(&child, None, s, g)));
        assert!(h.check(&child, Some(&w), s, g).is_ok());
    }

    #[test]
    fn an_abandoned_create_frees_the_slot() {
        let h = HandleTable::new(1);
        let w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        h.abandon(&w, s);
        assert!(is_einval(h.check(&w, None, s, g)));
        assert!(
            h.open(&w, b"b", |_| true).is_some(),
            "the slot is free again"
        );
    }

    #[test]
    fn the_memo_does_not_outlive_a_slot_change() {
        let h = HandleTable::new(4);
        let mut w = h.begin();
        let (s, g, _) = h.open(&w, b"a", |_| true).unwrap();
        h.end(&mut w, true);
        let r = h.begin();
        assert!(h.check(&r, None, s, g).is_ok());
        assert!(h.check(&r, None, s, g).is_ok(), "fast path");
        assert!(
            is_einval(h.check(&r, None, s, g.wrapping_add(1))),
            "another generation"
        );
        let w = h.begin();
        h.remove(&w, s, g);
        assert!(is_bad_dbi(h.check(&r, None, s, g)));
    }
}

// ---------------------------------------------------------------------------
// loom: the MemoCell seqlock (run via `just loom`, tests filtered `loom_`).
// ---------------------------------------------------------------------------

#[cfg(all(test, loom))]
mod loom_tests {
    use super::MemoCell;
    use loom::thread;
    use std::sync::Arc;

    /// A reader racing a rewrite from (epoch 1, generation 7) to (epoch 2,
    /// generation 8) must never see generation 8 paired with epoch 1, nor
    /// generation 7 with epoch 2.
    #[test]
    fn loom_memo_cell_never_mixes_two_writes() {
        loom::model(|| {
            let c = Arc::new(MemoCell::default());
            c.set(1, 7);
            let w = {
                let c = Arc::clone(&c);
                thread::spawn(move || c.set(2, 8))
            };
            assert!(!c.holds(1, 8));
            assert!(!c.holds(2, 7));
            w.join().unwrap();
            assert!(c.holds(2, 8));
        });
    }
}
