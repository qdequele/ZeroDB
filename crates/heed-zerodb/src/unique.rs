//! Unique transactions — the API of Meilisearch's heed fork
//! (`meilisearch/heed@mutexify-open-dbi`, ADR-0027), compiled only with the
//! `unique-txns` feature.
//!
//! The fork serializes opening and creating databases behind one mutex per
//! environment. A *unique* transaction is an ordinary transaction that holds
//! that mutex for its whole life, and only a unique transaction can open or
//! create a database ([`AsUniqueTxnRef`], `&mut UniqueRwTxn`).
//!
//! ZeroDB does not need the mutex: opening a database is already safe from
//! several threads (ADR-0024). The mutex is reproduced so the fork's blocking
//! behaviour holds — a unique transaction waits for the previous one to end,
//! and the `try_` constructors return `Ok(None)` instead of waiting — and a
//! caller that relies on it for its own mutual exclusion keeps it.

use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard, TryLockError};

use crate::txn::{AnyTls, RoTxn, RwTxn, WithTls, WithoutTls};
use crate::Result;

/// The environment's database-open mutex, taken by every unique transaction.
#[derive(Default)]
pub(crate) struct DbiOpenMutex(Mutex<()>);

impl DbiOpenMutex {
    /// Wait for the mutex. A poisoned mutex is cleared and taken, as in the
    /// fork: it guards no data, so a panic while holding it leaves nothing
    /// inconsistent.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        loop {
            match self.0.lock() {
                Ok(guard) => break guard,
                Err(_) => self.0.clear_poison(),
            }
        }
    }

    /// Take the mutex if it is free; `None` if a unique transaction holds it.
    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, ()>> {
        loop {
            match self.0.try_lock() {
                Ok(guard) => break Some(guard),
                Err(TryLockError::Poisoned(_)) => self.0.clear_poison(),
                Err(TryLockError::WouldBlock) => break None,
            }
        }
    }
}

/// A read transaction holding the environment's database-open mutex.
/// Derefs to the [`RoTxn`] it wraps.
pub struct UniqueRoTxn<'e, T = AnyTls> {
    txn: RoTxn<'e, T>,
    _lock: MutexGuard<'e, ()>,
}

impl<'e, T> UniqueRoTxn<'e, T> {
    pub(crate) fn new(txn: RoTxn<'e, T>, lock: MutexGuard<'e, ()>) -> UniqueRoTxn<'e, T> {
        UniqueRoTxn { txn, _lock: lock }
    }

    /// Commit the read transaction: release the snapshot, keep the databases
    /// it opened for transactions that begin after it, then release the
    /// mutex. Dropping it instead closes those databases, as in LMDB
    /// (ADR-0024). The fork's replacement for heed 0.22.1's `RoTxn::commit`.
    ///
    /// # Errors
    ///
    /// Infallible in ZeroDB; returns [`Result`] for API shape.
    pub fn commit(self) -> Result<()> {
        self.txn.commit_inner()
    }
}

impl<'e, T> Deref for UniqueRoTxn<'e, T> {
    type Target = RoTxn<'e, T>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

/// A write transaction holding the environment's database-open mutex.
/// Derefs to the [`RwTxn`] it wraps; the only transaction that can create a
/// database.
pub struct UniqueRwTxn<'e> {
    txn: RwTxn<'e>,
    _lock: MutexGuard<'e, ()>,
}

impl<'e> UniqueRwTxn<'e> {
    pub(crate) fn new(txn: RwTxn<'e>, lock: MutexGuard<'e, ()>) -> UniqueRwTxn<'e> {
        UniqueRwTxn { txn, _lock: lock }
    }

    /// Commit all operations of the transaction, then release the mutex
    /// (see [`RwTxn::commit`]).
    ///
    /// # Errors
    ///
    /// As [`RwTxn::commit`].
    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    /// Abandon all operations of the transaction, then release the mutex.
    pub fn abort(self) {
        self.txn.abort();
    }
}

impl<'e> Deref for UniqueRwTxn<'e> {
    type Target = RwTxn<'e>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DerefMut for UniqueRwTxn<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

/// A borrowed transaction that holds the database-open mutex: what opening a
/// database takes. Obtained through [`AsUniqueTxnRef`]; not nameable outside
/// the crate, as in the fork.
pub struct UniqueTxnRef<'a, 'e> {
    txn: &'a RoTxn<'e, AnyTls>,
    _lock: &'a MutexGuard<'e, ()>,
}

impl<'e> Deref for UniqueTxnRef<'_, 'e> {
    type Target = RoTxn<'e>;

    fn deref(&self) -> &Self::Target {
        self.txn
    }
}

/// A transaction that holds the database-open mutex, so it can open a
/// database: [`UniqueRoTxn`] (any TLS marker), [`UniqueRwTxn`], or a
/// reference to either.
pub trait AsUniqueTxnRef<'e> {
    /// Borrow the transaction together with its mutex guard.
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e>;
}

impl<'e, U> AsUniqueTxnRef<'e> for &U
where
    U: AsUniqueTxnRef<'e>,
{
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e> {
        U::as_unique_txn_ref(self)
    }
}

impl<'e> AsUniqueTxnRef<'e> for UniqueRoTxn<'e, WithoutTls> {
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e> {
        UniqueTxnRef {
            txn: &self.txn,
            _lock: &self._lock,
        }
    }
}

impl<'e> AsUniqueTxnRef<'e> for UniqueRoTxn<'e, WithTls> {
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e> {
        UniqueTxnRef {
            txn: &self.txn,
            _lock: &self._lock,
        }
    }
}

impl<'e> AsUniqueTxnRef<'e> for UniqueRoTxn<'e, AnyTls> {
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e> {
        UniqueTxnRef {
            txn: &self.txn,
            _lock: &self._lock,
        }
    }
}

impl<'e> AsUniqueTxnRef<'e> for UniqueRwTxn<'e> {
    fn as_unique_txn_ref(&self) -> UniqueTxnRef<'_, 'e> {
        UniqueTxnRef {
            txn: &self.txn,
            _lock: &self._lock,
        }
    }
}
