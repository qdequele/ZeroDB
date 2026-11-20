//! Nested read transactions over a write txn (SPEC 04 §5, ADR-0007).
//!
//! A [`NestedRoTxn`] is a read-only child of the active [`RwTxn`] that sees the
//! writer's uncommitted state (TXN-26; the feature LMDB ITS#10395 describes).
//! It reads the writer's dirty frames first, then the read-only map (TXN-27).
//! It claims no reader-table slot (TXN-25/32): the writer already gates GC at
//! `writer_txnid − 1`, and no allocation can occur while children live.
//!
//! ## Safety without `unsafe`
//!
//! The child holds a `&'p RwTxn`, so the borrow checker enforces writer
//! quiescence (TXN-29/30): while a child is alive, no `&mut RwTxn` method can
//! be called. The child is [`Send`] because `RwTxn` is `Sync` (ADR-0007), and
//! the compile-time assertions in this module's tests fail the build if
//! `RwTxn` stops being `Sync`. heed's child, by contrast, holds a raw
//! `MDB_txn` pointer and needs an `unsafe impl Send`.
//!
//! ## What the child sees
//!
//! The child's [`TxnRead`] forwards to the parent's. TXN-26 defines the
//! snapshot as the working roots at child-open time, and forwarding is
//! equivalent to that: the parent cannot be mutated between the first child's
//! open and the last child's drop, so its roots, open databases and dirty set
//! are constant for as long as any child can read them.
//!
//! ## The runtime backstop (TXN-29) and its memory ordering
//!
//! The live-child counter covers callers that bypass the borrow checker
//! (`unsafe` or FFI at an adapter boundary, such as lifetime-erased children).
//! Every mutating `RwTxn` entry checks `live() == 0` and otherwise returns
//! [`MdbError::BadTxn`](crate::error::MdbError::BadTxn), as LMDB does for a
//! transaction that has a child. The orderings:
//!
//! - open: `fetch_add(1, Relaxed)`. The opener already holds `&RwTxn`, and
//!   the child reaches its worker through the happens-before edge of the
//!   thread spawn or channel send, so the increment needs no ordering.
//! - child drop: `fetch_sub(1, Release)`. Every read the child performed
//!   happens-before any observer of the count reaching 0.
//! - writer-side check: `load(Acquire)`. It pairs with the `Release`
//!   decrements, so all child activity happens-before the writer mutates or
//!   commits, even without a join edge.
//!
//! `mem::forget(child)` consumes the child, so no read through it can happen
//! afterwards and nothing stays aliased. The counter never returns to 0, and
//! every later mutating operation and `commit` returns `BadTxn`: the writer
//! is blocked, and no memory is misused.

use crate::btree::Source;
use crate::page::DBRecord;
use crate::rotxn::{DbSel, TxnRead};
use crate::rwtxn::RwTxn;
use crate::sync::{AtomicUsize, Ordering};

/// The live-child counter on a write txn (SPEC 04 TXN-31, ADR-0007). It is an
/// `AtomicUsize` because a `Cell` would make `RwTxn` not `Sync`, and children
/// may drop on worker threads. It is its own type so that the loom models
/// below check the code that ships.
#[derive(Debug)]
pub(crate) struct ChildCounter(AtomicUsize);

impl ChildCounter {
    pub(crate) fn new() -> ChildCounter {
        ChildCounter(AtomicUsize::new(0))
    }

    /// A child was opened. `Relaxed`: the opener already holds `&RwTxn` and
    /// the child reaches its worker through a spawn/send happens-before edge.
    /// The counter is only checked through the `Acquire` load in
    /// [`ChildCounter::live`] (see the module docs).
    pub(crate) fn bump(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// A child dropped. `Release`: pairs with the `Acquire` in
    /// [`ChildCounter::live`] so every read the child performed
    /// happens-before the writer observes quiescence and resumes mutating
    /// (SPEC 04 TXN-31). The loom models check the shape of this protocol but
    /// cannot detect a weakening of this pair to `Relaxed`; see the note
    /// above the loom tests.
    pub(crate) fn release(&self) {
        let prev = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!(prev != 0, "child counter underflow");
    }

    /// Live children right now. `Acquire`: see [`ChildCounter::release`].
    pub(crate) fn live(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// A read-only child of the active write txn, seeing its uncommitted state
/// (SPEC 00 row 16, SPEC 04 §5; semantics of the LMDB fork only).
///
/// Obtained from [`RwTxn::nested_read_txn`] (or
/// [`Env::nested_read_txn`](crate::env::Env)). Reads through the writer's
/// dirty frames first, then the map (TXN-27); usable with the whole
/// [`Database`](crate::rotxn::Database) read API via [`TxnRead`]. Arbitrarily
/// many children may be live concurrently (TXN-28), each `Send`, so a caller
/// can move one to each worker thread while the writer is paused. While any
/// child lives the parent cannot mutate, commit, or abort: at compile time
/// through the shared borrow (TXN-30), at run time through the child counter
/// (TXN-29, [`MdbError::BadTxn`](crate::error::MdbError)). The child cannot
/// outlive its parent (TXN-31/33).
///
/// Covariant in `'p` (like heed's `RoTxn`): the heed adapter narrows a
/// child's lifetime when wrapping it.
pub struct NestedRoTxn<'p> {
    parent: &'p RwTxn<'p>,
    /// This child's own state in the env's slot table (ADR-0024): what it
    /// opens is its own, freed when it is dropped and published when it
    /// commits. It also accepts every handle its parent accepts.
    handles: crate::handles::TxnHandles,
}

impl<'p> NestedRoTxn<'p> {
    /// The identity of the environment this transaction belongs to
    /// ([`crate::env::Env::ident`]) — its parent's.
    #[must_use]
    pub fn env_ident(&self) -> usize {
        self.parent.env_ident()
    }

    /// Open a child of `parent`, bumping the live-child counter.
    /// `RwTxn::nested_read_txn` is the only caller and first refuses an
    /// errored parent with `MDB_BAD_TXN`, as `mdb_txn_begin` does.
    pub(crate) fn open(parent: &'p RwTxn<'p>) -> NestedRoTxn<'p> {
        parent.children().bump();
        NestedRoTxn {
            parent,
            handles: parent.env_inner().handles().begin(),
        }
    }

    /// `commit` (heed's `RoTxn::commit` on a nested read txn): end the child,
    /// keeping the databases it opened for transactions that begin after it.
    /// Its parent, begun before, does not gain them (ADR-0024, as LMDB).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`crate::error::Result`] for heed's API shape.
    pub fn commit(mut self) -> crate::error::Result<()> {
        self.end_handles(true);
        Ok(())
    }

    fn end_handles(&mut self, committed: bool) {
        if self.handles.is_idle() {
            return;
        }
        let parent = self.parent;
        parent
            .env_inner()
            .handles()
            .end(&mut self.handles, committed);
    }

    /// The snapshot this child observes: the **writer's** txnid (TXN-26 —
    /// the in-progress state, not the last committed meta).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.parent.txnid()
    }
}

impl Drop for NestedRoTxn<'_> {
    fn drop(&mut self) {
        // ADR-0024: an end without a commit frees what this child opened.
        // TXN-31: the decrement uses `Release` (see ChildCounter::release) and
        // may run on a worker thread. The parent needs no explicit resume: its
        // next `&mut` operation passes the TXN-29 guard.
        self.end_handles(false);
        self.parent.children().release();
    }
}

/// The child reads the parent's view (ADR-0007; the module docs explain why
/// this equals a snapshot taken at open). The whole read API therefore works
/// on a child unchanged.
impl TxnRead for NestedRoTxn<'_> {
    fn source(&self) -> Source<'_> {
        self.parent.source()
    }
    fn main_record(&self) -> &DBRecord {
        self.parent.main_record()
    }
    fn free_record(&self) -> &DBRecord {
        self.parent.free_record()
    }
    fn free_annex_count(&self) -> u64 {
        self.parent.free_annex_count()
    }
    fn page_size(&self) -> u32 {
        self.parent.page_size()
    }
    fn record_for(&self, db: &crate::rotxn::Database) -> DBRecord {
        self.parent.record_for(db)
    }
    fn handle_state(
        &self,
    ) -> (
        &crate::handles::HandleTable,
        &crate::handles::TxnHandles,
        Option<&crate::handles::TxnHandles>,
    ) {
        let (table, parent, _) = self.parent.handle_state();
        (table, &self.handles, Some(parent))
    }
    fn comparator_for(&self, sel: DbSel) -> crate::cmp::KeyCmp<'_> {
        self.parent.comparator_for(sel)
    }
    fn validated_pages(&self) -> Option<&crate::btree::ValidatedPages<'_>> {
        // The parent writer is immutably borrowed for this nested reader's
        // whole life, so its map-gated memo stays sound here.
        self.parent.validated_pages()
    }
}

// Loom models of the child counter (ADR-0007). Run with `just loom`. Each
// model has two spawned threads plus main, so loom explores it exhaustively.
//
// Limit: weakening both counter orderings to `Relaxed` is not caught here.
// The violating execution is load-buffering-shaped, which loom does not
// explore (tokio-rs/loom#180), yet it is real on AArch64: the child's frame
// load could complete after its relaxed decrement becomes visible. The
// `Release`/`Acquire` pair (SPEC 04 TXN-31) therefore rests on the per-site
// comments, not on loom. The first model checks the protocol shape (for
// example, decrementing before the read fails it); the second checks the
// case where the writer joins the workers.

#[cfg(all(test, loom))]
mod loom_tests {
    use super::ChildCounter;
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::thread;
    use std::sync::Arc;

    /// The TXN-29 backstop with the `Release` decrement and `Acquire` load as
    /// the only synchronization. This models an `unsafe` or FFI bypass of the
    /// borrow checker, the case the runtime guard exists for. Two "children" on
    /// worker threads read a model dirty frame and release; the "writer"
    /// (main thread) waits until it observes quiescence through the `Acquire`
    /// load, then mutates the frame. No child read may observe the mutation.
    #[test]
    fn loom_l6a_children_release_vs_writer_resume_counter_only() {
        loom::model(|| {
            let children = Arc::new(ChildCounter::new());
            // The model "dirty frame". Relaxed accesses on purpose: the
            // happens-before must come from the counter pairing alone.
            let frame = Arc::new(AtomicU64::new(1));

            // The writer opens two children before spawning, on one thread:
            // this program order is what justifies the `Relaxed` bump.
            children.bump();
            children.bump();

            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let (c, f) = (Arc::clone(&children), Arc::clone(&frame));
                    thread::spawn(move || {
                        let read = f.load(Ordering::Relaxed); // child read of the frozen frame
                        c.release(); // Drop impl: Release decrement
                        read
                    })
                })
                .collect();

            // The writer resumes only once the `Acquire` load observes 0, as
            // `RwTxn::guard_ok` requires. The yield tells loom this is a spin
            // loop, which keeps the model bounded.
            while children.live() != 0 {
                thread::yield_now();
            }
            frame.store(2, Ordering::Relaxed); // the first post-resume mutation

            for h in handles {
                let read = h.join().unwrap();
                assert_eq!(
                    read, 1,
                    "a child read observed the writer's post-resume mutation: \
                     the Release/Acquire counter pairing failed to order \
                     child reads before writer resume (TXN-29/31)"
                );
            }
        });
    }

    /// The intended usage: the writer joins the workers (as a scoped thread
    /// pool does) before resuming. After the join the counter must read 0, so
    /// the runtime guard never blocks a writer that used children correctly,
    /// and the join edge orders the mutation after every child read.
    #[test]
    fn loom_l6b_join_edge_then_quiescent() {
        loom::model(|| {
            let children = Arc::new(ChildCounter::new());
            let frame = Arc::new(AtomicU64::new(1));

            children.bump();
            children.bump();

            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let (c, f) = (Arc::clone(&children), Arc::clone(&frame));
                    thread::spawn(move || {
                        let read = f.load(Ordering::Relaxed);
                        c.release();
                        read
                    })
                })
                .collect();

            let mut reads = Vec::new();
            for h in handles {
                reads.push(h.join().unwrap());
            }
            assert_eq!(
                children.live(),
                0,
                "after joining all workers the writer must observe quiescence \
                 (the TXN-29 guard would spuriously block the resumed writer)"
            );
            frame.store(2, Ordering::Relaxed);
            assert!(reads.iter().all(|&r| r == 1));
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::env::testutil::mem_env;
    use crate::error::{Error, MdbError};

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 22; // 4 MiB: room for the overflow-value scenario

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    /// The heed adapter needs `NestedRoTxn<'p>` to be covariant in `'p`
    /// (ADR-0007), because heed returns a covariant `RoTxn<'a>`. This function
    /// compiles only while the type stays covariant.
    #[allow(dead_code)]
    fn _assert_covariant<'short, 'long: 'short>(x: NestedRoTxn<'long>) -> NestedRoTxn<'short> {
        x
    }

    /// ADR-0007 requires `RwTxn` to stay `Sync`, which makes `NestedRoTxn`
    /// (it holds a `&RwTxn`) `Send` with no `unsafe impl`. A `RwTxn` field
    /// that breaks `Sync` fails the build here.
    #[test]
    fn rwtxn_sync_contract_and_child_send() {
        assert_sync::<RwTxn<'static>>();
        assert_send::<NestedRoTxn<'static>>();
        // Children are also `Sync`, so worker threads may share one behind
        // `&`.
        assert_sync::<NestedRoTxn<'static>>();
    }

    /// TXN-26/27/28 on the heap dirty store (TXN-49 scenario 3): children see
    /// uncommitted puts, both inline values and a multi-page dirty overflow
    /// run; several children coexist; borrows point into the dirty frames.
    /// `mem_env` cannot commit, so reading committed pages through a child
    /// (the map side of TXN-27) is tested on real files in the `zerodb` crate.
    #[test]
    fn children_read_uncommitted_state() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();

        let mut txn = env.write_txn().unwrap();
        db.put(&mut txn, b"inline", b"uncommitted-inline").unwrap();
        let big = vec![0xAB_u8; 3 * PS as usize]; // spans a 4-page overflow run
        db.put(&mut txn, b"overflow", &big).unwrap();

        // TXN-28: several concurrent children, opened through both entry
        // points.
        let c1 = txn.nested_read_txn().unwrap();
        let c2 = env.nested_read_txn(&txn).unwrap();
        assert_eq!(c1.txnid(), txn.txnid()); // TXN-26
        assert_eq!(c2.txnid(), txn.txnid());

        for child in [&c1, &c2] {
            // Uncommitted inline value out of a dirty frame.
            assert_eq!(
                db.get(child, b"inline").unwrap(),
                Some(b"uncommitted-inline".as_slice())
            );
            // Uncommitted multi-page overflow value out of a dirty run frame.
            assert_eq!(db.get(child, b"overflow").unwrap(), Some(big.as_slice()));
            // Iteration sees the writer's in-progress view.
            let keys: Vec<Vec<u8>> = db.iter(child).map(|r| r.unwrap().0.to_vec()).collect();
            assert_eq!(keys, vec![b"inline".to_vec(), b"overflow".to_vec()]);
        }

        // A borrow taken through a child stays valid while children live
        // (TXN-41 stable frames; no invalidation point is reachable).
        let borrowed = db.get(&c1, b"inline").unwrap().unwrap();
        drop(c2);
        assert_eq!(borrowed, b"uncommitted-inline");
        drop(c1);

        // Once the last child drops, the writer mutates again (TXN-29), and
        // a later child sees both the earlier and the new writes.
        db.put(&mut txn, b"after", b"resumed").unwrap();
        let c3 = txn.nested_read_txn().unwrap();
        assert_eq!(db.get(&c3, b"after").unwrap(), Some(b"resumed".as_slice()));
        assert_eq!(db.get(&c3, b"overflow").unwrap(), Some(big.as_slice()));
        drop(c3);
        txn.abort();
    }

    /// `mem::forget(child)` never decrements the counter (ADR-0007). The
    /// writer is then blocked for good but sound: every mutating operation
    /// and `commit` returns `BadTxn` (TXN-29/33), nothing panics, and nothing
    /// dangles, since `forget` consumed the child. `RwTxn` has no assertion
    /// at drop that the counter is 0: the borrow checker rules out a live
    /// child at parent drop, so a nonzero counter there can only come from
    /// this sound `forget` path.
    #[test]
    fn forgotten_child_blocks_writer_soundly() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        db.put(&mut txn, b"k", b"v").unwrap();

        let child = txn.nested_read_txn().unwrap();
        std::mem::forget(child); // borrow ends here; counter stays at 1

        // TXN-29 runtime guard on every mutating entry point.
        match db.put(&mut txn, b"k2", b"v2") {
            Err(Error::Mdb(MdbError::BadTxn)) => {}
            other => panic!("put with a leaked child must be BadTxn, got {other:?}"),
        }
        match db.delete(&mut txn, b"k") {
            Err(Error::Mdb(MdbError::BadTxn)) => {}
            other => panic!("delete with a leaked child must be BadTxn, got {other:?}"),
        }
        match db.clear(&mut txn) {
            Err(Error::Mdb(MdbError::BadTxn)) => {}
            other => panic!("clear with a leaked child must be BadTxn, got {other:?}"),
        }
        // Reads on the parent remain fine (nothing is aliased; &self only).
        assert_eq!(db.get(&txn, b"k").unwrap(), Some(b"v".as_slice()));

        // TXN-33: commit refuses before writing anything. It consumes the
        // txn either way, and the drop that follows is an ordinary abort.
        match txn.commit() {
            Err(Error::Mdb(MdbError::BadTxn)) => {}
            other => panic!("commit with a leaked child must be BadTxn, got {other:?}"),
        }
    }

    /// An errored parent refuses new children with `MDB_BAD_TXN`, as
    /// `mdb_txn_begin` does on an `MDB_TXN_ERROR` parent.
    #[test]
    fn errored_parent_refuses_children() {
        let env = mem_env(PS, MAP);
        let db = env.main_database();
        let mut txn = env.write_txn().unwrap();
        // Try to drive the txn into the errored state with a map-sized value
        // (MapFull).
        let huge = vec![0u8; MAP as usize]; // provably exceeds the map
        match db.put(&mut txn, b"huge", &huge) {
            Err(_) => {}
            Ok(()) => panic!("a map-sized value must not fit"),
        }
        // Whether that failure errored the txn depends on where it failed.
        // If the txn is now errored, nested_read_txn must refuse with BadTxn.
        match txn.nested_read_txn() {
            Ok(child) => {
                // The put was rejected before it touched the txn, which is
                // still healthy: the child works as usual.
                assert_eq!(db.get(&child, b"huge").unwrap(), None);
            }
            Err(Error::Mdb(MdbError::BadTxn)) => {} // errored parent: correct refusal
            Err(other) => panic!("unexpected error {other:?}"),
        };
    }
}
