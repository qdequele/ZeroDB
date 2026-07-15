# SPEC 04 — Transactions & MVCC (Phase 0.4 deliverable)

Status: TO BE WRITTEN. Single-writer protocol, txnID assignment, snapshot
semantics of RoTxn, reader table design (in-process, lock-free slots, explicit
orderings), oldest-reader computation, commit pipeline with the exact fsync
barrier sequence and the invariant at every crash point.

Nested txns (per SPEC 00 §A / D-003): nested WRITE txns are unsupported
(clean error). Nested READ txns over the active write txn ARE required
(M1.9, fork semantics): a read-only txn parented to the write txn that sees
its uncommitted state, Send, fanned out to rayon workers while the writer is
paused. Spec its aliasing rules against the value-borrow contract below.

Must include a **write-txn value-borrow contract** section: a `get` during a
write txn may return bytes from a dirty page in heap memory (not the mmap).
Dirty-page storage must never move or reallocate while `&'txn [u8]` borrows
are live — this is a Rust soundness requirement (the likeliest soundness bug
outside the reader table). Define exactly which operations may invalidate
which borrows, how heed's borrow rules (`get` = &Txn, `put` = &mut Txn) map
onto it, and the miri tests that exercise get-then-put sequences.
