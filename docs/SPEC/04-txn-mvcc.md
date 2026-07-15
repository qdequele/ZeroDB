# SPEC 04 — Transactions & MVCC (Phase 0.4 deliverable)

Status: TO BE WRITTEN. Single-writer protocol, txnID assignment, snapshot
semantics of RoTxn, reader table design (in-process, lock-free slots, explicit
orderings), oldest-reader computation, nested txn shadowing, commit pipeline
with the exact fsync barrier sequence and the invariant at every crash point.

Must include a **write-txn value-borrow contract** section: a `get` during a
write txn may return bytes from a dirty page in heap memory (not the mmap).
Dirty-page storage must never move or reallocate while `&'txn [u8]` borrows
are live — this is a Rust soundness requirement (the likeliest soundness bug
outside the reader table). Define exactly which operations may invalidate
which borrows, how heed's borrow rules (`get` = &Txn, `put` = &mut Txn) map
onto it, and the miri tests that exercise get-then-put sequences.
