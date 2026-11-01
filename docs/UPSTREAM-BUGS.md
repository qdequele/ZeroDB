# Bugs found in the LMDB reference

ZeroDB's differential fuzzer runs every operation against the Meilisearch LMDB
fork (`mdb.master.nested-rtxns`, commit `cd767228`, vendored in
`lmdb-master-sys 0.2.6` / heed 0.22.1). Occasionally the reference itself
crashes. Such crashes are not ZeroDB divergences. Each one gets a repro in
this repo and a guard in the harness so fuzzing can continue; the guard is
removed once the fork is fixed.

## FORK-1: `SIGSEGV` in `_mdb_cursor_put` (APPEND after a same-txn clear of another database)

**Status:** open, not yet reported upstream (no matching issue on
`meilisearch/lmdb`). Found 2026-07-16 by the `diff_ops` fuzz target.

**Recipe** (all four steps are required):

1. Write txn A: create the named database `X` and commit.
2. Write txn B: create a second database `Y` and `clear(Y)`.
3. Still in txn B: an `MDB_APPEND` put of `(k1, v)` into `X` succeeds.
4. Still in txn B: an `MDB_APPEND` put of `(k2, v')` into `X` with `k2 <= k1`
   should return `MDB_KEYEXIST`, but crashes with `SIGSEGV` in
   `_mdb_cursor_put` (a near-null dereference, called from `mdb_put`).

Value sizes are irrelevant. Removing the `clear`, removing the first
successful append, or using an ascending key avoids the crash. Under ASan:
`SEGV on unknown address 0x00000000000a` at `_mdb_cursor_put+0x8b8`.

**Repro:** `crates/zerodb-oracle/examples/fork_segv_repro.rs`.

```bash
cargo build -p zerodb-oracle --example fork_segv_repro
VARIANT=with_clear ./target/debug/examples/fork_segv_repro   # exits 139
```

**Exposure:** milli uses both `clear` and `APPEND` puts while indexing, so the
combination inside one write txn is plausible.

**Harness guard:** `driver::classify` skips an `APPEND` put in any write txn
that has run a `ClearDb` (`Skip::KnownForkBug`), on both engines. Pinned by
`crates/zerodb-oracle/tests/fork_bug_guard.rs`.

**ZeroDB:** returns `KeyExist`, as SPEC 01 §S1 requires
([D-007](LMDB-COMPATIBILITY.md#d-007)).
