# Bugs found in the LMDB reference

ZeroDB's differential fuzzer runs every operation against the Meilisearch LMDB
fork (`mdb.master.nested-rtxns`, commit `cd767228`, vendored in
`lmdb-master-sys 0.2.6` / heed 0.22.1). Occasionally the reference itself
crashes. Such crashes are not ZeroDB divergences. Each one gets a repro in
this repo and, when the fuzzer's op stream can reach it, a guard in the
harness so fuzzing can continue; the guard is removed once the fork is
fixed.

## FORK-1: `SIGSEGV` in `_mdb_cursor_put` (put into a named database after a same-txn clear of the main database)

**Status:** open, not yet reported upstream (no matching issue on
`meilisearch/lmdb` or the OpenLDAP ITS). Found 2026-07-16 by the `diff_ops` fuzz target; still
reproduces on 2026-11-20. A ready-to-file report, with a standalone C
reproduction and the cause, is in
[maintainers/FORK-1-bug-report.md](maintainers/FORK-1-bug-report.md).
Stock LMDB is affected too: the same C reproduction crashes at the same
statement on OpenLDAP's `mdb.master` (`74dd6c3`) and on the `LMDB_0.9.36`
release (checked 2026-11-20), so the bug is not specific to the fork.

**Recipe** (all four steps are required):

1. Write txn A: create the named database `X` and commit, leaving it empty.
2. Write txn B: clear the main (unnamed) database, the one that holds `X`'s
   record (heed: `create_database(None)` then `clear`).
3. Still in txn B: a put into `X` succeeds.
4. Still in txn B: a second put into `X` crashes with `SIGSEGV` in
   `_mdb_cursor_put` (a near-null dereference, called from `mdb_put`). This
   holds for an `MDB_APPEND` put that should return `MDB_KEYEXIST`, an
   `MDB_APPEND` put that should succeed, and a put with no flags.

Value sizes are irrelevant. Removing the `clear` or the first put avoids the
crash. Dropping the main database (`mdb_drop(MAIN_DBI, 1)`) instead of
clearing it crashes the same way, and so does committing txn B after the
clear and making the two puts in a later write txn: `X`'s record is gone
until the environment is reopened. If `X` already holds keys, nothing
crashes, but every put and lookup in `X` returns `MDB_NOTFOUND`. Under ASan: `SEGV on unknown address 0x00000000000a` at
`_mdb_cursor_put` (`mdb.c:8107`).

**Cause:** the first put allocates `X`'s root without refreshing its stale
record; the second put's refresh finds the main database empty, returns
`MDB_NOTFOUND`, and `_mdb_cursor_put` inserts at the unpositioned cursor. See
the report for the code path.

**Repro:** `crates/zerodb-oracle/examples/fork_segv_repro.rs`.

```bash
cargo build -p zerodb-oracle --example fork_segv_repro
VARIANT=with_clear ./target/debug/examples/fork_segv_repro   # exits 139
```

**Exposure:** the trigger is clearing the main database, which holds the
named databases' records, while named databases are in use. SPEC 02 §6
records that milli stores no keys of its own in the main database; whether
any consumer clears it has not been checked.

**Harness:** no guard. The oracle never opens the root database: its
`DbName::Unnamed` is milli's named `"main"` database, so no fuzzed op stream
can empty the root, and the sequence the fuzzer found (a clear of `"main"`,
then `APPEND` puts into another named database) runs normally on LMDB. The
fuzzer found the crash before `Unnamed` was remapped to `"main"`.
`crates/zerodb-oracle/tests/lmdb_observations/fork1_sequences.rs` runs the
fuzz-shaped sequences on both engines with nothing skipped; a skip guarded
by `driver::classify` would be needed again only if the fuzzed op stream
gained the root database.

**ZeroDB:** returns `KeyExist`, as SPEC 01 §S1 requires
([D-007](LMDB-COMPATIBILITY.md#d-007)).
