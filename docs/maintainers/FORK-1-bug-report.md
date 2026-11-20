# Draft bug report: FORK-1

This is a ready-to-file report for [FORK-1](../UPSTREAM-BUGS.md#fork-1-sigsegv-in-_mdb_cursor_put-put-into-a-named-database-after-a-same-txn-clear-of-the-main-database).
It has **not been filed**. Stock LMDB crashes too (see "Stock LMDB" below),
so it belongs on the OpenLDAP ITS (LMDB's upstream tracker) as well as on
`meilisearch/lmdb`. Link each report from `docs/UPSTREAM-BUGS.md`.

Everything between the two rules is the report.

---

**Title:** SIGSEGV in `_mdb_cursor_put` after `mdb_drop(MAIN_DBI, 0)` and two
puts into an open named database in the same write transaction

**Version:** `mdb.master.nested-rtxns` at `cd767228` (the version vendored by
`lmdb-master-sys 0.2.6` / heed 0.22.1, reporting `LMDB 0.9.70`). Also
stock OpenLDAP LMDB: `mdb.master` at `74dd6c3` and the `LMDB_0.9.36`
release. macOS aarch64 and Linux.

**Summary.** In one write transaction: empty the main database with
`mdb_drop(txn, MAIN_DBI, 0)`, then put twice into a named database that was
created in an earlier transaction and is still empty. The first put
succeeds. The second put dereferences a null page pointer in
`_mdb_cursor_put`, whatever its flags: an `MDB_APPEND` put that should fail
with `MDB_KEYEXIST`, an `MDB_APPEND` put that should succeed, and a put with
no flags all crash.

**Steps.**

1. Write txn A: `mdb_dbi_open(txn, "x", MDB_CREATE)`, commit.
2. Write txn B: `mdb_dbi_open(txn, NULL, 0)` and `mdb_drop` it with `del = 0`.
3. In txn B: put one key into `x` (with or without `MDB_APPEND`). Succeeds.
4. In txn B: put a second key into `x`. Crashes.

**Expected.** Step 4 returns `MDB_KEYEXIST` for an `MDB_APPEND` key that
does not sort last, and succeeds otherwise; or, if putting into `x` after its
record was deleted from the main database is not allowed, an error such as
`MDB_BAD_DBI`.

**Actual.** `SIGSEGV`. Under AddressSanitizer:

```text
ERROR: AddressSanitizer: SEGV on unknown address 0x00000000000a
    #0 in _mdb_cursor_put mdb.c:8107
    #1 in mdb_put mdb.c:10245
    #2 in main repro.c:52
```

`mdb.c:8107` (line numbers of the version above) is
`nsize = IS_LEAF2(mc->mc_pg[mc->mc_top]) ? ...` after `new_sub:`, with
`mc->mc_pg[mc->mc_top] == NULL`; offset `0xa` is `mp_flags` in `MDB_page`.

**Reproduction.**

```c
/* mdb_drop(txn, MAIN_DBI, 0) followed by puts into a named database that is
 * already open, in the same write transaction: the second put crashes.
 *
 *   cc -I<liblmdb> repro.c <liblmdb>/mdb.c <liblmdb>/midl.c -o repro
 *   ./repro <empty-dir> [noclear] [asc] [plain]
 *
 * Default: the second put is MDB_APPEND with a key that sorts before the
 * first one, so it should fail with MDB_KEYEXIST.
 *   noclear  skip the mdb_drop (no crash)
 *   asc      the second key sorts after the first (should succeed)
 *   plain    the second put has no flags (should succeed)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lmdb.h"

#define CHECK(x) do { int rc_ = (x); if (rc_) { \
    fprintf(stderr, "%s: %s\n", #x, mdb_strerror(rc_)); exit(1); } } while (0)

static int has(int argc, char **argv, const char *opt) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], opt)) return 1;
    return 0;
}

int main(int argc, char **argv) {
    MDB_env *env; MDB_txn *txn; MDB_dbi x, main_dbi;
    unsigned char k1[2] = {0xbe, 0x68}, k2[500], v2[64] = {0};
    MDB_val key, val;

    if (argc < 2) { fprintf(stderr, "usage: %s <dir> [noclear] [asc] [plain]\n", argv[0]); return 1; }
    memset(k2, has(argc, argv, "asc") ? 0xff : 0x00, sizeof k2);

    CHECK(mdb_env_create(&env));
    CHECK(mdb_env_set_maxdbs(env, 4));
    CHECK(mdb_env_open(env, argv[1], 0, 0600));

    /* Transaction A: create the named database "x". */
    CHECK(mdb_txn_begin(env, NULL, 0, &txn));
    CHECK(mdb_dbi_open(txn, "x", MDB_CREATE, &x));
    CHECK(mdb_txn_commit(txn));

    /* Transaction B: empty the main database, then two puts into "x". */
    CHECK(mdb_txn_begin(env, NULL, 0, &txn));
    CHECK(mdb_dbi_open(txn, NULL, 0, &main_dbi));
    if (!has(argc, argv, "noclear")) CHECK(mdb_drop(txn, main_dbi, 0));

    key.mv_size = sizeof k1; key.mv_data = k1; val.mv_size = 0; val.mv_data = NULL;
    CHECK(mdb_put(txn, x, &key, &val, MDB_APPEND));          /* succeeds */

    key.mv_size = sizeof k2; key.mv_data = k2; val.mv_size = sizeof v2; val.mv_data = v2;
    int rc = mdb_put(txn, x, &key, &val, has(argc, argv, "plain") ? 0 : MDB_APPEND);
    printf("%s\nsecond put -> %d (%s)\n", mdb_version(NULL, NULL, NULL), rc, mdb_strerror(rc));

    mdb_txn_abort(txn);
    mdb_env_close(env);
    return 0;
}
```

Results on the version above (macOS 27.2, aarch64, Apple clang, `-O0`;
three runs each, identical):

| arguments | exit | second put |
|---|---|---|
| (none) | 139 (`SIGSEGV`) | — |
| `asc` | 139 | — |
| `plain` | 139 | — |
| `asc plain` | 139 | — |
| `noclear` | 0 | `MDB_KEYEXIST` |
| `noclear asc` | 0 | success |
| `noclear plain` | 0 | success |
| `noclear asc plain` | 0 | success |

A Rust reproduction through heed 0.22.1 is
`crates/zerodb-oracle/examples/fork_segv_repro.rs` in
<https://github.com/qdequele/ZeroDB> (`VARIANT=with_clear`, exit 139).

**Analysis.** A write transaction starts with `DB_STALE` set on every named
database (`mdb_txn_renew0`), so the first descent into `x` re-reads its
record from the main database (the `DB_STALE` block of `mdb_page_search`).

1. `mdb_drop(MAIN_DBI, 0)` empties the main database, which deletes `x`'s
   record with everything else in it.
2. The first put finds `x`'s cached record with `md_root == P_INVALID`, takes
   the `MDB_NO_ROOT` path and allocates a root leaf without calling
   `mdb_page_search`. `DB_STALE` stays set on `x`.
3. The second put sees a root, so it positions the cursor: `mdb_cursor_last`
   for `MDB_APPEND`, `mdb_cursor_set` otherwise. Both call
   `mdb_page_search`, whose `DB_STALE` block looks `x` up in the now empty
   main database: the inner `mdb_page_search` on `MAIN_DBI` returns
   `MDB_NOTFOUND`, and the block returns it as is.
4. `_mdb_cursor_put` treats `MDB_NOTFOUND` from positioning as "insert
   here" (`if (rc && rc != MDB_NOTFOUND) return rc;`), with a cursor that was
   never positioned (`mc_snum == 0`). `mdb_cursor_touch` does nothing for
   such a cursor, and the first use of `mc->mc_pg[mc->mc_top]` faults.

Two possible fixes: make the `DB_STALE` refresh return `MDB_BAD_DBI` (or
`MDB_NOTFOUND` mapped to it) when the named record is gone, as it already
does when the main database has entries but not this one; or clear
`DB_STALE` on the `MDB_NO_ROOT` path, which would only hide the deleted
record until commit.

**Stock LMDB.** Stock LMDB crashes the same way. The program above, built
against `libraries/liblmdb` of OpenLDAP's `mdb.master` (`74dd6c3`) and of
the `LMDB_0.9.36` release (`24aec4d`), gives the results in the table above
for all eight argument sets on both versions (macOS 27.2, aarch64, Apple
clang, `-O0`, three runs each, identical). Under AddressSanitizer the fault
is at the same statement, `nsize = IS_LEAF2(mc->mc_pg[mc->mc_top]) ? ...`
after `new_sub:`, at address `0xa`: `mdb.c:8122` on `mdb.master` and
`mdb.c:7103` on `LMDB_0.9.36`. The code involved is textually identical in
all three versions: the `DB_STALE` block of `mdb_page_search`, the
`MDB_APPEND` and positioning code at the top of `_mdb_cursor_put`, and
`mdb_drop`.

---

## Notes for ZeroDB (not part of the report)

- ZeroDB returns the expected results for this sequence (D-007).
- The repro and the analysis show the crash is wider than the oracle guard
  assumes: any second put into a named database after a same-transaction
  clear of the main database crashes, not only an `APPEND`. The guard in
  `driver::classify` skips only `APPEND` puts after a `ClearDb`.
