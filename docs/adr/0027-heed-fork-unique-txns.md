# ADR-0027: The API of Meilisearch's heed fork, behind a feature

- Status: Accepted (Option B, maintainer, 2026-10-18), implemented (#115). Both open questions resolved below.
- Related issue: #102
- Date: 2026-10-18

## Context

`heed-zerodb` reproduces the public API of heed 0.22.1 one to one, and
`crates/heed-shim` (a crate named `heed`) is the `[patch]` target consumers
point at (ADR-0003). From v1.55.0, Meilisearch no longer builds against stock
heed 0.22.1. Its manifest patches `heed` to a branch of its own fork,
[`meilisearch/heed@mutexify-open-dbi`](https://github.com/meilisearch/heed/compare/v0.22.1...mutexify-open-dbi)
(4 commits on heed 0.22.1; v1.55.0's lock file pins `14b1b33`, the branch
head). Its `cellulite` dependency takes heed from the same branch. With the
shim patched in, milli does not compile (`cannot find type UniqueRwTxn in
crate heed`).

The fork serializes opening and creating databases behind one mutex per
environment. LMDB requires that `mdb_dbi_open` is not called from several
transactions at once, and heed 0.22.1 leaves that to the caller. The fork's
API changes:

- **New transaction types.** `UniqueRwTxn<'e>` and `UniqueRoTxn<'e, T>` wrap an
  ordinary `RwTxn` / `RoTxn` together with the mutex guard, and deref to it
  (`UniqueRwTxn` also `DerefMut`). `UniqueTxnRef<'a, 'e>` borrows one of them
  with its guard; the trait `AsUniqueTxnRef<'e>` produces it and is
  implemented for `UniqueRoTxn` (all three TLS markers), `UniqueRwTxn`, and
  `&U` for any implementer. `UniqueTxnRef` is public but not re-exported.
- **New `Env` methods.** `unique_write_txn`, `try_unique_write_txn`,
  `unique_read_txn`, `try_unique_read_txn`. The blocking forms take the
  mutex, clearing poison if a previous holder panicked; the `try_` forms
  return `Ok(None)` when it is held. The mutex is taken before the
  transaction begins.
- **Changed signatures.** `Env::open_database<'e, KC, DC, U>(&'e self, &U, ..)`
  and `DatabaseOpenOptions::open<U>(&self, &U)` with `U: AsUniqueTxnRef<'e>`;
  `Env::create_database` and `DatabaseOpenOptions::create` take
  `&mut UniqueRwTxn`. A plain `RoTxn` or `RwTxn` can no longer open or create
  a database.
- **Moved.** `RoTxn::commit` becomes `UniqueRoTxn::commit`.
- **Removed.** `Env::non_free_pages_size`.
- **Added.** `EnvStat::non_free_page_size` and
  `DatabaseStat::non_free_page_size`: branch + leaf + overflow pages × page
  size, plain arithmetic over the stat.

Meilisearch v1.55.0 uses `unique_write_txn` (14 sites), `unique_read_txn` (8),
`UniqueRwTxn` (35), `UniqueRoTxn` (5) and `non_free_page_size` (14), and
none of the `try_` forms (counted in its `crates/` tree).

ZeroDB does not need the mutex. Opening a database is already safe from
several threads at once, and handle lifetimes follow LMDB's rules (ADR-0024),
including those for a read transaction that commits or is dropped. What the
mutex adds for a caller is ordering: a unique transaction waits for the
previous one to end, and `try_` reports that one is running. A caller may
rely on that for its own mutual exclusion, so the shim has to reproduce it.

hannoy (v0.1.7), the oracle (`crates/zerodb-oracle`, which links real heed
0.22.1 next to `heed-zerodb`) and `zerodb-tools` all use heed 0.22.1's API,
as does Meilisearch up to v1.54.

## Options

### Option A — Follow the fork in place of heed 0.22.1

`heed-zerodb` switches to the fork's API outright.

- Simplest: one API, one shim.
- Breaks every heed 0.22.1 consumer: hannoy, Meilisearch before v1.55, the
  oracle's A/B harness and `zerodb-tools`. The oracle would have to diff
  against the fork too, which links the same C LMDB but is not on crates.io.
- Rejected.

### Option B — Both APIs, chosen by a cargo feature (chosen)

A `unique-txns` feature on `heed-zerodb` switches its API to the fork's.
Without it nothing changes. A second patch target,
`crates/heed-shim-unique-txns` (also a crate named `heed`, version 0.22.1, as
the fork is), depends on `heed-zerodb` with the feature on.
`scripts/consumer.sh` picks it when the consumer's committed `Cargo.lock`
resolves heed from the `mutexify-open-dbi` branch, and `HEED_SHIM` overrides
the choice.

- One implementation; the feature only swaps the transaction-type entry
  points. Every `Database` method, iterator and codec is the same code in
  both builds.
- The feature is **not additive**, which goes against cargo's convention:
  it changes two signatures and removes two items, exactly as the fork does.
  A build that unifies it on sees the fork's API everywhere. That is the
  intended use, since a consumer builds against one heed or the other, and
  the workspace never turns it on (the oracle and tools would not compile).
- A second shim crate is needed because a consumer's crates depend on plain
  `heed` and none of them asks for `unique-txns`: the patch path has to make
  the choice.
- The existing heed 0.22.1 test files do not compile against the fork's API.
  They are compiled without the feature only, and the feature gets its own
  test file. CI runs both.

### Option C — Accept both APIs at once

Keep `RoTxn::commit` and `non_free_pages_size`, and make `open_database` /
`create_database` generic over both plain and unique transactions.

- Fails on the fork's call sites: `open_database::<Str, Str, _>` names three
  type parameters, heed 0.22.1's `open_database::<Str, Str>` two, and no one
  signature accepts both spellings.
- Opening a database through a plain transaction would stay possible, which
  the fork forbids; code written against the shim could then fail to build
  against the fork with LMDB.
- Rejected.

## Decision

Option B, mirroring the fork exactly:

1. **Mutex.** One `Mutex<()>` per environment, created at open and shared by
   every `Env` clone. A second open of the same environment in one process
   already fails with `EnvAlreadyOpened`, so an environment has exactly one
   mutex. As in the fork:
   - The blocking constructors clear a poisoned mutex and take it; the
     mutex guards no data.
   - The mutex is taken before the transaction begins, so
     `unique_write_txn` holds it while it waits for the writer lock, and
     `try_unique_write_txn` returns `None` only for the mutex, never for
     the writer lock.
   - Ordinary `read_txn` / `write_txn` do not take it.
   - `UniqueRwTxn` and `UniqueRoTxn` hold a `MutexGuard`, so they are not
     `Send`, as in the fork.
2. **`RoTxn::commit`** is not compiled with the feature.
   `UniqueRoTxn::commit` has its semantics: it keeps the databases the
   transaction opened, and dropping instead closes them (ADR-0024). Nested
   read transactions therefore cannot commit under the feature, as in the
   fork.
3. **`Env::non_free_pages_size`** is not compiled with the feature.
   `EnvStat::non_free_page_size` and `DatabaseStat::non_free_page_size` are
   added, with the fork's arithmetic. Summing them over the main database and
   every named database gives SPEC 05 GC-23's definition, as heed 0.22.1's
   `non_free_pages_size` did.
4. **Not changed:**
   - LMDB behaviour, the engine, the file format and the default build.
   - No new dependency: `std::sync::Mutex` only.
   - No `unsafe`.

## Consequences

- Meilisearch v1.55 and later can build on ZeroDB:
  `MEILISEARCH_REF=v1.55.0 scripts/consumer.sh check` picks the new shim.
  The gate's default ref is v1.55.0; v1.53.1, the last release on stock
  heed 0.22.1, stays runnable with `MEILISEARCH_REF=v1.53.1`.
- Two shim crates to keep in step. Both re-export the same crate, and their
  feature lists must match.
- CI gains a `heed-zerodb` test run with `--features unique-txns` and a check
  of the new shim.
- Tests (`crates/heed-zerodb/tests/unique_txns.rs`):
  - creating and opening through unique transactions and
    `DatabaseOpenOptions`;
  - `UniqueRoTxn::commit` keeping, and a drop closing, the databases it
    opened;
  - the `try_` forms returning `None` while the mutex is held, and plain
    transactions not taking it;
  - unique transactions on two clones waiting for each other;
  - poison recovery;
  - `non_free_page_size`;
  - generic use of `AsUniqueTxnRef`.
- Not tested: that a plain transaction cannot open a database. That is a
  compile-time property, and pinning it needs a compile-fail harness the
  workspace does not have.
- Docs:
  - `docs/COMPATIBILITY.md` and `docs/CONSUMER-GATE.md` describe the feature
    and the second shim;
  - SPEC 00 row 19 notes the replacement of `non_free_pages_size`;
  - `CHANGELOG.md` records the change.
- If the fork changes again, or is merged upstream into a heed release,
  this ADR is amended. If upstream heed adopts the API, the feature may
  become the default.

## Resolved questions

Both resolved by the maintainer on 2026-10-18.

1. **Feature and crate names.** Kept: `unique-txns` and
   `crates/heed-shim-unique-txns`. They name the API, not Meilisearch's
   branch (`mutexify-open-dbi`), so they still fit if upstream heed adopts
   it.
2. **Gate default.** The default `MEILISEARCH_REF` moves to v1.55.0 now.
