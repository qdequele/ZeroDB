# ADR-0024: Database handle lifetime (stale handles after abort and remove)

- Status: Approved (Option A), maintainer, 2026-10-17
- Related issue: #73 (divergence D-013)
- Date: 2026-10-17

## Context

In LMDB a database handle (`MDB_dbi`) is a slot in a per-environment table.
`mdb_dbi_open` hands out a slot; the handle is private to the transaction that
opened it until that transaction commits. If the transaction ends without
committing, `mdb_txn_end` frees every slot the transaction opened and bumps
the slot's sequence number. `mdb_drop(.., del = 1)` (heed's
`Database::remove`) frees the slot at once, and an abort does not undo that.
heed 0.22.1's `Database` is a plain `Copy` value around the slot number, so a
consumer can keep using a handle after its slot was freed.

ZeroDB's handle is also a slot number (`DbSel::Named(dbi)`, SPEC 02 §6.1), into
an env-level registry (`EnvInner::named`) that maps slot → name only. That
registry is append-only: a slot is never freed, and nothing records whether
the transaction that assigned it committed. SPEC 02 §6.1 and the
`NamedRegistry` doc comment call this "unobservable", because records always
resolve from the transaction's catalog. It is observable, and one case writes
data that should not exist.

### What LMDB does, and what ZeroDB does today

Observed through heed 0.22.1 on both engines, 20 cases,
`crates/zerodb-oracle/tests/lmdb_dbi_handle_observation.rs` (LMDB pinned,
macOS aarch64, 2026-10-17). "a" and "b" are named databases, `k` a key.

| Case | LMDB | ZeroDB today |
|---|---|---|
| `a` created in an aborted txn; put, get through the handle in a new write txn, commit; does `a` exist? | `EINVAL`, `EINVAL`, ok, no | ok, the value, ok, **yes** |
| the same handle, read from a read txn | `EINVAL` | not found |
| existing `a`, first opened in a write txn that aborts; read | `EINVAL` | the value |
| existing `a`, first opened in a read txn that is dropped; read | `EINVAL` | the value |
| existing `a`, first opened in a read txn that is committed; read | the value | the value |
| `a` already open, opened again in a write txn that aborts; read | the value | the value |
| `a` created in an uncommitted write txn, read from a read txn; again after the commit from that read txn; from a new read txn | `EINVAL`, `EINVAL`, the value | not found, not found, the value |
| `a` removed and committed; put through the old handle | `EINVAL` | ok |
| `a` removed in a txn that aborts; read through the old handle; does `a` exist; read through a new handle | `EINVAL`, yes, the value | the value, yes, the value |
| a read txn open while another txn removes `a` and commits; read from that read txn; from a new one | `MDB_BAD_DBI`, `EINVAL` | the snapshot's value, not found |
| `a` created in an aborted txn, then `b` created with `k = b`; read through the `a` handle | **`b`** (slot reused) | not found |
| `a` created in an aborted txn and created again (`k = 2`) as a second handle; the first handle reads, then writes `k = 1` | `2`, ok, and the second handle reads `1` | the same |
| `a` cleared in a txn that aborts; read | the value | the value |
| `a` removed, then the handle used in the same write txn | `EINVAL` | ok |
| existing `a`, first opened in a nested read txn of a write txn; the child dropped; read from the parent; the parent commits; read | `EINVAL`, `EINVAL` | the value, the value |
| the same, the parent aborts; read | `EINVAL` | the value |
| the same, but the child committed; read from the parent; the parent commits; read | `EINVAL`, the value | the value, the value |
| the same, the child committed, the parent aborts; read | the value | the value |
| a nested read child uses handles its parent created, or first opened, and has not committed | the values | the values |
| two read txns both first-open `a`; the first is dropped; read from the second; it commits; read through each handle from new txns | the value, `MDB_BAD_DBI`, `MDB_BAD_DBI` | the value, the value, the value |

Two things follow.

1. **A write re-creates a database the aborted transaction never created.**
   ZeroDB's write path finds no catalog entry, starts from an empty record,
   and the commit write-back (SPEC 02 §6.1) inserts it. LMDB refuses the
   write. This is the case that makes #73 critical.
2. **LMDB itself is unsafe in one case.** A freed slot is reused by the next
   open of any name, so a stale handle silently reads another database (the
   `a`/`b` row). The sequence number LMDB bumps exists to catch this, but
   it is checked only against transactions begun before the change
   (`MDB_BAD_DBI`), not against the handle, which carries no sequence.

No consumer is known to be affected. Meilisearch runs in production on LMDB,
where every one of these uses already fails with `EINVAL` or `MDB_BAD_DBI`, or
reads the wrong database. So matching LMDB's errors cannot break code that
works on LMDB. The consumer suites are the check that ZeroDB-only code paths
(none are known) do not rely on the leniency.

## Options

### Option A — Handle lifecycle in the engine (LMDB's slot table, plus a handle generation)

The engine's registry becomes LMDB's slot table:

- Each slot holds its name, whether it is open, the env-wide handle epoch at
  which it was published, and a generation counter.
- `open`/`create` reuses the lowest free slot, as LMDB does. A slot the
  transaction assigns is recorded as opened by that transaction.
- Commit (write txn, or `RoTxn::commit`) publishes the transaction's opened
  slots. Ending without a commit (abort, or dropping either kind of txn)
  frees them and bumps their generation.
- `Database::remove` frees the slot and bumps its generation at once. A later
  abort does not undo it.
- A handle is `(slot, generation)`. `zerodb::Database` and heed-zerodb's
  `Database` stay `Copy`; each gains a `u32`.
- Every operation checks the handle before resolving its record:
  - If the slot is free, its generation differs from the handle's, or it was
    published after this transaction began and this transaction did not open
    it: `EINVAL` (`Error::Io`, raw OS error 22, as heed reports it).
  - If the slot was valid when this transaction began but has been freed or
    reused since: `MDB_BAD_DBI`. A new core `MdbError::BadDbi` maps to heed's
    `MdbError::BadDbi`.
- `max_dbs` then counts open slots, as LMDB does. Today it counts every
  distinct name ever seen, including aborted creations.

This matches every LMDB row above except the `a`/`b` row. There the
generation check returns `EINVAL` instead of reading `b`: a deliberate
divergence, stricter than LMDB, filed for approval.

- Pros: one source of truth. The native `zerodb` API and the heed adapter
  behave alike. The write-back bug cannot occur, because a write needs a
  valid handle. `max_dbs` parity comes for free.
- Cons: more code in `zerodb-core`. The registry, the transactions' begin and
  end paths, and every database operation are touched, though each operation
  only gains one check.
- Cost on the read path: the check is one load of the slot's state per
  operation. It is kept in a fixed array of atomics sized `max_dbs` at open,
  so there is no lock and no allocation. A transaction can memo a successful
  check per slot, as the record resolution already does (`named_dense` in
  `rotxn.rs`). Rule 7 numbers (LMDB / before / after, the `get` and cursor
  benchmarks) go in the implementation PR.
- Crash safety: none involved. Handles are process memory; nothing on disk
  changes and no format version moves.
- ARM: the slot state is published by the committing thread and read by
  other threads' transactions. A Release store on publish or free, paired
  with an Acquire load in the check, is enough. A transaction that races a
  concurrent `remove` sees either the old or the new state, and both are
  answers LMDB can give.
- Multi-process: none involved. LMDB's slot table is per process too.

### Option B — Reproduce the errors in the heed adapter only

heed-zerodb keeps its own slot table with the same rules. The engine stays
append-only and lenient.

- Pros: `zerodb-core` is untouched, apart from the re-creation fix below.
- Cons:
  - Two registries that must agree on slot numbers.
  - The native API keeps every lenient row.
  - `max_dbs` still counts aborted names.
  - The re-creation bug is an engine bug and needs an engine fix anyway: a
    write through a slot with no catalog entry must fail.

  So B saves little, and it leaves the native API different from the adapter.

### Option C — Accept the leniency; fix only the re-creation

Approve D-013 as a divergence. Fix only row 1: a write through a handle whose
database has no catalog entry in the transaction fails (`EINVAL`) instead of
creating the database.

- Pros: the smallest change; no handle state.
- Cons: eight of the observed rows keep differing from LMDB, among them
  handles from dropped read txns and removed databases, and `max_dbs`
  counting. The divergence would be "use-after-close is served", the class
  of bug LMDB reports loudly.

## Decision

**Option A**, approved by the maintainer on 2026-10-17. The recommended answers
to questions 2 and 3 stand: a slot reused by another name gives `EINVAL`, and
a remove under an open read txn gives `MDB_BAD_DBI`, as LMDB does.

### The rules

The env keeps one slot table for named databases. The main database has no
slot and its handle is always valid.

- **Slot.** A slot holds a name, a generation, and a state:
  - *Pending*, with the set of live transactions that opened it;
  - *Published*, with the handle epoch at which it was published;
  - *Free*, remembering when it was freed, by which transaction, and when the
    freed instance had been published (if it ever was).

  A handle is `(slot, generation)`.
- **Epoch.** An env-wide counter, bumped on every slot change.
- **Transaction begin.** Every transaction (read, write, nested read) records
  the epoch at its begin.
- **Open and create.** `open_database` / `create_database` find the name's
  slot:
  - A Pending slot: the transaction joins its openers.
  - A Published slot: the transaction is recorded as an opener of that
    instance, so it can use the handle even if the slot was published after
    it began.
  - No slot, or a Free one: the transaction takes a slot and becomes its only
    opener. It reuses the Free slot that held the same name (same
    generation), else the lowest Free slot (the generation is bumped and the
    name changes), else a new slot. With no Free slot and `max_dbs` slots in
    use: `DbsFull`.
- **Commit.** A write txn's commit, or a read txn's `commit`, publishes the
  Pending slots it opened, after the commit succeeds. Ending without a commit
  removes the transaction from its slots' openers. A Pending slot left with no
  opener is freed. A failed write commit counts as an abort.
- **Remove.** `Database::remove` (`drop_db`) frees the slot at once. An abort
  does not undo it.
- **Check.** Every operation on a named database, reads included, first checks
  its handle against the transaction, in this order:
  1. The slot's generation differs from the handle's: `EINVAL`.
  2. The transaction opened this instance of the slot, and the slot is not
     Free: valid.
  3. The slot is Free and this transaction freed it: `EINVAL`.
  4. The slot was freed after the transaction began, and the freed instance
     was published before it began: `MDB_BAD_DBI`.
  5. The slot is Published, at or before the transaction began, and was not
     freed since: valid.
  6. Otherwise: `EINVAL`.

  A nested read child also accepts every handle its parent accepts.

`EINVAL` is `Error::Io` with raw OS error 22, as heed reports it. `MDB_BAD_DBI`
is a new core `MdbError::BadDbi`, mapped to heed's `MdbError::BadDbi`.

Rule 4 looks at the most recent free only. If a slot is freed twice while one
transaction is live, that transaction can get `EINVAL` where LMDB's sequence
check would say `MDB_BAD_DBI`. Both outcomes are errors, and no consumer
removes and re-creates databases under a long read.

### Cost

A check is lock-free in the common case: the transaction's memo cell for the
slot holds the epoch of its last valid verdict and the generation it was for.
The cell is written under the table lock and read as a seqlock, checked by a
loom model. A read txn keeps the cell beside its record memo, so a named read
looks its slot up once. The table lock is taken once per slot after a slot
change. Per-transaction state is two words plus a box allocated only by a
transaction that opens or removes a database.

Measured on an Apple Silicon Mac against the commit before the change, three
interleaved rounds, with LMDB as the drift control: the numbers are in the
implementation PR.

### Divergence D-026 (stricter than LMDB)

Two cases, both where LMDB serves a stale or inconsistent answer:

1. A handle whose slot was reused by another name returns `EINVAL`. LMDB reads
   the other database.
2. Two transactions first-opening the same name share the slot. It stays
   pending while either is live, and is published when either commits. LMDB
   frees it when the first ends, and the handle the second committed then
   gets `MDB_BAD_DBI` in later transactions.

D-013 is retired, its row kept with the ID.

## Consequences

- **Engine.** `NamedRegistry` becomes the slot table described above.
  `Database` gains a generation. Read txns and nested read txns gain a
  `commit`. Freeing a slot clears its comparator and sequential-writes
  registrations, which are per slot. Every named-database operation checks the
  handle. The write-back can no longer insert a catalog entry for a handle
  that was never valid. `MdbError::BadDbi` is added.
- **Adapter.** heed-zerodb carries the generation and maps the two errors to
  heed's (`Error::Io` errno 22, `MdbError::BadDbi`).
- **Tests.**
  - The observation file becomes differential: each case asserts that ZeroDB
    matches the LMDB pin, except the two D-026 cases, which assert ZeroDB's
    outcome.
  - New engine unit tests: slot reuse, generation wrap at `u32::MAX` (a slot
    reused 2^32 times must not validate an old handle), `max_dbs` after
    aborted creations, and an abort racing a read on another thread.
  - Existing tests that use a handle across an abort are reviewed against
    LMDB, never loosened (rule 2). The fuzz harness's handle bookkeeping
    (`dbi_handle_lifetime.rs`) already drops handles as LMDB does.
  - The Meilisearch and hannoy consumer suites must stay green.
- **SPEC.** SPEC 02 §6.1 (the dbi table and its "unobservable" claim, which is
  wrong) and SPEC 04 (transaction end publishes or frees opened slots) are
  rewritten. New rule IDs are appended; no existing ID is renumbered.
- **Docs.** DIVERGENCES (D-013 retired, D-026 added), COMPATIBILITY (handle
  errors) and the CHANGELOG.

## Open questions for human review

All resolved on 2026-10-17:
1. Option A (maintainer).
2. `EINVAL` for a reused slot (maintainer, the recommended answer).
3. `MDB_BAD_DBI` for a remove under an open read txn, as LMDB (maintainer,
   the recommended answer).
4. Nested read txns: observed and pinned. Dropping a child frees what it
   opened; a child's commit publishes it, for transactions begun after; and a
   child accepts its parent's handles.
