# ADR-0024: Database handle lifetime (stale handles after abort and remove)

- Status: Draft
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

Observed through heed 0.22.1 on both engines,
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

**Recommended: Option A**, with one stricter-than-LMDB divergence, filed as
D-026: a handle whose slot was reused by another name returns `EINVAL`
instead of reading the other database. Reading another database through a
stale handle is the hazard this ADR exists to remove. Matching it would
make ZeroDB serve the wrong data, a worse leniency than the one #73 reports.
D-013 is then retired; its row is kept with the ID, as D-001's is.

Option B costs about as much as A once the engine fix it still needs is
counted, and it leaves two behaviors. Option C keeps most of the gap.

Awaiting maintainer approval; no implementation before it (AGENTS.md rule 5).

## Consequences

- **Engine.** `NamedRegistry` becomes the slot table described in Option A.
  `Database` gains a generation. Every named-database operation checks the
  handle. The write-back can no longer insert a catalog entry for a handle
  that was never valid. `MdbError::BadDbi` is added.
- **Adapter.** heed-zerodb carries the generation and maps the two errors to
  heed's (`Error::Io` errno 22, `MdbError::BadDbi`).
- **Tests.**
  - The observation file becomes differential: each case asserts that ZeroDB
    matches the LMDB pin, except the `a`/`b` case, which asserts the D-026
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

1. **Option A, B or C?**
2. **The `a`/`b` case:** `EINVAL` (recommended), or match LMDB and read `b`?
3. **The open-read-during-remove case:** match LMDB's `MDB_BAD_DBI`
   (recommended, so consumers see one error taxonomy), or keep serving the
   read txn's snapshot, the MVCC answer D-009 chose for a similar race?
4. **Nested read txns:** whether a database first opened inside a nested read
   txn is freed when that child ends is not yet observed. It will be pinned in
   the oracle before implementation; the default is to follow LMDB.
