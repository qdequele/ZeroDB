# ADR-0040: Refuse unknown persisted flag bits at open

- Status: Proposed
- Related issue: #72 (the duplicate-key spike that found the gap)
- Date: 2026-11-02

## Context

### What the file can carry that open never reads

Three fields exist in the format only to hold features ZeroDB does not
implement yet (SPEC 02 §3, §3.1, §10):

| Field | Where | Size | Meant for |
|---|---|---|---|
| `env_flags` | meta page, offset 44 | u32 | persistent env-wide flags |
| `flags` | every database record, offset 42 | u16 | persistent database flags, as LMDB's `md_flags` (duplicate keys, integer keys, reverse order) |
| `leaf2_ksize` | every database record, offset 44 | u32 | the fixed item size of a fixed-size duplicate database |

A database record sits in three places: the meta's `free_db` and `main_db`,
and the 48-byte catalog value of every named database (SPEC 02 §6). Every
engine built so far writes all three fields as 0, and the SPEC calls them
reserved.

Nothing that opens a file reads them:

- **Env open** validates each meta slot (SPEC 02 §3.2 rules 1–5) and the
  selected slot's geometry (rule 6, `check_meta_shape` in
  `crates/zerodb-core/src/env/open.rs`): magic, format version, page size,
  txnid, CRC, file length, root range. Not `env_flags`, and not the flags or
  `leaf2_ksize` of `main_db` or `free_db`. The same `check_meta_shape` runs
  when a process picks up another process's commit (`read_snapshot_at`).
- **Named-database open and create** (`open_database` in
  `crates/zerodb-core/src/rotxn.rs`, the catalog lookup in
  `crates/zerodb-core/src/rwtxn/catalog.rs`) check that the name is a
  sub-database entry (`F_SUBDATA`) and decode its 48 bytes. The flags are
  never looked at. Neither does the per-transaction resolution of an open
  handle (`resolve_named_record`).
- **`zerodb-tools check`** is the only reader that objects: INV-21
  (`crates/zerodb-core/src/check.rs`) reports a nonzero `flags` or
  `leaf2_ksize` in `main_db`, `free_db` and every named record. It does not
  look at `env_flags` either.

And one writer actively erases: every commit writes `env_flags: 0`
(`crates/zerodb-core/src/rwtxn/commit.rs`), as do the compacting copy's
metas (`crates/zerodb/src/copy.rs`). The tree builder behind the compacting
copy writes every record with `flags: 0, leaf2_ksize: 0`
(`crates/zerodb-core/src/builder.rs`). A commit does keep the records'
`flags` and `leaf2_ksize`: it writes `main_db`, `free_db` and each named
record back from what it read.

### Why it matters now

A future feature persisted in any of these fields is invisible to an engine
built from today's main: that engine opens the file, reads it and writes to
it as if the feature did not exist.

- **Duplicate keys** would set a bit in the database record's `flags`, as
  LMDB's `MDB_DUPSORT`. An engine without them opens the database as a plain
  one, reads one value per key, and on `put` overwrites where it should add
  a duplicate.
- **A comparator fingerprint**, the spike's proposal for refusing a database
  opened with the wrong key order, would sit in the record's last bytes. An
  engine that ignores it writes keys in its own order into a tree sorted by
  another.
- **An env-wide flag** is erased by the first commit of any engine, including
  the one that set it.

Today the only way to stop an older engine is to bump `FORMAT_VERSION` for
each such feature. That refuses every older engine on every file, including
files that never use the feature.

The format has not been released since version 3 landed (ADR-0037). An
engine of format 1 or 2 already refuses a version-3 file. So if every
released format-3 engine refuses bits it does not know, every later feature
can claim its bit without a version bump. Once a lax format-3 engine is
released, that is no longer possible for this format.

### What LMDB does

Observed, not guessed:
`crates/zerodb-oracle/tests/lmdb_observations/lmdb_unknown_flag_bits_observation.rs`
(added with this ADR) patches bits LMDB does not define (`0x0100`, `0x4000`)
into a closed LMDB environment and reopens it, on the Meilisearch fork
through `lmdb-master-sys`. LMDB keeps its env flags in the free database's
`md_flags` (`mm_flags`), so the three places match ZeroDB's.

| Bit set in | `mdb_env_open` | `mdb_dbi_open` | read, write, commit | After the commit |
|---|---|---|---|---|
| env flags (`mm_flags`) | OK | OK | OK | bit kept |
| main database record | OK | OK, `mdb_dbi_flags` reports it | OK | bit kept |
| named database record | OK | OK, `mdb_dbi_flags` reports it | OK | bit kept |
| named record, while a handle to it is open | — | — | `MDB_INCOMPATIBLE` in the next read and write transaction | — |

So LMDB never validates persisted flag bits at open, and it keeps them
through every commit. Its one flag check compares a record with itself:
when a handle's record no longer carries the flags the handle was opened
with, LMDB refuses the handle with `MDB_INCOMPATIBLE` (`mdb_cursor_init` in
`mdb.c`, re-reading a stale handle's record). LMDB's only forward-compatibility
gate is its data version: `mm_version` differing from `MDB_DATA_VERSION`
fails open with `MDB_VERSION_MISMATCH`. ZeroDB maps its own format-version
mismatch to `MdbError::Invalid` (SPEC 02 §3.2 rule 2).

None of this is reachable through heed: heed passes only LMDB's defined
flags, ZeroDB's adapter refuses every `DatabaseFlags` value, and ZeroDB does
not open LMDB files. Only a file written by a later ZeroDB can carry a bit
this engine does not know.

## Options

### Option A — Keep open lax; bump the format version for each feature

No change now. Each persisted feature bumps `FORMAT_VERSION`.

- Every feature locks out every older engine on every file, even files that
  do not use it, and needs a migration story for files that do not.
- The commit that zeroes `env_flags` stays a trap for the first env-wide
  flag.

### Option B — Refuse unknown persisted bits at open; carry known bits on write

1. **The known sets.** Two masks in `zerodb-core`: `KNOWN_ENV_FLAGS: u32` and
   `KNOWN_DB_FLAGS: u16`, both 0 today. `leaf2_ksize` must be 0, since no
   implemented feature gives it a meaning. A feature that claims a bit adds
   it to the mask in the change that implements it.
2. **Env open.** A new SPEC 02 §3.2 rule 7 on the selected slot, in
   `check_meta_shape`: `env_flags` has no bit outside `KNOWN_ENV_FLAGS`, and
   `main_db` and `free_db` have no `flags` bit outside `KNOWN_DB_FLAGS` and a
   zero `leaf2_ksize`. Failure fails the open. Living in `check_meta_shape`,
   the rule also runs when a process reads another process's commit, so an
   engine sharing an environment with a newer one stops at its next
   transaction instead of erasing the newer one's flags. Like rule 6, it
   judges only the selected slot.
3. **Named-database open and create.** After the existing `F_SUBDATA` and
   length checks, `open_database` and `create_database` (on an existing
   name), in read and write transactions, refuse a record with an unknown
   `flags` bit or a nonzero `leaf2_ksize`. The check runs before a handle slot
   is taken. The rest of the environment stays usable: one database using a
   feature does not lock out the others.
4. **An already-open handle.** The per-transaction resolution of a handle's
   record applies the same check, and the operation fails. This is LMDB's
   answer in the same position (the last row above). It is reachable only
   when another process drops and recreates the database with a newer engine
   while the handle is open. `TxnRead::record_for` becomes fallible; the read
   transaction's memo still resolves once per transaction and handle.
5. **Writers carry what they read.** Commit takes `env_flags` from the base
   snapshot, as it does `map_size` (`Snapshot` gains the field). The
   compacting copy takes it from its snapshot, and the tree builder copies
   `flags` and `leaf2_ksize` from the source record. Because open refuses
   every unknown bit, carrying never copies a bit this engine does not
   understand. LMDB carries too (the "bit kept" column).
6. **`check`.** INV-21 names `env_flags` too, and says "only bits this engine
   implements" rather than "zero", so it stays true when a feature claims a
   bit.

Every file any engine has written so far opens: they all hold zeros in
these fields.

### Option C — Option B, plus compatibility classes for `env_flags`

As B for the database records, whose bits mirror LMDB's `md_flags` and each
change how a tree is read. For `env_flags`, split the 32 bits into classes
fixed now, as SQLite does with its read and write version bytes (header
offsets 18 and 19) and ext4 with its `incompat`, `ro_compat` and `compat`
feature sets:

- **required** (for example the low 16 bits): unknown → refuse the open;
- **read-compatible** (8 bits): unknown → a `READ_ONLY` open succeeds, a
  write transaction is refused;
- **optional** (8 bits): unknown → ignored, carried by every commit.

This can only be decided now. Once engines that refuse every unknown bit are
released, they refuse a bit in any class, so a class added later binds only
newer engines. The cost is two more masks, one test per class, and a write
transaction check for the read-compatible class. No feature in view needs
the weaker classes yet.

## Error

`MdbError::Incompatible`, for the env and the record checks alike:

- It is LMDB's code for a record whose flags are not what the opener holds
  (pinned above) and LMDB's message for it says "or DB flags changed".
- It stays distinct from `Invalid`, which means a damaged or foreign file:
  a consumer can tell "needs a newer engine" from corruption.
- The heed adapter maps it already (`heed::MdbError::Incompatible`), so no
  API changes.

The alternative for the env check is LMDB's `MDB_VERSION_MISMATCH`: the code
LMDB gives a file of another data version, which is what an unknown env flag
means. ZeroDB has no such variant today; adding one is an API change, and
for consistency the format-version mismatch (`Invalid` today) would move to
it too. Open question 1.

## Cost

A mask and a compare per field, never on a read or write path:

- at env open and when a process reads another process's commit, next to a
  CRC32C over the meta and an `fstat`;
- per `open_database` or `create_database`, next to a catalog descent;
- once per transaction and handle, when the handle's record is first
  resolved, next to the same descent.

`get`, `put`, cursors and commit do no new work, except the commit's copy of
one `u32` from the snapshot. The implementation shows the standard
three-column comparison (`just bench-ab`) to confirm that; no gain is
claimed. No format change: the rule accepts every file written so far.

## Scorecard

Against today's engine; "LMDB" says where LMDB stands on the same line.

| | A (lax, bump per feature) | B (refuse unknown bits) | C (B plus classes) |
|---|---|---|---|
| Faster | no change | no change (a compare per open) | as B |
| Reliable | an older engine silently misreads a newer file | an older engine refuses it, per database where possible | as B; an optional bit is ignored by design |
| Durable | an env flag is erased by the next commit | flags carried, as LMDB | as B |
| Secure | no change | a hostile record's flags are refused, not used | as B |
| Format | a version bump per feature | none; later features need no bump | none |
| LMDB parity | LMDB's behavior (accept, keep) without its carry for env flags | stricter than LMDB at open; LMDB's carry, LMDB's error | as B |

## Decision

Recommended: **Option B**, implemented before the next release, with
`Incompatible` for both checks. It is the smallest change that lets duplicate
keys, a comparator fingerprint or any env-wide flag land without a format
bump, and it fixes the commit that erases `env_flags` before anything sets
it. Option C is cheap and only possible now, but no planned feature needs an
optional or read-compatible bit; open question 2 asks whether to fix the
classes anyway.

Awaiting the maintainer's decision. No code changes until it is approved.

## Consequences

- **SPEC 02:** §3 (`env_flags` row: "only bits this engine implements"),
  §3.1 (`flags`, `leaf2_ksize` rows), §3.2 (new rule 7), §6 (the open and
  create check), §10 (a format hook is refused at open until a feature claims
  it), §11.
- **SPEC 01 §S8:** a ZeroDB step after step 5: a record with an unknown flag
  bit or a nonzero `leaf2_ksize` → `MDB_INCOMPATIBLE`.
- **SPEC 04 TXN-10 step 3:** resolving a handle's record can fail with
  `Incompatible`.
- **SPEC 06 REC-1a:** references rule 7 alongside rule 6.
- **SPEC 03 §11:** INV-21 amended as in Option B item 6 (the ID is kept). IDs
  **INV-30 to INV-39 are reserved for duplicate-key databases.** INV-22 to
  INV-29 belong to the free-page database (SPEC 05 §9), so the six
  invariants ADR-0011 planned as INV-22 to INV-27 take this block if that
  work resumes.
- **`docs/LMDB-COMPATIBILITY.md`:** a new behavior difference: LMDB opens and
  keeps unknown persisted flag bits, ZeroDB refuses them with
  `Incompatible`. Unreachable through heed (see Context).
- **Tests:** in `zerodb-core`, one test per field and place (`env_flags`;
  `flags` and `leaf2_ksize` of `main_db`, `free_db` and a named record; a
  named record changed under an open handle by a second process), each with
  the meta CRC recomputed so the rule under test is the one that fires. A
  carry test with a test-only known bit, through commit and the compacting
  copy. The decoder fuzz target learns rule 7. The LMDB observation test
  lands with this ADR.
- **Tools:** `stat`, `dump`, `load`, copy and `migrate-from-lmdb` open through
  the engine and refuse such a file with the same error. `check` keeps
  reporting INV-21 and names the unknown bits.

## Open questions for human review

1. The env-level error: `Incompatible` (recommended) or a new
   `VersionMismatch`, LMDB's code for a file of another data version, which
   would then also replace `Invalid` for a format-version mismatch?
2. Fix compatibility classes for `env_flags` now (Option C), while it is
   still possible, or keep every bit required (Option B)?
3. `leaf2_ksize`: refuse any nonzero value as one field (recommended), or
   treat it now as two 16-bit halves (a fixed item size at offset 44 and a
   fingerprint at 46, as the spike laid out) so each half can be claimed
   separately? Both refuse every file today's engine would misread; the
   choice only shapes how a later feature claims the bytes.
