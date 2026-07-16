# SPEC 01 — LMDB flag & semantics matrix (Phase 0.2 deliverable)

Status: **DONE** — 2026-07-15. **M1.10 landed addendum** — 2026-07-16 (the
env write-mode / durability / RDONLY flags implemented; see the *M1.10 landed
flag matrix* at the end of this file for each flag's landing test).

Source of truth: the **Meilisearch LMDB fork**, branch `mdb.master.nested-rtxns`,
as vendored by `lmdb-master-sys 0.2.6` inside **heed v0.22.1** (SPEC 00 pinned
SHAs). Headers and semantics were read directly from the fork's
`libraries/liblmdb/lmdb.h` and `mdb.c`, not from stock LMDB 0.9 nor from docs.rs.

- **lmdb fork commit read:** `cd767228d31285a73af44f7fb15267e95d1df86f`
  (`ITS#10395 LMDB: Allow multiple nested read txns from a write txn`,
  2026-04-07), the submodule the heed `v0.22.1` tag pins at
  `lmdb-master-sys/lmdb`.
- **Reported LMDB version** in this fork's `lmdb.h`: `0.9.70` (the fork keeps the
  0.9.x version string while carrying the nested-read-txn patch).
- **heed flag surface read from:** `heed/src/mdb/lmdb_flags.rs` @ heed v0.22.1
  (`86cd1f681953cd5f6870706f6139b851e975975e`).

This document classifies every env / database / write / copy flag and every
cursor-op constant the fork defines, against the frozen consumer contract in
`docs/SPEC/00-api-surface.md`. Classification legend:

- **MUST** — at least one SPEC 00 consumer uses it. Cites the SPEC 00 row and
  names the Phase 1 milestone that implements it.
- **SHOULD** — heed 0.22.1 exposes it but no consumer uses it. Phase 2 (cites a
  `2.x`), *unless* PLAN.md explicitly pulls its parity into Phase 1 (noted
  inline; see the durability-flag note in §S6 and the Mismatches section).
- **WON'T** — not implemented in Phase 1; one-line justification citing a
  divergence (D-001 cross-process/locking, D-003 nested write txns, D-004
  DUPSORT/DatabaseFlags → revived in Phase 2.8) or a written rationale.

Every **MUST**/**SHOULD** row carries a **differential-test slug** — the oracle
checklist for milestones 1.2 / 1.3 / 1.4 / 1.6 / 1.10 / 1.11 / 1.12.

Consistency with SPEC 00 was verified both directions: every SPEC 01 **MUST**
traces to a SPEC 00 MUST item, and every flag SPEC 00 marks MUST is **MUST**
here. Result: **no MUST mismatch** (see Mismatches section for two non-blocking
PLAN-vs-consumer scoping notes).

---

## Table 1 — Environment flags (`mdb_env` open flags)

`lmdb.h` group `mdb_env`, bit values verbatim from the fork. heed exposes these
as `EnvFlags` (`heed/src/mdb/lmdb_flags.rs`); the TLS choice is exposed instead
through `EnvOpenOptions::read_txn_with_tls()` / `read_txn_without_tls()`.

| Flag | Bit | heed API | Class | Milestone | Diff-test slug | Behavior & error/precedence notes (from `mdb.c`) |
|------|-----|----------|-------|-----------|----------------|--------------------------------------------------|
| `MDB_FIXEDMAP` | `0x01` | `EnvFlags::FIXED_MAP` | **WON'T** | — | — | "mmap at a fixed address (**experimental**)" per the header; upstream marks it experimental and it hard-codes a mmap address. No consumer. Rationale: experimental upstream, incompatible with our own-format mmap policy. |
| `MDB_NOSUBDIR` | `0x4000` | `EnvFlags::NO_SUB_DIR` | **WON'T** | — | — | Store the env as two files `name` / `name-lock` instead of a directory. SPEC 00 row 7: every consumer opens a **directory** env (`data.mdb`/`lock.mdb`), never `NO_SUB_DIR`. zerodb defines its own on-disk layout (D-002); no consumer need. |
| `MDB_NOSYNC` | `0x10000` | `EnvFlags::NO_SYNC` | **SHOULD** † | 1.10 | `durability_nosync_no_fsync` | Skip both data and meta fsync on commit; durability restored only by `mdb_env_sync`/next non-RDONLY commit. In `mdb_env_sync0` the fsync is elided when `NO_SYNC` set. No consumer passes it, but PLAN 1.10 scopes durability-flag parity into Phase 1 (†, §S6). Crash-tested in M1.11. |
| `MDB_RDONLY` | `0x20000` | `EnvFlags::READ_ONLY` | **SHOULD** | 1.10 | `env_rdonly_rejects_write` | *Env-level* read-only: `mdb_txn_begin` returns `EACCES` for a write txn (`env->me_flags & MDB_RDONLY & ~flags`); `mdb_env_sync0` returns `EACCES`. No consumer opens an RDONLY **env** (they all need `write_txn`). NB: the *txn-level* `MDB_RDONLY` set on every `read_txn()` is a separate, universal MUST covered by SPEC 00 rows 14/16 (M1.3/1.8), not this env bit. |
| `MDB_NOMETASYNC` | `0x40000` | `EnvFlags::NO_META_SYNC` | **SHOULD** † | 1.10 | `durability_nometasync` | fsync data pages but skip the meta-page fsync on commit; the meta is flushed on the next sync. `mfd = (flags & (NOSYNC\|NOMETASYNC)) ? me_fd : me_mfd` — routes the meta write to the non-syncing fd. No consumer; PLAN 1.10 (†, §S6). Crash-tested in M1.11. |
| `MDB_WRITEMAP` | `0x80000` | `EnvFlags::WRITE_MAP` | **MUST** | 1.10 | `env_writemap_put_get_parity`, `env_writemap_put_reserved` | SPEC 00 row 8 (Meilisearch `index_map`, gated on experimental writemap). Writes go through a **writable** mmap (`mmap(PROT_WRITE)`), not `malloc`+`pwrite`; dirty bytes live in the map. Interacts with `put_reserved` (returns a pointer into the map) and the SPEC 04 value-borrow contract. Commit uses `msync` instead of `pwrite`+`fdatasync`. Excludes `NO_MEM_INIT`/readahead paths. Must be spec'd as a second write mode (§S7). |
| `MDB_MAPASYNC` | `0x100000` | `EnvFlags::MAP_ASYNC` | **SHOULD** † | 1.10 | `durability_mapasync_writemap` | Only meaningful with `WRITE_MAP`: use `MS_ASYNC` instead of `MS_SYNC` for the commit `msync` (`flags = (MAPASYNC && !force) ? MS_ASYNC : MS_SYNC`). A crash can then lose/corrupt the last txns. No consumer; PLAN 1.10 (†, §S6). |
| `MDB_NOTLS` | `0x200000` | *(via `read_txn_without_tls()`; `EnvFlags::NO_TLS` **deprecated**)* | **MUST** | 1.2, 1.8 | `flag_notls_rotxn_is_send` | SPEC 00 rows 2/29: **every** production open is `WithoutTls`. Ties reader-table slots to the `MDB_txn` object instead of a thread-local, making `RoTxn: Send` (rayon/async fan-out). In the fork this is the branch taken in `mdb_txn_renew0` (`env->me_flags & MDB_NOTLS`). zerodb makes WithoutTls the default (and effectively only) mode; the WithTls path is a thin shim (SPEC 00 second table). The `EnvFlags::NO_TLS` constant is deprecated in heed 0.22 in favor of the `EnvOpenOptions` methods. |
| `MDB_NOLOCK` | `0x400000` | `EnvFlags::NO_LOCK` | **WON'T** | — | — | "caller manages their own locks" — the cross-process locking escape hatch. D-001: zerodb is single-process with no lock file, so the concept does not apply. No consumer. |
| `MDB_NORDAHEAD` | `0x800000` | `EnvFlags::NO_READ_AHEAD` | **SHOULD** | 2.7 / 3.7 | `env_nordahead_accepted_noop` | Turns off OS readahead (`madvise(MADV_RANDOM)`), no effect on Windows. No consumer passes it (hannoy instead issues its own `madvise(WILLNEED)`, SPEC 00 §B.9). zerodb may accept and treat as an access hint; real hinting is Phase 3.7. |
| `MDB_NOMEMINIT` | `0x1000000` | `EnvFlags::NO_MEM_INIT` | **SHOULD** | 2.7 | `env_nomeminit_accepted_noop` | Skip zero-filling `malloc`'d pages before writing them to the datafile (a data-leak/Valgrind trade-off). `clean_limit` in `mdb_page_dirty` keys off `(NOMEMINIT|WRITEMAP)`. No consumer; the header itself notes it "is not needed with `MDB_WRITEMAP`". zerodb controls its own dirty-page allocation, so this can be a documented no-op. |
| `MDB_PREVSNAPSHOT` | `0x2000000` | `EnvFlags::PREV_SNAPSHOT` | **MUST** | 1.2, 1.10 | `env_prevsnapshot_opens_older_meta` | SPEC 00 row 9 (milli `Index::rollback`). Opens the env on the **older** of the two meta pages. `mdb_env_pick_meta` XORs the newer-meta selection with this flag: `metas[(m0.txnid < m1.txnid) ^ (flags & PREVSNAPSHOT)]`. Open protocol subtleties in §S5 (requires exclusive access; auto-cleared on first commit). |

† **SHOULD but Phase-1-scheduled:** PLAN.md M1.10 explicitly implements
`NO_SYNC`/`NO_META_SYNC`/`MAP_ASYNC` durability parity even though no consumer
passes them. They are SHOULD by the "no consumer" rule but land in Phase 1 by
PLAN scope. See §S6 and Mismatches note 1.

---

## Table 2 — Database flags (`mdb_dbi_open` flags)

`lmdb.h` group `mdb_dbi_open`. heed exposes them as `DatabaseFlags` /
`AllDatabaseFlags`. **No consumer passes any `DatabaseFlags`** (SPEC 00 §B.1,
verified across all five consumers) → every persistent DB flag is **WON'T** in
Phase 1 under **D-004**, revived in **Phase 2.8**. `MDB_CREATE` is the sole
exception: it is not a persistent flag and is a MUST.

| Flag | Bit | heed API | Class | Milestone | Diff-test slug | Behavior & error/precedence notes (from `mdb.c`) |
|------|-----|----------|-------|-----------|----------------|--------------------------------------------------|
| `MDB_REVERSEKEY` | `0x02` | `DatabaseFlags::REVERSE_KEY` | **WON'T** (→2.8/2.4) | — | — | Keys compared with `mdb_cmp_memnr` (reverse memcmp). No consumer sets it; all use `DefaultComparator` unsigned lexicographic order (SPEC 00 row 53). D-004. |
| `MDB_DUPSORT` | `0x04` | `DatabaseFlags::DUP_SORT` | **WON'T** (→2.8) | — | — | Sorted duplicate values per key (sub-page/sub-tree encoding). Highest-defect-density area of LMDB. No consumer: multi-values are modeled as composite keys + roaring bitmaps (SPEC 00 §B.1). D-004. |
| `MDB_INTEGERKEY` | `0x08` | `DatabaseFlags::INTEGER_KEY` (deprecated) | **WON'T** (→2.4) | — | — | Native-byte-order fixed-size integer keys compared with `mdb_cmp_cint`; all keys must be the same size. No consumer: integers are stored **big-endian** so memcmp order = numeric order (SPEC 00 rows 52/53). heed itself deprecates this in favor of `IntegerComparator`. D-004. |
| `MDB_DUPFIXED` | `0x10` | `DatabaseFlags::DUP_FIXED` | **WON'T** (→2.8) | — | — | With `DUPSORT`, fixed-size dup items packed contiguously (enables `GET_MULTIPLE`/`MULTIPLE`). No consumer. D-004. |
| `MDB_INTEGERDUP` | `0x20` | `DatabaseFlags::INTEGER_DUP` (deprecated) | **WON'T** (→2.8) | — | — | With `DUPSORT`, dup values are integer-comparator ordered. No consumer. D-004. |
| `MDB_REVERSEDUP` | `0x40` | `DatabaseFlags::REVERSE_DUP` | **WON'T** (→2.8) | — | — | With `DUPSORT`, dup values compared reverse-memcmp. No consumer. D-004. |
| `MDB_CREATE` | `0x40000` | `.create()` / `DatabaseFlags` bit | **MUST** | 1.6 | `db_open_missing_no_create_notfound`, `db_create_in_txn_then_abort` | SPEC 00 rows 11/62 (`create_database`). Create the named DB if absent. In `mdb_dbi_open`: absent + no `CREATE` → `MDB_NOTFOUND`; `CREATE` inside an `MDB_RDONLY` txn → `EACCES`; name exists but is not a sub-DB node → `MDB_INCOMPATIBLE`; catalog full → `MDB_DBS_FULL`; opening a named DB while the **main** DB carries `DUPSORT`/`INTEGERKEY` → `MDB_INCOMPATIBLE` (with CREATE) or `MDB_NOTFOUND`. Reopening with a different persistent-flags set → `MDB_INCOMPATIBLE`. See §S8. |

Note: `mdb_dbi_open` rejects any bit outside `VALID_FLAGS`
(`REVERSEKEY|DUPSORT|INTEGERKEY|DUPFIXED|INTEGERDUP|REVERSEDUP|CREATE`) with
`EINVAL` — zerodb must reject unknown DB flags the same way even while the dup
flags are stubbed out.

---

## Table 3 — Write / put flags (`mdb_put` / `mdb_cursor_put`)

`lmdb.h` group `mdb_put`. heed's `PutFlags` exposes only
`NO_DUP_DATA`/`NO_OVERWRITE`/`APPEND`/`APPEND_DUP`; `CURRENT` and `RESERVE` are
driven by dedicated heed methods (`Cursor::put_current`, `Database::put_reserved`)
and never appear as a user-visible `PutFlags` bit; `MULTIPLE` is unexposed.

| Flag | Bit | heed API | Class | Milestone | Diff-test slug | Behavior & error/precedence notes (from `mdb_cursor_put`) |
|------|-----|----------|-------|-----------|----------------|-----------------------------------------------------------|
| `MDB_NOOVERWRITE` | `0x10` | `PutFlags::NO_OVERWRITE` | **SHOULD** ‡ | 1.10 | `flag_no_overwrite_returns_existing` | If the key already exists, **do not overwrite**: LMDB copies the existing value into the caller's `data` (`*data = d2`) and returns `MDB_KEYEXIST` → heed `MdbError::KeyExist`. The returned-existing-value contract is load-bearing (§S2). No consumer passes it, but PLAN 1.10 lists it in Phase-1 scope (‡, Mismatches note 2). |
| `MDB_NODUPDATA` | `0x20` | `PutFlags::NO_DUP_DATA` | **WON'T** (→2.8) | — | — | DUPSORT-only: skip if the key/value pair already exists; on `mdb_cursor_del` removes all dups. No consumer (no DUPSORT DB). D-004. |
| `MDB_CURRENT` | `0x40` | `Cursor::put_current` (internal) | **MUST** | 1.4 | `cursor_put_current_overwrite`, `cursor_put_current_uninit_einval` | SPEC 00 row 33 (milli/arroy/hannoy/cellulite `put_current`/`put_current_with_options`). Overwrite the value at the current cursor position. Requires the cursor be positioned (`C_INITIALIZED`) else `EINVAL`. The `_with_options` form re-encodes with a different data codec. `unsafe`: no live borrow into the entry may span the call (§S3). |
| `MDB_RESERVE` | `0x10000` | `Database::put_reserved` (internal) | **MUST** | 1.10 | `put_reserved_writes_into_map`, `put_reserved_overwrite_same_size` | SPEC 00 row 35 (milli word-prefix docids). Allocate space for the value and return a pointer to it (`data->mv_data = METADATA(page)` / `= olddata.mv_data`); the caller fills it in-place, avoiding a temp buffer. Not valid with DUPSORT. Lifetime + `WRITE_MAP` interaction in §S3/§S7. |
| `MDB_APPEND` | `0x20000` | `PutFlags::APPEND` | **MUST** | 1.10 | `flag_append_out_of_order`, `flag_append_ascending_ok`, `flag_append_equal_key_keyexist` | SPEC 00 rows 32/33 (arroy bulk item append; milli facet bulk via `put_current_with_options`). Fast bulk insert assuming ascending key order: LMDB positions at the **last** key and compares (`md_cmp(key, last)`). If `key > last` → insert at end (no page split). If `key <= last` (**including equal**) → `MDB_KEYEXIST` → heed `KeyExist` → arroy `InvalidItemAppend`. Only the last key is checked, not full order (§S1). |
| `MDB_APPENDDUP` | `0x40000` | `PutFlags::APPEND_DUP` | **WON'T** (→2.8) | — | — | DUPSORT append of a dup value in sorted order. No consumer (no DUPSORT DB). D-004. |
| `MDB_MULTIPLE` | `0x80000` | *(unexposed by heed)* | **WON'T** (→2.8) | — | — | DUPFIXED-only bulk-store of many fixed-size dup values in one call; `data[1].mv_size` carries the count. Returns `MDB_INCOMPATIBLE` if the DB is not DUPFIXED. Not exposed by heed; no consumer. D-004. |

‡ **SHOULD but Phase-1-scheduled:** PLAN.md M1.10 lists `NO_OVERWRITE` in
Phase-1 scope though no consumer uses it. SHOULD by rule, Phase 1 by PLAN. See
Mismatches note 2.

---

## Table 4 — Copy flags (`mdb_env_copy2`)

| Flag | Bit | heed API | Class | Milestone | Diff-test slug | Behavior notes |
|------|-----|----------|-------|-----------|----------------|----------------|
| `MDB_CP_COMPACT` | `0x01` | `CompactionOption::{Enabled,Disabled}` | **MUST** | 1.12 | `copy_compact_vs_raw` | SPEC 00 rows 17/59 (snapshots, s3, compact route, meilitool). `Enabled` → `mdb_env_copyfd1`: walk the B+tree (`mdb_env_cwalk`) and write pages **sequentially, omitting free pages** (compaction). `Disabled` → `mdb_env_copyfd0`: raw byte copy of the live map (includes free pages). Copy opens its **own** internal read txn (caller cannot supply one). Both variants are used. |

---

## Table 5 — Cursor operations (`MDB_cursor_op`)

`lmdb.h` enum `MDB_cursor_op`, in declaration order. heed drives these
internally from its cursor/iterator methods; the DUPSORT/DUPFIXED ops have no
heed user-facing method in the SPEC 00 surface.

| Op | heed navigation using it | Class | Milestone | Diff-test slug | Behavior & notes |
|----|--------------------------|-------|-----------|----------------|------------------|
| `MDB_FIRST` | `first`, `iter`, `is_empty` | **MUST** | 1.3 | `cursor_first` | Position at the first (min) key. SPEC 00 rows 41/43/40. |
| `MDB_FIRST_DUP` | — | **WON'T** (→2.8) | — | — | DUPSORT-only: first dup of current key. D-004. |
| `MDB_GET_BOTH` | — | **WON'T** (→2.8) | — | — | DUPSORT-only: exact key+data seek. D-004. |
| `MDB_GET_BOTH_RANGE` | — | **WON'T** (→2.8) | — | — | DUPSORT-only: key exact, data `>=`. D-004. |
| `MDB_GET_CURRENT` | `iter_mut`/`prefix_iter_mut` rewrite, `put_current`, `del_current` | **MUST** | 1.3, 1.4 | `cursor_get_current` | Return key/data at the current position without moving. Underlies in-place mutation passes. SPEC 00 rows 33/34/43. |
| `MDB_GET_MULTIPLE` | — | **WON'T** (→2.8) | — | — | DUPFIXED-only: a page of dup values. D-004. |
| `MDB_LAST` | `last`, `rev_iter` start | **MUST** | 1.3 | `cursor_last` | Position at the last (max) key. SPEC 00 row 42. Also the seek `MDB_APPEND` uses internally. |
| `MDB_LAST_DUP` | — | **WON'T** (→2.8) | — | — | DUPSORT-only: last dup of current key. D-004. |
| `MDB_NEXT` | `iter`, `range`, `prefix_iter` step | **MUST** | 1.3 | `cursor_next_iter` | Advance to the next key/value. SPEC 00 rows 43/44/45. |
| `MDB_NEXT_DUP` | — | **WON'T** (→2.8) | — | — | DUPSORT-only. D-004. |
| `MDB_NEXT_MULTIPLE` | — | **WON'T** (→2.8) | — | — | DUPFIXED-only. D-004. |
| `MDB_NEXT_NODUP` | — | **WON'T** (→2.8) | — | — | Skip to first item of the next key; only meaningful with DUPSORT (in a non-dup DB it degenerates to `NEXT`). No heed user in the SPEC 00 surface. D-004. |
| `MDB_PREV` | `rev_range`, `rev_iter`, `rev_prefix_iter` step | **MUST** | 1.3 | `cursor_prev_rev` | Step to the previous key/value. SPEC 00 rows 44/46. |
| `MDB_PREV_DUP` | — | **WON'T** (→2.8) | — | — | DUPSORT-only. D-004. |
| `MDB_PREV_NODUP` | — | **WON'T** (→2.8) | — | — | Previous key's last item; DUPSORT-oriented (degenerates to `PREV` without dups). D-004. |
| `MDB_SET` | `get`-by-cursor, `move_on_key` exact | **MUST** | 1.3 | `cursor_set_exact` | Position exactly at the specified key (no key/data returned). Exact-match miss → `MDB_NOTFOUND`. Backs `get`-style existence checks. |
| `MDB_SET_KEY` | *(heed exposes but SPEC 00 surface doesn't exercise)* | **SHOULD** | 2.7 | `cursor_set_key` | Like `SET` but returns key+data. Same machinery as `SET`; cheap to include, no consumer exercises it. |
| `MDB_SET_RANGE` | `range`, `prefix_iter`, `get_greater_than`, `get_lower_than_or_equal_to` | **MUST** | 1.3 | `cursor_set_range_gte` | Position at the first key **>=** the given key. The workhorse for ranges, prefix scans, and both facet neighbor seeks (`>` = SET_RANGE then skip-equal; `<=` = SET_RANGE then step back if `>`). SPEC 00 rows 44/45/47/48. |
| `MDB_PREV_MULTIPLE` | — | **WON'T** (→2.8) | — | — | DUPFIXED-only: previous page of dup values. D-004. |

---

## Semantics notes (subtleties found in `mdb.c` that Phase 1 implementers must match)

### §S1 — `MDB_APPEND`: last-key compare, not full-order validation

> Confirmed empirically via oracle self-test 2026-07-15 (`tests/flag_semantics.rs`: append tests — out-of-order and equal-to-last both `KeyExist`, empty-db append ok).

`_mdb_cursor_put` with `MDB_APPEND` does **not** perform a normal tree search.
It calls `mdb_cursor_last`, then `md_cmp(new_key, last_key)`:

- `new_key > last_key` → treated as `MDB_NOTFOUND` internally, cursor advanced,
  key inserted at the end **without splitting a full page** (the whole point).
- `new_key <= last_key` (**equal included**) → `MDB_KEYEXIST`.

Consequences to replicate exactly:
1. APPEND validates only against the **current last key**, not the full sorted
   order — but since every prior insert had to be strictly ascending too, the DB
   stays sorted. An APPEND into an empty DB always succeeds (root is
   `P_INVALID`, the `MDB_NO_ROOT` path).
2. The comparison uses the DB's comparator (`md_cmp`). zerodb only supports the
   default unsigned-lexicographic comparator in Phase 1 (SPEC 00 row 53), so
   "ascending" = memcmp-ascending. An APPEND that is byte-descending errors even
   if it would be numeric-ascending under some other ordering.
3. Equal-to-last is an error, not a silent overwrite — different from a plain
   `put`, which overwrites.

The heed → error path is `MDB_KEYEXIST` → `MdbError::KeyExist` → (arroy)
`Error::InvalidItemAppend`.

### §S2 — `MDB_NOOVERWRITE`: the returned-existing-value contract

> Confirmed empirically via oracle self-test 2026-07-15 (`tests/flag_semantics.rs`: NOOVERWRITE collision returns the existing value).

When the key exists, LMDB does **not** just return an error — it first writes the
existing value back into the caller's `MDB_val *data` (`*data = d2;`) and *then*
returns `MDB_KEYEXIST`. Callers may read the pre-existing value out of the same
buffer they passed in. Any zerodb `put_with_flags(NO_OVERWRITE)` equivalent must
surface the existing value alongside the `KeyExist` error (heed models this as
the data pointer being repointed at the existing item). A no-op that merely
returns `KeyExist` without exposing the existing bytes is a divergence.

### §S3 — `MDB_RESERVE` / `put_current` lifetime & aliasing rules

- `MDB_RESERVE` returns a pointer **into a dirty DB page** (`METADATA(omp)` for a
  fresh/overflow node, or `olddata.mv_data` when overwriting same-size). The
  caller must fill those bytes before any subsequent operation on the same txn
  that could move, split, spill, or free the page. After the write txn commits or
  aborts, the pointer is invalid. This is the SPEC 04 dirty-page value-borrow
  contract in its most explicit form and needs miri coverage on
  reserve-then-mutate sequences.
- `MDB_CURRENT` (`put_current`) requires the cursor be positioned
  (`C_INITIALIZED`); otherwise `EINVAL`. Because both `put_current` and
  `del_current` mutate the entry the cursor points at, **no live `&[u8]` borrow
  of the current key/value may span the call** — heed marks these `unsafe` for
  exactly this reason (SPEC 00 rows 33/34).
- Under `MDB_RESERVE`, LMDB deliberately skips the meminit memcpy of the
  reserved region (`if (!(flags & MDB_RESERVE)) { ...copy... }`) so the caller
  sees uninitialized-but-owned space; zerodb must not zero it either if it is to
  match writemap peek behavior.

### §S4 — key/value size bounds (`MDB_BAD_VALSIZE`)

`_mdb_cursor_put` enforces, before touching the tree:

- `key->mv_size - 1 >= ENV_MAXKEY(env)` → `MDB_BAD_VALSIZE`. Because the compare
  is on `mv_size - 1` (unsigned), a **zero-length key underflows** to `SIZE_MAX`
  and is rejected too — i.e. **empty keys are invalid**.
- `ENV_MAXKEY` in this fork build (`MDB_DEVEL == 0`, so `MDB_MAXKEYSIZE == 511`)
  is the **constant 511 bytes**, independent of page size —
  `mdb_env_get_maxkeysize` returns 511. (The `me_maxkey = me_nodemax - …`
  page-derived path is compiled out when `MDB_MAXKEYSIZE` is defined.)
- data size `> MAXDATASIZE` (`0xffffffff`, ~4 GiB) → `MDB_BAD_VALSIZE` for a
  non-DUPSORT DB. milli separately synthesizes `BadValSize` for empty or
  `> u16::MAX` keys (SPEC 00 row 57); zerodb must at minimum enforce the
  empty-key rejection and a documented max-key bound, and report the same
  `BadValSize`.

**Confirmed via oracle self-test 2026-07-15** (`zerodb-oracle`
`tests/key_bounds.rs`: `max_key_size_is_511_and_map_size_independent`,
`key_size_511_boundary_and_empty_key_rejection`), observed directly against the
fork through heed 0.22.1 on macOS aarch64:

| Probe | Observed result |
|-------|-----------------|
| `Env::max_key_size()` | **511**, identical for map_size 1 MiB and 64 MiB |
| key len **0** (empty) | `Err(heed::Error::Mdb(MdbError::BadValSize))` |
| key len 1 / 255 / 510 / **511** | `Ok(())` |
| key len **512** / 513 / 1024 / 65535 | `Err(heed::Error::Mdb(MdbError::BadValSize))` |

So the observable max key **is exactly 511 bytes** (511 accepted, 512 rejected),
the bound is **constant across map sizes**, and an **empty key is rejected** —
all with the **`BadValSize`** variant, matching the header analysis above.

Two caveats on scope:
- heed 0.22 exposes no page-size selector, and in this fork build
  `MDB_MAXKEYSIZE` is the compile-time constant 511, so page-size *variation*
  is not reachable through the frozen surface; map-size invariance (tested at
  two sizes) plus the constant `max_key_size()` is the strongest statement
  observable here. The `longer-keys` heed feature (which would raise the bound)
  is **not** enabled by Meilisearch and not by this oracle.
- The milli-vs-LMDB discrepancy is **real and remains**: milli assumes
  `NonZeroU16` (empty rejected, up to 65535 accepted) and synthesizes its own
  `BadValSize` for empty / `> u16::MAX` keys, but LMDB itself rejects everything
  `> 511`. For keys in `512..=65535` milli's type-level assumption says "valid"
  while LMDB returns `BadValSize`. Phase 1 zerodb **replicates LMDB exactly: max
  key = 511, empty rejected, `BadValSize` otherwise** — so zerodb does **not**
  diverge from the oracle here (no `docs/DIVERGENCES.md` entry needed). Whether
  to expose a larger key bound as an opt-in is a Phase 3 question, not a Phase 1
  parity gap.

### §S5 — `MDB_PREVSNAPSHOT` open protocol

> Confirmed empirically via oracle self-test 2026-07-15 (`tests/flag_semantics.rs`: PREV_SNAPSHOT opens older meta; flag auto-clears after first commit).

- Meta selection: `mdb_env_pick_meta` returns
  `metas[(metas[0]->mm_txnid < metas[1]->mm_txnid) ^ (flags & PREVSNAPSHOT)]`.
  Without the flag it picks the **higher-txnid** (newer) meta; with the flag it
  picks the **older** one — the previous committed snapshot.
- Exclusivity: opening with `PREVSNAPSHOT` requires **exclusive** access. In
  `mdb_env_open`, `if ((flags & MDB_PREVSNAPSHOT) && !excl) return EAGAIN;` (when
  not RDONLY/NOLOCK). Under D-001 (single-process, exclusive by construction)
  this is naturally satisfied, but zerodb must still refuse a prev-snapshot open
  if another handle in-process holds the env.
- Auto-clear: after the **first commit** on a prev-snapshot env, LMDB clears the
  flag (`env->me_flags ^= MDB_PREVSNAPSHOT`) and re-shares locks — subsequent
  meta picks revert to newest. This is exactly milli's `Index::rollback`: open on
  the older meta, commit once to make it the live root, then behave normally.
  zerodb's rollback path must replicate the "one committed txn rewrites history,
  then normal" behavior, not leave the env permanently pinned to the old meta.

### §S6 — durability lattice: `NO_SYNC` / `NO_META_SYNC` / `MAP_ASYNC` (× `WRITE_MAP`)

Commit durability is a lattice over these flags. From `mdb_env_write_meta` and
`mdb_env_sync0`:

| Mode | Data pages | Meta page | Crash exposure |
|------|-----------|-----------|----------------|
| default (none) | fsync (`fdatasync`, or `msync(MS_SYNC)` under WRITEMAP) | fsync via `me_mfd` sync fd | none (last committed txn durable) |
| `NO_META_SYNC` | fsync | **not** synced this commit (routed to `me_fd`); flushed on next sync | on crash, meta may lag data; recovery falls back to older meta (still consistent) |
| `NO_SYNC` | **not** synced | not synced (routed to `me_fd`) | last N txns may be lost/torn; DB stays structurally consistent only if the FS preserves write order |
| `MAP_ASYNC` (+`WRITE_MAP`) | `msync(MS_ASYNC)` | `msync(MS_ASYNC)` | kernel flushes lazily; a crash can lose or corrupt recent txns |

Key ordering invariants for zerodb's writer to reproduce (even though the I/O
layer differs — pwrite/io_uring vs writemap+msync):
- The **meta write is the commit point**; it must not become durable before the
  data pages it references, or a crash yields a meta pointing at unwritten pages.
- `NO_META_SYNC` still writes the meta (just to a non-syncing fd) — the two meta
  slots must remain a valid double buffer so recovery can pick the older intact
  meta.
- `force` (an explicit `mdb_env_sync`) overrides `NO_SYNC` and downgrades
  `MAP_ASYNC` to a synchronous flush. `mdb_env_sync` on an `MDB_RDONLY` env →
  `EACCES`.
- **PLAN note:** these three flags are SHOULD (no consumer) yet PLAN M1.10
  implements their parity in Phase 1; the crash-consistency behavior above is
  validated by the M1.11 harness, not just unit tests. See Mismatches note 1.

### §S7 — `WRITE_MAP` interactions

`MDB_WRITEMAP` changes **where dirty bytes live** and therefore ripples into
several other rules:
- Writes mutate the mapped file directly (writable mmap); there is no separate
  `malloc`'d dirty page to `pwrite`. Commit flushes with `msync` (sync or async
  per `MAP_ASYNC`) plus, on macOS/Windows, an `fdatasync` of the data fd.
- `put_reserved` (§S3) returns a pointer straight into the writable map, so the
  reserved bytes are literally the on-disk bytes; the value-borrow contract
  (SPEC 04) must treat writemap and non-writemap dirty storage uniformly from the
  borrow-checker's perspective but is backed differently.
- `WRITE_MAP` disables the meminit path (`clean_limit` keys off
  `NOMEMINIT|WRITEMAP`), and a writemap env does **not** open the separate meta
  sync fd (`me_mfd`) at open (`if (!(flags & (RDONLY|WRITEMAP))) mdb_fopen(...)`).
- `NO_SYNC | WRITE_MAP` is called out in the header as leaving "no hint for when
  to write transactions to disk"; `MAP_ASYNC | WRITE_MAP` is the intended relaxed
  mode. zerodb should spec both the heap-buffer writer and the writemap writer as
  two explicit modes (PLAN M1.10) and prove `put_reserved` parity in each.

### §S8 — `mdb_dbi_open` error precedence (named-DB catalog)

Order of checks, each with its exact code, for zerodb's M1.6 catalog to match:
1. `flags & ~VALID_FLAGS` → `EINVAL`.
2. txn blocked/broken → `MDB_BAD_TXN`.
3. main DB (`name == NULL`): persistent flags OR'd into the main record, always
   succeeds with `MAIN_DBI`.
4. main DB carries `DUPSORT|INTEGERKEY` and a **named** open is attempted →
   `MDB_INCOMPATIBLE` (if `CREATE`) else `MDB_NOTFOUND` — cannot mix a
   dup/integer main DB with named sub-DBs.
5. name found but the catalog node is not a sub-DB (`!= F_SUBDATA`) →
   `MDB_INCOMPATIBLE`.
6. name absent: no `CREATE` → propagate `MDB_NOTFOUND`; `CREATE` in an RDONLY
   txn → `EACCES`.
7. no free slot and `numdbs >= maxdbs` → `MDB_DBS_FULL`.
8. reopening an existing named DB with a **different** persistent-flags set →
   `MDB_INCOMPATIBLE` (checked against `PERSISTENT_FLAGS`).

Since Phase 1 rejects all persistent DB flags (D-004), checks 4/5/8's
dup/integer branches are dormant, but the `CREATE`/`NOTFOUND`/`EACCES`/`DBS_FULL`/
non-subDB-`INCOMPATIBLE` paths are all live and must be reproduced.

### §S9 — nested read txn vs nested write txn (the fork's defining change)

`mdb_txn_begin(env, parent, flags)` in this fork (SHA `cd76722…`) distinguishes:
- **Nested READ txn** (`flags & MDB_RDONLY`, parent is a write txn): allowed,
  **arbitrarily many**. The child **shares the parent's `dirty_list` and
  `mt_free_pgs`** and takes `mt_txnid = parent->mt_txnid`, so it sees the write
  txn's **uncommitted** state. `parent->mt_rdonly_child_count++`. This is the
  hot-path MUST (SPEC 00 row 16, §A; M1.9) — reads borrow the writer's dirty
  pages, so the nested reader couples to the SPEC 04 value-borrow contract and
  the M1.8 concurrency model.
- **Nested WRITE txn** (parent is a write txn, child not RDONLY): the fork still
  supports it (only one, no writemap: a `WRITEMAP && !RDONLY` child → `EINVAL`;
  a blocked parent → `MDB_BAD_TXN`). **zerodb does NOT** — D-003: nested write
  txns return a clean "unsupported" error (zero consumer call sites). The 1.14
  heed-suite gate excludes nested-write tests.

This is not a flag per se but is the semantic that flips M1.9's scope; it is
recorded here because `MDB_RDONLY`'s meaning as a **txn-begin** flag (vs the
env-flag row in Table 1) is what selects the nested-read path.

---

## Mismatches found (SPEC 00 ↔ SPEC 01 MUST consistency)

**MUST-consistency: none.** Every SPEC 01 **MUST** flag traces to a SPEC 00 MUST
row (`WRITE_MAP`→r8, `NO_TLS`→r2/r29, `PREV_SNAPSHOT`→r9, `CREATE`→r11/r62,
`MDB_CURRENT`→r33, `MDB_RESERVE`→r35, `MDB_APPEND`→r32/r33, `CP_COMPACT`→r17/r59,
cursor `FIRST/LAST/NEXT/PREV/SET/SET_RANGE/GET_CURRENT`→r30/r40–r48), and every
flag SPEC 00 marks MUST is MUST here. No SPEC 00 edit is required or made.

Two **non-blocking PLAN-vs-consumer scoping notes** (for maintainer awareness;
they are *not* SPEC 00↔01 MUST mismatches, since SPEC 00 correctly omits these
from its MUST table):

1. **Durability flags `NO_SYNC` / `NO_META_SYNC` / `MAP_ASYNC`** are SHOULD by
   the "no consumer uses it" rule, yet **PLAN.md M1.10** explicitly implements
   their parity in Phase 1 ("Env durability flags parity: NOSYNC / NOMETASYNC /
   MAPASYNC semantics"). Classified SHOULD here with milestone M1.10 noted (†).
   SPEC 00 row 140 already anticipates this ("Durability flags map onto the
   ZeroDB writer in M1.10"), so the two specs agree; the only tension is with the
   generic "SHOULD → Phase 2" heuristic. No action needed unless a maintainer
   wants to promote them to a distinct "Phase-1 SHOULD" label.

2. **`MDB_NOOVERWRITE`** is likewise SHOULD (no consumer passes `PutFlags::
   NO_OVERWRITE` in any of the five repos) but **PLAN.md M1.10** lists it in
   Phase-1 scope ("APPEND …, NO_OVERWRITE, CURRENT, RESERVE"). Classified SHOULD
   with milestone M1.10 noted (‡). SPEC 00 does not list it as a MUST, so no
   SPEC 00 edit is warranted; flagged only so the M1.10 implementer knows the
   returned-existing-value contract (§S2) has no live consumer exercising it and
   the oracle test is the sole guard.

---

## Row counts

| Table | Rows | MUST | SHOULD | WON'T |
|-------|------|------|--------|-------|
| 1 — Environment flags | 12 | 3 | 6 | 3 |
| 2 — Database flags | 7 | 1 | 0 | 6 |
| 3 — Write / put flags | 7 | 3 | 1 | 3 |
| 4 — Copy flags | 1 | 1 | 0 | 0 |
| 5 — Cursor operations | 19 | 7 | 1 | 11 |
| **Total** | **46** | **15** | **8** | **23** |

- MUST diff-test slugs feed the oracle checklist for M1.2, M1.3, M1.4, M1.6,
  M1.10, M1.11, M1.12.
- All 23 WON'T rows are justified: **20 by D-004** (6 DUPSORT/DUPFIXED/integer/
  reverse DB flags + 3 dup put-flags `NODUPDATA`/`APPENDDUP`/`MULTIPLE` + 11 dup
  cursor-ops, all revived in Phase 2.8) and **3 by D-001/written rationale**
  (`FIXEDMAP` experimental upstream, `NOSUBDIR` own-format, `NOLOCK` cross-process
  per D-001).

---

## M1.10 landed flag matrix (write flags and modes)

**Landed 2026-07-16** (PLAN §1.10). Every write-mode / durability / RDONLY flag
in Phase-1 scope, its landing test, and where it lives. The put-flag rows
(`APPEND` / `NO_OVERWRITE` / `CURRENT` / `RESERVE`) landed earlier in M1.4/M1.6;
they are re-listed here for a single flag-matrix view (PLAN §1.10 acceptance).

| Flag | Table | Behavior (landed) | Landing test | Kind |
|------|-------|-------------------|--------------|------|
| `MDB_WRITEMAP` | 1 | Writes go through a writable mmap (`zerodb-io::WriteMapBacking`); commit `msync` instead of `pwrite`+`fdatasync`. Realized as a commit-time write strategy — heap dirty frames during the txn, copied into the map at C2 (SPEC 04 §6.4, amended). | `env_writemap_put_get_parity`, `env_writemap_put_reserved` (oracle, both engines in WRITE_MAP); `writemap_put_get_reserved_persist`, `writemap_reopened_as_writemap_sees_data` (`crates/zerodb/tests/write_flags.rs`) | differential + e2e |
| `MDB_MAPASYNC` | 1 | With WRITE_MAP, C3/C5 use `msync(MS_ASYNC)`. | `durability_mapasync_writemap` (oracle); `map_async_makes_barriers_async` (barrier count); `mapasync_writemap_commit_and_force_sync` (e2e) | differential + control-flow |
| `MDB_NOSYNC` | 1 | Skip **both** C3 and C5 fsync (SPEC 06 REC-9). Restored by `force_sync`. | `durability_nosync_no_fsync` (oracle); `no_sync_skips_both_barriers` / `no_sync_dominates_no_meta_sync` (barrier count) | differential + control-flow |
| `MDB_NOMETASYNC` | 1 | fsync data (C3), skip meta fsync (C5); recovery falls back to newest durable meta, corruption-free (REC-10). | `durability_nometasync` (oracle); `no_meta_sync_skips_meta_barrier` (barrier count) | differential + control-flow |
| `MDB_RDONLY` | 1 | `write_txn` / `force_sync` → `EACCES` (`Io(PermissionDenied)`, matching the fork); reads fully functional; no store creation. | `env_rdonly_rejects_write` (oracle, direct two-engine); `read_only_env_rejects_write_txn_and_force_sync`, `read_only_open_of_missing_store_errors` (e2e) | differential + e2e |
| `MDB_NOTLS` | 1 | `RoTxn: Send` (default/only mode); reader-table slot tied to the txn object. | `flag_notls_rotxn_is_send` (compile assert + cross-thread move) | compile + e2e |
| `MDB_PREVSNAPSHOT` | 1 | Open on the older meta; auto-clears on first commit (M1.2 / SPEC 02 §3.2, TXN-65..67). | `env_prevsnapshot_opens_older_meta` (M1.2) | differential |
| `MDB_APPEND` | 3 | Last-key compare; `key <= last` → `KeyExist` (SPEC 01 §S1). | `flag_append_*` (M1.4 oracle) | differential |
| `MDB_NOOVERWRITE` | 3 | Return existing value + `KeyExist` (SPEC 01 §S2). | `flag_no_overwrite_returns_existing` (M1.4) | differential |
| `MDB_CURRENT` | 3 | `put_current` at cursor; `EINVAL` if uninitialized. | `cursor_put_current_*` (M1.4) | differential |
| `MDB_RESERVE` | 3 | `put_reserved`: caller fills the slot in place; not zeroed (SPEC 01 §S3). Under WRITE_MAP the slot is (logically) into the map. | `put_reserved_*` (M1.4), `env_writemap_put_reserved` (M1.10) | differential |

**`Env::force_sync`** (`mdb_env_sync` parity, SPEC 01 §S6): a synchronous
barrier that restores durability under `NO_SYNC`/`NO_META_SYNC`/`MAP_ASYNC`;
`EACCES` on a read-only env; poisons the env on an `msync`/`fsync` failure
(REC-13). Covered by the `durability_*` and `read_only_*` e2e tests.

**Meilisearch indexing flag-combo replay** (PLAN §1.10 acceptance): the exact
milli combo — `WithoutTls` + `map_size` + named DBs + `put`/`put_with_flags`
(APPEND, sorted) + `del` + `clear` + **nested reads mid-txn** — is replayed on
both engines and compared exhaustively by `milli_indexing_flag_combo_replay`
(oracle). `WRITE_MAP` (milli's experimental gate) is covered by the WRITE_MAP
rows above.

**Fuzz dimension:** `fuzz/fuzz_targets/diff_ops.rs` seeds an `EngineMode` from
the first input byte so ~25% of differential fuzz cases run **both** engines in
`WRITE_MAP` (a slice also with a relaxed durability flag), exercising the second
write mode continuously.
