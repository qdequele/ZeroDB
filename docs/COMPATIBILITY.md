# Compatibility: heed 0.22.1 and LMDB features on ZeroDB

The release contract. Every public item of heed 0.22.1 and every LMDB feature a
heed user can reach, with its status on the `heed-zerodb` adapter (what a
consumer sees as `heed::` through `crates/heed-shim`). Derived by reading both
sides' signatures, re-verified 2026-09-09. Keep this file in the same change as
any adapter surface change (`docs/RELEASING.md`).

Statuses: **Same** identical signature and semantics · **Emulated** heed's
shape kept, implemented differently (noted) · **Extension** ZeroDB-only ·
**No-op** compiles, does nothing real · **Unsupported** absent or errors.

## What a user must know about the files

| Fact | Where it is decided |
|---|---|
| The data file is **not an LMDB file**. Magic `ZDB1`, own page format, `format_version` 1 in each meta page. LMDB tools fail loudly on it (`MDB_INVALID`); use `zerodb-tools`. | D-002, ADR-0002, SPEC 02 |
| Through the adapter an env directory contains exactly one file, **`data.mdb`** (Meilisearch hardcodes that name for compaction and snapshots); the native API defaults to `zerodb.dat`. **No `lock.mdb` is ever created.** | ADR-0010, D-012, D-001 |
| Migration from LMDB is logical: `zerodb-tools migrate-from-lmdb <src> <dst>` (build with `--features migrate-lmdb`) or `mdb_dump` → `zerodb-tools load`. Dumps are `mdb_dump`-shaped and round-trip. | docs/TOOLS.md |
| Page size is chosen at creation: power of two in 4 K–64 K, independent of the OS page size. Adapter default = OS page size (LMDB parity); native default 4 K. Reopen adopts the persisted value. | SPEC 02 §0, M2.6 |
| **Max key 511 bytes**, empty key rejected (`BadValSize`), as LMDB. Max DB name 511 bytes, C-string (embedded NUL panics like heed). Max value 4 GiB − 1. | SPEC 01 §S4, SPEC 02, D-008 |
| A custom key comparator is **not persisted** (LMDB parity): reopening with a different comparator corrupts silently; an in-process mismatch is refused. | D-014, SPEC 03 §2.0 |
| `map_size` is fixed for the life of an open env; grow it by reopening. There is no `Env::resize`. | SPEC 02 §8, row below |
| Files are created mode `0600`; the env directory must exist. | SECURITY.md |

## Crate root

| heed item | Status | Notes |
|---|---|---|
| `pub use byteorder`, `pub use heed_types as types`, `BoxedError`, `BytesEncode`, `BytesDecode`, `Comparator`, `LexicographicComparator` | Same | The very same trait and codec types (verbatim re-exports of heed-traits 0.20.0 / heed-types 0.21.0), so codec identity and coherence hold |
| `Error` (5 variants, `Display`, `From`), `Result`, `Unspecified` | Same | |
| `MdbError` (21 variants + `Other(i32)`, `not_found`) | Same | `Display` prints LMDB's `mdb_strerror` text, pinned against heed for every variant |
| `MdbError::from_err_code` / `to_err_code` | Unsupported | No LMDB error codes exist here |
| `lmdb_version()`, `LmdbVersion` | Unsupported | |
| `mod cookbook` | Unsupported | Documentation module only |
| `EnvStat` | Extension | heed defines it but does not export it |
| `DATA_FILE_NAME` | Extension | `"data.mdb"` (ADR-0010) |
| cargo features (`serde`, `serde-bincode`, `serde-json`, `serde-rmp`, `preserve_order`, …, `longer-keys`, `posix-sem`) | No-op | Declared on the shim so consumer manifests resolve; bincode/json codecs are always on; **`SerdeRmp` is never available**; `longer-keys` does not raise the 511 limit |

## `EnvOpenOptions`

| Item | Status | Notes |
|---|---|---|
| `new`, `Default`, `Clone`, `Debug`, `PartialEq`, `Eq` | Same | |
| `read_txn_with_tls` | No-op | Retag only; `WithTls` read txns are not thread-pinned |
| `read_txn_without_tls`, `map_size`, `max_readers`, `max_dbs`, `unsafe flags`, `unsafe open` | Same | `open` re-imposes the fork's checks: `map_size` must be an OS-page multiple (D-006), `max_readers(0)` is `Io(InvalidInput)` (D-010) |
| `page_size(u32)` | Extension | |
| `serde` derive under the `serde` feature | Unsupported | |

### `EnvFlags`

| Flag | Status |
|---|---|
| `WRITE_MAP`, `NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC`, `READ_ONLY`, `PREV_SNAPSHOT`, `NO_TLS` | Same |
| `NO_SUB_DIR` | Unsupported — **refused at `open`** with `Io(Unsupported)` (D-016) |
| `FIXED_MAP`, `NO_LOCK`, `NO_READ_AHEAD`, `NO_MEM_INIT` | No-op — accepted, no observable effect (no lock file, no fixed mapping) |
| bitflags API: `empty`, `all`, `bits`, `from_bits`, `from_bits_truncate`, `contains`, `intersects`, `insert`, `remove`, `union`, bit operators | Same |
| bitflags API: `from_bits_retain`, `from_name`, `iter`, `iter_names`, `set`, `toggle`, `difference`, `symmetric_difference`, `complement`, `Extend`/`FromIterator`, hex/binary formatting, `serde` | Unsupported |
| `Debug` output | Emulated — prints the numeric value, not the flag names |

## `Env`

| Method | Status | Notes |
|---|---|---|
| `real_disk_size`, `try_clone_inner_file`, `info`, `stat`, `non_free_pages_size`, `database_options`, `open_database`, `create_database`, `write_txn`, `read_txn`, `static_read_txn`, `copy_to_file`, `copy_to_path`, `force_sync`, `path`, `max_readers`, `max_key_size`, `prepare_for_closing`, `Clone`, `Debug` | Same | `info().map_addr` is always null; page counts are ZeroDB's |
| `nested_read_txn(&RwTxn)` | Same | Available on every `T` (heed: `WithoutTls` only). The writer must not mutate while children live: enforced by the borrow (D-005) |
| `flags()` / `get_flags()` | Emulated | Reconstructed from the durability mode: only the six supported bits are ever reported |
| `clear_stale_readers()` | No-op | Returns `Ok(0)`, which is correct: no cross-process readers can exist (D-001) |
| `set_flags(EnvFlags, FlagSetMode)` | Unsupported | Runtime durability toggling is deferred (SPEC 00) |
| `resize(usize)` | Unsupported | Not present; reopen with a larger `map_size` after `MapFull` |
| `copy_to_fd` | Unsupported | Takes an LMDB FFI handle type |
| `nested_write_txn` | Unsupported | Nested **write** transactions do not exist (D-003) |
| `sync(force)`, `live_readers()`, `reader_list()`, `copy_to_path_with_progress` | Extension | |
| `EnvInfo`, `EnvClosingEvent` (`wait`, `wait_timeout`), `CompactionOption`, `FlagSetMode`, `DefaultComparator` | Same | `EnvClosingEvent` is additionally `Clone` |
| `env_closing_event(path)` | No-op | Always `None` |
| `IntegerComparator` | Emulated | Compares common prefix then length; heed's version indexes past the shorter key on unequal lengths |

## Transactions

| Item | Status | Notes |
|---|---|---|
| `RoTxn<'e, T = AnyTls>`: `id`, `commit`, `Deref` chain, `Drop`, `Send` for `WithoutTls` | Same | `commit` is a plain release. `RoTxn` is additionally `Sync` |
| `WithTls`, `WithoutTls`, `AnyTls`, `TlsUsage` | Same | `WithTls` has no real TLS binding |
| `RwTxn<'p>`: `nested_read_txn`, `commit`, `abort`, `Deref` to `RoTxn<WithoutTls>`, `Send` | Same | |
| Environment/transaction pairing | Same | Every `Database` operation and `open`/`create` panics with heed's message when the transaction belongs to another environment |

## `Database` and `DatabaseOpenOptions`

| Item | Status | Notes |
|---|---|---|
| `get`, `get_lower_than`, `get_lower_than_or_equal_to`, `get_greater_than`, `get_greater_than_or_equal_to`, `first`, `last`, `len`, `is_empty`, `stat`, `iter`, `rev_iter`, `iter_mut`, `rev_iter_mut`, `range`, `rev_range`, `range_mut`, `rev_range_mut`, `prefix_iter`, `rev_prefix_iter`, `prefix_iter_mut`, `rev_prefix_iter_mut`, `put`, `put_with_flags`, `delete`, `delete_range`, `clear`, `unsafe remove`, `remap_*`, `lazily_decode_data`, `Copy`, `Debug` | Same | Empty key → `BadValSize` on `get`/`delete`/neighbour seeks/forward prefix (LMDB parity). `delete_range` never returns `Decoding` (it does not decode keys) |
| `put_reserved` | Emulated | Unwritten tail is zero-filled (LMDB leaves page garbage); a failing closure leaves the old entry |
| `put_with_flags(APPEND \| NO_OVERWRITE)` | Same | |
| `put_with_flags(NO_DUP_DATA \| APPEND_DUP)` on a plain DB | Same | The fork accepts them on a non-DUPSORT database; pinned by the oracle |
| `get_duplicates`, `delete_one_duplicate` | Unsupported | DUPSORT (D-004) |
| `get_or_put`, `get_or_put_with_flags`, `get_or_put_reserved`, `get_or_put_reserved_with_flags` | Unsupported | Deferred; exact heed semantics must be read out before landing |
| `DatabaseStat` | Same | Page counts are ZeroDB's |
| `DatabaseOpenOptions`: `new`, `types`, `key_comparator`, `name`, `open`, `create`, `Copy`, `Clone`, `Debug` | Same | `key_comparator` reaches the engine (named DBs only, not persisted, D-014) |
| `DatabaseOpenOptions::flags(non-empty)` | Unsupported | Any `DatabaseFlags` → `Mdb(Incompatible)` at open/create (D-004) |
| `DatabaseOpenOptions::dup_sort_comparator` | No-op | Accepted, never used (no DUPSORT); LMDB also ignores `mdb_set_dupsort` on a non-DUPSORT DB |
| Stale handle after an aborted creating transaction | Emulated | LMDB returns `EINVAL`; ZeroDB handles are plain values and keep working (D-013, proposed) |

### `DatabaseFlags`

`REVERSE_KEY`, `DUP_SORT`, `INTEGER_KEY`, `DUP_FIXED`, `INTEGER_DUP`,
`REVERSE_DUP`: all **Unsupported** (`Mdb(Incompatible)` at open/create). Reverse
and integer key orders are available through `key_comparator` instead.

## Iterators

| Item | Status | Notes |
|---|---|---|
| `RoIter`, `RoRevIter`, `RwIter`, `RwRevIter` `<'txn, KC, DC, IM>` | Same | |
| `RoRange`, `RoRevRange`, `RoPrefix`, `RoRevPrefix`, `RwRange`, `RwRevRange`, `RwPrefix`, `RwRevPrefix` `<'txn, KC, DC, C, IM>` | Same | Since 0.1.0 (the comparator parameter was missing before) |
| `next`, `Debug`, `remap_types`, `remap_key_type`, `remap_data_type`, `lazily_decode_data` | Same | |
| `move_between_keys`, `move_through_duplicate_values`, `iteration_method::*` | No-op | Retags only (no duplicates); `MoveOnCurrentKeyDuplicates` is unreachable |
| `unsafe del_current`, `unsafe put_current`, `unsafe put_current_with_options` | Same | `put_current` always returns `Ok(true)` |
| `unsafe put_current_reserved_with_flags` | Emulated | Closure runs before the put on a zeroed buffer; a failing closure leaves the old entry (D-015, proposed) |
| `IterationMethod::MOVE_OPERATION` | Unsupported | The trait has no associated const |
| Behaviour after exhaustion | Same as heed | Not fused: `del_current` after the last in-range entry acts on the cursor's position, as heed's does |

## `ReservedSpace`

`size`, `remaining`, `written_mut`, `fill_zeroes`, `as_uninit_mut`,
`unsafe assume_written`, `io::Write`, `io::Seek`, `Debug`: **Same** (bytes are
always initialised; seek error text differs).

## LMDB features behind heed

| LMDB feature | Status | Documented |
|---|---|---|
| Nested **read** transactions inside a write transaction (fork feature) | Supported | SPEC 04 §5, ADR-0007, D-005 |
| Nested **write** transactions | Unsupported, unrepresentable | D-003 |
| Multi-process access, lock file, `mdb_reader_check` | Unsupported (single process) | D-001 |
| `MDB_WRITEMAP`, `NOSYNC`, `NOMETASYNC`, `MAPASYNC`, `RDONLY`, `PREVSNAPSHOT`, `NOTLS` | Supported | SPEC 01 |
| `MDB_NOSUBDIR` | Unsupported, refused at open | D-016 |
| `MDB_FIXEDMAP`, `NOLOCK`, `NORDAHEAD`, `NOMEMINIT` | Accepted no-ops | this file |
| `DUPSORT`, `DUPFIXED`, `INTEGERKEY`, `INTEGERDUP`, `REVERSEKEY`, `REVERSEDUP`, dup cursor ops, `MDB_MULTIPLE` | Unsupported | D-004, ADR-0011 (parked) |
| `mdb_set_compare` | Emulated (safe trait, named DBs, not persisted) | SPEC 03 §2.0, D-014 |
| `mdb_set_dupsort` | No-op | this file |
| `mdb_env_set_flags` at runtime, `mdb_env_set_mapsize` at runtime (`resize`) | Unsupported | this file |
| `MDB_APPEND`, `MDB_NOOVERWRITE`, `MDB_CURRENT`, `MDB_RESERVE` | Supported | SPEC 01 §S1–S3 |
| `MDB_CP_COMPACT` | Supported | ADR-0009 |
| Max key 511, max DB name 511, empty key invalid | Supported | SPEC 01 §S4 |
| Page size selection (LMDB 1.0) | Extension | M2.6 |
| Encryption at rest (LMDB 1.0) | Unsupported | PLAN §3.9 |
| Error text (`mdb_strerror`) | Same | oracle-pinned |

## What Meilisearch and hannoy actually touch

Everything they use is **Same** or a supported LMDB feature above. Verified on
Meilisearch v1.53.1 and hannoy v0.1.7-nested-rtxns by compiling and running
their test suites on the shim with zero source changes (`docs/CONSUMER-GATE.md`).
Differences a code path *could* observe and their current standing: the stale
dbi handle (D-013), the cursor reserved put (D-015), `RoTxn: Sync` (a widening),
`put_reserved` zero-fill (a widening), and `IntegerComparator` on unequal-length
keys (a bug fix relative to heed). None is reached by either consumer.
