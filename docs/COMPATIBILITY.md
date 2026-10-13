# Compatibility: heed 0.22.1 and LMDB features on ZeroDB

The release contract. Every public item of heed 0.22.1 and every LMDB feature a
heed user can reach, with its status on the `heed-zerodb` adapter (what a
consumer sees as `heed::` through `crates/heed-shim`). Derived by reading both
sides' signatures, re-verified 2026-09-09; ZeroDB extensions and file facts
updated 2026-10-05; multi-process access (ADR-0023) 2026-10-13. Keep this file in the same change as
any adapter surface change (`docs/RELEASING.md`).

Statuses: **Same** identical signature and semantics · **Emulated** heed's
shape kept, implemented differently (noted) · **Extension** ZeroDB-only ·
**No-op** compiles, does nothing real · **Unsupported** absent or errors.

Behavior differences from LMDB, approved or proposed, are recorded in
[`DIVERGENCES.md`](DIVERGENCES.md); this file refers to them by topic.

## What a user must know about the files

| Fact | Where it is decided |
|---|---|
| The data file is **not an LMDB file**. Magic `ZDB1`, own page format, `format_version` 2 in each meta page (since 2026-10-05: the meta page carries the commit's freed-page list). Files with `format_version` 1, written by earlier trees, are refused at open; dump them with a format-1 `zerodb-tools` and `load` here. LMDB tools fail loudly on it (`MDB_INVALID`); use `zerodb-tools`. | DIVERGENCES.md (file format; free-list placement), ADR-0002, ADR-0022, SPEC 02 |
| Through the adapter an env directory contains **`data.mdb`** (Meilisearch hardcodes that name for compaction and snapshots) and, as with LMDB, **`lock.mdb`**: the lock file is on by default, so several processes can open the env at once (ADR-0023). heed's `NO_LOCK` creates no lock file and leaves the env single-process; a read-only env on read-only media also runs without one (ADR-0023 amendment 17). The native API defaults to `zerodb.dat` and `zerodb.lock`. `lock.mdb` is ZeroDB's own format, not LMDB's: never share an env directory between LMDB and ZeroDB processes. | ADR-0010; ADR-0023; DIVERGENCES.md (env data-file name) |
| The lock file's name is derived from the data-file name: `zerodb.dat` → `zerodb.lock`, `data.mdb` → `lock.mdb` (as LMDB), any other name `<name>-lock` (LMDB's `MDB_NOSUBDIR` convention); an explicit `lock_file_name` overrides it. The open refuses (`Io(InvalidInput)`, nothing modified) a lock file that is the data file under another name (hard link, case variant on a case-insensitive filesystem) and a non-empty lock file that is not a ZeroDB lock file (or the all-zero remains of one), such as another env's data file or a stale LMDB `lock.mdb`. | SPEC 02 §8, ADR-0023 amendment 16 |
| Migration from LMDB is logical: `zerodb-tools migrate-from-lmdb <src> <dst>` (build with `--features migrate-lmdb`) or `mdb_dump` → `zerodb-tools load`. Dumps are `mdb_dump`-shaped and round-trip. | docs/TOOLS.md |
| Page size is chosen at creation: power of two in 4 K–64 K, independent of the OS page size. Adapter default = OS page size (LMDB parity); native default 4 K. Reopen adopts the persisted value. | SPEC 02 §0 |
| **Max key 511 bytes**, empty key rejected (`BadValSize`), as LMDB. Max DB name 511 bytes, C-string (embedded NUL panics like heed). Max value 4 GiB − 1. | SPEC 01 §S4, SPEC 02, DIVERGENCES.md (named-DB catalog record) |
| A custom key comparator is **not persisted** (LMDB parity): reopening with a different comparator corrupts silently; an in-process mismatch is refused. | DIVERGENCES.md (comparator persistence), SPEC 03 §2.0 |
| `map_size` can change on an open env with `Env::resize` (heed's `unsafe fn resize`), as `mdb_env_set_mapsize` does: `0` adopts the size recorded by another process, a size below the data rounds up to it, and a live write txn gets `EINVAL`. A txn begin that finds another process committed past this process's map fails with `MdbError::MapResized`; answer with `resize(0)` and begin again, as with LMDB. Unlike LMDB, live read txns and their borrowed values stay valid across it, and in a lock-file env it waits for the cross-process writer lock (a proposed divergence). | SPEC 02 §8, ADR-0023 amendment 15, row below |
| Multi-process limits: do not use an `Env` in a child after `fork()`; processes in different PID namespaces must not share an env; `Env::sync` serializes only with this process's commits. | SPEC 04 §12.1 |
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
| `FileTrust` | Extension | Re-export of `zerodb::FileTrust`, the argument of `EnvOpenOptions::file_trust` |
| `DATA_FILE_NAME` | Extension | `"data.mdb"` (ADR-0010) |
| cargo features (`serde`, `serde-bincode`, `serde-json`, `serde-rmp`, `preserve_order`, …, `longer-keys`, `posix-sem`) | No-op | Declared on the shim so consumer manifests resolve; bincode/json codecs are always on; **`SerdeRmp` is never available**; `longer-keys` does not raise the 511 limit |

## `EnvOpenOptions`

| Item | Status | Notes |
|---|---|---|
| `new`, `Default`, `Clone`, `Debug`, `PartialEq`, `Eq` | Same | |
| `read_txn_with_tls` | No-op | Retag only; `WithTls` read txns are not thread-pinned |
| `read_txn_without_tls`, `map_size`, `max_readers`, `max_dbs`, `unsafe flags`, `unsafe open` | Same | `open` re-imposes the fork's checks, which the native engine relaxes: `map_size` must be an OS-page multiple, `max_readers(0)` is `Io(InvalidInput)`. A process joining a running env adopts the lock file's reader count whatever `max_readers` asks for, and a first opener never shrinks the count an earlier run's lock file records, as LMDB (ADR-0023 §1 as amended) |
| `page_size(u32)` | Extension | |
| `file_trust(FileTrust)` | Extension | Opt-in: `unsafe` `FileTrust::trust_contents()` skips read-path page validation, as LMDB never validates; default validating (ADR-0014; an approved divergence) |
| `sequential_writes(bool)` | Extension | Opt-in rightmost-leaf fast path for ascending/APPEND loads, default off; identical results and files (ADR-0015; an approved divergence) |
| `max_dirty_bytes(usize)` | Extension | Bound on a write txn's dirty memory before it spills pages to the file; default LMDB's 131,072 pages (ADR-0017; an approved divergence) |
| `serde` derive under the `serde` feature | Unsupported | |

### `EnvFlags`

| Flag | Status |
|---|---|
| `WRITE_MAP`, `NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC`, `READ_ONLY`, `PREV_SNAPSHOT`, `NO_TLS` | Same — under `WRITE_MAP` dirty pages are written in place in the map, as LMDB does (ADR-0021) |
| `NO_SUB_DIR` | Unsupported — **refused at `open`** with `Io(Unsupported)` (an approved divergence) |
| `NO_READ_AHEAD` | Honored as in LMDB: the map is advised `madvise(MADV_RANDOM)` (no readahead around page faults) |
| `NO_LOCK` | Same — no lock file is created or used, and the env runs single-process; the caller must keep other processes out, as with LMDB (ADR-0023 amendment 17) |
| `READ_ONLY` and the lock file | Same — joins an existing lock file; on read-only media, where none can be created, opens without one and serves read txns; any other lock-file error (such as `EACCES`) fails the open (ADR-0023 amendment 17) |
| `FIXED_MAP`, `NO_MEM_INIT` | No-op — accepted, no observable effect (no fixed mapping) |
| bitflags API: `empty`, `all`, `bits`, `from_bits`, `from_bits_truncate`, `contains`, `intersects`, `insert`, `remove`, `union`, bit operators | Same |
| bitflags API: `from_bits_retain`, `from_name`, `iter`, `iter_names`, `set`, `toggle`, `difference`, `symmetric_difference`, `complement`, `Extend`/`FromIterator`, hex/binary formatting, `serde` | Unsupported |
| `Debug` output | Emulated — prints the numeric value, not the flag names |

## `Env`

| Method | Status | Notes |
|---|---|---|
| `real_disk_size`, `try_clone_inner_file`, `info`, `stat`, `non_free_pages_size`, `database_options`, `open_database`, `create_database`, `write_txn`, `read_txn`, `static_read_txn`, `copy_to_file`, `copy_to_path`, `force_sync`, `path`, `max_readers`, `max_key_size`, `prepare_for_closing`, `Clone`, `Debug` | Same | `info().map_addr` is always null; page counts are ZeroDB's |
| `nested_read_txn(&RwTxn)` | Same | Available on every `T` (heed: `WithoutTls` only). The writer must not mutate while children live: enforced by the borrow (an approved divergence; the fork leaves it to convention) |
| `flags()` / `get_flags()` | Emulated | Reconstructed from the durability mode and the open: only `WRITE_MAP`, `READ_ONLY`, `NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC` and `NO_LOCK` are ever reported. `NO_LOCK` is reported only for an env opened with it, not for a read-only env on read-only media, as LMDB (`OBS_RDONLY_READONLY_FS_REPORTS_NOLOCK`) |
| `clear_stale_readers()` | Same | Frees the reader slots of processes that died holding them and returns how many slots it freed, as `mdb_reader_check` counts. Returns `Ok(0)` with `NO_LOCK`, where every slot belongs to this process |
| `set_flags(EnvFlags, FlagSetMode)` | Unsupported | Runtime durability toggling is deferred (SPEC 00) |
| `resize(usize)` | Same | `unsafe` as in heed; a size that is not a multiple of the OS page size is `Io(InvalidInput)` with heed's message, a live write txn is `Io` (`EINVAL`). The caller's promise heed asks for (no live txns) is not needed: read txns and borrowed values survive a resize. Answers `MdbError::MapResized` with `resize(0)`. With a lock file it waits for the cross-process writer lock, also on a `READ_ONLY` env, while another process holds a write txn (a proposed divergence; LMDB's behavior is unobserved, and the ADR's design reading of LMDB is that `set_mapsize` takes no cross-process lock) |
| `copy_to_fd` | Unsupported | Takes an LMDB FFI handle type |
| `nested_write_txn` | Unsupported | Nested **write** transactions do not exist (an approved divergence) |
| `sync(force)`, `live_readers()`, `reader_list()`, `copy_to_path_with_progress` | Extension | |
| `set_sequential_writes(&db, Option<bool>)`, `sequential_writes(&db)` | Extension | Per-database override of the env's sequential-writes default; runtime state, not persisted (ADR-0015) |
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
| `get_duplicates`, `delete_one_duplicate` | Unsupported | No DUPSORT support (an approved divergence) |
| `get_or_put`, `get_or_put_with_flags`, `get_or_put_reserved`, `get_or_put_reserved_with_flags` | Unsupported | Deferred; exact heed semantics must be read out before landing |
| `DatabaseStat` | Same | Page counts are ZeroDB's |
| `DatabaseOpenOptions`: `new`, `types`, `key_comparator`, `name`, `open`, `create`, `Copy`, `Clone`, `Debug` | Same | `key_comparator` reaches the engine (named DBs only, not persisted, as in LMDB) |
| `DatabaseOpenOptions::flags(non-empty)` | Unsupported | Any `DatabaseFlags` → `Mdb(Incompatible)` at open/create (an approved divergence) |
| `DatabaseOpenOptions::dup_sort_comparator` | No-op | Accepted, never used (no DUPSORT); LMDB also ignores `mdb_set_dupsort` on a non-DUPSORT DB |
| Stale handle after an aborted creating transaction | Emulated | LMDB returns `EINVAL`; ZeroDB handles are plain values and keep working (a proposed divergence) |

### `DatabaseFlags`

`REVERSE_KEY`, `DUP_SORT`, `INTEGER_KEY`, `DUP_FIXED`, `INTEGER_DUP`,
`REVERSE_DUP`: all **Unsupported** (`Mdb(Incompatible)` at open/create). Reverse
and integer key orders are available through `key_comparator` instead.

## Iterators

| Item | Status | Notes |
|---|---|---|
| `RoIter`, `RoRevIter`, `RwIter`, `RwRevIter` `<'txn, KC, DC, IM>` | Same | |
| `RoRange`, `RoRevRange`, `RoPrefix`, `RoRevPrefix`, `RwRange`, `RwRevRange`, `RwPrefix`, `RwRevPrefix` `<'txn, KC, DC, C, IM>` | Same | Same (the comparator parameter is included) |
| `next`, `Debug`, `remap_types`, `remap_key_type`, `remap_data_type`, `lazily_decode_data` | Same | |
| `move_between_keys`, `move_through_duplicate_values`, `iteration_method::*` | No-op | Retags only (no duplicates); `MoveOnCurrentKeyDuplicates` is unreachable |
| `unsafe del_current`, `unsafe put_current`, `unsafe put_current_with_options` | Same | `put_current` always returns `Ok(true)` |
| `unsafe put_current_reserved_with_flags` | Emulated | Closure runs before the put on a zeroed buffer; a failing closure leaves the old entry (a proposed divergence) |
| `IterationMethod::MOVE_OPERATION` | Unsupported | The trait has no associated const |
| Behaviour after exhaustion | Same as heed | Not fused: `del_current` after the last in-range entry acts on the cursor's position, as heed's does |

## `ReservedSpace`

`size`, `remaining`, `written_mut`, `fill_zeroes`, `as_uninit_mut`,
`unsafe assume_written`, `io::Write`, `io::Seek`, `Debug`: **Same** (bytes are
always initialised; seek error text differs).

## LMDB features behind heed

| LMDB feature | Status | Documented |
|---|---|---|
| Nested **read** transactions inside a write transaction (fork feature) | Supported | SPEC 04 §5, ADR-0007, DIVERGENCES.md (writer quiescence) |
| Nested **write** transactions | Unsupported, unrepresentable | DIVERGENCES.md (nested write txns) |
| Multi-process access, lock file, `mdb_reader_check` | Supported, on by default; the lock file is ZeroDB's own format | ADR-0023, SPEC 04 §12 |
| `MDB_WRITEMAP`, `NOSYNC`, `NOMETASYNC`, `MAPASYNC`, `RDONLY`, `PREVSNAPSHOT`, `NOTLS` | Supported (`WRITE_MAP` in place, as LMDB) | SPEC 01, ADR-0021 |
| `MDB_NOSUBDIR` | Unsupported, refused at open | DIVERGENCES.md (`MDB_NOSUBDIR`) |
| `MDB_NORDAHEAD` | Honored (`MADV_RANDOM`) | SPEC 01 Table 1 |
| `MDB_NOLOCK` | Supported (no lock file, single process) | ADR-0023 amendment 17 |
| `MDB_FIXEDMAP`, `NOMEMINIT` | Accepted no-ops | this file |
| `DUPSORT`, `DUPFIXED`, `INTEGERKEY`, `INTEGERDUP`, `REVERSEKEY`, `REVERSEDUP`, dup cursor ops, `MDB_MULTIPLE` | Unsupported | DIVERGENCES.md (DUPSORT), ADR-0011 (parked) |
| `mdb_set_compare` | Emulated (safe trait, named DBs, not persisted) | SPEC 03 §2.0, DIVERGENCES.md (comparator persistence) |
| `mdb_set_dupsort` | No-op | this file |
| `mdb_env_set_flags` at runtime | Unsupported | this file |
| `mdb_env_set_mapsize` at runtime (`resize`) | Supported | SPEC 02 §8, ADR-0023 amendment 15 |
| `MDB_APPEND`, `MDB_NOOVERWRITE`, `MDB_CURRENT`, `MDB_RESERVE` | Supported | SPEC 01 §S1–S3 |
| `MDB_CP_COMPACT` | Supported | ADR-0009 |
| Max key 511, max DB name 511, empty key invalid | Supported | SPEC 01 §S4 |
| Page size selection (LMDB 1.0) | Extension | SPEC 02 §0 |
| Encryption at rest (LMDB 1.0) | Unsupported | not planned yet (a possible later improvement, with page checksums) |
| Error text (`mdb_strerror`) | Same | oracle-pinned |

## What Meilisearch and hannoy actually touch

Everything they use is **Same** or a supported LMDB feature above. Verified on
Meilisearch v1.53.1 and hannoy v0.1.7-nested-rtxns by compiling and running
their test suites on the shim with zero source changes (`docs/CONSUMER-GATE.md`).
Differences a code path *could* observe and their current standing: the stale
dbi handle and the cursor reserved put (both proposed divergences in
`DIVERGENCES.md`), `RoTxn: Sync` (a widening),
`put_reserved` zero-fill (a widening), and `IntegerComparator` on unequal-length
keys (a bug fix relative to heed). None is reached by either consumer.
