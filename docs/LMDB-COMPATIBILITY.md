# LMDB and heed compatibility

This page lists every public item of heed 0.22.1, and every LMDB feature a heed
user can reach, with its status in `heed-zerodb`. That crate is what a consumer
sees as `heed::` through the `crates/heed-shim` patch target. The page also
records every observable behavior difference from LMDB (the `D-0xx` entries).
Update it in the same change as any adapter surface change.

The LMDB reference is the Meilisearch fork (`mdb.master.nested-rtxns`, vendored
in `lmdb-master-sys 0.2.6`, heed 0.22.1). Behavior is observed with differential
tests in `crates/zerodb-oracle`, not guessed. A behavior difference that is not
listed in [Behavior differences](#behavior-differences) is a bug.

Statuses:

- **Same:** identical signature and semantics.
- **Emulated:** heed's shape is kept but the implementation differs (noted).
- **Extension:** ZeroDB only.
- **No-op:** compiles but does nothing real.
- **Unsupported:** absent, or returns an error.

## Facts about the files

| Fact | Reference |
|---|---|
| The data file is **not an LMDB file**. It has the magic `ZDB1`, its own page format, and format version 2 in each meta page. Files with format version 1 (pre-release builds) are refused at open: dump them with a format-1 `zerodb-tools` and `load` them here. LMDB tools fail loudly on the file (`MDB_INVALID`); use `zerodb-tools` instead. | [D-002](#d-002), [D-022](#d-022), ADR-0002, ADR-0022, SPEC 02 |
| Through the adapter, an env directory holds **`data.mdb`**, because Meilisearch hardcodes that name. As with LMDB, it also holds **`lock.mdb`**, which is on by default so several processes can open the env at once. heed's `NO_LOCK` creates no lock file and keeps the env single-process; so does a read-only env on read-only media. The native API defaults to `zerodb.dat` and `zerodb.lock`. `lock.mdb` uses ZeroDB's own format, so never share an env directory between LMDB and ZeroDB processes. | [D-012](#d-012), ADR-0010, ADR-0023 |
| The lock file's name is derived from the data file's: `zerodb.dat` gives `zerodb.lock`, `data.mdb` gives `lock.mdb`, and any other name gives `<name>-lock`. `lock_file_name` overrides it. Open refuses, with `Io(InvalidInput)` and nothing modified, a lock file that is the data file under another name, and a non-empty file that is not a ZeroDB lock file. | SPEC 02 §8, ADR-0023 |
| Migration from LMDB is logical: `zerodb-tools migrate-from-lmdb <src> <dst>` (built with `--features migrate-lmdb`), or `mdb_dump` followed by `zerodb-tools load`. Dumps are `mdb_dump`-shaped and round-trip. | [TOOLS.md](TOOLS.md) |
| The page size is chosen at creation: a power of two from 4 KiB to 64 KiB, independent of the OS page size. The adapter defaults to the OS page size (as LMDB does) and the native API to 4 KiB. A reopen adopts the stored value. | SPEC 02 §0 |
| **Keys are at most 511 bytes** and an empty key is rejected with `BadValSize`, as in LMDB. A database name is at most 511 bytes, as a C string (an embedded NUL panics, as in heed). A value is at most 4 GiB − 1. | SPEC 01 §S4, [D-008](#d-008) |
| A custom key comparator is **not persisted**, as in LMDB: reopening with a different comparator silently corrupts lookups. An in-process mismatch is refused. | [D-014](#d-014), SPEC 03 §2.0 |
| `Env::resize` (heed's `unsafe fn resize`) changes `map_size` on an open env, as `mdb_env_set_mapsize` does. `0` adopts the size another process recorded, a size below the data rounds up to it, and a live write txn gets `EINVAL`. A txn begin that finds another process has committed past this process's map fails with `MdbError::MapResized`; call `resize(0)` and begin again. Unlike LMDB, live read txns and the values they borrowed stay valid across a resize. | [D-023](#d-023), SPEC 02 §8 |
| Multi-process limits: do not use an `Env` in a child after `fork()`, do not share an env across PID namespaces, and note that `Env::sync` only serializes with this process's commits. | SPEC 04 §12.1 |
| Files are created with mode `0600`, and the env directory must already exist. | [SECURITY.md](../SECURITY.md) |

## heed API

### Crate root

| heed item | Status | Notes |
|---|---|---|
| `pub use byteorder`, `pub use heed_types as types`, `BoxedError`, `BytesEncode`, `BytesDecode`, `Comparator`, `LexicographicComparator` | Same | Verbatim re-exports of heed-traits 0.20.0 and heed-types 0.21.0, so codecs are the very same types |
| `Error` (5 variants, `Display`, `From`), `Result`, `Unspecified` | Same | |
| `MdbError` (21 variants plus `Other(i32)`, `not_found`) | Same | `Display` prints LMDB's `mdb_strerror` text, checked against heed for every variant |
| `MdbError::from_err_code`, `to_err_code` | Unsupported | There are no LMDB error codes here |
| `lmdb_version()`, `LmdbVersion`, `mod cookbook` | Unsupported | |
| `EnvStat` | Extension | heed defines it but does not export it |
| `FileTrust` | Extension | Argument of `EnvOpenOptions::file_trust` |
| `DATA_FILE_NAME` | Extension | `"data.mdb"` |
| Cargo features (`serde`, `serde-bincode`, `serde-json`, `serde-rmp`, `preserve_order`, `longer-keys`, `posix-sem`, …) | No-op | Declared on the shim so consumer manifests resolve. The bincode and JSON codecs are always on, **`SerdeRmp` is never available**, and `longer-keys` does not raise the 511-byte limit |

### `EnvOpenOptions`

| Item | Status | Notes |
|---|---|---|
| `new`, `Default`, `Clone`, `Debug`, `PartialEq`, `Eq` | Same | |
| `read_txn_with_tls` | No-op | Retag only; `WithTls` read txns are not pinned to a thread |
| `read_txn_without_tls`, `map_size`, `max_readers`, `max_dbs`, `unsafe flags`, `unsafe open` | Same | `open` re-imposes the fork's checks that the native engine relaxes ([D-006](#d-006), [D-010](#d-010)). A process that joins a running env adopts the lock file's reader count, as in LMDB |
| `page_size(u32)` | Extension | |
| `file_trust(FileTrust)` | Extension | Opt-in: the `unsafe` `FileTrust::trust_contents()` skips read-path page validation, as LMDB never validates. The default validates ([D-019](#d-019)) |
| `sequential_writes(bool)` | Extension | Opt-in rightmost-leaf fast path for ascending or `APPEND` loads, off by default; results and files are identical ([D-020](#d-020)) |
| `max_dirty_bytes(usize)` | Extension | Bounds a write txn's dirty memory before it spills pages to the file; defaults to LMDB's 131,072 pages ([D-021](#d-021)) |
| `serde` derive under the `serde` feature | Unsupported | |

### `EnvFlags`

| Flag | Status |
|---|---|
| `WRITE_MAP`, `NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC`, `READ_ONLY`, `NO_TLS` | Same. Under `WRITE_MAP`, dirty pages are written in place in the map, as in LMDB (ADR-0021) |
| `PREV_SNAPSHOT` | Same, except two cases ([D-025](#d-025)). With the lock file, other processes' opens wait until the opener's first commit. That commit is durable but not published, and the session's second commit rebuilds from the older snapshot, as in LMDB. A writable open fails with `EAGAIN` while another process has the env open. With `NO_LOCK` the first commit is published at once |
| `NO_SUB_DIR` | Unsupported: **refused at open** with `Io(Unsupported)` ([D-016](#d-016)) |
| `NO_READ_AHEAD` | Same: the map is advised `MADV_RANDOM`, so there is no readahead around page faults |
| `NO_LOCK` | Same: no lock file is created or used, and the env is single-process; the caller must keep other processes out |
| `READ_ONLY` with a lock file | Same: joins an existing lock file. On read-only media, where none can be created, it opens without one; any other lock-file error (such as `EACCES`) fails the open |
| `FIXED_MAP`, `NO_MEM_INIT` | No-op: accepted, with no observable effect |
| bitflags `empty`, `all`, `bits`, `from_bits`, `from_bits_truncate`, `contains`, `intersects`, `insert`, `remove`, `union`, bit operators | Same |
| bitflags `from_bits_retain`, `from_name`, `iter`, `iter_names`, `set`, `toggle`, `difference`, `symmetric_difference`, `complement`, `Extend`/`FromIterator`, hex and binary formatting, `serde` | Unsupported |
| `Debug` output | Emulated: prints the numeric value, not the flag names |

### `Env`

| Method | Status | Notes |
|---|---|---|
| `real_disk_size`, `try_clone_inner_file`, `info`, `stat`, `non_free_pages_size`, `database_options`, `open_database`, `create_database`, `write_txn`, `read_txn`, `static_read_txn`, `copy_to_file`, `copy_to_path`, `force_sync`, `path`, `max_readers`, `max_key_size`, `prepare_for_closing`, `Clone`, `Debug` | Same | `info().map_addr` is always null, and page counts are ZeroDB's. `info().number_of_readers` is LMDB's high-water mark and, with a lock file, counts other processes' readers too ([D-011](#d-011)) |
| `nested_read_txn(&RwTxn)` | Same | Available for every `T` (heed: `WithoutTls` only). The writer cannot mutate while children are live: the borrow checker enforces it ([D-005](#d-005)) |
| `flags()`, `get_flags()` | Emulated | Rebuilt from the open options: only `WRITE_MAP`, `READ_ONLY`, `NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC` and `NO_LOCK` are ever reported. `NO_LOCK` is reported only for an env opened with it, as in LMDB |
| `clear_stale_readers()` | Same | Frees the reader slots of dead processes and returns how many, as `mdb_reader_check` counts. Returns `Ok(0)` with `NO_LOCK` |
| `set_flags(EnvFlags, FlagSetMode)` | Unsupported | Changing durability at runtime is not implemented |
| `resize(usize)` | Same | `unsafe`, as in heed. A size that is not a multiple of the OS page size is `Io(InvalidInput)` with heed's message, and a live write txn is `Io(EINVAL)`. heed's "no live txns" promise is not needed: read txns survive a resize. With a lock file it waits for the cross-process writer lock ([D-023](#d-023)) |
| `copy_to_fd` | Unsupported | Takes an LMDB FFI handle type |
| `nested_write_txn` | Unsupported | Nested write txns do not exist, so a call does not compile ([D-003](#d-003)) |
| `sync(force)`, `live_readers()`, `reader_list()`, `copy_to_path_with_progress` | Extension | |
| `set_sequential_writes(&db, Option<bool>)`, `sequential_writes(&db)` | Extension | Per-database override of the env's sequential-writes default; runtime state, not persisted |
| `EnvInfo`, `EnvClosingEvent` (`wait`, `wait_timeout`), `CompactionOption`, `FlagSetMode`, `DefaultComparator` | Same | `EnvClosingEvent` is also `Clone` |
| `env_closing_event(path)` | No-op | Always `None` |
| `IntegerComparator` | Emulated | Compares the common prefix, then the length; heed's version reads past the shorter key when lengths differ |

### Transactions

| Item | Status | Notes |
|---|---|---|
| `RoTxn<'e, T = AnyTls>`: `id`, `commit`, `Deref` chain, `Drop`, `Send` for `WithoutTls` | Same | `commit` is a plain release. `RoTxn` is also `Sync` |
| `WithTls`, `WithoutTls`, `AnyTls`, `TlsUsage` | Same | `WithTls` has no real thread-local binding |
| `RwTxn<'p>`: `nested_read_txn`, `commit`, `abort`, `Deref` to `RoTxn<WithoutTls>`, `Send` | Same | |
| Env and transaction pairing | Same | Every `Database` operation, and `open`/`create`, panics with heed's message when the transaction belongs to another env |

### Meilisearch's heed fork (`unique-txns` feature)

From v1.55, Meilisearch builds against its own heed fork,
`meilisearch/heed@mutexify-open-dbi`. The fork opens and creates databases
behind one mutex per environment. The `unique-txns` feature of `heed-zerodb`
switches to that fork's API, and `crates/heed-shim-unique-txns` is the patch
target with the feature on (ADR-0027). The feature changes signatures, so a
build has one API or the other, never both.

| Item | Status | Notes |
|---|---|---|
| `UniqueRwTxn<'e>` (`commit`, `abort`, `Deref`/`DerefMut` to `RwTxn`), `UniqueRoTxn<'e, T>` (`commit`, `Deref` to `RoTxn`), `AsUniqueTxnRef<'e>` | Same | Each holds the env's mutex until it ends, so neither is `Send`, as in the fork |
| `Env::unique_write_txn`, `unique_read_txn`, `try_unique_write_txn`, `try_unique_read_txn` | Same | The mutex is shared by every clone of the `Env`. The blocking forms clear a poisoned mutex; the `try_` forms return `Ok(None)` while it is held. `read_txn` and `write_txn` do not take it |
| `Env::open_database`, `DatabaseOpenOptions::open` taking `&U: AsUniqueTxnRef`; `Env::create_database`, `DatabaseOpenOptions::create` taking `&mut UniqueRwTxn` | Same | ZeroDB itself does not need the mutex (ADR-0024); it is kept for the fork's waiting behavior |
| `EnvStat::non_free_page_size`, `DatabaseStat::non_free_page_size` | Same | |
| `RoTxn::commit`, `Env::non_free_pages_size` | Absent | Removed by the fork |

### `Database` and `DatabaseOpenOptions`

| Item | Status | Notes |
|---|---|---|
| `get`, `get_lower_than`, `get_lower_than_or_equal_to`, `get_greater_than`, `get_greater_than_or_equal_to`, `first`, `last`, `len`, `is_empty`, `stat`, `iter`, `rev_iter`, `iter_mut`, `rev_iter_mut`, `range`, `rev_range`, `range_mut`, `rev_range_mut`, `prefix_iter`, `rev_prefix_iter`, `prefix_iter_mut`, `rev_prefix_iter_mut`, `put`, `put_with_flags`, `delete`, `delete_range`, `clear`, `unsafe remove`, `remap_*`, `lazily_decode_data`, `Copy`, `Debug` | Same | An empty key gives `BadValSize` on reads, as in LMDB. `delete_range` never returns `Decoding` |
| `put_reserved` | Same | A closure that writes less than it reserved returns `Io(UnexpectedEof)`, and a failing closure returns its `Io` error with the reserved entry in place, as in heed. Bytes the closure did not write are zero, where LMDB leaves page garbage ([D-015](#d-015)) |
| `put_with_flags(APPEND \| NO_OVERWRITE)`, `put_with_flags(NO_DUP_DATA \| APPEND_DUP)` on a plain database | Same | The fork accepts the dup flags on a non-`DUP_SORT` database |
| `get_duplicates`, `delete_one_duplicate` | Unsupported | No `DUP_SORT` ([D-004](#d-004)) |
| `get_or_put`, `get_or_put_with_flags`, `get_or_put_reserved`, `get_or_put_reserved_with_flags` | Unsupported | |
| `DatabaseStat` | Same | Page counts are ZeroDB's |
| `DatabaseOpenOptions`: `new`, `types`, `key_comparator`, `name`, `open`, `create`, `Copy`, `Clone`, `Debug` | Same | `key_comparator` reaches the engine (named databases only, not persisted, as in LMDB) |
| `DatabaseOpenOptions::flags` with any flag | Unsupported | `Mdb(Incompatible)` at open or create ([D-004](#d-004)) |
| `DatabaseOpenOptions::dup_sort_comparator` | No-op | LMDB also ignores `mdb_set_dupsort` on a non-`DUP_SORT` database |
| Database handle after an aborted, dropped or removing txn | Same, except two cases | `EINVAL` or `MDB_BAD_DBI` as in LMDB (ADR-0024, SPEC 04 §13); two cases are stricter ([D-026](#d-026)) |

`DatabaseFlags` (`REVERSE_KEY`, `DUP_SORT`, `INTEGER_KEY`, `DUP_FIXED`,
`INTEGER_DUP`, `REVERSE_DUP`) are all **Unsupported**. Reverse and integer key
orders are available through `key_comparator` instead.

### Iterators and `ReservedSpace`

| Item | Status | Notes |
|---|---|---|
| `RoIter`, `RoRevIter`, `RwIter`, `RwRevIter`, `RoRange`, `RoRevRange`, `RoPrefix`, `RoRevPrefix`, `RwRange`, `RwRevRange`, `RwPrefix`, `RwRevPrefix` | Same | Including the comparator type parameter |
| `next`, `Debug`, `remap_types`, `remap_key_type`, `remap_data_type`, `lazily_decode_data` | Same | |
| `move_between_keys`, `move_through_duplicate_values`, `iteration_method::*` | No-op | Retags only, as there are no duplicates |
| `unsafe del_current`, `unsafe put_current`, `unsafe put_current_with_options` | Same | `put_current` always returns `Ok(true)` |
| `unsafe put_current_reserved_with_flags` | Emulated | The closure fills a zeroed buffer before the put ([D-015](#d-015)) |
| `IterationMethod::MOVE_OPERATION` | Unsupported | The trait has no associated const |
| Behavior after exhaustion | Same | Not fused, as in heed |
| `ReservedSpace`: `size`, `remaining`, `written_mut`, `fill_zeroes`, `as_uninit_mut`, `unsafe assume_written`, `io::Write`, `io::Seek`, `Debug` | Same | Bytes are always initialized; the seek error text differs |

## LMDB features

| LMDB feature | Status | Reference |
|---|---|---|
| Nested **read** transactions inside a write transaction (fork feature) | Supported | SPEC 04 §5, ADR-0007, [D-005](#d-005) |
| Nested **write** transactions | Unsupported | [D-003](#d-003) |
| Multi-process access, lock file, `mdb_reader_check` | Supported, on by default; the lock file is ZeroDB's own format | ADR-0023, SPEC 04 §12 |
| `MDB_WRITEMAP`, `NOSYNC`, `NOMETASYNC`, `MAPASYNC`, `RDONLY`, `PREVSNAPSHOT`, `NOTLS` | Supported | SPEC 01, ADR-0021 |
| `MDB_NOSUBDIR` | Refused at open | [D-016](#d-016) |
| `MDB_NORDAHEAD` | Supported (`MADV_RANDOM`) | SPEC 01 |
| `MDB_NOLOCK` | Supported (no lock file, single process) | ADR-0023 |
| `MDB_FIXEDMAP`, `NOMEMINIT`, `mdb_set_dupsort` | Accepted, no effect | |
| `DUPSORT`, `DUPFIXED`, `INTEGERKEY`, `INTEGERDUP`, `REVERSEKEY`, `REVERSEDUP`, dup cursor ops, `MDB_MULTIPLE` | Unsupported | [D-004](#d-004) |
| `mdb_set_compare` | Emulated (safe trait, named databases, not persisted) | SPEC 03 §2.0, [D-014](#d-014) |
| `mdb_env_set_flags` at runtime | Unsupported | |
| `mdb_env_set_mapsize` at runtime | Supported (`resize`) | SPEC 02 §8 |
| `MDB_APPEND`, `MDB_NOOVERWRITE`, `MDB_CURRENT`, `MDB_RESERVE`, `MDB_CP_COMPACT` | Supported | SPEC 01, ADR-0009 |
| Page size selection (LMDB 1.0) | Supported | SPEC 02 §0 |
| Encryption at rest (LMDB 1.0) | Unsupported | |
| Error text (`mdb_strerror`) | Same | |

## What Meilisearch and hannoy use

Everything they call is **Same** or a supported LMDB feature. This was checked
on Meilisearch v1.53.1, Meilisearch v1.55.0 (through the `unique-txns` shim)
and hannoy v0.1.7-nested-rtxns, by running their test suites on the shim with
no source changes ([maintainers/CONSUMER-GATE.md](maintainers/CONSUMER-GATE.md)).
None of the differences below is reached by either consumer.

## Behavior differences

Each entry has a stable ID that code and tests cite; IDs are never reused.
**Approved** means the maintainer accepted the difference. **Proposed** means
it is recorded but still awaits a decision. **Retired** means it is no longer a
difference. Details live in the linked ADR or SPEC section.

| ID | Area | LMDB | ZeroDB | Status |
|---|---|---|---|---|
| <a id="d-001"></a>D-001 | Process model | Cross-process access through a lock file. | Same since ADR-0023: the lock file is on by default, and `NO_LOCK` (`lock_file(false)` natively) keeps an env single-process, as `MDB_NOLOCK` does. | Retired |
| <a id="d-002"></a>D-002 | File format | LMDB 0.9 and 1.0 on-disk formats. | Own format; migrate with a logical dump and load. | Approved |
| <a id="d-003"></a>D-003 | Nested write txns | Child write txns with page shadowing. | Not representable: no entry point exists, so a call does not compile. heed itself exposes `Env::nested_write_txn`, so this is a gap in the drop-in API; no consumer calls it. Nested *read* txns are supported. | Approved |
| <a id="d-004"></a>D-004 | `DUP_SORT`, `DUP_FIXED` and other `DatabaseFlags` | Duplicate keys, integer keys. | Unsupported; no consumer uses any `DatabaseFlags`. The design (ADR-0011) was parked. | Approved |
| <a id="d-005"></a>D-005 | Writer during nested read txns | Not enforced: the write txn may mutate while nested readers are live. | Enforced for Rust soundness, by the shared `&wtxn` borrow and a runtime check on every mutation (SPEC 04 §5.2). Every consumer call site already pauses the writer. | Approved |
| <a id="d-006"></a>D-006 | `map_size` validation | heed rejects a `map_size` that is not a multiple of the OS page size. | The engine accepts any `map_size ≥ 2 × page_size`; the heed adapter re-imposes heed's check, so consumers see exact parity. | Approved |
| <a id="d-007"></a>D-007 | APPEND after a same-txn clear of another database | The fork crashes with `SIGSEGV` in `_mdb_cursor_put` ([UPSTREAM-BUGS.md](UPSTREAM-BUGS.md)). | Returns `KeyExist` (SPEC 01 §S1). The oracle skips the combination until the fork is fixed. | Approved |
| <a id="d-008"></a>D-008 | Raw bytes of a named-database catalog record | Reading a parent database shows each sub-database's record in LMDB's `MDB_db` layout. | Same entries and keys, but ZeroDB's own record layout (a consequence of D-002). Names with an embedded NUL are valid in the engine; the adapter reproduces heed's panic. | Approved |
| <a id="d-009"></a>D-009 | `open_database` of a dropped, reused slot | A fresh read txn can get a slot reused by a concurrent write txn, then fails with `EINVAL`. | Answers from the txn's committed catalog with `Ok(None)`, the correct snapshot answer. | Approved |
| <a id="d-010"></a>D-010 | `max_readers(0)` at open | Refused with `EINVAL` (`Io(InvalidInput)`). | The engine opens and every read txn fails with `ReadersFull`; the adapter re-imposes the open-time error. | Approved |
| <a id="d-011"></a>D-011 | `me_numreaders` | A high-water mark of simultaneous readers, shared across processes. | Reproduced exactly in `EnvInfo::num_readers`; the live count is also exposed as `live_readers`. Open question: switch `num_readers` to the live count? | Proposed |
| <a id="d-012"></a>D-012 | Env file names | `<dir>/data.mdb` and `lock.mdb`. | The adapter uses `data.mdb` and `lock.mdb` (ADR-0010); the native API uses `zerodb.dat` and `zerodb.lock`. No difference remains at the heed boundary. | Approved |
| <a id="d-013"></a>D-013 | Handle reuse after an aborted txn | Returns `EINVAL`. | Same since ADR-0024 (SPEC 04 §13), apart from the two D-026 cases. | Retired |
| <a id="d-014"></a>D-014 | Custom comparator persistence | A per-open function pointer, never persisted: reopening with another comparator silently corrupts lookups. | Same hazard, with two mitigations: a conflicting in-process registration is refused, and custom comparators are limited to named databases. Open question: store a comparator fingerprint in the database record (a format change) so a mismatch fails at open? | Proposed |
| <a id="d-015"></a>D-015 | Reserved puts | heed reserves the slot, then runs the closure: a failing closure leaves the partial entry and returns `Io`; an under-fill returns `Io(UnexpectedEof)`. | `Database::put_reserved` behaves as heed does, and also zeroes the bytes the closure did not write. The cursor form, `put_current_reserved_with_flags`, fills a zeroed buffer before the put: a failing closure leaves the old entry intact and an under-fill is accepted with a zero tail. No consumer calls the cursor form. Open question: build an engine-level cursor reserve, or approve the emulation? | Proposed (cursor form) |
| <a id="d-016"></a>D-016 | `MDB_NOSUBDIR` | The env is two plain files, `path` and `path-lock`. | Refused at open with `Io(Unsupported)`; every ZeroDB env is a directory. No consumer sets it. | Approved |
| <a id="d-017"></a>D-017 | Meta that names pages beyond the file | Not checked at open; found as `SIGBUS` on first touch. | Open refuses with `Mdb(Invalid)` on out-of-file geometry, an out-of-range root or an out-of-range txnid; reads past the snapshot get a typed error. An invalid newer meta fails the open rather than falling back (reachable only under `NO_SYNC`). | Approved |
| <a id="d-018"></a>D-018 | `max_readers` and `max_dbs` upper bound | Any `u32`. | Values above `1 << 20` are refused at open with `Io(InvalidInput)`, because the reader table is allocated eagerly. | Approved |
| <a id="d-019"></a>D-019 | Page validation | Never validated; a corrupt page is undefined behavior. | Validates by default (corruption is a typed error). The opt-in `FileTrust::trust_contents()` skips per-cell checks like LMDB but keeps bounds checks, so reads never leave the map (ADR-0014). **Change proposed:** ADR-0034 makes the trusting policy the default behind the heed API, as in LMDB; the design awaits approval. | Approved |
| <a id="d-020"></a>D-020 | Sequential-writes fast path | Every put descends from the root. | Opt-in, with a per-database override: a key that sorts last is appended at a remembered rightmost leaf. Results and files are identical (ADR-0015). | Approved |
| <a id="d-021"></a>D-021 | Dirty-page limit | A compile-time constant (131,072 pages). | Same default, plus the opt-in `max_dirty_bytes`. Results and files never depend on it (ADR-0017). | Approved |
| <a id="d-022"></a>D-022 | Freed-page list placement | Every commit's freed list goes into the free-page B-tree. | Each commit's freed list rides in its meta page; the tree is used only for overflow and lists carried for readers. Format version 2; results are identical (ADR-0022). | Approved |
| <a id="d-023"></a>D-023 | `resize` while another process writes | `mdb_env_set_mapsize` takes no lock and returns at once, also on a read-only env. | In a lock-file env it waits for the cross-process writer lock, as a write begin does, so every cached snapshot stays inside the new size. This includes a `READ_ONLY` env. A live write txn of the same env still gets `EINVAL` (ADR-0023). | Approved |
| <a id="d-024"></a>D-024 | Data-file length of a `WRITE_MAP` process that joins a running env | The joiner truncates the file to its own map size at open, up or down; shrinking leaves the holder's mapping past the end of the file. | The joiner never resizes the file at open; it grows the file to its map size at its first write-txn begin, under both writer locks. The file only ever grows. Contents and results are identical (ADR-0023). | Approved |
| <a id="d-025"></a>D-025 | Two `PREV_SNAPSHOT` cases | (a) If the opener ends before its first commit, a waiting process reads the older meta (or fails with `EINVAL` on macOS after a clean close). (b) A read-only `PREV_SNAPSHOT` open is not refused while another process holds the env, and rolls every process back to the older meta. | (a) A waiting process re-initializes the lock file and serves the newest meta. (b) A read-only `PREV_SNAPSHOT` joiner fails with `EAGAIN` and the running env is untouched, because matching LMDB would let a writer reuse pages that newer readers still read. | Approved |
| <a id="d-026"></a>D-026 | Database handles in two stale cases | (a) A handle whose slot was freed by an abort and reused for another name reads that other database. (b) When two read txns first-open the same name, later txns get `MDB_BAD_DBI` through either handle. | (a) `EINVAL`: a handle carries its slot's generation. (b) The slot is shared by its openers and both handles stay valid (SPEC 04 DBI-9). | Approved (a), Proposed (b) |
| <a id="d-027"></a>D-027 | Which freed pages a write txn reuses | (a) Pages freed by txn `N − 1` are never reused by txn `N`, even with no reader open. (b) Freed records are merged before searching for a run of contiguous pages. | (a) Pages freed by txn `N − 1` are reused when no reader pins an older snapshot (SPEC 05 GC-18), so files are never larger for this reason. (b) Same since ADR-0029: the gated records are merged and searched (SPEC 05 GC-16, GC-21). | Proposed (a), Resolved (b) |

### Checks the adapter re-imposes

The engine keeps its lenient native behavior, and `heed-zerodb` re-imposes the
fork's stricter checks at the heed boundary. Each one is tested in
`crates/heed-zerodb/tests/boundary.rs`:

- [D-006](#d-006): `open` rejects a `map_size` that is not a multiple of the OS
  page size with `Io(InvalidInput)`.
- [D-010](#d-010): `open` rejects `max_readers(0)` with `Io(InvalidInput)`.
- [D-008](#d-008): a database name with an embedded NUL panics, as heed's
  `CString::new(name).unwrap()` does.
- An empty key returns `MdbError::BadValSize` on reads.
