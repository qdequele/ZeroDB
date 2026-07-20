# SPEC 00 — API surface contract (Phase 0.1 deliverable)

Status: **DONE** — 2026-07-15.

> **M1.13 (2026-07-17):** the `heed-zerodb` adapter now **satisfies the full MUST
> table over ZeroDB's native API** — the integration rows 23–26 (registry /
> `EnvClosingEvent` / `Env: Clone`), 51–54 (`remap_*` / codecs /
> `DefaultComparator` / error taxonomy), and 61 (the `WithoutTls`/`PutFlags`/…
> type surface + `BytesEncode`/`BytesDecode`/`BoxedError` re-exported verbatim)
> are realized by `crates/heed-zerodb`, and the read/write/env rows 1–22, 27–49,
> 55–60 are wired through it (verified by the oracle re-run through the adapter
> and by the milli+hannoy compile gate, ADR-0003). The read-key taxonomy of
> rows 30/47/48 (empty key → `BadValSize`) is re-imposed at the adapter boundary
> (SPEC 03 §2.1). The WithTls rows stay a compile-only shim (Phase 2, OQ5).

This document is the frozen heed API contract that ZeroDB Phase 1 must reproduce.
It is derived by inventorying every `heed` item used by the five consumers below,
reading the call sites (not guessing), and mapping each item to the LMDB
primitive it wraps and to the Phase 1 milestone that must implement it.

Every **MUST** row (a heed item at least one consumer uses) is traceable to a
Phase 1 milestone (1.1–1.13). Items heed exposes but no consumer uses are in the
second table, marked SHOULD (Phase 2) or WON'T (with justification).

## Pinned consumer SHAs (the frozen contract)

The versions below are exactly what Meilisearch's `Cargo.lock` resolves to at the
pinned Meilisearch SHA. heed, arroy, hannoy and cellulite come from **crates.io**
(not git); there is **no `[patch]`** and no git override in the workspace
`Cargo.toml`. The repo SHAs are the git tags matching those crate versions.

| Repo | URL | Rev (tag/branch) | Commit SHA | Notes |
|------|-----|------------------|------------|-------|
| meilisearch | https://github.com/meilisearch/meilisearch | default branch (`main`), committed 2026-07-09 | `fff2ef5a42658b16a937d922aabc3fb7f89f2018` | shallow `--depth 1`; workspace incl. milli, index-scheduler, meilitool, dump, meilisearch-auth, meilisearch, meilisearch-types |
| heed | https://github.com/meilisearch/heed | `v0.22.1` | `86cd1f681953cd5f6870706f6139b851e975975e` | the API being reproduced |
| arroy | https://github.com/meilisearch/arroy | `v0.6.4` | `d712286a396e08315093c00ef9a177385a314a03` | milli dep `arroy = "0.6.4"` |
| hannoy | https://github.com/nnethercott/hannoy | `v0.1.3` | `e1e2f4d38fd4f1df69684f24183cdc7a207f8666` | milli dep `hannoy = { "0.1.3", features = ["arroy"] }` |
| cellulite | https://github.com/meilisearch/cellulite | `v0.3.2` | `b9c13c6d507155249033b9226bd549af4f285100` | milli dep `cellulite = "0.3.2"`; geo/spatial store, heed 0.22.1 |

**Underlying LMDB:** heed 0.22.1 bundles `lmdb-master-sys 0.2.6`, whose vendored
C is the **Meilisearch LMDB fork** (`github.com/meilisearch/lmdb`, submodule
branch `mdb.master.nested-rtxns`), not stock LMDB 0.9. The fork's one behavior
difference that matters here is read-only transactions nested inside a write
transaction (see Findings §A). ZeroDB targets parity with this fork's observable
behavior, via the oracle, not with stock LMDB where the two differ.

**"Used by" legend:** `milli` = crates/milli; `ms` = other Meilisearch workspace
crates (index-scheduler, meilitool, meilisearch, meilisearch-auth,
meilisearch-types, dump); `arroy`; `hannoy`; `cellulite` (geo/spatial store,
another milli dep). arroy, hannoy and cellulite never open an env themselves in
library code — the caller (milli) owns the `Env` and the transactions and passes
them in; their env-open only appears in their own tests/doctests, so their
env-lifecycle usage is not part of the frozen contract (marked "tests only" where
relevant; cellulite's tests open with the default `WithTls`, but production
cellulite is driven by milli's `WithoutTls` env and generic txn refs).

---

## Main contract table (MUST — reproduced in Phase 1)

| # | heed item | Used by | LMDB primitive | Notes / edge semantics observed at call sites | Phase 1 milestone |
|---|-----------|---------|----------------|-----------------------------------------------|-------------------|
| 1 | `EnvOpenOptions::new()` | milli, ms | — | Builder entry; default is `WithTls`, immediately converted (row 2). | 1.2 |
| 2 | `.read_txn_without_tls()` → `EnvOpenOptions<WithoutTls>` | milli, ms, (hannoy tests) | `MDB_NOTLS` env | **Every** production open uses this. `WithoutTls` makes `RoTxn: Send` so read txns move to rayon/async threads. WithoutTls is the universal mode; TLS-on path is never used. | 1.2, 1.8 |
| 3 | `.map_size(usize)` | milli, ms, arroy(t), hannoy(t) | `mdb_env_set_mapsize` | Always clamped to page size (`clamp_to_page_size`) before the call. No `Env::resize` exists in heed 0.22 — growth is handled by reopening with a larger map after `MapFull` (row 55). | 1.2 |
| 4 | `.max_dbs(u32)` | milli(`NUMBER_OF_DBS`=27+geo), ms (auth=2, meilitool=100, versioning) | `mdb_env_set_maxdbs` | Named-DB catalog capacity. | 1.6 |
| 5 | `.max_readers(u32)` | ms (index_map only; `MEILI_EXPERIMENTAL_INDEX_MAX_READERS`, default 1024) | `mdb_env_set_maxreaders` | Sizes the reader table. Only one call site. | 1.8 |
| 6 | `.flags(EnvFlags)` (unsafe) | milli, ms | `mdb_env_set_flags` bits at open | Only two flag *values* ever passed — rows 8, 9. Call is `unsafe`. | 1.10 |
| 7 | `unsafe { options.open(path) }` | milli, ms, arroy(t), hannoy(t) | `mdb_env_create` + `mdb_env_open` | Directory env (`data.mdb`/`lock.mdb`); never `NO_SUB_DIR`. **ADR-0010 / D-012:** the adapter satisfies the **`data.mdb` half** of this convention — an env opened through `heed-zerodb` materializes as `<dir>/data.mdb` (via `zerodb::EnvOpenOptions::data_file_name`; the native engine's default stays `zerodb.dat`). Required because consumers hardcode the name outside heed (Meilisearch compaction/snapshot paths). **`lock.mdb` is intentionally absent** (D-001: single-process, no reader protocol; nothing in the consumer tree reads it). Contents are still ZeroDB's own format (D-002) — the `ZDB1` magic makes real LMDB reject the file loudly rather than misread it. `Env::path()` still returns the directory. | 1.2, ADR-0010 |
| 8 | `EnvFlags::WRITE_MAP` | ms (index_map, gated on experimental writemap) | `MDB_WRITEMAP` | Writes go through the writable mmap instead of `malloc`+`pwrite`. Interacts with the dirty-page value-borrow model (SPEC 04) and `put_reserved` (row 35). Optional but present. | 1.10 |
| 9 | `EnvFlags::PREV_SNAPSHOT` | milli (`Index::rollback` only) | `MDB_PREVSNAPSHOT` | Opens the env on the **older** of the two meta pages — directly the meta double-buffer (SPEC 04). Used to roll an index back one committed txn. | 1.2, 1.10 |
| 10 | `Env::open_database::<KC,DC>(txn, Some(name))` | ms (upgrades, meilitool), cellulite | `mdb_dbi_open` (no create) | Opens an existing named DB; `None` for the returned dbi means absent. cellulite opens 4 named DBs per instance (`{prefix}-item/-cell/-update/-metadata`). | 1.6 |
| 11 | `Env::create_database(&mut wtxn, name)` | milli, arroy, hannoy, cellulite | `mdb_dbi_open` + `MDB_CREATE` | arroy/hannoy always create the **unnamed** DB (`None`); milli creates ~27 named DBs; cellulite creates 4 **named** DBs per instance (name-prefixed, so several cellulite indexes share one env). Create-in-write-txn. No `DatabaseFlags` argument (row 50). | 1.6 |
| 12 | `Env::database_options().name(n).create/open()` | milli (MAIN), meilitool (poly `Database<Unspecified,Unspecified>`) | `mdb_dbi_open` | Typed builder form of 10/11. | 1.6 |
| 13 | `Env::write_txn()` → `RwTxn` | milli, ms, arroy(t), hannoy, cellulite(t) | `mdb_txn_begin` (write) | Single top-level writer. | 1.4 |
| 14 | `Env::read_txn()` → `RoTxn<WithoutTls>` | milli, ms, arroy(t), hannoy, cellulite | `mdb_txn_begin` (`MDB_RDONLY`) | Snapshot reader; missing key → `Ok(None)`, never a `NotFound` error. | 1.3, 1.8 |
| 15 | `Env::static_read_txn()` → `RoTxn<'static, WithoutTls>` | milli (`env.clone().static_read_txn()`), ms | `mdb_txn_begin` (`MDB_RDONLY`) + owns Env | `'static` read txn that keeps the Env alive (blocks close). Handed to async handlers/other threads. Requires row 25 (Env: Clone). | 1.8 |
| 16 | `RwTxn::nested_read_txn()` / `Env::nested_read_txn(&wtxn)` | milli (5 sites), hannoy (1 site) | `mdb_txn_begin(env, wtxn, MDB_RDONLY)` **(fork-only)** | **Read-only child of an active write txn** — sees the write txn's uncommitted state. Opened `N = threads(+1)` times, fanned out to rayon workers for parallel reads during indexing/HNSW build while the wtxn is paused. `WithoutTls` (Send) required. **Not supported by upstream LMDB** — provided by the Meilisearch fork. Hot-path MUST. See Findings §A. | 1.9 (re-scoped), 1.8 |
| 17 | `Env::copy_to_file` / `copy_to_path(_, CompactionOption)` | ms (snapshots, s3, compact route), meilitool | `mdb_env_copy2` (`MDB_CP_COMPACT` when Enabled) | **Both** `CompactionOption::Enabled` (normal snapshot/compaction) and `Disabled` (experimental no-compaction, and raw S3 env copy) are used. copy opens its **own** internal read txn (caller cannot supply one). **Landed M1.12** as `zerodb::CopyToFile::copy_to_file(path, CompactionOption)` (extension trait; produces a single `zerodb.dat`-format file). `Enabled` = fresh compact rebuild via the bulk builder; `Disabled` = raw page copy + fresh metas at the snapshot. Reader-pin correctness (torn-free live pages) + oracle dump-equality vs heed. See ADR-0009. | 1.12 ✓ |
| 18 | `Env::real_disk_size()` | milli, ms, auth | fstat on the data file | On-disk file size (used bytes on disk). | 1.2, 1.12 |
| 19 | `Env::non_free_pages_size()` | milli, ms (incl. resize trigger: `>0.75*map_size`) | reads the **freelist DB** to subtract free pages | LMDB-format-specific: walks the free/GC DB. Needs a **native ZeroDB equivalent** over ZeroDB's own freelist. Drives auto-resize decisions. MUST. | 1.5 |
| 20 | `Env::info()` (reads `.map_size`) | milli, ms | `mdb_env_info` | Only the `map_size` field is read (resize math). | 1.2 |
| 21 | `Env::path()` | milli | `mdb_env_get_path` | — | 1.2 |
| 22 | `Env::try_clone_inner_file()` → `File` | milli, ms (s3 export) | `dup()` the data-file fd | Streams the raw `data.mdb` into a tarball while holding a write txn as an exclusive lock. Depends on the data file being a single regular file. | 1.2, 1.12 |
| 23 | `Env::prepare_for_closing()` → `EnvClosingEvent` | milli, ms | `mdb_env_close` deferred until last handle drops | Consumes the Env, returns a signal fired when the last clone is dropped and the env is truly closed. | 1.13 |
| 24 | `EnvClosingEvent::wait()` / `wait_timeout(dur)` | ms (index_map, shutdown, tests) | — (SignalEvent) | The IndexMap LRU waits on this before reopening a path on resize/delete. | 1.13 |
| 25 | `Env: Clone` (shared handle) | milli, ms | refcounted `MDB_env*` | `Index { env: env.clone() }`; `env.clone().static_read_txn()`. Backend Env must be cheaply cloneable, shared-ownership. | 1.13 |
| 26 | Same-process env registry + `EnvAlreadyOpened` | milli, ms | heed's global open-env map | One `Env` per path per process; opening a still-open path errors `EnvAlreadyOpened` (handled at milli/src/error.rs:684). `Index::rollback` deliberately `drop`s env+txn before reopening with `PREV_SNAPSHOT`; the IndexMap shares one env via clones. | 1.13 |
| 27 | `RwTxn::commit()` | milli, ms, arroy(t), hannoy, cellulite(t) | `mdb_txn_commit` | — | 1.4 |
| 28 | `RwTxn::abort()` | milli, ms, hannoy | `mdb_txn_abort` | Also used as a pure release of a write-lock-only txn (s3 export). | 1.4 |
| 29 | `RoTxn<'static, WithoutTls>` is `Send` | milli, ms, hannoy, arroy | `MDB_NOTLS` semantics | Read txns and frozen readers cross thread boundaries (rayon, async). Load-bearing for parallel reads. | 1.8 |
| 30 | `Database::get(txn, &key)` | milli, ms, arroy, hannoy, cellulite | `mdb_get` | Missing key = `Ok(None)`. arroy/hannoy treat a missing internal node as corruption (`missing_key` error), missing item as legit `None`. | 1.3 |
| 31 | `Database::put(txn, &key, &val)` | milli, ms, arroy, hannoy, cellulite | `mdb_put` (flags 0) | Overwrite allowed. Values 0 B – multi-MB (roaring bitmaps, vectors). | 1.4 |
| 32 | `Database::put_with_flags(txn, PutFlags::APPEND, k, v)` | arroy (writer.rs:453) | `mdb_put` + `MDB_APPEND` | Bulk item append in strictly ascending key order; misuse (key not > last) → `MdbError::KeyExist` → `Error::InvalidItemAppend`. | 1.10 |
| 33 | Cursor `put_current(k, v)` / `put_current_with_options::<NDC>(PutFlags, k, v)` (unsafe) | milli (`_with_options`, `APPEND`, facet bulk), arroy (`_with_options`, `empty`), hannoy (`_with_options`, `empty`), cellulite (plain `put_current`, builder.rs:210) | `mdb_cursor_put` at current pos (`MDB_CURRENT`, plus `MDB_APPEND` for milli) | In-place overwrite at the cursor; the `_with_options` form additionally **re-encodes the value with a different data codec** (`NDC`). milli's facet bulk uses `APPEND` for sorted build; cellulite uses the plain `put_current` to rewrite a cell's bitmap during `iter_mut`. `unsafe`: no live borrow into the entry may span the call. | 1.4, 1.10 |
| 34 | Cursor `del_current()` (unsafe) | arroy, hannoy, milli, cellulite | `mdb_cursor_del` | Delete-at-cursor during `iter_mut`/`prefix_iter_mut` passes (cellulite: builder.rs:208). `unsafe`: no live borrow of the current entry across the call. | 1.4 |
| 35 | `Database::put_reserved(txn, k, len, f)` (+ `ReservedSpace`) | milli (word-prefix docids) | `mdb_put` + `MDB_RESERVE` | Reserves `len` bytes in-place and writes the value directly into the map via the closure (avoids a temp buffer). Interacts with `WRITE_MAP` (row 8) and the dirty-page borrow model (SPEC 04). | 1.10 |
| 36 | `Database::delete(txn, &key)` → `bool` | milli, ms, arroy, hannoy, cellulite | `mdb_del` | Returns existed-flag; often ignored, sometimes propagated. | 1.4 |
| 37 | `Database::delete_range(txn, &range)` → `usize` | milli, ms, arroy | cursor walk + `mdb_cursor_del` | arroy deletes a whole index's tree-node keyspace via an inclusive `..=` range; milli bulk-deletes key ranges in upgrades/post-processing. | 1.3, 1.4 |
| 38 | `Database::clear(txn)` | milli, ms, arroy (upgrade path), cellulite | `mdb_drop(_, 0)` (empty, keep dbi) | Whole-DB wipe. (arroy/hannoy's *user-facing* `clear` is a per-prefix cursor loop, not this method — see rows 33/34.) | 1.6 |
| 39 | `Database::len(txn)` → `u64` | milli, ms, arroy, hannoy, cellulite | `mdb_stat.ms_entries` | Entry count (arroy/hannoy: total across all logical partitions in the single DB). | 1.3 |
| 40 | `Database::is_empty(txn)` | milli, ms, cellulite | `mdb_stat` / cursor-first | — | 1.3 |
| 41 | `Database::first(txn)` | ms (task queue) | `mdb_cursor_get(MDB_FIRST)` | Min key. | 1.3 |
| 42 | `Database::last(txn)` | ms (task queue: next uid = last+1) | `mdb_cursor_get(MDB_LAST)` | Max key. | 1.3 |
| 43 | `Database::iter(txn)` / `iter_mut(txn)` | milli, ms, arroy, hannoy, cellulite | `mdb_cursor_get(MDB_FIRST/NEXT)` | Forward full scan; `iter_mut` for in-place rewrite (rows 33/34). Ascending key order. | 1.3, 1.4 |
| 44 | `Database::range(txn, &R)` / `rev_range` | milli | cursor `MDB_SET_RANGE` + NEXT/PREV | Typed range bounds; `rev_range` (3 sites) iterates a bounded range descending. | 1.3 |
| 45 | `Database::prefix_iter` / `prefix_iter_mut` | milli, ms, arroy, hannoy, cellulite | `MDB_SET_RANGE` + prefix compare | Prefix scan implemented as a range from the prefix; used pervasively for word/facet/node partitions. | 1.3 |
| 46 | `Database::rev_prefix_iter` | milli (2 sites) | `MDB_SET_RANGE` then descend | Reverse prefix scan. | 1.3 |
| 47 | `Database::get_greater_than(txn, &k)` | milli (facet tree) | `mdb_cursor_get(MDB_SET_RANGE)` then skip-equal | Strict `>` neighbor seek. | 1.3 |
| 48 | `Database::get_lower_than_or_equal_to(txn, &k)` | milli (facet tree) | `MDB_SET_RANGE` then step back if `>` | `<=` neighbor seek. Facet level navigation relies on both 47/48. | 1.3 |
| 49 | `Database::stat(txn)` → `DatabaseStat` | milli (per-DB size reporting, index.rs:1950+), cellulite | `mdb_stat` | Depth, page counts, entries per named DB (used for `compute_size`). | 1.6 |
| 50 | `lazily_decode_data()` / `LazyDecode<DC>` | milli, arroy, hannoy | — (defers `BytesDecode`) | Iterate keys/raw values without decoding until needed (upgrades, prefix-filtered scans). Affects iterator typing, not the engine. | 1.3 |
| 51 | `remap_types` / `remap_key_type` / `remap_data_type` | milli, ms, arroy, hannoy, cellulite | — (client-side type reinterpretation) | Heavily used: DBs stored as `Database<Unspecified,Unspecified>` and remapped per call; `DecodeIgnore` to test existence without decoding. Pure client-side; engine sees only bytes. The backend must keep dbi identity stable across remaps. | 1.13 |
| 52 | Codecs: `Str`, `Bytes`, `Unit`, `DecodeIgnore`, `SerdeJson<T>`, `SerdeBincode<T>`, `U16/U32/U64/I128<BigEndian>`, `LazyDecode`, + in-repo custom codecs | milli, ms, arroy, hannoy, cellulite | — (client-side `BytesEncode`/`BytesDecode`) | Engine stores/returns raw bytes only. Custom codecs (arroy `KeyCodec`/`NodeCodec`/`MetadataCodec`; hannoy same + `UpdateStatusCodec`; milli `FacetGroupKeyCodec`, `CboRoaringBitmapCodec`, `ObkvCodec`, `StrBEU32Codec`, `UuidCodec`, …; cellulite `ItemKeyCodec`, `CellKeyCodec` (H3 cell index), `MetadataKey`, `UpdateType`, `ZerometryCodec`, `RoaringBitmapCodec`) all encode keys **big-endian** so lexicographic byte order = logical order. No custom comparator is set — see row 53. | 1.13 |
| 53 | `DefaultComparator` (memcmp key ordering) | milli, ms, arroy, hannoy, cellulite (implicit) | LMDB default `mdb_cmp_memn` | All ordering, prefix scans, ranges, `APPEND`, and neighbor seeks assume **unsigned lexicographic byte comparison**. No `IntegerComparator` / custom `mdb_set_compare`. This is the single ordering invariant the whole contract rests on. | 1.3 |
| 54 | `heed::Error` variants `Io`, `Mdb(MdbError)`, `Encoding`, `Decoding`, `EnvAlreadyOpened` | milli, ms, arroy, hannoy, cellulite | LMDB rc → error | Matched/converted in milli/src/error.rs & meilisearch-types/src/error.rs. `Encoding`/`Decoding` are also **constructed** by consumers to signal codec failures (many sites). Taxonomy must map 1:1. | 1.13 |
| 55 | `MdbError::MapFull` | milli, ms | `MDB_MAP_FULL` | Mapped to `MaxDatabaseSizeReached` / `DatabaseSizeLimitReached`; triggers resize-and-reopen. | 1.2, 1.10 |
| 56 | `MdbError::Invalid` | milli, ms | `MDB_INVALID` | Mapped to `InvalidStoreFile` (not a valid DB file at open). | 1.2 |
| 57 | `MdbError::BadValSize` | milli (constructed) | `MDB_BAD_VALSIZE` | Synthesized when a key is empty or `> u16::MAX` bytes. Encodes milli's assumption of LMDB's max key size (`NonZeroU16`). ZeroDB must enforce/report the same key-size bound. | 1.3 |
| 58 | `MdbError::KeyExist` | arroy | `MDB_KEYEXIST` | Returned by `APPEND` when the key is not strictly greater than the last (row 32). | 1.10 |
| 59 | `CompactionOption` enum (`Enabled`/`Disabled`) | ms, meilitool | `MDB_CP_COMPACT` flag toggle | Argument to rows 17. | 1.12 |
| 60 | `EnvInfo` struct (`.map_size`) / `DatabaseStat` struct | milli, ms | `mdb_env_info` / `mdb_stat` | Return types of rows 20/49; fields read must match. | 1.2, 1.6 |
| 61 | `WithoutTls` / `WithTls` marker types; `PutFlags`, `EnvFlags`, `DatabaseFlags` types; `BytesEncode`/`BytesDecode`/`BoxedError` traits | all | — (type surface) | Consumers name these concrete types/paths directly. The integration crate must re-export the exact type paths (Findings §C). | 1.13 |

---

## Second table — heed items NO consumer uses (SHOULD / WON'T)

| heed item | LMDB primitive | Priority | Justification |
|-----------|----------------|----------|---------------|
| `EnvOpenOptions::read_txn_with_tls()` / `WithTls` TLS-bound read txns | `MDB_env` TLS slots | SHOULD (2.x) | Every consumer opens `WithoutTls`. heed's default TLS path must still exist for API completeness, but ZeroDB can make WithoutTls the real (and possibly only meaningful) mode; provide a WithTls shim. |
| `Env::nested_write_txn` / `RwTxn::nested(...)` (child **write** txn) | `mdb_txn_begin(env, parent_wtxn, 0)` | WON'T (Phase 1) | **No consumer opens a nested write txn** (verified across all four repos). M1.9 write-txn implementation is descoped to a clean "unsupported" error; log in DIVERGENCES.md. Full impl only if a consumer later needs it. (Contrast row 16: nested *read* txns ARE used and are a MUST.) |
| `Env::force_sync()` | `mdb_env_sync(force)` | **LANDED M2.5** | Not called; consumers rely on sync-on-commit. **M2.5:** `force_sync()` = `mdb_env_sync(env, 1)`; the full `force` parameter is exposed as the new `Env::sync(force)` (zerodb extension — heed has only the forced form), reproducing `mdb_env_sync0`'s three decisions exactly: `MDB_RDONLY` → `EACCES` first, flush only if `force \|\| !NO_SYNC`, and `MS_ASYNC` only when `MAP_ASYNC && !force`. Tests: `zerodb-oracle/tests/force_sync_durability.rs` (8, incl. the `FaultBacking` journal-drain proof and the `EACCES` differential), `heed-zerodb/tests/phase2_extensions.rs`. |
| `Env::stat()` (env-level) / `EnvStat` | `mdb_stat` on main | **LANDED M2.1** | Only per-DB `stat()` (row 49) and `info().map_size` (row 20) are used. **M2.1:** `zerodb::Env::stat()` → `EnvStat { page_size, depth, branch_pages, leaf_pages, overflow_pages, entries }` over the main tree, read from the published snapshot (no read txn, so no reader slot). `EnvInfo` completed to the full `MDB_envinfo` shape (`map_size`, `last_pgno`, `last_txnid`, `max_readers`, `num_readers`, plus the `live_readers` extension); `me_mapaddr` is not exposed natively (`MDB_FIXEDMAP` is WON'T) and stays null in the adapter's mirrored struct. Tests: `zerodb/tests/env_stat_info.rs` (12), `zerodb-oracle/tests/env_info_differential.rs` (6), `heed-zerodb/tests/phase2_extensions.rs`. See D-011. |
| `Env::clear_stale_readers()` | `mdb_reader_check` | SHOULD (2.2) | Single-process model has no stale cross-process readers; expose introspection in Phase 2. |
| `Env::get_flags()` / `Env::flags()` getter / `FlagSetMode` / set-flags-after-open | `mdb_env_get_flags` / `mdb_env_set_flags` | SHOULD | Flags are only set at open (row 6); never read back or toggled at runtime. |
| `Env::max_readers()` getter, `Env::max_key_size()` | `mdb_env_get_maxreaders` / `mdb_env_get_maxkeysize` | `max_readers` **LANDED M2.1**; `max_key_size` SHOULD | `max_key_size` is assumed (row 57) but never queried; expose for completeness. **M2.1:** `heed_zerodb::Env::max_readers()` now returns the real reader-table capacity (it returned the hardcoded 126 in Phase 1), differentially verified against the fork at 1/8/126/1024. |
| `RoTxn::id()` / `RwTxn::id()` | `mdb_txn_id` | SHOULD | Not used. (ZeroDB stamps txnID on pages internally regardless — SPEC 02.) |
| `Database::get_or_put*` (4 variants) | `mdb_get`+`mdb_put` | SHOULD | Not used; convenience wrappers. |
| `Database::rev_iter` / `rev_iter_mut` / `range_mut` / `rev_range_mut` / `rev_prefix_iter_mut` | reverse/mut cursors | SHOULD | Consumers use only `rev_range`, `rev_prefix_iter`, `iter_mut`, `prefix_iter_mut` (rows 43–46). The remaining reverse/mut cursor variants share the same machinery, so cheap to include, but no consumer exercises them. |
| `Database::get_lower_than` / `get_greater_than_or_equal_to` | neighbor seeks | SHOULD | The other two of the four neighbor-seek variants; only `get_greater_than` and `get_lower_than_or_equal_to` are used (rows 47/48). |
| `Database::get_duplicates` | `mdb_cursor_get(MDB_GET_MULTIPLE/NEXT_DUP)` | WON'T (Phase 1) | DUPSORT-only; no consumer uses DUPSORT (see below). |
| `Database::delete_one_duplicate` | `mdb_cursor_del(0)` on a dup | WON'T (Phase 1) | DUPSORT-only; unused. |
| `DatabaseFlags::DUP_SORT`, `DUP_FIXED` | `MDB_DUPSORT` / `MDB_DUPFIXED` | WON'T Phase 1 / reclassify M1.7 as SHOULD | **No consumer creates a DUPSORT DB.** All multi-value data is modeled as composite keys + roaring-bitmap values. See Findings §B — this reshapes M1.7. |
| `DatabaseFlags::INTEGER_KEY` | `MDB_INTEGERKEY` | WON'T Phase 1 / SHOULD | Not used; integer keys use big-endian byte codecs so memcmp order = numeric order. |
| `DatabaseFlags::REVERSE_KEY` / `REVERSE_DUP` / `DUP_SORT`+integer combos | `MDB_REVERSEKEY` etc. | WON'T | Not used. |
| `Comparator` / `LexicographicComparator` / `IntegerComparator` / custom `mdb_set_compare` | `mdb_set_compare` / `mdb_set_dupsort` | SHOULD (2.4) | No consumer sets a custom key/dup comparator; all rely on `DefaultComparator` (row 53). Phase 2 exposes safe custom comparators. |
| `EncryptedEnv` / `EncryptedDatabase` / `EncryptedDatabaseOpenOptions` | LMDB 1.0 page encryption | WON'T Phase 1 | Encryption-at-rest is Phase 3.9. |
| `env_closing_event(path)` free function | global registry lookup | SHOULD | Consumers use `prepare_for_closing()` (row 23) which returns the event directly; the path-lookup form is unused. |
| `lmdb_version()` / `LmdbVersion` | `mdb_version` | WON'T | LMDB-specific version reporting; ZeroDB reports its own identity. |
| `EnvFlags` other than `WRITE_MAP`/`PREV_SNAPSHOT` (`NO_SYNC`, `NO_META_SYNC`, `MAP_ASYNC`, `NO_SUB_DIR`, `NO_LOCK`, `NO_READAHEAD`, `NO_TLS`, `NO_MEM_INIT`, …) | corresponding `MDB_*` | SHOULD/WON'T — see SPEC 01 | No consumer passes them. Durability flags map onto the ZeroDB writer in M1.10; cross-process flags (`NO_LOCK`) are WON'T per D-001 single-process model; `NO_TLS` is the implicit default. Full classification lives in `docs/SPEC/01-flags.md` (milestone 0.2). |


## Third table — ZeroDB extensions (APIs heed/LMDB do **not** have)

Phase 2 adds capabilities LMDB lacks entirely, so there is no heed signature to
mirror and no cross-engine differential to run — these are the "doc + unit tests
where it's zerodb-defined" half of the Phase 2 acceptance line. Every one is
**purely additive**: no existing heed-mirrored signature changes (PLAN ground
rule 2), and code that never calls them behaves exactly as before.

| ZeroDB item | Nearest LMDB concept | Milestone | Notes / tests |
|-------------|----------------------|-----------|---------------|
| `zerodb::EnvOpenOptions::page_size(u32)` / `get_page_size()`; `heed_zerodb::EnvOpenOptions::page_size(u32)` | none — LMDB 0.9 derives `me_psize` from the OS and offers no selector | 2.6 | Selects the **database** page size at env **creation**: a power of two in `[MIN_PAGE_SIZE, MAX_PAGE_SIZE]` = `[4096, 65536]` (SPEC 02 §0), independent of the OS page size. Invalid values → `Io(InvalidInput)` at `open` (the taxonomy already used for D-006/D-010 open-time rejections). **Creation-only**: reopening an existing store ignores the request and adopts the persisted geometry (SPEC 02 §3.2), exactly as for `map_size` — there is deliberately no "wrong expectation" error. Read the effective value back via `Env::page_size()` / `EnvStat::page_size`. Tests: `zerodb/tests/page_size_selection.rs` (10 — validation, 4K/8K/16K/32K/64K round-trips with GC + `check_image`, the reopen contract, and the cross-page-size equivalence property), `heed-zerodb/tests/phase2_extensions.rs`. |
| `zerodb::Env::sync(force: bool)`; `heed_zerodb::Env::sync(force: bool)` | `mdb_env_sync(env, force)` — but heed exposes only the `force = true` form | 2.5 | The `force = false` branch of `mdb_env_sync0`, unreachable through heed: a **no-op returning `Ok(())`** on a `NO_SYNC` env, and an asynchronous `msync` under `WRITE_MAP + MAP_ASYNC`. `force_sync()` is retained unchanged and is exactly `sync(true)`. |
| `zerodb::EnvInfo::live_readers`; `heed_zerodb::Env::live_readers()` | none — `me_numreaders` looks like this but is a high-water mark | 2.1 | The genuinely-live occupied-reader-slot count. Exists because LMDB's `me_numreaders` never decreases (D-011), which ZeroDB reproduces faithfully in `num_readers`. Exposed on the adapter as a **method**, not an `EnvInfo` field, so heed's mirrored struct keeps its exact shape. |

---

## Findings that affect the plan

### A. Nested-transaction verdict (milestone 1.9)

**No consumer opens a nested WRITE transaction.** `nested_write_txn` /
`RwTxn::nested` has zero call sites across meilisearch, milli, arroy and hannoy.
Every write txn is top-level (`env.write_txn()`). → M1.9's classic nested-child-
write-txn implementation should be **descoped**: return a clean "unsupported"
error, record it in `docs/DIVERGENCES.md`, and scope the 1.13/1.14 heed-suite
gate to exclude nested-write tests.

**However, nested READ transactions ARE used and are a hot-path MUST.** heed's
`RwTxn::nested_read_txn()` / `Env::nested_read_txn(&wtxn)` opens a **read-only
txn whose parent is the active write txn** (`mdb_txn_begin(env, wtxn,
MDB_RDONLY)`), which sees the write txn's *uncommitted* changes. Consumers create
`N = rayon_threads (+1)` of these and hand one to each worker thread to read the
in-progress write txn's data in parallel during indexing / HNSW build:

- milli: `update/new/words_prefix_docids.rs:46,156`,
  `update/new/indexer/post_processing/facet_bulk.rs:79`,
  `update/new/indexer/mod.rs:1056`, `update/upgrade/v1_32.rs:183`.
- hannoy: `src/parallel.rs:26` (`FrozenReader`, fanned out over rayon).

This capability **does not exist in upstream LMDB** (which rejects a read-only
child of a write txn); it is provided by the Meilisearch LMDB fork bundled in
`lmdb-master-sys 0.2.6` (branch `mdb.master.nested-rtxns`). Therefore **M1.9
should be re-scoped from "nested child write txns" to "nested read txns over a
write txn"**, and it is a MUST, not an optional/descoped item. It also couples
tightly to: (1) M1.8's concurrency model — these child readers run on rayon
threads and must observe the writer's dirty state without a data race while the
writer is quiesced; (2) SPEC 04's write-txn value-borrow contract — a nested
reader borrows bytes that may live in the writer's dirty pages; (3) Phase 3.8's
`RwTxn::snapshot()` idea, which is a *cleaner* native replacement for exactly
this pattern and should be designed with these five call sites as its target.

### B. Consumer usage NOT covered (or mis-framed) by the current PLAN.md

(Listed for the maintainer; this document does **not** edit PLAN.md.)

1. **DUPSORT/DUPFIXED (M1.7) is not used by any consumer.** No `DatabaseFlags`
   of any kind is ever passed; there is no `DUP_SORT`, `DUP_FIXED`,
   `INTEGER_KEY`, `get_duplicates`, `delete_one_duplicate`, or `APPEND_DUP`
   anywhere in milli, arroy, or hannoy. milli models facet/word-docids as
   composite keys + roaring-bitmap values (`FacetGroupKeyCodec`,
   `CboRoaringBitmapCodec`), not LMDB duplicates. PLAN.md 1.7 states "milli's
   facet/word-docids access patterns replayed with parity," which implies
   DUPSORT — that premise is **outdated**. Recommend reclassifying M1.7 as a
   **SHOULD** (heed exposes it; Phase 2) rather than a Phase-1 MUST. This removes
   one of the three named long poles ("highest-defect-density area of LMDB").
   Keep it a MUST only if the 1.13 heed-test-suite scope decision (0.5 ADR)
   requires DUPSORT tests to pass.

2. **`nested_read_txn` (Finding §A)** — PLAN.md 1.9 anticipates "neither milli
   nor hannoy uses nested txns (expected)" and is framed entirely around nested
   *write* txns. Nested *read* txns are used pervasively and are unmentioned. Add
   them to M1.9 (or a new sub-milestone) as a MUST.

3. **`EnvFlags::PREV_SNAPSHOT`** (milli `Index::rollback`) — not mentioned. It
   reopens the env on the *older* meta page, i.e. it is a direct consumer of the
   M1.2 meta double-buffer. ZeroDB must expose "open previous snapshot."

4. **`EnvFlags::WRITE_MAP`** (experimental writemap) — PLAN 1.10 mentions
   durability flags generically but not WRITE_MAP. It changes how writes hit
   storage (writable mmap) and interacts with `put_reserved` and the dirty-page
   borrow model; needs explicit handling even though ZeroDB's I/O layer differs.

5. **`Env::try_clone_inner_file()`** (S3 raw snapshot streaming) — not mentioned.
   Consumers `dup()` the underlying data-file fd and stream it while holding a
   write-lock txn. Requires the on-disk DB to be a single regular file that is
   consistent under a held write txn. Covered best by M1.2/M1.12.

6. **`static_read_txn` → `RoTxn<'static, WithoutTls>`** — a Send, env-owning
   read txn handed to async handlers. Depends on `Env: Clone`. Tie to M1.8/1.13.

7. **In-place cursor mutation with codec swap** — `put_current_with_options`
   (rewrite at cursor with a *different* data codec) and `del_current` are used
   by all of milli/arroy/hannoy in `iter_mut`/`prefix_iter_mut` passes. PLAN
   1.3/1.4 cover cursors generically; call out these specific unsafe write-cursor
   ops (and the value-borrow rule they require) explicitly.

8. **Facet neighbor seeks** `get_greater_than` / `get_lower_than_or_equal_to`
   (milli facet-tree navigation) — beyond `set_range`; ensure M1.3 covers the
   `>` and `<=` seek variants.

9. **hannoy `madvise` prefetch + `READER_AVAILABLE_MEMORY` env var**
   (`src/reader.rs`) — hannoy issues `madvise(WILLNEED)` over raw `Bytes` views
   of the mmap and gates it on an env var. Not a Phase-1 MUST (best-effort, maps
   to Phase 3.7 access hints), but the raw-`Bytes` zero-copy mmap access it
   relies on is M1.3. Design 3.7 to replace this hack.

10. **`cellulite` — RESOLVED (inventoried, SHA pinned).** milli re-exports and
    uses `cellulite` (a geo/spatial index; `NUMBER_OF_DBS = 27 +
    Cellulite::nb_dbs()`), heed 0.22.1, pinned at **v0.3.2 /
    `b9c13c6d507155249033b9226bd549af4f285100`**. It is now part of the frozen
    contract, added to the pinned-SHA table and legend. Its full heed surface was
    inventoried: `create_database`/`open_database` (named, prefixed), `write_txn`
    (tests)/`read_txn`, `commit`, `get`, `put`, `delete`, `clear`, `len`,
    `is_empty`, `iter`/`iter_mut`, `prefix_iter`, `stat`, cursor `del_current` +
    plain `put_current`, `remap_*`, custom BE codecs (`ItemKeyCodec`,
    `CellKeyCodec` over H3 cell indexes, `MetadataKey`, `UpdateType`,
    `ZerometryCodec`, `RoaringBitmapCodec`), and `heed::Error` via `#[from]`.
    **Every one of these was already an existing MUST row** (rows 10, 11, 13, 14,
    27, 30, 31, 33, 34, 36, 38, 39, 40, 43, 45, 49, 51, 52, 53, 54) — those rows
    are now tagged with `cellulite`. **cellulite adds ZERO new MUST rows and
    changes NO milestone scoping.** Crucially, despite being a spatial store,
    cellulite uses **no DUPSORT/DUPFIXED, no `DatabaseFlags` at all, no
    `INTEGER_KEY`, no custom comparator, and no nested txns** — it models cell
    membership as (H3-cell big-endian key → roaring bitmap) plain B-tree entries.
    This **reinforces** (does not flip) the M1.7 DUPSORT-descope recommendation in
    §B.1 and the single-comparator invariant in row 53: with all five consumers
    now inventoried, **not one uses LMDB duplicate sorting.**

### C. Constraints on the 0.5 heed-integration ADR

The concrete shapes below are load-bearing; the integration strategy (fork heed /
backend feature / adapter crate) must preserve all of them:

- **Concrete types, not generics.** Consumers name exact heed types and paths:
  `Env<WithoutTls>`, `RoTxn<'a, WithoutTls>`, `RoTxn<'static, WithoutTls>`,
  `RwTxn<'p>`, `Database<KC, DC>`, `WithoutTls`/`WithTls`, `EnvOpenOptions<T>`,
  `EnvFlags`, `PutFlags`, `DatabaseFlags`, `CompactionOption`, `EnvClosingEvent`,
  `MdbError`, `heed::Error`, `ReservedSpace`, `LazyDecode`, `DecodeIgnore`,
  `BytesEncode`/`BytesDecode`/`BoxedError`. The backend must expose these exact
  type paths (heed is **not** generic over a backend today).

- **`Env: Clone` with shared ownership.** `Index` stores `env.clone()`, and
  `env.clone().static_read_txn()` yields an env-owning `'static` read txn.
  Requires refcounted/`Arc`-style Env handles.

- **Same-process env registry + `EnvAlreadyOpened`.** One `Env` per path per
  process; a second open of a still-open path must error `EnvAlreadyOpened`
  (milli handles it). The IndexMap LRU and `Index::rollback` (drop-then-reopen)
  depend on this exact semantics.

- **`EnvClosingEvent` / `prepare_for_closing()`.** Deferred close with a
  wait/wait_timeout signal fired when the last clone drops; the IndexMap waits on
  it before reopening a path on resize/delete.

- **Error taxonomy 1:1.** `heed::Error::{Io, Mdb(MdbError), Encoding, Decoding,
  EnvAlreadyOpened}` and the specific `MdbError` variants observed
  (`MapFull`, `Invalid`, `BadValSize`, `KeyExist`) must exist and be produced
  under the same conditions (map full, bad file, oversized/empty key, APPEND
  misuse). `Encoding`/`Decoding` are also constructed by consumer code, so those
  variants must be publicly constructible with a `BoxedError`.

- **TLS model.** `WithoutTls` is universal and makes `RoTxn: Send`; the WithTls
  path can be a thin shim. Read-txn Send-ness is required for rayon/async.

- **`nested_read_txn` on both `RwTxn` and `Env`.** Must exist with the fork's
  read-child-of-write-txn semantics (Finding §A) — this is the single hardest
  semantic the adapter must guarantee, and it constrains the 0.4 dirty-page
  value-borrow contract and the 1.8 concurrency model.

- **`put_reserved` + `WRITE_MAP` interaction** constrains where value bytes live
  (map vs heap) and therefore the dirty-page borrow model in SPEC 04.

---

### Row counts

- Main contract table (**MUST**): **61 rows**. (cellulite was added as a fifth
  consumer and tags **20 existing rows**; it introduced **0 new rows** — every
  heed item it uses was already a MUST.)
- Second table: **12 SHOULD**, **9 WON'T** (21 rows total; a few rows carry a
  "WON'T Phase 1 / SHOULD Phase 2" dual note, counted by their Phase-1 verdict).
  Unchanged by cellulite — it uses no DUPSORT/DatabaseFlags/comparator, so no
  SHOULD/WON'T reclassification was triggered.
</content>
</invoke>
