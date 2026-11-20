# Brief for an independent review of the `unsafe` code

This page is for someone outside the project who reviews ZeroDB's `unsafe`
Rust. It lists every `unsafe` site in the engine crates, the invariant its
comment states, what tools already check it, what they found, and where to
start. It is not itself a review: the code and this brief were written by the
same process (see [PROVENANCE.md](../PROVENANCE.md)). Record the review's
scope, date, reviewer and findings in PROVENANCE.md when it is done.

## Background

ZeroDB is an LMDB-style engine: one writer, many lock-free readers, and pages
read straight out of a memory map of the data file. Read
[ARCHITECTURE.md](../ARCHITECTURE.md) first, then the `unsafe` policy in
[CONTRIBUTING.md](../../CONTRIBUTING.md#unsafe-policy), which lists the only
places `unsafe` is allowed. The decisions that shape the `unsafe` code:

- **ADR-0021**, `WRITE_MAP` in place: under `EnvFlags::WRITE_MAP` the writer
  edits dirty pages directly in a writable shared map, through `&mut [u8]`
  regions minted by an `unsafe fn` broker.
- **ADR-0023**, multiple processes: a shared lock file holds the reader table
  and the writer lock, reached through atomics over a `MAP_SHARED` mapping,
  the Linux `futex` and `fcntl` record locks.
- **ADR-0034**, trusted pages by default: as in LMDB, page cells are read
  without bounds checks unless the caller opts into `FileTrust::VALIDATE`.
  Opening an environment is therefore an `unsafe fn` whose contract is that
  only ZeroDB writes the file.

The rules the `SAFETY` comments cite (TXN-6, TXN-62, LCK-4 and so on) are in
[docs/SPEC/](../SPEC/), mostly SPEC 04.

## What tools already check

| Tool | What it runs | Where |
|---|---|---|
| miri (Stacked Borrows, data-race detector) | `zerodb-core`'s tests, including the `WRITE_MAP` in-place path over a heap-backed map (`zerodb_io::testmap::TestWriteMap`), with a reader thread running while the writer stores in place | every PR, `just miri` |
| miri | `zerodb-io`'s unit tests: the lock-file atomic views over an anonymous map, the grow-only map cell over heap mappings, the fault-injection backend. 25 tests that open real files are skipped under miri | every PR, `just miri` |
| miri | `heed-zerodb`'s unit tests over `TestWriteMap`, in both the in-place and heap-frame modes: the TLS-marker retags, the `Send` read transaction (including reads during commits), `ReservedSpace`, the lifetime-erased write cursor and the write iterator's `unsafe fn`s | every PR, `just miri` |
| loom | the reader table, nested read transactions, the stamp cache, the handle memo, the in-process writer lock and the Linux cross-process writer word | every PR, `just loom` |
| ThreadSanitizer, AddressSanitizer | the tests of `zerodb-io` (with real files, `fcntl`, the futex and child processes), `zerodb` and `heed-zerodb`, on x86-64 and aarch64 Linux | nightly, `just sanitize thread` / `just sanitize address` |

What the tools cannot reach:

- **Real mappings under miri.** Miri cannot see an OS mapping of a file, so
  the real `Mmap`, `MmapWritable`, `pwritev`, `pwrite` and the lock file's
  `open` run only natively and under the sanitizers. `TestWriteMap` stands
  in for the map, in both write modes; it imitates `pwrite` with a pointer
  store.
- **Other processes.** ThreadSanitizer checks the threads of one process. It
  does not see synchronization through the futex or the shared mapping
  between processes, so it says nothing about the cross-process protocol.
  The loom model of the writer word covers that protocol's logic, not the
  kernel calls.
- **The macOS writer lock** (`fcntl` on byte 1) has no model check.
- **The aarch64 CRC32C instructions** are skipped under miri
  (`#[cfg(not(miri))]`); natively, a test compares them with the table path.

## Findings so far

### F1. Reader slices of the map spanned pages the writer rewrites in place (fixed)

Under `WRITE_MAP`, the writer writes free pages in place through `&mut [u8]`
regions (`dirty::map_mut`). A reader in another thread of the same process
held `&[u8]` slices that covered those pages, although it never read them:

- `btree::Source` carried the whole map as one `&[u8]`, copied into every
  page lookup;
- `Source::bytes_from_classified` returned a slice from the page it was asked
  for up to the reader's snapshot high-water, not one page;
- `RoTxn::map_bytes` lent the whole map, and the raw `Env::copy` passed
  `&map[..]` over free pages to `write_all_at`.

TXN-62 keeps the bytes a reader actually reads disjoint from the bytes the
writer writes. But a live `&[u8]` asserts that none of the bytes it covers
change, and miri reported it under both aliasing models (a protected
`SharedReadOnly` popped under Stacked Borrows, a data race on a retag under
Tree Borrows).

[ADR-0021 amendment 1](../adr/0021-writemap-in-place.md) fixed it, in every
mode. The engine holds the map as `zerodb_core::page::MapPages`, a pointer
and a length built from the mapping's raw pointer (`memmap2::MmapRaw`), and
every borrow covers exactly the page, overflow run or value it reads. The
raw copy hands the kernel the map's address (`zerodb_io::write_map_range`,
a `pwrite`) instead of a borrow. The reproducer,
`heed-zerodb/src/miri_tests.rs`
`without_tls_read_txn_reads_on_another_thread_during_commits`, now runs under
miri, in both modes, and `zerodb-core/tests/writemap_in_place_miri.rs`
`reader_thread_during_in_place_commits_under_miri` checks the same in the
core.

Residual, recorded in the amendment: a corrupt file opened with
`FileTrust::VALIDATE` can still steer a reader to a free page below the
high-water, whose bytes an in-process `WRITE_MAP` writer may be storing to.
That is a race on bytes actually read; LMDB, which does not validate pages,
has the same exposure. Writes by the kernel (`pwrite`) and by other
processes remain outside the Rust memory model; with bounded borrows no
reference covers the bytes they change either.

### F2. `TestWriteMap` could not model the heap-frame mode (fixed with F1)

Without `WRITE_MAP`, the writer kept a cached whole-map view and committed
with `pwrite`. `TestWriteMap` imitates that `pwrite` with a pointer write
into the same buffer, which miri reported as a write under a protected
shared borrow while the view existed. With page-bounded borrows (F1) no view
covers the written pages, so the heed miri tests run both modes.

### F3. The write iterator's `unsafe fn` contract was narrower than heed's (fixed)

The `Safety` section of `RwIter::del_current`, `put_current` and the other
cursor writes said only that no borrow of the *current* entry may be live
across the call. heed's contract, which callers follow, covers any value read
from the database, since an update can rewrite the page an earlier entry is
on. The documentation (`heed-zerodb/src/iterator.rs`) and SPEC 01 §S3 now
state heed's contract.

## Where to start

In order of risk:

1. **The map aliasing model** (F1, fixed): `zerodb-io/src/mmap.rs` (module
   contract, rule 5, `Mmap::pages`, `MmapWritable::pages`),
   `zerodb-core/src/page/map.rs` (`MapPages`), `btree::Source` and
   `map_pages`, and the remaining whole-map borrows (`MapPages::whole`: the
   fault backend's seal and capture, the tools, test hooks). Check that no whole-map borrow runs in a thread while a
   writer of the same process stores.
2. **The `WRITE_MAP` broker**: `zerodb-core/src/dirty.rs` `map_mut` and its
   five callers (six call sites), `Backing::map_dirty_page`, `MmapWritable::slice_mut` and
   `write_at`, `zerodb-io/src/lib.rs` `write_at_page` / `map_dirty_page`.
   Check that TXN-62 is enforced at allocation (`RwTxn::allocate`'s release
   guard) and that no borrow of the map covers a brokered region.
3. **Trusted pages** (ADR-0034): `page/raw.rs` unchecked reads,
   `page/tree.rs` view accessors and `page/trust.rs`. Check that a
   `FileTrust::VALIDATE` environment never reaches an unchecked read before
   the page has been validated, including through the validation memo
   (`ValidatedPages`, "kind-tagged memo hit").
4. **The heed adapter's lifetime erasure**: `heed-zerodb/src/iterator.rs`
   `RwGuts::new` (a `&mut` re-created from a raw pointer with a stretched
   lifetime) and `RwGuts::step` (yielded borrows stretched to `'txn`).
5. **The lock file**: `zerodb-io/src/lockfile.rs` `LockMap` (atomic views
   and the `&[ReaderSlot]` cast), `ReaderSlot`'s layout
   (`zerodb-core/src/shared.rs`), and the `futex` and `fcntl` calls.
6. **`Send`/`Sync` claims**: `MapRegions`, `RoTxn<'_, WithoutTls>`, and the
   auto `Send` of `zerodb::RwTxn`, which rests on the writer lock being
   releasable from any thread (loom model `loom_wl2`).

## Inventory

Every `unsafe` site in the `src/` directories of the engine crates and
`zerodb-tools`, at the commit that adds this page. Line numbers drift; the
function names do not. "Coverage" names the tools that execute the site. The
oracle crate's FFI to LMDB (39 blocks) and the `open` calls in integration
tests, benches and examples are out of scope.

### zerodb-core (24 blocks, 7 `unsafe fn`, 2 `unsafe impl`)

| Site | Kind | Stated invariant | Coverage |
|---|---|---|---|
| `dirty.rs` `map_mut` | `unsafe fn` + block | The brokered contract: single writer (TXN-6); target page referenced by no snapshot (TXN-62); one live `&mut` per region, tied to `&mut DirtyStore`, never stored; no borrow of the map covers the region | miri (in place, with a reader thread) |
| `page/map.rs` `MapPages::from_raw_parts` | `unsafe fn` + block | The pointer is valid for reads of `len` bytes for `'a`; stores keep to the map contract | miri (through `TestWriteMap`) |
| `page/map.rs` `MapPages::get`, `whole` | block ×2 | The range is inside the region; `get`'s callers ask only for pages a pinned snapshot references or the writer's own frames; `whole` only where no store can run concurrently | miri |
| `page/map.rs` `MapPages` `Send`, `Sync` | `unsafe impl` ×2 | Lends only `&'a [u8]`, as the `&'a [u8]` it replaces | miri (reader thread) |
| `dirty.rs` `map_mut` callers (`unspill_in_place`, `bytes_mut` ×2, `insert_tree_frame`, `insert_copy`, `insert_run_frame`) | block ×6 | `&mut self` re-ties the borrow; the region is a tracked `Slot::Map`; `insert_copy`'s source never aliases the target | miri |
| `env/backing.rs` `Backing::map_dirty_page` | `unsafe fn` (declaration, default `None`) | Same brokered contract, stated for implementers | n/a |
| `page/raw.rs` `read_u16/u32/u64_unchecked` | `unsafe fn` ×3 + block ×3 | Caller guarantees `off + N <= buf.len()`; `read_unaligned` | miri |
| `page/tree.rs` `LeafRef::cell_abs`, `node_flags`, `key`, `value`; `BranchRef::cell_abs`, `child_pgno`, `key`; `leaf_lookup`'s `mid_key` | block ×8 | The view contract: the page passed validation, or inherits that proof (memo hit, engine-authored dirty frame, or `trust_contents`) | miri |
| `page/trust.rs` `FileTrust::trust_contents` | `unsafe fn` | The caller guarantees only ZeroDB wrote the file | contract only |
| `page/crc32c.rs` `crc_update` and two tests | block ×3 | The `crc` CPU feature was detected at run time | native tests (aarch64); not miri |

### zerodb-io (27 blocks, 5 `unsafe fn`, 3 `unsafe impl`)

| Site | Kind | Stated invariant | Coverage |
|---|---|---|---|
| `mmap.rs` `Mmap::pages`, `MmapWritable::pages` | block ×2 | Module contract 1–5: the file outlives the map; nothing faults past EOF; one writer writing only unshared pages; no live borrow is rewritten; borrows cover only the pages read. The mappings are `MmapRaw`, created without `unsafe` | sanitizers; mirrored by `TestWriteMap` under miri |
| `mmap.rs` `MmapWritable::write_at` | `unsafe fn` + block | Single writer; no live view read at the target; pointer from the map root | sanitizers; mirrored by `TestWriteMap` under miri |
| `mmap.rs` `MmapWritable::slice_mut` | `unsafe fn` + block | Exclusivity of the region is the caller's (the broker contract) | sanitizers; mirrored under miri |
| `mmap.rs` `MapRegions` `Send`, `Sync` | `unsafe impl` ×2 | Raw pointers own `Send + Sync` boxes; `owned` mutated under its mutex; nothing freed before `Drop` | miri, TSan |
| `mmap.rs` `MapRegions::view` (×2), `publish`, `Drop` (×2) | block ×5 | Views and mappings come from `Box::into_raw`, are published with Release, and are freed exactly once in `Drop` | miri (heap mappings, 3 threads), TSan |
| `lib.rs` `WriteMapBacking::write_at_page` | block | Single writer, a page no snapshot references or a meta slot | sanitizers |
| `lib.rs` `WriteMapBacking::map_dirty_page` | `unsafe fn` + block | Forwards the broker contract to `slice_mut`; bounds checked | sanitizers |
| `file.rs` `write_pages_vectored` (`pwritev`) | block | iovecs point at borrows live for the call; the kernel only reads them | sanitizers (ASan checks iovec reads) |
| `file.rs` `write_map_range` (`pwrite`) | block | The range is inside the mapping, which stays mapped while borrowed; the kernel only reads it; no reference is created | sanitizers; copy tests |
| `lockfile.rs` `LockMap::u64_at`, `u32_at` | block ×2 | Map page-aligned, offset aligned and in bounds (constant offsets, `debug_assert`); every access to those bytes in every process is atomic | miri (anonymous map), sanitizers |
| `lockfile.rs` `LockMap::slots` | block | Alignment, length, validity (every field atomic), lifetime, aliasing: listed in the comment | miri, sanitizers |
| `lockfile.rs` `record_lock`, `probe_locked` (`fcntl`) | block ×2 | Open descriptor; `flock` on the stack | sanitizers |
| `lockfile.rs` `one_byte_flock` (`mem::zeroed`) | block | `flock` is plain integers | sanitizers |
| `lockfile.rs` `futex_wait`, `futex_wake_all` (Linux) | block ×2 | The word is a live, 4-aligned `u32` in the shared mapping; the kernel only reads the arguments | sanitizers (Linux); the word protocol is loom-checked in `zerodb_core::writer_word` |
| `fault.rs` `FaultBacking::map_dirty_page` and two tests | `unsafe fn` + block ×3 | Pure forward of the broker contract | miri |
| `testmap.rs` `TestWriteMap` `Sync`, `root_pages`, `write_at_page`, `map_dirty_page` | `unsafe impl` + `unsafe fn` + block ×3 | The test stand-in for the writable map; the same contract as the real one | miri (it is the instrument) |

### zerodb (3 blocks, 1 `unsafe fn`)

| Site | Kind | Stated invariant | Coverage |
|---|---|---|---|
| `lib.rs` `EnvOpenOptions::open` | `unsafe fn` | heed's contract: only ZeroDB writes the file (ADR-0034) | contract only |
| `lib.rs` `open`'s default `FileTrust::trust_contents()` | block | The caller upholds `open`'s contract | contract only |
| `copy_tests.rs` | block ×2 | Test opens of the test's own files | n/a |

### heed-zerodb (12 blocks, 9 `unsafe fn`, 1 `unsafe impl`)

| Site | Kind | Stated invariant | Coverage |
|---|---|---|---|
| `txn.rs` `unsafe impl Send for RoTxn<'_, WithoutTls>` | `unsafe impl` | Every `InnerTxn` variant is `Send` by the engine's own guarantees; the impl keeps `WithTls`/`AnyTls` `!Send` | miri (cross-thread read), TSan |
| `txn.rs` the two `Deref` impls | block ×2 | `RoTxn<T>` is `repr(transparent)` over `InnerTxn`; `T` only appears in a ZST | miri |
| `iterator.rs` `RwGuts::new` | block | The pointee came from a live `&'txn mut`; lifetime-independent layout | miri |
| `iterator.rs` `RwGuts::step` | block | Yielded bytes are stable until the next mutation through the iterator, which the public `unsafe fn`s forbid callers to hold borrows across (F3) | miri |
| `iterator.rs` `del_current`, `put_current`, `put_current_with_options`, `put_current_reserved_with_flags` | `unsafe fn` ×4 | No key or value the iterator yielded may be live across the call (F3) | miri |
| `reserved_space.rs` `as_uninit_mut` | block | `MaybeUninit<u8>` has `u8`'s layout; the slot is initialized memory | miri |
| `reserved_space.rs` `assume_written` | `unsafe fn` | The caller wrote those bytes (always initialized here) | miri |
| `env.rs` `os_page_size` (`sysconf`) | block | A pure query | native |
| `env.rs` `EnvOpenOptions::open` | `unsafe fn` + block | Forwards the engine `open` contract | contract only |
| `env.rs` `EnvOpenOptions::flags`, `Env::resize`, `database.rs` `Database::remove` | `unsafe fn` ×3 | heed signature parity; no `unsafe` operation in the body (`NO_LOCK` puts multi-process exclusion on the caller) | n/a |
| `env.rs` tests; `miri_tests.rs` | block ×5 | Test opens and calls of the `unsafe fn`s above | n/a |

### zerodb-tools (3 blocks)

`commands.rs`, `common.rs`: opens with `FileTrust::VALIDATE`. `migrate.rs`
(`migrate-lmdb` feature): heed's `open` of a C LMDB environment, awaiting
maintainer review per CONTRIBUTING.md.

## Running the tools

```sh
just miri                    # nightly; the core dominates (about 50 minutes on an M-series Mac)
just loom
just sanitize thread         # nightly, Linux or macOS
just sanitize address
```
