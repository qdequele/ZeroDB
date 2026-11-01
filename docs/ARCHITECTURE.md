# Architecture

This page is a map of how ZeroDB works, for anyone about to read or change the
code. The [SPEC](SPEC/) is the exact reference for formats and algorithms, and
its rule IDs (such as `TXN-61` or `GC-18`) are cited throughout the code. This
page links to it rather than repeating it.

ZeroDB follows LMDB's design: one memory-mapped file, copy-on-write B+trees,
a single writer, lock-free readers, and two meta pages that make each commit
atomic. Its own additions are page validation, a meta-page checksum, and a
test harness that checks every behavior against LMDB itself.

## Crates

```
                    heed-shim, heed-shim-unique-txns   (crates named `heed`, patch targets)
                                 │
                            heed-zerodb                (the heed 0.22.1 API)
                                 │
    zerodb-tools ──────────▶   zerodb                  (native API; joins engine and I/O)
                              ╱       ╲
                    zerodb-core        zerodb-io
         (pages, B+trees, txns,        (mmap, pwrite and fsync,
          GC, reader table; no I/O)     lock file, fault injection)

    zerodb-oracle: differential tests against C LMDB, crash harness, benchmarks
    fuzz/:         differential and decoder fuzz targets
```

- **`zerodb-core`** is the engine. It knows the page format, the trees and
  transactions, but performs no I/O itself: it reads pages through a `Backing`
  trait and writes through it at commit. This keeps most of the logic testable
  under miri and loom.
- **`zerodb-io`** implements `Backing` over a real file: a growable read-only
  map, `pwritev` writes and `fdatasync`, or a writable map under `WRITE_MAP`.
  It also owns the shared lock file and a fault-injecting backing used by the
  crash tests.
- **`zerodb`** opens an environment by wiring the two together, and adds env
  copies. Its API mirrors heed's, on byte slices.
- **`heed-zerodb`** reproduces heed 0.22.1's typed API (codecs, iterators,
  `ReservedSpace`, error types) over `zerodb`, so heed users switch engines
  with a `[patch]` line.

## The file

An environment is a directory holding one data file (`zerodb.dat` natively,
`data.mdb` through heed) and, by default, a lock file. The data file is an
array of fixed-size pages. The page size is a power of two from 4 KiB to
64 KiB, chosen at creation and independent of the OS page size
([SPEC 02](SPEC/02-pages.md)).

Every page starts with a 32-byte header: its own page number, the id of the
transaction that last wrote it, and its kind. The kinds are:

- **Meta pages (pages 0 and 1).** Each holds the root and statistics of two
  trees (the free-page tree and the main tree), the file's high-water page,
  the map size, the committing transaction's id, and a CRC32C over its
  contents. A commit writes the older of the two slots, so the previous
  commit's meta survives until the new one is complete.
- **Branch and leaf pages.** These use LMDB's layout: a sorted array of 2-byte
  offsets grows from the top of the page and the cells grow down from the
  bottom. Branch cells hold a key and a child page number; leaf cells hold a
  key and either the value or a pointer to an overflow run.
- **Overflow runs** of contiguous pages hold values too large for a leaf.

Keys are at most 511 bytes. All integers are little-endian and read at
explicit offsets, never by casting structs over the map.

## Trees

Every database is a B+tree ([SPEC 03](SPEC/03-btree.md)). The **main tree**
holds the unnamed database's entries and the catalog: one record per named
database, giving its root and statistics. The **free-page tree** maps the id
of a transaction to the pages it freed.

Trees are **copy-on-write**. A write transaction never modifies a page that a
committed snapshot can see. Instead it copies the page to a fresh page number,
changes the copy, and updates the parent to point at it, up to the root. Pages
the transaction already copied are changed in place. Splits, merges and
borrows follow LMDB's rules, including splitting at the end of the page for
ascending inserts, so trees built from the same inserts have LMDB's shape.

Readers trust nothing they read from the map. Each page is validated (bounds,
cell offsets, key and value sizes) before its contents are used, so a corrupt
file produces a typed error, never undefined behavior. A per-transaction memo
and an environment-wide cache keyed by (page number, writer txn id) make each
page cost one validation for as long as it is unchanged. The opt-in
trusted-file mode skips the per-cell checks, as LMDB does.

## Transactions

The concurrency model is LMDB's ([SPEC 04](SPEC/04-txn-mvcc.md)):

- **One writer at a time.** A write transaction holds an in-process mutex and,
  with a lock file, a cross-process lock. Its modified pages live in a dirty
  store, in memory, or in place in the map under `WRITE_MAP`. If the store
  grows past a limit, some of its pages are written to the file early
  (spilling), as LMDB does, so a huge transaction does not run out of memory.
- **Any number of readers, never blocked.** A read transaction pins the latest
  committed transaction id in a slot of the **reader table**, then reads that
  snapshot through the map. Claiming and releasing a slot are lock-free atomic
  operations. With a lock file, the reader table lives in shared memory, so
  readers in other processes are visible.
- **Snapshot isolation.** A reader sees exactly the commit it pinned, however
  many commits land after it. Values are borrowed straight from the map with
  no copy, and stay valid for the reader's lifetime.
- **Nested read transactions** (a feature of Meilisearch's LMDB fork) read a
  write transaction's uncommitted state. The writer is paused while they live,
  which the borrow checker enforces.

## Free pages

Pages freed by a commit cannot be reused while a reader may still see them
([SPEC 05](SPEC/05-gc.md)). Each commit records the pages it freed under its
transaction id: in its meta page when the list fits (the meta free-list annex),
otherwise in the free-page tree. A writer may reuse pages freed by transaction
`F` only if no live reader pinned a snapshot older than `F`. It finds that
bound by scanning the reader table for the oldest pin. Reused runs of
contiguous pages serve overflow values, as in LMDB.

## Commit

A commit runs a fixed sequence of steps, with a crash-injection hook between
them ([SPEC 04 §9](SPEC/04-txn-mvcc.md), rule `TXN-61`):

1. Write each changed named database's record back into the catalog.
2. Save the freed-page list (into the meta annex or the free-page tree).
3. Write the dirty pages to their page numbers (`pwritev`, or nothing more
   under `WRITE_MAP`).
4. `fdatasync` the data, unless `NO_SYNC`.
5. Write the meta page into the older slot, with its CRC.
6. `fdatasync` the meta, unless `NO_META_SYNC` or `NO_SYNC`.
7. Publish the new snapshot to readers and release the writer lock.

Step 3 only writes pages that the previous commit does not reference (fresh
pages, or freed pages no reader can see). So at every step the previous
commit stays intact on disk. On open, the engine picks the newer meta whose
CRC is valid; a torn or partial meta is rejected and the older one wins
([SPEC 06](SPEC/06-recovery.md)). There is no log to replay.

## The lock file

The lock file (ADR-0023) is a small shared-memory file next to the data file.
It holds the reader table, the published transaction id, and the
cross-process writer lock: a futex word on Linux, an `fcntl` record lock
elsewhere. Its format is ZeroDB's own, not LMDB's. The first process to open
an environment initializes it, and later processes join. `NO_LOCK` skips it
and keeps the environment single-process.

## Safety

`unsafe` is confined to a short list of places: mmap access and page reads,
the lock file's shared atomics and futex, the `WRITE_MAP` page broker, the
hardware CRC path, FFI in the oracle, and the few spots heed's pointer model
forces in `heed-zerodb`. Every `unsafe` block states its invariants in a
`// SAFETY:` comment. Every atomic justifies its memory ordering for a weak
memory model (ARM). The full policy is in
[CONTRIBUTING.md](../CONTRIBUTING.md#unsafe-policy).

## How behavior is verified

ZeroDB claims LMDB's behavior, so the reference is LMDB itself, linked through
heed 0.22.1 in `crates/zerodb-oracle` and nowhere else.

- **The differential oracle.** A test is a sequence of `Op`s (begin, put,
  get, iterate, delete, commit, nested read, …). The driver runs the same
  sequence against two engines and compares every result: return values,
  error codes, iteration order, and reads after each transaction. The engines
  are LMDB, native ZeroDB, and ZeroDB through `heed-zerodb`. Observation tests
  (`lmdb_*_observation.rs`) record what LMDB does in a corner case first; the
  ZeroDB test then has to match it, or the difference has to be listed in
  [LMDB-COMPATIBILITY.md](LMDB-COMPATIBILITY.md#behavior-differences).
- **Fuzzing.** `fuzz/diff_ops` generates random operation sequences for the
  oracle. `fuzz_page_decode` and `fuzz_image_open` feed hostile bytes to the
  page decoder and to open.
- **Crash testing.** The crash harness runs workloads over a fault-injecting
  backing that tears or drops writes at each commit hook, and also kills real
  processes with `SIGKILL`. After each crash, the reopened environment must
  hold exactly one committed prefix of the workload and pass the invariant
  checker.
- **Model checking and miri.** The reader table and nested transactions are
  model-checked with loom, and the I/O-free core runs under miri.
- **The checker.** `zerodb-tools check` walks every tree and the free-page
  accounting and reports violations by rule ID.

## Where to start reading

| To understand | Start at (module) |
|---|---|
| Opening an environment | `zerodb`'s `EnvOpenOptions::open`, then `zerodb_core::env` |
| A point read | `zerodb_core::rotxn`, `btree` and `page::tree` |
| A write and its commit | `zerodb_core::rwtxn` |
| The reader table | `zerodb_core::readers` and `shared`; `zerodb_io::lockfile` |
| The page format | `zerodb_core::page`, alongside [SPEC 02](SPEC/02-pages.md) |
| An oracle test | `zerodb_oracle`'s `op` and `driver` modules, then any file in `crates/zerodb-oracle/tests/` |

Design decisions and their reasons are recorded as ADRs, indexed in
[DECISIONS.md](DECISIONS.md).
