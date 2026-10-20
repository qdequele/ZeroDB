# `zerodb-tools` — env utilities

Utilities for a zerodb env directory. They work on a **live** environment, as
LMDB's `mdb_stat` and `mdb_dump` do (ADR-0023 amendment 18):

- `stat`, `dump` and `check` open the env read-only through the engine, with
  its lock file, and so attach to an env that other processes have open. They
  take a reader slot and read one consistent snapshot while writers keep
  committing; a long `dump` or `check` pins that snapshot, so the file can
  grow while it runs, as with `mdb_dump` holding a read txn. The first tool to
  open a closed env creates its lock file, as any opener does.
- `load` and `migrate-from-lmdb` create a fresh env. They refuse a destination
  that holds data, or that another process has open, and keep the env open for
  the whole run. A process that opens the env after that check joins it, as
  with `mdb_load`: a reader sees each commit, a writer interleaves its txns.

The data file is `zerodb.dat` in a natively-created env and `data.mdb` in one
created through the `heed-zerodb` adapter (ADR-0010; the file-name entry in
`DIVERGENCES.md`). The read tools
probe **both** names and use the lock file named after the one found
(`zerodb.lock` or `lock.mdb`), the same one the processes using the env open;
a directory holding both data files is a hard error rather than a silent
pick, and `stat` prints an engine-identification line read from the file's own
`ZDB1` magic. A tool opens with the `max_readers` its env's lock file
records, whether a process has the env open or an earlier run left the lock
file finished. It falls back to the default (126) only when the lock file
records none: no lock file yet, or one left with the initializing magic by an
opener that never finished. A process that joins the env while a tool runs
adopts that count whatever it asks for, as LMDB joiners do. On read-only
media, where no lock file can be created, the tools open the env read-only
without one, as LMDB's tools do.

```
zerodb-tools <SUBCOMMAND> [ARGS]
zerodb-tools --version          # crate version + on-disk format version
zerodb-tools --help
```

**Installing.** `cargo install zerodb-tools` from crates.io, or download a
prebuilt tarball from the [releases page](https://github.com/qdequele/ZeroDB/releases)
(`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` and
`aarch64-apple-darwin`, each with a SHA-256 sum). From a checkout:
`cargo install --path crates/zerodb-tools`. Add `--features migrate-lmdb` for
`migrate-from-lmdb`, which links the C LMDB fork and is therefore not in the
prebuilt binaries.

**Exit codes.** `0` success; `1` = `check` found violations; `2` = usage error
or any other failure (unknown subcommand or option, unreadable env, …). Unknown
`--options` are rejected rather than treated as paths.

- **`stat <env-dir>`** — environment and per-database statistics: page size,
  map size, `real_disk_size`, `non_free_pages_size`, free-page count,
  high-water, and per-DB depth / branch·leaf·overflow page counts / entry
  counts (main DB first, then each named DB).

- **`dump <env-dir> [--out FILE]`** — a **logical** dump of every database
  (main + named) in an `mdb_dump`-shaped text format: `VERSION=3`, then one
  block per DB (`format=bytevalue`, `database=<hex-name>` for named DBs,
  `type=btree`, `HEADER=END`, space-prefixed hex `key`/`value` lines,
  `DATA=END`). The dump is deterministic and *logical* — it omits physical
  geometry (map size, page size), so dumps compare byte-identically across
  engines and page sizes. Writes to stdout unless `--out` is given.

- **`load <dump-file> <env-dir> [--page-size N] [--map-size BYTES]`** — rebuild
  a **fresh** env from a dump through ordinary write txns with sequential
  writes (ADR-0015), as `mdb_load` does, committing every 64 MiB of records;
  the map doubles whenever it fills, so `--map-size` is a starting size. The
  dump is read a line at a time, twice: a first pass rejects a malformed dump
  (or a key longer than 511 bytes) before the destination is created, the
  second writes the databases in dump order. A dump read from a pipe
  (`load /dev/stdin <env-dir>`) is copied during the first pass to an
  unlinked spool file in `<env-dir>`, so that file system needs room for the
  dump too. Memory stays at about one 64 MiB batch plus its write txn's dirty
  pages, whatever the dump's size. In a hand-edited dump, records out of
  order still land in key order, and of several records with the same key in
  a database the first is kept. The produced env is checked through the map
  before the command returns.
  Refuses a target that holds data under either data-file name, or that
  another process has open.

- **`check <env-dir>`** — run the invariant walker (SPEC 03 §11 / SPEC 05 §9)
  and report; exits non-zero on any violation. Attached, it walks the
  snapshot its read txn pins (that snapshot's roots, free tree and `last_pg`,
  through the map) and never reads a page that snapshot lists as free. If the
  env does not open as a valid one (which is exactly when `check` matters), it
  falls back to the raw data file, read whole into memory, but only while it
  holds the env's lock file exclusively, so no process can open the env
  during the walk; if another process has the env open it refuses and says
  so. On read-only media, where no lock file can exist, the raw walk runs and
  its report says it had no liveness guarantee.

- **`migrate-from-lmdb <src-lmdb-dir> <dst-env-dir> [--page-size N] [--map-size BYTES]`**
  — open a real LMDB env read-only and stream every DB (main + named sub-DBs)
  into a fresh zerodb env in batched write txns, verifying per-DB entry
  counts. **Requires** building with the `migrate-lmdb` feature (the one place
  a tool links C LMDB, via heed):
  `cargo run -p zerodb-tools --features migrate-lmdb -- migrate-from-lmdb …`.

Round-trip: `LMDB → migrate-from-lmdb → zerodb → dump` yields a dump
byte-identical to LMDB's own content rendered in the same format.

The engine also provides a compacting/raw snapshot copy (heed's `copy_to_file`
parity): `zerodb::CopyToFile::copy_to_file(path, CompactionOption::{Enabled,Disabled})`.
`Enabled` **streams** a fresh compact env (no free pages, O(tree depth × page
size) peak memory, a writer thread overlapping the tree walk as LMDB's copy
does, atomic-rename destination); `Disabled` copies the snapshot's pages
verbatim from the map, freelist preserved. Both open their own internal read
snapshot and are safe on a live env. See ADR-0009 (copy and tools design) and
the compaction-memory entry in `PERF-GAP-VS-LMDB.md`.
