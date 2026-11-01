# zerodb-tools

Command-line utilities for ZeroDB environments, the counterparts of LMDB's
`mdb_stat`, `mdb_dump` and `mdb_load`, plus an invariant checker and an LMDB
migration tool.

```
zerodb-tools <SUBCOMMAND> [ARGS]
zerodb-tools --version          # crate version and on-disk format version
zerodb-tools --help
```

## Installing

- From crates.io: `cargo install zerodb-tools`.
- Prebuilt binaries for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`
  and `aarch64-apple-darwin`, each with a SHA-256 sum, are on the
  [releases page](https://github.com/qdequele/ZeroDB/releases).
- From a checkout: `cargo install --path crates/zerodb-tools`.

`migrate-from-lmdb` links the C LMDB library, so it is only built with
`--features migrate-lmdb` and is not in the prebuilt binaries.

## Commands

**`stat <env-dir>`** prints environment and per-database statistics: page
size, map size, disk size, used and free pages, and for each database its
depth, page counts by kind and entry count.

**`dump <env-dir> [--out FILE]`** writes a logical dump of every database in
`mdb_dump`'s text format (`VERSION=3`, one block per database, hex
`key`/`value` lines). The dump leaves out physical geometry such as page size,
so dumps compare byte for byte across engines and page sizes. It writes to
stdout unless `--out` is given.

**`load <dump-file> <env-dir> [--page-size N] [--map-size BYTES]`** builds a
fresh environment from a dump through ordinary write transactions, committing
every 64 MiB of records, as `mdb_load` does. The map doubles whenever it fills,
so `--map-size` is only a starting size. A first pass rejects a malformed dump,
or a key longer than 511 bytes, before the destination is created. A dump read
from a pipe (`load /dev/stdin <env-dir>`) is spooled to an unlinked file in
`<env-dir>`, so that filesystem needs room for it. Memory stays at about one
64 MiB batch, whatever the dump's size. The result is checked before the
command returns. `load` refuses a destination that already holds data or that
another process has open.

**`check <env-dir>`** runs the invariant checker (SPEC 03 §11, SPEC 05 §9) and
exits non-zero on any violation. On an environment that opens normally, it
walks the snapshot its read transaction pins. If the environment does not open
(which is when `check` matters most), it walks the raw data file instead, but
only while it holds the lock file exclusively, so no process can open the
environment during the walk.

**`migrate-from-lmdb <src-lmdb-dir> <dst-env-dir> [--page-size N] [--map-size BYTES]`**
opens an LMDB environment read-only and copies every database into a fresh
ZeroDB environment, checking each database's entry count. A dump of the result
is byte-identical to LMDB's own content in the same format.

Exit codes: `0` on success, `1` when `check` finds violations, `2` on a usage
error or any other failure.

## Live environments

The tools work on an environment that other processes have open, as LMDB's
tools do:

- `stat`, `dump` and `check` open it read-only, take a reader slot, and read
  one consistent snapshot while writers keep committing. A long `dump` or
  `check` pins that snapshot, so the file can grow meanwhile.
- `load` and `migrate-from-lmdb` refuse a destination that another process has
  open. A process that opens it after that check joins it, as with `mdb_load`.

A natively created environment stores its data in `zerodb.dat`; one created
through the heed adapter uses `data.mdb`. The tools look for both, and a
directory holding both is an error. They use the lock file that matches the
data file (`zerodb.lock` or `lock.mdb`), and the reader count it records. On
read-only media, where no lock file can exist, they open without one.

## Copies

Snapshot copies are an engine API rather than a tool:
`CopyToFile::copy_to_file(path, CompactionOption::Enabled)` streams a fresh,
compact environment with bounded memory and an atomic rename at the end;
`CompactionOption::Disabled` copies the snapshot's pages as they are. Both are
safe on a live environment.
