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
size, map size, disk size, used and free pages, the free-page records (how
many, from how many freeing transactions, and the longest one's page count),
and for each database its depth, page counts by kind and entry count.

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

**`upgrade <old-env-dir> <new-env-dir> [--page-size N] [--map-size BYTES]`**
copies an environment in on-disk format 2, the format of the 0.2.0 release,
into a fresh environment in the current format. The current engine refuses a
format 2 file at open, and this is the one command that moves it forward:

```sh
zerodb-tools upgrade /srv/app/env /srv/app/env.new
```

The new environment keeps the old one's data-file name (`data.mdb` stays
`data.mdb`, so heed opens it as before) and its page size unless
`--page-size` names another. `upgrade` refuses a destination that already
holds data or that another process has open, checks every database's entry
count against the source, and runs `check` on the result. It never writes the
old environment: once it succeeds, replace the old directory with the new one.

Close every process that has the old environment open first. A format 2
environment has no lock file, so `upgrade` cannot see another process, but it
detects a commit made while it was reading and fails rather than copy a mixed
state. If it fails after creating the destination, delete the destination
and run it again. `dump` also reads format 2 files, so a dump of an
environment made before or after the upgrade is the same.

Format 1 files were never released and are not read; a file of a version
newer than the tools' is refused. `zerodb-tools --version` prints the format
the tools write.

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
