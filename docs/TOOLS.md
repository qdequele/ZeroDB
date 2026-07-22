# `zerodb-tools` — offline env utilities

Offline utilities for a zerodb env directory. **All tools operate offline**:
they take a best-effort exclusive `flock` on the env's data file and refuse if
the env may be live (D-001: zerodb has no cross-process reader protocol, so
pointing a tool at a running env would be unsafe — close it first).

The data file is `zerodb.dat` in a natively-created env and `data.mdb` in one
created through the `heed-zerodb` adapter (ADR-0010 / D-012). The read tools
probe **both** names; a directory holding both is a hard error rather than a
silent pick, and `stat` prints an engine-identification line read from the
file's own `ZDB1` magic.

```
zerodb-tools <SUBCOMMAND> [ARGS]
```

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
  a **fresh** env from a dump using the **streaming** bulk builder
  (bottom-up packed, O(tree depth × page size) build memory; the dump-text
  parse itself is still in-memory). Refuses a non-empty target.

- **`check <env-dir>`** — run the invariant walker (SPEC 03 §11 / SPEC 05 §9)
  over the data file and report; exits non-zero on any violation. Tolerant of
  a corrupt file (which is exactly when it matters).

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
size) peak memory, atomic-rename destination); `Disabled` copies the
snapshot's pages verbatim, freelist preserved. Both open their own internal
read snapshot and are safe on a live env. See ADR-0009 and
`PERF-GAP-VS-LMDB.md` C1.
