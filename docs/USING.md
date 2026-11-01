# Using ZeroDB

There are two ways to use ZeroDB:

- **The native API, crate `zerodb`.** It is heed-shaped and works on byte
  slices. Use it for new code.
- **The heed drop-in.** A consumer that already uses `heed` 0.22 switches
  engines with one `[patch]` line and no code changes. Meilisearch and hannoy
  run this way.

The defaults match LMDB. Every option below that trades safety for speed is
opt-in on purpose, and each section states what it costs next to what it gains.

## Getting started

### Native API

```toml
[dependencies]
zerodb = "0.2"
```

```rust
use zerodb::EnvOpenOptions;

let env = EnvOpenOptions::new()
    .map_size(1 << 30) // largest size the data file may grow to
    .max_dbs(8)
    .open("my-env")?;  // a directory; it must exist

let mut wtxn = env.write_txn()?;
let db = env.create_database(&mut wtxn, Some(&b"users"[..]))?;
db.put(&mut wtxn, b"alice", b"admin")?;
wtxn.commit()?;

let rtxn = env.read_txn()?;
assert_eq!(db.get(&rtxn, b"alice")?, Some(&b"admin"[..]));
```

A runnable version is in
[`crates/zerodb/examples/basic.rs`](../crates/zerodb/examples/basic.rs):
`cargo run -p zerodb --example basic`.

### As a heed replacement

```toml
[patch.crates-io]
heed = { git = "https://github.com/qdequele/ZeroDB", tag = "v0.2.0" }
```

The patch target is `crates/heed-shim`, a crate named `heed` that re-exports
`heed-zerodb`. For Meilisearch v1.55 and later, which use Meilisearch's heed fork
with unique transactions, patch to `crates/heed-shim-unique-txns` instead.

Two things change underneath:

- **The data file is not an LMDB file.** Migrate existing environments with
  `zerodb-tools migrate-from-lmdb`, or with `mdb_dump` followed by
  `zerodb-tools load` ([TOOLS.md](TOOLS.md)).
- **Some LMDB features are not supported**, such as `DUP_SORT`, nested write
  transactions and `NO_SUB_DIR`. The full list is in
  [LMDB-COMPATIBILITY.md](LMDB-COMPATIBILITY.md).

## Durability

By default every commit is durable when `commit()` returns, as in LMDB: the
data pages are flushed, then the meta page.

| Option | A commit pays | A crash can lose |
|---|---|---|
| default | two flushes (data, then meta) | nothing committed |
| `NO_META_SYNC` | one flush (data) | the last commit's meta update |
| `NO_SYNC` | no flush | every commit since the last explicit sync |
| `WRITE_MAP` with `MAP_ASYNC` | an asynchronous `msync` | as `NO_SYNC` |

With the default and `NO_META_SYNC`, a crash always leaves a consistent file:
the engine reopens on the newest meta page that is fully on disk. With
`NO_SYNC` or `MAP_ASYNC`, the file stays consistent only if the filesystem
writes data before the meta that points at it. An OS crash or power loss on a
filesystem that reorders writes can corrupt the file, as with LMDB (SPEC 06,
REC-11).

**Checkpoints.** A program that makes many small commits can open with
`NO_META_SYNC` or `NO_SYNC` and make the environment durable at points it
chooses, such as once per batch or per scheduler tick:

- `Env::force_sync()` is the heed call: `mdb_env_sync(env, 1)`, a real barrier.
- `Env::sync(force)` is the full `mdb_env_sync` (a ZeroDB extension). With
  `force = false` it does nothing on a `NO_SYNC` env, and issues an
  asynchronous `msync` under `MAP_ASYNC`.

## Performance options

### `NO_READ_AHEAD`: random reads bigger than memory

The map is advised `MADV_RANDOM`, so the kernel stops reading ahead around page
faults, exactly as LMDB does with `MDB_NORDAHEAD`. It matters when the data does
not fit in memory and reads are random. In one bigger-than-RAM test, an env
without the flag read about 10 GB from disk in 60 s for a 1.5 GB database,
because every fault pulled in a readahead window nobody needed
(2026-09-29 benchmark run).

The flag applies to the whole environment. If one environment holds both
random-access data (such as a vector graph) and data read in sequential scans,
check that the scans do not slow down. The flag does not replace explicit
prefetching (`MADV_WILLNEED` on pages the application knows it will read); the
two are complementary.

### `WRITE_MAP`: write dirty pages in the map

Dirty pages are written straight into a writable map, instead of being staged
on the heap and copied in at commit (ADR-0021). On YCSB with no-sync writes
(Graviton4, local NVMe, 10 M × 128-byte records), it makes ZeroDB 1.6–1.7×
faster than default LMDB, while still trailing LMDB's own `WRITE_MAP`.

**Cost:** a stray write through a dangling pointer anywhere in the process can
corrupt the database file directly. This is the same trade LMDB's
`MDB_WRITEMAP` makes.

### Trusted-file mode

`EnvOpenOptions::file_trust(FileTrust::trust_contents())` skips ZeroDB's
read-path page validation and trusts the mapped bytes, as LMDB does (ADR-0014).
Bounds checks stay, so reads never leave the map.

**Cost:** the constructor is `unsafe`. With it, a corrupt or hostile data file
causes undefined behavior instead of a typed error. Use it only where the
file's integrity is guaranteed some other way.

### Sequential writes

`EnvOpenOptions::sequential_writes(true)` sets an env-wide default, and
`Env::set_sequential_writes(&db, Some(bool))` overrides it per database
(ADR-0015). A put whose key sorts after the last key goes straight to the
rightmost leaf instead of descending from the root. Results and files are
identical either way.

**Cost:** ascending and `APPEND` loads get 14–24 % faster, but random-key puts
get 3–5 % slower. Turn it on for tables whose keys only grow (internal ids,
timestamps), and leave it off for random keys.

### Dirty memory bound

A write transaction keeps its modified pages in memory until it commits. Past
a limit (by default LMDB's 131,072 pages), it spills them to the file and keeps
going, as LMDB does. `EnvOpenOptions::max_dirty_bytes(bytes)` sets the limit in
bytes (ADR-0017). Lower it to bound the memory of very large transactions;
results and files never depend on it.

## Multiple processes

The lock file is on by default, so several processes can open one environment
at once, each with any number of readers and one writer at a time across all of
them (ADR-0023).

- `NO_LOCK` (heed), or `lock_file(false)` natively, creates no lock file. The
  environment is then single-process, and keeping other processes out is up to
  you, as with `MDB_NOLOCK`.
- `Env::clear_stale_readers()` frees reader slots that dead processes left
  behind, as `mdb_reader_check` does. `Env::reader_list()` shows who holds
  slots.
- If another process grows the map, a txn begin returns
  `MdbError::MapResized`. Call `resize(0)` to adopt the new size, then begin
  again.
- Do not use an `Env` in a child after `fork()`, and do not share an
  environment across PID namespaces.

## Build settings

Cargo applies `[profile]` settings only from the final binary's workspace, so
build tuning belongs to the consumer, not to ZeroDB.

- **`codegen-units = 1` is ZeroDB's worst case relative to LMDB**, not its best:
  it measured slower than the default 16 on most read benchmarks. ZeroDB marks
  its hot read-path helpers `#[inline(always)]`, so a `codegen-units = 1` build
  still inlines them without any flag. That change improved 37 benchmarks at
  `codegen-units = 1` and 31 at 16, with no regressions.
- **Fat LTO** also speeds up heed's own wrapper around LMDB, so it does not
  change the comparison between the two engines.
- **`target-cpu`** (`neoverse-v1` on Graviton3, `neoverse-v2` on Graviton4) and
  **PGO/BOLT** have not been measured yet. To measure them, build the
  benchmark ladder (`just bench`) three ways on one commit: the LMDB control,
  ZeroDB's default build, and ZeroDB with the flag. Compare the flag build with
  ZeroDB's own default build, not only with LMDB: LMDB's C core never sees
  `RUSTFLAGS`, so a win over LMDB may just be a win any Rust code gets on that
  CPU.

Each benchmark report in [`benches/results/`](../benches/results/README.md)
records the build setting its run used.

## Not available yet

| Feature | Use case | Tracking |
|---|---|---|
| Bounded relaxed durability | A crash loses at most a configured window of commits, never consistency. It would replace the `NO_SYNC` plus checkpoint pattern | [#35](https://github.com/qdequele/ZeroDB/issues/35) |

## See also

- [LMDB-COMPATIBILITY.md](LMDB-COMPATIBILITY.md): every heed item and LMDB
  feature, with its status.
- [TOOLS.md](TOOLS.md): `zerodb-tools` (`stat`, `check`, `dump`, `load`,
  `copy`, `migrate-from-lmdb`).
- [PERFORMANCE.md](PERFORMANCE.md): how ZeroDB compares with LMDB, and why.
- [ARCHITECTURE.md](ARCHITECTURE.md): how the engine works.
