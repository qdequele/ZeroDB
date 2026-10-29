# kv-compare

Compares ZeroDB with other embedded key-value stores — LMDB, RocksDB, fjall,
redb and SQLite — on the questions someone choosing a store asks. It is a
standalone crate, outside the ZeroDB workspace, so the other engines never
become ZeroDB dependencies ([ADR-0033](../../docs/adr/0033-kv-compare-bench.md)).

## What it measures

Each test measures one thing. Every test runs on two data sets with the same
record shape (16-byte keys, 100-byte values): one that fits in memory
(10 M records, about 1.2 GB) and one that does not (100 M records, about
12 GB, with each run limited to 8 GB of memory).

| Test | Question it answers | Phase |
|---|---|---|
| Read one key | How fast is a random lookup? | `get --threads 1` |
| Read 100 keys in order | How fast is a short scan from a random key? | `range --threads 1` |
| Write one key, saved to disk | How long does a durable commit take? | `write --batch 1 --durable` |
| Write 1,000 keys per commit | How fast are batched writes? | `write --batch 1000` |
| Load the data set | How long does an import take? | `load` (10,000 per commit) |
| Reads on more threads | Do reads scale across cores? | `get --threads 1…16` |
| Reads while one thread writes | Does a writer slow readers down? | `mixed --threads 1…16` |
| Disk space | How much disk does the data take? | size after `load` |
| Bytes written per byte of data | How hard is it on the SSD? | device writes during `write --batch 1000` |
| Memory | How much memory does it use? | peak resident memory |
| Inserts over time | Does throughput stay steady, or stall? | `grow` for 30 minutes, 100 per commit |

Each measurement starts from an empty OS page cache in a new process, reads
the data files once (as far as the memory limit allows), and runs 20 seconds
of the same work unmeasured before the 60 measured seconds, so it measures
warm caches rather than the first reads from disk. Reads pick keys uniformly
at random. Keys are inserted in random order. Values
are random bytes, so compression gains nothing. Each read or range read is its
own read transaction (or snapshot), the way a server answers independent
requests.

## Engine settings

Each engine uses its documented defaults except for two settings that must
match for a fair comparison:

- **Cache.** Engines with their own cache get 2 GiB (`--cache-mib`): fjall
  (block cache), RocksDB (LRU block cache), redb (page cache), SQLite
  (`cache_size`). SQLite's cache belongs to one connection and each thread
  has its own connection, so the 2 GiB is split evenly among the threads.
  LMDB and ZeroDB read through the OS page cache and have no cache setting.
- **Durability.** With `--durable`, each commit waits until the data is on
  disk: LMDB and ZeroDB default sync, fjall `PersistMode::SyncAll`, RocksDB
  `sync = true`, redb `Durability::Immediate`, SQLite `synchronous = FULL`.
  Without it, a commit does not wait for the disk, so a crash can lose the
  last commits: LMDB and ZeroDB
  `NO_SYNC`, fjall and RocksDB defaults, redb `Durability::None`, SQLite
  `synchronous = NORMAL`.

Other choices: LMDB and ZeroDB open with `NO_READ_AHEAD` (LMDB's advice for
random reads on data larger than memory). RocksDB gets a bloom filter and
background threads for every core, as its tuning guide recommends (fjall has a
bloom filter by default). SQLite uses WAL mode and a `WITHOUT ROWID` table.
`zerodb-trusted` is ZeroDB with page validation off
(`FileTrust::trust_contents`), the way LMDB reads pages.

## Running it

On a dedicated Linux host with a local SSD and passwordless `sudo`:

```bash
cargo build --release
DATA=/data/kvc nohup ./run.sh > run.log 2>&1 &
python3 report.py results/<run>/results.jsonl
```

`run.sh` runs every measurement in its own process and memory-limited cgroup,
after dropping the page cache. Its header lists the settings (data-set sizes,
memory cap, thread counts, test lengths). The full suite takes about 10 hours
with all seven engines. `report.py` writes `report.md` (tables) and
`charts/*.{light,dark}.svg`.

One phase by hand:

```bash
./target/release/kv-compare load --engine zerodb --dir /tmp/kv --records 1000000
./target/release/kv-compare get --engine zerodb --dir /tmp/kv --records 1000000 --threads 4 --seconds 30
```

Each invocation prints one JSON line (and appends it to `--out`): operation
counts and rates, p50/p99/p99.9 latencies, disk size, bytes written (by the
process, and to the device named by `KVC_DEVICE`), peak memory, and a
per-second timeline.

Building on macOS needs `libclang` for RocksDB's bindings:

```bash
LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib \
DYLD_FALLBACK_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib \
cargo build --release
```

macOS numbers are for trying the tool only: macOS `fsync` does not flush the
disk cache, and engines differ in whether they use `F_FULLFSYNC`, so durable
writes do not compare there.
