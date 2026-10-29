### What each store is

| | ZeroDB | LMDB | fjall | RocksDB | redb | SQLite |
|---|---|---|---|---|---|---|
| Design | B+ tree, memory-mapped | B+ tree, memory-mapped | LSM tree | LSM tree | Copy-on-write B-tree | B-tree |
| Written in | Rust | C | Rust | C++ | Rust | C |
| Writers at the same time | 1 | 1 | Many | Many | 1 | 1 |
| Read-write transactions | Yes | Yes | Optional mode | Optional mode | Yes | Yes |
| Several processes on one database | Yes | Yes | No | One writer, read-only others | One writer, read-only others | Yes |
| Largest key | 511 bytes | 511 bytes | 64 KiB | No practical limit | No practical limit | No practical limit |

### Speed of each basic operation

Each test does one thing, on a data set that fits in memory and one that does not.

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/op-read.dark.svg"><img alt="op read" src="benches/results/2026-10-27-kv-compare/charts/op-read.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/op-range.dark.svg"><img alt="op range" src="benches/results/2026-10-27-kv-compare/charts/op-range.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/op-write-durable.dark.svg"><img alt="op write durable" src="benches/results/2026-10-27-kv-compare/charts/op-write-durable.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/op-write-batch.dark.svg"><img alt="op write batch" src="benches/results/2026-10-27-kv-compare/charts/op-write-batch.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/op-load.dark.svg"><img alt="op load" src="benches/results/2026-10-27-kv-compare/charts/op-load.light.svg"></picture>

### Many threads

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/get-threads-small.dark.svg"><img alt="get threads small" src="benches/results/2026-10-27-kv-compare/charts/get-threads-small.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/get-threads-large.dark.svg"><img alt="get threads large" src="benches/results/2026-10-27-kv-compare/charts/get-threads-large.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/mixed-threads-small.dark.svg"><img alt="mixed threads small" src="benches/results/2026-10-27-kv-compare/charts/mixed-threads-small.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/mixed-threads-large.dark.svg"><img alt="mixed threads large" src="benches/results/2026-10-27-kv-compare/charts/mixed-threads-large.light.svg"></picture>

### Cost

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/cost-disk.dark.svg"><img alt="cost disk" src="benches/results/2026-10-27-kv-compare/charts/cost-disk.light.svg"></picture>

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/cost-write-amp.dark.svg"><img alt="cost write amp" src="benches/results/2026-10-27-kv-compare/charts/cost-write-amp.light.svg"></picture>

### Steady over time?

<picture><source media="(prefers-color-scheme: dark)" srcset="benches/results/2026-10-27-kv-compare/charts/stability.dark.svg"><img alt="stability" src="benches/results/2026-10-27-kv-compare/charts/stability.light.svg"></picture>

