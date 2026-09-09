# ZeroDB documentation map

| Path | What it is | Authority |
|---|---|---|
| [`SPEC/`](SPEC/) | The on-disk format & algorithm specification, 7 volumes: `00` API surface & LMDB row-by-row contract, `01` flags, `02` pages & file format, `03` B+tree & cursors, `04` transactions & MVCC, `05` GC/freelist, `06` recovery & durability | **Source of truth.** When code and spec disagree, the spec wins until a human amends it |
| [`adr/`](adr/) | Architecture Decision Records `0001`–`0012` (oracle linkage, on-disk format, heed integration, write path, GC, reader table, nested read txns, crash harness, copy/tools, file naming, DUPSORT, prefetch) | Decisions with status (Proposed/Approved); indexed in [`DECISIONS.md`](DECISIONS.md) |
| [`DECISIONS.md`](DECISIONS.md) | One-line index of every ADR with status | Index |
| [`DIVERGENCES.md`](DIVERGENCES.md) | Every sanctioned behavior difference vs the LMDB fork, numbered `D-001`…, each with rationale and sign-off state | Nothing diverges silently |
| [`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md) | The performance ledger: every LMDB implementation trick, cited to `mdb.c`, marked DONE / PARKED / open — with the profile evidence and referee numbers per lever | Engineering log |
| [`UPSTREAM-BUGS.md`](UPSTREAM-BUGS.md) | Bugs found **in the LMDB fork itself** by ZeroDB's differential fuzzer, with repro recipes and ready-to-file issue drafts | Kept until fixed upstream |
| [`CONSUMER-GATE.md`](CONSUMER-GATE.md) | Meilisearch on ZeroDB: the zero-source-change drop-in check, the consumer test suites, and the LMDB-vs-ZeroDB Meilisearch benchmark (`scripts/consumer.sh`) | Gate + bench procedure |
| [`TOOLS.md`](TOOLS.md) | Manual for `zerodb-tools` (`stat`/`dump`/`load`/`check`/`migrate-from-lmdb`) | Manual |

Top-level companions: [`PLAN.md`](../PLAN.md) (the milestone roadmap),
[`PROGRESS.md`](../PROGRESS.md) (the append-only engineering log),
[`CLAUDE.md`](../CLAUDE.md) (the development law: oracle rules, unsafe policy,
gate battery).
