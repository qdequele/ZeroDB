# ZeroDB documentation map

| Path | What it is | Authority |
|---|---|---|
| [`SPEC/`](SPEC/) | The on-disk format & algorithm specification, 7 volumes: `00` API surface & LMDB row-by-row contract, `01` flags, `02` pages & file format, `03` B+tree & cursors, `04` transactions & MVCC, `05` GC/freelist, `06` recovery & durability | **Source of truth.** When code and spec disagree, the spec wins until a human amends it |
| [`adr/`](adr/) | Architecture Decision Records `0001`–`0022`: the engine's foundations (oracle linkage, on-disk format, heed integration, write path, GC, reader table, nested read txns, crash harness, copy/tools, file naming, DUPSORT, prefetch, release policy) and the performance decisions after them (trusted-file mode, sequential writes, page spilling, validated-pages cache, in-place `WRITE_MAP`, the meta free-list annex, and the parked or pending ones) | Decisions with status (Draft/Proposed/Approved/Accepted, parked where measured and not adopted); indexed in [`DECISIONS.md`](DECISIONS.md) |
| [`DECISIONS.md`](DECISIONS.md) | One-line index of every ADR with status | Index |
| [`DIVERGENCES.md`](DIVERGENCES.md) | Every sanctioned behavior difference vs the LMDB fork, numbered `D-001`…, each with rationale and sign-off state | Nothing diverges silently |
| [`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md) | The performance ledger: every LMDB implementation trick, cited to `mdb.c`, marked DONE / PARKED / open — with the profile evidence and referee numbers per lever | Engineering log |
| [`UPSTREAM-BUGS.md`](UPSTREAM-BUGS.md) | Bugs found **in the LMDB fork itself** by ZeroDB's differential fuzzer, with repro recipes and ready-to-file issue drafts | Kept until fixed upstream |
| [`COMPATIBILITY.md`](COMPATIBILITY.md) | The release contract: every heed 0.22.1 item and every LMDB feature with its status on the adapter (Same / Emulated / Extension / No-op / Unsupported), plus what a user must know about the files | **Release contract** |
| [`RELEASING.md`](RELEASING.md) | How a version is cut: gate, consumer gate, docs truth pass, versions, CHANGELOG, tag, release workflow (ADR-0013) | Procedure |
| [`BENCH-MAP.md`](BENCH-MAP.md) | The LMDB-vs-ZeroDB microbench ladder: what every rung isolates, which PERF-GAP item it implicates, and how to read a jump between adjacent rungs (`just bench`, `just bench-report`) | Bench procedure |
| [`CONSUMER-GATE.md`](CONSUMER-GATE.md) | Meilisearch on ZeroDB: the zero-source-change drop-in check, the consumer test suites, and the LMDB-vs-ZeroDB Meilisearch benchmark (`scripts/consumer.sh`) | Gate + bench procedure |
| [`TOOLS.md`](TOOLS.md) | Manual for `zerodb-tools` (`stat`/`dump`/`load`/`check`/`migrate-from-lmdb`) | Manual |

Top-level companions: [`AGENTS.md`](../AGENTS.md) (the project rules: oracle
rules, unsafe policy, check battery), [`CONTRIBUTING.md`](../CONTRIBUTING.md)
(how to send a change), [`CHANGELOG.md`](../CHANGELOG.md) (release history).
Planned and open work is tracked in GitHub issues.

The spec, the ADRs, `DIVERGENCES.md` and the performance documents keep their
own index codes: milestone numbers of the original roadmap (`M1.4`, Phases
0–3), divergence IDs (`D-001`…) and performance-inventory items (`B12`…). See
the note at the top of [`DECISIONS.md`](DECISIONS.md).
