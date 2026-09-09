# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a suspected vulnerability. Report it
privately through GitHub's *Report a vulnerability* button on this repository
(Security → Advisories), or by email to the maintainer listed in `Cargo.toml`.
You will get an acknowledgement within a few days and a fix or a written
assessment before any public disclosure. Data-corruption bugs, memory-safety
bugs in the `unsafe` surface, and crash-consistency violations all count.

## Security model

ZeroDB is an embedded storage engine, not a security boundary. It assumes the
database file is written only by trusted processes on a trusted filesystem, the
same assumption LMDB makes.

**What it promises**

- **Crash consistency.** A power loss or `SIGKILL` at any point leaves the file
  at some previously committed transaction, never a torn state: two
  double-buffered meta pages carry a mandatory CRC32C, and data pages are
  flushed before the meta that references them (SPEC 06). This is exercised
  continuously by a fault-injecting write backend and a `SIGKILL` harness
  (ADR-0008).
- **Memory safety and freedom from panics for every legal sequence of API
  calls**, verified by differential fuzzing against LMDB, `miri` on the core,
  and `loom` on the reader table.
- **Single-writer, many-reader MVCC isolation within one process.** Readers
  never block the writer or each other; a reader always sees one committed
  snapshot.
- **Environment/transaction pairing is checked.** Using a database handle with
  a transaction from a different environment panics with heed's message
  instead of touching the wrong file.

**What it does not promise**

- **Safety against a hostile or corrupt database file.** The meta CRC detects
  accidental tearing, not tampering; it is unkeyed. A first-release hardening
  pass made the open path and the read path refuse out-of-range page
  references, zero-child branch pages, and hostile free-page lists with typed
  errors, and added a fuzz target that feeds arbitrary bytes to `open`. That
  work is a best effort, not a guarantee: a crafted file can still be able to
  cause a panic or an out-of-memory in the offline `zerodb-tools`. Run
  `zerodb-tools check` on files you trust or in a sandbox.
- **Cross-process access.** One process per environment; there is no lock
  file and no reader table shared between processes (D-001). Opening the same
  environment from two processes is undefined.
- **Encryption at rest, or protection from a local user with write access to
  the environment directory.** Files are created with mode `0600`.
- **Bounded memory for a write transaction.** All dirty pages of a write
  transaction are held in RAM until commit, and a single value may be up to
  4 GiB. There is no `MDB_TXN_FULL` equivalent.
- **Anything about the C LMDB fork.** `zerodb-tools migrate-from-lmdb` (an
  opt-in build feature) parses an LMDB file with the C library; any LMDB
  memory-safety issue is in scope for that one command and no other part of
  the engine.

## Supported versions

Only the latest release on the `0.x` line receives fixes. Releases are git
tags with GitHub release notes (ADR-0013); a fix ships as the next patch tag.

## Where the `unsafe` is

`unsafe` is confined to the memory map and the unchecked page-field readers
behind a validated-view contract (`zerodb-core::page::raw`, `zerodb-io`), one
`pwritev` and one `flock` FFI call, the differential oracle's LMDB FFI, and the
few pointer-shaped constructs heed's own API forces on the adapter. Every block
carries a `// SAFETY:` comment; the policy is in `CLAUDE.md`.
