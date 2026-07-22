# ADR-0014: Cross-process readers — shared reader table + lock protocol (D-001)

- Status: Approved — Quentin, 2026-07-22 (chat: 'ADR-0014 -> Option B'): **full multi-process**, overriding the draft's stage-1-only recommendation; remaining open questions resolved per recommendation, Q3 amended (recorded in Decision)
- Milestone: 2.10 (new; parity-closure track, 2026-07-22 directive)
- Date: 2026-07-22
- Closes: DIVERGENCES.md D-001 *as filed* ("cross-process **readers** via lock
  file"); public tracking issue [#70](https://github.com/qdequele/ZeroDB/issues/70)

## Context

D-001 is the founder-approved single-process scope decision: no lock file, a
same-process registry rejects double-open (`EnvAlreadyOpened`), the MVCC
reader table is ordinary process memory (ADR-0006,
`crates/zerodb-core/src/readers.rs` — whose crash note explicitly leans on
D-001: "a crashed process has no surviving readers to respect"), and
`zerodb-tools` refuses live envs outright
(`crates/zerodb-tools/tests/live_env_refusal.rs`). On 2026-07-22 the
maintainer directed that the four LMDB-ahead divergences be closed; this is
the largest operational gap: no live `stat`/`check`/backup from another
process, no external-tool attach, no LMDB-style hot-copy driven out-of-process.

**LMDB prior art** (fork `mdb.master.nested-rtxns`): `lock.mdb` is a shared
mmap holding `MDB_txninfo` — a versioned header, the writer mutex, and a
reader slot array of `{pid, tid, txnid}`. Writer serialization across
processes uses robust pthread mutexes on Linux (POSIX semaphores / SysV on
platforms without robust mutexes, e.g. macOS). Stale readers are reaped by
`mdb_reader_check`: pid liveness probe plus robust-mutex `EOWNERDEAD`
handling. The GC's oldest-reader gate scans the shared slots, so a reader in
any process holds page reclamation back — exactly the semantics our GC-18
implements in-process today.

**libmdbx prior art**: `mdbx.lck`, robust futexes on Linux with elaborate
per-OS fallbacks, automatic stale-reader reclamation, a versioned lck header
that is wiped and rebuilt when incompatible, and defenses LMDB lacks against
pid-reuse ABA (boot-id/temporal checks). Its lock code is the cautionary tale
for scope: multi-writer arbitration across processes is by far the largest and
most platform-conditional part.

Constraints from the repo law:

- Pure Rust; allowlisted deps only — `memmap2` (shared map) and `libc`
  (`flock`/`fcntl`, `kill(pid, 0)`) already suffice; **no** pthread
  robust-mutex dependency (absent on macOS anyway, and glibc-specific
  semantics on Linux).
- `unsafe` only in the sanctioned zones — the lock-file mmap lands exactly in
  `zerodb-core::readers` + `zerodb-io` (mmap), both already sanctioned.
- All cross-process atomics: explicit orderings, ARM-first reasoning
  (CLAUDE.md); one-word-per-slot observations, as ADR-0006 D1 already
  established in-process.
- Lock-file layout must not assume a 4K OS page and is versioned
  independently of the data format (SPEC 02 stays untouched except §8's file
  inventory).
- No consumer runs two *writer* processes against one env: Meilisearch owns
  its env from one process; every real demand (meilitool, snapshots, S3
  streaming, `zerodb-tools`) is read-only attach.

## Options

### Option A — Stage 1: read-only cross-process attach (recommended)

One read-write opener, N read-only attachers. New companion file
`zerodb.lck` (native name; see Open Q4): versioned header + slot array where
each slot is the existing one-`AtomicU64`-txnid protocol (TXN-14/15 verbatim,
now in shared memory) plus a `pid` word written after claim. Single-writer
exclusivity across processes via an OS-level file lock held for the RW env's
lifetime; RO attachers take the shared/presence side of the same primitive
(sub-choice, Open Q2: Linux OFD `fcntl` byte-range locks — auto-released on
process death, per-open-description — vs. `flock` on a second token file for
macOS portability). RO `read_txn` begin: claim slot → pin txnid → **re-read
the live meta and verify** (the SPEC 04 §4 publish-and-verify loop, with the
on-file meta page as the publish cell instead of the in-process
`Arc<Snapshot>`). Writer GC gate (GC-18) scans the *shared* table; stale
slots reaped under the writer lock via pid liveness, with the claim-word
protocol making a half-dead claim harmless (Open Q3 for pid-reuse depth).

- Pros: closes D-001 as filed (readers); unlocks every known consumer use
  case (live stat/check/copy/backup); reuses the loom-verified slot protocol
  and the GC gate unchanged in shape; no robust-mutex machinery; writer path
  performance untouched (one extra flock at open, shared-table scan already
  existed in-process).
- Cons: two writer processes still refuse (`EnvAlreadyOpened` semantics
  extend cross-process via the writer lock — a *clean error*, not arbitration);
  reader-begin gains a meta re-read + verify loop (bounded, lock-free); a new
  on-disk artifact to version, document (new SPEC 07), and crash-test.
- Crash safety: lck carries **no durable truth** — it is rebuilt/validated at
  RW open (LMDB/libmdbx both treat the lock file as scratch); a torn lck can
  at worst delay reclamation until reaped. Data-file recovery is unchanged.
- ARM: slot claim/pin stays the existing CAS + `SeqCst` publish; meta verify
  adds `Acquire` reads of the meta page bytes the writer published with the
  existing C5/C6 barriers.

### Option B — Full LMDB-style multi-process write arbitration

Everything in A, plus a cross-process writer mutex so any process can begin a
write txn (queueing like LMDB). Requires robust-lock semantics: a writer
dying mid-commit must leave the mutex takeable *and* the next writer must
re-validate both metas before trusting in-process assumptions (our commit
pipeline's poison state is per-process). Without robust pthread mutexes the
practical primitive is again OFD/flock — workable, but now on the hot commit
path, and dead-writer takeover needs its own recovery protocol and crash
matrix (H-stages × dying lock-holder).

- Pros: exact LMDB capability envelope.
- Cons: the entire extra surface serves a use case **no consumer has**
  (scope rule: a feature with no consumer should not get a staged build-out);
  largest and most platform-divergent code in both prior-art engines;
  meaningfully expands the crash-test matrix.

### Option C — Keep single-process, tools stay offline

Status quo. Rejected by the 2026-07-22 directive; recorded for completeness.

## Decision

**Option B — full multi-process support** (maintainer decision, Quentin,
2026-07-22, overriding the draft's stage-1-only recommendation): ZeroDB gets
the complete LMDB capability envelope — concurrent reader processes **and**
cross-process write arbitration.

Delivery is staged inside milestone 2.10 so the riskiest piece lands last,
behind tests (scope rule):

- **2.10a — RO attach** (everything in Option A): `zerodb.lck` + shared
  reader table + single-RW exclusivity + stale-reader reaping. Independently
  shippable; already closes D-001 as filed.
- **2.10b — write arbitration**: the writer file-lock becomes a cross-process
  mutex (acquired at `write_txn` begin, released at commit/abort), plus
  **dead-writer takeover**: acquiring the lock after a writer died mid-txn
  triggers the reopen-grade meta re-validation (SPEC 06) before the new
  writer proceeds. Safe on COW grounds: an uncommitted dead writer only ever
  wrote pages no live snapshot references (TXN-62) and never advanced a meta.

With write arbitration in scope from the start, pid-reuse hardening moves up:
the boot-id/start-time slot word (libmdbx level, Q3 as amended) ships in the
**v1** lck format instead of being a reserved field.

## Consequences

- New **SPEC 07 — lock file & cross-process protocol** (layout, versioning,
  claim/reap state machine, meta publish-and-verify for out-of-process
  readers); SPEC 04 §4 amended to name the shared table as the slot substrate;
  SPEC 02 §8 file inventory gains `zerodb.lck`.
- `readers.rs` crash note rewritten (the D-001 argument disappears); reaping
  logic added behind the writer lock; `Env::clear_stale_readers()` (2.2)
  stops being a constant 0 and gets real semantics + tests.
- `zerodb-tools`: live RO attach replaces blanket refusal for `stat`/`check`/
  `copy` read paths (`live_env_refusal.rs` reworked to assert the *writer*
  exclusion instead).
- Oracle: cross-process differential (spawn child readers against both
  engines; LMDB fork does support this, so it *is* differentially testable);
  crash harness extension: `kill -9` a reader mid-pin → writer reaps → GC
  advances; `kill -9` during slot claim → claim word recovery. **2.10b adds
  the dying-lock-holder dimension**: `kill -9` the writer at each H-stage
  while a second process waits on the writer lock — takeover must recover to
  the REC-6 floor before the next write txn begins.
- SPEC 07 additionally specifies the writer-mutex acquisition/release points
  (`write_txn` begin, commit C6, abort) and the dead-writer takeover
  re-validation protocol.
- DIVERGENCES.md: retire D-001 on landing; close issue #70; PROGRESS.md line;
  README capability matrix update.

## Open questions for human review

All resolved 2026-07-22 with the approval:

1. **Scope ratification** — **Resolved: Option B chosen by the maintainer.**
   Full multi-process including write arbitration is in scope, staged
   2.10a → 2.10b.
2. **Lock primitive** — resolved per recommendation: `flock` token-file pair
   as the portable baseline, OFD `fcntl` as a Linux fast-path behind `cfg`,
   chosen at open. (With 2.10b the lock sits on the write-txn begin path: one
   `flock` syscall per write txn, noise next to the commit fsyncs.)
3. **Pid-reuse (ABA) depth** — resolved **as amended**: the boot-id/
   start-time slot word ships in the v1 lck format (write arbitration raises
   the ABA stakes; reserving it would have forced a format bump later).
4. **File naming at the adapter boundary** — resolved per recommendation:
   native `zerodb.lck` only; nothing in the Meilisearch tree reads
   `lock.mdb`; revisit under the D-012 pattern only if a gate demands it.
5. **RO attach + map growth** — resolved per recommendation: typed error in
   2.10; remap belongs to 3.2 (auto-geometry).
