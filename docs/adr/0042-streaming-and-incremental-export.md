# ADR-0042: Streaming and incremental environment export

- Status: Draft, awaiting approval. Nothing is implemented.
- Related issue: none yet (to open before approval)
- Date: 2026-11-08

## Context

A consumer that keeps its environments in object storage (S3 or similar) and
opens them on demand, for example a serverless Meilisearch that downloads an
index when a request for it arrives, needs two things from the engine:

1. **An export that does not need local scratch space.** Today an export is
   `CopyToFile::copy_to_file` (to a path, through a staging file) or
   `copy_to_open_file` (into a `File`) (`crates/zerodb/src/copy.rs`, ADR-0009,
   ADR-0032). Both need a local file at least the size of the image.
   Meilisearch's S3 snapshot does exactly this: a raw copy into a temporary
   file, which it then reads back into a tarball and uploads. For a large
   index that is a second copy of the index on disk and twice the disk I/O
   before the first byte leaves the machine.
2. **An export proportional to what changed.** Re-uploading a whole
   multi-gigabyte environment after each indexing batch costs time, bandwidth
   and storage, even when the batch rewrote a small fraction of its pages.

Lazy loading itself (downloading an environment when a request needs it,
evicting it later) stays in the consumer. The engine reads pages straight
from a memory map (`Backing::bytes`, `crates/zerodb-core/src/env/backing.rs`),
and fetching pages from object storage on a page fault would need either a
userfaultfd or FUSE layer (FFI, outside the `unsafe` policy, Linux-only) or a
buffer-pool read path, which is a different engine. This ADR covers only the
two engine primitives above.

What the engine already provides:

- **A consistent snapshot without stopping the writer.** A copy pins snapshot
  `T` with its own read transaction. No page reachable from `T` is rewritten
  while the pin holds (SPEC 04 TXN-20/21, TXN-62; the `WRITE_MAP` in-place
  writes obey the same rule, ADR-0021). A page free at `T` may change during
  the copy, which is harmless because the copy's free list lists it as free.
- **Page order of the two copy modes.** A raw copy (`CompactionOption::Disabled`)
  knows its two meta pages before it writes anything (they are synthesized
  from `T`) and writes the data pages `2..=last_pg` in ascending order. A
  compacting copy (`Enabled`) emits data pages in ascending order (the
  streaming builder allocates and emits each page at once,
  `zerodb-core::builder::EnvStream`) but emits the two meta pages **last**,
  because the roots are known only when the build ends.
- **A writer stamp on every page.** Every page header carries the id of the
  write transaction that last wrote it (SPEC 02 §2, offset 8), and every page
  a write transaction writes is stamped with its id (SPEC 04 TXN-3). The
  format reserved this field for incremental page shipping (ADR-0002, SPEC 02
  §2 and its consumer table). Overflow runs carry the stamp in their head page
  only; interior pages are pure payload with no header (SPEC 02 §5).

LMDB has neither primitive. `mdb_env_copyfd2` writes into a descriptor, which
can be a pipe, so it streams. heed exposes it only as `copy_to_fd`, which
ZeroDB's adapter does not support because it takes an LMDB FFI handle type.
LMDB has no incremental copy. Both primitives would therefore be ZeroDB
extensions in the native `zerodb` crate, behind new functions. The default
behavior and heed's surface do not change.

## Options

### Part 1: export without a local file

#### Option 1A: `copy_to_writer(&mut impl Write)`, raw only

A raw image is sequential: metas first, then pages in ascending order. It
streams into any `Write` (a pipe, a tar builder, a multipart upload writer).
The image length, `(last_pg + 1) × page_size`, is known before the first byte,
which a tar header needs. A compacting copy cannot stream into a plain
`Write`: its metas come last but belong at offset 0.

Pros: the smallest API, and exactly what Meilisearch's raw S3 snapshot needs.
Cons: no compacting export without a temporary file. A raw image keeps its
free pages, so it can be larger than a compacted one.

#### Option 1B: an image sink with a stated order (recommended)

```rust
/// Receives an environment image, in order.
pub trait ImageSink {
    /// Called once, first. `image_len` is the image's length in bytes when it
    /// is known before any page (a raw copy), `None` otherwise (a compacting
    /// copy).
    fn begin(&mut self, page_size: u32, image_len: Option<u64>) -> io::Result<()>;
    /// The two meta pages (`2 × page_size` bytes at image offset 0). Called
    /// exactly once: before any `pages` call for a raw copy, after the last
    /// one for a compacting copy.
    fn metas(&mut self, bytes: &[u8]) -> io::Result<()>;
    /// Data pages at image offset `offset`. Offsets start at `2 × page_size`,
    /// ascend, and are contiguous across calls.
    fn pages(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()>;
}

fn copy_to_sink(&self, sink: &mut dyn ImageSink, option: CompactionOption)
    -> Result<Option<ExportPoint>>;
```

The crate provides two adapters: `WriteSink<W: Write>`, which accepts only
the raw order and fails a compacting copy with a typed error before reading
anything, and `FileSink`, which writes at offsets (today's
`copy_to_open_file` becomes a thin wrapper over it). A consumer uploading a
compacting copy to S3 implements the sink itself. It uploads parts 2..n as
pages arrive and part 1 last: part 1 is the metas plus the first pages up to
the 5 MiB minimum part size, held in memory until `metas` arrives.

A raw copy returns `Some(ExportPoint)` (Part 2), so it can serve as the base
of a later incremental export. A compacting copy returns `None`: it renumbers
pages, so it can never be a base.

Pros: both modes stream with no local file, and the order contract is
explicit and testable. Today's two copy forms become special cases, so the
behavior is unchanged for heed.
Cons: a new public trait. A sink that needs the metas first and cannot
reorder has to use the raw mode.

#### Option 1C: two-pass compaction

The compacting builder is deterministic. A first pass with a counting sink
computes the roots and statistics, so the metas are known before the second
pass writes anything, and a plain `Write` works for both modes. This reads
every live entry twice, doubling the compacting export's CPU and read I/O for
an API convenience that 1B gets without the extra pass.

#### Option 1D: do nothing

The consumer keeps writing to a temporary file. This is correct today. It
costs one extra local copy of the image per export.

### Part 2: incremental export

#### Option 2A: a reachability walk pruned by the writer stamp (recommended)

Given an **export point** `N`, the txnid and identity of an earlier raw image,
and a newly pinned snapshot `T ≥ N`, the delta is the set of pages reachable
from `T` whose stamp is greater than `N`, plus `T`'s two meta pages. Applying
it to the raw image of `N`, by writing each page at its page number,
extending or trimming the file to `(T.last_pg + 1) × page_size` and writing
`T`'s metas, produces an image that opens at exactly `T`.

**Why pages with stamp `≤ N` can be skipped.** Let `P` be a page reachable
from `T` with stamp `s ≤ N`.

1. `P` was last written by transaction `s`, because every write stamps the
   page (TXN-3). Pages written by an aborted transaction, including spilled
   pages (ADR-0017), are never linked from a committed tree: a transaction
   links only pages it wrote itself or pages reachable from its base. So `s`
   is a committed transaction of `T`'s history.
2. From `s` to `T` no transaction wrote `P`, or its stamp would exceed `s`.
   A transaction writes only pages that are free in its base snapshot or that
   it allocated itself (TXN-62), and nothing links a free page without
   writing it. So `P` stayed reachable at every commit from `s` to `T`,
   including `N`, with the same bytes.
3. Every page under `P` is also unchanged. `P`'s bytes, and so its child
   page numbers, are the same at `N` and `T`. A child rewritten after `N`
   would have had to be free in the rewriting transaction's base, but it was
   reachable through `P` throughout. So once a page's stamp is `≤ N`, its
   whole subtree, overflow runs included, is skipped without reading it.

The walk covers the main tree, every named database's tree (from the catalog
records in the main tree), and the free-page tree. The meta free-list annex
(ADR-0022) travels inside the metas. A page reached with stamp `> N` is
shipped; for a leaf, so is each `F_BIGDATA` overflow run whose head page has
stamp `> N`, as a whole run of `ovf_pages` pages. The walk reads only the
changed pages and the headers of their direct children, so its cost follows
the size of the change, not the size of the environment.

Pages the delta does not ship and that are free at `T` keep whatever the base
held. This is the guarantee a raw copy already gives for free pages: they are
listed free, so their bytes are never read as live data.

**Delta stream format (version 1).** Little-endian, sequential, so it can be
produced into any `Write` and applied from any `Read`:

| Part | Content |
|---|---|
| Header (64 bytes) | magic `ZDBDELTA`, format version `u32 = 1`, page size `u32`, base txnid `u64`, base meta digest `u32`, target txnid `u64`, target `last_pg` `u64`, reserved zeros, CRC32C of the preceding header bytes |
| Target metas | `2 × page_size` bytes: `T`'s two meta pages as a raw copy synthesizes them |
| Runs | each: `pgno u64`, `count u32`, CRC32C `u32` of the run's bytes, then `count × page_size` bytes; page numbers ascending and non-overlapping |
| End | `pgno = u64::MAX`, `count = 0`, then the CRC32C of every byte before the end record |

The **base meta digest** is the CRC32C of the base image's live meta page
(the image the delta applies to). It catches the common mistakes, such as a
delta applied to another index or to an image at a different txnid. It does
not prove identity: two images with identical metas can hold different live
pages (see the history hazard below), and the digest cannot tell them apart.
The caller must therefore match each delta to the exact image it was
exported against, for example by recording the base image's object key and
version next to the delta. The digest is a guard against mistakes, not a
substitute for that bookkeeping.

**API sketch** (in `zerodb`, beside `CopyToFile`):

```rust
/// What a raw export or an applied delta produced: enough to name it as the
/// base of the next incremental export.
pub struct ExportPoint { pub txnid: u64, pub page_size: u32, pub last_pg: u64, pub meta_digest: u32 }

/// Write the pages changed since `since` into `out`. Pins its own snapshot,
/// as the copies do. Fails with a typed error when `since.txnid` is above the
/// live txnid or the page size differs.
fn export_delta(&self, since: &ExportPoint, out: &mut dyn Write) -> Result<ExportPoint>;

/// Apply a delta to a raw image in place. Checks the header against the
/// image's live meta before writing anything.
///
/// # Safety
///
/// For the whole call, no environment may have `image` open, and none may
/// open it: neither an environment opened before the call (its readers map
/// the pages this overwrites) nor one opened during it.
pub unsafe fn apply_delta(image: &File, delta: &mut dyn Read, durable: bool)
    -> Result<ExportPoint>;
```

**Exclusive access.** `apply_delta` overwrites pages that are reachable in
the base. A reader of an environment already open on the image would see
those pages change under its snapshot, through its map. Zeroing the metas
does not stop it, because a reader never re-reads the metas once it has
pinned a snapshot. So exclusive access for the whole call is a precondition,
and `apply_delta` is an `unsafe fn` with that contract, like
`EnvOpenOptions::open` (ADR-0034). An image the consumer has just downloaded
is open nowhere, so the contract is easy to meet in the intended use. Open
question 3 asks whether the engine should enforce it through the lock file
instead.

**Order of writes.** A partly applied image is neither `N` nor `T`, so the
apply runs in this order:

1. Verify the header (its CRC, the page size, and the base meta digest)
   against the base.
2. Zero both meta slots and `fdatasync`, always, whatever `durable` says.
   Writeback does not keep write order, so without this sync a run written
   in step 3 could reach the disk while an old meta survives, and a crash
   would leave a file that opens at `N` over overwritten pages. After the
   sync, an interrupted apply leaves a file that open refuses (SPEC 02 §3.2).
3. Write the runs. For each run, check its CRC, and check that its page
   numbers are at least 2, at most the target `last_pg`, and above the
   previous run's.
4. Require the end record and check the stream CRC. A corrupted page number
   can send a run to the wrong place without failing the run's own CRC; the
   stream CRC catches it. If the end record is missing or the CRC fails,
   return an error here: the metas stay zeroed and the file stays
   unopenable.
5. Set the file length to `(target last_pg + 1) × page_size`.
6. With `durable`, `fdatasync`.
7. Write the target metas.
8. With `durable`, `fdatasync` again.

Without `durable`, steps 6 and 8 are skipped. The result is then safe
against the process dying, but not against a power loss after the call
returns, the same guarantee an unsynced copy gives.

A caller that needs to keep the base clones it first (cheap on APFS, XFS or
btrfs, as in ADR-0032). Deltas chain: a delta from `T` applies to the image
that a delta from `N` produced.

**The history hazard.** The proof above assumes the image of `N` belongs to
`T`'s history. Transaction ids are reused (TXN-2), so `N` can name two
different snapshots in two cases:

- a `PREV_SNAPSHOT` open that rolls back past `N` (TXN-67);
- a crash under `NO_SYNC` or `NO_META_SYNC` that reopens below `N`, when the
  export read commits that never became durable.

After either case, later commits reuse the ids and the delta is silently
wrong. The meta digest does not catch this: the live environment no longer
has `N`'s meta to compare with.

Comparing txnids after the fact does not catch it either. If an environment
recovers to txnid 99 after exporting at 100, another process can commit a
new 100 before the caller's first check. The check then sees txnid 100,
accepts the stale export point, and the delta is wrong. So the contract does
not rely on detection. It prevents both cases instead:

- **Recovery must never land below a recorded export point.** Making the
  exported snapshot durable is not enough. Under relaxed durability
  (`NO_SYNC`, `NO_META_SYNC`, or `WRITE_MAP` with `MAP_ASYNC`), the commits
  after the export write both meta slots in turn with no barrier between
  them. A power loss can tear one meta write and lose the other, and recovery
  then selects a meta below the export point (the crash model of ADR-0038
  allows torn and reordered writes). So one of these must hold:
  - the exported snapshot is itself durable before the export point is
    recorded (it was committed with default durability, or the caller ran
    `force_sync` after the export), and every later commit, by every
    process, runs with default durability for as long as the point is kept.
    Each commit is then durable before the next one overwrites the other
    meta slot, so recovery never selects a meta older than the exported
    snapshot;
  - or, under relaxed durability, the caller discards every export point
    after any unclean shutdown (a crash of the process, the kernel or the
    machine), detected for example with a marker it writes only after
    closing the environment cleanly, and the next export is a full raw
    export.
- **An environment with recorded export points is never opened with
  `PREV_SNAPSHOT`.** If it is, the caller discards every export point and
  the next export is a full raw export.

A caller that cannot guarantee both makes every export a full raw export.
Without a format change the engine cannot enforce these rules. Open
question 2 covers what enforcing them would take.

Pros: uses only the existing format and invariants. The stamp was reserved
for this. The cost is proportional to the change. The apply side is simple
and checks itself.
Cons: works only from raw images, since a compacting copy renumbers pages, so
a chain starts from a raw export and a compaction (Meilisearch's index
compaction swaps the data file) starts a new chain. It adds a new on-disk
stream format to version and keep. It relies on the history contract above.
The delta of a batch that rewrites leaves across most of an index's
databases may approach the full size; that ratio is unmeasured.

#### Option 2B: a linear scan of the file by stamp

Read every page header in `2..=last_pg` and ship pages with stamp `> N`.
This is unsound. Interior overflow pages have no header, so their payload can
look like any header. Free pages keep stale headers, and a stale overflow head
can claim a run that now overlaps live pages. It would also read the whole
file on every export. Rejected.

#### Option 2C: track written pages in the engine

Record every page number written since an export point, in memory or in a
persisted log, and ship those. Memory tracking does not survive a restart.
Persisting it touches the commit path, durability and the format. 2A obtains
the same set from data the format already has. Rejected.

#### Option 2D: leave it to the consumer or the platform

The consumer re-exports in full (Part 1 only), or relies on block-level
incremental snapshots of the volume (EBS snapshots, `btrfs send`). These
work today and need nothing from the engine. They do not give a per-index,
object-store-friendly delta that a serverless reader can apply.

## Decision

Proposed: **Option 1B** and **Option 2A**, staged as CONTRIBUTING rule 6
asks. Part 1 is independent of Part 2 and can land first.

Before Part 2's implementation, a spike tests the riskiest assumption, the
skip proof in the presence of every page-writing path. Under a randomized
workload it takes a raw export at `N`, keeps committing, exports a delta at
`T`, applies it, and checks three things: the applied image's logical dump
equals a raw copy of `T`; every page reachable from `T` is byte-identical;
and `zerodb-tools check` passes. The workload covers puts, deletes, overflow
values, named-database creation and drops, aborted transactions, spilling
(`max_dirty_bytes`), `WRITE_MAP`, `NO_SYNC`, and reserved puts written in
place. The spike also reports the delta-to-full size ratio on the Meilisearch
incremental-additions workload, which decides whether Part 2 is worth
shipping.

## Consequences

- `crates/zerodb/src/copy.rs`: `ImageSink`, `WriteSink`, `FileSink`,
  `copy_to_sink`, and `ExportPoint` returned by raw exports. Today's copy
  forms keep their behavior and become wrappers; the existing copy tests and
  the oracle copy differential pin that. The ADR-0032 clone and file-copy
  routes stay specific to the path and `File` forms.
- `zerodb-core`: a walk of the pages of a snapshot with stamp `> N`, in the
  I/O-free crate so it runs under miri. `zerodb`: `export_delta` and
  `apply_delta` over it.
- SPEC 04 gains an explicit rule for step 2 of the proof, which TXN-62
  implies but no rule states: a write transaction writes only pages free in
  its base snapshot or allocated by itself, and no transaction links a free
  page without writing it. Code and tests can then cite that rule. SPEC 02
  documents the delta stream format and updates its consumer table for the
  `txnid` stamp. SPEC 00's extensions table lists the new functions.
- `zerodb-tools`: `export-delta` and `apply-delta` commands, and `check`
  usable on an applied image.
- `docs/LMDB-COMPATIBILITY.md`: both are extensions and change no heed
  behavior.
- No new dependency and no new `unsafe` block. `apply_delta` is declared
  `unsafe fn` for its exclusive-access contract; its body needs no `unsafe`.
  Calls of it, in `zerodb-tools` and tests, need an `unsafe` policy entry
  like the one for `EnvOpenOptions::open` (ADR-0034), each with a
  `// SAFETY:` comment saying why no environment has the image open.
- An image downloaded from object storage is opened through the `unsafe`
  `EnvOpenOptions::open` (ADR-0034). Whether such an image counts as the
  caller's own file or should be opened with `FileTrust::VALIDATE` is the
  caller's call, but the docs must say so.

## Open questions for human review

1. Is a delta format worth versioning and keeping before the spike reports
   the delta-to-full ratio on real Meilisearch workloads, or should Part 2
   wait for that number?
2. Should the engine enforce the history contract? A lineage id in the meta
   alone is not enough: if both metas carry the same id, a crash that
   recovers below an export point keeps the id, and a check comparing ids
   accepts the divergent history. Enforcement would need two parts:
   - a durable export watermark, the highest exported txnid, made durable
     before an export point is handed out;
   - a lineage id that any open rotates when it selects a meta below that
     watermark, or opens with `PREV_SNAPSHOT`.

   An export point would then carry the lineage id, and `export_delta`
   would refuse a point whose id differs. That is a format change and needs
   its own ADR. Until then, the caller rules under "the history hazard"
   apply.
3. `apply_delta` requires exclusive access as an `unsafe` contract. Should
   it enforce it instead, by taking the image's lock file (ADR-0023) and
   refusing while any process has it open? That works only for an image
   inside an environment directory, not for a bare file. Relatedly,
   `apply_delta` zeroes the base's metas first, so a failed apply loses the
   base. Is that acceptable, or should it always write into a clone and
   rename?
4. Should `ImageSink` live in `zerodb` only, or also be exposed through the
   heed adapter as an extension next to `copy_to_path_with_progress`?
5. Part 1 makes a compacting export stream with its metas last. Should the
   compacting copy also report its final image length ahead of the metas,
   for consumers that must announce a length up front?
