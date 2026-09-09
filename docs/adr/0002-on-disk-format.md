# ADR-0002: On-disk format principles

- Status: **Approved** — Quentin, 2026-07-16 (commit 602169c "Ratify Phase 0 queue: ADR-0001/0002/0003 approved"; that commit updated the DECISIONS.md index but not this status line — corrected 2026-09-09)
- Milestone: 0.4 (spec), implemented 1.1–1.6
- Date: 2026-07-15

## Context

ZeroDB defines **its own on-disk format** (D-002, APPROVED): no byte-level
compatibility with LMDB 0.9/1.0; migration is logical (dump/load). [SPEC 02](../SPEC/02-pages.md)
and [SPEC 03](../SPEC/03-btree.md) are the full byte-level and algorithmic
specs; this ADR records the format-defining *choices* behind them and the
alternatives weighed, so the specs can stay descriptive.

Binding inputs (not relitigated here): little-endian only; runtime page size
4 KiB–64 KiB power of two, stored in meta, independent of OS page size; double
meta pages with a **mandatory** CRC (torn-meta detection is Phase 1); a common
page header carrying the writer txnID stamp (feeds Phase 3.10) and a reserved
data-page checksum field (Phase 3.9); DUPSORT format hooks reserved but
unimplemented (D-004, Phase 2.8); observable max key = 511 bytes and empty key
rejected (SPEC 01 §S4); no `#[repr(C)]` casting of unaligned data (CLAUDE.md).
LMDB fork sources were read for algorithmic understanding only (CLAUDE.md rule
4); no C was transliterated.

## Decisions

### D1 — Checksum algorithm: CRC32C (Castagnoli)

CRC32C, polynomial `0x1EDC6F41` (reflected in/out, init/final `0xFFFFFFFF`).
Used for the **mandatory meta CRC** (Phase 1) and the **reserved data-page
checksum** field (Phase 3.9). Phase 1 implementation is a software table; Phase
3.9 switches to the ARMv8-A `crc32c*` hardware instructions (primary target is
Graviton — CLAUDE.md), which compute exactly this polynomial.

- *Alternatives:* CRC32 (ISO/zlib, poly `0x04C11DB7`) — no dedicated ARM
  instruction, so slower and pointless when CRC32C has one. xxHash/xxh3 — faster
  bulk hashing but not a CRC (weaker torn-write/bit-error guarantees for a
  fixed-size 168-byte meta) and no hardware acceleration on ARM. A bare
  signature/magic (rejected by PLAN: "a signature field alone is not
  sufficient" for torn-meta detection).
- *Why:* hardware-accelerated on the primary platform, strong for the small
  fixed meta block, reused unchanged for Phase 3.9 data pages.

### D2 — Common 32-byte page header with txnID stamp + reserved checksum

Every page (leaf/branch/overflow/meta) shares a 32-byte header carrying `pgno`,
writer `txnid` (u64), `flags`, a reserved data-page `checksum` (u32), and an
8-byte variant tail. See SPEC 02 §2.

- *Alternatives:* a minimal 16-byte LMDB-style header (pgno + pad + flags +
  lower/upper), adding txnid/checksum only where needed. Rejected: a **uniform**
  header makes page shipping (3.10) and a future all-page checksum (3.9) a flag
  flip rather than a format change, and makes the decoder branch only on
  `flags`. The 16 extra bytes/page cost is negligible (≤ 0.4 % at 4 KiB, less at
  64 KiB).
- *Why:* pays for Phase 3.10/3.9 now at trivial space cost; single decode entry
  point.

### D3 — Body-relative intra-page offsets

`lower`, `upper`, and node pointers are measured from the end of the common
header (absolute offset 32), not from the page start. See SPEC 02 §0.

- *Alternatives:* LMDB's page-start offsets, which cannot represent a 65536-byte
  page's `upper` in a `u16` and need the `PAGEBASE` compile-time hack.
- *Why:* the largest body-relative offset is `65536 − 32 = 65504 < 65536`, so
  64 KiB pages (common on ARM) need no special case. Cleaner and directly
  supports the full page-size range.

### D4 — Distinct leaf/branch node headers; full 8-byte child pgno

Leaf node header = 8 bytes (`flags`, `ksize`, `dsize`); branch node header = 10
bytes (`child_pgno` u64, `ksize`). See SPEC 02 §4. Branch pgno is a full `u64`.

- *Alternatives:* LMDB packs the child pgno into `lo`/`hi`/`flags` (48-bit) to
  save 2 bytes/branch node. Rejected: the packing is error-prone, and 48 bits is
  a latent cap; a full u64 matches our `pgno` type everywhere and simplifies
  unaligned reads.
- *Why:* clarity and uniformity of the pgno type outweigh 2 bytes per branch
  node (branch nodes are a small fraction of total nodes).

### D5 — Inline-vs-overflow threshold = `max_node_size`

A value is inlined iff its leaf cell `8 + ksize + dsize ≤ max_node_size`, where
`max_node_size = ((psize − 32)/2 rounded even) − 2` (the largest cell that still
guarantees two entries per page). See SPEC 02 §4.2.

- *Alternatives:* a fixed byte threshold (e.g. "> 2 KiB → overflow"),
  independent of page size. Rejected: it would either waste overflow pages on
  small pages or under-use large pages; the "two entries must fit" rule is the
  natural B+tree bound and matches LMDB's `me_nodemax` intent.
- *Why:* scales correctly across the 4 K–64 K page range; guarantees `MIN_KEYS`.

### D6 — Split-point and rebalance thresholds (LMDB parity)

Normal split at the median `(nkeys+1)/2` with a fit-adjust; **append split** at
`nkeys` (new key alone on a fresh right page) for APPEND workloads (SPEC 01 §S1).
Rebalance thresholds: leaf `min_keys=1`, branch `min_keys=2`, fill threshold
25.0 % (250 permille); merge absorbs right into left. See SPEC 03 §6.4/§10.

- *Alternatives:* a different fill threshold (e.g. 33 %/50 %) or always-merge-
  toward-a-fixed-side. Rejected for Phase 1: parity with the oracle is the goal;
  changing thresholds changes page counts and would diverge from LMDB's
  `non_free_pages_size` within the tolerance band (PLAN 1.5). GC/space redesign
  is Phase 3.1.
- *Why:* Phase 1 is strict behavior parity; these are the oracle's numbers.

### D7 — Max DB-name length = 511 bytes

Named-DB names are catalog keys in the main DB (SPEC 02 §6), so they are bounded
by `MAX_KEY_SIZE = 511`.

- *Alternatives:* a shorter dedicated cap (e.g. 255). Rejected: names *are* keys;
  a separate limit is a needless special case and a divergence risk. 511 is
  ample (milli's longest DB name is well under 64 bytes).
- *Why:* zero special-casing; consistent with the key-size rule.

### D8 — Format-version policy

A single `u32 format_version` (= 1) in the meta body. Any change that makes an
older reader misinterpret bytes bumps it; open rejects an unknown version with
`MdbError::Invalid` (SPEC 02 §3.2). Reserved fields and behavior-gated features
(data-page checksums, DUPSORT hooks) do **not** bump the version — they are
already carved out — so Phase 3.9/2.8 activation is not a format break for files
that never used them. A Phase-2.8 file that actually *contains* DUP structures is
a different matter: it is simply not a valid Phase-1 file (INV-21 rejects the
hooks), which is acceptable because such a file could only be produced by a
Phase-2.8 build.

- *Alternatives:* semantic major/minor versioning, or feature-flag bitmap in the
  meta. Deferred: a single monotone integer is enough until we actually ship an
  incompatible change; a feature bitmap can be introduced *at* that version bump.
- *Why:* simplest thing that detects an incompatible file at open.

### D9 — Meta CRC coverage = fixed 168-byte content block

The meta CRC covers exactly bytes `[0, 168)` (common header + meta body through
`main_db`); the `meta_crc` field and the reserved tail `[172, psize)` are
excluded and the tail is zeroed. See SPEC 02 §3.3.

- *Alternatives:* CRC the whole page. Rejected: the tail is reserved zeros of
  page-size-dependent length; a fixed content block makes the CRC independent of
  `psize` and unambiguous, and avoids hashing kilobytes of zeros.
- *Why:* deterministic, page-size-independent, cheap.

### D10 — Magic as a 4-byte tag; little-endian everywhere else

`MAGIC` is the 4 ASCII bytes `"ZDB1"` (`5A 44 42 31`) stored as a byte array
(endianness-free). All other multi-byte integer fields are little-endian on all
platforms (restating the binding decision for completeness). See SPEC 02 §0/§1.

- *Alternatives:* an integer magic (endianness-ambiguous), or a big-endian on-
  disk variant (rejected — single-endianness removes a whole class of bugs; the
  primary and dev platforms are all little-endian ARM/x86).
- *Why:* a byte-array tag reads identically regardless of endianness; single-
  endianness keeps the codec trivial.

## Consequences

- **Easier:** Phase 3.10 page shipping and Phase 3.9 checksums become flag flips
  (fields already reserved). 64 K pages need no special-casing (D3). One decode
  entry point keyed on `flags` (D2).
- **Harder / costs:** +16 bytes/page vs a minimal header (D2); +2 bytes/branch
  node vs LMDB packing (D4). Both negligible. Page counts must match LMDB within
  the 1.5 tolerance band, constraining split/threshold choices to parity (D6).
- **Tests/invariants to add:** SPEC 03 INV-1..INV-21 in the check tool (M1.12);
  proptest round-trips for every page/node encode/decode (M1.1); a torn-meta
  recovery test (write meta, corrupt CRC, reopen → older meta wins, M1.2/M1.11);
  CRC32C software-vs-hardware equivalence test (deferred to 3.9).
- **SPEC sections owned elsewhere:** commit ordering & fsync barriers, the
  dirty-page value-borrow contract, and `next_pgno` bookkeeping are SPEC 04/06 —
  this ADR and SPEC 02 only fix the *format* those pipelines read/write, and
  flag the interface assumptions inline (SPEC 02 §3 "SPEC 04 interface note";
  SPEC 02 §7 "SPEC 05 interface note").

## Open questions for human review

1. **GC-key byte order** (SPEC 02 §7): SPEC 02 fixes only that a GC entry is
   `(8-byte txnid key → page-id-list value)`; whether those txnid keys are stored
   little-endian (engine-native) or big-endian (so memcmp scan order = numeric
   order) is left to SPEC 05. If SPEC 05 wants numeric scan order it must store
   them big-endian — the only place an engine-internal key is not little-endian.
   Flag for the SPEC 05 author.
2. **`last_pg` vs `next_pgno` in the meta** (SPEC 02 §3): the format stores
   `last_pg` (highest allocated). If SPEC 04 finds it cleaner to persist
   `next_pgno` directly, that is a meta-field change and must amend SPEC 02 §3 —
   raising it here so the choice is explicit rather than silent.
3. **Header size 32 vs alignment ambition:** 32 bytes keeps `pgno`/`txnid`
   8-byte aligned relative to the page and leaves the body starting at 32. If a
   future decision wants values 8-byte aligned within cells (Phase 3.6 hannoy
   arena), that is an opt-in table type, not a change to the B+tree page — noted
   so 3.6 does not reopen this.

## Amendment — OQ1 & OQ2 resolved by SPEC 04/05 (2026-07-15, milestone 0.4)

OQ1 and OQ2 above are **RESOLVED** by [SPEC 05](../SPEC/05-gc.md) and
[SPEC 04](../SPEC/04-txn-mvcc.md), written in the same milestone. OQ3 remains a
non-blocking note (no reopen needed). No SPEC 02 format field changed as a result
of either resolution.

- **OQ1 — GC-key byte order → BIG-ENDIAN (resolved, SPEC 05 §1 GC-2).** The 8-byte
  txnid key of a GC entry is stored **big-endian**, the sole exception to the
  little-endian-everywhere rule (§D10). Reason: the GC DB is a memcmp-ordered
  B+tree and the reclamation scan (SPEC 05 §6) walks freeing-txnids in **numeric
  ascending** order via a single forward cursor from `first()`; big-endian is the
  only encoding for which memcmp order equals unsigned-numeric order. Consumer keys
  are unaffected (opaque codec bytes). The check tool asserts BE decoding (SPEC 05
  INV-23). SPEC 02 §7 already reserved this choice to SPEC 05; its "SPEC 05
  interface note" is now satisfied.
- **OQ2 — `last_pg` vs `next_pgno` → KEEP `last_pg` in the meta (resolved, SPEC 05
  §5 GC-15).** The writer tracks `next_pgno` (lowest never-allocated page) **in
  memory**; the meta continues to persist **`last_pg = next_pgno − 1`** exactly as
  SPEC 02 §3 defines. **No meta field is added or changed.** At open,
  `next_pgno = meta.last_pg + 1`. This is precisely the assumption SPEC 02 §3's
  "SPEC 04 interface note" permitted, so no §3 table amendment is required.
- **OQ3 — cell value alignment:** unchanged; remains an opt-in Phase-3.6 table
  type, not a B+tree page change. No action.

Both resolutions are **format-neutral**: OQ1 fixes the *encoding of a value the
format already reserved to SPEC 05*, and OQ2 confirms the *existing* meta field.
`format_version` stays 1 (§D8).
