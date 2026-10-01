# ADR-0020: Shrink the 32-byte common page header

- Status: Spike approved (Quentin 2026-10-01: "test version first"); the format change itself is not approved — it waits for the spike's numbers
- Milestone: Phase 3 (performance / on-disk format); **STOP zone — on-disk
  format change, human approval required before any implementation**
- Date: 2026-10-01

## Context

ZeroDB's common page header is 32 bytes (SPEC 02 §2); LMDB's (the vendored fork,
`lmdb-master-sys 0.2.6`, `PAGEHDRSZ = offsetof(MDB_page, mp_ptrs)`) is **16**:
8-byte `pgno`, 2-byte `mp_pad`, 2-byte `mp_flags`, 4-byte lower/upper union.
ZeroDB's extra 16 bytes are `txnid` (8, the writer stamp — ADR-0018 keys its
validation cache on `(pgno, stamp)`; INV-20; Phase 3.10 page shipping),
`reserved0` (2), `checksum` (4, reserved for Phase 3.9, always 0 today), and a
wider variant tail (the `leaf2_ksize`/`reserved1` slots).

A header 2× LMDB's quantizes the leaf body more coarsely, so each page holds
slightly fewer entries and the file is larger.

**Measured trigger (2026-10-01, public AWS Graviton4 suite, rust-storage-bench
"medium": 100M × (16 B key + 200 B value), APPEND-loaded in one txn, 4 KiB
pages, 8 GB cap, Zipfian 95/5):**

- File: ZeroDB 23,297 MiB vs LMDB 21,841 MiB (**+6.7 %**), already after the load.
- Throughput: **0.86×** LMDB on NVMe, **0.91×** on EBS io2; ~10 % more device
  bytes written.
- Cause is header quantization alone. The entry costs 8 (node hdr) + 16 + 200 +
  2 (slot) = 226 B. ZeroDB leaf body 4096 − 32 = 4064 → ⌊4064/226⌋ = **17**;
  LMDB 4096 − 16 = 4080 → ⌊4080/226⌋ = **18**. 18/17 = **+5.9 %** leaves (plus a
  matching fan-out loss in branches). Both file sizes match this arithmetic.

This is the only place in the engine where a fixed constant (`HEADER_SIZE`) sits
between us and LMDB's density, and the medium point lands exactly on a
quantization cliff. SPEC 02 §0 already chose body-relative offsets specifically
so a smaller header stays possible without the `PAGEBASE` hack; this ADR asks
whether to take that option.

## Options

### Option A — Keep 32 bytes (status quo)
Gains: nothing changes; `checksum` (Phase 3.9) and the DUPFIXED `leaf2_ksize`
slots keep a reserved home; no format bump. Breaks: nothing. Loses: the measured
density gap persists (+6.7 % file, ~0.86× on write-bound workloads at the cliff
sizes). LMDB does not pay it (16-byte header); libmdbx widens its *meta* but
keeps a 20-byte page header (`sizeof(bytes) = PAGEHDRSZ`; from public mdbx docs,
**not** verified against a vendored source here — treat as unverified).

### Option B — 24 bytes, keep `pgno` + `txnid`
Layout: `pgno` u64 @0, `txnid` u64 @8, `flags` u16 @16, `lower` u16 @18,
`upper` u16 @20, 2 spare @22; overflow reuses @18.. for `ovf_pages` u32. Drops
the standalone `checksum` slot and the wide reserved tail.
- **Gains:** recovers the **entire** measured regression. At the medium point a
  24-byte header gives ⌊(4096−24)/226⌋ = **18** entries/leaf, identical to
  LMDB's 18 (see table): the cliff that costs 5.9 % is *fully* on the 32→24
  step, not the 24→16 step. Keeps `txnid` (ADR-0018 cache, INV-20, Phase 3.10)
  and `pgno` (INV-4 self-identification) intact.
- **Breaks:** Phase 3.9 data-page CRC loses its reserved slot. It would then
  need its own home — a later `format_version` bump that widens the header back
  (self-defeating), or a per-page CRC stored *in the variant tail of the pages
  that opt in* / in a side structure. **Recommendation if B is taken: declare
  Phase 3.9 data-page checksums a format-bump feature** (they are already gated
  and unimplemented), since a checksum is pointless without also being written,
  i.e. it never ships "for free" anyway. The DUPFIXED `leaf2_ksize` header slot
  also disappears; Phase 2.8 is parked (M2.8a) and, when it lands, `leaf2_ksize`
  can live in the DBRecord (offset 44, already reserved) and be re-derived per
  page, so no header slot is strictly required.
- LMDB: n/a (LMDB packs the same fields into 16 by giving `pgno` the only u64
  and overlapping lower/upper with `pb_pages`; B keeps an 8-byte `txnid` LMDB
  lacks, hence 24 not 16).

### Option C — 16 bytes (LMDB-identical width)
To reach 16 with an 8-byte `pgno` + 2 `flags` + 4 lower/upper leaves only 2
bytes — not enough for an 8-byte `txnid`. So C forces dropping **either**
`txnid` **or** `pgno`:
- Drop `txnid`: breaks ADR-0018's `(pgno, stamp)` validation cache (the central
  Phase-3 read-path win — it would have to fall back to per-txn memos, undoing
  the measured search speedup), breaks INV-20, and removes the Phase 3.10
  page-shipping stamp. Not acceptable without re-opening ADR-0018.
- Drop `pgno`: breaks INV-4 self-identification (the check tool's
  `pgno == offset/psize` assertion, SPEC 02 §2 line 104) and the security-review
  geometry checks that lean on a page naming itself. LMDB keeps `pgno` and drops
  our `txnid` — it simply never had a stamp — so "be like LMDB" means C-drop-
  txnid, i.e. revert ADR-0018. Gains over B: one more entry/leaf only at the
  *narrow* 24→16 cliffs (mean +0.28 % at 4 K, table below) — small, and bought
  by discarding a measured read-path optimization. **Not recommended.**

## Quantified gain

Extra leaves a larger header forces, as a percentage, swept over inline entry
sizes (16 B key, value 0–2000 B, even-padded cell + 2 B slot; leaves ∝
1/entries-per-leaf):

| psize | 32 vs 16 mean / worst | 32 vs 24 mean / worst | 24 vs 16 mean / worst |
|------:|----------------------:|----------------------:|----------------------:|
| 4 KiB | +0.45 % / +50 %       | +0.17 % / +50 %       | +0.28 % / +50 %       |
| 8 KiB | +0.17 % / +25 %       | +0.04 % / +16.7 %     | +0.12 % / +25 %       |
| 16 KiB| +0.11 % / +12.5 %     | +0.05 % / +11.1 %     | +0.07 % / +12.5 %     |

**Reading the table honestly:** the *mean* gain is tiny — an extra 16 B is
~0.4 % of a 4 KiB page, and most entry sizes land mid-page where header width is
rounding noise. The benefit is **concentrated at cliffs**, where an entry size
divides the body such that one more entry just fits. The worst-case +50 % is a
real but **narrow** window (at 4 K, 16 B key it is only `dsize` 1329–1334, a
6-byte-wide band where 3 entries fit at 16 B but 2 at 32 B). Larger pages shrink
every column — a consumer chasing density can also just raise `page_size`
(public knob since M2.6).

**Where common sizes sit.** 4 KiB cliffs (16 B key) where 32→16 buys a whole
entry: total entry ≈ 193, 203, **225 (the medium bench, +5.9 %)**, 239, 271,
339, 369, 407, 451, 509, 581 B — i.e. values roughly 160–560 B. Below ~150 B and
in the mid-ranges between cliffs the gain is ~0 %.

**Meilisearch / hannoy estimate (4 KiB, representative inline points):**

| case | entries/leaf @32 → @16 | 32 vs 16 |
|------|-----------------------:|---------:|
| word-docids tiny roaring (12 B key, 24 B val) | 81 → 81 | +0.0 % |
| small roaring (12 B, 64 B) | 50 → 50 | +0.0 % |
| **roaring ~200 B (12 B, 200 B)** | 18 → 19 | **+5.6 %** |
| roaring 512 B (12 B, 512 B) | 7 → 8 | +14 % |
| roaring near inline-max (12 B, 1800 B) | 2 → 2 | +0.0 % |
| hannoy quantized vec (8 B, 128 B) | 27 → 27 | +0.0 % |
| hannoy small (8 B, 32 B) | 81 → 81 | +0.0 % |

Milli roaring-bitmap values are **size-distributed**, so a real index is a
*mixture*: the mass sitting in the 160–560 B cliff band gets 5–14 %, the small-
and large-tail entries get ~0 %, and anything above the inline threshold
(> 2030 B at 4 K) goes to an **overflow run** where the header is one page
amortized over the whole value — negligible. A blended index gain is therefore
**well under the per-cliff numbers** and depends entirely on the index's value-
size histogram; it is not safe to quote a single figure without measuring a real
milli dump. hannoy's fixed-size vectors land wherever their (dim × dtype) size
falls — a single point, either on a cliff or not, knowable exactly per model.

## Blast radius (surveyed)

- **`HEADER_SIZE`**: one definition (`crates/zerodb-core/src/page/mod.rs:60`),
  **68 occurrences across 10 files, all in `zerodb-core`** — page/tree.rs (33),
  page/geometry.rs (10), page/overflow.rs (7), page/header.rs (5), btree.rs (4),
  rwtxn.rs (3), page/mod.rs (3), and 3 test files (1 each). It is a real
  constant everywhere; **no magic 32 is hardcoded** in `zerodb-tools`,
  `zerodb-oracle`, `fuzz`, or `heed-zerodb` (0 references — they go through
  core's accessors). This is the single most important finding: the width is
  genuinely centralized.
- **Header field offsets** (`page/header.rs`): `OFF_FLAGS=16`, `OFF_RESERVED0=18`,
  `OFF_CHECKSUM=20`, `OFF_VARIANT=24` all move under B/C. Variant tail
  (`page/tree.rs`): `OFF_LOWER=24`, `OFF_UPPER=26`, `OFF_LEAF2_KSIZE=28`,
  `OFF_RESERVED1=30` all move (and `leaf2_ksize`/`reserved1` are dropped under
  B). `page/overflow.rs`: `OFF_OVF_PAGES = OFF_VARIANT` moves. ~12 offset
  constants to re-pin, all in `zerodb-core::page`.
- **Meta page layout moves.** `page/meta.rs` meta-body offsets are absolute:
  `OFF_MAGIC=32 … OFF_META_CRC=168` and `META_CONTENT_LEN=168`. If the header
  shrinks to 24 the whole meta body shifts down 8 bytes (magic→24, CRC→160) and
  `META_CONTENT_LEN` changes; the CRC byte range (§3.3), the creation protocol
  (§3.4) and the worked example (§3.5) all change. DBRecord offsets are
  record-relative and are unaffected. This is the largest spec-churn area.
- **Format version / existing files.** `FORMAT_VERSION = 1` is referenced in 6
  files (rwtxn, page/mod, page/meta, builder, zerodb-tools/lib, zerodb/copy).
  A header change is an **incompatible** format change → bump to `2`; open
  already rejects a mismatched version (`§3.2` step 2 → `MdbError::Invalid`), so
  old files are refused cleanly. The project is **pre-release** and migration is
  logical (dump/load, D-002), so there is **no in-place migration to build** —
  but every existing bench DB, corpus image, and test fixture must be
  **recreated**, not converted.
- **Fuzz corpora.** `fuzz/corpus/fuzz_image_open` and the `fuzz_page_decode`
  target embed raw file/page images with 32-byte headers; these corpora are
  invalidated and must be regenerated (the `diff_ops` corpus is structured
  operations and is unaffected).
- **SPEC / INV.** SPEC 02 §1 (`HEADER_SIZE`, `META_CONTENT_LEN`), §2 and §2.1
  (the two layout tables), §2.2 and §4.x worked examples (every absolute byte
  offset), §3 (meta body table + example), §4.2 `max_node_size` formula and its
  worked numbers, §5 overflow `capacity(N)`/`N` formulas and example, §9
  alignment summary; SPEC 03 (byte-offset examples). INV-20 is preserved under
  B, broken under C-drop-txnid; INV-4 broken under C-drop-pgno. ADR-0002 §D3/§D8
  and this ADR must be cross-linked.

## Spike plan (CLAUDE.md rule 7 — smallest risky-assumption test first)

The riskiest assumption is **"the file-size win survives on a real workload and
the read/write hot paths don't regress from the re-pinned offsets."** The
cheapest test of it, **before any `format_version` or meta-move work**:

1. Change `HEADER_SIZE` to 24 and the variant-tail offsets in `zerodb-core::page`
   only; leave the meta body where it is for the spike by keeping an 8-byte pad
   between header and meta magic (so meta code is untouched — the spike does not
   claim a shippable layout, only measures density + hot-path cost).
2. Run the standard A/B (LMDB vs ZeroDB-before vs ZeroDB-after) on the medium
   rung plus a milli-dump-shaped case, and the `get/*` / YCSB ladders, on the
   public bench host. Confirm file size drops to ≈ LMDB and that scan/get/put
   rungs are flat.

**Abandon the spike if:** the measured file-size drop on a *real milli dump* (not
the synthetic cliff point) is under ~1–2 %, or any hot-path rung regresses from
the new offsets, or the meta-move turns out to force a `max_node_size`/overflow
boundary shift that moves the inline/overflow decision for common values (a
behavior change, not just a size change). If it survives, *then* do the real
layout (meta body at 24, `format_version = 2`, regen corpora/fixtures, spec
rewrite) as a second, separately-reviewed change.

## Recommendation (for the human)

1. If anything is done, do **Option B (24 bytes)**, not C: B recovers the full
   measured regression (the medium cliff is entirely on the 32→24 step) while
   keeping `txnid` and `pgno`, so ADR-0018, INV-20 and INV-4 all stand. C buys
   almost nothing more and would revert a measured read-path win.
2. The **mean** gain is small and cliff-concentrated; the honest case for B is
   "match LMDB's file size on write-bound bulk loads and remove a visible
   benchmark gap," not "a broad speedup." Whether the medium cliff alone
   justifies a format change is a **judgment call for a maintainer** — it is one
   synthetic point; a real milli-dump measurement (the spike) should gate it.
3. If approved, treat Phase 3.9 data-page CRC as a future format-bump feature
   (it never shipped free) and plan to source DUPFIXED `leaf2_ksize` from the
   DBRecord, not the header.
4. **Do not bundle** this with any other format change (CLAUDE.md rule 7's
   explicit warning). Shrinking the header is already a full format-version bump
   touching the meta layout and all corpora; a second orthogonal change riding
   along is how a stage becomes 4,000 lines.

## Open questions for human review

- Is a cliff-concentrated, histogram-dependent file-size win (measured +6.7 % at
  one synthetic point, likely low single digits blended on a real index) worth a
  `format_version` bump and recreating every bench/fixture, pre-release?
- Does Phase 3.9 data-page CRC have a committed design that *needs* the reserved
  header slot? If yes, B costs it a home and the trade changes.
- Should this wait and ride the next *mandatory* format bump (if one is already
  foreseen), rather than spend a bump on header width alone?
- libmdbx's 20-byte page header is cited from public docs, not a vendored
  source — worth confirming against an actual mdbx tree before using it as prior
  art in the decision.
