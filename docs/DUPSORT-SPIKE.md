# Duplicate-key databases (DUPSORT): spike report and recommendation

Date: 2026-11-02. Design under test: [ADR-0011](adr/0011-dupsort.md).
Code: the `dupsort-spike` cargo feature, off by default and enabled by no
consumer build.

## Decision

**No-go, accepted by the maintainer on 2026-11-02.** Duplicate keys stay
unsupported (a `DUP_SORT` open is refused with `Incompatible`, as before). The
spike branch is kept, unmerged, as the starting point if a consumer appears. A
reading of milli's databases found no Meilisearch use that would pay for it:
most of the index maps a key to a roaring bitmap, which search intersects
whole, and duplicates would replace that with per-document items.

## Recommendation

**No-go on an implementation now.** Freeze the on-disk layout decisions on paper
before the format is released, and keep the spike branch as the starting point
for when a consumer needs duplicate keys.

- Meilisearch (checked at v1.55) and hannoy open no database with a duplicate
  flag, so this work does not move ZeroDB toward its first goal.
- It is large: about 10,000 lines including tests (below), in LMDB's most
  defect-prone area.
- Its cost to the main paths is small in speed but real in risk (below).
- The layout questions are cheap to settle now and expensive after release.
  The spike shows that ADR-0011's sub-page design fits today's format (version
  2, with the free-list annex) and ADR-0020's smaller page header unchanged,
  so settling them needs no format bump.

On the scorecard: **faster**: no gain for Meilisearch, a branch per operation for
everyone else; **reliable**: lower, until the full fuzz and differential gates
below are met; **durable**: unchanged (no new fsync ordering; sub-tree pages are
ordinary dirty pages); **secure**: a larger decoder surface for hostile files
(the sub-page parser), which needs fuzzing.

## What the spike built

About 720 lines of engine and tool code and 420 lines of tests, all behind the
feature:

- the embedded sub-page codec ([`page/subpage.rs`](../crates/zerodb-core/src/page/subpage.rs)),
  pure slice logic with unit tests;
- the persisted `DUPSORT` bit in the database record (SPEC 02 §3.1 offset 42),
  set at create and kept on reopen and on `clear`;
- `put`, `get` (first duplicate), `delete` (all duplicates) and `len` (pairs)
  for duplicate sets that fit one leaf cell. A larger set is refused with
  `ErrorKind::Unsupported`; there is no sub-tree promotion;
- a logical pair walk, `for_each_pair`, kept separate from the physical cell
  walk `for_each_entry_flagged`;
- a duplicate-comparator slot, registered by the heed adapter's `create` and
  `open`;
- `check` support: sub-page well-formedness, and entry counts per pair.

It did not build: sub-trees, DUPFIXED, the duplicate cursor operations
(`get_both`, `next_dup`, …), `NO_DUP_DATA`/`APPEND_DUP`, `delete_one_duplicate`,
heed's iterators, cursors and range/prefix reads on a duplicate database (they
still yield one raw sub-page per key), `migrate-from-lmdb`, or a DUPSORT main
database.

## The three paths the parked work broke

Three tests, written first and committed failing, cover them:

| Path | Test | Failure before the fix | Fix |
|---|---|---|---|
| Compacting `copy_to_file` | `zerodb-oracle/tests/differential/dup_spike_paths.rs` `compacting_copy_keeps_duplicates` (against LMDB) | The rebuild pushed every cell with node flags `0` and built every record with flags `0`: each duplicate set became one plain value holding sub-page bytes, the database lost `DUPSORT`, and the copy **still passed `check`** | Copy cells as stored, `F_DUPDATA` included; carry the record flags (`EnvStream::named_db_flagged`); count a sub-page's values as entries in the stream builder |
| `dump` then `load` | `zerodb-tools/tests/dup_dump_load.rs` | `dump` wrote one record per cell (raw sub-page bytes) and no flag lines. `load` rejected unknown header lines, and it inserts with `NO_OVERWRITE` ("first record wins"), so with a pair-walking dump it keeps one value per key: the parked symptom | `dump` walks pairs and writes `duplicates=1` and `dupsort=1`, as `mdb_dump` does (vendored `mdb_dump.c`); `load` reads them, creates the database DUPSORT and adds every pair |
| heed `open` with a custom duplicate comparator, after an env reopen | `dup_spike_paths.rs` `custom_dup_comparator_reopens` (against LMDB) | The adapter's `open` lacked heed's `CDUP: Comparator` bound, so it could not register the ordering. The reopened handle sorted new duplicates by memcmp: `get` returned `c` where LMDB returns `d`. With the approved comparator fingerprint, the same omission becomes "cannot reopen", the parked symptom | `open` and `create` take heed's `CDUP: Comparator + 'static` bound and register the duplicate ordering on every open, as heed calls `mdb_set_dupsort` after every `mdb_dbi_open` |

The common cause is that there are **two walks**, physical cells and logical
pairs, and each caller must pick one deliberately. The compacting copy needs
cells, `dump` needs pairs, and `load` needs pair semantics on insert. The parked
work changed the one shared walk, and every caller silently got the other
meaning.

## Other things the change reaches

Found by building the spike, beyond the three paths:

- **`clear` dropped the persistent flags.** It reset the record to
  `DBRecord::empty()`. The spike keeps the flags, as LMDB's `mdb_drop(_, 0)`
  does; SPEC 02 §6.1 now says so (no behavior change today: the flags are
  always 0).
- **The fast `clear`/`drop` walk skips leaves.** With sub-trees it must read
  leaves of DUPSORT databases to free their sub-trees (LMDB's `mdb_drop0` does
  the same). The cost falls on DUPSORT databases only.
- **The leaf decoder's flag mask is global.** Admitting `F_DUPDATA` admits it in
  every database. Whether a database may hold one is a record-level fact, so
  `check` must enforce it, and the decoder must reject `F_DUPDATA | F_BIGDATA`.
- **The builder counts cells, not entries.** `TreeStream` and every
  `stat`/`len` source must count pairs (SPEC 03 §12.1 O2).
- **`check` counts entries per cell** (INV-18) and rejects record flags (INV-21);
  both need the duplicate rule.
- **Invariant IDs collide.** ADR-0011 numbers its duplicate invariants
  INV-22..27, but SPEC 05 uses INV-22..28 for the free list, and both appear in
  `check` output. Format 3's free-list records (ADR-0037) have since taken
  INV-29 as well, so duplicate invariants must start at the next free ID
  (**INV-30** today), and the SPEC should reserve a block for them. The spike
  uses INV-30 for sub-page well-formedness.
- **Every physical-walk caller needs a decision.** The cell walk is used by the
  compacting copy, `dump`, `migrate-from-lmdb` (which also creates databases
  without flags) and the tests that compare content through it. The stream
  builder, `check` and `delete_range` (which counts entries per cell) need the
  same decision.
- **heed's read surface is the bulk of the work.** `iter`, `rev_iter`, ranges,
  prefixes, `get_duplicates` and the write cursor's `put_current`/`del_current`
  all yield or address cells today. `database.rs` and `iterator.rs` in
  `heed-zerodb` hold about 55 public functions, most of them reads over the
  1,880-line B+tree module.
- **Older engines read a DUPSORT file silently.** Open validates neither the
  meta's `env_flags` nor a record's `flags`, and commit rewrites `env_flags` as
  0. An engine without duplicate support opens a DUPSORT database, reads
  single-value keys, and on `put` overwrites where it should add a duplicate.
  Only pages holding a duplicate cell fail, loudly (`F_DUPDATA` is a rejected
  flag).

## Format check

**Format version 2 (meta free-list annex).** The annex starts at meta offset
176, after `main_db` (120..168). Record flags and the DUPFIXED/fingerprint slot
are record-relative (offsets 42..48): in the meta they sit at 162..168, inside
the CRC range `[0, 172)`, and in a named database they sit in the catalog
value. The annex changes nothing the design uses, and the spike reads and
writes DUPSORT records under version 2 with no format change.

**Format version 3 (16-byte free-list keys, ADR-0037)**, merged after the
spike, changes only the free-list tree's keys and values. The record, leaf
cell and meta layouts the design uses are unchanged, so the check above
still holds.

**ADR-0020 (24-byte page header).**

- The sub-page has its own 8-byte header and depends on the page header only
  through the leaf's inline bound (`max_node_size`). For a 16-byte key and
  8-byte duplicates, one sub-page holds 166 values at 4 KiB pages and 2,726 at
  64 KiB, under both the 32- and 24-byte headers (unit test
  `capacity_under_both_header_widths`).
- The one conflict is the `leaf2_ksize` slot at page-header offset 28, which
  ADR-0020 removes. DUPFIXED sub-tree leaves (`P_LEAF2` pages) can take their
  item size from the sub-tree's own record instead (record offset 44, u16), as
  ADR-0020 suggests. A sub-tree is only ever reached through that record, so
  every reader has it.
- The cost: a `P_LEAF2` page can no longer be decoded on its own. The page
  decoder and `fuzz_page_decode` must take the item size as a parameter.

## Preconditions for going ahead

The issue's recorded list, still valid:

1. **Fix the three breakages.** Done in the spike, with tests. A real
   implementation keeps the tests and the two-walk split.
2. **Define the open layout details before the format freezes.** The spike
   proposes:
   - **Sub-page `upper`:** always equal to `lower`. A sub-page carries no free
     space, because every change rewrites the whole leaf cell; the decoder
     rejects any other value. A packed (DUPFIXED) sub-page has no pointer
     array: `lower = count × leaf2_ksize` and `upper = lower`.
   - **Embedded sub-tree record:** the 48-byte record as for a named database,
     with:
     - `root`, `depth`, `branch_pages` and `leaf_pages` of the sub-tree;
     - `overflow_pages = 0` (duplicates never overflow, SPEC 03 §12.1 O1);
     - `entries` = this key's duplicate count;
     - `flags = 0` (the parent record holds the database flags);
     - `leaf2_ksize` = the DUPFIXED item size;
     - `cmp_fingerprint = 0` (the parent's applies).

     The parent record's page counters do not include sub-tree pages
     (SPEC 03 §12.1 O2).
3. **Renumber the duplicate invariants** from the next free ID (INV-30 today), and
   reserve a block for them in the SPEC.
4. **Fix the oracle's iteration blind spot.** The harness stops at the first
   error in both engines, so "errored, then continued differently" compares as
   equal.
5. **Run both differential fuzz pairs for 600 s** (not 180 s), plus a
   duplicate-aware `fuzz_page_decode` for the sub-page parser.

Found by the spike:

6. **Refuse unknown persistent flag bits at open, before release.** Make open
   reject a record (and the meta's `env_flags`) carrying flag bits the engine
   does not implement, with `Incompatible`. Without it, adding DUPSORT later
   needs a format version bump so that older engines refuse DUPSORT files.
   With it, no bump is needed.
7. **Give comparators a stable identity before persisting a fingerprint.** The
   approved fingerprint (record offset 46) would be computed from comparator
   names. The heed adapter names a comparator by `std::any::type_name`, which
   Rust documents as unstable across compiler versions, so a toolchain upgrade
   could make a database impossible to reopen. The fingerprint needs a stable
   name, or it should record only "custom ordering present".
8. **Measure the main paths.** Put a three-column `just bench-ab` comparison
   (LMDB, ZeroDB before, ZeroDB after) on the standard suite in front of the
   merge. The spike's `get` looks the record up twice for its DUPSORT test; a
   real implementation must carry the flag on the tree handle it already
   builds.

## Size of the full work on today's format

The estimate is extrapolated from the spike: the spike's ~720 lines cover
point operations on sub-page-sized sets; stages are as in ADR-0011.

| Part | Engine and tools | Tests |
|---|---:|---:|
| Layout freeze in SPEC 02/03; flags; sub-page sets; `NO_DUP_DATA`, `APPEND_DUP`, `delete_one_duplicate`; `check`, copy, `dump`/`load`, `migrate-from-lmdb`; heed `open`/`create` | 1,500 | 1,000 |
| Sub-tree promotion and demotion; `clear`/`drop`/`delete_range` through sub-trees; stat rules | 1,200 | 800 |
| Duplicate cursor (ADR-0011's sub-cursor model), the eleven duplicate cursor operations, heed's iterators and write cursor on duplicate databases | 1,500 | 1,000 |
| DUPFIXED: packed pages and sub-pages, `GET_MULTIPLE`/`MULTIPLE`, FFI differential | 800 | 500 |
| Oracle op model, fuzz targets, crash workload, iteration blind-spot fix, 600 s runs | 800 | |
| **Total** | **≈ 5,800** | **≈ 3,300** |

This is consistent with the parked first stage, which was about 4,300 lines
over 46 files and covered roughly the first row.

## Cost to the engine's main paths

- **Speed, databases without duplicates.** The extra work is:
  - one test of the record's `DUPSORT` bit per `put`, `get` and `delete` (the
    record is already loaded);
  - one test of the cell's `F_DUPDATA` flag per cursor step (the cell header is
    already read).

  Copy, dump and load are unchanged for them. This is expected to be within
  noise but is **not measured** (precondition 8).
- **Risk.**
  - Every shared walk gains a physical-or-logical choice: the parked failures
    were exactly that, with every test green.
  - The leaf decoder starts admitting `F_DUPDATA` for every file.
  - The cursor gains a second level whose validity must be re-derived after
    each mutation (ADR-0011's sub-cursor rule).
  - Sub-tree promotion and demotion is the known defect seam in LMDB and
    libmdbx.

  The spike's tests cover the three paths above, not these.

## Verification (macOS aarch64, 2026-11-02)

**Passed:**
- `cargo fmt --check`;
- `cargo clippy -D warnings` with the feature off and on, on the host and on
  `x86_64-unknown-linux-gnu`;
- `cargo test --workspace` with the feature off;
- the `zerodb-core`, `zerodb` and `zerodb-tools` suites and the two oracle
  differentials with the feature on;
- the sub-page codec tests under miri.

**Not run:** the differential fuzz, the crash harness and benchmarks. The
spike is not proposed for merging, and its default-build changes are limited
to:
- `clear` keeping record flags (always 0 today);
- a flags argument on internal create and copy paths (passed 0);
- heed's `CDUP: Comparator` bound on the adapter's `open`/`create`.

## Running the spike

```sh
cargo test -p zerodb-core --features dupsort-spike subpage
cargo test -p zerodb-oracle --features dupsort-spike --test differential dup_spike_paths
cargo test -p zerodb-tools --features dupsort-spike --test dup_dump_load
```
