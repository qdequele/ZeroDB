# SPEC 02 — On-disk page formats

Status: **DONE** — 2026-07-15 (milestone 0.4). Source of truth for the encode/
decode implementation in `zerodb-core::page` (M1.1) and the meta protocol in
M1.2. **This is our own format** (D-002): it is *not* byte-compatible with LMDB
0.9/1.0. Migration between LMDB and ZeroDB is logical (dump/load), never by
sharing a file. Algorithms that consume these layouts live in
[SPEC 03](03-btree.md); the format-defining rationale is
[ADR-0002](../adr/0002-on-disk-format.md).

Reading note: the LMDB fork sources were read to understand *what information a
page must carry and why* (clean-room, CLAUDE.md rule 4). The layouts below are
ZeroDB's own design; where a field mirrors an LMDB idea it is called out, but no
LMDB struct is transliterated.

---

## §0 — Conventions binding the whole document

- **Endianness: little-endian only.** Every multi-byte integer field is stored
  little-endian, on every target platform. There is no big-endian on-disk
  variant. (Consumer *keys* are frequently big-endian — SPEC 00 row 52 — but
  that is codec-level key content, opaque to the engine; it does not change how
  the engine stores its own integer fields.)
- **No `#[repr(C)]` casting of possibly-unaligned data** (CLAUDE.md unsafe
  policy). Every field is defined by an explicit byte **offset** and **width**
  and is read/written with `read_unaligned`-style accessors. The layouts below
  are chosen to keep the load-bearing integers **naturally aligned relative to
  the page start** (u64 fields on 8-byte offsets, u32 on 4-byte, u16 on 2-byte)
  so that, because pages themselves are page-size-aligned in the mmap, the
  common-header reads are in practice aligned; the code must nonetheless never
  *rely* on it and must use unaligned accessors. Cell (node) contents are only
  2-byte aligned and MUST use unaligned reads for their u32/u64 fields.
- **Page size** is a runtime value `psize`, chosen at env creation, a power of
  two in `[4096, 65536]` (4 KiB – 64 KiB). It is stored in the meta page and is
  **independent of the OS page size** (ARM distros often use 64 KiB OS pages;
  the DB page size is its own knob). Phase 1 exposed no selector through heed
  (SPEC 01 §S4). **Milestone 2.6 (2026-07-20) promotes it to a public knob:**
  `zerodb::EnvOpenOptions::page_size(u32)` and the new
  `heed_zerodb::EnvOpenOptions::page_size(u32)` (a ZeroDB extension — heed/LMDB
  0.9 have no equivalent). Bounds are the constants `zerodb::MIN_PAGE_SIZE` /
  `MAX_PAGE_SIZE`; a value outside them, or not a power of two, is rejected at
  `open` with `Io(InvalidInput)`. Selection applies **only when creating** a
  store — reopening adopts the persisted `psize` from the meta page (§3.2) and
  silently ignores the request, exactly as for `map_size`.
- **Page number** (`pgno`) is a `u64` index into the file: byte offset of a
  page = `pgno * psize`. `PGNO_INVALID = u64::MAX` (`0xFFFF_FFFF_FFFF_FFFF`)
  denotes "no page" (empty tree / end of chain).
- **Body-relative offsets.** All intra-page offsets (the node-pointer array and
  the `lower`/`upper` free-space bounds) are measured **from the first byte
  after the common header** (i.e. from absolute offset `HEADER_SIZE = 32`), not
  from the page start. This is a deliberate divergence from LMDB (which
  measures from the page start and needs the `PAGEBASE` hack to represent a
  65536-byte page in a `u16`). With body-relative `u16` offsets the largest
  representable offset is `65536 − 32 = 65504`, so 64 KiB pages need no special
  case. See ADR-0002 §D3.

---

## §1 — Constants

| Name | Value | Meaning |
|------|-------|---------|
| `MAGIC` | bytes `5A 44 42 31` (ASCII `"ZDB1"`) | 4-byte file identifier, stored as a byte array (endianness-free). |
| `FORMAT_VERSION` | `1` (`u32`) | On-disk format version. Bumped only on an incompatible change (ADR-0002 §D8). |
| `HEADER_SIZE` | `32` | Size of the common page header, bytes. |
| `PGNO_INVALID` | `0xFFFF_FFFF_FFFF_FFFF` | Sentinel "no page". |
| `META_A_PGNO` / `META_B_PGNO` | `0` / `1` | The two fixed meta-page slots. |
| `FIRST_DATA_PGNO` | `2` | Lowest page number a tree/overflow page may occupy. |
| `MAX_KEY_SIZE` | `511` | Max key length in bytes (Phase 1 parity, SPEC 01 §S4). Empty key (len 0) is invalid. |
| `MAX_DATA_SIZE` | `0xFFFF_FFFF` (`u32::MAX`) | Max value length in bytes (~4 GiB), matches LMDB `MAXDATASIZE`. |
| `MAX_DB_NAME` | `511` | Max named-DB name length (a catalog key; = `MAX_KEY_SIZE`). ADR-0002 §D7. |
| `FILL_THRESHOLD_PERMILLE` | `250` | 25.0 % — below this a page is a merge/borrow candidate (SPEC 03). |
| `MIN_KEYS_LEAF` | `1` | Min entries a non-root leaf may hold after delete. |
| `MIN_KEYS_BRANCH` | `2` | Min children a non-root branch may hold. |
| `META_CONTENT_LEN` | `168` | Number of leading bytes of a meta page covered by its CRC (see §3). |

**Page-type flags** (`u16`, in the common header `flags` field; a page has
exactly one of the first four structural bits set):

| Bit | Name | Meaning |
|-----|------|---------|
| `0x0001` | `P_LEAF` | Leaf page (key → value entries). |
| `0x0002` | `P_BRANCH` | Branch (internal) page (separator key → child pgno). |
| `0x0004` | `P_OVERFLOW` | Overflow page: head of a contiguous run holding one large value. |
| `0x0008` | `P_META` | Meta page (slots 0 and 1 only). |
| `0x0020` | `P_LEAF2` | **Reserved, Phase 2.8** — DUPFIXED packed-key leaf. Never set in Phase 1. |
| `0x0040` | `P_SUBP` | **Reserved, Phase 2.8** — DUPSORT embedded sub-page. Never set in Phase 1. |

Bits `0x0010`, `0x0080`–`0x8000` are reserved and MUST be zero in Phase 1. (In-
memory-only markers such as LMDB's `P_DIRTY`/`P_LOOSE`/`P_KEEP` are **not**
on-disk state in ZeroDB; dirty tracking lives in the txn, SPEC 04.)

---

## §2 — Common page header (32 bytes)

Every page — leaf, branch, overflow, meta — begins with this fixed 32-byte
header. A meta page's `P_META`-specific content follows it (§3); tree pages use
the variant tail (§2.1).

| Off | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 0 | 8 | `pgno` | u64 | This page's own number. Self-identifying; the check tool asserts `pgno == file_offset / psize` (INV-4). |
| 8 | 8 | `txnid` | u64 | txnid of the write transaction that last wrote this page (the "writer stamp", LMDB-1.0 idea). Feeds Phase 3.10 incremental page shipping. Always `≤` the live meta's txnid (INV-20). |
| 16 | 2 | `flags` | u16 | Page-type bitfield (§1). |
| 18 | 2 | `reserved0` | u16 | Reserved, MUST be 0 in Phase 1. |
| 20 | 4 | `checksum` | u32 | **Data-page** CRC32C over the page body — **Phase 3.9**. MUST be written as `0` and ignored on read in Phase 1. (The *meta* page uses a separate mandatory CRC in §3; this field stays 0 even on meta pages.) |
| 24 | 8 | *variant tail* | — | Interpreted per page type; see §2.1. |

`checksum` deliberately occupies a fixed slot now so enabling Phase 3.9 is a
behavior flag, not a format-version bump.

### §2.1 — Variant tail (offsets 24–31)

**Branch / Leaf pages** — free-space bounds (both *body-relative*, §0):

| Off | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 24 | 2 | `lower` | u16 | Body-relative offset of the end of the node-pointer array. `num_keys = lower / 2`. Grows **up** as entries are added. |
| 26 | 2 | `upper` | u16 | Body-relative offset of the start of the cell heap. Cells grow **down** from `psize − HEADER_SIZE`. Free space = `upper − lower`. |
| 28 | 2 | `leaf2_ksize` | u16 | **Reserved, Phase 2.8** (DUPFIXED fixed key size). MUST be 0 in Phase 1. |
| 30 | 2 | `reserved1` | u16 | Reserved, MUST be 0. |

**Overflow pages** — run length instead of bounds:

| Off | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 24 | 4 | `ovf_pages` | u32 | Number of contiguous pages in this overflow run, `≥ 1`. The head page carries this; interior pages of the run are raw payload and are never decoded independently. |
| 28 | 4 | `reserved2` | u32 | Reserved, MUST be 0. |

**Meta pages** — bytes 24–31 are `reserved` (MUST be 0) and are covered by the
meta CRC; the meta body proper begins at absolute offset 32 (§3).

### §2.2 — Node-pointer array and cell heap (branch/leaf)

Immediately after the header (absolute offset `HEADER_SIZE = 32`, body-relative
0) lies the **node-pointer array**: `num_keys` entries of `u16`, each a
body-relative offset to a cell, **kept sorted by key ascending** (pointer[i]
points at the i-th smallest key). The array grows upward; `lower` is its end.

Cells (nodes) are packed in the **cell heap** growing downward from the body
end. `upper` marks the lowest occupied cell. Each cell is padded so its length
is a multiple of 2 (2-byte cell alignment; matches LMDB's `EVEN`). Node bodies
use unaligned reads for their u32/u64 fields (§0).

```
absolute offset:
0        32                 32+lower        32+upper                   32+bodysize
| header | ptr0 ptr1 ... ptrN |  free space  | cellK ... cell1 cell0 |
           (u16 each, sorted)                 (heap, grows downward)
```

---

## §3 — Meta page (slots 0 and 1)

Pages 0 and 1 are the two meta slots. A write transaction with id `N` writes
slot `N & 1` (double buffer; SPEC 04 owns the commit sequence, this doc owns the
format and the selection rule §3.2). The meta page begins with the common
header (§2) with `flags = P_META`, `pgno = 0` or `1`, and `txnid` = the txnid
this meta commits. Bytes 18–31 of the header are `reserved` (0). The **meta
body** begins at offset 32:

| Off | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 0 | 8 | *(common header `pgno`)* | u64 | 0 or 1. |
| 8 | 8 | *(common header `txnid`)* | u64 | Commit point of this meta (duplicated at body offset 64 for a uniform decode; both MUST agree — INV-2). |
| 16 | 2 | *(common header `flags`)* | u16 | `P_META` (0x0008). |
| 18 | 14 | reserved | — | MUST be 0; covered by the meta CRC. |
| 32 | 4 | `magic` | \[u8;4] | `MAGIC` = `5A 44 42 31`. |
| 36 | 4 | `format_version` | u32 | `FORMAT_VERSION`. |
| 40 | 4 | `page_size` | u32 | `psize` (4096–65536, power of two). The authoritative record of the DB's page size. |
| 44 | 4 | `env_flags` | u32 | Persistent env flags (reserved; 0 in Phase 1). Durability flags (SPEC 01 §S6) are *runtime* open flags, not persisted here in Phase 1. |
| 48 | 8 | `map_size` | u64 | Configured map size in bytes (SPEC 00 row 20; `Env::info().map_size`). See §5. |
| 56 | 8 | `last_pg` | u64 | Highest page number allocated as of this txn (file high-water). Next allocation starts at `last_pg + 1`. See "SPEC 04 interface" note below. |
| 64 | 8 | `txnid` | u64 | Commit txnid (== header `txnid`). |
| 72 | 48 | `free_db` | DBRecord | Root/stats of the free (GC) DB — `FREE_DBI`. §3.1. |
| 120 | 48 | `main_db` | DBRecord | Root/stats of the main/catalog DB — `MAIN_DBI`. §3.1. |
| 168 | 4 | `meta_crc` | u32 | **Mandatory CRC32C** over bytes `[0, 168)` of this page (see §3.3). |
| 172 | psize−172 | reserved | — | MUST be 0; **not** covered by the CRC. |

> **SPEC 04 interface note.** This layout fixes the fields the commit pipeline
> reads/writes: `txnid`, `last_pg`, `map_size`, `free_db.root`, `main_db.root`.
> Whether the writer tracks `next_pgno` as a separate in-memory value and
> persists `last_pg = next_pgno − 1` (this doc's assumption) versus persisting
> `next_pgno` directly is a SPEC 04 choice; the *format* stores `last_pg` (last
> allocated), and SPEC 04 must not add a persisted meta field without amending
> this table. The commit **ordering** (write data pages → fsync → write meta →
> fsync) and the write-txn value-borrow contract are SPEC 04/06, not here.

### §3.1 — DBRecord (48 bytes)

A DBRecord captures one B+tree's root and statistics. Two live in every meta
(`free_db`, `main_db`); further named DBs get one DBRecord each, stored as the
*value* of their catalog entry in the main DB (§6, M1.6).

| Off | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 0 | 8 | `root` | u64 | Root page number, or `PGNO_INVALID` for an empty tree. |
| 8 | 8 | `branch_pages` | u64 | Count of branch pages in the tree. |
| 16 | 8 | `leaf_pages` | u64 | Count of leaf pages. |
| 24 | 8 | `overflow_pages` | u64 | Count of overflow pages (sum of all runs). |
| 32 | 8 | `entries` | u64 | Number of key/value pairs (`Database::len`, SPEC 00 row 39). |
| 40 | 2 | `depth` | u16 | Tree height (0 = empty, 1 = root-is-leaf). |
| 42 | 2 | `flags` | u16 | Persistent DB flags — **reserved, Phase 2.8** (D-004; DUPSORT/INTEGERKEY/… land here). 0 in Phase 1. |
| 44 | 4 | `leaf2_ksize` | u32 | **Reserved, Phase 2.8** (DUPFIXED). 0 in Phase 1. |

### §3.2 — Double-buffer selection (open protocol)

At env open the engine reads both slots and validates each independently:

1. `magic == MAGIC`, else the file is not a ZeroDB env → `MdbError::Invalid`
   (SPEC 00 row 56).
2. `format_version == FORMAT_VERSION`, else `MdbError::Invalid` (version
   mismatch; ADR-0002 §D8).
3. `page_size` is a power of two in `[4096, 65536]`, else `MdbError::Invalid`.
4. Header `txnid` (offset 8) equals body `txnid` (offset 64), else the slot is
   inconsistent and is **discarded** (INV-2). This guards a torn write that
   updated one copy but not the other.
5. `meta_crc` matches the recomputed CRC32C over `[0, 168)` (§3.3). A slot that
   fails the CRC is **torn** and is discarded.

This numbered list is the **single owner** of the meta-slot validation
predicate; SPEC 06 REC-1 references it rather than restating it.

Selection among the *CRC-valid* slots:

- Normal open: pick the slot with the **higher `txnid`** — the most recent
  committed snapshot — and use its `main_db.root` / `free_db.root`.
- If exactly one slot is CRC-valid (the other torn by a power-cut mid-write),
  the valid one wins **regardless of txnid** — this is the torn-meta recovery
  guarantee (PLAN §1.2). Because a writer only ever overwrites the *older* slot,
  the surviving slot is always a complete, consistent earlier snapshot.
- If **both** slots fail validation → `MdbError::Invalid` (unrecoverable;
  M1.11 must never produce this from a single torn write).
- `PREV_SNAPSHOT` (SPEC 01 §S5, SPEC 00 row 9): pick the **lower-txnid** valid
  slot instead — milli's `Index::rollback`. Formally the selected index is
  `(txnid[0] < txnid[1]) XOR prev_snapshot`, restricted to CRC-valid slots.
- **Exactly one valid slot + `PREV_SNAPSHOT`**: this layer reports the single
  valid slot as a typed one-valid outcome regardless of the flag; the **env
  layer** (M1.2) maps that combination to `MdbError::Invalid` per SPEC 06
  REC-2† (ratified 2026-07-16) — there are not two committed snapshots to
  identify an older from.

### §3.3 — CRC32C coverage (exact byte range)

`meta_crc` = CRC32C (Castagnoli, polynomial `0x1EDC6F41`, reflected input/
output, init `0xFFFFFFFF`, final XOR `0xFFFFFFFF`) computed over **exactly the
first `META_CONTENT_LEN = 168` bytes of the meta page** — absolute offsets
`[0, 168)`. That range covers the common header (including the reserved bytes
18–31, which MUST be 0) and the meta body through the end of `main_db`. The
`meta_crc` field itself (offset 168) and the reserved tail (`[172, psize)`) are
**excluded**. Reserved bytes inside the covered range MUST be zero so the CRC is
deterministic. Implementation of CRC32C is ADR-0002 §D1 (software table in
Phase 1; ARMv8 `crc32c` instructions in Phase 3.9).

### §3.4 — Env-creation protocol (both slots initialised, empty DB)

Creating a new env writes **both** meta slots (pages 0 and 1) as valid, identical
**empty** metas at **txnid 0**, then fsyncs, before the env is usable:

1. Allocate the file to `2 * psize` bytes (the two meta slots only; no data page).
2. Write slot 0 and slot 1 identically: `txnid = 0`, `magic`, `format_version`,
   `page_size`, `map_size`, `env_flags = 0`, `last_pg = 1` (pages 0 and 1 exist;
   no data page yet), `free_db` and `main_db` both **empty**
   (`root = PGNO_INVALID`, all stats 0, `depth = 0`), fresh `meta_crc` on each.
3. `fsync(data)` so both slots are durable.

After creation the live snapshot is txnid 0 (both slots valid; the higher-txnid
rule §3.2 is a tie broken to either — they are identical). The **first commit**
is txn 1, which per the slot rule (a txn `N` writes slot `N & 1`, §3/SPEC 04 §9)
writes **slot 1** (`1 & 1 = 1`), leaving slot 0 as the intact txnid-0 fallback.
A fresh DB therefore reports `depth = 0`, `leaf_pages = 0`, `entries = 0` for both
DBs (stat parity with the oracle: an empty env has **no** root leaf allocated —
the first inserted key allocates the root leaf, SPEC 03 §9 grow).

### §3.5 — Worked example: creation meta, slot 0 (page size 4096, txnid 0, empty)

The slot-0 meta of a just-created env, `map_size = 1 MiB`. Slot 1 is byte-
identical (also txnid 0). Bytes shown by absolute offset; unshown ranges are 0.

```
off 0   : 00 00 00 00 00 00 00 00   pgno = 0
off 8   : 00 00 00 00 00 00 00 00   txnid = 0  (common header stamp)
off 16  : 08 00                     flags = P_META (0x0008)
off 18  : 00 00                     reserved0
off 20  : 00 00 00 00               checksum (data-page CRC; unused, 0)
off 24  : 00 00 00 00 00 00 00 00   reserved (meta variant tail)
off 32  : 5A 44 42 31               magic "ZDB1"
off 36  : 01 00 00 00               format_version = 1
off 40  : 00 10 00 00               page_size = 4096 (0x1000)
off 44  : 00 00 00 00               env_flags = 0
off 48  : 00 00 10 00 00 00 00 00   map_size = 1 MiB (0x100000)
off 56  : 01 00 00 00 00 00 00 00   last_pg = 1  (only metas 0,1; no data page)
off 64  : 00 00 00 00 00 00 00 00   txnid = 0  (body copy)
off 72  : FF FF FF FF FF FF FF FF   free_db.root = PGNO_INVALID (empty GC DB)
off 80  : 00 * 40                   free_db stats all 0, depth 0
off 120 : FF FF FF FF FF FF FF FF   main_db.root = PGNO_INVALID (empty)
off 128 : 00 00 00 00 00 00 00 00   main_db.branch_pages = 0
off 136 : 00 00 00 00 00 00 00 00   main_db.leaf_pages = 0
off 144 : 00 00 00 00 00 00 00 00   main_db.overflow_pages = 0
off 152 : 00 00 00 00 00 00 00 00   main_db.entries = 0
off 160 : 00 00                     main_db.depth = 0  (empty)
off 162 : 00 00                     main_db.flags = 0
off 164 : 00 00 00 00               main_db.leaf2_ksize = 0
off 168 : <c0 c1 c2 c3>             meta_crc = CRC32C over bytes [0,168)
off 172 .. 4096 : 00                reserved tail (excluded from CRC)
```

If the first commit (txn 1) then inserts one key, it allocates the root leaf at
`pgno 2`, and slot 1 records `main_db.root = 2`, `depth = 1`, `leaf_pages = 1`,
`entries = 1`, `last_pg = 2`. Open thereafter picks slot 1 (higher txnid, valid
CRC); slot 0 (txnid 0) remains the double-buffer fallback.

---

## §4 — Tree pages: nodes (cells)

Cells live in the heap of branch/leaf pages (§2.2). There are two cell shapes.
Both use unaligned little-endian reads.

### §4.1 — Branch node (10-byte header + key)

Points to a child subtree. The leftmost node of a branch (index 0) has an
**empty key** (`ksize = 0`); it is the "≤ everything to its right" child. All
other nodes carry the separator key (the smallest key reachable in that child).

| Off (in cell) | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 0 | 8 | `child_pgno` | u64 | Child page number. |
| 8 | 2 | `ksize` | u16 | Separator key length (0 for index 0; else 1–511). |
| 10 | `ksize` | `key` | bytes | Separator key. |

Cell length = `10 + ksize`, rounded up to even. ZeroDB stores the full 8-byte
child pgno (ADR-0002 §D4) rather than LMDB's packed 48-bit lo/hi/flags encoding.

### §4.2 — Leaf node (8-byte header + key + value-or-pointer)

| Off (in cell) | Size | Name | Type | Meaning |
|----:|-----:|------|------|---------|
| 0 | 2 | `flags` | u16 | Leaf-node flags (below). |
| 2 | 2 | `ksize` | u16 | Key length, 1–511. |
| 4 | 4 | `dsize` | u32 | Value length in bytes (the *logical* length, even when stored on overflow pages). ≤ `MAX_DATA_SIZE`. |
| 8 | `ksize` | `key` | bytes | Key. |
| 8+ksize | (see below) | `value` | bytes | Inline value, **or** an 8-byte overflow head pgno if `F_BIGDATA`. |

Leaf-node flags:

| Bit | Name | Meaning |
|-----|------|---------|
| `0x0001` | `F_BIGDATA` | Value is stored on an overflow run. The leaf cell's value area is exactly 8 bytes: the `u64` head pgno of the run. `dsize` still gives the true value length. |
| `0x0002` | `F_SUBDATA` | **Active in Phase 1** — the leaf value is a 48-byte sub-DB `DBRecord`, i.e. a named-DB catalog entry (M1.6; see §6). Only its *DUPSORT* interaction (`F_SUBDATA\|F_DUPDATA`, §10) is deferred to Phase 2.8; the plain catalog use is Phase 1. |
| `0x0004` | `F_DUPDATA` | **Reserved, Phase 2.8** — value is a DUPSORT sub-page/sub-tree (D-004). |

- Inline: cell length = `8 + ksize + dsize`, rounded up to even.
- BIGDATA: cell length = `8 + ksize + 8`, rounded up to even; the 8-byte value
  area holds `child`/head `pgno` of the overflow run.

**Inline-vs-overflow rule (value-size threshold).** Let

```
max_node_size = ((psize − HEADER_SIZE) / 2) rounded DOWN to even, − 2
```

(the largest cell that still guarantees two entries fit on a page — this is
ZeroDB's analogue of LMDB's `me_nodemax`, using `HEADER_SIZE = 32`). A value is
stored **inline** iff the inline leaf cell `8 + ksize + dsize` (before even-
rounding) is `≤ max_node_size`; otherwise the value goes to an **overflow run**
and the leaf holds a BIGDATA pointer. (Worked numbers: for `psize = 4096`,
`max_node_size = ((4096−32)/2 even) − 2 = 2030`; for `psize = 65536`,
`max_node_size = ((65536−32)/2 even) − 2 = 32750`.) ADR-0002 §D5.

### §4.3 — Worked example: leaf page (psize 4096, two entries)

Root leaf `pgno = 2`, `txnid = 1`, entries `("aa" → "X")`, `("bb" → "YZ")`.
Body size = `4096 − 32 = 4064`. Cells packed from the top; `"aa"` inserted
first at body offset `4064 − 12 = 4052`, `"bb"` at `4052 − 12 = 4040`. Node
pointers sorted by key: ptr\[0]→"aa"(4052), ptr\[1]→"bb"(4040). `lower = 4`
(two u16 pointers), `upper = 4040`.

```
Common header:
off 0   : 02 00 00 00 00 00 00 00   pgno = 2
off 8   : 01 00 00 00 00 00 00 00   txnid = 1
off 16  : 01 00                     flags = P_LEAF (0x0001)
off 18  : 00 00                     reserved0
off 20  : 00 00 00 00               checksum = 0
off 24  : 04 00                     lower = 4     (num_keys = 2)
off 26  : C8 0F                     upper = 4040  (0x0FC8)
off 28  : 00 00                     leaf2_ksize = 0
off 30  : 00 00                     reserved1

Node-pointer array (body-relative, at absolute 32):
off 32  : D4 0F                     ptr[0] = 4052 (0x0FD4)  -> "aa"
off 34  : C8 0F                     ptr[1] = 4040 (0x0FC8)  -> "bb"

Cell "aa" at absolute 32+4052 = 4084:
off 4084: 00 00                     flags = 0 (inline)
off 4086: 02 00                     ksize = 2
off 4088: 01 00 00 00               dsize = 1
off 4092: 61 61                     key  = "aa"
off 4094: 58                        value = "X"
off 4095: 00                        even-pad byte

Cell "bb" at absolute 32+4040 = 4072:
off 4072: 00 00                     flags = 0
off 4074: 02 00                     ksize = 2
off 4076: 02 00 00 00               dsize = 2
off 4080: 62 62                     key  = "bb"
off 4082: 59 5A                     value = "YZ"   (cell length 12, no pad)
```

### §4.4 — Worked example: branch page (psize 4096, two children)

Branch `pgno = 5`, `txnid = 7`, two children: index 0 (empty key) → child
`pgno 2`; index 1 (separator "m") → child `pgno 3`. Cell 0 length =
`10 + 0 = 10`; cell 1 length = `10 + 1 = 11 → 12`. Placed from top: cell0 at
`4064 − 10 = 4054`, cell1 at `4054 − 12 = 4042`. Pointers sorted by key: empty
key sorts first, so ptr\[0]→cell0(4054), ptr\[1]→cell1(4042). `lower = 4`,
`upper = 4042`.

```
off 0   : 05 00 00 00 00 00 00 00   pgno = 5
off 8   : 07 00 00 00 00 00 00 00   txnid = 7
off 16  : 02 00                     flags = P_BRANCH (0x0002)
off 24  : 04 00                     lower = 4
off 26  : CA 0F                     upper = 4042 (0x0FCA)
off 32  : D6 0F                     ptr[0] = 4054 (0x0FD6) -> child 2, empty key
off 34  : CA 0F                     ptr[1] = 4042 (0x0FCA) -> child 3, key "m"

Cell 0 at absolute 32+4054 = 4086:
off 4086: 02 00 00 00 00 00 00 00   child_pgno = 2
off 4094: 00 00                     ksize = 0   (leftmost, empty key)

Cell 1 at absolute 32+4042 = 4074:
off 4074: 03 00 00 00 00 00 00 00   child_pgno = 3
off 4082: 01 00                     ksize = 1
off 4084: 6D                        key = "m"
off 4085: 00                        even-pad
```

---

## §5 — Overflow pages and value chains

A value that fails the inline rule (§4.2) is stored on a **contiguous run** of
`N` pages. The **head** page (lowest pgno of the run) carries the common header
with `flags = P_OVERFLOW`, its own `pgno`, the writer `txnid`, and
`ovf_pages = N` in the variant tail. The value bytes occupy the run *densely*:
they begin at the head page's body (absolute offset `HEADER_SIZE` within the
head page) and continue through the full extent of the remaining `N−1` pages —
i.e. interior pages have **no** header; they are pure payload. Therefore:

```
capacity(N) = N * psize − HEADER_SIZE
N           = ceil( (HEADER_SIZE + dsize) / psize )
```

The leaf node with `F_BIGDATA` stores `dsize` (true length) and, in its 8-byte
value area, the head `pgno`. To read the value: fetch head page, read `dsize`
bytes starting at `head*psize + HEADER_SIZE`.

Overflow runs are never shared between entries (INV-11); an overwrite that
changes the size frees the old run (to the GC DB, SPEC 05) and allocates a new
one. Allocation of a run requires `N` *contiguous* free pages, else the run is
allocated fresh from end-of-file (SPEC 03 §overflow, SPEC 05 for reuse policy).

### §5.1 — Worked example: overflow head (psize 4096, value length 5000)

`N = ceil((32 + 5000) / 4096) = ceil(5032/4096) = 2`. Run occupies pgno 8 and 9.
Value bytes fill `8*4096 + 32 .. 8*4096 + 32 + 5000`. The referencing leaf node
carries `F_BIGDATA`, `dsize = 5000`, value area = `08 00 00 00 00 00 00 00`.

```
Head page (pgno 8):
off (8*4096)+0  : 08 00 00 00 00 00 00 00   pgno = 8
off (8*4096)+8  : <txnid>
off (8*4096)+16 : 04 00                      flags = P_OVERFLOW (0x0004)
off (8*4096)+24 : 02 00 00 00                ovf_pages = 2
off (8*4096)+28 : 00 00 00 00                reserved2
off (8*4096)+32 : <first 4064 payload bytes> ...
Page 9 (interior, no header):
off (9*4096)+0  : <remaining 936 payload bytes> <then unused to end of page>
```

---

## §6 — The catalog (main DB) and named databases (M1.6)

Named databases are entries in the **main DB** (the tree rooted at
`meta.main_db.root`). A catalog entry is an ordinary leaf node whose **key** is
the DB name (1–`MAX_DB_NAME` bytes) and whose **value** is a 48-byte DBRecord
(§3.1), flagged `F_SUBDATA` (Phase 2.8 activates the flag's DUP interactions;
in Phase 1 `F_SUBDATA` marks "this leaf value is a sub-DB DBRecord, not user
data"). The unnamed/default DB *is* the main DB itself: consumers that
`create_database(None)` (arroy/hannoy) use `main_db` directly, and its user
entries and its catalog entries coexist in one tree — a name collision with a
real user key is possible only if a consumer both stores plain keys in the main
DB and opens named sub-DBs, which milli does not do (SPEC 00). `FREE_DBI` (the
GC DB) is **not** a catalog entry; its DBRecord lives directly in the meta
(`free_db`) because it must be reachable before any tree walk.

Reserved for Phase 2.8: `DBRecord.flags` carries persistent DB flags; opening a
name with mismatched flags → `MdbError::Incompatible` (SPEC 01 §S8). All the
`DatabaseFlags` bits are 0 in Phase 1 (D-004).

### §6.1 — M1.6 implementation notes (open/create/clear/drop, write-back)

- **dbi table.** A named `Database` handle carries a small integer *dbi index*
  into an env-level registry (`EnvInner::named`, ZeroDB's analogue of LMDB's
  `me_dbxs`) that maps dbi → **name only**. The record always resolves lazily
  from the transaction's catalog view (SPEC 04 TXN-10 step 3): `open_database`
  searches the main tree for the name; `create_database` inserts an empty
  `F_SUBDATA` record eagerly (LMDB `MDB_CREATE` semantics) so it is visible
  in-txn and discarded with the dirty set on abort. Assignment is append-only
  within a process (an interim simplification like the M1.5 reader registry);
  it is unobservable through the SPEC-00 surface because resolution is always
  catalog-driven (an aborted create resolves to *absent*; re-creating re-uses
  the dbi). `max_dbs` counts distinct named DBs; the `max_dbs+1`-th distinct
  name → `MdbError::DbsFull`.
- **`F_SUBDATA` preservation.** A catalog value is always a 48-byte inline
  record, never `F_BIGDATA`. The `F_SUBDATA` node flag is threaded through the
  write path (`OwnedLeafCell.flags`) so it survives leaf splits, merges, and
  rebalances — the `check` walker (§below / SPEC 03 §11) relies on it to
  distinguish sub-DB records from user keys and to follow every named-DB tree.
- **Write-back timing (chosen mechanics).** A named DB's working record is held
  per-txn (`RwTxn::open`) and mutated in place during ops. Dirty records are
  written back into the main catalog at **commit step C1a — immediately before
  `freelist_save`** (SPEC 04 §9, matching LMDB's sub-DB flush order in
  `mdb_txn_commit`): the write-back is a same-size 48-byte overwrite of the
  existing `F_SUBDATA` entry, but it COWs main-tree leaves and may free pages,
  which the subsequent `freelist_save` must capture. (Rejected alternative:
  write-back on every mutation — it would touch the main tree on every named
  put/del and complicate the interleaving for no benefit.)
- **clear vs drop.** `clear` (`mdb_drop(_, 0)`) frees the tree's pages and
  resets the record to empty but keeps the catalog entry (rewritten empty at
  commit). `drop` (`mdb_drop(_, 1)`) additionally deletes the catalog entry
  (decrementing `main_db.entries`); for the **main** DB there is no entry to
  remove, so `drop` degrades to `clear` (LMDB: the main dbi is a core DB).
- **Name = byte string.** A DB name is a leaf key (1–`MAX_DB_NAME` bytes),
  arbitrary bytes including `0x00`. heed's C-string names are a stricter
  adapter-boundary rule imposed in M1.13 (DIVERGENCES D-008).

---

## §7 — GC / freelist pages

The free (GC) DB is a normal B+tree rooted at `meta.free_db.root`; its pages are
ordinary `P_LEAF`/`P_BRANCH` pages — there is **no** distinct freelist page
type. What is special is the *content* of its entries (the encoding SPEC 05
owns; the *page* format is exactly §2/§4):

- **Key**: an 8-byte `u64` txnid stored **big-endian** (SPEC 05 GC-2, resolving
  ADR-0002 OQ1), the id of the write txn that freed the pages. Big-endian is the
  **one** engine-internal key that is not little-endian: the GC tree is ordered by
  memcmp, and SPEC 05's reclamation scan needs numeric-ascending txnid order, for
  which memcmp order must equal numeric order. SPEC 02 fixes only that a GC entry
  is `(8-byte txnid key → page-id-list value)`; SPEC 05 §1 owns the byte order and
  chose big-endian.
- **Value**: a page-id list (PIL) — a `u64` count `c` followed by `c` `u64`
  page numbers. When the PIL exceeds `max_node_size` it is stored on an
  overflow run exactly like any large value (§5); there is no bespoke spill
  format. The in-tree ordering of the ids within the list is SPEC 05's choice.

The check tool treats GC entries as the authority for "free" in the
reachability-xor-freeness invariant (INV-10, INV-14).

---

## §8 — File geometry and growth (map_size, MAP_FULL)

- **Layout.** The env is a single regular data file (SPEC 00 row 22 depends on
  it) named **`zerodb.dat`** inside the env **directory** (the directory-env
  convention, SPEC 00 row 7; the path passed to `open` is the directory, not the
  file). Page 0 = meta A, page 1 = meta B, pages `≥ 2` = tree/overflow/GC pages.
  The file length is always a whole number of pages. (M1.2 fixed the filename;
  it is ZeroDB's own layout, not LMDB's `data.mdb`/`lock.mdb` — D-002.)
- **map_size.** `meta.map_size` is the size of the mmap region and the ceiling
  on the data file. It is set at env creation and read back via
  `Env::info().map_size` (SPEC 00 row 20). Growth beyond it is *not* automatic
  in Phase 1 (auto-geometry is Phase 3.2): heed callers grow by reopening with a
  larger `map_size` after a `MapFull` (SPEC 00 rows 3, 55).
  - **Runtime `map_size` selection (M1.2).** At open the effective `map_size` is
    the caller's `EnvOpenOptions::map_size` when set, else the persisted
    `meta.map_size`.
  - **Mapping strategy (amended M1.4, ADR-0004 D4/OQ2 — approved).** The read
    mmap covers `max(map_size, file length)` from open, so the base address is
    fixed for the env's life and **no remap ever happens in Phase 1** (file
    growth happens *underneath* the fixed `MAP_SHARED` mapping via the commit's
    positioned writes). Pages beyond EOF are mapped but never dereferenced: a
    committed meta only references pages made durable before it (SPEC 06
    REC-7/REC-14), and the writer reads its own new pages from the dirty set,
    never the map (SPEC 04 TXN-38). *(Supersedes the M1.2 map-the-file-length
    wording; the earlier "grows … and remaps" plan is retired.)*
  - **`map_size` value leniency vs heed (D-006, APPROVED).** heed rejects a
    `map_size` that is not a multiple of the **OS** page size; ZeroDB does not
    (its DB page size is independent of the OS page size, §0), so it accepts any
    `map_size ≥ 2·psize` — the kernel rounds the mapping length internally, and
    the reported `map_size`/`MapFull` boundary use the exact configured value.
    Unobservable to consumers, which always clamp before the call (SPEC 00
    row 3). See `docs/DIVERGENCES.md` D-006 (approved 2026-07-16 via ADR-0004
    OQ2).
- **Allocation.** New pages are taken first from the GC DB (SPEC 05; where a
  reader still pins them, from end-of-file), otherwise by bumping `last_pg`.
  A single-page allocation needs one free page; an `N`-page overflow run needs
  `N` contiguous pages.
- **MAP_FULL condition.** Let `map_pages = map_size / psize` (the number of pages
  the map can hold; valid page numbers are `0 .. map_pages − 1`). Allocating a run
  of `n` pages by extending the file hands out `[next_pgno .. next_pgno + n)` and
  would make the new high-water `next_pgno + n`. If the GC DB cannot satisfy the
  request from already-allocated free pages **and**
  `next_pgno + n > map_pages` (the run would **end past** the map), the write fails
  with `MdbError::MapFull` (SPEC 00 row 55 → heed `MdbError::MapFull` →
  `MaxDatabaseSizeReached`). The txn aborts cleanly with no partial state.
  Equivalently: the last page of the run, `next_pgno + n − 1`, must be
  `≤ map_pages − 1`; a request whose last page would be `map_pages` (one past the
  end) is rejected. (SPEC 05 GC-17 states the same boundary for the allocator.)
  - **Worked boundary (map_size 1 MiB, psize 4096).** `map_pages = 1048576 / 4096
    = 256`, so valid pages are `0..255`. With `next_pgno = 256` (pages 0..255 all
    allocated), a single-page allocation (`n = 1`) has `next_pgno + n = 257 > 256`
    → **`MapFull`**. The last usable data page is `pgno 255`; requesting `pgno 256`
    fails. (A run allocated earlier, e.g. `next_pgno = 254, n = 2`, gives pages
    254,255 with `254 + 2 = 256 ≤ 256` → **allowed**, exactly filling the map.)
- **real_disk_size** (SPEC 00 row 18) = actual on-disk file length (`fstat`),
  `= (last_pg + 1) * psize` for a freshly grown file. **non_free_pages_size**
  (SPEC 00 row 19) = `real_disk_size − (free-page count from the GC DB) * psize`;
  the GC-DB walk that yields the free-page count is ZeroDB's native replacement
  for milli reading LMDB's freelist DB (SPEC 05 provides the walk; M1.5).

---

## §9 — Alignment guarantees (summary)

- The common header's `pgno`/`txnid` (`u64`) sit at page-relative offsets 0 and
  8; because a page starts on a `psize`-aligned mmap boundary and `psize ≥ 4096`,
  these are 8-byte aligned in practice — but code MUST still use unaligned
  accessors (§0), never `#[repr(C)]` casts.
- The node-pointer array is `u16` at a 2-byte-aligned body position (body starts
  at offset 32).
- Cell (node) contents are only **2-byte** aligned. `child_pgno` (u64) and
  `dsize` (u32) inside cells are read with `read_unaligned`. This is the primary
  reason `#[repr(C)]` casting is forbidden here (CLAUDE.md).
- Phase 3.6 (hannoy) will add an *opt-in* value-alignment table type; the Phase
  1 B+tree makes **no** value-alignment promise beyond 2-byte cell alignment.

---

## §10 — DUPSORT format hooks (reserved — Phase 2.8, D-004)

No Phase 1 consumer uses duplicates (SPEC 00 §B.1, D-004). The format reserves,
but does not implement:

- Leaf-node flags `F_DUPDATA` (0x04) and `F_SUBDATA` (0x02) combined → the leaf
  value is either an **embedded sub-page** (`P_SUBP`, small dup sets packed in
  the leaf's value area) or a **sub-tree** DBRecord (large dup sets, own B+tree
  of the duplicate values). SPEC 03 §12 owns the sub-tree algorithms.
- Page flag `P_LEAF2` (0x20) + `leaf2_ksize` (header offset 28; DBRecord offset
  44) → DUPFIXED packed-key leaves (no per-key node headers; keys contiguous,
  `LEAF2KEY(p,i) = body + i*leaf2_ksize`).
- DBRecord.flags (offset 42) → persistent `DUP_SORT`/`DUP_FIXED`/… bits.

In Phase 1 all these fields MUST be zero and the flags MUST never be set; the
decoder MUST reject a page that sets them (defensive; a Phase-2.8 file is not a
valid Phase-1 file). Cross-reference SPEC 01 Table 2 (all DB flags WON'T→2.8)
and SPEC 03 §12.

---

## §11 — Cross-reference index (layout → SPEC 01 flag/behavior)

| Layout element | Implements / enables | SPEC 01 ref |
|----------------|----------------------|-------------|
| Meta double buffer + `meta_crc` (§3.2/§3.3) | torn-meta recovery; `PREV_SNAPSHOT` older-meta open | §S5, Table 1 `MDB_PREVSNAPSHOT` |
| `env_flags` reserved (meta §3) | durability flags are runtime, not persisted (Phase 1) | §S6 |
| BIGDATA + overflow run (§4.2/§5) | large values (roaring bitmaps, vectors); `put_reserved` target | §S3, Table 3 `MDB_RESERVE` |
| `max_node_size` inline rule (§4.2) | where value bytes live (map vs overflow); interacts with `WRITE_MAP` | §S7 |
| `MAX_KEY_SIZE = 511`, empty-key invalid (§1) | `BadValSize` bounds | §S4 |
| `MAX_DATA_SIZE` (§1) | `BadValSize` on oversized value | §S4 |
| Catalog `F_SUBDATA` entries + DBRecord.flags (§6) | `mdb_dbi_open` precedence, `Incompatible`/`DbsFull` | §S8, Table 2 |
| GC DB tree pages (§7) | `non_free_pages_size` native walk | Table 1 note; SPEC 00 row 19 |
| Reserved DUP hooks (§10) | all DUPSORT/DUPFIXED/DatabaseFlags | Table 2, Table 3 dup rows |
| `checksum` header field (§2) | Phase 3.9 data-page checksums | (Phase 3.9) |
| `txnid` writer stamp (§2) | Phase 3.10 incremental page shipping | (Phase 3.10) |
