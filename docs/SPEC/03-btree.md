# SPEC 03 — B+tree algorithms

Status: **DONE** — 2026-07-15 (milestone 0.4). Algorithmic source of truth for
the read path (M1.3), the write path (M1.4), and the check tool (M1.12). Formats
are in [SPEC 02](02-pages.md); this document defines the operations over them.
Pseudocode is illustrative, not code to transliterate; the LMDB fork was read to
understand the *algorithms* (CLAUDE.md rule 4), and the semantics below are
pinned to the oracle (SPEC 00/01), not to LMDB's C.

> **AMENDED 2026-07-20 (milestone 2.4).** The ordering statement below was
> absolute in Phase 1; it is now parameterized. Read §2.0 first — everything
> after it that says "memcmp" means "the tree's ordering", which is memcmp for
> every tree except a named database with a registered comparator.

---

## §2.0 — Key ordering (AMENDED, milestone 2.4)

**Phase 1 (superseded).** Ordering was **unsigned lexicographic byte
comparison** of keys everywhere (`memcmp`; SPEC 00 row 53, SPEC 01 Table 2).
"`k1 < k2`", "ascending", "greater" all meant memcmp order. No custom
comparator existed, and that single invariant underpinned every operation here.

**Phase 2.4 (current).** Every tree carries an ordering, `cmp`, and every
comparison in this document is `cmp`-relative. The rest of the spec continues
to be written in memcmp language because memcmp is the default and the only
ordering any consumer uses (SPEC 00 row 53); substitute `cmp` throughout.

| Tree | Ordering | Settable? |
|---|---|---|
| A **named** database | `DefaultComparator` (memcmp) unless one is registered | yes — `Env::{create,open}_database_with_comparator` |
| The **main / unnamed** database | memcmp, **always** | no — refused with `Io(InvalidInput)` |
| The **GC / free** database | memcmp, **always** | not reachable from the public API |

**BT-1 (amended).** Within one tree, the ordering is total, deterministic, and
fixed for the tree's lifetime. All of §2, §4, §6, §7, §10 and the INV-5/INV-6
invariants of §11 hold with respect to *that tree's* ordering, not with respect
to byte order.

Why the main DB is excluded: it doubles as the named-DB **catalog** (SPEC 02
§6). Its keys are database names and its `F_SUBDATA` values are engine-internal
`DBRecord` bytes, so making its order caller-defined would put engine metadata
under user code — and a single misbehaving comparator would corrupt the catalog
rather than one database. Why the GC tree is excluded: its keys are big-endian
txnids (SPEC 05 §1), for which memcmp order *is* numeric order, and the
oldest-reader gate depends on that.

### Consequences elsewhere in the engine

- **Prefix iteration (§4 "prefix iteration") is byte-defined.** It is realized
  as the range `[prefix, prefix_successor(prefix))`, and `prefix_successor` is a
  byte-increment. Under a custom comparator that range is still a well-defined
  *comparator* range between those two byte strings, but it is **not** "the keys
  starting with `prefix`" — prefix containment is not a property an arbitrary
  order preserves. heed reaches the same conclusion from the other direction:
  its prefix iterators require `C: LexicographicComparator`, not merely
  `Comparator`. Callers using a custom comparator should use explicit ranges.
- **The compacting copy refuses.** `Env::copy_to_file(CompactionOption::Enabled)`
  returns `Io(InvalidInput)` on an environment with any registered comparator.
  The bulk builder (SPEC 02, `build_multi_db_image`) sorts and debug-asserts in
  memcmp, and the `zerodb-tools` dump format records no comparator identity, so
  a comparator-aware compaction is a separate piece of work spanning the
  builder, the dump format and the tools. `CompactionOption::Disabled` (the raw
  page copy) is byte-level and unaffected.
- **`check` / `zerodb-tools check` is memcmp-defined.** It walks a *file*, which
  carries no comparator, so INV-5 (ascending keys in a page) and INV-6
  (separator bounds) are evaluated in byte order and a custom-comparator
  database legitimately reports violations of exactly those two. Every other
  invariant — page typing, reachability, depth uniformity, counter accuracy,
  GC structure — is ordering-independent and must still pass.
- **The comparator is not persisted.** See §2.0.1.

### §2.0.1 — Non-persistence hazard (D-014)

The comparator lives in process memory (`ComparatorRegistry`, keyed by dbi) and
is **never written to the file**. Reopening a database under a different
ordering than the one that built it silently yields wrong results and, once
written to, permanent corruption. LMDB has the identical hazard with
`mdb_set_compare`, and neither engine detects it.

ZeroDB detects the *in-process* case: a second registration for the same dbi
with a different `Comparator::name` is refused (`Io(InvalidInput)`). The
*cross-open* case is **not** detected. A stored comparator fingerprint checked
at open is the natural fix and there is nowhere to put one: `DBRecord` is
exactly 48 bytes with every offset assigned (SPEC 02 §3.1), and its only two
unused *values* — `flags` (offset 42) and `leaf2_ksize` (offset 44) — are
already reserved for DUPSORT/DUPFIXED in milestone 2.8. Widening the record or
repurposing those fields is an on-disk **format** change, which CLAUDE.md rule 6
puts behind an ADR and human approval. **Deliberately not taken in 2.4**; the
hazard is documented, filed as D-014, and left for a maintainer decision.

---

## §1 — Tree shape and terminology

- A database is a B+tree: internal **branch** pages hold `(separator key →
  child pgno)`; **leaf** pages hold `(key → value)`. All values live at the leaf
  level (values too large to inline hang off leaves via overflow runs, SPEC 02
  §5). The tree is rooted at a `DBRecord.root` (SPEC 02 §3.1); an empty tree has
  `root = PGNO_INVALID` and `depth = 0`.
- Every branch page's node 0 has an **empty separator key** and points to the
  subtree containing all keys `<` node 1's key. For `i ≥ 1`, node `i`'s
  separator is the smallest key present anywhere in child `i`'s subtree. Thus in
  a branch, child `i` covers `[sep(i), sep(i+1))` with `sep(0) = −∞`.
- **All leaves are at the same depth** (`= DBRecord.depth`); the tree grows/
  shrinks only at the root (§9).
- A **cursor** is a root-to-leaf path: arrays `page[0..top]` (pages from root to
  current leaf) and `ki[0..top]` (the chosen child/entry index at each level),
  plus flags `INITIALIZED` and `EOF`. `top` is the leaf level.

---

## §2 — Search (descend to a key)

`search(tree, key) -> Cursor` positions a cursor at the leaf that would contain
`key`, with `ki[top]` = the index of the first entry `≥ key` (the *lower-bound*
slot; may equal `NUMKEYS(leaf)` if `key` is greater than every entry on that
leaf). It does **not** decide membership — the caller compares.

```
search(tree, key):
    if tree.root == PGNO_INVALID: return EMPTY cursor (INITIALIZED=false)
    pg = tree.root; depth = 0; cursor.clear()
    loop:
        cursor.page[depth] = pg
        node = load(pg)
        if node.is_leaf:
            i = node_lower_bound(node, key)   # first entry with entrykey >= key
            cursor.ki[depth] = i
            cursor.top = depth
            cursor.INITIALIZED = true
            return cursor
        else: # branch
            i = branch_child_index(node, key)
            cursor.ki[depth] = i
            pg = child_pgno(node, i)
            depth += 1
```

- `node_lower_bound(leaf, key)`: binary search over the sorted node-pointer
  array; returns the least `i` with `entrykey(i) >= key`, else `NUMKEYS`.
- `branch_child_index(branch, key)`: the child whose range contains `key`.
  Binary-search the separators (node 0 = `−∞`): return the greatest `i` with
  `sep(i) <= key` (node 0 always qualifies). Equivalent: `upper_bound(key) − 1`.
- **Empty tree**: returns an uninitialized cursor; `get` → `None`, first/last →
  EOF, set/set_range → NotFound/EOF as per §4.

`get(tree, key)`: `c = search(tree, key)`; if `c` initialized and
`c.ki[top] < NUMKEYS(leaf)` and `entrykey(c.ki[top]) == key` → return value
(inline slice, or read the overflow run for BIGDATA); else `None` (heed maps a
missing key to `Ok(None)`, never an error — SPEC 00 rows 14/30).

### §2.1 — Key-size validation on reads vs. writes (observed via the oracle, M1.3)

The engine surfaces the same `search`, but LMDB validates key size differently
depending on the entry point — pinned by `zerodb-oracle` differential tests
(`read_differential.rs`) and replicated in Phase 1:

| Operation | empty key (len 0) | oversized key (len > 511) |
|-----------|-------------------|---------------------------|
| `put` / `put_with_flags` / `put_reserved` | `BadValSize` | `BadValSize` (maxkey checked up front — SPEC 01 §S4) |
| `get` / `set` (exact) | `BadValSize` | **`Ok(None)`** — search finds nothing, no error |
| `del` | `BadValSize` | **`Ok(false)`** — search finds nothing (del does **not** check maxkey up front, unlike put) |
| `set_range` / `get_greater_than` / `get_lower_than_or_equal_to` | `BadValSize` (an explicit `MDB_SET_RANGE` with a zero-size key is rejected) | `None`/last per the seek — no error |
| `prefix_iter` (forward) | `BadValSize` (realized as `set_range(prefix)`) | empty scan — no error |
| `rev_prefix_iter` (reverse) | **works** — full reverse iteration (its successor is unbounded, so it seeks via `last`, not `set_range`) | empty scan — no error |

The unifying rule: **write ops** (`put*`) validate `maxkey` before searching, so
both an empty and an oversized key are `BadValSize`; **read/search ops** reject
only the *empty* key (an explicit zero-size `MDB_SET`/`MDB_SET_RANGE` fails),
while an *oversized* key is not an error — it simply matches nothing. The one
asymmetry is `rev_prefix_iter` of an empty prefix, which seeks via `last` rather
than a zero-size set-range and therefore succeeds.

---

## §3 — Reading a value (zero-copy + overflow)

- Inline value: return `&[u8]` borrowed from the mapped page (RoTxn) or from the
  dirty page (RwTxn) — the borrow lifetime and dirty-page stability rules are
  SPEC 04's value-borrow contract. This doc assumes those hold.
- `F_BIGDATA` value: read `dsize` bytes starting at `head*psize + HEADER_SIZE`
  where `head` = the 8-byte pgno in the leaf cell (SPEC 02 §5). Still zero-copy:
  the overflow run is contiguous in the map.
- `Database::len` reads `DBRecord.entries`; `is_empty` = `entries == 0`
  (SPEC 00 rows 39/40).

---

## §4 — Cursor state machine (read positioning)

Each operation below states exact positioning and the empty-DB / EOF edges.
`NUMKEYS(p)` is entries on page `p`. A cursor at EOF has `INITIALIZED=true`,
`EOF=true` and yields `None` until repositioned. Ops map to SPEC 01 Table 5
`MDB_cursor_op`s (slug in parentheses) and to SPEC 00 rows.

**Path bound.** A cursor's root-to-leaf frame stack is inline with capacity
**32** (LMDB's `CURSOR_STACK`). With the minimum branch fanout of 2, depth 32
already addresses 2^31 leaf pages — beyond any representable env — so the
bound is unreachable for a well-formed tree; a descent that would exceed it
(a corrupt `depth` or a page cycle) fails with the same typed structural
error as the per-descent iteration guard (INV-7), never unbounded growth.

### first (`MDB_FIRST`) — SPEC 00 r41/r43

Descend taking child 0 at every branch level to the leftmost leaf; `ki[top]=0`.
Empty tree → `None`, cursor stays uninitialized. Sets `EOF=false`.

### last (`MDB_LAST`) — SPEC 00 r42

Descend taking the last child (`NUMKEYS−1`) at every branch to the rightmost
leaf; `ki[top]=NUMKEYS(leaf)−1`. Empty tree → `None`.

### next (`MDB_NEXT`) — SPEC 00 r43/r44/r45

- If not `INITIALIZED`: behave as **first** (LMDB starts an uninitialized
  forward iterator at the beginning).
- Else `ki[top] += 1`. If `ki[top] < NUMKEYS(leaf)` → done. Otherwise ascend:
  walk up while the parent's child index is the last child; if the root is
  reached that way → set `EOF`, return `None`. Else advance the parent's child
  index by 1 and descend child-0 to the new leftmost leaf, `ki[top]=0`.
- At EOF, `next` keeps returning `None`.

### prev (`MDB_PREV`) — SPEC 00 r44/r46

- If not `INITIALIZED`: behave as **last**.
- Else if `ki[top] > 0` → `ki[top] -= 1`, done. Otherwise ascend while the
  parent's child index is 0; if root reached → `None` (before-begin; cursor
  becomes uninitialized/at-begin, a subsequent `next` yields first). Else
  decrement the parent's child index and descend to the *rightmost* leaf of that
  child, `ki[top]=NUMKEYS(leaf)−1`.

### set (`MDB_SET`) — SPEC 00 r30 (get-by-cursor), SPEC 01 `cursor_set_exact`

Exact match. `c = search(tree,key)`; if positioned on an entry with
`entrykey == key` → position there, return it. Else → `NotFound`
(`MDB_NOTFOUND`); cursor is left *unpositioned for iteration* (LMDB leaves it
usable only via a fresh op). Used by existence checks.

### set_key (`MDB_SET_KEY`) — SHOULD, SPEC 01 `cursor_set_key`

As **set**, but returns the found key+value. Same machinery; included for
completeness (no Phase-1 consumer, SPEC 01 Table 5).

### set_range (`MDB_SET_RANGE`, `≥`) — SPEC 00 r44/r45/r47/r48

Lower-bound seek. `c = search(tree,key)`. If `ki[top] < NUMKEYS(leaf)` → the
entry there is the first `≥ key`; return it. If `ki[top] == NUMKEYS(leaf)` (key
is past the end of that leaf) → perform **next**'s ascend-and-descend to land on
the first entry of the following leaf; if none → `EOF`/`None`. Empty tree →
`None`. This is the workhorse behind ranges, prefix scans, and both neighbor
seeks.

### get_greater_than (`>`) — SPEC 00 r47 (milli facet tree)

```
get_greater_than(key):
    r = set_range(key)                 # first entry >= key
    if r is None: return None          # nothing >= key
    if current_key == key: return next()   # skip the equal one
    return r                            # already strictly greater
```

Positioning: on the least entry with `entrykey > key`. Empty/no-such → `None`.

### get_lower_than_or_equal_to (`≤`) — SPEC 00 r48 (milli facet tree)

```
get_lower_than_or_equal_to(key):
    r = set_range(key)                 # first entry >= key
    if r is None:                      # key is greater than everything
        return last()                  # the max entry is <= key (if any)
    if current_key == key: return r    # exact hit is <= key
    return prev()                      # first >= key was strictly >, step back
```

Positioning: on the greatest entry with `entrykey <= key`. If the tree is empty,
or `key <` the minimum entry (so `set_range` hit the first entry which is `>`
key and `prev` runs off the front) → `None`.

### prefix iteration — SPEC 00 r45/r46 (`prefix_iter`, `rev_prefix_iter`)

Prefix `P` is realized as a bounded range, **no** special comparator:

```
prefix_iter(P):
    start at set_range(P)
    while current is Some and current_key.starts_with(P):
        yield current; next()
    stop at the first key that does not start with P (or EOF)
```

`rev_prefix_iter(P)`: seek to the first key `> P·0xFF…` (the least key strictly
greater than every key with prefix `P`) via set_range of the prefix's successor,
step `prev`, then iterate `prev` while `current_key.starts_with(P)`. Concretely
LMDB/heed compute the smallest key greater than all `P`-prefixed keys
(`prefix_successor(P)` = increment the last non-`0xFF` byte, dropping trailing
`0xFF`s; if `P` is all `0xFF` there is no successor and iteration starts at
`last`). Empty result if no key has prefix `P`.

### range / rev_range — SPEC 00 r44

Forward: `set_range(lower_bound)` then `next` while `current_key` satisfies the
upper bound (inclusive/exclusive per the `Bound`), respecting an excluded lower
bound by skipping an equal first key. Reverse: start at the upper bound
(`set_range(upper)` adjusted for inclusive/exclusive, or `last` for an unbounded
upper) and step `prev` while the lower bound holds. Bounds semantics mirror
Rust `RangeBounds`; the engine only provides the primitive seeks (set_range,
next, prev, first, last) and the iterator adapter enforces the bounds.

### get_current (`MDB_GET_CURRENT`) — SPEC 00 r33/r34/r43

Return key+value at the current position without moving. Requires
`INITIALIZED` and not `EOF`, else `MDB_NOTFOUND`/`EINVAL` (mirrors LMDB;
underlies `iter_mut`/`prefix_iter_mut` rewrite and the write-cursor ops §7).

**EOF/empty summary.** Empty tree: first/last/set_range/get_current → `None`;
next/prev → `None`; set → `NotFound`. Non-empty at the boundary: `next` past the
max → `EOF`+`None` (idempotent); `prev` past the min → before-begin (`next`
resumes at first).

---

## §5 — Copy-on-write (COW) rules

The write path (M1.4) mutates a private, shadow copy of every page it touches.
The rules (SPEC 04 owns dirty-set *storage*; this section owns *when* pages are
copied and how pgnos propagate):

1. **First-touch copy.** Before modifying page `P` reachable from the committed
   root, allocate a fresh page `P'` (a new pgno, §8), `memcpy` `P`'s contents,
   stamp `P'.txnid = current_txnid`, and add `P'` to the txn's dirty set. All
   further edits in this txn hit `P'`. `P` (the old version) is scheduled for
   the GC DB at commit (SPEC 05) — readers on the old root still see it.
2. **Already-dirty pages** (created or copied earlier in this same txn, i.e.
   `P.txnid == current_txnid` and present in the dirty set) are edited in place;
   no re-copy.
3. **Parent-chain dirtying / pgno remapping.** When `P` is copied to `P'`, the
   parent branch node that pointed at `P`'s old pgno must be updated to `P'`.
   Because search descends root→leaf, COW is applied top-down along the cursor
   path: copying a leaf forces copying its parent to rewrite the child pgno,
   which forces copying its parent, up to the root. The new root pgno is
   recorded in the txn's working `DBRecord.root` and written to the meta at
   commit (SPEC 02 §3).
4. **Cursor fix-up.** After a page is copied/split/merged, every live cursor in
   the same txn positioned on the affected page(s) must still point at the
   logically-same entry. *(Clarified 2026-07-16, M1.4 / ADR-0004 D5.)* LMDB
   tracks and repairs **sibling** cursors because C permits many live cursors
   in one write txn; under ZeroDB's borrow model **at most one cursor can
   exist across a mutation** — mutations reach the tree through `&mut RwTxn`
   or through the single write cursor holding it exclusively — so fix-up
   reduces to the *acting* cursor's own position. The M1.4 write cursor tracks
   its position **by key** and re-seeks after each of its own mutations, which
   is trivially stable across splits/merges; no sibling-cursor tracking
   infrastructure exists (observable behavior is oracle-gated either way). If
   a later phase exposes concurrent write cursors, that requires a new ADR.
5. **Overflow pages are COW'd as whole runs**: modifying a BIGDATA value frees
   the old run and allocates a new one (§8); overflow pages are never edited in
   place across txns.

miri must exercise get-then-put sequences (PLAN 1.4): a `&[u8]` obtained by
`get` before a `put` must not dangle — enforced by SPEC 04's dirty-page
stability contract; this doc requires only that COW never *moves* an
already-dirty page's backing storage while a borrow into it is live.

---

## §6 — Insert with page split (put)

`put(tree, key, value, flags)` (SPEC 00 r31/r32/r35, SPEC 01 Table 3):

```
put(key, value, flags):
    validate: 1 <= key.len <= 511 else BadValSize (empty key rejected)   # SPEC01 §S4
              value.len <= MAX_DATA_SIZE else BadValSize
    if flags has APPEND: see §6.3
    c = search(tree, key)                       # COW along the descent path (§5)
    if c positioned on entrykey == key:         # key exists
        if flags has NO_OVERWRITE:              # SPEC01 §S2
            return KeyExist, exposing the existing value in the caller buffer
        replace value at c (§6.1)
    else:
        insert new node at c.ki[top] (§6.2)
    update DBRecord stats (entries, page counts, depth)
```

### §6.1 — Replace an existing value

- Same encoded cell size (inline↔inline same length, or BIGDATA↔BIGDATA same run
  length): overwrite the value bytes in place on the (dirtied) leaf. This is the
  `MDB_RESERVE` / `put_current` same-size fast path (SPEC 01 §S3).
- Different size: delete the old node (freeing an old overflow run if any, §8)
  and insert the new one at the same slot; may trigger a split (§6.2) or, if it
  shrinks the page below threshold, is left to the caller's next rebalance (put
  itself never merges — only delete does, §8/§10).

### §6.2 — Insert into a leaf; split when full

Compute the new cell size (§SPEC 02 §4.2 inline rule decides inline vs BIGDATA;
allocate the overflow run first if BIGDATA). If it fits in the leaf's free space
(`upper − lower ≥ cell_size + 2` for the new pointer) → insert: shift the
node-pointer array to open slot `ki[top]`, write the pointer, place the cell in
the heap (decrement `upper`), bump `lower += 2`. Done.

Otherwise **split** the leaf:

```
split(page P at insertion index newindx, new cell):
    allocate right sibling R (new pgno)
    choose split_indx (§6.4)
    move entries [split_indx .. NUMKEYS) from P to R (in order)
    insert the new cell into whichever of P / R now owns newindx
    sepkey = first key of R                     # smallest key in the right page
    propagate (sepkey -> R.pgno) into the parent branch at ki[top-1]+1:
        if the parent has room: insert there
        else: split the parent recursively (branch split, §6.5), which may
              propagate up to a root split (§9 grow)
```

### §6.3 — APPEND-optimized insert (SPEC 01 §S1)

`put(..., APPEND)` does **not** do a normal search. It positions at the
**last** entry (`last()`), compares `key` against the last key:

- `key > last_key` (the tree's ordering, §2.0; memcmp by default): insert at the end. If the rightmost leaf is full,
  split with the **end-of-page insert-point policy** (§6.4): put the *new* key
  alone on a fresh right page instead of splitting the full page in half — this
  keeps sequentially-loaded pages ~100 % full and avoids repeated half-empty
  pages (milli facet bulk / arroy item append). *(As of the 2026-07-16
  ADR-0005 D5 amendment this split behavior is no longer APPEND-specific — it is
  the general behavior for any end-of-page insert, §6.4. APPEND's distinctness
  is now only its **last-key-compare validation** below, not its split rule.)*
- `key == last_key` or `key < last_key`: `KeyExist` (`MDB_KEYEXIST` → heed
  `KeyExist` → arroy `InvalidItemAppend`). Equal-to-last is an error, **not** a
  silent overwrite (contrast plain put).
- APPEND into an **empty** tree always succeeds (root is `PGNO_INVALID`; create
  the first leaf, §9). Only the last key is validated, not full order (but since
  every prior append was strictly ascending, the tree stays sorted).

### §6.4 — Split-point policy (ADR-0002 §D6; end-of-page rule ADR-0005 D5)

**[AMENDED 2026-07-16 by ADR-0005 D5; RATIFIED — Quentin, 2026-07-16, standing
directive. The end-of-page insert-point rule below now governs *any* insert
that lands at `newindx == nkeys` (plain puts included), not just APPEND; the
median-fit-adjust rule is scoped to non-end inserts. The pre-amendment text is
preserved for the record at the end of this section.]**

Setup. The full page holds `nkeys` cells; the new cell is inserted at position
`newindx` (`0 ≤ newindx ≤ nkeys`). Consider the **post-insert sequence** of
`nkeys + 1` cells, indices `0 .. nkeys`, with the new cell occupying slot
`newindx`. A split point `s` sends post-insert indices `[0, s)` to the **left**
page `L` and `[s, nkeys+1)` to the **right** page `R` (half-open: index `s`
belongs to `R`). Let `C = psize − HEADER_SIZE` be the body capacity and, for a
page holding a set of cells, `used = Σ(cell_size + 2)` (each cell plus its 2-byte
pointer). A split is **feasible** at `s` iff `used(L) ≤ C` **and** `used(R) ≤ C`.

- **End-of-page insert-point split (`newindx == nkeys`) — MUST.** When the new
  cell lands at the very end of the page (`newindx == nkeys`; the new key is
  greater than every existing key on the page), force `s = nkeys`: **all**
  `nkeys` existing cells stay on `L`, and the new cell alone starts `R`. This
  is the fork's `mdb_page_split` behavior for any end insert — it keeps
  sequentially-loaded pages ~100 % full and roughly doubles leaf fill on
  ascending workloads (milli's dominant put pattern), where the median rule
  would instead leave a cascade of ~50 %-full leaves (ADR-0005 D5: measured
  400 ascending ~500 B puts → ~132 leaves under the median rule vs ~68 for the
  fork). APPEND (§6.3) is one case of this rule; it keeps its distinct
  last-key-compare validation (§6.3) but its split behavior is now the general
  end-of-page behavior. This rule is applied to **leaf splits** (see the
  branch-scope note below).

- **Normal split — median then fit-adjust (exact) — non-end inserts
  (`newindx < nkeys`):**

  ```
  choose_split(nkeys, newindx, newcell):
      s = (nkeys + 1) / 2            # integer division; median of the post-insert seq
      loop:
          if used(L(s)) <= C and used(R(s)) <= C:
              return s               # feasible
          if used(L(s)) > C:         # left overfull  -> move boundary left
              s -= 1
          else:                      # right overfull -> move boundary right
              s += 1
  ```

  - **Direction & tie-break.** `used(L(s))` is monotonically **non-decreasing**
    in `s` and `used(R(s))` monotonically **non-increasing** in `s` (moving the
    boundary right adds cells to `L`, removes them from `R`). Because index `s`
    belongs to `R` (half-open `[s, …)`), the tie case `newindx == s` places the
    **new cell as the first entry of `R`** — this is the deterministic tie-break
    (no ambiguity about which side the boundary cell lands on).
  - **Termination.** The feasible set of `s` is a contiguous, **non-empty**
    interval (non-empty because a single cell `≤ max_node_size` and
    `max_node_size` guarantees two entries fit on a page, SPEC 02 §4.2, and a key
    is `≤ MAX_KEY_SIZE = 511 ≪ max_node_size`; oversized values are already on
    overflow so the inline cell is small). By the monotonicity above, `used(L) >
    C` means `s` is *above* the feasible interval (so `s -= 1` steps toward it) and
    `used(R) > C` means `s` is *below* it (so `s += 1` steps toward it); both
    cannot hold at once. Each iteration strictly reduces the distance to the
    interval, so the loop reaches a feasible `s` in `≤ nkeys` steps and never
    oscillates.
  - **Observable page-count parity.** This median-plus-fit-adjust follows the
    **same policy shape** as LMDB's split decision (`mdb_page_split`'s
    `split_indx`/`newindx` refinement); it is **not** claimed to pick a
    byte-identical boundary in every case. The gate is the **PLAN 1.5 tolerance
    band**: page counts must track the oracle within that band, not exactly.

- **Branch-split scope (ADR-0005 D5).** The end-of-page insert-point rule is
  applied to **leaf splits only** in Phase 1. Branch splits (§6.5) keep the
  median-fit-adjust `choose_split` for every `newindx`, including
  `newindx == nkeys`. Rationale: a literal "new cell alone on `R`" would leave
  the right branch with a single child (node 0 only), violating the branch
  `min_keys = 2` occupancy invariant (INV-8) — the fork itself does not put the
  new child alone but lands its end-insert split at `nkeys − 1` (two children on
  `R`) via a fit loop, a different computation from the leaf "new alone" case.
  Leaves carry essentially all of the ascending-workload fill-factor effect
  (there are far more leaves than branches; ADR-0005 D5's ~2× ratio is a leaf
  phenomenon), so leaves-only captures the benefit without the branch-occupancy
  hazard. The PLAN 1.5 tolerance band and the oracle page-count parity tests
  gate the resulting counts either way.

- **Rising separator.** For a **leaf** split the separator promoted to the parent
  is the first key of `R` (the key at post-insert index `s`, or the sole new
  cell under the end-of-page rule); for a **branch** split it is the removed
  median key (§6.5).

**Pre-amendment text (superseded 2026-07-16, ADR-0005 D5 — preserved for the
record).** Before the ratified end-of-page rule, the median-fit-adjust
`choose_split` governed **every** non-APPEND insert (including end inserts,
`newindx == nkeys`), and only APPEND forced the insert-point split:

> - *Normal split — median then fit-adjust (exact):* applied for every
>   `newindx` regardless of position; `s = (nkeys + 1) / 2` then fit-adjust as
>   above.
> - *Append split* (APPEND at the rightmost position, §6.3): force `s = nkeys`,
>   i.e. `newindx = nkeys` and the new cell is the sole entry of `R`; all
>   `nkeys` existing cells stay on `L`. This keeps sequentially-loaded pages
>   ~100 % full (the point of APPEND).

The amendment generalizes the APPEND split behavior to any end-of-page insert;
the measured consequence (ADR-0005 D5) is the ~2× leaf-fill improvement on
ascending plain-put workloads.

### §6.5 — Branch split

A branch splits like a leaf, except the **separator that rises to the parent is
removed from both children** (a branch separator is not duplicated — the classic
B+tree "the median key moves up, it does not stay down"). Concretely: pick
`split_indx`, the key at `split_indx` becomes the parent separator; children
`[0..split_indx)` stay left, `(split_indx..nkeys)` go right, and the rising
key's *child pointer* becomes right page's node-0 (empty-key) child. Contrast
leaf split, where the split key stays in the right leaf (leaves hold data, so no
key is discarded).

---

## §7 — Write-cursor ops: put_current / del_current (M1.4)

These mutate at the cursor's current position during `iter_mut` /
`prefix_iter_mut` passes (SPEC 00 rows 33/34; SPEC 01 §S3). They are `unsafe` in
heed because no live `&[u8]` borrow of the current entry may span the call.

### put_current (`MDB_CURRENT`) — SPEC 00 r33

Requires `INITIALIZED` and not `EOF`, else `EINVAL` (SPEC 01 §S3). Rewrites the
value of the entry at `ki[top]` **keeping the key**:

- Same encoded size → in-place overwrite (fast path; the `_with_options` codec-
  swap form re-encodes the value first, then must land the same or a new size).
- Different size → delete+reinsert at the same key (may split, §6.2).
- **`APPEND` via `put_current_with_options`** (milli facet bulk): heed passes the
  caller's `PutFlags` straight to the underlying put with **no forced
  `MDB_CURRENT`**, so `APPEND` here behaves exactly like a plain `MDB_APPEND`
  (§6.3): the engine does its own **last-key compare** and **ignores the cursor's
  current position**. If `key > last_key` (the tree's ordering, §2.0) it appends at the end
  (append-split policy, §6.4), *wherever* the iterator happens to be parked; if
  `key ≤ last_key` (equal included) it returns `KeyExist` **even when the cursor
  is sitting on the last entry**, and does not overwrite. The cursor position is
  irrelevant to the outcome.

  > Confirmed via oracle self-test 2026-07-15
  > (`crates/zerodb-oracle/tests/flag_semantics.rs`:
  > `cursor_put_current_append_ignores_position_when_greater` — key `>` last
  > succeeds while the cursor is parked at the *first* entry;
  > `cursor_put_current_append_not_greater_is_keyexist` — key `<` last and key
  > `==` last both `KeyExist` while the cursor is parked at the *last* entry, and
  > the stored value is left unmodified. Verified against the fork through heed
  > 0.22.1, matching SPEC 01 §S1.)
- `RESERVE` form: returns a pointer to the reserved bytes inside the dirty leaf
  (or overflow head) for the caller to fill; those bytes MUST be written before
  any op that could move/split/free the page (SPEC 04 borrow contract, SPEC 01
  §S3). ZeroDB must not zero the reserved region (parity with writemap peek).

### del_current (`MDB_cursor_del`) — SPEC 00 r34

Delete the entry at `ki[top]`. Frees an associated overflow run (§8). Then
**rebalance** the leaf (§10) if it fell below threshold. The cursor is left
positioned so that a following `next` yields the entry that followed the deleted
one (LMDB leaves `ki[top]` pointing at the successor slot; if the page was
merged/rebalanced, the cursor is fixed up per §5.4). `delete(key)` (SPEC 00 r36)
= `set(key)` then `del_current`, returning whether the key existed;
`delete_range` (r37) and `clear` (r38) are cursor walks / whole-tree resets.

---

## §8 — Page allocation and overflow chain alloc/free

- **Single page**: obtain a pgno from the GC DB's reusable set (SPEC 05 gates
  reuse on the oldest live reader — until M1.8's reader table exists, the
  oldest reader comes from the interim mutexed reader registry of SPEC 04
  TXN-21 as amended by ADR-0005 OQ1, so only pages freed at-or-before the
  oldest live reader's snapshot are reusable); if none, bump `next_pgno`
  (`= last_pg + 1`), growing the file, and
  fail with `MapFull` if it would exceed `map_size / psize` (SPEC 02 §8).
- **Overflow run of N pages** needs `N` *contiguous* free pages. Try the GC DB
  for a contiguous run of length `≥ N` (SPEC 05); else allocate `N` fresh
  contiguous pages at end-of-file. `N = ceil((HEADER_SIZE + dsize) / psize)`
  (SPEC 02 §5).
- **Free**: a page removed from the tree (COW-obsoleted, split donor emptied,
  merged-away, or an overflow run of a replaced/deleted BIGDATA value) is
  recorded in the txn's freed-page list and written to the GC DB at commit,
  keyed by the txn's id (SPEC 02 §7, SPEC 05). A whole overflow run frees all
  `N` of its pgnos.
- The freed page is **not** reusable within the same txn by default (a reader on
  the pre-txn root may still reach it); the loose-page fast path (SPEC 05) is the
  narrow exception for pages allocated *and* freed inside the current txn.

---

## §9 — Root grow / shrink

- **Grow** (empty → 1 leaf): first insert into an empty tree allocates a leaf,
  sets `DBRecord.root = leaf.pgno`, `depth = 1`.
- **Grow** (root split): when the root page splits (§6.5), allocate a new branch
  page with two children (old root's two halves), set it as `root`, `depth += 1`.
  The tree only gets taller here.
- **Shrink** (root collapse): after a delete-driven rebalance, if the root is a
  **branch with a single child**, replace the root with that child and
  `depth -= 1` (free the old root, §8). If the root is a **leaf that became
  empty**, set `root = PGNO_INVALID`, `depth = 0` (free the leaf).

---

## §10 — Delete with rebalance / merge / borrow

After `del_current` removes an entry, if the leaf is a non-root page below the
fill threshold it is rebalanced. Thresholds (ADR-0002 §D6; match LMDB for Phase
1 parity, SPEC 02 §1):

- **Leaf**: `min_keys = 1`, must stay `≥ FILL_THRESHOLD` (25.0 %, 250 permille)
  of page-body fill.
- **Branch**: `min_keys = 2` (a branch must always have ≥ 2 children), fill
  threshold effectively `> 0` (any underful branch with < 2 keys triggers).
- A page at/above threshold **and** with `≥ min_keys` needs no rebalance.

```
rebalance(page P at cursor):
    if P is the root: handle root shrink (§9) and return
    choose a sibling:
        if P is the leftmost child of its parent: sibling = right neighbor (fromleft=false)
        else: sibling = left neighbor (fromleft=true)
    if sibling is above threshold AND has > min_keys:
        BORROW one entry from the sibling across the parent separator (node_move)
        update the parent separator key accordingly
    else:
        MERGE P and the sibling into one page (page_merge):
            move all entries of the right page into the left page (in order)
            drop the parent's separator node that pointed at the right page
            free the now-empty right page (§8)
        then rebalance the PARENT branch (the merge removed a parent entry,
        which may push the parent below its own threshold) — recurse up,
        possibly collapsing the root (§9).
```

- **Borrow** (a.k.a. rotate / `node_move`): moves the boundary entry from the
  fuller sibling into `P` and rewrites the parent separator so ordering holds.
  Preferred when it avoids a merge (keeps height stable).
- **Merge** direction: LMDB always merges the *right* page into the *left* one
  (when the underful page is the right sibling it merges itself into the left;
  when it is the left it merges the right into itself). ZeroDB follows the same
  left-absorbs-right rule so cursor fix-up (§5.4) is deterministic.
- Merges can cascade: merging leaves removes a branch separator, which may make
  the branch underful, recursing to the root. A root branch left with one child
  collapses (§9), reducing depth.

---

## §11 — Tree invariants (enforced by `check`, M1.12)

Numbered so tests and the check tool can cite them (`INV-n`). "Reachable" = on
some DB tree walked from a meta root. Applies to the live meta's snapshot.

- **INV-1** — Live meta validity: chosen meta has `magic == MAGIC`,
  `format_version == FORMAT_VERSION`, and `page_size` a power of two in
  `[4096,65536]` (SPEC 02 §3.2).
- **INV-2** — Meta CRC: the live meta's `meta_crc` matches CRC32C over `[0,168)`;
  header `txnid` == body `txnid`; on a torn slot the older intact slot is used
  (SPEC 02 §3.3).
- **INV-3** — Page-size uniformity: every page is exactly `psize` bytes; the file
  length is a whole multiple of `psize`.
- **INV-4** — Self pgno (**written pages only**): every page that is *reachable*
  through a DB tree (a leaf/branch/overflow-head of the main, named, or GC trees)
  has a header `pgno` equal to its file index (`file_offset / psize`). A page that
  is **free** (listed in the GC free set, SPEC 05) is **exempt**: a page that was
  allocated (bumping `next_pgno`) and then freed within a txn without ever being
  written — a mid-file "hole" — may be entirely zero on disk and carries no valid
  header. Only written pages self-identify. (Overflow *interior* pages are also
  exempt: they are raw payload, SPEC 02 §5.)
- **INV-5** — Intra-page key order: within any leaf/branch page the node-pointer
  array is strictly ascending by key (the tree's ordering, §2.0 — the file-level
  `check` tool evaluates this in memcmp, so a custom-comparator DB reports
  INV-5/INV-6 by design); no duplicate keys within a page
  (Phase 1 has no DUPSORT).
- **INV-6** — Separator bounds: for every branch, child `i`'s subtree keys all
  lie in `[sep(i), sep(i+1))` (with `sep(0) = −∞`, `sep(last+1) = +∞`); node 0's
  separator is empty. For `i ≥ 1`, `sep(i) ≤` the minimum key of child `i`
  (**not** necessarily equality): a delete that removes the former minimum key of
  child `i` does **not** rewrite the parent separator, so `sep(i)` may be strictly
  *less* than child `i`'s current minimum. This is LMDB parity — separators are
  only guaranteed to be valid *lower bounds* that route search correctly, not to
  equal the child minimum. The routing/ordering guarantee (every key of child `i`
  is `≥ sep(i)` and `< sep(i+1)`) is what the check tool enforces; equality holds
  only for freshly built/split branches.
- **INV-7** — Uniform depth: all leaves are at depth `DBRecord.depth`; the tree
  is height-balanced.
- **INV-8** — Minimum occupancy (the algorithms' guarantee, **not** a fill-ratio
  invariant): every non-root page has `≥ min_keys` (leaf 1, branch 2). The 25.0 %
  `FILL_THRESHOLD` is a **delete-time rebalance *trigger*** (§10), **not** a
  steady-state property of committed pages. A `put` that replaces a value with a
  shorter one (§6.1), or a delete that borrows/merges and lands a page just above
  `min_keys`, may legally leave a committed page **below** 25 % full — matching
  LMDB, which likewise does not re-pack pages after value-shrinking puts. The
  check tool therefore asserts only `≥ min_keys` on non-root pages (and the bounds
  of INV-9), **not** `≥ FILL_THRESHOLD`. The root is exempt from `min_keys`. The
  threshold governs *when delete rebalances*, and is verified indirectly by the
  file-size tolerance band (PLAN 1.5), not as a per-page assertion.
- **INV-9** — Bounds consistency: `HEADER_SIZE`-body offsets satisfy
  `0 ≤ lower ≤ upper ≤ psize − HEADER_SIZE`; `num_keys = lower/2`; cells do not
  overlap the pointer array or each other; every pointer targets a cell fully
  inside `[upper, bodysize)`.
- **INV-10** — Reachability XOR freeness: every page in `[2, last_pg]` is either
  reachable exactly once through some DB tree (main, its named sub-DBs, or the
  GC DB), or listed exactly once in the GC DB's free set — never both, never
  neither (SPEC 02 §7/§8).
- **INV-11** — Overflow integrity: every `F_BIGDATA` leaf node points to a
  `P_OVERFLOW` head with `ovf_pages == ceil((HEADER_SIZE + dsize)/psize)`; the
  run's `ovf_pages` pages are contiguous, reachable only through that one node
  (no sharing), and none is independently in any tree or free set.
- **INV-12** — Key size: every stored key has length `1..=511`; no empty keys.
- **INV-13** — Value size: every `dsize ≤ MAX_DATA_SIZE` (`0xFFFF_FFFF`).
- **INV-14** — GC well-formedness: GC entries are `(8-byte txnid key →
  page-id-list value)`; every listed page number is in `[2, last_pg]`; no page
  appears in two GC entries (subsumed by INV-10 but checked directly).
- **INV-15** — Catalog consistency: each named-DB catalog entry (main DB,
  `F_SUBDATA`) holds a 48-byte DBRecord with a valid `root` (`PGNO_INVALID` or a
  reachable branch/leaf) and a `depth` consistent with its tree.
- **INV-16** — Single parent: no page is referenced as a child by two different
  branch nodes (no aliasing in the reachable graph).
- **INV-17** — High-water: `last_pg ≥` every reachable/free pgno; the file is at
  least `(last_pg + 1) * psize` bytes. Trailing never-written pages do **not**
  inflate `last_pg`: at commit, loose pages that sit at the very end of the file
  and were never written are dropped from the high-water so `next_pgno` shrinks
  back (SPEC 05 GC-10 extend-then-loose rule) — the file does not grow by holes at
  its tail. Mid-file holes (free, never-written pages below `last_pg`) are
  permitted and are covered by INV-4's free-page exemption.
- **INV-18** — Stat accuracy: each `DBRecord`'s `entries`, `leaf_pages`,
  `branch_pages`, `overflow_pages`, and `depth` equal the values obtained by
  walking the tree.
- **INV-19** — Root shape: if `depth == 0` then `root == PGNO_INVALID`
  (empty); if `depth == 1` the root is a leaf; if `depth ≥ 2` the root is a
  branch with `≥ 2` children.
- **INV-20** — txnid monotonicity: every page's writer-stamp `txnid ≤` the live
  meta's `txnid`; no page claims a future txn.
- **INV-21** — Reserved fields zero: in Phase 1, `reserved*` header fields,
  `checksum` (data-page, §2), `leaf2_ksize`, DBRecord `flags`/`leaf2_ksize`, and
  the `P_LEAF2`/`P_SUBP`/`F_SUBDATA`(non-catalog)/`F_DUPDATA` bits are all zero/
  unset (a page setting a Phase-2.8 hook is not a valid Phase-1 page).

---

## §12 — DUPSORT trees (RESERVED — Phase 2.8, D-004)

No Phase 1 consumer uses duplicates (SPEC 00 §B.1, D-004). Phase 2.8 will add,
against the format hooks reserved in SPEC 02 §10:

- **Sub-page** encoding for small duplicate sets: the dup values live in an
  embedded `P_SUBP` mini-page inside the leaf value area; cursor dup-ops
  (`FIRST_DUP`/`NEXT_DUP`/`GET_BOTH`/…) walk it.
- **Sub-tree** promotion for large dup sets: the leaf value becomes an
  `F_SUBDATA|F_DUPDATA` DBRecord rooting a secondary B+tree of the duplicate
  values; the same §2–§10 algorithms recurse one level down.
- **DUPFIXED** (`P_LEAF2`): packed fixed-size dup keys, enabling
  `GET_MULTIPLE`/`MULTIPLE` bulk ops.
- Dup-aware cursor ops and `APPEND_DUP` (SPEC 01 Table 3/5 dup rows).

Phase 1 implementers MUST NOT emit any of these structures; INV-21 rejects them.
The differential-fuzz budget for this area (≥ 2 h clean) is deferred to 2.8
(PLAN §2.8).

### §12.1 — 2.8a pinned fork observations (2026-07-20; ADR-0011 Q5 first act)

Observed against the oracle (heed =0.22.1 / lmdb-master-sys 0.2.6, fork
`mdb.master.nested-rtxns`, macOS aarch64) by
`crates/zerodb-oracle/tests/dup_pin_semantics.rs` and `dup_pin_ffi.rs` —
**before any zerodb dup code exists**. These tables are the normative record
the 2.8 implementation must match; the tests are the executable form. Items
marked ⚠ contradict previously written spec/ADR text and are **pending human
adjudication (the 2.8a stop-report)** — the observation is the truth about the
fork; whether zerodb replicates or diverges is the open decision.

**O1 — dup value size bound.** In a DUPSORT DB the value is bounded exactly
like a key: len 0..=511 → `Ok` (empty dup values are legal), len ≥ 512 →
`BadValSize`. Key 511 + value 511 together → `Ok`. Non-dup DBs accept the same
lengths (inline/overflow). Confirms ADR-0011 Decision 1 (dup values are
sub-tree keys; no `F_BIGDATA` inside dup structures).

**O2 — `Database::stat` folding and growth.** `entries` counts **pairs**
(so does `len`). `depth`/`branch_pages`/`leaf_pages`/`overflow_pages` cover
the **main tree only**: growing one key's dup set from 3 pairs to 403 pairs
(~40 KiB, well past sub-page capacity → promoted sub-tree) leaves
`depth=1 branch=0 leaf=1` unchanged. Deleting back down restores nothing to
observe (counters never moved). A key reduced to a single dup reads back
normally. Consequence for zerodb: the parent `DBRecord`'s page counters and
`depth` must **not** fold dup sub-tree pages; `entries` is Σ pairs.

**O3 — GET_BOTH / GET_BOTH_RANGE** (FFI; dup set `k1 → [d1,d3,d5]`,
`k2 → [e1]`):

| Probe | Observed |
|---|---|
| GET_BOTH exact (k1,d3) | `OK key=k1 data=d3` |
| GET_BOTH absent dup (below/between/above) | `NOTFOUND` |
| GET_BOTH missing key | `NOTFOUND` |
| GET_BOTH empty data | `BAD_VALSIZE` (dup data validated like a key on the read path — asymmetric with put, which accepts empty) |
| GET_BOTH_RANGE (k1,d2) | `OK key=k1 data=d3` (first dup ≥ given) |
| GET_BOTH_RANGE exact (k1,d5) | `OK key=k1 data=d5` |
| GET_BOTH_RANGE empty data | `BAD_VALSIZE` |
| GET_BOTH_RANGE (k1,d9) past last dup | `NOTFOUND`; afterwards `GET_CURRENT` → `NOTFOUND` (position invalidated) but `NEXT` → `OK k2/e1` (the main position survives at k1, stepping on) |
| GET_BOTH_RANGE missing key | `NOTFOUND` (no ≥-key fallback: the KEY match is exact) |

**O4 — dup cursor-op taxonomy** (FFI): on a **non-dup** DB:
`FIRST_DUP`/`LAST_DUP`/`GET_BOTH`/`GET_BOTH_RANGE` → `MDB_INCOMPATIBLE`
(⚠ Table 5 / ADR-0011 guessed `EINVAL`); `NEXT_DUP`/`PREV_DUP` **degenerate to
plain `NEXT`/`PREV`** (they cross keys!); `NEXT_NODUP`/`PREV_NODUP` behave as
`NEXT`/`PREV` (Table 5 already said so). On a **dup** DB with an unpositioned
cursor: `FIRST_DUP` → `EINVAL`, `NEXT_DUP` → first entry (inherits
NEXT-from-scratch = FIRST), `PREV_DUP` → `NOTFOUND`. Positioned:
`NEXT_DUP` at the last dup of a key → `NOTFOUND` (never crosses keys);
`NEXT` at the last dup → next key's first dup; `NEXT_NODUP` → next key's
**first** dup; `PREV_NODUP` from key b → previous key's **last** dup.

**O5 — put-flag semantics on a dup DB.** Plain put of an exactly-existing
pair → `Ok`, `entries` unchanged (idempotent no-op). `NODUPDATA`: new pair
`Ok`; exact pair → `KeyExist`, and (FFI) the out-data still points at the
caller's bytes (no §S2-style rewrite — trivially, the existing item equals the
input). `NOOVERWRITE` on an existing key with a NEW value → `KeyExist`, and
(FFI) out-data is rewritten to the **first dup** of the key (the §S2 contract,
dup flavor). `delete(key)` (no value) removes **all** dups (`entries` -= dup
count). `delete_one_duplicate(k,v)`: exact pair → `true`, absent pair →
`false`. RESERVE (`put_reserved`): ⚠ lmdb.h says "must not be specified with
DUPSORT" (SPEC 01 Table 3 repeated it) but the fork **accepts** it and stores
the reserved bytes as an ordinary dup value.

**O6 — APPENDDUP / APPEND.** `APPENDDUP` compares only against the current
**last dup of that key** under the dup ordering: first dup of any key → `Ok`
(the key need **not** be the DB's last key — an earlier key's dup set can be
appended to); `new > last` → `Ok`; `new ≤ last` (equal included) → `KeyExist`.
`APPEND` on a dup DB: `key > last key` → `Ok`; **equal key → `KeyExist`
regardless of the dup value and regardless of `APPENDDUP` also being set**
(the fork refuses equal keys under APPEND even for dup insertion;
`APPEND|APPENDDUP` only helps for fresh keys).

**O7 — dup-only put flags on a NON-dup DB are silently IGNORED.** ⚠
`NODUPDATA` and `APPENDDUP` on a non-dup DB behave as a plain overwrite put:
no error, no KeyExist, no order check (out-of-order APPENDDUP keys accepted,
values silently overwritten). `mdb_del` with a data argument on a non-dup DB
ignores the data bytes and deletes the key (lmdb.h documents this one). heed's
`get_duplicates` on a non-dup DB returns an iterator whose first step
(`FIRST_DUP`) yields `Err(Incompatible)`; on a missing key it returns `None`
(the `MDB_SET` fails first).

**O8 — persisted-flag handling at open.** ⚠⚠ **The fork's `mdb_dbi_open`
performs NO persistent-flags mismatch check** (read directly in the vendored
`mdb.c`: on an existing named DB the persisted `MDB_db` — including
`md_flags` — is copied into the slot and the caller's flag bits are silently
**ignored**). Observed: a DUPSORT DB opened with no flags behaves DUPSORT; a
plain DB opened with `DUP_SORT` requested behaves plain (double-put keeps 1
entry); extra flags on reopen → `Ok`; **no `MDB_INCOMPATIBLE` on any
mismatch, same-process or across env reopen**. This **falsifies SPEC 01 §S8
item 8** and the ADR-0011 assumption "mismatch → Incompatible". Persistence
itself is real: flags live in the on-disk record and survive reopen (O9).
Unknown flag bits (outside `VALID_FLAGS`) → `EINVAL`; `REVERSEDUP` or
`DUPFIXED` **without** `DUPSORT` are accepted at open (no combination check).

**O9 — main-DB DUPSORT (§S8 items 3/4).** `create_database(None)` with
`DUP_SORT` → `Ok`; the unnamed root becomes a working dup DB; its flags are
OR'd into the main record, **persisted**, and survive env reopen (a later
flag-less unnamed open behaves DUPSORT). While the main DB carries
`DUPSORT`: named `create` → `Incompatible`, named `open` → `NotFound`
(`None`) — exactly §S8 item 4, confirmed live.

**O10 — DUPFIXED size discipline.** ⚠ The fork does **not** enforce item-size
uniformity: after two 4-byte items, a 5-byte and a 3-byte put both return
`Ok` and the stored dup set becomes garbage (4 items, wrong bytes — silent
corruption; neither a clean `BadValSize` nor an un-fixing of the page). The
per-key first item fixes the accepted size *per DB record*... observably the
corruption is immediate. A different key may still start at another size
(2-byte item on a fresh key → `Ok`, reads back clean). 2.8c must adjudicate
replicate-vs-diverge before any DUPFIXED code.

**O11 — built-in orderings.** `INTEGERDUP`/`INTEGERKEY` on same-size items:
numeric order for native-endian 4-byte and 8-byte values (`mdb_cmp_cint`
family). Mixed sizes are accepted and, on little-endian, an 8-byte 7 sorts
numerically among 4-byte items (`[1,2,7,300,70000]`) — formally undefined per
lmdb.h, pinned as observed on LE (all target platforms are LE).
`REVERSEDUP`/`REVERSEKEY`: bytes compared from the **end** toward the front
(`c < ax < by < az`).

**O12 — iteration shape.** Full iteration of a dup DB yields one entry per
**pair**, the key repeated, dups in dup order (`[a=1,a=5,a=9,b=2]`);
`rev_iter` is the exact reverse; `first`/`last` return (first key, first dup)
/ (last key, last dup); `get` returns the **first** dup of the key.
