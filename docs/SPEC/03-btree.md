# SPEC 03 — B+tree algorithms

Status: **DONE** — 2026-07-15 (milestone 0.4). Algorithmic source of truth for
the read path (M1.3), the write path (M1.4), and the check tool (M1.12). Formats
are in [SPEC 02](02-pages.md); this document defines the operations over them.
Pseudocode is illustrative, not code to transliterate; the LMDB fork was read to
understand the *algorithms* (CLAUDE.md rule 4), and the semantics below are
pinned to the oracle (SPEC 00/01), not to LMDB's C.

Ordering is **unsigned lexicographic byte comparison** of keys everywhere
(`memcmp`; SPEC 00 row 53, SPEC 01 Table 2). "`k1 < k2`", "ascending",
"greater", etc. all mean memcmp order. No custom comparator exists in Phase 1;
this single ordering invariant underpins every operation here.

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
   the same txn positioned on the affected page(s) has its `page[]`/`ki[]`
   adjusted so it still points at the logically-same entry (LMDB tracks sibling
   cursors; ZeroDB does the same for cursors open in the write txn).
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

- `key > last_key` (memcmp): insert at the end. If the rightmost leaf is full,
  split with the **append policy** (§6.4): put the *new* key alone on a fresh
  right page instead of splitting the full page in half — this keeps
  sequentially-loaded pages ~100 % full and avoids repeated half-empty pages
  (the point of APPEND, milli facet bulk / arroy item append).
- `key == last_key` or `key < last_key`: `KeyExist` (`MDB_KEYEXIST` → heed
  `KeyExist` → arroy `InvalidItemAppend`). Equal-to-last is an error, **not** a
  silent overwrite (contrast plain put).
- APPEND into an **empty** tree always succeeds (root is `PGNO_INVALID`; create
  the first leaf, §9). Only the last key is validated, not full order (but since
  every prior append was strictly ascending, the tree stays sorted).

### §6.4 — Split-point policy (ADR-0002 §D6)

Setup. The full page holds `nkeys` cells; the new cell is inserted at position
`newindx` (`0 ≤ newindx ≤ nkeys`). Consider the **post-insert sequence** of
`nkeys + 1` cells, indices `0 .. nkeys`, with the new cell occupying slot
`newindx`. A split point `s` sends post-insert indices `[0, s)` to the **left**
page `L` and `[s, nkeys+1)` to the **right** page `R` (half-open: index `s`
belongs to `R`). Let `C = psize − HEADER_SIZE` be the body capacity and, for a
page holding a set of cells, `used = Σ(cell_size + 2)` (each cell plus its 2-byte
pointer). A split is **feasible** at `s` iff `used(L) ≤ C` **and** `used(R) ≤ C`.

- **Normal split — median then fit-adjust (exact):**

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

- **Append split** (APPEND at the rightmost position, §6.3): force
  `s = nkeys`, i.e. `newindx = nkeys` and the new cell is the sole entry of `R`;
  all `nkeys` existing cells stay on `L`. This keeps sequentially-loaded pages
  ~100 % full (the point of APPEND).

- **Rising separator.** For a **leaf** split the separator promoted to the parent
  is the first key of `R` (the key at post-insert index `s`); for a **branch**
  split it is the removed median key (§6.5).

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
  current position**. If `key > last_key` (memcmp) it appends at the end
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
  reuse on the oldest live reader — until M1.8's reader table exists, oldest
  reader = current txn, so only pages freed by *earlier committed* txns are
  reusable); if none, bump `next_pgno` (`= last_pg + 1`), growing the file, and
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
  array is strictly ascending by key (memcmp); no duplicate keys within a page
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
