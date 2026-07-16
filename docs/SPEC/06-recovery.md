# SPEC 06 — Durability & recovery

Status: **DONE** — 2026-07-15 (milestone 0.4). Behavioral source of truth for
open-time meta selection under crash (M1.2), the durability guarantee at every
commit-pipeline cut point (M1.4), the durability-flag crash windows (M1.10), and
the crash-consistency test protocol (M1.11). Formats: [SPEC 02](02-pages.md)
(§3 meta double-buffer + CRC). The commit pipeline whose cut points this doc
reasons about is defined in [SPEC 04](04-txn-mvcc.md) §9; the GC atomicity it
relies on is [SPEC 05](05-gc.md) §4. Durability-flag semantics come from
[SPEC 01](01-flags.md) §S6/§S7.

Clean-room note: the fork's `mdb_env_open`/`mdb_env_pick_meta` (meta selection)
and `mdb_env_write_meta`/`mdb_env_sync0` (sync routing) were read to understand
the *algorithm* (CLAUDE.md rule 4); the guarantees below are ZeroDB's own,
strengthened by the **mandatory** meta CRC (torn-meta detection is Phase 1, not
Phase 3 — PLAN 0.4). Normative rules are numbered **REC-n**.

The crash-safety spine, stated once: **the meta write is the commit point, and it
is never made durable before the data pages it references.** Everything else here
is a consequence.

---

## §1 — Open-time meta selection (crash recovery entry point)

At env open the engine reads both meta slots (pages 0 and 1) and selects the live
snapshot. This is where a crash is recovered. Validation is per SPEC 02 §3.2;
this section defines the **recovery decision** and its error taxonomy.

- **REC-1** — **Validate each slot independently.** The validation predicate is
  owned by **SPEC 02 §3.2** (the numbered list: `magic`, `format_version`,
  `page_size` ∈ {power of two, 4096–65536}, header `txnid` == body `txnid`, and —
  mandatory — `meta_crc` over `[0,168)`, SPEC 02 §3.3). REC-1 does **not** restate
  the list; it references SPEC 02 §3.2 as the single owner. A slot failing any
  check is **invalid** (torn or foreign) and is discarded from selection.
- **REC-2** — **Selection among valid slots:**

  | Valid slots | PREV_SNAPSHOT off | PREV_SNAPSHOT on |
  |-------------|-------------------|------------------|
  | both | higher `txnid` wins | lower `txnid` wins |
  | exactly one | that one wins (regardless of txnid) | **`MdbError::Invalid` (REC-5)** † |
  | neither | `MdbError::Invalid` (REC-3) | `MdbError::Invalid` (REC-3) |

  The both-valid rule is the formula from SPEC 02 §3.2: selected index =
  `(txnid[0] < txnid[1]) XOR prev_snapshot`. The **one-valid, PREV_SNAPSHOT-off**
  rule is the torn-meta recovery guarantee: because a writer only ever overwrites
  the *older* slot (SPEC 04 TXN-63), the surviving slot is always a complete
  earlier snapshot, so it wins even though it is not the higher txnid.

  † **One-valid, PREV_SNAPSHOT-on → hard error (conservative, REC-5).**
  PREV_SNAPSHOT means "give me the *older of two* committed snapshots." With only
  one valid slot there are not two committed snapshots to identify an older from —
  the requested rollback target is either destroyed (older slot torn) or
  unidentifiable — so ZeroDB fails `MdbError::Invalid` rather than guess. This is
  **zerodb-defined behavior**: the fork has no meta CRC, so its selection under a
  torn slot is not observable/pinnable, and there is no oracle to match here.
  **Ratified 2026-07-16 (Quentin, chat)** — a maintainer may prefer to serve the
  lone valid older slot instead; until ratified, the conservative hard error
  stands.
- **REC-3** — **Both invalid → `MdbError::Invalid`** (heed maps to
  `InvalidStoreFile`, SPEC 00 row 56). This is unrecoverable. **M1.11's guarantee:
  a single torn write must never produce this** — at most one slot can be torn by
  one interrupted commit (only the older slot is being written), so the other is
  always intact. Both-invalid can only arise from a non-single-torn cause
  (foreign file, wrong version, wholesale corruption), which is correctly a hard
  error.
- **REC-4** — **One-valid fallback WARN path.** When exactly one slot is valid and
  the other is torn, open **succeeds** on the valid slot but MUST emit a warning
  (log line / diagnostic) that a torn meta was detected and the older snapshot was
  used — the operator should know a commit was lost to a crash. This is not an
  error; it is the designed recovery. (No consumer branches on it; it is
  observability, PLAN 1.2.)
- **REC-5** — **PREV_SNAPSHOT recovery interaction** (SPEC 01 §S5, SPEC 04
  TXN-65..67). PREV_SNAPSHOT requires **both** slots valid to identify the older
  (REC-2): it selects the **lower**-txnid valid slot only when two valid slots
  exist; with fewer, it fails `MdbError::Invalid` (REC-2 †). The first commit after
  a PREV_SNAPSHOT open **self-resolves** the abandoned newer branch: because that
  commit is `older_txnid + 1` and the abandoned newer snapshot was also
  `older_txnid + 1`, they share slot parity, so the commit **overwrites the
  abandoned slot in place** and normal higher-txnid selection resumes (SPEC 04
  TXN-67; confirmed by the oracle). No separate invalidation step is needed. The
  only residual, cosmetic open question (SPEC 04 §10 conflict block) is that the
  new commit reuses txnid `older_txnid + 1`, orphaning the abandoned commit's
  beyond-high-water pages — a bounded space leak milli's rollback already
  tolerates (LMDB parity). This spec fixes the observable guarantee: after the
  single post-rollback commit, a normal reopen sees the rolled-back-then-committed
  state as live, not the abandoned newer branch.
- **REC-22** — **PREV_SNAPSHOT is guaranteed only against cleanly-committed
  history; a PREV_SNAPSHOT open MUST warn.** Rollback via PREV_SNAPSHOT relies on
  two disk properties that hold after a *clean* commit but are **not** guaranteed
  after a crash:
  1. **Sector-aligned tears yield a fully valid *stale* meta (not a CRC
     collision).** The entire CRC-covered region `[0,168)` **and** the `meta_crc`
     field (offset 168) both lie within the meta page's **first 512-byte sector**,
     and the rest of the page is zeros (SPEC 02 §3). So a power cut that tears the
     meta write at **sector granularity** leaves the slot holding either the
     *complete old* content (sector 0 not yet written) or the *complete new* content
     (sector 0 written) — **both CRC-valid**. The CRC therefore does **not** reject
     a sector-aligned tear; it only catches a *sub-sector* tear that splits sector
     0. Consequently, after a crash a slot may deterministically present a fully
     valid but **stale** snapshot (the slot's previous content), and PREV_SNAPSHOT
     cannot be sure the "older" slot is the intended rollback target — this is a
     txnid-selection question (REC-2/REC-6), not something the CRC resolves.
  2. **Overwritten prior-snapshot pages.** The immediately-previous snapshot's
     *pages* stay intact only because reuse is gated so a writer never clobbers the
     one-older snapshot (SPEC 05 GC-18; a page freed by txn `N` is reclaimable by
     `N+1`, but pages freed by `N+1` are not, so `N` survives). After a crash
     mid-commit of a *later* txn (e.g. H1 of txn `N+1`, SPEC 04 §9), or under
     relaxed durability (§3), that gate's on-disk realization may be incomplete, so
     the older meta's referenced pages are not provably intact.
  Therefore a PREV_SNAPSHOT open MUST emit a **warning** that rollback is
  guaranteed only against cleanly-committed history, and MUST NOT claim
  crash-proof rollback. The **exact guard** — e.g. refusing PREV_SNAPSHOT unless
  the older meta's referenced pages verify intact, or requiring both slots
  cleanly valid — was **ratified 2026-07-16 (Quentin, chat): Phase 1 ships the warning only, no verification guard**; a stronger guard remains an ADR seam for Phase 3.
  This rule deliberately **does not overclaim**: Phase 1 provides best-effort
  single-step rollback with an honest warning, not a guaranteed crash-consistent
  rollback.

---

## §2 — Guarantee at every commit-pipeline cut point

The commit pipeline (SPEC 04 §9, TXN-61) is one function with crash hooks
`H0..H4` between steps `C0..C6`. This section states the invariant that MUST hold
if power is lost **exactly** at each hook. "Recovers to `X`" means: on the next
open, REC-1/REC-2 select snapshot `X` and the check tool (SPEC 03 §11 + SPEC 05
§9) passes on it. Let the committing txn be `N`, the last committed snapshot
`N−1`.

- **REC-6** — **Crash-stage invariant table** (default durability mode; relaxed
  modes in §3):

  | Hook | Just completed | Recovers to | Invariant that MUST hold |
  |------|----------------|-------------|--------------------------|
  | H0 | C1 freelist_save (all in dirty set, nothing on disk) | `N−1` | Disk is byte-identical to the `N−1` commit; txn `N` is invisible; GC unchanged. |
  | **H1** | C2 wrote (some/all) dirty data pages, **no fsync** | `N−1` | Meta slots untouched → `N−1` selected. Written data pages occupy only pages `N−1` does not reference (SPEC 04 TXN-62), so torn/partial data pages are unreferenced garbage. No corruption; `N` invisible. |
  | **H2** | C3 fsync(data) done | `N−1` | Same as H1 but `N`'s data is now fully durable and still unreferenced. `N−1` selected; `N` invisible. |
  | **H3** | C4 wrote meta slot `N&1`, **no fsync(meta)** | `N−1` **or** `N` | The meta write is a single page. If it did not reach disk, or reached disk **torn**, its CRC fails → discarded → `N−1` (the intact `(N−1)&1` slot) selected (REC-3 impossible: only one slot in flight). If it reached disk **intact**, `N` may be selected — and that is safe **because `N`'s data was fsynced at C3** (H2), so every page `N`'s meta references is durable. Never a torn meta accepted; never a meta referencing unwritten pages. |
  | **H4** | C5 fsync(meta) done | `N` | `N` is durable and selected. `N−1`'s slot still holds a valid older snapshot (untouched this commit). |

- **REC-7** — **The single load-bearing ordering** (SPEC 04 TXN-61/TXN-62): C3
  (fsync data) MUST complete before C4 (write meta), and C4 MUST target the older
  slot only. This is what makes H3 safe: an accepted meta `N` can only reference
  already-durable pages. Reordering C3 after C4 would allow a crash to accept a
  meta pointing at unwritten data — the one corruption this design forbids.
- **REC-8** — **Meta CRC catches *sub-sector* tears; *sector-aligned* tears are
  handled by txnid selection.** The meta is exactly one `psize` page with a CRC over
  `[0,168)` (SPEC 02 §3.3), and both the covered region and the CRC field sit in the
  first 512-byte sector (the tail is zeros). Two crash cases:
  - A **sub-sector** partial write (a tear that splits sector 0) leaves the CRC
    inconsistent with the covered bytes → the slot is rejected at open (REC-1), and
    the intact other slot wins.
  - A **sector-aligned** tear leaves the slot holding either the *complete old* or
    *complete new* content — **both CRC-valid**. The CRC does not fire here; instead
    the double buffer + **txnid selection** (REC-2) recovers the correct snapshot:
    a stale (old) slot content has a lower txnid than the intact other slot, so the
    intact newer/older snapshot is chosen. This is why the crash-stage conclusion
    (REC-6 H3: recover to `{N−1, N}`) rests on **txnid selection**, not on the CRC.
  We do not attempt to "repair" a torn meta. The CRC is mandatory in Phase 1 as the
  guard against sub-sector tears and foreign/garbage slots (data-page checksums stay
  Phase 3.9); it is **not** claimed to detect every torn meta — sector-aligned tears
  are caught by the double-buffer/txnid design, not the CRC.

---

## §3 — Durability flags: crash windows (SPEC 01 §S6 lattice)

The default mode (§2) loses nothing on crash. The relaxed flags trade durability
for speed; each has a defined crash window. ZeroDB must reproduce LMDB's exposure
even though the I/O layer differs (pwrite/io_uring vs writemap+msync). All modes
preserve **structural consistency** (the check tool passes on whatever snapshot is
recovered) **except** where explicitly noted as FS-order-dependent.

- **REC-9** — **Durability lattice** (SPEC 01 §S6, mapped onto the pipeline):

  | Mode | C3 fsync(data) | C5 fsync(meta) | On crash may lose | Corruption-free? |
  |------|----------------|----------------|-------------------|------------------|
  | default | yes | yes | nothing | yes, unconditionally |
  | `NO_META_SYNC` | yes | **no** (meta page written but not fsynced this commit) | the most recent commits whose meta never reached disk | **yes** — data is always durable and each meta only ever references data fsynced before it; recovery falls back to the newest *durable* meta (REC-2), an intact older snapshot |
  | `NO_SYNC` | **no** | **no** | the last N commits (data + meta) | **conditional** — structurally safe **iff** the filesystem preserves write order (data before meta). If the FS can make a meta durable before its data, a crash can yield a meta referencing unwritten pages → corruption. LMDB documents this; ZeroDB inherits it. |
  | `MAP_ASYNC` (+`WRITE_MAP`) | `msync(MS_ASYNC)` | `msync(MS_ASYNC)` | recent commits flushed lazily by the kernel | same conditional as `NO_SYNC`: the kernel may write pages out of pipeline order |

- **REC-10** — **`NO_META_SYNC` corruption-freedom argument.** Even though the
  meta is not fsynced, C3 still fsyncs data every commit. Every meta slot only
  references pages that were durable before that meta was written (REC-7 still
  holds — only the *meta's own* durability is relaxed). So the worst case is: the
  newest meta on disk is some `N−k` (`k ≥ 0`), and it is fully consistent because
  `N−k`'s data was fsynced. Recovery picks the highest valid meta (REC-2). No torn
  meta is accepted (CRC, REC-8). Result: **lose up to the last `k` commits'
  visibility, never corruption.** The double buffer must remain a valid pair
  (SPEC 01 §S6): even the un-fsynced meta write still targets the correct slot
  (SPEC 04 TXN-63), so the older intact slot is always available.

  **M1.11 amendment — the reclaim-clobber window (`NO_META_SYNC`) — PENDING
  HUMAN RATIFICATION (found by the ADR-0008 crash harness, 2026-07-16; repro
  seed 15797139550980166469).** The argument above shows the recovered meta's
  pages were durable *when written*, not that they *remain unclobbered*. The
  hole: after commit `N` returns, its meta write is issued but un-fsynced
  (C5 skipped). Txn `N+1` may legally reclaim pages freed by txn `N`
  (GC-18) — pages that belong to **snapshot `N−1`** — and its C2 writes them
  *before* its C3 barrier would make meta `N` durable. In the window between
  commit `N+1`'s C2 and C3, a power cut can persist txn `N+1`'s data while
  meta `N` tears (sub-sector → CRC-rejected) or drops entirely: recovery then
  selects meta `N−1`, one or more of whose pages now hold txn `N+1`'s bytes —
  **structural corruption of the fallback snapshot** (walker: INV-20 "page
  stamped by future txn"). So the corrected claim is: `NO_META_SYNC` recovery
  to the **newest issued** meta is fully consistent; recovery that falls
  **below** it, when a younger txn's data also persisted, is not guaranteed
  structurally consistent. The default mode is immune (C5 makes meta `N`
  durable before txn `N+1` can exist, so the only fallback is to `N−1`
  against txn `N`'s own writes, which TXN-62 confines to pages `N−1` does not
  reference). **LMDB parity:** the fork shares this window verbatim under
  `MDB_NOMETASYNC` (same reclaim gate, no meta CRC — a torn meta may even be
  *accepted* there); libmdbx's steady/weak-meta machinery (steady-gated page
  reclaim; a third meta slot) exists precisely to close it. Phase 1 keeps
  LMDB parity and documents the window; steady-meta gating is a **Phase 3
  candidate**. The harness (ADR-0008 D4) asserts full REC-18 on every
  `NO_META_SYNC` image whose recovery lands on the newest issued meta, and
  window/taxonomy obligations only (walk/data waived, counted as "stale
  fallbacks") on the precisely-delimited clobber-window images.
- **REC-11** — **`NO_SYNC`/`MAP_ASYNC` window.** With neither fsync, an unbounded
  suffix of recent commits may be lost, and — unlike `NO_META_SYNC` — a
  reordering filesystem can make a meta durable before its referenced data,
  producing a meta that points at pages never written = corruption detectable only
  as a failed tree walk (not by the meta CRC, which would be valid). ZeroDB
  therefore documents `NO_SYNC`/`MAP_ASYNC` as **"structural consistency requires
  ordered writeback"** and does not claim corruption-freedom for them. `force`
  (an explicit sync, Phase 2.5) overrides `NO_SYNC` and downgrades `MAP_ASYNC` to
  a synchronous flush (SPEC 01 §S6). These flags are crash-tested in M1.11
  (§5) to characterize — not to guarantee-away — their window.
- **REC-12** — **`WRITE_MAP` msync ordering** (SPEC 01 §S7). Under `WRITE_MAP`,
  C2 writes dirty bytes straight into the writable map and C3/C5 are `msync`s
  instead of `pwrite`+`fdatasync`. The **same ordering** as REC-7 applies:
  `msync(data range, MS_SYNC)` (C3) MUST complete before writing the meta into the
  map (C4) and `msync(meta page, MS_SYNC)` (C5). On macOS/Windows an additional
  `fdatasync` of the data fd is issued (SPEC 01 §S7) because `msync` alone is not
  a durability barrier there. A writemap env opens **no** separate meta sync fd
  (SPEC 01 §S7): the meta durability is the meta-page `msync`. The crash-stage
  table (REC-6) holds verbatim with `msync` substituted for fsync.

---

## §4 — File growth, truncate, and fsync-failure handling

- **REC-13** — **fsync-gate (poisoning).** If any C3/C5 `fsync`/`msync` **fails**,
  the txn's durability is unknown and the OS may have dropped the dirty pages
  (Linux consumes an fsync error once, then reports success). ZeroDB therefore
  **poisons** the env: the failed commit returns an I/O error (SPEC 04 §8.1), and
  **every subsequent** `write_txn`/`commit` on that env returns a poisoned-env
  error until the env is closed and reopened (reopen re-runs REC-1/REC-2 and
  recovers the last durable snapshot). A poisoned env still serves existing read
  txns from their pinned snapshots (they read already-mapped pages). The poison
  flag is a `Sync` atomic on `EnvInner` set before the commit error is returned
  (store `Release`, checked `Acquire` at each write-txn begin). This matches the
  modern "an fsync failure is not retryable in place" reality; LMDB's own handling
  is weaker, and treating a failed barrier as fatal is a strengthening ZeroDB
  adopts (recorded, not a consumer-visible divergence — consumers surface it as an
  `Io` error either way).
- **REC-14** — **File growth crash safety.** Growing the file (SPEC 05 GC-16
  extend; durability ordering GC-28) writes new data pages into the extended
  region and fsyncs them (C2/C3) **before**
  the meta that records the new `last_pg` (C4). So a crash during growth either
  recovers to `N−1` (meta not yet updated; the extended region is unreferenced) or
  to `N` (meta durable, and the region it references is durable). A meta's
  `last_pg` can never, on any recovered snapshot, exceed the real file length
  (REC-7 ordering guarantees the referenced high-water is durable first). If the
  file extension (`ftruncate`-up / ensuring the mmap covers the region) itself is
  not yet durable at a crash, the recovered meta is `N−1`, whose `last_pg` is
  within the old length — safe.
- **REC-15** — **No in-place truncate in the commit path.** Phase 1 never shrinks
  the data file during a normal commit (map_size is fixed; there is no
  auto-shrink — Phase 3.2). File-shrinking only happens via a **compacting copy**
  (`copy_to_file`, SPEC 00 row 17, M1.12), which writes a fresh file and atomically
  replaces — never mutates the live file's length under a reader. So there is no
  truncate-crash window in the transactional path. (If a future geometry feature
  adds shrink, it must define its own crash protocol; noted so Phase 3.2 does not
  assume this section covers it.)
- **REC-16** — **Meta-only durability vs data.** The commit point is the meta
  (REC-7). A crash between two commits never leaves a *partial* logical
  transaction: because all of a txn's pages (data + GC, SPEC 05 GC-14) are flushed
  before its meta, and the meta is the sole switch, recovery is all-or-nothing per
  txn. There is no redo/undo log and none is needed — the double-buffered,
  CRC-guarded, ordered-flush meta *is* the recovery mechanism.

---

## §5 — Crash-test protocol (PLAN 1.11, two mechanisms)

SIGKILL alone cannot tear a write — the OS page cache survives process death, so
only power loss tears or reorders un-fsynced sectors. The harness therefore has
**two** mechanisms, and every recovered image is verified against the same
obligations (REC-18).

- **REC-17** — **Mechanism 1: process-kill + fsync-barrier hooks.** Run a workload
  in a child process. At random points — **and specifically at each commit hook
  `H0..H4`** (SPEC 04 §9; the pipeline exposes these hooks for exactly this
  purpose) — `SIGKILL` the child, reopen the env in the parent, and verify
  (REC-18). This exercises the *control-flow* cut points (did we order the steps
  correctly?) but, because the page cache survives, it cannot by itself simulate a
  torn/reordered sector — mechanism 2 does that.
- **REC-18** — **Per-cycle verification obligations** (both mechanisms). After every
  crash+reopen:
  1. Open succeeds selecting a valid meta, **or** fails only with the designed
     `MdbError::Invalid` **only** where two-plus slots were deliberately corrupted
     (a single-torn cycle MUST open, REC-3).
  2. The recovered snapshot is one of `{N−1, N}` for a crash during commit `N`
     (REC-6) — never an intermediate/partial state, never a torn meta accepted.
  3. The **check tool** (SPEC 03 §11 INV-1..21 + SPEC 05 §9 INV-22..27) passes on
     the recovered snapshot: reachability-xor-freeness, tree invariants, GC
     well-formedness, `last_pg` ≥ every referenced page, meta CRC valid.
  4. **Monotonic durability:** the recovered txnid is `≥` the last txnid the
     harness observed as *acknowledged-committed* before the crash (a commit that
     returned `Ok` and completed C5/H4 must never disappear) — in default mode.
     Under relaxed modes (§3) this obligation is weakened to the mode's documented
     window (REC-9): `NO_META_SYNC`/`NO_SYNC`/`MAP_ASYNC` cycles assert only "no
     corruption per obligation 3" and "loss bounded by the mode's window," not
     zero loss.
- **REC-19** — **Mechanism 2: fault-injection write backend** (`zerodb-io`,
  M1.11). A write backend that simulates power loss on the set of writes **not yet
  covered by an fsync**: it may (a) **drop** un-fsynced writes, (b) **reorder**
  them, and (c) **tear** an individual page write at a sub-page (sector) boundary
  — CrashMonkey/ALICE-style. On a simulated crash it emits a disk **image** from a
  legal subset/permutation of the un-fsynced writes plus all fsynced ones; the
  harness reopens each such image and runs REC-18. This is the mechanism that
  actually validates REC-8 — but note the CRC only rejects a **sub-sector** meta
  tear (a tear splitting the meta's first 512-byte sector); a **sector-aligned**
  meta tear yields a fully CRC-valid old-or-new meta that is resolved by **txnid
  selection**, not by CRC rejection. The harness author MUST NOT assert a CRC
  failure for sector-aligned tears (none fires); it asserts the REC-18 obligations
  (recover to `{N−1, N}`, check-tool clean) instead. This mechanism also validates
  REC-11 (reordering under `NO_SYNC`).
- **REC-20** — **Barrier model for the fault backend.** The backend tracks, per
  page write, whether it is "durable" (an fsync/msync covering it has returned) or
  "pending." A `crash()` call produces images where: every durable write is
  present and intact; each pending write is independently present-intact,
  present-torn, or absent; and pending writes may appear in any order relative to
  each other. The commit pipeline's fsync calls (C3, C5) are the barriers that move
  writes from pending to durable. This directly encodes REC-7: because C3 fsyncs
  data before C4 writes meta, no image can contain a durable meta `N` without
  durable `N`-data — the fault backend cannot construct that image, which is the
  formal statement of the crash-safety spine.
- **REC-21** — **Coverage target** (PLAN 1.11 acceptance): ≥ 10k crash-recovery
  cycles clean in CI across both mechanisms, over randomized write workloads
  (put/del/commit/abort, values 0 B–16 MB) and across the durability modes of §3
  (each mode asserting its own REC-18 obligation strength). A single clean run is
  not sufficient; the ≥10k-cycle bar is the milestone gate.

**M1.11 amendments (ADR-0008, Approved 2026-07-16 — implementation of this
section; behavior clarifications per CLAUDE.md rule 3):**

- **Cycle accounting (REC-21).** One *cycle* = one recovered-and-verified
  crash state: each materialized fault-plan image variant (mechanism 2) and
  each SIGKILL recovery (mechanism 1) counts as one (ratified OQ1). A `both`
  run targets ≈80/20 image/SIGKILL **by cycle** with a ≥1k-verified-SIGKILL
  floor on full (≥10k) runs (ratified OQ6).
- **`NO_SYNC`/`MAP_ASYNC` sub-model split (REC-11/REC-19, ADR-0008 D4).** The
  fault backend runs these modes under two materialization sub-models:
  *ordered* (pending writes persist only as an issue-order prefix, modeling an
  order-preserving filesystem) — REC-11's conditional guarantee applies, so
  the full REC-18 obligations are asserted; and *adversarial* (full REC-20
  drop/reorder/tear) — only "open never panics; errors confined to the
  designed taxonomy (`Invalid`)" is asserted, with walk/loss outcomes logged
  for characterization (ratified OQ3: asserting more would invent guarantees
  REC-11 does not make). Bounded-window modes (default, `WRITE_MAP`,
  `NO_META_SYNC`) run the full adversarial model **with** full REC-18
  assertions — REC-9 promises corruption-freedom there, with one
  precisely-scoped exception: `NO_META_SYNC` images landing in the
  reclaim-clobber window (REC-10 amendment) carry window/taxonomy
  obligations only and are counted as "stale fallbacks".
- **Legal-window encoding (REC-6/REC-18.2).** The harness derives each cut's
  legal recovered set as `[floor, ceil]` where floor/ceil are the txnids
  selected over the durable-only / all-applied materializations of the cut —
  a self-adapting encoding of the REC-6 rows. Two shape assertions ride on
  it: `ceil − floor ≤ 1` for bounded-window modes (a barrier failed to fold
  otherwise; under `NO_META_SYNC` this holds because every C3 `fdatasync`
  covers the whole file, folding the previous commit's pending meta too), and
  `floor ≥ acked-at-cut` for default/`WRITE_MAP` (REC-18.4 at the barrier
  level).
- **Env-creation window (REC-18.1 scope note).** A crash *inside env
  creation* (SPEC 02 §3.4/§3.5 — before any transaction exists) may leave a
  partially created store: a one-valid-slot open (REC-4) on a file shorter
  than two meta pages, or a designed `Invalid`. This window predates the
  commit protocol; REC-6 does not cover it, and the harness accepts either
  outcome only when **zero** commits were ever acknowledged. From the first
  acknowledged commit on, REC-18 applies in full.
- **Write-alignment audit (ADR-0008 D5).** Every commit write is a positive
  whole-page multiple at a page offset (the `Backing::write_at_page` shape
  makes the offset structural); the fault backend debug-asserts the length on
  every journaled write, so every crash cycle doubles as a continuous
  O_DIRECT-friendliness audit. `O_DIRECT` itself is deferred to Phase 3.5
  (io_uring backend).

---

## §6 — Cross-reference index

| Concern | Rule(s) | Interlocks with |
|---------|---------|-----------------|
| open-time meta selection | REC-1..5 | SPEC 02 §3.2, SPEC 04 TXN-65 |
| torn-meta CRC | REC-3/REC-8 | SPEC 02 §3.3, INV-2 |
| crash-stage invariants | REC-6/REC-7 | SPEC 04 §9 TXN-61/62 |
| durability flags | REC-9..12 | SPEC 01 §S6/§S7, SPEC 04 §9 |
| WRITE_MAP msync | REC-12 | SPEC 01 §S7 |
| fsync-gate / poison | REC-13 | SPEC 04 §8.1/TXN-60 |
| file growth safety | REC-14..16 | SPEC 05 GC-16/GC-28 |
| PREV_SNAPSHOT recovery + crash window | REC-5, REC-22 | SPEC 04 TXN-65..67 (self-resolving, §10) |
| crash-test protocol | REC-17..21 | PLAN 1.11, `zerodb-io` fault backend |

**Rule count: REC-1 … REC-22 (22 normative rules; REC-22 is the PREV_SNAPSHOT
crash-window warning, placed with §1's PREV_SNAPSHOT topic).**

> **Conflicts for human review (SPEC 06):**
> 1. **PREV_SNAPSHOT abandoned slot self-resolves (REC-5, SPEC 04 §10 TXN-67).**
>    The first post-rollback commit shares slot parity with the abandoned newer
>    snapshot, so it overwrites that slot in place — no explicit "rewrite-superseded
>    vs bump-past-stale" mechanism is needed (the earlier framing is retired). The
>    only residual, cosmetic question is txnid reuse orphaning a bounded page set
>    (milli/LMDB already tolerate it). No SPEC 02 format change.
> 2. **PREV_SNAPSHOT crash-consistency guard (REC-22).** Whether to refuse
>    PREV_SNAPSHOT unless the older meta's referenced pages verify intact (vs the
>    Phase-1 best-effort-plus-warning stance) is a **human decision pending** / ADR
>    seam. Phase 1 does not overclaim crash-proof rollback.
> 3. **One-valid PREV_SNAPSHOT → hard error (REC-2 †).** zerodb-defined (the fork
>    has no meta CRC, so its torn-slot behavior is unpinnable); **ratified 2026-07-16 — Phase 1 ships the documented warning only, no extra interlock; guard redesign deferred (Phase 3 candidate). Original note: human ratification
>    pending** on whether to instead serve the lone valid older slot.
> 4. **`NO_META_SYNC` reclaim-clobber window (REC-10 amendment, M1.11) —
>    RATIFIED 2026-07-17 (Quentin, standing directive, session lead): scoped claim adopted; steady-gated reclaim = Phase 3 candidate.** The ADR-0008 crash harness materialized a
>    legal power-loss image (repro seed 15797139550980166469) where the
>    fallback snapshot is structurally corrupted by a younger txn's legally
>    reclaimed pages — REC-10's original blanket "never corruption" overclaims.
>    Engine matches LMDB (`MDB_NOMETASYNC` shares the window; libmdbx's
>    steady/weak metas fix it). Decision needed: ratify the scoped claim +
>    Phase 3 steady-gating candidate (recommended, keeps Phase 1 parity), or
>    mandate an engine fix now (mdbx-grade: steady-gated reclaim needs a
>    survivable steady meta — effectively a third slot — i.e. an ADR-scale
>    format/GC change).
