# ADR-0012: Prefetch and access hints (`will_need` / `advise`)

- Status: Draft
- Milestone: 3.7
- Date: 2026-07-20

## Context

Milestone 3.7 exposes memory-access hints (madvise-family) so consumers can tell
the kernel which mmap pages they are about to touch. Unlike 3.4 (parked this same
day because its stated milli consumer turned out to be legacy — see PLAN §3.4 and
PROGRESS 2026-07-20), 3.7 has a **verified live consumer** whose hack we can read
line-by-line.

**The consumer, exactly.** hannoy `Reader::prefetch_graph`
(`hannoy/src/reader.rs:447-539`, called from `Reader::open` at `:419`) does today:

- reads a memory budget from the `READER_AVAILABLE_MEMORY` env var (`:463`);
- walks the HNSW graph **top-down** — all nodes in layers `>0` first (`:495`),
  then a BFS over layer-0 links (`:517`) — i.e. a graph-scattered, *ordered*
  sequence of item ids, not a contiguous key range;
- for each item's value bytes (already borrowed from the map via `iter`/`get`),
  computes the covering page range by hand and calls
  `madvise(start_page, size, WillNeed)` through the `madvise` crate on a **raw
  pointer into the mmap** (`madvise_page`, `:478-491`);
- decrements the budget by the advised size and **stops early** when it is spent
  (`:497`, `:518`);
- treats every failure as non-fatal ("It's OK for this operation to fail, it's
  not integral for search to work", `:444`);
- and `#[cfg]`-outs Windows entirely because "madvise crate does not support
  windows" (`:433-442`).

**What this tells the design.** The engine must provide a *hint primitive*, not a
prefetch *policy*. hannoy owns the graph and the budget; it knows which pages are
worth faulting and in what order — the engine knows none of that. The right thing
to lift into the engine is exactly the unsafe, unportable, page-arithmetic core
of `madvise_page`, leaving hannoy's traversal and budget loop untouched. This is
also what keeps the change small (rule 7): a single value→pages→WillNeed call.

**Forces.**
- *heed-contract tension.* The frozen contract (SPEC 00) is the heed surface;
  heed has no `will_need`/`advise`. So 3.7 is a **zerodb extension beyond heed**
  (the explicit remit of Phase 2/3). A heed-API consumer like hannoy must be able
  to reach it without forking heed and must still compile against upstream heed.
  This is a public-API-shape decision → ADR-gated (rule 6).
- *reads stay mmap* (PLAN §3.5/§3.7). Hints apply to the read mmap and are valid
  in WRITE_MAP mode too (WillNeed only faults pages in; it never mutates).
- *no new dependency.* `memmap2` (already on the allowlist) exposes safe
  `Mmap::advise_range(Advice, offset, len)` / `MmapMut::advise_range`. That
  replaces hannoy's `madvise`-crate dep, its raw-pointer arithmetic, its manual
  page alignment, and its Windows gap (memmap2 handles platforms). **No new
  `unsafe`** is required (see Decision).
- *advisory, non-observable.* madvise changes timing, never results. The oracle
  cannot (and must not) see it: a differential test asserts behaviour is
  unchanged; correctness of the hint is a *bench* question, not a parity one.
- *target platforms.* The win is cold-cache search latency on Graviton + EBS
  gp3, where a page fault is a network round-trip. That is precisely where
  hannoy added its hack; it is where the bench must run.

## Options

### Option A — slice-level `RoTxn::will_need(&self, bytes: &[u8]) -> Result<usize>` (primary)

Engine checks `bytes` lies within its mmap (address range test against
`base .. base+len`), converts to `(offset, len)`, calls
`mmap.advise_range(Advice::WillNeed, offset, len)`, and returns the number of
bytes advised (page-rounded). A slice not from the map returns a benign error
(prefetch is best-effort); no panic.

- **1:1 drop-in for the consumer.** hannoy replaces its unsafe `madvise_page`
  closure with `rtxn.will_need(item)?` and keeps its exact graph traversal +
  budget accounting (the returned byte count feeds the same `available_memory -=`
  line). It deletes the `madvise` dep, the raw pointers, the page math, the
  `page_size::get()` call, and the Windows `#[cfg]` fork.
- **Right altitude.** Policy (which items, what order, how much) stays in hannoy;
  the engine supplies only the map→madvise mechanism.
- **No new unsafe.** The in-map check is integer/address arithmetic on
  `as_ptr() as usize` (no deref, no provenance games needed to *call* the safe
  `advise_range`); it lives in `zerodb-io` (sanctioned mmap area) behind a safe
  signature. `advise_range` itself is safe memmap2 API.
- Cons: the caller must pass a slice that really is from this env's map; a stray
  heap slice is silently a no-op/err. Acceptable for a best-effort hint, and
  detectable in debug (see Consequences).
- Crash-safety: none — read-only hint, no page state changes, no fsync ordering.
- ARM: memmap2 issues the platform madvise; `WillNeed`/`Random`/`Sequential`
  all exist on linux-aarch64. macOS maps them to `posix_madvise`.

### Option B — key-level `Database::prefetch(&self, rtxn, keys: impl IntoIterator) -> Result<usize>`

Engine looks up each key, finds its value's page span from the leaf cell, and
advises. No caller-supplied pointers.

- Pro: fully encapsulated; no in-map pointer check.
- Cons: **wrong altitude and redundant work.** hannoy already holds each value's
  bytes from its own traversal; re-looking-up by key repeats the B+tree descent
  it just did. It also nudges prefetch *policy* (batch size, ordering) toward the
  engine, which cannot see the HNSW graph. A key-batch API cannot express "stop
  when my byte budget is spent" without leaking the budget into the engine.
- Verdict: reject as the primary. (A key-level convenience could be layered on A
  later for consumers that *don't* already hold the bytes; no consumer needs it
  now — YAGNI.)

### Option C — range-level `RoTxn::advise(&self, range: impl RangeBounds<Key>, hint: AccessPattern)`

Advise a contiguous **key range** with `WillNeed | Sequential | Random`.

- Pro: fits *scan-shaped* consumers — cursor iteration, `copy_to_file`
  compaction, `zerodb-tools dump` — where `Sequential` readahead is the right
  hint. This is the `txn.advise(range, …)` half of PLAN §3.7.
- Cons: **does not fit hannoy.** hannoy's access is graph-scattered, not a key
  range; `advise(range)` cannot express it. Different consumer, different shape.
- Verdict: real, but a *separate* consumer with no in-hand caller today. Stage it
  after A rather than bundle it (rule 7: don't fold orthogonal surface into the
  first change). Design sketch kept here so A's types don't foreclose it.

## Decision

Adopt **Option A (`RoTxn::will_need(bytes) -> Result<usize>`)** as the 3.7
primitive — it is the minimal, safe, no-new-unsafe replacement for the one
verified consumer's hack, and it leaves prefetch policy where the domain
knowledge is. Reject **B** (wrong altitude, redundant descent). Defer **C** to a
follow-up milestone/ADR-amendment when a scan consumer is wired (milli iteration
or compaction), so the first landing stays single-purpose.

**Surfacing (how a heed-API consumer reaches a non-heed method).** Provide the
method as an **extension trait** `ZerodbReadHints` (name TBD) exported from
`heed-zerodb`, implemented for its `RoTxn`, with the actual work in
`zerodb`/`zerodb-core`. hannoy imports the trait under the same `#[cfg]` it
already uses to gate the zerodb backend; on upstream heed the trait is simply not
in scope, so its existing `madvise_page` path stays. This keeps the frozen heed
surface (SPEC 00) untouched — the extension is additive and opt-in — and avoids
forking heed. (Alternative considered: a cfg-gated inherent method on the adapter
`RoTxn`; rejected because a trait composes better with hannoy's dual-backend
build and documents "this is beyond heed" at the call site.)

**Receiver.** `RoTxn`, matching PLAN's `txn.advise` phrasing and hannoy's
call-during-read ergonomics, even though the hint is snapshot-independent (the
map is env-wide). An `Env`-level twin can be added if a non-txn caller appears.

## Consequences

Easier: hannoy loses ~90 lines of unsafe/platform-specific prefetch and one
dependency; the hint becomes cross-platform for free; future scan consumers get a
natural home (Option C).

To build (all behind human approval — nothing implemented in this ADR):
- `zerodb-io`: `Mmap::advise_range(Advice, offset, len)` + `will_need(offset,
  len)` thin safe wrappers over `memmap2::advise_range`; and an in-map
  `offset_of(bytes) -> Option<usize>` address-range check. Miri can't exercise
  madvise → gate the syscall path `#[cfg(not(miri))]`, keep the offset math
  miri-clean and unit-tested.
- `zerodb-core`/`zerodb`: `RoTxn::will_need(&[u8]) -> Result<usize>`; benign
  error (reuse the closest heed-mirrored error, likely `Error::Io` of
  `InvalidInput`, or a dedicated non-fatal variant — see open questions) when the
  slice is out of map. `debug_assert` on the out-of-map path so tests catch a
  caller passing a heap slice.
- `heed-zerodb`: the `ZerodbReadHints` extension trait + impl.
- Consumer patch (`docs/patches/`): swap hannoy `madvise_page` → `will_need`;
  drop the `madvise` dep and the Windows `#[cfg]`; keep `READER_AVAILABLE_MEMORY`
  as hannoy's own budget knob (engine stays policy-free).

Tests / invariants:
- Oracle: a differential test asserting `will_need` is **semantically invisible**
  — same key/cursor results with and without the call (parity must not move).
- A unit test that `will_need` on an out-of-map slice is a benign no-op/err (no
  UB, no panic) and that an in-map value advises `len`-covering pages.
- Bench (required by CLAUDE.md for the perf claim): hannoy cold-cache search
  latency with vs without `will_need`, on linux-aarch64 + EBS gp3 (drop caches
  between runs). No claim ships without the criterion/latency diff pasted in the
  summary.

SPEC: add a **new section for zerodb-beyond-heed extensions** (SPEC 00 is the
frozen heed contract and must not absorb non-heed surface). Proposal: a new
`docs/SPEC/07-extensions.md` (or a clearly fenced "Extensions (non-heed)" appendix
in SPEC 00) documenting `will_need`'s contract: advisory, best-effort, no
observable semantic effect, benign on out-of-map input. Decide the location as
part of approving this ADR.

## Open questions for human review

1. **Surfacing:** extension trait in `heed-zerodb` (recommended) vs cfg-gated
   inherent method? Trait name (`ZerodbReadHints`?) and crate location.
2. **Error contract:** out-of-map / unsupported-platform → `Ok(0)` (pure
   best-effort, never errors) vs a benign `Err` the caller may ignore? hannoy
   treats failure as fine either way; `Ok(0)` is simplest and cannot tempt a
   caller into `?`-propagating a non-fatal hint.
3. **Include `advise(range, Sequential|Random)` (Option C) now or defer?** Defer
   is recommended (no in-hand consumer), but confirm — if compaction/dump want
   `Sequential` readahead soon it may be worth one landing.
4. **Receiver:** `RoTxn` only, or also `Env`/`RwTxn`? (Deferring the twins.)
5. **SPEC home** for non-heed extensions: new `07-extensions.md` vs fenced SPEC 00
   appendix.
6. **`will_need` vs `prefetch` naming**; and whether the returned `usize` is
   advised-bytes (feeds hannoy's budget directly) — recommended — or a unit.
