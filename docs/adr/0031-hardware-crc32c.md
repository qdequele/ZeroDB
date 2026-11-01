# ADR-0031: Hardware CRC32C for the meta checksum

- Status: **Accepted (Option B, aarch64 only, maintainer, 2026-10-19),
  implemented.** It amends the `unsafe` policy in `AGENTS.md`. Drafted as
  ADR-0029 and renumbered on 2026-10-20, when the branch was rebased on
  `main`, where 0029 is the free-page run search.
- Related: #117 (the slicing-by-8 table, the software path on `main`);
  reports `benches/results/archive/2026-10-19-meta-crc-slicing.md`,
  `-hardware.md` and `-graviton.md`; ADR-0002 §D1; ADR-0022.
- Date: 2026-10-19

## Context

Every commit writes a meta page whose CRC32C covers bytes `[0, 172)` plus
the free-list annex ids (SPEC 02 §3.3, ADR-0022). The annex holds up to
`(psize − 176) / 8` page ids, 2,026 at 16 KiB pages. Under sustained
single-delete commits on a large tree it stays between 1,000 and 2,000 ids
most of the time, so each commit checksums 8–16 KiB. Opening an environment
and refreshing a snapshot written by another process (`read_snapshot_at`,
ADR-0023) checksum the same bytes.

The byte-at-a-time table ran at 0.39 GB/s, so a single-delete commit took
36 µs (LMDB: 7–8 µs). #117 replaced it with slicing-by-8 (about 1.9 GB/s on
the Mac: 8.8 µs at a full 16 KiB annex). While this ADR was written, an
unmerged slicing-by-16 variant was measured too (2.9 GB/s: 5.6 µs at a full
annex, a single-delete commit at 17 µs); when the branch was rebased on
`main` it was dropped in favor of #117's table. The spike numbers below that
say "slicing-by-16" come from that variant. Against slicing-by-8 the
hardware path gains a little more (see Implementation).

ADR-0002 §D1 chose CRC32C because CPUs compute it in hardware: ARMv8 has the
`crc32c{b,h,w,x}` instructions (the `crc` feature: optional in ARMv8.0,
mandatory from ARMv8.1, present on every Graviton and Apple M-series core),
and x86-64 has `crc32` in SSE4.2. It planned to switch to them in Phase 3.9,
when data pages get checksums too. Nothing in this ADR depends on Phase 3.9.

LMDB has no meta checksum (`mdb_env_write_meta` writes ~100 bytes of meta
fields and nothing else), so there is no LMDB technique to copy here. This is
the cost of a deliberate difference (PERF-GAP "Deliberate differences"), and
the aim is to make it negligible.

### What Rust requires

The intrinsics are `std::arch::aarch64::__crc32cd` and friends (and
`core::arch::x86_64::_mm_crc32_u64`). They are declared with
`#[target_feature(enable = "crc")]`. With Rust ≥ 1.86 (MSRV 1.98) a function
that carries the same attribute can call them without `unsafe`. Calling such
a function from ordinary code is `unsafe`, even when the feature is enabled
for the whole build. The spike hit exactly that on `aarch64-apple-darwin`,
where `crc` is on by default (`error[E0133] … the crc target feature being
enabled in the build configuration does not remove the requirement`). So any
hardware path needs at least one `unsafe` call.

The feature also cannot be assumed at compile time on the primary target:
`rustc --print cfg --target aarch64-unknown-linux-gnu` enables only `neon`,
and `x86_64-unknown-linux-gnu` only SSE2. A compile-time-only path would
never run on Graviton without a `-C target-feature=+crc` that consumers do not
pass. Runtime detection (`std::arch::is_aarch64_feature_detected!("crc")`,
cached by std in an atomic; constant-true where the target enables the
feature, as on macOS) is what makes it run there.

`zerodb-core` is `#![deny(unsafe_code)]`, with narrow `#[allow(unsafe_code)]`
on each sanctioned site (`lib.rs` lists them). The `unsafe` policy in
`AGENTS.md` lists `zerodb-core::page` for mmap access and page casting only.
CPU intrinsics are not on the list, hence this ADR.

### Spike (Apple M-series, macOS, scratch code, not merged)

CRC32C throughput, same pseudo-random input, output checked equal to the
table path at every size:

| input | slicing-by-16 | hardware, 1 stream | hardware, 3 streams (no combine) |
|---|---:|---:|---:|
| 172 B (empty annex) | 69 ns | 11 ns | 7 ns |
| 4,172 B (500 ids) | 1,415 ns (2.95 GB/s) | 494 ns (8.4 GB/s) | 170 ns |
| 8,172 B (1,000 ids) | 2,748 ns (2.97 GB/s) | 994 ns (8.2 GB/s) | 332 ns |
| 16,380 B (full 16 KiB) | 5,571 ns (2.94 GB/s) | 2,006 ns (8.2 GB/s) | 685 ns |

The 3-stream column leaves out the step that merges the three partial CRCs,
so it is a lower bound.

End to end, the one-stream path wired into `crc32c.rs` behind runtime
detection (the shape of Option B below). With it, the existing CRC tests
pass, including the bit-at-a-time reference comparison. Delete repro:
200,000 random 26-byte keys with 1,500-byte values, 16 KiB pages,
`WRITE_MAP | MAP_ASYNC | NO_SYNC`, trusted-file mode, every key deleted in
shuffled order, one delete per write transaction. 5 interleaved runs, median
µs per commit:

| LMDB | ZeroDB, slicing-by-16 | ZeroDB, hardware 1 stream |
|---:|---:|---:|
| 6.72 | 16.90 | 14.85 |

12 % off the commit on this workload. Commits with a short annex (most
`commit/*` rungs) save well under 1 µs.

## Options

### Option A — Keep the software table (status quo)

No `unsafe`, no policy change. With slicing-by-8 the CRC stays about 5 µs of
a delete-heavy commit on the Mac and 8.8 µs at a full 16 KiB annex. A 64
KiB-page environment (allowed, SPEC 02) would pay about 35 µs at a full
annex.

### Option B — Hardware CRC, one stream, aarch64, runtime detection (recommended)

In `zerodb-core::page::crc32c`:

```rust
#[cfg(all(target_arch = "aarch64", not(miri)))]
#[target_feature(enable = "crc")]
fn crc_update_hw(mut crc: u32, bytes: &[u8]) -> u32 {
    // 8 bytes per __crc32cd, the tail per __crc32cb; no `unsafe` inside.
}

fn crc_update(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: `crc_update_hw` only requires the `crc` CPU feature,
        // which was just detected on this CPU.
        #[allow(unsafe_code)]
        return unsafe { crc_update_hw(crc, bytes) };
    }
    crc_update_table(crc, bytes) // the slicing-by-8 table, unchanged
}
```

- **`unsafe` footprint:** one block, one line, one `#[allow(unsafe_code)]`.
  Its only obligation is "the CPU has `crc`", discharged by the line above it.
  There is no pointer, aliasing or memory-ordering reasoning. Loads go through
  `as_chunks::<8>()` and `u64::from_le_bytes`, so there are no unaligned
  casts (AGENTS.md's `read_unaligned` rule does not even come into play).
- **Fallback:** the slicing-by-8 table stays. It serves aarch64 CPUs without
  the feature, every other architecture, and miri (which cannot execute the
  intrinsic, so `not(miri)` routes it to the table).
- **Correctness argument:** CRC32C is a function, so either path must give the
  same value for every input. A wrong result would make a valid meta look
  torn (open falls back to the older slot, or fails) or, far less likely,
  accept a torn one. Tests (below) compare the hardware path with the table
  path and with the bit-at-a-time reference on every length 0–300 from every
  offset, and on 16 KiB pages. CI runs them on `ubuntu-24.04-arm`, where the
  hardware path is taken, and on x86-64, where the table path is.
- **Crash safety:** unchanged. Same checksum value, same coverage, same write
  ordering. Files are byte-identical.
- **ARM:** this is the primary-target path (Graviton 2/3/4 all have `crc`).
  No dependence on OS page size or on memory ordering.
- **Cons:** the first CPU-feature-specific code in the engine; the policy list
  grows by one entry.

### Option C — Option B plus x86-64 (SSE4.2 `_mm_crc32_u64`)

Same structure behind `is_x86_feature_detected!("sse4.2")`. Gives development
machines and x86 CI the same speed. Costs a second `unsafe` site and a second
intrinsic family. x86-64 is a development platform here (AGENTS.md "Target
platforms"), and this spike could not measure it (no x86 host at hand). It
can be added later under the same rule if wanted.

### Option D — Hardware CRC, three interleaved streams

`crc32cx` has a latency of about 3 cycles but issues one per cycle, so three
independent streams run nearly 3× faster (685 ns at 16 KiB in the spike,
before merging). The three partial CRCs must then be combined: shift each
forward by the length of what follows it and XOR. The shift is a
multiplication by `x^(8n)` modulo the polynomial. Doing it fast needs the
`pmull` carry-less multiply (`aes` feature, more intrinsics and `unsafe`), or
a safe GF(2) matrix/table shift (tens of ns). It saves about 1 µs more per
delete-heavy commit than Option B, for clearly more code and a harder
correctness argument. Worth revisiting only if the meta CRC shows up again
after Option B, or when data pages get checksums (Phase 3.9) and the volume
checksummed per commit grows with every dirty page.

## Decision

**Option B, approved by the maintainer on 2026-10-19 (aarch64 only; x86-64
keeps the table path).** It takes the speed
ADR-0002 §D1 chose CRC32C for, on the primary platform, with one detected,
single-line `unsafe` call and the current safe table kept as fallback and
oracle. C and D can follow under the same rule if numbers ask for them.

## Consequences

If Option B is approved:

- **`AGENTS.md`, `unsafe` policy:** add an entry, for example: "the CRC32C
  hardware path in `zerodb-core::page::crc32c` (ADR-0031): one call of the
  `#[target_feature(enable = "crc")]` checksum function, guarded by runtime
  detection of that CPU feature". Update the sanctioned-sites comment in
  `crates/zerodb-core/src/lib.rs` to match.
- **Tests:** keep `matches_bitwise_reference` and
  `concat_matches_whole_at_every_split` (they exercise whichever path the CPU
  takes). Add a test that calls the table path and, when the feature is
  detected, the hardware path directly on the same inputs and asserts
  equality, so both stay covered on aarch64 CI. Miri keeps covering the
  table path.
- **Benchmarks:** `scripts/bench-ab.sh` on `^del/random/n1` (the rung that
  fills the annex) and `^(commit/|env/txn/)`, three columns, on this Mac and,
  if a host is available, on Graviton, where the table/hardware ratio may
  differ. A perf-ledger entry and a scorecard.
- **SPEC:** none. SPEC 02 §3.3 defines the checksum value, not how it is
  computed. ADR-0002 §D1's "Phase 3.9 switches to the hardware instructions"
  becomes "done for the meta CRC (ADR-0031)".
- **Durability, format, API, LMDB parity:** unchanged.

### Implementation

`e6d69fa` (2026-10-20), on `main`'s slicing-by-8 table (#117). First built on
the slicing-by-16 variant (pre-rebase commit `b2f5b6c`) and measured there:
on the Mac, 16 KiB CRC 5.66 → 1.99 µs, single-delete commit 16.71 →
14.64 µs (LMDB 6.62), crud-bench deletes 0.82× → 0.93× LMDB; on Graviton4
(`benches/results/archive/2026-10-19-meta-crc-graviton.md`), 16 KiB CRC 5.66 →
0.75 µs, single-delete commit 0.91× (4 KiB pages) and 0.83× (16 KiB) of the
slicing-by-16 one.

Rebased build against `main`, Mac, busy host (load 15–22; absolute numbers
inflated, see `benches/results/archive/2026-10-19-meta-crc-hardware.md`): 16 KiB
CRC 10.53 → 2.44 µs; single-delete commit 21.96 → 17.19 µs, paired
0.68–0.80 over 7 runs. A 3-round criterion run at load 14 was invalid (LMDB
drift up to 16 %); `del/random/n1` read 0.772 (±0.288). Not rerun on
Graviton.

## Open questions for human review

1. ~~Approve amending the `unsafe` policy for this one call site (Option
   B)?~~ Approved, 2026-10-19.
2. ~~Include x86-64 now (Option C)?~~ No: aarch64 only, 2026-10-19.
3. ~~Is a Mac A/B enough to merge, or should it wait for a Graviton run?~~
   Graviton4 run done, 2026-10-19 (`benches/results/archive/2026-10-19-meta-crc-graviton.md`):
   run-time detection takes the hardware path on Linux aarch64, where the
   feature is off at compile time; CRC of 16 KiB 5.66 → 0.75 µs; the hardware
   step alone makes a single-delete commit 0.91× (4 KiB pages) and 0.83×
   (16 KiB) of the slicing one; with both steps `del/random/n1` goes from
   2.01× to 1.64× LMDB (0.814 ±0.039) and crud-bench deletes 1.44–1.49×, no
   rung regressed. (Those runs measured the slicing-by-16 variant; see
   Implementation for the rebased build.)
