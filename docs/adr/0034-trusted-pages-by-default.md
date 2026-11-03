# ADR-0034: Trust page contents by default, as LMDB does

- Status: **Accepted (Option B, maintainer, 2026-11-02)**
- Related: ADR-0014 (the trusted-file policy this makes the default),
  divergence D-019, issue #147
- Date: 2026-11-01

## Context

ZeroDB checks every map page structurally the first time a transaction sees
it, so that a corrupt or hostile data file returns a typed error instead of
causing undefined behaviour. LMDB does no such check: it reads pages through
raw pointers and treats the file as trusted. ADR-0014 added a policy that
reads pages the way LMDB does, `FileTrust::trust_contents()`, but kept
validation as the default (divergence D-019: "opt-in, default validating").

The maintainer decided on 2026-11-01 to promote the trusting policy instead:
"promote only the trusted mode because it's what LMDB does". It matches the
project's rule in `CONTRIBUTING.md`: default behaviour matches LMDB, and
capabilities LMDB lacks ship behind explicit options. Validation is such a
capability.

### What validation costs today

Graviton4 `c8g.xlarge`, same binary, validating vs trusting:

| Path | Validating | Trusting | Source |
|---|---:|---:|---|
| Full scan (`scan/full/fwd`, `rev`) ÷ LMDB | 1.47× | 1.05–1.09× | the 2026-10-31 "build profiles and scans graviton" run |
| Full scan over a 300 MB tree (`scan/full/big`) ÷ LMDB | 1.60× | 1.03× | same |
| Prefix bucket (`scan/prefix/bucket`) ÷ LMDB | 1.27× | 1.08× | same |
| Random get p50, 10M keys (LMDB 0.96–1.08 µs) | 1.23 µs | 1.12 µs | the 2026-10-31 "stamp cache graviton" run |
| Get in a 50/50 read/update mix p50 (LMDB 1.13 µs) | 1.39 µs | 1.27 µs | same |
| Values in overflow pages (`get/val/v4k`, `v2page`) ÷ LMDB, bench server | 1.29×, 1.27× | 0.91×, 0.89× | ADR-0014, overflow amendment |

The validating default also keeps an environment-wide validated-pages cache
(`crates/zerodb-core/src/stamps.rs`, up to 64 MiB per environment) and the
work in issue #147 lead 2 exists only because of it.

### What stays checked under the trusting policy

Unchanged from ADR-0014: page numbers against the snapshot's high-water
(reads never leave the committed map), page type and the O(1) header fields
(reserved fields, free-space bounds), the meta pages at open (checksum,
geometry), and every free-list page id before it is handed out. The per-cell
walk does not run, and overflow values are read without their run header.
`zerodb-tools check` (`check::check_image`) always validates fully.

### The soundness constraint

`FileTrust::trust_contents()` is `unsafe` because, with it, a corrupt or
modified file is undefined behaviour. A default cannot be trusting behind a
safe constructor, or safe code could reach that undefined behaviour:

- **heed adapter** (`crates/heed-zerodb`): `EnvOpenOptions::open` is already
  `unsafe`, as it is in heed. heed 0.22.1 documents it as `unsafe` "because
  of the potential for Undefined Behavior (UB) using the map if the
  underlying file is subsequently modified, in or out of process". The
  caller already accepts that a file changed behind the engine's back is
  undefined behaviour, which is most of what `trust_contents` asks.
  Meilisearch and hannoy open every environment through this path.
- **Native API** (`crates/zerodb`): `EnvOpenOptions::open` is safe.
  `zerodb-tools`, the crash harness and most engine tests use it.

## Options

### Option A — Trusting by default in the heed adapter only

`heed_zerodb::EnvOpenOptions` defaults to trusting; `file_trust(FileTrust::VALIDATE)`
opts back into validation. The native `zerodb::EnvOpenOptions` keeps
validating by default, because its `open` is safe.

- **Mechanism:** the adapter stores `Option<FileTrust>`, `None` by default.
  Its `unsafe fn open` resolves `None` to `unsafe { FileTrust::trust_contents() }`,
  justified by the contract of `open` itself. That is one new `unsafe` block in
  `heed-zerodb`, which the `unsafe` policy in `CONTRIBUTING.md` must list.
- **Pros:** every consumer of the heed API gets LMDB's read cost with no code
  change; the obligation rides on a contract heed users already accept; the
  native safe API stays sound; tools keep validating files of uncertain
  origin.
- **Cons:** two defaults in one project; the adapter's `# Safety` section on
  `open` grows a sentence; tests that feed corrupt files through the heed
  adapter must ask for validation explicitly.

### Option B — Trusting by default everywhere; native `open` becomes `unsafe`

Both `EnvOpenOptions` default to trusting, and `zerodb::EnvOpenOptions::open`
becomes `unsafe fn`, carrying the same contract.

- **Pros:** one default across the project; the native API matches LMDB's
  model exactly.
- **Cons:** every native caller (tools, benches, examples and the test files
  in `crates/zerodb/tests`) gains an `unsafe` block or must pass `VALIDATE`; `zerodb-tools`
  would have to opt into validation explicitly; a breaking change to the
  native API with no consumer asking for it.

### Option C — Keep validating by default

Status quo. Rejected by the maintainer's decision above.

## Decision

**Option B.** Both `EnvOpenOptions` default to trusting, and
`zerodb::EnvOpenOptions::open` becomes `unsafe fn`, carrying the same
contract as heed's `open`. The maintainer chose one default across the
project, matching LMDB's model on every API, over keeping the native `open`
safe.

The drafted recommendation was Option A; it is superseded by this decision.

### How the default is held

`zerodb-core`'s open functions are safe and take a `FileTrust`, so a
trusting value obtained without `unsafe` would let safe code open a file
without validation. The builders therefore never store the trusting value
as their default: they store `Option<FileTrust>`, `None` meaning "the
default", and `unsafe fn open` resolves `None` to
`FileTrust::trust_contents()` under its own contract.
`zerodb::EnvOpenOptions::get_file_trust` returns that `Option`.
`FileTrust::trust_contents` stays `unsafe`.

### Answers to the review questions

1. Option B.
2. Both `open`s state the file-integrity obligation in full in their
   `# Safety` section: heed's wording covers a file modified while mapped,
   not one that was already corrupt or hostile when opened.
3. The validated-pages cache stays, built only under the validating policy.

## Consequences

- **Divergence D-019** changes from "opt-in, default validating" to: pages
  are trusted by default, as in LMDB; validation is an opt-in extension.
  D-019 stops being a behaviour difference from LMDB.
- **Native API break:** `zerodb::EnvOpenOptions::open` is `unsafe fn`, and
  `get_file_trust` returns `Option<FileTrust>`. ZeroDB has no external users
  of the native API yet.
- **`unsafe` policy (`CONTRIBUTING.md`):** calling the native `open` is
  sanctioned wherever an environment is opened (the heed adapter, tools,
  tests, benches, examples, the oracle), each call with a `// SAFETY:`
  comment naming why the file is ZeroDB's own or that the call validates.
  The adapter's `open` resolves the default inside its own `unsafe fn`.
- **`zerodb-tools`** opts into `FileTrust::VALIDATE` for every environment
  it opens: it inspects files of uncertain origin. `check` validates fully
  regardless.
- **Docs to update in the implementing change:** both `open`s (`# Safety`:
  the file must have been written by ZeroDB and not modified by anything
  else, or the caller opts into `FileTrust::VALIDATE`) and both
  `file_trust` setters; `docs/LMDB-COMPATIBILITY.md` and its D-019;
  `docs/SPEC/02-pages.md` and `docs/SPEC/04-txn-mvcc.md` (which policy is
  the default); ADR-0014's status line; `docs/PERFORMANCE.md` (the trusted
  column becomes the default one); README wording that promises a typed
  error on corruption.
- **Snapshots of uncertain origin:** Meilisearch can import a raw snapshot a
  user supplied. With LMDB that file is trusted today, so this matches
  current exposure; the `open` docs point to `zerodb-tools check`, or to
  opening such a file with `FileTrust::VALIDATE`.
- **Tests:**
  - tests that open corrupt or hostile files set `FileTrust::VALIDATE`
    explicitly; hostile-image fuzzing (`fuzz/fuzz_targets/fuzz_image_open.rs`)
    and `fuzz_page_decode` open through `zerodb-core` with the validating
    policy and are unchanged;
  - the crash harness opens with `FileTrust::VALIDATE`: its files are valid
    only if the engine is, and a torn file must be a reported failure rather
    than undefined behaviour;
  - the oracle differentials, `diff_ops` fuzzing and the heed suite run on
    valid files and must pass unchanged under the new default;
  - tests that both builders default to trusting and that
    `file_trust(VALIDATE)` restores typed errors on a corrupted page.
- **Measurement gate (three columns: LMDB, ZeroDB before, ZeroDB after):**
  the read ladder `^(get|seek|scan)/`, `commit/batch/n1`, and the
  Meilisearch and hannoy consumer suites (`scripts/consumer.sh`) on Graviton4.
  Keep the change if no consumer workload regresses; the micro-ladder is
  expected to match today's trusting column.
