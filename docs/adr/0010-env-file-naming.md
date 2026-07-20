# ADR-0010: Env directory presentation — the data-file name (`zerodb.dat` vs heed's `data.mdb` contract)

- Status: **Approved** — Option A adopted (approved by Quentin, 2026-07-20,
  standing directive as session lead). **Implemented 2026-07-20**; see
  §Consequences for the acceptance criteria and PROGRESS.md for the landing
  note. D-012 is flipped to APPROVED/resolved.
- Milestone: filed against the **M1.14 gate remainder** (this was a gate
  blocker: compaction and snapshot-restore were latent-broken behind a green
  suite).
- Date: 2026-07-20
- **Numbering note (resolved):** ADR number 0009 is taken by
  [`0009-copy-and-tools.md`](0009-copy-and-tools.md); this document is
  **ADR-0010** and now lives at `0010-env-file-naming.md`, matching the
  DECISIONS.md index.

## Context

### The two contracts

- **heed / LMDB on-disk contract (SPEC 00 row 7):** every consumer opens a
  **directory** env (never `NO_SUB_DIR`); LMDB materializes it as
  `<dir>/data.mdb` (the map) + `<dir>/lock.mdb` (reader table). The *directory*
  is the API-level env identity, but the **file name `data.mdb` leaks into
  consumer code** (below).
- **zerodb contract (SPEC 02 §8, M1.2):** the env is a single regular file
  **`zerodb.dat`** inside the env directory; no lock file exists at all (D-001,
  single-process). The name is a single constant, `zerodb::DATA_FILE_NAME`
  (`crates/zerodb/src/lib.rs:34`), joined onto the canonicalized dir in
  `EnvOpenOptions::open` (`lib.rs:267`). Everything below that is
  **name-agnostic**: `zerodb_io::open_or_create` takes the full `data_path`
  (`crates/zerodb-io/src/lib.rs:183`). The name also appears in
  `zerodb-tools` (the flock guard and dir probes, `src/lock.rs` /
  `src/commands.rs`), the oracle's env-lifecycle/crash fixtures, and a number
  of engine tests that hand-craft images.

**D-002 does not cover this.** D-002 sanctions zerodb's own on-disk **format**;
it says nothing about the **file name** heed consumers observe in the env
directory. The name difference is a separate, unsanctioned divergence — filed
as **D-012** (docs/DIVERGENCES.md) alongside this ADR.

### The latent break (found during the M1.14 index-scheduler gate)

Meilisearch hardcodes `data.mdb` in **production** code, not just tests.
Empirically confirmed: an adapter-created env dir contains exactly
`["zerodb.dat"]` — no `data.mdb`, no `lock.mdb`. All suites are green **only
because index-scheduler ships no compaction or snapshot-restore round-trip
test**. The call sites and their exact failure modes on a zerodb env:

| Site | What it does | Failure mode on zerodb |
|------|--------------|------------------------|
| `index-scheduler/src/scheduler/process_batch.rs:707` | Index compaction: `index.path().join("data.mdb")` → `fs::metadata(...).len()` for the pre-compaction size, then `copy_to_file` into `data.mdb.cpy` and `persist()` (rename) over `data.mdb`, close + reopen the index | **Loud failure**: the `fs::metadata` probe hits ENOENT and the compaction task errors out before copying. (Were the probe absent, the rename would land the compacted image under a name the env never reads — silent no-op compaction.) |
| `meilisearch/src/routes/tasks/compact.rs:158,161` | Tasks-env compaction route, same probe + `data.mdb.cpy` + persist pattern | Same loud ENOENT. (Site not in the original report; found by grep.) |
| `index-scheduler/src/scheduler/process_snapshot_creation.rs:138,202,212` | Snapshots: `env.copy_to_path(dst.join("data.mdb"), option)` for tasks env, every index, and the auth env | **Silent data loss on restore**: `copy_to_path` succeeds (the adapter writes a zerodb-format image at whatever path it is given), producing a snapshot dir whose only file is `data.mdb`. Restoring extracts it and opens the dir; zerodb looks for `zerodb.dat`, finds nothing, and **creates a fresh empty env**. The worst failure class in this table. |
| `index-scheduler/src/scheduler/enterprise_edition/s3.rs:238,301,311` | S3 snapshot streaming: tarball entry paths `tasks/data.mdb`, `indexes/<uuid>/data.mdb`, `auth/data.mdb` (bytes from the real env fd) | Same silent-empty-restore class: the streamed bytes are genuine, but the extracted dir is unopenable as a populated zerodb env. |
| `meilitool/src/main.rs:491,492` | Offline compaction: joins `data.mdb` / `data.mdb.cpy` | Loud ENOENT, same as process_batch. |

### `lock.mdb` — does anything touch it? (grepped, read-only)

Grep of the Meilisearch clone for `lock.mdb`: **exactly one hit**, a doc
comment (`index-scheduler/src/index_mapper/mod.rs:489` — "The folder located at
this path is containing the data.mdb, the lock.mdb and an optional
data.mdb.cpy file"). **No production or test code stats, opens, copies,
deletes, or otherwise depends on `lock.mdb`.** This matches LMDB's own
behavior: `lock.mdb` is recreated on open, and Meilisearch's snapshot/copy
paths deliberately ship only `data.mdb`. Conclusion: **zerodb does not need a
`lock.mdb` at all** — D-001 stands, and fabricating a placeholder would be a
lie (a file implying cross-process locking the engine does not provide).

### Constraints on any fix

- **ADR-0003 / M1.13's value proposition**: milli + hannoy (and by extension
  Meilisearch) build on zerodb with **zero `.rs` edits** via the `[patch]`
  shim. The fix must preserve that property or it forfeits the adapter's whole
  point.
- The M1.13 precedent for exactly this shape of problem is the
  **adapter-boundary re-imposition** pattern (DIVERGENCES.md, D-006/D-008/
  D-010): the core keeps its lenient/native behavior; `heed-zerodb` re-imposes
  the fork's observable contract at the heed boundary.
- **Migration**: there are **no zerodb envs in production anywhere** (Phase 1
  is pre-adoption). Only dev/CI throwaway envs exist. Migration cost is
  therefore near-zero regardless of option, and this is the cheapest moment
  this decision will ever have.

## Options

### Option A — core keeps `zerodb.dat`; `heed-zerodb` presents `data.mdb`

Add a data-file-name knob to the native open path
(`zerodb::EnvOpenOptions::data_file_name(...)`, default `DATA_FILE_NAME =
"zerodb.dat"`); the adapter sets `"data.mdb"` unconditionally. `zerodb-io` is
already name-agnostic, so the plumbing is one option field threaded to the
existing `canonical_dir.join(...)` at `lib.rs:267`.

- **Zero-consumer-source-edits:** ✔ preserved. All five call-site families
  above become correct: the metadata probe finds the file; snapshot dirs
  containing `data.mdb` reopen through the adapter; the compaction
  rename-over-and-reopen flow is coherent (POSIX rename over an mmapped file
  is fine — the old inode persists until the env closes and reopens, which is
  exactly Meilisearch's step-5/6 close-reopen sequence).
- **Operators/tooling:** the file name now depends on which stack created the
  env — native zerodb says `zerodb.dat`, Meilisearch-via-adapter says
  `data.mdb`. An operator pointing `mdb_stat`/`mdb_dump` at an adapter env
  gets LMDB's `MDB_INVALID` ("not a valid LMDB file") — confusing but *loud*
  and immediate (the `ZDB1` magic guarantees rejection, not misreading).
  `zerodb-tools` must probe both names (see Consequences).
- **Core cost:** small — one builder option, tools probe order, SPEC 02 §8
  amendment, a handful of test updates. No format change (`format_version`
  stays 1; the name is outside the format).
- **Migration:** none required (no production envs). Pre-change dev envs need
  a one-time `mv zerodb.dat data.mdb` if reopened through the adapter.
- **`lock.mdb`:** not created (grep says nothing needs it).
- Crash-safety: **no write-sequence changes** — the name is resolved once at
  open, before any write; every SPEC 06 invariant is name-independent.
- Consistent with the established D-006/D-008/D-010 boundary-re-imposition
  pattern: the heed contract is a heed-boundary concern.

### Option B — core renames its data file to `data.mdb` unconditionally

One name everywhere; `DATA_FILE_NAME = "data.mdb"`.

- **Zero-consumer-source-edits:** ✔ preserved. Maximum compatibility, no
  adapter/native split, tools stay single-name.
- **But the name is a standing lie.** Every zerodb env everywhere — including
  native, non-heed uses — claims the `.mdb` extension while holding `ZDB1`
  bytes. That actively misleads operators, backup/triage tooling, and
  `mdb_stat`-style utilities *in contexts that have nothing to do with heed
  compatibility*. It also degrades `migrate-from-lmdb` ergonomics: a dir
  containing `data.mdb` becomes ambiguous ("LMDB env to migrate, or already
  zerodb?") — the magic remains the true discriminator, but every name-led
  operator workflow loses its cheap signal. (Honesty note: the name was never
  a *format* guarantee even for LMDB — 0.9 vs 1.0 share it incompatibly — but
  `.mdb` strongly connotes the LMDB family, and zerodb is not in it.)
- **Core cost:** const rename + broad test/docs churn (30+ files reference the
  name); SPEC 02 §8 amendment.
- **Migration:** same near-zero as A.
- Rejected as the primary choice: it buys nothing over A at the heed boundary
  (A already achieves exact parity there) and pays a permanent identity cost
  in the native/tools/operator surface.

### Option C — core keeps `zerodb.dat`; patch the consumers

Carry Meilisearch/meilitool patches replacing the `"data.mdb"` literals.

- **Zero-consumer-source-edits:** ✘ **destroyed** — this breaks the exact
  property M1.13 delivered and ADR-0003 chose its whole strategy around
  ("zero `.rs` edits in the happy path"). Every upstream Meilisearch change to
  these five files re-breaks the patch; third-party heed users get nothing.
- **Rejected.** Recorded only to show it was weighed.

### Option D — hybrids

- **D1 — A + a `lock.mdb` placeholder:** rejected. The grep shows nothing
  reads, stats, or deletes `lock.mdb`; an empty placeholder would imply
  cross-process locking that D-001 explicitly does not provide, and would be
  dutifully shipped into snapshots as a junk file. Write nothing extra.
- **D2 — A + an engine-marker file** (e.g. `zerodb.engine` declaring the real
  engine): rejected as a *file*. The authoritative discriminator already
  exists — the first four bytes of the data file are the `ZDB1` magic
  (SPEC 02 §1/D10) — and tools/check already key on magic, not name. Adopted
  in spirit only: `zerodb-tools stat` should print the engine/format line so
  operators get the "what is this really?" answer from the tool, not from a
  marker file.
- **D3 — engine-side fallback probing** (open `data.mdb`, else `zerodb.dat`):
  rejected for the **engine** open path — a dir containing both would be
  ambiguous, and the adapter must be deterministic about which file it
  creates. Adopted for **tools only** (read paths where ambiguity can be made
  a hard error; see Consequences).

## Decision

**Adopt Option A** (approved; implemented 2026-07-20): the core keeps
`zerodb.dat` as its native default; `zerodb::EnvOpenOptions` gains a
`data_file_name` option; `heed-zerodb` sets it to `"data.mdb"`
unconditionally, so every env opened through the adapter materializes as
`<dir>/data.mdb` — and **no `lock.mdb` is ever created** (nothing consumes
it; D-001 stands).

Rationale: Option A is the only choice that fixes all five production call
sites while preserving *both* identities that matter — the zero-consumer-
source-edits property that is the adapter's entire value proposition
(ADR-0003), and zerodb's honest native identity (a `ZDB1`-format file should
not masquerade under LMDB's extension in contexts that never touch heed). It
is precisely the established M1.13 boundary-re-imposition pattern
(D-006/D-008/D-010) applied to the filesystem surface: the heed contract —
including its file name — is imposed at the heed boundary, where it is load-
bearing, and nowhere else. The cost is one plumbed option plus a two-name
probe in the tools, there are zero production envs to migrate, and the one
genuinely dangerous failure mode today (snapshot restore silently yielding an
empty env) becomes structurally impossible because the adapter reads the same
name it writes.

## Consequences

### Implementation sketch (for the implementing milestone)

- `zerodb::EnvOpenOptions::data_file_name(impl Into<OsString>)` (default:
  `DATA_FILE_NAME`); `open` joins it at the existing single join point
  (`lib.rs:267`). Document it as an integration knob; empty names and names
  containing a path separator are rejected `Io(InvalidInput)` (consistent with
  the D-006/D-010 open-time taxonomy).
- `heed-zerodb::EnvOpenOptions::open` (env.rs:188–201) sets `"data.mdb"`.
  `Env::path()` still returns the directory (unchanged, heed parity).
  `copy_to_file`/`copy_to_path` are untouched — they already write a
  single-file image wherever the caller says, which is exactly what the
  snapshot call sites need.
- `zerodb-tools`: dir-taking commands (`stat`/`dump`/`check`/`load` target,
  flock guard) probe `data.mdb` then `zerodb.dat`; **both present is a hard
  error** (no silent pick). `migrate-from-lmdb` distinguishes an LMDB
  `data.mdb` from a zerodb one by magic and says so explicitly on mismatch.
  `stat` prints an engine/format identification line (D2-in-spirit).
- The oracle/crash fixtures and engine tests keep the native name; the
  adapter-suite gains the round-trip tests below.
- **No new `unsafe`, no atomics, no ordering decisions** — pure path plumbing
  resolved once at open, before any write. Crash-safety invariants (SPEC 06)
  are untouched: between any two steps of any write sequence, the invariant
  set is identical to today's because the name never changes after open.

### SPEC updates (same change as the implementation, CLAUDE.md rule 3)

- SPEC 02 §8: the layout bullet ("named `zerodb.dat`") becomes "named by the
  opener; native default `zerodb.dat`, `data.mdb` when opened through
  heed-zerodb (ADR-0010, D-012)".
- SPEC 00 row 7: note that the adapter satisfies the `data.mdb` half of the
  directory-env convention and that `lock.mdb` is intentionally absent
  (D-001, D-012).
- DIVERGENCES.md D-012: flip wording from "latent break" to "resolved at the
  adapter boundary" once implemented.

### Acceptance criteria the implementing milestone inherits

1. **Adapter naming**: a fresh env dir created through `heed-zerodb` contains
   **exactly** `["data.mdb"]` — no `zerodb.dat`, no `lock.mdb`. Asserted by
   directory listing in a test.
2. **Native default unchanged**: `zerodb::EnvOpenOptions::open` without the
   option still creates `zerodb.dat` (regression test), and an existing-name
   mismatch (option says `data.mdb`, dir has only `zerodb.dat`) creates a
   fresh env at the configured name — deterministic, no fallback probing in
   the engine.
3. **The test the current suites miss — snapshot round-trip through the
   adapter**: populate an env, `copy_to_path(dst.join("data.mdb"), option)`
   for **both** `CompactionOption::Enabled` and `Disabled`, reopen `dst` as
   an env through the adapter, and assert full logical equality with the
   source. This is the exact `process_snapshot_creation.rs` shape.
4. **Compaction-persist round-trip through the adapter** (the
   `process_batch.rs`/`meilitool` shape): `fs::metadata(dir.join("data.mdb"))`
   succeeds on a live adapter env; `copy_to_file` into `data.mdb.cpy`; rename
   over `data.mdb`; close the env (`prepare_for_closing` + `wait`); reopen
   through the adapter; assert contents equal (and, for `Enabled` after
   deletions, that the file shrank).
5. **Tools**: probe-both-names behavior tested for `stat`/`dump`/`check` and
   the flock guard; both-names-present is a hard error;
   `migrate-from-lmdb` gives the explicit magic-based error on a zerodb-format
   `data.mdb`.
6. **Oracle**: the env-lifecycle differential's garbage-file case
   (`env_lifecycle_differential.rs`) extended to the adapter's `data.mdb`
   name; the through-adapter oracle re-run stays at zero divergences.
7. SPEC 02 §8 / SPEC 00 row 7 amended in the same change; D-012 updated;
   this file `git mv`'d to `0010-env-file-naming.md`.
8. Standard gate: `cargo fmt`, `clippy -D warnings`, `cargo test --workspace`,
   `cargo miri test -p zerodb-core`, `just fuzz-quick`,
   `just crash-test-quick`.

## Open questions — resolved at implementation (2026-07-20)

1. **Knob visibility**: should `data_file_name` be `pub` on
   `zerodb::EnvOpenOptions` (useful to embedders, slightly widens the frozen
   surface) or `pub(crate)`-with-a-doc-hidden hook reserved for the adapter?
   Recommendation: `pub` but documented as an integration knob — the native
   API is zerodb's own, not frozen by SPEC 00.
2. **Legacy-name convenience**: should the *adapter* (not the engine) offer an
   opt-in one-time `zerodb.dat → data.mdb` auto-rename for pre-change dev
   envs? Recommendation: no — there are no production envs; `mv` is enough.
3. **Numbering**: confirm the `git mv` to `0010-env-file-naming.md` (see
   header note).

**Resolutions (implementation, 2026-07-20):**

1. **Knob visibility** — implemented as **`pub`**, per the recommendation:
   `zerodb::EnvOpenOptions::data_file_name(impl Into<OsString>)` plus
   `get_data_file_name()`, documented as an integration knob. The native API is
   ZeroDB's own and is not frozen by SPEC 00. Validation is at `open` (empty, or
   any embedded path separator → `Io(InvalidInput)`), keeping the builder
   chainable and matching the D-006/D-010 open-time taxonomy. Note the check is
   on the **raw** name, not the normalized one: `"a/"` would normalize to the
   single component `a`, but an integration knob whose on-disk result differs
   from the value passed is a trap, so it is rejected.
2. **Legacy-name convenience** — **no** auto-rename, per the recommendation.
   There are no production envs; `mv zerodb.dat data.mdb` covers the dev case.
   Adding a rename would also make the adapter's open path non-deterministic,
   which is the property criterion 2 exists to protect.
3. **Numbering** — done; this file is `0010-env-file-naming.md`.
