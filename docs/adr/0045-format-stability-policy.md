# ADR-0045: On-disk format stability: when `format_version` may change, and what a user is promised

- Status: Draft, awaiting maintainer approval
- Related issue: #182 (release `main`, a format-stability policy, a one-command
  upgrade); ADR-0013 (release and versioning); ADR-0002 §D8 (the version field)
- Date: 2026-11-20

## Context

### Where the format stands

`format_version` (SPEC 02 §0, meta offset 36) names the on-disk layout. The
engine opens only files of its own version and refuses every other one at
open (SPEC 02 §3.2 rule 2). Three versions exist:

| Version | Introduced by | Released in | What changed |
|---|---|---|---|
| 1 | first engine | never (development builds before 2026-10-05) | — |
| 2 | ADR-0022 | 0.2.0 (2026-10-05) | meta free-list annex: `fl_count` at 168, `meta_crc` at 172 with split coverage, annex ids at 176 |
| 3 | ADR-0037 | `main`, unreleased | free-database keys become `BE(txnid) ‖ BE(seq)`, every free-database value is inline and bounded |

Between versions 2 and 3, only the free database's records changed. The meta
page, the page header, leaf, branch and overflow pages, and the named-database
catalog are the same (SPEC 02 §2–§7). A user's data lives entirely in
structures that did not change.

ADR-0013 rule 2 makes a `format_version` change a minor release while 0.x,
and rule 5 promises that a file opens unchanged in every later release *with
the same* `FORMAT_VERSION`. Nothing says how often the version may change, how
long an old version stays readable, or what a user does when it changes.
Until this change, upgrading meant installing two versions of
`zerodb-tools`: `dump` from the release that wrote the file, `load` from the
new one.

### Why a policy is needed now

Issue #182 is about cutting the next release from `main`. That release carries
version 3, so every 0.2.0 user meets a format change. An engine nobody can
adopt without knowing whether the next release opens their files needs a
written rule before the release, not after.

### What LMDB does

LMDB records a data-format version in its meta page (`MDB_DATA_VERSION`), and
its users expect a file to keep opening across the releases of one LMDB line.
That expectation is what ZeroDB is measured against. ZeroDB is not
LMDB-file-compatible (D-002), so it inherits the expectation, not the format.
Which error LMDB returns for a file of another data version is not recorded
here; question 3 asks for it to be observed.

### Format changes that are pending

A promise of stability constrains these. Each is listed with whether it needs
a `format_version` change:

| Change | State | Needs a version change? |
|---|---|---|
| ADR-0040: refuse unknown persisted flag bits at open | Accepted, not implemented (branch `qdequele/silly-torvalds-ba6e41`) | No. Every file written so far has those bits zero, so the check refuses nothing that exists. It is what lets later features be added without a version change (below). |
| ADR-0020: shrink the 32-byte page header to LMDB's 16 | Spike approved; the format change waits for numbers that were never recorded | Yes, and it changes every page. |
| ADR-0038: steady and weak metas, a durable watermark | Proposed | Yes: new meta fields and a new recovery rule. |
| D-014: a comparator fingerprint in the database record | Open question in `docs/LMDB-COMPATIBILITY.md` | Yes: the 48-byte record has no free field, so the fingerprint needs a wider record (or reusing `leaf2_ksize`, which DUPFIXED would need back). |
| Issue #51: a fixed-record database for hannoy vectors | Needs an ADR | Not necessarily: an opt-in database type can be marked by a database-record flag bit, which ADR-0040 makes every older engine refuse cleanly. |

## Options

### Option A — No promise until 1.0

Keep ADR-0013 as it is: a version may change in any minor release; the
CHANGELOG says how to migrate.

- Pros: no constraint on ADR-0020, ADR-0038 or D-014.
- Cons: the signal issue #182 calls out stays: three versions in two releases,
  and no statement a user can plan around. Every adopter must assume each minor
  release may need a migration.

### Option B — A bounded promise, a supported upgrade, and a freeze point (recommended)

Rules, from the next release on:

1. **When the version may change.** Only in a minor release while 0.x, and only
   in a major release from 1.0 on. Never in a patch release. At most one change
   per release. Each change is an ADR approved before the implementation
   (CONTRIBUTING.md rule 5), as today.
2. **What a user is promised when it changes.**
   1. The CHANGELOG entry's first line says so, with the old and new versions
      and the upgrade command; the README's release notes repeat it.
   2. The new engine refuses a file of another version at open with a typed
      error, never a crash and never a write to the file. It never converts a
      file in place.
   3. The same release's `zerodb-tools` upgrades the file in one command:
      `zerodb-tools upgrade <old-env> <new-env>`, and its `dump` reads the old
      version too. No second tool version is needed.
   4. The **read window**: the tools of every release read every format
      version written by any release since the policy took effect. At the time
      of writing that is version 2 (0.2.0) and version 3 (this release).
      Version 1 was never released and is not read.
   5. Downgrading is a `dump` with the new tools and a `load` with the old
      ones: the dump is `mdb_dump`'s text format, which does not depend on the
      on-disk format.
3. **Additive features do not change the version.** Once ADR-0040 is
   implemented, a feature that only some files use (a new database type, a
   persisted database flag) is marked by a flag bit set only in files that use
   it. Files that do not use it keep the same version and still open in older
   releases; files that do are refused by older releases with a typed error.
   `format_version` changes only when an existing structure changes layout.
4. **The freeze point.** The project states "no `format_version` change is
   planned" only once ADR-0040 is implemented and ADR-0020, ADR-0038 and D-014
   each have a decision: either landed, or rejected for the 1.x line. Until
   then, rules 1–3 hold, and the release notes say which pending changes may
   still change the version.

- Pros: a user knows when a migration can happen (a minor release, announced),
  what it costs (one command, one tool), and how far back the tools reach. The
  freeze point forces the expensive decisions (the page header, durability
  metas) before 1.0 instead of after.
- Cons: every format change from now on also ships a reader of the previous
  layout in `zerodb-tools`. For a change like ADR-0037 that is small (the
  version 2 reader and the `upgrade` command are about 400 lines with their
  documentation, because only the free database changed); for ADR-0020, which changes every page, the reader must decode the
  old page header. The read window grows by one reader per format change.

### Option C — The engine opens older versions

The engine reads (or converts in place) older versions at open.

- Pros: no upgrade step for users.
- Cons: every read path and validator carries every layout; an in-place
  conversion is a crash-safety design of its own (a torn conversion must not
  lose the file), with no LMDB precedent. It also slows the hot path or
  duplicates it. Rejected.

### Option D — Freeze the format now

Promise that version 3 is final.

- Cons: blocks ADR-0020's measured gain (+6.7 % file size, 0.86× LMDB
  throughput on NVMe at the "medium" workload) and ADR-0038's durability mode,
  or forces them into a 2.0. Premature while those decisions are open.

## Decision

Proposed: **Option B**. The maintainer decides, and also answers the open
questions below.

## Consequences

- **Implemented with this ADR, as tooling** (it changes no format and no
  engine behavior): `zerodb-tools upgrade <old-env-dir> <new-env-dir>` and a
  `dump` that reads version 2 (`crates/zerodb-tools/src/legacy.rs`,
  `docs/TOOLS.md`). The reader validates each meta slot with SPEC 02 §3.2's
  predicate for version 2 (`MetaPage::validate_version`; the meta layout is
  the same in versions 2 and 3), walks the main and named databases with the
  engine's page decoders under the same checks as `FileTrust::VALIDATE`, and
  never reads the free database, the only structure version 3 changed. It is
  tested against a version 2 file written by the 0.2.0 engine and the dump
  0.2.0's own tools made of it
  (`crates/zerodb-tools/tests/upgrade_format_v2.rs`). If this ADR is rejected,
  the reader still serves 0.2.0 users and costs nothing at run time.
- A version 2 environment has no lock file (0.2.0 is single-process), so the
  reader cannot take a reader slot. It reads the metas again after the walk
  and fails if a transaction committed meanwhile; the docs say to close every
  process first.
- ADR-0013 rule 5 gains the read-window and one-command promises; when this
  ADR is approved, ADR-0013's text and `docs/maintainers/RELEASING.md` step 6
  ("dump on the old binary, load on the new one") are updated to point here.
- Each future format-change ADR gains a required section: "the reader of the
  previous version in `zerodb-tools`", sized and tested against a fixture
  written by the previous release.
- Release notes list the pending changes of the freeze-point table until the
  freeze point is reached.

## Open questions for human review

1. Option B, or another?
2. The read window: every version since the policy (as proposed), or only the
   immediately previous one (N−1)? N−1 bounds the reader count; the full window
   lets a user skip releases.
3. The error at open for another version: today `MdbError::Invalid`. LMDB has
   a dedicated `MDB_VERSION_MISMATCH`; which code LMDB returns for a data file
   of another data version must be observed with a differential test in
   `crates/zerodb-oracle`, not assumed. If it is `MDB_VERSION_MISMATCH`, should
   ZeroDB return the same (a behavior change heed users can see)?
4. Should the next release wait for ADR-0040's implementation, so that it is
   the first release whose files later releases can extend without a version
   change? Recommended: yes; the change refuses nothing that exists today.
5. ADR-0020 and ADR-0038: decide before the freeze point, as rule 4 requires?
   ADR-0020's abandon gate (a real-milli-dump measurement) has no recorded
   result.
6. D-014: is a comparator fingerprint wanted at all? If yes, it should share
   one version change with ADR-0020 rather than cost one of its own.
