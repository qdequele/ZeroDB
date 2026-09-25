---
description: One ZeroDB-vs-LMDB performance iteration — pick a rung, profile, change one lever, A/B it, keep or revert, log it
argument-hint: [rung or regex to focus on, e.g. del/range/half — empty = pick from the scoreboard]
---

One iteration = **one lever**, measured, then kept or reverted, then written to
the ledger. Drive it repeatedly with `/loop /perf-iterate [focus]`. Focus:
`$ARGUMENTS` (empty = choose from the scoreboard).

The tools: `just bench-ab`, `just bench-profile`, `just perf-ledger`,
`just bench-report --json`. The reading guide is `docs/BENCH-MAP.md`, and the
known mechanisms are in `docs/PERF-GAP-VS-LMDB.md`. The ratio is always
ZeroDB ÷ LMDB, so above 1 means ZeroDB is slower.

## Where things run

Edit on this machine. **Measure, profile and gate on the bench server.**
The helpers are in a separate `benches-server` scripts checkout, outside this repo
(`$BS` below). Each one syncs the working tree first, uncommitted changes
included, so they always see the candidate.

```
export ZERODB_SRC=$PWD BS=<path to the benches-server scripts> BENCH_HOST=<user@bench-host>
$BS/remote.sh -- '<command>'            # gate / profile; exit code is the command's
TARGET='^<rung>$' $BS/ab.sh '^<family>/' # bench-ab in bench mode; last line = local verdict.json
```

The local `just bench-ab` is the fallback when the server is unreachable. It
is only valid when the load precondition below holds.

## 0. Preconditions: STOP and report if any fails

- The current branch is not `main`. Perf work lives on its own branch.
- `git status --porcelain` is empty. `bench-ab` compares `HEAD` with the working
  tree, so the working tree must hold **only** this iteration's change.
- The bench server answers (`$BS/remote.sh --no-sync -- true`). If it does
  not, the local machine must be quiet instead: a load average below half the
  CPU count. Otherwise LMDB drifts and every verdict comes back `invalid`.
- `just perf-ledger show --last 20` has been read. Never re-try a lever the ledger
  shows as `reverted` unless you can say what is different this time. Put that
  reason in the new hypothesis.

## 1. Pick the target rung

If `$ARGUMENTS` names a rung or regex, use it. Otherwise:

1. Get the scoreboard: the newest bench-server ladder
   (`benches/results/*bench-server*.md`, or `ladder.json` from
   `$BS/run.sh ladder`). Use the macOS `*engine-ladder*.md` only when there is
   no server run. The two disagree, and the server's 4 KiB pages are the
   common Linux case.
2. Drop the rungs that are:
   - marked `noise`;
   - at or below 1.10×;
   - explained **by design** in PERF-GAP section D or B9 (e.g. `env/open/create`);
   - under `commit/sync/*` or `maint/*`. Those are barrier-bound, and only
     Graviton + EBS can referee them.
3. Rank what is left by ratio × Meilisearch relevance. The hot path for
   indexing and search is `del/*`, `put/*`, `commit/batch/*`, `get/*`, `scan/*`,
   `seek/*` and `mixed/*`. Once-per-lifetime costs rank last.
4. Skip a rung with two or more `reverted` ledger entries and no new idea.
   If nothing is left, **STOP: the loop is done.** Report the remaining gaps.

## 2. Profile before hypothesising

```
$BS/remote.sh -- 'scripts/bench-profile.sh <rung> && scripts/bench-profile.sh <rung> lmdb'
ssh "$BENCH_HOST" 'head -80 /srv/zerodb-bench/zerodb/target/bench-profile/<rung_with_underscores>-{zerodb,lmdb}/report.txt'
```

On Linux the profiler is `perf`. For system-call-bound rungs also run
`perf trace -s -- <bench exe> --bench --profile-time 3 '^<rung>/<engine>$'`
on both engines: counting calls is how the `futex`-per-release cost was found.

Read the heaviest frames of both profiles. Then read the PERF-GAP item that
BENCH-MAP links to this rung, and the SPEC file for the code area (CLAUDE.md
rule 3).

## 3. Write the hypothesis down, then check the stop zones

Write one sentence: *"<mechanism> costs <x> on <rung> because <cause>;
<change> removes it; expected: <rung> ≥ N % faster, no other rung moves."*

**STOP zones.** If the lever touches any of these, draft the ADR with `/adr`,
record `just perf-ledger add --outcome blocked-adr ...`, and **end the
iteration without implementing**. A human approves these:

- commit or fsync ordering, durability, crash recovery;
- GC or page reclamation, the reader table, nested read txns;
- the on-disk format;
- public API shape;
- a new dependency;
- `unsafe` outside the modules CLAUDE.md sanctions.

## 4. Implement the smallest change that tests the hypothesis

Delegate to the `implementer` agent. Pass it the hypothesis, the profile
findings, the files and the SPEC section. One mechanism only; no drive-by
cleanups. Every behaviour stays the same, because this is a Phase 1 parity
engine.

## 5. Correctness gate, before any timing

A fast wrong engine is worth nothing. Run:

```
$BS/remote.sh -- 'cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && just fuzz-quick'
```

Append `&& cargo +nightly miri test -p zerodb-core` if `zerodb-core` logic
changed. Append `&& just stress`, and `&& just loom` if the readers or nested
txns are involved, for anything concurrent (locks, atomics, the writer slot).
**Never edit, weaken or `#[ignore]` a test to get green** (CLAUDE.md rule 2).
If the gate cannot pass without touching a test, revert the change, log it as
`abandoned` with the failing test named, and end the iteration.

## 6. Measure it

```
TARGET='^<rung>$' $BS/ab.sh '^<family>/'
```

The filter is the whole family, so the neighbouring rungs, and the family base
rung, show whether the gain is real or just moved. Three interleaved rounds by
default. Read the `verdict.json` path it prints last → `verdict`:

| verdict | action |
|---|---|
| `invalid` | Re-run once. If still `invalid`, STOP and report that the machine is too noisy (LMDB drifted). |
| `regressed` | Revert. |
| `flat` | Revert. A change that does not pay for itself is just complexity. |
| `improved` | Continue to step 7. |

## 7. Breadth check, for `improved` only

Run `ROUNDS=1 $BS/ab.sh '<every suite the changed code path serves>'`. For
example, a btree change gets `'^(get|put|del|scan|seek|mixed)/'`. For any rung
this reports as `regressed`, confirm with `ROUNDS=3` on that rung alone. A
confirmed regression anywhere means revert. Say so in the ledger entry.

## 8. Keep or revert, and always log it

**Keep:**

1. Commit the change. Put the three-column table from `bench-ab` in the commit
   body: LMDB | ZeroDB before | ZeroDB after. That table is the
   performance-claim evidence.
2. Update the PERF-GAP item (and BENCH-MAP, if the rung now reads differently).
   Label the numbers with their host, e.g. `bench server, x86-64, 4 KiB`.
3. Record it:
   ```
   just perf-ledger add --rung <rung> --hypothesis "<sentence>" --outcome kept \
       --verdict <run>/verdict.json --commit <sha>
   ```
   Then commit the ledger line.

**Revert:** run `git restore --staged --worktree -- .`, and `git clean -fd`
for any files the attempt created. Both are safe because step 0 guaranteed a
clean tree. Then run the same `perf-ledger add` with
`--outcome reverted` and a `--notes` line saying *why* it did not pay: the
profile was wrong, the effect was below the noise, the cost moved to <rung>,
and so on. Commit the ledger line. **A disproved idea is a result.**

## 9. Report

Report:

- the target rung and the hypothesis;
- the verdict and the three-column table;
- the ledger line;
- the suggested next target.

Do not start a second lever in the same iteration. `/loop` calls this command
again.

## Stop conditions for `/loop`

End the loop (`ScheduleWakeup` with `stop: true`) when any of these happens:

- step 1 finds nothing left;
- two consecutive `invalid` runs;
- a `blocked-adr`;
- a correctness gate that cannot pass without touching tests;
- three consecutive `reverted` iterations. The scoreboard, the profiles or the
  hypotheses are wrong, and a human should look.

Every claim this loop makes is macOS-indicative. A batch of kept changes goes
to `just bench-gate` (the whole ladder against the merge base with `main`),
then to `just consumer-bench`, then to Graviton + EBS before a release note
states any number.
