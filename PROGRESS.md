# Progress log

(one line per completed milestone; agents read this at session start)

M0.1 done 2026-07-15 — notes: SPEC 00 written; 5 consumers pinned (meilisearch fff2ef5a, heed v0.22.1, arroy v0.6.4, hannoy v0.1.3, cellulite v0.3.2); 61 MUST / 12 SHOULD / 9 WON'T. Human decisions pending before 0.2+: oracle must target the Meilisearch LMDB fork (mdb.master.nested-rtxns) not stock LMDB; M1.9 rescope (nested READ txns in a wtxn = hot-path MUST, nested write = unused); M1.7 DUPSORT unused by all consumers (descope candidate); PLAN gaps in SPEC 00 Findings §B (PREV_SNAPSHOT, WRITE_MAP, try_clone_inner_file, static_read_txn, cursor-mutation ops, get_greater_than/lower_than_or_equal_to). PLAN.md deliberately NOT edited.

M0.1 decisions approved 2026-07-15 (Quentin, chat) and applied: oracle = Meilisearch LMDB fork (lmdb-master-sys 0.2.6); M1.9 rescoped to nested READ txns over a wtxn (critical path, ADR-first; nested writes = D-003); M1.7 DUPSORT descoped to Phase 2.8 (D-004, contingent on 0.5 ADR test-suite scope); SPEC 00 Findings §B folded into milestones 1.2/1.3/1.4/1.8/1.10. PLAN.md, CLAUDE.md, DIVERGENCES.md, SPEC 02/03/04, agents, justfile updated in the same change.
