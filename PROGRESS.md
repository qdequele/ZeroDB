# Progress log

(one line per completed milestone; agents read this at session start)

M0.1 done 2026-07-15 — notes: SPEC 00 written; 5 consumers pinned (meilisearch fff2ef5a, heed v0.22.1, arroy v0.6.4, hannoy v0.1.3, cellulite v0.3.2); 61 MUST / 12 SHOULD / 9 WON'T. Human decisions pending before 0.2+: oracle must target the Meilisearch LMDB fork (mdb.master.nested-rtxns) not stock LMDB; M1.9 rescope (nested READ txns in a wtxn = hot-path MUST, nested write = unused); M1.7 DUPSORT unused by all consumers (descope candidate); PLAN gaps in SPEC 00 Findings §B (PREV_SNAPSHOT, WRITE_MAP, try_clone_inner_file, static_read_txn, cursor-mutation ops, get_greater_than/lower_than_or_equal_to). PLAN.md deliberately NOT edited.
