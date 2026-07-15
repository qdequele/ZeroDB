# Sanctioned behavior divergences vs LMDB

Rules:
- Every entry needs a maintainer sign-off line. Agents may add entries only
  with status PROPOSED; a human flips them to APPROVED.
- Phase 1 goal is zero APPROVED functional divergences visible through heed,
  except the ones below.

| ID | Area | LMDB behavior | zerodb behavior | Status | Approved by |
|----|------|---------------|-----------------|--------|-------------|
| D-001 | Process model | Cross-process readers via lock file | Single-process only; no lock file | APPROVED | (founder decision, see PLAN.md rule 7) |
| D-002 | File format | LMDB 0.9/1.0 on-disk formats | Own format; migration via logical dump/load | APPROVED | (founder decision) |
| D-003 | Nested write txns | Child write txns with page shadowing | Unsupported — clean error. Zero call sites across all five consumers (SPEC 00 §A). Nested READ txns (fork semantics) ARE supported (M1.9). | APPROVED | Quentin, 2026-07-15 (chat: "go with your recommendations") |
| D-004 | DUPSORT/DUPFIXED, DatabaseFlags | Duplicate keys, integer keys, custom comparators | Unsupported in Phase 1 — no consumer uses any DatabaseFlags (SPEC 00 §B.1). Implemented in Phase 2.8. | APPROVED | Quentin, 2026-07-15 (chat: "go with your recommendations") |
| D-005 | Writer quiescence during nested read txns | The fork does not enforce it: a write txn may technically mutate while nested read children are live (undefined interplay; Meilisearch always pauses the writer by convention). | ZeroDB **enforces** writer quiescence for Rust soundness — while any nested read txn is live the parent write txn must not mutate: compile-time via the shared `&wtxn` borrow (SPEC 04 TXN-30) plus a runtime `child_count == 0` assert on every `&mut` op (SPEC 04 TXN-29). **Not observable to any consumer** — all six nested-read call sites (milli 5, hannoy 1) pause the writer already, so no functional divergence is visible through heed. | PROPOSED | (agent-proposed 2026-07-15; awaiting human. See SPEC 04 §5.2/§10.) |
