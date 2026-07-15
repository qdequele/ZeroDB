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
