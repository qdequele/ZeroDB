---
description: Run one PLAN.md milestone end-to-end with the right agent
argument-hint: <milestone id, e.g. 1.3>
---

Execute milestone $ARGUMENTS from PLAN.md.

1. Read PLAN.md, PROGRESS.md, and the SPEC files relevant to milestone $ARGUMENTS.
2. Restate the milestone's deliverables and acceptance criteria.
3. Confirm prerequisites: previous milestones marked done in PROGRESS.md and
   `cargo test --workspace` green. If not, STOP and report.
4. If the milestone is 0.5: it is ADR-only — draft the heed-integration ADR
   (see /adr) and STOP for human approval; implement nothing.
   If the milestone is 1.4, 1.5, 1.8, 1.9, 1.11, or any 3.x touching GC/commit/
   readers: delegate implementation to the critical-implementer agent (ADR
   first). Otherwise delegate to the implementer agent.
5. Delegate test coverage to the test-writer agent.
6. Run the full check suite from CLAUDE.md; paste results.
7. For critical milestones, run the spec-reviewer agent on the diff and include
   its report.
8. If everything is green, append the completion line to PROGRESS.md and
   summarize what a human must review (unsafe blocks, orderings, ADRs).
