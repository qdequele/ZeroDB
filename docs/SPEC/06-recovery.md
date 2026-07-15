# SPEC 06 — Durability & recovery (Phase 0.4 deliverable)

Status: TO BE WRITTEN. Open-time meta selection (mandatory meta CRC, see SPEC
02), torn-write handling, what is guaranteed after crash at each pipeline
stage, NOSYNC/NOMETASYNC semantics mapping, and the crash-injection test
protocol.

The test protocol has two mechanisms (SIGKILL alone cannot tear a write — the
OS page cache survives process death; only power loss tears/reorders sectors):
1. Process kill + fsync-barrier hooks (harness kills between pipeline steps).
2. A fault-injection write backend in `zerodb-io` that tears, reorders, and
   drops writes not yet covered by an fsync (CrashMonkey/ALICE-style),
   producing disk images to reopen-and-verify.
