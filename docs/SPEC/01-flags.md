# SPEC 01 — LMDB flag & semantics matrix (Phase 0.2 deliverable)

Status: TO BE WRITTEN in milestone 0.2.

Enumerate all LMDB env/db/write/cursor flags from LMDB 1.0 headers. For each:
MUST (consumer-used) / SHOULD (heed-exposed) / WON'T (justify). Record exact
error codes and precedence rules (e.g., APPEND with out-of-order key,
NO_OVERWRITE on dup db). Every MUST/SHOULD row must eventually link to a
differential test.
