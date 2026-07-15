# SPEC 03 — B+tree algorithms (Phase 0.4 deliverable)

Status: TO BE WRITTEN. Search, cursor state machine (all LMDB cursor ops and
their exact positioning/error semantics, incl. `get_greater_than` and
`get_lower_than_or_equal_to`), insert with split, delete with rebalance/merge,
COW rules (first-touch copy, parent chain), overflow chains, DUPSORT trees
(Phase 2.8 — spec the format reservation only in Phase 1, see D-004).
Include the tree invariants the `check` tool enforces.
