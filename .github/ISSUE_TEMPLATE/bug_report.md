---
name: Bug report
about: Something ZeroDB does wrong (a crash, a wrong result, a corrupt file)
labels: bug
---

**What happened**

<!-- What you did, what you expected, and what happened instead. -->

**Reproduction**

<!-- The smallest code or command sequence that shows it. For a suspected
difference from LMDB, an operation sequence the oracle in
crates/zerodb-oracle can replay is ideal. -->

**Environment**

- ZeroDB version or commit:
- Used through: native `zerodb` API / heed drop-in (which consumer and version?)
- Platform and kernel (for example Linux aarch64, 6.8):
- Page size, and environment flags (`WRITE_MAP`, `NO_SYNC`, `NO_LOCK`, …):

<!-- Security issues: please do not open a public issue; see SECURITY.md. -->
