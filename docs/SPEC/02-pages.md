# SPEC 02 — On-disk page formats (Phase 0.4 deliverable)

Status: TO BE WRITTEN in milestone 0.4. Own format (not LMDB-compatible).

Must define, with byte-level layouts and worked examples:
- File header / meta pages (x2): magic, format version, page size, geometry,
  root pids, txnID, GC root, double-buffer protocol, and a **mandatory
  CRC32C** — torn-meta detection is a Phase 1 requirement (a half-written
  meta from a power cut must fail validation at open so the older meta wins);
  a signature field alone is not sufficient.
- Common page header: pid, type flags, writer txnID (LMDB-1.0-style stamp,
  required for Phase 3.10 page shipping), entry count, bounds,
  reserved checksum field (data-page checksums are Phase 3.9; only the meta
  CRC is mandatory in Phase 1).
- Branch, leaf, overflow, GC page layouts; node encodings; max key size rule.
- DUPSORT sub-page and sub-tree encodings (align with SPEC 03) — format
  reserved in Phase 1, implemented in Phase 2.8 (D-004: no consumer uses it).
- Endianness (little only), alignment guarantees, page sizes 4K–64K.
