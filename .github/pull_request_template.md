## What and why

<!-- What this change does and the problem it solves. Link the issue. -->

## Checks

- [ ] `cargo fmt`, `cargo clippy` (also for `x86_64-unknown-linux-gnu` on macOS) and `cargo test --workspace` pass
- [ ] `just fuzz-quick`, and `just crash-test-quick` for write, commit or recovery changes
- [ ] `cargo +nightly miri test -p zerodb-core` for core changes, `just loom` for reader-table or nested-txn changes
- [ ] No test deleted, weakened or ignored, and no oracle expectation changed
- [ ] SPEC updated if behavior changed; ADR linked for format, durability, concurrency or public API changes
- [ ] New behavior differences from LMDB are listed in `docs/LMDB-COMPATIBILITY.md`
- [ ] CHANGELOG entry for user-visible changes

## Performance

<!-- For a performance claim: LMDB / ZeroDB before / ZeroDB after, from
`just bench-ab`, with the machine named. Otherwise delete this section. -->
