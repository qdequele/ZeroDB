# Format 2 fixture

An environment in on-disk format 2, written by the 0.2.0 release, for the
`dump` and `upgrade` tests in `tests/upgrade_format_v2.rs`.

- `env/zerodb.dat`: the data file, written by `gen_v2_fixture.rs.txt`.
- `dump-by-0.2.0.txt`: `zerodb-tools dump` of it by the 0.2.0 tools.

To regenerate both, from a checkout of the `v0.2.0` tag:

```sh
cp gen_v2_fixture.rs.txt crates/zerodb/examples/gen_v2_fixture.rs
cargo run --release -p zerodb --example gen_v2_fixture -- /tmp/v2
cargo run --release -p zerodb-tools -- dump /tmp/v2 --out dump-by-0.2.0.txt
```

The file is mostly pages freed while a read transaction was open, which is
what makes the free database hold records; it compresses to about 70 KB.
