//! Milestone 1.2 differential env-lifecycle tests: `LmdbEngine` vs the native
//! `ZerodbEngine`.
//!
//! Only the environment-lifecycle op ([`Op::Reopen`]) is implemented by
//! `ZerodbEngine` at M1.2; every other op is gated out symmetrically by the
//! driver (`Engine::implements`), so these sequences exercise **create / open /
//! reopen / reopen-with-larger-map-size** at parity while the data ops are
//! skipped on both sides. As later milestones fill ops in, the same sequences
//! start exercising more.
//!
//! ## Map-size units (why `map_size_kib` values are multiples of 16 here)
//!
//! heed rejects any `map_size` that is not a multiple of the **OS** page size
//! (`Io(InvalidInput)` — observed: 16384 on Apple Silicon), and SPEC 00 row 3
//! records that every consumer clamps `map_size` to the page size before the
//! call (`clamp_to_page_size`). zerodb is more lenient — it stores `map_size`
//! and maps the file length, so it accepts un-clamped values (logged in
//! `docs/DIVERGENCES.md` D-006, PROPOSED). To keep these differential tests on
//! the *shared* semantic rather than heed's input-validation quirk, every
//! `Reopen` here uses `map_size_kib` that is a multiple of 16, so the resulting
//! `map_size` (`1 MiB + kib*4096`) is a multiple of 64 KiB — hence a multiple of
//! the OS page size on 4 KiB, 16 KiB, and 64 KiB kernels alike.
//!
//! Separately, `garbage_store_error_kind_parity` is a genuine cross-engine
//! parity check on the *error kind* for a foreign/garbage store file: it feeds
//! each engine its own on-disk garbage (the formats differ — SPEC 02 D-002 — so
//! the *bytes* cannot be shared) and asserts both normalize to
//! [`OracleError::Invalid`]. The corrupted-meta / PREV_SNAPSHOT recovery tests
//! that depend on our specific format are zerodb-only self-tests in
//! `crates/zerodb/tests/env_lifecycle.rs`.

use zerodb_oracle::tempdir::TempDir;
use zerodb_oracle::{decode_ops, run, DbName, LmdbEngine, Op, OracleError, PutFlag, ZerodbEngine};

fn k(bytes: &[u8]) -> zerodb_oracle::Key {
    zerodb_oracle::Key(bytes.to_vec())
}
fn v(bytes: &[u8]) -> zerodb_oracle::Value {
    zerodb_oracle::Value(bytes.to_vec())
}

/// create → open → reopen cycles at parity. Data ops are interspersed (gated out
/// on both engines) to prove the symmetric-skip seam holds around real work.
#[test]
fn differential_create_open_reopen_cycles() {
    let ops = vec![
        Op::Reopen { map_size_kib: 0 },
        // These are gated (not yet implemented by zerodb) → skipped on both.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k(b"k"),
            val: v(b"v"),
        },
        Op::Commit,
        // Reopen again after the gated work.
        Op::Reopen { map_size_kib: 32 },
        Op::Reopen { map_size_kib: 16 }, // a smaller request: monotonic, still Ok
    ];
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("env-lifecycle differential diverged:\n{d}");
    }
}

/// Reopen with a strictly larger map size, repeatedly (SPEC 02 §8 growth =
/// reopen-larger). Both engines must accept each reopen.
#[test]
fn differential_reopen_with_larger_map_size() {
    let ops: Vec<Op> = [0u16, 64, 128, 256, 512, 1024]
        .into_iter()
        .map(|kib| Op::Reopen { map_size_kib: kib })
        .collect();
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("reopen-larger differential diverged:\n{d}");
    }
}

/// A mixed sequence of every op kind: all non-env ops are gated out, so the
/// differential reduces to the reopen parity, proving the gate does not leak a
/// spurious divergence on a realistic script.
#[test]
fn differential_mixed_sequence_only_env_survives() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        },
        Op::BeginNestedRo,
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        Op::Reopen { map_size_kib: 16 },
        Op::First { db: 0 },
        Op::Commit,
        Op::VerifyGet {
            db: 0,
            key: k(b"a"),
        },
        Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
            flag: PutFlag::Append,
        },
        Op::Reopen { map_size_kib: 48 },
    ];
    assert!(run::<LmdbEngine, ZerodbEngine>(&ops).is_ok());
}

/// Randomized differential: arbitrary op sequences (the same decode path the
/// fuzz target uses) must never diverge between LMDB and zerodb at M1.2 — the
/// gate restricts to `Reopen`, and reopen parity must hold for every sequence.
///
/// `Reopen` map sizes are normalized to multiples of 16 KiB-of-`map_size_kib`
/// (→ 64 KiB `map_size` steps) so every request is a multiple of the OS page
/// size on 4 KiB/16 KiB/64 KiB kernels; see the module doc (heed rejects
/// non-page-multiple map sizes; zerodb is more lenient — DIVERGENCES D-006).
#[test]
fn differential_random_sequences_do_not_diverge() {
    // A handful of deterministic seeds (no proptest harness needed; keeps this
    // fast and dependency-light). Each seed decodes to a bounded op vec.
    for seed in 0u64..64 {
        let mut data = Vec::new();
        // Cheap PRNG expansion of the seed into bytes.
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        for _ in 0..96 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            data.push((x & 0xff) as u8);
        }
        let mut ops = decode_ops(&data, 40);
        for op in &mut ops {
            if let Op::Reopen { map_size_kib } = op {
                // Normalize to a multiple of 16 (→ 64 KiB map_size step), bounded
                // to 0..=1008, so every map_size is OS-page-safe and modest.
                *map_size_kib = (*map_size_kib % 64) * 16;
            }
        }
        if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
            panic!("random differential diverged (seed {seed}):\n{d}");
        }
    }
}

// ---------------------------------------------------------------------------
// Error-kind parity for a foreign/garbage store file.
// ---------------------------------------------------------------------------

/// Normalize a heed open error the way `LmdbEngine` does (kept in sync with
/// `crate::lmdb::to_oracle`; both collapse a foreign file to `Invalid`).
fn normalize_heed(e: heed::Error) -> OracleError {
    match e {
        heed::Error::Mdb(heed::MdbError::Invalid) => OracleError::Invalid,
        heed::Error::Mdb(heed::MdbError::MapFull) => OracleError::MapFull,
        heed::Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        other => OracleError::Other(format!("{other:?}")),
    }
}

/// Normalize a zerodb open error the same way (mirrors
/// `zerodb_engine::to_oracle`).
fn normalize_zerodb(e: zerodb::Error) -> OracleError {
    match e {
        zerodb::Error::Mdb(zerodb::MdbError::Invalid) => OracleError::Invalid,
        zerodb::Error::Mdb(zerodb::MdbError::MapFull) => OracleError::MapFull,
        zerodb::Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        other => OracleError::Other(format!("{other:?}")),
    }
}

/// Feeding each engine a garbage store file (its own layout — the formats differ,
/// so the bytes can't be shared) yields the same normalized error **kind**:
/// `Invalid`. This is the differential form of the "wrong page size / foreign
/// file → error parity" acceptance point.
#[test]
fn garbage_store_error_kind_parity() {
    // LMDB side: garbage `data.mdb` in a directory env.
    let lmdb_kind = {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("data.mdb"), vec![0xFFu8; 8192]).unwrap();
        let mut opts = heed::EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(1 << 20);
        opts.max_dbs(4);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        match unsafe { opts.open(dir.path()) } {
            Ok(_) => panic!("LMDB unexpectedly opened a garbage store"),
            Err(e) => normalize_heed(e),
        }
    };

    // zerodb side: garbage `zerodb.dat` in a directory env.
    let zerodb_kind = {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(zerodb::DATA_FILE_NAME), vec![0xFFu8; 8192]).unwrap();
        match zerodb::EnvOpenOptions::new().open(dir.path()) {
            Ok(_) => panic!("zerodb unexpectedly opened a garbage store"),
            Err(e) => normalize_zerodb(e),
        }
    };

    assert_eq!(lmdb_kind, OracleError::Invalid);
    assert_eq!(zerodb_kind, OracleError::Invalid);
    assert_eq!(
        lmdb_kind, zerodb_kind,
        "foreign/garbage store must error the same kind on both engines"
    );
}
