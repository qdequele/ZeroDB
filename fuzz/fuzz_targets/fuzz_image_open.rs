#![no_main]
//! Fuzz target `fuzz_image_open` (first-release security review, 2026-09).
//!
//! Treats the fuzz input as a whole env-file **image** (padded/truncated to
//! whole pages), opens it through the same I/O-free core entry point the real
//! mmap path uses (`open_with_backing` over a heap backing), and — when it
//! opens — drives the read API and the invariant checker over it.
//!
//! The oracle: **a typed error or a clean result, never a panic, hang,
//! unbounded allocation, or out-of-bounds read.** (The SIGBUS half of the
//! open-validation fix is covered by `zerodb/tests/hostile_file.rs`; a heap
//! backing turns any would-be wild map read into an OOB slice index, which
//! the sanitizer catches here.)
//!
//! Two images per input: the raw bytes (exercises slot validation), and a
//! "repaired" variant whose meta headers/CRCs are made valid so the fuzzer
//! explores hostile *geometry* (last_pg, roots, depths, records) behind a
//! passing checksum — the exact shape of the H1/H4 findings.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use libfuzzer_sys::fuzz_target;
use zerodb_core::check::check_image;
use zerodb_core::env::testutil::VecBacking;
use zerodb_core::env::{open_with_backing, DurabilityFlags, Env};
use zerodb_core::page::{crc32c, FORMAT_VERSION, MAGIC, P_META};

const PS: u32 = 4096;
const MAX_PAGES: usize = 16;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Open one image and, if it opens, run reads + a mutation attempt over it.
fn exercise(img: Vec<u8>) {
    // The checker must terminate quickly on anything (H3).
    let _ = check_image(&img, PS);

    let path = PathBuf::from(format!(
        "/virtual/fuzz-image-open-{}",
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let map_size = img.len() as u64;
    let opened: Result<Env, _> = open_with_backing(
        path,
        Box::new(VecBacking(img)),
        PS,
        map_size,
        false,
        4,
        4,
        DurabilityFlags::default(),
    );
    let Ok(env) = opened else { return };

    let db = env.main_database();
    if let Ok(txn) = env.read_txn() {
        let _ = db.len(&txn);
        let _ = db.get(&txn, b"probe");
        let _ = db.first(&txn);
        let _ = db.last(&txn);
        let _ = db.get_greater_than_or_equal_to(&txn, b"m");
        let mut it = db.iter(&txn);
        // A valid image of <= MAX_PAGES pages holds a bounded entry count;
        // the cap is belt-and-braces against an undetected cycle.
        for _ in 0..MAX_PAGES * 512 {
            match it.next() {
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    }
    // The write path decodes the GC tree (H4) and the main tree (M8); the
    // heap backing cannot commit, but put/delete exercise every decode.
    if let Ok(mut txn) = env.write_txn() {
        let _ = db.put(&mut txn, b"fuzz", b"v");
        let _ = db.delete(&mut txn, b"probe");
    };
}

fuzz_target!(|data: &[u8]| {
    let ps = PS as usize;
    let want_pages = (data.len() / ps + 1).clamp(2, MAX_PAGES);
    let mut img = vec![0u8; want_pages * ps];
    let n = data.len().min(img.len());
    img[..n].copy_from_slice(&data[..n]);

    // Pass 1: raw bytes (slot validation, selection, checker).
    exercise(img.clone());

    // Pass 2: force-valid meta headers + CRCs so hostile geometry survives
    // selection (magic/version/page_size/txnid-match/CRC are made to pass;
    // last_pg, map_size, and both DBRecords stay fuzz-controlled).
    for slot in 0..2usize {
        let base = slot * ps;
        let s = &mut img[base..base + ps];
        s[0..8].copy_from_slice(&(slot as u64).to_le_bytes()); // header pgno
        s[16..18].copy_from_slice(&P_META.to_le_bytes()); // flags
        s[32..36].copy_from_slice(&MAGIC);
        s[36..40].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        s[40..44].copy_from_slice(&PS.to_le_bytes());
        let body_txnid = s[64..72].to_vec();
        s[8..16].copy_from_slice(&body_txnid); // header txnid = body txnid
        let crc = crc32c(&s[..168]);
        s[168..172].copy_from_slice(&crc.to_le_bytes());
    }
    exercise(img);
});
