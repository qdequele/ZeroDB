//! Helpers shared by the hostile-input tests: hand-crafted images (valid
//! CRCs, hostile geometry) opened over a heap backing, so the suites also run
//! under miri.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb_core::env::testutil::VecBacking;
use zerodb_core::env::{open_with_backing, DurabilityFlags, Env};
use zerodb_core::error::Error;
use zerodb_core::page::{LeafMut, MetaPage};

pub const PS: u32 = 4096;
pub const MAP: u64 = 1 << 20; // 256 pages

/// A registry path no other test of this binary uses.
pub fn unique_path(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/virtual/{tag}-{}-{n}", std::process::id()))
}

/// Build an image of `file_pages` pages: both meta slots encoded from
/// `edit(meta)`, then each `(pgno, frame)` copied in. The vector length is
/// exactly the "file" size (`VecBacking::real_disk_size` returns it), so a
/// meta naming geometry past `file_pages` simulates a truncated/hostile file.
pub fn craft_image(
    file_pages: usize,
    edit: impl Fn(&mut MetaPage),
    pages: &[(u64, Vec<u8>)],
) -> Vec<u8> {
    let ps = PS as usize;
    let mut buf = vec![0u8; file_pages * ps];
    for slot in [0u64, 1] {
        let mut m = MetaPage::create(slot, PS, MAP);
        edit(&mut m);
        let base = slot as usize * ps;
        m.encode(&mut buf[base..base + ps]).expect("valid meta");
    }
    for (pgno, frame) in pages {
        let base = *pgno as usize * ps;
        buf[base..base + frame.len()].copy_from_slice(frame);
    }
    buf
}

/// Open `buf` as an environment over a heap backing.
pub fn open_image(buf: Vec<u8>, tag: &str) -> Result<Env, Error> {
    open_with_backing(
        unique_path(tag),
        Box::new(VecBacking::new(buf, MAP)),
        PS,
        false,
        8,
        8,
        DurabilityFlags::default(),
    )
}

/// A one-entry leaf page frame stamped `(pgno, txnid)`.
pub fn leaf_frame(pgno: u64, txnid: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; PS as usize];
    let mut leaf = LeafMut::init(&mut buf, PS, pgno, txnid).unwrap();
    leaf.insert_inline(0, key, 0, val).unwrap();
    buf
}
