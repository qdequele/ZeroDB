//! A heap-backed stand-in for the writable map (`test-backing` feature;
//! ADR-0021 M1) so the **in-place `WRITE_MAP` brokered discipline runs under
//! miri**: the real `MmapWritable` is OS memory miri cannot see, while
//! [`TestWriteMap`] holds the "map" in an `UnsafeCell<Box<[u8]>>` — plain
//! heap memory on which Stacked/Tree Borrows check every access.
//!
//! It implements exactly the [`Backing`] surface the in-place path uses:
//!
//! - [`Backing::bytes`] mints a fresh whole-buffer `&[u8]` per call (as a
//!   remapped-never `MAP_SHARED` view, this is what the engine's read paths
//!   hold — the writer re-derives its copy per spill, ADR-0021 B2);
//! - [`Backing::map_dirty_page`] (the `unsafe fn` broker, ADR-0021 B1) mints
//!   the region `&mut [u8]` from a root-derived raw pointer, exactly as
//!   `MmapWritable::slice_mut` does;
//! - [`Backing::write_at_page`] (the C4 meta write, and C2 for any heap
//!   frame) copies through a raw pointer **without** minting a `&mut` —
//!   mirroring the kernel-side mutation of a real `pwrite`/`memcpy` that the
//!   borrow system never sees, while still letting miri flag a read of the
//!   written locations through a stale view.
//!
//! Any violation of the brokered contract — two live `&mut` into one region,
//! a stale `&[u8]` read at in-place-written locations (the ADR-0021 B2
//! hazard), a frame borrow outliving its `&mut` op — is undefined behavior
//! on this backing and miri reports it. `zerodb-core`'s
//! `writemap_in_place_miri` integration test drives the full txn surface
//! (COW touch, splits, overflow runs, `put_reserved`, spill/unspill, nested
//! reads, abort, readers across commits) over this backing under
//! `cargo +nightly miri test -p zerodb-core`.
//!
//! Test infrastructure only (the `zerodb-core` and `heed-zerodb` dev-deps
//! enable the feature); consumer builds never compile it. `unsafe` lives in
//! `zerodb-io` per the unsafe policy (this module is the mmap stand-in).

use std::cell::UnsafeCell;

use zerodb_core::env::Backing;
use zerodb_core::page::MetaPage;

/// The in-memory writable "map". See the module docs.
pub struct TestWriteMap {
    /// The mapped bytes. `UnsafeCell` because the broker hands out `&mut`
    /// regions and `write_at_page` mutates through `&self`, exactly like the
    /// real `MAP_SHARED` mapping; every access derives from this cell so
    /// miri tracks the aliasing the engine's discipline must uphold.
    bytes: UnsafeCell<Box<[u8]>>,
}

// SAFETY: the same aliasing contract as the real writable map
// (`MmapWritable::map`): mutation happens only through the single writer
// (TXN-6) — the brokered regions target pages no live snapshot references
// (TXN-62), the meta slots are never lent out as borrows (TXN-18) — so no
// `&[u8]` another thread actually dereferences is mutated while borrowed.
// A test that broke that contract would be UB on the real backing too; on
// this one, miri (single- or multi-threaded) reports it.
#[allow(unsafe_code)]
unsafe impl Sync for TestWriteMap {}

impl TestWriteMap {
    /// A map of `len` bytes holding `image` at offset 0 (zero-filled past it).
    ///
    /// # Panics
    ///
    /// If `image` exceeds `len`.
    #[must_use]
    pub fn new(len: usize, image: &[u8]) -> TestWriteMap {
        assert!(image.len() <= len, "image larger than the map");
        let mut buf = vec![0u8; len];
        buf[..image.len()].copy_from_slice(image);
        TestWriteMap {
            bytes: UnsafeCell::new(buf.into_boxed_slice()),
        }
    }

    /// A fresh two-meta env image (SPEC 02 §3.4) of `map_size` bytes at
    /// `page_size` — the standard starting point for a test env.
    ///
    /// # Panics
    ///
    /// On invalid geometry (`map_size < 2 * page_size`, bad page size).
    #[must_use]
    pub fn fresh_env(page_size: u32, map_size: usize) -> TestWriteMap {
        let ps = page_size as usize;
        assert!(map_size >= 2 * ps, "map must hold both meta slots");
        let mut head = vec![0u8; 2 * ps];
        for slot in [0u64, 1] {
            let meta = MetaPage::create(slot, page_size, map_size as u64);
            let base = slot as usize * ps;
            meta.encode(&mut head[base..base + ps])
                .expect("creation meta encodes");
        }
        TestWriteMap::new(map_size, &head)
    }

    fn len(&self) -> usize {
        self.bytes().len()
    }

    /// Page-region bounds as a byte range, or `None` when out of the map.
    fn region(&self, pgno: u64, psize: u32, pages: u64) -> Option<(usize, usize)> {
        let ps = psize as usize;
        let off = usize::try_from(pgno).ok()?.checked_mul(ps)?;
        let len = usize::try_from(pages).ok()?.checked_mul(ps)?;
        let end = off.checked_add(len)?;
        if len == 0 || end > self.len() {
            return None;
        }
        Some((off, len))
    }
}

#[allow(unsafe_code)]
impl Backing for TestWriteMap {
    fn bytes(&self) -> &[u8] {
        // SAFETY: a fresh whole-buffer shared view per call, derived from the
        // cell's root (no `&mut` is created on the way — `addr_of!` projects
        // the place raw). Sound under the map contract (see `Sync` above):
        // the single writer mutates only TXN-62 regions and re-derives its
        // own read view after each spill, so no caller reads this view at
        // locations a later in-place write touched. miri enforces exactly
        // that: a stale read at written locations is reported.
        unsafe {
            let b = self.bytes.get();
            &*std::ptr::addr_of!(**b)
        }
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "TestWriteMap has no file",
        ))
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        let ps = psize as usize;
        debug_assert!(!data.is_empty() && data.len().is_multiple_of(ps));
        let (off, _) = self
            .region(pgno, psize, (data.len() / ps) as u64)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("write [{pgno}, +{}) outside the test map", data.len()),
                )
            })?;
        // SAFETY: bounds checked above; the copy goes through a root-derived
        // raw pointer and mints no reference, mirroring the fd-side mutation
        // of the real backing. Exclusivity is the commit path's TXN-62/TXN-18
        // contract (see `Sync`); `data` is a caller buffer distinct from the
        // map (dirty heap frame or the C4 meta buffer).
        unsafe {
            let b = self.bytes.get();
            let base: *mut u8 = std::ptr::addr_of_mut!(**b).cast();
            std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(off), data.len());
        }
        Ok(())
    }

    fn dirty_in_map(&self) -> bool {
        true
    }

    // clippy cannot see that this is an `unsafe fn` whose documented contract
    // covers exactly what `mut_from_ref` fears (the lint fires on unsafe fns
    // too); ADR-0021 B1's substance is the `unsafe fn` itself.
    #[allow(clippy::mut_from_ref)]
    unsafe fn map_dirty_page(&self, pgno: u64, psize: u32, pages: u64) -> Option<&mut [u8]> {
        let (off, len) = self.region(pgno, psize, pages)?;
        // SAFETY: bounds checked above; the region `&mut` derives from the
        // cell's root (never through a shared view), exactly as
        // `MmapWritable::slice_mut`. Exclusivity of the region is the
        // caller's brokered contract (`Backing::map_dirty_page`) — the one
        // this backing exists to let miri check.
        unsafe {
            let b = self.bytes.get();
            let base: *mut u8 = std::ptr::addr_of_mut!(**b).cast();
            Some(std::slice::from_raw_parts_mut(base.add(off), len))
        }
    }

    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync(&self, _async_flush: bool) -> std::io::Result<()> {
        Ok(())
    }
}
