//! [`MapPages`]: a view of the mapped data file that lends slices of the
//! pages being read, never a reference to the whole map (ADR-0021
//! amendment 1).
//!
//! # Why not a `&[u8]` over the whole map
//!
//! A live `&[u8]` asserts that none of the bytes it covers change while it
//! lives. Under `WRITE_MAP` the single writer stores into free pages of the
//! map in place (TXN-45b), and without it the writer's commit `pwrite`s
//! them; those free pages lie inside any whole-map slice a reader in
//! another thread could hold. TXN-62 keeps the pages a reader dereferences
//! disjoint from the pages the writer stores to, so a reader that borrows
//! only the pages it reads never covers a byte that changes.
//!
//! # Contract
//!
//! [`MapPages::get`] is safe and bounds-checked. Its soundness rests on the
//! engine invariants stated in `zerodb-io`'s mmap contract: callers ask only
//! for pages a snapshot they pin references (TXN-20/21, SPEC 06 REC-14), the
//! writer's own frames, or the meta slots on paths that read them back as a
//! copy. [`MapPages::whole`] lends every byte at once and is only for code
//! that runs where no store can happen concurrently (see its docs).

use std::marker::PhantomData;
use std::ptr::NonNull;

/// A `Copy` view of a mapped (or, in tests, heap) region of `len` bytes,
/// valid for `'a`. It holds a pointer and a length, not a reference, so
/// holding or copying it asserts nothing about bytes it does not lend.
#[derive(Clone, Copy)]
pub struct MapPages<'a> {
    ptr: NonNull<u8>,
    len: usize,
    _region: PhantomData<&'a [u8]>,
}

// SAFETY: `MapPages` lends only shared `&'a [u8]` slices, exactly as the
// `&'a [u8]` it stands in for, which is `Send + Sync`. Concurrent stores into
// the region are made only by the single writer into pages no slice handed
// to another thread covers (the module contract).
unsafe impl Send for MapPages<'_> {}
// SAFETY: as for `Send` above.
unsafe impl Sync for MapPages<'_> {}

impl<'a> MapPages<'a> {
    /// An empty region (a backing with no map, such as a closed env).
    #[must_use]
    pub const fn empty() -> MapPages<'static> {
        MapPages {
            ptr: NonNull::dangling(),
            len: 0,
            _region: PhantomData,
        }
    }

    /// A view of `bytes`, for heap regions nothing writes to while the view
    /// lives (test backings, file images read into memory).
    #[must_use]
    pub fn from_slice(bytes: &'a [u8]) -> MapPages<'a> {
        MapPages {
            ptr: NonNull::from(bytes).cast(),
            len: bytes.len(),
            _region: PhantomData,
        }
    }

    /// A view of the `len` bytes at `ptr`.
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for reads of `len` bytes for all of
    /// `'a` (a mapping that stays mapped, or a heap buffer that outlives
    /// `'a`), with provenance over the whole range. Stores into the region
    /// while the view lives must keep to the module contract: only the
    /// single writer stores, and only into pages no slice lent to another
    /// thread covers.
    #[must_use]
    pub unsafe fn from_raw_parts(ptr: *const u8, len: usize) -> MapPages<'a> {
        MapPages {
            // SAFETY: the caller guarantees `ptr` is non-null.
            ptr: unsafe { NonNull::new_unchecked(ptr.cast_mut()) },
            len,
            _region: PhantomData,
        }
    }

    /// The region's length in bytes.
    #[must_use]
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the region is empty.
    #[must_use]
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The region's base address, for handing a range of it to the kernel
    /// (a `pwrite` from the map) without creating a reference to it.
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// The `len` bytes at offset `off`, or `None` if they are not all inside
    /// the region.
    #[must_use]
    #[inline(always)]
    pub fn get(&self, off: usize, len: usize) -> Option<&'a [u8]> {
        let end = off.checked_add(len)?;
        if end > self.len {
            return None;
        }
        // SAFETY: `[off, end)` is inside the region, which the constructor
        // guarantees is valid for reads for `'a`; no store targets it while
        // the slice lives (the module contract).
        Some(unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(off), len) })
    }

    /// The first `len` bytes of the region as a region of its own, or `None`
    /// if the region is shorter.
    #[must_use]
    pub fn prefix(&self, len: usize) -> Option<MapPages<'a>> {
        (len <= self.len).then_some(MapPages { len, ..*self })
    }

    /// Every byte of the region as one slice.
    ///
    /// Only for code where no write transaction of this process can store
    /// into the region while the slice lives: environment open, a tool
    /// running in its own process, the writer's own thread between its
    /// stores, and test assertions. The read path never uses it.
    #[must_use]
    pub fn whole(&self) -> &'a [u8] {
        // SAFETY: the region is valid for reads for `'a` (constructor); the
        // documented callers run where no store happens while it lives.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

#[cfg(test)]
mod tests {
    use super::MapPages;

    #[test]
    fn get_is_bounded() {
        let buf: Vec<u8> = (0..=255).collect();
        let m = MapPages::from_slice(&buf);
        assert_eq!(m.len(), 256);
        assert_eq!(m.get(10, 3), Some(&buf[10..13]));
        assert_eq!(m.get(256, 0), Some(&[][..]));
        assert_eq!(m.get(250, 7), None);
        assert_eq!(m.get(usize::MAX, 2), None);
        assert_eq!(m.whole(), &buf[..]);
        assert_eq!(m.prefix(16).map(|p| p.whole()), Some(&buf[..16]));
        assert!(m.prefix(257).is_none());
        assert!(MapPages::empty().is_empty());
        assert_eq!(MapPages::empty().get(0, 1), None);
    }
}
