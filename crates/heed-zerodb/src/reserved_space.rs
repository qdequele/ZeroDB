//! `ReservedSpace` — heed's `MDB_RESERVE` write buffer (SPEC 00 row 35). heed
//! reserves bytes *inside the map*; since PERF-GAP B6 (2026-07-21) the adapter
//! does the equivalent: `Database::put_reserved` hands the caller a
//! `ReservedSpace` wrapping the engine's **in-frame slot** (the mutable slice
//! into the dirty page or overflow-run frame, SPEC 04 TXN-47) — no
//! intermediate heap buffer, no copy. The slot may carry stale frame bytes
//! (a COWed page's old cell heap), so the put path zeroes the unwritten tail
//! after the caller's closure runs, preserving the pre-B6 zero-tail contract.
//! API-compatible with heed's: `io::Write` + `size`/`remaining`/`written_mut`/
//! `fill_zeroes`.

use std::io;

/// A buffer the caller fills for a reserved-space put.
pub struct ReservedSpace<'a> {
    bytes: &'a mut [u8],
    written: usize,
    write_head: usize,
}

impl<'a> ReservedSpace<'a> {
    /// Wrap the reserved slot (since B6: the engine's in-frame slice, whose
    /// bytes may be stale until written or [`zero_unwritten_tail`]ed).
    pub(crate) fn new(bytes: &'a mut [u8]) -> ReservedSpace<'a> {
        ReservedSpace {
            bytes,
            written: 0,
            write_head: 0,
        }
    }

    /// Zero every byte past the written high-water mark (PERF-GAP B6): the
    /// space wraps the engine's in-frame slot, whose bytes are whatever the
    /// (COWed) frame held there — zeroing the tail preserves the adapter's
    /// shipped contract (the pre-B6 heap buffer was zero-initialized) and
    /// keeps stores byte-deterministic.
    pub(crate) fn zero_unwritten_tail(&mut self) {
        self.bytes[self.written..].fill(0);
    }

    /// The total number of bytes this buffer has.
    #[inline]
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// The remaining number of writable bytes.
    #[inline]
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.write_head
    }

    /// A slice of all bytes previously written.
    #[inline]
    pub fn written_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.written]
    }

    /// Fill the remaining space with zeroes and mark the buffer fully written.
    #[inline]
    pub fn fill_zeroes(&mut self) {
        self.bytes[self.write_head..].fill(0);
        self.written = self.bytes.len();
        self.write_head = self.bytes.len();
    }

    /// The entire reserved space as (already-initialized) `MaybeUninit` bytes.
    #[inline]
    pub fn as_uninit_mut(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        let len = self.bytes.len();
        let ptr = self.bytes.as_mut_ptr().cast::<std::mem::MaybeUninit<u8>>();
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`; the slot is a
        // live `&mut [u8]` (initialized memory — since B6 it is dirty-frame
        // bytes, possibly *stale* in value but never uninitialized).
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }

    /// Mark the first `len` bytes as written.
    ///
    /// # Safety
    ///
    /// The caller guarantees those bytes are initialized (always true here —
    /// the slot is a live `&mut [u8]`, so every byte is initialized memory;
    /// "written" only moves the high-water mark used by `remaining`/
    /// `written_mut` and the B6 zero-tail).
    #[inline]
    pub unsafe fn assume_written(&mut self, len: usize) {
        debug_assert!(len <= self.bytes.len());
        self.written = len;
        self.write_head = len;
    }
}

impl io::Write for ReservedSpace<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let remaining = &mut self.bytes[self.write_head..];
        if buf.len() > remaining.len() {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        remaining[..buf.len()].copy_from_slice(buf);
        self.write_head += buf.len();
        self.written = self.written.max(self.write_head);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Seek for ReservedSpace<'_> {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (base, offset) = match pos {
            io::SeekFrom::Start(start) => (start, 0),
            io::SeekFrom::End(offset) => (self.written as u64, offset),
            io::SeekFrom::Current(offset) => (self.write_head as u64, offset),
        };
        let Some(new_pos) = base.checked_add_signed(offset) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        };
        if new_pos > self.written as u64 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek past end"));
        }
        self.write_head = new_pos as usize;
        Ok(new_pos)
    }
}

impl std::fmt::Debug for ReservedSpace<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ReservedSpace").finish()
    }
}
