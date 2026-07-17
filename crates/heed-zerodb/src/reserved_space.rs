//! `ReservedSpace` — heed's `MDB_RESERVE` write buffer (SPEC 00 row 35). heed
//! reserves bytes *inside the map*; the adapter reserves a heap buffer the
//! caller fills, which `Database::put_reserved` then writes into the dirty page
//! via ZeroDB's native `put_reserved` (SPEC 04 §6). API-compatible with heed's:
//! `io::Write` + `size`/`remaining`/`written_mut`/`fill_zeroes`.

use std::io;

/// A buffer the caller fills for a reserved-space put. Must be fully written.
pub struct ReservedSpace<'a> {
    bytes: &'a mut [u8],
    written: usize,
    write_head: usize,
}

impl<'a> ReservedSpace<'a> {
    /// Wrap a (zero-initialized) buffer of the reserved size.
    pub(crate) fn new(bytes: &'a mut [u8]) -> ReservedSpace<'a> {
        ReservedSpace {
            bytes,
            written: 0,
            write_head: 0,
        }
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
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`; the buffer is
        // heap-owned and fully initialized (zeroed at creation).
        unsafe { std::slice::from_raw_parts_mut(ptr, len) }
    }

    /// Mark the first `len` bytes as written.
    ///
    /// # Safety
    ///
    /// The caller guarantees those bytes are initialized (always true here — the
    /// backing buffer starts zeroed).
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
