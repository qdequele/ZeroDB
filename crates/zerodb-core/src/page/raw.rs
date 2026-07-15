//! Alignment-safe little-endian field accessors.
//!
//! SPEC 02 §0 forbids `#[repr(C)]` casting of possibly-unaligned page data.
//! Every multi-byte field is therefore read and written through these helpers,
//! which copy the exact bytes into a stack array and use `from_le_bytes` /
//! `to_le_bytes`. These are pure safe Rust — no `unsafe`, no alignment
//! assumptions — and are equivalent to `read_unaligned`/`write_unaligned` for
//! little-endian integers.
//!
//! All functions panic on out-of-bounds access; callers in this crate validate
//! bounds against the page size before invoking them (the view constructors in
//! `tree`/`overflow`/`meta` do this and surface a typed [`PageError`] instead).
//!
//! [`PageError`]: super::PageError

/// Read a little-endian `u16` at byte offset `off`.
#[inline]
pub(crate) fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

/// Read a little-endian `u32` at byte offset `off`.
#[inline]
pub(crate) fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Read a little-endian `u64` at byte offset `off`.
#[inline]
pub(crate) fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

/// Write a little-endian `u16` at byte offset `off`.
#[inline]
pub(crate) fn write_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

/// Write a little-endian `u32` at byte offset `off`.
#[inline]
pub(crate) fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

/// Write a little-endian `u64` at byte offset `off`.
#[inline]
pub(crate) fn write_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_u16_u32_u64_unaligned() {
        // Deliberately use an odd base offset to prove alignment independence.
        let mut buf = [0u8; 32];
        write_u16(&mut buf, 1, 0xABCD);
        write_u32(&mut buf, 3, 0x1234_5678);
        write_u64(&mut buf, 7, 0x0102_0304_0506_0708);
        assert_eq!(read_u16(&buf, 1), 0xABCD);
        assert_eq!(read_u32(&buf, 3), 0x1234_5678);
        assert_eq!(read_u64(&buf, 7), 0x0102_0304_0506_0708);
    }

    #[test]
    fn little_endian_byte_order() {
        let mut buf = [0u8; 8];
        write_u32(&mut buf, 0, 0x0000_1000); // 4096
        assert_eq!(buf[0], 0x00);
        assert_eq!(buf[1], 0x10);
        assert_eq!(buf[2], 0x00);
        assert_eq!(buf[3], 0x00);
    }
}
