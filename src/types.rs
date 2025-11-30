//! Type encoding/decoding for heed-compatible API.
//!
//! This module provides traits and implementations for encoding/decoding
//! keys and values in a type-safe manner.

use crate::error::{Error, Result};

/// Trait for types that can be used as database keys or values.
pub trait BytesEncode<'a> {
    /// The type to encode.
    type EItem: ?Sized + 'a;

    /// Encodes the item to bytes.
    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>>;
}

/// Trait for types that can be decoded from database bytes.
pub trait BytesDecode<'a> {
    /// The type to decode into.
    type DItem: 'a;

    /// Decodes bytes into the item.
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem>;
}

/// Marker for raw byte slices - no encoding/decoding.
pub struct Bytes;

impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(bytes)
    }
}

/// Marker for owned byte vectors.
pub struct OwnedBytes;

impl<'a> BytesEncode<'a> for OwnedBytes {
    type EItem = Vec<u8>;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(item.as_slice()))
    }
}

impl<'a> BytesDecode<'a> for OwnedBytes {
    type DItem = Vec<u8>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(bytes.to_vec())
    }
}

/// Marker for UTF-8 strings.
pub struct Str;

impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        std::str::from_utf8(bytes).map_err(|_| Error::Corrupted)
    }
}

/// Marker for owned strings.
pub struct OwnedStr;

impl<'a> BytesEncode<'a> for OwnedStr {
    type EItem = String;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for OwnedStr {
    type DItem = String;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Corrupted)
    }
}

/// Marker for u32 in native endian.
pub struct U32;

impl<'a> BytesEncode<'a> for U32 {
    type EItem = u32;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Owned(item.to_ne_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for U32 {
    type DItem = u32;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 4 {
            return Err(Error::Corrupted);
        }
        Ok(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

/// Marker for u64 in native endian.
pub struct U64;

impl<'a> BytesEncode<'a> for U64 {
    type EItem = u64;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Owned(item.to_ne_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for U64 {
    type DItem = u64;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 8 {
            return Err(Error::Corrupted);
        }
        Ok(u64::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

/// Marker for i32 in native endian.
pub struct I32;

impl<'a> BytesEncode<'a> for I32 {
    type EItem = i32;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Owned(item.to_ne_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for I32 {
    type DItem = i32;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 4 {
            return Err(Error::Corrupted);
        }
        Ok(i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

/// Marker for i64 in native endian.
pub struct I64;

impl<'a> BytesEncode<'a> for I64 {
    type EItem = i64;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Owned(item.to_ne_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for I64 {
    type DItem = i64;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 8 {
            return Err(Error::Corrupted);
        }
        Ok(i64::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

/// Unit type for databases that only store keys (sets).
pub struct Unit;

impl<'a> BytesEncode<'a> for Unit {
    type EItem = ();

    fn bytes_encode(_item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(&[]))
    }
}

impl<'a> BytesDecode<'a> for Unit {
    type DItem = ();

    fn bytes_decode(_bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_roundtrip() {
        let data = b"hello world";
        let encoded = Bytes::bytes_encode(data).unwrap();
        let decoded = Bytes::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn str_roundtrip() {
        let s = "hello world";
        let encoded = Str::bytes_encode(s).unwrap();
        let decoded = Str::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn u32_roundtrip() {
        let n: u32 = 12345678;
        let encoded = U32::bytes_encode(&n).unwrap();
        let decoded = U32::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn u64_roundtrip() {
        let n: u64 = 12345678901234;
        let encoded = U64::bytes_encode(&n).unwrap();
        let decoded = U64::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn i32_roundtrip() {
        let n: i32 = -12345;
        let encoded = I32::bytes_encode(&n).unwrap();
        let decoded = I32::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn i64_roundtrip() {
        let n: i64 = -12345678901234;
        let encoded = I64::bytes_encode(&n).unwrap();
        let decoded = I64::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }
}
