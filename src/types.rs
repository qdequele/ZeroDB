//! Type encoding/decoding for heed-compatible API.
//!
//! This module provides traits and implementations for encoding/decoding
//! keys and values in a type-safe manner.

use std::marker::PhantomData;

use crate::error::{Error, Result};

// Re-export byteorder for Heed compatibility
pub use byteorder::{BigEndian, LittleEndian, NativeEndian, ByteOrder, BE, LE};

/// Alias for NativeEndian
pub type NE = NativeEndian;

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

/// Trait for types that decode to owned values (no lifetime dependency on input).
///
/// This is used by the typed Database API when the decoded values are copied.
pub trait OwnedDecode {
    /// The owned decoded type.
    type OwnedItem;

    /// Decodes bytes into an owned item.
    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem>;
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

impl OwnedDecode for Bytes {
    type OwnedItem = Vec<u8>;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        Ok(bytes.to_vec())
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

impl OwnedDecode for OwnedBytes {
    type OwnedItem = Vec<u8>;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for Str {
    type OwnedItem = String;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Corrupted)
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

impl OwnedDecode for OwnedStr {
    type OwnedItem = String;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for U32 {
    type OwnedItem = u32;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for U64 {
    type OwnedItem = u64;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for I32 {
    type OwnedItem = i32;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for I64 {
    type OwnedItem = i64;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
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

impl OwnedDecode for Unit {
    type OwnedItem = ();

    fn decode_owned(_bytes: &[u8]) -> Result<Self::OwnedItem> {
        Ok(())
    }
}

// =============================================================================
// DecodeIgnore - ignores the value when decoding
// =============================================================================

/// A type that ignores the value when decoding.
///
/// This is useful when you only care about keys and want to skip
/// decoding values entirely.
pub struct DecodeIgnore;

impl<'a> BytesEncode<'a> for DecodeIgnore {
    type EItem = ();

    fn bytes_encode(_item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Borrowed(&[]))
    }
}

impl<'a> BytesDecode<'a> for DecodeIgnore {
    type DItem = ();

    fn bytes_decode(_bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(())
    }
}

impl OwnedDecode for DecodeIgnore {
    type OwnedItem = ();

    fn decode_owned(_bytes: &[u8]) -> Result<Self::OwnedItem> {
        Ok(())
    }
}

// =============================================================================
// Endian-aware integer types (Heed compatible)
// =============================================================================

/// A u16 encoded in a specific byte order.
pub struct U16<O>(PhantomData<O>);

/// Alias for big-endian u16 (Heed compatible).
pub type BEU16 = U16<BigEndian>;

impl<'a, O: ByteOrder> BytesEncode<'a> for U16<O> {
    type EItem = u16;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        let mut buf = [0u8; 2];
        O::write_u16(&mut buf, *item);
        Ok(std::borrow::Cow::Owned(buf.to_vec()))
    }
}

impl<'a, O: ByteOrder> BytesDecode<'a> for U16<O> {
    type DItem = u16;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 2 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u16(bytes))
    }
}

impl<O: ByteOrder> OwnedDecode for U16<O> {
    type OwnedItem = u16;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        if bytes.len() != 2 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u16(bytes))
    }
}

/// A u32 encoded in a specific byte order.
pub struct U32BE<O>(PhantomData<O>);

/// Alias for big-endian u32 (Heed compatible).
pub type BEU32 = U32BE<BigEndian>;

impl<'a, O: ByteOrder> BytesEncode<'a> for U32BE<O> {
    type EItem = u32;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        let mut buf = [0u8; 4];
        O::write_u32(&mut buf, *item);
        Ok(std::borrow::Cow::Owned(buf.to_vec()))
    }
}

impl<'a, O: ByteOrder> BytesDecode<'a> for U32BE<O> {
    type DItem = u32;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 4 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u32(bytes))
    }
}

impl<O: ByteOrder> OwnedDecode for U32BE<O> {
    type OwnedItem = u32;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        if bytes.len() != 4 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u32(bytes))
    }
}

/// A u64 encoded in a specific byte order.
pub struct U64BE<O>(PhantomData<O>);

/// Alias for big-endian u64 (Heed compatible).
pub type BEU64 = U64BE<BigEndian>;

impl<'a, O: ByteOrder> BytesEncode<'a> for U64BE<O> {
    type EItem = u64;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        let mut buf = [0u8; 8];
        O::write_u64(&mut buf, *item);
        Ok(std::borrow::Cow::Owned(buf.to_vec()))
    }
}

impl<'a, O: ByteOrder> BytesDecode<'a> for U64BE<O> {
    type DItem = u64;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 8 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u64(bytes))
    }
}

impl<O: ByteOrder> OwnedDecode for U64BE<O> {
    type OwnedItem = u64;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        if bytes.len() != 8 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u64(bytes))
    }
}

/// A u128 encoded in a specific byte order.
pub struct U128<O>(PhantomData<O>);

/// Alias for big-endian u128 (Heed compatible).
pub type BEU128 = U128<BigEndian>;

impl<'a, O: ByteOrder> BytesEncode<'a> for U128<O> {
    type EItem = u128;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        let mut buf = [0u8; 16];
        O::write_u128(&mut buf, *item);
        Ok(std::borrow::Cow::Owned(buf.to_vec()))
    }
}

impl<'a, O: ByteOrder> BytesDecode<'a> for U128<O> {
    type DItem = u128;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 16 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u128(bytes))
    }
}

impl<O: ByteOrder> OwnedDecode for U128<O> {
    type OwnedItem = u128;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        if bytes.len() != 16 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_u128(bytes))
    }
}

/// A i128 encoded in a specific byte order.
pub struct I128<O>(PhantomData<O>);

/// Alias for big-endian i128 (Heed compatible).
pub type BEI128 = I128<BigEndian>;

impl<'a, O: ByteOrder> BytesEncode<'a> for I128<O> {
    type EItem = i128;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        let mut buf = [0u8; 16];
        O::write_i128(&mut buf, *item);
        Ok(std::borrow::Cow::Owned(buf.to_vec()))
    }
}

impl<'a, O: ByteOrder> BytesDecode<'a> for I128<O> {
    type DItem = i128;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != 16 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_i128(bytes))
    }
}

impl<O: ByteOrder> OwnedDecode for I128<O> {
    type OwnedItem = i128;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        if bytes.len() != 16 {
            return Err(Error::Corrupted);
        }
        Ok(O::read_i128(bytes))
    }
}

// =============================================================================
// Serde types (feature-gated)
// =============================================================================

/// A type that serializes and deserializes using JSON.
#[cfg(feature = "serde")]
pub struct SerdeJson<T>(PhantomData<T>);

#[cfg(feature = "serde")]
impl<'a, T> BytesEncode<'a> for SerdeJson<T>
where
    T: serde::Serialize + 'a,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        serde_json::to_vec(item)
            .map(std::borrow::Cow::Owned)
            .map_err(|_| Error::Corrupted)
    }
}

#[cfg(feature = "serde")]
impl<'a, T> BytesDecode<'a> for SerdeJson<T>
where
    T: serde::de::DeserializeOwned + 'a,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        serde_json::from_slice(bytes).map_err(|_| Error::Corrupted)
    }
}

#[cfg(feature = "serde")]
impl<T> OwnedDecode for SerdeJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type OwnedItem = T;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        serde_json::from_slice(bytes).map_err(|_| Error::Corrupted)
    }
}

/// A type that serializes and deserializes using Bincode.
#[cfg(feature = "serde")]
pub struct SerdeBincode<T>(PhantomData<T>);

#[cfg(feature = "serde")]
impl<'a, T> BytesEncode<'a> for SerdeBincode<T>
where
    T: serde::Serialize + 'a,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>> {
        bincode::serialize(item)
            .map(std::borrow::Cow::Owned)
            .map_err(|_| Error::Corrupted)
    }
}

#[cfg(feature = "serde")]
impl<'a, T> BytesDecode<'a> for SerdeBincode<T>
where
    T: serde::de::DeserializeOwned + 'a,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        bincode::deserialize(bytes).map_err(|_| Error::Corrupted)
    }
}

#[cfg(feature = "serde")]
impl<T> OwnedDecode for SerdeBincode<T>
where
    T: serde::de::DeserializeOwned,
{
    type OwnedItem = T;

    fn decode_owned(bytes: &[u8]) -> Result<Self::OwnedItem> {
        bincode::deserialize(bytes).map_err(|_| Error::Corrupted)
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

    #[test]
    fn decode_ignore_roundtrip() {
        let _encoded = DecodeIgnore::bytes_encode(&()).unwrap();
        // DecodeIgnore decodes anything to ()
        let data = b"some arbitrary data";
        let decoded = DecodeIgnore::bytes_decode(data).unwrap();
        assert_eq!(decoded, ());
    }

    #[test]
    fn beu16_roundtrip() {
        let n: u16 = 0x1234;
        let encoded = BEU16::bytes_encode(&n).unwrap();
        // Big endian: 0x12, 0x34
        assert_eq!(&*encoded, &[0x12, 0x34]);
        let decoded = BEU16::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn beu32_roundtrip() {
        let n: u32 = 0x12345678;
        let encoded = BEU32::bytes_encode(&n).unwrap();
        // Big endian: 0x12, 0x34, 0x56, 0x78
        assert_eq!(&*encoded, &[0x12, 0x34, 0x56, 0x78]);
        let decoded = BEU32::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn beu64_roundtrip() {
        let n: u64 = 0x123456789ABCDEF0;
        let encoded = BEU64::bytes_encode(&n).unwrap();
        // Big endian bytes
        assert_eq!(&*encoded, &[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]);
        let decoded = BEU64::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn beu128_roundtrip() {
        let n: u128 = 0x0102030405060708090A0B0C0D0E0F10;
        let encoded = BEU128::bytes_encode(&n).unwrap();
        let decoded = BEU128::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn bei128_roundtrip() {
        let n: i128 = -0x0102030405060708090A0B0C0D0E0F10;
        let encoded = BEI128::bytes_encode(&n).unwrap();
        let decoded = BEI128::bytes_decode(&encoded).unwrap();
        assert_eq!(decoded, n);
    }
}
