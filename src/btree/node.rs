//! Node representation within B+tree pages.
//!
//! Nodes are the individual entries within branch and leaf pages.
//! This module provides efficient access to node data within page buffers.

use crate::error::{Error, Result};
use crate::page::{NodeFlags, PageNo};

/// Minimum node header size.
pub const NODE_HEADER_SIZE: usize = 8;

/// A reference to a node within a page buffer.
///
/// This is a zero-copy view into the page data.
#[derive(Debug, Clone, Copy)]
pub struct NodeRef<'a> {
    /// Raw node data.
    data: &'a [u8],
    /// Key portion of the node.
    key: &'a [u8],
    /// Data portion (for leaf nodes).
    value: &'a [u8],
    /// Flags (for leaf nodes).
    flags: NodeFlags,
    /// Child page number (for branch nodes) or data size.
    extra: u64,
}

impl<'a> NodeRef<'a> {
    /// Parses a branch node from raw data.
    ///
    /// Branch node layout:
    /// - lo (4 bytes): lower 32 bits of child pgno
    /// - hi_and_ksize (2 bytes): (pgno_hi << 12) | key_size
    /// - key (variable): key bytes
    pub fn parse_branch(data: &'a [u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(Error::Corrupted);
        }

        let lo = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let hi_and_ksize = u16::from_le_bytes([data[4], data[5]]);

        let pgno_hi = (hi_and_ksize >> 12) as u64;
        let key_size = (hi_and_ksize & 0x0FFF) as usize;

        if data.len() < 6 + key_size {
            return Err(Error::Corrupted);
        }

        let child_pgno = (pgno_hi << 32) | (lo as u64);
        let key = &data[6..6 + key_size];

        Ok(Self {
            data,
            key,
            value: &[],
            flags: NodeFlags::empty(),
            extra: child_pgno,
        })
    }

    /// Parses a leaf node from raw data.
    ///
    /// Leaf node layout:
    /// - lo (4 bytes): data size low bits OR overflow pgno low bits
    /// - hi_and_ksize (2 bytes): (size_hi << 12) | key_size
    /// - flags (2 bytes): node flags
    /// - key (variable): key bytes
    /// - data (variable): value bytes OR overflow pgno (8 bytes)
    pub fn parse_leaf(data: &'a [u8]) -> Result<Self> {
        if data.len() < NODE_HEADER_SIZE {
            return Err(Error::Corrupted);
        }

        let lo = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let hi_and_ksize = u16::from_le_bytes([data[4], data[5]]);
        let flags_bits = u16::from_le_bytes([data[6], data[7]]);

        let flags = NodeFlags::from_bits_truncate(flags_bits);
        let size_hi = (hi_and_ksize >> 12) as u64;
        let key_size = (hi_and_ksize & 0x0FFF) as usize;

        if data.len() < NODE_HEADER_SIZE + key_size {
            return Err(Error::Corrupted);
        }

        let key = &data[NODE_HEADER_SIZE..NODE_HEADER_SIZE + key_size];
        let value_offset = NODE_HEADER_SIZE + key_size;

        if flags.contains(NodeFlags::BIGDATA) {
            // Overflow: value contains page number
            if data.len() < value_offset + 8 {
                return Err(Error::Corrupted);
            }
            let overflow_pgno = u64::from_le_bytes([
                data[value_offset],
                data[value_offset + 1],
                data[value_offset + 2],
                data[value_offset + 3],
                data[value_offset + 4],
                data[value_offset + 5],
                data[value_offset + 6],
                data[value_offset + 7],
            ]);

            Ok(Self {
                data,
                key,
                value: &[],
                flags,
                extra: overflow_pgno,
            })
        } else {
            // Inline data
            let data_size = ((size_hi << 32) | (lo as u64)) as usize;
            if data.len() < value_offset + data_size {
                return Err(Error::Corrupted);
            }

            let value = &data[value_offset..value_offset + data_size];

            Ok(Self {
                data,
                key,
                value,
                flags,
                extra: data_size as u64,
            })
        }
    }

    /// Returns the key.
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    /// Returns the value (for leaf nodes with inline data).
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Returns the child page number (for branch nodes).
    pub fn child_pgno(&self) -> PageNo {
        self.extra
    }

    /// Returns the overflow page number (for leaf nodes with BIGDATA).
    pub fn overflow_pgno(&self) -> Option<PageNo> {
        if self.flags.contains(NodeFlags::BIGDATA) {
            Some(self.extra)
        } else {
            None
        }
    }

    /// Returns the node flags.
    pub fn flags(&self) -> NodeFlags {
        self.flags
    }

    /// Returns true if this is an overflow (big data) node.
    pub fn is_overflow(&self) -> bool {
        self.flags.contains(NodeFlags::BIGDATA)
    }

    /// Returns true if this node contains a sub-database.
    pub fn is_subdb(&self) -> bool {
        self.flags.contains(NodeFlags::SUBDATA)
    }

    /// Returns true if this node has duplicate data.
    pub fn has_dupdata(&self) -> bool {
        self.flags.contains(NodeFlags::DUPDATA)
    }

    /// Returns the total size of this node in bytes.
    pub fn size(&self) -> usize {
        if self.flags.contains(NodeFlags::BIGDATA) {
            NODE_HEADER_SIZE + self.key.len() + 8
        } else {
            NODE_HEADER_SIZE + self.key.len() + self.value.len()
        }
    }
}

/// A mutable node being constructed.
#[derive(Debug, Clone)]
pub struct Node {
    /// Key data.
    pub key: Vec<u8>,
    /// Value data (for leaf nodes).
    pub value: Vec<u8>,
    /// Node flags.
    pub flags: NodeFlags,
    /// Child page number (for branch nodes) or overflow pgno.
    pub pgno: PageNo,
}

impl Node {
    /// Creates a new branch node.
    pub fn branch(key: Vec<u8>, child_pgno: PageNo) -> Self {
        Self {
            key,
            value: Vec::new(),
            flags: NodeFlags::empty(),
            pgno: child_pgno,
        }
    }

    /// Creates a new leaf node with inline data.
    pub fn leaf(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            flags: NodeFlags::empty(),
            pgno: 0,
        }
    }

    /// Creates a new leaf node with overflow data.
    pub fn leaf_overflow(key: Vec<u8>, overflow_pgno: PageNo) -> Self {
        Self {
            key,
            value: Vec::new(),
            flags: NodeFlags::BIGDATA,
            pgno: overflow_pgno,
        }
    }

    /// Returns the serialized size of this node.
    pub fn size(&self) -> usize {
        if self.flags.contains(NodeFlags::BIGDATA) {
            NODE_HEADER_SIZE + self.key.len() + 8
        } else if self.flags.is_empty() && self.value.is_empty() {
            // Branch node
            6 + self.key.len()
        } else {
            NODE_HEADER_SIZE + self.key.len() + self.value.len()
        }
    }

    /// Writes this node as a branch node to the buffer.
    pub fn write_branch(&self, buf: &mut [u8]) -> Result<usize> {
        let size = 6 + self.key.len();
        if buf.len() < size {
            return Err(Error::PageFull);
        }

        let lo = (self.pgno & 0xFFFF_FFFF) as u32;
        let pgno_hi = ((self.pgno >> 32) & 0x0F) as u16;
        let key_size = self.key.len() as u16;

        if key_size > 0x0FFF {
            return Err(Error::BadValSize);
        }

        let hi_and_ksize = (pgno_hi << 12) | key_size;

        buf[0..4].copy_from_slice(&lo.to_le_bytes());
        buf[4..6].copy_from_slice(&hi_and_ksize.to_le_bytes());
        buf[6..6 + self.key.len()].copy_from_slice(&self.key);

        Ok(size)
    }

    /// Writes this node as a leaf node to the buffer.
    pub fn write_leaf(&self, buf: &mut [u8]) -> Result<usize> {
        let size = self.size();
        if buf.len() < size {
            return Err(Error::PageFull);
        }

        let key_size = self.key.len() as u16;
        if key_size > 0x0FFF {
            return Err(Error::BadValSize);
        }

        let (lo, size_hi) = if self.flags.contains(NodeFlags::BIGDATA) {
            ((self.pgno & 0xFFFF_FFFF) as u32, ((self.pgno >> 32) & 0x0F) as u16)
        } else {
            let data_size = self.value.len() as u64;
            ((data_size & 0xFFFF_FFFF) as u32, ((data_size >> 32) & 0x0F) as u16)
        };

        let hi_and_ksize = (size_hi << 12) | key_size;

        buf[0..4].copy_from_slice(&lo.to_le_bytes());
        buf[4..6].copy_from_slice(&hi_and_ksize.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.bits().to_le_bytes());
        buf[NODE_HEADER_SIZE..NODE_HEADER_SIZE + self.key.len()].copy_from_slice(&self.key);

        let value_offset = NODE_HEADER_SIZE + self.key.len();
        if self.flags.contains(NodeFlags::BIGDATA) {
            buf[value_offset..value_offset + 8].copy_from_slice(&self.pgno.to_le_bytes());
        } else {
            buf[value_offset..value_offset + self.value.len()].copy_from_slice(&self.value);
        }

        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_node_roundtrip() {
        let node = Node::branch(b"testkey".to_vec(), 12345);
        let mut buf = vec![0u8; 100];
        let size = node.write_branch(&mut buf).unwrap();

        let parsed = NodeRef::parse_branch(&buf[..size]).unwrap();
        assert_eq!(parsed.key(), b"testkey");
        assert_eq!(parsed.child_pgno(), 12345);
    }

    #[test]
    fn leaf_node_roundtrip() {
        let node = Node::leaf(b"key".to_vec(), b"value".to_vec());
        let mut buf = vec![0u8; 100];
        let size = node.write_leaf(&mut buf).unwrap();

        let parsed = NodeRef::parse_leaf(&buf[..size]).unwrap();
        assert_eq!(parsed.key(), b"key");
        assert_eq!(parsed.value(), b"value");
        assert!(!parsed.is_overflow());
    }

    #[test]
    fn overflow_node_roundtrip() {
        let node = Node::leaf_overflow(b"bigkey".to_vec(), 999);
        let mut buf = vec![0u8; 100];
        let size = node.write_leaf(&mut buf).unwrap();

        let parsed = NodeRef::parse_leaf(&buf[..size]).unwrap();
        assert_eq!(parsed.key(), b"bigkey");
        assert!(parsed.is_overflow());
        assert_eq!(parsed.overflow_pgno(), Some(999));
    }
}
