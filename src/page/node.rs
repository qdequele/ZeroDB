//! Node structures for B+tree pages.
//!
//! Nodes are the individual key-value entries stored within branch and leaf pages.
//! The format is carefully designed to pack efficiently while maintaining alignment.

use bitflags::bitflags;

use super::PageNo;
use crate::error::{Error, Result};

/// Minimum size of a node header in bytes.
pub const NODE_HEADER_SIZE: usize = 8;

bitflags! {
    /// Flags for leaf node entries.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NodeFlags: u16 {
        /// Data is stored on overflow page(s).
        const BIGDATA = 0x01;
        /// Data is a sub-database.
        const SUBDATA = 0x02;
        /// Data has duplicates (sub-page or sub-tree).
        const DUPDATA = 0x04;
    }
}

/// Branch node - contains key and child page pointer.
///
/// Branch nodes are stored in branch pages and point to child pages.
/// The key is the largest key in the subtree rooted at the child page.
///
/// Layout in memory/disk:
/// ```text
/// Offset  Size  Field
/// 0       4     lo (lower 32 bits of child page number)
/// 4       2     hi_and_ksize ((pgno_hi << 12) | key_size)
/// 6       N     key data
/// ```
///
/// Note: LMDB uses a compact encoding where the high 4 bits of the page number
/// are packed with the key size. This limits key size to 4095 bytes per node
/// and page numbers to 48 bits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchNode {
    /// Child page number.
    pub child_pgno: PageNo,
    /// Key data.
    pub key: Vec<u8>,
}

impl BranchNode {
    /// Creates a new branch node.
    pub fn new(child_pgno: PageNo, key: Vec<u8>) -> Self {
        Self { child_pgno, key }
    }

    /// Returns the size of this node when serialized.
    pub fn size(&self) -> usize {
        6 + self.key.len()
    }

    /// Reads a branch node from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte slice containing the node data
    pub fn read_from(data: &[u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(Error::Corrupted);
        }

        let lo = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let hi_and_ksize = u16::from_le_bytes([data[4], data[5]]);

        // Extract page number high bits and key size
        let pgno_hi = (hi_and_ksize >> 12) as u64;
        let key_size = (hi_and_ksize & 0x0FFF) as usize;

        // Reconstruct full page number
        let child_pgno = (pgno_hi << 32) | (lo as u64);

        if data.len() < 6 + key_size {
            return Err(Error::Corrupted);
        }

        let key = data[6..6 + key_size].to_vec();

        Ok(Self { child_pgno, key })
    }

    /// Writes the branch node to a byte slice.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte slice to write to
    pub fn write_to(&self, data: &mut [u8]) -> Result<()> {
        let size = self.size();
        if data.len() < size {
            return Err(Error::Corrupted);
        }

        let lo = (self.child_pgno & 0xFFFF_FFFF) as u32;
        let pgno_hi = ((self.child_pgno >> 32) & 0x0F) as u16;
        let key_size = self.key.len() as u16;

        if key_size > 0x0FFF {
            return Err(Error::BadValSize);
        }

        let hi_and_ksize = (pgno_hi << 12) | key_size;

        data[0..4].copy_from_slice(&lo.to_le_bytes());
        data[4..6].copy_from_slice(&hi_and_ksize.to_le_bytes());
        data[6..6 + self.key.len()].copy_from_slice(&self.key);

        Ok(())
    }
}

/// Leaf node - contains key and value data.
///
/// Leaf nodes store actual key-value pairs. For small values, the data is
/// stored inline. For large values (overflow), only the overflow page number
/// is stored.
///
/// Layout in memory/disk:
/// ```text
/// Offset  Size  Field
/// 0       4     lo (data size low bits OR overflow page number low bits)
/// 4       2     hi_and_ksize ((size_hi << 12) | key_size)
/// 6       2     flags
/// 8       N     key data
/// 8+N     M     value data (inline) OR overflow pgno (8 bytes if BIGDATA)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
    /// Node flags.
    pub flags: NodeFlags,
    /// Key data.
    pub key: Vec<u8>,
    /// Value data (inline) or empty if overflow.
    pub data: Vec<u8>,
    /// Overflow page number (if BIGDATA flag is set).
    pub overflow_pgno: Option<PageNo>,
}

impl LeafNode {
    /// Creates a new leaf node with inline data.
    pub fn new(key: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            flags: NodeFlags::empty(),
            key,
            data,
            overflow_pgno: None,
        }
    }

    /// Creates a new leaf node with overflow data.
    pub fn new_overflow(key: Vec<u8>, overflow_pgno: PageNo) -> Self {
        // For overflow nodes, we store the data size in the data field temporarily
        // This is used during serialization
        Self {
            flags: NodeFlags::BIGDATA,
            key,
            data: Vec::new(),
            overflow_pgno: Some(overflow_pgno),
        }
    }

    /// Creates a new leaf node for a sub-database.
    pub fn new_subdb(key: Vec<u8>, db_data: Vec<u8>) -> Self {
        Self {
            flags: NodeFlags::SUBDATA,
            key,
            data: db_data,
            overflow_pgno: None,
        }
    }

    /// Returns true if this node's data is stored on overflow pages.
    pub fn is_overflow(&self) -> bool {
        self.flags.contains(NodeFlags::BIGDATA)
    }

    /// Returns true if this node is a sub-database.
    pub fn is_subdb(&self) -> bool {
        self.flags.contains(NodeFlags::SUBDATA)
    }

    /// Returns true if this node has duplicate data.
    pub fn has_duplicates(&self) -> bool {
        self.flags.contains(NodeFlags::DUPDATA)
    }

    /// Returns the size of this node when serialized.
    pub fn size(&self) -> usize {
        let data_size = if self.is_overflow() {
            8 // overflow page number
        } else {
            self.data.len()
        };
        NODE_HEADER_SIZE + self.key.len() + data_size
    }

    /// Reads a leaf node from a byte slice.
    pub fn read_from(data: &[u8]) -> Result<Self> {
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

        let key = data[NODE_HEADER_SIZE..NODE_HEADER_SIZE + key_size].to_vec();
        let value_offset = NODE_HEADER_SIZE + key_size;

        if flags.contains(NodeFlags::BIGDATA) {
            // Overflow: data contains page number
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
                flags,
                key,
                data: Vec::new(),
                overflow_pgno: Some(overflow_pgno),
            })
        } else {
            // Inline data
            let data_size = ((size_hi << 32) | (lo as u64)) as usize;
            if data.len() < value_offset + data_size {
                return Err(Error::Corrupted);
            }

            let value_data = data[value_offset..value_offset + data_size].to_vec();

            Ok(Self {
                flags,
                key,
                data: value_data,
                overflow_pgno: None,
            })
        }
    }

    /// Writes the leaf node to a byte slice.
    pub fn write_to(&self, data: &mut [u8]) -> Result<()> {
        let size = self.size();
        if data.len() < size {
            return Err(Error::Corrupted);
        }

        let key_size = self.key.len() as u16;
        if key_size > 0x0FFF {
            return Err(Error::BadValSize);
        }

        let (lo, size_hi) = if self.is_overflow() {
            // For overflow, lo contains overflow page number low bits
            let pgno = self.overflow_pgno.unwrap_or(0);
            ((pgno & 0xFFFF_FFFF) as u32, ((pgno >> 32) & 0x0F) as u16)
        } else {
            let data_size = self.data.len() as u64;
            (
                (data_size & 0xFFFF_FFFF) as u32,
                ((data_size >> 32) & 0x0F) as u16,
            )
        };

        let hi_and_ksize = (size_hi << 12) | key_size;

        data[0..4].copy_from_slice(&lo.to_le_bytes());
        data[4..6].copy_from_slice(&hi_and_ksize.to_le_bytes());
        data[6..8].copy_from_slice(&self.flags.bits().to_le_bytes());
        data[NODE_HEADER_SIZE..NODE_HEADER_SIZE + self.key.len()].copy_from_slice(&self.key);

        let value_offset = NODE_HEADER_SIZE + self.key.len();
        if self.is_overflow() {
            let pgno = self.overflow_pgno.unwrap_or(0);
            data[value_offset..value_offset + 8].copy_from_slice(&pgno.to_le_bytes());
        } else {
            data[value_offset..value_offset + self.data.len()].copy_from_slice(&self.data);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_node_roundtrip() {
        let node = BranchNode::new(12345, b"test_key".to_vec());

        let mut buf = vec![0u8; node.size()];
        node.write_to(&mut buf).unwrap();

        let recovered = BranchNode::read_from(&buf).unwrap();
        assert_eq!(node, recovered);
    }

    #[test]
    fn branch_node_large_pgno() {
        // Test page number with high bits set
        let pgno: PageNo = 0x0F_1234_5678;
        let node = BranchNode::new(pgno, b"key".to_vec());

        let mut buf = vec![0u8; node.size()];
        node.write_to(&mut buf).unwrap();

        let recovered = BranchNode::read_from(&buf).unwrap();
        assert_eq!(recovered.child_pgno, pgno);
    }

    #[test]
    fn leaf_node_inline_roundtrip() {
        let node = LeafNode::new(b"key".to_vec(), b"value_data".to_vec());

        let mut buf = vec![0u8; node.size()];
        node.write_to(&mut buf).unwrap();

        let recovered = LeafNode::read_from(&buf).unwrap();
        assert_eq!(node.key, recovered.key);
        assert_eq!(node.data, recovered.data);
        assert!(!recovered.is_overflow());
    }

    #[test]
    fn leaf_node_overflow_roundtrip() {
        let node = LeafNode::new_overflow(b"key".to_vec(), 999);
        assert!(node.is_overflow());

        let mut buf = vec![0u8; node.size()];
        node.write_to(&mut buf).unwrap();

        let recovered = LeafNode::read_from(&buf).unwrap();
        assert!(recovered.is_overflow());
        assert_eq!(recovered.overflow_pgno, Some(999));
    }

    #[test]
    fn leaf_node_subdb() {
        let node = LeafNode::new_subdb(b"dbname".to_vec(), vec![0; 48]);
        assert!(node.is_subdb());
    }

    #[test]
    fn node_flags() {
        assert!(!NodeFlags::empty().contains(NodeFlags::BIGDATA));
        assert!(NodeFlags::BIGDATA.contains(NodeFlags::BIGDATA));
        assert!((NodeFlags::BIGDATA | NodeFlags::DUPDATA).contains(NodeFlags::DUPDATA));
    }
}
