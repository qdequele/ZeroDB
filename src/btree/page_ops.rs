//! Page-level operations for B+tree pages.
//!
//! This module provides abstractions for reading and writing
//! branch and leaf pages.

use crate::error::{Error, Result};
use crate::page::{PageHeader, PageNo, PAGE_HEADER_SIZE};

use super::node::{Node, NodeRef};
use super::{CompareFn, SearchResult};

/// Size of a node pointer (offset within page).
const NODE_PTR_SIZE: usize = 2;

/// A branch page view.
///
/// Branch pages contain keys and child page pointers.
/// They form the internal nodes of the B+tree.
pub struct BranchPage<'a> {
    /// Raw page data.
    data: &'a [u8],
    /// Page header.
    header: PageHeader,
    /// Page size.
    page_size: usize,
}

impl<'a> BranchPage<'a> {
    /// Creates a new branch page view.
    pub fn new(data: &'a [u8], page_size: usize) -> Result<Self> {
        let header = PageHeader::read_from(data)?;
        if !header.flags.is_branch() {
            return Err(Error::Corrupted);
        }
        Ok(Self {
            data,
            header,
            page_size,
        })
    }

    /// Returns the number of keys in this page.
    pub fn num_keys(&self) -> usize {
        self.header.num_keys()
    }

    /// Returns the page number.
    pub fn page_no(&self) -> PageNo {
        self.header.page_no
    }

    /// Returns a node at the given index.
    pub fn node(&self, index: usize) -> Result<NodeRef<'a>> {
        if index >= self.num_keys() {
            return Err(Error::Corrupted);
        }

        let ptr_offset = PAGE_HEADER_SIZE + index * NODE_PTR_SIZE;
        if ptr_offset + NODE_PTR_SIZE > self.data.len() {
            return Err(Error::Corrupted);
        }

        let node_offset = u16::from_le_bytes([
            self.data[ptr_offset],
            self.data[ptr_offset + 1],
        ]) as usize;

        if node_offset >= self.page_size {
            return Err(Error::Corrupted);
        }

        NodeRef::parse_branch(&self.data[node_offset..])
    }

    /// Searches for a key in this page.
    ///
    /// Returns the index of the child to follow.
    pub fn search(&self, key: &[u8], compare: CompareFn) -> Result<usize> {
        let num_keys = self.num_keys();
        if num_keys == 0 {
            return Ok(0);
        }

        // Binary search
        let mut low = 0;
        let mut high = num_keys;

        while low < high {
            let mid = (low + high) / 2;
            let node = self.node(mid)?;

            match compare(key, node.key()) {
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        // For branch pages, we want the child where key would go
        // If low > 0 and key < keys[low], we go to child[low-1]
        Ok(low.saturating_sub(1).min(num_keys.saturating_sub(1)))
    }

    /// Returns the child page number at the given index.
    pub fn child(&self, index: usize) -> Result<PageNo> {
        let node = self.node(index)?;
        Ok(node.child_pgno())
    }
}

/// A leaf page view.
///
/// Leaf pages contain the actual key-value data.
pub struct LeafPage<'a> {
    /// Raw page data.
    data: &'a [u8],
    /// Page header.
    header: PageHeader,
    /// Page size.
    page_size: usize,
}

impl<'a> LeafPage<'a> {
    /// Creates a new leaf page view.
    pub fn new(data: &'a [u8], page_size: usize) -> Result<Self> {
        let header = PageHeader::read_from(data)?;
        if !header.flags.is_leaf() {
            return Err(Error::Corrupted);
        }
        Ok(Self {
            data,
            header,
            page_size,
        })
    }

    /// Returns the number of keys in this page.
    pub fn num_keys(&self) -> usize {
        self.header.num_keys()
    }

    /// Returns the page number.
    pub fn page_no(&self) -> PageNo {
        self.header.page_no
    }

    /// Returns a node at the given index.
    pub fn node(&self, index: usize) -> Result<NodeRef<'a>> {
        if index >= self.num_keys() {
            return Err(Error::Corrupted);
        }

        let ptr_offset = PAGE_HEADER_SIZE + index * NODE_PTR_SIZE;
        if ptr_offset + NODE_PTR_SIZE > self.data.len() {
            return Err(Error::Corrupted);
        }

        let node_offset = u16::from_le_bytes([
            self.data[ptr_offset],
            self.data[ptr_offset + 1],
        ]) as usize;

        if node_offset >= self.page_size {
            return Err(Error::Corrupted);
        }

        NodeRef::parse_leaf(&self.data[node_offset..])
    }

    /// Searches for a key in this page.
    pub fn search(&self, key: &[u8], compare: CompareFn) -> Result<SearchResult> {
        let num_keys = self.num_keys();
        if num_keys == 0 {
            return Ok(SearchResult::NotFound(0));
        }

        // Binary search
        let mut low = 0;
        let mut high = num_keys;

        while low < high {
            let mid = (low + high) / 2;
            let node = self.node(mid)?;

            match compare(key, node.key()) {
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(SearchResult::Found(mid)),
            }
        }

        Ok(SearchResult::NotFound(low))
    }

    /// Returns the key at the given index.
    pub fn key(&self, index: usize) -> Result<&'a [u8]> {
        Ok(self.node(index)?.key())
    }

    /// Returns the value at the given index.
    pub fn value(&self, index: usize) -> Result<&'a [u8]> {
        Ok(self.node(index)?.value())
    }
}

/// A mutable page builder for creating new pages.
pub struct PageBuilder {
    /// Page data buffer.
    data: Vec<u8>,
    /// Page size.
    page_size: usize,
    /// Current lower bound (end of node pointers).
    lower: usize,
    /// Current upper bound (start of node data).
    upper: usize,
    /// Is this a branch page?
    is_branch: bool,
}

impl PageBuilder {
    /// Creates a new branch page builder.
    pub fn new_branch(page_no: PageNo, page_size: usize) -> Self {
        let mut data = vec![0u8; page_size];
        let header = PageHeader::new_branch(page_no, page_size);
        header.write_to(&mut data).unwrap();

        Self {
            data,
            page_size,
            lower: PAGE_HEADER_SIZE,
            upper: page_size,
            is_branch: true,
        }
    }

    /// Creates a new leaf page builder.
    pub fn new_leaf(page_no: PageNo, page_size: usize) -> Self {
        let mut data = vec![0u8; page_size];
        let header = PageHeader::new_leaf(page_no, page_size);
        header.write_to(&mut data).unwrap();

        Self {
            data,
            page_size,
            lower: PAGE_HEADER_SIZE,
            upper: page_size,
            is_branch: false,
        }
    }

    /// Returns the amount of free space remaining.
    pub fn free_space(&self) -> usize {
        if self.upper > self.lower {
            self.upper - self.lower - NODE_PTR_SIZE
        } else {
            0
        }
    }

    /// Returns true if the page can fit another node of the given size.
    pub fn can_fit(&self, node_size: usize) -> bool {
        self.free_space() >= node_size + NODE_PTR_SIZE
    }

    /// Adds a branch node to the page.
    pub fn add_branch(&mut self, node: &Node) -> Result<()> {
        let node_size = 6 + node.key.len();
        if !self.can_fit(node_size) {
            return Err(Error::PageFull);
        }

        // Write node data at upper
        self.upper -= node_size;
        node.write_branch(&mut self.data[self.upper..])?;

        // Write node pointer at lower
        let ptr = self.upper as u16;
        self.data[self.lower..self.lower + 2].copy_from_slice(&ptr.to_le_bytes());
        self.lower += NODE_PTR_SIZE;

        // Update header
        self.update_header();

        Ok(())
    }

    /// Adds a leaf node to the page.
    pub fn add_leaf(&mut self, node: &Node) -> Result<()> {
        let node_size = node.size();
        if !self.can_fit(node_size) {
            return Err(Error::PageFull);
        }

        // Write node data at upper
        self.upper -= node_size;
        node.write_leaf(&mut self.data[self.upper..])?;

        // Write node pointer at lower
        let ptr = self.upper as u16;
        self.data[self.lower..self.lower + 2].copy_from_slice(&ptr.to_le_bytes());
        self.lower += NODE_PTR_SIZE;

        // Update header
        self.update_header();

        Ok(())
    }

    /// Adds a node (branch or leaf based on page type).
    pub fn add(&mut self, node: &Node) -> Result<()> {
        if self.is_branch {
            self.add_branch(node)
        } else {
            self.add_leaf(node)
        }
    }

    /// Updates the page header with current bounds.
    fn update_header(&mut self) {
        self.data[12..14].copy_from_slice(&(self.lower as u16).to_le_bytes());
        self.data[14..16].copy_from_slice(&(self.upper as u16).to_le_bytes());
    }

    /// Returns the number of nodes in this page.
    pub fn num_keys(&self) -> usize {
        (self.lower - PAGE_HEADER_SIZE) / NODE_PTR_SIZE
    }

    /// Finalizes and returns the page data.
    pub fn finish(self) -> Vec<u8> {
        self.data
    }

    /// Returns a reference to the page data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_leaf_page() {
        let page_size = 4096;
        let mut builder = PageBuilder::new_leaf(5, page_size);

        // Add some nodes
        builder.add_leaf(&Node::leaf(b"key1".to_vec(), b"value1".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"key2".to_vec(), b"value2".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"key3".to_vec(), b"value3".to_vec())).unwrap();

        let data = builder.finish();

        // Verify we can read it back
        let page = LeafPage::new(&data, page_size).unwrap();
        assert_eq!(page.num_keys(), 3);
        assert_eq!(page.key(0).unwrap(), b"key1");
        assert_eq!(page.value(0).unwrap(), b"value1");
        assert_eq!(page.key(2).unwrap(), b"key3");
    }

    #[test]
    fn build_branch_page() {
        let page_size = 4096;
        let mut builder = PageBuilder::new_branch(10, page_size);

        builder.add_branch(&Node::branch(b"key1".to_vec(), 100)).unwrap();
        builder.add_branch(&Node::branch(b"key2".to_vec(), 200)).unwrap();

        let data = builder.finish();

        let page = BranchPage::new(&data, page_size).unwrap();
        assert_eq!(page.num_keys(), 2);
        assert_eq!(page.child(0).unwrap(), 100);
        assert_eq!(page.child(1).unwrap(), 200);
    }

    #[test]
    fn leaf_page_search() {
        use super::super::default_compare;

        let page_size = 4096;
        let mut builder = PageBuilder::new_leaf(5, page_size);

        builder.add_leaf(&Node::leaf(b"apple".to_vec(), b"1".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"banana".to_vec(), b"2".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"cherry".to_vec(), b"3".to_vec())).unwrap();

        let data = builder.finish();
        let page = LeafPage::new(&data, page_size).unwrap();

        // Search for existing key
        assert!(matches!(page.search(b"banana", default_compare), Ok(SearchResult::Found(1))));

        // Search for non-existing key
        assert!(matches!(page.search(b"blueberry", default_compare), Ok(SearchResult::NotFound(2))));

        // Search before first
        assert!(matches!(page.search(b"aardvark", default_compare), Ok(SearchResult::NotFound(0))));

        // Search after last
        assert!(matches!(page.search(b"zebra", default_compare), Ok(SearchResult::NotFound(3))));
    }
}
