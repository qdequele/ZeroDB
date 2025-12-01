//! B+tree search operations.

use crate::error::{Error, Result};
use crate::page::{PageHeader, PageNo};

use super::page_ops::{BranchPage, LeafPage};
use super::{CompareFn, SearchResult};

/// Result of searching through the tree.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TreeSearchResult<'a> {
    /// The leaf page containing the key (or where it would be).
    pub leaf_data: &'a [u8],
    /// The page number of the leaf.
    pub leaf_pgno: PageNo,
    /// The search result within the leaf.
    pub result: SearchResult,
    /// Path from root to leaf (page numbers).
    pub path: Vec<PageNo>,
    /// Indices within each page on the path.
    pub indices: Vec<usize>,
}

/// Searches for a key in a page, returning the search result.
///
/// For branch pages, returns the index of the child to follow.
/// For leaf pages, returns Found or NotFound.
pub fn search_page(
    data: &[u8],
    key: &[u8],
    page_size: usize,
    compare: CompareFn,
) -> Result<(bool, SearchResult)> {
    let header = PageHeader::read_from(data)?;

    if header.flags.is_leaf() {
        let page = LeafPage::new(data, page_size)?;
        let result = page.search(key, compare)?;
        Ok((true, result))
    } else if header.flags.is_branch() {
        let page = BranchPage::new(data, page_size)?;
        let index = page.search(key, compare)?;
        Ok((false, SearchResult::NotFound(index)))
    } else {
        Err(Error::Corrupted)
    }
}

/// Determines if a page is a leaf page.
#[allow(dead_code)]
pub fn is_leaf_page(data: &[u8]) -> Result<bool> {
    let header = PageHeader::read_from(data)?;
    Ok(header.flags.is_leaf())
}

/// Determines if a page is a branch page.
#[allow(dead_code)]
pub fn is_branch_page(data: &[u8]) -> Result<bool> {
    let header = PageHeader::read_from(data)?;
    Ok(header.flags.is_branch())
}

/// Gets the child page number from a branch page at the given index.
#[allow(dead_code)]
pub fn get_child_pgno(data: &[u8], index: usize, page_size: usize) -> Result<PageNo> {
    let page = BranchPage::new(data, page_size)?;
    page.child(index)
}

#[cfg(test)]
mod tests {
    use super::super::default_compare;
    use super::super::node::Node;
    use super::super::page_ops::PageBuilder;
    use super::*;

    #[test]
    fn search_leaf_page() {
        let page_size = 4096;
        let mut builder = PageBuilder::new_leaf(1, page_size);
        builder
            .add_leaf(&Node::leaf(b"a".to_vec(), b"1".to_vec()))
            .unwrap();
        builder
            .add_leaf(&Node::leaf(b"c".to_vec(), b"3".to_vec()))
            .unwrap();
        builder
            .add_leaf(&Node::leaf(b"e".to_vec(), b"5".to_vec()))
            .unwrap();
        let data = builder.finish();

        let (is_leaf, result) = search_page(&data, b"c", page_size, default_compare).unwrap();
        assert!(is_leaf);
        assert!(matches!(result, SearchResult::Found(1)));

        let (is_leaf, result) = search_page(&data, b"b", page_size, default_compare).unwrap();
        assert!(is_leaf);
        assert!(matches!(result, SearchResult::NotFound(1)));
    }

    #[test]
    fn search_branch_page() {
        let page_size = 4096;
        let mut builder = PageBuilder::new_branch(1, page_size);
        builder
            .add_branch(&Node::branch(b"m".to_vec(), 10))
            .unwrap();
        builder
            .add_branch(&Node::branch(b"z".to_vec(), 20))
            .unwrap();
        let data = builder.finish();

        let (is_leaf, result) = search_page(&data, b"a", page_size, default_compare).unwrap();
        assert!(!is_leaf);
        // Should return index 0 (first child for keys < "m")
        assert_eq!(result.index(), 0);

        let (is_leaf, _result) = search_page(&data, b"p", page_size, default_compare).unwrap();
        assert!(!is_leaf);
        // Should return index 0 or 1 depending on implementation
    }
}
