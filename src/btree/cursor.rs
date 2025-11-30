//! B+tree cursor for navigating through the tree.
//!
//! The cursor maintains position state and provides forward/backward traversal.

use crate::error::{Error, Result};
use crate::page::PageNo;

use super::page_ops::{BranchPage, LeafPage};
use super::{default_compare, CompareFn, SearchResult, P_INVALID};

/// A single level in the cursor stack.
#[derive(Debug, Clone)]
pub struct CursorLevel {
    /// Page number at this level.
    pub page_no: PageNo,
    /// Current key index within the page.
    pub index: usize,
}

/// Cursor state for navigating a B+tree.
///
/// The cursor tracks a path from root to leaf and current position.
#[derive(Debug, Clone)]
pub struct CursorState {
    /// Root page number.
    pub root: PageNo,
    /// Stack of pages from root to current position.
    /// Last element is the leaf level.
    pub stack: Vec<CursorLevel>,
    /// True if cursor is positioned at a valid entry.
    pub valid: bool,
    /// Comparison function for keys.
    pub compare: CompareFn,
}

impl CursorState {
    /// Creates a new cursor state for the given root.
    pub fn new(root: PageNo) -> Self {
        Self {
            root,
            stack: Vec::new(),
            valid: false,
            compare: default_compare,
        }
    }

    /// Creates a new cursor with a custom comparison function.
    pub fn with_compare(root: PageNo, compare: CompareFn) -> Self {
        Self {
            root,
            stack: Vec::new(),
            valid: false,
            compare,
        }
    }

    /// Returns the current leaf page number.
    pub fn leaf_pgno(&self) -> Option<PageNo> {
        self.stack.last().map(|l| l.page_no)
    }

    /// Returns the current index within the leaf.
    pub fn leaf_index(&self) -> Option<usize> {
        self.stack.last().map(|l| l.index)
    }

    /// Returns true if the cursor is at a valid position.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Clears the cursor state.
    pub fn clear(&mut self) {
        self.stack.clear();
        self.valid = false;
    }
}

/// Cursor operations on page data.
///
/// These functions take page data and cursor state, performing navigation.
pub struct CursorOps;

impl CursorOps {
    /// Searches for a key and positions the cursor.
    ///
    /// If the key is found, the cursor points to it.
    /// If not found, the cursor points to the insert position.
    pub fn search(
        state: &mut CursorState,
        key: &[u8],
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<SearchResult> {
        state.clear();

        if state.root == P_INVALID {
            return Ok(SearchResult::NotFound(0));
        }

        let mut pgno = state.root;

        // Navigate down to the leaf
        loop {
            let page_data = get_page(pgno)?;

            // Check if this is a leaf or branch
            if is_leaf(&page_data, page_size)? {
                let page = LeafPage::new(&page_data, page_size)?;
                let result = page.search(key, state.compare)?;
                let index = result.index();

                state.stack.push(CursorLevel { page_no: pgno, index });
                state.valid = result.is_found() || index < page.num_keys();

                return Ok(result);
            } else {
                let page = BranchPage::new(&page_data, page_size)?;
                let index = page.search(key, state.compare)?;
                let child = page.child(index)?;

                state.stack.push(CursorLevel { page_no: pgno, index });
                pgno = child;
            }
        }
    }

    /// Positions the cursor at the first key.
    pub fn first(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        state.clear();

        if state.root == P_INVALID {
            return Ok(false);
        }

        let mut pgno = state.root;

        // Navigate to leftmost leaf
        loop {
            let page_data = get_page(pgno)?;

            if is_leaf(&page_data, page_size)? {
                let page = LeafPage::new(&page_data, page_size)?;

                if page.num_keys() == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                state.stack.push(CursorLevel { page_no: pgno, index: 0 });
                state.valid = true;
                return Ok(true);
            } else {
                let page = BranchPage::new(&page_data, page_size)?;

                if page.num_keys() == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                let child = page.child(0)?;
                state.stack.push(CursorLevel { page_no: pgno, index: 0 });
                pgno = child;
            }
        }
    }

    /// Positions the cursor at the last key.
    pub fn last(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        state.clear();

        if state.root == P_INVALID {
            return Ok(false);
        }

        let mut pgno = state.root;

        // Navigate to rightmost leaf
        loop {
            let page_data = get_page(pgno)?;

            if is_leaf(&page_data, page_size)? {
                let page = LeafPage::new(&page_data, page_size)?;
                let num_keys = page.num_keys();

                if num_keys == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                state.stack.push(CursorLevel {
                    page_no: pgno,
                    index: num_keys - 1,
                });
                state.valid = true;
                return Ok(true);
            } else {
                let page = BranchPage::new(&page_data, page_size)?;
                let num_keys = page.num_keys();

                if num_keys == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                let child = page.child(num_keys - 1)?;
                state.stack.push(CursorLevel {
                    page_no: pgno,
                    index: num_keys - 1,
                });
                pgno = child;
            }
        }
    }

    /// Moves the cursor to the next entry.
    pub fn next(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        if !state.valid || state.stack.is_empty() {
            return Ok(false);
        }

        // Try to move right in the current leaf
        let leaf_level = state.stack.len() - 1;
        let leaf = &state.stack[leaf_level];
        let page_data = get_page(leaf.page_no)?;
        let page = LeafPage::new(&page_data, page_size)?;

        if leaf.index + 1 < page.num_keys() {
            state.stack[leaf_level].index += 1;
            return Ok(true);
        }

        // Need to go up and right
        Self::move_up_right(state, page_size, get_page)
    }

    /// Moves the cursor to the previous entry.
    pub fn prev(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        if !state.valid || state.stack.is_empty() {
            return Ok(false);
        }

        // Try to move left in the current leaf
        let leaf_level = state.stack.len() - 1;
        let leaf = &state.stack[leaf_level];

        if leaf.index > 0 {
            state.stack[leaf_level].index -= 1;
            return Ok(true);
        }

        // Need to go up and left
        Self::move_up_left(state, page_size, get_page)
    }

    /// Moves up the tree and then right to find the next leaf.
    fn move_up_right(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        // Pop the leaf
        state.stack.pop();

        // Go up until we can go right
        while let Some(level) = state.stack.last_mut() {
            let page_data = get_page(level.page_no)?;
            let page = BranchPage::new(&page_data, page_size)?;

            if level.index + 1 < page.num_keys() {
                level.index += 1;
                let child = page.child(level.index)?;

                // Go down to leftmost leaf
                return Self::descend_left(state, child, page_size, get_page);
            }

            state.stack.pop();
        }

        // No more entries
        state.valid = false;
        Ok(false)
    }

    /// Moves up the tree and then left to find the previous leaf.
    fn move_up_left(
        state: &mut CursorState,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        // Pop the leaf
        state.stack.pop();

        // Go up until we can go left
        while let Some(level) = state.stack.last_mut() {
            if level.index > 0 {
                let page_data = get_page(level.page_no)?;
                let page = BranchPage::new(&page_data, page_size)?;

                level.index -= 1;
                let child = page.child(level.index)?;

                // Go down to rightmost leaf
                return Self::descend_right(state, child, page_size, get_page);
            }

            state.stack.pop();
        }

        // No more entries
        state.valid = false;
        Ok(false)
    }

    /// Descends to the leftmost leaf from the given page.
    fn descend_left(
        state: &mut CursorState,
        mut pgno: PageNo,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        loop {
            let page_data = get_page(pgno)?;

            if is_leaf(&page_data, page_size)? {
                let page = LeafPage::new(&page_data, page_size)?;

                if page.num_keys() == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                state.stack.push(CursorLevel { page_no: pgno, index: 0 });
                state.valid = true;
                return Ok(true);
            } else {
                let page = BranchPage::new(&page_data, page_size)?;
                let child = page.child(0)?;
                state.stack.push(CursorLevel { page_no: pgno, index: 0 });
                pgno = child;
            }
        }
    }

    /// Descends to the rightmost leaf from the given page.
    fn descend_right(
        state: &mut CursorState,
        mut pgno: PageNo,
        page_size: usize,
        get_page: impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        loop {
            let page_data = get_page(pgno)?;

            if is_leaf(&page_data, page_size)? {
                let page = LeafPage::new(&page_data, page_size)?;
                let num_keys = page.num_keys();

                if num_keys == 0 {
                    state.valid = false;
                    return Ok(false);
                }

                state.stack.push(CursorLevel {
                    page_no: pgno,
                    index: num_keys - 1,
                });
                state.valid = true;
                return Ok(true);
            } else {
                let page = BranchPage::new(&page_data, page_size)?;
                let num_keys = page.num_keys();
                let child = page.child(num_keys - 1)?;
                state.stack.push(CursorLevel {
                    page_no: pgno,
                    index: num_keys - 1,
                });
                pgno = child;
            }
        }
    }

    /// Gets the key/value at the current cursor position.
    pub fn get_current<'a>(
        state: &CursorState,
        page_data: &'a [u8],
        page_size: usize,
    ) -> Result<Option<(&'a [u8], &'a [u8])>> {
        if !state.valid {
            return Ok(None);
        }

        let leaf = match state.stack.last() {
            Some(l) => l,
            None => return Ok(None),
        };

        let page = LeafPage::new(page_data, page_size)?;

        if leaf.index >= page.num_keys() {
            return Ok(None);
        }

        let node = page.node(leaf.index)?;
        Ok(Some((node.key(), node.value())))
    }
}

/// Checks if a page is a leaf page.
fn is_leaf(data: &[u8], _page_size: usize) -> Result<bool> {
    use crate::page::PageHeader;
    let header = PageHeader::read_from(data)?;
    Ok(header.flags.is_leaf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::node::Node;
    use super::super::page_ops::PageBuilder;
    use std::collections::HashMap;

    fn create_test_pages() -> (HashMap<PageNo, Vec<u8>>, PageNo) {
        let page_size = 4096;
        let mut pages = HashMap::new();

        // Create a simple tree: one leaf page
        let mut builder = PageBuilder::new_leaf(1, page_size);
        builder.add_leaf(&Node::leaf(b"apple".to_vec(), b"1".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"banana".to_vec(), b"2".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"cherry".to_vec(), b"3".to_vec())).unwrap();
        pages.insert(1, builder.finish());

        (pages, 1)
    }

    #[test]
    fn cursor_search_found() {
        let (pages, root) = create_test_pages();
        let page_size = 4096;
        let mut state = CursorState::new(root);

        let result = CursorOps::search(&mut state, b"banana", page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();

        assert!(matches!(result, SearchResult::Found(1)));
        assert!(state.is_valid());
    }

    #[test]
    fn cursor_search_not_found() {
        let (pages, root) = create_test_pages();
        let page_size = 4096;
        let mut state = CursorState::new(root);

        let result = CursorOps::search(&mut state, b"blueberry", page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();

        assert!(matches!(result, SearchResult::NotFound(2)));
    }

    #[test]
    fn cursor_first_last() {
        let (pages, root) = create_test_pages();
        let page_size = 4096;
        let mut state = CursorState::new(root);

        // First
        let found = CursorOps::first(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(found);
        assert_eq!(state.leaf_index(), Some(0));

        // Last
        let found = CursorOps::last(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(found);
        assert_eq!(state.leaf_index(), Some(2));
    }

    #[test]
    fn cursor_next_prev() {
        let (pages, root) = create_test_pages();
        let page_size = 4096;
        let mut state = CursorState::new(root);

        // Start at first
        CursorOps::first(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert_eq!(state.leaf_index(), Some(0));

        // Next
        let found = CursorOps::next(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(found);
        assert_eq!(state.leaf_index(), Some(1));

        // Next
        let found = CursorOps::next(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(found);
        assert_eq!(state.leaf_index(), Some(2));

        // Next (should fail - end of tree)
        let found = CursorOps::next(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(!found);

        // Prev from last
        CursorOps::last(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        let found = CursorOps::prev(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();
        assert!(found);
        assert_eq!(state.leaf_index(), Some(1));
    }

    #[test]
    fn cursor_get_current() {
        let (pages, root) = create_test_pages();
        let page_size = 4096;
        let mut state = CursorState::new(root);

        CursorOps::first(&mut state, page_size, |pgno| {
            pages.get(&pgno).cloned().ok_or(Error::Corrupted)
        }).unwrap();

        let page_data = pages.get(&1).unwrap();
        let kv = CursorOps::get_current(&state, page_data, page_size).unwrap();
        assert!(kv.is_some());
        let (k, v) = kv.unwrap();
        assert_eq!(k, b"apple");
        assert_eq!(v, b"1");
    }
}
