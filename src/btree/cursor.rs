//! B+tree cursor for navigating through the tree.
//!
//! The cursor maintains position state and provides forward/backward traversal.

use std::collections::HashMap;

use crate::error::Result;
use crate::page::PageNo;

use super::page_ops::{BranchPage, LeafPage};
use super::{default_compare, CompareFn, SearchResult, P_INVALID};

/// Prefetches memory into CPU cache.
///
/// This is a hint to the processor that the memory at the given pointer
/// will be accessed soon. This can improve performance for sequential scans.
#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    {
        // Use SSE prefetch instruction
        // _MM_HINT_T0 = prefetch into all cache levels
        unsafe {
            std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "x86")]
    {
        unsafe {
            std::arch::x86::_mm_prefetch(ptr as *const i8, std::arch::x86::_MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // ARM64 prefetch using inline assembly
        // PRFM PLDL1KEEP - prefetch for load, L1 cache, keep in cache
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "x86", target_arch = "aarch64")))]
    {
        // No-op for unsupported architectures
        let _ = ptr;
    }
}

/// Prefetches a memory range for reading.
///
/// This prefetches data in cache-line sized chunks (typically 64 bytes).
#[inline]
pub fn prefetch_range(data: &[u8], len: usize) {
    const CACHE_LINE_SIZE: usize = 64;
    let len = len.min(data.len());
    let mut offset = 0;
    while offset < len {
        prefetch_read(unsafe { data.as_ptr().add(offset) });
        offset += CACHE_LINE_SIZE;
    }
}

/// Maximum number of pages to cache in cursor.
const CURSOR_CACHE_SIZE: usize = 16;

/// A single level in the cursor stack.
#[derive(Debug, Clone)]
pub struct CursorLevel {
    /// Page number at this level.
    pub page_no: PageNo,
    /// Current key index within the page.
    pub index: usize,
}

/// Page cache for cursor operations.
///
/// Caches recently accessed pages to avoid repeated reads during traversal.
#[derive(Debug, Clone, Default)]
pub struct PageCache {
    /// Cached pages by page number.
    pages: HashMap<PageNo, Vec<u8>>,
    /// Access order for LRU eviction.
    access_order: Vec<PageNo>,
    /// Maximum cache size.
    capacity: usize,
}

impl PageCache {
    /// Creates a new page cache with default capacity.
    pub fn new() -> Self {
        Self {
            pages: HashMap::with_capacity(CURSOR_CACHE_SIZE),
            access_order: Vec::with_capacity(CURSOR_CACHE_SIZE),
            capacity: CURSOR_CACHE_SIZE,
        }
    }

    /// Creates a new page cache with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pages: HashMap::with_capacity(capacity),
            access_order: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Gets a page from the cache.
    #[inline]
    pub fn get(&mut self, pgno: PageNo) -> Option<&[u8]> {
        if self.pages.contains_key(&pgno) {
            // Move to end of access order (most recently used)
            if let Some(pos) = self.access_order.iter().position(|&p| p == pgno) {
                self.access_order.remove(pos);
                self.access_order.push(pgno);
            }
            self.pages.get(&pgno).map(|v| v.as_slice())
        } else {
            None
        }
    }

    /// Inserts a page into the cache.
    #[inline]
    pub fn insert(&mut self, pgno: PageNo, data: Vec<u8>) {
        // Evict if at capacity
        while self.pages.len() >= self.capacity && !self.access_order.is_empty() {
            let evict = self.access_order.remove(0);
            self.pages.remove(&evict);
        }

        self.pages.insert(pgno, data);
        self.access_order.push(pgno);
    }

    /// Clears the cache.
    pub fn clear(&mut self) {
        self.pages.clear();
        self.access_order.clear();
    }

    /// Returns the number of cached pages.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
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
    /// Page cache for avoiding repeated reads.
    pub cache: PageCache,
}

impl CursorState {
    /// Creates a new cursor state for the given root.
    pub fn new(root: PageNo) -> Self {
        Self {
            root,
            stack: Vec::new(),
            valid: false,
            compare: default_compare,
            cache: PageCache::new(),
        }
    }

    /// Creates a new cursor with a custom comparison function.
    pub fn with_compare(root: PageNo, compare: CompareFn) -> Self {
        Self {
            root,
            stack: Vec::new(),
            valid: false,
            compare,
            cache: PageCache::new(),
        }
    }

    /// Creates a new cursor with a custom cache capacity.
    pub fn with_cache_capacity(root: PageNo, capacity: usize) -> Self {
        Self {
            root,
            stack: Vec::new(),
            valid: false,
            compare: default_compare,
            cache: PageCache::with_capacity(capacity),
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

    /// Clears the cursor state (keeps cache).
    pub fn clear(&mut self) {
        self.stack.clear();
        self.valid = false;
    }

    /// Clears cursor state and cache.
    pub fn clear_all(&mut self) {
        self.stack.clear();
        self.valid = false;
        self.cache.clear();
    }

    /// Gets a page from cache or fetches it.
    #[inline]
    pub fn get_page_cached(
        &mut self,
        pgno: PageNo,
        get_page: &impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<&[u8]> {
        // Check cache first
        if self.cache.pages.contains_key(&pgno) {
            // Update LRU order
            if let Some(pos) = self.cache.access_order.iter().position(|&p| p == pgno) {
                self.cache.access_order.remove(pos);
                self.cache.access_order.push(pgno);
            }
            return Ok(self.cache.pages.get(&pgno).unwrap().as_slice());
        }

        // Fetch and cache
        let data = get_page(pgno)?;
        self.cache.insert(pgno, data);
        Ok(self.cache.pages.get(&pgno).unwrap().as_slice())
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

    /// Searches for a key with page caching.
    ///
    /// Uses the cursor's internal cache to avoid repeated page reads.
    #[inline]
    pub fn search_cached(
        state: &mut CursorState,
        key: &[u8],
        page_size: usize,
        get_page: &impl Fn(PageNo) -> Result<Vec<u8>>,
    ) -> Result<SearchResult> {
        state.clear();

        if state.root == P_INVALID {
            return Ok(SearchResult::NotFound(0));
        }

        let mut pgno = state.root;
        let compare = state.compare; // Copy compare function before borrowing

        // Navigate down to the leaf using cached pages
        loop {
            // Fetch and cache the page
            if !state.cache.pages.contains_key(&pgno) {
                let data = get_page(pgno)?;
                state.cache.insert(pgno, data);
            }
            let page_data = state.cache.pages.get(&pgno).unwrap();

            if is_leaf(page_data, page_size)? {
                let page = LeafPage::new(page_data, page_size)?;
                let result = page.search(key, compare)?;
                let index = result.index();
                let num_keys = page.num_keys();

                state.stack.push(CursorLevel { page_no: pgno, index });
                state.valid = result.is_found() || index < num_keys;

                return Ok(result);
            } else {
                let page = BranchPage::new(page_data, page_size)?;
                let index = page.search(key, compare)?;
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
            // Prefetch ahead in the current page for sequential access
            let next_idx = state.stack[leaf_level].index;
            if next_idx + 1 < page.num_keys() {
                // Prefetch the next node we'll access
                if let Ok(node) = page.node(next_idx + 1) {
                    prefetch_read(node.key().as_ptr());
                }
            }
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
    use crate::error::Error;
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
