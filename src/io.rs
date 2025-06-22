//! I/O operations for database file access

use crate::error::{Error, PageId, Result};
use crate::page::{Page, PageHeader, PAGE_SIZE};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::sync::atomic::fence;

/// Memory access advice for madvise
#[derive(Debug, Clone, Copy)]
pub enum MadviseAdvice {
    /// Sequential access pattern
    Sequential,
    /// Random access pattern
    Random,
    /// Pages will be needed soon
    WillNeed,
    /// Pages won't be needed soon
    DontNeed,
}

/// I/O backend trait
pub trait IoBackend: Send + Sync {
    /// Read a page from disk (allocating)
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>>;

    /// Get a zero-copy page reference
    ///
    /// # Safety
    /// The caller must ensure the returned reference is not used beyond the lifetime
    /// of the transaction that owns it.
    unsafe fn get_page_ref<'a>(&self, page_id: PageId) -> Result<&'a Page> {
        // Default implementation falls back to read_page
        // This is inefficient but safe for backends that don't support zero-copy
        let _ = page_id;
        Err(Error::Custom("Zero-copy not supported by this backend".into()))
    }

    /// Write a page to disk
    fn write_page(&self, page: &Page) -> Result<()>;

    /// Sync data to disk
    fn sync(&self) -> Result<()>;

    /// Get the current size in pages
    fn size_in_pages(&self) -> u64;

    /// Grow the file to accommodate more pages
    fn grow(&self, new_size: u64) -> Result<()>;

    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Standard I/O backend using memory mapping
pub struct MmapBackend {
    /// The underlying file
    file: File,
    /// Memory map (protected by RwLock for concurrent reads)
    pub(crate) mmap: Arc<RwLock<MmapMut>>,
    /// Current file size in bytes
    pub(crate) file_size: AtomicU64,
    /// Page size (usually 4KB)
    page_size: usize,
    /// File path for reopening on resize
    #[allow(dead_code)]
    path: std::path::PathBuf,
    /// Generation counter for detecting mmap changes
    generation: AtomicUsize,
}

impl MmapBackend {
    /// Get a safe page reference that holds the read lock
    /// This prevents use-after-free by ensuring the mmap cannot be resized while the page is in use
    pub fn get_page_safe(&self, page_id: PageId) -> Result<PageGuard> {
        // Validate page ID
        let offset = self.validate_page_id(page_id)?;
        
        // Hold the read lock for the lifetime of the page reference
        let guard = self.read_lock()?;
        
        // Get the page pointer
        let page_ptr = unsafe {
            let base = guard.as_ptr();
            base.add(offset) as *const Page
        };
        
        // Return the guard which keeps the lock held
        Ok(PageGuard {
            page: unsafe { &*page_ptr },
            _guard: guard,
        })
    }
    
    /// Create a new mmap backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_options(path, 10 * 1024 * 1024) // Default 10MB
    }
    
    /// Validate that a page ID is within bounds
    #[inline]
    fn validate_page_id(&self, page_id: PageId) -> Result<usize> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;
        
        if offset >= size || offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        Ok(offset)
    }

    /// Create with initial size
    pub fn with_options(path: impl AsRef<Path>, initial_size: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::Io(e.to_string()))?;

        // Get current file size
        let metadata = file.metadata().map_err(|e| Error::Io(e.to_string()))?;
        let mut file_size = metadata.len();

        // Ensure minimum size
        let min_size = PAGE_SIZE as u64 * 4; // At least 4 pages (2 meta + 2 data)
        if file_size < min_size {
            file_size = initial_size.max(min_size);
            file.set_len(file_size).map_err(|e| Error::Io(e.to_string()))?;
        }

        // Ensure size is page-aligned
        let page_size = PAGE_SIZE;
        file_size = (file_size / page_size as u64) * page_size as u64;

        // Create memory map
        let mmap = unsafe {
            MmapOptions::new()
                .len(file_size as usize)
                .map_mut(&file)
                .map_err(|e| Error::Io(e.to_string()))?
        };

        Ok(Self {
            file,
            mmap: Arc::new(RwLock::new(mmap)),
            file_size: AtomicU64::new(file_size),
            page_size,
            path,
            generation: AtomicUsize::new(0),
        })
    }

    /// Helper function to lock the mmap for reading
    fn read_lock(&self) -> Result<std::sync::RwLockReadGuard<'_, MmapMut>> {
        self.mmap.read()
            .map_err(|_| Error::Custom("MmapBackend: RwLock poisoned".into()))
    }
    
    /// Helper function to lock the mmap for writing
    fn write_lock(&self) -> Result<std::sync::RwLockWriteGuard<'_, MmapMut>> {
        self.mmap.write()
            .map_err(|_| Error::Custom("MmapBackend: RwLock poisoned".into()))
    }

    /// Advise the kernel about memory access patterns
    #[cfg(unix)]
    pub fn madvise(&self, advice: MadviseAdvice) -> Result<()> {
        let mmap = self.read_lock()?;
        let ptr = mmap.as_ptr() as *mut libc::c_void;
        let len = self.file_size.load(Ordering::Acquire) as usize;

        let advice_flag = match advice {
            MadviseAdvice::Sequential => libc::MADV_SEQUENTIAL,
            MadviseAdvice::Random => libc::MADV_RANDOM,
            MadviseAdvice::WillNeed => libc::MADV_WILLNEED,
            MadviseAdvice::DontNeed => libc::MADV_DONTNEED,
        };

        let result = unsafe { libc::madvise(ptr, len, advice_flag) };

        if result != 0 {
            return Err(Error::Io(std::io::Error::last_os_error().to_string()));
        }

        Ok(())
    }

    /// Prefetch pages into memory
    pub fn prefetch_pages(&self, start_page: PageId, num_pages: usize) -> Result<()> {
        // Validate start page first
        if num_pages == 0 {
            return Ok(());
        }
        
        let offset = start_page.0 as usize * self.page_size;
        let len = num_pages * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset >= size || offset + len > size {
            return Err(Error::InvalidPageId(start_page));
        }
        
        // Also validate end page
        let end_page = PageId(start_page.0 + num_pages as u64 - 1);
        if end_page.0 >= self.size_in_pages() {
            return Err(Error::InvalidPageId(end_page));
        }

        #[cfg(unix)]
        {
            let mmap = self.read_lock()?;
            let ptr = unsafe { mmap.as_ptr().add(offset) } as *mut libc::c_void;

            // Use madvise WILLNEED to prefetch
            let result = unsafe { libc::madvise(ptr, len, libc::MADV_WILLNEED) };

            if result != 0 {
                return Err(Error::Io(std::io::Error::last_os_error().to_string()));
            }
        }

        Ok(())
    }

    /// Get a raw pointer to the mmap base along with the current generation
    ///
    /// # Safety
    /// The caller must ensure the pointer is only used while the generation is unchanged
    #[inline]
    #[allow(dead_code)]
    pub(crate) unsafe fn mmap_ptr_with_generation(&self) -> (*const u8, usize) {
        let mmap = self.mmap.read().unwrap_or_else(|poisoned| {
            poisoned.into_inner()
        });
        let ptr = mmap.as_ptr();
        let gen = self.generation.load(Ordering::Acquire);
        // Ensure all reads of generation happen after getting the pointer
        fence(Ordering::Acquire);
        (ptr, gen)
    }

    /// Get a mutable slice of the memory map for a page
    #[allow(dead_code)]
    fn get_page_slice_mut(&self, page_id: PageId) -> Result<&mut [u8]> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        // This is safe because we have exclusive access through the mmap write lock
        let mut mmap = self.write_lock()?;
        let ptr = mmap.as_mut_ptr();

        unsafe { Ok(std::slice::from_raw_parts_mut(ptr.add(offset), self.page_size)) }
    }
}

impl IoBackend for MmapBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        let mmap = self.read_lock()?;

        // Create a boxed page to hold the data
        let mut page = Page::new(page_id, crate::page::PageFlags::empty());

        // Copy the entire page data
        let src = &mmap[offset..offset + self.page_size];
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                page.as_mut() as *mut Page as *mut u8,
                self.page_size,
            );
        }

        Ok(page)
    }

    #[inline]
    unsafe fn get_page_ref<'a>(&self, page_id: PageId) -> Result<&'a Page> {
        // WARNING: This method has a use-after-free vulnerability!
        // The returned reference can become invalid if grow() is called.
        // Consider using get_page_safe() instead which holds the lock.
        //
        // Validate page ID first
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        // IMPORTANT: This method is fundamentally unsafe because it returns a reference
        // with an arbitrary lifetime that outlives the lock guard. The caller MUST ensure:
        // 1. They hold a read transaction for the entire lifetime 'a
        // 2. No grow() operations happen during lifetime 'a
        // 3. The reference is not used after the transaction ends
        
        // Get the page pointer - we need to hold the read lock briefly
        let mmap = self.read_lock()?;
        let base_ptr = mmap.as_ptr();
        
        // Validate offset again with memory barrier
        fence(Ordering::Acquire);
        let current_size = self.file_size.load(Ordering::Acquire) as usize;
        if offset + self.page_size > current_size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        let page_ptr = unsafe { base_ptr.add(offset) } as *const Page;
        
        // Drop the lock before returning the reference
        drop(mmap);
        
        // Return the reference - caller is responsible for safety
        Ok(unsafe { &*page_ptr })
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        let page_id = PageId(page.header.pgno);
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        let mut mmap = self.write_lock()?;

        // Write the entire page data
        let dst = &mut mmap[offset..offset + self.page_size];
        unsafe {
            std::ptr::copy_nonoverlapping(
                page as *const Page as *const u8,
                dst.as_mut_ptr(),
                self.page_size,
            );
        }

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let mmap = self.read_lock()?;
        mmap.flush().map_err(|e| Error::Io(e.to_string()))?;
        Ok(())
    }

    fn size_in_pages(&self) -> u64 {
        self.file_size.load(Ordering::Acquire) / self.page_size as u64
    }

    fn grow(&self, new_size: u64) -> Result<()> {
        let new_size_bytes = new_size * self.page_size as u64;
        let current_size = self.file_size.load(Ordering::Acquire);

        if new_size_bytes <= current_size {
            return Ok(());
        }

        // Take exclusive write lock for the entire operation
        let mut mmap_guard = self.write_lock()?;
        
        // Double-check size under lock
        let current_size = self.file_size.load(Ordering::Acquire);
        if new_size_bytes <= current_size {
            return Ok(());
        }

        // Grow the file
        self.file.set_len(new_size_bytes).map_err(|e| Error::Io(e.to_string()))?;

        // Create new mmap
        let new_mmap = unsafe {
            MmapOptions::new()
                .len(new_size_bytes as usize)
                .map_mut(&self.file)
                .map_err(|e| Error::Io(e.to_string()))?
        };

        // Update generation counter BEFORE replacing mmap
        // This ensures any concurrent readers will see the change
        self.generation.fetch_add(1, Ordering::AcqRel);
        
        // Memory barrier to ensure generation update is visible
        fence(Ordering::Release);

        // Replace the old mmap
        *mmap_guard = new_mmap;

        // Update size with release ordering
        self.file_size.store(new_size_bytes, Ordering::Release);
        
        // Another memory barrier to ensure all changes are visible
        fence(Ordering::Release);

        Ok(())
    }
}

/// Zero-copy page access for reading
pub struct PageRef<'a> {
    data: &'a [u8],
}

/// Safe page guard that ensures the page reference is valid for its lifetime
/// This prevents use-after-free by tying the page lifetime to the mmap guard
pub struct PageGuard<'a> {
    page: &'a Page,
    _guard: std::sync::RwLockReadGuard<'a, MmapMut>,
}

impl<'a> PageGuard<'a> {
    /// Get the page reference
    pub fn page(&self) -> &'a Page {
        self.page
    }
}

impl<'a> std::ops::Deref for PageGuard<'a> {
    type Target = Page;
    
    fn deref(&self) -> &Self::Target {
        self.page
    }
}

impl<'a> PageRef<'a> {
    /// Create from a memory-mapped region
    pub fn from_mmap(data: &'a [u8]) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Custom("Invalid page size".into()));
        }
        Ok(Self { data })
    }

    /// Get the page header
    pub fn header(&self) -> &PageHeader {
        unsafe { &*(self.data.as_ptr() as *const PageHeader) }
    }

    /// Get page data (excluding header)
    pub fn data(&self) -> &[u8] {
        &self.data[std::mem::size_of::<PageHeader>()..]
    }
}

/// Zero-copy page access for writing
pub struct PageRefMut<'a> {
    data: &'a mut [u8],
}

impl<'a> PageRefMut<'a> {
    /// Create from a memory-mapped region
    pub fn from_mmap(data: &'a mut [u8]) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Custom("Invalid page size".into()));
        }
        Ok(Self { data })
    }

    /// Get the page header
    pub fn header(&self) -> &PageHeader {
        unsafe { &*(self.data.as_ptr() as *const PageHeader) }
    }

    /// Get mutable page header
    pub fn header_mut(&mut self) -> &mut PageHeader {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut PageHeader) }
    }

    /// Get mutable page data (excluding header)
    pub fn data_mut(&mut self) -> &mut [u8] {
        let header_size = std::mem::size_of::<PageHeader>();
        &mut self.data[header_size..]
    }
}

/// File locking for exclusive access
#[cfg(unix)]
pub fn lock_file(file: &File) -> Result<()> {
    use libc::{flock, LOCK_EX, LOCK_NB};
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let result = unsafe { flock(fd, LOCK_EX | LOCK_NB) };

    if result != 0 {
        return Err(Error::Custom("Failed to acquire file lock".into()));
    }

    Ok(())
}

#[cfg(windows)]
pub fn lock_file(file: &File) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    let handle = file.as_raw_handle() as isize;
    let mut overlapped = OVERLAPPED::default();

    let result = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };

    if result == 0 {
        return Err(Error::Custom("Failed to acquire file lock".into()));
    }

    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub fn lock_file(_file: &File) -> Result<()> {
    // No file locking on other platforms
    Ok(())
}


/// Backend type selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum BackendType {
    /// Memory-mapped I/O (default)
    #[default]
    Mmap,
}


/// Create an I/O backend based on the specified type
pub fn create_backend(path: impl AsRef<Path>, backend_type: BackendType) -> Result<Box<dyn IoBackend>> {
    match backend_type {
        BackendType::Mmap => {
            Ok(Box::new(MmapBackend::new(path)?))
        }
    }
}

/// Create an I/O backend with automatic selection based on platform
pub fn create_auto_backend(path: impl AsRef<Path>) -> Result<Box<dyn IoBackend>> {
    // Use mmap backend
    Ok(Box::new(MmapBackend::new(path)?))
}
