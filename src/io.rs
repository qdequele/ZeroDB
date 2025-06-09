//! I/O operations with io_uring support

use crate::error::{Error, PageId, Result};
use crate::page::{Page, PageHeader, PAGE_SIZE};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    /// Memory map (protected by mutex for resizing)
    pub(crate) mmap: Arc<Mutex<MmapMut>>,
    /// Current file size in bytes
    pub(crate) file_size: AtomicU64,
    /// Page size (usually 4KB)
    page_size: usize,
    /// File path for reopening on resize
    #[allow(dead_code)]
    path: std::path::PathBuf,
}

impl MmapBackend {
    /// Create a new mmap backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_options(path, 10 * 1024 * 1024) // Default 10MB
    }

    /// Create with initial size
    pub fn with_options(path: impl AsRef<Path>, initial_size: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
            mmap: Arc::new(Mutex::new(mmap)),
            file_size: AtomicU64::new(file_size),
            page_size,
            path,
        })
    }

    /// Advise the kernel about memory access patterns
    #[cfg(unix)]
    pub fn madvise(&self, advice: MadviseAdvice) -> Result<()> {
        let mmap = self.mmap.lock().unwrap();
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
        let offset = start_page.0 as usize * self.page_size;
        let len = num_pages * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + len > size {
            return Err(Error::InvalidPageId(start_page));
        }

        #[cfg(unix)]
        {
            let mmap = self.mmap.lock().unwrap();
            let ptr = unsafe { mmap.as_ptr().add(offset) } as *mut libc::c_void;

            // Use madvise WILLNEED to prefetch
            let result = unsafe { libc::madvise(ptr, len, libc::MADV_WILLNEED) };

            if result != 0 {
                return Err(Error::Io(std::io::Error::last_os_error().to_string()));
            }
        }

        Ok(())
    }

    /// Get a raw pointer to the mmap base
    ///
    /// # Safety
    /// The caller must ensure proper synchronization when accessing the memory
    #[inline]
    pub(crate) unsafe fn mmap_ptr(&self) -> *const u8 {
        // We need to get the pointer without holding the lock
        // This is safe because the mmap base address doesn't change until grow() is called
        // and grow() requires exclusive access
        let mmap = self.mmap.lock().unwrap();
        mmap.as_ptr()
    }

    /// Get a mutable slice of the memory map for a page
    #[allow(dead_code)]
    fn get_page_slice_mut(&self, page_id: PageId) -> Result<&mut [u8]> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        // This is safe because we have exclusive access through the mmap mutex
        let mut mmap = self.mmap.lock().unwrap();
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

        let mmap = self.mmap.lock().unwrap();

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
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        // Get the base pointer without holding the lock
        // This is safe because:
        // 1. The mmap base address is stable until grow() is called
        // 2. grow() requires exclusive access (write transaction)
        // 3. Read transactions cannot call grow()
        let base_ptr = unsafe { self.mmap_ptr() };
        let page_ptr = unsafe { base_ptr.add(offset) } as *const Page;

        // Return a reference with the requested lifetime
        // The caller is responsible for ensuring this lifetime is valid
        Ok(unsafe { &*page_ptr })
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        let page_id = PageId(page.header.pgno);
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;

        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }

        let mut mmap = self.mmap.lock().unwrap();

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
        let mmap = self.mmap.lock().unwrap();
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

        // Grow the file
        self.file.set_len(new_size_bytes).map_err(|e| Error::Io(e.to_string()))?;

        // Remap
        let mut mmap_guard = self.mmap.lock().unwrap();

        // Create new mmap
        let new_mmap = unsafe {
            MmapOptions::new()
                .len(new_size_bytes as usize)
                .map_mut(&self.file)
                .map_err(|e| Error::Io(e.to_string()))?
        };

        // Replace the old mmap
        *mmap_guard = new_mmap;

        // Update size
        self.file_size.store(new_size_bytes, Ordering::Release);

        Ok(())
    }
}

/// Zero-copy page access for reading
pub struct PageRef<'a> {
    data: &'a [u8],
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

/// io_uring backend for Linux
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub struct IoUringBackend {
    /// The underlying io_uring backend
    inner: crate::io_uring_parallel::ParallelIoUringBackend,
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl IoUringBackend {
    /// Create a new io_uring backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        // Open the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .map_err(|e| Error::Io(e.to_string()))?;
        
        // Get current file size
        let metadata = file.metadata().map_err(|e| Error::Io(e.to_string()))?;
        let mut file_size = metadata.len();
        
        // Ensure minimum size
        let min_size = PAGE_SIZE as u64 * 4; // At least 4 pages (2 meta + 2 data)
        if file_size < min_size {
            file_size = min_size;
            file.set_len(file_size).map_err(|e| Error::Io(e.to_string()))?;
        }
        
        // Create io_uring configuration with optimal settings
        let config = crate::io_uring_parallel::IoUringConfig {
            sq_entries: 1024,      // Larger submission queue for better batching
            concurrent_ops: 256,   // Allow many concurrent operations
            kernel_poll: true,     // Enable kernel polling for lower latency
            sq_poll: false,        // Disable SQ polling for now (requires CAP_SYS_NICE)
        };
        
        // Create the io_uring backend
        let inner = crate::io_uring_parallel::ParallelIoUringBackend::new(file, config)?;
        
        Ok(Self { inner })
    }
    
    /// Create with custom io_uring configuration
    pub fn with_config(path: impl AsRef<Path>, config: crate::io_uring_parallel::IoUringConfig) -> Result<Self> {
        // Open the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .map_err(|e| Error::Io(e.to_string()))?;
        
        // Get current file size
        let metadata = file.metadata().map_err(|e| Error::Io(e.to_string()))?;
        let mut file_size = metadata.len();
        
        // Ensure minimum size
        let min_size = PAGE_SIZE as u64 * 4;
        if file_size < min_size {
            file_size = min_size;
            file.set_len(file_size).map_err(|e| Error::Io(e.to_string()))?;
        }
        
        // Create the io_uring backend
        let inner = crate::io_uring_parallel::ParallelIoUringBackend::new(file, config)?;
        
        Ok(Self { inner })
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl IoBackend for IoUringBackend {
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>> {
        self.inner.read_page(page_id)
    }
    
    unsafe fn get_page_ref<'a>(&self, page_id: PageId) -> Result<&'a Page> {
        // io_uring doesn't support zero-copy page references
        // because pages are not memory-mapped
        Err(Error::Custom("Zero-copy not supported by io_uring backend".into()))
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        self.inner.write_page(page)
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn size_in_pages(&self) -> u64 {
        self.inner.size_in_pages()
    }

    fn grow(&self, new_size: u64) -> Result<()> {
        self.inner.grow(new_size)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Backend type selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    /// Memory-mapped I/O (default)
    Mmap,
    /// io_uring (Linux only, requires kernel 5.1+)
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
}

impl Default for BackendType {
    fn default() -> Self {
        // Default to mmap for compatibility
        BackendType::Mmap
    }
}

/// Create an I/O backend based on the specified type
pub fn create_backend(path: impl AsRef<Path>, backend_type: BackendType) -> Result<Box<dyn IoBackend>> {
    match backend_type {
        BackendType::Mmap => {
            Ok(Box::new(MmapBackend::new(path)?))
        }
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        BackendType::IoUring => {
            match IoUringBackend::new(path) {
                Ok(backend) => Ok(Box::new(backend)),
                Err(e) => {
                    // Fall back to mmap if io_uring fails
                    eprintln!("Failed to create io_uring backend: {}. Falling back to mmap.", e);
                    Ok(Box::new(MmapBackend::new(path)?))
                }
            }
        }
    }
}

/// Create an I/O backend with automatic selection based on platform
pub fn create_auto_backend(path: impl AsRef<Path>) -> Result<Box<dyn IoBackend>> {
    // Try io_uring first on Linux if available
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        if let Ok(backend) = IoUringBackend::new(&path) {
            return Ok(Box::new(backend));
        }
    }
    
    // Fall back to mmap
    Ok(Box::new(MmapBackend::new(path)?))
}
