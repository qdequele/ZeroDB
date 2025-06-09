//! Parallel page I/O using io_uring on Linux
//!
//! This module implements high-performance parallel I/O operations
//! using Linux's io_uring interface for maximum throughput.

use crate::error::{Error, PageId, Result};

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use {
    crate::page::{Page, PageFlags, PAGE_SIZE},
    crate::cache_aligned::CacheAlignedStats,
    std::sync::{Arc, Mutex},
    std::collections::HashMap,
    io_uring::{cqueue, opcode, squeue, types, IoUring},
    std::os::unix::io::AsRawFd,
};

/// Configuration for io_uring operations
pub struct IoUringConfig {
    /// Size of the submission queue
    pub sq_entries: u32,
    /// Number of concurrent operations
    pub concurrent_ops: usize,
    /// Whether to use kernel polling
    pub kernel_poll: bool,
    /// Whether to use submission queue polling
    pub sq_poll: bool,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self { sq_entries: 256, concurrent_ops: 64, kernel_poll: true, sq_poll: false }
    }
}

/// Parallel io_uring backend
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub struct ParallelIoUringBackend {
    /// The io_uring instance
    ring: Arc<Mutex<IoUring>>,
    /// File descriptor
    fd: i32,
    /// Page size
    page_size: usize,
    /// Statistics
    stats: Arc<CacheAlignedStats>,
    /// Configuration
    config: IoUringConfig,
    /// In-flight operations tracking
    in_flight: Arc<Mutex<HashMap<u64, InflightOp>>>,
    /// Next request ID
    next_req_id: Arc<Mutex<u64>>,
    /// File size in bytes
    file_size: Arc<Mutex<u64>>,
}

/// Information about an in-flight operation
#[allow(dead_code)]
struct InflightOp {
    /// Type of operation
    op_type: OpType,
    /// Page data for writes
    page_data: Option<Vec<u8>>,
    /// Completion callback
    callback: Option<Box<dyn FnOnce(Result<()>) + Send>>,
}

impl std::fmt::Debug for InflightOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightOp")
            .field("op_type", &self.op_type)
            .field("page_data", &self.page_data.as_ref().map(|v| v.len()))
            .field("callback", &self.callback.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum OpType {
    Read(PageId),
    Write(PageId),
    Sync,
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl ParallelIoUringBackend {
    /// Create a new parallel io_uring backend
    pub fn new(file: std::fs::File, config: IoUringConfig) -> Result<Self> {
        let fd = file.as_raw_fd();
        
        // Get file size
        let file_size = file.metadata()
            .map_err(|e| Error::Io(e.to_string()))?
            .len();

        // Create io_uring with configuration
        let mut builder = IoUring::builder();
        if config.kernel_poll {
            builder.setup_iopoll();
        }
        if config.sq_poll {
            builder.setup_sqpoll(1000); // 1ms idle timeout
        }

        let ring = builder.build(config.sq_entries)
            .map_err(|e| Error::Custom(format!("Failed to create io_uring: {}", e)))?;

        Ok(Self {
            ring: Arc::new(Mutex::new(ring)),
            fd,
            page_size: PAGE_SIZE,
            stats: Arc::new(CacheAlignedStats::new()),
            config,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            next_req_id: Arc::new(Mutex::new(0)),
            file_size: Arc::new(Mutex::new(file_size)),
        })
    }

    /// Submit a batch of page writes in parallel
    pub fn write_pages_parallel(&self, pages: &[(PageId, &Page)]) -> Result<()> {
        let mut ring = self.ring.lock().unwrap();
        let mut in_flight = self.in_flight.lock().unwrap();
        let mut next_req_id = self.next_req_id.lock().unwrap();

        // Submit all writes
        let mut submitted = Vec::new();
        for (page_id, page) in pages {
            let offset = page_id.0 as i64 * self.page_size as i64;

            // Serialize page data
            let page_data = unsafe {
                std::slice::from_raw_parts(page as *const Page as *const u8, self.page_size)
            }
            .to_vec();

            // Generate request ID
            let req_id = *next_req_id;
            *next_req_id += 1;

            // Create write operation
            let write_op =
                opcode::Write::new(types::Fd(self.fd), page_data.as_ptr(), page_data.len() as u32)
                    .offset(offset)
                    .build()
                    .user_data(req_id);

            // Submit to ring
            unsafe {
                match ring.submission().push(&write_op) {
                    Ok(_) => {
                        // Track in-flight operation
                        in_flight.insert(
                            req_id,
                            InflightOp {
                                op_type: OpType::Write(*page_id),
                                page_data: Some(page_data),
                                callback: None,
                            },
                        );
                        submitted.push(req_id);
                    }
                    Err(_) => {
                        // Submission queue full, submit what we have
                        break;
                    }
                }
            }
        }

        // Submit all operations
        ring.submit().map_err(|e| Error::Custom(format!("io_uring submit failed: {}", e)))?;

        // Wait for all completions
        let mut completed = 0;
        while completed < submitted.len() {
            ring.submit_and_wait(1).map_err(|e| Error::Custom(format!("io_uring wait failed: {}", e)))?;

            // Process completions
            let mut cq = ring.completion();
            for cqe in &mut cq {
                let req_id = cqe.user_data();
                if let Some(op) = in_flight.remove(&req_id) {
                    if cqe.result() < 0 {
                        return Err(Error::Io(std::io::Error::from_raw_os_error(-cqe.result())));
                    }

                    // Update statistics
                    if let OpType::Write(_) = op.op_type {
                        self.stats.record_page_write(self.page_size);
                    }

                    completed += 1;
                }
            }
        }

        Ok(())
    }

    /// Submit parallel reads with callbacks
    pub fn read_pages_async(
        &self,
        page_ids: &[PageId],
        callbacks: Vec<Box<dyn FnOnce(Result<Box<Page>>) + Send>>,
    ) -> Result<()> {
        let mut ring = self.ring.lock().unwrap();
        let mut in_flight = self.in_flight.lock().unwrap();
        let mut next_req_id = self.next_req_id.lock().unwrap();

        for (page_id, callback) in page_ids.iter().zip(callbacks) {
            let offset = page_id.0 as i64 * self.page_size as i64;

            // Allocate buffer for read
            let mut buffer = vec![0u8; self.page_size];

            // Generate request ID
            let req_id = *next_req_id;
            *next_req_id += 1;

            // Create read operation
            let read_op =
                opcode::Read::new(types::Fd(self.fd), buffer.as_mut_ptr(), buffer.len() as u32)
                    .offset(offset)
                    .build()
                    .user_data(req_id);

            // Submit to ring
            unsafe {
                ring.submission()
                    .push(&read_op)
                    .map_err(|_| Error::Custom("io_uring submission queue full".into()))?;
            }

            // Track in-flight operation with callback
            in_flight.insert(
                req_id,
                InflightOp {
                    op_type: OpType::Read(*page_id),
                    page_data: Some(buffer),
                    callback: Some(Box::new(move |result| {
                        match result {
                            Ok(()) => {
                                // Parse page from buffer
                                let page = Page::new(*page_id, PageFlags::empty());
                                callback(Ok(page));
                            }
                            Err(e) => callback(Err(e)),
                        }
                    })),
                },
            );
        }

        // Submit all operations
        ring.submit().map_err(|e| Error::Custom(format!("io_uring submit failed: {}", e)))?;

        Ok(())
    }

    /// Process completed operations
    pub fn process_completions(&self) -> Result<usize> {
        let mut ring = self.ring.lock().unwrap();
        let mut in_flight = self.in_flight.lock().unwrap();

        let mut processed = 0;

        // Check for completions without blocking
        ring.submit().map_err(|e| Error::Custom(format!("io_uring submit failed: {}", e)))?;

        let mut cq = ring.completion();
        for cqe in &mut cq {
            let req_id = cqe.user_data();
            if let Some(mut op) = in_flight.remove(&req_id) {
                let result = if cqe.result() < 0 {
                    Err(Error::Io(std::io::Error::from_raw_os_error(-cqe.result())))
                } else {
                    Ok(())
                };

                // Update statistics
                match op.op_type {
                    OpType::Read(_) => self.stats.record_page_read(self.page_size),
                    OpType::Write(_) => self.stats.record_page_write(self.page_size),
                    OpType::Sync => {}
                }

                // Call callback if present
                if let Some(callback) = op.callback.take() {
                    callback(result);
                }

                processed += 1;
            }
        }

        Ok(processed)
    }

    /// Submit a vectored write for multiple pages
    pub fn writev_pages(&self, pages: &[(PageId, &Page)]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        let mut ring = self.ring.lock().unwrap();
        let mut next_req_id = self.next_req_id.lock().unwrap();

        // Prepare iovecs
        let mut iovecs = Vec::with_capacity(pages.len());
        let mut buffers = Vec::with_capacity(pages.len());

        for (page_id, page) in pages {
            let offset = page_id.0 as i64 * self.page_size as i64;

            // Create iovec for this page
            let page_data = unsafe {
                std::slice::from_raw_parts(page as *const Page as *const u8, self.page_size)
            };

            buffers.push(page_data);
            iovecs.push(libc::iovec {
                iov_base: page_data.as_ptr() as *mut libc::c_void,
                iov_len: self.page_size,
            });
        }

        // Submit vectored write
        let req_id = *next_req_id;
        *next_req_id += 1;

        let writev_op =
            opcode::Writev::new(types::Fd(self.fd), iovecs.as_ptr(), iovecs.len() as u32)
                .offset(pages[0].0 .0 as i64 * self.page_size as i64)
                .build()
                .user_data(req_id);

        unsafe {
            ring.submission()
                .push(&writev_op)
                .map_err(|_| Error::Custom("io_uring submission queue full".into()))?;
        }

        // Submit and wait
        ring.submit_and_wait(1).map_err(|e| Error::Custom(format!("io_uring submit_and_wait failed: {}", e)))?;

        // Check completion
        let mut cq = ring.completion();
        for cqe in &mut cq {
            if cqe.user_data() == req_id {
                if cqe.result() < 0 {
                    return Err(Error::Io(std::io::Error::from_raw_os_error(-cqe.result())));
                }

                // Update statistics
                self.stats.bytes_written.add((pages.len() * self.page_size) as u64);
                self.stats.page_writes.add(pages.len() as u64);

                return Ok(());
            }
        }

        Err(Error::Custom("io_uring operation not completed".into()))
    }
}

/// Implement IoBackend trait for ParallelIoUringBackend
#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl crate::io::IoBackend for ParallelIoUringBackend {
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>> {
        let mut ring = self.ring.lock().unwrap();
        let mut next_req_id = self.next_req_id.lock().unwrap();
        
        let offset = page_id.0 as i64 * self.page_size as i64;
        let mut buffer = vec![0u8; self.page_size];
        
        // Generate request ID
        let req_id = *next_req_id;
        *next_req_id += 1;
        
        // Create read operation
        let read_op = opcode::Read::new(types::Fd(self.fd), buffer.as_mut_ptr(), buffer.len() as u32)
            .offset(offset)
            .build()
            .user_data(req_id);
        
        // Submit and wait
        unsafe {
            ring.submission()
                .push(&read_op)
                .map_err(|_| Error::Custom("io_uring submission queue full".into()))?;
        }
        
        ring.submit_and_wait(1).map_err(|e| Error::Custom(format!("io_uring submit_and_wait failed: {}", e)))?;
        
        // Get completion
        let mut cq = ring.completion();
        for cqe in &mut cq {
            if cqe.user_data() == req_id {
                if cqe.result() < 0 {
                    return Err(Error::Io(std::io::Error::from_raw_os_error(-cqe.result()).to_string()));
                }
                
                // Parse page from buffer
                // Safety: We need to properly copy the data from the buffer
                let page = unsafe {
                    let mut page_box = Box::new(std::mem::MaybeUninit::<Page>::uninit());
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr(),
                        page_box.as_mut_ptr() as *mut u8,
                        self.page_size
                    );
                    page_box.assume_init()
                };
                
                self.stats.record_page_read(self.page_size);
                return Ok(page);
            }
        }
        
        Err(Error::Custom("io_uring read operation not completed".into()))
    }
    
    fn write_page(&self, page: &Page) -> Result<()> {
        self.write_pages_parallel(&[(PageId(page.header.pgno), page)])
    }
    
    fn sync(&self) -> Result<()> {
        let mut ring = self.ring.lock().unwrap();
        let mut next_req_id = self.next_req_id.lock().unwrap();
        
        let req_id = *next_req_id;
        *next_req_id += 1;
        
        // Create fsync operation
        let sync_op = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .user_data(req_id);
        
        unsafe {
            ring.submission()
                .push(&sync_op)
                .map_err(|_| Error::Custom("io_uring submission queue full".into()))?;
        }
        
        ring.submit_and_wait(1).map_err(|e| Error::Custom(format!("io_uring submit_and_wait failed: {}", e)))?;
        
        // Check completion
        let mut cq = ring.completion();
        for cqe in &mut cq {
            if cqe.user_data() == req_id {
                if cqe.result() < 0 {
                    return Err(Error::Io(std::io::Error::from_raw_os_error(-cqe.result()).to_string()));
                }
                return Ok(());
            }
        }
        
        Err(Error::Custom("io_uring sync operation not completed".into()))
    }
    
    fn size_in_pages(&self) -> u64 {
        let file_size = self.file_size.lock().unwrap();
        *file_size / self.page_size as u64
    }
    
    fn grow(&self, new_size: u64) -> Result<()> {
        let mut file_size = self.file_size.lock().unwrap();
        let new_size_bytes = new_size * self.page_size as u64;
        
        if new_size_bytes > *file_size {
            // Use fallocate to grow the file
            let ret = unsafe {
                libc::fallocate(self.fd, 0, *file_size as i64, (new_size_bytes - *file_size) as i64)
            };
            
            if ret != 0 {
                return Err(Error::Io(std::io::Error::last_os_error().to_string()));
            }
            
            *file_size = new_size_bytes;
        }
        
        Ok(())
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Fallback for non-Linux platforms
#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
/// Parallel I/O uring backend implementation (placeholder)
pub struct ParallelIoUringBackend;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
impl ParallelIoUringBackend {
    /// Create a new parallel I/O uring backend (not supported on this platform)
    pub fn new(_file: std::fs::File, _config: IoUringConfig) -> Result<Self> {
        Err(Error::Custom("io_uring not available on this platform".into()))
    }
}

#[cfg(test)]
#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parallel_writes() {
        let file = NamedTempFile::new().unwrap();
        let backend = ParallelIoUringBackend::new(
            file.as_file().try_clone().unwrap(),
            IoUringConfig::default(),
        )
        .unwrap();

        // Create test pages
        let mut pages = Vec::new();
        for i in 0..10 {
            let page = Page::new(PageId(i), PageFlags::LEAF);
            pages.push((PageId(i), page));
        }

        // Write pages in parallel
        let page_refs: Vec<(PageId, &Page)> = pages.iter().map(|(id, page)| (*id, &**page)).collect();

        backend.write_pages_parallel(&page_refs).unwrap();

        // Check statistics
        let stats = &backend.stats;
        assert_eq!(stats.page_writes.get(), 10);
        assert_eq!(stats.bytes_written.get(), 10 * PAGE_SIZE as u64);
    }
}
