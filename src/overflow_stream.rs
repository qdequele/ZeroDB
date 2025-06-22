//! Streaming API for large overflow values
//!
//! This module provides streaming read/write capabilities for values
//! that are too large to fit in memory, supporting values up to 10GB+

use crate::error::{Error, PageId, Result};
use crate::page::{PageFlags, PAGE_SIZE};
use crate::txn::{mode::Mode, Transaction, Write};
use std::io::{self, Read, Write as IoWrite};

/// Streaming reader for overflow values
pub struct OverflowReader<'txn, M: Mode> {
    txn: &'txn Transaction<'txn, M>,
    first_page_id: PageId,
    overflow_count: u32,
    current_page: u32,
    position_in_page: usize,
    data_per_page: usize,
}

impl<'txn, M: Mode> OverflowReader<'txn, M> {
    /// Create a new overflow reader
    pub fn new(txn: &'txn Transaction<'txn, M>, first_page_id: PageId) -> Result<Self> {
        // Get overflow count from first page
        let overflow_count = {
            let page = txn.get_page(first_page_id)?;
            if !page.header.flags.contains(PageFlags::OVERFLOW) {
                return Err(Error::Corruption {
                    details: "Expected overflow page".into(),
                    page_id: Some(first_page_id),
                });
            }
            page.header.overflow
        };

        Ok(Self {
            txn,
            first_page_id,
            overflow_count,
            current_page: 0,
            position_in_page: 0,
            data_per_page: PAGE_SIZE - crate::page::PageHeader::SIZE,
        })
    }

    /// Get the total size by scanning pages (if not stored)
    pub fn size(&self) -> Result<u64> {
        // For now, calculate by scanning - in future could store in metadata
        let mut total_size = 0u64;
        
        for i in 0..self.overflow_count {
            let page_id = PageId(self.first_page_id.0 + i as u64);
            let page = self.txn.get_page(page_id)?;
            
            if i == self.overflow_count - 1 {
                // Last page - scan for actual data
                let mut actual_len = self.data_per_page;
                while actual_len > 0 && page.data[actual_len - 1] == 0 {
                    actual_len -= 1;
                }
                total_size += actual_len as u64;
            } else {
                total_size += self.data_per_page as u64;
            }
        }
        
        Ok(total_size)
    }
}

impl<'txn, M: Mode> Read for OverflowReader<'txn, M> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.current_page >= self.overflow_count {
            return Ok(0); // EOF
        }

        let mut total_read = 0;

        while total_read < buf.len() && self.current_page < self.overflow_count {
            let page_id = PageId(self.first_page_id.0 + self.current_page as u64);
            let page = self.txn.get_page(page_id)
                .map_err(io::Error::other)?;

            // Calculate available data in current page
            let available = if self.current_page == self.overflow_count - 1 {
                // Last page - need to find actual data length
                let mut actual_len = self.data_per_page;
                while actual_len > self.position_in_page && page.data[actual_len - 1] == 0 {
                    actual_len -= 1;
                }
                actual_len.saturating_sub(self.position_in_page)
            } else {
                self.data_per_page - self.position_in_page
            };

            if available == 0 {
                break; // No more data
            }

            let to_read = (buf.len() - total_read).min(available);
            buf[total_read..total_read + to_read]
                .copy_from_slice(&page.data[self.position_in_page..self.position_in_page + to_read]);

            total_read += to_read;
            self.position_in_page += to_read;

            // Move to next page if needed
            if self.position_in_page >= self.data_per_page {
                self.current_page += 1;
                self.position_in_page = 0;
            }
        }

        Ok(total_read)
    }
}

/// Streaming writer for overflow values
pub struct OverflowWriter<'txn> {
    txn: &'txn mut Transaction<'txn, Write>,
    first_page_id: Option<PageId>,
    current_pages: Vec<PageId>,
    position_in_page: usize,
    data_per_page: usize,
    total_written: usize,
}

impl<'txn> OverflowWriter<'txn> {
    /// Create a new overflow writer
    pub fn new(txn: &'txn mut Transaction<'txn, Write>) -> Self {
        Self {
            txn,
            first_page_id: None,
            current_pages: Vec::new(),
            position_in_page: 0,
            data_per_page: PAGE_SIZE - crate::page::PageHeader::SIZE,
            total_written: 0,
        }
    }

    /// Finish writing and return the first page ID and count
    pub fn finish(self) -> Result<(PageId, u32)> {
        match self.first_page_id {
            Some(first_id) => Ok((first_id, self.current_pages.len() as u32)),
            None => Err(Error::InvalidParameter("No data written to overflow writer")),
        }
    }

    /// Allocate a new overflow page
    fn alloc_new_page(&mut self) -> Result<PageId> {
        let page_id = if self.current_pages.is_empty() {
            // First allocation - use consecutive allocation
            let num_pages_estimate = 100; // Start with estimate, can grow
            self.txn.alloc_consecutive_pages(num_pages_estimate, PageFlags::OVERFLOW)?
        } else {
            // Allocate next consecutive page
            let last_id = self.current_pages.last().expect("current_pages should not be empty");
            PageId(last_id.0 + 1)
        };

        // Initialize the page
        let page = self.txn.get_consecutive_page_mut(page_id)?;
        page.header.flags = PageFlags::OVERFLOW;
        
        if self.current_pages.is_empty() {
            self.first_page_id = Some(page_id);
            // Overflow count will be updated when finish() is called
        } else {
            page.header.overflow = 1; // Continuation page
        }

        self.current_pages.push(page_id);
        Ok(page_id)
    }
}

impl<'txn> IoWrite for OverflowWriter<'txn> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut total_written = 0;

        while total_written < buf.len() {
            // Allocate new page if needed
            if self.position_in_page == 0 || self.position_in_page >= self.data_per_page {
                self.alloc_new_page()
                    .map_err(io::Error::other)?;
                self.position_in_page = 0;
            }

            let current_page_idx = self.current_pages.len() - 1;
            let page_id = self.current_pages[current_page_idx];
            
            // Get mutable page and write data
            let page = self.txn.get_consecutive_page_mut(page_id)
                .map_err(io::Error::other)?;

            let available = self.data_per_page - self.position_in_page;
            let to_write = (buf.len() - total_written).min(available);

            page.data[self.position_in_page..self.position_in_page + to_write]
                .copy_from_slice(&buf[total_written..total_written + to_write]);

            total_written += to_write;
            self.position_in_page += to_write;
            self.total_written += to_write;
        }

        Ok(total_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Update overflow count in first page
        if let Some(first_id) = self.first_page_id {
            let page = self.txn.get_consecutive_page_mut(first_id)
                .map_err(io::Error::other)?;
            page.header.overflow = self.current_pages.len() as u32;
        }
        Ok(())
    }
}

/// Write a value using streaming API
pub fn write_overflow_value_streaming<'txn, R: Read>(
    txn: &'txn mut Transaction<'txn, Write>,
    mut reader: R,
) -> Result<(PageId, u32)> {
    let mut writer = OverflowWriter::new(txn);
    io::copy(&mut reader, &mut writer)
        .map_err(|e| Error::Io(e.to_string()))?;
    writer.flush()
        .map_err(|e| Error::Io(e.to_string()))?;
    writer.finish()
}

/// Read a value using streaming API
pub fn read_overflow_value_streaming<'txn, M: Mode, W: IoWrite>(
    txn: &'txn Transaction<'txn, M>,
    first_page_id: PageId,
    mut writer: W,
) -> Result<u64> {
    let mut reader = OverflowReader::new(txn, first_page_id)?;
    io::copy(&mut reader, &mut writer)
        .map_err(|e| Error::Io(e.to_string()))
}