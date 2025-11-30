//! Memory mapping abstraction for ZeroDB.
//!
//! This module provides a safe abstraction over memory-mapped files,
//! supporting both read-only and read-write mappings.

use std::fs::{File, OpenOptions};
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use memmap2::{Mmap, MmapMut, MmapOptions};

use crate::error::{Error, Result};

/// A memory-mapped file that can be either read-only or read-write.
pub struct MemoryMap {
    inner: MmapInner,
    len: usize,
}

enum MmapInner {
    ReadOnly(Mmap),
    ReadWrite(MmapMut),
}

impl MemoryMap {
    /// Creates a new read-only memory map from a file.
    ///
    /// # Safety
    ///
    /// The file must not be modified by another process while mapped.
    pub unsafe fn open_read_only(path: &Path, len: usize) -> Result<Self> {
        let file = File::open(path)?;

        // Ensure file is at least `len` bytes
        let file_len = file.metadata()?.len() as usize;
        if file_len < len {
            return Err(Error::Invalid);
        }

        let mmap = unsafe {
            MmapOptions::new()
                .len(len)
                .map(&file)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        };

        Ok(Self {
            inner: MmapInner::ReadOnly(mmap),
            len,
        })
    }

    /// Creates a new read-write memory map from a file.
    ///
    /// # Safety
    ///
    /// The file must not be modified by another process while mapped.
    /// Writes to the mapping will be visible to other processes.
    pub unsafe fn open_read_write(path: &Path, len: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Ensure file is at least `len` bytes, extend if needed
        let file_len = file.metadata()?.len() as usize;
        if file_len < len {
            file.set_len(len as u64)?;
        }

        let mmap = unsafe {
            MmapOptions::new()
                .len(len)
                .map_mut(&file)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        };

        Ok(Self {
            inner: MmapInner::ReadWrite(mmap),
            len,
        })
    }

    /// Creates a new anonymous (not file-backed) read-write memory map.
    ///
    /// This is useful for dirty page buffers during write transactions.
    pub fn anonymous(len: usize) -> Result<Self> {
        let mmap = MmapOptions::new()
            .len(len)
            .map_anon()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Self {
            inner: MmapInner::ReadWrite(mmap),
            len,
        })
    }

    /// Returns the length of the memory map.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the memory map is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns true if the memory map is writable.
    pub fn is_writable(&self) -> bool {
        matches!(self.inner, MmapInner::ReadWrite(_))
    }

    /// Returns a slice of the memory map.
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            MmapInner::ReadOnly(mmap) => mmap,
            MmapInner::ReadWrite(mmap) => mmap,
        }
    }

    /// Returns a mutable slice of the memory map.
    ///
    /// # Panics
    ///
    /// Panics if the memory map is read-only.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.inner {
            MmapInner::ReadOnly(_) => panic!("Cannot get mutable slice from read-only mmap"),
            MmapInner::ReadWrite(mmap) => mmap,
        }
    }

    /// Returns a mutable slice if writable, None otherwise.
    pub fn try_as_mut_slice(&mut self) -> Option<&mut [u8]> {
        match &mut self.inner {
            MmapInner::ReadOnly(_) => None,
            MmapInner::ReadWrite(mmap) => Some(mmap),
        }
    }

    /// Flushes changes to disk synchronously.
    pub fn flush(&self) -> Result<()> {
        match &self.inner {
            MmapInner::ReadOnly(_) => Ok(()),
            MmapInner::ReadWrite(mmap) => {
                mmap.flush().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(())
            }
        }
    }

    /// Flushes changes to disk asynchronously.
    pub fn flush_async(&self) -> Result<()> {
        match &self.inner {
            MmapInner::ReadOnly(_) => Ok(()),
            MmapInner::ReadWrite(mmap) => {
                mmap.flush_async().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(())
            }
        }
    }

    /// Flushes a range of the memory map to disk.
    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        match &self.inner {
            MmapInner::ReadOnly(_) => Ok(()),
            MmapInner::ReadWrite(mmap) => {
                mmap.flush_range(offset, len)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(())
            }
        }
    }

    /// Returns a page-aligned slice at the given page number.
    pub fn page(&self, page_no: u64, page_size: usize) -> Option<&[u8]> {
        let offset = page_no as usize * page_size;
        if offset + page_size <= self.len {
            Some(&self.as_slice()[offset..offset + page_size])
        } else {
            None
        }
    }

    /// Returns a mutable page-aligned slice at the given page number.
    pub fn page_mut(&mut self, page_no: u64, page_size: usize) -> Option<&mut [u8]> {
        let offset = page_no as usize * page_size;
        if offset + page_size <= self.len {
            Some(&mut self.as_mut_slice()[offset..offset + page_size])
        } else {
            None
        }
    }
}

impl Deref for MemoryMap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for MemoryMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// File operations for the database file.
pub struct DataFile {
    file: File,
    path: std::path::PathBuf,
}

impl DataFile {
    /// Opens an existing data file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Creates a new data file.
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Opens or creates the data file.
    pub fn open_or_create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Returns the file length.
    pub fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Returns true if the file is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Sets the file length, extending or truncating as needed.
    pub fn set_len(&self, len: u64) -> Result<()> {
        self.file.set_len(len)?;
        Ok(())
    }

    /// Syncs the file to disk (full fsync).
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Syncs file data to disk without syncing metadata (fdatasync).
    /// This is faster than sync() as it doesn't update file metadata.
    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Returns a reference to the underlying file.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Returns the path to the file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Writes data at the specified offset.
    #[cfg(unix)]
    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        Ok(self.file.write_at(buf, offset)?)
    }

    /// Writes data at the specified offset.
    #[cfg(windows)]
    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        use std::os::windows::fs::FileExt;
        Ok(self.file.seek_write(buf, offset)?)
    }

    /// Writes multiple buffers at their specified offsets.
    /// This batches writes to reduce syscall overhead.
    #[cfg(unix)]
    pub fn write_batch(&self, writes: &[(&[u8], u64)]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        for (buf, offset) in writes {
            self.file.write_all_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Writes multiple buffers at their specified offsets.
    #[cfg(windows)]
    pub fn write_batch(&self, writes: &[(&[u8], u64)]) -> Result<()> {
        for (buf, offset) in writes {
            self.write_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Reads data at the specified offset.
    #[cfg(unix)]
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        Ok(self.file.read_at(buf, offset)?)
    }

    /// Reads data at the specified offset.
    #[cfg(windows)]
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::windows::fs::FileExt;
        Ok(self.file.seek_read(buf, offset)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn anonymous_mmap() {
        let mmap = MemoryMap::anonymous(4096).unwrap();
        assert_eq!(mmap.len(), 4096);
        assert!(mmap.is_writable());
    }

    #[test]
    fn data_file_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let file = DataFile::create(&path).unwrap();
        assert!(file.is_empty().unwrap());

        file.set_len(4096).unwrap();
        assert_eq!(file.len().unwrap(), 4096);
    }

    #[test]
    fn mmap_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and write to file
        {
            let file = DataFile::create(&path).unwrap();
            file.set_len(4096).unwrap();
        }

        // Memory map and write
        {
            let mut mmap = unsafe { MemoryMap::open_read_write(&path, 4096).unwrap() };
            mmap.as_mut_slice()[0..4].copy_from_slice(&[1, 2, 3, 4]);
            mmap.flush().unwrap();
        }

        // Re-open and verify
        {
            let mmap = unsafe { MemoryMap::open_read_only(&path, 4096).unwrap() };
            assert_eq!(&mmap.as_slice()[0..4], &[1, 2, 3, 4]);
        }
    }

    #[test]
    fn page_access() {
        let mmap = MemoryMap::anonymous(4096 * 4).unwrap();

        assert!(mmap.page(0, 4096).is_some());
        assert!(mmap.page(3, 4096).is_some());
        assert!(mmap.page(4, 4096).is_none());
    }
}
