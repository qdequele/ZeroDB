//! Cache-aligned data structures for optimal CPU performance
//!
//! This module provides cache-line aligned versions of common data structures
//! to prevent false sharing and improve cache efficiency.

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::cell::UnsafeCell;

/// CPU cache line size (64 bytes on most modern x86_64 and ARM processors)
#[cfg(target_arch = "x86_64")]
pub const CACHE_LINE_SIZE: usize = 64;

#[cfg(target_arch = "aarch64")]
pub const CACHE_LINE_SIZE: usize = 64;

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub const CACHE_LINE_SIZE: usize = 64; // Default fallback

/// Macro to create cache-aligned types
#[macro_export]
macro_rules! cache_aligned {
    ($name:ident, $inner:ty) => {
        #[repr(align(64))]
        pub struct $name {
            inner: $inner,
            _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<$inner>()],
        }
        
        impl $name {
            pub fn new(inner: $inner) -> Self {
                Self {
                    inner,
                    _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<$inner>()],
                }
            }
        }
        
        impl std::ops::Deref for $name {
            type Target = $inner;
            
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }
        
        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Cache-aligned atomic counter
#[repr(align(64))]
pub struct CacheAlignedCounter {
    value: AtomicU64,
    _padding: [u8; CACHE_LINE_SIZE - 8],
}

impl CacheAlignedCounter {
    pub const fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
            _padding: [0; CACHE_LINE_SIZE - 8],
        }
    }
    
    #[inline]
    pub fn increment(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }
    
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
    
    #[inline]
    pub fn add(&self, val: u64) -> u64 {
        self.value.fetch_add(val, Ordering::Relaxed)
    }
}

/// Multi-producer single-consumer queue with cache-aligned elements
pub struct CacheAlignedQueue<T> {
    /// Head pointer (consumer side)
    head: CacheAlignedCounter,
    /// Tail pointer (producer side)
    tail: CacheAlignedCounter,
    /// Ring buffer
    buffer: Vec<CacheAlignedSlot<T>>,
    /// Capacity mask for efficient modulo
    capacity_mask: usize,
}

#[repr(align(64))]
struct CacheAlignedSlot<T> {
    /// Sequence number for this slot
    sequence: AtomicUsize,
    /// The actual data
    data: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Sync for CacheAlignedSlot<T> {}
unsafe impl<T: Send> Send for CacheAlignedSlot<T> {}

impl<T> CacheAlignedQueue<T> {
    /// Create a new queue with the given capacity (must be power of 2)
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be power of 2");
        
        let mut buffer = Vec::with_capacity(capacity);
        for i in 0..capacity {
            buffer.push(CacheAlignedSlot {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        
        Self {
            head: CacheAlignedCounter::new(0),
            tail: CacheAlignedCounter::new(0),
            buffer,
            capacity_mask: capacity - 1,
        }
    }
    
    /// Try to enqueue an item
    pub fn try_enqueue(&self, item: T) -> Result<(), T> {
        let mut tail = self.tail.get() as usize;
        
        loop {
            let slot = &self.buffer[tail & self.capacity_mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            
            let diff = seq as isize - tail as isize;
            
            if diff == 0 {
                // Slot is ready for writing
                match self.tail.value.compare_exchange_weak(
                    tail as u64,
                    (tail + 1) as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We got the slot, write data
                        unsafe {
                            (*slot.data.get()).write(item);
                        }
                        slot.sequence.store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => {
                        tail = actual as usize;
                    }
                }
            } else if diff < 0 {
                // Queue is full
                return Err(item);
            } else {
                // Another thread is ahead of us
                tail = self.tail.get() as usize;
            }
        }
    }
    
    /// Try to dequeue an item
    pub fn try_dequeue(&self) -> Option<T> {
        let mut head = self.head.get() as usize;
        
        loop {
            let slot = &self.buffer[head & self.capacity_mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            
            let diff = seq as isize - (head + 1) as isize;
            
            if diff == 0 {
                // Slot has data
                match self.head.value.compare_exchange_weak(
                    head as u64,
                    (head + 1) as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We got the slot, read data
                        let item = unsafe {
                            (*slot.data.get()).assume_init_read()
                        };
                        slot.sequence.store(head + self.buffer.len() + 1, Ordering::Release);
                        return Some(item);
                    }
                    Err(actual) => {
                        head = actual as usize;
                    }
                }
            } else if diff < 0 {
                // Queue is empty
                return None;
            } else {
                // Another thread is ahead of us
                head = self.head.get() as usize;
            }
        }
    }
}

/// Cache-aligned statistics collector
#[repr(align(64))]
pub struct CacheAlignedStats {
    /// Page reads
    pub page_reads: CacheAlignedCounter,
    /// Page writes
    pub page_writes: CacheAlignedCounter,
    /// Cache hits
    pub cache_hits: CacheAlignedCounter,
    /// Cache misses
    pub cache_misses: CacheAlignedCounter,
    /// Transaction count
    pub txn_count: CacheAlignedCounter,
    /// Bytes read
    pub bytes_read: CacheAlignedCounter,
    /// Bytes written
    pub bytes_written: CacheAlignedCounter,
}

impl CacheAlignedStats {
    pub const fn new() -> Self {
        Self {
            page_reads: CacheAlignedCounter::new(0),
            page_writes: CacheAlignedCounter::new(0),
            cache_hits: CacheAlignedCounter::new(0),
            cache_misses: CacheAlignedCounter::new(0),
            txn_count: CacheAlignedCounter::new(0),
            bytes_read: CacheAlignedCounter::new(0),
            bytes_written: CacheAlignedCounter::new(0),
        }
    }
    
    /// Record a page read
    #[inline]
    pub fn record_page_read(&self, size: usize) {
        self.page_reads.increment();
        self.bytes_read.add(size as u64);
    }
    
    /// Record a page write
    #[inline]
    pub fn record_page_write(&self, size: usize) {
        self.page_writes.increment();
        self.bytes_written.add(size as u64);
    }
    
    /// Record a cache hit
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.increment();
    }
    
    /// Record a cache miss
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.increment();
    }
}

/// False sharing prevention wrapper
/// Ensures that frequently accessed data doesn't share cache lines
#[repr(align(64))]
pub struct Padded<T> {
    data: T,
}

impl<T> Padded<T> {
    pub const fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> std::ops::DerefMut for Padded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    
    #[test]
    fn test_cache_alignment() {
        // Test that our structures are properly aligned
        assert_eq!(mem::align_of::<CacheAlignedCounter>(), CACHE_LINE_SIZE);
        assert_eq!(mem::size_of::<CacheAlignedCounter>(), CACHE_LINE_SIZE);
        
        let counter = CacheAlignedCounter::new(0);
        let addr = &counter as *const _ as usize;
        assert_eq!(addr % CACHE_LINE_SIZE, 0, "Counter not cache aligned");
    }
    
    #[test]
    fn test_cache_aligned_queue() {
        let queue = CacheAlignedQueue::new(16);
        
        // Test enqueue/dequeue
        for i in 0..10 {
            assert!(queue.try_enqueue(i).is_ok());
        }
        
        for i in 0..10 {
            assert_eq!(queue.try_dequeue(), Some(i));
        }
        
        assert_eq!(queue.try_dequeue(), None);
    }
    
    #[test]
    fn test_stats_collector() {
        let stats = CacheAlignedStats::new();
        
        stats.record_page_read(4096);
        stats.record_page_write(4096);
        stats.record_cache_hit();
        stats.record_cache_miss();
        
        assert_eq!(stats.page_reads.get(), 1);
        assert_eq!(stats.page_writes.get(), 1);
        assert_eq!(stats.cache_hits.get(), 1);
        assert_eq!(stats.cache_misses.get(), 1);
        assert_eq!(stats.bytes_read.get(), 4096);
        assert_eq!(stats.bytes_written.get(), 4096);
    }
}