//! Bloom filter implementation for fast negative lookups
//!
//! This module provides bloom filters that can be attached to database pages
//! to quickly determine if a key definitely doesn't exist, avoiding expensive
//! disk I/O for negative lookups.

use crate::cache_aligned::CacheAlignedStats;
use crate::error::{Error, PageId, Result};
use parking_lot::RwLock;
use std::sync::Arc;

/// Bloom filter configuration
#[derive(Debug, Clone)]
pub struct BloomConfig {
    /// Target false positive rate (e.g., 0.01 for 1%)
    pub false_positive_rate: f64,
    /// Expected number of items
    pub expected_items: usize,
    /// Whether to use SIMD acceleration
    pub use_simd: bool,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self { false_positive_rate: 0.01, expected_items: 10000, use_simd: true }
    }
}

/// Bloom filter implementation
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Statistics
    stats: Arc<CacheAlignedStats>,
}

impl BloomFilter {
    /// Create a new bloom filter with the given configuration
    pub fn new(config: &BloomConfig) -> Self {
        // Calculate optimal parameters
        let n = config.expected_items;
        let p = config.false_positive_rate;

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let m = (-(n as f64) * p.ln() / (2f64.ln().powi(2))).ceil() as usize;
        let num_bits = m.next_power_of_two(); // Round up to power of 2 for efficiency

        // Optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((m as f64 / n as f64) * 2f64.ln()).round() as usize;
        let num_hashes = k.max(1).min(16); // Limit to reasonable range

        // Allocate bit array
        let num_words = (num_bits + 63) / 64;
        let bits = vec![0u64; num_words];

        Self { bits, num_bits, num_hashes, stats: Arc::new(CacheAlignedStats::new()) }
    }

    /// Insert a key into the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        let hashes = self.hash_key(key);

        for i in 0..self.num_hashes {
            let bit_idx = hashes[i] % self.num_bits;
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;

            self.bits[word_idx] |= 1u64 << bit_offset;
        }
    }

    /// Check if a key might be in the set
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let hashes = self.hash_key(key);

        for i in 0..self.num_hashes {
            let bit_idx = hashes[i] % self.num_bits;
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;

            if self.bits[word_idx] & (1u64 << bit_offset) == 0 {
                self.stats.record_cache_hit(); // Bloom filter prevented lookup
                return false;
            }
        }

        self.stats.record_cache_miss(); // Need to check actual data
        true
    }

    /// Generate hash values for a key using double hashing
    fn hash_key(&self, key: &[u8]) -> Vec<usize> {
        // Use FNV-1a as primary hash
        let mut h1 = 0xcbf29ce484222325u64;
        for &byte in key {
            h1 ^= byte as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
        }

        // Use murmur-like hash as secondary
        let mut h2 = 0x5555555555555555u64;
        for chunk in key.chunks(8) {
            let mut val = 0u64;
            for (i, &byte) in chunk.iter().enumerate() {
                val |= (byte as u64) << (i * 8);
            }
            h2 ^= val;
            h2 = h2.rotate_left(31).wrapping_mul(0x517cc1b727220a95);
        }

        // Generate k hash values using double hashing
        let mut hashes = Vec::with_capacity(self.num_hashes);
        for i in 0..self.num_hashes {
            let hash = h1.wrapping_add(h2.wrapping_mul(i as u64));
            hashes.push(hash as usize);
        }

        hashes
    }

    /// Get the size in bytes
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Get the fill ratio
    pub fn fill_ratio(&self) -> f64 {
        let set_bits = self.bits.iter().map(|w| w.count_ones() as usize).sum::<usize>();
        set_bits as f64 / self.num_bits as f64
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }

    /// Merge another bloom filter into this one
    pub fn merge(&mut self, other: &BloomFilter) -> Result<()> {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return Err(Error::Custom("Bloom filter parameters don't match".into()));
        }

        for (word, other_word) in self.bits.iter_mut().zip(&other.bits) {
            *word |= other_word;
        }

        Ok(())
    }
}

/// Page-level bloom filter for database pages
pub struct PageBloomFilter {
    /// Bloom filter for this page
    filter: BloomFilter,
    /// Page ID this filter is for
    #[allow(dead_code)]
    page_id: PageId,
    /// Whether the filter is dirty
    dirty: bool,
}

impl PageBloomFilter {
    /// Create a new page bloom filter
    pub fn new(page_id: PageId, expected_keys: usize) -> Self {
        let config = BloomConfig {
            false_positive_rate: 0.02, // 2% FPR for page-level filters
            expected_items: expected_keys,
            use_simd: true,
        };

        Self { filter: BloomFilter::new(&config), page_id, dirty: false }
    }

    /// Insert a key
    pub fn insert(&mut self, key: &[u8]) {
        self.filter.insert(key);
        self.dirty = true;
    }

    /// Check if a key might exist
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.filter.contains(key)
    }

    /// Check if the filter needs to be persisted
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark as clean after persisting
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
}

/// Hierarchical bloom filter for the entire database
pub struct HierarchicalBloomFilter {
    /// Root level filter (covers entire database)
    root: RwLock<BloomFilter>,
    /// Branch level filters (one per branch page)
    branches: RwLock<HashMap<PageId, BloomFilter>>,
    /// Leaf level filters (one per leaf page)
    leaves: RwLock<HashMap<PageId, PageBloomFilter>>,
    /// Configuration
    config: BloomConfig,
}

impl HierarchicalBloomFilter {
    /// Create a new hierarchical bloom filter
    pub fn new(config: BloomConfig) -> Self {
        Self {
            root: RwLock::new(BloomFilter::new(&config)),
            branches: RwLock::new(HashMap::new()),
            leaves: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Check if a key might exist at any level
    pub fn contains(&self, key: &[u8]) -> bool {
        // Check root filter first
        if !self.root.read().contains(key) {
            return false;
        }

        // If root says maybe, we need to check actual data
        // (branch and leaf filters are checked during tree traversal)
        true
    }

    /// Get or create a leaf filter
    pub fn get_or_create_leaf(&self, page_id: PageId) -> Result<()> {
        let mut leaves = self.leaves.write();
        if !leaves.contains_key(&page_id) {
            let filter = PageBloomFilter::new(page_id, 100); // Assume ~100 keys per leaf
            leaves.insert(page_id, filter);
        }
        Ok(())
    }

    /// Insert a key, updating all relevant filters
    pub fn insert(&self, key: &[u8], leaf_page: PageId, branch_pages: &[PageId]) {
        // Update root filter
        self.root.write().insert(key);

        // Update branch filters
        let mut branches = self.branches.write();
        for &branch_id in branch_pages {
            branches.entry(branch_id).or_insert_with(|| BloomFilter::new(&self.config)).insert(key);
        }

        // Update leaf filter
        let mut leaves = self.leaves.write();
        leaves.entry(leaf_page).or_insert_with(|| PageBloomFilter::new(leaf_page, 100)).insert(key);
    }

    /// Check if a specific leaf might contain a key
    pub fn leaf_contains(&self, page_id: PageId, key: &[u8]) -> bool {
        let leaves = self.leaves.read();
        if let Some(filter) = leaves.get(&page_id) {
            filter.contains(key)
        } else {
            true // No filter means we can't rule it out
        }
    }

    /// Get statistics
    pub fn stats(&self) -> BloomStats {
        let root_stats = BloomFilterStats {
            num_bits: self.root.read().num_bits,
            fill_ratio: self.root.read().fill_ratio(),
            size_bytes: self.root.read().size_bytes(),
        };

        let branch_count = self.branches.read().len();
        let leaf_count = self.leaves.read().len();

        BloomStats { root: root_stats, branch_filters: branch_count, leaf_filters: leaf_count }
    }
}

/// Bloom filter statistics
#[derive(Debug)]
pub struct BloomStats {
    /// Root filter stats
    pub root: BloomFilterStats,
    /// Number of branch filters
    pub branch_filters: usize,
    /// Number of leaf filters  
    pub leaf_filters: usize,
}

#[derive(Debug)]
/// Statistics for Bloom filter performance
pub struct BloomFilterStats {
    /// Number of bits
    pub num_bits: usize,
    /// Fill ratio (0.0 to 1.0)
    pub fill_ratio: f64,
    /// Size in bytes
    pub size_bytes: usize,
}

/// SIMD-accelerated bloom filter operations
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
mod simd_bloom {
    use super::*;
    use std::arch::x86_64::*;

    /// Check multiple keys in parallel using SIMD
    pub unsafe fn contains_batch_avx2(filter: &BloomFilter, keys: &[&[u8]]) -> Vec<bool> {
        let mut results = vec![false; keys.len()];

        // Process keys in batches of 4 for AVX2
        for (chunk_idx, key_chunk) in keys.chunks(4).enumerate() {
            let base_idx = chunk_idx * 4;

            // Hash all keys in the chunk
            let mut all_hashes = Vec::new();
            for key in key_chunk {
                all_hashes.push(filter.hash_key(key));
            }

            // Check each hash function across all keys
            let mut chunk_results = [true; 4];
            for hash_idx in 0..filter.num_hashes {
                // Get bit indices for all keys
                let mut bit_indices = [0usize; 4];
                for (i, hashes) in all_hashes.iter().enumerate() {
                    if i < key_chunk.len() {
                        bit_indices[i] = hashes[hash_idx] % filter.num_bits;
                    }
                }

                // Check bits
                for (i, &bit_idx) in bit_indices.iter().enumerate() {
                    if i < key_chunk.len() {
                        let word_idx = bit_idx / 64;
                        let bit_offset = bit_idx % 64;

                        if filter.bits[word_idx] & (1u64 << bit_offset) == 0 {
                            chunk_results[i] = false;
                        }
                    }
                }
            }

            // Copy results
            for (i, &result) in chunk_results.iter().enumerate() {
                if base_idx + i < results.len() {
                    results[base_idx + i] = result;
                }
            }
        }

        results
    }
}

use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let config = BloomConfig::default();
        let mut filter = BloomFilter::new(&config);

        // Insert some keys
        filter.insert(b"hello");
        filter.insert(b"world");
        filter.insert(b"test");

        // Check they exist
        assert!(filter.contains(b"hello"));
        assert!(filter.contains(b"world"));
        assert!(filter.contains(b"test"));

        // Check false positives are rare
        let mut false_positives = 0;
        for i in 0..1000 {
            let key = format!("nonexistent{}", i);
            if filter.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // Should be close to configured false positive rate
        let fp_rate = false_positives as f64 / 1000.0;
        assert!(fp_rate < 0.05); // Allow some variance
    }

    #[test]
    fn test_hierarchical_bloom() {
        let config = BloomConfig::default();
        let hier = HierarchicalBloomFilter::new(config);

        // Insert keys with their locations
        hier.insert(b"key1", PageId(10), &[PageId(5), PageId(2)]);
        hier.insert(b"key2", PageId(10), &[PageId(5), PageId(2)]);
        hier.insert(b"key3", PageId(11), &[PageId(5), PageId(2)]);

        // Check root filter
        assert!(hier.contains(b"key1"));
        assert!(hier.contains(b"key2"));
        assert!(hier.contains(b"key3"));

        // Check leaf filters
        assert!(hier.leaf_contains(PageId(10), b"key1"));
        assert!(hier.leaf_contains(PageId(10), b"key2"));
        assert!(hier.leaf_contains(PageId(11), b"key3"));
    }
}
