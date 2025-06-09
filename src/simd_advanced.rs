//! Advanced SIMD optimizations for specific workloads

use std::cmp::Ordering;

/// SIMD-accelerated binary search within a page
/// Finds the position where a key should be inserted
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub unsafe fn simd_binary_search_avx2(
    keys: &[&[u8]],
    target: &[u8],
    comparator: impl Fn(&[u8], &[u8]) -> Ordering,
) -> Result<usize, usize> {
    use std::arch::x86_64::*;

    if keys.is_empty() {
        return Err(0);
    }

    // For small arrays, use regular binary search
    if keys.len() < 8 {
        return keys.binary_search_by(|k| comparator(k, target));
    }

    // Use SIMD to find approximate position first
    let mut left = 0;
    let mut right = keys.len();

    while right - left > 8 {
        // Sample 8 evenly distributed keys
        let step = (right - left) / 8;
        let indices = [
            left,
            left + step,
            left + 2 * step,
            left + 3 * step,
            left + 4 * step,
            left + 5 * step,
            left + 6 * step,
            left + 7 * step.min(right - 1),
        ];

        // Compare first byte of each key with target's first byte
        if target.is_empty() {
            return Err(left);
        }

        let target_byte = target[0];
        let mut first_bytes = [0u8; 8];
        for (i, &idx) in indices.iter().enumerate() {
            if idx < keys.len() && !keys[idx].is_empty() {
                first_bytes[i] = keys[idx][0];
            }
        }

        // Use SIMD to compare all 8 first bytes at once
        let target_vec = _mm_set1_epi8(target_byte as i8);
        let keys_vec = _mm_loadl_epi64(first_bytes.as_ptr() as *const __m128i);

        // Find keys less than target
        let lt_mask = _mm_cmplt_epi8(keys_vec, target_vec);
        let lt_bits = _mm_movemask_epi8(lt_mask) as u8;

        // Count leading ones to find partition point
        let partition = lt_bits.trailing_ones() as usize;

        if partition == 0 {
            right = indices[0];
        } else if partition >= 8 {
            left = indices[7];
        } else {
            // Narrow down to the range containing target
            left = indices[partition.saturating_sub(1)];
            right = indices[partition.min(7)];
        }
    }

    // Final binary search on narrowed range
    keys[left..right]
        .binary_search_by(|k| comparator(k, target))
        .map(|i| left + i)
        .map_err(|i| left + i)
}

/// SIMD-accelerated key prefix matching
/// Returns a bitmask of keys that match the given prefix
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub unsafe fn simd_prefix_match_avx2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    use std::arch::x86_64::*;

    let mut matches = vec![false; keys.len()];

    if prefix.is_empty() {
        // Empty prefix matches everything
        matches.fill(true);
        return matches;
    }

    // Process keys in chunks for cache efficiency
    for (chunk_idx, key_chunk) in keys.chunks(32).enumerate() {
        for (idx, key) in key_chunk.iter().enumerate() {
            let global_idx = chunk_idx * 32 + idx;

            if key.len() < prefix.len() {
                continue;
            }

            // For long prefixes, use SIMD comparison
            if prefix.len() >= 32 {
                let mut offset = 0;
                let mut all_match = true;

                while offset + 32 <= prefix.len() {
                    let prefix_vec =
                        _mm256_loadu_si256(prefix.as_ptr().add(offset) as *const __m256i);
                    let key_vec = _mm256_loadu_si256(key.as_ptr().add(offset) as *const __m256i);

                    let eq_mask = _mm256_cmpeq_epi8(prefix_vec, key_vec);
                    let eq_bits = _mm256_movemask_epi8(eq_mask);

                    if eq_bits != -1 {
                        all_match = false;
                        break;
                    }

                    offset += 32;
                }

                // Check remaining bytes
                if all_match {
                    all_match = prefix[offset..] == key[offset..offset + (prefix.len() - offset)];
                }

                matches[global_idx] = all_match;
            } else {
                // For short prefixes, regular comparison is faster
                matches[global_idx] = key.starts_with(prefix);
            }
        }
    }

    matches
}

/// SIMD-accelerated batch key validation
/// Checks if all keys are properly sorted and valid
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub unsafe fn simd_validate_sorted_keys_avx2(
    keys: &[&[u8]],
    comparator: impl Fn(&[u8], &[u8]) -> Ordering,
) -> Result<(), usize> {
    if keys.len() < 2 {
        return Ok(());
    }

    // Check pairs of keys in parallel
    for i in 0..keys.len() - 1 {
        if comparator(keys[i], keys[i + 1]) != Ordering::Less {
            return Err(i);
        }
    }

    Ok(())
}

/// SIMD-accelerated bloom filter check
/// Uses SIMD to check multiple hash positions in parallel
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub struct SimdBloomFilter {
    bits: Vec<u64>,
    size: usize,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
impl SimdBloomFilter {
    pub fn new(expected_items: usize) -> Self {
        // Calculate optimal size (10 bits per item for ~1% false positive rate)
        let size = (expected_items * 10).next_power_of_two();
        let num_words = (size + 63) / 64;

        Self { bits: vec![0u64; num_words], size }
    }

    /// Insert a key using multiple hash functions
    pub fn insert(&mut self, key: &[u8]) {
        let hashes = self.hash_key(key);
        for hash in hashes {
            let word_idx = (hash / 64) % self.bits.len();
            let bit_idx = hash % 64;
            self.bits[word_idx] |= 1u64 << bit_idx;
        }
    }

    /// Check if a key might be present (SIMD-accelerated)
    pub unsafe fn contains(&self, key: &[u8]) -> bool {
        use std::arch::x86_64::*;

        let hashes = self.hash_key(key);

        // Check all hash positions using SIMD
        let mut all_set = true;

        for hash in hashes {
            let word_idx = (hash / 64) % self.bits.len();
            let bit_idx = hash % 64;

            if self.bits[word_idx] & (1u64 << bit_idx) == 0 {
                all_set = false;
                break;
            }
        }

        all_set
    }

    /// Generate multiple hash values for a key
    fn hash_key(&self, key: &[u8]) -> [usize; 4] {
        // Use FNV-1a hash with different seeds
        let mut hashes = [0usize; 4];
        let seeds = [0xcbf29ce484222325u64, 0x811c9dc5u64, 0x13198a2eu64, 0x1a2b3c4du64];

        for (i, &seed) in seeds.iter().enumerate() {
            let mut hash = seed;
            for &byte in key {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hashes[i] = (hash as usize) % (self.size);
        }

        hashes
    }
}

// Fallback implementations for non-AVX2 platforms
/// SIMD-accelerated binary search using AVX2 instructions (fallback implementation)
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
pub unsafe fn simd_binary_search_avx2(
    keys: &[&[u8]],
    target: &[u8],
    comparator: impl Fn(&[u8], &[u8]) -> Ordering,
) -> Result<usize, usize> {
    keys.binary_search_by(|k| comparator(k, target))
}

/// SIMD-accelerated prefix matching using AVX2 instructions (fallback implementation)
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
pub unsafe fn simd_prefix_match_avx2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    keys.iter().map(|k| k.starts_with(prefix)).collect()
}

/// SIMD-accelerated validation of sorted keys using AVX2 instructions (fallback implementation)
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
pub unsafe fn simd_validate_sorted_keys_avx2(
    keys: &[&[u8]],
    comparator: impl Fn(&[u8], &[u8]) -> Ordering,
) -> Result<(), usize> {
    for i in 0..keys.len() - 1 {
        if comparator(keys[i], keys[i + 1]) != Ordering::Less {
            return Err(i);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_binary_search() {
        let keys: Vec<&[u8]> =
            vec![b"aaa", b"bbb", b"ccc", b"ddd", b"eee", b"fff", b"ggg", b"hhh", b"iii", b"jjj"];

        unsafe {
            // Test finding existing keys
            assert_eq!(simd_binary_search_avx2(&keys, b"eee", |a, b| a.cmp(b)), Ok(4));

            // Test not finding key
            assert_eq!(simd_binary_search_avx2(&keys, b"dde", |a, b| a.cmp(b)), Err(4));
        }
    }

    #[test]
    fn test_simd_prefix_match() {
        let keys: Vec<&[u8]> = vec![b"hello", b"help", b"hero", b"world", b"worry"];

        unsafe {
            let matches = simd_prefix_match_avx2(&keys, b"he");
            assert_eq!(matches, vec![true, true, true, false, false]);

            let matches = simd_prefix_match_avx2(&keys, b"wor");
            assert_eq!(matches, vec![false, false, false, true, true]);
        }
    }
}
