//! SIMD-optimized operations for performance-critical paths

use std::cmp::Ordering;

/// SIMD-optimized byte comparison
/// 
/// Uses platform-specific SIMD instructions to compare byte slices faster
/// than the default implementation.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub fn compare_bytes_simd(a: &[u8], b: &[u8]) -> Ordering {
    use std::arch::x86_64::*;
    
    let len = a.len().min(b.len());
    
    // For small comparisons, fall back to standard comparison
    if len < 32 {
        return compare_bytes_scalar(a, b);
    }
    
    unsafe {
        let mut offset = 0;
        
        // Process 32 bytes at a time using AVX2
        while offset + 32 <= len {
            let a_vec = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
            let b_vec = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);
            
            // Compare for equality
            let eq_mask = _mm256_cmpeq_epi8(a_vec, b_vec);
            let eq_bits = _mm256_movemask_epi8(eq_mask) as u32;
            
            // If not all equal, find the first difference
            if eq_bits != 0xFFFFFFFF {
                let first_diff = eq_bits.trailing_ones() as usize;
                return a[offset + first_diff].cmp(&b[offset + first_diff]);
            }
            
            offset += 32;
        }
        
        // Process remaining bytes with standard comparison
        while offset < len {
            match a[offset].cmp(&b[offset]) {
                Ordering::Equal => offset += 1,
                other => return other,
            }
        }
        
        // If all compared bytes are equal, compare lengths
        a.len().cmp(&b.len())
    }
}

/// SSE2-optimized byte comparison (more widely available than AVX2)
#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
pub fn compare_bytes_simd(a: &[u8], b: &[u8]) -> Ordering {
    use std::arch::x86_64::*;
    
    let len = a.len().min(b.len());
    
    // For small comparisons, fall back to standard comparison
    if len < 16 {
        return compare_bytes_scalar(a, b);
    }
    
    unsafe {
        let mut offset = 0;
        
        // Process 16 bytes at a time using SSE2
        while offset + 16 <= len {
            let a_vec = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
            let b_vec = _mm_loadu_si128(b.as_ptr().add(offset) as *const __m128i);
            
            // Compare for equality
            let eq_mask = _mm_cmpeq_epi8(a_vec, b_vec);
            let eq_bits = _mm_movemask_epi8(eq_mask) as u16;
            
            // If not all equal, find the first difference
            if eq_bits != 0xFFFF {
                let first_diff = eq_bits.trailing_ones() as usize;
                return a[offset + first_diff].cmp(&b[offset + first_diff]);
            }
            
            offset += 16;
        }
        
        // Process remaining bytes
        while offset < len {
            match a[offset].cmp(&b[offset]) {
                Ordering::Equal => offset += 1,
                other => return other,
            }
        }
        
        a.len().cmp(&b.len())
    }
}

/// ARM NEON optimized comparison
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline(always)]
pub fn compare_bytes_simd(a: &[u8], b: &[u8]) -> Ordering {
    use std::arch::aarch64::*;
    
    let len = a.len().min(b.len());
    
    if len < 16 {
        return compare_bytes_scalar(a, b);
    }
    
    unsafe {
        let mut offset = 0;
        
        // Process 16 bytes at a time using NEON
        while offset + 16 <= len {
            let a_vec = vld1q_u8(a.as_ptr().add(offset));
            let b_vec = vld1q_u8(b.as_ptr().add(offset));
            
            // Compare for equality
            let eq_mask = vceqq_u8(a_vec, b_vec);
            
            // Check if all bytes are equal
            let reduced = vminvq_u8(eq_mask);
            if reduced != 0xFF {
                // Find first difference
                for i in 0..16 {
                    if a[offset + i] != b[offset + i] {
                        return a[offset + i].cmp(&b[offset + i]);
                    }
                }
            }
            
            offset += 16;
        }
        
        // Process remaining bytes
        while offset < len {
            match a[offset].cmp(&b[offset]) {
                Ordering::Equal => offset += 1,
                other => return other,
            }
        }
        
        a.len().cmp(&b.len())
    }
}

/// Fallback scalar comparison
#[inline(always)]
fn compare_bytes_scalar(a: &[u8], b: &[u8]) -> Ordering {
    let len = a.len().min(b.len());
    
    // Unroll loop for better performance
    let mut i = 0;
    while i + 8 <= len {
        // Compare 8 bytes at a time
        let a_chunk = &a[i..i + 8];
        let b_chunk = &b[i..i + 8];
        
        for j in 0..8 {
            match a_chunk[j].cmp(&b_chunk[j]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        
        i += 8;
    }
    
    // Handle remaining bytes
    while i < len {
        match a[i].cmp(&b[i]) {
            Ordering::Equal => i += 1,
            other => return other,
        }
    }
    
    a.len().cmp(&b.len())
}

/// Default implementation for platforms without SIMD
#[cfg(not(any(
    all(target_arch = "x86_64", any(target_feature = "avx2", not(target_feature = "avx2"))),
    all(target_arch = "aarch64", target_feature = "neon")
)))]
#[inline(always)]
pub fn compare_bytes_simd(a: &[u8], b: &[u8]) -> Ordering {
    compare_bytes_scalar(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simd_comparison() {
        // Test equal slices
        assert_eq!(compare_bytes_simd(b"hello", b"hello"), Ordering::Equal);
        
        // Test different lengths
        assert_eq!(compare_bytes_simd(b"hello", b"hello world"), Ordering::Less);
        assert_eq!(compare_bytes_simd(b"hello world", b"hello"), Ordering::Greater);
        
        // Test differences
        assert_eq!(compare_bytes_simd(b"abc", b"abd"), Ordering::Less);
        assert_eq!(compare_bytes_simd(b"xyz", b"abc"), Ordering::Greater);
        
        // Test long strings (to trigger SIMD paths)
        let long_a = b"abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz";
        let long_b = b"abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyy";
        assert_eq!(compare_bytes_simd(long_a, long_b), Ordering::Greater);
    }
    
    #[test]
    fn test_simd_matches_scalar() {
        let test_cases = vec![
            (b"".as_ref(), b"".as_ref()),
            (b"a", b"b"),
            (b"hello", b"world"),
            (b"aaaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaab"), // 16 bytes
            (b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"), // 32 bytes
        ];
        
        for (a, b) in test_cases {
            assert_eq!(
                compare_bytes_simd(a, b),
                a.cmp(b),
                "SIMD comparison differs from standard for {:?} vs {:?}",
                a, b
            );
        }
    }
}