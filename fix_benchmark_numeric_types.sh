#!/bin/bash

# Fix all ambiguous numeric types in benchmarks
sed -i '' 's/\bi\.to_be_bytes()/\(i as u32\).to_be_bytes()/g' benches/zerodb_performance.rs
sed -i '' 's/\bcounter\.to_be_bytes()/\(counter as u32\).to_be_bytes()/g' benches/zerodb_performance.rs
sed -i '' 's/\bkey\.to_be_bytes()/\(key as u32\).to_be_bytes()/g' benches/zerodb_performance.rs
sed -i '' 's/\btarget\.to_be_bytes()/\(target as u32\).to_be_bytes()/g' benches/zerodb_performance.rs
sed -i '' 's/\bts\.to_be_bytes()/\(ts as u64\).to_be_bytes()/g' benches/zerodb_performance.rs
sed -i '' 's/(\([0-9]\+\))\.to_be_bytes()/\1u32.to_be_bytes()/g' benches/zerodb_performance.rs

echo "Fixed numeric types in benchmarks"