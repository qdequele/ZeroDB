#!/usr/bin/env python3
"""Fix benchmark compilation issues"""
import re

def fix_database_comparison():
    with open('benches/database_comparison.rs', 'r') as f:
        content = f.read()
    
    # Fix heed database operations to use string types consistently
    # For heed with Str types, we need to use string keys/values
    content = re.sub(
        r'db\.put\(&mut wtxn, key\.as_bytes\(\), &value\[\.\.]\)\.unwrap\(\);',
        'db.put(&mut wtxn, &key, "x").unwrap();',
        content
    )
    
    # Fix heed get operations
    content = re.sub(
        r'if let Some\(val\) = db\.get\(&rtxn, key\)\.unwrap\(\) \{',
        'if let Some(val) = db.get(&rtxn, key.as_str()).unwrap() {',
        content
    )
    
    with open('benches/database_comparison.rs', 'w') as f:
        f.write(content)

def fix_zerodb_performance():
    with open('benches/zerodb_performance.rs', 'r') as f:
        content = f.read()
    
    # Fix ambiguous numeric types
    content = re.sub(r'(\d+)\.to_be_bytes\(\)', r'\1u64.to_be_bytes()', content)
    
    # Fix any u32 database types
    content = re.sub(r'Database<u32, Vec<u8>>', 'Database<Vec<u8>, Vec<u8>>', content)
    
    with open('benches/zerodb_performance.rs', 'w') as f:
        f.write(content)

if __name__ == '__main__':
    fix_database_comparison()
    fix_zerodb_performance()
    print("Fixed benchmark files")