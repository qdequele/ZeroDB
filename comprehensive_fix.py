#!/usr/bin/env python3
"""
Comprehensive fix script for all ZeroDB compilation issues
"""
import os
import re
import glob

def fix_file_comprehensive(filepath):
    """Fix all known issues in a file"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Skip source files, only fix tests/benches/examples
    if not any(x in filepath for x in ['tests/', 'benches/', 'examples/']):
        return False
    
    print(f"Processing {filepath}")
    
    # 1. Fix cursor iteration patterns - use raw methods consistently
    content = re.sub(
        r'for\s+\(([^)]+)\)\s+in\s+cursor\s*\{([^}]+)\}',
        lambda m: f"""if let Some({m.group(1)}) = cursor.first_raw()? {{
    {m.group(2).strip()}
    while let Some({m.group(1)}) = cursor.next_raw()? {{
        {m.group(2).strip()}
    }}
}}""",
        content,
        flags=re.DOTALL
    )
    
    # 2. Fix cursor.next_entry() calls
    content = re.sub(r'cursor\.next_entry\(\)', r'cursor.next_raw()', content)
    
    # 3. Fix cursor iteration in for loops
    content = re.sub(
        r'for\s+([^i][^n][^\\s]*)\s+in\s+([^{]+\.cursor\([^)]*\)\?)\s*\{',
        lambda m: f'let mut cursor = {m.group(2).strip()};\nif let Some({m.group(1)}) = cursor.first_raw()? {{\n    loop {{',
        content
    )
    
    # 4. Fix Database<u32> references
    content = re.sub(r'Database<u32,\s*u32>', 'Database<String, String>', content)
    content = re.sub(r'Database<u32,\s*Vec<u8>>', 'Database<String, Vec<u8>>', content)
    
    # 5. Fix operations on integer keys/values for String databases
    if 'Database<String,' in content:
        # Fix put operations
        content = re.sub(r'\.put\(&mut\s+txn,\s*(\d+),\s*(\d+)\)', r'.put(&mut txn, \1.to_string(), \2.to_string())', content)
        content = re.sub(r'\.put\(&mut\s+txn,\s*([a-zA-Z_]\w*),\s*([a-zA-Z_]\w*\s*\*\s*\d+)\)', r'.put(&mut txn, \1.to_string(), (\2).to_string())', content)
        
        # Fix get operations
        content = re.sub(r'\.get\(&txn,\s*&(\d+)\)', r'.get(&txn, &\1.to_string())', content)
        content = re.sub(r'\.get\(&txn,\s*&([a-zA-Z_]\w*)\)\?', r'.get(&txn, &\1.to_string())?', content)
        
        # Fix delete operations
        content = re.sub(r'\.delete\(&mut\s+txn,\s*&(\d+)\)', r'.delete(&mut txn, &\1.to_string())', content)
        content = re.sub(r'\.delete\(&mut\s+txn,\s*&([a-zA-Z_]\w*)\)\?', r'.delete(&mut txn, &\1.to_string())?', content)
    
    # 6. Fix import issues
    content = re.sub(r'use zerodb::DurabilityMode', r'use zerodb::env::DurabilityMode', content)
    
    # 7. Fix Vec<u8> to_string issues  
    content = re.sub(r'&key\.to_string\(\)', r'&key', content)
    
    # 8. Fix Error references without full path
    content = re.sub(r'Error::(Custom|MapFull|NotFound|DatabaseFull)', r'zerodb::Error::\1', content)
    
    # 9. Fix method calls that don't exist
    content = re.sub(r'\.stat\(&txn\)\?', r'// stat() method not available', content)
    content = re.sub(r'\.checksum\(true\)', r'// checksum() method not available', content)
    content = re.sub(r'\.page_size\(\d+\)', r'// page_size() method not available', content)
    
    # 10. Fix LMDB/heed API issues
    content = re.sub(r'db\.cursor\(&rtxn\)\.unwrap\(\)', r'db.iter(&rtxn).unwrap()', content)
    
    # 11. Fix unwrap on Database
    content = re.sub(r'env\.open_database\([^)]+\)\?\.unwrap\(\)', 
                    lambda m: f'{m.group(0)[:-11]}.ok_or(zerodb::Error::Custom("Database not found".into()))?', content)
    
    # 12. Fix iterator usage - replace iter() with cursor pattern
    if '.iter(' in content and 'heed' not in content:
        content = re.sub(
            r'for\s+([^i][^n][^\s]*)\s+in\s+db\.iter\(&([^)]+)\)\?\s*\{',
            lambda m: f'''let mut cursor = db.cursor(&{m.group(2)})?;
if let Some({m.group(1)}) = cursor.first_raw()? {{
    loop {{''',
            content
        )
    
    # 13. Fix numeric type annotations
    content = re.sub(r'(\d+)\.to_be_bytes\(\)', r'\1u64.to_be_bytes()', content)
    
    # 14. Fix String comparisons with Vec<u8>
    if 'String::from_utf8' in content:
        content = re.sub(
            r'assert_eq!\(String::from_utf8\(([^)]+)\.clone\(\)\)\.unwrap\(\),\s*([^)]+)\);',
            r'assert_eq!(String::from_utf8(\1).unwrap(), \2);',
            content
        )
    
    # 15. Fix return type mismatches in assert_eq
    content = re.sub(
        r'assert_eq!\(db\.get\(&txn, &([^)]+)\.to_string\(\)\)\?, Some\(([^)]+)\)\);',
        r'assert_eq!(db.get(&txn, &\1.to_string())?, Some(\2.to_string()));',
        content
    )
    
    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  ✅ Fixed {filepath}")
        return True
    return False

def main():
    """Main execution"""
    # Find all Rust files in tests, benches, examples
    patterns = [
        'tests/**/*.rs',
        'benches/**/*.rs', 
        'examples/**/*.rs'
    ]
    
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(pattern, recursive=True))
    
    fixed_count = 0
    for filepath in all_files:
        if fix_file_comprehensive(filepath):
            fixed_count += 1
    
    print(f"\n🎉 Fixed {fixed_count} files")
    
    # Check compilation status
    print("\n📋 Checking compilation status...")
    os.system("cargo check --all-targets 2>&1 | grep -E 'error\\[E|could not compile' | sort | uniq -c | sort -nr")

if __name__ == '__main__':
    main()