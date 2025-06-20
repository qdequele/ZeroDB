#!/usr/bin/env python3
"""
Fix remaining type issues in test files where Database<String, String> is used
but the actual operations still use integers.
"""
import os
import re
import glob

def fix_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Track changes
    changes = 0
    original_content = content
    
    # Fix patterns where we have Database<String, String> but still use integers
    if 'Database<String, String>' in content:
        # Fix put operations with raw integers
        content = re.sub(r'db\.put\(&mut txn, (\d+), (\d+)\)', r'db.put(&mut txn, \1.to_string(), \2.to_string())', content)
        content = re.sub(r'db\.put\(&mut txn, i, (i \* \d+)\)', r'db.put(&mut txn, i.to_string(), (\1).to_string())', content)
        content = re.sub(r'db\.put\(&mut txn, (\w+), (\w+ \* \d+)\)', r'db.put(&mut txn, \1.to_string(), (\2).to_string())', content)
        
        # Fix get operations with raw integers
        content = re.sub(r'db\.get\(&txn, &(\d+)\)', r'db.get(&txn, &\1.to_string())', content)
        content = re.sub(r'db\.get\(&txn, &(\w+)\)\?', r'db.get(&txn, &\1.to_string())?', content)
        
        # Fix delete operations with raw integers  
        content = re.sub(r'db\.delete\(&mut txn, &(\d+)\)', r'db.delete(&mut txn, &\1.to_string())', content)
        content = re.sub(r'db\.delete\(&mut txn, &(\w+)\)\?', r'db.delete(&mut txn, &\1.to_string())?', content)
        
        # Fix assert_eq comparisons
        content = re.sub(r'assert_eq!\(db\.get\(&txn, &(\w+)\.to_string\(\)\)\?, Some\((\w+)\)\);', 
                        r'assert_eq!(db.get(&txn, &\1.to_string())?, Some(\2.to_string()));', content)
        content = re.sub(r'assert_eq!\(db\.get\(&txn, &(\w+)\.to_string\(\)\)\?, Some\((\w+ \* \d+)\)\);', 
                        r'assert_eq!(db.get(&txn, &\1.to_string())?, Some((\2).to_string()));', content)
        
        # Fix loop variables that should be strings
        content = re.sub(r'for i in 0\.\.(\d+) \{([^}]+)db\.([a-z_]+)\(&mut txn, (\w+)\.to_string\(\), ([^)]+)\)\?;', 
                        lambda m: f'for i in 0..{m.group(1)} {{' + m.group(2) + f'db.{m.group(3)}(&mut txn, {m.group(4)}.to_string(), {m.group(5)})?;', content)
    
    # Special handling for assert statements in delete tests
    if 'test_delete' in filepath:
        content = re.sub(r'assert_eq!\(db\.get\(&txn, &i\.to_string\(\)\)\?, Some\(i \* (\d+)\)\);', 
                        r'assert_eq!(db.get(&txn, &i.to_string())?, Some((i * \1).to_string()));', content)
        content = re.sub(r'assert_eq!\(db\.get\(&txn, &i\.to_string\(\)\)\?, Some\(i\)\);', 
                        r'assert_eq!(db.get(&txn, &i.to_string())?, Some(i.to_string()));', content)
    
    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    test_files = glob.glob('tests/**/*.rs', recursive=True)
    
    fixed_count = 0
    for filepath in test_files:
        if fix_file(filepath):
            print(f"Fixed: {filepath}")
            fixed_count += 1
    
    print(f"Fixed {fixed_count} files")

if __name__ == '__main__':
    main()