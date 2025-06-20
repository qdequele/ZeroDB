#!/bin/bash

# Fix Database<u32 occurrences in test files
find tests -name "*.rs" -type f -exec sed -i '' \
    -e 's/Database<u32, u32>/Database<String, String>/g' \
    -e 's/Database<u32, Vec<u8>>/Database<String, Vec<u8>>/g' \
    {} +

# Fix common patterns where u32 is used as key/value
find tests -name "*.rs" -type f -exec sed -i '' \
    -e 's/db\.put(&mut txn, i,/db.put(\&mut txn, i.to_string(),/g' \
    -e 's/db\.put(&mut txn, \([0-9]\+\),/db.put(\&mut txn, \1.to_string(),/g' \
    -e 's/db\.get(&txn, &i)/db.get(\&txn, \&i.to_string())/g' \
    -e 's/db\.get(&txn, &\([0-9]\+\))/db.get(\&txn, \&\1.to_string())/g' \
    -e 's/db\.delete(&mut txn, &i)/db.delete(\&mut txn, \&i.to_string())/g' \
    -e 's/db\.delete(&mut txn, &\([0-9]\+\))/db.delete(\&mut txn, \&\1.to_string())/g' \
    {} +

# Fix value conversions for u32 -> String
find tests -name "*.rs" -type f -exec sed -i '' \
    -e 's/, i)/, i.to_string())/g' \
    -e 's/, i \* 10)/, (i * 10).to_string())/g' \
    -e 's/, \([0-9]\+\))/, \1.to_string())/g' \
    {} +

echo "Fixed u32 usage in test files"