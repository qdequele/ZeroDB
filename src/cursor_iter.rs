//! Iterator wrappers for cursors to provide heed-like iteration

use crate::{
    comparator::Comparator,
    cursor::Cursor,
    db::{Database, Key, Value},
    error::Result,
    txn::{mode::Mode, Transaction},
};
use std::ops::Range;

/// Simple iterator wrapper - user must ensure database outlives the iterator
pub struct Iter<'txn, K, V, C>
where
    K: Key,
    V: Value,
    C: Comparator,
{
    cursor: Cursor<'txn, K, V, C>,
}

impl<'txn, K, V, C> Iter<'txn, K, V, C>
where
    K: Key,
    V: Value,
    C: Comparator,
{
    /// Create a new iterator
    /// 
    /// # Safety
    /// The database must outlive the transaction and iterator
    pub fn new<M: Mode>(db: &'txn Database<K, V, C>, txn: &'txn Transaction<'txn, M>) -> Result<Self> {
        let cursor = db.cursor(txn)?;
        Ok(Self { cursor })
    }
}

impl<'txn, K, V, C> Iterator for Iter<'txn, K, V, C>
where
    K: Key,
    V: Value,
    C: Comparator,
{
    type Item = Result<(Vec<u8>, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next() {
            Ok(Some((key, value))) => Some(Ok((key, value))),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Convenience function to create an iterator
/// 
/// This provides a heed-compatible API for iteration.
/// 
/// # Example
/// ```ignore
/// // Instead of using cursors directly:
/// let mut cursor = db.cursor(&txn)?;
/// while let Some((key, value)) = cursor.next()? {
///     // process
/// }
/// 
/// // You can use iterators:
/// for result in iter(&db, &txn)? {
///     let (key, value) = result?;
///     // process
/// }
/// ```
pub fn iter<'txn, K, V, C, M>(
    db: &'txn Database<K, V, C>,
    txn: &'txn Transaction<'txn, M>,
) -> Result<Iter<'txn, K, V, C>>
where
    K: Key,
    V: Value,
    C: Comparator,
    M: Mode,
{
    Iter::new(db, txn)
}

/// Create an iterator starting from a specific key
pub fn iter_from<'txn, K, V, C, M>(
    db: &'txn Database<K, V, C>,
    txn: &'txn Transaction<'txn, M>,
    start_key: &K,
) -> Result<Iter<'txn, K, V, C>>
where
    K: Key,
    V: Value,
    C: Comparator,
    M: Mode,
{
    let mut cursor = db.cursor(txn)?;
    cursor.seek(start_key)?;
    Ok(Iter { cursor })
}

/// Create a range iterator
pub fn range<'txn, K, V, C, M>(
    db: &'txn Database<K, V, C>,
    txn: &'txn Transaction<'txn, M>,
    range: Range<&K>,
) -> Result<impl Iterator<Item = Result<(Vec<u8>, V)>> + 'txn>
where
    K: Key + PartialOrd,
    V: Value + 'txn,
    C: Comparator,
    M: Mode,
{
    let mut cursor = db.cursor(txn)?;
    cursor.seek(range.start)?;
    
    // Pre-encode the end key to avoid repeated encoding
    let end_bytes = range.end.encode()?;
    
    Ok(std::iter::from_fn(move || {
        match cursor.current() {
            Ok(Some((key, value))) => {
                // Check if we're past the range
                if key >= end_bytes {
                    return None;
                }
                
                // Move to next for subsequent calls
                let result = Some(Ok((key.clone(), value)));
                let _ = cursor.next();
                result
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::DatabaseFlags,
        env::EnvBuilder,
    };
    use tempfile::TempDir;

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let env = EnvBuilder::new().open(temp_dir.path())?;
        let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::empty())?;

        // Insert test data
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            db.put(&mut txn, key, value)?;
        }
        txn.commit()?;

        // Test basic iteration
        let txn = env.read_txn()?;
        let items: Vec<_> = iter(&db, &txn)?.collect::<Result<Vec<_>>>()?;
        assert_eq!(items.len(), 10);
        assert_eq!(items[0].0, b"key00");
        assert_eq!(items[9].0, b"key09");
        
        // Test range iteration
        let start = b"key02".to_vec();
        let end = b"key05".to_vec();
        let items: Vec<_> = range(&db, &txn, &start..&end)?.collect::<Result<Vec<_>>>()?;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"key02");
        assert_eq!(items[2].0, b"key04");

        Ok(())
    }
}