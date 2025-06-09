//! Batched write transactions and group commit for improved throughput
//!
//! This module implements write batching where multiple write transactions
//! can be accumulated and committed together, reducing I/O overhead.

use crate::env::Environment;
use crate::error::{Error, Result};
use crate::txn::{Transaction, Write};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A write operation to be batched
pub struct BatchedWrite {
    /// The operations to perform
    ops: Vec<WriteOp>,
    /// Channel to send result back
    result_tx: Sender<Result<()>>,
}

/// Individual write operation
pub enum WriteOp {
    /// Put a key-value pair
    Put {
        /// Optional database name
        db_name: Option<String>,
        /// Key to insert
        key: Vec<u8>,
        /// Value to insert
        value: Vec<u8>,
    },
    /// Delete a key
    Delete {
        /// Optional database name
        db_name: Option<String>,
        /// Key to delete
        key: Vec<u8>,
    },
    /// Clear a database
    Clear {
        /// Optional database name to clear
        db_name: Option<String>,
    },
}

/// Batch commit configuration
pub struct BatchConfig {
    /// Maximum number of operations to batch
    pub max_batch_size: usize,
    /// Maximum time to wait before committing a batch
    pub max_batch_delay: Duration,
    /// Whether to use group commit optimization
    pub enable_group_commit: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            max_batch_delay: Duration::from_millis(10),
            enable_group_commit: true,
        }
    }
}

/// Batch committer manages write batching
pub struct BatchCommitter {
    /// Queue of pending writes
    pending: Arc<Mutex<VecDeque<BatchedWrite>>>,
    /// Condition variable for signaling
    cond: Arc<Condvar>,
    /// Configuration
    config: BatchConfig,
    /// Channel for submitting writes
    submit_tx: Sender<BatchedWrite>,
    submit_rx: Receiver<BatchedWrite>,
}

impl BatchCommitter {
    /// Create a new batch committer
    pub fn new(config: BatchConfig) -> Self {
        let (submit_tx, submit_rx) = bounded(1000);

        Self {
            pending: Arc::new(Mutex::new(VecDeque::new())),
            cond: Arc::new(Condvar::new()),
            config,
            submit_tx,
            submit_rx,
        }
    }

    /// Start the batch committer background thread
    pub fn start(self, env: Arc<Environment<crate::env::state::Open>>) -> BatchCommitHandle {
        let pending = self.pending.clone();
        let cond = self.cond.clone();
        let config = self.config;
        let submit_rx = self.submit_rx;

        let handle = std::thread::spawn(move || {
            Self::commit_loop(env, pending, cond, config, submit_rx);
        });

        BatchCommitHandle { submit_tx: self.submit_tx.clone(), thread: Some(handle) }
    }

    /// Main commit loop
    fn commit_loop(
        env: Arc<Environment<crate::env::state::Open>>,
        pending: Arc<Mutex<VecDeque<BatchedWrite>>>,
        _cond: Arc<Condvar>,
        config: BatchConfig,
        submit_rx: Receiver<BatchedWrite>,
    ) {
        let mut batch = Vec::new();
        let mut last_commit = Instant::now();

        loop {
            // Collect writes for this batch
            batch.clear();

            // Wait for writes or timeout
            let timeout = config.max_batch_delay.saturating_sub(last_commit.elapsed());

            // Try to receive with timeout
            match submit_rx.recv_timeout(timeout) {
                Ok(write) => {
                    batch.push(write);

                    // Drain more writes up to batch size
                    while batch.len() < config.max_batch_size {
                        match submit_rx.try_recv() {
                            Ok(write) => batch.push(write),
                            Err(_) => break,
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Check if we have pending writes to commit
                    let mut pending_guard = pending.lock().unwrap();
                    if !pending_guard.is_empty() {
                        // Move pending to batch
                        while let Some(write) = pending_guard.pop_front() {
                            batch.push(write);
                            if batch.len() >= config.max_batch_size {
                                break;
                            }
                        }
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    // Channel closed, exit
                    break;
                }
            }

            // Commit the batch if we have writes
            if !batch.is_empty() {
                Self::commit_batch(&env, &mut batch, config.enable_group_commit);
                last_commit = Instant::now();
            }
        }
    }

    /// Commit a batch of writes
    fn commit_batch(
        env: &Environment<crate::env::state::Open>,
        batch: &mut Vec<BatchedWrite>,
        enable_group_commit: bool,
    ) {
        // Start a write transaction
        let mut txn = match env.begin_write_txn() {
            Ok(txn) => txn,
            Err(e) => {
                // Send error to all writers
                for write in batch.drain(..) {
                    let _ = write.result_tx.send(Err(e.clone()));
                }
                return;
            }
        };

        // Apply all operations
        let mut results = Vec::new();
        for write in batch.iter() {
            let result = Self::apply_write_ops(&mut txn, &write.ops);
            results.push(result);
        }

        // Commit the transaction
        let commit_result = if enable_group_commit {
            // Group commit optimization: sync only once for all writes
            txn.commit()
        } else {
            txn.commit()
        };

        // Send results back
        for (write, op_result) in batch.drain(..).zip(results.drain(..)) {
            let final_result = match op_result {
                Ok(()) => commit_result.clone().map_err(|e| e.into()),
                Err(e) => Err(e),
            };
            let _ = write.result_tx.send(final_result);
        }
    }

    /// Apply write operations to a transaction
    fn apply_write_ops(txn: &mut Transaction<'_, Write>, ops: &[WriteOp]) -> Result<()> {
        for op in ops {
            match op {
                WriteOp::Put { db_name, key, value } => {
                    // Get database
                    let mut db_info = *txn.db_info(db_name.as_deref())?;
                    let mut root = db_info.root;

                    // Insert using btree - it returns the old value if key existed
                    let _old_value = crate::btree::BTree::<
                        crate::comparator::LexicographicComparator,
                    >::insert(
                        txn, &mut root, &mut db_info, key, value
                    )?;
                    db_info.root = root;

                    // Update db info (insert already updated entries count and root if needed)
                    txn.update_db_info(db_name.as_deref(), db_info)?;
                }
                WriteOp::Delete { db_name, key } => {
                    // Get database
                    let mut db_info = *txn.db_info(db_name.as_deref())?;
                    let mut root = db_info.root;

                    // Delete using btree - it returns the old value if key existed
                    let _old_value = crate::btree::BTree::<
                        crate::comparator::LexicographicComparator,
                    >::delete(
                        txn, &mut root, &mut db_info, key
                    )?;
                    db_info.root = root;

                    // Update db info (delete already updated entries count)
                    txn.update_db_info(db_name.as_deref(), db_info)?;
                }
                WriteOp::Clear { db_name } => {
                    // Get database
                    let mut db_info = *txn.db_info(db_name.as_deref())?;

                    // Clear by creating new empty root
                    let (new_root_id, _new_root) = txn.alloc_page(crate::page::PageFlags::LEAF)?;
                    db_info.root = new_root_id;
                    db_info.entries = 0;
                    db_info.depth = 1;
                    db_info.leaf_pages = 1;
                    db_info.branch_pages = 0;
                    db_info.overflow_pages = 0;
                    txn.update_db_info(db_name.as_deref(), db_info)?;
                }
            }
        }

        Ok(())
    }
}

/// Handle for submitting batched writes
pub struct BatchCommitHandle {
    /// Channel for submitting writes
    submit_tx: Sender<BatchedWrite>,
    /// Background thread handle
    thread: Option<std::thread::JoinHandle<()>>,
}

impl BatchCommitHandle {
    /// Submit a write operation
    pub fn write(&self, ops: Vec<WriteOp>) -> Result<()> {
        let (result_tx, result_rx) = bounded(1);

        let write = BatchedWrite { ops, result_tx };

        // Submit the write
        self.submit_tx
            .send(write)
            .map_err(|_| Error::Custom("Batch committer shut down".into()))?;

        // Wait for result
        result_rx.recv().map_err(|_| Error::Custom("Failed to receive commit result".into()))?
    }

    /// Submit a single put operation
    pub fn put(&self, db_name: Option<String>, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write(vec![WriteOp::Put { db_name, key, value }])
    }

    /// Submit a single delete operation
    pub fn delete(&self, db_name: Option<String>, key: Vec<u8>) -> Result<()> {
        self.write(vec![WriteOp::Delete { db_name, key }])
    }
}

impl Drop for BatchCommitHandle {
    fn drop(&mut self) {
        // Signal shutdown by dropping the sender
        // The receiver will get disconnected error and exit
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_batch_commit() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(crate::env::EnvBuilder::new().open(dir.path()).unwrap());

        // Create batch committer
        let config = BatchConfig {
            max_batch_size: 10,
            max_batch_delay: Duration::from_millis(100),
            enable_group_commit: true,
        };

        let committer = BatchCommitter::new(config);
        let handle = committer.start(env.clone());

        // Submit some writes
        for i in 0..20 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            handle.put(None, key, value).unwrap();
        }

        // Verify writes
        let txn = env.begin_txn().unwrap();
        for i in 0..20 {
            let key = format!("key{}", i).into_bytes();
            let db_info = txn.db_info(None).unwrap();
            let value = crate::btree::BTree::<crate::comparator::LexicographicComparator>::search(
                &txn,
                db_info.root,
                &key,
            )
            .unwrap();
            assert!(value.is_some());
        }
    }
}
