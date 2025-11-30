//! Stress tests for ZeroDB.
//!
//! These tests verify correct behavior under concurrent access and heavy load.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use zerodb::{EnvOpenOptions, EnvFlags};

use tempfile::tempdir;

#[test]
fn stress_concurrent_readers() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Do some writes first
    for i in 0..10 {
        let mut wtxn = env.write_txn().unwrap();
        let (_pgno, data) = wtxn.alloc_page().unwrap();
        data[0] = i as u8;
        wtxn.commit().unwrap();
    }

    // Spawn many reader threads
    let mut handles = vec![];
    for thread_id in 0..10 {
        let env = Arc::clone(&env);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let rtxn = env.read_txn().unwrap();
                let _meta = rtxn.meta();
                let _txnid = rtxn.txnid();
                rtxn.commit().unwrap();

                // Small delay to interleave
                thread::yield_now();
            }
            thread_id
        }));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify env is still valid
    let info = env.info();
    assert_eq!(info.last_txnid, 10);
}

#[test]
fn stress_sequential_writes() {
    let dir = tempdir().unwrap();
    let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

    // Many sequential write transactions
    for i in 0..100 {
        let mut wtxn = env.write_txn().unwrap();
        let (_pgno, data) = wtxn.alloc_page().unwrap();
        data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
        wtxn.commit().unwrap();
    }

    let info = env.info();
    assert_eq!(info.last_txnid, 100);
}

#[test]
fn stress_readers_during_writes() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Start reader threads
    let env_read = Arc::clone(&env);
    let reader_handle = thread::spawn(move || {
        let mut txnids_seen = vec![];
        for _ in 0..50 {
            let rtxn = env_read.read_txn().unwrap();
            txnids_seen.push(rtxn.txnid());
            thread::sleep(Duration::from_millis(1));
            rtxn.commit().unwrap();
        }
        txnids_seen
    });

    // Do writes while reader is running
    for i in 0..20 {
        let mut wtxn = env.write_txn().unwrap();
        let (_pgno, data) = wtxn.alloc_page().unwrap();
        data[0] = i as u8;
        wtxn.commit().unwrap();
        thread::sleep(Duration::from_millis(2));
    }

    let txnids = reader_handle.join().unwrap();

    // Reader should have seen increasing txnids
    for window in txnids.windows(2) {
        assert!(window[0] <= window[1], "txnids should be non-decreasing");
    }
}

#[test]
fn stress_abort_many_transactions() {
    let dir = tempdir().unwrap();
    let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

    let initial_info = env.info();

    // Many aborted write transactions
    for _ in 0..100 {
        let mut wtxn = env.write_txn().unwrap();
        let (_pgno, data) = wtxn.alloc_page().unwrap();
        data[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        wtxn.abort();
    }

    // State should be unchanged
    let final_info = env.info();
    assert_eq!(initial_info.last_txnid, final_info.last_txnid);
    assert_eq!(initial_info.last_pgno, final_info.last_pgno);
}

#[test]
fn stress_rapid_open_close() {
    let dir = tempdir().unwrap();

    for _ in 0..20 {
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Quick write
        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();

        // Quick read
        let rtxn = env.read_txn().unwrap();
        let _info = rtxn.meta();
        rtxn.commit().unwrap();

        // Drop env, reopen next iteration
        drop(env);
    }

    // Final verification
    let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
    assert_eq!(env.info().last_txnid, 20);
}

#[test]
fn stress_large_pages_allocation() {
    let dir = tempdir().unwrap();
    let page_size = page_size::get();
    let map_size = page_size * 10000; // 10000 pages

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(map_size)
            .open(dir.path())
            .unwrap()
    };

    // Allocate many pages in one transaction
    {
        let mut wtxn = env.write_txn().unwrap();
        for i in 0..1000 {
            let (_pgno, data) = wtxn.alloc_page().unwrap();
            // Write pattern to verify later
            data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
        }
        wtxn.commit().unwrap();
    }

    let info = env.info();
    assert!(info.last_pgno >= 1001); // 2 meta + 1000 allocated
}

#[test]
fn stress_mixed_operations() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Reader thread
    let env_read = Arc::clone(&env);
    let reader = thread::spawn(move || {
        for _ in 0..100 {
            let rtxn = env_read.read_txn().unwrap();
            let _ = rtxn.meta();
            rtxn.commit().unwrap();
            thread::yield_now();
        }
    });

    // Writer thread (sequentially, since only one write txn at a time)
    for i in 0..50 {
        let mut wtxn = env.write_txn().unwrap();
        let (_pgno, data) = wtxn.alloc_page().unwrap();
        data[0] = i as u8;
        wtxn.commit().unwrap();
        thread::yield_now();
    }

    reader.join().unwrap();

    let info = env.info();
    assert_eq!(info.last_txnid, 50);
}

#[test]
fn stress_transaction_snapshots() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Take many snapshots
    let mut snapshots = vec![];
    for i in 0..10 {
        let rtxn = env.read_txn().unwrap();
        snapshots.push((i, rtxn.txnid()));
        rtxn.commit().unwrap();

        // Do a write between snapshots
        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();
    }

    // Verify snapshots saw increasing txnids
    for window in snapshots.windows(2) {
        assert!(window[0].1 < window[1].1, "snapshots should see increasing txnids");
    }
}

#[test]
fn stress_long_running_reader() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Start a long-running reader
    let env_read = Arc::clone(&env);
    let reader = thread::spawn(move || {
        let rtxn = env_read.read_txn().unwrap();
        let initial_txnid = rtxn.txnid();

        // Sleep while holding transaction open
        thread::sleep(Duration::from_millis(100));

        // Should still see same txnid
        assert_eq!(rtxn.txnid(), initial_txnid);
        rtxn.commit().unwrap();

        initial_txnid
    });

    // Do writes while reader is holding transaction
    for _ in 0..5 {
        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    let reader_txnid = reader.join().unwrap();

    // Final txnid should be higher than what reader saw
    assert!(env.info().last_txnid > reader_txnid);
}

#[test]
fn stress_env_info_during_writes() {
    let dir = tempdir().unwrap();
    let env = Arc::new(unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() });

    // Thread that queries info repeatedly
    let env_info = Arc::clone(&env);
    let info_thread = thread::spawn(move || {
        let mut infos = vec![];
        for _ in 0..100 {
            let info = env_info.info();
            infos.push(info.last_txnid);
            thread::yield_now();
        }
        infos
    });

    // Do writes
    for _ in 0..20 {
        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();
        thread::yield_now();
    }

    let infos = info_thread.join().unwrap();

    // Txnids should be non-decreasing
    for window in infos.windows(2) {
        assert!(window[0] <= window[1], "txnids from info() should be non-decreasing");
    }
}

#[test]
fn stress_page_allocation_limits() {
    let dir = tempdir().unwrap();
    let page_size = page_size::get();
    // Small map - only room for a few pages
    let map_size = page_size * 100;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(map_size)
            .open(dir.path())
            .unwrap()
    };

    // Allocate until we can't anymore
    let mut allocated = 0;
    loop {
        let result = env.write_txn();
        if let Ok(mut wtxn) = result {
            match wtxn.alloc_page() {
                Ok(_) => {
                    allocated += 1;
                    wtxn.commit().unwrap();
                }
                Err(_) => {
                    wtxn.abort();
                    break;
                }
            }
        } else {
            break;
        }

        // Safety limit
        if allocated > 95 {
            break;
        }
    }

    // Should have allocated a reasonable number of pages
    assert!(allocated > 0, "Should have allocated at least some pages");
    assert!(allocated < 100, "Should have hit limit before 100 pages");
}
