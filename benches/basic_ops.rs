//! Benchmarks for ZeroDB basic operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::cell::RefCell;
use std::collections::BTreeMap;
use tempfile::tempdir;

use zerodb::{
    btree::{CursorOps, CursorState, Node, PageBuilder, insert_into_leaf},
    error::{Error, Result},
    page::PageNo,
    EnvOpenOptions,
};

/// Test helper for managing pages.
struct PageStore {
    pages: RefCell<BTreeMap<PageNo, Vec<u8>>>,
    next_pgno: RefCell<PageNo>,
}

impl PageStore {
    fn new(start_pgno: PageNo) -> Self {
        Self {
            pages: RefCell::new(BTreeMap::new()),
            next_pgno: RefCell::new(start_pgno),
        }
    }

    fn get(&self, pgno: PageNo) -> Result<Vec<u8>> {
        self.pages
            .borrow()
            .get(&pgno)
            .cloned()
            .ok_or(Error::Corrupted)
    }

    fn alloc(&self) -> Result<PageNo> {
        let mut next = self.next_pgno.borrow_mut();
        let pgno = *next;
        *next += 1;
        Ok(pgno)
    }

    fn set(&self, pgno: PageNo, data: Vec<u8>) -> Result<()> {
        self.pages.borrow_mut().insert(pgno, data);
        Ok(())
    }
}

fn bench_env_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("env_open");

    group.bench_function("new_env", |b| {
        b.iter(|| {
            let dir = tempdir().unwrap();
            let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
            black_box(env)
        })
    });

    group.bench_function("reopen_env", |b| {
        let dir = tempdir().unwrap();
        // Create env once
        {
            let _env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
        }
        b.iter(|| {
            let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
            black_box(env)
        })
    });

    group.finish();
}

fn bench_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    let dir = tempdir().unwrap();
    let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

    group.bench_function("read_txn", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            black_box(rtxn.txnid());
            rtxn.commit().unwrap();
        })
    });

    group.bench_function("write_txn_empty", |b| {
        b.iter(|| {
            let wtxn = env.write_txn().unwrap();
            wtxn.commit().unwrap();
        })
    });

    group.bench_function("write_txn_alloc_page", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            let (pgno, _data) = wtxn.alloc_page().unwrap();
            black_box(pgno);
            wtxn.commit().unwrap();
        })
    });

    group.finish();
}

fn bench_btree_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_search");
    let page_size = 4096;

    // Create a page with various numbers of keys
    // Note: A 4096 byte page can fit ~100-150 keys depending on size
    for num_keys in [10, 50, 100] {
        let store = PageStore::new(2);
        let root_pgno = store.alloc().unwrap();
        let mut builder = PageBuilder::new_leaf(root_pgno, page_size);

        for i in 0..num_keys {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{:04}", i).into_bytes();
            builder.add_leaf(&Node::leaf(key, value)).unwrap();
        }
        store.set(root_pgno, builder.finish()).unwrap();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("search_existing", num_keys),
            &num_keys,
            |b, &num_keys| {
                let search_key = format!("key{:04}", num_keys / 2).into_bytes();
                b.iter(|| {
                    let mut state = CursorState::new(root_pgno);
                    let result = CursorOps::search(
                        &mut state,
                        &search_key,
                        page_size,
                        |pgno| store.get(pgno),
                    ).unwrap();
                    black_box(result)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("search_missing", num_keys),
            &num_keys,
            |b, _| {
                let search_key = b"nonexistent_key";
                b.iter(|| {
                    let mut state = CursorState::new(root_pgno);
                    let result = CursorOps::search(
                        &mut state,
                        search_key,
                        page_size,
                        |pgno| store.get(pgno),
                    ).unwrap();
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_btree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert");
    let page_size = 4096;

    group.bench_function("insert_single", |b| {
        b.iter(|| {
            let store = PageStore::new(2);
            let root_pgno = store.alloc().unwrap();
            let builder = PageBuilder::new_leaf(root_pgno, page_size);
            store.set(root_pgno, builder.finish()).unwrap();

            let leaf_data = store.get(root_pgno).unwrap();
            let node = Node::leaf(b"testkey".to_vec(), b"testvalue".to_vec());
            let (new_data, _split) = insert_into_leaf(
                &leaf_data,
                node,
                0,
                root_pgno,
                page_size,
            ).unwrap();
            store.set(root_pgno, new_data).unwrap();
            black_box(store)
        })
    });

    group.bench_function("insert_100_sequential", |b| {
        b.iter(|| {
            let store = PageStore::new(2);
            let root_pgno = store.alloc().unwrap();
            let builder = PageBuilder::new_leaf(root_pgno, page_size);
            store.set(root_pgno, builder.finish()).unwrap();

            for i in 0..100 {
                let leaf_data = store.get(root_pgno).unwrap();
                let key = format!("key{:04}", i).into_bytes();
                let value = format!("val{:04}", i).into_bytes();

                let mut state = CursorState::new(root_pgno);
                let result = CursorOps::search(&mut state, &key, page_size, |pgno| store.get(pgno)).unwrap();
                let insert_index = result.index();

                let node = Node::leaf(key, value);
                let (new_data, _split) = insert_into_leaf(
                    &leaf_data,
                    node,
                    insert_index,
                    root_pgno,
                    page_size,
                ).unwrap();
                store.set(root_pgno, new_data).unwrap();
            }
            black_box(store)
        })
    });

    group.finish();
}

fn bench_cursor_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor_iteration");
    let page_size = 4096;

    for num_keys in [10, 50, 100] {
        let store = PageStore::new(2);
        let root_pgno = store.alloc().unwrap();
        let mut builder = PageBuilder::new_leaf(root_pgno, page_size);

        for i in 0..num_keys {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{:04}", i).into_bytes();
            builder.add_leaf(&Node::leaf(key, value)).unwrap();
        }
        store.set(root_pgno, builder.finish()).unwrap();

        group.throughput(Throughput::Elements(num_keys as u64));
        group.bench_with_input(
            BenchmarkId::new("iterate_forward", num_keys),
            &num_keys,
            |b, _| {
                b.iter(|| {
                    let mut state = CursorState::new(root_pgno);
                    CursorOps::first(&mut state, page_size, |pgno| store.get(pgno)).unwrap();

                    let mut count = 0;
                    loop {
                        let leaf_data = store.get(state.leaf_pgno().unwrap()).unwrap();
                        if let Some((k, _v)) = CursorOps::get_current(&state, &leaf_data, page_size).unwrap() {
                            black_box(k);
                            count += 1;
                        }
                        if !CursorOps::next(&mut state, page_size, |pgno| store.get(pgno)).unwrap() {
                            break;
                        }
                    }
                    black_box(count)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("iterate_backward", num_keys),
            &num_keys,
            |b, _| {
                b.iter(|| {
                    let mut state = CursorState::new(root_pgno);
                    CursorOps::last(&mut state, page_size, |pgno| store.get(pgno)).unwrap();

                    let mut count = 0;
                    loop {
                        let leaf_data = store.get(state.leaf_pgno().unwrap()).unwrap();
                        if let Some((k, _v)) = CursorOps::get_current(&state, &leaf_data, page_size).unwrap() {
                            black_box(k);
                            count += 1;
                        }
                        if !CursorOps::prev(&mut state, page_size, |pgno| store.get(pgno)).unwrap() {
                            break;
                        }
                    }
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

fn bench_page_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_building");
    let page_size = 4096;

    group.bench_function("build_leaf_10_nodes", |b| {
        b.iter(|| {
            let mut builder = PageBuilder::new_leaf(1, page_size);
            for i in 0..10 {
                let key = format!("key{:04}", i).into_bytes();
                let value = format!("value{:04}", i).into_bytes();
                builder.add_leaf(&Node::leaf(key, value)).unwrap();
            }
            black_box(builder.finish())
        })
    });

    group.bench_function("build_leaf_50_nodes", |b| {
        b.iter(|| {
            let mut builder = PageBuilder::new_leaf(1, page_size);
            for i in 0..50 {
                let key = format!("key{:04}", i).into_bytes();
                let value = format!("value{:04}", i).into_bytes();
                builder.add_leaf(&Node::leaf(key, value)).unwrap();
            }
            black_box(builder.finish())
        })
    });

    group.bench_function("build_branch_10_nodes", |b| {
        b.iter(|| {
            let mut builder = PageBuilder::new_branch(1, page_size);
            for i in 0..10 {
                let key = format!("key{:04}", i).into_bytes();
                builder.add_branch(&Node::branch(key, i as u64)).unwrap();
            }
            black_box(builder.finish())
        })
    });

    group.finish();
}

fn bench_node_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_serialization");

    group.bench_function("leaf_node_write", |b| {
        let node = Node::leaf(b"testkey".to_vec(), b"testvalue".to_vec());
        b.iter(|| {
            let mut buf = vec![0u8; 100];
            let size = node.write_leaf(&mut buf).unwrap();
            black_box(size)
        })
    });

    group.bench_function("branch_node_write", |b| {
        let node = Node::branch(b"testkey".to_vec(), 12345);
        b.iter(|| {
            let mut buf = vec![0u8; 100];
            let size = node.write_branch(&mut buf).unwrap();
            black_box(size)
        })
    });

    group.bench_function("leaf_node_parse", |b| {
        let node = Node::leaf(b"testkey".to_vec(), b"testvalue".to_vec());
        let mut buf = vec![0u8; 100];
        let size = node.write_leaf(&mut buf).unwrap();
        b.iter(|| {
            let parsed = zerodb::btree::NodeRef::parse_leaf(&buf[..size]).unwrap();
            black_box(parsed.key().len())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_env_open,
    bench_transactions,
    bench_btree_search,
    bench_btree_insert,
    bench_cursor_iteration,
    bench_page_building,
    bench_node_serialization,
);

criterion_main!(benches);
