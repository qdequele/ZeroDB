//! The engine interface every store implements, and the per-engine adapters.
//!
//! Every adapter uses the engine's documented defaults. The only settings
//! changed are the ones that must match for a fair comparison: the cache size
//! (engines that keep their own cache; LMDB and ZeroDB use the OS page cache)
//! and the durability of a commit. `README.md` lists each setting.

use std::path::Path;

/// One key-value pair handed to [`Engine::write`].
pub type Pair<'a> = (&'a [u8], &'a [u8]);

/// Settings shared by every engine.
#[derive(Clone, Debug)]
pub struct Config {
    /// Each commit waits until its data is on disk.
    pub durable: bool,
    /// Size of the engine's own cache, for engines that keep one.
    pub cache_bytes: u64,
    /// Threads that use the engine at once: the readers plus the writer, if
    /// any. An engine whose cache belongs to one connection (SQLite) splits
    /// `cache_bytes` among them, so its total stays within the budget.
    pub connections: usize,
    /// Map size for the memory-mapped engines (LMDB, ZeroDB).
    pub map_bytes: usize,
}

/// A store under test. Shared by all worker threads of a phase.
pub trait Engine: Send + Sync {
    /// Insert or overwrite every pair in one atomic commit.
    fn write(&self, pairs: &[Pair<'_>]);
    /// A handle for one reader thread.
    fn reader(&self) -> Box<dyn Reader + '_>;
    /// Make everything written so far durable.
    fn sync(&self);
}

/// Reads made from one thread. Each call is its own read transaction or
/// snapshot, the way a server answers independent requests.
pub trait Reader {
    /// Value length for `key`, if present.
    fn get(&mut self, key: &[u8]) -> Option<usize>;
    /// Read up to `n` entries in key order starting at `start`; returns the
    /// total value bytes read.
    fn range(&mut self, start: &[u8], n: usize) -> usize;
}

/// Engine names accepted on the command line.
pub const ENGINES: &[&str] = &[
    "lmdb",
    "zerodb",
    "zerodb-trusted",
    "fjall",
    "rocksdb",
    "redb",
    "sqlite",
];

/// Open (creating if absent) the engine `name` in `dir`.
pub fn open(name: &str, dir: &Path, cfg: &Config) -> Box<dyn Engine> {
    std::fs::create_dir_all(dir).expect("create data dir");
    match name {
        "lmdb" => Box::new(lmdb::Heed::open(dir, cfg, false)),
        "zerodb" => Box::new(zerodb::Heed::open(dir, cfg, false)),
        "zerodb-trusted" => Box::new(zerodb::Heed::open(dir, cfg, true)),
        "fjall" => Box::new(Fjall::open(dir, cfg)),
        "rocksdb" => Box::new(Rocks::open(dir, cfg)),
        "redb" => Box::new(Redb::open(dir, cfg)),
        "sqlite" => Box::new(Sqlite::open(dir, cfg)),
        other => panic!("unknown engine {other}; expected one of {ENGINES:?}"),
    }
}

// ---------------------------------------------------------------------------
// LMDB and ZeroDB: one body over the two API-identical heed crates, so neither
// gets code the other does not.
// ---------------------------------------------------------------------------

macro_rules! heed_engine {
    ($module:ident, $heed:ident, $trust:tt) => {
        pub mod $module {
            use std::ops::Bound;
            use std::path::Path;

            use $heed::types::Bytes;
            use $heed::{Database, Env, EnvFlags, EnvOpenOptions, WithoutTls};

            use super::{Config, Pair, Reader};

            pub struct Heed {
                env: Env<WithoutTls>,
                db: Database<Bytes, Bytes>,
            }

            impl Heed {
                #[allow(unused_variables, unused_mut)]
                pub fn open(dir: &Path, cfg: &Config, trusted: bool) -> Heed {
                    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
                    opts.map_size(cfg.map_bytes);
                    heed_engine!(@trust opts, trusted, $trust);
                    // NO_READ_AHEAD: LMDB's advice for random reads on data
                    // larger than memory; set for both engines.
                    let mut flags = EnvFlags::NO_READ_AHEAD;
                    if !cfg.durable {
                        flags |= EnvFlags::NO_SYNC;
                    }
                    // SAFETY: one process opens this private benchmark
                    // directory; NO_SYNC and NO_READ_AHEAD change durability
                    // and I/O hints, not how processes share the file.
                    let env = unsafe {
                        opts.flags(flags);
                        opts.open(dir)
                    }
                    .expect("open env");
                    let mut w = env.write_txn().expect("write_txn");
                    let db = env.create_database(&mut w, None).expect("create_database");
                    w.commit().expect("commit");
                    Heed { env, db }
                }
            }

            impl super::Engine for Heed {
                fn write(&self, pairs: &[Pair<'_>]) {
                    let mut w = self.env.write_txn().expect("write_txn");
                    for (k, v) in pairs {
                        self.db.put(&mut w, k, v).expect("put");
                    }
                    w.commit().expect("commit");
                }

                fn reader(&self) -> Box<dyn Reader + '_> {
                    Box::new(HeedReader { env: &self.env, db: self.db })
                }

                fn sync(&self) {
                    self.env.force_sync().expect("force_sync");
                }
            }

            struct HeedReader<'a> {
                env: &'a Env<WithoutTls>,
                db: Database<Bytes, Bytes>,
            }

            impl Reader for HeedReader<'_> {
                fn get(&mut self, key: &[u8]) -> Option<usize> {
                    let r = self.env.read_txn().expect("read_txn");
                    self.db.get(&r, key).expect("get").map(<[u8]>::len)
                }

                fn range(&mut self, start: &[u8], n: usize) -> usize {
                    let r = self.env.read_txn().expect("read_txn");
                    let bounds = (Bound::Included(start), Bound::Unbounded);
                    let it = self.db.range(&r, &bounds).expect("range");
                    it.take(n).map(|e| e.expect("entry").1.len()).sum()
                }
            }
        }
    };
    (@trust $opts:ident, $trusted:ident, no) => {
        assert!(!$trusted, "trusted-file mode is a ZeroDB option");
    };
    (@trust $opts:ident, $trusted:ident, yes) => {
        if $trusted {
            // SAFETY: the file was created by this benchmark with ZeroDB and
            // nothing else writes to it (the `trust_contents` contract).
            $opts.file_trust(unsafe { heed_zerodb::FileTrust::trust_contents() });
        }
    };
}

heed_engine!(lmdb, heed, no);
heed_engine!(zerodb, heed_zerodb, yes);

// ---------------------------------------------------------------------------
// fjall
// ---------------------------------------------------------------------------

pub struct Fjall {
    db: fjall::Database,
    ks: fjall::Keyspace,
    durable: bool,
}

impl Fjall {
    fn open(dir: &Path, cfg: &Config) -> Fjall {
        let db = fjall::Database::builder(dir)
            .cache_size(cfg.cache_bytes)
            .open()
            .expect("open fjall");
        let ks = db
            .keyspace("kv", fjall::KeyspaceCreateOptions::default)
            .expect("keyspace");
        Fjall {
            db,
            ks,
            durable: cfg.durable,
        }
    }
}

impl Engine for Fjall {
    fn write(&self, pairs: &[Pair<'_>]) {
        let mode = self.durable.then_some(fjall::PersistMode::SyncAll);
        let mut b = self.db.batch().durability(mode);
        for (k, v) in pairs {
            b.insert(&self.ks, *k, *v);
        }
        b.commit().expect("commit");
    }

    fn reader(&self) -> Box<dyn Reader + '_> {
        Box::new(FjallReader { ks: &self.ks })
    }

    fn sync(&self) {
        self.db
            .persist(fjall::PersistMode::SyncAll)
            .expect("persist");
    }
}

struct FjallReader<'a> {
    ks: &'a fjall::Keyspace,
}

impl Reader for FjallReader<'_> {
    fn get(&mut self, key: &[u8]) -> Option<usize> {
        self.ks.get(key).expect("get").map(|v| v.len())
    }

    fn range(&mut self, start: &[u8], n: usize) -> usize {
        self.ks
            .range(start..)
            .take(n)
            .map(|g| g.value().expect("entry").len())
            .sum()
    }
}

// ---------------------------------------------------------------------------
// RocksDB
// ---------------------------------------------------------------------------

pub struct Rocks {
    db: rocksdb::DB,
    durable: bool,
}

impl Rocks {
    fn open(dir: &Path, cfg: &Config) -> Rocks {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        // RocksDB's own tuning guide: background threads for every core and a
        // bloom filter, which fjall has by default.
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        opts.increase_parallelism(cores as i32);
        let mut table = rocksdb::BlockBasedOptions::default();
        table.set_block_cache(&rocksdb::Cache::new_lru_cache(cfg.cache_bytes as usize));
        table.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&table);
        let db = rocksdb::DB::open(&opts, dir).expect("open rocksdb");
        Rocks {
            db,
            durable: cfg.durable,
        }
    }
}

impl Engine for Rocks {
    fn write(&self, pairs: &[Pair<'_>]) {
        let mut b = rocksdb::WriteBatch::default();
        for (k, v) in pairs {
            b.put(k, v);
        }
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(self.durable);
        self.db.write_opt(b, &wo).expect("write");
    }

    fn reader(&self) -> Box<dyn Reader + '_> {
        Box::new(RocksReader { db: &self.db })
    }

    fn sync(&self) {
        self.db.flush_wal(true).expect("flush_wal");
    }
}

struct RocksReader<'a> {
    db: &'a rocksdb::DB,
}

impl Reader for RocksReader<'_> {
    fn get(&mut self, key: &[u8]) -> Option<usize> {
        self.db.get_pinned(key).expect("get").map(|v| v.len())
    }

    fn range(&mut self, start: &[u8], n: usize) -> usize {
        let mut it = self.db.raw_iterator();
        it.seek(start);
        let mut bytes = 0;
        for _ in 0..n {
            match it.value() {
                Some(v) => bytes += v.len(),
                None => break,
            }
            it.next();
        }
        it.status().expect("iterator");
        bytes
    }
}

// ---------------------------------------------------------------------------
// redb
// ---------------------------------------------------------------------------

const REDB_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");

pub struct Redb {
    db: redb::Database,
    durable: bool,
}

impl Redb {
    fn open(dir: &Path, cfg: &Config) -> Redb {
        let db = redb::Builder::new()
            .set_cache_size(cfg.cache_bytes as usize)
            .create(dir.join("data.redb"))
            .expect("open redb");
        let w = db.begin_write().expect("begin_write");
        w.open_table(REDB_TABLE).expect("open_table");
        w.commit().expect("commit");
        Redb {
            db,
            durable: cfg.durable,
        }
    }
}

impl Engine for Redb {
    fn write(&self, pairs: &[Pair<'_>]) {
        let mut w = self.db.begin_write().expect("begin_write");
        let d = if self.durable {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        };
        w.set_durability(d).expect("set_durability");
        {
            let mut t = w.open_table(REDB_TABLE).expect("open_table");
            for (k, v) in pairs {
                t.insert(*k, *v).expect("insert");
            }
        }
        w.commit().expect("commit");
    }

    fn reader(&self) -> Box<dyn Reader + '_> {
        Box::new(RedbReader { db: &self.db })
    }

    fn sync(&self) {
        // An empty commit with `Immediate` durability persists every earlier
        // `None` commit.
        let w = self.db.begin_write().expect("begin_write");
        w.commit().expect("commit");
    }
}

struct RedbReader<'a> {
    db: &'a redb::Database,
}

impl Reader for RedbReader<'_> {
    fn get(&mut self, key: &[u8]) -> Option<usize> {
        use redb::ReadableDatabase;
        let r = self.db.begin_read().expect("begin_read");
        let t = r.open_table(REDB_TABLE).expect("open_table");
        t.get(key).expect("get").map(|v| v.value().len())
    }

    fn range(&mut self, start: &[u8], n: usize) -> usize {
        use redb::ReadableDatabase;
        let r = self.db.begin_read().expect("begin_read");
        let t = r.open_table(REDB_TABLE).expect("open_table");
        t.range(start..)
            .expect("range")
            .take(n)
            .map(|e| e.expect("entry").1.value().len())
            .sum()
    }
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

pub struct Sqlite {
    path: std::path::PathBuf,
    writer: std::sync::Mutex<rusqlite::Connection>,
    cache_kib: u64,
}

impl Sqlite {
    fn open(dir: &Path, cfg: &Config) -> Sqlite {
        let path = dir.join("data.sqlite");
        // Each connection keeps its own page cache: share the budget.
        let cache_kib = cfg.cache_bytes / 1024 / cfg.connections.max(1) as u64;
        let c = Sqlite::connect(&path, cache_kib);
        // WAL lets readers run while the writer commits. FULL syncs the log on
        // every commit; NORMAL syncs it only at checkpoints.
        let sync = if cfg.durable { "FULL" } else { "NORMAL" };
        c.execute_batch(&format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = {sync};
             CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;"
        ))
        .expect("init sqlite");
        let mode: String = c
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .expect("journal_mode");
        let level: i64 = c
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .expect("synchronous");
        assert_eq!(mode, "wal", "SQLite journal mode");
        assert_eq!(
            level,
            if cfg.durable { 2 } else { 1 },
            "SQLite synchronous level"
        );
        Sqlite {
            path,
            writer: std::sync::Mutex::new(c),
            cache_kib,
        }
    }

    fn connect(path: &Path, cache_kib: u64) -> rusqlite::Connection {
        let c = rusqlite::Connection::open(path).expect("open sqlite");
        c.execute_batch(&format!("PRAGMA cache_size = -{cache_kib};"))
            .expect("cache_size");
        c
    }
}

impl Engine for Sqlite {
    fn write(&self, pairs: &[Pair<'_>]) {
        let mut c = self.writer.lock().expect("writer lock");
        let tx = c.transaction().expect("begin");
        {
            let mut st = tx
                .prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")
                .expect("prepare");
            for (k, v) in pairs {
                st.execute((k, v)).expect("insert");
            }
        }
        tx.commit().expect("commit");
    }

    fn reader(&self) -> Box<dyn Reader + '_> {
        Box::new(SqliteReader {
            c: Sqlite::connect(&self.path, self.cache_kib),
        })
    }

    fn sync(&self) {
        let c = self.writer.lock().expect("writer lock");
        c.execute_batch("PRAGMA wal_checkpoint(FULL);")
            .expect("checkpoint");
    }
}

struct SqliteReader {
    c: rusqlite::Connection,
}

impl Reader for SqliteReader {
    fn get(&mut self, key: &[u8]) -> Option<usize> {
        // Select the value itself, not `length(v)`: every engine reads the
        // value's bytes, and SQLite can answer `length` from the record header.
        let mut st = self
            .c
            .prepare_cached("SELECT v FROM kv WHERE k = ?1")
            .expect("prepare");
        let mut rows = st.query([key]).expect("query");
        rows.next()
            .expect("row")
            .map(|r| r.get_ref(0).expect("v").as_blob().expect("blob").len())
    }

    fn range(&mut self, start: &[u8], n: usize) -> usize {
        let mut st = self
            .c
            .prepare_cached("SELECT v FROM kv WHERE k >= ?1 ORDER BY k LIMIT ?2")
            .expect("prepare");
        let mut rows = st.query((start, n as i64)).expect("query");
        let mut bytes = 0;
        while let Some(r) = rows.next().expect("row") {
            bytes += r.get_ref(0).expect("v").as_blob().expect("blob").len();
        }
        bytes
    }
}
