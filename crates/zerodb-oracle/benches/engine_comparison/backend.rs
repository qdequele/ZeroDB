//! The two backends, generated from ONE macro body.
//!
//! Fairness is structural: every operation below exists exactly once in source
//! and is expanded over the two API-identical crate paths (`heed` = the
//! Meilisearch LMDB fork, `heed_zerodb` = the zerodb adapter). Neither engine
//! can be given a hand-tuned body, because there is only one body.
//!
//! `$setpage` is the single unavoidable asymmetry: LMDB derives its page size
//! from the OS and exposes no selector, so zerodb is *pinned* to that same
//! value. Without it a 4 KiB-vs-16 KiB geometry gap would swamp everything.

/// Generate one `mod` per backend with an identical op surface.
macro_rules! bench_backend {
    ($mod:ident, $heed:ident, $setpage:tt) => {
        pub mod $mod {
            use std::fs::File;
            use std::hint::black_box;
            use std::io::Write as _;
            use std::ops::Bound;
            use std::path::Path;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            use std::time::{Duration, Instant};

            use $heed::types::Bytes;
            use $heed::{
                CompactionOption, Database, Env, EnvFlags, EnvOpenOptions, PutFlags, WithoutTls,
            };

            pub type Db = Database<Bytes, Bytes>;
            pub type BEnv = Env<WithoutTls>;

            // ---------------------------------------------------------------
            // env / database lifecycle
            // ---------------------------------------------------------------

            /// Open (creating if absent) an env at `dir`.
            pub fn open(dir: &Path, map_size: usize, no_sync: bool, page: u32) -> BEnv {
                let mut opts = EnvOpenOptions::new().read_txn_without_tls();
                opts.map_size(map_size);
                opts.max_dbs(16);
                bench_backend!(@setpage opts, page, $setpage);
                // SAFETY: single-process, private temp dir; the only flag ever
                // set is NO_SYNC (durability, not a cross-process behavior).
                unsafe {
                    if no_sync {
                        opts.flags(EnvFlags::NO_SYNC);
                    }
                    opts.open(dir)
                }
                .expect("open env")
            }

            /// Create one database. `None` = the unnamed root DB.
            pub fn create_db(env: &BEnv, name: Option<&str>) -> Db {
                let mut w = env.write_txn().expect("write_txn");
                let db = env
                    .create_database::<Bytes, Bytes>(&mut w, name)
                    .expect("create_database");
                w.commit().expect("commit");
                db
            }

            /// Create every named database in one txn, in order.
            pub fn create_dbs(env: &BEnv, names: &[String]) -> Vec<Db> {
                let mut w = env.write_txn().expect("write_txn");
                let dbs = names
                    .iter()
                    .map(|n| {
                        env.create_database::<Bytes, Bytes>(&mut w, Some(n.as_str()))
                            .expect("create_database")
                    })
                    .collect();
                w.commit().expect("commit");
                dbs
            }

            /// Create one env per directory in `dirs`, with one named DB, and
            /// close it — env setup cost per lifetime.
            ///
            /// One directory per env, not one directory reused: reopening the
            /// same path is a *reopen*, and that is `reopen`'s rung. The
            /// directories are made in the caller's untimed setup so only the
            /// engine work is measured.
            pub fn open_create_close(dirs: &[std::path::PathBuf], map_size: usize, page: u32) {
                for d in dirs {
                    let e = open(d, map_size, true, page);
                    let _ = create_db(&e, Some("bench"));
                    drop(e);
                }
            }

            /// Reopen an already-populated env `n` times (no create) — the cost
            /// of mapping and validating an existing image.
            pub fn reopen(dir: &Path, map_size: usize, page: u32, n: usize) {
                for _ in 0..n {
                    let e = open(dir, map_size, true, page);
                    drop(e);
                }
            }

            /// `n` read txns begun and dropped — reader-table slot pin/unpin.
            pub fn rotxn_churn(env: &BEnv, n: usize) -> usize {
                let mut live = 0usize;
                for _ in 0..n {
                    let r = env.read_txn().expect("read_txn");
                    live += 1;
                    drop(r);
                }
                live
            }

            /// `n` empty write txns begun and committed — the commit floor.
            pub fn empty_commit_churn(env: &BEnv, n: usize) {
                for _ in 0..n {
                    let w = env.write_txn().expect("write_txn");
                    w.commit().expect("commit");
                }
            }

            // ---------------------------------------------------------------
            // write path
            // ---------------------------------------------------------------

            /// Insert every key in ONE write txn, then commit (bulk-write shape).
            pub fn bulk_put(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                let mut w = env.write_txn().expect("write_txn");
                for k in keys {
                    db.put(&mut w, k.as_slice(), val).expect("put");
                }
                w.commit().expect("commit");
            }

            /// Same, with `MDB_APPEND` — keys MUST be ascending.
            pub fn bulk_put_append(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                let mut w = env.write_txn().expect("write_txn");
                for k in keys {
                    db.put_with_flags(&mut w, PutFlags::APPEND, k.as_slice(), val)
                        .expect("put append");
                }
                w.commit().expect("commit");
            }

            /// Same, through `MDB_RESERVE` (milli's document-serialization path).
            pub fn bulk_put_reserved(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                let mut w = env.write_txn().expect("write_txn");
                for k in keys {
                    db.put_reserved(&mut w, k.as_slice(), val.len(), |space| {
                        space.write_all(val)
                    })
                    .expect("put_reserved");
                }
                w.commit().expect("commit");
            }

            /// `per_txn` puts per committed txn — isolates commit / barrier cost
            /// and, as `per_txn` grows, dirty-page write amplification.
            pub fn commit_churn(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8], per_txn: usize) {
                for chunk in keys.chunks(per_txn.max(1)) {
                    let mut w = env.write_txn().expect("write_txn");
                    for k in chunk {
                        db.put(&mut w, k.as_slice(), val).expect("put");
                    }
                    w.commit().expect("commit");
                }
            }

            /// Delete every listed key in one txn; returns the hit count.
            pub fn delete_keys(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                let mut w = env.write_txn().expect("write_txn");
                let mut hits = 0usize;
                for k in keys {
                    if db.delete(&mut w, k.as_slice()).expect("delete") {
                        hits += 1;
                    }
                }
                w.commit().expect("commit");
                hits
            }

            /// `delete_range` over `[lo, hi)` in one txn.
            pub fn delete_range(env: &BEnv, db: Db, lo: &[u8], hi: &[u8]) -> usize {
                let mut w = env.write_txn().expect("write_txn");
                // `Range<&[u8]>` cannot implement `RangeBounds<[u8]>` (the impl
                // requires `Sized`); the `Bound` tuple is the unsized-key form,
                // and it states the half-open interval outright.
                let span = (Bound::Included(lo), Bound::Excluded(hi));
                let n = db.delete_range(&mut w, &span).expect("delete_range");
                w.commit().expect("commit");
                n
            }

            /// Drain `[keys.first(), keys.last())` through the WRITE CURSOR —
            /// `range_mut` + `del_current`, the loop milli writes at each of
            /// its nine `del_current` call sites. Distinct from
            /// `delete_range`, which materialises the key set and issues point
            /// deletes: this is the path PERF-GAP B8a is about, and the only
            /// rung that exercises post-delete cursor position (SPEC 03 §5.4a).
            pub fn cursor_drain(env: &BEnv, db: Db, keys: &[Vec<u8>], _val: &[u8]) {
                let (Some(lo), Some(hi)) = (keys.first(), keys.last()) else {
                    return;
                };
                let mut w = env.write_txn().expect("write_txn");
                {
                    let span = (
                        Bound::Included(lo.as_slice()),
                        Bound::Excluded(hi.as_slice()),
                    );
                    let mut it = db.range_mut(&mut w, &span).expect("range_mut");
                    while it.next().is_some() {
                        // SAFETY: the entry borrow from `next` is dropped with
                        // the condition's temporary, before this call.
                        unsafe { it.del_current() }.expect("del_current");
                    }
                }
                w.commit().expect("commit");
            }

            /// Drop every entry (`MDB_DROP` with the handle kept).
            pub fn clear_db(env: &BEnv, db: Db) {
                let mut w = env.write_txn().expect("write_txn");
                db.clear(&mut w).expect("clear");
                w.commit().expect("commit");
            }

            /// Delete then re-insert the same keys, one committed txn each way,
            /// `rounds` times — freelist / GC churn (pages must be reclaimed and
            /// handed back out).
            pub fn delete_reinsert(
                env: &BEnv,
                db: Db,
                keys: &[Vec<u8>],
                val: &[u8],
                rounds: usize,
            ) {
                for _ in 0..rounds {
                    let mut w = env.write_txn().expect("write_txn");
                    for k in keys {
                        db.delete(&mut w, k.as_slice()).expect("delete");
                    }
                    w.commit().expect("commit");
                    let mut w = env.write_txn().expect("write_txn");
                    for k in keys {
                        db.put(&mut w, k.as_slice(), val).expect("put");
                    }
                    w.commit().expect("commit");
                }
            }

            // ---------------------------------------------------------------
            // read path
            // ---------------------------------------------------------------

            /// Point lookups; returns the hit count so the reads can't be elided.
            pub fn point_get(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut hits = 0usize;
                for k in keys {
                    if db.get(&r, k.as_slice()).expect("get").is_some() {
                        hits += 1;
                    }
                }
                hits
            }

            /// Point lookups that also READ the returned value's first and last
            /// byte through `black_box` — unlike `point_get`, this cannot let an
            /// overflow-value rung (`v4k`, `v2page`) skip the actual page-chase
            /// and memcpy of the value bytes.
            pub fn point_get_touch(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut hits = 0usize;
                for k in keys {
                    if let Some(v) = db.get(&r, k.as_slice()).expect("get") {
                        if !v.is_empty() {
                            black_box(v[0]);
                            black_box(v[v.len() - 1]);
                        }
                        hits += 1;
                    }
                }
                hits
            }

            /// Point lookups spread over `dbs` round-robin — the multi-named-DB
            /// rung (catalog resolution repeated against different records).
            pub fn point_get_multi(env: &BEnv, dbs: &[Db], keys: &[Vec<u8>]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut hits = 0usize;
                for (i, k) in keys.iter().enumerate() {
                    if dbs[i % dbs.len()]
                        .get(&r, k.as_slice())
                        .expect("get")
                        .is_some()
                    {
                        hits += 1;
                    }
                }
                hits
            }

            /// The same key `n` times in one txn — pure per-call overhead with
            /// every page already resident and every memo warm.
            pub fn point_get_hot(env: &BEnv, db: Db, key: &[u8], n: usize) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut bytes = 0usize;
                for _ in 0..n {
                    bytes += db.get(&r, key).expect("get").map_or(0, |v| v.len());
                }
                bytes
            }

            /// Full forward cursor scan; returns the entry count.
            pub fn scan(env: &BEnv, db: Db) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut n = 0usize;
                for kv in db.iter(&r).expect("iter") {
                    let _ = kv.expect("entry");
                    n += 1;
                }
                n
            }

            /// Full reverse cursor scan.
            pub fn rev_scan(env: &BEnv, db: Db) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut n = 0usize;
                for kv in db.rev_iter(&r).expect("rev_iter") {
                    let _ = kv.expect("entry");
                    n += 1;
                }
                n
            }

            /// Bounded range scan over `[lo, hi)`.
            pub fn range_scan(env: &BEnv, db: Db, lo: &[u8], hi: &[u8]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut n = 0usize;
                let span = (Bound::Included(lo), Bound::Excluded(hi));
                for kv in db.range(&r, &span).expect("range") {
                    let _ = kv.expect("entry");
                    n += 1;
                }
                n
            }

            /// Prefix scan.
            pub fn prefix_scan(env: &BEnv, db: Db, prefix: &[u8]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut n = 0usize;
                for kv in db.prefix_iter(&r, prefix).expect("prefix_iter") {
                    let _ = kv.expect("entry");
                    n += 1;
                }
                n
            }

            /// `first` + `last`, `n` times — leftmost / rightmost descent only.
            pub fn first_last(env: &BEnv, db: Db, n: usize) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut bytes = 0usize;
                for _ in 0..n {
                    bytes += db.first(&r).expect("first").map_or(0, |(k, _)| k.len());
                    bytes += db.last(&r).expect("last").map_or(0, |(k, _)| k.len());
                }
                bytes
            }

            /// `len`, `n` times — the DB-record read, no tree walk.
            pub fn db_len(env: &BEnv, db: Db, n: usize) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut total = 0usize;
                for _ in 0..n {
                    total = total.wrapping_add(db.len(&r).expect("len") as usize);
                }
                total
            }

            /// `MDB_SET_RANGE`-shaped seeks: a fresh positioned descent per probe
            /// with no sequential locality for a leaf memo to exploit.
            pub fn seek_ge(env: &BEnv, db: Db, probes: &[Vec<u8>]) -> usize {
                let r = env.read_txn().expect("read_txn");
                let mut hits = 0usize;
                for k in probes {
                    if db
                        .get_greater_than_or_equal_to(&r, k.as_slice())
                        .expect("seek")
                        .is_some()
                    {
                        hits += 1;
                    }
                }
                hits
            }

            // ---------------------------------------------------------------
            // composite / maintenance
            // ---------------------------------------------------------------

            /// milli-shaped: one write txn touching every named DB round-robin,
            /// each put immediately read back (extractor → write_db pattern).
            pub fn mixed_rw(env: &BEnv, dbs: &[Db], keys: &[Vec<u8>], val: &[u8]) -> usize {
                let mut hits = 0usize;
                let mut w = env.write_txn().expect("write_txn");
                for (i, k) in keys.iter().enumerate() {
                    let db = dbs[i % dbs.len()];
                    db.put(&mut w, k.as_slice(), val).expect("put");
                    if db.get(&w, k.as_slice()).expect("get").is_some() {
                        hits += 1;
                    }
                }
                w.commit().expect("commit");
                hits
            }

            /// A single forward scan that checks `stop` every `chunk` entries
            /// instead of only at end-of-scan, so a reader notices the writer is
            /// done within a fraction of a full scan rather than up to one whole
            /// scan late. Not part of `Backend`: it exists only to give
            /// `writer_under_readers`'s reader threads a responsive stop
            /// condition, and it is generated identically for both engines by
            /// this same macro body.
            fn scan_chunked_until_stop(
                env: &BEnv,
                db: Db,
                stop: &AtomicBool,
                chunk: usize,
                ready: Option<&AtomicUsize>,
            ) {
                let r = env.read_txn().expect("read_txn");
                // Signal only once the read txn is open, so the writer's timer
                // starts under real reader pressure (a spawned thread has not
                // necessarily opened anything yet).
                if let Some(ready) = ready {
                    ready.fetch_add(1, Ordering::Release);
                }
                let mut n = 0usize;
                for kv in db.iter(&r).expect("iter") {
                    let _ = kv.expect("entry");
                    n += 1;
                    if n % chunk == 0 && stop.load(Ordering::Relaxed) {
                        return;
                    }
                }
            }

            /// `readers` threads full-scanning in a loop (polling `stop` every
            /// 1024 entries) while this thread commits `batches` write txns.
            /// Returns the ELAPSED TIME OF THE WRITER LOOP ONLY — from the first
            /// `write_txn` to the last `commit` — so the caller's criterion
            /// `iter_custom` times exactly the writer's work under concurrent
            /// reader pressure, not the reader threads' join tail or the
            /// fixture's drop.
            pub fn writer_under_readers(
                env: &BEnv,
                db: Db,
                keys: &[Vec<u8>],
                val: &[u8],
                readers: usize,
                batches: usize,
                per_batch: usize,
            ) -> Duration {
                const READER_STOP_POLL_CHUNK: usize = 1024;
                let stop = AtomicBool::new(false);
                let ready = AtomicUsize::new(0);
                std::thread::scope(|s| {
                    for _ in 0..readers {
                        s.spawn(|| {
                            let mut first = Some(&ready);
                            while !stop.load(Ordering::Relaxed) {
                                scan_chunked_until_stop(
                                    env,
                                    db,
                                    &stop,
                                    READER_STOP_POLL_CHUNK,
                                    first.take(),
                                );
                            }
                        });
                    }
                    // Time the writer only once every reader holds a read txn.
                    while ready.load(Ordering::Acquire) < readers {
                        std::thread::yield_now();
                    }
                    let start = Instant::now();
                    for b in 0..batches {
                        let lo = (b * per_batch) % keys.len();
                        let hi = (lo + per_batch).min(keys.len());
                        let mut w = env.write_txn().expect("write_txn");
                        for k in &keys[lo..hi] {
                            db.put(&mut w, k.as_slice(), val).expect("put");
                        }
                        w.commit().expect("commit");
                    }
                    let elapsed = start.elapsed();
                    stop.store(true, Ordering::Relaxed);
                    elapsed
                })
            }

            /// `mdb_env_copy2` into `dest`, compacting or raw.
            pub fn copy_to(env: &BEnv, dest: &Path, compact: bool) {
                let mut f = File::create(dest).expect("create copy dest");
                let opt = if compact {
                    CompactionOption::Enabled
                } else {
                    CompactionOption::Disabled
                };
                env.copy_to_file(&mut f, opt).expect("copy_to_file");
            }

            /// Zero-sized marker so the generic rung shapes in `harness.rs` can
            /// name this backend as a type parameter.
            pub struct Marker;

            impl crate::harness::Backend for Marker {
                const NAME: &'static str = stringify!($mod);
                type Env = BEnv;
                type Db = Db;

                fn open(dir: &Path, map_size: usize, no_sync: bool, page: u32) -> BEnv {
                    open(dir, map_size, no_sync, page)
                }
                fn create_db(env: &BEnv, name: Option<&str>) -> Db {
                    create_db(env, name)
                }
                fn create_dbs(env: &BEnv, names: &[String]) -> Vec<Db> {
                    create_dbs(env, names)
                }
                fn open_create_close(dirs: &[std::path::PathBuf], map_size: usize, page: u32) {
                    open_create_close(dirs, map_size, page)
                }
                fn reopen(dir: &Path, map_size: usize, page: u32, n: usize) {
                    reopen(dir, map_size, page, n)
                }
                fn rotxn_churn(env: &BEnv, n: usize) -> usize {
                    rotxn_churn(env, n)
                }
                fn empty_commit_churn(env: &BEnv, n: usize) {
                    empty_commit_churn(env, n)
                }

                fn bulk_put(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                    bulk_put(env, db, keys, val)
                }
                fn bulk_put_append(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                    bulk_put_append(env, db, keys, val)
                }
                fn bulk_put_reserved(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                    bulk_put_reserved(env, db, keys, val)
                }
                fn commit_churn(
                    env: &BEnv,
                    db: Db,
                    keys: &[Vec<u8>],
                    val: &[u8],
                    per_txn: usize,
                ) {
                    commit_churn(env, db, keys, val, per_txn)
                }
                fn delete_keys(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                    delete_keys(env, db, keys)
                }
                fn delete_range(env: &BEnv, db: Db, lo: &[u8], hi: &[u8]) -> usize {
                    delete_range(env, db, lo, hi)
                }
                fn clear_db(env: &BEnv, db: Db) {
                    clear_db(env, db)
                }
                fn cursor_drain(env: &BEnv, db: Db, keys: &[Vec<u8>], val: &[u8]) {
                    cursor_drain(env, db, keys, val)
                }
                fn delete_reinsert(
                    env: &BEnv,
                    db: Db,
                    keys: &[Vec<u8>],
                    val: &[u8],
                    rounds: usize,
                ) {
                    delete_reinsert(env, db, keys, val, rounds)
                }

                fn point_get(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                    point_get(env, db, keys)
                }
                fn point_get_touch(env: &BEnv, db: Db, keys: &[Vec<u8>]) -> usize {
                    point_get_touch(env, db, keys)
                }
                fn point_get_multi(env: &BEnv, dbs: &[Db], keys: &[Vec<u8>]) -> usize {
                    point_get_multi(env, dbs, keys)
                }
                fn point_get_hot(env: &BEnv, db: Db, key: &[u8], n: usize) -> usize {
                    point_get_hot(env, db, key, n)
                }
                fn scan(env: &BEnv, db: Db) -> usize {
                    scan(env, db)
                }
                fn rev_scan(env: &BEnv, db: Db) -> usize {
                    rev_scan(env, db)
                }
                fn range_scan(env: &BEnv, db: Db, lo: &[u8], hi: &[u8]) -> usize {
                    range_scan(env, db, lo, hi)
                }
                fn prefix_scan(env: &BEnv, db: Db, prefix: &[u8]) -> usize {
                    prefix_scan(env, db, prefix)
                }
                fn first_last(env: &BEnv, db: Db, n: usize) -> usize {
                    first_last(env, db, n)
                }
                fn db_len(env: &BEnv, db: Db, n: usize) -> usize {
                    db_len(env, db, n)
                }
                fn seek_ge(env: &BEnv, db: Db, probes: &[Vec<u8>]) -> usize {
                    seek_ge(env, db, probes)
                }

                fn mixed_rw(env: &BEnv, dbs: &[Db], keys: &[Vec<u8>], val: &[u8]) -> usize {
                    mixed_rw(env, dbs, keys, val)
                }
                fn writer_under_readers(
                    env: &BEnv,
                    db: Db,
                    keys: &[Vec<u8>],
                    val: &[u8],
                    readers: usize,
                    batches: usize,
                    per_batch: usize,
                ) -> Duration {
                    writer_under_readers(env, db, keys, val, readers, batches, per_batch)
                }
                fn copy_to(env: &BEnv, dest: &Path, compact: bool) {
                    copy_to(env, dest, compact)
                }
            }
        }
    };
    (@setpage $opts:ident, $page:ident, set) => {
        $opts.page_size($page);
    };
    (@setpage $opts:ident, $page:ident, noset) => {
        // LMDB derives its page size from the OS; nothing to set.
        let _ = $page;
    };
}

bench_backend!(lmdb, heed, noset);
bench_backend!(zerodb, heed_zerodb, set);
