//! kv-compare: one test phase of one engine per invocation, one JSON line out.
//!
//! ```text
//! kv-compare <phase> --engine <name> --dir <path> [options]
//! ```
//!
//! Phases (see README.md for what each one answers):
//!
//! - `load`   insert records `0..records`, `--batch` per commit, then sync
//! - `get`    `--threads` threads read random existing keys
//! - `range`  `--threads` threads read `--range-len` keys from a random start
//! - `write`  one thread overwrites random keys, `--batch` per commit
//! - `mixed`  `--threads` reader threads plus one `write` thread
//! - `grow`   one thread inserts new keys, `--batch` per commit; with
//!   `--threads`, that many readers run alongside
//!
//! Every phase except `load` opens the data set an earlier `load` left in
//! `--dir`.

mod engine;
mod stats;

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use engine::{Config, Engine};
use stats::{Histogram, Rng, Values};

struct Args {
    phase: String,
    engine: String,
    dir: PathBuf,
    records: u64,
    value_size: usize,
    /// Reader threads; `None` = the phase's default.
    threads: Option<usize>,
    seconds: u64,
    batch: usize,
    range_len: usize,
    durable: bool,
    /// Unmeasured seconds of the same work before measuring.
    warmup: u64,
    /// Read every file of the data set once before starting.
    prefetch: bool,
    cache_mib: u64,
    map_gib: u64,
    label: String,
    out: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!(
        "usage: kv-compare <load|get|range|write|mixed|grow> --engine <{}> --dir <path>\n\
         \x20 [--records N] [--value-size B] [--threads T] [--seconds S] [--batch K]\n\
         \x20 [--range-len L] [--durable] [--warmup S] [--prefetch] [--cache-mib M] [--map-gib G]\n\
         \x20 [--label X] [--out FILE]",
        engine::ENGINES.join("|")
    );
    std::process::exit(2)
}

fn parse() -> Args {
    let mut it = std::env::args().skip(1);
    let phase = it.next().unwrap_or_else(|| usage());
    let mut a = Args {
        phase,
        engine: String::new(),
        dir: PathBuf::new(),
        records: 1_000_000,
        value_size: 100,
        threads: None,
        seconds: 30,
        batch: 0,
        range_len: 100,
        durable: false,
        warmup: 0,
        prefetch: false,
        cache_mib: 2048,
        map_gib: 1024,
        label: String::new(),
        out: None,
    };
    while let Some(flag) = it.next() {
        if flag == "--durable" || flag == "--prefetch" {
            a.durable |= flag == "--durable";
            a.prefetch |= flag == "--prefetch";
            continue;
        }
        let v = it.next().unwrap_or_else(|| usage());
        let num = || {
            v.replace('_', "")
                .parse::<u64>()
                .unwrap_or_else(|_| usage())
        };
        match flag.as_str() {
            "--engine" => a.engine = v.clone(),
            "--dir" => a.dir = PathBuf::from(&v),
            "--records" => a.records = num(),
            "--value-size" => a.value_size = num() as usize,
            "--threads" => a.threads = Some(num() as usize),
            "--seconds" => a.seconds = num(),
            "--warmup" => a.warmup = num(),
            "--batch" => a.batch = num() as usize,
            "--range-len" => a.range_len = num() as usize,
            "--cache-mib" => a.cache_mib = num(),
            "--map-gib" => a.map_gib = num(),
            "--label" => a.label = v.clone(),
            "--out" => a.out = Some(PathBuf::from(&v)),
            _ => usage(),
        }
    }
    if a.engine.is_empty() || a.dir.as_os_str().is_empty() {
        usage();
    }
    if a.batch == 0 {
        a.batch = if a.phase == "load" { 10_000 } else { 1 };
    }
    a
}

/// What one phase measured.
#[derive(Default)]
struct Outcome {
    secs: f64,
    reads: Histogram,
    writes: Histogram,
    read_timeline: Vec<u64>,
    write_timeline: Vec<u64>,
    records_written: u64,
}

/// Run `readers` reader threads and, if `writer`, one writer thread until
/// `seconds` elapse or the writer returns `None` (with `seconds` = `None`,
/// only the latter), sampling the completed-operation counts once a second.
/// The first `warmup` seconds run the same work unmeasured, so caches reach
/// their steady state before measuring starts.
fn drive(
    warmup: u64,
    seconds: Option<u64>,
    eng: &dyn Engine,
    readers: usize,
    read_op: &(dyn Fn(&mut dyn engine::Reader, &mut Rng) + Sync),
    writer: Option<&(dyn Fn(u64) -> Option<usize> + Sync)>,
) -> Outcome {
    let stop = AtomicBool::new(false);
    // Relaxed throughout: workers only need to see the flag flip eventually;
    // a sample landing on either side of the switch changes nothing.
    let measuring = AtomicBool::new(warmup == 0);
    let read_count = AtomicU64::new(0);
    let write_count = AtomicU64::new(0);
    let start = Instant::now();
    let warm_until = start + Duration::from_secs(warmup);
    let deadline = seconds.map(|s| warm_until + Duration::from_secs(s));
    let mut measure_start = start;
    let mut out = Outcome::default();

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for t in 0..readers {
            let (stop, measuring, read_count) = (&stop, &measuring, &read_count);
            handles.push(s.spawn(move || {
                let mut r = eng.reader();
                let mut rng = Rng::new(0x1000 + t as u64);
                let mut h = Histogram::new();
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    read_op(r.as_mut(), &mut rng);
                    if measuring.load(Ordering::Relaxed) {
                        h.record(t0.elapsed().as_nanos() as u64);
                    }
                    // Relaxed: a statistics counter, read only by the sampler.
                    read_count.fetch_add(1, Ordering::Relaxed);
                }
                h
            }));
        }
        let write_handle = writer.map(|w| {
            let (stop, measuring, write_count) = (&stop, &measuring, &write_count);
            s.spawn(move || {
                let mut h = Histogram::new();
                let mut records = 0u64;
                let mut commit = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    let Some(n) = w(commit) else { break };
                    if measuring.load(Ordering::Relaxed) {
                        h.record(t0.elapsed().as_nanos() as u64);
                        records += n as u64;
                    }
                    commit += 1;
                    write_count.fetch_add(n as u64, Ordering::Relaxed);
                }
                // A bounded writer (load) ends the phase when it finishes;
                // the phase lasted until then, not until the next sample.
                let ended = (!stop.swap(true, Ordering::Relaxed)).then(Instant::now);
                (h, records, ended)
            })
        });

        // Sample once a second until the deadline or the writer ends.
        let (mut last_r, mut last_w) = (0, 0);
        let mut tick = start;
        while !stop.load(Ordering::Relaxed) {
            tick += Duration::from_secs(1);
            let now = Instant::now();
            let until = deadline.map_or(tick, |d| tick.min(d));
            if until > now {
                std::thread::sleep(until - now);
            }
            let (r, w) = (
                read_count.load(Ordering::Relaxed),
                write_count.load(Ordering::Relaxed),
            );
            if readers > 0 {
                out.read_timeline.push(r - last_r);
            }
            if writer.is_some() {
                out.write_timeline.push(w - last_w);
            }
            (last_r, last_w) = (r, w);
            if !measuring.load(Ordering::Relaxed) && Instant::now() >= warm_until {
                measuring.store(true, Ordering::Relaxed);
                measure_start = Instant::now();
                out.read_timeline.clear();
                out.write_timeline.clear();
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                stop.store(true, Ordering::Relaxed);
            }
        }
        for h in handles {
            out.reads.merge(&h.join().expect("reader thread"));
        }
        out.secs = measure_start.elapsed().as_secs_f64();
        if let Some(h) = write_handle {
            let (h, records, ended) = h.join().expect("writer thread");
            out.writes = h;
            out.records_written = records;
            if let Some(ended) = ended {
                out.secs = (ended - measure_start).as_secs_f64();
            }
        }
    });
    out
}

fn main() {
    let a = parse();
    let (readers, writes) = match a.phase.as_str() {
        "load" | "write" => (0, true),
        "get" | "range" => (a.threads.unwrap_or(1), false),
        "mixed" => (a.threads.unwrap_or(1), true),
        "grow" => (a.threads.unwrap_or(0), true),
        _ => usage(),
    };
    let cfg = Config {
        durable: a.durable,
        cache_bytes: a.cache_mib << 20,
        connections: readers + usize::from(writes),
        map_bytes: (a.map_gib << 30) as usize,
    };
    let values = Values::new(a.value_size);
    let written_before = stats::written_bytes();
    let device_before = stats::device_written_bytes();
    if a.prefetch {
        stats::prefetch(&a.dir);
    }
    let eng = engine::open(&a.engine, &a.dir, &cfg);
    let n = a.records;

    let get = |r: &mut dyn engine::Reader, rng: &mut Rng| {
        let k = stats::key(rng.below(n));
        assert!(r.get(&k).is_some(), "key of a loaded record is missing");
    };
    let range = |r: &mut dyn engine::Reader, rng: &mut Rng| {
        let k = stats::key(rng.below(n));
        std::hint::black_box(r.range(&k, a.range_len));
    };
    // Write `ids` in one commit. `salt` picks the values: an overwrite must
    // store a value different from the current one, or an engine that skips
    // identical rewrites (SQLite does) would do no work.
    let batch_of = |ids: &mut dyn Iterator<Item = u64>, salt: u64| -> usize {
        let keys: Vec<[u8; 16]> = ids.map(stats::key).collect();
        let pairs: Vec<engine::Pair<'_>> = keys
            .iter()
            .map(|k| {
                let seed = u64::from_le_bytes(k[..8].try_into().unwrap());
                (&k[..], values.get(seed.wrapping_add(salt)))
            })
            .collect();
        eng.write(&pairs);
        keys.len()
    };
    // Overwrite random existing records. The seed differs on every run:
    // with a fixed seed a rerun on the same data would write each key's
    // current value again, which an engine that skips identical rewrites
    // (SQLite does) turns into no work.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let update_rng = std::sync::Mutex::new(Rng::new(nanos ^ u64::from(std::process::id()) << 32));
    let update = |_: u64| {
        let mut rng = update_rng.lock().expect("rng");
        let ids: Vec<u64> = (0..a.batch).map(|_| rng.below(n)).collect();
        let salt = rng.next();
        drop(rng);
        Some(batch_of(&mut ids.into_iter(), salt))
    };
    // Insert records `first..first + count` in commits of `batch`.
    let insert_from = |first: u64, count: u64| {
        move |commit: u64| {
            let lo = commit * a.batch as u64;
            if lo >= count {
                return None;
            }
            let hi = (lo + a.batch as u64).min(count);
            Some(batch_of(&mut (first + lo..first + hi), 0))
        }
    };

    let secs = Some(a.seconds);
    let out = match a.phase.as_str() {
        "load" => {
            let out = drive(0, None, eng.as_ref(), 0, &get, Some(&insert_from(0, n)));
            eng.sync();
            out
        }
        "get" => drive(a.warmup, secs, eng.as_ref(), readers, &get, None),
        "range" => drive(a.warmup, secs, eng.as_ref(), readers, &range, None),
        "write" => drive(a.warmup, secs, eng.as_ref(), 0, &get, Some(&update)),
        "mixed" => drive(a.warmup, secs, eng.as_ref(), readers, &get, Some(&update)),
        // Readers only when asked for.
        "grow" => drive(
            a.warmup,
            secs,
            eng.as_ref(),
            readers,
            &get,
            Some(&insert_from(n, u64::MAX / 2)),
        ),
        _ => usage(),
    };

    let disk = stats::disk_bytes(&a.dir);
    // Closing the engine finishes its pending writes; count them too.
    drop(eng);
    let delta = |before: Option<u64>, after: Option<u64>| Some(after? - before?);
    let written = delta(written_before, stats::written_bytes());
    let device = device_before.and_then(|before| {
        // Writes a commit handed to the OS without waiting may still be in the
        // page cache; flush them so the device counter includes them.
        // SAFETY: `sync` takes no arguments and only schedules writeback.
        unsafe { libc::sync() };
        delta(Some(before), stats::device_written_bytes())
    });
    let line = report(&a, &out, disk, written, device);
    println!("{line}");
    if let Some(path) = &a.out {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open --out");
        writeln!(f, "{line}").expect("write --out");
    }
}

fn report(a: &Args, o: &Outcome, disk: u64, written: Option<u64>, device: Option<u64>) -> String {
    let us = |ns: u64| ns as f64 / 1000.0;
    let mut s = String::new();
    let opt = |v: Option<u64>| v.map_or("null".to_string(), |v| v.to_string());
    let list = |v: &[u64]| v.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    write!(
        s,
        "{{\"engine\":\"{}\",\"phase\":\"{}\",\"label\":\"{}\",\"records\":{},\"value_size\":{},\
         \"threads\":{},\"batch\":{},\"durable\":{},\"range_len\":{},\"cache_mib\":{},\"secs\":{:.3},",
        a.engine, a.phase, a.label, a.records, a.value_size, a.threads.unwrap_or(0), a.batch, a.durable, a.range_len,
        a.cache_mib, o.secs
    )
    .unwrap();
    write!(
        s,
        "\"reads\":{},\"reads_per_sec\":{:.1},\"read_p50_us\":{:.2},\"read_p99_us\":{:.2},\"read_p999_us\":{:.2},",
        o.reads.count(),
        o.reads.count() as f64 / o.secs,
        us(o.reads.quantile(0.50)),
        us(o.reads.quantile(0.99)),
        us(o.reads.quantile(0.999))
    )
    .unwrap();
    write!(
        s,
        "\"commits\":{},\"records_written\":{},\"records_written_per_sec\":{:.1},\
         \"commit_p50_us\":{:.2},\"commit_p99_us\":{:.2},\"commit_p999_us\":{:.2},",
        o.writes.count(),
        o.records_written,
        o.records_written as f64 / o.secs,
        us(o.writes.quantile(0.50)),
        us(o.writes.quantile(0.99)),
        us(o.writes.quantile(0.999))
    )
    .unwrap();
    write!(
        s,
        "\"disk_bytes\":{},\"written_bytes\":{},\"device_written_bytes\":{},\"peak_rss_bytes\":{},\
         \"read_timeline\":[{}],\"write_timeline\":[{}]}}",
        disk,
        opt(written),
        opt(device),
        stats::peak_rss(),
        list(&o.read_timeline),
        list(&o.write_timeline)
    )
    .unwrap();
    s
}
