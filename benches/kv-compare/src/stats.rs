//! Latency histogram, data generation and process measurements.

use std::path::Path;

/// Log-linear latency histogram in nanoseconds: 32 buckets per power of two,
/// so each recorded value is kept within about 3%.
#[derive(Clone)]
pub struct Histogram {
    counts: Vec<u64>,
    total: u64,
}

const SUB: u64 = 32;

impl Default for Histogram {
    fn default() -> Histogram {
        Histogram::new()
    }
}

impl Histogram {
    pub fn new() -> Histogram {
        Histogram {
            counts: vec![0; 64 * SUB as usize],
            total: 0,
        }
    }

    fn index(v: u64) -> usize {
        if v < SUB {
            return v as usize;
        }
        let e = 63 - u64::from(v.leading_zeros()); // highest set bit, >= 5
        let sub = (v >> (e - 5)) & (SUB - 1);
        ((e - 4) * SUB + sub) as usize
    }

    fn value(i: usize) -> u64 {
        let i = i as u64;
        if i < SUB {
            return i;
        }
        let e = i / SUB + 4;
        let sub = i % SUB;
        // The middle of the bucket.
        ((SUB + sub) << (e - 5)) + ((1u64 << (e - 5)) >> 1)
    }

    pub fn record(&mut self, ns: u64) {
        self.counts[Histogram::index(ns)] += 1;
        self.total += 1;
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.total += other.total;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    /// The value below which a fraction `q` of the samples fall.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = ((self.total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= target {
                return Histogram::value(i);
            }
        }
        Histogram::value(self.counts.len() - 1)
    }
}

/// Fast deterministic random numbers (SplitMix64); each thread seeds its own.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(n)) >> 64) as u64
    }
}

/// The 16-byte key of record `i`. The first 8 bytes are a bijective scramble
/// of `i`, so records `0..n` are inserted in random key order and a key range
/// holds records from all over the data set.
pub fn key(i: u64) -> [u8; 16] {
    let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 29;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 32;
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&x.to_be_bytes());
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

/// Incompressible value bytes: windows into one random buffer, so writing a
/// value costs no generation work and compression gains nothing.
pub struct Values {
    buf: Vec<u8>,
    size: usize,
}

impl Values {
    pub fn new(size: usize) -> Values {
        let mut rng = Rng::new(0x5eed);
        let len = (1 << 22) + size;
        let buf = (0..len.div_ceil(8))
            .flat_map(|_| rng.next().to_le_bytes())
            .take(len)
            .collect();
        Values { buf, size }
    }

    pub fn get(&self, seed: u64) -> &[u8] {
        // The top bits of the product depend on every bit of `seed`. The low
        // bits depend only on its low bits, which neighbouring keys share: a
        // modulo here gave runs of sorted records the same value, and
        // engines that compress blocks stored them in a fraction of the space.
        let off = (seed.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 42) as usize;
        &self.buf[off..off + self.size]
    }
}

/// Bytes the files under `dir` occupy on disk (allocated blocks, as `du`).
pub fn disk_bytes(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let Ok(m) = e.metadata() else { continue };
            if m.is_dir() {
                total += disk_bytes(&e.path());
            } else {
                total += m.blocks() * 512;
            }
        }
    }
    total
}

/// Read every file under `dir` once, sequentially, so its pages start in
/// the OS page cache (as far as memory allows) instead of on disk.
pub fn prefetch(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let path = e.path();
        if path.is_dir() {
            prefetch(&path);
        } else if let Ok(mut f) = std::fs::File::open(&path) {
            let _ = std::io::copy(&mut f, &mut std::io::sink());
        }
    }
}

/// Peak resident memory of this process, in bytes.
pub fn peak_rss() -> u64 {
    // SAFETY: `getrusage` fills the zero-initialised struct it is given.
    let ru = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    };
    let maxrss = ru.ru_maxrss as u64;
    // Linux reports KiB, macOS bytes.
    if cfg!(target_os = "macos") {
        maxrss
    } else {
        maxrss * 1024
    }
}

/// Bytes this process has caused to be written to storage, background
/// threads included: Linux `/proc/self/io` `write_bytes` (counted when a page
/// is dirtied) minus `cancelled_write_bytes` (dirty pages of files deleted or
/// truncated before they reached the disk). `None` off Linux.
pub fn written_bytes() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| -> Option<u64> {
        io.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse().ok())
    };
    Some(field("write_bytes: ")?.saturating_sub(field("cancelled_write_bytes: ")?))
}

/// Bytes written to the block device `KVC_DEVICE` (for example `nvme1n1`)
/// since boot, from `/sys/block/<dev>/stat`. Run on an otherwise idle disk,
/// the difference over a phase is what the engine made the disk write,
/// file-system journal included. `None` when unset or unreadable.
pub fn device_written_bytes() -> Option<u64> {
    let dev = std::env::var("KVC_DEVICE").ok()?;
    let stat = std::fs::read_to_string(format!("/sys/block/{dev}/stat")).ok()?;
    // Field 7 is sectors written; a sector here is always 512 bytes.
    stat.split_whitespace()
        .nth(6)?
        .parse::<u64>()
        .ok()
        .map(|s| s * 512)
}
