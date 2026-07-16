//! The logical dump text format — `mdb_dump`-shaped (M1.12, PLAN §1.12).
//!
//! ## Format (read clean-room from the fork's `mdb_dump.c` / `mdb_load.c`)
//!
//! `mdb_dump` writes a header of `key=value` lines, then `HEADER=END`, then the
//! records (each key line and value line prefixed by a single space, hex-encoded
//! in `format=bytevalue` mode), then `DATA=END`; with `-a` it emits one such
//! block per database (the main DB, then each named sub-DB, whose block carries
//! a `database=NAME` line). ZeroDB's dump keeps that structure and the exact
//! `bytevalue` record encoding, so the shape is `mdb_dump`-compatible.
//!
//! **This is a *logical* dump, deliberately.** It carries only what a
//! migration/round-trip needs — the databases and their records — and
//! **omits the physical env geometry** (`mapsize`, `maxreaders`, `db_pagesize`)
//! that real `mdb_dump` includes. That omission is what lets a dump compare
//! **byte-identically across engines and geometries** (LMDB vs ZeroDB, 4 K vs
//! 64 K pages): logical content is engine-independent, physical layout is not
//! (D-002). Names are hex-encoded (`database=<hex>`) rather than escaped, so a
//! name containing `0x00`/newlines (valid in ZeroDB, D-008) round-trips
//! unambiguously.
//!
//! ### Grammar
//!
//! ```text
//! VERSION=3\n
//! <db-block>*
//! ```
//! where each `<db-block>` is:
//! ```text
//! format=bytevalue\n
//! [database=<hex-name>\n]      # present iff a named DB; absent for the main DB
//! type=btree\n
//! HEADER=END\n
//! ( <SP><hex-key>\n <SP><hex-value>\n )*
//! DATA=END\n
//! ```
//! The main DB block comes first (no `database=` line); named-DB blocks follow
//! in ascending name order. Within a block, records are in ascending key order.
//! `<SP>` is a single ASCII space; an empty value renders as just `<SP>\n`.

use std::fmt::Write as _;

/// One database section of a dump: `name = None` for the main/unnamed DB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpDb {
    /// The database name, or `None` for the main DB.
    pub name: Option<Vec<u8>>,
    /// The database's records, in ascending key order.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The dump-format version line value.
pub const DUMP_VERSION: u32 = 3;

/// Lowercase-hex encode `bytes` (two digits per byte), the `bytevalue` encoding.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").expect("write to String is infallible");
    }
    s
}

/// Decode a lowercase/uppercase-hex string to bytes.
fn from_hex(s: &str) -> Result<Vec<u8>, DumpError> {
    let s = s.trim_end_matches(['\r']);
    if s.len() % 2 != 0 {
        return Err(DumpError::BadHex(s.to_string()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_digit(bytes[i])?;
        let lo = hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_digit(c: u8) -> Result<u8, DumpError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(DumpError::BadHex(format!("byte 0x{c:02x}"))),
    }
}

/// Render a set of databases (main first, named DBs in ascending name order) to
/// the dump text. Deterministic: the same logical content always yields the same
/// bytes. Callers must pass the main DB as `name = None` first (if present) and
/// named DBs sorted by name.
#[must_use]
pub fn render(dbs: &[DumpDb]) -> String {
    let mut out = String::new();
    writeln!(out, "VERSION={DUMP_VERSION}").expect("infallible");
    for db in dbs {
        writeln!(out, "format=bytevalue").expect("infallible");
        if let Some(name) = &db.name {
            writeln!(out, "database={}", to_hex(name)).expect("infallible");
        }
        writeln!(out, "type=btree").expect("infallible");
        writeln!(out, "HEADER=END").expect("infallible");
        for (k, v) in &db.entries {
            writeln!(out, " {}", to_hex(k)).expect("infallible");
            writeln!(out, " {}", to_hex(v)).expect("infallible");
        }
        writeln!(out, "DATA=END").expect("infallible");
    }
    out
}

/// A dump parse/validation error.
#[derive(Debug)]
pub enum DumpError {
    /// The `VERSION=` line is missing, malformed, or an unsupported version.
    BadVersion(String),
    /// A hex field could not be decoded.
    BadHex(String),
    /// A record line (leading space) had no matching value line.
    DanglingKey,
    /// A structural line was unexpected at this point.
    Unexpected(String),
    /// The dump ended inside a block (no `DATA=END`).
    Truncated,
}

impl std::fmt::Display for DumpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DumpError::BadVersion(s) => write!(f, "bad or missing VERSION line: {s}"),
            DumpError::BadHex(s) => write!(f, "bad hex field: {s}"),
            DumpError::DanglingKey => write!(f, "record key without a value line"),
            DumpError::Unexpected(s) => write!(f, "unexpected line: {s:?}"),
            DumpError::Truncated => write!(f, "dump truncated (no DATA=END)"),
        }
    }
}

impl std::error::Error for DumpError {}

/// Parse dump text back into database sections. The inverse of [`render`];
/// tolerant of `\r\n` line endings.
///
/// # Errors
///
/// [`DumpError`] on any malformed line, hex field, or truncated block.
pub fn parse(text: &str) -> Result<Vec<DumpDb>, DumpError> {
    let mut lines = text.lines().peekable();

    // VERSION line.
    match lines.next() {
        Some(l) => {
            let l = l.trim_end();
            let v = l
                .strip_prefix("VERSION=")
                .ok_or_else(|| DumpError::BadVersion(l.to_string()))?;
            let v: u32 = v
                .parse()
                .map_err(|_| DumpError::BadVersion(l.to_string()))?;
            if v != DUMP_VERSION {
                return Err(DumpError::BadVersion(format!("unsupported version {v}")));
            }
        }
        None => return Err(DumpError::BadVersion("empty input".into())),
    }

    let mut dbs = Vec::new();
    while let Some(&first) = lines.peek() {
        let first = first.trim_end();
        if first.is_empty() {
            lines.next();
            continue;
        }
        // A block starts with `format=...`.
        if first != "format=bytevalue" {
            return Err(DumpError::Unexpected(first.to_string()));
        }
        lines.next(); // consume format line
        let mut name: Option<Vec<u8>> = None;
        // Header lines until HEADER=END.
        loop {
            let l = lines.next().ok_or(DumpError::Truncated)?;
            let l = l.trim_end();
            if l == "HEADER=END" {
                break;
            }
            if let Some(hexname) = l.strip_prefix("database=") {
                name = Some(from_hex(hexname)?);
            } else if l == "type=btree" {
                // ignored (only btree is produced)
            } else {
                return Err(DumpError::Unexpected(l.to_string()));
            }
        }
        // Records until DATA=END.
        let mut entries = Vec::new();
        loop {
            let l = lines.next().ok_or(DumpError::Truncated)?;
            let lt = l.trim_end();
            if lt == "DATA=END" {
                break;
            }
            // A record line begins with a single space, then hex.
            let key_hex = l
                .strip_prefix(' ')
                .ok_or_else(|| DumpError::Unexpected(l.to_string()))?;
            let key = from_hex(key_hex.trim_end())?;
            let vline = lines.next().ok_or(DumpError::DanglingKey)?;
            let val_hex = vline
                .strip_prefix(' ')
                .ok_or_else(|| DumpError::Unexpected(vline.to_string()))?;
            let val = from_hex(val_hex.trim_end())?;
            entries.push((key, val));
        }
        dbs.push(DumpDb { name, entries });
    }
    Ok(dbs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_render_parse() {
        let dbs = vec![
            DumpDb {
                name: None,
                entries: vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), Vec::new())],
            },
            DumpDb {
                name: Some(b"posts".to_vec()),
                entries: vec![(vec![0, 1, 2], vec![255, 254])],
            },
        ];
        let text = render(&dbs);
        assert!(text.starts_with("VERSION=3\n"));
        assert_eq!(parse(&text).unwrap(), dbs);
    }

    #[test]
    fn empty_value_line_is_just_space() {
        let dbs = vec![DumpDb {
            name: None,
            entries: vec![(b"k".to_vec(), Vec::new())],
        }];
        let text = render(&dbs);
        // key line, then a value line that is a single space.
        assert!(text.contains("\n 6b\n \nDATA=END\n"));
        assert_eq!(parse(&text).unwrap(), dbs);
    }

    #[test]
    fn name_with_nul_round_trips() {
        // D-008: a name with an embedded 0x00 is valid in zerodb; hex encoding
        // round-trips it.
        let dbs = vec![DumpDb {
            name: Some(vec![b'x', 0, b'y']),
            entries: vec![],
        }];
        let text = render(&dbs);
        assert_eq!(parse(&text).unwrap(), dbs);
    }

    #[test]
    fn rejects_bad_version() {
        assert!(parse("VERSION=9\nformat=bytevalue\n").is_err());
        assert!(parse("nope\n").is_err());
    }

    #[test]
    fn tolerates_crlf() {
        let text = "VERSION=3\r\nformat=bytevalue\r\nHEADER=END\r\n 6b\r\n 76\r\nDATA=END\r\n";
        let dbs = parse(text).unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].entries, vec![(b"k".to_vec(), b"v".to_vec())]);
    }
}
