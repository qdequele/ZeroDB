//! The logical dump text format — `mdb_dump`-shaped.
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
//! (ZeroDB has its own on-disk format). Names are hex-encoded
//! (`database=<hex>`) rather than escaped, so a name containing
//! `0x00`/newlines (valid in ZeroDB, unlike heed's C-string names) round-trips
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

use std::io::Write;

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

/// Append the lowercase-hex encoding of `bytes` (two digits per byte, the
/// `bytevalue` encoding) to `out`.
fn push_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[usize::from(b >> 4)]);
        out.push(DIGITS[usize::from(b & 0xf)]);
    }
}

/// Decode a lowercase/uppercase-hex field, appending the bytes to `out`.
fn decode_hex_into(field: &[u8], out: &mut Vec<u8>) -> Result<(), DumpError> {
    if !field.len().is_multiple_of(2) {
        return Err(DumpError::BadHex(lossy(field)));
    }
    out.reserve(field.len() / 2);
    for pair in field.as_chunks::<2>().0 {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(())
}

fn hex_digit(c: u8) -> Result<u8, DumpError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(DumpError::BadHex(format!("byte 0x{c:02x}"))),
    }
}

/// Writes dump text as it goes, so dumping a database never holds more than
/// one record in memory.
///
/// Call [`DumpWriter::begin_db`], then [`DumpWriter::record`] for each record
/// in ascending key order, then [`DumpWriter::end_db`], once per database:
/// the main database first, then the named ones in ascending name order.
pub struct DumpWriter<W: Write> {
    out: W,
    /// The line being encoded, reused across lines.
    line: Vec<u8>,
}

impl<W: Write> DumpWriter<W> {
    /// Start a dump: writes the `VERSION=` line.
    ///
    /// # Errors
    ///
    /// The write error.
    pub fn new(mut out: W) -> std::io::Result<Self> {
        writeln!(out, "VERSION={DUMP_VERSION}")?;
        Ok(DumpWriter {
            out,
            line: Vec::new(),
        })
    }

    /// Start a database block; `name` is `None` for the main database.
    ///
    /// # Errors
    ///
    /// The write error.
    pub fn begin_db(&mut self, name: Option<&[u8]>) -> std::io::Result<()> {
        self.out.write_all(b"format=bytevalue\n")?;
        if let Some(name) = name {
            self.hex_line(b"database=", name)?;
        }
        self.out.write_all(b"type=btree\nHEADER=END\n")
    }

    /// Write one record of the current block.
    ///
    /// # Errors
    ///
    /// The write error.
    pub fn record(&mut self, key: &[u8], val: &[u8]) -> std::io::Result<()> {
        self.hex_line(b" ", key)?;
        self.hex_line(b" ", val)
    }

    /// End the current database block.
    ///
    /// # Errors
    ///
    /// The write error.
    pub fn end_db(&mut self) -> std::io::Result<()> {
        self.out.write_all(b"DATA=END\n")
    }

    /// Flush and return the output.
    ///
    /// # Errors
    ///
    /// The flush error.
    pub fn finish(mut self) -> std::io::Result<W> {
        self.out.flush()?;
        Ok(self.out)
    }

    /// Write `prefix`, the hex encoding of `bytes`, and a newline.
    fn hex_line(&mut self, prefix: &[u8], bytes: &[u8]) -> std::io::Result<()> {
        self.line.clear();
        self.line.extend_from_slice(prefix);
        push_hex(&mut self.line, bytes);
        self.line.push(b'\n');
        self.out.write_all(&self.line)
    }
}

/// Render a set of databases (main first, named DBs in ascending name order) to
/// the dump text. Deterministic: the same logical content always yields the same
/// bytes. Callers must pass the main DB as `name = None` first (if present) and
/// named DBs sorted by name.
#[must_use]
pub fn render(dbs: &[DumpDb]) -> String {
    let write = || -> std::io::Result<Vec<u8>> {
        let mut w = DumpWriter::new(Vec::new())?;
        for db in dbs {
            w.begin_db(db.name.as_deref())?;
            for (k, v) in &db.entries {
                w.record(k, v)?;
            }
            w.end_db()?;
        }
        w.finish()
    };
    let bytes = write().expect("writing to a Vec cannot fail");
    String::from_utf8(bytes).expect("the dump text is ASCII")
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
    /// Reading the dump failed.
    Io(std::io::Error),
}

impl From<std::io::Error> for DumpError {
    fn from(e: std::io::Error) -> Self {
        DumpError::Io(e)
    }
}

impl std::fmt::Display for DumpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DumpError::BadVersion(s) => write!(f, "bad or missing VERSION line: {s}"),
            DumpError::BadHex(s) => write!(f, "bad hex field: {s}"),
            DumpError::DanglingKey => write!(f, "record key without a value line"),
            DumpError::Unexpected(s) => write!(f, "unexpected line: {s:?}"),
            DumpError::Truncated => write!(f, "dump truncated (no DATA=END)"),
            DumpError::Io(e) => write!(f, "reading the dump: {e}"),
        }
    }
}

impl std::error::Error for DumpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DumpError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Strip trailing ASCII whitespace (`\r`, spaces, tabs) from a line.
fn trim_end(line: &[u8]) -> &[u8] {
    let end = line
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(0, |i| i + 1);
    &line[..end]
}

/// A line or field for an error message.
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A streaming dump parser. It reads one line at a time, so its memory is
/// one line (the longest record's hex) however large the dump is.
///
/// Call [`DumpReader::next_db`] to reach the next database block, then
/// [`DumpReader::next_record`] until it returns `false` at the block's
/// `DATA=END`. It accepts exactly what [`parse`] accepts, since [`parse`] is
/// built on it: `\r\n` line endings, blank lines between blocks, and
/// trailing whitespace on a line.
pub struct DumpReader<R> {
    input: R,
    /// The current line, without its `\n` (and a `\r` before it).
    line: Vec<u8>,
    /// A block's header was read and its `DATA=END` was not.
    in_block: bool,
}

impl<R: std::io::BufRead> DumpReader<R> {
    /// Start reading a dump: reads and checks the `VERSION=` line.
    ///
    /// # Errors
    ///
    /// [`DumpError::BadVersion`] for a missing, malformed or unsupported
    /// version line; [`DumpError::Io`] if reading fails.
    pub fn new(input: R) -> Result<Self, DumpError> {
        let mut reader = DumpReader {
            input,
            line: Vec::new(),
            in_block: false,
        };
        if !reader.read_line()? {
            return Err(DumpError::BadVersion("empty input".into()));
        }
        let l = trim_end(&reader.line);
        let v = l
            .strip_prefix(b"VERSION=")
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<u32>().ok())
            .ok_or_else(|| DumpError::BadVersion(lossy(l)))?;
        if v != DUMP_VERSION {
            return Err(DumpError::BadVersion(format!("unsupported version {v}")));
        }
        Ok(reader)
    }

    /// Read the next line into `self.line`. Returns `false` at the end of
    /// the input.
    fn read_line(&mut self) -> Result<bool, DumpError> {
        self.line.clear();
        if self.input.read_until(b'\n', &mut self.line)? == 0 {
            return Ok(false);
        }
        if self.line.last() == Some(&b'\n') {
            self.line.pop();
            if self.line.last() == Some(&b'\r') {
                self.line.pop();
            }
        }
        Ok(true)
    }

    /// Advance to the next database block and read its header. Returns
    /// `None` at the end of the dump, else `Some(name)`, where `name` is
    /// `None` for the main database. Records of the current block that were
    /// not read are read and checked first.
    ///
    /// # Errors
    ///
    /// [`DumpError`] on a malformed header or record, a truncated block, or
    /// a read error.
    #[allow(clippy::option_option)]
    pub fn next_db(&mut self) -> Result<Option<Option<Vec<u8>>>, DumpError> {
        if self.in_block {
            let (mut key, mut val) = (Vec::new(), Vec::new());
            while self.next_record(&mut key, &mut val)? {}
        }
        // Blank lines between blocks are skipped; a block starts with
        // `format=bytevalue`.
        loop {
            if !self.read_line()? {
                return Ok(None);
            }
            let l = trim_end(&self.line);
            if l.is_empty() {
                continue;
            }
            if l != b"format=bytevalue" {
                return Err(DumpError::Unexpected(lossy(l)));
            }
            break;
        }
        // Header lines until HEADER=END.
        let mut name = None;
        loop {
            if !self.read_line()? {
                return Err(DumpError::Truncated);
            }
            let l = trim_end(&self.line);
            if l == b"HEADER=END" {
                break;
            }
            if let Some(hexname) = l.strip_prefix(b"database=") {
                let mut n = Vec::new();
                decode_hex_into(hexname, &mut n)?;
                name = Some(n);
            } else if l == b"type=btree" {
                // ignored (only btree is produced)
            } else {
                return Err(DumpError::Unexpected(lossy(l)));
            }
        }
        self.in_block = true;
        Ok(Some(name))
    }

    /// Read the current block's next record into `key` and `val`, replacing
    /// their contents. Returns `false` at the block's `DATA=END`, or when no
    /// block is open.
    ///
    /// # Errors
    ///
    /// [`DumpError`] on a malformed record line or hex field, a key line
    /// without a value line, a truncated block, or a read error.
    pub fn next_record(&mut self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> Result<bool, DumpError> {
        if !self.in_block {
            return Ok(false);
        }
        if !self.read_line()? {
            return Err(DumpError::Truncated);
        }
        if trim_end(&self.line) == b"DATA=END" {
            self.in_block = false;
            return Ok(false);
        }
        key.clear();
        val.clear();
        // A record line begins with a single space, then hex.
        let key_hex = self
            .line
            .strip_prefix(b" ")
            .ok_or_else(|| DumpError::Unexpected(lossy(&self.line)))?;
        decode_hex_into(trim_end(key_hex), key)?;
        if !self.read_line()? {
            return Err(DumpError::DanglingKey);
        }
        let val_hex = self
            .line
            .strip_prefix(b" ")
            .ok_or_else(|| DumpError::Unexpected(lossy(&self.line)))?;
        decode_hex_into(trim_end(val_hex), val)?;
        Ok(true)
    }
}

/// Parse dump text back into database sections. The inverse of [`render`];
/// tolerant of `\r\n` line endings. It holds every record in memory; `load`
/// reads a dump with [`DumpReader`] instead.
///
/// # Errors
///
/// [`DumpError`] on any malformed line, hex field, or truncated block.
pub fn parse(text: &str) -> Result<Vec<DumpDb>, DumpError> {
    let mut reader = DumpReader::new(text.as_bytes())?;
    let mut dbs = Vec::new();
    let (mut key, mut val) = (Vec::new(), Vec::new());
    while let Some(name) = reader.next_db()? {
        let mut entries = Vec::new();
        while reader.next_record(&mut key, &mut val)? {
            entries.push((key.clone(), val.clone()));
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
        // A name with an embedded 0x00 is valid in zerodb; hex encoding
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

    /// The streaming reader reads records one at a time, skips the unread
    /// records of a block when asked for the next one, and reports the end.
    #[test]
    fn reader_streams_blocks_and_records() {
        let dbs = vec![
            DumpDb {
                name: None,
                entries: vec![
                    (b"a".to_vec(), b"1".to_vec()),
                    (b"b".to_vec(), b"2".to_vec()),
                ],
            },
            DumpDb {
                name: Some(b"n".to_vec()),
                entries: vec![(b"c".to_vec(), b"3".to_vec())],
            },
        ];
        let text = render(&dbs);
        let mut r = DumpReader::new(text.as_bytes()).unwrap();
        let (mut k, mut v) = (Vec::new(), Vec::new());
        assert_eq!(r.next_db().unwrap(), Some(None));
        assert!(r.next_record(&mut k, &mut v).unwrap());
        assert_eq!((k.as_slice(), v.as_slice()), (&b"a"[..], &b"1"[..]));
        // Leave `b` unread: the next block is still found.
        assert_eq!(r.next_db().unwrap(), Some(Some(b"n".to_vec())));
        assert!(r.next_record(&mut k, &mut v).unwrap());
        assert_eq!((k.as_slice(), v.as_slice()), (&b"c"[..], &b"3"[..]));
        assert!(!r.next_record(&mut k, &mut v).unwrap());
        assert!(!r.next_record(&mut k, &mut v).unwrap());
        assert_eq!(r.next_db().unwrap(), None);
    }

    /// The parser `load` used before it streamed: the whole text through
    /// `str::lines`. Kept as the reference [`parse`] must agree with.
    fn legacy_parse(text: &str) -> Result<Vec<DumpDb>, DumpError> {
        fn from_hex(s: &str) -> Result<Vec<u8>, DumpError> {
            let s = s.trim_end_matches(['\r']);
            let mut out = Vec::new();
            decode_hex_into(s.as_bytes(), &mut out)?;
            Ok(out)
        }
        let mut lines = text.lines().peekable();
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
            if first != "format=bytevalue" {
                return Err(DumpError::Unexpected(first.to_string()));
            }
            lines.next();
            let mut name: Option<Vec<u8>> = None;
            loop {
                let l = lines.next().ok_or(DumpError::Truncated)?;
                let l = l.trim_end();
                if l == "HEADER=END" {
                    break;
                }
                if let Some(hexname) = l.strip_prefix("database=") {
                    name = Some(from_hex(hexname)?);
                } else if l == "type=btree" {
                } else {
                    return Err(DumpError::Unexpected(l.to_string()));
                }
            }
            let mut entries = Vec::new();
            loop {
                let l = lines.next().ok_or(DumpError::Truncated)?;
                if l.trim_end() == "DATA=END" {
                    break;
                }
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

    /// The error's kind, for comparing two parsers' verdicts.
    fn kind(
        r: &Result<Vec<DumpDb>, DumpError>,
    ) -> Result<&Vec<DumpDb>, std::mem::Discriminant<DumpError>> {
        r.as_ref().map_err(std::mem::discriminant)
    }

    /// The streaming parser accepts and rejects exactly what the in-memory
    /// parser did, with the same result and the same error kind, over
    /// valid dumps and many single and double mutations of them (byte
    /// replaced, inserted or deleted, drawn from the characters the grammar
    /// cares about).
    #[test]
    fn streaming_parse_matches_legacy_parse() {
        let seeds = [
            render(&[
                DumpDb {
                    name: None,
                    entries: vec![(b"a".to_vec(), b"1".to_vec()), (b"bc".to_vec(), Vec::new())],
                },
                DumpDb {
                    name: Some(vec![0, b'\n', 0xff]),
                    entries: vec![(vec![0, 1, 2], vec![255, 254])],
                },
                DumpDb {
                    name: Some(b"empty".to_vec()),
                    entries: vec![],
                },
            ]),
            "VERSION=3\r\n\r\nformat=bytevalue\r\ntype=btree\r\nHEADER=END\r\n 6B \r\n 76\t\r\nDATA=END"
                .to_string(),
            "VERSION=3\n".to_string(),
            String::new(),
        ];
        let alphabet = b" \r\n\t0aF=gVDEHx";
        // A fixed xorshift sequence: the test is deterministic.
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        let mut checked = 0;
        for seed in &seeds {
            assert_eq!(kind(&parse(seed)), kind(&legacy_parse(seed)), "{seed:?}");
            for _ in 0..4000 {
                let mut bytes = seed.clone().into_bytes();
                for _ in 0..1 + next(2) {
                    let at = next(bytes.len() + 1);
                    let c = alphabet[next(alphabet.len())];
                    match next(3) {
                        0 if at < bytes.len() => bytes[at] = c,
                        1 if at < bytes.len() => {
                            bytes.remove(at);
                        }
                        _ => bytes.insert(at, c),
                    }
                }
                let text = String::from_utf8(bytes).expect("ASCII mutations");
                assert_eq!(kind(&parse(&text)), kind(&legacy_parse(&text)), "{text:?}");
                checked += 1;
            }
        }
        assert_eq!(checked, 4 * 4000);
    }
}
